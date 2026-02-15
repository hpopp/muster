//! muster — Batteries-included Raft-like clustering for Rust services.
//!
//! Provides leader election, quorum replication, peer discovery, and
//! state-machine synchronization over TCP + MessagePack.
//!
//! # Quick start
//!
//! 1. Implement [`StateMachine`] on your application type.
//! 2. Construct a [`Config`] and a storage backend ([`RedbStorage`] included).
//! 3. Create a [`MusterNode`] and call [`start`](MusterNode::start).
//! 4. Use [`replicate`](MusterNode::replicate) for quorum writes.

pub mod config;
pub mod discovery;
pub mod storage;
pub mod transport;

mod catchup;
mod election;
mod handlers;
mod heartbeat;
mod replication;
mod rpc;
mod server;
mod state;

pub use config::{Config, DiscoveryConfig};
pub use state::{ClusterState, PeerState, Role};
pub use storage::{RedbStorage, RedbStorageError, Storage};
pub use transport::ClusterTransport;

use std::fmt::Debug;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;

// ============================================================================
// StateMachine trait
// ============================================================================

/// A state machine that muster replicates across the cluster.
///
/// Implement this trait on your application type to get clustering support.
pub trait StateMachine: Send + Sync + 'static {
    /// The type of write operations replicated across the cluster.
    type WriteOp: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static;

    /// A serializable snapshot of the full application state.
    type Snapshot: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Apply a single write operation to the local state.
    fn apply(&self, op: &Self::WriteOp) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Create a snapshot of the current state (for syncing lagging followers).
    fn snapshot(&self) -> Result<Self::Snapshot, Box<dyn std::error::Error + Send + Sync>>;

    /// Restore state from a snapshot.
    fn restore(
        &self,
        snapshot: Self::Snapshot,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

// ============================================================================
// NodeState — persisted cluster metadata
// ============================================================================

/// Persistent node state (serialized to storage).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeState {
    pub current_term: u64,
    pub last_applied_sequence: u64,
    pub node_id: String,
    pub voted_for: Option<String>,
}

impl NodeState {
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            current_term: 0,
            voted_for: None,
            last_applied_sequence: 0,
        }
    }
}

// ============================================================================
// MusterError
// ============================================================================

/// Errors returned by muster operations.
#[derive(Debug, Error)]
pub enum MusterError {
    #[error("Deserialization error: {0}")]
    Deserialization(String),
    #[error("Failed to reach quorum")]
    NoQuorum,
    #[error("Not the leader")]
    NotLeader { leader_address: Option<String> },
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("State machine error: {0}")]
    StateMachine(String),
    #[error("Storage error: {0}")]
    Storage(String),
}

// ============================================================================
// MusterNode
// ============================================================================

/// A cluster node that replicates state machine operations.
///
/// Generic over `S` (your [`StateMachine`]) and `T` (your [`Storage`] backend).
pub struct MusterNode<S: StateMachine, T: Storage> {
    pub(crate) cluster: RwLock<ClusterState>,
    pub(crate) config: Config,
    pub(crate) replication_lock: Mutex<()>,
    pub(crate) state_machine: S,
    pub(crate) storage: T,
    pub(crate) sync_in_progress: AtomicBool,
    pub(crate) transport: Arc<ClusterTransport>,
}

/// Convenience alias for `MusterNode` using the default `RedbStorage` backend.
pub type RedbNode<S> = MusterNode<S, RedbStorage>;

impl<S: StateMachine, T: Storage> MusterNode<S, T> {
    /// Create a new cluster node.
    ///
    /// Loads persisted cluster state from `storage` (or initializes fresh state).
    pub fn new(config: Config, storage: T, state_machine: S) -> Result<Arc<Self>, MusterError> {
        let cluster_state = load_cluster_state(&config, &storage)?;
        let transport = ClusterTransport::new();

        Ok(Arc::new(Self {
            cluster: RwLock::new(cluster_state),
            config,
            replication_lock: Mutex::new(()),
            state_machine,
            storage,
            sync_in_progress: AtomicBool::new(false),
            transport,
        }))
    }

    /// Start cluster background tasks (heartbeat, election, discovery, TCP server).
    ///
    /// Returns join handles for the spawned tasks. In single-node mode, returns
    /// an empty vec (no cluster tasks needed).
    pub fn start(self: &Arc<Self>) -> Vec<JoinHandle<()>> {
        if self.config.is_single_node() {
            tracing::info!("Running in single-node mode (no peers configured)");
            return vec![];
        }

        let server_handle = server::start_cluster_server(Arc::clone(self));
        let tasks_handle = start_cluster_tasks(Arc::clone(self));

        vec![server_handle, tasks_handle]
    }

    /// Replicate a write operation across the cluster.
    ///
    /// - **Leader / single-node**: logs the operation, fans out to followers
    ///   (if any), waits for quorum, then applies locally.
    /// - **Follower**: transparently forwards the write to the leader via TCP.
    ///   The leader executes the full replicate flow and returns the result.
    ///
    /// Returns the log sequence number on success.
    pub async fn replicate(self: &Arc<Self>, operation: S::WriteOp) -> Result<u64, MusterError> {
        // Single-node or leader: run locally
        if self.config.is_single_node() || self.is_leader().await {
            return self.replicate_local(operation).await;
        }

        // Follower: forward to leader via TCP
        self.forward_to_leader(operation).await
    }

    /// Run the replicate flow locally (leader path only).
    ///
    /// Called directly by the leader and by the `ForwardWrite` handler.
    /// Returns `NotLeader` if this node is not the leader.
    pub(crate) async fn replicate_local(
        self: &Arc<Self>,
        operation: S::WriteOp,
    ) -> Result<u64, MusterError> {
        if !self.config.is_single_node() {
            self.ensure_leader().await?;
        }

        // Serialize and append to replication log
        let op_bytes =
            rmp_serde::to_vec(&operation).map_err(|e| MusterError::Serialization(e.to_string()))?;
        let sequence = self
            .storage
            .append_next_log_entry(&op_bytes)
            .map_err(|e| MusterError::Storage(e.to_string()))?;

        // Update local sequence
        {
            let mut cluster = self.cluster.write().await;
            cluster.last_applied_sequence = sequence;
        }

        if !self.config.is_single_node() {
            // Fan out to followers and await quorum
            replication::replicate_to_peers(self, sequence, &op_bytes).await?;
        }

        // Apply to local state machine (after quorum in cluster mode)
        self.state_machine
            .apply(&operation)
            .map_err(|e| MusterError::StateMachine(e.to_string()))?;

        Ok(sequence)
    }

    /// Forward a write operation to the current leader over TCP.
    async fn forward_to_leader(
        self: &Arc<Self>,
        operation: S::WriteOp,
    ) -> Result<u64, MusterError> {
        let leader_addr = self.leader_address().await.ok_or(MusterError::NotLeader {
            leader_address: None,
        })?;

        let op_bytes =
            rmp_serde::to_vec(&operation).map_err(|e| MusterError::Serialization(e.to_string()))?;

        let response = self
            .transport
            .send(
                &leader_addr,
                rpc::ClusterMessage::ForwardWriteRequest(rpc::ForwardWriteRequest { op_bytes }),
            )
            .await
            .map_err(|e| MusterError::Storage(format!("Forward to leader failed: {e}")))?;

        match response {
            rpc::ClusterMessage::ForwardWriteResponse(resp) if resp.success => Ok(resp.sequence),
            rpc::ClusterMessage::ForwardWriteResponse(resp) => {
                let msg = resp.error_message.unwrap_or_default();
                match resp.error_kind.as_deref() {
                    Some("not_leader") => Err(MusterError::NotLeader {
                        leader_address: None,
                    }),
                    Some("no_quorum") => Err(MusterError::NoQuorum),
                    _ => Err(MusterError::Storage(msg)),
                }
            }
            other => Err(MusterError::Storage(format!(
                "Unexpected forward response: {other:?}"
            ))),
        }
    }

    /// Get information about the current cluster state.
    pub async fn cluster_info(&self) -> ClusterInfo {
        let cluster = self.cluster.read().await;
        ClusterInfo {
            node_id: self.config.node_id.clone(),
            role: cluster.role,
            term: cluster.current_term,
            sequence: cluster.last_applied_sequence,
            leader_id: cluster.leader_id.clone(),
            leader_address: cluster.get_leader_address(),
            peers: cluster
                .peer_states
                .iter()
                .map(|(id, p)| PeerInfo {
                    id: id.clone(),
                    address: p.address.clone(),
                    status: p.status.clone(),
                    sequence: p.sequence,
                })
                .collect(),
        }
    }

    /// Check if this node is the current leader.
    pub async fn is_leader(&self) -> bool {
        let cluster = self.cluster.read().await;
        cluster.role == Role::Leader
    }

    /// Get the leader's address, if known.
    pub async fn leader_address(&self) -> Option<String> {
        let cluster = self.cluster.read().await;
        cluster.get_leader_address()
    }

    /// Persist cluster state to storage. Call this on graceful shutdown.
    pub async fn persist_state(&self) -> Result<(), MusterError> {
        let cluster = self.cluster.read().await;
        self.persist_cluster_state(&cluster)
    }

    // -- Internal helpers --

    pub(crate) async fn ensure_leader(&self) -> Result<(), MusterError> {
        let cluster = self.cluster.read().await;
        if cluster.role != Role::Leader {
            return Err(MusterError::NotLeader {
                leader_address: cluster.get_leader_address(),
            });
        }
        Ok(())
    }

    pub(crate) fn persist_cluster_state(&self, cluster: &ClusterState) -> Result<(), MusterError> {
        let state = NodeState {
            node_id: self.config.node_id.clone(),
            current_term: cluster.current_term,
            voted_for: cluster.voted_for.clone(),
            last_applied_sequence: cluster.last_applied_sequence,
        };
        let data =
            rmp_serde::to_vec(&state).map_err(|e| MusterError::Serialization(e.to_string()))?;
        self.storage
            .put_node_state(&data)
            .map_err(|e| MusterError::Storage(e.to_string()))?;
        Ok(())
    }
}

// ============================================================================
// ClusterInfo — public read-only cluster state
// ============================================================================

/// Information about the cluster state (returned by [`MusterNode::cluster_info`]).
#[derive(Debug, Clone)]
pub struct ClusterInfo {
    pub node_id: String,
    pub role: Role,
    pub term: u64,
    pub sequence: u64,
    pub leader_id: Option<String>,
    pub leader_address: Option<String>,
    pub peers: Vec<PeerInfo>,
}

/// Information about a single peer node.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub id: String,
    pub address: String,
    pub status: String,
    pub sequence: u64,
}

// ============================================================================
// Internal orchestration
// ============================================================================

fn load_cluster_state<T: Storage>(
    config: &Config,
    storage: &T,
) -> Result<ClusterState, MusterError> {
    let node_state = match storage
        .get_node_state()
        .map_err(|e| MusterError::Storage(e.to_string()))?
    {
        Some(data) => {
            rmp_serde::from_slice(&data).map_err(|e| MusterError::Deserialization(e.to_string()))?
        }
        None => {
            let state = NodeState::new(config.node_id.clone());
            let data =
                rmp_serde::to_vec(&state).map_err(|e| MusterError::Serialization(e.to_string()))?;
            storage
                .put_node_state(&data)
                .map_err(|e| MusterError::Storage(e.to_string()))?;
            state
        }
    };

    let role = if config.is_single_node() {
        Role::Leader
    } else {
        Role::Follower
    };

    Ok(ClusterState::new(
        role,
        node_state.current_term,
        node_state.voted_for,
        node_state.last_applied_sequence,
    ))
}

fn start_cluster_tasks<S: StateMachine, T: Storage>(node: Arc<MusterNode<S, T>>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let heartbeat_handle = heartbeat::start_heartbeat_task(Arc::clone(&node));
        let election_handle = election::start_election_monitor(Arc::clone(&node));
        let disc = build_discovery(&node.config);

        if let Some(disc) = disc {
            let poll_interval =
                std::time::Duration::from_secs(node.config.discovery.poll_interval_secs);
            let discovery_handle = start_discovery_task(Arc::clone(&node), disc, poll_interval);

            tokio::select! {
                _ = heartbeat_handle => {
                    tracing::error!("Heartbeat task ended unexpectedly");
                }
                _ = election_handle => {
                    tracing::error!("Election monitor ended unexpectedly");
                }
                _ = discovery_handle => {
                    tracing::error!("Discovery task ended unexpectedly");
                }
            }
        } else {
            tokio::select! {
                _ = heartbeat_handle => {
                    tracing::error!("Heartbeat task ended unexpectedly");
                }
                _ = election_handle => {
                    tracing::error!("Election monitor ended unexpectedly");
                }
            }
        }
    })
}

fn start_discovery_task<S: StateMachine, T: Storage>(
    node: Arc<MusterNode<S, T>>,
    disc: discovery::Discovery,
    poll_interval: std::time::Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(poll_interval);
        loop {
            interval.tick().await;
            match disc.discover_peers().await {
                Ok(peers) => {
                    let mut cluster = node.cluster.write().await;
                    cluster.update_discovered_peers(peers);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Peer discovery failed");
                }
            }
        }
    })
}

fn build_discovery(config: &Config) -> Option<discovery::Discovery> {
    if config.is_single_node() {
        return None;
    }

    if let Some(ref dns_name) = config.discovery.dns_name {
        Some(discovery::Discovery::Dns(discovery::DnsPoll::new(
            dns_name.clone(),
            config.cluster_port,
        )))
    } else if !config.discovery.peers.is_empty() {
        Some(discovery::Discovery::Static(discovery::StaticList::new(
            config.discovery.peers.clone(),
        )))
    } else {
        None
    }
}
