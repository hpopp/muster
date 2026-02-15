//! In-memory cluster state — roles, peers, terms, and quorum logic.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

/// Role of this node in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Candidate,
    Follower,
    Leader,
}

/// Status of a peer node.
#[derive(Debug, Clone)]
pub struct PeerState {
    pub address: String,
    pub last_seen: std::time::Instant,
    pub sequence: u64,
    pub status: String,
}

/// In-memory cluster state.
#[derive(Debug)]
pub struct ClusterState {
    pub(crate) current_term: u64,
    pub(crate) last_applied_sequence: u64,
    pub(crate) last_heartbeat: std::time::Instant,
    /// Direct address of the leader (set from heartbeats).
    pub(crate) leader_address: Option<String>,
    pub(crate) leader_id: Option<String>,
    pub(crate) peer_states: HashMap<String, PeerState>,
    pub(crate) role: Role,
    pub(crate) voted_for: Option<String>,
    pub(crate) votes_received: usize,
}

impl ClusterState {
    /// Create a new cluster state (called from `load_cluster_state` in lib.rs).
    pub(crate) fn new(
        role: Role,
        current_term: u64,
        voted_for: Option<String>,
        last_applied_sequence: u64,
    ) -> Self {
        Self {
            role,
            current_term,
            voted_for,
            last_applied_sequence,
            leader_id: None,
            leader_address: None,
            peer_states: HashMap::new(),
            votes_received: 0,
            last_heartbeat: std::time::Instant::now(),
        }
    }

    /// Start a new election.
    pub(crate) fn start_election(&mut self, node_id: &str) {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(node_id.to_string());
        self.votes_received = 1; // Vote for self
        self.leader_id = None;
        tracing::info!(term = self.current_term, "Starting election");
    }

    /// Become leader.
    pub(crate) fn become_leader(&mut self, node_id: &str) {
        self.role = Role::Leader;
        self.leader_id = Some(node_id.to_string());
        tracing::info!(term = self.current_term, "Became leader");
    }

    /// Become follower.
    pub(crate) fn become_follower(&mut self, term: u64, leader_id: Option<String>) {
        self.role = Role::Follower;
        self.current_term = term;
        self.leader_id = leader_id;
        self.leader_address = None;
        self.votes_received = 0;
        self.voted_for = None;
    }

    /// Record a vote received.
    pub(crate) fn record_vote(&mut self) {
        debug_assert_eq!(
            self.role,
            Role::Candidate,
            "only candidates should receive votes"
        );
        self.votes_received += 1;
    }

    /// Check if we have enough votes to become leader.
    pub(crate) fn has_majority(&self, cluster_size: usize) -> bool {
        self.votes_received > cluster_size / 2
    }

    /// Update heartbeat timestamp.
    pub(crate) fn update_heartbeat(&mut self) {
        self.last_heartbeat = std::time::Instant::now();
    }

    /// Check if heartbeat has timed out.
    pub(crate) fn heartbeat_timeout(&self, timeout_ms: u64) -> bool {
        self.last_heartbeat.elapsed().as_millis() > timeout_ms as u128
    }

    /// Update a peer's state.
    pub(crate) fn update_peer(&mut self, peer_id: &str, status: &str, sequence: u64) {
        if let Some(peer) = self.peer_states.get_mut(peer_id) {
            peer.status = status.to_string();
            peer.sequence = sequence;
            peer.last_seen = std::time::Instant::now();
        }
    }

    /// Get the leader's address if known.
    pub fn get_leader_address(&self) -> Option<String> {
        if let Some(ref addr) = self.leader_address {
            return Some(addr.clone());
        }
        let leader_id = self.leader_id.as_ref()?;
        self.peer_states.get(leader_id).map(|p| p.address.clone())
    }

    /// Current cluster size (discovered peers + self).
    pub(crate) fn cluster_size(&self) -> usize {
        self.peer_states.len() + 1
    }

    /// Required quorum size (majority).
    pub(crate) fn quorum_size(&self) -> usize {
        self.cluster_size() / 2 + 1
    }

    /// Update the set of known peers from discovery results.
    pub(crate) fn update_discovered_peers(&mut self, addrs: Vec<SocketAddr>) {
        let discovered: HashSet<String> = addrs.iter().map(|a| a.to_string()).collect();

        for addr_str in &discovered {
            if !self.peer_states.contains_key(addr_str) {
                self.peer_states.insert(
                    addr_str.clone(),
                    PeerState {
                        address: addr_str.clone(),
                        status: "discovered".to_string(),
                        sequence: 0,
                        last_seen: std::time::Instant::now(),
                    },
                );
                tracing::info!(peer = %addr_str, "Discovered new peer");
            }
        }

        let removed: Vec<String> = self
            .peer_states
            .keys()
            .filter(|k| !discovered.contains(*k))
            .cloned()
            .collect();

        for peer_id in &removed {
            self.peer_states.remove(peer_id);
            tracing::info!(peer = %peer_id, "Peer removed from discovery");
        }
    }
}
