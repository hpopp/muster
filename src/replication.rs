//! Quorum-based replication for write operations.

use std::sync::Arc;
use tracing::{debug, warn};

use crate::rpc::{ClusterMessage, ReplicateRequest};
use crate::storage::Storage;
use crate::{MusterError, MusterNode, StateMachine};

/// Fan out a replicated write to peers and await quorum.
///
/// Called by `MusterNode::replicate` after the leader has applied locally
/// and appended to its replication log.
pub(crate) async fn replicate_to_peers<S: StateMachine, T: Storage>(
    node: &Arc<MusterNode<S, T>>,
    sequence: u64,
    operation_bytes: &[u8],
) -> Result<u64, MusterError> {
    let info = gather_cluster_info(node).await;

    debug_assert!(
        info.quorum_size > 0 && info.quorum_size <= info.peers.len() + 1,
        "quorum size must be between 1 and cluster size"
    );

    let handles = fan_out_replicate(node, &info, sequence, operation_bytes);
    await_quorum(node, handles, info.quorum_size, sequence).await
}

struct ClusterSnapshot {
    peers: Vec<(String, String)>,
    leader_id: String,
    term: u64,
    quorum_size: usize,
}

async fn gather_cluster_info<S: StateMachine, T: Storage>(
    node: &MusterNode<S, T>,
) -> ClusterSnapshot {
    let cluster = node.cluster.read().await;
    ClusterSnapshot {
        peers: cluster
            .peer_states
            .iter()
            .map(|(id, p)| (id.clone(), p.address.clone()))
            .collect(),
        leader_id: node.config.node_id.clone(),
        term: cluster.current_term,
        quorum_size: cluster.quorum_size(),
    }
}

fn fan_out_replicate<S: StateMachine, T: Storage>(
    node: &MusterNode<S, T>,
    info: &ClusterSnapshot,
    sequence: u64,
    operation_bytes: &[u8],
) -> Vec<tokio::task::JoinHandle<Option<String>>> {
    info.peers
        .iter()
        .map(|(peer_id, peer_addr)| {
            let op = operation_bytes.to_vec();
            let lid = info.leader_id.clone();
            let term = info.term;
            let peer_id = peer_id.clone();
            let peer_addr = peer_addr.clone();
            let transport = Arc::clone(&node.transport);

            tokio::spawn(async move {
                match send_replicate(&transport, &peer_addr, &lid, term, sequence, op).await {
                    Ok(true) => Some(peer_id),
                    Ok(false) => None,
                    Err(e) => {
                        debug!(peer = %peer_id, error = %e, "Replication failed");
                        None
                    }
                }
            })
        })
        .collect()
}

async fn await_quorum<S: StateMachine, T: Storage>(
    _node: &MusterNode<S, T>,
    handles: Vec<tokio::task::JoinHandle<Option<String>>>,
    quorum_size: usize,
    sequence: u64,
) -> Result<u64, MusterError> {
    let mut acks = 1; // Count self

    for handle in handles {
        if let Ok(Some(peer_id)) = handle.await {
            acks += 1;
            debug!(peer = %peer_id, acks, quorum = quorum_size, "Replication ack received");

            if acks >= quorum_size {
                return Ok(sequence);
            }
        }
    }

    if acks >= quorum_size {
        Ok(sequence)
    } else {
        warn!(acks, quorum = quorum_size, "Failed to reach quorum");
        Err(MusterError::NoQuorum)
    }
}

async fn send_replicate(
    transport: &crate::ClusterTransport,
    peer_addr: &str,
    leader_id: &str,
    term: u64,
    sequence: u64,
    operation: Vec<u8>,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let request = ReplicateRequest {
        leader_id: leader_id.to_string(),
        term,
        sequence,
        operation,
    };

    let response = transport
        .send(
            peer_addr,
            ClusterMessage::ReplicateRequest(Box::new(request)),
        )
        .await?;

    match response {
        ClusterMessage::ReplicateResponse(resp) => Ok(resp.success),
        other => {
            warn!(peer = %peer_addr, "Unexpected replication response: {other:?}");
            Ok(false)
        }
    }
}
