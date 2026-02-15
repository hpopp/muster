//! Heartbeat mechanism for leader-follower communication.

use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::discovery;
use crate::rpc::{ClusterMessage, HeartbeatRequest};
use crate::state::Role;
use crate::storage::Storage;
use crate::{MusterNode, StateMachine};

pub(crate) fn start_heartbeat_task<S: StateMachine, T: Storage>(
    node: Arc<MusterNode<S, T>>,
) -> JoinHandle<()> {
    let heartbeat_interval = Duration::from_millis(node.config.heartbeat_interval_ms);

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(heartbeat_interval);
        loop {
            interval.tick().await;
            send_heartbeats(&node).await;
        }
    })
}

async fn send_heartbeats<S: StateMachine, T: Storage>(node: &Arc<MusterNode<S, T>>) {
    let cluster = node.cluster.read().await;
    if cluster.role != Role::Leader {
        return;
    }

    let info = gather_heartbeat_info(node, &cluster);
    drop(cluster);

    for (peer_id, peer_addr) in &info.peers {
        let node = Arc::clone(node);
        let peer_id = peer_id.clone();
        let peer_addr = peer_addr.clone();
        let info = info.clone();

        tokio::spawn(async move {
            heartbeat_peer(&node, &peer_id, &peer_addr, &info).await;
        });
    }
}

#[derive(Clone)]
struct HeartbeatInfo {
    peers: Vec<(String, String)>,
    leader_id: String,
    leader_address: String,
    term: u64,
    sequence: u64,
}

fn gather_heartbeat_info<S: StateMachine, T: Storage>(
    node: &MusterNode<S, T>,
    cluster: &crate::ClusterState,
) -> HeartbeatInfo {
    HeartbeatInfo {
        peers: cluster
            .peer_states
            .iter()
            .map(|(id, p)| (id.clone(), p.address.clone()))
            .collect(),
        leader_id: node.config.node_id.clone(),
        leader_address: discovery::compute_cluster_address(node.config.cluster_port),
        term: cluster.current_term,
        sequence: cluster.last_applied_sequence,
    }
}

async fn heartbeat_peer<S: StateMachine, T: Storage>(
    node: &MusterNode<S, T>,
    peer_id: &str,
    peer_addr: &str,
    info: &HeartbeatInfo,
) {
    let request = HeartbeatRequest {
        leader_address: Some(info.leader_address.clone()),
        leader_id: info.leader_id.clone(),
        sequence: info.sequence,
        term: info.term,
    };

    let response = node
        .transport
        .send(peer_addr, ClusterMessage::HeartbeatRequest(request))
        .await;

    match response {
        Ok(ClusterMessage::HeartbeatResponse(resp)) => {
            let mut cluster = node.cluster.write().await;

            if resp.term > cluster.current_term {
                cluster.become_follower(resp.term, None);
                warn!(peer_term = resp.term, "Peer has higher term, stepping down");
            } else {
                cluster.update_peer(peer_id, "synced", resp.sequence);
            }
        }
        Ok(other) => {
            warn!(peer = %peer_id, "Unexpected heartbeat response: {other:?}");
        }
        Err(e) => {
            debug!(peer = %peer_id, error = %e, "Failed to send heartbeat");
            let mut cluster = node.cluster.write().await;
            cluster.update_peer(peer_id, "unreachable", 0);
        }
    }
}
