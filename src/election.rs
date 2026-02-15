//! Leader election with term-based voting.

use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::discovery;
use crate::rpc::{ClusterMessage, VoteRequest};
use crate::state::Role;
use crate::storage::Storage;
use crate::{MusterNode, StateMachine};

pub(crate) fn start_election_monitor<S: StateMachine, T: Storage>(
    node: Arc<MusterNode<S, T>>,
) -> JoinHandle<()> {
    let election_timeout = node.config.election_timeout_ms;
    let check_interval = Duration::from_millis(election_timeout / 3);

    tokio::spawn(async move {
        // Random startup delay to stagger initial elections across nodes
        let startup_delay = rand::random::<u64>() % 350 + 150;
        debug!("Election monitor starting in {}ms", startup_delay);
        tokio::time::sleep(Duration::from_millis(startup_delay)).await;

        let jitter = rand::random::<u64>() % (election_timeout / 2);
        let effective_timeout = election_timeout + jitter;

        // Reset heartbeat timer after startup delay
        {
            let mut cluster = node.cluster.write().await;
            cluster.update_heartbeat();
        }

        let mut interval = tokio::time::interval(check_interval);
        loop {
            interval.tick().await;
            check_election(&node, effective_timeout).await;
        }
    })
}

async fn check_election<S: StateMachine, T: Storage>(node: &Arc<MusterNode<S, T>>, timeout: u64) {
    let mut cluster = node.cluster.write().await;

    match cluster.role {
        Role::Leader => {}
        Role::Follower => {
            if cluster.heartbeat_timeout(timeout) {
                info!("Heartbeat timeout, starting election");
                cluster.start_election(&node.config.node_id);
                drop(cluster);
                request_votes(node).await;
            }
        }
        Role::Candidate => {
            let cluster_size = cluster.cluster_size();
            if cluster.has_majority(cluster_size) {
                cluster.become_leader(&node.config.node_id);
                cluster.leader_address =
                    Some(discovery::compute_cluster_address(node.config.cluster_port));

                if let Err(e) = node.persist_cluster_state(&cluster) {
                    warn!(error = %e, "Failed to persist cluster state");
                }
            } else if cluster.heartbeat_timeout(timeout * 2) {
                debug!("Election timed out, restarting");
                cluster.start_election(&node.config.node_id);
                drop(cluster);
                request_votes(node).await;
            }
        }
    }
}

async fn request_votes<S: StateMachine, T: Storage>(node: &Arc<MusterNode<S, T>>) {
    let cluster = node.cluster.read().await;
    let term = cluster.current_term;
    let sequence = cluster.last_applied_sequence;
    let peers: Vec<_> = cluster
        .peer_states
        .iter()
        .map(|(id, p)| (id.clone(), p.address.clone()))
        .collect();
    drop(cluster);

    for (peer_id, peer_addr) in peers {
        let node = Arc::clone(node);

        tokio::spawn(async move {
            request_vote(&node, &peer_id, &peer_addr, term, sequence).await;
        });
    }
}

async fn request_vote<S: StateMachine, T: Storage>(
    node: &MusterNode<S, T>,
    peer_id: &str,
    peer_addr: &str,
    term: u64,
    sequence: u64,
) {
    let request = VoteRequest {
        candidate_id: node.config.node_id.clone(),
        term,
        last_sequence: sequence,
    };

    let response = node
        .transport
        .send(peer_addr, ClusterMessage::VoteRequest(request))
        .await;

    match response {
        Ok(ClusterMessage::VoteResponse(resp)) => {
            let mut cluster = node.cluster.write().await;

            if resp.term > cluster.current_term {
                cluster.become_follower(resp.term, None);
                return;
            }

            if resp.vote_granted && cluster.role == Role::Candidate && cluster.current_term == term
            {
                cluster.record_vote();
                debug!(peer = %peer_id, "Vote granted");
            }
        }
        Ok(other) => {
            warn!(peer = %peer_id, "Unexpected vote response: {other:?}");
        }
        Err(e) => {
            debug!(peer = %peer_id, error = %e, "Failed to request vote");
        }
    }
}
