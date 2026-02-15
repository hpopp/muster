//! Integration tests for cluster info / observability.

mod common;

use std::time::Duration;

use common::{spawn_cluster, wait_for_leader, CounterOp};
use muster::Role;

/// `cluster_info` reflects correct role, term, and peer count.
#[tokio::test]
async fn cluster_info_reflects_state() {
    let nodes = spawn_cluster(3).await;

    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(10))
        .await
        .expect("cluster should elect a leader within 10s");

    let leader_info = nodes[leader_idx].node.cluster_info().await;
    assert_eq!(leader_info.role, Role::Leader);
    assert!(leader_info.term >= 1);
    assert_eq!(leader_info.peers.len(), 2);

    for (i, n) in nodes.iter().enumerate() {
        if i != leader_idx {
            let info = n.node.cluster_info().await;
            assert_eq!(info.role, Role::Follower);
            assert_eq!(info.peers.len(), 2);
        }
    }
}

/// Sequence is updated after writes.
#[tokio::test]
async fn cluster_info_sequence_updates() {
    let nodes = spawn_cluster(3).await;

    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(10))
        .await
        .expect("cluster should elect a leader within 10s");

    let info_before = nodes[leader_idx].node.cluster_info().await;
    assert_eq!(info_before.sequence, 0);

    nodes[leader_idx]
        .node
        .replicate(CounterOp::Increment(1))
        .await
        .unwrap();

    let info_after = nodes[leader_idx].node.cluster_info().await;
    assert_eq!(info_after.sequence, 1);
}

/// `leader_address` is populated on followers after heartbeats propagate.
#[tokio::test]
async fn followers_know_leader_address() {
    let nodes = spawn_cluster(3).await;

    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(10))
        .await
        .expect("cluster should elect a leader within 10s");

    // Let heartbeats propagate.
    tokio::time::sleep(Duration::from_millis(500)).await;

    for (i, n) in nodes.iter().enumerate() {
        if i != leader_idx {
            let addr = n.node.leader_address().await;
            assert!(
                addr.is_some(),
                "follower node-{} should know the leader address",
                i + 1
            );
        }
    }
}
