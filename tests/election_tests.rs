//! Integration tests for leader election.

mod common;

use std::time::Duration;

use common::{spawn_cluster, wait_for_leader};
use muster::Role;

/// A 3-node cluster should converge on exactly one leader.
#[tokio::test]
async fn three_node_elects_one_leader() {
    let nodes = spawn_cluster(3).await;

    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(10))
        .await
        .expect("cluster should elect a leader within 10s");

    // Verify exactly one leader and two followers.
    let mut leaders = 0;
    let mut followers = 0;
    for n in &nodes {
        match n.role().await {
            Role::Leader => leaders += 1,
            Role::Follower => followers += 1,
            Role::Candidate => {} // transient, ignore
        }
    }

    assert_eq!(leaders, 1, "expected exactly 1 leader");
    assert_eq!(followers, 2, "expected 2 followers");
    assert!(nodes[leader_idx].is_leader().await);
}

/// A 5-node cluster should also elect exactly one leader.
#[tokio::test]
async fn five_node_elects_one_leader() {
    let nodes = spawn_cluster(5).await;

    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(10))
        .await
        .expect("cluster should elect a leader within 10s");

    assert!(nodes[leader_idx].is_leader().await);

    let info = nodes[leader_idx].node.cluster_info().await;
    assert_eq!(info.role, Role::Leader);
    assert_eq!(info.peers.len(), 4);
}

/// All nodes should agree on the same leader ID.
#[tokio::test]
async fn all_nodes_agree_on_leader() {
    let nodes = spawn_cluster(3).await;

    wait_for_leader(&nodes, Duration::from_secs(10))
        .await
        .expect("cluster should elect a leader within 10s");

    // Give heartbeats a moment to propagate leader identity.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut infos = Vec::new();
    for n in &nodes {
        infos.push(n.node.cluster_info().await);
    }

    let leader_ids: Vec<_> = infos.iter().filter_map(|i| i.leader_id.clone()).collect();
    assert!(
        !leader_ids.is_empty(),
        "at least one node should know the leader"
    );

    let expected = &leader_ids[0];
    for id in &leader_ids {
        assert_eq!(id, expected, "all nodes should agree on the leader");
    }
}
