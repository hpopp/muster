//! Integration tests for follower → leader write forwarding over TCP.

mod common;

use std::time::Duration;

use common::{
    spawn_cluster, wait_for_convergence, wait_for_leader, wait_for_leader_address, CounterOp,
};

/// A write submitted to a follower is transparently forwarded to the leader.
#[tokio::test]
async fn follower_write_forwards_to_leader() {
    let nodes = spawn_cluster(3).await;

    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(10))
        .await
        .expect("cluster should elect a leader within 10s");

    // Wait for followers to learn the leader's address via heartbeats.
    assert!(
        wait_for_leader_address(&nodes, Duration::from_secs(5)).await,
        "followers should learn the leader address"
    );

    // Pick a follower.
    let follower_idx = (0..nodes.len()).find(|&i| i != leader_idx).unwrap();

    // Write through the follower — should be forwarded to the leader.
    let seq = nodes[follower_idx]
        .node
        .replicate(CounterOp::Increment(7))
        .await
        .expect("forwarded write should succeed");
    assert_eq!(seq, 1);

    // All nodes should converge.
    assert!(
        wait_for_convergence(&nodes, 7, Duration::from_secs(10)).await,
        "all nodes should converge to counter=7"
    );
}

/// Multiple writes through different followers all converge.
#[tokio::test]
async fn writes_from_multiple_followers_converge() {
    let nodes = spawn_cluster(3).await;

    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(10))
        .await
        .expect("cluster should elect a leader within 10s");

    assert!(
        wait_for_leader_address(&nodes, Duration::from_secs(5)).await,
        "followers should learn the leader address"
    );

    let followers: Vec<usize> = (0..nodes.len()).filter(|&i| i != leader_idx).collect();

    // Each follower writes 5 increments.
    for &fi in &followers {
        for _ in 0..5 {
            nodes[fi]
                .node
                .replicate(CounterOp::Increment(1))
                .await
                .expect("forwarded write should succeed");
        }
    }

    // 2 followers * 5 increments = 10.
    assert!(
        wait_for_convergence(&nodes, 10, Duration::from_secs(10)).await,
        "all nodes should converge to counter=10"
    );
}

/// Mixing leader and follower writes produces the correct total.
#[tokio::test]
async fn mixed_leader_and_follower_writes() {
    let nodes = spawn_cluster(3).await;

    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(10))
        .await
        .expect("cluster should elect a leader within 10s");

    assert!(
        wait_for_leader_address(&nodes, Duration::from_secs(5)).await,
        "followers should learn the leader address"
    );

    let follower_idx = (0..nodes.len()).find(|&i| i != leader_idx).unwrap();

    // Leader writes 3 increments.
    for _ in 0..3 {
        nodes[leader_idx]
            .node
            .replicate(CounterOp::Increment(1))
            .await
            .unwrap();
    }

    // Follower writes 7 increments.
    for _ in 0..7 {
        nodes[follower_idx]
            .node
            .replicate(CounterOp::Increment(1))
            .await
            .unwrap();
    }

    assert!(
        wait_for_convergence(&nodes, 10, Duration::from_secs(10)).await,
        "all nodes should converge to counter=10"
    );
}
