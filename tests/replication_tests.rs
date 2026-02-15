//! Integration tests for write replication and state convergence.

mod common;

use std::time::Duration;

use common::{spawn_cluster, wait_for_convergence, wait_for_leader, CounterOp};

/// A write on the leader replicates to all followers.
#[tokio::test]
async fn leader_write_replicates_to_followers() {
    let nodes = spawn_cluster(3).await;

    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(10))
        .await
        .expect("cluster should elect a leader within 10s");

    // Write through the leader.
    let seq = nodes[leader_idx]
        .node
        .replicate(CounterOp::Increment(42))
        .await
        .expect("replicate should succeed");
    assert_eq!(seq, 1);

    // All nodes should converge to 42.
    assert!(
        wait_for_convergence(&nodes, 42, Duration::from_secs(10)).await,
        "all nodes should converge to counter=42"
    );
}

/// Multiple sequential writes maintain correct ordering and final state.
#[tokio::test]
async fn sequential_writes_converge() {
    let nodes = spawn_cluster(3).await;

    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(10))
        .await
        .expect("cluster should elect a leader within 10s");

    for i in 1..=10 {
        let seq = nodes[leader_idx]
            .node
            .replicate(CounterOp::Increment(1))
            .await
            .expect("replicate should succeed");
        assert_eq!(seq, i);
    }

    assert!(
        wait_for_convergence(&nodes, 10, Duration::from_secs(10)).await,
        "all nodes should converge to counter=10"
    );
}

/// Reset operation replicates correctly after increments.
#[tokio::test]
async fn reset_replicates() {
    let nodes = spawn_cluster(3).await;

    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(10))
        .await
        .expect("cluster should elect a leader within 10s");

    nodes[leader_idx]
        .node
        .replicate(CounterOp::Increment(100))
        .await
        .unwrap();

    assert!(
        wait_for_convergence(&nodes, 100, Duration::from_secs(10)).await,
        "all nodes should converge to 100"
    );

    nodes[leader_idx]
        .node
        .replicate(CounterOp::Reset)
        .await
        .unwrap();

    assert!(
        wait_for_convergence(&nodes, 0, Duration::from_secs(10)).await,
        "all nodes should converge to 0 after reset"
    );
}

/// Sequence numbers are monotonically increasing.
#[tokio::test]
async fn sequence_numbers_are_monotonic() {
    let nodes = spawn_cluster(3).await;

    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(10))
        .await
        .expect("cluster should elect a leader within 10s");

    let mut prev = 0u64;
    for _ in 0..20 {
        let seq = nodes[leader_idx]
            .node
            .replicate(CounterOp::Increment(1))
            .await
            .unwrap();
        assert!(seq > prev, "sequence should be monotonically increasing");
        prev = seq;
    }
}
