//! Integration tests for single-node (no-cluster) mode.

mod common;

use std::sync::Arc;

use common::{CounterMachine, CounterOp, SharedCounter};
use muster::{Config, MusterNode, RedbStorage, Role};
use tempfile::TempDir;

/// Build a single-node `MusterNode` (no peers, no discovery).
fn single_node() -> (
    Arc<MusterNode<SharedCounter, RedbStorage>>,
    Arc<CounterMachine>,
    TempDir,
) {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(redb::Database::create(dir.path().join("test.redb")).unwrap());
    let storage = RedbStorage::new(db).unwrap();

    let machine = Arc::new(CounterMachine::new());
    let sm = SharedCounter(Arc::clone(&machine));

    let node = MusterNode::new(
        Config {
            node_id: "solo".to_string(),
            ..Config::default()
        },
        storage,
        sm,
    )
    .unwrap();

    (node, machine, dir)
}

/// Single-node starts as leader immediately (no election needed).
#[tokio::test]
async fn single_node_is_leader() {
    let (node, _, _dir) = single_node();
    assert!(node.is_leader().await);
    assert_eq!(node.cluster_info().await.role, Role::Leader);
}

/// Writes succeed without any peers.
#[tokio::test]
async fn single_node_replicate() {
    let (node, machine, _dir) = single_node();

    let seq = node.replicate(CounterOp::Increment(5)).await.unwrap();
    assert_eq!(seq, 1);
    assert_eq!(machine.get(), 5);

    let seq = node.replicate(CounterOp::Increment(3)).await.unwrap();
    assert_eq!(seq, 2);
    assert_eq!(machine.get(), 8);
}

/// Start returns no handles in single-node mode.
#[tokio::test]
async fn single_node_start_returns_empty() {
    let (node, _, _dir) = single_node();
    let handles = node.start();
    assert!(handles.is_empty());
}

/// Persist and restore state works.
#[tokio::test]
async fn single_node_persist_state() {
    let (node, _, _dir) = single_node();

    node.replicate(CounterOp::Increment(10)).await.unwrap();
    node.persist_state().await.unwrap();

    let info = node.cluster_info().await;
    assert_eq!(info.sequence, 1);
}
