//! Shared test helpers for muster integration tests.
//!
//! Provides a trivial `CounterMachine` state machine and utilities for
//! spinning up multi-node clusters on localhost with real TCP.

// Each test binary compiles this module independently and only uses a subset
// of exports, so unused items are expected.
#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use muster::{Config, DiscoveryConfig, MusterNode, RedbStorage, StateMachine};
use tempfile::TempDir;

// ============================================================================
// CounterMachine — trivial state machine for testing
// ============================================================================

/// A trivial write operation: increment by N or reset to 0.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum CounterOp {
    Increment(u64),
    Reset,
}

/// Snapshot is just the current value.
pub type CounterSnapshot = u64;

/// A thread-safe counter that implements `StateMachine`.
pub struct CounterMachine {
    value: AtomicU64,
}

impl CounterMachine {
    pub fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::SeqCst)
    }
}

impl StateMachine for CounterMachine {
    type WriteOp = CounterOp;
    type Snapshot = CounterSnapshot;

    fn apply(&self, op: &Self::WriteOp) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match op {
            CounterOp::Increment(n) => {
                self.value.fetch_add(*n, Ordering::SeqCst);
            }
            CounterOp::Reset => {
                self.value.store(0, Ordering::SeqCst);
            }
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<Self::Snapshot, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.get())
    }

    fn restore(
        &self,
        snapshot: Self::Snapshot,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.value.store(snapshot, Ordering::SeqCst);
        Ok(())
    }
}

// ============================================================================
// Port allocation
// ============================================================================

/// Bind to port 0 and return the OS-assigned port.
///
/// The listener is dropped immediately so the port is available for the node
/// to bind. There's a small race window, but it's fine for tests.
pub fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

// ============================================================================
// Cluster helpers
// ============================================================================

/// A running test node with its temp dir guard and state machine handle.
pub struct TestNode {
    pub node: Arc<MusterNode<SharedCounter, RedbStorage>>,
    pub machine: Arc<CounterMachine>,
    _handles: Vec<tokio::task::JoinHandle<()>>,
    _dir: TempDir,
}

impl TestNode {
    pub async fn counter(&self) -> u64 {
        self.machine.get()
    }

    pub async fn is_leader(&self) -> bool {
        self.node.is_leader().await
    }

    pub async fn role(&self) -> muster::Role {
        self.node.cluster_info().await.role
    }
}

/// Spawn a single node that knows about the given peer addresses.
pub async fn spawn_node(node_id: &str, cluster_port: u16, peers: Vec<String>) -> TestNode {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(redb::Database::create(dir.path().join("test.redb")).unwrap());
    let storage = RedbStorage::new(db).unwrap();

    let machine = Arc::new(CounterMachine::new());

    // We need a wrapper that delegates to the Arc'd machine so the test
    // can read the counter value after handing ownership to MusterNode.
    let machine_clone = Arc::clone(&machine);
    let sm = SharedCounter(machine_clone);

    let config = Config {
        node_id: node_id.to_string(),
        cluster_port,
        heartbeat_interval_ms: 100, // Fast for tests
        election_timeout_ms: 500,   // Fast for tests
        discovery: DiscoveryConfig {
            dns_name: None,
            peers,
            poll_interval_secs: 1,
        },
    };

    let node = MusterNode::new(config, storage, sm).unwrap();
    let handles = node.start();

    TestNode {
        node,
        machine,
        _handles: handles,
        _dir: dir,
    }
}

/// Wrapper that delegates `StateMachine` to an `Arc<CounterMachine>` so we can
/// keep a handle to read the counter from tests.
pub struct SharedCounter(pub Arc<CounterMachine>);

impl StateMachine for SharedCounter {
    type WriteOp = CounterOp;
    type Snapshot = CounterSnapshot;

    fn apply(&self, op: &Self::WriteOp) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.0.apply(op)
    }

    fn snapshot(&self) -> Result<Self::Snapshot, Box<dyn std::error::Error + Send + Sync>> {
        self.0.snapshot()
    }

    fn restore(
        &self,
        snapshot: Self::Snapshot,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.0.restore(snapshot)
    }
}

/// Spawn a cluster of N nodes on localhost with random ports.
///
/// Returns nodes in order `[node-1, node-2, ..., node-N]`.
pub async fn spawn_cluster(n: usize) -> Vec<TestNode> {
    assert!(n >= 1, "cluster must have at least 1 node");

    // Allocate ports first so every node knows the full peer list at startup.
    let ports: Vec<u16> = (0..n).map(|_| free_port()).collect();

    let mut nodes = Vec::with_capacity(n);
    for i in 0..n {
        let node_id = format!("node-{}", i + 1);
        let peers: Vec<String> = ports
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, port)| format!("127.0.0.1:{port}"))
            .collect();

        nodes.push(spawn_node(&node_id, ports[i], peers).await);
    }

    nodes
}

/// Wait until exactly one node is the leader (up to `timeout`).
///
/// Returns the index of the leader node.
pub async fn wait_for_leader(nodes: &[TestNode], timeout: Duration) -> Option<usize> {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let mut leader_idx = None;
        let mut leader_count = 0;

        for (i, n) in nodes.iter().enumerate() {
            if n.is_leader().await {
                leader_idx = Some(i);
                leader_count += 1;
            }
        }

        if leader_count == 1 {
            return leader_idx;
        }

        if tokio::time::Instant::now() >= deadline {
            return None;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wait until all followers know the leader's address (up to `timeout`).
///
/// This is important for forwarding tests: a follower can't forward writes
/// until it has received at least one heartbeat from the leader.
pub async fn wait_for_leader_address(nodes: &[TestNode], timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let mut all_known = true;
        for n in nodes {
            if !n.is_leader().await && n.node.leader_address().await.is_none() {
                all_known = false;
                break;
            }
        }

        if all_known {
            return true;
        }

        if tokio::time::Instant::now() >= deadline {
            return false;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wait until all nodes converge to the expected counter value (up to `timeout`).
pub async fn wait_for_convergence(nodes: &[TestNode], expected: u64, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let mut all_match = true;
        for n in nodes {
            if n.counter().await != expected {
                all_match = false;
                break;
            }
        }

        if all_match {
            return true;
        }

        if tokio::time::Instant::now() >= deadline {
            return false;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
