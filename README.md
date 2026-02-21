[![CI](https://github.com/hpopp/muster/actions/workflows/ci.yml/badge.svg)](https://github.com/hpopp/muster/actions/workflows/ci.yml)
[![Version](https://img.shields.io/badge/version-0.2.0-orange.svg)](https://github.com/hpopp/muster/commits/main)
[![Last Updated](https://img.shields.io/github/last-commit/hpopp/muster.svg)](https://github.com/hpopp/muster/commits/main)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

# Muster

Batteries-included Raft-like clustering for Rust services.

Provides leader election, quorum replication, peer discovery, and state-machine synchronization over TCP + MessagePack.

## Features

- **Leader election** with term-based voting and randomized timeouts
- **Quorum replication** -- writes are committed only after a majority of nodes acknowledge
- **Transparent leader forwarding** -- followers automatically forward writes to the leader over TCP
- **Peer discovery** via DNS (Kubernetes headless services, Docker Compose) or static peer lists
- **Pluggable storage** -- bring your own backend via the `Storage` trait, or use the included `RedbStorage`
- **State machine abstraction** -- implement the `StateMachine` trait to replicate any application logic
- **Snapshot sync** -- lagging followers catch up via full state snapshots

## Quick Start

### 1. Implement `StateMachine`

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
enum MyWriteOp {
    Set { key: String, value: String },
    Delete { key: String },
}

#[derive(Serialize, Deserialize)]
struct MySnapshot {
    entries: Vec<(String, String)>,
}

struct MyApp { /* your state */ }

impl muster::StateMachine for MyApp {
    type WriteOp = MyWriteOp;
    type Snapshot = MySnapshot;

    fn apply(&self, op: &MyWriteOp) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Apply the write to your local state
        Ok(())
    }

    fn snapshot(&self) -> Result<MySnapshot, Box<dyn std::error::Error + Send + Sync>> {
        // Return a serializable snapshot of your full state
        Ok(MySnapshot { entries: vec![] })
    }

    fn restore(&self, snapshot: MySnapshot) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Restore state from a snapshot
        Ok(())
    }
}
```

### 2. Configure and start a node

```rust
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = muster::Config {
        node_id: "node-1".to_string(),
        cluster_port: 9993,
        discovery: muster::DiscoveryConfig {
            dns_name: Some("my-service".to_string()),
            ..Default::default()
        },
        ..Default::default()
    };

    // Share a redb instance with your app, or bring your own Storage impl
    let db = Arc::new(redb::Database::create("data/myapp.redb")?);
    let storage = muster::RedbStorage::new(db.clone())?;
    let app = MyApp { /* ... */ };

    let node = muster::MusterNode::new(config, storage, app)?;
    let handles = node.start();

    // Replicate a write -- works on any node (followers forward to the leader)
    node.replicate(MyWriteOp::Set {
        key: "hello".into(),
        value: "world".into(),
    }).await?;

    Ok(())
}
```

### 3. Query cluster state

```rust
let info = node.cluster_info().await;
println!("Role: {:?}, Leader: {:?}", info.role, info.leader_id);

let is_leader = node.is_leader().await;
let leader_addr = node.leader_address().await;
```

## Storage

Muster's internal state (replication log, node metadata) is stored via the `Storage` trait. The included `RedbStorage` shares an `Arc<redb::Database>` with your application using `_muster_`-prefixed table names to avoid collisions.

To use a different backend, implement the `Storage` trait:

```rust
pub trait Storage: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    fn append_log_entry(&self, sequence: u64, data: &[u8]) -> Result<(), Self::Error>;
    fn append_next_log_entry(&self, data: &[u8]) -> Result<u64, Self::Error>;
    fn read_log_from(&self, from_sequence: u64) -> Result<Vec<(u64, Vec<u8>)>, Self::Error>;
    fn latest_sequence(&self) -> Result<u64, Self::Error>;
    fn get_node_state(&self) -> Result<Option<Vec<u8>>, Self::Error>;
    fn put_node_state(&self, data: &[u8]) -> Result<(), Self::Error>;
}
```

## Configuration

`Config` is a plain struct -- muster does no file or environment reading. Build it however you want.

| Field                          | Description                           | Default    |
| ------------------------------ | ------------------------------------- | ---------- |
| `node_id`                      | Unique node identifier                | (required) |
| `cluster_port`                 | TCP port for inter-node communication | `9993`     |
| `heartbeat_interval_ms`        | Leader heartbeat interval             | `300`      |
| `election_timeout_ms`          | Follower election timeout             | `3000`     |
| `discovery.dns_name`           | DNS name for peer discovery           | `None`     |
| `discovery.peers`              | Static peer addresses (host:port)     | `[]`       |
| `discovery.poll_interval_secs` | Discovery poll interval               | `5`        |

When no peers or DNS name is configured, the node runs in **single-node mode** (no cluster tasks spawned, all writes applied locally).

## License

Copyright (c) 2026 Henry Popp

This project is MIT licensed. See the [LICENSE](LICENSE) for details.
