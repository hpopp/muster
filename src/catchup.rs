//! Follower catch-up mechanism for syncing new/lagging nodes.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info};

use crate::rpc::{ClusterMessage, LogEntry, SnapshotPayload, SyncRequest, SyncResponse};
use crate::storage::Storage;
use crate::{MusterNode, StateMachine};

#[derive(Debug, Error)]
pub(crate) enum CatchupError {
    #[error("Network error: {0}")]
    Network(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("State machine error: {0}")]
    StateMachine(String),
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Sync already in progress")]
    AlreadySyncing,
}

/// Request sync from the leader.
pub(crate) async fn request_sync<S: StateMachine, T: Storage>(
    node: Arc<MusterNode<S, T>>,
    leader_addr: &str,
) -> Result<(), CatchupError> {
    if node
        .sync_in_progress
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        debug!("Sync already in progress, skipping.");
        return Err(CatchupError::AlreadySyncing);
    }

    let result = do_sync(Arc::clone(&node), leader_addr).await;
    node.sync_in_progress.store(false, Ordering::SeqCst);
    result
}

async fn do_sync<S: StateMachine, T: Storage>(
    node: Arc<MusterNode<S, T>>,
    leader_addr: &str,
) -> Result<(), CatchupError> {
    let my_sequence = {
        let cluster = node.cluster.read().await;
        cluster.last_applied_sequence
    };

    info!(my_sequence, leader = %leader_addr, "Requesting sync from leader.");

    let response = send_sync_request(&node.transport, leader_addr, my_sequence).await?;

    // Apply snapshot if provided
    if let Some(ref payload) = response.snapshot {
        let snapshot: S::Snapshot = rmp_serde::from_slice(&payload.data).map_err(|e| {
            CatchupError::Serialization(format!("Failed to deserialize snapshot: {e}"))
        })?;
        node.state_machine
            .restore(snapshot)
            .map_err(|e| CatchupError::StateMachine(e.to_string()))?;
        info!(sequence = payload.sequence, "Applied snapshot.");
    }

    // Apply log entries
    let entry_count = response.log_entries.len();
    for entry in &response.log_entries {
        let op: S::WriteOp = rmp_serde::from_slice(&entry.data).map_err(|e| {
            CatchupError::Serialization(format!("Failed to deserialize log entry: {e}"))
        })?;
        node.state_machine
            .apply(&op)
            .map_err(|e| CatchupError::StateMachine(e.to_string()))?;
        node.storage
            .append_log_entry(entry.sequence, &entry.data)
            .map_err(|e| CatchupError::Storage(e.to_string()))?;
    }

    // Update our sequence
    {
        let mut cluster = node.cluster.write().await;
        cluster.last_applied_sequence = response.leader_sequence;
    }

    info!(
        sequence = response.leader_sequence,
        entries = entry_count,
        "Sync complete."
    );
    Ok(())
}

async fn send_sync_request(
    transport: &crate::ClusterTransport,
    leader_addr: &str,
    from_sequence: u64,
) -> Result<SyncResponse, CatchupError> {
    let request = SyncRequest { from_sequence };

    let response = transport
        .send(leader_addr, ClusterMessage::SyncRequest(request))
        .await
        .map_err(|e| CatchupError::Network(format!("Failed to contact leader: {e}")))?;

    match response {
        ClusterMessage::SyncResponse(resp) => Ok(*resp),
        other => Err(CatchupError::Network(format!(
            "Unexpected sync response: {other:?}"
        ))),
    }
}

/// Handle a sync request from a follower (leader only).
pub(crate) async fn handle_sync_request<S: StateMachine, T: Storage>(
    node: Arc<MusterNode<S, T>>,
    from_sequence: u64,
) -> Result<SyncResponse, CatchupError> {
    let leader_sequence = node
        .storage
        .latest_sequence()
        .map_err(|e| CatchupError::Storage(e.to_string()))?;

    let lag = leader_sequence.saturating_sub(from_sequence);
    let needs_snapshot = from_sequence == 0 || lag > 10_000;

    let snapshot = if needs_snapshot {
        let snap = node
            .state_machine
            .snapshot()
            .map_err(|e| CatchupError::StateMachine(e.to_string()))?;
        let snap_bytes =
            rmp_serde::to_vec(&snap).map_err(|e| CatchupError::Serialization(e.to_string()))?;
        Some(SnapshotPayload {
            sequence: leader_sequence,
            data: snap_bytes,
        })
    } else {
        None
    };

    let start_sequence = snapshot
        .as_ref()
        .map(|s| s.sequence + 1)
        .unwrap_or(from_sequence + 1);

    let raw_entries = node
        .storage
        .read_log_from(start_sequence)
        .map_err(|e| CatchupError::Storage(e.to_string()))?;

    let log_entries: Vec<LogEntry> = raw_entries
        .into_iter()
        .map(|(seq, data)| LogEntry {
            sequence: seq,
            data,
        })
        .collect();

    info!(
        from_sequence,
        leader_sequence,
        snapshot = snapshot.is_some(),
        log_entries = log_entries.len(),
        "Serving sync request."
    );

    Ok(SyncResponse {
        snapshot,
        log_entries,
        leader_sequence,
    })
}
