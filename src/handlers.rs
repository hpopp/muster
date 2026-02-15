//! Incoming cluster message handlers.
//!
//! Each function takes `Arc<MusterNode<S, T>>` + a request struct and returns
//! a response. The TCP server dispatches incoming `ClusterMessage` variants here.

use std::sync::Arc;
use tracing::debug;

use crate::catchup;
use crate::rpc::{
    ForwardWriteResponse, HeartbeatRequest, HeartbeatResponse, ReplicateRequest, ReplicateResponse,
    SyncRequest, SyncResponse, VoteRequest, VoteResponse,
};
use crate::state::Role;
use crate::storage::Storage;
use crate::{MusterNode, StateMachine};

// ============================================================================
// Forward Write
// ============================================================================

pub(crate) async fn handle_forward_write<S: StateMachine, T: Storage>(
    node: Arc<MusterNode<S, T>>,
    op_bytes: Vec<u8>,
) -> ForwardWriteResponse {
    // Deserialize the WriteOp from opaque bytes
    let op: S::WriteOp = match rmp_serde::from_slice(&op_bytes) {
        Ok(op) => op,
        Err(e) => {
            return ForwardWriteResponse {
                success: false,
                sequence: 0,
                error_kind: Some("deserialization".to_string()),
                error_message: Some(format!("Failed to deserialize forwarded write: {e}")),
            };
        }
    };

    // Run the leader's replicate flow (log → quorum → apply).
    // Uses replicate_local to avoid re-forwarding if we're no longer the leader.
    match node.replicate_local(op).await {
        Ok(sequence) => ForwardWriteResponse {
            success: true,
            sequence,
            error_kind: None,
            error_message: None,
        },
        Err(e) => {
            let error_kind = match &e {
                crate::MusterError::NotLeader { .. } => "not_leader",
                crate::MusterError::NoQuorum => "no_quorum",
                crate::MusterError::Storage(_) => "storage",
                crate::MusterError::StateMachine(_) => "state_machine",
                crate::MusterError::Serialization(_) => "serialization",
                crate::MusterError::Deserialization(_) => "deserialization",
            };
            ForwardWriteResponse {
                success: false,
                sequence: 0,
                error_kind: Some(error_kind.to_string()),
                error_message: Some(e.to_string()),
            }
        }
    }
}

// ============================================================================
// Heartbeat
// ============================================================================

pub(crate) async fn handle_heartbeat<S: StateMachine, T: Storage>(
    node: Arc<MusterNode<S, T>>,
    req: HeartbeatRequest,
) -> HeartbeatResponse {
    if let TermCheck::Stale {
        current_term,
        sequence,
    } = check_term(&node, req.term).await
    {
        return HeartbeatResponse {
            term: current_term,
            success: false,
            sequence,
        };
    }

    let (term, my_sequence, leader_sequence, leader_addr) = {
        let mut cluster = node.cluster.write().await;

        if req.term > cluster.current_term || cluster.role != Role::Follower {
            cluster.become_follower(req.term, Some(req.leader_id.clone()));
        }

        cluster.leader_id = Some(req.leader_id);
        if let Some(addr) = req.leader_address {
            cluster.leader_address = Some(addr);
        }
        cluster.update_heartbeat();

        (
            cluster.current_term,
            cluster.last_applied_sequence,
            req.sequence,
            cluster.leader_address.clone(),
        )
    };

    // If we're behind the leader, trigger a background sync
    if leader_sequence > my_sequence {
        if let Some(addr) = leader_addr {
            let sync_node = Arc::clone(&node);
            tokio::spawn(async move {
                if let Err(e) = catchup::request_sync(sync_node, &addr).await {
                    match e {
                        catchup::CatchupError::AlreadySyncing => {}
                        _ => tracing::warn!(error = %e, "Background sync failed."),
                    }
                }
            });
        }
    }

    HeartbeatResponse {
        term,
        success: true,
        sequence: my_sequence,
    }
}

// ============================================================================
// Replication
// ============================================================================

pub(crate) async fn handle_replicate<S: StateMachine, T: Storage>(
    node: Arc<MusterNode<S, T>>,
    req: ReplicateRequest,
) -> Result<ReplicateResponse, String> {
    if let TermCheck::Stale { sequence, .. } = check_term(&node, req.term).await {
        return Ok(ReplicateResponse {
            success: false,
            sequence,
        });
    }

    {
        let mut cluster = node.cluster.write().await;
        if req.term > cluster.current_term {
            cluster.become_follower(req.term, Some(req.leader_id.clone()));
        }
        cluster.update_heartbeat();
    }

    // Serialize the apply + log path
    let _repl_guard = node.replication_lock.lock().await;

    // Deserialize the WriteOp from opaque bytes
    let op: S::WriteOp = rmp_serde::from_slice(&req.operation)
        .map_err(|e| format!("Failed to deserialize operation: {e}"))?;

    // Apply to local state machine
    node.state_machine
        .apply(&op)
        .map_err(|e| format!("Failed to apply write: {e}"))?;

    // Append to replication log
    node.storage
        .append_log_entry(req.sequence, &req.operation)
        .map_err(|e| format!("Failed to append to replication log: {e}"))?;

    // Update sequence
    let mut cluster = node.cluster.write().await;
    cluster.last_applied_sequence = req.sequence;

    debug!(sequence = req.sequence, "Applied replicated write");

    Ok(ReplicateResponse {
        success: true,
        sequence: req.sequence,
    })
}

// ============================================================================
// Vote
// ============================================================================

pub(crate) async fn handle_vote<S: StateMachine, T: Storage>(
    node: Arc<MusterNode<S, T>>,
    req: VoteRequest,
) -> VoteResponse {
    if let TermCheck::Stale { current_term, .. } = check_term(&node, req.term).await {
        return VoteResponse {
            term: current_term,
            vote_granted: false,
        };
    }

    let mut cluster = node.cluster.write().await;

    if req.term > cluster.current_term {
        cluster.become_follower(req.term, None);
    }

    let log_up_to_date = req.last_sequence >= cluster.last_applied_sequence;
    let can_vote =
        cluster.voted_for.is_none() || cluster.voted_for.as_deref() == Some(&req.candidate_id);
    let grant = log_up_to_date && can_vote;

    if grant {
        cluster.voted_for = Some(req.candidate_id.clone());
        cluster.update_heartbeat();

        if let Err(e) = node.persist_cluster_state(&cluster) {
            tracing::warn!(error = %e, "Failed to persist vote");
        }

        debug!(candidate = %req.candidate_id, term = req.term, "Granted vote");
    }

    VoteResponse {
        term: cluster.current_term,
        vote_granted: grant,
    }
}

// ============================================================================
// Sync
// ============================================================================

pub(crate) async fn handle_sync<S: StateMachine, T: Storage>(
    node: Arc<MusterNode<S, T>>,
    req: SyncRequest,
) -> Result<SyncResponse, String> {
    node.ensure_leader().await.map_err(|e| e.to_string())?;

    catchup::handle_sync_request(node, req.from_sequence)
        .await
        .map_err(|e| format!("Sync failed: {e}"))
}

// ============================================================================
// Helpers
// ============================================================================

enum TermCheck {
    Stale { current_term: u64, sequence: u64 },
    Ok,
}

async fn check_term<S: StateMachine, T: Storage>(node: &MusterNode<S, T>, term: u64) -> TermCheck {
    let cluster = node.cluster.read().await;
    if term < cluster.current_term {
        TermCheck::Stale {
            current_term: cluster.current_term,
            sequence: cluster.last_applied_sequence,
        }
    } else {
        TermCheck::Ok
    }
}
