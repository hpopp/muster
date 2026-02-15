//! Wire protocol types for inter-node cluster communication.
//!
//! All domain-specific data (write operations, snapshots) is carried as
//! opaque `Vec<u8>` — muster handles serialization at the handler level,
//! keeping these types free of generics.

use serde::{Deserialize, Serialize};

/// Tagged envelope for the TCP wire protocol (MessagePack encoded).
#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum ClusterMessage {
    ForwardWriteRequest(ForwardWriteRequest),
    ForwardWriteResponse(ForwardWriteResponse),
    HeartbeatRequest(HeartbeatRequest),
    HeartbeatResponse(HeartbeatResponse),
    ReplicateRequest(Box<ReplicateRequest>),
    ReplicateResponse(ReplicateResponse),
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    SyncRequest(SyncRequest),
    SyncResponse(Box<SyncResponse>),
}

// ============================================================================
// Forward Write (follower → leader)
// ============================================================================

/// Sent by a follower to the leader so the leader can run the full
/// replicate flow (log → quorum → apply) on behalf of the follower.
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ForwardWriteRequest {
    /// Serialized `S::WriteOp` (opaque bytes).
    pub op_bytes: Vec<u8>,
}

/// Leader's response after running the replicated write.
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ForwardWriteResponse {
    pub success: bool,
    pub sequence: u64,
    /// Error kind tag when `success` is false: "not_leader", "no_quorum",
    /// "storage", "state_machine", "serialization", "deserialization".
    pub error_kind: Option<String>,
    pub error_message: Option<String>,
}

// ============================================================================
// Heartbeat
// ============================================================================

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct HeartbeatRequest {
    /// Leader's advertise address (for follower forwarding).
    #[serde(default)]
    pub leader_address: Option<String>,
    pub leader_id: String,
    pub sequence: u64,
    pub term: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct HeartbeatResponse {
    pub sequence: u64,
    pub success: bool,
    pub term: u64,
}

// ============================================================================
// Replication
// ============================================================================

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ReplicateRequest {
    pub leader_id: String,
    /// Serialized `S::WriteOp` (opaque bytes).
    pub operation: Vec<u8>,
    pub sequence: u64,
    pub term: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ReplicateResponse {
    pub sequence: u64,
    pub success: bool,
}

// ============================================================================
// Voting
// ============================================================================

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct VoteRequest {
    pub candidate_id: String,
    pub last_sequence: u64,
    pub term: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct VoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

// ============================================================================
// Sync / Catchup
// ============================================================================

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct SyncRequest {
    pub from_sequence: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct SyncResponse {
    pub leader_sequence: u64,
    pub log_entries: Vec<LogEntry>,
    pub snapshot: Option<SnapshotPayload>,
}

/// A single replication log entry (sequence + opaque data).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct LogEntry {
    pub data: Vec<u8>,
    pub sequence: u64,
}

/// Full state snapshot sent to lagging followers.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct SnapshotPayload {
    pub data: Vec<u8>,
    pub sequence: u64,
}
