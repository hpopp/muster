//! TCP server for inter-node cluster communication.

use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::bytes::Bytes;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, error, info, warn};

use crate::rpc::{ClusterMessage, ReplicateResponse, SyncResponse};
use crate::storage::Storage;
use crate::{handlers, MusterNode, StateMachine};

pub(crate) fn start_cluster_server<S: StateMachine, T: Storage>(
    node: Arc<MusterNode<S, T>>,
) -> JoinHandle<()> {
    let port = node.config.cluster_port;

    tokio::spawn(async move {
        let addr = format!("0.0.0.0:{port}");
        let listener = match TcpListener::bind(&addr).await {
            Ok(l) => {
                info!(addr = %addr, "Cluster TCP server listening");
                l
            }
            Err(e) => {
                error!(error = %e, addr = %addr, "Failed to bind cluster TCP server");
                return;
            }
        };

        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    debug!(peer = %peer_addr, "Accepted cluster connection");
                    let node = Arc::clone(&node);

                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(node, stream).await {
                            debug!(peer = %peer_addr, error = %e, "Cluster connection closed");
                        }
                    });
                }
                Err(e) => {
                    warn!(error = %e, "Failed to accept cluster connection");
                }
            }
        }
    })
}

async fn handle_connection<S: StateMachine, T: Storage>(
    node: Arc<MusterNode<S, T>>,
    stream: tokio::net::TcpStream,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    stream.set_nodelay(true)?;

    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(64 * 1024 * 1024)
        .new_codec();

    let mut framed = Framed::new(stream, codec);

    while let Some(frame) = framed.next().await {
        let frame = frame?;
        let msg: ClusterMessage = rmp_serde::from_slice(&frame)?;
        let response = dispatch(Arc::clone(&node), msg).await;
        let payload = rmp_serde::to_vec_named(&response)?;
        framed.send(Bytes::from(payload)).await?;
    }

    Ok(())
}

async fn dispatch<S: StateMachine, T: Storage>(
    node: Arc<MusterNode<S, T>>,
    msg: ClusterMessage,
) -> ClusterMessage {
    match msg {
        ClusterMessage::ForwardWriteRequest(req) => {
            let resp = handlers::handle_forward_write(node, req.op_bytes).await;
            ClusterMessage::ForwardWriteResponse(resp)
        }
        ClusterMessage::HeartbeatRequest(req) => {
            let resp = handlers::handle_heartbeat(node, req).await;
            ClusterMessage::HeartbeatResponse(resp)
        }
        ClusterMessage::ReplicateRequest(req) => {
            let resp = match handlers::handle_replicate(node, *req).await {
                Ok(resp) => resp,
                Err(e) => {
                    error!(error = %e, "Replicate handler error");
                    ReplicateResponse {
                        success: false,
                        sequence: 0,
                    }
                }
            };
            ClusterMessage::ReplicateResponse(resp)
        }
        ClusterMessage::VoteRequest(req) => {
            let resp = handlers::handle_vote(node, req).await;
            ClusterMessage::VoteResponse(resp)
        }
        ClusterMessage::SyncRequest(req) => {
            let resp = match handlers::handle_sync(node, req).await {
                Ok(resp) => resp,
                Err(e) => {
                    error!(error = %e, "Sync handler error");
                    SyncResponse {
                        leader_sequence: 0,
                        log_entries: vec![],
                        snapshot: None,
                    }
                }
            };
            ClusterMessage::SyncResponse(Box::new(resp))
        }
        other => {
            warn!(?other, "Unexpected message type received by server");
            ClusterMessage::HeartbeatResponse(crate::rpc::HeartbeatResponse {
                term: 0,
                success: false,
                sequence: 0,
            })
        }
    }
}
