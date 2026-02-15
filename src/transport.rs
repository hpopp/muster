//! TCP transport layer for inter-node cluster communication.
//!
//! Each peer gets a pool of dedicated background tasks, each owning its own
//! TCP connection. Requests are distributed across the pool via round-robin.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio_util::bytes::{Bytes, BytesMut};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::debug;

use crate::rpc::ClusterMessage;

/// Number of TCP connections per peer.
const POOL_SIZE: usize = 4;

/// Error type for transport operations.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("Connection failed: {0}")]
    Connect(std::io::Error),
    #[error("Serialization error: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("Deserialization error: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
    #[error("Connection closed by peer")]
    ConnectionClosed,
    #[error("Peer task unavailable")]
    PeerUnavailable,
}

type PeerConnection = Framed<TcpStream, LengthDelimitedCodec>;
type PeerRequest = (Vec<u8>, oneshot::Sender<Result<Vec<u8>, TransportError>>);

struct PeerHandle {
    senders: Vec<mpsc::Sender<PeerRequest>>,
    next: AtomicUsize,
}

impl PeerHandle {
    fn pick_sender(&self) -> Option<mpsc::Sender<PeerRequest>> {
        let len = self.senders.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        for i in 0..len {
            let idx = (start + i) % len;
            let tx = &self.senders[idx];
            if !tx.is_closed() {
                return Some(tx.clone());
            }
        }
        None
    }
}

/// Manages outbound TCP connections to cluster peers.
///
/// Peer addresses are expected to be cluster TCP addresses (host:cluster_port).
pub struct ClusterTransport {
    peers: RwLock<HashMap<String, PeerHandle>>,
}

impl ClusterTransport {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            peers: RwLock::new(HashMap::new()),
        })
    }

    /// Send a message and wait for a response.
    pub(crate) async fn send(
        &self,
        peer_addr: &str,
        message: ClusterMessage,
    ) -> Result<ClusterMessage, TransportError> {
        let payload = rmp_serde::to_vec(&message)?;

        let tx = self.get_or_spawn_peer(peer_addr).await;

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send((payload, reply_tx))
            .await
            .map_err(|_| TransportError::PeerUnavailable)?;

        let response_bytes = reply_rx
            .await
            .map_err(|_| TransportError::PeerUnavailable)??;

        let msg: ClusterMessage = rmp_serde::from_slice(&response_bytes)?;
        Ok(msg)
    }

    /// Send a message without waiting for a response (fire-and-forget).
    #[allow(dead_code)]
    pub(crate) async fn send_no_reply(
        &self,
        peer_addr: &str,
        message: ClusterMessage,
    ) -> Result<(), TransportError> {
        let payload = rmp_serde::to_vec(&message)?;

        let tx = self.get_or_spawn_peer(peer_addr).await;

        let (reply_tx, _reply_rx) = oneshot::channel();
        tx.send((payload, reply_tx))
            .await
            .map_err(|_| TransportError::PeerUnavailable)?;

        Ok(())
    }

    /// Remove a peer's connection pool.
    #[allow(dead_code)]
    pub(crate) async fn disconnect(&self, peer_addr: &str) {
        self.peers.write().await.remove(peer_addr);
    }

    async fn get_or_spawn_peer(&self, peer_addr: &str) -> mpsc::Sender<PeerRequest> {
        {
            let peers = self.peers.read().await;
            if let Some(handle) = peers.get(peer_addr) {
                if let Some(tx) = handle.pick_sender() {
                    return tx;
                }
            }
        }

        let mut peers = self.peers.write().await;
        if let Some(handle) = peers.get(peer_addr) {
            if let Some(tx) = handle.pick_sender() {
                return tx;
            }
        }

        let handle = spawn_peer_pool(peer_addr.to_string());
        let tx = handle
            .pick_sender()
            .expect("freshly spawned pool must have live senders");
        peers.insert(peer_addr.to_string(), handle);
        tx
    }
}

impl std::fmt::Debug for ClusterTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterTransport").finish()
    }
}

fn spawn_peer_pool(addr: String) -> PeerHandle {
    let mut senders = Vec::with_capacity(POOL_SIZE);

    for conn_idx in 0..POOL_SIZE {
        let (tx, mut rx) = mpsc::channel::<PeerRequest>(64);
        let addr = addr.clone();

        tokio::spawn(async move {
            let mut conn: Option<PeerConnection> = None;

            while let Some((payload, reply_tx)) = rx.recv().await {
                let result = peer_send_recv(&mut conn, &addr, &payload).await;
                let result = result.map(|bytes| bytes.to_vec());
                let _ = reply_tx.send(result);
            }

            debug!(peer = %addr, conn_idx, "Peer pool task exiting (channel closed)");
        });

        senders.push(tx);
    }

    PeerHandle {
        senders,
        next: AtomicUsize::new(0),
    }
}

async fn peer_send_recv(
    conn: &mut Option<PeerConnection>,
    addr: &str,
    payload: &[u8],
) -> Result<BytesMut, TransportError> {
    if conn.is_none() {
        *conn = Some(connect(addr).await?);
    }

    match send_and_recv(conn.as_mut().unwrap(), payload).await {
        Ok(resp) => Ok(resp),
        Err(_) => {
            debug!(peer = %addr, "Connection lost, reconnecting");
            *conn = None;
            *conn = Some(connect(addr).await?);
            send_and_recv(conn.as_mut().unwrap(), payload).await
        }
    }
}

async fn connect(addr: &str) -> Result<PeerConnection, TransportError> {
    debug!(peer = %addr, "Connecting to peer");
    let stream = TcpStream::connect(addr)
        .await
        .map_err(TransportError::Connect)?;
    stream.set_nodelay(true).map_err(TransportError::Connect)?;

    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(64 * 1024 * 1024)
        .new_codec();

    Ok(Framed::new(stream, codec))
}

async fn send_and_recv(
    conn: &mut PeerConnection,
    payload: &[u8],
) -> Result<BytesMut, TransportError> {
    use futures_util::{SinkExt, StreamExt};

    conn.send(Bytes::copy_from_slice(payload))
        .await
        .map_err(|_| TransportError::ConnectionClosed)?;

    match conn.next().await {
        Some(Ok(frame)) => Ok(frame),
        Some(Err(_)) => Err(TransportError::ConnectionClosed),
        None => Err(TransportError::ConnectionClosed),
    }
}
