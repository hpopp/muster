//! Pluggable peer discovery strategies.
//!
//! - **DNS**: Resolves a DNS name to find peers (Docker Compose + Kubernetes).
//! - **Static**: Uses a fixed list of peer addresses from configuration.

use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use tracing::{info, trace, warn};

/// Peer discovery strategy.
pub enum Discovery {
    Dns(DnsPoll),
    Static(StaticList),
}

impl Discovery {
    /// Discover current cluster peers.
    pub async fn discover_peers(&self) -> anyhow::Result<Vec<SocketAddr>> {
        match self {
            Discovery::Dns(d) => d.discover().await,
            Discovery::Static(d) => d.discover().await,
        }
    }
}

/// DNS-based peer discovery.
pub struct DnsPoll {
    dns_name: String,
    local_ip: Option<IpAddr>,
    port: u16,
}

impl DnsPoll {
    pub fn new(dns_name: String, port: u16) -> Self {
        let local_ip = detect_local_ip();
        if let Some(ip) = local_ip {
            info!(%ip, dns_name = %dns_name, port, "DNS discovery initialized");
        } else {
            warn!(dns_name = %dns_name, port, "DNS discovery initialized (could not detect local IP)");
        }
        Self {
            dns_name,
            port,
            local_ip,
        }
    }

    async fn discover(&self) -> anyhow::Result<Vec<SocketAddr>> {
        let lookup = format!("{}:{}", self.dns_name, self.port);

        let addrs: Vec<SocketAddr> = tokio::net::lookup_host(&lookup)
            .await?
            .filter(|addr| {
                if let Some(local_ip) = self.local_ip {
                    addr.ip() != local_ip
                } else {
                    true
                }
            })
            .collect();

        trace!(
            dns = %self.dns_name,
            peers = addrs.len(),
            "DNS discovery completed"
        );

        Ok(addrs)
    }
}

/// Static peer list discovery.
pub struct StaticList {
    peer_addrs: Vec<String>,
}

impl StaticList {
    pub fn new(peer_addrs: Vec<String>) -> Self {
        Self { peer_addrs }
    }

    async fn discover(&self) -> anyhow::Result<Vec<SocketAddr>> {
        let mut peers = Vec::new();
        for addr_str in &self.peer_addrs {
            match tokio::net::lookup_host(addr_str.as_str()).await {
                Ok(addrs) => peers.extend(addrs),
                Err(e) => {
                    warn!(peer = %addr_str, error = %e, "Failed to resolve static peer");
                }
            }
        }
        Ok(peers)
    }
}

/// Detect the local IP address of this node.
pub fn detect_local_ip() -> Option<IpAddr> {
    if let Ok(hostname) = std::env::var("HOSTNAME") {
        if let Ok(addrs) = (hostname.as_str(), 0u16).to_socket_addrs() {
            for addr in addrs {
                if !addr.ip().is_loopback() {
                    return Some(addr.ip());
                }
            }
        }
    }

    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    socket.local_addr().ok().map(|a| a.ip())
}

/// Compute the cluster TCP address for this node.
///
/// Uses `detect_local_ip` to find the routable IP and combines it
/// with the given cluster port. Falls back to `0.0.0.0` if detection fails.
pub(crate) fn compute_cluster_address(cluster_port: u16) -> String {
    if let Some(ip) = detect_local_ip() {
        format!("{ip}:{cluster_port}")
    } else {
        format!("0.0.0.0:{cluster_port}")
    }
}
