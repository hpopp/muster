/// Cluster configuration.
///
/// The consumer constructs this struct however they want (env vars, TOML, etc.)
/// — muster does no file I/O or env reading.
#[derive(Debug, Clone)]
pub struct Config {
    /// Unique node identifier.
    pub node_id: String,
    /// TCP port for inter-node cluster communication.
    pub cluster_port: u16,
    /// How often the leader sends heartbeats (ms).
    pub heartbeat_interval_ms: u64,
    /// How long a follower waits before starting an election (ms).
    pub election_timeout_ms: u64,
    /// Discovery configuration.
    pub discovery: DiscoveryConfig,
}

/// Peer discovery configuration.
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// DNS name to resolve for peer discovery (e.g., a Kubernetes headless service).
    /// Peers are resolved at the `cluster_port`.
    pub dns_name: Option<String>,
    /// Static peer addresses including the cluster port (e.g., `["node-2:9993", "node-3:9993"]`).
    pub peers: Vec<String>,
    /// How often to poll for peer changes (seconds).
    pub poll_interval_secs: u64,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            dns_name: None,
            peers: Vec::new(),
            poll_interval_secs: 5,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node_id: String::new(),
            cluster_port: 9993,
            heartbeat_interval_ms: 300,
            election_timeout_ms: 3000,
            discovery: DiscoveryConfig::default(),
        }
    }
}

impl Config {
    /// Returns true if no peers or DNS discovery is configured.
    pub fn is_single_node(&self) -> bool {
        self.discovery.peers.is_empty() && self.discovery.dns_name.is_none()
    }
}
