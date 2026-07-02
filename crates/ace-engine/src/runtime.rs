//! Shared outpace runtime setup for the daemon and CLI commands.

use crate::ace_provider::AceProvider;
use crate::broadcast::{Broadcast, BroadcastRegistry};
use crate::config::{load_or_create_identity, Config};
use crate::http::{router, AppState, BroadcastState};
use crate::manager::StreamManager;
use crate::provider::ProviderRegistry;
use std::collections::HashMap;
use std::net::SocketAddrV4;
use std::sync::Arc;

/// Default tracker for minted broadcasts (B1) — the same public UDP tracker `AceProvider`
/// falls back to for bare infohashes with none of their own. A freshly minted broadcast
/// self-announces to this tracker *and* DHT (`ace_provider::announce_infohash_periodically`,
/// spawned from `http.rs`'s ingest handler) as soon as it's minted, independent of whether
/// anything is locally following it — a pure origin needs to be discoverable too.
const DEFAULT_BROADCAST_TRACKERS: &[&str] = &["udp://t1.torrentstream.org:2710/announce"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BroadcastIngestUrls {
    pub raw: String,
    pub rtmp: String,
}

pub struct EngineRuntime {
    pub config: Config,
    pub networks: Vec<String>,
    pub manager: Arc<StreamManager>,
    pub seed_registry: ace_swarm::listen::SeedRegistry,
    pub broadcasts: BroadcastState,
    identity: Arc<ace_wire::identity::Identity>,
}

pub fn config_from_env() -> Result<Config, Box<dyn std::error::Error>> {
    let mut config = Config::default();
    if let Ok(bind) = std::env::var("OUTPACE_BIND") {
        config.bind = bind.parse()?;
    }
    if let Ok(bind) = std::env::var("OUTPACE_RTMP_BIND") {
        config.rtmp_bind = bind.parse()?;
    }
    if let Ok(dir) = std::env::var("OUTPACE_DATA_DIR") {
        config.data_dir = dir.into();
    }
    if let Ok(v) = std::env::var("OUTPACE_PEER_LISTEN") {
        config.peer_listen = v.parse()?;
    }
    if let Ok(v) = std::env::var("OUTPACE_SEED_STORE_BYTES") {
        config.seed_store_bytes = v.parse()?;
    }
    if let Ok(v) = std::env::var("OUTPACE_MAX_UNCHOKED") {
        config.max_unchoked = v.parse()?;
    }
    if let Ok(v) = std::env::var("OUTPACE_MAX_INBOUND") {
        config.max_inbound_peers = v.parse()?;
    }
    if let Ok(v) = std::env::var("OUTPACE_ENABLE_SEEDING") {
        config.enable_seeding = matches!(v.as_str(), "1" | "true");
    }
    if let Ok(v) = std::env::var("OUTPACE_ENABLE_INBOUND") {
        config.enable_inbound = matches!(v.as_str(), "1" | "true");
    }
    if let Ok(v) = std::env::var("OUTPACE_EXPERIMENTAL_ACE_COMPAT") {
        config.experimental_ace_compat = matches!(v.as_str(), "1" | "true");
    }
    Ok(config)
}

pub fn bootstrap_peers_from_env() -> Vec<SocketAddrV4> {
    // OUTPACE_ACE_PEERS=ip:port,ip:port — bootstrap peers for the proven live path
    // until DHT / ut_metadata discovery is wired.
    std::env::var("OUTPACE_ACE_PEERS")
        .unwrap_or_default()
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect()
}

pub fn broadcast_ingest_urls(
    http_bind: std::net::SocketAddr,
    rtmp_bind: std::net::SocketAddr,
    public_host: Option<String>,
    name: &str,
) -> BroadcastIngestUrls {
    let raw_host = public_host
        .as_deref()
        .map(str::to_string)
        .unwrap_or_else(|| http_bind.ip().to_string());
    let rtmp_host = public_host.unwrap_or_else(|| rtmp_bind.ip().to_string());
    BroadcastIngestUrls {
        raw: format!(
            "http://{}:{}/broadcast/{}",
            raw_host,
            http_bind.port(),
            name
        ),
        rtmp: format!("rtmp://{}:{}/live/{}", rtmp_host, rtmp_bind.port(), name),
    }
}

pub async fn build_runtime(
    config: Config,
    bootstrap_peers: Vec<SocketAddrV4>,
) -> Result<EngineRuntime, Box<dyn std::error::Error>> {
    let identity = Arc::new(load_or_create_identity(&config.data_dir)?);

    // Register enabled providers. Only "ace" exists today; the registry is the path for more.
    let seed_registry = ace_swarm::listen::SeedRegistry::new();
    let mut registry = ProviderRegistry::new();
    if config.networks.iter().any(|n| n == "ace") {
        let provider = AceProvider::new(identity.clone(), config.bind.port())
            .with_bootstrap_peers(bootstrap_peers)
            .with_seed_registry(seed_registry.clone())
            .with_seed_store_bytes(config.seed_store_bytes)
            .with_seeding_enabled(config.enable_seeding);
        registry.register(Arc::new(provider));
    }
    let networks: Vec<String> = registry.networks().iter().map(|s| s.to_string()).collect();

    let manager = StreamManager::new(registry);
    manager.spawn_reaper();
    let broadcasts = BroadcastState {
        registry: BroadcastRegistry::new(),
        seed_registry: seed_registry.clone(),
        trackers: DEFAULT_BROADCAST_TRACKERS
            .iter()
            .map(|s| s.to_string())
            .collect(),
        store_bytes: config.seed_store_bytes,
        inbound_peer_port: config.enable_inbound.then_some(config.peer_listen.port()),
    };

    Ok(EngineRuntime {
        config,
        networks,
        manager,
        seed_registry,
        broadcasts,
        identity,
    })
}

pub async fn serve_http(runtime: EngineRuntime) -> Result<(), Box<dyn std::error::Error>> {
    let EngineRuntime {
        config,
        networks,
        manager,
        seed_registry,
        broadcasts,
        identity,
    } = runtime;

    eprintln!(
        "outpace: node_id={} data_dir={}",
        hex_node_id(&identity),
        config.data_dir.display()
    );

    let state = AppState {
        manager,
        networks: networks.clone(),
        resolve_content_ids_in_getstream: true,
        ace_session_aliases: Arc::new(std::sync::Mutex::new(HashMap::new())),
        experimental_ace_compat: config.experimental_ace_compat,
        broadcasts: Some(broadcasts),
    };

    // Inbound seeding (S2): OFF by default so operators do not expose a peer listener unless
    // they opt in. Piece headers are now preserved/generated and official-consumer piece
    // acceptance is live-proven (note 33).
    if config.enable_inbound {
        let peer_listener = tokio::net::TcpListener::bind(config.peer_listen).await?;
        eprintln!(
            "outpace: inbound seeding ENABLED on {} (max {} peers)",
            config.peer_listen, config.max_inbound_peers
        );
        let listener_peer_id = ace_wire::handshake::random_peer_id();
        let inbound_registry = seed_registry.clone();
        let max_inbound = config.max_inbound_peers;
        let listener_identity = identity.clone();
        tokio::spawn(async move {
            ace_swarm::listen::PeerListener::serve(
                peer_listener,
                inbound_registry,
                listener_peer_id,
                [0u8; 8],
                max_inbound,
                listener_identity,
            )
            .await;
        });
    }

    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    eprintln!(
        "outpace: listening on http://{} networks={:?}",
        config.bind, networks
    );
    eprintln!("  play:  http://{}/streams/ace/<infohash>.ts", config.bind);
    axum::serve(listener, router(state)).await?;
    Ok(())
}

pub async fn mint_broadcast(runtime: &EngineRuntime, name: &str) -> Broadcast {
    let (bc, _) = runtime
        .broadcasts
        .registry
        .start_or_resume(
            name,
            name,
            &runtime.broadcasts.trackers,
            &runtime.broadcasts.seed_registry,
            runtime.broadcasts.store_bytes,
        )
        .await;
    bc
}

pub fn announce_broadcast(runtime: &EngineRuntime, bc: &Broadcast) {
    if let Some(port) = runtime.broadcasts.inbound_peer_port {
        let trackers = runtime.broadcasts.trackers.clone();
        tokio::spawn(crate::ace_provider::announce_infohash_periodically(
            trackers.clone(),
            bc.infohash,
            port,
        ));
        tokio::spawn(crate::ace_provider::announce_infohash_periodically(
            trackers,
            bc.content_id,
            port,
        ));
    }
}

fn hex_node_id(identity: &ace_wire::identity::Identity) -> String {
    identity
        .node_id()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broadcast_urls_use_raw_and_rtmp_labels() {
        let http = "127.0.0.1:6878".parse().unwrap();
        let rtmp = "127.0.0.1:1935".parse().unwrap();
        let urls = broadcast_ingest_urls(http, rtmp, None, "mychan");

        assert_eq!(urls.raw, "http://127.0.0.1:6878/broadcast/mychan");
        assert_eq!(urls.rtmp, "rtmp://127.0.0.1:1935/live/mychan");
    }

    #[test]
    fn broadcast_urls_use_public_host_for_displayed_hosts() {
        let http = "0.0.0.0:6878".parse().unwrap();
        let rtmp = "0.0.0.0:1935".parse().unwrap();
        let urls = broadcast_ingest_urls(http, rtmp, Some("stream.example".to_string()), "mychan");

        assert_eq!(urls.raw, "http://stream.example:6878/broadcast/mychan");
        assert_eq!(urls.rtmp, "rtmp://stream.example:1935/live/mychan");
    }
}
