//! outpace daemon entry point: one shared swarm download per stream, fanned out to many
//! HTTP clients behind a clean `/streams/{network}/{id}` API (no acexy wrapper needed).

use ace_engine::ace_provider::AceProvider;
use ace_engine::broadcast::BroadcastRegistry;
use ace_engine::config::{load_or_create_identity, Config};
use ace_engine::http::{router, AppState, BroadcastState};
use ace_engine::manager::StreamManager;
use ace_engine::provider::ProviderRegistry;
use std::collections::HashMap;
use std::sync::Arc;

/// Default tracker for minted broadcasts (B1) — the same public UDP tracker `AceProvider`
/// falls back to for bare infohashes with none of their own. A freshly minted broadcast
/// self-announces to this tracker *and* DHT (`ace_provider::announce_infohash_periodically`,
/// spawned from `http.rs`'s ingest handler) as soon as it's minted, independent of whether
/// anything is locally following it — a pure origin needs to be discoverable too.
const DEFAULT_BROADCAST_TRACKERS: &[&str] = &["udp://t1.torrentstream.org:2710/announce"];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::default();
    if let Ok(bind) = std::env::var("OUTPACE_BIND") {
        config.bind = bind.parse()?;
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
    let identity = Arc::new(load_or_create_identity(&config.data_dir)?);
    eprintln!(
        "outpace: node_id={} data_dir={}",
        hex_node_id(&identity),
        config.data_dir.display()
    );

    // Register enabled providers. Only "ace" exists today; the registry is the seam for more.
    let seed_registry = ace_swarm::listen::SeedRegistry::new();
    let mut registry = ProviderRegistry::new();
    if config.networks.iter().any(|n| n == "ace") {
        // OUTPACE_ACE_PEERS=ip:port,ip:port — bootstrap peers for the proven live path
        // until DHT / ut_metadata discovery is wired.
        let bootstrap = std::env::var("OUTPACE_ACE_PEERS")
            .unwrap_or_default()
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect::<Vec<_>>();
        let provider = AceProvider::new(identity.clone(), config.bind.port())
            .with_bootstrap_peers(bootstrap)
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

fn hex_node_id(identity: &ace_wire::identity::Identity) -> String {
    identity
        .node_id()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
