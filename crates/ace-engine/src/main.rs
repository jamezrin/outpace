//! outpace daemon entry point: one shared swarm download per stream, fanned out to many
//! HTTP clients behind a clean `/streams/{network}/{id}` API (no acexy wrapper needed).

use ace_engine::ace_provider::AceProvider;
use ace_engine::config::{load_or_create_identity, Config};
use ace_engine::http::{router, AppState};
use ace_engine::manager::StreamManager;
use ace_engine::provider::ProviderRegistry;
use std::sync::Arc;

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
    let identity = Arc::new(load_or_create_identity(&config.data_dir)?);
    eprintln!(
        "outpace: node_id={} data_dir={}",
        hex_node_id(&identity),
        config.data_dir.display()
    );

    // Register enabled providers. Only "ace" exists today; the registry is the seam for more.
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
            .with_bootstrap_peers(bootstrap);
        registry.register(Arc::new(provider));
    }
    let networks: Vec<String> = registry.networks().iter().map(|s| s.to_string()).collect();

    let manager = StreamManager::new(registry);
    manager.spawn_reaper();
    let state = AppState { manager, networks: networks.clone() };

    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    eprintln!("outpace: listening on http://{} networks={:?}", config.bind, networks);
    eprintln!("  play:  http://{}/streams/ace/<infohash>.ts", config.bind);
    axum::serve(listener, router(state)).await?;
    Ok(())
}

fn hex_node_id(identity: &ace_wire::identity::Identity) -> String {
    identity.node_id().iter().map(|b| format!("{b:02x}")).collect()
}
