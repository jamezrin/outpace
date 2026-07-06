//! Shared outpace runtime setup for the daemon and CLI commands.

use crate::ace_provider::AceProvider;
use crate::broadcast::{Broadcast, BroadcastRegistry};
use crate::config::{load_or_create_identity, CacheType, Config};
use crate::http::{router, AppState, BroadcastState};
use crate::manager::StreamManager;
use crate::provider::ProviderRegistry;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr, SocketAddrV4};
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
    if let Ok(v) = std::env::var("OUTPACE_CACHE_TYPE") {
        config.cache_type = match v.as_str() {
            "memory" => CacheType::Memory,
            "disk" => CacheType::Disk,
            other => return Err(format!("invalid OUTPACE_CACHE_TYPE: {other}").into()),
        };
    }
    if let Ok(v) = std::env::var("OUTPACE_CACHE_DIR") {
        config.cache_dir = v.into();
    }
    if let Ok(v) = std::env::var("OUTPACE_PREFETCH_PIECES") {
        config.prefetch_pieces = v.parse()?;
    }
    if let Ok(v) = std::env::var("OUTPACE_SESSION_BUFFER") {
        let n: usize = v.parse()?;
        if n == 0 {
            return Err("OUTPACE_SESSION_BUFFER must be >= 1".into());
        }
        config.session_buffer = n;
    }
    if let Ok(v) = std::env::var("OUTPACE_MAX_UNCHOKED") {
        config.max_unchoked = v.parse()?;
    }
    if let Ok(v) = std::env::var("OUTPACE_MAX_INBOUND") {
        config.max_inbound_peers = v.parse()?;
    }
    if let Ok(v) = std::env::var("OUTPACE_SEED_TTL_SECS") {
        config.seed_ttl_secs = v.parse()?;
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
    http_bind: SocketAddr,
    rtmp_bind: SocketAddr,
    public_host: Option<String>,
    name: &str,
) -> BroadcastIngestUrls {
    let public_host = public_host.as_deref();
    let raw_host = public_host
        .map(url_host)
        .unwrap_or_else(|| url_host_from_ip(http_bind.ip()));
    let rtmp_host = public_host
        .map(url_host)
        .unwrap_or_else(|| url_host_from_ip(rtmp_bind.ip()));
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

fn url_host(host: &str) -> String {
    host.parse()
        .map(url_host_from_ip)
        .unwrap_or_else(|_| host.to_string())
}

fn url_host_from_ip(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) => format!("[{ip}]"),
    }
}

pub async fn build_runtime(
    config: Config,
    bootstrap_peers: Vec<SocketAddrV4>,
) -> Result<EngineRuntime, Box<dyn std::error::Error>> {
    let identity = Arc::new(load_or_create_identity(&config.data_dir)?);

    // Fail fast on a misconfigured disk cache, and start from a clean slate: wipe the cache root
    // so per-infohash dirs orphaned by a hard crash (no `Drop` ran) don't survive a restart. The
    // cache is ephemeral (piece data goes stale; broadcasts rebuild theirs from live ingest), so
    // wiping is always safe. A bad OUTPACE_CACHE_DIR surfaces here rather than degrading per stream.
    if config.cache_type == CacheType::Disk {
        if config.cache_dir.exists() {
            std::fs::remove_dir_all(&config.cache_dir).map_err(|e| {
                format!(
                    "cannot clear OUTPACE_CACHE_DIR {}: {e}",
                    config.cache_dir.display()
                )
            })?;
        }
        std::fs::create_dir_all(&config.cache_dir).map_err(|e| {
            format!(
                "cannot create OUTPACE_CACHE_DIR {}: {e}",
                config.cache_dir.display()
            )
        })?;
    }

    // Register enabled providers. Only "ace" exists today; the registry is the path for more.
    let seed_registry = ace_swarm::listen::SeedRegistry::new();
    let mut registry = ProviderRegistry::new();
    // The single resolved external inbound endpoint. Advertise the *peer* port, never the HTTP
    // API port (issue #21); `None` when inbound serving is off (no listener to back it). Both
    // the leech/seeder self-announce and `broadcasts.inbound_peer_port` are threaded from this
    // one value so tracker + DHT + PEX advertise one identical endpoint.
    let inbound_peer_port = config.enable_inbound.then_some(config.peer_listen.port());
    if config.networks.iter().any(|n| n == "ace") {
        let provider = AceProvider::new(identity.clone(), config.peer_listen.port())
            .with_bootstrap_peers(bootstrap_peers)
            .with_seed_registry(seed_registry.clone())
            .with_seed_store_bytes(config.seed_store_bytes)
            .with_cache(config.cache_type, config.cache_dir.clone())
            .with_prefetch_pieces(config.prefetch_pieces)
            .with_seeding_enabled(config.enable_seeding)
            .with_inbound_announce_port(inbound_peer_port);
        registry.register(Arc::new(provider));
    }
    let networks: Vec<String> = registry.networks().iter().map(|s| s.to_string()).collect();

    let manager = StreamManager::with_buffer(registry, config.session_buffer);
    manager.spawn_reaper();
    if config.seed_ttl_secs > 0 {
        let seed_registry_reap = seed_registry.clone();
        let ttl = std::time::Duration::from_secs(config.seed_ttl_secs);
        tokio::spawn(async move {
            // Sweep at a fraction of the TTL so an idle entry is reclaimed within ~1.25x the TTL.
            let interval = (ttl / 4).max(std::time::Duration::from_secs(5));
            loop {
                tokio::time::sleep(interval).await;
                let n = seed_registry_reap.reap(ttl);
                if n > 0 {
                    crate::alog!("[seed] reaped {n} idle leech registry entr(y/ies)");
                }
            }
        });
    }
    let broadcasts = BroadcastState {
        registry: BroadcastRegistry::with_persist(
            &config.data_dir,
            config.cache_type,
            config.cache_dir.clone(),
        ),
        seed_registry: seed_registry.clone(),
        trackers: DEFAULT_BROADCAST_TRACKERS
            .iter()
            .map(|s| s.to_string())
            .collect(),
        store_bytes: config.seed_store_bytes,
        inbound_peer_port,
    };

    // Reload any broadcasts persisted by a previous run so their identity/infohash survives
    // the restart and they are immediately servable, then restart each one's self-announce.
    let reloaded = broadcasts
        .registry
        .reload_persisted(&broadcasts.seed_registry, broadcasts.store_bytes)
        .await;
    if !reloaded.is_empty() {
        crate::alog!(
            "[broadcast] reloaded {} persisted broadcast(s)",
            reloaded.len()
        );
    }
    for bc in &reloaded {
        broadcasts.spawn_announce(bc);
    }

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

    let rtmp_bind = config.rtmp_bind;
    let rtmp_broadcasts = broadcasts.clone();
    let state = AppState {
        manager,
        networks: networks.clone(),
        resolve_content_ids_in_getstream: true,
        ace_session_aliases: Arc::new(std::sync::Mutex::new(HashMap::new())),
        experimental_ace_compat: config.experimental_ace_compat,
        broadcasts: Some(broadcasts),
    };

    // Inbound seeding (S2): ON by default, matching the Acestream engine's out-of-the-box
    // behavior as a full P2P participant (bind the peer port, accept inbound peers, seed,
    // self-announce). Piece headers are preserved/generated and official-consumer piece
    // acceptance is live-proven (note 33). Opt out with `OUTPACE_ENABLE_INBOUND=0`.
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

    tokio::spawn(async move {
        if let Err(e) = crate::rtmp::serve_rtmp(rtmp_bind, rtmp_broadcasts).await {
            eprintln!("outpace: RTMP ingest stopped: {e}");
        }
    });
    eprintln!(
        "outpace: RTMP ingest listening on rtmp://{}/live/<name>",
        config.rtmp_bind
    );

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

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn rtmp_bind_env_override_sets_config_rtmp_bind() {
        let _guard = ENV_LOCK.lock().unwrap();
        let old = std::env::var_os("OUTPACE_RTMP_BIND");

        std::env::set_var("OUTPACE_RTMP_BIND", "127.0.0.1:19935");
        let config = config_from_env();
        match old {
            Some(value) => std::env::set_var("OUTPACE_RTMP_BIND", value),
            None => std::env::remove_var("OUTPACE_RTMP_BIND"),
        }

        let config = config.unwrap();
        assert_eq!(config.rtmp_bind, "127.0.0.1:19935".parse().unwrap());
    }

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

    #[test]
    fn broadcast_urls_use_bracketed_ipv6_bind_hosts() {
        let http = "[::1]:6878".parse().unwrap();
        let rtmp = "[::1]:1935".parse().unwrap();
        let urls = broadcast_ingest_urls(http, rtmp, None, "mychan");

        assert_eq!(urls.raw, "http://[::1]:6878/broadcast/mychan");
        assert_eq!(urls.rtmp, "rtmp://[::1]:1935/live/mychan");
    }

    #[test]
    fn parses_prefetch_and_session_buffer() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("OUTPACE_PREFETCH_PIECES", "32");
        std::env::set_var("OUTPACE_SESSION_BUFFER", "512");
        let c = config_from_env().unwrap();
        assert_eq!(c.prefetch_pieces, 32);
        assert_eq!(c.session_buffer, 512);
        std::env::remove_var("OUTPACE_PREFETCH_PIECES");
        std::env::remove_var("OUTPACE_SESSION_BUFFER");
    }

    #[test]
    fn rejects_zero_session_buffer() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("OUTPACE_SESSION_BUFFER", "0");
        let err = config_from_env().err();
        std::env::remove_var("OUTPACE_SESSION_BUFFER");
        assert!(err.is_some(), "session_buffer=0 must be rejected");
    }

    #[test]
    fn parses_cache_type_and_dir() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("OUTPACE_CACHE_TYPE", "disk");
        std::env::set_var("OUTPACE_CACHE_DIR", "/tmp/outpace-cache-test");
        let c = config_from_env().unwrap();
        assert_eq!(c.cache_type, CacheType::Disk);
        assert_eq!(
            c.cache_dir,
            std::path::PathBuf::from("/tmp/outpace-cache-test")
        );
        std::env::remove_var("OUTPACE_CACHE_TYPE");
        std::env::remove_var("OUTPACE_CACHE_DIR");
    }

    #[test]
    fn rejects_invalid_cache_type() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("OUTPACE_CACHE_TYPE", "nvme");
        let err = config_from_env().err();
        std::env::remove_var("OUTPACE_CACHE_TYPE");
        assert!(err.is_some(), "unknown cache type must be rejected");
    }

    #[tokio::test]
    async fn build_runtime_reloads_persisted_broadcasts_and_serves_them() {
        use crate::broadcast::BroadcastRegistry;

        let data_dir =
            std::env::temp_dir().join(format!("outpace-rt-test-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&data_dir).unwrap();

        // Mint a broadcast under this data_dir with a throwaway registry (writes the record),
        // capturing its identity, then drop the registry to simulate a shutdown.
        let infohash = {
            let seed = ace_swarm::listen::SeedRegistry::new();
            let reg = BroadcastRegistry::with_persist(
                &data_dir,
                CacheType::Memory,
                std::path::PathBuf::new(),
            );
            let (bc, fresh) = reg
                .start_or_resume("news", "News", &[], &seed, 1 << 20)
                .await;
            assert!(fresh);
            bc.infohash
        };

        // A fresh daemon start over the same data_dir must reload it and serve it.
        // `enable_inbound` off keeps the reload path offline — this test covers reload+serve,
        // not the (network) self-announce that a reloaded broadcast would otherwise spawn.
        let config = Config {
            data_dir: data_dir.clone(),
            networks: vec![],
            enable_inbound: false,
            ..Config::default()
        };
        let runtime = build_runtime(config, vec![]).await.unwrap();

        let reloaded = runtime.broadcasts.registry.get("news").await;
        assert!(
            reloaded.is_some(),
            "persisted broadcast reloaded into the registry"
        );
        assert_eq!(
            reloaded.unwrap().infohash,
            infohash,
            "identity survives restart"
        );
        assert!(
            runtime.seed_registry.serves(&infohash),
            "reloaded broadcast is immediately servable"
        );

        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
