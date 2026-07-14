//! Shared outpace runtime setup for the daemon and CLI commands.

use crate::ace_provider::AceProvider;
use crate::broadcast::{Broadcast, BroadcastRegistry};
use crate::config::{load_or_create_identity, safe_in_memory_pool_limit, CacheType, Config};
use crate::http::{router, AppState, BroadcastState};
use crate::manager::StreamManager;
use crate::provider::ProviderRegistry;
use std::net::{IpAddr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

/// Default tracker for minted broadcasts (B1) — the same public UDP tracker `AceProvider`
/// falls back to for bare infohashes with none of their own. A freshly minted broadcast
/// self-announces to this tracker *and* DHT (`ace_provider::announce_infohash_periodically`,
/// spawned from `http.rs`'s ingest handler) as soon as it's minted, independent of whether
/// anything is locally following it — a pure origin needs to be discoverable too.
const DEFAULT_BROADCAST_TRACKERS: &[&str] = &["udp://t1.torrentstream.org:2710/announce"];

/// How often the daemon logs its public-reachability status (issue #22). Slow-changing signal,
/// so a couple of minutes keeps the noise low (cf. the 4-min seeder self-announce cadence).
const REACHABILITY_STATUS_INTERVAL: std::time::Duration = std::time::Duration::from_secs(120);

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
    /// Daemon-wide public-reachability observability (issue #22). `Some` only when inbound
    /// serving is enabled — with inbound off we can't be dialed, so harvesting/counting is
    /// entirely absent and the status logger never runs.
    pub reachability: Option<Arc<ace_swarm::reachability::ReachabilityMonitor>>,
    announce_port_tx: tokio::sync::watch::Sender<Option<u16>>,
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
    if let Ok(v) = std::env::var("OUTPACE_REQUEST_TIMEOUT_MS") {
        config.live_recovery.request_timeout_ms = v.parse()?;
    }
    if let Ok(v) = std::env::var("OUTPACE_STALE_UPSTREAM_TIMEOUT_MS") {
        config.live_recovery.stale_upstream_timeout_ms = v.parse()?;
    }
    if let Ok(v) = std::env::var("OUTPACE_REQUEST_CHECK_INTERVAL_MS") {
        config.live_recovery.request_check_interval_ms = v.parse()?;
    }
    if let Ok(v) = std::env::var("OUTPACE_MAX_ACTIVE_UPSTREAMS") {
        config.live_recovery.max_active_upstreams = v.parse()?;
    }
    if let Ok(v) = std::env::var("OUTPACE_MAX_PARALLEL_CONNECT") {
        config.live_recovery.max_parallel_connect = v.parse()?;
    }
    if let Ok(v) = std::env::var("OUTPACE_MAX_PIECE_ADVANCE") {
        config.live_recovery.max_piece_advance = v.parse()?;
    }
    if let Ok(v) = std::env::var("OUTPACE_MAX_REASM_PIECES_AHEAD") {
        config.live_recovery.max_reasm_pieces_ahead = v.parse()?;
    }
    if let Ok(v) = std::env::var("OUTPACE_HLS_SEGMENT_PACKETS") {
        config.hls.segment_packets = v.parse()?;
    }
    if let Ok(v) = std::env::var("OUTPACE_HLS_WINDOW_SEGMENTS") {
        config.hls.window_segments = v.parse()?;
    }
    if let Ok(v) = std::env::var("OUTPACE_HLS_SEGMENT_DURATION_MS") {
        config.hls.segment_duration_ms = v.parse()?;
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
    if let Some(v) = bool_from_env("OUTPACE_ENABLE_SEEDING")? {
        config.enable_seeding = v;
    }
    if let Some(v) = bool_from_env("OUTPACE_ENABLE_INBOUND")? {
        config.enable_inbound = v;
    }
    if let Some(v) = bool_from_env("OUTPACE_ENABLE_PORT_MAPPING")? {
        config.enable_port_mapping = v;
    }
    if let Ok(v) = std::env::var("OUTPACE_PORT_MAP_BACKEND") {
        config.port_map_backend = v.parse()?;
    }
    if let Ok(v) = std::env::var("OUTPACE_PORT_MAP_EXTERNAL_PORT") {
        config.port_map_external_port = Some(v.parse()?);
    }
    if let Some(v) = bool_from_env("OUTPACE_EXPERIMENTAL_ACE_COMPAT")? {
        config.experimental_ace_compat = v;
    }
    config.live_recovery.validate()?;
    config.hls.validate()?;
    validate_cache_budget(&config)?;
    Ok(config)
}

fn bool_from_env(name: &str) -> Result<Option<bool>, Box<dyn std::error::Error>> {
    let value = match std::env::var(name) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(format!("{name} must be valid UTF-8 and one of: 1, true, 0, false").into())
        }
    };
    match value.as_str() {
        "1" | "true" => Ok(Some(true)),
        "0" | "false" => Ok(Some(false)),
        _ => Err(format!("invalid {name}: {value:?}; expected one of: 1, true, 0, false").into()),
    }
}

/// Create and resolve the disk/data roots before any persistent state is loaded or cache cleanup
/// runs. Validation and later deletion must use this exact canonical cache path: lexically
/// collapsing `..` first is unsafe because `symlink/..` is resolved by the filesystem relative to
/// the symlink target, not its lexical parent.
///
/// A cache nested under the data directory is safe and remains the documented default. A cache
/// equal to or containing the data directory is rejected because startup wipes the cache root.
fn prepare_disk_cache_paths(config: &mut Config) -> Result<(), Box<dyn std::error::Error>> {
    if config.cache_type != CacheType::Disk {
        return Ok(());
    }

    // Directory creation is non-destructive and lets canonicalize apply the same symlink/`..`
    // semantics that remove_dir_all will use. Do this before identity creation so a rejected
    // relationship cannot overwrite or delete persistent state.
    std::fs::create_dir_all(&config.cache_dir).map_err(|e| {
        format!(
            "cannot create OUTPACE_CACHE_DIR {}: {e}",
            config.cache_dir.display()
        )
    })?;
    std::fs::create_dir_all(&config.data_dir).map_err(|e| {
        format!(
            "cannot create OUTPACE_DATA_DIR {}: {e}",
            config.data_dir.display()
        )
    })?;

    let cache_dir = config.cache_dir.canonicalize()?;
    let data_dir = config.data_dir.canonicalize()?;
    if data_dir.starts_with(&cache_dir) {
        return Err(format!(
            "OUTPACE_CACHE_DIR {} must not equal or contain OUTPACE_DATA_DIR {}; use a cache directory nested inside the data directory or a separate path",
            config.cache_dir.display(),
            config.data_dir.display()
        )
        .into());
    }
    config.cache_dir = cache_dir;
    config.data_dir = data_dir;
    Ok(())
}

/// Reject an in-memory retention budget that leaves insufficient target-relative address space
/// for allocator overhead, store indexes, runtime buffers, and other active sessions. Disk cache
/// accounting remains `u64` and is not constrained by the process's maximum object size.
fn validate_cache_budget(config: &Config) -> Result<(), String> {
    validate_cache_budget_for_object_limit(config, isize::MAX as u64)
}

fn validate_cache_budget_for_object_limit(
    config: &Config,
    max_object_bytes: u64,
) -> Result<(), String> {
    let safe_limit = safe_in_memory_pool_limit(max_object_bytes);
    if config.cache_type == CacheType::Memory && config.seed_store_bytes > safe_limit {
        return Err(format!(
            "OUTPACE_SEED_STORE_BYTES={} exceeds this target's conservative {}-byte memory-cache limit; reduce it or set OUTPACE_CACHE_TYPE=disk",
            config.seed_store_bytes, safe_limit
        ));
    }
    Ok(())
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

/// Resolve the tracker list minted into broadcast descriptors and used for broadcast
/// self-announce. `OUTPACE_TRACKERS` — comma-separated `scheme://…` (typically
/// `udp://host:port/announce`) URLs — FULLY REPLACES [`DEFAULT_BROADCAST_TRACKERS`] when set
/// (non-empty after trimming); unset or blank leaves the public default in place so behavior is
/// byte-identical to before.
///
/// Per-entry handling (see [`classify_tracker_entry`]):
/// - No `scheme://` prefix: obviously unusable — a broadcast pointed only at such entries would
///   be undiscoverable — so it is dropped with a warning rather than minted into a broken
///   descriptor. Ditto entries longer than the announce path's
///   [`ace_swarm::discover::MAX_TRACKER_URL_LEN`], which its resolver would skip anyway.
/// - Well-formed but not lowercase `udp://` (e.g. `http://…`, `ws://…`, `UDP://…`): KEPT —
///   other consumers of the minted descriptor may use it — but outpace's own self-announce
///   resolver (`ace_swarm::discover::resolve_trackers`) only strips a literal lowercase
///   `udp://` prefix, so outpace itself will never announce there; a warning says so.
/// - The kept list is clamped to [`ace_swarm::discover::MAX_TRACKERS`] with a warning — the
///   same cap the announce path applies.
///
/// If *every* override entry is dropped the result is empty (DHT-only discovery); we deliberately
/// do NOT reinstate the public default in that case, since the operator explicitly opted out of
/// it and silently re-announcing on a public tracker would defeat the point of the override.
pub fn broadcast_trackers_from_env() -> Vec<String> {
    use ace_swarm::discover::{MAX_TRACKERS, MAX_TRACKER_URL_LEN};
    let raw = std::env::var("OUTPACE_TRACKERS").unwrap_or_default();
    if raw.trim().is_empty() {
        return DEFAULT_BROADCAST_TRACKERS
            .iter()
            .map(|s| s.to_string())
            .collect();
    }
    let mut out = Vec::new();
    for s in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        match classify_tracker_entry(s) {
            TrackerEntryClass::Udp => out.push(s.to_string()),
            TrackerEntryClass::NonUdpScheme => {
                crate::alog!(
                    "[broadcast] WARNING: OUTPACE_TRACKERS entry {s:?} is not a lowercase udp:// URL; outpace only self-announces to udp:// trackers, so this entry is minted into broadcast descriptors for other clients only"
                );
                out.push(s.to_string());
            }
            TrackerEntryClass::TooLong => {
                crate::alog!(
                    "[broadcast] WARNING: ignoring over-long OUTPACE_TRACKERS entry ({} bytes > {MAX_TRACKER_URL_LEN} max); the announce path would skip it anyway",
                    s.len()
                );
            }
            TrackerEntryClass::Malformed => {
                crate::alog!(
                    "[broadcast] WARNING: ignoring malformed OUTPACE_TRACKERS entry {s:?} (no scheme:// prefix); it will not be minted into broadcast descriptors"
                );
            }
        }
    }
    if out.len() > MAX_TRACKERS {
        crate::alog!(
            "[broadcast] WARNING: OUTPACE_TRACKERS has {} usable entries; keeping only the first {MAX_TRACKERS} (the announce path's cap)",
            out.len()
        );
        out.truncate(MAX_TRACKERS);
    }
    out
}

/// How one `OUTPACE_TRACKERS` entry is treated. Pure classification (no logging) so the warn
/// branches of [`broadcast_trackers_from_env`] are unit-testable without capturing stderr.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrackerEntryClass {
    /// Lowercase `udp://…` — minted into descriptors AND self-announced by outpace.
    Udp,
    /// Well-formed `scheme://rest` but not lowercase `udp://` (e.g. `http://`, `ws://`,
    /// `UDP://`). Kept in the minted descriptor for other clients, but never self-announced:
    /// `ace_swarm::discover::resolve_trackers` only strips a literal lowercase `udp://` prefix.
    NonUdpScheme,
    /// Longer than [`ace_swarm::discover::MAX_TRACKER_URL_LEN`]; the announce path skips such
    /// entries, so minting them would be dead weight — dropped.
    TooLong,
    /// No `scheme://` prefix — obviously unusable, dropped. This is only a minimal sanity
    /// check, not a full URL validator.
    Malformed,
}

fn classify_tracker_entry(s: &str) -> TrackerEntryClass {
    if s.len() > ace_swarm::discover::MAX_TRACKER_URL_LEN {
        return TrackerEntryClass::TooLong;
    }
    match s.split_once("://") {
        Some((scheme, rest)) if !scheme.is_empty() && !rest.is_empty() => {
            if scheme == "udp" {
                TrackerEntryClass::Udp
            } else {
                TrackerEntryClass::NonUdpScheme
            }
        }
        _ => TrackerEntryClass::Malformed,
    }
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
    mut config: Config,
    bootstrap_peers: Vec<SocketAddrV4>,
) -> Result<EngineRuntime, Box<dyn std::error::Error>> {
    config.live_recovery.validate()?;
    config.hls.validate()?;
    validate_cache_budget(&config)?;

    // This must precede identity creation and, critically, the disk cache's remove_dir_all.
    // `build_runtime` is the single validation boundary for both env-derived Config and direct
    // library/test callers, and it runs before any destructive operation.
    prepare_disk_cache_paths(&mut config)?;
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
    // Public-reachability monitor (issue #22): only created when inbound serving is on. With
    // inbound off we can never be dialed, so nothing harvests `yourip` or counts inbound peers
    // and the whole feature is inert — no behavior change (issue #22 task 5).
    let reachability = config
        .enable_inbound
        .then(|| Arc::new(ace_swarm::reachability::ReachabilityMonitor::new()));
    let mut registry = ProviderRegistry::new();
    // The single resolved external inbound endpoint. Advertise the *peer* port, never the HTTP
    // API port (issue #21); `None` when inbound serving is off (no listener to back it). Both
    // the leech/seeder self-announce and `broadcasts.inbound_peer_port` are threaded from this
    // one value so tracker + DHT + PEX advertise one identical endpoint.
    let inbound_peer_port = config.enable_inbound.then_some(config.peer_listen.port());
    let (announce_port_tx, announce_port_rx) = tokio::sync::watch::channel(inbound_peer_port);
    if config.networks.iter().any(|n| n == "ace") {
        let provider = AceProvider::new(identity.clone(), config.peer_listen.port())
            .with_bootstrap_peers(bootstrap_peers)
            .with_seed_registry(seed_registry.clone())
            .with_seed_store_bytes(config.seed_store_bytes)
            .with_cache(config.cache_type, config.cache_dir.clone())
            .with_prefetch_pieces(config.prefetch_pieces)
            .with_live_recovery(config.live_recovery)
            .with_seeding_enabled(config.enable_seeding)
            .with_inbound_announce_port_receiver(announce_port_rx.clone())
            .with_reachability(reachability.clone());
        registry.register(Arc::new(provider));
    }
    let networks: Vec<String> = registry.networks().iter().map(|s| s.to_string()).collect();

    let manager = StreamManager::with_config(registry, config.session_buffer, config.hls);
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
    if config.enable_inbound {
        let rechoke_registry = seed_registry.clone();
        tokio::spawn(async move {
            // BitTorrent's classic rechoke cadence is ~10s; rotate the optimistic-unchoke slot on
            // that beat so newcomers periodically get a turn even on a saturated stream.
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                ticker.tick().await;
                rechoke_registry.rechoke_all();
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
        // Single resolved tracker list — `OUTPACE_TRACKERS` overrides the public default here, so
        // BOTH descriptor minting (`mint_broadcast`) and self-announce (`announce_broadcast`),
        // which read `broadcasts.trackers`, stay in lockstep.
        trackers: broadcast_trackers_from_env(),
        store_bytes: config.seed_store_bytes,
        inbound_peer_port: announce_port_rx,
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
        reachability,
        announce_port_tx,
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
        reachability,
        announce_port_tx,
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
        ace_sessions: Arc::new(crate::http::AceSessionStore::default()),
        experimental_ace_compat: config.experimental_ace_compat,
        broadcasts: Some(broadcasts),
    };

    // Held for the process lifetime so the gateway port-mapping renewal task keeps running and
    // the mapping is torn down on shutdown (handle drop). `None` unless port mapping is enabled
    // and succeeds. The `_` prefix marks it as a lifetime guard rather than a read value.
    let mut _port_map_handle: Option<ace_swarm::portmap::PortMapHandle> = None;

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
        let max_unchoked = config.max_unchoked;
        let listener_identity = identity.clone();
        let listener_reachability = reachability.clone();
        tokio::spawn(async move {
            ace_swarm::listen::PeerListener::serve(
                peer_listener,
                inbound_registry,
                listener_peer_id,
                [0u8; 8],
                max_inbound,
                listener_identity,
                max_unchoked,
                listener_reachability,
            )
            .await;
        });

        // NAT reachability (issue #20): map the peer port on the home gateway so peers behind
        // NAT can dial us. Gated behind the new `enable_port_mapping` (default off) in addition
        // to `enable_inbound`; best-effort and non-fatal — a failure logs and the daemon
        // continues. `spawn_port_mapping` returns immediately and does ALL gateway discovery in
        // a background task, so startup never blocks on slow SSDP / NAT-PMP retries. The task
        // logs and publishes the resolved endpoint (for the announce path, #21, via
        // `PortMapHandle::endpoint_receiver`) once established. The handle is held for the
        // process lifetime so its renewal task runs and the mapping is deleted on shutdown
        // (handle drop).
        if config.enable_port_mapping {
            let pm_cfg = ace_swarm::portmap::PortMapConfig {
                backend: config.port_map_backend,
                internal_port: config.peer_listen.port(),
                external_port: config.port_map_external_port,
                lease_seconds: ace_swarm::portmap::DEFAULT_LEASE_SECONDS,
            };
            eprintln!(
                "outpace: port mapping ENABLED (backend={}, internal port {}); resolving gateway in background",
                config.port_map_backend,
                config.peer_listen.port()
            );
            _port_map_handle = ace_swarm::portmap::spawn_port_mapping(pm_cfg);
            if let Some(handle) = &_port_map_handle {
                let mapped_rx = handle.endpoint_receiver();
                tokio::spawn(propagate_mapped_announce_port(
                    mapped_rx,
                    announce_port_tx,
                    config.peer_listen.port(),
                ));
            }
        }

        // Public-reachability status (issue #22): turn note 24's open "nothing proven to dial us
        // back" question into an observable answer. We hold a monitor only when inbound serving
        // is on, so this runs exactly when there is a listener that could actually be reached.
        // The port-mapping endpoint receiver (when mapping is enabled) feeds the `yourip` vs
        // mapped-external-IP cross-check that flags double-NAT / CGNAT.
        if let Some(monitor) = reachability.clone() {
            let endpoint_rx = _port_map_handle.as_ref().map(|h| h.endpoint_receiver());
            tokio::spawn(reachability_status_loop(monitor, endpoint_rx));
        }
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
    eprintln!("  MPEG-TS: http://{}/streams/ace/<id>.ts", config.bind);
    eprintln!("  HLS:     http://{}/streams/ace/<id>.m3u8", config.bind);
    eprintln!("  status:  http://{}/streams", config.bind);
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C signal handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    eprintln!("outpace: shutdown signal received; draining HTTP connections");
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
    if runtime.broadcasts.inbound_peer_port.borrow().is_some() {
        let trackers = runtime.broadcasts.trackers.clone();
        tokio::spawn(crate::ace_provider::announce_infohash_periodically_dynamic(
            trackers.clone(),
            bc.infohash,
            runtime.broadcasts.inbound_peer_port.clone(),
        ));
        tokio::spawn(crate::ace_provider::announce_infohash_periodically_dynamic(
            trackers,
            bc.content_id,
            runtime.broadcasts.inbound_peer_port.clone(),
        ));
    }
}

async fn propagate_mapped_announce_port(
    mut mapped_rx: tokio::sync::watch::Receiver<Option<ace_swarm::portmap::MappedEndpoint>>,
    announce_tx: tokio::sync::watch::Sender<Option<u16>>,
    local_port: u16,
) {
    loop {
        let port = mapped_rx
            .borrow_and_update()
            .as_ref()
            .map_or(local_port, |endpoint| endpoint.external_port);
        announce_tx.send_replace(Some(port));
        if mapped_rx.changed().await.is_err() {
            announce_tx.send_replace(Some(local_port));
            // Keep the sender alive so existing announce loops continue their normal periodic
            // fallback announces after the mapping task has ended.
            return std::future::pending().await;
        }
    }
}

/// Periodically log the daemon's public-reachability status (issue #22): whether an unsolicited
/// inbound peer has confirmed us reachable, the public IP peers echo via `yourip`, and — when a
/// gateway mapping (#20) is present — a `yourip` vs mapped-external-IP cross-check that warns on
/// a mismatch (the double-NAT / CGNAT signature). `endpoint_rx` is `None` when port mapping is
/// disabled, in which case the cross-check is simply skipped.
async fn reachability_status_loop(
    monitor: Arc<ace_swarm::reachability::ReachabilityMonitor>,
    endpoint_rx: Option<tokio::sync::watch::Receiver<Option<ace_swarm::portmap::MappedEndpoint>>>,
) {
    use ace_swarm::reachability::CrossCheck;
    let mut ticker = tokio::time::interval(REACHABILITY_STATUS_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        let mapped = endpoint_rx.as_ref().and_then(|rx| rx.borrow().clone());
        let mapped_ip = mapped.as_ref().and_then(|e| e.external_ip);
        // The status line's "external" endpoint is the gateway-mapped one (only when the backend
        // reported an external IP); the observed `yourip` is always reported separately.
        let external = mapped
            .as_ref()
            .and_then(|e| e.external_ip.map(|ip| (ip, e.external_port)));
        crate::alog!("[reachability] {}", monitor.status_line(external));
        if let CrossCheck::Mismatch { observed, mapped } = monitor.cross_check(mapped_ip) {
            crate::alog!(
                "[reachability] WARNING: observed public IP {observed} (from peer yourip) differs from the mapped external IP {mapped} — likely double-NAT / CGNAT (the gateway mapped a private upstream address); inbound dials to {mapped} may never reach us"
            );
        }
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
    fn broadcast_trackers_env_override_parsing() {
        let _g = ENV_LOCK.lock().unwrap();
        let old = std::env::var_os("OUTPACE_TRACKERS");

        let default: Vec<String> = DEFAULT_BROADCAST_TRACKERS
            .iter()
            .map(|s| s.to_string())
            .collect();

        // Unset -> untouched defaults.
        std::env::remove_var("OUTPACE_TRACKERS");
        assert_eq!(broadcast_trackers_from_env(), default);

        // Blank / whitespace-only -> defaults (treated as unset).
        std::env::set_var("OUTPACE_TRACKERS", "   ");
        assert_eq!(broadcast_trackers_from_env(), default);

        // Single URL fully replaces the default.
        std::env::set_var("OUTPACE_TRACKERS", "udp://a.local:1/announce");
        assert_eq!(
            broadcast_trackers_from_env(),
            vec!["udp://a.local:1/announce".to_string()]
        );

        // Multiple, comma-separated, with surrounding whitespace and a trailing comma.
        std::env::set_var(
            "OUTPACE_TRACKERS",
            " udp://a.local:1/announce , udp://b.local:2/announce ,",
        );
        assert_eq!(
            broadcast_trackers_from_env(),
            vec![
                "udp://a.local:1/announce".to_string(),
                "udp://b.local:2/announce".to_string(),
            ]
        );

        // Malformed entry (no scheme) is dropped; valid siblings survive.
        std::env::set_var("OUTPACE_TRACKERS", "udp://a.local:1/announce,not-a-url");
        assert_eq!(
            broadcast_trackers_from_env(),
            vec!["udp://a.local:1/announce".to_string()]
        );

        // All entries malformed -> empty list; we deliberately do NOT fall back to the public
        // default (the operator opted out of it), so the broadcast is DHT-only rather than
        // silently re-announced on t1.torrentstream.org.
        std::env::set_var("OUTPACE_TRACKERS", "nope,also-bad");
        assert!(broadcast_trackers_from_env().is_empty());

        // Non-udp and uppercase-UDP schemes are KEPT (minted for other descriptor consumers)
        // even though outpace itself won't self-announce to them (warn-only path).
        std::env::set_var(
            "OUTPACE_TRACKERS",
            "http://t.local/announce,UDP://h.local:1/announce,udp://a.local:1/announce",
        );
        assert_eq!(
            broadcast_trackers_from_env(),
            vec![
                "http://t.local/announce".to_string(),
                "UDP://h.local:1/announce".to_string(),
                "udp://a.local:1/announce".to_string(),
            ]
        );

        // Over-long entries are dropped (announce path would skip them anyway).
        let long = format!("udp://{}:1/announce", "h".repeat(300));
        std::env::set_var(
            "OUTPACE_TRACKERS",
            format!("{long},udp://a.local:1/announce"),
        );
        assert_eq!(
            broadcast_trackers_from_env(),
            vec!["udp://a.local:1/announce".to_string()]
        );

        // The kept list is clamped to the announce path's MAX_TRACKERS cap.
        let many: Vec<String> = (0..70).map(|i| format!("udp://t{i}.local:1/a")).collect();
        std::env::set_var("OUTPACE_TRACKERS", many.join(","));
        let clamped = broadcast_trackers_from_env();
        assert_eq!(clamped.len(), ace_swarm::discover::MAX_TRACKERS);
        assert_eq!(clamped[..], many[..ace_swarm::discover::MAX_TRACKERS]);

        match old {
            Some(v) => std::env::set_var("OUTPACE_TRACKERS", v),
            None => std::env::remove_var("OUTPACE_TRACKERS"),
        }
    }

    #[test]
    fn tracker_entry_classification_matches_self_announce_reality() {
        use TrackerEntryClass::*;
        // Self-announced: only a literal lowercase udp:// scheme (what
        // ace_swarm::discover::resolve_trackers strips).
        assert_eq!(classify_tracker_entry("udp://t.local:1/announce"), Udp);
        // Kept-but-warned: well-formed, but outpace never self-announces to these.
        assert_eq!(
            classify_tracker_entry("http://t.local/announce"),
            NonUdpScheme
        );
        assert_eq!(
            classify_tracker_entry("ws://t.local/announce"),
            NonUdpScheme
        );
        assert_eq!(
            classify_tracker_entry("UDP://t.local:1/announce"),
            NonUdpScheme,
            "resolve_trackers only matches lowercase udp://, so uppercase must be flagged"
        );
        // Dropped-and-warned.
        assert_eq!(classify_tracker_entry("t.local:1"), Malformed);
        assert_eq!(classify_tracker_entry("://rest"), Malformed);
        assert_eq!(classify_tracker_entry("udp://"), Malformed);
        let long = format!("udp://{}:1/a", "h".repeat(300));
        assert_eq!(classify_tracker_entry(&long), TooLong);
    }

    #[tokio::test]
    async fn minted_broadcast_descriptor_reflects_tracker_override() {
        // Resolve the override under the env lock, then drop the guard before the async mint so a
        // std mutex is never held across an await.
        let trackers = {
            let _g = ENV_LOCK.lock().unwrap();
            let old = std::env::var_os("OUTPACE_TRACKERS");
            std::env::set_var(
                "OUTPACE_TRACKERS",
                "udp://priv.local:1337/announce,udp://priv2.local:2/announce",
            );
            let t = broadcast_trackers_from_env();
            match old {
                Some(v) => std::env::set_var("OUTPACE_TRACKERS", v),
                None => std::env::remove_var("OUTPACE_TRACKERS"),
            }
            t
        };

        let reg = crate::broadcast::BroadcastRegistry::new();
        let seed = ace_swarm::listen::SeedRegistry::new();
        let (bc, _fresh) = reg
            .start_or_resume("t", "T", &trackers, &seed, 1 << 20)
            .await;
        let decoded = ace_wire::transport::decode_transport(&bc.transport_bytes).unwrap();
        assert_eq!(
            decoded.trackers,
            vec![
                "udp://priv.local:1337/announce".to_string(),
                "udp://priv2.local:2/announce".to_string(),
            ],
            "minted descriptor must carry the OUTPACE_TRACKERS override, not the public default"
        );
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
    fn port_mapping_env_overrides_parse_and_default_off() {
        let _g = ENV_LOCK.lock().unwrap();
        // Default: no env set -> port mapping is off, backend auto, no external override.
        std::env::remove_var("OUTPACE_ENABLE_PORT_MAPPING");
        std::env::remove_var("OUTPACE_PORT_MAP_BACKEND");
        std::env::remove_var("OUTPACE_PORT_MAP_EXTERNAL_PORT");
        let c = config_from_env().unwrap();
        assert!(!c.enable_port_mapping);
        assert_eq!(c.port_map_backend, ace_swarm::portmap::PortMapBackend::Auto);
        assert_eq!(c.port_map_external_port, None);

        // Overrides applied.
        std::env::set_var("OUTPACE_ENABLE_PORT_MAPPING", "1");
        std::env::set_var("OUTPACE_PORT_MAP_BACKEND", "natpmp");
        std::env::set_var("OUTPACE_PORT_MAP_EXTERNAL_PORT", "9000");
        let c = config_from_env().unwrap();
        assert!(c.enable_port_mapping);
        assert_eq!(
            c.port_map_backend,
            ace_swarm::portmap::PortMapBackend::Natpmp
        );
        assert_eq!(c.port_map_external_port, Some(9000));

        std::env::remove_var("OUTPACE_ENABLE_PORT_MAPPING");
        std::env::remove_var("OUTPACE_PORT_MAP_BACKEND");
        std::env::remove_var("OUTPACE_PORT_MAP_EXTERNAL_PORT");
    }

    #[test]
    fn exposed_boolean_gates_use_one_strict_parser() {
        let _g = ENV_LOCK.lock().unwrap();
        let names = [
            "OUTPACE_ENABLE_SEEDING",
            "OUTPACE_ENABLE_INBOUND",
            "OUTPACE_ENABLE_PORT_MAPPING",
            "OUTPACE_EXPERIMENTAL_ACE_COMPAT",
        ];
        for name in names {
            std::env::remove_var(name);
            for (value, expected) in [("1", true), ("true", true), ("0", false), ("false", false)] {
                std::env::set_var(name, value);
                assert_eq!(
                    bool_from_env(name).unwrap(),
                    Some(expected),
                    "{name}={value}"
                );
            }
            std::env::set_var(name, "ture");
            let error = config_from_env().unwrap_err().to_string();
            assert!(error.contains(name), "unexpected error: {error}");
            assert!(
                error.contains("expected one of"),
                "unexpected error: {error}"
            );
            std::env::remove_var(name);
        }
    }

    #[tokio::test]
    async fn mapped_port_replaces_local_port_and_loss_falls_back() {
        use ace_swarm::portmap::MappedEndpoint;
        use std::time::Duration;

        const LOCAL: u16 = 8621;
        const EXTERNAL: u16 = 49152;
        let (mapped_tx, mapped_rx) = tokio::sync::watch::channel(None);
        let (announce_tx, mut announce_rx) = tokio::sync::watch::channel(Some(LOCAL));
        assert_eq!(*announce_rx.borrow(), Some(LOCAL));
        tokio::spawn(propagate_mapped_announce_port(
            mapped_rx,
            announce_tx,
            LOCAL,
        ));

        mapped_tx
            .send(Some(MappedEndpoint {
                external_ip: None,
                external_port: EXTERNAL,
                lease_duration: Duration::from_secs(3600),
                backend: "test",
            }))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while *announce_rx.borrow_and_update() != Some(EXTERNAL) {
                announce_rx.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(*announce_rx.borrow_and_update(), Some(EXTERNAL));

        drop(mapped_tx);
        tokio::time::timeout(Duration::from_secs(1), announce_rx.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*announce_rx.borrow(), Some(LOCAL));
    }

    #[test]
    fn invalid_port_map_backend_env_is_rejected() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("OUTPACE_PORT_MAP_BACKEND", "bogus");
        let err = config_from_env().err();
        std::env::remove_var("OUTPACE_PORT_MAP_BACKEND");
        assert!(err.is_some(), "invalid backend must be rejected");
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
    fn default_config_has_live_recovery_and_hls_defaults() {
        let c = Config::default();
        assert_eq!(c.live_recovery.request_timeout_ms, 4000);
        assert_eq!(c.live_recovery.stale_upstream_timeout_ms, 12000);
        assert_eq!(c.live_recovery.request_check_interval_ms, 1000);
        assert_eq!(c.live_recovery.max_active_upstreams, 4);
        assert_eq!(c.live_recovery.max_parallel_connect, 12);
        assert_eq!(c.live_recovery.max_piece_advance, 256);
        assert_eq!(c.live_recovery.max_reasm_pieces_ahead, 512);
        assert_eq!(c.hls.segment_packets, 256);
        assert_eq!(c.hls.window_segments, 6);
        assert_eq!(c.hls.segment_duration_ms, 1000);
    }

    #[test]
    fn parses_live_recovery_and_hls_knobs() {
        let _g = ENV_LOCK.lock().unwrap();
        let keys = [
            "OUTPACE_REQUEST_TIMEOUT_MS",
            "OUTPACE_STALE_UPSTREAM_TIMEOUT_MS",
            "OUTPACE_REQUEST_CHECK_INTERVAL_MS",
            "OUTPACE_MAX_ACTIVE_UPSTREAMS",
            "OUTPACE_MAX_PARALLEL_CONNECT",
            "OUTPACE_MAX_PIECE_ADVANCE",
            "OUTPACE_MAX_REASM_PIECES_AHEAD",
            "OUTPACE_HLS_SEGMENT_PACKETS",
            "OUTPACE_HLS_WINDOW_SEGMENTS",
            "OUTPACE_HLS_SEGMENT_DURATION_MS",
        ];
        for key in keys {
            std::env::remove_var(key);
        }
        std::env::set_var("OUTPACE_REQUEST_TIMEOUT_MS", "2500");
        std::env::set_var("OUTPACE_STALE_UPSTREAM_TIMEOUT_MS", "10000");
        std::env::set_var("OUTPACE_REQUEST_CHECK_INTERVAL_MS", "500");
        std::env::set_var("OUTPACE_MAX_ACTIVE_UPSTREAMS", "3");
        std::env::set_var("OUTPACE_MAX_PARALLEL_CONNECT", "9");
        std::env::set_var("OUTPACE_MAX_PIECE_ADVANCE", "128");
        std::env::set_var("OUTPACE_MAX_REASM_PIECES_AHEAD", "256");
        std::env::set_var("OUTPACE_HLS_SEGMENT_PACKETS", "64");
        std::env::set_var("OUTPACE_HLS_WINDOW_SEGMENTS", "4");
        std::env::set_var("OUTPACE_HLS_SEGMENT_DURATION_MS", "1500");

        let c = config_from_env().unwrap();

        assert_eq!(c.live_recovery.request_timeout_ms, 2500);
        assert_eq!(c.live_recovery.stale_upstream_timeout_ms, 10000);
        assert_eq!(c.live_recovery.request_check_interval_ms, 500);
        assert_eq!(c.live_recovery.max_active_upstreams, 3);
        assert_eq!(c.live_recovery.max_parallel_connect, 9);
        assert_eq!(c.live_recovery.max_piece_advance, 128);
        assert_eq!(c.live_recovery.max_reasm_pieces_ahead, 256);
        assert_eq!(c.hls.segment_packets, 64);
        assert_eq!(c.hls.window_segments, 4);
        assert_eq!(c.hls.segment_duration_ms, 1500);

        for key in keys {
            std::env::remove_var(key);
        }
    }

    #[test]
    fn rejects_invalid_live_recovery_relationships() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("OUTPACE_REQUEST_TIMEOUT_MS", "12000");
        std::env::set_var("OUTPACE_STALE_UPSTREAM_TIMEOUT_MS", "12000");
        let err = config_from_env().unwrap_err().to_string();
        std::env::remove_var("OUTPACE_REQUEST_TIMEOUT_MS");
        std::env::remove_var("OUTPACE_STALE_UPSTREAM_TIMEOUT_MS");
        assert!(
            err.contains("OUTPACE_REQUEST_TIMEOUT_MS must be < OUTPACE_STALE_UPSTREAM_TIMEOUT_MS"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_hls_segment_byte_count_overflow() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("OUTPACE_HLS_SEGMENT_PACKETS", usize::MAX.to_string());
        let err = config_from_env().unwrap_err().to_string();
        std::env::remove_var("OUTPACE_HLS_SEGMENT_PACKETS");
        assert!(
            err.contains("OUTPACE_HLS_SEGMENT_PACKETS")
                && (err.contains("byte count overflows") || err.contains("object limit")),
            "unexpected error: {err}"
        );
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

    #[test]
    fn disk_cache_may_be_nested_under_data_dir() {
        let root = std::env::temp_dir().join(format!(
            "outpace-safe-cache-paths-{}",
            rand::random::<u64>()
        ));
        let mut config = Config {
            data_dir: root.clone(),
            cache_type: CacheType::Disk,
            cache_dir: root.join("state/../cache"),
            ..Config::default()
        };
        prepare_disk_cache_paths(&mut config).unwrap();
        assert!(config.cache_dir.starts_with(&config.data_dir));
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn disk_cache_equal_to_data_dir_is_rejected_before_cleanup() {
        let root = std::env::temp_dir().join(format!(
            "outpace-cache-equals-data-{}",
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let marker = root.join("persistent-state");
        std::fs::write(&marker, b"keep").unwrap();
        let config = Config {
            data_dir: root.clone(),
            cache_type: CacheType::Disk,
            cache_dir: root.clone(),
            enable_inbound: false,
            ..Config::default()
        };

        let error = build_runtime(config, vec![])
            .await
            .err()
            .expect("equal cache/data paths must fail")
            .to_string();
        assert!(
            error.contains("must not equal or contain"),
            "unexpected error: {error}"
        );
        assert!(
            marker.exists(),
            "validation must run before destructive cache cleanup"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn disk_cache_ancestor_of_data_dir_is_rejected_before_cleanup() {
        let root = std::env::temp_dir().join(format!(
            "outpace-cache-contains-data-{}",
            rand::random::<u64>()
        ));
        let data_dir = root.join("persistent");
        std::fs::create_dir_all(&data_dir).unwrap();
        let marker = data_dir.join("identity-and-broadcast-state");
        std::fs::write(&marker, b"keep").unwrap();
        let config = Config {
            data_dir,
            cache_type: CacheType::Disk,
            cache_dir: root.clone(),
            enable_inbound: false,
            ..Config::default()
        };

        let error = build_runtime(config, vec![])
            .await
            .err()
            .expect("a cache containing the data directory must fail")
            .to_string();
        assert!(
            error.contains("must not equal or contain"),
            "unexpected error: {error}"
        );
        assert!(
            marker.exists(),
            "validation must run before destructive cache cleanup"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlink_sensitive_parent_path_cannot_bypass_cache_guard() {
        use std::os::unix::fs::symlink;

        let root = std::env::temp_dir().join(format!(
            "outpace-cache-symlink-parent-{}",
            rand::random::<u64>()
        ));
        let a = root.join("a");
        let b = root.join("b");
        let symlink_target = b.join("sub");
        let data_dir = b.join("state");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&symlink_target).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        symlink(&symlink_target, a.join("link")).unwrap();
        let marker = data_dir.join("MUST_SURVIVE");
        std::fs::write(&marker, b"persistent state").unwrap();

        let config = Config {
            data_dir,
            cache_type: CacheType::Disk,
            // Filesystem resolution is `<target-of-link>/..` = `b`, not lexical `a`.
            cache_dir: a.join("link/.."),
            enable_inbound: false,
            ..Config::default()
        };

        let error = build_runtime(config, vec![])
            .await
            .err()
            .expect("symlink-sensitive cache ancestor must fail")
            .to_string();
        assert!(
            error.contains("must not equal or contain"),
            "unexpected error: {error}"
        );
        assert!(
            marker.exists(),
            "validated cache path and deleted path must have identical filesystem semantics"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn simulated_32_bit_memory_cache_enforces_conservative_boundary() {
        let object_limit = i32::MAX as u64;
        let safe_limit = safe_in_memory_pool_limit(object_limit);
        let mut config = Config {
            cache_type: CacheType::Memory,
            seed_store_bytes: safe_limit,
            ..Config::default()
        };
        validate_cache_budget_for_object_limit(&config, object_limit).unwrap();

        config.seed_store_bytes = safe_limit + 1;
        let err = validate_cache_budget_for_object_limit(&config, object_limit).unwrap_err();
        assert!(
            err.contains("OUTPACE_SEED_STORE_BYTES"),
            "unexpected error: {err}"
        );

        config.cache_type = CacheType::Disk;
        config.seed_store_bytes = u64::MAX;
        validate_cache_budget_for_object_limit(&config, object_limit).unwrap();
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

    #[tokio::test]
    async fn build_runtime_wires_max_unchoked_without_error() {
        let dir = std::env::temp_dir().join(format!("outpace-rt-mu-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let config = Config {
            data_dir: dir.clone(),
            cache_dir: dir.join("cache"),
            max_unchoked: 2,
            enable_inbound: true,
            bind: "127.0.0.1:0".parse().unwrap(),
            peer_listen: "127.0.0.1:0".parse().unwrap(),
            rtmp_bind: "127.0.0.1:0".parse().unwrap(),
            ..Config::default()
        };
        let runtime = build_runtime(config, vec![]).await.unwrap();
        assert_eq!(runtime.config.max_unchoked, 2);
        assert!(
            runtime.reachability.is_some(),
            "a reachability monitor exists when inbound serving is on"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn reachability_monitor_is_absent_when_inbound_is_disabled() {
        // Issue #22 task 5: with inbound serving off we can never be dialed, so the whole
        // reachability feature must be inert — no monitor is created, nothing harvests `yourip`
        // or counts inbound peers, and the status logger never runs.
        let dir = std::env::temp_dir().join(format!("outpace-rt-reach-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let config = Config {
            data_dir: dir.clone(),
            cache_dir: dir.join("cache"),
            networks: vec![],
            enable_inbound: false,
            bind: "127.0.0.1:0".parse().unwrap(),
            peer_listen: "127.0.0.1:0".parse().unwrap(),
            rtmp_bind: "127.0.0.1:0".parse().unwrap(),
            ..Config::default()
        };
        let runtime = build_runtime(config, vec![]).await.unwrap();
        assert!(
            runtime.reachability.is_none(),
            "no reachability monitor when inbound serving is disabled"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
