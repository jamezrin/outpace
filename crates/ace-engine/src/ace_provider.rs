//! The `"ace"` provider: resolves an identifier to a [`StreamInfo`], discovers peers via
//! trackers, and follows the live edge from a peer, emitting contiguous MPEG-TS. Built on
//! the cracked live protocol (see `docs/protocol/notes/19.md`).
//!
//! LIVE-GATED: the peer I/O path requires the real Acestream swarm and is verified in the
//! operator's environment (it cannot run in CI/sandbox). Content-id → transport-file
//! resolution uses the signed catalog or a broadcast originated by this daemon. An opt-in
//! BEP-9 fallback supports outpace's raw wrapped-transport hash namespace, not an established
//! public content-id derivation (see [`ace_swarm::resolve`]). Cold infohashes use the
//! authenticated catalog and require a descriptor whose computed hash matches the request.

mod announce;
#[cfg(test)]
mod content_id_policy_tests;
#[cfg(test)]
mod discovery_tests;
#[cfg(test)]
mod live_start_tests;
use announce::announce_seeder_periodically;
pub use announce::{announce_infohash_periodically, announce_infohash_periodically_dynamic};
mod live;
use live::*;
mod reconnect;
mod resolution;
mod vod;
use vod::AceVodContent;
#[cfg(test)]
use vod::VodSource;
mod warm_peers;
use warm_peers::WarmPeerCache;
#[cfg(test)]
mod reconnect_tests;
#[cfg(test)]
mod resync_tests;
use reconnect::{CandidateKind, SessionCandidates};

use crate::config::{CacheType, LiveRecoveryConfig, StartupBufferConfig};
use crate::provider::{
    ProviderError, SourceStats, StreamProvider, TsSource, VodByteSource, VodContent,
};
use crate::startup_buffer::StartupBufferedSource;
use ace_peer::session::{connect, PeerSession};
use ace_swarm::dht::dht_announce_peer;
use ace_swarm::discover::{
    announce_seeder, discover_peers, discover_peers_incremental, DiscoveryOptions,
    MAX_DISCOVERY_PEERS,
};
use ace_swarm::listen::{SeedLease, SeedRegistry};
use ace_swarm::reachability::ReachabilityMonitor;
use ace_swarm::resolve::{
    catalog_transport_bytes, hex20, infohash_hex, resolve_infohash_via_catalog,
    resolve_via_catalog, resolve_via_peer, stream_info_from_transport,
    stream_info_from_transport_url, transport_bytes_from_url, transport_bytes_via_peer,
    vod_info_from_transport, InfohashIndex, ResolveCache, ResolveError,
};
use ace_swarm::scheduler::{ActivePeers, PeerAssignment, Scheduler};
use ace_swarm::store::{BackendKind, PieceStore};
use ace_swarm::types::{StreamInfo, StreamMetadata, VodInfo};
use ace_swarm::vod::download_vod_pieces;
use ace_wire::extended::{ExtendedHandshake, LivePosition, NodeFields, OutgoingExtendedHandshake};
use ace_wire::handshake::random_peer_id;
use ace_wire::identity::Identity;
use ace_wire::live::LiveWindow;
use ace_wire::live_codec::{build_piece, chunk_request, LiveChunk};
use ace_wire::message::PeerMessage;
use ace_wire::reassembly::PieceReassembler;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::net::{IpAddr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

/// How many pieces behind the live edge to start, so we have buffer immediately.
const PREFETCH_PIECES: u64 = 8;
const UNKNOWN_BITRATE_BUFFER_PREFETCH_PIECES: u64 = 32;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// Legacy single-peer helper handle. The production path now assigns real peer handles
/// starting at 1, but the old helper is kept as a short-term bisect fallback.
const SINGLE_PEER_ID: u64 = 0;
/// One total deadline for gossip-only harvesting of a stale connected batch.
const STALE_GOSSIP_BUDGET: Duration = Duration::from_millis(750);

struct OwnedTask(tokio::task::JoinHandle<()>);
impl Drop for OwnedTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

type PeerDiscovery = Arc<
    dyn Fn(DiscoveryOptions, mpsc::Sender<SocketAddrV4>) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

struct DiscoveryRun {
    receiver: mpsc::Receiver<SocketAddrV4>,
    _worker: OwnedTask,
}
impl DiscoveryRun {
    fn start(discovery: &PeerDiscovery, options: DiscoveryOptions) -> Self {
        let (sender, receiver) = mpsc::channel(MAX_DISCOVERY_PEERS);
        Self {
            receiver,
            _worker: OwnedTask(tokio::spawn(discovery(options, sender))),
        }
    }
}

#[cfg(test)]
fn completed_discovery<F, Fut>(factory: F) -> PeerDiscovery
where
    F: Fn(DiscoveryOptions) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Vec<SocketAddrV4>> + Send + 'static,
{
    Arc::new(move |options, sender| {
        let future = factory(options);
        Box::pin(async move {
            for peer in future.await {
                if sender.send(peer).await.is_err() {
                    return;
                }
            }
        })
    })
}

fn drain_discovery(run: &mut Option<DiscoveryRun>, candidates: &mut SessionCandidates) {
    let Some(discovery) = run.as_mut() else {
        return;
    };
    loop {
        match discovery.receiver.try_recv() {
            Ok(peer) => candidates.learn(peer, CandidateKind::Discovered),
            Err(mpsc::error::TryRecvError::Empty) => return,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                *run = None;
                return;
            }
        }
    }
}
async fn next_discovery_peer(run: &mut Option<DiscoveryRun>) -> Option<SocketAddrV4> {
    let Some(discovery) = run.as_mut() else {
        return std::future::pending().await;
    };
    let peer = discovery.receiver.recv().await;
    if peer.is_none() {
        *run = None;
    }
    peer
}

/// Briefly collect other near-complete candidates after the first live handshake.
const UPSTREAM_SELECTION_GRACE: Duration = Duration::from_millis(250);
/// Process gossip while withholding media from an uncorroborated first window.
const LIVE_START_CORROBORATION: Duration = Duration::from_secs(1);
/// How often an active session re-announces itself as a seeder to its trackers, so
/// outpace becomes organically discoverable while it's serving (see
/// `docs/protocol/notes/24-seeder-self-announce.md`). Doesn't yet honor a tracker's
/// returned `interval` — a fixed, conservative cadence is a deliberate simplification,
/// not a correctness requirement.
const SEEDER_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(4 * 60);
/// Time budget for each periodic DHT `announce_peer` walk (see `dht_announce_peer`) — bounds
/// how long a self-announce round can take before the next one is due.
const DHT_ANNOUNCE_BUDGET: Duration = Duration::from_secs(15);
/// Per-peer read ceiling while resolving a content-id (a silent peer shouldn't stall us).
const RESOLVE_PEER_TIMEOUT: Duration = Duration::from_secs(6);
/// Background discovery can spend longer/deeper than startup discovery because it does not
/// gate first byte. It should still finish before the stale-upstream timer fires, so a new
/// peer can enter the pool before we reconnect.
const BACKGROUND_DISCOVERY_BUDGET: Duration = Duration::from_secs(8);
const BACKGROUND_DISCOVERY_PEER_TARGET: usize = 64;

/// Acestream's hardcoded public UDP tracker (see `docs/protocol/notes/03`). Used for
/// content-id/metadata discovery and as the fallback list where a descriptor has none. DHT
/// discovery runs alongside this tracker in `discover_peers`.
const DEFAULT_ACE_TRACKERS: &[&str] = &["udp://t1.torrentstream.org:2710/announce"];

/// How long a resolved content-id → `StreamInfo` stays cached.
const RESOLVE_CACHE_TTL: Duration = Duration::from_secs(300);
/// How many verified live descriptors the infohash index keeps (#164). Each entry is a few KiB
/// at most (geometry, trackers, pubkey, metadata).
const INFOHASH_INDEX_CAPACITY: usize = 256;
const INFOHASH_FAILURE_TTL: Duration = Duration::from_secs(5);
const INFOHASH_FAILURE_CAPACITY: usize = 128;

/// Byte ceiling for an infohash's shared reseed store. With `SEED_STORE_RETENTION` set this is a
/// hard safety cap; the age bound is the primary limiter on a live stream.
const SEED_STORE_BYTES: u64 = 128 * 1024 * 1024;

/// Default age bound for a live seed store: retain roughly the last 45s of downloaded pieces for
/// reseeding rather than filling `SEED_STORE_BYTES`. This tracks bitrate (window × rate) instead of
/// a fixed byte cap, so idle/steady RAM stays a fraction of the old default while still covering the
/// recent pieces live peers actually re-request. VOD stores keep the byte-only policy (`None`).
const SEED_STORE_RETENTION: Duration = Duration::from_secs(45);

/// Disk mode promises not to turn the configured on-disk budget into an equal per-stream RAM
/// allocation. If a per-stream directory becomes unavailable after startup, keep playback alive
/// with a zero-retention store: writes are immediately evicted and no piece payload accumulates.
const DISK_FAILURE_MEMORY_BYTES: u64 = 0;
static DISK_STORE_FAILURES: AtomicU64 = AtomicU64::new(0);

/// Build a [`PieceStore`] for `infohash` honoring the configured cache backend. In disk mode the
/// store lives under `<cache_dir>/<infohash_hex>-<generation>`, where `generation` is a
/// process-unique counter so each store instance owns a private directory — a stale store instance
/// can never clobber a re-created same-infohash store's data. If the directory cannot be prepared
/// (an unexpected mid-run I/O error — the common misconfiguration is caught at startup), playback
/// continues with a zero-retention memory store. This deliberately disables cache/seeding for that
/// stream rather than silently allocating the configured disk budget in RAM.
pub(crate) fn build_piece_store(
    piece_length: u64,
    chunk_length: u64,
    max_bytes: u64,
    retention: Option<Duration>,
    cache_type: CacheType,
    cache_dir: &Path,
    infohash: &[u8; 20],
) -> PieceStore {
    match cache_type {
        // Age retention applies to the in-RAM store only. Disk mode exists to retain *more* reseed
        // data than RAM allows, and its `shared_put_chunk_with_header` write path bypasses the
        // age-eviction code entirely, so a retention window there would be silently ineffective —
        // disk stores stay byte-only by design.
        CacheType::Memory => {
            let store = PieceStore::new(piece_length, chunk_length, max_bytes);
            match retention {
                Some(window) => store.with_retention(window),
                None => store,
            }
        }
        CacheType::Disk => {
            let dir = cache_dir.join(disk_store_subdir(infohash));
            PieceStore::with_backend(
                piece_length,
                chunk_length,
                max_bytes,
                BackendKind::Disk { dir: dir.clone() },
            )
            .unwrap_or_else(|e| {
                let failures = DISK_STORE_FAILURES.fetch_add(1, Ordering::Relaxed) + 1;
                crate::alog!(
                    "[cache] ERROR: disk store creation failure #{failures} at {}: {e}; continuing with ZERO piece retention (no RAM cache fallback)",
                    dir.display(),
                );
                PieceStore::new(piece_length, chunk_length, DISK_FAILURE_MEMORY_BYTES)
            })
        }
    }
}

/// Per-instance disk cache subdirectory name: `<infohash_hex>-<generation>`. The readable infohash
/// prefix aids operators; the monotonic suffix guarantees a fresh directory per store instance so
/// a stale store's `Drop` can never delete a re-created same-infohash store's data.
fn disk_store_subdir(infohash: &[u8; 20]) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static GENERATION: AtomicU64 = AtomicU64::new(0);
    let generation = GENERATION.fetch_add(1, Ordering::Relaxed);
    format!("{}-{generation}", infohash_hex(infohash))
}

/// Seeding configuration threaded through the download loop: the shared per-infohash
/// piece store (so downloaded data becomes servable to inbound peers too — T7), its size
/// budget, and whether reciprocal serving over THIS outbound connection is enabled at all.
#[derive(Clone)]
struct SeedConfig {
    metrics: Option<Arc<crate::live_stats::LiveMetrics>>,
    registry: SeedRegistry,
    store_bytes: u64,
    /// Age bound for the live seed store (primary limiter; `store_bytes` is the hard ceiling).
    store_retention: Option<Duration>,
    enabled: bool,
    /// Pieces behind the live edge the fresh follower starts at (playback cushion).
    prefetch_pieces: u64,
    /// Live lag-recovery policy and active upstream bounds.
    live_recovery: LiveRecoveryConfig,
    /// Backend the per-infohash seed store uses for piece data.
    cache_type: CacheType,
    /// Root dir for disk-mode piece files (per-infohash subdir derived from this).
    cache_dir: PathBuf,
    warm_peers: WarmPeerCache,
}

pub struct AceProvider {
    identity: Arc<Identity>,
    /// External inbound endpoint to advertise as a dial-able seeder on the periodic tracker +
    /// DHT self-announce. `None` when inbound seeding is disabled — we run no listener, so we
    /// must not invite peers to dial us (mirrors `BroadcastState::inbound_peer_port`). `Some`
    /// is the local `peer_port` today, or the mapped external port once #20 lands.
    announce_peer_port: tokio::sync::watch::Receiver<Option<u16>>,
    default_trackers: Vec<String>,
    bootstrap_peers: Vec<SocketAddrV4>,
    resolve_cache: ResolveCache,
    /// Raw wrapped-transport hashes resolved over BEP-9 peers with explicit opt-in. The raw
    /// hash does not establish a public content-id binding, and the infohash does not bind
    /// trackers/categories, so these results must
    /// never make an infohash openable (#164): they are never put into `infohash_index`.
    peer_resolve_cache: ResolveCache,
    bep9_content_id_fallback: bool,
    /// Live descriptors keyed by swarm infohash. Filled only by signed-catalog content-id
    /// resolutions (never BEP-9 or a transport url; own broadcasts are read from the seed
    /// registry instead), so a later bare-infohash open uses the real geometry, pubkey and
    /// trackers (#164). Separate from `resolve_cache`, which is keyed by the content-id string.
    infohash_index: InfohashIndex,
    /// Bounded short-lived suppression of failed cold lookups; never a source of metadata.
    infohash_failures: std::sync::Mutex<VecDeque<([u8; 20], Instant)>>,
    seed_registry: SeedRegistry,
    seed_store_bytes: u64,
    /// Age bound for live seed stores; `None` disables it (byte-only). Default `SEED_STORE_RETENTION`.
    seed_store_retention: Option<Duration>,
    prefetch_pieces: Option<u64>,
    startup_buffer: StartupBufferConfig,
    live_recovery: LiveRecoveryConfig,
    enable_seeding: bool,
    cache_type: CacheType,
    cache_dir: PathBuf,
    warm_peers: WarmPeerCache,
    /// Records the public IP peers echo back in `yourip` on our outbound handshakes (issue #22).
    /// `None` unless inbound serving is enabled — with inbound off we can't be dialed anyway, so
    /// harvesting is inert (never wired). Shared with the inbound listener and the daemon's
    /// periodic reachability status logger.
    reachability: Option<Arc<ReachabilityMonitor>>,
}

impl AceProvider {
    /// `peer_port` is the local AceStream peer-listener port (`config.peer_listen.port()`).
    /// Discovery and self-announces use the dynamic inbound endpoint when configured.
    /// Self-announcing as a dial-able seeder is opt-in via
    /// [`with_inbound_announce_port`](Self::with_inbound_announce_port); by default we do not
    /// advertise an inbound endpoint (a pure leecher / one-shot CLI play has no listener).
    pub fn new(identity: Arc<Identity>, _peer_port: u16) -> Self {
        AceProvider {
            identity,
            announce_peer_port: tokio::sync::watch::channel(None).1,
            default_trackers: DEFAULT_ACE_TRACKERS.iter().map(|s| s.to_string()).collect(),
            bootstrap_peers: Vec::new(),
            resolve_cache: ResolveCache::new(RESOLVE_CACHE_TTL),
            peer_resolve_cache: ResolveCache::new(RESOLVE_CACHE_TTL),
            bep9_content_id_fallback: false,
            infohash_index: InfohashIndex::new(INFOHASH_INDEX_CAPACITY),
            infohash_failures: std::sync::Mutex::new(VecDeque::new()),
            seed_registry: SeedRegistry::new(),
            seed_store_bytes: SEED_STORE_BYTES,
            seed_store_retention: Some(SEED_STORE_RETENTION),
            prefetch_pieces: None,
            startup_buffer: StartupBufferConfig::default(),
            live_recovery: LiveRecoveryConfig::default(),
            enable_seeding: true,
            cache_type: CacheType::Memory,
            cache_dir: PathBuf::new(),
            warm_peers: WarmPeerCache::memory(),
            reachability: None,
        }
    }

    /// Opt into resolving content ids as raw wrapped-transport hashes over BEP-9.
    pub fn with_bep9_content_id_fallback(mut self, enabled: bool) -> Self {
        self.bep9_content_id_fallback = enabled;
        self
    }

    /// Share a [`ReachabilityMonitor`] so `yourip` values peers echo on our outbound handshakes
    /// are harvested into the daemon-wide public-reachability view (issue #22). Pass `None`
    /// (the default) to disable harvesting entirely — the engine only supplies a monitor when
    /// inbound serving is on.
    pub fn with_reachability(mut self, reachability: Option<Arc<ReachabilityMonitor>>) -> Self {
        self.reachability = reachability;
        self
    }

    /// Set the external inbound endpoint advertised by the periodic seeder self-announce so
    /// tracker + DHT (and, by the same token, peers that discover us) all learn the same
    /// dial-able port. Pass `config.enable_inbound.then_some(config.peer_listen.port())` — the
    /// same resolved value threaded into `BroadcastState::inbound_peer_port`, so the leech and
    /// broadcast paths advertise the identical endpoint (the mapped external port once #20
    /// lands). `None` disables the self-announce entirely.
    pub fn with_inbound_announce_port(mut self, port: Option<u16>) -> Self {
        self.announce_peer_port = tokio::sync::watch::channel(port).1;
        self
    }

    /// Supply the daemon-wide, dynamically resolved inbound port. It starts at the local
    /// listener port, switches to the gateway-assigned external port after mapping succeeds,
    /// and falls back to the listener port if the mapping task disappears.
    pub fn with_inbound_announce_port_receiver(
        mut self,
        port: tokio::sync::watch::Receiver<Option<u16>>,
    ) -> Self {
        self.announce_peer_port = port;
        self
    }

    /// Port to advertise on the periodic seeder self-announce, or `None` to not announce at
    /// all. This is always the peer endpoint (`peer_port`/external), **never** the HTTP API
    /// port, and is `None` without an inbound listener to back it.
    #[cfg(test)]
    fn seeder_announce_port(&self) -> Option<u16> {
        *self.announce_peer_port.borrow()
    }

    fn discovery_announce_port(&self) -> u16 {
        self.announce_peer_port.borrow().unwrap_or(0)
    }

    /// Trackers used for content-id/metadata discovery and as the fallback where a descriptor
    /// has none (a bare infohash opens only from a verified descriptor). Transport files supply
    /// their own. Operators can extend this; DHT discovery runs alongside.
    pub fn with_trackers(mut self, trackers: Vec<String>) -> Self {
        self.default_trackers = trackers;
        self
    }

    /// Known peers to try in addition to tracker discovery. This mirrors the proven live
    /// path (a directly-supplied `ip:port`), letting the daemon serve a stream before DHT /
    /// ut_metadata discovery is wired.
    pub fn with_bootstrap_peers(mut self, peers: Vec<SocketAddrV4>) -> Self {
        self.bootstrap_peers = peers;
        self
    }

    /// Share a `SeedRegistry` with the inbound listener, so pieces this provider downloads
    /// become servable to peers connecting in. Defaults to a private (unshared) registry.
    pub fn with_seed_registry(mut self, registry: SeedRegistry) -> Self {
        self.seed_registry = registry;
        self
    }

    /// Override how many bytes of piece data each infohash's shared store retains. First-writer
    /// wins per infohash: `SeedRegistry::get_or_create` only sizes a store when it's first
    /// created, so changing this after a stream has already opened has no effect on it.
    pub fn with_seed_store_bytes(mut self, bytes: u64) -> Self {
        self.seed_store_bytes = bytes;
        self
    }

    /// Bound live seed stores by age: retain roughly the last `window` of downloaded pieces for
    /// reseeding instead of filling the byte budget, so RAM tracks bitrate. A zero `window`
    /// disables the age bound (byte-only). Does not affect VOD stores. First-writer wins per
    /// infohash, same as [`with_seed_store_bytes`](Self::with_seed_store_bytes).
    pub fn with_seed_store_retention(mut self, window: Duration) -> Self {
        self.seed_store_retention = (!window.is_zero()).then_some(window);
        self
    }

    /// Select where the per-infohash seed store keeps piece data. In `Disk` mode each store
    /// lives under `<cache_dir>/<infohash_hex>`. A nonempty directory also stores private
    /// productive-peer hints in either backend; an empty directory keeps hints in memory.
    /// Defaults to `Memory`.
    pub fn with_cache(mut self, cache_type: CacheType, cache_dir: PathBuf) -> Self {
        self.cache_type = cache_type;
        self.warm_peers = WarmPeerCache::new(&cache_dir, false);
        self.cache_dir = cache_dir;
        self
    }

    #[cfg(test)]
    fn with_warm_cache_loopback_policy(mut self, cache_dir: PathBuf) -> Self {
        self.warm_peers = WarmPeerCache::new(&cache_dir, true);
        self.cache_dir = cache_dir;
        self
    }

    /// Set an exact operator prefetch depth, or preserve `None` for derived policy selection.
    pub fn with_prefetch_pieces(mut self, pieces: Option<u64>) -> Self {
        self.prefetch_pieces = pieces;
        self
    }

    /// Store the validated startup buffering policy for installation around the live source.
    pub fn with_startup_buffer(mut self, startup_buffer: StartupBufferConfig) -> Self {
        self.startup_buffer = startup_buffer;
        self
    }

    #[cfg(test)]
    pub(crate) fn prefetch_policy(&self) -> Option<u64> {
        self.prefetch_pieces
    }

    #[cfg(test)]
    pub(crate) fn startup_buffer_config(&self) -> StartupBufferConfig {
        self.startup_buffer
    }

    #[cfg(test)]
    pub(crate) fn live_recovery_config(&self) -> LiveRecoveryConfig {
        self.live_recovery
    }

    fn prefetch_policy_for(&self, info: &StreamInfo) -> u64 {
        self.prefetch_pieces
            .unwrap_or_else(|| {
                derived_prefetch_pieces(
                    self.startup_buffer.target_ms,
                    info.metadata.bitrate,
                    info.piece_length,
                    info.sig_len,
                )
            })
            .min(self.live_recovery.max_reasm_pieces_ahead)
    }

    /// Override the live lag-recovery and active upstream policy. Values are validated by
    /// runtime config parsing before this builder is called.
    pub fn with_live_recovery(mut self, live_recovery: LiveRecoveryConfig) -> Self {
        self.live_recovery = live_recovery;
        self
    }

    /// Enable/disable reciprocal serving over outbound (leecher) connections — answering a
    /// peer's `Interested`/chunk-requests and advertising `Have` for newly-completed pieces.
    /// Defaults to `true` (S1 behavior). Setting `false` stops reciprocal serving on these
    /// outbound connections; it does not gate the inbound peer listener or the seeder
    /// self-announce (both keyed on `enable_inbound`).
    pub fn with_seeding_enabled(mut self, enabled: bool) -> Self {
        self.enable_seeding = enabled;
        self
    }
}

/// Resolve the live history depth needed to fill the startup reservoir. Descriptor bytes/s use
/// media payload bytes per piece plus a two-piece scheduling margin. Without a bitrate hint,
/// enabled startup buffering uses a conservative historical window; disabled buffering keeps
/// the legacy depth.
fn derived_prefetch_pieces(
    target_ms: u64,
    bitrate: Option<u64>,
    piece_length: u64,
    sig_len: usize,
) -> u64 {
    if target_ms == 0 {
        return PREFETCH_PIECES;
    }
    let Some(bytes_per_second) = bitrate.filter(|rate| *rate > 0) else {
        return UNKNOWN_BITRATE_BUFFER_PREFETCH_PIECES;
    };
    let payload = piece_length.saturating_sub(sig_len as u64).max(1) as u128;
    let Some(byte_millis) = (target_ms as u128).checked_mul(bytes_per_second as u128) else {
        return u64::MAX;
    };
    let Some(piece_byte_millis) = 1_000_u128.checked_mul(payload) else {
        return u64::MAX;
    };
    let pieces = byte_millis.div_ceil(piece_byte_millis).saturating_add(2);
    u64::try_from(pieces).unwrap_or(u64::MAX)
}

#[async_trait]
impl StreamProvider for AceProvider {
    fn network(&self) -> &'static str {
        "ace"
    }

    async fn resolve_vod(&self, id: &str) -> Result<Box<dyn VodContent>, ProviderError> {
        self.resolve_vod_inner(id).await
    }

    fn check_openable(&self, _id: &str) -> Result<(), ProviderError> {
        // Cold infohash lookup is asynchronous. Actual resolution still requires an
        // authenticated descriptor bound to the requested infohash before discovery.
        Ok(())
    }

    fn remember_live_descriptor(&self, info: &StreamInfo) {
        self.infohash_index.put(info.clone());
    }

    async fn open(&self, id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        // One resolver for every live entry point. A bare infohash without a verified
        // descriptor fails closed here, before any discovery (#164).
        let opening = Instant::now();
        crate::alog!("[ace] discovery stage=open-request");
        let info = self.resolve_live_info(id).await?;
        crate::alog!(
            "[ace] discovery stage=resolved resolve_ms={}",
            opening.elapsed().as_millis()
        );

        let trackers = info.trackers.clone();
        let infohash = info.infohash;
        let port = self.discovery_announce_port();
        let discovery: PeerDiscovery = Arc::new(move |options, sender| {
            let trackers = trackers.clone();
            Box::pin(async move {
                discover_peers_incremental(
                    &trackers,
                    &infohash,
                    &random_peer_id(),
                    port,
                    options,
                    sender,
                )
                .await
            })
        });
        self.open_resolved(id, info, discovery).await
    }
}

/// Test-only builders shared by the provider and HTTP tests.
#[cfg(test)]
pub(crate) mod test_support {
    use ace_wire::bencode::Bencode;
    use std::collections::BTreeMap;

    pub(crate) fn bep9_enabled(provider: &super::AceProvider) -> bool {
        provider.bep9_content_id_fallback
    }

    /// A synthetic live `AceStreamTransport` built with outpace's own encoder and a freshly
    /// generated RSA source key (#164). Nothing is derived from a real stream; callers compute
    /// its infohash at test time. Returns `(transport_bytes, pubkey_der)`.
    pub(crate) fn synthetic_live_transport(piece_length: i64) -> (Vec<u8>, Vec<u8>) {
        let pubkey = ace_wire::live_auth::LiveSourceAuth::generate().pubkey_der();
        let mut d: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        d.insert(b"name".to_vec(), Bencode::Bytes(b"Synthetic Live".to_vec()));
        d.insert(b"piece_length".to_vec(), Bencode::Int(piece_length));
        d.insert(b"chunk_length".to_vec(), Bencode::Int(16_384));
        d.insert(b"bitrate".to_vec(), Bencode::Int(1_000_000));
        d.insert(b"authmethod".to_vec(), Bencode::Bytes(b"RSA".to_vec()));
        d.insert(b"pubkey".to_vec(), Bencode::Bytes(pubkey.clone()));
        d.insert(
            b"trackers".to_vec(),
            Bencode::List(vec![Bencode::Bytes(b"udp://tracker.invalid:80".to_vec())]),
        );
        (
            ace_wire::transport::encode_transport(&Bencode::Dict(d)),
            pubkey,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LiveRecoveryConfig;
    use ace_swarm::scheduler::{ActivePeers, PeerAssignment};

    fn default_live_recovery() -> LiveRecoveryConfig {
        LiveRecoveryConfig::default()
    }

    #[test]
    fn recovery_channel_capacities_are_checked() {
        let policy = LiveRecoveryConfig {
            max_active_upstreams: 2,
            ..LiveRecoveryConfig::default()
        };
        assert_eq!(recovery_channel_capacities(policy).unwrap(), (64, 2));

        let invalid = LiveRecoveryConfig {
            max_active_upstreams: usize::MAX,
            ..LiveRecoveryConfig::default()
        };
        assert!(recovery_channel_capacities(invalid).is_err());
    }

    #[tokio::test]
    async fn queued_live_outputs_keep_each_discontinuity_with_its_chunk() {
        let (tx, rx) = mpsc::channel(8);
        for (bytes, discontinuity) in [
            (b"pre".as_slice(), false),
            (b"post-one".as_slice(), true),
            (b"between".as_slice(), false),
            (b"post-two".as_slice(), true),
        ] {
            tx.send(LiveOutput {
                bytes: Bytes::copy_from_slice(bytes),
                discontinuity,
            })
            .await
            .unwrap();
        }
        drop(tx);
        let mut source = AceSource {
            metrics: None,
            _sampler: None,
            rx,
            discontinuity: false,
            peers: Arc::new(AtomicU32::new(0)),
            downloaded: Arc::new(AtomicU64::new(0)),
            uploaded: Arc::new(AtomicU64::new(0)),
            peers_served: Arc::new(AtomicU32::new(0)),
            metadata: StreamMetadata::default(),
        };

        for (expected, gap) in [
            (b"pre".as_slice(), false),
            (b"post-one".as_slice(), true),
            (b"between".as_slice(), false),
            (b"post-two".as_slice(), true),
        ] {
            assert_eq!(source.next().await.unwrap().as_ref(), expected);
            assert_eq!(source.take_discontinuity(), gap);
        }
    }

    #[tokio::test]
    async fn network_is_ace() {
        let p = AceProvider::new(Arc::new(Identity::generate()), 6878);
        assert_eq!(p.network(), "ace");
    }

    #[test]
    fn startup_policy_builders_preserve_absent_and_explicit_values() {
        let startup = crate::config::StartupBufferConfig {
            target_ms: 12_000,
            max_bytes: 33_554_432,
            timeout_ms: 9_000,
        };
        let default = AceProvider::new(Arc::new(Identity::generate()), 6878);
        assert_eq!(default.prefetch_policy(), None);
        assert_eq!(
            default.startup_buffer_config(),
            crate::config::StartupBufferConfig::default()
        );

        let configured = AceProvider::new(Arc::new(Identity::generate()), 6878)
            .with_prefetch_pieces(Some(8))
            .with_startup_buffer(startup);
        assert_eq!(configured.prefetch_policy(), Some(8));
        assert_eq!(configured.startup_buffer_config(), startup);
    }

    #[test]
    fn prefetch_derives_history_from_target_bitrate_and_payload_with_two_piece_margin() {
        assert_eq!(
            derived_prefetch_pieces(30_000, Some(1_000_000), 524_288, 128),
            60
        );
    }

    #[test]
    fn prefetch_positive_fractional_byte_still_requires_one_history_piece() {
        assert_eq!(derived_prefetch_pieces(1, Some(1), 1_048_576, 96), 3);
    }

    #[test]
    fn prefetch_fallback_depends_on_whether_prebuffer_is_enabled() {
        assert_eq!(derived_prefetch_pieces(30_000, None, 1_048_576, 96), 32);
        assert_eq!(derived_prefetch_pieces(0, None, 1_048_576, 96), 8);
    }

    #[test]
    fn prefetch_disabled_with_known_bitrate_preserves_eight_pieces() {
        assert_eq!(derived_prefetch_pieces(0, Some(1_000_000), 524_288, 128), 8);
        assert_eq!(derived_prefetch_pieces(0, Some(0), 524_288, 128), 8);
    }

    #[test]
    fn prefetch_zero_bitrate_uses_unknown_history_fallback() {
        assert_eq!(derived_prefetch_pieces(30_000, Some(0), 524_288, 128), 32);
    }

    #[test]
    fn prefetch_bounds_huge_rates_payload_and_peer_window() {
        assert_eq!(
            derived_prefetch_pieces(u64::MAX, Some(u64::MAX), 0, 128),
            u64::MAX
        );
        assert_eq!(derived_prefetch_pieces(1_000, Some(1), 128, 128), 3);
        let mut descriptor = info();
        descriptor.metadata.bitrate = Some(u64::MAX);
        let recovery = LiveRecoveryConfig {
            max_reasm_pieces_ahead: 64,
            ..Default::default()
        };
        let provider =
            AceProvider::new(Arc::new(Identity::generate()), 6878).with_live_recovery(recovery);
        let depth = provider.prefetch_policy_for(&descriptor);
        assert_eq!(depth, 64);
        assert_eq!(prefetch_start(180, 200, depth), 180);
        assert_eq!(prefetch_start(0, 20, u64::MAX), 0);
    }

    #[test]
    fn prefetch_explicit_override_is_not_reinterpreted() {
        let provider =
            AceProvider::new(Arc::new(Identity::generate()), 6878).with_prefetch_pieces(Some(3));
        assert_eq!(provider.prefetch_policy_for(&info()), 3);
    }

    #[tokio::test]
    async fn resolve_vod_rejects_bare_infohash() {
        // A bare infohash carries no transport descriptor, so it has no VOD piece hashes.
        // This resolves synchronously (no network) to a clear error.
        let p = AceProvider::new(Arc::new(Identity::generate()), 6878);
        let err = p
            .resolve_vod("00112233445566778899aabbccddeeff00112233")
            .await
            .err();
        assert!(err.is_some(), "a bare infohash is not a VOD target");
    }

    #[tokio::test]
    async fn vod_source_trims_received_pieces_to_the_requested_byte_range() {
        // Three whole "pieces" of 4 bytes arrive in order. The source is opened for a range that
        // starts 2 bytes into the first covering piece and spans 5 bytes total, so it must drop
        // the leading 2 bytes and stop after 5 emitted bytes — never leaking bytes outside the
        // range even though the covering pieces carry more.
        let (tx, rx) = mpsc::channel::<Bytes>(4);
        tx.send(Bytes::from_static(b"AAAA")).await.unwrap();
        tx.send(Bytes::from_static(b"BBBB")).await.unwrap();
        tx.send(Bytes::from_static(b"CCCC")).await.unwrap();
        drop(tx);
        let mut src = VodSource {
            prefix: VecDeque::new(),
            rx,
            skip: 2,
            remaining: 5,
            emit_len: 5,
        };
        assert_eq!(src.content_length(), 5);
        let mut got = Vec::new();
        while let Some(chunk) = src.next().await {
            got.extend_from_slice(&chunk);
        }
        assert_eq!(&got, b"AABBB");
    }

    #[tokio::test]
    async fn vod_source_emits_cached_prefix_before_downloaded_suffix() {
        // First covering piece "AAAA" is cached (prefix); the suffix "BBBB","CCCC" streams in.
        // The range starts 2 bytes into the first piece and spans 5 bytes, so trimming must apply
        // uniformly across the prefix/stream boundary: "AA" + "BBB".
        let (tx, rx) = mpsc::channel::<Bytes>(4);
        tx.send(Bytes::from_static(b"BBBB")).await.unwrap();
        tx.send(Bytes::from_static(b"CCCC")).await.unwrap();
        drop(tx);
        let mut src = VodSource {
            prefix: VecDeque::from(vec![Bytes::from_static(b"AAAA")]),
            rx,
            skip: 2,
            remaining: 5,
            emit_len: 5,
        };
        let mut got = Vec::new();
        while let Some(chunk) = src.next().await {
            got.extend_from_slice(&chunk);
        }
        assert_eq!(&got, b"AABBB");
    }

    // Build `total_len` bytes plus a matching VodInfo (SHA-1 piece hashes over that content).
    fn make_vod_content(
        piece_length: u64,
        chunk_length: u64,
        total_len: u64,
    ) -> (Vec<u8>, VodInfo) {
        use sha1::{Digest, Sha1};
        let content: Vec<u8> = (0..total_len).map(|i| (i % 251) as u8).collect();
        let piece_count = total_len.div_ceil(piece_length);
        let mut piece_hashes = Vec::new();
        for p in 0..piece_count {
            let start = (p * piece_length) as usize;
            let end = ((p + 1) * piece_length).min(total_len) as usize;
            let h: [u8; 20] = Sha1::digest(&content[start..end]).into();
            piece_hashes.push(h);
        }
        let info = VodInfo {
            infohash: [0x42; 20],
            piece_length,
            chunk_length,
            trackers: vec![],
            piece_hashes,
            total_length: total_len,
        };
        (content, info)
    }

    // A minimal standard-BitTorrent VOD seeder that tallies how many block bytes it serves, so a
    // test can assert a piece was fetched from the swarm at most once.
    async fn spawn_counting_vod_seeder(
        content: Vec<u8>,
        info: VodInfo,
        served: Arc<AtomicU64>,
    ) -> SocketAddrV4 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = match listener.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!(),
        };
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => break,
                };
                let content = content.clone();
                let info = info.clone();
                let served = served.clone();
                tokio::spawn(async move {
                    let mut hs = [0u8; ace_wire::handshake::HANDSHAKE_LEN];
                    if sock.read_exact(&mut hs).await.is_err() {
                        return;
                    }
                    let reply =
                        ace_wire::handshake::Handshake::new(info.infohash, random_peer_id())
                            .encode();
                    if sock.write_all(&reply).await.is_err() {
                        return;
                    }
                    let nbytes = (info.piece_count() as usize).div_ceil(8);
                    let mut bits = vec![0u8; nbytes];
                    for p in 0..info.piece_count() as usize {
                        bits[p / 8] |= 0x80 >> (p % 8);
                    }
                    let _ = sock.write_all(&PeerMessage::Bitfield(bits).encode()).await;
                    let _ = sock.write_all(&PeerMessage::Unchoke.encode()).await;
                    let mut buf: Vec<u8> = Vec::new();
                    let mut tmp = [0u8; 4096];
                    loop {
                        loop {
                            match PeerMessage::decode(&buf) {
                                Ok(Some((msg, used))) => {
                                    buf.drain(..used);
                                    if let PeerMessage::Request {
                                        index,
                                        begin,
                                        length,
                                    } = msg
                                    {
                                        let start = (index as u64 * info.piece_length
                                            + begin as u64)
                                            as usize;
                                        let end = start + length as usize;
                                        let block = content[start..end].to_vec();
                                        served.fetch_add(block.len() as u64, Ordering::SeqCst);
                                        let piece = PeerMessage::Piece {
                                            index,
                                            begin,
                                            block,
                                        }
                                        .encode();
                                        if sock.write_all(&piece).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                                Ok(None) => break,
                                Err(_) => return,
                            }
                        }
                        let n = match sock.read(&mut tmp).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&tmp[..n]);
                    }
                });
            }
        });
        addr
    }

    async fn read_all(mut src: Box<dyn VodByteSource>) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = src.next().await {
            out.extend_from_slice(&chunk);
        }
        out
    }

    #[tokio::test]
    async fn open_range_downloads_each_covering_piece_at_most_once() {
        // 3 pieces of 16 bytes (one 16-byte block per piece). Two reads that both fall inside
        // piece 0: the second must be served from the cache the first populated, not re-downloaded.
        let (content, info) = make_vod_content(16, 16, 48);
        assert_eq!(info.piece_count(), 3);
        let served = Arc::new(AtomicU64::new(0));
        let addr = spawn_counting_vod_seeder(content.clone(), info.clone(), served.clone()).await;
        let registry = SeedRegistry::new();
        let store = registry.lease_store(info.infohash, || PieceStore::new(16, 16, 128));
        let vod = AceVodContent {
            info,
            bootstrap_peers: vec![addr],
            announce_peer_port: tokio::sync::watch::channel(None).1,
            store,
            peers: Arc::new(tokio::sync::Mutex::new(None)),
            range_lock: Arc::new(tokio::sync::Mutex::new(())),
        };

        let first = read_all(vod.open_range(0, 3).await.unwrap()).await;
        assert_eq!(first, content[0..=3]);
        let second = read_all(vod.open_range(8, 11).await.unwrap()).await;
        assert_eq!(second, content[8..=11]);

        // Piece 0 is 16 bytes; both reads cover it, but the swarm served it exactly once.
        assert_eq!(
            served.load(Ordering::SeqCst),
            16,
            "the covering piece is downloaded once, then reused from cache"
        );
    }

    #[tokio::test]
    async fn open_range_spanning_a_new_piece_downloads_only_the_missing_suffix() {
        // Read piece 0 first (caches it), then read [0, 20] which spans pieces 0 and 1: piece 0 is
        // served from cache, only piece 1 is fetched. The assembled bytes must still be correct.
        let (content, info) = make_vod_content(16, 16, 48);
        let served = Arc::new(AtomicU64::new(0));
        let addr = spawn_counting_vod_seeder(content.clone(), info.clone(), served.clone()).await;
        let registry = SeedRegistry::new();
        let store = registry.lease_store(info.infohash, || PieceStore::new(16, 16, 128));
        let vod = AceVodContent {
            info,
            bootstrap_peers: vec![addr],
            announce_peer_port: tokio::sync::watch::channel(None).1,
            store,
            peers: Arc::new(tokio::sync::Mutex::new(None)),
            range_lock: Arc::new(tokio::sync::Mutex::new(())),
        };

        let _ = read_all(vod.open_range(0, 15).await.unwrap()).await; // piece 0
        let spanning = read_all(vod.open_range(0, 20).await.unwrap()).await;
        assert_eq!(spanning, content[0..=20]);
        // Piece 0 (16) downloaded once for the first read; piece 1 (16) for the spanning read.
        assert_eq!(served.load(Ordering::SeqCst), 32);
    }

    #[tokio::test]
    async fn downloaded_vod_piece_is_served_to_an_inbound_bittorrent_peer() {
        let (content, info) = make_vod_content(16, 8, 32);
        let upstream =
            spawn_counting_vod_seeder(content.clone(), info.clone(), Arc::new(AtomicU64::new(0)))
                .await;
        let registry = SeedRegistry::new();
        let store = registry.lease_store(info.infohash, || PieceStore::new(16, 8, 128));
        let vod = AceVodContent {
            info: info.clone(),
            bootstrap_peers: vec![upstream],
            announce_peer_port: tokio::sync::watch::channel(None).1,
            store,
            peers: Arc::new(tokio::sync::Mutex::new(None)),
            range_lock: Arc::new(tokio::sync::Mutex::new(())),
        };
        assert_eq!(
            read_all(vod.open_range(0, 15).await.unwrap()).await,
            content[..16]
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(ace_swarm::listen::PeerListener::serve(
            listener,
            registry,
            random_peer_id(),
            [0; 8],
            8,
            Arc::new(Identity::generate()),
            8,
            None,
        ));
        let mut peer = PeerSession::new(TcpStream::connect(addr).await.unwrap());
        peer.perform_handshake(info.infohash, random_peer_id())
            .await
            .unwrap();
        peer.send(&PeerMessage::Interested).await.unwrap();
        loop {
            if matches!(peer.read_message().await.unwrap(), PeerMessage::Unchoke) {
                break;
            }
        }
        peer.send(&PeerMessage::Request {
            index: 0,
            begin: 4,
            length: 8,
        })
        .await
        .unwrap();
        loop {
            if let PeerMessage::Piece {
                index,
                begin,
                block,
            } = peer.read_message().await.unwrap()
            {
                assert_eq!((index, begin), (0, 4));
                assert_eq!(block, content[4..12]);
                break;
            }
        }
    }

    #[tokio::test]
    async fn cancelled_range_finishes_background_transaction_before_overlap_downloads() {
        let (content, info) = make_vod_content(16, 16, 32);
        let served = Arc::new(AtomicU64::new(0));
        let upstream =
            spawn_counting_vod_seeder(content.clone(), info.clone(), served.clone()).await;
        let registry = SeedRegistry::new();
        let store = registry.lease_store(info.infohash, || PieceStore::new(16, 16, 128));
        let range_lock = Arc::new(tokio::sync::Mutex::new(()));
        let vod = AceVodContent {
            info,
            bootstrap_peers: vec![upstream],
            announce_peer_port: tokio::sync::watch::channel(None).1,
            store,
            peers: Arc::new(tokio::sync::Mutex::new(None)),
            range_lock: range_lock.clone(),
        };

        let cancelled = vod.open_range(0, 15).await.unwrap();
        drop(cancelled);
        // The supervisor owns the lock until its downloader observes cancellation and exits.
        let guard = tokio::time::timeout(Duration::from_secs(1), range_lock.clone().lock_owned())
            .await
            .expect("cancelled background transaction shuts down");
        drop(guard);

        assert_eq!(
            read_all(vod.open_range(0, 15).await.unwrap()).await,
            content[..16]
        );
        assert_eq!(
            served.load(Ordering::SeqCst),
            16,
            "cancelled overlap cannot race a duplicate piece download/write"
        );
    }

    #[tokio::test]
    async fn leech_lease_evicts_registry_entry_when_dropped() {
        let reg = ace_swarm::listen::SeedRegistry::new();
        let ih = [9u8; 20];
        {
            let (_store, _lease) = reg.lease_store(ih, || {
                ace_swarm::store::PieceStore::new(1 << 20, 1 << 14, 1 << 20)
            });
            assert!(
                reg.serves(&ih),
                "served while the leech loop holds its lease"
            );
        }
        assert!(
            !reg.serves(&ih),
            "entry evicted after the leech loop drops its lease"
        );
    }

    #[test]
    fn disk_store_dir_is_process_unique_per_instance() {
        let tmp = std::env::temp_dir().join(format!("outpace-uniqdir-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let ih = [7u8; 20];
        // Two stores for the SAME infohash must land in DIFFERENT directories.
        let _s1 = build_piece_store(1 << 20, 1 << 14, 1 << 20, None, CacheType::Disk, &tmp, &ih);
        let _s2 = build_piece_store(1 << 20, 1 << 14, 1 << 20, None, CacheType::Disk, &tmp, &ih);
        let dirs: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(
            dirs.len(),
            2,
            "each store instance owns its own dir: {dirs:?}"
        );
        assert!(
            dirs.iter().all(|d| d.starts_with(&infohash_hex(&ih))),
            "dir names keep the readable infohash prefix: {dirs:?}"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn disk_construction_failure_keeps_playback_store_at_zero_retention() {
        let invalid_root = std::env::temp_dir().join(format!(
            "outpace-cache-failure-{}-{}",
            std::process::id(),
            DISK_STORE_FAILURES.load(Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&invalid_root);
        std::fs::write(&invalid_root, b"file blocks child directory creation").unwrap();
        let mut store = build_piece_store(
            8,
            4,
            128 * 1024 * 1024,
            None,
            CacheType::Disk,
            &invalid_root,
            &[0x38; 20],
        );

        store.put_chunk(0, 0, &[1, 2, 3, 4]);
        assert!(
            store.chunk(0, 0).is_none(),
            "disk failure policy must not retain the configured disk budget in RAM"
        );
        assert!(store.have_pieces().is_empty());
        std::fs::remove_file(invalid_root).unwrap();
    }

    #[tokio::test]
    async fn unrecognized_id_shape_is_backend_error() {
        let p = AceProvider::new(Arc::new(Identity::generate()), 6878);
        // Neither a 40-hex infohash nor a cid:<40hex> content-id.
        assert!(matches!(
            p.open("not-a-hex-infohash").await,
            Err(ProviderError::Backend(_))
        ));
    }

    const PLACEHOLDER_ID: &str = "0123456789abcdef0123456789abcdef01234567";

    fn test_provider() -> AceProvider {
        AceProvider::new(Arc::new(Identity::generate()), 0)
    }

    #[test]
    fn cold_infohash_is_admitted_for_async_catalog_resolution() {
        let p = test_provider();
        assert!(
            p.check_openable(PLACEHOLDER_ID).is_ok(),
            "the synchronous pre-check must allow the asynchronous catalog lookup"
        );
    }

    #[tokio::test]
    async fn cold_infohash_catalog_result_is_verified_and_indexed() {
        let (transport, _) = test_support::synthetic_live_transport(524_288);
        let expected = stream_info_from_transport(&transport).unwrap();
        let id = infohash_hex(&expected.infohash).to_ascii_uppercase();
        let p = test_provider();
        let actual = p
            .resolve_infohash_with_catalog(&id, std::future::ready(Ok(expected.clone())))
            .await
            .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(p.verified_info_for_infohash(&id).unwrap(), expected);
        assert_eq!(
            p.resolve_infohash_with_catalog(&id, async {
                panic!("verified index hit must not query the catalog again")
            })
            .await
            .unwrap(),
            expected
        );
    }

    #[tokio::test]
    async fn cold_infohash_rejects_mismatched_catalog_descriptor_without_indexing_it() {
        let (transport, _) = test_support::synthetic_live_transport(524_288);
        let foreign = stream_info_from_transport(&transport).unwrap();
        let foreign_id = infohash_hex(&foreign.infohash);
        let p = test_provider();
        assert!(matches!(
            p.resolve_infohash_with_catalog(PLACEHOLDER_ID, std::future::ready(Ok(foreign)))
                .await,
            Err(ProviderError::Unresolvable(_))
        ));
        assert!(p.verified_info_for_infohash(PLACEHOLDER_ID).is_err());
        assert!(p.verified_info_for_infohash(&foreign_id).is_err());
    }

    #[tokio::test]
    async fn cold_infohash_failed_lookup_is_short_cached() {
        let p = test_provider();
        assert!(p
            .resolve_infohash_with_catalog(
                PLACEHOLDER_ID,
                std::future::ready(Err(ResolveError::Catalog("not found"))),
            )
            .await
            .is_err());
        assert!(p
            .resolve_infohash_with_catalog(PLACEHOLDER_ID, async {
                panic!("a recent failure must suppress another catalog request")
            })
            .await
            .is_err());
    }

    #[tokio::test]
    async fn cold_infohash_failure_expires_and_verified_metadata_wins() {
        let (transport, _) = test_support::synthetic_live_transport(524_288);
        let expected = stream_info_from_transport(&transport).unwrap();
        let id = infohash_hex(&expected.infohash);
        let p = test_provider();
        p.infohash_failures.lock().unwrap().push_back((
            expected.infohash,
            Instant::now() - INFOHASH_FAILURE_TTL - Duration::from_secs(1),
        ));
        assert_eq!(
            p.resolve_infohash_with_catalog(&id, std::future::ready(Ok(expected.clone())))
                .await
                .unwrap(),
            expected
        );
        p.remember_infohash_failure(expected.infohash);
        assert_eq!(
            p.resolve_infohash_with_catalog(&id, async {
                panic!("a verified descriptor must win over an earlier failure")
            })
            .await
            .unwrap(),
            expected
        );
    }

    #[test]
    fn cold_infohash_failure_cache_is_bounded_and_deduplicated() {
        let p = test_provider();
        for n in 0..=INFOHASH_FAILURE_CAPACITY {
            p.remember_infohash_failure([n as u8; 20]);
        }
        p.remember_infohash_failure([INFOHASH_FAILURE_CAPACITY as u8; 20]);
        let entries = p.infohash_failures.lock().unwrap();
        assert_eq!(entries.len(), INFOHASH_FAILURE_CAPACITY);
        assert!(!entries.iter().any(|(key, _)| *key == [0; 20]));
        assert_eq!(
            entries
                .iter()
                .filter(|(key, _)| *key == [INFOHASH_FAILURE_CAPACITY as u8; 20])
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn unavailable_infohash_fails_closed_after_catalog_lookup() {
        let p = test_provider();
        let err = p
            .resolve_infohash_with_catalog(
                PLACEHOLDER_ID,
                std::future::ready(Err(ResolveError::Catalog("not found"))),
            )
            .await
            .expect_err("missing catalog metadata must not invent stream geometry");
        match err {
            ProviderError::Unresolvable(msg) => {
                assert!(msg.contains(&format!("cid:{PLACEHOLDER_ID}")), "{msg}");
            }
            other => panic!("expected Unresolvable, got {other:?}"),
        }
        assert!(p.check_openable(PLACEHOLDER_ID).is_ok());
        assert!(p.verified_info_for_infohash(PLACEHOLDER_ID).is_err());
    }

    #[tokio::test]
    async fn bare_infohash_uses_the_verified_descriptors_geometry_and_pubkey() {
        let (transport, pubkey) = test_support::synthetic_live_transport(524_288);
        let verified = ace_swarm::resolve::stream_info_from_transport(&transport).unwrap();
        let p = test_provider();
        p.remember_live_descriptor(&verified);

        // Hex ids are case-insensitive: an upper-case infohash must hit the same entry.
        let id = infohash_hex(&verified.infohash).to_ascii_uppercase();
        assert!(p.check_openable(&id).is_ok());
        let info = p.resolve_live_info(&id).await.unwrap();
        assert_eq!(
            info.infohash,
            ace_wire::infohash::infohash_of_transport(&transport)
        );
        assert_eq!(info.piece_length, 524_288, "not the old 1 MiB guess");
        assert_eq!(info.chunk_length, 16_384);
        assert_eq!(info.sig_len, 96);
        assert_eq!(
            info.source_pubkey, pubkey,
            "pubkey enables RSA piece verification"
        );
        assert_eq!(info.metadata.title.as_deref(), Some("Synthetic Live"));
    }

    #[tokio::test]
    async fn warm_infohash_vector_preserves_non_default_geometry() {
        let transport = include_bytes!("../../../tests/vectors/transport/synthetic-live-512k.bin");
        let descriptor = ace_wire::transport::decode_transport(transport).unwrap();
        let verified = ace_swarm::resolve::stream_info_from_transport(transport).unwrap();
        let p = test_provider();
        p.resolve_cache.put(PLACEHOLDER_ID, verified.clone());
        p.resolve_live_info(&format!("cid:{PLACEHOLDER_ID}"))
            .await
            .unwrap();

        let info = p
            .resolve_live_info(&infohash_hex(&verified.infohash))
            .await
            .unwrap();
        assert_eq!(info.piece_length, 524_288);
        assert_eq!(info.chunk_length, 16_384);
        assert_eq!(info.chunks_per_piece(), 32);
        assert_eq!(info.sig_len, 96);
        assert_eq!(info.source_pubkey, descriptor.pubkey);
        assert!(!info.source_pubkey.is_empty());
        assert_eq!(info.trackers, vec!["udp://tracker.invalid:80"]);
    }

    #[tokio::test]
    async fn warm_infohash_continuity_authenticates_pieces_before_emitting() {
        let auth = ace_wire::live_auth::LiveSourceAuth::generate();
        let (transport, _) = test_support::synthetic_live_transport(524_288);
        // Keep the descriptor and its computed infohash bound to this test's signing key.
        let decoded = ace_wire::transport::decode_transport(&transport).unwrap();
        let ace_wire::bencode::Bencode::Dict(mut fields) = decoded.raw else {
            panic!("synthetic transport must be a dict");
        };
        fields.insert(
            b"pubkey".to_vec(),
            ace_wire::bencode::Bencode::Bytes(auth.pubkey_der()),
        );
        let transport =
            ace_wire::transport::encode_transport(&ace_wire::bencode::Bencode::Dict(fields));
        let verified = ace_swarm::resolve::stream_info_from_transport(&transport).unwrap();
        let p = test_provider();
        p.resolve_cache.put(PLACEHOLDER_ID, verified.clone());
        p.resolve_live_info(&format!("cid:{PLACEHOLDER_ID}"))
            .await
            .unwrap();
        let info = p
            .resolve_live_info(&infohash_hex(&verified.infohash))
            .await
            .unwrap();
        let (mut continuity, start) = Continuity::fresh(&info, 7, 7, 0, default_live_recovery());
        let payload = vec![0x47; info.piece_length as usize - auth.signature_len()];
        let mut piece = payload.clone();
        piece.extend(auth.sign(&payload));
        let mut corrupt = piece.clone();
        corrupt[0] ^= 1;
        let chunk_length = info.chunk_length as usize;
        for (index, block) in corrupt.chunks(chunk_length).enumerate() {
            let result = continuity
                .reasm
                .add_block(start, (index * chunk_length) as u64, block);
            if index + 1 == info.chunks_per_piece() as usize {
                assert!(result.is_err(), "bad RSA signature must reject the piece");
            } else {
                result.unwrap();
            }
        }
        assert!(continuity.reasm.take_ready().is_empty());
        assert_eq!(continuity.reasm.next_needed(), start);

        for (index, block) in piece.chunks(chunk_length).enumerate() {
            continuity
                .reasm
                .add_block(start, (index * chunk_length) as u64, block)
                .unwrap();
        }
        assert_eq!(continuity.reasm.take_ready(), payload);
        assert_eq!(continuity.reasm.next_needed(), start + 1);
    }

    #[tokio::test]
    async fn rejected_signed_piece_retries_through_full_peer_pool() {
        let auth = ace_wire::live_auth::LiveSourceAuth::generate();
        let payload: Vec<u8> = (0..4)
            .flat_map(|cc| {
                let mut packet = vec![0x55; 188];
                packet[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10 | cc]);
                packet
            })
            .collect();
        let mut authentic = payload.clone();
        authentic.extend(auth.sign(&payload));
        let mut corrupt = authentic.clone();
        corrupt[0] ^= 1;
        let info = StreamInfo {
            infohash: [0; 20],
            piece_length: authentic.len() as u64,
            chunk_length: authentic.len() as u64 / 2,
            trackers: vec![],
            metadata: StreamMetadata::default(),
            sig_len: auth.signature_len(),
            source_pubkey: auth.pubkey_der(),
        };
        let policy = LiveRecoveryConfig {
            max_active_upstreams: 1,
            max_piece_advance: 1,
            request_timeout_ms: 5_000,
            request_check_interval_ms: 10,
            ..default_live_recovery()
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = match listener.local_addr().unwrap() {
            std::net::SocketAddr::V4(addr) => addr,
            _ => unreachable!(),
        };
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (retry_tx, retry_rx) = tokio::sync::oneshot::channel();
        let (allow_tx, allow_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
        let chunk_length = info.chunk_length as usize;
        let server = tokio::spawn(async move {
            let mut retry_tx = Some(retry_tx);
            let mut allow_rx = Some(allow_rx);
            let mut finish_rx = Some(finish_rx);
            let mut session = PeerSession::new(server);
            session.send(&PeerMessage::Unchoke).await.unwrap();
            for attempt in 0..2 {
                let mut chunks = Vec::new();
                while chunks.len() < 2 {
                    let msg = session.read_message().await.unwrap();
                    if let PeerMessage::Unknown { id: 6, payload } = msg {
                        assert_eq!(u32::from_be_bytes(payload[4..8].try_into().unwrap()), 7);
                        chunks.push(u16::from_be_bytes(payload[8..10].try_into().unwrap()));
                    }
                }
                assert_eq!(chunks, vec![0, 1]);
                if attempt == 0 {
                    for chunk in [0, 0, 1] {
                        let begin = chunk as usize * chunk_length;
                        session
                            .send(&build_piece(
                                0,
                                7,
                                chunk,
                                [0; 8],
                                &corrupt[begin..begin + chunk_length],
                            ))
                            .await
                            .unwrap();
                    }
                } else {
                    retry_tx.take().unwrap().send(()).unwrap();
                    allow_rx.take().unwrap().await.unwrap();
                    // Deliver the previously rejected last chunk first. Stale chunk counts
                    // must not free the slot and start a third request before completion.
                    session
                        .send(&build_piece(0, 7, 1, [0; 8], &authentic[chunk_length..]))
                        .await
                        .unwrap();
                    assert!(
                        tokio::time::timeout(Duration::from_millis(100), session.read_message())
                            .await
                            .is_err(),
                        "partial retry must stay in flight"
                    );
                    session
                        .send(&build_piece(0, 7, 0, [0; 8], &authentic[..chunk_length]))
                        .await
                        .unwrap();
                    // Late copies must not emit the authenticated piece twice.
                    for chunk in 0..2 {
                        let begin = chunk as usize * chunk_length;
                        session
                            .send(&build_piece(
                                0,
                                7,
                                chunk,
                                [0; 8],
                                &authentic[begin..begin + chunk_length],
                            ))
                            .await
                            .unwrap();
                    }
                    finish_rx.take().unwrap().await.unwrap();
                }
            }
        });
        let store = Arc::new(tokio::sync::Mutex::new(PieceStore::new(
            info.piece_length,
            info.chunk_length,
            4096,
        )));
        let seed = SeedConfig {
            metrics: None,
            registry: SeedRegistry::new(),
            store_bytes: 4096,
            store_retention: None,
            enabled: false,
            prefetch_pieces: 0,
            live_recovery: policy,
            cache_type: CacheType::Memory,
            cache_dir: PathBuf::new(),
            warm_peers: WarmPeerCache::memory(),
        };
        let (tx, mut rx) = mpsc::channel(4);
        let pool = tokio::spawn(async move {
            let mut continuity = None;
            follow_peer_pool(
                vec![ConnectedUpstream {
                    session: PeerSession::new(client),
                    addr,
                    window: live_pos(7, 7),
                    yourip: None,
                }],
                &info,
                &Identity::generate(),
                2,
                &tx,
                &Arc::new(AtomicU64::new(0)),
                &Arc::new(AtomicU64::new(0)),
                &Arc::new(AtomicU32::new(0)),
                &seed,
                &store,
                &mut continuity,
                vec![],
                vec![],
                completed_discovery(|_| Box::pin(async { vec![] })),
                &mut SessionCandidates::default(),
                &Arc::new(AtomicU32::new(0)),
                None,
            )
            .await;
        });
        tokio::time::timeout(Duration::from_secs(2), retry_rx)
            .await
            .expect("pool must re-request the rejected piece without waiting for timeout")
            .unwrap();
        assert!(rx.try_recv().is_err(), "corrupt piece must emit nothing");
        allow_tx.send(()).unwrap();
        let output = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        // The existing TS resynchronizer retains one packet for its next-sync check.
        assert_eq!(output.bytes.as_ref(), &payload[..564]);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), rx.recv())
                .await
                .is_err(),
            "late duplicate blocks must not emit another piece"
        );
        finish_tx.send(()).unwrap();
        server.await.unwrap();
        pool.await.unwrap();
    }

    #[tokio::test]
    async fn content_id_resolution_records_the_descriptor_under_its_infohash() {
        let (transport, _) = test_support::synthetic_live_transport(524_288);
        let verified = ace_swarm::resolve::stream_info_from_transport(&transport).unwrap();
        let p = test_provider();
        let ih = infohash_hex(&verified.infohash);
        assert!(p.verified_info_for_infohash(&ih).is_err());

        // A cached content-id resolution (no network) must also feed the infohash index.
        p.resolve_cache.put(PLACEHOLDER_ID, verified.clone());
        let via_cid = p
            .resolve_live_info(&format!("cid:{PLACEHOLDER_ID}"))
            .await
            .unwrap();
        assert_eq!(via_cid, verified);
        assert_eq!(p.resolve_live_info(&ih).await.unwrap(), verified);

        // The content id itself is not an infohash: the two namespaces stay separate (#165).
        assert!(matches!(
            p.verified_info_for_infohash(PLACEHOLDER_ID),
            Err(ProviderError::Unresolvable(_))
        ));
    }

    #[tokio::test]
    async fn own_broadcast_opens_by_infohash_with_its_minted_geometry() {
        let seed = SeedRegistry::new();
        let broadcasts = crate::broadcast::BroadcastRegistry::new();
        let (bc, _) = broadcasts
            .start_or_resume(
                "t164",
                "T164",
                &["udp://tracker.invalid:80".into()],
                &seed,
                1 << 20,
            )
            .await;
        let id = infohash_hex(&bc.infohash);

        // A provider that does not share the broadcast's registry still refuses it.
        assert!(matches!(
            test_provider().verified_info_for_infohash(&id),
            Err(ProviderError::Unresolvable(_))
        ));

        let p = test_provider().with_seed_registry(seed);
        assert!(p.check_openable(&id).is_ok());
        let info = p.resolve_live_info(&id).await.unwrap();
        assert_eq!(info.infohash, bc.infohash);
        assert_eq!(info.piece_length, crate::broadcast::PIECE_LENGTH);
        assert_eq!(info.source_pubkey, bc.auth.pubkey_der());
    }

    #[tokio::test]
    async fn disabled_bep9_policy_does_not_read_peer_cache() {
        let (transport, _) = test_support::synthetic_live_transport(524_288);
        let info = stream_info_from_transport(&transport).unwrap();
        let p = test_provider();
        p.peer_resolve_cache.put(PLACEHOLDER_ID, info);
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            p.resolve_content_id_with_catalog(
                PLACEHOLDER_ID,
                std::future::ready(Err(ResolveError::Catalog("synthetic catalog failure"))),
            ),
        )
        .await;
        let error = result
            .expect("disabled policy must fail before peer I/O")
            .unwrap_err();
        assert!(error.to_string().contains("fallback is disabled"));
    }

    #[tokio::test]
    async fn peer_resolved_content_id_does_not_make_its_infohash_openable() {
        let (transport, _) = test_support::synthetic_live_transport(524_288);
        let verified = ace_swarm::resolve::stream_info_from_transport(&transport).unwrap();
        let p = test_provider().with_bep9_content_id_fallback(true);
        p.peer_resolve_cache.put(PLACEHOLDER_ID, verified.clone());

        // The caller who chose this content id gets its descriptor...
        let via_cid = p
            .resolve_live_info(&format!("cid:{PLACEHOLDER_ID}"))
            .await
            .unwrap();
        assert_eq!(via_cid, verified);

        // ...but BEP-9 binds the blob only to that content id, so the infohash stays closed.
        let ih = infohash_hex(&verified.infohash);
        assert!(matches!(
            p.verified_info_for_infohash(&ih),
            Err(ProviderError::Unresolvable(_))
        ));
        assert!(matches!(
            p.resolve_infohash_with_catalog(
                &ih,
                std::future::ready(Err(ResolveError::Catalog("not found")))
            )
            .await,
            Err(ProviderError::Unresolvable(_))
        ));
    }

    #[tokio::test]
    async fn own_broadcast_wins_over_a_conflicting_index_entry() {
        use ace_wire::bencode::Bencode;
        let seed = SeedRegistry::new();
        let broadcasts = crate::broadcast::BroadcastRegistry::new();
        let (bc, _) = broadcasts
            .start_or_resume(
                "t164b",
                "T164B",
                &["udp://own.invalid:80".into()],
                &seed,
                1 << 20,
            )
            .await;
        let own = seed.broadcast_transport_for_infohash(&bc.infohash).unwrap();

        // Same infohash, different (unbound) trackers.
        let decoded = ace_wire::transport::decode_transport(&own).unwrap();
        let Bencode::Dict(mut dict) = decoded.raw else {
            panic!("transport is not a dict");
        };
        dict.insert(
            b"trackers".to_vec(),
            Bencode::List(vec![Bencode::Bytes(b"udp://evil.invalid:80".to_vec())]),
        );
        let patched = ace_wire::transport::encode_transport(&Bencode::Dict(dict));
        assert_eq!(
            ace_wire::infohash::infohash_of_transport(&patched),
            bc.infohash
        );

        let p = test_provider().with_seed_registry(seed);
        let evil = ace_swarm::resolve::stream_info_from_transport(&patched).unwrap();
        assert_eq!(evil.trackers, vec!["udp://evil.invalid:80".to_string()]);
        p.remember_live_descriptor(&evil);

        let info = p
            .resolve_live_info(&infohash_hex(&bc.infohash))
            .await
            .unwrap();
        assert_eq!(info.trackers, vec!["udp://own.invalid:80".to_string()]);
    }

    #[test]
    fn check_openable_leaves_non_infohash_ids_to_open() {
        let p = test_provider();
        assert!(p.check_openable(&format!("cid:{PLACEHOLDER_ID}")).is_ok());
        let turl = crate::transport_url::encode_transport_url("https://example.invalid/x.acelive")
            .unwrap();
        assert!(p.check_openable(&turl).is_ok());
    }

    #[tokio::test]
    async fn content_id_with_bad_hex_is_rejected_without_network() {
        let p = AceProvider::new(Arc::new(Identity::generate()), 6878);
        // `cid:` dispatch reaches resolution but the hex is invalid → immediate Backend error,
        // no discovery/connect attempted.
        assert!(matches!(
            p.open("cid:nothex").await,
            Err(ProviderError::Backend(_))
        ));
    }

    #[tokio::test]
    async fn open_rejects_transport_url_with_blocked_host() {
        let p = AceProvider::new(Arc::new(Identity::generate()), 6878);
        // A valid transport-url id whose host is loopback is SSRF-blocked, so open() fails closed
        // via the decode+fetch branch (proven by the "transport url" message, not the generic
        // unrecognized-id fallthrough).
        let id = crate::transport_url::encode_transport_url("http://127.0.0.1:1/x").unwrap();
        let err = p.open(&id).await.err().expect("blocked host must error");
        match err {
            ProviderError::Backend(msg) => assert!(msg.contains("transport url"), "{msg}"),
            other => panic!("expected Backend error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn open_rejects_transport_url_id_with_bad_scheme() {
        // Defense in depth: even a hand-crafted transport-url id encoding a non-http scheme is
        // rejected at the fetch layer, not just at the input surface.
        use base64ct::{Base64UrlUnpadded, Encoding};
        let p = AceProvider::new(Arc::new(Identity::generate()), 6878);
        let id = format!(
            "turl-{}",
            Base64UrlUnpadded::encode_string(b"file:///etc/passwd")
        );
        let err = p.open(&id).await.err().expect("bad scheme must error");
        match err {
            ProviderError::Backend(msg) => assert!(msg.contains("transport url"), "{msg}"),
            other => panic!("expected Backend error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn peer_worker_sends_chunk_requests_from_commands() {
        use tokio::io::AsyncReadExt;

        let (client, mut server) = tokio::io::duplex(4096);
        let session = PeerSession::new(client).with_timeout(Duration::from_millis(250));
        let addr: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();
        let (command_tx, command_rx) = mpsc::channel(4);
        let (event_tx, _event_rx) = mpsc::channel(4);
        let worker = tokio::spawn(peer_worker(
            1,
            addr,
            session,
            command_rx,
            event_tx,
            Arc::new(AtomicU64::new(0)),
        ));

        command_tx
            .send(PeerCommand::RequestPiece {
                piece: 42,
                chunks_per_piece: 2,
            })
            .await
            .unwrap();

        let expected = [chunk_request(42, 0).encode(), chunk_request(42, 1).encode()].concat();
        let mut got = vec![0u8; expected.len()];
        server.read_exact(&mut got).await.unwrap();
        assert_eq!(got, expected);

        drop(command_tx);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn peer_worker_emits_messages_from_session() {
        use tokio::io::AsyncWriteExt;

        let (client, mut server) = tokio::io::duplex(4096);
        let session = PeerSession::new(client).with_timeout(Duration::from_millis(250));
        let addr: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();
        let (command_tx, command_rx) = mpsc::channel(4);
        let (event_tx, mut event_rx) = mpsc::channel(4);
        let worker = tokio::spawn(peer_worker(
            7,
            addr,
            session,
            command_rx,
            event_tx,
            Arc::new(AtomicU64::new(0)),
        ));

        server
            .write_all(&PeerMessage::Unchoke.encode())
            .await
            .unwrap();
        match event_rx.recv().await.unwrap() {
            PeerEvent::Message {
                peer_id,
                addr: event_addr,
                msg,
            } => {
                assert_eq!(peer_id, 7);
                assert_eq!(event_addr, addr);
                assert_eq!(msg, PeerMessage::Unchoke);
            }
            other => panic!("unexpected event: {other:?}"),
        }

        command_tx.send(PeerCommand::Stop).await.unwrap();
        worker.await.unwrap();
    }

    // Note: the "no peers -> Backend error" path is intentionally not unit-tested, since
    // discovery now always consults the live DHT (network). It's exercised by the live
    // capture path instead.

    #[test]
    fn first_request_after_unchoke_covers_the_prefetch_window() {
        let mut scheduler = Scheduler::new(default_live_recovery().max_piece_advance as usize);
        let mut active = single_active_peer(100, 109);
        let pieces = schedule_piece_requests(&mut scheduler, &mut active, 100, 109);
        assert_eq!(pieces, (100..=109).collect::<Vec<_>>());
    }

    #[test]
    fn caught_up_requests_nothing() {
        // The reassembler cursor is already past the head -> nothing new yet (the loop
        // keeps waiting for window updates to advance it).
        let mut scheduler = Scheduler::new(default_live_recovery().max_piece_advance as usize);
        let mut active = single_active_peer(100, 109);
        assert!(schedule_piece_requests(&mut scheduler, &mut active, 110, 109).is_empty());
    }

    #[test]
    fn window_update_drives_a_forward_request() {
        // The head advanced by one piece -> request exactly that new piece. This is the
        // step the pre-fix loop never took (it only advanced on a `Have` that never came).
        let mut one = Scheduler::new(default_live_recovery().max_piece_advance as usize);
        let mut one_active = single_active_peer(100, 110);
        assert_eq!(
            schedule_piece_requests(&mut one, &mut one_active, 110, 110),
            vec![110]
        );

        let mut many = Scheduler::new(default_live_recovery().max_piece_advance as usize);
        let mut many_active = single_active_peer(100, 113);
        assert_eq!(
            schedule_piece_requests(&mut many, &mut many_active, 110, 113),
            vec![110, 111, 112, 113]
        );
    }

    #[test]
    fn forward_request_is_bounded_against_a_bogus_head() {
        // A garbled window update claiming a wildly-advanced head can't burst-request the
        // whole range; it's clamped, and subsequent updates catch up.
        let mut scheduler = Scheduler::new(default_live_recovery().max_piece_advance as usize);
        let mut active = single_active_peer(0, 10_000_000);
        let pieces = schedule_piece_requests(&mut scheduler, &mut active, 101, 10_000_000);
        assert_eq!(pieces.first(), Some(&101));
        assert_eq!(
            pieces.len() as u64,
            default_live_recovery().max_piece_advance
        );
    }

    #[test]
    fn continuity_uses_configured_live_recovery_bounds() {
        let info = StreamInfo {
            infohash: [0x01; 20],
            piece_length: 1_048_576,
            chunk_length: 16_384,
            trackers: vec![],
            metadata: StreamMetadata::default(),
            sig_len: 96,
            source_pubkey: vec![],
        };
        let policy = LiveRecoveryConfig {
            max_piece_advance: 7,
            max_reasm_pieces_ahead: 9,
            ..LiveRecoveryConfig::default()
        };
        let (mut c, start) = Continuity::fresh(&info, 10, 20, 2, policy);

        assert_eq!(start, 18);
        c.register_active_peer(
            1,
            "127.0.0.1:1".parse().unwrap(),
            LivePosition {
                min_piece: 10,
                max_piece: 100,
                position: -1,
                distance_from_source: 1,
            },
        );
        c.head = 100;
        c.set_peer_unchoked(1, true);

        let pieces = schedule_piece_assignments(
            &mut c.scheduler,
            &mut c.active_peers,
            c.reasm.next_needed(),
            c.head,
        );

        assert_eq!(pieces.len(), 7);
    }

    #[test]
    fn stale_upstream_budget_expires_after_no_forward_progress() {
        let now = std::time::Instant::now();
        let timeout = default_live_recovery().stale_upstream_timeout();
        assert_eq!(
            stale_upstream_budget(now, now + Duration::from_secs(3), timeout),
            Some(timeout - Duration::from_secs(3))
        );
        assert_eq!(stale_upstream_budget(now, now + timeout, timeout), None);
        assert_eq!(
            stale_upstream_budget(now, now + timeout + Duration::from_millis(1), timeout),
            None
        );
    }

    fn win(min: i64, max: i64) -> LivePosition {
        LivePosition {
            min_piece: min,
            max_piece: max,
            position: -1,
            distance_from_source: 1,
        }
    }

    fn test_addr() -> SocketAddrV4 {
        use std::net::Ipv4Addr;
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8621)
    }

    #[test]
    fn timed_out_requests_returns_only_aged_pieces_and_prunes_passed_ones() {
        let (mut c, _) = Continuity::fresh(
            &info(),
            5,
            15,
            PREFETCH_PIECES,
            LiveRecoveryConfig::default(),
        ); // next_needed = 7
        let base = Instant::now();
        c.requested_at.insert(4, base); // below cursor -> pruned, not returned
        c.requested_at.insert(8, base); // aged
        let request_timeout = default_live_recovery().request_timeout();
        c.requested_at.insert(9, base + request_timeout); // fresh
        let now = base + request_timeout;
        let mut out = c.timed_out_requests(now);
        out.sort_unstable();
        assert_eq!(out, vec![8]);
        assert!(!c.requested_at.contains_key(&4), "passed piece pruned");
    }

    fn test_pool_peer(addr: SocketAddrV4) -> (PeerRuntime, mpsc::Receiver<PeerCommand>) {
        let (commands, receiver) = mpsc::channel(8);
        (
            PeerRuntime {
                addr,
                min_piece: 7,
                max_piece: 8,
                unchoked_peer: false,
                produced_output: false,
                seen_ids: HashSet::new(),
                commands,
                worker: tokio::spawn(std::future::pending()),
            },
            receiver,
        )
    }

    #[tokio::test]
    async fn timed_out_piece_retries_when_all_peer_slots_are_full() {
        let policy = LiveRecoveryConfig {
            max_piece_advance: 1,
            ..default_live_recovery()
        };
        let (mut c, _) = Continuity::fresh(&info(), 7, 7, 0, policy);
        let addr = "127.0.0.1:1".parse().unwrap();
        let (peer, mut commands) = test_pool_peer(addr);
        let mut peers = BTreeMap::from([(1, peer)]);
        c.register_active_peer(1, addr, live_pos(7, 7));
        c.set_peer_unchoked(1, true);
        advance_pool_requests(&mut peers, &mut c, 32).await;
        assert!(matches!(
            commands.try_recv(),
            Ok(PeerCommand::RequestPiece { piece: 7, .. })
        ));
        let now = Instant::now() + policy.request_timeout();
        retransmit_stalled_requests(&mut peers, &mut c, 32, now).await;
        let retry = commands.try_recv();
        shutdown_peer_runtimes(&mut peers);
        assert!(
            matches!(retry, Ok(PeerCommand::RequestPiece { piece: 7, .. })),
            "timeout must queue a retry even when every peer was at capacity"
        );
    }

    #[tokio::test]
    async fn timed_out_piece_still_prefers_a_peer_with_spare_capacity() {
        let policy = LiveRecoveryConfig {
            max_piece_advance: 1,
            ..default_live_recovery()
        };
        let (mut c, _) = Continuity::fresh(&info(), 7, 7, 0, policy);
        let addr = "127.0.0.1:1".parse().unwrap();
        let (peer, mut original) = test_pool_peer(addr);
        let mut peers = BTreeMap::from([(1, peer)]);
        c.register_active_peer(1, addr, live_pos(7, 7));
        c.set_peer_unchoked(1, true);
        advance_pool_requests(&mut peers, &mut c, 32).await;
        original.try_recv().unwrap();
        let (peer, mut faster) = test_pool_peer(addr);
        peers.insert(2, peer);
        c.register_active_peer(2, addr, live_pos(7, 7));
        c.set_peer_unchoked(2, true);
        retransmit_stalled_requests(
            &mut peers,
            &mut c,
            32,
            Instant::now() + policy.request_timeout(),
        )
        .await;
        let retry = faster.try_recv();
        let unexpected = original.try_recv();
        shutdown_peer_runtimes(&mut peers);
        assert!(matches!(
            retry,
            Ok(PeerCommand::RequestPiece { piece: 7, .. })
        ));
        assert!(
            unexpected.is_err(),
            "normal retry should still prefer spare peer capacity"
        );
        assert_eq!(
            c.active_peers.in_flight_count(1),
            1,
            "original late response stays tracked"
        );
    }

    #[tokio::test]
    async fn rejected_piece_releases_duplicate_assignments_without_dropping_other_requests() {
        for rejecting_peer_choked in [true, false] {
            let policy = LiveRecoveryConfig {
                max_piece_advance: 2,
                ..default_live_recovery()
            };
            let (mut c, _) = Continuity::fresh(&info(), 7, 8, 1, policy);
            let addr = "127.0.0.1:1".parse().unwrap();
            let (peer, mut original) = test_pool_peer(addr);
            let mut peers = BTreeMap::from([(1, peer)]);
            c.register_active_peer(1, addr, live_pos(7, 8));
            c.set_peer_unchoked(1, true);
            advance_pool_requests(&mut peers, &mut c, 32).await;
            original.try_recv().unwrap(); // piece 7
            original.try_recv().unwrap(); // unrelated piece 8
            let (peer, mut rejecting) = test_pool_peer(addr);
            peers.insert(2, peer);
            c.register_active_peer(2, addr, live_pos(7, 7));
            c.set_peer_unchoked(2, true);
            let now = Instant::now() + policy.request_timeout();
            c.requested_at.insert(8, now);
            retransmit_stalled_requests(&mut peers, &mut c, 32, now).await;
            assert!(matches!(
                rejecting.try_recv(),
                Ok(PeerCommand::RequestPiece { piece: 7, .. })
            ));
            assert_eq!(c.active_peers.in_flight_count(1), 2);
            if rejecting_peer_choked {
                c.set_peer_unchoked(2, false);
            } else {
                c.update_peer_window(2, 8, 9);
            }
            c.received_chunks.insert(7, HashSet::from([0]));
            c.received_chunks.insert(8, HashSet::from([0]));
            c.release_rejected_piece(7);
            assert!(!c.received_chunks.contains_key(&7));
            assert!(!c.requested_at.contains_key(&7));
            assert_eq!(c.received_chunks.get(&8).unwrap().len(), 1);
            assert_eq!(c.requested_at.get(&8), Some(&now));
            advance_pool_requests(&mut peers, &mut c, 32).await;
            let retry = original.try_recv();
            let unexpected = rejecting.try_recv();
            shutdown_peer_runtimes(&mut peers);
            assert!(matches!(
                retry,
                Ok(PeerCommand::RequestPiece { piece: 7, .. })
            ));
            assert!(unexpected.is_err());
            assert_eq!(
                c.active_peers.in_flight_count(1),
                2,
                "piece 8 remains outstanding"
            );
            assert_eq!(
                c.active_peers.in_flight_count(2),
                0,
                "all rejected copies must release"
            );
        }
    }

    #[test]
    fn rejected_malformed_block_resets_partial_bytes_with_chunk_counts() {
        let mut stream = info();
        stream.piece_length = 8;
        stream.chunk_length = 4;
        stream.sig_len = 0;
        let (mut c, _) = Continuity::fresh(&stream, 7, 7, 0, default_live_recovery());
        c.reasm.add_block(7, 0, &[1, 2, 3, 4]).unwrap();
        assert!(!c.note_chunk(7, 0, 2));
        assert!(c.reasm.add_block(7, 8, &[9]).is_err());
        c.release_rejected_piece(7);
        assert_eq!(c.reasm.next_needed(), 7);
        c.reasm.add_block(7, 4, &[5, 6, 7, 8]).unwrap();
        assert!(!c.note_chunk(7, 1, 2));
        assert!(
            c.reasm.take_ready().is_empty(),
            "retry must not use discarded partial bytes"
        );
        c.reasm.add_block(7, 0, &[1, 2, 3, 4]).unwrap();
        assert!(c.note_chunk(7, 0, 2));
        assert_eq!(c.reasm.take_ready(), vec![1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[tokio::test]
    async fn timed_out_piece_without_eligible_peer_retries_on_unchoke_and_keeps_partial_data() {
        let mut stream = info();
        stream.piece_length = 8;
        stream.chunk_length = 4;
        stream.sig_len = 0;
        let policy = LiveRecoveryConfig {
            max_piece_advance: 1,
            ..default_live_recovery()
        };
        let (mut c, _) = Continuity::fresh(&stream, 7, 7, 0, policy);
        let addr = "127.0.0.1:1".parse().unwrap();
        let (peer, mut commands) = test_pool_peer(addr);
        let mut peers = BTreeMap::from([(1, peer)]);
        c.register_active_peer(1, addr, live_pos(7, 7));
        c.set_peer_unchoked(1, true);
        advance_pool_requests(&mut peers, &mut c, 2).await;
        commands.try_recv().unwrap();
        c.reasm.add_block(7, 0, &[1, 2, 3, 4]).unwrap();
        assert!(!c.note_chunk(7, 0, 2));
        c.set_peer_unchoked(1, false);
        retransmit_stalled_requests(
            &mut peers,
            &mut c,
            2,
            Instant::now() + policy.request_timeout(),
        )
        .await;
        assert!(
            commands.try_recv().is_err(),
            "choked peer cannot receive retry requests"
        );
        assert_eq!(c.active_peers.in_flight_count(1), 0);
        assert!(!c.requested_at.contains_key(&7));
        assert_eq!(c.received_chunks.get(&7).unwrap().len(), 1);
        c.set_peer_unchoked(1, true);
        advance_pool_requests(&mut peers, &mut c, 2).await;
        let retry = commands.try_recv();
        shutdown_peer_runtimes(&mut peers);
        assert!(matches!(
            retry,
            Ok(PeerCommand::RequestPiece {
                piece: 7,
                chunks_per_piece: 2
            })
        ));
        c.reasm.add_block(7, 4, &[5, 6, 7, 8]).unwrap();
        assert!(c.note_chunk(7, 1, 2));
        assert_eq!(c.reasm.take_ready(), vec![1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn skip_evicted_gap_jumps_to_lowest_covered_when_cursor_is_stranded() {
        let (mut c, _) = Continuity::fresh(
            &info(),
            100,
            110,
            PREFETCH_PIECES,
            LiveRecoveryConfig::default(),
        ); // next_needed = 102
        c.register_active_peer(1, test_addr(), win(105, 120)); // evicted 102..104
        c.set_peer_unchoked(1, true);
        // Not stuck long enough yet.
        assert_eq!(c.skip_evicted_gap(c.next_needed_since), None);
        // Stuck past the timeout with no peer covering 102 -> skip to 105.
        let now = c.next_needed_since + default_live_recovery().request_timeout();
        assert_eq!(c.skip_evicted_gap(now), Some(105));
        assert_eq!(c.reasm.next_needed(), 105);
    }

    #[test]
    fn skip_far_behind_live_resyncs_forward_even_when_peers_cover_the_cursor() {
        // Reproduces the production wedge: the cursor is stuck on a piece a peer's window still
        // covers (so `skip_evicted_gap` refuses to skip), while the live edge has raced far ahead.
        let lr = LiveRecoveryConfig {
            max_reasm_pieces_ahead: 16,
            ..LiveRecoveryConfig::default()
        };
        let (mut c, _) = Continuity::fresh(&info(), 100, 110, PREFETCH_PIECES, lr); // next=102, head=110
        c.register_active_peer(1, test_addr(), win(100, 10_000)); // still "covers" 102
        c.set_peer_unchoked(1, true);
        // Live edge is now well beyond next + max_reasm_pieces_ahead (102 + 16).
        c.head = 5_000;
        // `skip_evicted_gap` would never fire here (a peer covers the cursor).
        assert_eq!(
            c.skip_evicted_gap(c.next_needed_since + Duration::from_secs(60)),
            None
        );
        // The live-edge re-sync jumps forward to head - prefetch regardless of coverage.
        let target = c.skip_far_behind_live(c.next_needed_since);
        assert_eq!(target, Some(5_000 - PREFETCH_PIECES));
        assert_eq!(c.reasm.next_needed(), 5_000 - PREFETCH_PIECES);
    }

    #[test]
    fn skip_far_behind_live_stays_put_within_the_lookahead_window() {
        let (mut c, _) = Continuity::fresh(
            &info(),
            100,
            110,
            PREFETCH_PIECES,
            LiveRecoveryConfig::default(),
        ); // next=102, head=110, lag=8 << max_reasm_pieces_ahead
        assert_eq!(c.skip_far_behind_live(c.next_needed_since), None);
        assert_eq!(c.reasm.next_needed(), 102);
    }

    #[test]
    fn skip_evicted_gap_does_not_skip_while_a_peer_still_covers_the_cursor() {
        let (mut c, _) = Continuity::fresh(
            &info(),
            100,
            110,
            PREFETCH_PIECES,
            LiveRecoveryConfig::default(),
        ); // next_needed = 102
        c.register_active_peer(1, test_addr(), win(100, 120)); // still covers 102
        c.set_peer_unchoked(1, true);
        let now = c.next_needed_since + default_live_recovery().request_timeout() * 3;
        assert_eq!(c.skip_evicted_gap(now), None);
        assert_eq!(c.reasm.next_needed(), 102);
    }

    #[test]
    fn stale_deadline_refreshes_only_for_output_after_playback_starts() {
        assert!(
            should_refresh_stale_deadline(0, false, true),
            "startup can stay alive on handshake/window/chunk activity before first output"
        );
        assert!(
            should_refresh_stale_deadline(1024, true, false),
            "contiguous MPEG-TS output is real playback progress"
        );
        assert!(
            !should_refresh_stale_deadline(1024, false, true),
            "after playback starts, non-output activity must not mask a visible stall"
        );
    }

    #[test]
    fn acestream_have_payload_decodes_to_the_head_piece() {
        // The captured live HAVE (note 22): `[u32 stream=0][u32 piece]`. We read the piece
        // at bytes [4..8]; this exact payload was `head -> 5360483` in the operator's log.
        let payload = [0x00, 0x00, 0x00, 0x00, 0x00, 0x51, 0xcb, 0x63];
        let piece = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]) as u64;
        assert_eq!(piece, 5_360_483);
    }

    #[test]
    fn advance_head_only_when_window_moves_forward() {
        // A myinfo update past the head advances it; one at/behind the head is ignored.
        let ahead = b"d9:max_piecei210ee";
        let behind = b"d9:max_piecei150ee";
        assert_eq!(advance_head_from_window(ahead, 200), Some(210));
        assert_eq!(advance_head_from_window(behind, 200), None);
        assert_eq!(advance_head_from_window(b"not-a-window", 200), None);
    }

    fn info() -> StreamInfo {
        StreamInfo {
            infohash: [0; 20],
            piece_length: 4,
            chunk_length: 2,
            trackers: vec![],
            metadata: Default::default(),
            sig_len: 0,
            source_pubkey: vec![],
        }
    }

    #[tokio::test]
    async fn seeder_announce_never_fires_without_an_inbound_port() {
        // `None` (no inbound listener to back an advertisable endpoint) must never even attempt
        // a tracker announce, which would otherwise invite peers to dial a port nobody serves.
        let res = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            announce_seeder_periodically(info(), tokio::sync::watch::channel(None).1),
        )
        .await;
        assert!(res.is_err(), "must never resolve without an inbound port");
    }

    #[tokio::test]
    async fn closed_inbound_port_watch_keeps_self_announce_pending() {
        let (port_tx, port_rx) = tokio::sync::watch::channel(None);
        drop(port_tx);

        let res = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            announce_infohash_periodically_dynamic(vec![], [0; 20], port_rx),
        )
        .await;

        assert!(
            res.is_err(),
            "a closed disabled-port watch must not cancel the live follower"
        );
    }

    #[test]
    fn leech_self_announce_uses_the_peer_port_never_the_http_port() {
        // Regression for #21: a leeching provider configured the way `build_runtime` wires it
        // (peer_listen 8621, HTTP/bind 6878, inbound enabled) must advertise the PEER port on
        // its seeder self-announce — dialing the HTTP port would speak the wrong protocol.
        const PEER_PORT: u16 = 8621;
        const HTTP_PORT: u16 = 6878;
        let p = AceProvider::new(Arc::new(Identity::generate()), PEER_PORT)
            .with_inbound_announce_port(Some(PEER_PORT));
        assert_eq!(p.seeder_announce_port(), Some(PEER_PORT));
        assert_ne!(
            p.seeder_announce_port(),
            Some(HTTP_PORT),
            "must never advertise the HTTP API port"
        );
        // Discovery announces (which also register us as a peer) use the same resolved port.
        assert_eq!(p.discovery_announce_port(), PEER_PORT);

        // Without an inbound listener (the default, and `enable_inbound = false`), we advertise
        // no dial-able endpoint at all — matching the broadcast path's `inbound_peer_port`.
        let leech_only = AceProvider::new(Arc::new(Identity::generate()), PEER_PORT);
        assert_eq!(leech_only.seeder_announce_port(), None);
        assert_eq!(leech_only.discovery_announce_port(), 0);
    }

    #[tokio::test]
    async fn resolved_or_disabled_port_is_encoded_in_the_tracker_discovery_announce() {
        use tokio::net::UdpSocket;
        const LOCAL_PORT: u16 = 8621;
        const MAPPED_PORT: u16 = 48621;
        let (_port_tx, port_rx) = tokio::sync::watch::channel(Some(MAPPED_PORT));
        let provider = AceProvider::new(Arc::new(Identity::generate()), LOCAL_PORT)
            .with_inbound_announce_port_receiver(port_rx);
        assert_eq!(provider.discovery_announce_port(), MAPPED_PORT);

        async fn capture_wire_port(port: u16) -> u16 {
            let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr = match server.local_addr().unwrap() {
                std::net::SocketAddr::V4(addr) => addr,
                _ => unreachable!(),
            };
            let capture = tokio::spawn(async move {
                let mut buf = [0_u8; 2048];
                let (_, peer) = server.recv_from(&mut buf).await.unwrap();
                let txid = u32::from_be_bytes(buf[12..16].try_into().unwrap());
                let mut response = Vec::new();
                response.extend_from_slice(&0_u32.to_be_bytes());
                response.extend_from_slice(&txid.to_be_bytes());
                response.extend_from_slice(&42_u64.to_be_bytes());
                server.send_to(&response, peer).await.unwrap();
                let (len, peer) = server.recv_from(&mut buf).await.unwrap();
                assert_eq!(len, 98);
                let announced_port = u16::from_be_bytes(buf[96..98].try_into().unwrap());
                let txid = u32::from_be_bytes(buf[12..16].try_into().unwrap());
                let mut response = Vec::new();
                response.extend_from_slice(&1_u32.to_be_bytes());
                response.extend_from_slice(&txid.to_be_bytes());
                response.extend_from_slice(&1800_u32.to_be_bytes());
                response.extend_from_slice(&0_u32.to_be_bytes());
                response.extend_from_slice(&0_u32.to_be_bytes());
                server.send_to(&response, peer).await.unwrap();
                announced_port
            });
            ace_tracker::client::announce(
                addr,
                &[1_u8; 20],
                &[2_u8; 20],
                port,
                50,
                ace_tracker::codec::TransferState::default(),
                ace_tracker::codec::AnnounceEvent::Started,
            )
            .await
            .unwrap();
            capture.await.unwrap()
        }
        assert_eq!(
            capture_wire_port(provider.discovery_announce_port()).await,
            MAPPED_PORT
        );
        assert_ne!(MAPPED_PORT, LOCAL_PORT);
        let inbound_disabled = AceProvider::new(Arc::new(Identity::generate()), LOCAL_PORT);
        assert_eq!(
            capture_wire_port(inbound_disabled.discovery_announce_port()).await,
            0
        );
    }

    #[test]
    fn prefetch_start_honors_configured_depth() {
        // window 100..=200, prefetch 32 -> start 168 (not 200 - 8)
        assert_eq!(prefetch_start(100, 200, 32), 168);
        // clamps to min_piece when the window is shorter than prefetch
        assert_eq!(prefetch_start(195, 200, 32), 195);
    }

    #[test]
    fn fresh_starts_prefetch_pieces_behind_head_clamped_to_min() {
        let (c, start) = Continuity::fresh(
            &info(),
            100,
            200,
            PREFETCH_PIECES,
            LiveRecoveryConfig::default(),
        );
        assert_eq!(start, 200 - PREFETCH_PIECES);
        assert_eq!(c.head, 200);
        assert_eq!(c.scheduler.in_flight_count(), 0);
        assert_eq!(c.reasm.next_needed(), start);
    }

    #[test]
    fn fresh_clamps_start_to_min_piece_on_a_narrow_window() {
        // min_piece is closer to head than PREFETCH_PIECES allows -> clamp, don't request
        // an evicted piece.
        let (_c, start) = Continuity::fresh(
            &info(),
            198,
            200,
            PREFETCH_PIECES,
            LiveRecoveryConfig::default(),
        );
        assert_eq!(start, 198);
    }

    #[test]
    fn resume_continues_seamlessly_when_the_new_window_still_covers_our_position() {
        // We'd already emitted through piece 150; the new peer's window still covers the
        // next needed piece, so resume exactly there.
        let (mut c, start) = Continuity::fresh(
            &info(),
            100,
            149,
            PREFETCH_PIECES,
            LiveRecoveryConfig::default(),
        );
        for piece in start..=150 {
            c.reasm.add_block(piece, 0, &[1, 1]).unwrap();
            c.reasm.add_block(piece, 2, &[2, 2]).unwrap();
        }
        assert_eq!(c.reasm.take_ready().len(), ((150 - start + 1) * 4) as usize);
        assert_eq!(c.reasm.next_needed(), 151);
        let addr: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();
        let resume = c.resume(addr, 100, 160);
        assert_eq!(
            resume, 151,
            "continues right after what we've actually emitted"
        );
        assert_eq!(c.head, 160);
        assert_eq!(c.reasm.next_needed(), 151);
    }

    #[test]
    fn resume_retries_from_the_reassembler_cursor_not_the_old_request_frontier() {
        // In-flight scheduler entries only mean "asked the previous peer", not "received".
        // If that peer dies before delivery, the next peer must be asked for the first
        // still-missing piece rather than skipping past the old request frontier.
        let (mut c, start) = Continuity::fresh(
            &info(),
            100,
            149,
            PREFETCH_PIECES,
            LiveRecoveryConfig::default(),
        );
        c.active_peers = single_active_peer(100, 150);
        let assigned = c.active_peers.assign(&mut c.scheduler, start, 150);
        assert!(!assigned.is_empty());
        assert!(c.scheduler.in_flight_count() > 0);
        let addr: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();

        let resume = c.resume(addr, 100, 160);

        assert_eq!(resume, start);
        assert_eq!(c.reasm.next_needed(), start);
        assert_eq!(
            c.scheduler.in_flight_count(),
            0,
            "pieces requested from the dropped peer must be requeueable"
        );
    }

    #[test]
    fn peer_window_must_cover_the_next_needed_piece_on_reconnect() {
        let (c, start) = Continuity::fresh(
            &info(),
            100,
            149,
            PREFETCH_PIECES,
            LiveRecoveryConfig::default(),
        );
        let stale = LivePosition {
            min_piece: 100,
            max_piece: (start - 1) as i64,
            position: (start - 1) as i64,
            distance_from_source: 1,
        };
        let covering = LivePosition {
            min_piece: 100,
            max_piece: start as i64,
            position: start as i64,
            distance_from_source: 1,
        };

        assert!(!c.window_can_resume(&stale));
        assert!(c.window_can_resume(&covering));
    }

    #[test]
    fn live_edge_peer_stays_usable_while_the_next_needed_piece_does_not_exist_yet() {
        let (mut c, _start) = Continuity::fresh(
            &info(),
            100,
            149,
            PREFETCH_PIECES,
            LiveRecoveryConfig::default(),
        );
        // Caught up to the live edge: the piece we want next is one past the newest piece the
        // source has produced, so no peer in the swarm can advertise it yet.
        c.reasm.skip_to(150);
        assert_eq!(c.reasm.next_needed(), 150);
        assert_eq!(c.head, 149);

        let at_live_edge = LivePosition {
            min_piece: 100,
            max_piece: 149,
            position: 149,
            distance_from_source: 1,
        };
        let lagging = LivePosition {
            min_piece: 100,
            max_piece: 120,
            position: 120,
            distance_from_source: 1,
        };

        assert!(
            c.window_can_resume(&at_live_edge),
            "a peer current with the live edge must stay usable while we wait on the next piece"
        );
        assert!(
            !c.window_can_resume(&lagging),
            "a peer far behind the live edge still cannot serve the piece we need"
        );
    }

    #[test]
    fn active_peers_assign_missing_pieces_to_unchoked_peers_with_coverage() {
        let mut active = ActivePeers::new();
        let addr1: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();
        let addr2: SocketAddrV4 = "1.2.3.5:8621".parse().unwrap();
        active.insert(1, addr1, live_pos(100, 102));
        active.insert(2, addr2, live_pos(103, 105));
        active.set_unchoked(1, true);
        active.set_unchoked(2, true);
        let mut scheduler = Scheduler::new(2);

        let assigned = active.assign(&mut scheduler, 101, 105);

        assert_eq!(
            assigned,
            vec![
                PeerAssignment {
                    peer_id: 1,
                    piece: 101
                },
                PeerAssignment {
                    peer_id: 1,
                    piece: 102
                },
                PeerAssignment {
                    peer_id: 2,
                    piece: 103
                },
                PeerAssignment {
                    peer_id: 2,
                    piece: 104
                },
            ]
        );
        assert_eq!(active.in_flight_count(1), 2);
        assert_eq!(active.in_flight_count(2), 2);
    }

    #[test]
    fn active_peer_drop_returns_its_in_flight_pieces_for_requeue() {
        let mut active = ActivePeers::new();
        let addr1: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();
        let addr2: SocketAddrV4 = "1.2.3.5:8621".parse().unwrap();
        active.insert(1, addr1, live_pos(100, 105));
        active.insert(2, addr2, live_pos(100, 105));
        active.set_unchoked(1, true);
        active.set_unchoked(2, true);
        let mut scheduler = Scheduler::new(4);
        let assigned = active.assign(&mut scheduler, 100, 105);
        assert!(assigned.iter().any(|a| a.peer_id == 1));

        let dropped = active.remove(1);
        for piece in &dropped {
            scheduler.on_drop(*piece);
        }

        assert!(!dropped.is_empty());
        assert_eq!(active.in_flight_count(1), 0);
        let reassigned = active.assign(&mut scheduler, 100, 105);
        assert!(
            reassigned.iter().any(|a| dropped.contains(&a.piece)),
            "dropped peer pieces should be eligible for reassignment"
        );
        assert!(reassigned.iter().all(|a| a.peer_id == 2));
    }

    #[test]
    fn resume_skips_forward_over_an_unrecoverable_eviction_gap() {
        // We were disconnected long enough that the new peer's window no longer has the
        // piece we needed next (min_piece has advanced past it) — must skip, not stall.
        let (mut c, _start) = Continuity::fresh(
            &info(),
            100,
            149,
            PREFETCH_PIECES,
            LiveRecoveryConfig::default(),
        );
        let addr: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();
        let resume = c.resume(addr, 500, 600); // min_piece way ahead of 151
        assert_eq!(resume, 500);
        assert_eq!(
            c.reasm.next_needed(),
            500,
            "reassembler cursor jumped past the gap"
        );
        assert_eq!(c.scheduler.in_flight_count(), 0);
    }

    #[test]
    fn resume_gap_arms_output_keyframe_gate() {
        let (mut c, _) = Continuity::fresh(
            &info(),
            100,
            149,
            PREFETCH_PIECES,
            LiveRecoveryConfig::default(),
        );
        assert!(!c.output_gate_armed());

        c.resume("127.0.0.1:1".parse().unwrap(), 170, 200);

        assert!(c.output_gate_armed());
    }

    #[test]
    fn arming_output_gate_discards_buffered_pre_gap_ts_bytes() {
        const TS_PACKET: usize = 188;

        fn packet(fill: u8) -> Vec<u8> {
            let mut bytes = vec![fill; TS_PACKET];
            bytes[0] = 0x47;
            bytes
        }

        let (mut c, _) = Continuity::fresh(
            &info(),
            100,
            149,
            PREFETCH_PIECES,
            LiveRecoveryConfig::default(),
        );
        assert!(
            c.resync.push(&packet(0x11)).is_empty(),
            "one packet is buffered until lookahead confirms sync"
        );

        c.arm_output_gate();
        let post_gap = [packet(0x22), packet(0x33)].concat();
        let aligned = c.resync.push(&post_gap);

        assert_eq!(aligned.len(), TS_PACKET);
        assert_eq!(aligned[1], 0x22);
    }

    #[test]
    fn resync_boundary_loss_marks_one_discontinuity_and_rearms_output_gate() {
        const TS_PACKET: usize = 188;
        fn packet(fill: u8) -> Vec<u8> {
            let mut bytes = vec![fill; TS_PACKET];
            bytes[0] = 0x47;
            bytes
        }

        let (mut c, _) = Continuity::fresh(
            &info(),
            100,
            149,
            PREFETCH_PIECES,
            LiveRecoveryConfig::default(),
        );
        let initial = [packet(1), packet(2), packet(3)].concat();
        assert!(!c.resync_output(&initial).is_empty());
        assert!(!c.take_discontinuity());

        let boundary = [vec![0; 96], packet(4), packet(5), packet(6)].concat();
        let prefix = c.resync_output(&boundary);
        assert_eq!(prefix.len(), 1);
        assert!(!prefix[0].discontinuity);
        assert_eq!(prefix[0].bytes[1], 3);
        assert!(c.output_gate_armed());
        assert!(c.take_discontinuity());
        assert!(!c.take_discontinuity());
    }

    #[test]
    fn skip_evicted_gap_arms_output_keyframe_gate() {
        let (mut c, _) = Continuity::fresh(
            &info(),
            100,
            110,
            PREFETCH_PIECES,
            LiveRecoveryConfig::default(),
        );
        c.register_active_peer(1, test_addr(), win(105, 120));
        c.set_peer_unchoked(1, true);
        assert!(!c.output_gate_armed());

        let now = c.next_needed_since + default_live_recovery().request_timeout();
        assert_eq!(c.skip_evicted_gap(now), Some(105));

        assert!(c.output_gate_armed());
    }

    #[test]
    fn resume_head_never_regresses() {
        let (mut c, _start) = Continuity::fresh(
            &info(),
            100,
            200,
            PREFETCH_PIECES,
            LiveRecoveryConfig::default(),
        );
        let addr: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();
        c.resume(addr, 100, 150); // a new peer with a "smaller" (staler) window
        assert_eq!(c.head, 200, "head must not go backward");
    }

    fn addrs(ports: &[u16]) -> Vec<SocketAddrV4> {
        ports
            .iter()
            .map(|&p| SocketAddrV4::new([1, 2, 3, 4].into(), p))
            .collect()
    }

    fn live_pos(min_piece: i64, max_piece: i64) -> LivePosition {
        LivePosition {
            min_piece,
            max_piece,
            position: max_piece,
            distance_from_source: 1,
        }
    }

    fn single_active_peer(min_piece: i64, max_piece: i64) -> ActivePeers {
        let mut active = ActivePeers::new();
        let addr: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();
        active.insert(SINGLE_PEER_ID, addr, live_pos(min_piece, max_piece));
        active.set_unchoked(SINGLE_PEER_ID, true);
        active
    }

    #[test]
    fn pool_refill_candidates_skip_active_and_cooling_peers() {
        let peers = addrs(&[1, 2, 3, 4]);
        let active: HashSet<SocketAddrV4> = addrs(&[2]).into_iter().collect();
        let mut candidates = SessionCandidates::default();
        for addr in peers {
            candidates.learn(addr, CandidateKind::Discovered);
        }
        candidates.failed(addrs(&[3])[0], Instant::now());
        assert_eq!(
            pool_refill_candidates(&candidates.eligible_discovered(Instant::now()), &active),
            addrs(&[1, 4])
        );
    }

    #[test]
    fn peer_connect_stats_reports_failure_classes() {
        let mut stats = PeerConnectStats::default();
        stats.record_failure(PeerConnectFailure {
            addr: addrs(&[1])[0],
            stage: PeerConnectStage::Tcp,
        });
        stats.record_failure(PeerConnectFailure {
            addr: addrs(&[2])[0],
            stage: PeerConnectStage::Handshake,
        });
        stats.record_failure(PeerConnectFailure {
            addr: addrs(&[3])[0],
            stage: PeerConnectStage::Window,
        });
        stats.record_failure(PeerConnectFailure {
            addr: addrs(&[4])[0],
            stage: PeerConnectStage::Window,
        });

        assert_eq!(stats.summary(), "attempted=4 tcp=1 handshake=1 window=2");
    }

    #[test]
    fn newly_discovered_refill_candidates_are_deduped_against_known_peers() {
        let mut candidates = SessionCandidates::default();
        for addr in addrs(&[1, 2]) {
            candidates.learn(addr, CandidateKind::Discovered);
        }
        let before = candidates.all();
        for addr in addrs(&[2, 3, 1, 4, 3]) {
            candidates.learn(addr, CandidateKind::Discovered);
        }
        let after = candidates.all();
        assert_eq!(
            after
                .iter()
                .copied()
                .filter(|addr| !before.contains(addr))
                .collect::<Vec<_>>(),
            addrs(&[3, 4])
        );
        assert_eq!(after.len(), 4);
    }

    #[test]
    fn background_discovery_options_leave_stale_timer_margin() {
        let opts = background_discovery_options();
        assert!(opts.peer_target > 8);
        assert!(opts.dht_budget < default_live_recovery().stale_upstream_timeout());
    }

    #[test]
    fn upstream_selection_prefers_the_freshest_live_head() {
        let stale = LivePosition {
            min_piece: 100,
            max_piece: 120,
            position: 120,
            distance_from_source: 1,
        };
        let fresh = LivePosition {
            min_piece: 105,
            max_piece: 124,
            position: 124,
            distance_from_source: 3,
        };

        assert!(
            prefer_window(&fresh, &stale),
            "a newer advertised head should beat a lower distance value"
        );
        assert!(!prefer_window(&stale, &fresh));
    }

    #[test]
    fn upstream_selection_uses_distance_as_a_tiebreaker() {
        let near = LivePosition {
            min_piece: 100,
            max_piece: 120,
            position: 120,
            distance_from_source: 1,
        };
        let far = LivePosition {
            min_piece: 90,
            max_piece: 120,
            position: 120,
            distance_from_source: 4,
        };

        assert!(prefer_window(&near, &far));
        assert!(!prefer_window(&far, &near));
    }
}

#[cfg(test)]
mod stats_tests;
