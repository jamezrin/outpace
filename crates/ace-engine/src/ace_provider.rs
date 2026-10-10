//! The `"ace"` provider: resolves an identifier to a [`StreamInfo`], discovers peers via
//! trackers, and follows the live edge from a peer, emitting contiguous MPEG-TS. Built on
//! the cracked live protocol (see `docs/protocol/notes/19.md`).
//!
//! LIVE-GATED: the peer I/O path requires the real Acestream swarm and is verified in the
//! operator's environment (it cannot run in CI/sandbox). Content-id → transport-file
//! resolution first uses the official signed catalog path, with BEP-9 `ut_metadata` as a
//! fallback (see [`ace_swarm::resolve`]). A bare infohash opens only from a transport descriptor
//! this process has already verified, and otherwise fails closed (issue #164).

#[cfg(test)]
mod discovery_tests;
#[cfg(test)]
mod live_start_tests;
mod reconnect;
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
    catalog_transport_bytes, hex20, infohash_hex, resolve_via_catalog, resolve_via_peer,
    stream_info_from_transport, stream_info_from_transport_url, transport_bytes_from_url,
    transport_bytes_via_peer, vod_info_from_transport, InfohashIndex, ResolveCache, ResolveError,
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
    /// Content ids resolved over BEP-9 peers. BEP-9 binds a blob only to the content id the
    /// caller chose, and the infohash does not bind trackers/categories, so these results must
    /// never make an infohash openable (#164): they are never put into `infohash_index`.
    peer_resolve_cache: ResolveCache,
    /// Live descriptors keyed by swarm infohash. Filled only by signed-catalog content-id
    /// resolutions (never BEP-9 or a transport url; own broadcasts are read from the seed
    /// registry instead), so a later bare-infohash open uses the real geometry, pubkey and
    /// trackers (#164). Separate from `resolve_cache`, which is keyed by the content-id string.
    infohash_index: InfohashIndex,
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
            infohash_index: InfohashIndex::new(INFOHASH_INDEX_CAPACITY),
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

    /// Resolve a content-id to a [`StreamInfo`] by fetching its `AceStreamTransport` metadata
    /// from the signed catalog path, falling back to BEP-9 `ut_metadata` from a metadata-swarm
    /// peer (cached with a TTL). The content-id itself is the metadata-swarm handshake key;
    /// the result carries the real infohash. Only a signed-catalog result is recorded in the
    /// infohash index; a BEP-9 result goes to `peer_resolve_cache` and never makes its infohash
    /// openable (#164).
    async fn resolve_content_id(&self, content_id: &str) -> Result<StreamInfo, ProviderError> {
        if let Some(info) = self.resolve_cache.get(content_id) {
            // Re-record: the bounded infohash index may have evicted it since (#164).
            self.infohash_index.put(info.clone());
            return Ok(info);
        }
        if let Some(info) = self.peer_resolve_cache.get(content_id) {
            return Ok(info);
        }
        let key = hex20(content_id).map_err(|_| ProviderError::Backend("bad content-id".into()))?;

        match resolve_via_catalog(content_id).await {
            Ok(info) => {
                let ih = infohash_hex(&info.infohash);
                crate::alog!("[ace] resolved cid:{content_id} via catalog -> infohash {ih}");
                self.resolve_cache.put(content_id, info.clone());
                self.infohash_index.put(info.clone());
                return Ok(info);
            }
            Err(e) => crate::alog!("[ace] resolve cid:{content_id}: catalog failed: {e:?}"),
        }

        let all = if self.bootstrap_peers.is_empty() {
            discover_peers(
                &self.default_trackers,
                &key,
                &random_peer_id(),
                self.discovery_announce_port(),
            )
            .await
        } else {
            self.bootstrap_peers.clone()
        };
        crate::alog!(
            "[ace] resolve cid:{content_id}: {} metadata peer(s)",
            all.len()
        );

        for addr in all {
            let Ok(Ok(session)) =
                tokio::time::timeout(CONNECT_TIMEOUT, connect(&addr.to_string())).await
            else {
                continue; // unreachable peer; don't waste the log on it
            };
            // Bound each peer's reads so a connected-but-silent peer doesn't stall resolution.
            let mut session = session.with_timeout(RESOLVE_PEER_TIMEOUT);
            match resolve_via_peer(&mut session, key, &self.identity).await {
                Ok(info) => {
                    let ih = infohash_hex(&info.infohash);
                    crate::alog!("[ace] resolved cid:{content_id} via {addr} -> infohash {ih}");
                    // BEP-9 only binds the blob to the content id the caller chose: keep it
                    // out of the infohash index (#164).
                    self.peer_resolve_cache.put(content_id, info.clone());
                    return Ok(info);
                }
                Err(ResolveError::Peer(why)) => crate::alog!("[ace] resolve {addr}: {why}"),
                Err(e) => crate::alog!("[ace] resolve {addr}: {e:?}"),
            }
        }
        Err(ProviderError::Backend(
            "content-id resolution: no metadata peer responded".into(),
        ))
    }

    /// Resolve a live `id` to a verified [`StreamInfo`]: the single resolver behind every live
    /// entry point (native `/streams`, compat `/ace/getstream` + `/ace/manifest.m3u8`, and
    /// `outpace play`). See #164.
    ///
    /// - `cid:<40hex>`: signed catalog, then BEP-9 peers ([`Self::resolve_content_id`]).
    /// - a transport-url id: fetched under the SSRF guard; not recorded in the infohash index.
    /// - a bare 40-hex infohash: a broadcast this daemon originates, or a descriptor resolved
    ///   through the signed catalog and indexed ([`Self::verified_info_for_infohash`]);
    ///   otherwise [`ProviderError::Unresolvable`]. BEP-9 and transport-url descriptors never
    ///   qualify.
    ///
    /// Every signed-catalog content-id descriptor resolved here is recorded in the infohash
    /// index, so the stream can later be opened by its infohash too (BEP-9 results are not). outpace never guesses live geometry.
    async fn resolve_live_info(&self, id: &str) -> Result<StreamInfo, ProviderError> {
        if let Some(content_id) = id.strip_prefix("cid:") {
            return self.resolve_content_id(content_id).await;
        }
        if let Some(url) = crate::transport_url::decode_transport_url(id) {
            // Not recorded in the index: a transport URL is caller-supplied, and the index is
            // shared state whose trackers the infohash does not bind (#164).
            return stream_info_from_transport_url(&url)
                .await
                .map_err(|e| ProviderError::Backend(format!("transport url: {e:?}")));
        }
        if is_bare_hex40(id) {
            return match self.verified_info_for_infohash(id) {
                Ok(info) => {
                    crate::alog!("[ace] open {id}: using a descriptor verified in this process");
                    Ok(info)
                }
                Err(e) => {
                    crate::alog!("[ace] open {id}: refused, no verified descriptor for it");
                    Err(e)
                }
            };
        }
        Err(ProviderError::Backend(
            "id must be a 40-hex infohash, cid:<40hex>, or a transport-url id".into(),
        ))
    }

    /// The verified live descriptor for a bare 40-hex infohash, or
    /// [`ProviderError::Unresolvable`] with a user-facing reason. Offline and synchronous, so
    /// the compat routes can pre-check an id before minting playback URLs
    /// ([`StreamProvider::check_openable`]).
    ///
    /// A broadcast this daemon originates wins: its transport is self-minted. Otherwise the
    /// shared index serves a descriptor resolved through the signed catalog. BEP-9 and
    /// transport-url descriptors are never in the index: neither binds the descriptor's
    /// trackers to the infohash (#164).
    fn verified_info_for_infohash(&self, id: &str) -> Result<StreamInfo, ProviderError> {
        let infohash = hex20(id).map_err(|_| ProviderError::Backend("bad infohash".into()))?;
        // Decoding recomputes the infohash from the minted bytes.
        if let Some(transport) = self
            .seed_registry
            .broadcast_transport_for_infohash(&infohash)
        {
            if let Ok(info) = stream_info_from_transport(&transport) {
                if info.infohash == infohash {
                    return Ok(info);
                }
            }
        }
        if let Some(info) = self.infohash_index.get(&infohash) {
            return Ok(info);
        }
        Err(ProviderError::Unresolvable(unresolved_infohash_message(id)))
    }

    /// Resolve `id` to a single-file VOD, returning a handle that knows its total length and can
    /// open verified byte ranges. Errors if the id is live, multi-file, or carries no VOD
    /// descriptor (a bare infohash has no piece hashes). Peer discovery and the actual download
    /// are deferred to [`AceVodContent::open_range`], so a seek fetches only its covering pieces.
    async fn resolve_vod_inner(&self, id: &str) -> Result<Box<dyn VodContent>, ProviderError> {
        let info = self.resolve_vod_info(id).await?;
        let infohash = info.infohash;
        let piece_length = info.piece_length;
        let chunk_length = info.chunk_length;
        let store = self.seed_registry.lease_store(infohash, || {
            build_piece_store(
                piece_length,
                chunk_length,
                self.seed_store_bytes,
                // VOD keeps downloaded pieces for range serving — byte-only, no age bound.
                None,
                self.cache_type,
                &self.cache_dir,
                &infohash,
            )
        });
        Ok(Box::new(AceVodContent {
            info,
            bootstrap_peers: self.bootstrap_peers.clone(),
            announce_peer_port: self.announce_peer_port.clone(),
            store,
            peers: Arc::new(tokio::sync::Mutex::new(None)),
            range_lock: Arc::new(tokio::sync::Mutex::new(())),
        }))
    }

    /// Fetch the VOD transport descriptor for `id` and decode it into a [`VodInfo`]. Accepts a
    /// `cid:<40hex>` content-id (catalog, then metadata-swarm peer) or a transport-url id; a
    /// bare infohash is rejected (it carries no descriptor / piece hashes).
    async fn resolve_vod_info(&self, id: &str) -> Result<VodInfo, ProviderError> {
        let bytes = if let Some(content_id) = id.strip_prefix("cid:") {
            let key =
                hex20(content_id).map_err(|_| ProviderError::Backend("bad content-id".into()))?;
            match catalog_transport_bytes(content_id).await {
                Ok(b) => b,
                Err(e) => {
                    crate::alog!("[ace] vod resolve cid:{content_id}: catalog failed: {e:?}");
                    self.vod_transport_via_peers(key).await?
                }
            }
        } else if let Some(url) = crate::transport_url::decode_transport_url(id) {
            transport_bytes_from_url(&url)
                .await
                .map_err(|e| ProviderError::Backend(format!("transport url: {e:?}")))?
        } else if id.len() == 40 && id.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(ProviderError::Backend(
                "a bare infohash is not a VOD target (no descriptor with piece hashes)".into(),
            ));
        } else {
            return Err(ProviderError::Backend(
                "id must be cid:<40hex> or a transport-url id for VOD".into(),
            ));
        };
        vod_info_from_transport(&bytes)
            .map_err(|e| ProviderError::Backend(format!("vod descriptor: {e:?}")))
    }

    /// Fetch VOD transport bytes over a metadata-swarm peer (BEP-9 `ut_metadata`), trying each
    /// discovered/bootstrap peer in turn — the VOD analogue of the peer fallback in
    /// [`Self::resolve_content_id`].
    async fn vod_transport_via_peers(&self, key: [u8; 20]) -> Result<Vec<u8>, ProviderError> {
        let all = if self.bootstrap_peers.is_empty() {
            discover_peers(
                &self.default_trackers,
                &key,
                &random_peer_id(),
                self.discovery_announce_port(),
            )
            .await
        } else {
            self.bootstrap_peers.clone()
        };
        for addr in all {
            let Ok(Ok(session)) =
                tokio::time::timeout(CONNECT_TIMEOUT, connect(&addr.to_string())).await
            else {
                continue;
            };
            let mut session = session.with_timeout(RESOLVE_PEER_TIMEOUT);
            match transport_bytes_via_peer(&mut session, key, &self.identity).await {
                Ok(b) => return Ok(b),
                Err(e) => crate::alog!("[ace] vod resolve {addr}: {e:?}"),
            }
        }
        Err(ProviderError::Backend(
            "vod content-id resolution: no metadata peer responded".into(),
        ))
    }
}

/// Whether `id` is a bare 40-hex string: a swarm infohash, or a content id missing its `cid:`
/// prefix (#165). The two are indistinguishable by shape.
fn is_bare_hex40(id: &str) -> bool {
    id.len() == 40 && id.bytes().all(|b| b.is_ascii_hexdigit())
}

/// The user-facing reason a bare 40-hex id cannot be opened (#164), with the `cid:` hint for a
/// content id pasted without its prefix (#165).
fn unresolved_infohash_message(id: &str) -> String {
    format!(
        "no verified transport descriptor for infohash {id}: outpace does not guess live stream \
         geometry. Open the stream by content id first (cid:<content-id> on /streams, \
         content_id=<content-id> on /ace/getstream, acestream://<content-id> for outpace play); \
         its infohash then works in this process. If {id} is itself a content id, open it as one \
         (cid:{id})."
    )
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

/// The leading run of `[first_piece, end_piece)` already present in `cache`. Returns those pieces'
/// bytes plus `download_from`, the first index still needing download — so `open_range` serves the
/// cached prefix from memory and only fetches `[download_from, end_piece)`. For the common
/// sequential-read pattern (each segment covers a still-hot piece plus at most one new one), the
/// shared piece is served from cache and never re-downloaded.
async fn cached_prefix(
    store: &Arc<tokio::sync::Mutex<PieceStore>>,
    info: &VodInfo,
    first_piece: u64,
    end_piece: u64,
) -> (Vec<Bytes>, u64) {
    let mut prefix = Vec::new();
    let mut p = first_piece;
    while p < end_piece {
        match PieceStore::shared_block(store, p, 0, info.piece_size(p) as u32).await {
            Some(bytes) => {
                prefix.push(Bytes::from(bytes));
                p += 1;
            }
            None => break,
        }
    }
    (prefix, p)
}

/// A resolved VOD ready to serve byte ranges. Holds the descriptor geometry plus the discovery
/// inputs `open_range` needs, and a shared bounded piece cache + once-discovered peer list so the
/// many range reads of one playback (HLS segments, seeks) resolve peers once and download each
/// covering piece at most once.
struct AceVodContent {
    info: VodInfo,
    bootstrap_peers: Vec<SocketAddrV4>,
    announce_peer_port: tokio::sync::watch::Receiver<Option<u16>>,
    /// Shared cache/reseed store and the producer lease anchoring its registry lifetime.
    store: (Arc<tokio::sync::Mutex<PieceStore>>, SeedLease),
    /// Peers discovered once and reused by every range read (VOD swarms are stable, unlike the
    /// live edge). `None` until the first read discovers them.
    peers: Arc<tokio::sync::Mutex<Option<Vec<SocketAddrV4>>>>,
    /// Serializes cache inspection plus download streams so overlapping requests never fetch
    /// and write the same piece concurrently.
    range_lock: Arc<tokio::sync::Mutex<()>>,
}

impl AceVodContent {
    /// Peers for this VOD, discovered once and cached. Bootstrap peers (tests / explicit config)
    /// are used directly. VOD swarms are standard BitTorrent, discovered like the live path.
    async fn peers(&self) -> Vec<SocketAddrV4> {
        let mut guard = self.peers.lock().await;
        if let Some(peers) = guard.as_ref() {
            return peers.clone();
        }
        let discovered = if self.bootstrap_peers.is_empty() {
            let announce_port = self.announce_peer_port.borrow().unwrap_or(0);
            discover_peers(
                &self.info.trackers,
                &self.info.infohash,
                &random_peer_id(),
                announce_port,
            )
            .await
        } else {
            self.bootstrap_peers.clone()
        };
        *guard = Some(discovered.clone());
        discovered
    }
}

#[async_trait]
impl VodContent for AceVodContent {
    fn content_length(&self) -> u64 {
        self.info.total_length
    }

    async fn open_range(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Box<dyn VodByteSource>, ProviderError> {
        let total = self.info.total_length;
        if total == 0 || start > end || end >= total {
            return Err(ProviderError::Backend(format!(
                "vod range [{start}, {end}] out of bounds for {total}-byte content"
            )));
        }
        let piece_length = self.info.piece_length;
        let range_guard = self.range_lock.clone().lock_owned().await;
        // The contiguous pieces covering [start, end]. `download_vod_pieces` verifies each whole
        // piece against its SHA-1 hash before emitting it, so trimming its output to the range
        // never yields an unverified byte.
        let first_piece = start / piece_length;
        let end_piece = end / piece_length + 1;

        // Serve any already-cached leading pieces from memory; only download the missing suffix.
        let (prefix, download_from) =
            cached_prefix(&self.store.0, &self.info, first_piece, end_piece).await;

        let (tx, rx) = mpsc::channel::<Bytes>(64);
        if download_from < end_piece {
            // VOD swarms are standard BitTorrent; discover the same way live does (once, cached).
            let peers = self.peers().await;
            crate::alog!(
                "[ace] open_range [{start}, {end}]: pieces [{download_from}, {end_piece}) \
                 (prefix {} cached), {} peer(s)",
                prefix.len(),
                peers.len()
            );
            if peers.is_empty() {
                return Err(ProviderError::Backend("no VOD peers discovered".into()));
            }
            let info = self.info.clone();
            let store = self.store.0.clone();
            let chunk_length = self.info.chunk_length;
            // Download the missing suffix, teeing every whole verified piece into the cache as it
            // streams so a later overlapping read finds it instead of re-downloading. The
            // downloader runs detached: if the consumer drops (leaving the tee loop, dropping
            // `drx`), `download_vod_pieces` sees `ConsumerGone` on its next send and stops.
            let (dtx, mut drx) = mpsc::channel::<Bytes>(64);
            let downloader = tokio::spawn(async move {
                if let Err(e) =
                    download_vod_pieces(info, peers, dtx, download_from, end_piece).await
                {
                    crate::alog!("[ace] vod range download ended: {e:?}");
                }
            });
            tokio::spawn(async move {
                // Keep the range transaction serialized until the downloader has observed
                // cancellation and exited, not merely until the HTTP consumer drops its source.
                let _range_guard = range_guard;
                let mut idx = download_from;
                loop {
                    let piece = tokio::select! {
                        _ = tx.closed() => break,
                        piece = drx.recv() => match piece {
                            Some(piece) => piece,
                            None => break,
                        },
                    };
                    // The downloader emits only whole pieces after SHA-1 verification. Populate
                    // the shared store only here, never from partial/unverified peer blocks.
                    for (chunk, data) in piece.chunks(chunk_length as usize).enumerate() {
                        PieceStore::shared_put_chunk_with_header(
                            &store,
                            idx,
                            chunk as u16,
                            [0; 8],
                            data,
                        )
                        .await;
                    }
                    idx += 1;
                    if tx.send(piece).await.is_err() {
                        break;
                    }
                }
                drop(drx);
                let _ = downloader.await;
            });
        } else {
            drop(range_guard);
        }
        // else: the whole range is cached — `tx` drops here, closing `rx`, so the source emits
        // only the cached prefix.

        // The first covering piece begins at `first_piece * piece_length`; drop the bytes before
        // `start`, then emit exactly the range length.
        let skip = start - first_piece * piece_length;
        let emit_len = end - start + 1;
        Ok(Box::new(VodSource {
            prefix: prefix.into(),
            rx,
            skip,
            remaining: emit_len,
            emit_len,
        }))
    }
}

/// Streams verified whole-piece bytes trimmed to a byte range: it drops `skip` leading bytes (the
/// offset of `start` inside the first covering piece) and stops after `emit_len` bytes, so a caller
/// only ever sees the requested `[start, end]`. Pieces come first from `prefix` (leading pieces
/// already in the VOD's cache) and then from `rx` (the freshly downloaded suffix) — a single
/// contiguous, in-order stream of the covering pieces.
struct VodSource {
    /// Cached leading covering pieces, emitted (and trimmed) before anything from `rx`.
    prefix: VecDeque<Bytes>,
    rx: mpsc::Receiver<Bytes>,
    /// Bytes still to drop from the front before emitting (offset of `start` in the first piece).
    skip: u64,
    /// Bytes still to emit before end-of-range.
    remaining: u64,
    /// Total bytes this source will emit (the range length); constant after construction.
    emit_len: u64,
}

#[async_trait]
impl VodByteSource for VodSource {
    fn content_length(&self) -> u64 {
        self.emit_len
    }
    async fn next(&mut self) -> Option<Bytes> {
        while self.remaining > 0 {
            // Cached prefix pieces first, then the downloaded suffix.
            let mut chunk = match self.prefix.pop_front() {
                Some(chunk) => chunk,
                None => self.rx.recv().await?,
            };
            if self.skip > 0 {
                if self.skip >= chunk.len() as u64 {
                    self.skip -= chunk.len() as u64;
                    continue;
                }
                chunk = chunk.slice(self.skip as usize..);
                self.skip = 0;
            }
            if chunk.len() as u64 > self.remaining {
                chunk = chunk.slice(..self.remaining as usize);
            }
            self.remaining -= chunk.len() as u64;
            return Some(chunk);
        }
        None
    }
}

struct LiveOutput {
    bytes: Bytes,
    discontinuity: bool,
}

struct AceSource {
    rx: mpsc::Receiver<LiveOutput>,
    discontinuity: bool,
    peers: Arc<AtomicU32>,
    downloaded: Arc<AtomicU64>,
    uploaded: Arc<AtomicU64>,
    peers_served: Arc<AtomicU32>,
    metadata: StreamMetadata,
}

#[async_trait]
impl TsSource for AceSource {
    async fn next(&mut self) -> Option<Bytes> {
        let output = self.rx.recv().await?;
        self.discontinuity = output.discontinuity;
        Some(output.bytes)
    }
    fn take_discontinuity(&mut self) -> bool {
        std::mem::take(&mut self.discontinuity)
    }
    fn stats(&self) -> SourceStats {
        SourceStats {
            peers: self.peers.load(Ordering::Relaxed),
            bitrate: 0,
            buffer_ms: 0,
            downloaded: self.downloaded.load(Ordering::Relaxed),
            uploaded: self.uploaded.load(Ordering::Relaxed),
            peers_served: self.peers_served.load(Ordering::Relaxed),
        }
    }
    fn metadata(&self) -> StreamMetadata {
        self.metadata.clone()
    }
}

#[async_trait]
impl StreamProvider for AceProvider {
    fn network(&self) -> &'static str {
        "ace"
    }

    async fn resolve_vod(&self, id: &str) -> Result<Box<dyn VodContent>, ProviderError> {
        self.resolve_vod_inner(id).await
    }

    fn check_openable(&self, id: &str) -> Result<(), ProviderError> {
        if is_bare_hex40(id) {
            self.verified_info_for_infohash(id).map(|_| ())
        } else {
            Ok(())
        }
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

impl AceProvider {
    // Shared resolved-live entry point; injected discovery keeps protocol regressions offline.
    async fn open_resolved(
        &self,
        id: &str,
        info: StreamInfo,
        discovery: PeerDiscovery,
    ) -> Result<Box<dyn TsSource>, ProviderError> {
        let resolved_at = Instant::now();
        // Bootstrap peers are the proven/direct path and must be tried without waiting for
        // tracker/DHT discovery. Background refill can still discover more peers after start.
        let mut initial_run = None;
        let peers = if self.bootstrap_peers.is_empty() {
            let mut run = DiscoveryRun::start(&discovery, DiscoveryOptions::default());
            self.warm_peers.initialized().await;
            let hints = self.warm_peers.hints(&info.infohash);
            let mut peers: Vec<_> = hints.into_iter().map(|(addr, _)| addr).collect();
            if peers.is_empty() {
                peers.extend(run.receiver.recv().await);
            }
            while let Ok(peer) = run.receiver.try_recv() {
                peers.push(peer);
            }
            if !peers.is_empty() {
                initial_run = Some(run);
            }
            peers
        } else {
            self.bootstrap_peers.clone()
        };
        crate::alog!(
            "[ace] discovery stage=first-candidates after_resolution_ms={} count={}",
            resolved_at.elapsed().as_millis(),
            peers.len()
        );
        crate::alog!("[ace] open {id}: discovered {} peer(s)", peers.len());
        if peers.is_empty() {
            return Err(ProviderError::Backend(
                "no peers (no trackers/bootstrap)".into(),
            ));
        }

        let (tx, rx) = mpsc::channel::<LiveOutput>(256);
        let peer_count = Arc::new(AtomicU32::new(0));
        let downloaded = Arc::new(AtomicU64::new(0));
        let uploaded = Arc::new(AtomicU64::new(0));
        let peers_served = Arc::new(AtomicU32::new(0));
        let identity = self.identity.clone();
        let stats_peers = peer_count.clone();
        let stats_downloaded = downloaded.clone();
        let stats_uploaded = uploaded.clone();
        let stats_peers_served = peers_served.clone();
        let prefetch_pieces = self.prefetch_policy_for(&info);
        let startup_buffer = self.startup_buffer;
        let bitrate = info.metadata.bitrate;
        let seed = SeedConfig {
            registry: self.seed_registry.clone(),
            store_bytes: self.seed_store_bytes,
            store_retention: self.seed_store_retention,
            enabled: self.enable_seeding,
            prefetch_pieces,
            live_recovery: self.live_recovery,
            cache_type: self.cache_type,
            cache_dir: self.cache_dir.clone(),
            warm_peers: self.warm_peers.clone(),
        };
        let announce_info = info.clone();
        let metadata = info.metadata.clone();
        let announce_port = self.announce_peer_port.clone();
        let reachability = self.reachability.clone();
        tokio::spawn(async move {
            // Run the download loop and the periodic seeder self-announce concurrently;
            // whichever ends first (normally `follow_live`, when the consumer drops) tears
            // down the other — no separate lifecycle to manage.
            tokio::select! {
                _ = follow_live(info, peers, identity, tx, stats_peers, downloaded, uploaded, peers_served, seed, discovery, reachability, initial_run) => {},
                _ = announce_seeder_periodically(announce_info, announce_port) => {},
            }
        });
        let source = AceSource {
            rx,
            discontinuity: false,
            peers: peer_count,
            downloaded: stats_downloaded,
            uploaded: stats_uploaded,
            peers_served: stats_peers_served,
            metadata,
        };
        Ok(StartupBufferedSource::new(
            Box::new(source),
            startup_buffer,
            bitrate,
        ))
    }
}

/// Periodically re-announce this infohash as a seeder (`left=0`, event=Completed) to its
/// trackers, so outpace becomes organically discoverable to peers looking for this stream
/// while we're serving it; see `docs/protocol/notes/24-seeder-self-announce.md`.
/// A no-op loop (never announces) when `port` is `None` — i.e. no inbound listener backs an
/// advertisable endpoint (`enable_inbound` off, or a one-shot CLI leech): we must not invite
/// peers to dial a port nobody is serving on. The advertised `port` is always the peer
/// endpoint, never the HTTP API port (issue #21).
async fn announce_seeder_periodically(
    info: StreamInfo,
    port: tokio::sync::watch::Receiver<Option<u16>>,
) {
    announce_infohash_periodically_dynamic(info.trackers, info.infohash, port).await
}

/// Dynamic form used by daemon announce paths. Port changes interrupt the normal interval so
/// the newly mapped (or fallback) endpoint is advertised promptly.
pub async fn announce_infohash_periodically_dynamic(
    trackers: Vec<String>,
    infohash: [u8; 20],
    mut port_rx: tokio::sync::watch::Receiver<Option<u16>>,
) {
    let peer_id = random_peer_id();
    loop {
        let port = *port_rx.borrow_and_update();
        if let Some(port) = port {
            announce_infohash_once(&trackers, &infohash, &peer_id, port).await;
        }
        tokio::select! {
            _ = tokio::time::sleep(SEEDER_ANNOUNCE_INTERVAL) => {},
            changed = port_rx.changed() => {
                if changed.is_err() {
                    std::future::pending::<()>().await;
                }
            }
        }
    }
}

/// The tracker+DHT self-announce loop, decoupled from `StreamInfo` so both the leech path
/// (a followed live stream) and B1 origination (a broadcast we minted ourselves, which has
/// no `StreamInfo` at all — just an infohash and trackers) can reuse the same primitive.
pub async fn announce_infohash_periodically(trackers: Vec<String>, infohash: [u8; 20], port: u16) {
    let peer_id = random_peer_id();
    loop {
        announce_infohash_once(&trackers, &infohash, &peer_id, port).await;
        tokio::time::sleep(SEEDER_ANNOUNCE_INTERVAL).await;
    }
}

async fn announce_infohash_once(
    trackers: &[String],
    infohash: &[u8; 20],
    peer_id: &[u8; 20],
    port: u16,
) {
    let peers = announce_seeder(trackers, infohash, peer_id, port).await;
    // DHT self-announce too, not just tracker: real Acestream swarms are largely
    // DHT-populated (README.md), so tracker-only self-announce under-serves
    // discoverability. `dht_announce_peer` is a separate primitive (not folded into
    // `announce_seeder` itself) because it's a multi-second live network call that
    // would otherwise turn `announce_seeder`'s fast offline unit test into a slow,
    // network-dependent one.
    let dht_announced = dht_announce_peer(infohash, port, DHT_ANNOUNCE_BUDGET).await;
    crate::alog!(
            "[ace] seeder self-announce for {}: {} tracker peer(s) seen, DHT announce_peer sent to {dht_announced} node(s)",
            hex_preview(infohash),
            peers.len(),
        );
}

fn prefer_window(candidate: &LivePosition, current: &LivePosition) -> bool {
    let score = |w: &LivePosition| (w.max_piece, w.position, -w.distance_from_source);
    score(candidate) > score(current)
}

struct ConnectedUpstream {
    session: PeerSession<TcpStream>,
    addr: SocketAddrV4,
    window: LivePosition,
    /// The public IP this peer echoed to us in `yourip`, if any — harvested for reachability
    /// observability (issue #22) when this upstream is activated.
    yourip: Option<IpAddr>,
}

enum PeerConnectAttempt {
    Connected(ConnectedUpstream),
    Failed(PeerConnectFailure),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PeerConnectFailure {
    addr: SocketAddrV4,
    stage: PeerConnectStage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PeerConnectStage {
    Tcp,
    Handshake,
    Window,
    TotalTimeout,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct PeerConnectStats {
    connected: usize,
    tcp: usize,
    handshake: usize,
    window: usize,
    total_timeout: usize,
    task: usize,
}

impl PeerConnectStats {
    fn record_connected(&mut self) {
        self.connected += 1;
    }

    fn record_failure(&mut self, failure: PeerConnectFailure) {
        let _addr = failure.addr;
        match failure.stage {
            PeerConnectStage::Tcp => self.tcp += 1,
            PeerConnectStage::Handshake => self.handshake += 1,
            PeerConnectStage::Window => self.window += 1,
            PeerConnectStage::TotalTimeout => self.total_timeout += 1,
        }
    }

    fn record_task_failure(&mut self) {
        self.task += 1;
    }

    fn has_observations(&self) -> bool {
        self.connected + self.tcp + self.handshake + self.window + self.total_timeout + self.task
            > 0
    }

    fn summary(&self) -> String {
        let attempted = self.connected
            + self.tcp
            + self.handshake
            + self.window
            + self.total_timeout
            + self.task;
        let mut parts = vec![format!("attempted={attempted}")];
        if self.connected > 0 {
            parts.push(format!("connected={}", self.connected));
        }
        if self.tcp > 0 {
            parts.push(format!("tcp={}", self.tcp));
        }
        if self.handshake > 0 {
            parts.push(format!("handshake={}", self.handshake));
        }
        if self.window > 0 {
            parts.push(format!("window={}", self.window));
        }
        if self.total_timeout > 0 {
            parts.push(format!("total_timeout={}", self.total_timeout));
        }
        if self.task > 0 {
            parts.push(format!("task={}", self.task));
        }
        parts.join(" ")
    }
}

#[derive(Debug)]
enum PeerCommand {
    RequestPiece { piece: u64, chunks_per_piece: u16 },
    Send(PeerMessage),
    Stop,
}

#[derive(Debug)]
enum PeerEvent {
    Message {
        peer_id: u64,
        addr: SocketAddrV4,
        msg: PeerMessage,
    },
    Lost {
        peer_id: u64,
        addr: SocketAddrV4,
    },
}

impl PeerEvent {
    fn is_current(&self, peers: &BTreeMap<u64, PeerRuntime>) -> bool {
        let (peer_id, addr) = match self {
            Self::Message { peer_id, addr, .. } | Self::Lost { peer_id, addr } => (peer_id, addr),
        };
        peers.get(peer_id).is_some_and(|peer| peer.addr == *addr)
    }
}

async fn peer_worker<S>(
    peer_id: u64,
    addr: SocketAddrV4,
    mut session: PeerSession<S>,
    mut commands: mpsc::Receiver<PeerCommand>,
    events: mpsc::Sender<PeerEvent>,
    request_floor: Arc<AtomicU64>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    loop {
        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(PeerCommand::RequestPiece { piece, chunks_per_piece }) => {
                        for chunk in 0..chunks_per_piece {
                            if piece < request_floor.load(Ordering::Relaxed) {
                                break;
                            }
                            if session.send(&chunk_request(piece as u32, chunk)).await.is_err() {
                                let _ = events.send(PeerEvent::Lost { peer_id, addr }).await;
                                return;
                            }
                        }
                    }
                    Some(PeerCommand::Send(msg)) => {
                        if session.send(&msg).await.is_err() {
                            let _ = events.send(PeerEvent::Lost { peer_id, addr }).await;
                            return;
                        }
                    }
                    Some(PeerCommand::Stop) | None => return,
                }
            }
            message = session.read_message() => {
                match message {
                    Ok(msg) => {
                        if events.send(PeerEvent::Message { peer_id, addr, msg }).await.is_err() {
                            return;
                        }
                    }
                    Err(_) => {
                        let _ = events.send(PeerEvent::Lost { peer_id, addr }).await;
                        return;
                    }
                }
            }
        }
    }
}

struct PeerRuntime {
    addr: SocketAddrV4,
    min_piece: u64,
    max_piece: u64,
    unchoked_peer: bool,
    produced_output: bool,
    seen_ids: HashSet<u8>,
    commands: mpsc::Sender<PeerCommand>,
    worker: tokio::task::JoinHandle<()>,
}

struct SessionPeerCount(Arc<AtomicU32>);
impl Drop for SessionPeerCount {
    fn drop(&mut self) {
        self.0.store(0, Ordering::Relaxed);
    }
}

impl Drop for PeerRuntime {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

// Bound the entire handshake/window exchange, not just TCP establishment: a silent
// preferred source must not monopolize a reconnect batch for the peer I/O timeout.
async fn connect_upstream(addr: SocketAddrV4, infohash: [u8; 20]) -> PeerConnectAttempt {
    match tokio::time::timeout(CONNECT_TIMEOUT, connect_upstream_inner(addr, infohash)).await {
        Ok(result) => result,
        Err(_) => PeerConnectAttempt::Failed(PeerConnectFailure {
            addr,
            stage: PeerConnectStage::TotalTimeout,
        }),
    }
}

async fn connect_upstream_inner(addr: SocketAddrV4, infohash: [u8; 20]) -> PeerConnectAttempt {
    crate::alog!("[ace] discovery stage=tcp-attempt");
    let mut session = match tokio::time::timeout(CONNECT_TIMEOUT, connect(&addr.to_string())).await
    {
        Ok(Ok(session)) => session,
        Ok(Err(_)) | Err(_) => {
            return PeerConnectAttempt::Failed(PeerConnectFailure {
                addr,
                stage: PeerConnectStage::Tcp,
            });
        }
    };
    crate::alog!("[ace] discovery stage=tcp-connected");
    if session
        .perform_handshake(infohash, random_peer_id())
        .await
        .is_err()
    {
        return PeerConnectAttempt::Failed(PeerConnectFailure {
            addr,
            stage: PeerConnectStage::Handshake,
        });
    }
    crate::alog!("[ace] discovery stage=bt-handshake");
    let Some((window, yourip)) = read_peer_window(&mut session).await else {
        return PeerConnectAttempt::Failed(PeerConnectFailure {
            addr,
            stage: PeerConnectStage::Window,
        });
    };
    PeerConnectAttempt::Connected(ConnectedUpstream {
        session,
        addr,
        window,
        yourip,
    })
}

fn pool_refill_candidates(
    peers: &[SocketAddrV4],
    active: &HashSet<SocketAddrV4>,
) -> Vec<SocketAddrV4> {
    peers
        .iter()
        .copied()
        .filter(|addr| !active.contains(addr))
        .collect()
}

fn background_discovery_options() -> DiscoveryOptions {
    DiscoveryOptions {
        peer_target: BACKGROUND_DISCOVERY_PEER_TARGET,
        dht_budget: BACKGROUND_DISCOVERY_BUDGET,
    }
}

/// Connect to and BT-handshake the peers **concurrently**, then briefly choose among peers
/// that also advertised a live window. Dead/firewalled peers no longer serialize the time
/// to first byte — a couple of unreachable peers at the front of the list used to cost
/// `CONNECT_TIMEOUT` each before we ever reached a live one (the "slow to load" report).
/// Candidate snapshots are ordered by learned-source priority and exclude cooling peers.
/// Each snapshot is visited once. Peers with fewer prior admissions are tried in separate
/// cohorts, so a fast but unproductive source cannot repeatedly cancel an unadmitted peer.
async fn connect_pool(
    peers: &[SocketAddrV4],
    infohash: [u8; 20],
    candidates: &mut SessionCandidates,
    live_recovery: LiveRecoveryConfig,
) -> Vec<ConnectedUpstream> {
    // Isolate completed exploration cohorts so fast peers cannot repeatedly abort an
    // untouched alternative. Stale rejection and failure advance history; aborts do not.
    let batches: Vec<Vec<_>> = peers
        .chunk_by(|a, b| candidates.explorations(*a) == candidates.explorations(*b))
        .flat_map(|cohort| cohort.chunks(live_recovery.max_parallel_connect))
        .map(<[_]>::to_vec)
        .collect();
    let mut stats = PeerConnectStats::default();
    for batch in batches {
        let mut set = tokio::task::JoinSet::new();
        for &addr in &batch {
            set.spawn(connect_upstream(addr, infohash));
        }
        let mut connected: Vec<ConnectedUpstream> = Vec::new();
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok(PeerConnectAttempt::Connected(candidate)) => {
                    stats.record_connected();
                    connected.push(candidate);
                    break;
                }
                Ok(PeerConnectAttempt::Failed(failure)) => {
                    candidates.explored(failure.addr);
                    candidates.failed(failure.addr, Instant::now());
                    stats.record_failure(failure);
                }
                Err(_) => stats.record_task_failure(),
            }
        }
        if connected.is_empty() {
            continue;
        }

        let deadline = tokio::time::Instant::now() + UPSTREAM_SELECTION_GRACE;
        loop {
            if connected.len() >= live_recovery.max_active_upstreams {
                break;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, set.join_next()).await {
                Ok(Some(Ok(PeerConnectAttempt::Connected(candidate)))) => {
                    stats.record_connected();
                    connected.push(candidate);
                }
                Ok(Some(Ok(PeerConnectAttempt::Failed(failure)))) => {
                    candidates.explored(failure.addr);
                    candidates.failed(failure.addr, Instant::now());
                    stats.record_failure(failure);
                }
                Ok(Some(Err(_))) => stats.record_task_failure(),
                Ok(None) | Err(_) => break,
            }
        }
        connected.sort_by(|a, b| {
            if prefer_window(&a.window, &b.window) {
                std::cmp::Ordering::Less
            } else if prefer_window(&b.window, &a.window) {
                std::cmp::Ordering::Greater
            } else {
                a.addr.cmp(&b.addr)
            }
        });
        connected.truncate(live_recovery.max_active_upstreams);
        if stats.has_observations() {
            crate::alog!("[ace] initial upstream selection: {}", stats.summary());
        }
        // Dropping `set` aborts candidates beyond the selected pool.
        return connected;
    }
    if stats.has_observations() {
        crate::alog!("[ace] no usable upstreams: {}", stats.summary());
    }
    Vec::new()
}

/// Follow the live edge from a peer, pushing contiguous TS. Races a fresh connection on
/// peer loss and refreshes discovery when the current peer set is exhausted; ends when the
/// consumer drops.
#[allow(clippy::too_many_arguments)]
async fn follow_live(
    info: StreamInfo,
    peers: Vec<SocketAddrV4>,
    identity: Arc<Identity>,
    tx: mpsc::Sender<LiveOutput>,
    peer_count: Arc<AtomicU32>,
    downloaded: Arc<AtomicU64>,
    uploaded: Arc<AtomicU64>,
    peers_served: Arc<AtomicU32>,
    seed: SeedConfig,
    discovery: PeerDiscovery,
    reachability: Option<Arc<ReachabilityMonitor>>,
    initial_run: Option<DiscoveryRun>,
) {
    follow_live_session_with_run(
        info,
        peers,
        identity,
        tx,
        peer_count,
        downloaded,
        uploaded,
        peers_served,
        seed,
        discovery,
        reachability,
        initial_run,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
async fn follow_live_session(
    info: StreamInfo,
    peers: Vec<SocketAddrV4>,
    identity: Arc<Identity>,
    tx: mpsc::Sender<LiveOutput>,
    peer_count: Arc<AtomicU32>,
    downloaded: Arc<AtomicU64>,
    uploaded: Arc<AtomicU64>,
    peers_served: Arc<AtomicU32>,
    seed: SeedConfig,
    discovery: PeerDiscovery,
    reachability: Option<Arc<ReachabilityMonitor>>,
) {
    follow_live_session_with_run(
        info,
        peers,
        identity,
        tx,
        peer_count,
        downloaded,
        uploaded,
        peers_served,
        seed,
        discovery,
        reachability,
        None,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn follow_live_session_with_run(
    info: StreamInfo,
    peers: Vec<SocketAddrV4>,
    identity: Arc<Identity>,
    tx: mpsc::Sender<LiveOutput>,
    peer_count: Arc<AtomicU32>,
    downloaded: Arc<AtomicU64>,
    uploaded: Arc<AtomicU64>,
    peers_served: Arc<AtomicU32>,
    seed: SeedConfig,
    discovery: PeerDiscovery,
    reachability: Option<Arc<ReachabilityMonitor>>,
    mut rediscovery: Option<DiscoveryRun>,
) {
    let _peer_count_guard = SessionPeerCount(peer_count.clone());
    let _warm_flush = WarmFlush(seed.warm_peers.clone());
    let chunks_per_piece = info.chunks_per_piece();
    let mut candidates = SessionCandidates::default();
    for addr in peers {
        candidates.learn(addr, CandidateKind::Discovered);
    }
    for (addr, kind) in seed.warm_peers.hints(&info.infohash) {
        candidates.learn(addr, kind);
    }
    let mut continuity: Option<Continuity> = None;
    // One owned discovery attempt can finish while learned candidates are retried.
    // Acquire the leech producer lease once for the whole session. `store` is created here and
    // reused across every reconnect (preserving buffered pieces, exactly like the old idempotent
    // `get_or_create`); `_seed_lease` lives for the entire `follow_live` body and drops on return
    // (including any early return), evicting the registry entry once no producer holds it.
    let (store, _seed_lease) = seed.registry.lease_store(info.infohash, || {
        build_piece_store(
            info.piece_length,
            info.chunk_length,
            seed.store_bytes,
            seed.store_retention,
            seed.cache_type,
            &seed.cache_dir,
            &info.infohash,
        )
    });
    loop {
        if tx.is_closed() {
            return;
        }
        drain_discovery(&mut rediscovery, &mut candidates);
        let ready = candidates.eligible(Instant::now());
        let (sources, pex) = candidates.learned_counts();
        crate::alog!(
            "[ace] reconnect candidates: eligible={} learned_source={sources} learned_pex={pex}",
            ready.len()
        );
        let mut upstreams = tokio::select! {
            _ = tx.closed() => return,
            connected = connect_pool(&ready,info.infohash,&mut candidates,seed.live_recovery) => connected,
        };
        if let Some(c) = &continuity {
            let mut usable = Vec::new();
            let mut stale = Vec::new();
            for upstream in upstreams {
                if c.window_can_resume(&upstream.window) {
                    usable.push(upstream);
                } else {
                    stale.push(upstream);
                }
            }
            if !stale.is_empty() {
                for upstream in &stale {
                    crate::alog!("[ace] {}: stale advertised window min={} max={} cannot cover next needed piece {}; harvesting gossip only",upstream.addr,upstream.window.min_piece,upstream.window.max_piece,c.reasm.next_needed());
                    candidates.explored(upstream.addr);
                    candidates.failed(upstream.addr, Instant::now());
                }
                tokio::select! {
                    _ = tx.closed() => return,
                    _ = harvest_stale_gossip(stale,&identity,&mut candidates) => {},
                }
            }
            upstreams = usable;
        }
        if upstreams.is_empty() {
            peer_count.store(0, Ordering::Relaxed);
            // Gossip may just have supplied an immediately eligible source. Try it before
            // rediscovery or waiting, rather than dropping it with the stale relay.
            let newly_ready = candidates.eligible(Instant::now());
            if newly_ready.iter().any(|a| !ready.contains(a)) {
                continue;
            }
            let delay = candidates
                .retry_delay(Instant::now())
                .max(Duration::from_millis(50));
            if ready.is_empty() && !candidates.all().is_empty() {
                // Keep cooling histories; a genuinely new batch can wake the retry earlier.
                tokio::select! {
                    _=tx.closed()=>return,
                    _=tokio::time::sleep(delay)=>{},
                    peer=next_discovery_peer(&mut rediscovery), if rediscovery.is_some()=> {
                        if let Some(peer)=peer {candidates.learn(peer,CandidateKind::Discovered);}
                    }
                }
                continue;
            }
            if rediscovery.is_none() {
                let options = if candidates.learned_counts() == (0, 0) {
                    background_discovery_options()
                } else {
                    DiscoveryOptions::default()
                };
                crate::alog!(
                    "[ace] failed-round rediscovery: target={} dht_budget={:?}",
                    options.peer_target,
                    options.dht_budget
                );
                rediscovery = Some(DiscoveryRun::start(&discovery, options));
            }
            tokio::select! {
                _=tx.closed()=>return,
                _=tokio::time::sleep(delay)=>{},
                peer=next_discovery_peer(&mut rediscovery)=> {
                    if let Some(peer)=peer {candidates.learn(peer,CandidateKind::Discovered);}
                }
            }
            continue;
        }
        let windows = upstreams
            .iter()
            .map(|u| format!("{}:{}..{}", u.addr, u.window.min_piece, u.window.max_piece))
            .collect::<Vec<_>>()
            .join(", ");
        let active_addrs: HashSet<SocketAddrV4> = upstreams.iter().map(|u| u.addr).collect();
        let refill_cohort =
            candidates.exploration_cohort(Instant::now(), &active_addrs, &HashSet::new());
        let refill_candidates = pool_refill_candidates(
            &candidates
                .eligible_discovered(Instant::now())
                .into_iter()
                .filter(|addr| Some(candidates.explorations(*addr)) == refill_cohort)
                .collect::<Vec<_>>(),
            &active_addrs,
        );
        let known_refill_peers = candidates.all();
        crate::alog!(
            "[ace] connected + handshaked upstream pool ({} peer(s)): {windows}",
            upstreams.len()
        );
        peer_count.store(upstreams.len() as u32, Ordering::Relaxed);
        let end = tokio::select! {
            _ = tx.closed() => return,
            end = follow_peer_pool_with_discovery(
            upstreams,
            &info,
            &identity,
            chunks_per_piece,
            &tx,
            &downloaded,
            &uploaded,
            &peers_served,
            &seed,
            &store,
            &mut continuity,
            refill_candidates,
            known_refill_peers,
            discovery.clone(),
            &mut candidates,
            &peer_count,
            reachability.as_ref(),
            &mut rediscovery,
        )
        => end,
        };
        match end {
            FollowEnd::ConsumerGone => return,
            FollowEnd::PeerLost(lost) => {
                peer_count.store(0, Ordering::Relaxed);
                crate::alog!("[ace] rebuilding upstream pool: reason=PeerLost lost={lost} learned_source={} learned_pex={}",candidates.learned_counts().0,candidates.learned_counts().1);
            }
            FollowEnd::PoolStale { stalled, lost } => {
                peer_count.store(0, Ordering::Relaxed);
                for addr in stalled {
                    candidates.failed(addr, Instant::now());
                }
                crate::alog!("[ace] rebuilding upstream pool: reason=PoolStale lost={lost} learned_source={} learned_pex={}",candidates.learned_counts().0,candidates.learned_counts().1);
            }
        }
    }
}

enum FollowEnd {
    ConsumerGone,
    PeerLost(usize),
    /// Connected peers stopped producing contiguous output past the stale timeout.
    /// Both these peers and genuine drops retain session candidates with bounded cooldowns;
    /// reconnect still uses Continuity::resume/skip_to to recover an evicted gap.
    PoolStale {
        stalled: Vec<SocketAddrV4>,
        lost: usize,
    },
}

/// Piece-continuity state that must survive a peer reconnect within one `follow_live`
/// session. Recreating it fresh per connection (the pre-fix behavior) either re-emitted
/// pieces already served into the live broadcast — a duplicate splice — or left the
/// reassembler waiting forever for a piece the new peer's window had already evicted, since
/// `PieceReassembler` only ever emits strictly contiguously from its cursor. Real swarm
/// connections drop and reconnect routinely, so this was a guaranteed visible stutter on
/// every hop, not an edge case. See `docs/protocol/notes/23-reconnect-continuity.md`.
struct WarmFlush(WarmPeerCache);
impl Drop for WarmFlush {
    fn drop(&mut self) {
        self.0.request_flush();
    }
}

struct Continuity {
    live_recovery: LiveRecoveryConfig,
    reasm: PieceReassembler,
    resync: ace_media::mpegts::TsResync,
    output_gate: ace_media::mpegts::KeyframeGate,
    transport_recovery: Option<TransportRecovery>,
    discontinuity_pending: bool,
    scheduler: Scheduler,
    active_peers: ActivePeers,
    received_chunks: BTreeMap<u64, HashSet<u16>>,
    /// None marks mixed producers; entries share the bounded reassembly accept window.
    piece_producers: BTreeMap<u64, Option<SocketAddrV4>>,
    /// When each still-outstanding piece was (re-)requested — drives per-piece retransmission
    /// independent of the whole-pool stale timer.
    requested_at: HashMap<u64, Instant>,
    /// When the playback cursor (`reasm.next_needed()`) last advanced. If it stays put past the
    /// configured request timeout and no upstream window still covers it, the piece was evicted
    /// and we skip forward rather than freeze.
    next_needed_since: Instant,
    head: u64,
    /// Only a fresh lone-window pool delays media; reconnect continuity stays authoritative.
    startup_deadline: Option<Instant>,
    /// Workers discard queued stale chunk writes after new live-head evidence.
    request_floor: Arc<AtomicU64>,
    /// Pieces behind the live edge to leave as a cushion when re-syncing forward to live.
    prefetch: u64,
    emitted: u64,
    next_log: u64,
    authenticated_logged: bool,
}

struct TransportRecovery {
    started: Instant,
    discarded_bytes: usize,
    withheld_bytes: usize,
    loss_events: usize,
}

/// First piece to request given a peer window and a configured prefetch depth.
fn prefetch_start(min_piece: u64, max_piece: u64, prefetch: u64) -> u64 {
    max_piece.saturating_sub(prefetch).max(min_piece)
}

impl Continuity {
    /// The very first peer connection for this stream: bootstrap from its window.
    fn fresh(
        info: &StreamInfo,
        min_piece: u64,
        max_piece: u64,
        prefetch: u64,
        live_recovery: LiveRecoveryConfig,
    ) -> (Continuity, u64) {
        let start = prefetch_start(min_piece, max_piece, prefetch);
        // Strip the per-piece signature tail from the emitted media stream, and — when the
        // resolved transport gave us the source's `pubkey` — verify each piece's in-band RSA
        // signature before its bytes can be served (issue #10). A descriptor without a
        // parseable pubkey yields `sig_len == 0` and an empty `source_pubkey`, so nothing is
        // stripped or verified.
        let reasm = PieceReassembler::new(info.piece_length, start)
            .with_piece_trailer(info.sig_len as u64)
            .with_source_pubkey(info.source_pubkey.clone())
            .with_max_pieces_ahead(live_recovery.max_reasm_pieces_ahead);
        (
            Continuity {
                live_recovery,
                reasm,
                resync: ace_media::mpegts::TsResync::new(),
                output_gate: ace_media::mpegts::KeyframeGate::new_passthrough(),
                transport_recovery: None,
                discontinuity_pending: false,
                scheduler: Scheduler::new(live_recovery.max_piece_advance as usize),
                active_peers: ActivePeers::new(),
                received_chunks: BTreeMap::new(),
                piece_producers: BTreeMap::new(),
                requested_at: HashMap::new(),
                next_needed_since: Instant::now(),
                head: max_piece,
                startup_deadline: None,
                request_floor: Arc::new(AtomicU64::new(start)),
                prefetch,
                emitted: 0,
                next_log: 1 << 20,
                authenticated_logged: false,
            },
            start,
        )
    }

    /// A reconnect to a new peer after losing the previous one: keep going from where we
    /// left off rather than restarting near the new peer's head (which would duplicate
    /// already-served pieces into the broadcast). Only skips forward — an unavoidable,
    /// logged gap — if the new peer's window has already evicted the piece we still needed
    /// (we were disconnected longer than the live window covers). Returns the piece index
    /// to advertise as our position in the outgoing handshake.
    fn resume(&mut self, addr: SocketAddrV4, min_piece: u64, max_piece: u64) -> u64 {
        self.head = self.head.max(max_piece);
        self.scheduler.clear_in_flight();
        self.active_peers = ActivePeers::new();
        self.received_chunks.clear();
        self.piece_producers.clear();
        self.requested_at.clear();
        self.next_needed_since = Instant::now();
        let next = self.reasm.next_needed();
        let resume = next.max(min_piece);
        if resume > next {
            crate::alog!(
                "[ace] {addr}: reconnect gap — peer's window already evicted pieces {next}..{}; skipping ahead",
                resume - 1
            );
            self.reasm.skip_to(resume);
            self.arm_output_gate();
        }
        resume
    }

    /// Whether a peer advertising `window` can still serve the piece we need next.
    ///
    /// A window that already covers the piece trivially can. The subtle case is the live edge:
    /// once we are caught up, `next_needed` is the piece *after* the newest one the source has
    /// produced, so no peer in the swarm can advertise it yet. Requiring coverage there rejects
    /// every peer at once and leaves the pool reconnecting into "no usable upstreams" until some
    /// advertisement drifts forward — a multi-second output gap on a stream that was healthy.
    /// A peer that is current with the live edge is precisely the one to wait on: it delivers the
    /// piece via a live HAVE as soon as the source produces it. A peer whose window ends short of
    /// the edge is genuinely behind and still rejected.
    fn window_can_resume(&self, window: &LivePosition) -> bool {
        let needed = i64::try_from(self.reasm.next_needed()).unwrap_or(i64::MAX);
        if window.max_piece >= needed {
            return true;
        }
        let head = i64::try_from(self.head).unwrap_or(i64::MAX);
        needed > head && window.max_piece >= head
    }

    /// Rejected bytes cannot complete a request. Forget the partial chunk count and all
    /// outstanding copies, including requests assigned before a retransmission to another peer.
    fn release_rejected_piece(&mut self, piece: u64) {
        self.reasm.discard_partial(piece);
        self.received_chunks.remove(&piece);
        self.piece_producers.remove(&piece);
        self.scheduler.on_drop(piece);
        self.active_peers.complete_everywhere(piece);
        self.requested_at.remove(&piece);
    }

    fn note_chunk(&mut self, piece: u64, chunk: u16, chunks_per_piece: u16) -> bool {
        let chunks_per_piece = chunks_per_piece.max(1) as usize;
        let chunks = self.received_chunks.entry(piece).or_default();
        chunks.insert(chunk);
        if chunks.len() >= chunks_per_piece {
            self.received_chunks.remove(&piece);
            self.scheduler.on_complete(piece);
            // The piece may have been re-requested from more than one peer (retransmission);
            // clear it from every peer's slot and stop its retransmit timer.
            self.active_peers.complete_everywhere(piece);
            self.requested_at.remove(&piece);
            true
        } else {
            false
        }
    }

    /// Pieces still outstanding past the configured request timeout — candidates to re-request.
    /// Also prunes timer bookkeeping for pieces the cursor has already advanced past.
    fn timed_out_requests(&mut self, now: Instant) -> Vec<u64> {
        let next = self.reasm.next_needed();
        self.requested_at.retain(|&p, _| p >= next);
        self.requested_at
            .iter()
            .filter(|(_, &at)| now.duration_since(at) >= self.live_recovery.request_timeout())
            .map(|(&p, _)| p)
            .collect()
    }

    /// If the cursor has been stuck past the configured request timeout on a piece no unchoked
    /// upstream can still serve (evicted from every window), skip forward to the lowest piece some peer
    /// does have. Returns the skip target if it skipped. This is the mid-session analogue of
    /// [`resume`](Self::resume)'s reconnect-gap skip — recovering without a full teardown.
    fn skip_evicted_gap(&mut self, now: Instant) -> Option<u64> {
        let next = self.reasm.next_needed();
        if now.duration_since(self.next_needed_since) < self.live_recovery.request_timeout()
            || self.active_peers.any_unchoked_covers(next)
        {
            return None;
        }
        let floor = self.active_peers.lowest_covered_piece()?;
        if floor <= next {
            return None;
        }
        self.reasm.skip_to(floor);
        self.arm_output_gate();
        self.active_peers.prune_below(floor);
        self.requested_at.retain(|&p, _| p >= floor);
        self.received_chunks.retain(|&p, _| p >= floor);
        self.piece_producers.retain(|&p, _| p >= floor);
        self.next_needed_since = now;
        Some(floor)
    }

    /// Enforce the configured live cushion immediately, before requests or publication.
    /// This also drops stale partial/completed pieces and releases their peer slots when
    /// a newly learned head outruns the cursor. Once output exists, a forward skip uses
    /// the same fresh discontinuity gate as an unknown whole-piece loss (issue #169).
    /// Provisional startup positioning has no previously published stream to interrupt.
    fn skip_far_behind_live(&mut self, now: Instant) -> Option<u64> {
        self.request_floor
            .fetch_max(self.head.saturating_sub(self.prefetch), Ordering::Relaxed);
        let next = self.reasm.next_needed();
        if self.head.saturating_sub(next) <= self.prefetch {
            return None;
        }
        let target = self.head.saturating_sub(self.prefetch);
        self.reasm.skip_to(target);
        if self.emitted > 0 {
            self.arm_output_gate();
        } else {
            // No published stream exists to mark discontinuous at provisional startup.
            self.resync = ace_media::mpegts::TsResync::new();
        }
        self.active_peers.prune_below(target);
        self.requested_at.retain(|&p, _| p >= target);
        self.received_chunks.retain(|&p, _| p >= target);
        self.piece_producers.retain(|&p, _| p >= target);
        self.next_needed_since = now;
        Some(target)
    }

    fn arm_output_gate(&mut self) {
        if let Some(recovery) = self.transport_recovery.take() {
            crate::alog!(
                "[mpegts] transport recovery superseded by piece skip: discarded_bytes={} withheld_bytes={} duration_ms={} loss_events={}",
                recovery.discarded_bytes,
                recovery.withheld_bytes,
                recovery.started.elapsed().as_millis(),
                recovery.loss_events
            );
        }
        self.output_gate = ace_media::mpegts::KeyframeGate::new();
        self.resync = ace_media::mpegts::TsResync::new();
        self.discontinuity_pending = true;
    }

    fn filter_output_after_discontinuity(&mut self, aligned: &[u8]) -> Option<LiveOutput> {
        let output = self.output_gate.push_report(aligned);
        if let Some(recovery) = &mut self.transport_recovery {
            recovery.withheld_bytes += output.withheld_bytes;
        }
        if let Some(reason) = output.resumed {
            if let Some(recovery) = self.transport_recovery.take() {
                crate::alog!(
                    "[mpegts] transport recovery resumed: reason={reason:?} discarded_bytes={} withheld_bytes={} duration_ms={} loss_events={}",
                    recovery.discarded_bytes,
                    recovery.withheld_bytes,
                    recovery.started.elapsed().as_millis(),
                    recovery.loss_events
                );
            }
        }
        (!output.bytes.is_empty()).then(|| LiveOutput {
            bytes: Bytes::from(output.bytes),
            discontinuity: self.take_discontinuity(),
        })
    }

    fn resync_output(&mut self, bytes: &[u8]) -> Vec<LiveOutput> {
        let output = self.resync.push_report(bytes);
        let mut chunks = Vec::new();
        let mut start = 0;
        let mut emitted = self.emitted;
        for boundary in output.boundaries {
            if let Some(prefix) =
                self.filter_output_after_discontinuity(&output.bytes[start..boundary.output_offset])
            {
                emitted += prefix.bytes.len() as u64;
                chunks.push(prefix);
            }
            crate::alog!(
                "[mpegts] transport resync discarded boundary bytes: discarded_bytes={} batch_aligned_offset={} stream_output_offset={}",
                boundary.discarded_bytes,
                boundary.output_offset,
                emitted
            );
            let recovery = self
                .transport_recovery
                .get_or_insert_with(|| TransportRecovery {
                    started: Instant::now(),
                    discarded_bytes: 0,
                    withheld_bytes: 0,
                    loss_events: 0,
                });
            recovery.discarded_bytes += boundary.discarded_bytes;
            recovery.loss_events += 1;
            self.output_gate.rearm_for_discontinuity();
            self.discontinuity_pending = true;
            start = boundary.output_offset;
        }
        if let Some(suffix) = self.filter_output_after_discontinuity(&output.bytes[start..]) {
            chunks.push(suffix);
        }
        chunks
    }

    fn take_discontinuity(&mut self) -> bool {
        std::mem::take(&mut self.discontinuity_pending)
    }

    #[cfg(test)]
    fn output_gate_armed(&self) -> bool {
        !self.output_gate.is_locked()
    }

    fn register_active_peer(&mut self, id: u64, addr: SocketAddrV4, window: LivePosition) {
        self.active_peers.insert(id, addr, window);
    }

    fn set_peer_unchoked(&mut self, id: u64, unchoked: bool) {
        self.active_peers.set_unchoked(id, unchoked);
    }

    fn update_peer_window(&mut self, id: u64, min_piece: u64, max_piece: u64) {
        self.active_peers.update_window(id, min_piece, max_piece);
    }
}

// Each typed receipt owns its endpoint reservation, including queued discovered transports.
enum PoolRefill {
    ReservedDiscovered(ConnectedUpstream),
    Learned(ConnectedUpstream),
}
fn receive_pool_refill(
    refill: PoolRefill,
    learned_pending: &mut HashSet<SocketAddrV4>,
    _candidates: &mut SessionCandidates,
) -> Option<ConnectedUpstream> {
    match refill {
        PoolRefill::Learned(upstream) | PoolRefill::ReservedDiscovered(upstream) => {
            learned_pending.remove(&upstream.addr).then_some(upstream)
        }
    }
}

type CandidateConnectCompletion = (SocketAddrV4, Option<PeerConnectFailure>, bool);

enum PoolWake {
    ConsumerGone,
    Peer(Option<PeerEvent>),
    Refill(Option<PoolRefill>),
    Discovery(Option<SocketAddrV4>),
    ConnectCompletion(Option<Result<CandidateConnectCompletion, tokio::task::JoinError>>),
    CandidateRetry,
}

#[allow(clippy::too_many_arguments)]
async fn activate_upstream_peer(
    peer_id: u64,
    mut upstream: ConnectedUpstream,
    start: u64,
    identity: &Identity,
    continuity: &mut Continuity,
    event_tx: &mpsc::Sender<PeerEvent>,
    reachability: Option<&Arc<ReachabilityMonitor>>,
    replacing: Option<(&mut BTreeMap<u64, PeerRuntime>, u64)>,
) -> Result<(PeerRuntime, Option<SocketAddrV4>), SocketAddrV4> {
    // Harvest the public IP this peer echoed to us in `yourip` (issue #22). This is the single
    // funnel every activated outbound upstream passes through, so recording here captures the
    // observation once per live peer. Inert when no monitor is supplied (inbound serving off).
    if let (Some(monitor), Some(ip)) = (reachability, upstream.yourip) {
        monitor.observe_yourip(ip);
    }
    let peer_min = upstream.window.min_piece.max(0) as u64;
    let peer_max = upstream.window.max_piece.max(0) as u64;
    let hs = OutgoingExtendedHandshake {
        ace_metadata_version: 1,
        ut_metadata_id: 2,
        mi: Some(LivePosition {
            min_piece: start as i64,
            max_piece: continuity.head as i64,
            position: -1,
            distance_from_source: 1,
        }),
        node: NodeFields {
            ts: 5000 + peer_id as i64,
            ..NodeFields::default()
        },
        peer_ip: Some(upstream.addr.ip().octets()),
        metadata_size: None,
    };
    if upstream
        .session
        .send_signed_extended_handshake(&hs, identity)
        .await
        .is_err()
        || upstream
            .session
            .send(&PeerMessage::Interested)
            .await
            .is_err()
    {
        return Err(upstream.addr);
    }
    // Only successful writes consume the old provisional transport. A failed candidate
    // leaves its runtime, gossip reader, and request slots intact.
    let replaced = replacing.and_then(|(peers, id)| drop_peer_runtime(id, peers, continuity));
    continuity.register_active_peer(peer_id, upstream.addr, upstream.window);
    let (command_tx, command_rx) = mpsc::channel(64);
    let worker = tokio::spawn(peer_worker(
        peer_id,
        upstream.addr,
        upstream.session,
        command_rx,
        event_tx.clone(),
        continuity.request_floor.clone(),
    ));
    Ok((
        PeerRuntime {
            addr: upstream.addr,
            min_piece: peer_min,
            max_piece: peer_max,
            unchoked_peer: false,
            produced_output: false,
            seen_ids: HashSet::new(),
            commands: command_tx,
            worker,
        },
        replaced,
    ))
}

struct OwnedTask(tokio::task::JoinHandle<()>);
impl Drop for OwnedTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn recovery_channel_capacities(
    live_recovery: LiveRecoveryConfig,
) -> Result<(usize, usize), String> {
    live_recovery.validate()?;
    let event_capacity = live_recovery
        .max_active_upstreams
        .checked_mul(32)
        .ok_or_else(|| {
            "OUTPACE_MAX_ACTIVE_UPSTREAMS event channel capacity overflowed".to_string()
        })?;
    Ok((event_capacity, live_recovery.max_active_upstreams))
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
async fn follow_peer_pool(
    upstreams: Vec<ConnectedUpstream>,
    info: &StreamInfo,
    identity: &Identity,
    chunks_per_piece: u16,
    tx: &mpsc::Sender<LiveOutput>,
    downloaded: &Arc<AtomicU64>,
    uploaded: &Arc<AtomicU64>,
    peers_served: &Arc<AtomicU32>,
    seed: &SeedConfig,
    store: &Arc<tokio::sync::Mutex<PieceStore>>,
    continuity: &mut Option<Continuity>,
    refill_candidates: Vec<SocketAddrV4>,
    known_refill_peers: Vec<SocketAddrV4>,
    discovery: PeerDiscovery,
    candidates: &mut SessionCandidates,
    peer_count: &Arc<AtomicU32>,
    reachability: Option<&Arc<ReachabilityMonitor>>,
) -> FollowEnd {
    follow_peer_pool_with_discovery(
        upstreams,
        info,
        identity,
        chunks_per_piece,
        tx,
        downloaded,
        uploaded,
        peers_served,
        seed,
        store,
        continuity,
        refill_candidates,
        known_refill_peers,
        discovery,
        candidates,
        peer_count,
        reachability,
        &mut None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn follow_peer_pool_with_discovery(
    upstreams: Vec<ConnectedUpstream>,
    info: &StreamInfo,
    identity: &Identity,
    chunks_per_piece: u16,
    tx: &mpsc::Sender<LiveOutput>,
    downloaded: &Arc<AtomicU64>,
    uploaded: &Arc<AtomicU64>,
    peers_served: &Arc<AtomicU32>,
    seed: &SeedConfig,
    store: &Arc<tokio::sync::Mutex<PieceStore>>,
    continuity: &mut Option<Continuity>,
    refill_candidates: Vec<SocketAddrV4>,
    known_refill_peers: Vec<SocketAddrV4>,
    discovery: PeerDiscovery,
    candidates: &mut SessionCandidates,
    peer_count: &Arc<AtomicU32>,
    reachability: Option<&Arc<ReachabilityMonitor>>,
    discovery_run: &mut Option<DiscoveryRun>,
) -> FollowEnd {
    debug_assert!(!upstreams.is_empty());
    let lone_fresh = continuity.is_none() && upstreams.len() == 1;
    let primary = upstreams[0].window;
    let primary_addr = upstreams[0].addr;
    let min_piece = primary.min_piece.max(0) as u64;
    let max_piece = primary.max_piece.max(0) as u64;
    let start = match continuity {
        None => {
            let (c, start) = Continuity::fresh(
                info,
                min_piece,
                max_piece,
                seed.prefetch_pieces,
                seed.live_recovery,
            );
            *continuity = Some(c);
            crate::alog!(
                "[ace] {primary_addr}: window min={min_piece} max={max_piece} -> start={start} head={max_piece}"
            );
            start
        }
        Some(c) => {
            let start = c.resume(primary_addr, min_piece, max_piece);
            crate::alog!(
                "[ace] {primary_addr}: reconnected; window min={min_piece} max={max_piece} -> resuming from {start} head={}",
                c.head
            );
            start
        }
    };
    let continuity = continuity.as_mut().expect("initialized just above");
    let live_recovery = continuity.live_recovery;
    if lone_fresh {
        continuity.startup_deadline = Some(Instant::now() + LIVE_START_CORROBORATION);
        crate::alog!(
            "[ace] live start: withholding media for independent window evidence (up to 1000 ms)"
        );
    }
    let (event_capacity, refill_capacity) = match recovery_channel_capacities(live_recovery) {
        Ok(capacities) => capacities,
        Err(err) => {
            crate::alog!("[ace] invalid live recovery configuration: {err}");
            return FollowEnd::ConsumerGone;
        }
    };
    let (event_tx, mut event_rx) = mpsc::channel(event_capacity);
    let (refill_tx, mut refill_rx) = mpsc::channel(refill_capacity);
    // A live-held clone of the refill sender so peers learned from `id=12` peer-exchange
    // gossip can be connected and fed into the same pool-add path (keeps `refill_rx` open
    // even after the background refill task finishes).
    let pex_tx = refill_tx.clone();
    let mut learned_connects: tokio::task::JoinSet<(
        SocketAddrV4,
        Option<PeerConnectFailure>,
        bool,
    )> = tokio::task::JoinSet::new();
    let mut learned_pending = HashSet::new();
    if discovery_run.is_none() && !refill_candidates.is_empty() {
        *discovery_run = Some(DiscoveryRun::start(
            &discovery,
            background_discovery_options(),
        ));
    }
    for addr in refill_candidates {
        candidates.learn(addr, CandidateKind::Discovered);
    }
    let _ = known_refill_peers; // Knowledge already lives in the session candidate store.
                                // No detached discovery/refill producer: the session owns its stream and this pool owns
                                // one capped transport set, including queued reservations of every provenance.
    drop(refill_tx);
    let mut peers: BTreeMap<u64, PeerRuntime> = BTreeMap::new();
    let mut loss_count = 0usize;
    let mut next_peer_id = 1u64;

    for upstream in upstreams {
        let peer_id = next_peer_id;
        next_peer_id += 1;
        match activate_upstream_peer(
            peer_id,
            upstream,
            start,
            identity,
            continuity,
            &event_tx,
            reachability,
            None,
        )
        .await
        {
            Ok((runtime, _)) => {
                candidates.admitted(runtime.addr);
                peers.insert(peer_id, runtime);
            }
            Err(addr) => {
                candidates.explored(addr);
                record_pool_losses(candidates, &mut loss_count, [addr]);
            }
        }
    }
    if peers.is_empty() {
        return FollowEnd::PeerLost(loss_count);
    }
    peer_count.store(peers.len() as u32, Ordering::Relaxed);

    if peers.len() < live_recovery.max_active_upstreams || continuity.startup_deadline.is_some() {
        spawn_candidate_connects(
            candidates,
            &peers,
            info.infohash,
            live_recovery.max_parallel_connect,
            &pex_tx,
            &mut learned_connects,
            &mut learned_pending,
            true,
        );
    }
    let mut last_progress = Instant::now();
    let mut refill_closed = false;
    let mut observed_capacity = (peers.len(), learned_pending.len());
    let mut observed_retry = Instant::now();
    loop {
        let mut released_failed_slot = false;
        while let Some(Ok(result)) = learned_connects.try_join_next() {
            released_failed_slot |=
                finish_candidate_connect(result, &mut learned_pending, candidates);
        }
        let capacity_released =
            peers.len() < observed_capacity.0 || learned_pending.len() < observed_capacity.1;
        if (released_failed_slot || capacity_released)
            && (peers.len() < live_recovery.max_active_upstreams
                || continuity.startup_deadline.is_some())
        {
            spawn_candidate_connects(
                candidates,
                &peers,
                info.infohash,
                live_recovery.max_parallel_connect,
                &pex_tx,
                &mut learned_connects,
                &mut learned_pending,
                true,
            );
        }
        observed_capacity = (peers.len(), learned_pending.len());
        let now = Instant::now();
        if continuity
            .startup_deadline
            .is_some_and(|deadline| now >= deadline)
        {
            continuity.startup_deadline = None;
            last_progress = now;
            crate::alog!("[ace] live start: corroboration deadline reached; using best known window without freshness proof");
            let lost = advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
            record_pool_losses(candidates, &mut loss_count, lost);
        }
        let Some(stale_budget) =
            stale_upstream_budget(last_progress, now, live_recovery.stale_upstream_timeout())
                .or_else(|| {
                    continuity
                        .startup_deadline
                        .map(|deadline| deadline.saturating_duration_since(now))
                })
        else {
            let stalled = peers.values().map(|p| p.addr).collect::<Vec<_>>();
            crate::alog!(
                "[ace] upstream pool stale — no live progress for {:?}; reconnecting {} peer(s)",
                live_recovery.stale_upstream_timeout(),
                stalled.len()
            );
            shutdown_peer_runtimes(&mut peers);
            return FollowEnd::PoolStale {
                stalled,
                lost: loss_count,
            };
        };
        // Self-heal a single stuck piece well before the whole-pool stale timeout: re-request
        // pieces outstanding past the request timeout (to a faster peer where possible) and
        // skip a piece evicted from every upstream window.
        let newly_lost =
            retransmit_stalled_requests(&mut peers, continuity, chunks_per_piece, now).await;
        record_pool_losses(candidates, &mut loss_count, newly_lost);
        if peers.is_empty() {
            return FollowEnd::PeerLost(loss_count);
        }
        peer_count.store(peers.len() as u32, Ordering::Relaxed);

        // Wake at least every configured request-check interval so the sweep above runs even
        // while no peer sends anything.
        let wait = stale_budget
            .min(live_recovery.request_check_interval())
            .min(
                continuity
                    .startup_deadline
                    .map(|deadline| deadline.saturating_duration_since(now))
                    .unwrap_or(stale_budget),
            );
        let active = peers.values().map(|peer| peer.addr).collect();
        let retry_deadline = if (peers.len() < live_recovery.max_active_upstreams
            || continuity.startup_deadline.is_some())
            && learned_pending.len() < live_recovery.max_parallel_connect
            && learned_connects.len() < live_recovery.max_parallel_connect
        {
            candidates.retry_deadline_after(observed_retry, &active, &learned_pending)
        } else {
            None
        };
        let event = match tokio::time::timeout(wait, async {
            tokio::select! {
                _ = tx.closed() => PoolWake::ConsumerGone,
                event = event_rx.recv() => PoolWake::Peer(event),
                result=learned_connects.join_next(), if !learned_connects.is_empty()=>PoolWake::ConnectCompletion(result),
                _ = async {
                    tokio::time::sleep_until(retry_deadline.expect("enabled retry deadline").into()).await;
                }, if retry_deadline.is_some() => PoolWake::CandidateRetry,
                peer=next_discovery_peer(discovery_run), if discovery_run.is_some()=>PoolWake::Discovery(peer),
                upstream = refill_rx.recv(), if !refill_closed && (peers.len() < live_recovery.max_active_upstreams || continuity.startup_deadline.is_some()) => {
                    PoolWake::Refill(upstream)
                }
            }
        })
        .await
        {
            Ok(event) => event,
            Err(_) => continue, // sweep tick: re-check stale budget and retransmit
        };

        let event = match event {
            PoolWake::ConsumerGone => return FollowEnd::ConsumerGone,
            PoolWake::CandidateRetry => {
                observed_retry = Instant::now();
                spawn_candidate_connects(
                    candidates,
                    &peers,
                    info.infohash,
                    live_recovery.max_parallel_connect,
                    &pex_tx,
                    &mut learned_connects,
                    &mut learned_pending,
                    true,
                );
                continue;
            }
            PoolWake::ConnectCompletion(result) => {
                if let Some(Ok(result)) = result {
                    if finish_candidate_connect(result, &mut learned_pending, candidates)
                        && (peers.len() < live_recovery.max_active_upstreams
                            || continuity.startup_deadline.is_some())
                    {
                        spawn_candidate_connects(
                            candidates,
                            &peers,
                            info.infohash,
                            live_recovery.max_parallel_connect,
                            &pex_tx,
                            &mut learned_connects,
                            &mut learned_pending,
                            true,
                        );
                    }
                }
                continue;
            }
            PoolWake::Discovery(peer) => {
                if let Some(peer) = peer {
                    candidates.learn(peer, CandidateKind::Discovered);
                }
                if peers.len() < live_recovery.max_active_upstreams
                    || continuity.startup_deadline.is_some()
                {
                    spawn_candidate_connects(
                        candidates,
                        &peers,
                        info.infohash,
                        live_recovery.max_parallel_connect,
                        &pex_tx,
                        &mut learned_connects,
                        &mut learned_pending,
                        true,
                    );
                }
                continue;
            }
            PoolWake::Peer(Some(event)) if event.is_current(&peers) => event,
            PoolWake::Peer(Some(_)) => continue,
            PoolWake::Peer(None) => {
                shutdown_peer_runtimes(&mut peers);
                return FollowEnd::PeerLost(loss_count);
            }
            PoolWake::Refill(Some(upstream)) => {
                let Some(upstream) =
                    receive_pool_refill(upstream, &mut learned_pending, candidates)
                else {
                    spawn_candidate_connects(
                        candidates,
                        &peers,
                        info.infohash,
                        live_recovery.max_parallel_connect,
                        &pex_tx,
                        &mut learned_connects,
                        &mut learned_pending,
                        true,
                    );
                    continue;
                };
                candidates.learn(upstream.addr, CandidateKind::Discovered);
                if peers.values().any(|p| p.addr == upstream.addr) {
                    continue;
                }
                if !continuity.window_can_resume(&upstream.window) {
                    crate::alog!(
                        "[ace] {}: background refill stale window min={} max={} cannot cover next needed piece {}; dropping",
                        upstream.addr,
                        upstream.window.min_piece,
                        upstream.window.max_piece,
                        continuity.reasm.next_needed()
                    );
                    candidates.explored(upstream.addr);
                    candidates.failed(upstream.addr, Instant::now());
                    let gossip_budget = continuity
                        .startup_deadline
                        .map(|deadline| deadline.saturating_duration_since(Instant::now()))
                        .unwrap_or(STALE_GOSSIP_BUDGET)
                        .min(STALE_GOSSIP_BUDGET);
                    tokio::select! {
                        _ = tx.closed() => return FollowEnd::ConsumerGone,
                        _ = harvest_stale_gossip_with_budget(vec![upstream],identity,candidates,gossip_budget) => {},
                    }
                    spawn_candidate_connects(
                        candidates,
                        &peers,
                        info.infohash,
                        live_recovery.max_parallel_connect,
                        &pex_tx,
                        &mut learned_connects,
                        &mut learned_pending,
                        true,
                    );
                    continue;
                }
                let active = peers.values().map(|peer| peer.addr).collect();
                if candidates
                    .exploration_cohort(Instant::now(), &active, &learned_pending)
                    .is_some_and(|cohort| candidates.explorations(upstream.addr) > cohort)
                {
                    // Preserve the opportunity of a less-explored peer still connecting
                    // or queued. Dropping this older transport consumes no new opportunity.
                    continue;
                }
                let corroborates = continuity.startup_deadline.is_some()
                    && upstream.window.max_piece.max(0) as u64 >= continuity.head;
                if corroborates
                    && peers.len() >= live_recovery.max_active_upstreams
                    && upstream.window.max_piece.max(0) as u64 == continuity.head
                {
                    // Independent matching evidence is enough to start. Keep the sole
                    // healthy provisional transport rather than churn it for capacity.
                    continuity.startup_deadline = None;
                    last_progress = Instant::now();
                    crate::alog!("[ace] live start: matching independent window confirmed; retaining current upstream");
                    let lost =
                        advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                    record_pool_losses(candidates, &mut loss_count, lost);
                    continue;
                }
                let replacement = if peers.len() >= live_recovery.max_active_upstreams {
                    if !corroborates {
                        continue;
                    }
                    peers
                        .iter()
                        .min_by_key(|(_, peer)| peer.max_piece)
                        .map(|(&id, _)| id)
                } else {
                    None
                };
                candidates.learn(upstream.addr, CandidateKind::Discovered);
                let peer_id = next_peer_id;
                next_peer_id += 1;
                let addr = upstream.addr;
                continuity.head = continuity.head.max(upstream.window.max_piece.max(0) as u64);
                continuity.skip_far_behind_live(Instant::now());
                match activate_upstream_peer(
                    peer_id,
                    upstream,
                    continuity.reasm.next_needed(),
                    identity,
                    continuity,
                    &event_tx,
                    reachability,
                    replacement.map(|id| (&mut peers, id)),
                )
                .await
                {
                    Ok((runtime, replaced)) => {
                        if let Some(replaced) = replaced {
                            // Already admitted once: no extra exploration or physical-loss
                            // event for retiring a provisional, nonproducing transport.
                            candidates.failed(replaced, Instant::now());
                        }
                        if corroborates {
                            continuity.startup_deadline = None;
                            last_progress = Instant::now();
                            crate::alog!("[ace] live start: independent upstream window confirmed; starting at best known head");
                        }
                        candidates.admitted(runtime.addr);
                        crate::alog!(
                            "[ace] {addr}: added to active upstream pool ({} peer(s))",
                            peers.len() + 1
                        );
                        peers.insert(peer_id, runtime);
                        peer_count.store(peers.len() as u32, Ordering::Relaxed);
                        let newly_lost =
                            advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                        record_pool_losses(candidates, &mut loss_count, newly_lost);
                    }
                    Err(addr) => {
                        candidates.explored(addr);
                        record_pool_losses(candidates, &mut loss_count, [addr]);
                    }
                }
                continue;
            }
            PoolWake::Refill(None) => {
                refill_closed = true;
                continue;
            }
        };

        let (peer_id, addr, msg) = match event {
            PeerEvent::Lost { peer_id, addr } => {
                let lost_producer = peers.get(&peer_id).is_some_and(|peer| peer.produced_output);
                if let Some(lost) = drop_peer_runtime(peer_id, &mut peers, continuity) {
                    crate::alog!("[ace] {addr}: upstream peer lost");
                    record_pool_losses(candidates, &mut loss_count, [lost]);
                    let newly_lost =
                        advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                    record_pool_losses(candidates, &mut loss_count, newly_lost);
                    peer_count.store(peers.len() as u32, Ordering::Relaxed);
                }
                if lost_producer && !peers.values().any(|peer| peer.produced_output) {
                    let cursor = continuity.reasm.next_needed();
                    let stale_sources: Vec<_> = peers
                        .iter()
                        .filter_map(|(&id, peer)| {
                            (peer.max_piece < cursor
                                && candidates.kind(peer.addr) == CandidateKind::Source)
                                .then_some(id)
                        })
                        .collect();
                    for id in stale_sources {
                        if let Some(source) = drop_peer_runtime(id, &mut peers, continuity) {
                            // This transport still works but its old window cannot cover
                            // the cursor. Refresh current source priority after the last
                            // producer is lost; retain all usable fallback transports.
                            candidates.refresh_stale_transport(source, Instant::now());
                            crate::alog!(
                                "[ace] refreshing stale source window after producer loss"
                            );
                        }
                    }
                    peer_count.store(peers.len() as u32, Ordering::Relaxed);
                }
                if peers.is_empty() {
                    return FollowEnd::PeerLost(loss_count);
                }
                continue;
            }
            PeerEvent::Message { peer_id, addr, msg } => (peer_id, addr, msg),
        };

        let mut made_activity = false;
        let mut made_output = false;
        match msg {
            PeerMessage::Unchoke => {
                continuity.set_peer_unchoked(peer_id, true);
                crate::alog!(
                    "[ace] {addr}: UNCHOKE -> scheduling from piece {} toward head {}",
                    continuity.reasm.next_needed(),
                    continuity.head
                );
                let newly_lost =
                    advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                record_pool_losses(candidates, &mut loss_count, newly_lost);
                made_activity = true;
            }
            PeerMessage::Choke => {
                continuity.set_peer_unchoked(peer_id, false);
            }
            PeerMessage::Have(p) => {
                let old_head = continuity.head;
                continuity.head = continuity.head.max(p as u64);
                update_runtime_window(&mut peers, continuity, peer_id, p as u64);
                made_activity |= continuity.head > old_head;
                let newly_lost =
                    advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                record_pool_losses(candidates, &mut loss_count, newly_lost);
            }
            PeerMessage::Extended { ref payload, .. } => {
                if let Some(new_head) = advance_head_from_window(payload, continuity.head) {
                    continuity.head = new_head;
                    update_runtime_window(&mut peers, continuity, peer_id, new_head);
                    made_activity = true;
                    let newly_lost =
                        advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                    record_pool_losses(candidates, &mut loss_count, newly_lost);
                }
            }
            m @ PeerMessage::Piece { .. } => {
                if let Some(lc) = LiveChunk::from_message(&m) {
                    continuity.skip_far_behind_live(Instant::now());
                    let piece = lc.piece as u64;
                    if continuity.startup_deadline.is_some()
                        || piece < continuity.reasm.next_needed()
                    {
                        continue;
                    }
                    PieceStore::shared_put_chunk_with_header(
                        store,
                        piece,
                        lc.chunk,
                        lc.piece_header,
                        &lc.data,
                    )
                    .await;
                    let begin = lc.chunk as u64 * info.chunk_length;
                    if let Err(e) = continuity.reasm.add_block(lc.piece as u64, begin, &lc.data) {
                        // A malformed block, or a completed piece whose live-source signature
                        // didn't verify (#10): drop it rather than emit unauthenticated bytes.
                        // The piece stays incomplete, so it's re-requested from the pool.
                        crate::alog!("[ace] {addr}: dropped piece {piece} block: {e:?}");
                        continuity.release_rejected_piece(piece);
                        let newly_lost =
                            advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                        record_pool_losses(candidates, &mut loss_count, newly_lost);
                        continue;
                    }
                    continuity
                        .piece_producers
                        .entry(piece)
                        .and_modify(|producer| {
                            if *producer != Some(addr) {
                                *producer = None;
                            }
                        })
                        .or_insert(Some(addr));
                    continuity.note_chunk(piece, lc.chunk, chunks_per_piece);
                    made_activity = true;
                    let before = continuity.reasm.next_needed();
                    let ready = continuity.reasm.take_ready();
                    let after = continuity.reasm.next_needed();
                    let productive: Vec<_> = continuity
                        .piece_producers
                        .range(before..after)
                        .filter_map(|(_, producer)| *producer)
                        .collect();
                    continuity
                        .piece_producers
                        .retain(|&piece, _| piece >= after);
                    if !ready.is_empty() {
                        if !continuity.authenticated_logged
                            && info.sig_len > 0
                            && ace_wire::live_auth::signature_len_from_pubkey_der(
                                &info.source_pubkey,
                            ) == Some(info.sig_len)
                        {
                            continuity.authenticated_logged = true;
                            crate::alog!("[ace] discovery stage=authenticated-contiguous");
                        }
                        for output in continuity.resync_output(&ready) {
                            let aligned = output.bytes;
                            if continuity.emitted == 0 {
                                crate::alog!("[ace] discovery stage=first-source-output");
                            }
                            continuity.emitted += aligned.len() as u64;
                            if continuity.emitted >= continuity.next_log {
                                crate::alog!(
                                    "[ace] {addr}: served {} MiB (head={}, next piece needed={})",
                                    continuity.emitted >> 20,
                                    continuity.head,
                                    continuity.reasm.next_needed()
                                );
                                continuity.next_log = continuity.emitted + (4 << 20);
                            }
                            let len = aligned.len() as u64;
                            if tx
                                .send(LiveOutput {
                                    bytes: aligned,
                                    discontinuity: output.discontinuity,
                                })
                                .await
                                .is_err()
                            {
                                shutdown_peer_runtimes(&mut peers);
                                return FollowEnd::ConsumerGone;
                            }
                            downloaded.fetch_add(len, Ordering::Relaxed);
                            made_output = true;
                        }
                    }
                    if made_output
                        && info.sig_len > 0
                        && ace_wire::live_auth::signature_len_from_pubkey_der(&info.source_pubkey)
                            == Some(info.sig_len)
                    {
                        for producer in productive {
                            seed.warm_peers.record_productive(
                                info.infohash,
                                producer,
                                candidates.kind(producer),
                            );
                        }
                    }
                    let newly_lost =
                        advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                    record_pool_losses(candidates, &mut loss_count, newly_lost);
                }
            }
            PeerMessage::Interested => {
                let should_unchoke = if seed.enabled {
                    peers
                        .get_mut(&peer_id)
                        .map(|peer| {
                            let first = !peer.unchoked_peer;
                            peer.unchoked_peer = true;
                            first
                        })
                        .unwrap_or(false)
                } else {
                    false
                };
                if should_unchoke {
                    if let Some(lost) =
                        send_peer_command(peer_id, PeerMessage::Unchoke, &mut peers, continuity)
                            .await
                    {
                        record_pool_losses(candidates, &mut loss_count, [lost]);
                    }
                }
            }
            PeerMessage::Unknown { id: 6, ref payload } if seed.enabled && payload.len() >= 10 => {
                // payload: [stream u32 @0..4][piece u32 @4..8][chunk u16 @8..10]
                let p = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
                let c = u16::from_be_bytes([payload[8], payload[9]]);
                if let Some((data, header)) = PieceStore::shared_chunk(store, p as u64, c).await {
                    let len = data.len();
                    if let Some(lost) = send_peer_command(
                        peer_id,
                        build_piece(0, p, c, header, &data),
                        &mut peers,
                        continuity,
                    )
                    .await
                    {
                        record_pool_losses(candidates, &mut loss_count, [lost]);
                    } else {
                        uploaded.fetch_add(len as u64, Ordering::Relaxed);
                        peers_served.store(peers.len() as u32, Ordering::Relaxed);
                    }
                }
            }
            PeerMessage::Unknown { id: 4, ref payload } if payload.len() == 8 => {
                let piece =
                    u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]) as u64;
                let window_advanced = peers
                    .get(&peer_id)
                    .is_some_and(|peer| piece > peer.max_piece);
                if piece > continuity.head || window_advanced {
                    // Another upstream can advertise the shared head first. Its gossip
                    // must not suppress this producer's independently advancing window.
                    continuity.head = continuity.head.max(piece);
                    update_runtime_window(&mut peers, continuity, peer_id, piece);
                    made_activity = true;
                    let newly_lost =
                        advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                    record_pool_losses(candidates, &mut loss_count, newly_lost);
                }
            }
            PeerMessage::Unknown { id: 10, .. } => {}
            PeerMessage::Unknown {
                id: 12,
                ref payload,
            } => {
                let advertised = ace_wire::peer_exchange::parse_peer_exchange_detailed(payload);
                let ranked = ace_wire::peer_exchange::rank_by_window_coverage(
                    &advertised,
                    continuity.reasm.next_needed(),
                );
                for addr in ranked {
                    candidates.learn(addr, CandidateKind::Pex);
                }
                if peers.len() < live_recovery.max_active_upstreams
                    || continuity.startup_deadline.is_some()
                {
                    spawn_candidate_connects(
                        candidates,
                        &peers,
                        info.infohash,
                        live_recovery.max_parallel_connect,
                        &pex_tx,
                        &mut learned_connects,
                        &mut learned_pending,
                        true,
                    );
                }
            }
            PeerMessage::Unknown {
                id: 36,
                ref payload,
            } => {
                if let Some(source) = ace_wire::peer_exchange::parse_peer_announce(payload) {
                    candidates.learn(source, CandidateKind::Source);
                    crate::alog!("[ace] source-node announce from {addr}: retained {source}");
                    if peers.len() < live_recovery.max_active_upstreams
                        || continuity.startup_deadline.is_some()
                    {
                        spawn_candidate_connects(
                            candidates,
                            &peers,
                            info.infohash,
                            live_recovery.max_parallel_connect,
                            &pex_tx,
                            &mut learned_connects,
                            &mut learned_pending,
                            true,
                        );
                    }
                }
            }
            // Peer telemetry we don't act on: id=11 bencode stats, id=13 keepalive, id=34
            // counter. Explicit no-ops so they don't spam the "unhandled msg" log.
            PeerMessage::Unknown {
                id: 11 | 13 | 34, ..
            } => {}
            PeerMessage::Unknown { id, ref payload } => {
                if let Some(new_head) = advance_head_from_window(payload, continuity.head) {
                    crate::alog!(
                        "[ace] {addr}: live window update (msg id={id}) head {} -> {new_head}",
                        continuity.head
                    );
                    continuity.head = new_head;
                    update_runtime_window(&mut peers, continuity, peer_id, new_head);
                    made_activity = true;
                    let newly_lost =
                        advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                    record_pool_losses(candidates, &mut loss_count, newly_lost);
                } else {
                    let should_log = peers
                        .get_mut(&peer_id)
                        .map(|peer| peer.seen_ids.insert(id))
                        .unwrap_or(false);
                    if should_log {
                        crate::alog!(
                            "[ace] {addr}: unhandled msg id={id} ({} bytes) {}",
                            payload.len(),
                            hex_preview(payload)
                        );
                    }
                }
            }
            _ => {}
        }
        if peers.is_empty() {
            return FollowEnd::PeerLost(loss_count);
        }
        if should_refresh_stale_deadline(continuity.emitted, made_output, made_activity) {
            last_progress = Instant::now();
        }
        if made_output {
            if let Some(peer) = peers.get_mut(&peer_id) {
                peer.produced_output = true;
            }
            candidates.productive(addr);
            // Contiguous output means the playback cursor advanced: reset the per-piece
            // eviction-skip timer so it only fires when the cursor is genuinely stuck.
            continuity.next_needed_since = Instant::now();
        }
    }
}

fn record_pool_losses(
    candidates: &mut SessionCandidates,
    loss_count: &mut usize,
    addrs: impl IntoIterator<Item = SocketAddrV4>,
) {
    for addr in addrs {
        candidates.failed(addr, Instant::now());
        *loss_count = loss_count.saturating_add(1);
    }
}

fn update_runtime_window(
    peers: &mut BTreeMap<u64, PeerRuntime>,
    continuity: &mut Continuity,
    peer_id: u64,
    max_piece: u64,
) {
    if let Some(peer) = peers.get_mut(&peer_id) {
        peer.max_piece = peer.max_piece.max(max_piece);
        continuity.update_peer_window(peer_id, peer.min_piece, peer.max_piece);
    }
}

async fn send_peer_command(
    peer_id: u64,
    msg: PeerMessage,
    peers: &mut BTreeMap<u64, PeerRuntime>,
    continuity: &mut Continuity,
) -> Option<SocketAddrV4> {
    let sender = peers.get(&peer_id).map(|peer| peer.commands.clone())?;
    if sender.send(PeerCommand::Send(msg)).await.is_err() {
        drop_peer_runtime(peer_id, peers, continuity)
    } else {
        None
    }
}

#[cfg(test)]
fn spawn_learned_connects(
    candidates: &mut SessionCandidates,
    peers: &BTreeMap<u64, PeerRuntime>,
    infohash: [u8; 20],
    max_parallel: usize,
    tx: &mpsc::Sender<PoolRefill>,
    tasks: &mut tokio::task::JoinSet<(SocketAddrV4, Option<PeerConnectFailure>, bool)>,
    pending: &mut HashSet<SocketAddrV4>,
) {
    spawn_candidate_connects(
        candidates,
        peers,
        infohash,
        max_parallel,
        tx,
        tasks,
        pending,
        false,
    );
}

fn finish_candidate_connect(
    (addr, failure, queued): (SocketAddrV4, Option<PeerConnectFailure>, bool),
    pending: &mut HashSet<SocketAddrV4>,
    candidates: &mut SessionCandidates,
) -> bool {
    if !queued {
        pending.remove(&addr);
    }
    if let Some(failure) = failure {
        candidates.explored(failure.addr);
        candidates.failed(failure.addr, Instant::now());
        return true;
    }
    false
}

#[allow(clippy::too_many_arguments)]
fn spawn_candidate_connects(
    candidates: &mut SessionCandidates,
    peers: &BTreeMap<u64, PeerRuntime>,
    infohash: [u8; 20],
    max_parallel: usize,
    tx: &mpsc::Sender<PoolRefill>,
    tasks: &mut tokio::task::JoinSet<(SocketAddrV4, Option<PeerConnectFailure>, bool)>,
    pending: &mut HashSet<SocketAddrV4>,
    include_discovered: bool,
) {
    let (sources, pex) = candidates.learned_counts();
    if !include_discovered && sources + pex == 0 {
        return;
    }
    let active = peers.values().map(|peer| peer.addr).collect();
    let cohort = candidates.exploration_cohort(Instant::now(), &active, pending);
    let eligible = if include_discovered {
        candidates.eligible(Instant::now())
    } else {
        candidates.eligible_learned(Instant::now())
    };
    for addr in eligible {
        if Some(candidates.explorations(addr)) != cohort {
            continue;
        }
        if tasks.len() >= max_parallel || pending.len() >= max_parallel {
            break;
        }
        if peers.values().any(|p| p.addr == addr) || !pending.insert(addr) {
            continue;
        }
        // Reserve a retry slot before spawning: gossip floods cannot duplicate pending work.
        candidates.attempting(addr, Instant::now() + CONNECT_TIMEOUT);
        let discovered = candidates.kind(addr) == CandidateKind::Discovered;
        let tx = tx.clone();
        tasks.spawn(async move {
            let (failure, queued) = match connect_upstream(addr, infohash).await {
                PeerConnectAttempt::Connected(upstream) => {
                    let receipt = if discovered {
                        PoolRefill::ReservedDiscovered(upstream)
                    } else {
                        PoolRefill::Learned(upstream)
                    };
                    (None, tx.try_send(receipt).is_ok())
                }
                PeerConnectAttempt::Failed(failure) => (Some(failure), false),
            };
            // Successful queued peers remain pending until activation/rejection; neither
            // cooldown expiry nor gossip can race an older cohort into their capacity.
            (addr, failure, queued)
        });
    }
}

// Stale peers are gossip transports only. One deadline covers the whole batch,
// including our handshake writes; no Interested, media requests or continuity updates.
async fn harvest_stale_gossip(
    upstreams: Vec<ConnectedUpstream>,
    identity: &Identity,
    candidates: &mut SessionCandidates,
) {
    harvest_stale_gossip_with_budget(upstreams, identity, candidates, STALE_GOSSIP_BUDGET).await;
}

async fn harvest_stale_gossip_with_budget(
    upstreams: Vec<ConnectedUpstream>,
    identity: &Identity,
    candidates: &mut SessionCandidates,
    budget: Duration,
) {
    let deadline = tokio::time::Instant::now() + budget;
    let mut tasks = tokio::task::JoinSet::new();
    for mut upstream in upstreams {
        let hs = OutgoingExtendedHandshake {
            ace_metadata_version: 1,
            ut_metadata_id: 2,
            mi: None,
            node: NodeFields::default(),
            peer_ip: Some(upstream.addr.ip().octets()),
            metadata_size: None,
        };
        let handshake = PeerMessage::Extended {
            ext_id: 0,
            payload: hs.sign_and_encode(identity),
        };
        tasks.spawn(async move {
            let mut learned = SessionCandidates::default();
            let _ = tokio::time::timeout_at(deadline, async {
                if upstream.session.send(&handshake).await.is_err() {
                    return;
                }
                for _ in 0..32 {
                    match upstream.session.read_message().await {
                        Ok(PeerMessage::Unknown { id: 12, payload }) => {
                            for addr in ace_wire::peer_exchange::parse_peer_exchange(&payload) {
                                learned.learn(addr, CandidateKind::Pex);
                            }
                        }
                        Ok(PeerMessage::Unknown { id: 36, payload }) => {
                            if let Some(addr) =
                                ace_wire::peer_exchange::parse_peer_announce(&payload)
                            {
                                learned.learn(addr, CandidateKind::Source);
                            }
                        }
                        Ok(_) => {}
                        Err(_) => return,
                    }
                }
            })
            .await;
            learned.into_learned()
        });
    }
    while let Some(joined) = tasks.join_next().await {
        if let Ok(learned) = joined {
            for (addr, kind) in learned {
                candidates.learn(addr, kind);
            }
        }
    }
}

async fn advance_pool_requests(
    peers: &mut BTreeMap<u64, PeerRuntime>,
    continuity: &mut Continuity,
    chunks_per_piece: u16,
) -> Vec<SocketAddrV4> {
    continuity.skip_far_behind_live(Instant::now());
    if continuity.startup_deadline.is_some() {
        return Vec::new();
    }
    let assignments = schedule_piece_assignments(
        &mut continuity.scheduler,
        &mut continuity.active_peers,
        continuity.reasm.next_needed(),
        continuity.head,
    );
    // Start (or keep) a retransmit timer for each freshly-assigned piece.
    let now = Instant::now();
    for assignment in &assignments {
        continuity
            .requested_at
            .entry(assignment.piece)
            .or_insert(now);
    }
    let mut failed = Vec::new();
    for assignment in assignments {
        let Some(sender) = peers
            .get(&assignment.peer_id)
            .map(|peer| peer.commands.clone())
        else {
            continuity.scheduler.on_drop(assignment.piece);
            continue;
        };
        if sender
            .send(PeerCommand::RequestPiece {
                piece: assignment.piece,
                chunks_per_piece,
            })
            .await
            .is_err()
        {
            failed.push(assignment.peer_id);
        }
    }
    failed.sort_unstable();
    failed.dedup();
    failed
        .into_iter()
        .filter_map(|peer_id| drop_peer_runtime(peer_id, peers, continuity))
        .collect()
}

/// Periodic self-heal for the request pipeline (runs on each pool loop tick): re-requeue any
/// piece outstanding past the configured request timeout and skip a piece evicted from every
/// upstream window, then re-issue requests. A timed-out piece is only requeued in the *scheduler* (its
/// original peer keeps the in-flight slot), so [`ActivePeers::assign`] steers the retry to a
/// peer with more spare capacity — i.e. a different, faster one when available. If no retry
/// was assigned, release the old slots for that piece so full capacity cannot prevent retrying.
/// Returns any peers dropped while re-issuing requests.
async fn retransmit_stalled_requests(
    peers: &mut BTreeMap<u64, PeerRuntime>,
    continuity: &mut Continuity,
    chunks_per_piece: u16,
    now: Instant,
) -> Vec<SocketAddrV4> {
    let mut changed = false;
    let head = continuity.head;
    if let Some(target) = continuity.skip_far_behind_live(now) {
        crate::alog!(
            "[ace] cursor fell too far behind live edge {head}; re-syncing forward to piece {target}"
        );
        changed = true;
    }
    if let Some(floor) = continuity.skip_evicted_gap(now) {
        crate::alog!(
            "[ace] next needed piece evicted from all upstream windows; skipping ahead to {floor}"
        );
        changed = true;
    }
    let timed_out = continuity.timed_out_requests(now);
    if !timed_out.is_empty() {
        let request_timeout = continuity.live_recovery.request_timeout();
        crate::alog!(
            "[ace] re-requesting {} piece(s) outstanding > {:?} (from {})",
            timed_out.len(),
            request_timeout,
            continuity.reasm.next_needed()
        );
        for &piece in &timed_out {
            continuity.scheduler.on_drop(piece);
            continuity.requested_at.remove(&piece);
        }
        changed = true;
    }
    if changed {
        let mut lost = advance_pool_requests(peers, continuity, chunks_per_piece).await;
        let mut released = false;
        for piece in timed_out {
            if !continuity.requested_at.contains_key(&piece) {
                continuity.active_peers.complete_everywhere(piece);
                released = true;
            }
        }
        if released {
            lost.extend(advance_pool_requests(peers, continuity, chunks_per_piece).await);
        }
        lost
    } else {
        Vec::new()
    }
}

fn drop_peer_runtime(
    peer_id: u64,
    peers: &mut BTreeMap<u64, PeerRuntime>,
    continuity: &mut Continuity,
) -> Option<SocketAddrV4> {
    let peer = peers.remove(&peer_id)?;
    for piece in continuity.active_peers.remove(peer_id) {
        continuity.scheduler.on_drop(piece);
    }
    let _ = peer.commands.try_send(PeerCommand::Stop);
    peer.worker.abort();
    Some(peer.addr)
}

fn shutdown_peer_runtimes(peers: &mut BTreeMap<u64, PeerRuntime>) {
    for (_, peer) in std::mem::take(peers) {
        let _ = peer.commands.try_send(PeerCommand::Stop);
        peer.worker.abort();
    }
}

#[allow(dead_code)]
#[allow(clippy::too_many_arguments)]
async fn follow_one_peer(
    session: &mut PeerSession<TcpStream>,
    info: &StreamInfo,
    identity: &Identity,
    addr: SocketAddrV4,
    window: LivePosition,
    chunks_per_piece: u16,
    tx: &mpsc::Sender<Bytes>,
    downloaded: &Arc<AtomicU64>,
    uploaded: &Arc<AtomicU64>,
    peers_served: &Arc<AtomicU32>,
    seed: &SeedConfig,
    continuity: &mut Option<Continuity>,
) -> FollowEnd {
    // 1. Use the peer's advertised live window, already read during upstream selection.
    let min_piece = window.min_piece.max(0) as u64;
    let max_piece = window.max_piece.max(0) as u64;
    let start = match continuity {
        None => {
            let (c, start) = Continuity::fresh(
                info,
                min_piece,
                max_piece,
                seed.prefetch_pieces,
                seed.live_recovery,
            );
            *continuity = Some(c);
            crate::alog!("[ace] {addr}: window min={min_piece} max={max_piece} -> start={start} head={max_piece}");
            start
        }
        Some(c) => {
            let start = c.resume(addr, min_piece, max_piece);
            crate::alog!(
                "[ace] {addr}: reconnected; window min={min_piece} max={max_piece} -> resuming from {start} head={}",
                c.head
            );
            start
        }
    };
    let continuity = continuity.as_mut().expect("initialized just above");
    continuity.register_active_peer(SINGLE_PEER_ID, addr, window);

    // 2. Advertise our matching position + interest.
    let hs = OutgoingExtendedHandshake {
        ace_metadata_version: 1,
        ut_metadata_id: 2,
        mi: Some(LivePosition {
            min_piece: start as i64,
            max_piece: continuity.head as i64,
            position: -1,
            distance_from_source: 1,
        }),
        node: NodeFields {
            ts: 5000,
            ..NodeFields::default()
        },
        peer_ip: Some(addr.ip().octets()),
        metadata_size: None,
    };
    if session
        .send_signed_extended_handshake(&hs, identity)
        .await
        .is_err()
        || session.send(&PeerMessage::Interested).await.is_err()
    {
        return FollowEnd::PeerLost(1);
    }

    let mut unchoked = false;
    let peer_min = min_piece;
    let mut peer_max = max_piece;
    let store = seed.registry.get_or_create(info.infohash, || {
        build_piece_store(
            info.piece_length,
            info.chunk_length,
            seed.store_bytes,
            seed.store_retention,
            seed.cache_type,
            &seed.cache_dir,
            &info.infohash,
        )
    });
    let mut unchoked_peer = false;
    let mut last_progress = Instant::now();
    // Diagnostic: surface each unmodelled Acestream message id once (note 22 follow-up).
    let mut seen_ids: HashSet<u8> = HashSet::new();

    loop {
        let Some(read_budget) = stale_upstream_budget(
            last_progress,
            Instant::now(),
            seed.live_recovery.stale_upstream_timeout(),
        ) else {
            crate::alog!(
                "[ace] {addr}: stale upstream — no live progress for {:?}; reconnecting",
                seed.live_recovery.stale_upstream_timeout()
            );
            return FollowEnd::PeerLost(1);
        };
        let msg = match tokio::time::timeout(read_budget, session.read_message()).await {
            Ok(Ok(m)) => m,
            Ok(Err(_)) => return FollowEnd::PeerLost(1),
            Err(_) => {
                crate::alog!(
                    "[ace] {addr}: stale upstream — no live progress for {:?}; reconnecting",
                    seed.live_recovery.stale_upstream_timeout()
                );
                return FollowEnd::PeerLost(1);
            }
        };
        let mut made_activity = false;
        let mut made_output = false;
        match msg {
            PeerMessage::Unchoke => {
                unchoked = true;
                continuity.set_peer_unchoked(SINGLE_PEER_ID, true);
                crate::alog!(
                    "[ace] {addr}: UNCHOKE -> scheduling from piece {} toward head {}",
                    continuity.reasm.next_needed(),
                    continuity.head
                );
                if advance_requests(session, continuity, chunks_per_piece)
                    .await
                    .is_err()
                {
                    return FollowEnd::PeerLost(1);
                }
                made_activity = true;
            }
            PeerMessage::Choke => {
                unchoked = false;
                continuity.set_peer_unchoked(SINGLE_PEER_ID, false);
            }
            PeerMessage::Have(p) => {
                let old_head = continuity.head;
                continuity.head = continuity.head.max(p as u64);
                peer_max = peer_max.max(p as u64);
                continuity.update_peer_window(SINGLE_PEER_ID, peer_min, peer_max);
                made_activity |= continuity.head > old_head;
                if unchoked
                    && advance_requests(session, continuity, chunks_per_piece)
                        .await
                        .is_err()
                {
                    return FollowEnd::PeerLost(1);
                }
            }
            // The live edge advances via a periodic `myinfo` window update (engine symbol
            // `got_myinfo`), NOT a standard `Have` — see note 22. Depending on the peer it
            // arrives as a re-sent extended handshake (ext_id 0) or a custom Acestream
            // message id. Recognize it by content (a bencode window dict carrying
            // `max_piece`) regardless of carrier, advance the head, and request the newly
            // available pieces.
            PeerMessage::Extended { ref payload, .. } => {
                if let Some(new_head) = advance_head_from_window(payload, continuity.head) {
                    continuity.head = new_head;
                    peer_max = peer_max.max(new_head);
                    continuity.update_peer_window(SINGLE_PEER_ID, peer_min, peer_max);
                    made_activity = true;
                    if unchoked
                        && advance_requests(session, continuity, chunks_per_piece)
                            .await
                            .is_err()
                    {
                        return FollowEnd::PeerLost(1);
                    }
                }
            }
            m @ PeerMessage::Piece { .. } => {
                if let Some(lc) = LiveChunk::from_message(&m) {
                    let piece = lc.piece as u64;
                    PieceStore::shared_put_chunk_with_header(
                        &store,
                        piece,
                        lc.chunk,
                        lc.piece_header,
                        &lc.data,
                    )
                    .await;
                    let begin = lc.chunk as u64 * info.chunk_length;
                    if let Err(e) = continuity.reasm.add_block(lc.piece as u64, begin, &lc.data) {
                        // A malformed block, or a completed piece whose live-source signature
                        // didn't verify (#10): drop it rather than emit unauthenticated bytes.
                        // The piece stays incomplete, so it's re-requested from the pool.
                        crate::alog!("[ace] {addr}: dropped piece {piece} block: {e:?}");
                        continuity.release_rejected_piece(piece);
                        if unchoked
                            && advance_requests(session, continuity, chunks_per_piece)
                                .await
                                .is_err()
                        {
                            return FollowEnd::PeerLost(1);
                        }
                        continue;
                    }
                    continuity.note_chunk(piece, lc.chunk, chunks_per_piece);
                    made_activity = true;
                    let ready = continuity.reasm.take_ready();
                    if !ready.is_empty() {
                        for output in continuity.resync_output(&ready) {
                            let aligned = output.bytes;
                            continuity.emitted += aligned.len() as u64;
                            if continuity.emitted >= continuity.next_log {
                                crate::alog!(
                                    "[ace] {addr}: served {} MiB (head={}, next piece needed={})",
                                    continuity.emitted >> 20,
                                    continuity.head,
                                    continuity.reasm.next_needed()
                                );
                                continuity.next_log = continuity.emitted + (4 << 20);
                            }
                            let len = aligned.len() as u64;
                            if tx.send(aligned).await.is_err() {
                                return FollowEnd::ConsumerGone;
                            }
                            downloaded.fetch_add(len, Ordering::Relaxed);
                            made_output = true;
                        }
                    }
                    if unchoked
                        && advance_requests(session, continuity, chunks_per_piece)
                            .await
                            .is_err()
                    {
                        return FollowEnd::PeerLost(1);
                    }
                }
            }
            PeerMessage::Interested => {
                if seed.enabled && !unchoked_peer {
                    let _ = session.send(&PeerMessage::Unchoke).await;
                    unchoked_peer = true;
                }
            }
            PeerMessage::Unknown { id: 6, ref payload } if seed.enabled && payload.len() >= 10 => {
                // payload: [stream u32 @0..4][piece u32 @4..8][chunk u16 @8..10]
                let p = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
                let c = u16::from_be_bytes([payload[8], payload[9]]);
                if let Some((data, header)) = PieceStore::shared_chunk(&store, p as u64, c).await {
                    if session
                        .send(&build_piece(0, p, c, header, &data))
                        .await
                        .is_ok()
                    {
                        uploaded.fetch_add(data.len() as u64, Ordering::Relaxed);
                        peers_served.store(1, Ordering::Relaxed); // single-peer follow; multi-peer aggregation is S2
                    }
                }
            }
            // Acestream live HAVE (note 22 capture): an 8-byte `[u32 stream=0][u32 piece]`.
            // This is the live-edge advancement signal — the engine announces each new piece
            // at the head with id=4 (NOT the standard 4-byte BT HAVE, which it never sends),
            // and the advancing trailing edge / eviction pointer with id=10. Advance the head
            // on id=4 and request the newly-available pieces.
            PeerMessage::Unknown { id: 4, ref payload } if payload.len() == 8 => {
                let piece =
                    u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]) as u64;
                if piece > continuity.head {
                    continuity.head = piece;
                    peer_max = peer_max.max(piece);
                    continuity.update_peer_window(SINGLE_PEER_ID, peer_min, peer_max);
                    made_activity = true;
                    if unchoked
                        && advance_requests(session, continuity, chunks_per_piece)
                            .await
                            .is_err()
                    {
                        return FollowEnd::PeerLost(1);
                    }
                }
            }
            // Trailing edge (oldest still-available piece). Informational: if it ever passes
            // what we still need, we've fallen irrecoverably behind this peer's window.
            PeerMessage::Unknown { id: 10, .. } => {}
            // Any other Acestream-custom message: it may be a `myinfo` window update carried
            // as a bencode dict (note 22). Recognize it by content and advance; if it isn't a
            // window update, log the id once so a live run reveals carriers we don't decode.
            PeerMessage::Unknown { id, ref payload } => {
                if let Some(new_head) = advance_head_from_window(payload, continuity.head) {
                    crate::alog!(
                        "[ace] {addr}: live window update (msg id={id}) head {} -> {new_head}",
                        continuity.head
                    );
                    continuity.head = new_head;
                    peer_max = peer_max.max(new_head);
                    continuity.update_peer_window(SINGLE_PEER_ID, peer_min, peer_max);
                    made_activity = true;
                    if unchoked
                        && advance_requests(session, continuity, chunks_per_piece)
                            .await
                            .is_err()
                    {
                        return FollowEnd::PeerLost(1);
                    }
                } else if seen_ids.insert(id) {
                    crate::alog!(
                        "[ace] {addr}: unhandled msg id={id} ({} bytes) {}",
                        payload.len(),
                        hex_preview(payload)
                    );
                }
            }
            _ => {}
        }
        if should_refresh_stale_deadline(continuity.emitted, made_output, made_activity) {
            last_progress = Instant::now();
        }
    }
}

/// If `payload` carries a live-window (`myinfo`) update whose head is beyond `head`, return
/// the new head; otherwise `None`. The single place window recognition feeds the loop.
fn advance_head_from_window(payload: &[u8], head: u64) -> Option<u64> {
    let w = LiveWindow::from_myinfo_payload(payload)?;
    (w.max_piece > head && w.max_piece <= u32::MAX as u64).then_some(w.max_piece)
}

fn stale_upstream_budget(
    last_progress: Instant,
    now: Instant,
    stale_upstream_timeout: Duration,
) -> Option<Duration> {
    let elapsed = now.saturating_duration_since(last_progress);
    if elapsed >= stale_upstream_timeout {
        None
    } else {
        Some(stale_upstream_timeout - elapsed)
    }
}

fn should_refresh_stale_deadline(emitted: u64, made_output: bool, made_activity: bool) -> bool {
    made_output || (emitted == 0 && made_activity)
}

fn schedule_piece_requests(
    scheduler: &mut Scheduler,
    active_peers: &mut ActivePeers,
    next_needed: u64,
    head: u64,
) -> Vec<u64> {
    schedule_piece_assignments(scheduler, active_peers, next_needed, head)
        .into_iter()
        .filter_map(|assignment| (assignment.peer_id == SINGLE_PEER_ID).then_some(assignment.piece))
        .collect()
}

fn schedule_piece_assignments(
    scheduler: &mut Scheduler,
    active_peers: &mut ActivePeers,
    next_needed: u64,
    head: u64,
) -> Vec<PeerAssignment> {
    active_peers.assign(scheduler, next_needed, head)
}

/// Fill the request pipeline from the stream cursor toward the known head, constrained by
/// this peer's advertised window. The scheduler owns in-flight bookkeeping; this function
/// only turns assigned pieces into Acestream chunk requests.
async fn advance_requests(
    session: &mut PeerSession<TcpStream>,
    continuity: &mut Continuity,
    chunks_per_piece: u16,
) -> ace_peer::Result<()> {
    let pieces = schedule_piece_requests(
        &mut continuity.scheduler,
        &mut continuity.active_peers,
        continuity.reasm.next_needed(),
        continuity.head,
    );
    for piece in pieces {
        for chunk in 0..chunks_per_piece {
            session.send(&chunk_request(piece as u32, chunk)).await?;
        }
    }
    Ok(())
}

/// Hex preview of a message prefix for diagnostics (avoids pulling in a hex crate).
fn hex_preview(bytes: &[u8]) -> String {
    bytes.iter().take(24).map(|b| format!("{b:02x}")).collect()
}

/// Read messages until the peer's extended handshake arrives; return its live `mi` window and
/// the `yourip` public address it echoed to us (issue #22 — the swarm telling us how it sees us
/// on this outbound connection; `None` when the peer sent no usable `yourip`).
async fn read_peer_window(
    session: &mut PeerSession<TcpStream>,
) -> Option<(LivePosition, Option<IpAddr>)> {
    for _ in 0..32 {
        let msg = session.read_message().await.ok()?;
        if let PeerMessage::Extended { ext_id: 0, payload } = msg {
            let eh = ExtendedHandshake::parse(&payload).ok()?;
            let yourip = eh.yourip();
            let mi = eh.raw.get(b"mi")?;
            let get = |k: &[u8]| mi.get(k).and_then(|v| v.as_int()).unwrap_or(-1);
            let min_piece = get(b"min_piece");
            let max_piece = get(b"max_piece");
            if min_piece < 0 || max_piece < min_piece || max_piece > u32::MAX as i64 {
                return None;
            }
            return Some((
                LivePosition {
                    min_piece,
                    max_piece,
                    position: get(b"position"),
                    distance_from_source: get(b"distance_from_source"),
                },
                yourip,
            ));
        }
    }
    None
}

/// Test-only builders shared by the provider and HTTP tests.
#[cfg(test)]
pub(crate) mod test_support {
    use ace_wire::bencode::Bencode;
    use std::collections::BTreeMap;

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

    #[tokio::test]
    async fn bare_infohash_without_a_verified_descriptor_fails_closed() {
        let p = test_provider();
        // No descriptor and no bootstrap peers: the refusal must come before any discovery, so
        // the timeout only trips if open() regressed into tracker/DHT I/O.
        let err = tokio::time::timeout(Duration::from_secs(2), p.open(PLACEHOLDER_ID))
            .await
            .expect("must fail before any network I/O")
            .err()
            .expect("a bare infohash with no verified descriptor must not stream");
        match err {
            ProviderError::Unresolvable(msg) => {
                assert!(msg.contains(&format!("cid:{PLACEHOLDER_ID}")), "{msg}");
            }
            other => panic!("expected Unresolvable, got {other:?}"),
        }
        assert!(matches!(
            p.check_openable(PLACEHOLDER_ID),
            Err(ProviderError::Unresolvable(_))
        ));
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
        assert!(p.check_openable(&ih).is_err());

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
            p.check_openable(PLACEHOLDER_ID),
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
            test_provider().check_openable(&id),
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
    async fn peer_resolved_content_id_does_not_make_its_infohash_openable() {
        let (transport, _) = test_support::synthetic_live_transport(524_288);
        let verified = ace_swarm::resolve::stream_info_from_transport(&transport).unwrap();
        let p = test_provider();
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
            p.check_openable(&ih),
            Err(ProviderError::Unresolvable(_))
        ));
        assert!(matches!(
            p.resolve_live_info(&ih).await,
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
