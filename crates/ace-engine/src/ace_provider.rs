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
use announce::announce_seeder_periodically;
pub use announce::{announce_infohash_periodically, announce_infohash_periodically_dynamic};
mod live;
#[cfg(test)]
use live::AceSource;
mod resolution;
mod vod;
use ace_swarm::live_session::{DiscoveryRun, LiveOutput, PeerDiscovery, WarmPeerCache};
use vod::AceVodContent;
#[cfg(test)]
use vod::VodSource;
#[cfg(test)]
mod resync_tests;

use crate::config::{CacheType, LiveRecoveryConfig, StartupBufferConfig};
use crate::provider::{
    ProviderError, SourceStats, StreamProvider, TsSource, VodByteSource, VodContent,
};
use crate::startup_buffer::StartupBufferedSource;
use ace_peer::session::connect;
#[cfg(test)]
use ace_peer::session::PeerSession;
use ace_swarm::dht::dht_announce_peer;
use ace_swarm::discover::{
    announce_seeder, discover_peers, discover_peers_incremental, DiscoveryOptions,
};
use ace_swarm::listen::{SeedLease, SeedRegistry};
use ace_swarm::reachability::ReachabilityMonitor;
use ace_swarm::resolve::{
    catalog_transport_bytes, hex20, infohash_hex, resolve_infohash_via_catalog,
    resolve_via_catalog, resolve_via_peer, stream_info_from_transport,
    stream_info_from_transport_url, transport_bytes_from_url, transport_bytes_via_peer,
    vod_info_from_transport, InfohashIndex, ResolveCache, ResolveError,
};
use ace_swarm::store::{BackendKind, PieceStore};
use ace_swarm::types::{StreamInfo, StreamMetadata, VodInfo};
use ace_swarm::vod::download_vod_pieces;
#[cfg(test)]
use ace_wire::extended::{LivePosition, NodeFields, OutgoingExtendedHandshake};
use ace_wire::handshake::random_peer_id;
use ace_wire::identity::Identity;
#[cfg(test)]
use ace_wire::live_codec::build_piece;
#[cfg(test)]
use ace_wire::message::PeerMessage;
#[cfg(test)]
use ace_wire::reassembly::PieceReassembler;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::VecDeque;
use std::net::SocketAddrV4;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
#[cfg(test)]
use tokio::net::TcpStream;
use tokio::sync::mpsc;

/// How many pieces behind the live edge to start, so we have buffer immediately.
const PREFETCH_PIECES: u64 = 8;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const UNKNOWN_BITRATE_BUFFER_PREFETCH_PIECES: u64 = 32;
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

impl AceProvider {
    fn store_factory(&self, info: &StreamInfo) -> ace_swarm::live_session::StoreFactory {
        let piece_length = info.piece_length;
        let chunk_length = info.chunk_length;
        let max_bytes = self.seed_store_bytes;
        let retention = self.seed_store_retention;
        let cache_type = self.cache_type;
        let cache_dir = self.cache_dir.clone();
        Arc::new(move |infohash| {
            build_piece_store(
                piece_length,
                chunk_length,
                max_bytes,
                retention,
                cache_type,
                &cache_dir,
                &infohash,
            )
        })
    }
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
mod tests;
