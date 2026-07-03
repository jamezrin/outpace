//! The `"ace"` provider: resolves an identifier to a [`StreamInfo`], discovers peers via
//! trackers, and follows the live edge from a peer, emitting contiguous MPEG-TS. Built on
//! the cracked live protocol (see `docs/protocol/notes/19.md`).
//!
//! LIVE-GATED: the peer I/O path requires the real Acestream swarm and is verified in the
//! operator's environment (it cannot run in CI/sandbox). Content-id → transport-file
//! resolution first uses the official signed catalog path, with BEP-9 `ut_metadata` as a
//! fallback (see [`ace_swarm::resolve`]); the infohash form works directly.

use crate::provider::{ProviderError, SourceStats, StreamProvider, TsSource};
use ace_peer::session::{connect, PeerSession};
use ace_swarm::dht::dht_announce_peer;
use ace_swarm::discover::{
    announce_seeder, discover_peers, discover_peers_with_options, DiscoveryOptions,
};
use ace_swarm::listen::SeedRegistry;
use ace_swarm::resolve::{
    hex20, resolve_via_catalog, resolve_via_peer, stream_info_from_infohash, ResolveCache,
    ResolveError,
};
use ace_swarm::scheduler::{ActivePeers, PeerAssignment, Scheduler};
use ace_swarm::store::PieceStore;
use ace_swarm::types::StreamInfo;
use ace_wire::extended::{ExtendedHandshake, LivePosition, NodeFields, OutgoingExtendedHandshake};
use ace_wire::handshake::random_peer_id;
use ace_wire::identity::Identity;
use ace_wire::live::LiveWindow;
use ace_wire::live_codec::{build_piece, chunk_request, LiveChunk};
use ace_wire::message::PeerMessage;
use ace_wire::reassembly::PieceReassembler;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddrV4;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

/// How many pieces behind the live edge to start, so we have buffer immediately.
const PREFETCH_PIECES: u64 = 8;
/// Upper bound on pieces requested in a single forward step. A live window is ~100 pieces
/// and advances ~1 piece per update, so this is only a guard: a malformed/garbled window
/// update can never trigger an unbounded request burst (it just gets re-advanced next
/// update). See `docs/protocol/notes/22-live-edge-never-advances.md`.
const MAX_PIECE_ADVANCE: u64 = 256;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// How many peers to race connecting to at once (so dead peers don't serialize the time to
/// first byte). A live swarm returns dozens; we only need one good upstream to follow.
const MAX_PARALLEL_CONNECT: usize = 12;
/// How many simultaneously connected upstreams to keep in the active live follower.
/// Small on purpose: enough to avoid committing playback to one stale peer, but low enough
/// not to look like an abusive client in public swarms.
const MAX_ACTIVE_UPSTREAMS: usize = 4;
/// Legacy single-peer helper handle. The production path now assigns real peer handles
/// starting at 1, but the old helper is kept as a short-term bisect fallback.
const SINGLE_PEER_ID: u64 = 0;
/// After the first peer completes handshake + advertises a live window, briefly collect any
/// other already-near-complete candidates so we can prefer a fresher live head without paying
/// the full timeout of dead peers.
const UPSTREAM_SELECTION_GRACE: Duration = Duration::from_millis(250);
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
/// A connected peer that keeps the TCP session alive but does not deliver pieces or live-edge
/// advancement is a bad upstream for playback. Reconnect so another discovered peer gets a
/// chance instead of freezing forever after the initial prefetch window.
const STALE_UPSTREAM_TIMEOUT: Duration = Duration::from_secs(12);
/// How long a single requested piece may stay outstanding before we re-request it (from a
/// different peer when one is available — see `ActivePeers`/`Scheduler`). Well below
/// `STALE_UPSTREAM_TIMEOUT` so a single dropped/evicted request self-heals in seconds instead
/// of waiting for the whole-pool stale teardown. Also the grace before skipping a piece that
/// has been evicted from every upstream window.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(4);
/// Upper bound on how long the pool loop sleeps between retransmit/skip sweeps, so a stall is
/// noticed within ~1s even when no peer sends anything.
const REQUEST_CHECK_INTERVAL: Duration = Duration::from_secs(1);
/// Background discovery can spend longer/deeper than startup discovery because it does not
/// gate first byte. It should still finish before the stale-upstream timer fires, so a new
/// peer can enter the pool before we reconnect.
const BACKGROUND_DISCOVERY_BUDGET: Duration = Duration::from_secs(8);
const BACKGROUND_DISCOVERY_PEER_TARGET: usize = 64;

/// Acestream's hardcoded public UDP tracker (see `docs/protocol/notes/03`). A bare
/// content-id/infohash carries no tracker of its own, so we announce here to find peers.
/// DHT discovery runs alongside this tracker in `discover_peers`.
const DEFAULT_ACE_TRACKERS: &[&str] = &["udp://t1.torrentstream.org:2710/announce"];

/// How long a resolved content-id → `StreamInfo` stays cached.
const RESOLVE_CACHE_TTL: Duration = Duration::from_secs(300);

/// Bytes of recently-downloaded data retained per active peer connection for reseeding.
const SEED_STORE_BYTES: u64 = 128 * 1024 * 1024;

/// Seeding configuration threaded through the download loop: the shared per-infohash
/// piece store (so downloaded data becomes servable to inbound peers too — T7), its size
/// budget, and whether reciprocal serving over THIS outbound connection is enabled at all.
#[derive(Clone)]
struct SeedConfig {
    registry: SeedRegistry,
    store_bytes: u64,
    enabled: bool,
    /// Pieces behind the live edge the fresh follower starts at (playback cushion).
    prefetch_pieces: u64,
}

pub struct AceProvider {
    identity: Arc<Identity>,
    port: u16,
    default_trackers: Vec<String>,
    bootstrap_peers: Vec<SocketAddrV4>,
    resolve_cache: ResolveCache,
    seed_registry: SeedRegistry,
    seed_store_bytes: u64,
    prefetch_pieces: u64,
    enable_seeding: bool,
}

impl AceProvider {
    pub fn new(identity: Arc<Identity>, port: u16) -> Self {
        AceProvider {
            identity,
            port,
            default_trackers: DEFAULT_ACE_TRACKERS.iter().map(|s| s.to_string()).collect(),
            bootstrap_peers: Vec::new(),
            resolve_cache: ResolveCache::new(RESOLVE_CACHE_TTL),
            seed_registry: SeedRegistry::new(),
            seed_store_bytes: SEED_STORE_BYTES,
            prefetch_pieces: PREFETCH_PIECES,
            enable_seeding: true,
        }
    }

    /// Trackers used for a bare infohash (which carries none); transport files supply their
    /// own. Operators can extend this; DHT discovery is a documented follow-up.
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

    /// Override how many pieces behind the live edge a fresh follower starts at, tuning the
    /// immediate playback cushion against extra startup latency. Defaults to `PREFETCH_PIECES`.
    pub fn with_prefetch_pieces(mut self, pieces: u64) -> Self {
        self.prefetch_pieces = pieces;
        self
    }

    /// Enable/disable reciprocal serving over outbound (leecher) connections — answering a
    /// peer's `Interested`/chunk-requests and advertising `Have` for newly-completed pieces.
    /// Defaults to `true` (S1 behavior). Setting `false` makes this provider a pure leecher.
    pub fn with_seeding_enabled(mut self, enabled: bool) -> Self {
        self.enable_seeding = enabled;
        self
    }

    /// Resolve a content-id to a [`StreamInfo`] by fetching its `AceStreamTransport` metadata
    /// from the signed catalog path, falling back to BEP-9 `ut_metadata` from a metadata-swarm
    /// peer (cached with a TTL). The content-id itself is the metadata-swarm handshake key;
    /// the result carries the real infohash.
    async fn resolve_content_id(&self, content_id: &str) -> Result<StreamInfo, ProviderError> {
        if let Some(info) = self.resolve_cache.get(content_id) {
            return Ok(info);
        }
        let key = hex20(content_id).map_err(|_| ProviderError::Backend("bad content-id".into()))?;

        match resolve_via_catalog(content_id).await {
            Ok(info) => {
                let ih: String = info.infohash.iter().map(|b| format!("{b:02x}")).collect();
                crate::alog!("[ace] resolved cid:{content_id} via catalog -> infohash {ih}");
                self.resolve_cache.put(content_id, info.clone());
                return Ok(info);
            }
            Err(e) => crate::alog!("[ace] resolve cid:{content_id}: catalog failed: {e:?}"),
        }

        let all = if self.bootstrap_peers.is_empty() {
            discover_peers(&self.default_trackers, &key, &random_peer_id(), self.port).await
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
                    let ih: String = info.infohash.iter().map(|b| format!("{b:02x}")).collect();
                    crate::alog!("[ace] resolved cid:{content_id} via {addr} -> infohash {ih}");
                    self.resolve_cache.put(content_id, info.clone());
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
}

struct AceSource {
    rx: mpsc::Receiver<Bytes>,
    peers: Arc<AtomicU32>,
    downloaded: Arc<AtomicU64>,
    uploaded: Arc<AtomicU64>,
    peers_served: Arc<AtomicU32>,
}

#[async_trait]
impl TsSource for AceSource {
    async fn next(&mut self) -> Option<Bytes> {
        self.rx.recv().await
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
}

#[async_trait]
impl StreamProvider for AceProvider {
    fn network(&self) -> &'static str {
        "ace"
    }

    async fn open(&self, id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        // Two id shapes: a bare 40-hex infohash resolves directly with default live geometry;
        // a `cid:<40hex>` content-id is resolved over the network via ut_metadata (the engine
        // does content_id→infohash internally — we do it ourselves, no Acestream API).
        let info = if let Some(content_id) = id.strip_prefix("cid:") {
            self.resolve_content_id(content_id).await?
        } else if id.len() == 40 && id.bytes().all(|b| b.is_ascii_hexdigit()) {
            stream_info_from_infohash(id, self.default_trackers.clone())
                .map_err(|_| ProviderError::Backend("bad infohash".into()))?
        } else {
            return Err(ProviderError::Backend(
                "id must be a 40-hex infohash or cid:<40hex> content-id".into(),
            ));
        };

        // Bootstrap peers are the proven/direct path and must be tried without waiting for
        // tracker/DHT discovery. Background refill can still discover more peers after start.
        let peers = if self.bootstrap_peers.is_empty() {
            discover_peers(&info.trackers, &info.infohash, &random_peer_id(), self.port).await
        } else {
            self.bootstrap_peers.clone()
        };
        crate::alog!("[ace] open {id}: discovered {} peer(s)", peers.len());
        if peers.is_empty() {
            return Err(ProviderError::Backend(
                "no peers (no trackers/bootstrap)".into(),
            ));
        }

        let (tx, rx) = mpsc::channel::<Bytes>(256);
        let peer_count = Arc::new(AtomicU32::new(0));
        let downloaded = Arc::new(AtomicU64::new(0));
        let uploaded = Arc::new(AtomicU64::new(0));
        let peers_served = Arc::new(AtomicU32::new(0));
        let identity = self.identity.clone();
        let stats_peers = peer_count.clone();
        let stats_downloaded = downloaded.clone();
        let stats_uploaded = uploaded.clone();
        let stats_peers_served = peers_served.clone();
        let seed = SeedConfig {
            registry: self.seed_registry.clone(),
            store_bytes: self.seed_store_bytes,
            enabled: self.enable_seeding,
            prefetch_pieces: self.prefetch_pieces,
        };
        let announce_info = info.clone();
        let announce_port = self.port;
        let discovery_port = self.port;
        let announce_enabled = self.enable_seeding;
        tokio::spawn(async move {
            // Run the download loop and the periodic seeder self-announce concurrently;
            // whichever ends first (normally `follow_live`, when the consumer drops) tears
            // down the other — no separate lifecycle to manage.
            tokio::select! {
                _ = follow_live(info, peers, identity, tx, stats_peers, downloaded, uploaded, peers_served, seed, discovery_port) => {},
                _ = announce_seeder_periodically(announce_info, announce_port, announce_enabled) => {},
            }
        });
        Ok(Box::new(AceSource {
            rx,
            peers: peer_count,
            downloaded: stats_downloaded,
            uploaded: stats_uploaded,
            peers_served: stats_peers_served,
        }))
    }
}

/// Periodically re-announce this infohash as a seeder (`left=0`, event=Completed) to its
/// trackers, so outpace becomes organically discoverable to peers looking for this stream
/// while we're serving it; see `docs/protocol/notes/24-seeder-self-announce.md`.
/// A no-op loop (never
/// announces) when `enabled` is false, matching the S1 `enable_seeding` gate: we shouldn't
/// advertise ourselves as a seeder if we've deliberately disabled serving.
async fn announce_seeder_periodically(info: StreamInfo, port: u16, enabled: bool) {
    if !enabled {
        return std::future::pending().await;
    }
    announce_infohash_periodically(info.trackers, info.infohash, port).await
}

/// The tracker+DHT self-announce loop, decoupled from `StreamInfo` so both the leech path
/// (a followed live stream) and B1 origination (a broadcast we minted ourselves, which has
/// no `StreamInfo` at all — just an infohash and trackers) can reuse the same primitive.
pub async fn announce_infohash_periodically(trackers: Vec<String>, infohash: [u8; 20], port: u16) {
    let peer_id = random_peer_id();
    loop {
        let peers = announce_seeder(&trackers, &infohash, &peer_id, port).await;
        // DHT self-announce too, not just tracker: real Acestream swarms are largely
        // DHT-populated (README.md), so tracker-only self-announce under-serves
        // discoverability. `dht_announce_peer` is a separate primitive (not folded into
        // `announce_seeder` itself) because it's a multi-second live network call that
        // would otherwise turn `announce_seeder`'s fast offline unit test into a slow,
        // network-dependent one.
        let dht_announced = dht_announce_peer(&infohash, port, DHT_ANNOUNCE_BUDGET).await;
        crate::alog!(
            "[ace] seeder self-announce for {}: {} tracker peer(s) seen, DHT announce_peer sent to {dht_announced} node(s)",
            hex_preview(&infohash),
            peers.len(),
        );
        tokio::time::sleep(SEEDER_ANNOUNCE_INTERVAL).await;
    }
}

/// Which peers to try connecting to next: everyone except those already known bad this
/// session — unless that would leave nothing to try, in which case give the whole list
/// another chance. `peers` is a fixed, one-time-discovered set for this stream, so
/// permanently avoiding every peer that ever failed once (a transient timeout, or a peer
/// that just never happened to unchoke us) would end the session rather than let it retry —
/// worse than the wasted reconnect attempt this is guarding against.
fn candidates_for_reconnect(
    peers: &[SocketAddrV4],
    excluded: &HashSet<SocketAddrV4>,
) -> Vec<SocketAddrV4> {
    let filtered: Vec<SocketAddrV4> = peers
        .iter()
        .copied()
        .filter(|a| !excluded.contains(a))
        .collect();
    if filtered.is_empty() {
        peers.to_vec()
    } else {
        filtered
    }
}

fn merge_discovered_peers(peers: &mut Vec<SocketAddrV4>, discovered: Vec<SocketAddrV4>) -> usize {
    let mut known: HashSet<SocketAddrV4> = peers.iter().copied().collect();
    let before = peers.len();
    for addr in discovered {
        if known.insert(addr) {
            peers.push(addr);
        }
    }
    peers.len() - before
}

fn reconcile_peer_rediscovery(
    peers: &mut Vec<SocketAddrV4>,
    excluded: &mut HashSet<SocketAddrV4>,
    discovered: Vec<SocketAddrV4>,
) -> usize {
    let added = merge_discovered_peers(peers, discovered);
    if added == 0 {
        // No fresh candidates appeared. Let previously-lost peers back into rotation after a
        // discovery cycle so a reachable peer whose live window later advances can be retried.
        excluded.clear();
    }
    added
}

fn prefer_window(candidate: &LivePosition, current: &LivePosition) -> bool {
    let score = |w: &LivePosition| (w.max_piece, w.position, -w.distance_from_source);
    score(candidate) > score(current)
}

struct ConnectedUpstream {
    session: PeerSession<TcpStream>,
    addr: SocketAddrV4,
    window: LivePosition,
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
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct PeerConnectStats {
    connected: usize,
    tcp: usize,
    handshake: usize,
    window: usize,
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
        }
    }

    fn record_task_failure(&mut self) {
        self.task += 1;
    }

    fn has_observations(&self) -> bool {
        self.connected + self.tcp + self.handshake + self.window + self.task > 0
    }

    fn summary(&self) -> String {
        let attempted = self.connected + self.tcp + self.handshake + self.window + self.task;
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

async fn peer_worker<S>(
    peer_id: u64,
    addr: SocketAddrV4,
    mut session: PeerSession<S>,
    mut commands: mpsc::Receiver<PeerCommand>,
    events: mpsc::Sender<PeerEvent>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    loop {
        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(PeerCommand::RequestPiece { piece, chunks_per_piece }) => {
                        for chunk in 0..chunks_per_piece {
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
    seen_ids: HashSet<u8>,
    commands: mpsc::Sender<PeerCommand>,
    worker: tokio::task::JoinHandle<()>,
}

async fn connect_upstream(addr: SocketAddrV4, infohash: [u8; 20]) -> PeerConnectAttempt {
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
    let Some(window) = read_peer_window(&mut session).await else {
        return PeerConnectAttempt::Failed(PeerConnectFailure {
            addr,
            stage: PeerConnectStage::Window,
        });
    };
    PeerConnectAttempt::Connected(ConnectedUpstream {
        session,
        addr,
        window,
    })
}

fn pool_refill_candidates(
    peers: &[SocketAddrV4],
    excluded: &HashSet<SocketAddrV4>,
    active: &HashSet<SocketAddrV4>,
) -> Vec<SocketAddrV4> {
    peers
        .iter()
        .copied()
        .filter(|addr| !excluded.contains(addr) && !active.contains(addr))
        .collect()
}

fn take_new_refill_candidates(
    known: &mut HashSet<SocketAddrV4>,
    discovered: Vec<SocketAddrV4>,
) -> Vec<SocketAddrV4> {
    discovered
        .into_iter()
        .filter(|addr| known.insert(*addr))
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
/// `excluded` accumulates every peer lost so far this session (not just the most recent),
/// so a reconnect doesn't keep re-picking a peer that's already proven bad — see
/// `candidates_for_reconnect` for the "everyone excluded" fallback.
async fn connect_pool(
    peers: &[SocketAddrV4],
    infohash: [u8; 20],
    excluded: &HashSet<SocketAddrV4>,
) -> Vec<ConnectedUpstream> {
    let candidates = candidates_for_reconnect(peers, excluded);
    let mut stats = PeerConnectStats::default();
    for batch in candidates.chunks(MAX_PARALLEL_CONNECT) {
        let mut set = tokio::task::JoinSet::new();
        for &addr in batch {
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
                Ok(PeerConnectAttempt::Failed(failure)) => stats.record_failure(failure),
                Err(_) => stats.record_task_failure(),
            }
        }
        if connected.is_empty() {
            continue;
        }

        let deadline = tokio::time::Instant::now() + UPSTREAM_SELECTION_GRACE;
        loop {
            if connected.len() >= MAX_ACTIVE_UPSTREAMS {
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
        connected.truncate(MAX_ACTIVE_UPSTREAMS);
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
    mut peers: Vec<SocketAddrV4>,
    identity: Arc<Identity>,
    tx: mpsc::Sender<Bytes>,
    peer_count: Arc<AtomicU32>,
    downloaded: Arc<AtomicU64>,
    uploaded: Arc<AtomicU64>,
    peers_served: Arc<AtomicU32>,
    seed: SeedConfig,
    discovery_port: u16,
) {
    let chunks_per_piece = info.chunks_per_piece();
    // Every peer lost so far this session (cumulative, not just the most recent) — see
    // `candidates_for_reconnect`.
    let mut excluded: HashSet<SocketAddrV4> = HashSet::new();
    // Piece-continuity state (reassembler, resync filter, request frontier, head) survives
    // across reconnects — see `Continuity`. `None` only before the very first connection.
    let mut continuity: Option<Continuity> = None;
    // `emitted` byte count at the last time a stalled pool was retried without excluding its
    // peers. If we stall again having produced nothing since, those peers are genuinely
    // unproductive (e.g. a frozen source) and get excluded so we look elsewhere.
    let mut last_stall_emitted: Option<u64> = None;
    loop {
        if tx.is_closed() {
            return;
        }
        let mut upstreams = connect_pool(&peers, info.infohash, &excluded).await;
        if upstreams.is_empty() {
            peer_count.store(0, Ordering::Relaxed);
            let known = peers.len();
            let discovered = discover_peers(
                &info.trackers,
                &info.infohash,
                &random_peer_id(),
                discovery_port,
            )
            .await;
            let found = discovered.len();
            let added = reconcile_peer_rediscovery(&mut peers, &mut excluded, discovered);
            crate::alog!(
                "[ace] no reachable peer among {known} known; rediscovery found {found}, added {added} (known now {})",
                peers.len()
            );
            continue;
        }
        if let Some(c) = &continuity {
            upstreams.retain(|upstream| {
                let usable = c.window_can_resume(&upstream.window);
                if !usable {
                    crate::alog!(
                        "[ace] {}: stale advertised window min={} max={} cannot cover next needed piece {}; trying another peer",
                        upstream.addr,
                        upstream.window.min_piece,
                        upstream.window.max_piece,
                        c.reasm.next_needed()
                    );
                    excluded.insert(upstream.addr);
                }
                usable
            });
            if upstreams.is_empty() {
                continue;
            }
        }
        let windows = upstreams
            .iter()
            .map(|u| format!("{}:{}..{}", u.addr, u.window.min_piece, u.window.max_piece))
            .collect::<Vec<_>>()
            .join(", ");
        let active_addrs: HashSet<SocketAddrV4> = upstreams.iter().map(|u| u.addr).collect();
        let refill_candidates = pool_refill_candidates(&peers, &excluded, &active_addrs);
        let known_refill_peers = peers.clone();
        crate::alog!(
            "[ace] connected + handshaked upstream pool ({} peer(s)): {windows}",
            upstreams.len()
        );
        peer_count.store(upstreams.len() as u32, Ordering::Relaxed);
        match follow_peer_pool(
            upstreams,
            &info,
            &identity,
            chunks_per_piece,
            &tx,
            &downloaded,
            &uploaded,
            &peers_served,
            &seed,
            &mut continuity,
            refill_candidates,
            known_refill_peers,
            discovery_port,
            &peer_count,
        )
        .await
        {
            FollowEnd::ConsumerGone => return,
            FollowEnd::PeerLost(lost) => {
                peer_count.store(0, Ordering::Relaxed);
                excluded.extend(lost);
                continue;
            }
            FollowEnd::PoolStale { stalled, lost } => {
                peer_count.store(0, Ordering::Relaxed);
                excluded.extend(lost);
                let emitted = continuity.as_ref().map(|c| c.emitted);
                if retry_stalled_pool(emitted, &mut last_stall_emitted) {
                    // We've produced output since the last stall retry: these peers are
                    // productive, just hit a gap. Reconnect to them (don't exclude) so
                    // `Continuity::resume`/`skip_to` can skip past the evicted piece.
                } else {
                    // Stalled again with nothing emitted in between — give up on them.
                    crate::alog!(
                        "[ace] {} stalled peer(s) made no progress on retry; excluding",
                        stalled.len()
                    );
                    excluded.extend(stalled);
                }
                continue;
            }
        }
    }
}

enum FollowEnd {
    ConsumerGone,
    PeerLost(Vec<SocketAddrV4>),
    /// The pool stopped producing contiguous output for `STALE_UPSTREAM_TIMEOUT`, but its
    /// peers were still connected (reachable). Unlike `PeerLost`, these `stalled` peers are
    /// NOT excluded on their own: a stall is usually us falling behind the live edge (the
    /// piece we still need got evicted from the peers' windows) rather than a bad peer, and
    /// the reconnect's `Continuity::resume`/`skip_to` is exactly how we skip that gap. Retry
    /// them (bounded by no-progress in `follow_live`); `lost` are genuine drops to exclude.
    PoolStale {
        stalled: Vec<SocketAddrV4>,
        lost: Vec<SocketAddrV4>,
    },
}

/// Piece-continuity state that must survive a peer reconnect within one `follow_live`
/// session. Recreating it fresh per connection (the pre-fix behavior) either re-emitted
/// pieces already served into the live broadcast — a duplicate splice — or left the
/// reassembler waiting forever for a piece the new peer's window had already evicted, since
/// `PieceReassembler` only ever emits strictly contiguously from its cursor. Real swarm
/// connections drop and reconnect routinely, so this was a guaranteed visible stutter on
/// every hop, not an edge case. See `docs/protocol/notes/23-reconnect-continuity.md`.
struct Continuity {
    reasm: PieceReassembler,
    resync: ace_media::mpegts::TsResync,
    scheduler: Scheduler,
    active_peers: ActivePeers,
    received_chunks: BTreeMap<u64, HashSet<u16>>,
    /// When each still-outstanding piece was (re-)requested — drives per-piece retransmission
    /// (`REQUEST_TIMEOUT`) independent of the whole-pool stale timer.
    requested_at: HashMap<u64, Instant>,
    /// When the playback cursor (`reasm.next_needed()`) last advanced. If it stays put past
    /// `REQUEST_TIMEOUT` and no upstream window still covers it, the piece was evicted and we
    /// skip forward rather than freeze.
    next_needed_since: Instant,
    head: u64,
    emitted: u64,
    next_log: u64,
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
    ) -> (Continuity, u64) {
        let start = prefetch_start(min_piece, max_piece, prefetch);
        (
            Continuity {
                reasm: PieceReassembler::new(info.piece_length, start)
                    .with_piece_trailer(info.sig_len as u64),
                resync: ace_media::mpegts::TsResync::new(),
                scheduler: Scheduler::new(MAX_PIECE_ADVANCE as usize),
                active_peers: ActivePeers::new(),
                received_chunks: BTreeMap::new(),
                requested_at: HashMap::new(),
                next_needed_since: Instant::now(),
                head: max_piece,
                emitted: 0,
                next_log: 1 << 20,
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
        }
        resume
    }

    fn window_can_resume(&self, window: &LivePosition) -> bool {
        let needed = i64::try_from(self.reasm.next_needed()).unwrap_or(i64::MAX);
        window.max_piece >= needed
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

    /// Pieces still outstanding past `REQUEST_TIMEOUT` — candidates to re-request. Also prunes
    /// timer bookkeeping for pieces the cursor has already advanced past.
    fn timed_out_requests(&mut self, now: Instant) -> Vec<u64> {
        let next = self.reasm.next_needed();
        self.requested_at.retain(|&p, _| p >= next);
        self.requested_at
            .iter()
            .filter(|(_, &at)| now.duration_since(at) >= REQUEST_TIMEOUT)
            .map(|(&p, _)| p)
            .collect()
    }

    /// If the cursor has been stuck past `REQUEST_TIMEOUT` on a piece no unchoked upstream can
    /// still serve (evicted from every window), skip forward to the lowest piece some peer
    /// does have. Returns the skip target if it skipped. This is the mid-session analogue of
    /// [`resume`](Self::resume)'s reconnect-gap skip — recovering without a full teardown.
    fn skip_evicted_gap(&mut self, now: Instant) -> Option<u64> {
        let next = self.reasm.next_needed();
        if now.duration_since(self.next_needed_since) < REQUEST_TIMEOUT
            || self.active_peers.any_unchoked_covers(next)
        {
            return None;
        }
        let floor = self.active_peers.lowest_covered_piece()?;
        if floor <= next {
            return None;
        }
        self.reasm.skip_to(floor);
        self.active_peers.prune_below(floor);
        self.requested_at.retain(|&p, _| p >= floor);
        self.received_chunks.retain(|&p, _| p >= floor);
        self.next_needed_since = now;
        Some(floor)
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

enum PoolWake {
    Peer(Option<PeerEvent>),
    Refill(Option<ConnectedUpstream>),
}

async fn activate_upstream_peer(
    peer_id: u64,
    mut upstream: ConnectedUpstream,
    start: u64,
    identity: &Identity,
    continuity: &mut Continuity,
    event_tx: &mpsc::Sender<PeerEvent>,
) -> Result<PeerRuntime, SocketAddrV4> {
    let peer_min = upstream.window.min_piece.max(0) as u64;
    let peer_max = upstream.window.max_piece.max(0) as u64;
    continuity.register_active_peer(peer_id, upstream.addr, upstream.window);
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
        for piece in continuity.active_peers.remove(peer_id) {
            continuity.scheduler.on_drop(piece);
        }
        return Err(upstream.addr);
    }
    let (command_tx, command_rx) = mpsc::channel(64);
    let worker = tokio::spawn(peer_worker(
        peer_id,
        upstream.addr,
        upstream.session,
        command_rx,
        event_tx.clone(),
    ));
    Ok(PeerRuntime {
        addr: upstream.addr,
        min_piece: peer_min,
        max_piece: peer_max,
        unchoked_peer: false,
        seen_ids: HashSet::new(),
        commands: command_tx,
        worker,
    })
}

async fn refill_upstream_pool(
    initial_candidates: Vec<SocketAddrV4>,
    trackers: Vec<String>,
    infohash: [u8; 20],
    discovery_port: u16,
    known_peers: Vec<SocketAddrV4>,
    refills: mpsc::Sender<ConnectedUpstream>,
) {
    let mut known: HashSet<SocketAddrV4> = known_peers.into_iter().collect();
    known.extend(initial_candidates.iter().copied());
    let mut stats = PeerConnectStats::default();
    let mut sent = 0usize;
    let mut candidates = initial_candidates;
    let discovery_handle = tokio::spawn(async move {
        discover_peers_with_options(
            &trackers,
            &infohash,
            &random_peer_id(),
            discovery_port,
            background_discovery_options(),
        )
        .await
    });
    let mut discovery_handle = Some(discovery_handle);
    loop {
        for batch in candidates.chunks(MAX_PARALLEL_CONNECT) {
            let mut set = tokio::task::JoinSet::new();
            for &addr in batch {
                set.spawn(connect_upstream(addr, infohash));
            }
            while let Some(joined) = set.join_next().await {
                match joined {
                    Ok(PeerConnectAttempt::Connected(upstream)) => {
                        stats.record_connected();
                        if refills.send(upstream).await.is_err() {
                            if stats.has_observations() {
                                crate::alog!(
                                    "[ace] background upstream refill stopped: {}",
                                    stats.summary()
                                );
                            }
                            return;
                        }
                        sent += 1;
                        if sent >= MAX_ACTIVE_UPSTREAMS {
                            break;
                        }
                    }
                    Ok(PeerConnectAttempt::Failed(failure)) => stats.record_failure(failure),
                    Err(_) => stats.record_task_failure(),
                }
            }
            if sent >= MAX_ACTIVE_UPSTREAMS {
                break;
            }
        }
        if sent >= MAX_ACTIVE_UPSTREAMS {
            break;
        }
        let Some(handle) = discovery_handle.take() else {
            break;
        };
        let discovered = handle.await.unwrap_or_default();
        let found = discovered.len();
        candidates = take_new_refill_candidates(&mut known, discovered);
        crate::alog!(
            "[ace] background upstream discovery: found {found}, added {}",
            candidates.len()
        );
        if candidates.is_empty() {
            break;
        }
    }
    if stats.has_observations() {
        crate::alog!(
            "[ace] background upstream refill finished: {}",
            stats.summary()
        );
    }
}

fn abort_refill(handle: &mut Option<tokio::task::JoinHandle<()>>) {
    if let Some(handle) = handle.take() {
        handle.abort();
    }
}

#[allow(clippy::too_many_arguments)]
async fn follow_peer_pool(
    upstreams: Vec<ConnectedUpstream>,
    info: &StreamInfo,
    identity: &Identity,
    chunks_per_piece: u16,
    tx: &mpsc::Sender<Bytes>,
    downloaded: &Arc<AtomicU64>,
    uploaded: &Arc<AtomicU64>,
    peers_served: &Arc<AtomicU32>,
    seed: &SeedConfig,
    continuity: &mut Option<Continuity>,
    refill_candidates: Vec<SocketAddrV4>,
    known_refill_peers: Vec<SocketAddrV4>,
    discovery_port: u16,
    peer_count: &Arc<AtomicU32>,
) -> FollowEnd {
    debug_assert!(!upstreams.is_empty());
    let primary = upstreams[0].window;
    let primary_addr = upstreams[0].addr;
    let min_piece = primary.min_piece.max(0) as u64;
    let max_piece = primary.max_piece.max(0) as u64;
    let start = match continuity {
        None => {
            let (c, start) = Continuity::fresh(info, min_piece, max_piece, seed.prefetch_pieces);
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
    let store = seed.registry.get_or_create(info.infohash, || {
        PieceStore::new(info.piece_length, info.chunk_length, seed.store_bytes)
    });
    let (event_tx, mut event_rx) = mpsc::channel(MAX_ACTIVE_UPSTREAMS * 32);
    let (refill_tx, mut refill_rx) = mpsc::channel(MAX_ACTIVE_UPSTREAMS);
    // A live-held clone of the refill sender so peers learned from `id=12` peer-exchange
    // gossip can be connected and fed into the same pool-add path (keeps `refill_rx` open
    // even after the background refill task finishes).
    let pex_tx = refill_tx.clone();
    let mut pex_tried: HashSet<SocketAddrV4> = HashSet::new();
    let mut refill_handle = if refill_candidates.is_empty() {
        None
    } else {
        crate::alog!(
            "[ace] background upstream refill: trying {} candidate(s)",
            refill_candidates.len()
        );
        Some(tokio::spawn(refill_upstream_pool(
            refill_candidates,
            info.trackers.clone(),
            info.infohash,
            discovery_port,
            known_refill_peers,
            refill_tx,
        )))
    };
    let mut peers: BTreeMap<u64, PeerRuntime> = BTreeMap::new();
    let mut lost_addrs = Vec::new();
    let mut next_peer_id = 1u64;

    for upstream in upstreams {
        let peer_id = next_peer_id;
        next_peer_id += 1;
        match activate_upstream_peer(peer_id, upstream, start, identity, continuity, &event_tx)
            .await
        {
            Ok(runtime) => {
                peers.insert(peer_id, runtime);
            }
            Err(addr) => lost_addrs.push(addr),
        }
    }
    if peers.is_empty() {
        abort_refill(&mut refill_handle);
        return FollowEnd::PeerLost(lost_addrs);
    }
    peer_count.store(peers.len() as u32, Ordering::Relaxed);

    let mut last_progress = Instant::now();
    let mut refill_closed = refill_handle.is_none();
    loop {
        let now = Instant::now();
        let Some(stale_budget) = stale_upstream_budget(last_progress, now) else {
            let stalled = peers.values().map(|p| p.addr).collect::<Vec<_>>();
            crate::alog!(
                "[ace] upstream pool stale — no live progress for {STALE_UPSTREAM_TIMEOUT:?}; reconnecting {} peer(s)",
                stalled.len()
            );
            shutdown_peer_runtimes(&mut peers);
            abort_refill(&mut refill_handle);
            return FollowEnd::PoolStale {
                stalled,
                lost: lost_addrs,
            };
        };
        // Self-heal a single stuck piece well before the whole-pool stale timeout: re-request
        // pieces outstanding past REQUEST_TIMEOUT (to a faster peer where possible) and skip a
        // piece evicted from every upstream window.
        let newly_lost =
            retransmit_stalled_requests(&mut peers, continuity, chunks_per_piece, now).await;
        lost_addrs.extend(newly_lost);
        if peers.is_empty() {
            abort_refill(&mut refill_handle);
            return FollowEnd::PeerLost(lost_addrs);
        }
        peer_count.store(peers.len() as u32, Ordering::Relaxed);

        // Wake at least every REQUEST_CHECK_INTERVAL so the sweep above runs even while no peer
        // sends anything.
        let wait = stale_budget.min(REQUEST_CHECK_INTERVAL);
        let event = match tokio::time::timeout(wait, async {
            tokio::select! {
                event = event_rx.recv() => PoolWake::Peer(event),
                upstream = refill_rx.recv(), if !refill_closed && peers.len() < MAX_ACTIVE_UPSTREAMS => {
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
            PoolWake::Peer(Some(event)) => event,
            PoolWake::Peer(None) => {
                shutdown_peer_runtimes(&mut peers);
                abort_refill(&mut refill_handle);
                return FollowEnd::PeerLost(lost_addrs);
            }
            PoolWake::Refill(Some(upstream)) => {
                if !continuity.window_can_resume(&upstream.window) {
                    crate::alog!(
                        "[ace] {}: background refill stale window min={} max={} cannot cover next needed piece {}; dropping",
                        upstream.addr,
                        upstream.window.min_piece,
                        upstream.window.max_piece,
                        continuity.reasm.next_needed()
                    );
                    lost_addrs.push(upstream.addr);
                    continue;
                }
                let peer_id = next_peer_id;
                next_peer_id += 1;
                let addr = upstream.addr;
                continuity.head = continuity.head.max(upstream.window.max_piece.max(0) as u64);
                match activate_upstream_peer(
                    peer_id,
                    upstream,
                    continuity.reasm.next_needed(),
                    identity,
                    continuity,
                    &event_tx,
                )
                .await
                {
                    Ok(runtime) => {
                        crate::alog!(
                            "[ace] {addr}: added to active upstream pool ({} peer(s))",
                            peers.len() + 1
                        );
                        peers.insert(peer_id, runtime);
                        peer_count.store(peers.len() as u32, Ordering::Relaxed);
                        let newly_lost =
                            advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                        lost_addrs.extend(newly_lost);
                    }
                    Err(addr) => lost_addrs.push(addr),
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
                if let Some(lost) = drop_peer_runtime(peer_id, &mut peers, continuity) {
                    crate::alog!("[ace] {addr}: upstream peer lost");
                    lost_addrs.push(lost);
                    let newly_lost =
                        advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                    lost_addrs.extend(newly_lost);
                    peer_count.store(peers.len() as u32, Ordering::Relaxed);
                }
                if peers.is_empty() {
                    abort_refill(&mut refill_handle);
                    return FollowEnd::PeerLost(lost_addrs);
                }
                continue;
            }
            PeerEvent::Message { peer_id, addr, msg } => {
                if !peers.contains_key(&peer_id) {
                    continue;
                }
                (peer_id, addr, msg)
            }
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
                lost_addrs.extend(newly_lost);
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
                lost_addrs.extend(newly_lost);
            }
            PeerMessage::Extended { ref payload, .. } => {
                if let Some(new_head) = advance_head_from_window(payload, continuity.head) {
                    continuity.head = new_head;
                    update_runtime_window(&mut peers, continuity, peer_id, new_head);
                    made_activity = true;
                    let newly_lost =
                        advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                    lost_addrs.extend(newly_lost);
                }
            }
            m @ PeerMessage::Piece { .. } => {
                if let Some(lc) = LiveChunk::from_message(&m) {
                    let piece = lc.piece as u64;
                    store.lock().await.put_chunk_with_header(
                        piece,
                        lc.chunk,
                        lc.piece_header,
                        &lc.data,
                    );
                    let begin = lc.chunk as u64 * info.chunk_length;
                    if continuity
                        .reasm
                        .add_block(lc.piece as u64, begin, &lc.data)
                        .is_err()
                    {
                        continue;
                    }
                    continuity.note_chunk(piece, lc.chunk, chunks_per_piece);
                    made_activity = true;
                    let ready = continuity.reasm.take_ready();
                    if !ready.is_empty() {
                        let aligned = continuity.resync.push(&ready);
                        if !aligned.is_empty() {
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
                            if tx.send(Bytes::from(aligned)).await.is_err() {
                                shutdown_peer_runtimes(&mut peers);
                                abort_refill(&mut refill_handle);
                                return FollowEnd::ConsumerGone;
                            }
                            downloaded.fetch_add(len, Ordering::Relaxed);
                            made_output = true;
                        }
                    }
                    let newly_lost =
                        advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                    lost_addrs.extend(newly_lost);
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
                        lost_addrs.push(lost);
                    }
                }
            }
            PeerMessage::Unknown { id: 6, ref payload } if seed.enabled && payload.len() >= 10 => {
                // payload: [stream u32 @0..4][piece u32 @4..8][chunk u16 @8..10]
                let p = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
                let c = u16::from_be_bytes([payload[8], payload[9]]);
                let (data, header) = {
                    let guard = store.lock().await;
                    (
                        guard.chunk(p as u64, c).map(|d| d.to_vec()),
                        guard.piece_header(p as u64).unwrap_or([0u8; 8]),
                    )
                };
                if let Some(data) = data {
                    let len = data.len();
                    if let Some(lost) = send_peer_command(
                        peer_id,
                        build_piece(0, p, c, header, &data),
                        &mut peers,
                        continuity,
                    )
                    .await
                    {
                        lost_addrs.push(lost);
                    } else {
                        uploaded.fetch_add(len as u64, Ordering::Relaxed);
                        peers_served.store(peers.len() as u32, Ordering::Relaxed);
                    }
                }
            }
            PeerMessage::Unknown { id: 4, ref payload } if payload.len() == 8 => {
                let piece =
                    u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]) as u64;
                if piece > continuity.head {
                    continuity.head = piece;
                    update_runtime_window(&mut peers, continuity, peer_id, piece);
                    made_activity = true;
                    let newly_lost =
                        advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                    lost_addrs.extend(newly_lost);
                }
            }
            PeerMessage::Unknown { id: 10, .. } => {}
            PeerMessage::Unknown {
                id: 12,
                ref payload,
            } => {
                // Peer-exchange gossip: connect to peers we don't already have and feed the
                // successes into the same pool-add path as the background refill, so a stalled
                // pool has fresh, swarm-sourced upstreams to fall back on (notes 41-43).
                if peers.len() < MAX_ACTIVE_UPSTREAMS {
                    let advertised = ace_wire::peer_exchange::parse_peer_exchange(payload);
                    let total = advertised.len();
                    let spawned =
                        harvest_peers(&advertised, &peers, &mut pex_tried, info.infohash, &pex_tx);
                    if spawned > 0 {
                        crate::alog!(
                            "[ace] peer-exchange from {addr}: {spawned} new peer(s) to try (of {total} advertised)"
                        );
                    }
                }
            }
            PeerMessage::Unknown {
                id: 36,
                ref payload,
            } => {
                // Source-node descriptor (the stream origin, re-announced by every peer). The
                // source never stalls, so try it once if we don't already have it.
                if peers.len() < MAX_ACTIVE_UPSTREAMS {
                    if let Some(source) = ace_wire::peer_exchange::parse_peer_announce(payload) {
                        if harvest_peers(&[source], &peers, &mut pex_tried, info.infohash, &pex_tx)
                            > 0
                        {
                            crate::alog!("[ace] source-node announce from {addr}: trying {source}");
                        }
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
                    lost_addrs.extend(newly_lost);
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
            abort_refill(&mut refill_handle);
            return FollowEnd::PeerLost(lost_addrs);
        }
        if should_refresh_stale_deadline(continuity.emitted, made_output, made_activity) {
            last_progress = Instant::now();
        }
        if made_output {
            // Contiguous output means the playback cursor advanced: reset the per-piece
            // eviction-skip timer so it only fires when the cursor is genuinely stuck.
            continuity.next_needed_since = Instant::now();
        }
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

/// Connect-race `advertised` peers we don't already have and feed the successes into the
/// pool-add channel (`pex_tx`), skipping already-active peers and any addr tried before this
/// session (`pex_tried`). Bounded to `MAX_PARALLEL_CONNECT` spawns per call. Returns how many
/// connect attempts were spawned. Shared by peer-exchange (`id=12`) and source-node (`id=36`).
fn harvest_peers(
    advertised: &[SocketAddrV4],
    peers: &BTreeMap<u64, PeerRuntime>,
    pex_tried: &mut HashSet<SocketAddrV4>,
    infohash: [u8; 20],
    pex_tx: &mpsc::Sender<ConnectedUpstream>,
) -> usize {
    if pex_tried.len() > 1024 {
        pex_tried.clear();
    }
    let active: HashSet<SocketAddrV4> = peers.values().map(|p| p.addr).collect();
    let mut spawned = 0usize;
    for &cand in advertised {
        if spawned >= MAX_PARALLEL_CONNECT {
            break;
        }
        if active.contains(&cand) || !pex_tried.insert(cand) {
            continue;
        }
        let tx = pex_tx.clone();
        tokio::spawn(async move {
            if let PeerConnectAttempt::Connected(upstream) = connect_upstream(cand, infohash).await
            {
                // Drop the connection if the pool isn't currently taking peers.
                let _ = tx.try_send(upstream);
            }
        });
        spawned += 1;
    }
    spawned
}

async fn advance_pool_requests(
    peers: &mut BTreeMap<u64, PeerRuntime>,
    continuity: &mut Continuity,
    chunks_per_piece: u16,
) -> Vec<SocketAddrV4> {
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
/// piece outstanding past `REQUEST_TIMEOUT` and skip a piece evicted from every upstream
/// window, then re-issue requests. A timed-out piece is only requeued in the *scheduler* (its
/// original peer keeps the in-flight slot), so [`ActivePeers::assign`] steers the retry to a
/// peer with more spare capacity — i.e. a different, faster one when available. Returns any
/// peers dropped while re-issuing requests.
async fn retransmit_stalled_requests(
    peers: &mut BTreeMap<u64, PeerRuntime>,
    continuity: &mut Continuity,
    chunks_per_piece: u16,
    now: Instant,
) -> Vec<SocketAddrV4> {
    let mut changed = false;
    if let Some(floor) = continuity.skip_evicted_gap(now) {
        crate::alog!(
            "[ace] next needed piece evicted from all upstream windows; skipping ahead to {floor}"
        );
        changed = true;
    }
    let timed_out = continuity.timed_out_requests(now);
    if !timed_out.is_empty() {
        crate::alog!(
            "[ace] re-requesting {} piece(s) outstanding > {REQUEST_TIMEOUT:?} (from {})",
            timed_out.len(),
            continuity.reasm.next_needed()
        );
        for piece in timed_out {
            continuity.scheduler.on_drop(piece);
            continuity.requested_at.remove(&piece);
        }
        changed = true;
    }
    if changed {
        advance_pool_requests(peers, continuity, chunks_per_piece).await
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
            let (c, start) = Continuity::fresh(info, min_piece, max_piece, seed.prefetch_pieces);
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
        return FollowEnd::PeerLost(vec![addr]);
    }

    let mut unchoked = false;
    let peer_min = min_piece;
    let mut peer_max = max_piece;
    let store = seed.registry.get_or_create(info.infohash, || {
        PieceStore::new(info.piece_length, info.chunk_length, seed.store_bytes)
    });
    let mut unchoked_peer = false;
    let mut last_progress = Instant::now();
    // Diagnostic: surface each unmodelled Acestream message id once (note 22 follow-up).
    let mut seen_ids: HashSet<u8> = HashSet::new();

    loop {
        let Some(read_budget) = stale_upstream_budget(last_progress, Instant::now()) else {
            crate::alog!("[ace] {addr}: stale upstream — no live progress for {STALE_UPSTREAM_TIMEOUT:?}; reconnecting");
            return FollowEnd::PeerLost(vec![addr]);
        };
        let msg = match tokio::time::timeout(read_budget, session.read_message()).await {
            Ok(Ok(m)) => m,
            Ok(Err(_)) => return FollowEnd::PeerLost(vec![addr]),
            Err(_) => {
                crate::alog!("[ace] {addr}: stale upstream — no live progress for {STALE_UPSTREAM_TIMEOUT:?}; reconnecting");
                return FollowEnd::PeerLost(vec![addr]);
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
                    return FollowEnd::PeerLost(vec![addr]);
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
                    return FollowEnd::PeerLost(vec![addr]);
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
                        return FollowEnd::PeerLost(vec![addr]);
                    }
                }
            }
            m @ PeerMessage::Piece { .. } => {
                if let Some(lc) = LiveChunk::from_message(&m) {
                    let piece = lc.piece as u64;
                    store.lock().await.put_chunk_with_header(
                        piece,
                        lc.chunk,
                        lc.piece_header,
                        &lc.data,
                    );
                    let begin = lc.chunk as u64 * info.chunk_length;
                    if continuity
                        .reasm
                        .add_block(lc.piece as u64, begin, &lc.data)
                        .is_err()
                    {
                        continue;
                    }
                    continuity.note_chunk(piece, lc.chunk, chunks_per_piece);
                    made_activity = true;
                    let ready = continuity.reasm.take_ready();
                    if !ready.is_empty() {
                        let aligned = continuity.resync.push(&ready);
                        if !aligned.is_empty() {
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
                            if tx.send(Bytes::from(aligned)).await.is_err() {
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
                        return FollowEnd::PeerLost(vec![addr]);
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
                let (data, header) = {
                    let guard = store.lock().await;
                    (
                        guard.chunk(p as u64, c).map(|d| d.to_vec()),
                        guard.piece_header(p as u64).unwrap_or([0u8; 8]),
                    )
                };
                if let Some(data) = data {
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
                        return FollowEnd::PeerLost(vec![addr]);
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
                        return FollowEnd::PeerLost(vec![addr]);
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
    (w.max_piece > head).then_some(w.max_piece)
}

fn stale_upstream_budget(last_progress: Instant, now: Instant) -> Option<Duration> {
    let elapsed = now.saturating_duration_since(last_progress);
    if elapsed >= STALE_UPSTREAM_TIMEOUT {
        None
    } else {
        Some(STALE_UPSTREAM_TIMEOUT - elapsed)
    }
}

fn should_refresh_stale_deadline(emitted: u64, made_output: bool, made_activity: bool) -> bool {
    made_output || (emitted == 0 && made_activity)
}

/// Decide whether a just-stalled pool's (reachable) peers should be reconnected to rather than
/// excluded. Retry while the session keeps producing output between stalls (`emitted` differs
/// from the last retry's watermark); once two stalls bracket zero output, stop retrying so we
/// look elsewhere instead of looping on a frozen source. Updates the watermark on retry.
fn retry_stalled_pool(emitted: Option<u64>, last_stall_emitted: &mut Option<u64>) -> bool {
    if emitted != *last_stall_emitted {
        *last_stall_emitted = emitted;
        true
    } else {
        false
    }
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

/// Read messages until the peer's extended handshake arrives; return its live `mi` window.
async fn read_peer_window(session: &mut PeerSession<TcpStream>) -> Option<LivePosition> {
    for _ in 0..32 {
        let msg = session.read_message().await.ok()?;
        if let PeerMessage::Extended { ext_id: 0, payload } = msg {
            let eh = ExtendedHandshake::parse(&payload).ok()?;
            let mi = eh.raw.get(b"mi")?;
            let get = |k: &[u8]| mi.get(k).and_then(|v| v.as_int()).unwrap_or(-1);
            return Some(LivePosition {
                min_piece: get(b"min_piece"),
                max_piece: get(b"max_piece"),
                position: get(b"position"),
                distance_from_source: get(b"distance_from_source"),
            });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use ace_swarm::scheduler::{ActivePeers, PeerAssignment};

    #[tokio::test]
    async fn network_is_ace() {
        let p = AceProvider::new(Arc::new(Identity::generate()), 6878);
        assert_eq!(p.network(), "ace");
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
    async fn peer_worker_sends_chunk_requests_from_commands() {
        use tokio::io::AsyncReadExt;

        let (client, mut server) = tokio::io::duplex(4096);
        let session = PeerSession::new(client).with_timeout(Duration::from_millis(250));
        let addr: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();
        let (command_tx, command_rx) = mpsc::channel(4);
        let (event_tx, _event_rx) = mpsc::channel(4);
        let worker = tokio::spawn(peer_worker(1, addr, session, command_rx, event_tx));

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
        let worker = tokio::spawn(peer_worker(7, addr, session, command_rx, event_tx));

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
        let mut scheduler = Scheduler::new(MAX_PIECE_ADVANCE as usize);
        let mut active = single_active_peer(100, 109);
        let pieces = schedule_piece_requests(&mut scheduler, &mut active, 100, 109);
        assert_eq!(pieces, (100..=109).collect::<Vec<_>>());
    }

    #[test]
    fn caught_up_requests_nothing() {
        // The reassembler cursor is already past the head -> nothing new yet (the loop
        // keeps waiting for window updates to advance it).
        let mut scheduler = Scheduler::new(MAX_PIECE_ADVANCE as usize);
        let mut active = single_active_peer(100, 109);
        assert!(schedule_piece_requests(&mut scheduler, &mut active, 110, 109).is_empty());
    }

    #[test]
    fn window_update_drives_a_forward_request() {
        // The head advanced by one piece -> request exactly that new piece. This is the
        // step the pre-fix loop never took (it only advanced on a `Have` that never came).
        let mut one = Scheduler::new(MAX_PIECE_ADVANCE as usize);
        let mut one_active = single_active_peer(100, 110);
        assert_eq!(
            schedule_piece_requests(&mut one, &mut one_active, 110, 110),
            vec![110]
        );

        let mut many = Scheduler::new(MAX_PIECE_ADVANCE as usize);
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
        let mut scheduler = Scheduler::new(MAX_PIECE_ADVANCE as usize);
        let mut active = single_active_peer(0, 10_000_000);
        let pieces = schedule_piece_requests(&mut scheduler, &mut active, 101, 10_000_000);
        assert_eq!(pieces.first(), Some(&101));
        assert_eq!(pieces.len() as u64, MAX_PIECE_ADVANCE);
    }

    #[test]
    fn stale_upstream_budget_expires_after_no_forward_progress() {
        let now = std::time::Instant::now();
        assert_eq!(
            stale_upstream_budget(now, now + Duration::from_secs(3)),
            Some(STALE_UPSTREAM_TIMEOUT - Duration::from_secs(3))
        );
        assert_eq!(
            stale_upstream_budget(now, now + STALE_UPSTREAM_TIMEOUT),
            None
        );
        assert_eq!(
            stale_upstream_budget(now, now + STALE_UPSTREAM_TIMEOUT + Duration::from_millis(1)),
            None
        );
    }

    #[test]
    fn stalled_pool_retries_while_making_progress_then_gives_up() {
        let mut watermark = None;
        // First stall (some bytes already served): retry, record watermark.
        assert!(retry_stalled_pool(Some(40), &mut watermark));
        // Recovered and served more before stalling again: retry again.
        assert!(retry_stalled_pool(Some(52), &mut watermark));
        // Stalled again with nothing emitted since: stop retrying (exclude the peers).
        assert!(!retry_stalled_pool(Some(52), &mut watermark));
        // If it later produces output, it's productive again -> retry resumes.
        assert!(retry_stalled_pool(Some(53), &mut watermark));
    }

    #[test]
    fn stalled_pool_before_any_playback_retries_once_then_gives_up() {
        let mut watermark = None;
        assert!(retry_stalled_pool(Some(0), &mut watermark)); // first stall at 0 bytes: one retry
        assert!(!retry_stalled_pool(Some(0), &mut watermark)); // still 0 -> give up
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
        let (mut c, _) = Continuity::fresh(&info(), 5, 15, PREFETCH_PIECES); // next_needed = 7
        let base = Instant::now();
        c.requested_at.insert(4, base); // below cursor -> pruned, not returned
        c.requested_at.insert(8, base); // aged
        c.requested_at.insert(9, base + REQUEST_TIMEOUT); // fresh
        let now = base + REQUEST_TIMEOUT;
        let mut out = c.timed_out_requests(now);
        out.sort_unstable();
        assert_eq!(out, vec![8]);
        assert!(!c.requested_at.contains_key(&4), "passed piece pruned");
    }

    #[test]
    fn skip_evicted_gap_jumps_to_lowest_covered_when_cursor_is_stranded() {
        let (mut c, _) = Continuity::fresh(&info(), 100, 110, PREFETCH_PIECES); // next_needed = 102
        c.register_active_peer(1, test_addr(), win(105, 120)); // evicted 102..104
        c.set_peer_unchoked(1, true);
        // Not stuck long enough yet.
        assert_eq!(c.skip_evicted_gap(c.next_needed_since), None);
        // Stuck past the timeout with no peer covering 102 -> skip to 105.
        let now = c.next_needed_since + REQUEST_TIMEOUT;
        assert_eq!(c.skip_evicted_gap(now), Some(105));
        assert_eq!(c.reasm.next_needed(), 105);
    }

    #[test]
    fn skip_evicted_gap_does_not_skip_while_a_peer_still_covers_the_cursor() {
        let (mut c, _) = Continuity::fresh(&info(), 100, 110, PREFETCH_PIECES); // next_needed = 102
        c.register_active_peer(1, test_addr(), win(100, 120)); // still covers 102
        c.set_peer_unchoked(1, true);
        let now = c.next_needed_since + REQUEST_TIMEOUT * 3;
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
            sig_len: 0,
        }
    }

    #[tokio::test]
    async fn seeder_announce_never_fires_when_seeding_disabled() {
        // enabled=false must never even attempt a tracker announce (which would otherwise
        // misrepresent us as a seeder while we've deliberately disabled serving).
        let res = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            announce_seeder_periodically(info(), 6878, false),
        )
        .await;
        assert!(res.is_err(), "must never resolve when seeding is disabled");
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
        let (c, start) = Continuity::fresh(&info(), 100, 200, PREFETCH_PIECES);
        assert_eq!(start, 200 - PREFETCH_PIECES);
        assert_eq!(c.head, 200);
        assert_eq!(c.scheduler.in_flight_count(), 0);
        assert_eq!(c.reasm.next_needed(), start);
    }

    #[test]
    fn fresh_clamps_start_to_min_piece_on_a_narrow_window() {
        // min_piece is closer to head than PREFETCH_PIECES allows -> clamp, don't request
        // an evicted piece.
        let (_c, start) = Continuity::fresh(&info(), 198, 200, PREFETCH_PIECES);
        assert_eq!(start, 198);
    }

    #[test]
    fn resume_continues_seamlessly_when_the_new_window_still_covers_our_position() {
        // We'd already emitted through piece 150; the new peer's window still covers the
        // next needed piece, so resume exactly there.
        let (mut c, start) = Continuity::fresh(&info(), 100, 149, PREFETCH_PIECES);
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
        let (mut c, start) = Continuity::fresh(&info(), 100, 149, PREFETCH_PIECES);
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
        let (c, start) = Continuity::fresh(&info(), 100, 149, PREFETCH_PIECES);
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
        let (mut c, _start) = Continuity::fresh(&info(), 100, 149, PREFETCH_PIECES);
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
    fn resume_head_never_regresses() {
        let (mut c, _start) = Continuity::fresh(&info(), 100, 200, PREFETCH_PIECES);
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
    fn candidates_exclude_previously_lost_peers() {
        let peers = addrs(&[1, 2, 3]);
        let excluded: HashSet<SocketAddrV4> = [addrs(&[2])[0]].into_iter().collect();
        let candidates = candidates_for_reconnect(&peers, &excluded);
        assert_eq!(
            candidates,
            addrs(&[1, 3]),
            "peer 2 was excluded, having failed before"
        );
    }

    #[test]
    fn candidates_accumulate_across_multiple_losses() {
        // The bug this fixes: excluding only the MOST RECENT loss let a session flip-flop
        // back to a peer that had already failed earlier in the same run.
        let peers = addrs(&[1, 2, 3]);
        let excluded: HashSet<SocketAddrV4> = addrs(&[1, 2]).into_iter().collect();
        let candidates = candidates_for_reconnect(&peers, &excluded);
        assert_eq!(
            candidates,
            addrs(&[3]),
            "both earlier failures stay excluded, not just the latest"
        );
    }

    #[test]
    fn candidates_fall_back_to_the_full_list_once_everyone_has_failed() {
        // The peer list is fixed for this stream (discovered once at `open`); permanently
        // blacklisting every peer that ever failed once would end the session rather than
        // give a transient failure another chance.
        let peers = addrs(&[1, 2]);
        let excluded: HashSet<SocketAddrV4> = addrs(&[1, 2]).into_iter().collect();
        let candidates = candidates_for_reconnect(&peers, &excluded);
        assert_eq!(
            candidates, peers,
            "nothing left to try -> give the whole list another chance"
        );
    }

    #[test]
    fn rediscovery_merges_only_new_peers_and_keeps_existing_order() {
        let mut peers = addrs(&[1, 2]);
        let added = merge_discovered_peers(&mut peers, addrs(&[2, 3, 1, 4]));

        assert_eq!(added, 2);
        assert_eq!(peers, addrs(&[1, 2, 3, 4]));
    }

    #[test]
    fn rediscovery_reports_zero_when_every_peer_was_already_known() {
        let mut peers = addrs(&[1, 2]);
        let added = merge_discovered_peers(&mut peers, addrs(&[2, 1, 2]));

        assert_eq!(added, 0);
        assert_eq!(peers, addrs(&[1, 2]));
    }

    #[test]
    fn rediscovery_without_new_peers_allows_excluded_peers_to_be_retried() {
        let mut peers = addrs(&[1, 2]);
        let mut excluded: HashSet<SocketAddrV4> = addrs(&[1]).into_iter().collect();

        let added = reconcile_peer_rediscovery(&mut peers, &mut excluded, addrs(&[2, 1]));

        assert_eq!(added, 0);
        assert!(
            excluded.is_empty(),
            "a refresh cycle with no new peers should let known peers be re-evaluated"
        );
    }

    #[test]
    fn pool_refill_candidates_skip_active_and_excluded_peers() {
        let peers = addrs(&[1, 2, 3, 4]);
        let active: HashSet<SocketAddrV4> = addrs(&[2]).into_iter().collect();
        let excluded: HashSet<SocketAddrV4> = addrs(&[3]).into_iter().collect();

        assert_eq!(
            pool_refill_candidates(&peers, &excluded, &active),
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
        let mut known: HashSet<SocketAddrV4> = addrs(&[1, 2]).into_iter().collect();

        assert_eq!(
            take_new_refill_candidates(&mut known, addrs(&[2, 3, 1, 4, 3])),
            addrs(&[3, 4])
        );
        assert!(known.contains(&addrs(&[1])[0]));
        assert!(known.contains(&addrs(&[2])[0]));
        assert!(known.contains(&addrs(&[3])[0]));
        assert!(known.contains(&addrs(&[4])[0]));
    }

    #[test]
    fn background_discovery_options_leave_stale_timer_margin() {
        let opts = background_discovery_options();
        assert!(opts.peer_target > 8);
        assert!(opts.dht_budget < STALE_UPSTREAM_TIMEOUT);
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
