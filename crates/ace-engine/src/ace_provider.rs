//! The `"ace"` provider: resolves an identifier to a [`StreamInfo`], discovers peers via
//! trackers, and follows the live edge from a peer, emitting contiguous MPEG-TS. Built on
//! the cracked live protocol (see `docs/protocol/notes/19.md`).
//!
//! LIVE-GATED: the peer I/O path requires the real Acestream swarm and is verified in the
//! operator's environment (it cannot run in CI/sandbox). Content-id → transport-file
//! resolution over `ut_metadata` is the one remaining network step (see
//! [`ace_swarm::resolve`]); the infohash form works directly.

use crate::provider::{ProviderError, SourceStats, StreamProvider, TsSource};
use ace_peer::session::{connect, PeerSession};
use ace_swarm::dht::dht_announce_peer;
use ace_swarm::discover::{announce_seeder, discover_peers};
use ace_swarm::resolve::{hex20, resolve_via_peer, stream_info_from_infohash, ResolveCache, ResolveError};
use ace_swarm::types::StreamInfo;
use ace_wire::extended::{ExtendedHandshake, LivePosition, NodeFields, OutgoingExtendedHandshake};
use ace_wire::handshake::random_peer_id;
use ace_wire::identity::Identity;
use ace_swarm::store::PieceStore;
use ace_swarm::listen::SeedRegistry;
use ace_wire::live::LiveWindow;
use ace_wire::live_codec::{build_piece, chunk_request, LiveChunk};
use ace_wire::message::PeerMessage;
use ace_wire::reassembly::PieceReassembler;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashSet;
use std::net::SocketAddrV4;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
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
/// How often an active session re-announces itself as a seeder to its trackers, so
/// outpace becomes organically discoverable while it's serving (Task 7 approach (2),
/// `docs/RESUME.md`). Doesn't yet honor a tracker's returned `interval` — a fixed,
/// conservative cadence is a deliberate simplification, not a correctness requirement.
const SEEDER_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(4 * 60);
/// Time budget for each periodic DHT `announce_peer` walk (see `dht_announce_peer`) — bounds
/// how long a self-announce round can take before the next one is due.
const DHT_ANNOUNCE_BUDGET: Duration = Duration::from_secs(15);
/// Per-peer read ceiling while resolving a content-id (a silent peer shouldn't stall us).
const RESOLVE_PEER_TIMEOUT: Duration = Duration::from_secs(6);

/// Acestream's hardcoded public UDP tracker (see `docs/protocol/notes/03`). A bare
/// content-id/infohash carries no tracker of its own, so we announce here to find peers.
/// DHT discovery is a documented follow-up.
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
}

pub struct AceProvider {
    identity: Arc<Identity>,
    port: u16,
    default_trackers: Vec<String>,
    bootstrap_peers: Vec<SocketAddrV4>,
    resolve_cache: ResolveCache,
    seed_registry: SeedRegistry,
    seed_store_bytes: u64,
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

    /// Enable/disable reciprocal serving over outbound (leecher) connections — answering a
    /// peer's `Interested`/chunk-requests and advertising `Have` for newly-completed pieces.
    /// Defaults to `true` (S1 behavior). Setting `false` makes this provider a pure leecher.
    pub fn with_seeding_enabled(mut self, enabled: bool) -> Self {
        self.enable_seeding = enabled;
        self
    }

    /// Resolve a content-id to a [`StreamInfo`] by fetching its `AceStreamTransport` metadata
    /// over BEP-9 `ut_metadata` from a metadata-swarm peer (cached with a TTL). The content-id
    /// itself is the metadata-swarm handshake key; the result carries the real infohash.
    async fn resolve_content_id(&self, content_id: &str) -> Result<StreamInfo, ProviderError> {
        if let Some(info) = self.resolve_cache.get(content_id) {
            return Ok(info);
        }
        let key = hex20(content_id).map_err(|_| ProviderError::Backend("bad content-id".into()))?;
        let mut peers = discover_peers(&self.default_trackers, &key, &random_peer_id(), self.port).await;
        let mut all = self.bootstrap_peers.clone();
        all.append(&mut peers);
        eprintln!("[ace] resolve cid:{content_id}: {} metadata peer(s)", all.len());

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
                    eprintln!("[ace] resolved cid:{content_id} via {addr} -> infohash {ih}");
                    self.resolve_cache.put(content_id, info.clone());
                    return Ok(info);
                }
                Err(ResolveError::Peer(why)) => eprintln!("[ace] resolve {addr}: {why}"),
                Err(e) => eprintln!("[ace] resolve {addr}: {e:?}"),
            }
        }
        Err(ProviderError::Backend("content-id resolution: no metadata peer responded".into()))
    }
}

struct AceSource {
    rx: mpsc::Receiver<Bytes>,
    peers: Arc<AtomicU32>,
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

        let mut peers = discover_peers(&info.trackers, &info.infohash, &random_peer_id(), self.port).await;
        // Bootstrap peers (proven live path) are tried first.
        let mut all = self.bootstrap_peers.clone();
        all.append(&mut peers);
        let peers = all;
        eprintln!("[ace] open {id}: discovered {} peer(s)", peers.len());
        if peers.is_empty() {
            return Err(ProviderError::Backend("no peers (no trackers/bootstrap)".into()));
        }

        let (tx, rx) = mpsc::channel::<Bytes>(256);
        let peer_count = Arc::new(AtomicU32::new(0));
        let uploaded = Arc::new(AtomicU64::new(0));
        let peers_served = Arc::new(AtomicU32::new(0));
        let identity = self.identity.clone();
        let stats_peers = peer_count.clone();
        let stats_uploaded = uploaded.clone();
        let stats_peers_served = peers_served.clone();
        let seed = SeedConfig {
            registry: self.seed_registry.clone(),
            store_bytes: self.seed_store_bytes,
            enabled: self.enable_seeding,
        };
        let announce_info = info.clone();
        let announce_port = self.port;
        let announce_enabled = self.enable_seeding;
        tokio::spawn(async move {
            // Run the download loop and the periodic seeder self-announce concurrently;
            // whichever ends first (normally `follow_live`, when the consumer drops or no
            // peer is reachable) tears down the other — no separate lifecycle to manage.
            tokio::select! {
                _ = follow_live(info, peers, identity, tx, stats_peers, uploaded, peers_served, seed) => {},
                _ = announce_seeder_periodically(announce_info, announce_port, announce_enabled) => {},
            }
        });
        Ok(Box::new(AceSource { rx, peers: peer_count, uploaded: stats_uploaded, peers_served: stats_peers_served }))
    }
}

/// Periodically re-announce this infohash as a seeder (`left=0`, event=Completed) to its
/// trackers, so outpace becomes organically discoverable to peers looking for this stream
/// while we're serving it — Task 7 approach (2), `docs/RESUME.md`. A no-op loop (never
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
        // DHT-populated (docs/RESUME.md), so tracker-only self-announce under-serves
        // discoverability. `dht_announce_peer` is a separate primitive (not folded into
        // `announce_seeder` itself) because it's a multi-second live network call that
        // would otherwise turn `announce_seeder`'s fast offline unit test into a slow,
        // network-dependent one.
        let dht_announced = dht_announce_peer(&infohash, port, DHT_ANNOUNCE_BUDGET).await;
        eprintln!(
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
fn candidates_for_reconnect(peers: &[SocketAddrV4], excluded: &HashSet<SocketAddrV4>) -> Vec<SocketAddrV4> {
    let filtered: Vec<SocketAddrV4> = peers.iter().copied().filter(|a| !excluded.contains(a)).collect();
    if filtered.is_empty() {
        peers.to_vec()
    } else {
        filtered
    }
}

/// Connect to and BT-handshake the peers **concurrently**, returning the first that
/// succeeds (the rest are aborted). Dead/firewalled peers no longer serialize the time to
/// first byte — a couple of unreachable peers at the front of the list used to cost
/// `CONNECT_TIMEOUT` each before we ever reached a live one (the "slow to load" report).
/// `excluded` accumulates every peer lost so far this session (not just the most recent),
/// so a reconnect doesn't keep re-picking a peer that's already proven bad — see
/// `candidates_for_reconnect` for the "everyone excluded" fallback.
async fn connect_any(
    peers: &[SocketAddrV4],
    infohash: [u8; 20],
    excluded: &HashSet<SocketAddrV4>,
) -> Option<(PeerSession<TcpStream>, SocketAddrV4)> {
    let candidates = candidates_for_reconnect(peers, excluded);
    for batch in candidates.chunks(MAX_PARALLEL_CONNECT) {
        let mut set = tokio::task::JoinSet::new();
        for &addr in batch {
            set.spawn(async move {
                let mut session =
                    tokio::time::timeout(CONNECT_TIMEOUT, connect(&addr.to_string()))
                        .await
                        .ok()?
                        .ok()?;
                session.perform_handshake(infohash, random_peer_id()).await.ok()?;
                Some((session, addr))
            });
        }
        // First task to fully connect+handshake wins; dropping `set` aborts the others.
        while let Some(joined) = set.join_next().await {
            if let Ok(Some(win)) = joined {
                return Some(win);
            }
        }
    }
    None
}

/// Follow the live edge from a peer, pushing contiguous TS. Races a fresh connection on
/// peer loss; ends when the consumer drops or no peer is reachable.
#[allow(clippy::too_many_arguments)]
async fn follow_live(
    info: StreamInfo,
    peers: Vec<SocketAddrV4>,
    identity: Arc<Identity>,
    tx: mpsc::Sender<Bytes>,
    peer_count: Arc<AtomicU32>,
    uploaded: Arc<AtomicU64>,
    peers_served: Arc<AtomicU32>,
    seed: SeedConfig,
) {
    let chunks_per_piece = info.chunks_per_piece();
    // Every peer lost so far this session (cumulative, not just the most recent) — see
    // `candidates_for_reconnect`.
    let mut excluded: HashSet<SocketAddrV4> = HashSet::new();
    // Piece-continuity state (reassembler, resync filter, request frontier, head) survives
    // across reconnects — see `Continuity`. `None` only before the very first connection.
    let mut continuity: Option<Continuity> = None;
    loop {
        if tx.is_closed() {
            return;
        }
        let Some((mut session, addr)) = connect_any(&peers, info.infohash, &excluded).await else {
            eprintln!("[ace] no reachable peer among {} discovered", peers.len());
            return;
        };
        eprintln!("[ace] {addr}: connected + handshaked");
        peer_count.store(1, Ordering::Relaxed);
        match follow_one_peer(
            &mut session,
            &info,
            &identity,
            addr,
            chunks_per_piece,
            &tx,
            &uploaded,
            &peers_served,
            &seed,
            &mut continuity,
        )
        .await
        {
            FollowEnd::ConsumerGone => return,
            FollowEnd::PeerLost => {
                peer_count.store(0, Ordering::Relaxed);
                excluded.insert(addr);
                continue;
            }
        }
    }
}

enum FollowEnd {
    ConsumerGone,
    PeerLost,
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
    requested_to: Option<u64>,
    head: u64,
    emitted: u64,
    next_log: u64,
}

impl Continuity {
    /// The very first peer connection for this stream: bootstrap from its window.
    fn fresh(info: &StreamInfo, min_piece: u64, max_piece: u64) -> (Continuity, u64) {
        let start = max_piece.saturating_sub(PREFETCH_PIECES).max(min_piece);
        (
            Continuity {
                reasm: PieceReassembler::new(info.piece_length, start),
                resync: ace_media::mpegts::TsResync::new(),
                requested_to: None,
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
        let next = self.requested_to.map(|r| r + 1).unwrap_or_else(|| self.reasm.next_needed());
        let resume = next.max(min_piece);
        if resume > next {
            eprintln!(
                "[ace] {addr}: reconnect gap — peer's window already evicted pieces {next}..{}; skipping ahead",
                resume - 1
            );
            self.reasm.skip_to(resume);
        }
        self.requested_to = Some(resume.saturating_sub(1));
        resume
    }
}

#[allow(clippy::too_many_arguments)]
async fn follow_one_peer(
    session: &mut PeerSession<TcpStream>,
    info: &StreamInfo,
    identity: &Identity,
    addr: SocketAddrV4,
    chunks_per_piece: u16,
    tx: &mpsc::Sender<Bytes>,
    uploaded: &Arc<AtomicU64>,
    peers_served: &Arc<AtomicU32>,
    seed: &SeedConfig,
    continuity: &mut Option<Continuity>,
) -> FollowEnd {
    // 1. Read the peer's advertised live window (their unsolicited extended handshake).
    let Some(window) = read_peer_window(session).await else {
        eprintln!("[ace] {addr}: no extended handshake / window");
        return FollowEnd::PeerLost;
    };
    let min_piece = window.min_piece.max(0) as u64;
    let max_piece = window.max_piece.max(0) as u64;
    let start = match continuity {
        None => {
            let (c, start) = Continuity::fresh(info, min_piece, max_piece);
            *continuity = Some(c);
            eprintln!("[ace] {addr}: window min={min_piece} max={max_piece} -> start={start} head={max_piece}");
            start
        }
        Some(c) => {
            let start = c.resume(addr, min_piece, max_piece);
            eprintln!(
                "[ace] {addr}: reconnected; window min={min_piece} max={max_piece} -> resuming from {start} head={}",
                c.head
            );
            start
        }
    };
    let continuity = continuity.as_mut().expect("initialized just above");

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
        node: NodeFields { ts: 5000, ..NodeFields::default() },
        peer_ip: Some(addr.ip().octets()),
    };
    if session.send_signed_extended_handshake(&hs, identity).await.is_err()
        || session.send(&PeerMessage::Interested).await.is_err()
    {
        return FollowEnd::PeerLost;
    }

    let mut unchoked = false;
    let store = seed
        .registry
        .get_or_create(info.infohash, || PieceStore::new(info.piece_length, info.chunk_length, seed.store_bytes));
    let mut unchoked_peer = false;
    // Diagnostic: surface each unmodelled Acestream message id once (note 22 follow-up).
    let mut seen_ids: HashSet<u8> = HashSet::new();

    loop {
        let msg = match session.read_message().await {
            Ok(m) => m,
            Err(_) => return FollowEnd::PeerLost,
        };
        match msg {
            PeerMessage::Unchoke => {
                unchoked = true;
                eprintln!("[ace] {addr}: UNCHOKE -> requesting pieces {start}..={}", continuity.head);
                if advance_requests(session, &mut continuity.requested_to, start, continuity.head, chunks_per_piece)
                    .await
                    .is_err()
                {
                    return FollowEnd::PeerLost;
                }
            }
            PeerMessage::Choke => unchoked = false,
            PeerMessage::Have(p) => {
                continuity.head = continuity.head.max(p as u64);
                if unchoked
                    && advance_requests(session, &mut continuity.requested_to, start, continuity.head, chunks_per_piece)
                        .await
                        .is_err()
                {
                    return FollowEnd::PeerLost;
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
                    if unchoked
                        && advance_requests(session, &mut continuity.requested_to, start, continuity.head, chunks_per_piece)
                            .await
                            .is_err()
                    {
                        return FollowEnd::PeerLost;
                    }
                }
            }
            m @ PeerMessage::Piece { .. } => {
                if let Some(lc) = LiveChunk::from_message(&m) {
                    let piece = lc.piece as u64;
                    store.lock().await.put_chunk(piece, lc.chunk, &lc.data);
                    let begin = lc.chunk as u64 * info.chunk_length;
                    if continuity.reasm.add_block(lc.piece as u64, begin, &lc.data).is_err() {
                        continue;
                    }
                    let ready = continuity.reasm.take_ready();
                    if !ready.is_empty() {
                        let aligned = continuity.resync.push(&ready);
                        if !aligned.is_empty() {
                            continuity.emitted += aligned.len() as u64;
                            if continuity.emitted >= continuity.next_log {
                                eprintln!(
                                    "[ace] {addr}: served {} MiB (head={}, next piece needed={})",
                                    continuity.emitted >> 20,
                                    continuity.head,
                                    continuity.reasm.next_needed()
                                );
                                continuity.next_log = continuity.emitted + (4 << 20);
                            }
                            if tx.send(Bytes::from(aligned)).await.is_err() {
                                return FollowEnd::ConsumerGone;
                            }
                        }
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
                let data = store.lock().await.chunk(p as u64, c).map(|d| d.to_vec());
                if let Some(data) = data {
                    // piece_header [0u8;8] until note 21 pins the engine's exact bytes.
                    if session.send(&build_piece(0, p, c, [0u8; 8], &data)).await.is_ok() {
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
                let piece = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]) as u64;
                if piece > continuity.head {
                    continuity.head = piece;
                    if unchoked
                        && advance_requests(session, &mut continuity.requested_to, start, continuity.head, chunks_per_piece)
                            .await
                            .is_err()
                    {
                        return FollowEnd::PeerLost;
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
                    eprintln!("[ace] {addr}: live window update (msg id={id}) head {} -> {new_head}", continuity.head);
                    continuity.head = new_head;
                    if unchoked
                        && advance_requests(session, &mut continuity.requested_to, start, continuity.head, chunks_per_piece)
                            .await
                            .is_err()
                    {
                        return FollowEnd::PeerLost;
                    }
                } else if seen_ids.insert(id) {
                    eprintln!(
                        "[ace] {addr}: unhandled msg id={id} ({} bytes) {}",
                        payload.len(),
                        hex_preview(payload)
                    );
                }
            }
            _ => {}
        }
    }
}

/// If `payload` carries a live-window (`myinfo`) update whose head is beyond `head`, return
/// the new head; otherwise `None`. The single place window recognition feeds the loop.
fn advance_head_from_window(payload: &[u8], head: u64) -> Option<u64> {
    let w = LiveWindow::from_myinfo_payload(payload)?;
    (w.max_piece > head).then_some(w.max_piece)
}

/// Send chunk requests for every chunk of pieces `[from, to]`.
async fn request_range(
    session: &mut PeerSession<TcpStream>,
    from: u64,
    to: u64,
    chunks_per_piece: u16,
) -> ace_peer::Result<()> {
    for piece in from..=to {
        for chunk in 0..chunks_per_piece {
            session.send(&chunk_request(piece as u32, chunk)).await?;
        }
    }
    Ok(())
}

/// The next contiguous piece range to request given the frontier we've already requested
/// up to (`requested_to`), the live start, and the best-known live head. `None` once we're
/// caught up to the head (nothing new to ask for yet). Bounded by [`MAX_PIECE_ADVANCE`] so
/// a bogus head can't produce an unbounded request. Pure (no I/O) so it is unit-tested.
fn next_request_range(requested_to: Option<u64>, start: u64, head: u64) -> Option<(u64, u64)> {
    let from = requested_to.map(|r| r + 1).unwrap_or(start);
    if from > head {
        return None;
    }
    let to = head.min(from + MAX_PIECE_ADVANCE - 1);
    Some((from, to))
}

/// Request the next batch of pieces toward `head`, advancing `requested_to`. No-op when
/// already caught up. This is the single forward-progress primitive every live-edge
/// advancement signal (UNCHOKE, `Have`, a `myinfo` window update) funnels through.
async fn advance_requests(
    session: &mut PeerSession<TcpStream>,
    requested_to: &mut Option<u64>,
    start: u64,
    head: u64,
    chunks_per_piece: u16,
) -> ace_peer::Result<()> {
    if let Some((from, to)) = next_request_range(*requested_to, start, head) {
        request_range(session, from, to, chunks_per_piece).await?;
        *requested_to = Some(to);
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

    #[tokio::test]
    async fn network_is_ace() {
        let p = AceProvider::new(Arc::new(Identity::generate()), 6878);
        assert_eq!(p.network(), "ace");
    }

    #[tokio::test]
    async fn unrecognized_id_shape_is_backend_error() {
        let p = AceProvider::new(Arc::new(Identity::generate()), 6878);
        // Neither a 40-hex infohash nor a cid:<40hex> content-id.
        assert!(matches!(p.open("not-a-hex-infohash").await, Err(ProviderError::Backend(_))));
    }

    #[tokio::test]
    async fn content_id_with_bad_hex_is_rejected_without_network() {
        let p = AceProvider::new(Arc::new(Identity::generate()), 6878);
        // `cid:` dispatch reaches resolution but the hex is invalid → immediate Backend error,
        // no discovery/connect attempted.
        assert!(matches!(p.open("cid:nothex").await, Err(ProviderError::Backend(_))));
    }

    // Note: the "no peers -> Backend error" path is intentionally not unit-tested, since
    // discovery now always consults the live DHT (network). It's exercised by the live
    // capture path instead.

    #[test]
    fn first_request_after_unchoke_covers_the_prefetch_window() {
        // Fresh frontier (None): request from `start` up to the head.
        assert_eq!(next_request_range(None, 100, 109), Some((100, 109)));
    }

    #[test]
    fn caught_up_requests_nothing() {
        // Already requested through the head -> nothing new yet (the old behavior that
        // then stalled forever; now the loop keeps getting window updates to advance it).
        assert_eq!(next_request_range(Some(109), 100, 109), None);
    }

    #[test]
    fn window_update_drives_a_forward_request() {
        // The head advanced by one piece -> request exactly that new piece. This is the
        // step the pre-fix loop never took (it only advanced on a `Have` that never came).
        assert_eq!(next_request_range(Some(109), 100, 110), Some((110, 110)));
        assert_eq!(next_request_range(Some(109), 100, 113), Some((110, 113)));
    }

    #[test]
    fn forward_request_is_bounded_against_a_bogus_head() {
        // A garbled window update claiming a wildly-advanced head can't burst-request the
        // whole range; it's clamped, and subsequent updates catch up.
        let (from, to) = next_request_range(Some(100), 0, 10_000_000).unwrap();
        assert_eq!(from, 101);
        assert_eq!(to - from + 1, MAX_PIECE_ADVANCE);
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
        StreamInfo { infohash: [0; 20], piece_length: 4, chunk_length: 2, trackers: vec![] }
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
    fn fresh_starts_prefetch_pieces_behind_head_clamped_to_min() {
        let (c, start) = Continuity::fresh(&info(), 100, 200);
        assert_eq!(start, 200 - PREFETCH_PIECES);
        assert_eq!(c.head, 200);
        assert_eq!(c.requested_to, None);
        assert_eq!(c.reasm.next_needed(), start);
    }

    #[test]
    fn fresh_clamps_start_to_min_piece_on_a_narrow_window() {
        // min_piece is closer to head than PREFETCH_PIECES allows -> clamp, don't request
        // an evicted piece.
        let (_c, start) = Continuity::fresh(&info(), 198, 200);
        assert_eq!(start, 198);
    }

    #[test]
    fn resume_continues_seamlessly_when_the_new_window_still_covers_our_position() {
        // We'd already requested through piece 150; the new peer's window still has it.
        let (mut c, _start) = Continuity::fresh(&info(), 100, 149);
        c.requested_to = Some(150);
        let addr: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();
        let resume = c.resume(addr, 100, 160);
        assert_eq!(resume, 151, "continues right after what we'd already requested");
        assert_eq!(c.head, 160);
        // No gap: the reassembler's cursor is untouched (still whatever fresh() set it to).
        assert_eq!(c.reasm.next_needed(), 149 - PREFETCH_PIECES);
    }

    #[test]
    fn resume_skips_forward_over_an_unrecoverable_eviction_gap() {
        // We were disconnected long enough that the new peer's window no longer has the
        // piece we needed next (min_piece has advanced past it) — must skip, not stall.
        let (mut c, _start) = Continuity::fresh(&info(), 100, 149);
        c.requested_to = Some(150);
        let addr: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();
        let resume = c.resume(addr, 500, 600); // min_piece way ahead of 151
        assert_eq!(resume, 500);
        assert_eq!(c.reasm.next_needed(), 500, "reassembler cursor jumped past the gap");
        assert_eq!(c.requested_to, Some(499));
    }

    #[test]
    fn resume_head_never_regresses() {
        let (mut c, _start) = Continuity::fresh(&info(), 100, 200);
        let addr: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();
        c.resume(addr, 100, 150); // a new peer with a "smaller" (staler) window
        assert_eq!(c.head, 200, "head must not go backward");
    }

    fn addrs(ports: &[u16]) -> Vec<SocketAddrV4> {
        ports.iter().map(|&p| SocketAddrV4::new([1, 2, 3, 4].into(), p)).collect()
    }

    #[test]
    fn candidates_exclude_previously_lost_peers() {
        let peers = addrs(&[1, 2, 3]);
        let excluded: HashSet<SocketAddrV4> = [addrs(&[2])[0]].into_iter().collect();
        let candidates = candidates_for_reconnect(&peers, &excluded);
        assert_eq!(candidates, addrs(&[1, 3]), "peer 2 was excluded, having failed before");
    }

    #[test]
    fn candidates_accumulate_across_multiple_losses() {
        // The bug this fixes: excluding only the MOST RECENT loss let a session flip-flop
        // back to a peer that had already failed earlier in the same run.
        let peers = addrs(&[1, 2, 3]);
        let excluded: HashSet<SocketAddrV4> = addrs(&[1, 2]).into_iter().collect();
        let candidates = candidates_for_reconnect(&peers, &excluded);
        assert_eq!(candidates, addrs(&[3]), "both earlier failures stay excluded, not just the latest");
    }

    #[test]
    fn candidates_fall_back_to_the_full_list_once_everyone_has_failed() {
        // The peer list is fixed for this stream (discovered once at `open`); permanently
        // blacklisting every peer that ever failed once would end the session rather than
        // give a transient failure another chance.
        let peers = addrs(&[1, 2]);
        let excluded: HashSet<SocketAddrV4> = addrs(&[1, 2]).into_iter().collect();
        let candidates = candidates_for_reconnect(&peers, &excluded);
        assert_eq!(candidates, peers, "nothing left to try -> give the whole list another chance");
    }
}
