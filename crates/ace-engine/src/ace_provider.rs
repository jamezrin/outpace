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
use ace_swarm::discover::discover_peers;
use ace_swarm::resolve::{hex20, resolve_via_peer, stream_info_from_infohash, ResolveCache, ResolveError};
use ace_swarm::types::StreamInfo;
use ace_wire::extended::{ExtendedHandshake, LivePosition, NodeFields, OutgoingExtendedHandshake};
use ace_wire::handshake::random_peer_id;
use ace_wire::identity::Identity;
use ace_swarm::store::PieceStore;
use ace_wire::live_codec::{build_piece, chunk_request, LiveChunk};
use ace_wire::message::PeerMessage;
use ace_wire::reassembly::PieceReassembler;
use async_trait::async_trait;
use bytes::Bytes;
use std::net::SocketAddrV4;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

/// How many pieces behind the live edge to start, so we have buffer immediately.
const PREFETCH_PIECES: u64 = 8;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
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

pub struct AceProvider {
    identity: Arc<Identity>,
    port: u16,
    default_trackers: Vec<String>,
    bootstrap_peers: Vec<SocketAddrV4>,
    resolve_cache: ResolveCache,
}

impl AceProvider {
    pub fn new(identity: Arc<Identity>, port: u16) -> Self {
        AceProvider {
            identity,
            port,
            default_trackers: DEFAULT_ACE_TRACKERS.iter().map(|s| s.to_string()).collect(),
            bootstrap_peers: Vec::new(),
            resolve_cache: ResolveCache::new(RESOLVE_CACHE_TTL),
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
        tokio::spawn(async move {
            follow_live(info, peers, identity, tx, stats_peers, uploaded, peers_served).await;
        });
        Ok(Box::new(AceSource { rx, peers: peer_count, uploaded: stats_uploaded, peers_served: stats_peers_served }))
    }
}

/// Follow the live edge from the first responsive peer, pushing contiguous TS. Reconnects to
/// the next peer on failure; ends when the consumer drops.
async fn follow_live(
    info: StreamInfo,
    peers: Vec<SocketAddrV4>,
    identity: Arc<Identity>,
    tx: mpsc::Sender<Bytes>,
    peer_count: Arc<AtomicU32>,
    uploaded: Arc<AtomicU64>,
    peers_served: Arc<AtomicU32>,
) {
    let chunks_per_piece = info.chunks_per_piece();
    for addr in peers {
        if tx.is_closed() {
            return;
        }
        let Ok(Ok(mut session)) =
            tokio::time::timeout(CONNECT_TIMEOUT, connect(&addr.to_string())).await
        else {
            eprintln!("[ace] {addr}: connect failed/timed out");
            continue;
        };
        if session.perform_handshake(info.infohash, random_peer_id()).await.is_err() {
            eprintln!("[ace] {addr}: BT handshake failed");
            continue;
        }
        eprintln!("[ace] {addr}: connected + handshaked");
        peer_count.store(1, Ordering::Relaxed);
        match follow_one_peer(&mut session, &info, &identity, addr, chunks_per_piece, &tx, &uploaded, &peers_served).await {
            FollowEnd::ConsumerGone => return,
            FollowEnd::PeerLost => {
                peer_count.store(0, Ordering::Relaxed);
                continue;
            }
        }
    }
}

enum FollowEnd {
    ConsumerGone,
    PeerLost,
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
) -> FollowEnd {
    // 1. Read the peer's advertised live window (their unsolicited extended handshake).
    let Some(window) = read_peer_window(session).await else {
        eprintln!("[ace] {addr}: no extended handshake / window");
        return FollowEnd::PeerLost;
    };
    let mut head = window.max_piece.max(0) as u64;
    let start = head.saturating_sub(PREFETCH_PIECES);
    eprintln!("[ace] {addr}: window min={} max={} -> start={start} head={head}", window.min_piece, window.max_piece);

    // 2. Advertise our matching position + interest.
    let hs = OutgoingExtendedHandshake {
        ace_metadata_version: 1,
        ut_metadata_id: 2,
        mi: Some(LivePosition {
            min_piece: start as i64,
            max_piece: head as i64,
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

    let mut reasm = PieceReassembler::new(info.piece_length, start);
    // Acestream's 1 MiB live pieces are each internally TS-aligned but don't byte-chain
    // (~one partial packet of junk per piece boundary); re-lock packet alignment so the
    // served stream is clean MPEG-TS.
    let mut resync = ace_media::mpegts::TsResync::new();
    let mut requested_to: Option<u64> = None;
    let mut unchoked = false;
    let mut store = PieceStore::new(info.piece_length, info.chunk_length, SEED_STORE_BYTES);
    let mut unchoked_peer = false;

    loop {
        let msg = match session.read_message().await {
            Ok(m) => m,
            Err(_) => return FollowEnd::PeerLost,
        };
        match msg {
            PeerMessage::Unchoke => {
                unchoked = true;
                eprintln!("[ace] {addr}: UNCHOKE -> requesting pieces {start}..={head}");
                if request_range(session, start, head, chunks_per_piece).await.is_err() {
                    return FollowEnd::PeerLost;
                }
                requested_to = Some(head);
            }
            PeerMessage::Choke => unchoked = false,
            PeerMessage::Have(p) => {
                head = head.max(p as u64);
                if unchoked {
                    let from = requested_to.map(|r| r + 1).unwrap_or(start);
                    if from <= head {
                        if request_range(session, from, head, chunks_per_piece).await.is_err() {
                            return FollowEnd::PeerLost;
                        }
                        requested_to = Some(head);
                    }
                }
            }
            m @ PeerMessage::Piece { .. } => {
                if let Some(lc) = LiveChunk::from_message(&m) {
                    store.put_chunk(lc.piece as u64, lc.chunk, &lc.data);
                    let begin = lc.chunk as u64 * info.chunk_length;
                    if reasm.add_block(lc.piece as u64, begin, &lc.data).is_err() {
                        continue;
                    }
                    let ready = reasm.take_ready();
                    if !ready.is_empty() {
                        let aligned = resync.push(&ready);
                        if !aligned.is_empty() && tx.send(Bytes::from(aligned)).await.is_err() {
                            return FollowEnd::ConsumerGone;
                        }
                    }
                }
            }
            PeerMessage::Interested => {
                if !unchoked_peer {
                    let _ = session.send(&PeerMessage::Unchoke).await;
                    unchoked_peer = true;
                }
            }
            PeerMessage::Unknown { id: 6, ref payload } if payload.len() >= 10 => {
                // payload: [stream u32 @0..4][piece u32 @4..8][chunk u16 @8..10]
                let p = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
                let c = u16::from_be_bytes([payload[8], payload[9]]);
                if let Some(data) = store.chunk(p as u64, c).map(|d| d.to_vec()) {
                    // piece_header [0u8;8] until note 21 pins the engine's exact bytes.
                    if session.send(&build_piece(0, p, c, [0u8; 8], &data)).await.is_ok() {
                        uploaded.fetch_add(data.len() as u64, Ordering::Relaxed);
                        peers_served.store(1, Ordering::Relaxed); // single-peer follow; multi-peer aggregation is S2
                    }
                }
            }
            _ => {}
        }
    }
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
}
