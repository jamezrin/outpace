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
use ace_swarm::resolve::stream_info_from_infohash;
use ace_swarm::types::StreamInfo;
use ace_wire::extended::{ExtendedHandshake, LivePosition, NodeFields, OutgoingExtendedHandshake};
use ace_wire::handshake::random_peer_id;
use ace_wire::identity::Identity;
use ace_wire::live_codec::{chunk_request, LiveChunk};
use ace_wire::message::PeerMessage;
use ace_wire::reassembly::PieceReassembler;
use async_trait::async_trait;
use bytes::Bytes;
use std::net::SocketAddrV4;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

/// How many pieces behind the live edge to start, so we have buffer immediately.
const PREFETCH_PIECES: u64 = 8;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(6);

/// Acestream's hardcoded public UDP tracker (see `docs/protocol/notes/03`). A bare
/// content-id/infohash carries no tracker of its own, so we announce here to find peers.
/// DHT discovery is a documented follow-up.
const DEFAULT_ACE_TRACKERS: &[&str] = &["udp://t1.torrentstream.org:2710/announce"];

pub struct AceProvider {
    identity: Arc<Identity>,
    port: u16,
    default_trackers: Vec<String>,
    bootstrap_peers: Vec<SocketAddrV4>,
}

impl AceProvider {
    pub fn new(identity: Arc<Identity>, port: u16) -> Self {
        AceProvider {
            identity,
            port,
            default_trackers: DEFAULT_ACE_TRACKERS.iter().map(|s| s.to_string()).collect(),
            bootstrap_peers: Vec::new(),
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
}

struct AceSource {
    rx: mpsc::Receiver<Bytes>,
    peers: Arc<AtomicU32>,
}

#[async_trait]
impl TsSource for AceSource {
    async fn next(&mut self) -> Option<Bytes> {
        self.rx.recv().await
    }
    fn stats(&self) -> SourceStats {
        SourceStats { peers: self.peers.load(Ordering::Relaxed), bitrate: 0, buffer_ms: 0 }
    }
}

#[async_trait]
impl StreamProvider for AceProvider {
    fn network(&self) -> &'static str {
        "ace"
    }

    async fn open(&self, id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        // Infohash form resolves directly; content-id needs the ut_metadata fetch (live step).
        let info = if id.len() == 40 && id.bytes().all(|b| b.is_ascii_hexdigit()) {
            stream_info_from_infohash(id, self.default_trackers.clone())
                .map_err(|_| ProviderError::Backend("bad infohash".into()))?
        } else {
            return Err(ProviderError::Backend(
                "content-id resolution (ut_metadata transport fetch) not yet wired".into(),
            ));
        };

        let mut peers = discover_peers(&info.trackers, &info.infohash, &random_peer_id(), self.port).await;
        // Bootstrap peers (proven live path) are tried first.
        let mut all = self.bootstrap_peers.clone();
        all.append(&mut peers);
        let peers = all;
        if peers.is_empty() {
            return Err(ProviderError::Backend("no peers (no trackers/bootstrap)".into()));
        }

        let (tx, rx) = mpsc::channel::<Bytes>(256);
        let peer_count = Arc::new(AtomicU32::new(0));
        let identity = self.identity.clone();
        let stats_peers = peer_count.clone();
        tokio::spawn(async move {
            follow_live(info, peers, identity, tx, stats_peers).await;
        });
        Ok(Box::new(AceSource { rx, peers: peer_count }))
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
) {
    let chunks_per_piece = info.chunks_per_piece();
    for addr in peers {
        if tx.is_closed() {
            return;
        }
        let Ok(Ok(mut session)) =
            tokio::time::timeout(CONNECT_TIMEOUT, connect(&addr.to_string())).await
        else {
            continue;
        };
        if session.perform_handshake(info.infohash, random_peer_id()).await.is_err() {
            continue;
        }
        peer_count.store(1, Ordering::Relaxed);
        match follow_one_peer(&mut session, &info, &identity, addr, chunks_per_piece, &tx).await {
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

async fn follow_one_peer(
    session: &mut PeerSession<TcpStream>,
    info: &StreamInfo,
    identity: &Identity,
    addr: SocketAddrV4,
    chunks_per_piece: u16,
    tx: &mpsc::Sender<Bytes>,
) -> FollowEnd {
    // 1. Read the peer's advertised live window (their unsolicited extended handshake).
    let Some(window) = read_peer_window(session).await else {
        return FollowEnd::PeerLost;
    };
    let mut head = window.max_piece.max(0) as u64;
    let start = head.saturating_sub(PREFETCH_PIECES);

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
    let mut requested_to: Option<u64> = None;
    let mut unchoked = false;

    loop {
        let msg = match session.read_message().await {
            Ok(m) => m,
            Err(_) => return FollowEnd::PeerLost,
        };
        match msg {
            PeerMessage::Unchoke => {
                unchoked = true;
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
                    let begin = lc.chunk as u64 * info.chunk_length;
                    if reasm.add_block(lc.piece as u64, begin, &lc.data).is_err() {
                        continue;
                    }
                    let ready = reasm.take_ready();
                    if !ready.is_empty() && tx.send(Bytes::from(ready)).await.is_err() {
                        return FollowEnd::ConsumerGone;
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
    async fn content_id_form_is_unwired_backend_error() {
        let p = AceProvider::new(Arc::new(Identity::generate()), 6878);
        // Non-hex id (a content-id shape) is rejected as the live-gated path.
        assert!(matches!(p.open("not-a-hex-infohash").await, Err(ProviderError::Backend(_))));
    }

    #[tokio::test]
    async fn no_trackers_and_no_bootstrap_yields_no_peers() {
        // Clear the default tracker so this stays offline/deterministic.
        let p = AceProvider::new(Arc::new(Identity::generate()), 6878).with_trackers(vec![]);
        let hex = "0123456789abcdef0123456789abcdef01234567";
        assert!(matches!(p.open(hex).await, Err(ProviderError::Backend(_))));
    }
}
