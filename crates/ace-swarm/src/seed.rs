//! Serving peers: a pure unchoke policy (`Choker`) and the `SeederSession` serve loop.
use crate::store::PieceStore;
use ace_peer::session::PeerSession;
use ace_peer::Result;
use ace_wire::bencode::Bencode;
use ace_wire::extended::{ExtendedHandshake, LivePosition, NodeFields, OutgoingExtendedHandshake};
use ace_wire::identity::Identity;
use ace_wire::live_codec::{build_piece, live_bitfield};
use ace_wire::message::PeerMessage;
use std::collections::HashMap as StdHashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;
use tokio::sync::Mutex;

pub struct SeederSession;

impl SeederSession {
    /// Serve one already-connected peer from `store`/`metadata`: send our signed extended handshake
    /// advertising the store's current live window (`mi`) — **required** before a real leech
    /// client (including our own `AceProvider`, which waits for this as the peer's first
    /// message) will proceed at all — then advertise held pieces with Acestream's live
    /// bitfield, then answer each Acestream chunk-request (id=6
    /// `[stream u32][piece u32][chunk u16]`) with a `Piece` built from the store. Unchoking is
    /// gated by `coordinator` when `Some` — the peer is unchoked/choked as the coordinator's
    /// `max_unchoked` policy flips its watch, so a choked peer's chunk-requests are dropped
    /// (`Unknown { id: 6, .. }` requires `unchoked`). When `coordinator` is `None`, falls back to
    /// legacy behavior: unchoke inline on the peer's first `Interested` and never choke again.
    /// `piece_header` is the fallback 8-byte per-piece timestamp header used only when the store
    /// has no header for that piece (note 33). Returns on close.
    ///
    /// Upload accounting (bytes/peers served) is not tracked here; the S1 reciprocal seeder in
    /// `ace_provider::follow_one_peer` inlines this loop and counts via atomics. A standalone
    /// seeder built on this method (S2) will need its own counters.
    #[allow(clippy::too_many_arguments)]
    pub async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
        session: &mut PeerSession<S>,
        store: Option<Arc<Mutex<PieceStore>>>,
        metadata: Option<Arc<Vec<u8>>>,
        piece_header: [u8; 8],
        identity: &Identity,
        peer_ip: [u8; 4],
        coordinator: Option<Arc<ServeCoordinator>>,
    ) -> Result<()> {
        let debug = std::env::var_os("OUTPACE_SEED_DEBUG").is_some();
        // Advertise our identity + current live window using the profile observed from an
        // official local source node (note 32). SeederSession backs the standalone inbound
        // listener (S2) today; S1's reciprocal path inlines its own serve loop instead.
        let (min, max) = if let Some(store) = &store {
            let guard = store.lock().await;
            complete_piece_window(&guard).unwrap_or((0, 0))
        } else {
            (0, 0)
        };
        if debug {
            crate::swarm_log!(
                "[seed-session] peer={}.{}.{}.{} advertise window min={min} max={max} position={max} distance=-1",
                peer_ip[0], peer_ip[1], peer_ip[2], peer_ip[3]
            );
        }
        let hs = OutgoingExtendedHandshake {
            ace_metadata_version: 1,
            ut_metadata_id: 2,
            mi: Some(LivePosition {
                min_piece: min as i64,
                max_piece: max as i64,
                position: max as i64,
                distance_from_source: -1,
            }),
            node: NodeFields::default(),
            peer_ip: Some(peer_ip),
            metadata_size: metadata.as_ref().map(|m| m.len() as i64),
        };
        session
            .send_signed_extended_handshake(&hs, identity)
            .await?;

        // Coordinator-gated (S2 multi-peer) unchoke, or legacy inline unchoke when absent.
        let mut coord_rx = coordinator.as_ref().map(|c| c.join());
        let peer_id = coord_rx.as_ref().map(|(id, _)| *id);
        // Deregister on every exit path (error, close, drop).
        struct LeaveGuard(Option<(Arc<ServeCoordinator>, u64)>);
        impl Drop for LeaveGuard {
            fn drop(&mut self) {
                if let Some((c, id)) = self.0.take() {
                    c.leave(id);
                }
            }
        }
        let _leave = LeaveGuard(match (&coordinator, peer_id) {
            (Some(c), Some(id)) => Some((c.clone(), id)),
            _ => None,
        });
        let mut unchoked = false;
        let mut advertised_bitfields = false;
        loop {
            let msg = if let Some((_, rx)) = coord_rx.as_mut() {
                tokio::select! {
                    m = session.read_message() => m?,
                    changed = rx.changed() => {
                        // Sender dropped (entry/coordinator gone) → end the session cleanly.
                        if changed.is_err() {
                            return Ok(());
                        }
                        let want = *rx.borrow();
                        if want != unchoked {
                            unchoked = want;
                            session
                                .send(&if want { PeerMessage::Unchoke } else { PeerMessage::Choke })
                                .await?;
                            if debug {
                                crate::swarm_log!(
                                    "[seed-session] -> {}",
                                    if want { "Unchoke" } else { "Choke" }
                                );
                            }
                        }
                        continue;
                    }
                }
            } else {
                session.read_message().await?
            };
            if debug {
                crate::swarm_log!("[seed-session] <- {}", seed_message_summary(&msg));
            }
            match msg {
                PeerMessage::Extended { ext_id: 0, .. } if !advertised_bitfields => {
                    if let Some(store) = &store {
                        advertise_live_bitfields(session, store, debug).await?;
                    }
                    advertised_bitfields = true;
                }
                PeerMessage::Extended { ext_id: 2, payload } => {
                    if let Some(metadata) = &metadata {
                        if let Some(ace_wire::ut_metadata::MetadataMessage::Request { piece }) =
                            ace_wire::ut_metadata::MetadataMessage::parse(&payload)
                        {
                            let piece = piece.max(0) as usize;
                            let start = piece * ace_wire::ut_metadata::METADATA_BLOCK_LEN;
                            if start < metadata.len() {
                                let end = (start + ace_wire::ut_metadata::METADATA_BLOCK_LEN)
                                    .min(metadata.len());
                                let payload = ace_wire::ut_metadata::data_piece(
                                    piece as i64,
                                    metadata.len() as i64,
                                    &metadata[start..end],
                                );
                                session
                                    .send(&PeerMessage::Extended { ext_id: 2, payload })
                                    .await?;
                            }
                        }
                    }
                }
                PeerMessage::Interested => {
                    match (&coordinator, peer_id) {
                        (Some(c), Some(id)) => c.set_interested(id, true),
                        _ => {
                            if !unchoked {
                                session.send(&PeerMessage::Unchoke).await?;
                                unchoked = true;
                                if debug {
                                    crate::swarm_log!("[seed-session] -> Unchoke");
                                }
                            }
                        }
                    }
                }
                PeerMessage::NotInterested => {
                    if let (Some(c), Some(id)) = (&coordinator, peer_id) {
                        c.set_interested(id, false);
                    }
                }
                PeerMessage::Unknown { id: 6, payload } if payload.len() >= 10 && unchoked => {
                    // payload: [stream u32 @0..4][piece u32 @4..8][chunk u16 @8..10]
                    if let Some(store) = &store {
                        let piece =
                            u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
                        let chunk = u16::from_be_bytes([payload[8], payload[9]]);
                        let (data, header) = {
                            let guard = store.lock().await;
                            (
                                guard.chunk(piece as u64, chunk).map(|d| d.to_vec()),
                                guard.piece_header(piece as u64).unwrap_or(piece_header),
                            )
                        };
                        if let Some(data) = data {
                            let reply = build_piece(0, piece, chunk, header, &data);
                            session.send(&reply).await?;
                            if debug {
                                crate::swarm_log!(
                                    "[seed-session] -> Piece stream=0 piece={piece} chunk={chunk} bytes={}",
                                    data.len()
                                );
                            }
                        } else if debug {
                            crate::swarm_log!("[seed-session] miss piece={piece} chunk={chunk}");
                        }
                    }
                    // Missing/evicted chunk: silently skip (a future task may send a reject).
                }
                _ => {}
            }
        }
    }
}

async fn advertise_live_bitfields<S: AsyncRead + AsyncWrite + Unpin>(
    session: &mut PeerSession<S>,
    store: &Arc<Mutex<PieceStore>>,
    debug: bool,
) -> Result<()> {
    let pieces = store.lock().await.have_pieces();
    if debug && !pieces.is_empty() {
        crate::swarm_log!(
            "[seed-session] -> live Bitfield for {} complete piece(s)",
            pieces.len()
        );
    }
    let mut ranges = pieces.into_iter().peekable();
    while let Some(first) = ranges.next() {
        let mut last = first;
        while matches!(ranges.peek(), Some(next) if *next == last + 1) {
            last = ranges.next().expect("peeked");
        }
        let count = (last - first + 1).min(u32::MAX as u64) as u32;
        session.send(&live_bitfield(first as u32, count)).await?;
    }
    Ok(())
}

fn complete_piece_window(store: &PieceStore) -> Option<(u64, u64)> {
    let pieces = store.have_pieces();
    Some((*pieces.first()?, *pieces.last()?))
}

fn seed_message_summary(msg: &PeerMessage) -> String {
    match msg {
        PeerMessage::KeepAlive => "KeepAlive".to_string(),
        PeerMessage::Choke => "Choke".to_string(),
        PeerMessage::Unchoke => "Unchoke".to_string(),
        PeerMessage::Interested => "Interested".to_string(),
        PeerMessage::NotInterested => "NotInterested".to_string(),
        PeerMessage::Have(piece) => format!("Have piece={piece}"),
        PeerMessage::Bitfield(bytes) => format!("Bitfield bytes={}", bytes.len()),
        PeerMessage::Request {
            index,
            begin,
            length,
        } => {
            format!("BT Request index={index} begin={begin} length={length}")
        }
        PeerMessage::Piece {
            index,
            begin,
            block,
        } => {
            format!("Piece index={index} begin={begin} bytes={}", block.len())
        }
        PeerMessage::Cancel {
            index,
            begin,
            length,
        } => {
            format!("Cancel index={index} begin={begin} length={length}")
        }
        PeerMessage::Extended { ext_id, payload } if *ext_id == 0 => {
            match ExtendedHandshake::parse(payload) {
                Ok(eh) => format!("ExtendedHandshake {}", extended_summary(&eh)),
                Err(_) => format!("ExtendedHandshake bytes={} parse=err", payload.len()),
            }
        }
        PeerMessage::Extended { ext_id, payload } => {
            format!(
                "Extended ext_id={ext_id} bytes={} head={}",
                payload.len(),
                hex_preview(payload)
            )
        }
        PeerMessage::Unknown { id: 6, payload } if payload.len() >= 10 => {
            let stream = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            let piece = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
            let chunk = u16::from_be_bytes([payload[8], payload[9]]);
            format!(
                "ACE Request stream={stream} piece={piece} chunk={chunk} bytes={}",
                payload.len()
            )
        }
        PeerMessage::Unknown { id, payload } => {
            format!(
                "Unknown id={id} bytes={} head={}",
                payload.len(),
                hex_preview(payload)
            )
        }
    }
}

fn extended_summary(eh: &ExtendedHandshake) -> String {
    let top = |k: &[u8]| eh.raw.get(k).and_then(Bencode::as_int);
    let mi = eh.raw.get(b"mi");
    let mi_int = |k: &[u8]| mi.and_then(|m| m.get(k)).and_then(Bencode::as_int);
    format!(
        "bytes ace_metadata={:?} ut_metadata={:?} ts={:?} p={:?} mi[min={:?} max={:?} pos={:?} dist={:?}]",
        eh.ace_metadata_version,
        eh.ut_metadata_id(),
        top(b"ts"),
        top(b"p"),
        mi_int(b"min_piece"),
        mi_int(b"max_piece"),
        mi_int(b"position"),
        mi_int(b"distance_from_source"),
    )
}

fn hex_preview(bytes: &[u8]) -> String {
    bytes.iter().take(256).map(|b| format!("{b:02x}")).collect()
}

/// Decides which interested peers to unchoke. Live-appropriate: unchoke up to `max_unchoked`
/// interested peers (stable order) plus one rotating "optimistic" peer so newcomers get a turn.
///
/// S2: invoked by the multi-peer serve coordinator. The S1 reciprocal path serves a single
/// peer and unchokes it inline, so this policy has no production caller yet.
pub struct Choker {
    max_unchoked: usize,
}

impl Choker {
    pub fn new(max_unchoked: usize) -> Self {
        Choker { max_unchoked }
    }

    /// Peers to unchoke now. `interested` is the current interested set (caller-stable order);
    /// `tick` rotates the optimistic slot over time.
    pub fn choose(&self, interested: &[u64], tick: u64) -> Vec<u64> {
        let mut out: Vec<u64> = interested.iter().take(self.max_unchoked).copied().collect();
        let rest = &interested[out.len()..];
        if !rest.is_empty() {
            out.push(rest[(tick as usize) % rest.len()]);
        }
        out
    }
}

/// Per-infohash multi-peer serve coordinator. Tracks every inbound connection for one stream and
/// applies [`Choker`] so no more than `max_unchoked` (+1 rotating optimistic) peers are unchoked at
/// once. Each connection observes a `watch<bool>` (true = unchoked) and sends Choke/Unchoke on the
/// wire when it flips. Recompute runs on every interest change, peer leave, and rechoke tick.
pub struct ServeCoordinator {
    choker: Choker,
    next_id: AtomicU64,
    state: StdMutex<CoordState>,
}

#[derive(Default)]
struct CoordState {
    /// Interested peers in stable (join) order — the order `Choker::choose` consumes.
    interested: Vec<u64>,
    /// Per-peer unchoke signal sender.
    senders: StdHashMap<u64, watch::Sender<bool>>,
    tick: u64,
}

impl ServeCoordinator {
    pub fn new(max_unchoked: usize) -> Arc<Self> {
        Arc::new(ServeCoordinator {
            choker: Choker::new(max_unchoked),
            next_id: AtomicU64::new(1),
            state: StdMutex::new(CoordState::default()),
        })
    }

    /// Register a connection. Returns its peer id and a receiver that reports its unchoke state
    /// (starts choked). Call [`leave`](Self::leave) when the connection ends.
    pub fn join(&self) -> (u64, watch::Receiver<bool>) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = watch::channel(false);
        self.state.lock().unwrap().senders.insert(id, tx);
        (id, rx)
    }

    /// Report a peer's interest. Recomputes the unchoked set.
    pub fn set_interested(&self, peer: u64, interested: bool) {
        {
            let mut st = self.state.lock().unwrap();
            let present = st.interested.iter().position(|&p| p == peer);
            match (interested, present) {
                (true, None) => st.interested.push(peer),
                (false, Some(i)) => {
                    st.interested.remove(i);
                }
                _ => {}
            }
        }
        self.recompute();
    }

    /// Deregister a connection (also drops it from the interested set). Recomputes.
    pub fn leave(&self, peer: u64) {
        {
            let mut st = self.state.lock().unwrap();
            st.senders.remove(&peer);
            if let Some(i) = st.interested.iter().position(|&p| p == peer) {
                st.interested.remove(i);
            }
        }
        self.recompute();
    }

    /// Advance the optimistic-unchoke rotation and recompute. Called periodically by the listener's
    /// rechoke ticker (via `SeedRegistry::rechoke_all`).
    pub fn rechoke(&self) {
        self.state.lock().unwrap().tick += 1;
        self.recompute();
    }

    /// Apply the choker and flip each peer's watch to its desired state. `watch::send` only wakes
    /// receivers when the value actually changes, so unchanged peers cost nothing.
    fn recompute(&self) {
        let st = self.state.lock().unwrap();
        let chosen = self.choker.choose(&st.interested, st.tick);
        for (id, tx) in st.senders.iter() {
            let want = chosen.contains(id);
            if *tx.borrow() != want {
                let _ = tx.send(want);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unchokes_up_to_max_plus_one_optimistic() {
        let c = Choker::new(2);
        // first 2 always unchoked; the 3rd slot rotates through the remainder by tick.
        assert_eq!(c.choose(&[10, 20, 30, 40], 0), vec![10, 20, 30]);
        assert_eq!(c.choose(&[10, 20, 30, 40], 1), vec![10, 20, 40]);
        assert_eq!(c.choose(&[10, 20, 30, 40], 2), vec![10, 20, 30]); // wraps
    }

    #[test]
    fn fewer_interested_than_max_unchokes_all() {
        let c = Choker::new(4);
        assert_eq!(c.choose(&[10, 20], 0), vec![10, 20]);
        assert_eq!(c.choose(&[], 0), Vec::<u64>::new());
    }

    #[test]
    fn coordinator_never_unchokes_more_than_max_plus_optimistic() {
        let coord = ServeCoordinator::new(2);
        let mut rxs = Vec::new();
        let mut ids = Vec::new();
        for _ in 0..5 {
            let (id, rx) = coord.join();
            ids.push(id);
            rxs.push(rx);
        }
        for id in &ids {
            coord.set_interested(*id, true);
        }
        let unchoked = rxs.iter().filter(|rx| *rx.borrow()).count();
        assert!(unchoked <= 3, "max_unchoked(2) + 1 optimistic = 3, got {unchoked}");
        assert!(unchoked >= 2, "should unchoke up to the cap when enough are interested");
    }

    #[test]
    fn coordinator_unchokes_a_single_interested_peer() {
        let coord = ServeCoordinator::new(4);
        let (id, rx) = coord.join();
        assert!(!*rx.borrow(), "not unchoked before Interested");
        coord.set_interested(id, true);
        assert!(*rx.borrow(), "unchoked after Interested when under the cap");
    }

    #[test]
    fn coordinator_rechokes_when_a_peer_leaves() {
        // max_unchoked = 1 with three interested peers: `a` takes the guaranteed slot; the single
        // optimistic slot at tick 0 goes to the first of the remainder (`b`), so `c` is CHOKED.
        let coord = ServeCoordinator::new(1);
        let (a, rx_a) = coord.join();
        let (b, _rx_b) = coord.join();
        let (c, rx_c) = coord.join();
        coord.set_interested(a, true);
        coord.set_interested(b, true);
        coord.set_interested(c, true);
        assert!(*rx_a.borrow(), "first-come peer holds the guaranteed slot");
        assert!(!*rx_c.borrow(), "third peer is choked (past max + optimistic)");
        // When `a` leaves, the guaranteed slot frees up and `c` must get unchoked.
        coord.leave(a);
        assert!(*rx_c.borrow(), "a genuinely-choked peer becomes unchoked after another leaves");
    }

    #[test]
    fn debug_summary_keeps_full_short_unknown_payload() {
        let payload = (0u8..20).collect::<Vec<_>>();
        let summary = seed_message_summary(&PeerMessage::Unknown { id: 11, payload });

        assert!(
            summary.contains("head=000102030405060708090a0b0c0d0e0f10111213"),
            "short diagnostics should include enough payload to decode id=11 stats: {summary}"
        );
    }

    use crate::store::PieceStore;
    use ace_peer::session::PeerSession;
    use ace_wire::extended::ExtendedHandshake;
    use ace_wire::live_codec::{chunk_request, LiveChunk};
    use ace_wire::message::PeerMessage;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn serves_ut_metadata_piece_when_metadata_is_registered() {
        use ace_wire::ut_metadata::{request_piece, MetadataMessage};
        use tokio::io::duplex;

        let metadata = Arc::new(b"AceStreamTransport-metadata".to_vec());
        let (client, server) = duplex(4096);
        let identity = Arc::new(ace_wire::identity::Identity::from_seed([7u8; 32]));
        let mut server_session = PeerSession::new(server);
        let mut client_session = PeerSession::new(client);

        let server_task = tokio::spawn(async move {
            SeederSession::serve(
                &mut server_session,
                None,
                Some(metadata),
                [0u8; 8],
                &identity,
                [127, 0, 0, 1],
                None,
            )
            .await
        });

        let msg = client_session.read_message().await.unwrap();
        let PeerMessage::Extended { ext_id: 0, payload } = msg else {
            panic!("expected extended handshake");
        };
        let parsed = ExtendedHandshake::parse(&payload).unwrap();
        assert_eq!(parsed.metadata_size(), Some(27));

        client_session
            .send(&PeerMessage::Extended {
                ext_id: 2,
                payload: request_piece(0),
            })
            .await
            .unwrap();
        let msg = client_session.read_message().await.unwrap();
        let PeerMessage::Extended { ext_id: 2, payload } = msg else {
            panic!("expected ut_metadata data");
        };
        match MetadataMessage::parse(&payload).unwrap() {
            MetadataMessage::Data { piece, data, .. } => {
                assert_eq!(piece, 0);
                assert_eq!(data, b"AceStreamTransport-metadata");
            }
            other => panic!("expected data, got {other:?}"),
        }

        drop(client_session);
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn serve_advertises_complete_piece_window_in_extended_handshake() {
        let store = Arc::new(Mutex::new(PieceStore::new(8, 4, 1024)));
        store.lock().await.put_chunk(5, 0, &[5, 5, 5, 5]);
        store.lock().await.put_chunk(5, 1, &[5, 5, 5, 5]);
        store.lock().await.put_chunk(6, 0, &[6, 6, 6, 6]); // partial head, not available.

        let (client, server) = tokio::io::duplex(64 * 1024);
        let identity = Identity::generate();
        let mut us = PeerSession::new(server);
        let serve_store = store.clone();
        let serve_task = tokio::spawn(async move {
            let _ = SeederSession::serve(
                &mut us,
                Some(serve_store),
                None,
                [0u8; 8],
                &identity,
                [127, 0, 0, 1],
                None,
            )
            .await;
        });

        let mut peer = PeerSession::new(client).with_timeout(Duration::from_millis(30));
        let PeerMessage::Extended { ext_id: 0, payload } = peer.read_message().await.unwrap()
        else {
            panic!("expected signed extended handshake");
        };
        let handshake = ExtendedHandshake::parse(&payload).unwrap();
        let mi = handshake.raw.get(b"mi").expect("mi");

        assert_eq!(mi.get(b"min_piece").and_then(Bencode::as_int), Some(5));
        assert_eq!(mi.get(b"max_piece").and_then(Bencode::as_int), Some(5));
        assert_eq!(
            mi.get(b"download_window_end").and_then(Bencode::as_int),
            Some(5),
            "mi must not move the live end onto a partial head piece"
        );

        serve_task.abort();
    }

    #[tokio::test]
    async fn serve_advertises_official_source_window_in_extended_handshake() {
        let store = Arc::new(Mutex::new(PieceStore::new(8, 4, 1024)));
        for piece in 20..=25 {
            store.lock().await.put_chunk(piece, 0, &[piece as u8; 4]);
            store.lock().await.put_chunk(piece, 1, &[piece as u8; 4]);
        }

        let (client, server) = tokio::io::duplex(64 * 1024);
        let identity = Identity::generate();
        let mut us = PeerSession::new(server);
        let serve_store = store.clone();
        let serve_task = tokio::spawn(async move {
            let _ = SeederSession::serve(
                &mut us,
                Some(serve_store),
                None,
                [0u8; 8],
                &identity,
                [127, 0, 0, 1],
                None,
            )
            .await;
        });

        let mut peer = PeerSession::new(client).with_timeout(Duration::from_millis(30));
        let PeerMessage::Extended { ext_id: 0, payload } = peer.read_message().await.unwrap()
        else {
            panic!("expected signed extended handshake");
        };
        let handshake = ExtendedHandshake::parse(&payload).unwrap();
        let mi = handshake.raw.get(b"mi").expect("mi");

        assert_eq!(mi.get(b"min_piece").and_then(Bencode::as_int), Some(20));
        assert_eq!(mi.get(b"max_piece").and_then(Bencode::as_int), Some(25));
        assert_eq!(
            mi.get(b"position").and_then(Bencode::as_int),
            Some(25),
            "official source nodes advertise their current position at max_piece"
        );
        assert_eq!(
            mi.get(b"distance_from_source").and_then(Bencode::as_int),
            Some(-1)
        );
        assert_eq!(mi.get(b"is_accessible").and_then(Bencode::as_int), Some(0));
        assert_eq!(
            mi.get(b"download_window_end").and_then(Bencode::as_int),
            Some(25),
            "official source nodes advertise download_window_end at max_piece"
        );
        assert_eq!(mi.get(b"lsp").and_then(Bencode::as_int), Some(25));
        assert_eq!(
            handshake.raw.get(b"lsp").and_then(Bencode::as_int),
            Some(25)
        );
        assert!(
            handshake.raw.get(b"node_state").is_some(),
            "official source-node handshakes include node_state"
        );
        assert_eq!(
            mi.get(b"live_window_size").and_then(Bencode::as_int),
            Some(115)
        );
        assert_eq!(
            mi.get(b"ping_from_source").and_then(Bencode::as_int),
            Some(-1)
        );

        serve_task.abort();
    }

    #[tokio::test]
    async fn serve_advertises_live_bitfield_before_peer_is_interested() {
        let store = Arc::new(Mutex::new(PieceStore::new(4, 4, 1024)));
        store.lock().await.put_chunk(5, 0, &[9, 9, 9, 9]);

        let (client, server) = tokio::io::duplex(64 * 1024);
        let identity = Identity::generate();

        let mut us = PeerSession::new(server);
        let serve_store = store.clone();
        let serve_task = tokio::spawn(async move {
            let _ = SeederSession::serve(
                &mut us,
                Some(serve_store),
                None,
                [0u8; 8],
                &identity,
                [127, 0, 0, 1],
                None,
            )
            .await;
        });

        let mut peer = PeerSession::new(client).with_timeout(Duration::from_millis(30));
        assert!(
            matches!(
                peer.read_message().await.unwrap(),
                PeerMessage::Extended { ext_id: 0, .. }
            ),
            "the signed extended handshake is still the first seeder message"
        );
        assert!(
            peer.read_message().await.is_err(),
            "seeder must wait for the peer's extended handshake before sending availability"
        );
        peer.send(&PeerMessage::Extended {
            ext_id: 0,
            payload: b"d1:md11:ut_metadatai2eee".to_vec(),
        })
        .await
        .unwrap();
        assert_eq!(
            peer.read_message().await.unwrap(),
            PeerMessage::Bitfield(vec![0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 0x80]),
            "official leechers expect Acestream's live bitfield before they express interest"
        );
        assert!(
            peer.read_message().await.is_err(),
            "seeder must not send standard BT Have advertisements before the peer expresses interest"
        );

        peer.send(&PeerMessage::Interested).await.unwrap();
        assert_eq!(
            peer.read_message().await.unwrap(),
            PeerMessage::Unchoke,
            "interested peer should be unchoked"
        );
        assert!(
            peer.read_message().await.is_err(),
            "live availability is already carried by id=5; standard BT Have bursts make the official engine disconnect"
        );
        serve_task.abort();
    }

    #[tokio::test]
    async fn coordinator_gated_serve_unchokes_only_when_chosen() {
        use tokio::time::timeout;

        let store = Arc::new(Mutex::new(PieceStore::new(4, 4, 1024)));
        store.lock().await.put_chunk(5, 0, &[9, 9, 9, 9]);

        let coord = ServeCoordinator::new(1);
        let (client, server) = tokio::io::duplex(64 * 1024);
        let identity = Identity::generate();
        let mut server_session = PeerSession::new(server);
        let mut client_session = PeerSession::new(client);

        let srv = tokio::spawn(async move {
            SeederSession::serve(
                &mut server_session,
                Some(store),
                None,
                [0u8; 8],
                &identity,
                [0, 0, 0, 0],
                Some(coord),
            )
            .await
        });

        // Signed extended handshake first, as always.
        let msg = timeout(Duration::from_millis(500), client_session.read_message())
            .await
            .expect("timed out waiting for extended handshake")
            .unwrap();
        assert!(matches!(msg, PeerMessage::Extended { ext_id: 0, .. }));

        // The peer's own extended handshake triggers the live-bitfield advertisement.
        client_session
            .send(&PeerMessage::Extended {
                ext_id: 0,
                payload: b"d1:md11:ut_metadatai2eee".to_vec(),
            })
            .await
            .unwrap();
        let msg = timeout(Duration::from_millis(500), client_session.read_message())
            .await
            .expect("timed out waiting for bitfield")
            .unwrap();
        assert!(matches!(msg, PeerMessage::Bitfield(_)));

        client_session
            .send(&PeerMessage::Interested)
            .await
            .unwrap();
        let msg = timeout(Duration::from_millis(500), client_session.read_message())
            .await
            .expect("timed out waiting for Unchoke")
            .unwrap();
        assert_eq!(
            msg,
            PeerMessage::Unchoke,
            "the lone interested peer, chosen by the coordinator under max_unchoked=1, must be unchoked"
        );

        srv.abort();
    }

    #[tokio::test]
    async fn coordinator_gated_serve_keeps_a_non_chosen_peer_choked() {
        use tokio::time::timeout;

        // max_unchoked = 0: no guaranteed slot; `Choker::choose` fills only the single rotating
        // optimistic slot, which at tick 0 goes to the FIRST interested peer. Peer A becomes
        // interested first (and is fully driven to its Unchoke before B even sends Interested), so
        // A deterministically holds the optimistic slot and B must stay choked over the real wire.
        let coord = ServeCoordinator::new(0);

        // Spin up one serve session and drive its client past the handshake to the point where the
        // next message the seeder would send is the Choke/Unchoke reaction to interest. Returns the
        // client session with A/B's own extended handshake already exchanged and bitfield consumed.
        async fn drive_to_interest_ready(
            coord: Arc<ServeCoordinator>,
        ) -> (
            PeerSession<tokio::io::DuplexStream>,
            tokio::task::JoinHandle<Result<()>>,
        ) {
            let store = Arc::new(Mutex::new(PieceStore::new(4, 4, 1024)));
            store.lock().await.put_chunk(5, 0, &[9, 9, 9, 9]);
            let identity = Identity::generate();
            let (client, server) = tokio::io::duplex(64 * 1024);
            let mut server_session = PeerSession::new(server);
            let mut client_session = PeerSession::new(client);

            let srv = tokio::spawn(async move {
                SeederSession::serve(
                    &mut server_session,
                    Some(store),
                    None,
                    [0u8; 8],
                    &identity,
                    [0, 0, 0, 0],
                    Some(coord),
                )
                .await
            });

            let msg = timeout(Duration::from_millis(500), client_session.read_message())
                .await
                .expect("timed out waiting for extended handshake")
                .unwrap();
            assert!(matches!(msg, PeerMessage::Extended { ext_id: 0, .. }));
            client_session
                .send(&PeerMessage::Extended {
                    ext_id: 0,
                    payload: b"d1:md11:ut_metadatai2eee".to_vec(),
                })
                .await
                .unwrap();
            let msg = timeout(Duration::from_millis(500), client_session.read_message())
                .await
                .expect("timed out waiting for bitfield")
                .unwrap();
            assert!(matches!(msg, PeerMessage::Bitfield(_)));

            (client_session, srv)
        }

        // Bring A fully up and INTERESTED first, and confirm it is unchoked — so A deterministically
        // occupies the sole optimistic slot before B expresses any interest.
        let (mut a, srv_a) = drive_to_interest_ready(coord.clone()).await;
        a.send(&PeerMessage::Interested).await.unwrap();
        let msg = timeout(Duration::from_millis(500), a.read_message())
            .await
            .expect("timed out waiting for peer A's Unchoke")
            .unwrap();
        assert_eq!(
            msg,
            PeerMessage::Unchoke,
            "peer A holds the single optimistic slot and must be unchoked"
        );

        // Now bring B up and interested. With A already in the optimistic slot, B is beyond capacity
        // and must NOT be unchoked — assert no Unchoke arrives within a generous window.
        let (mut b, srv_b) = drive_to_interest_ready(coord.clone()).await;
        b.send(&PeerMessage::Interested).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
        loop {
            match timeout(deadline - tokio::time::Instant::now(), b.read_message()).await {
                Err(_) => break, // window elapsed with no Unchoke — the choked peer stayed choked.
                Ok(Ok(PeerMessage::Unchoke)) => {
                    panic!("peer B is beyond capacity (max_unchoked=0) and must stay choked");
                }
                Ok(Ok(_)) => continue, // ignore any other frame the seeder may emit
                Ok(Err(_)) => break,   // stream closed — also no Unchoke observed.
            }
        }

        srv_a.abort();
        srv_b.abort();
    }

    #[tokio::test]
    async fn serves_a_requested_chunk_from_the_store() {
        // Store holds piece 5, chunk 0 = [9,9,9,9] (geometry: 4-byte chunks, 1 chunk/piece).
        let store = Arc::new(Mutex::new(PieceStore::new(4, 4, 1024)));
        store.lock().await.put_chunk(5, 0, &[9, 9, 9, 9]);

        let (client, server) = tokio::io::duplex(64 * 1024);

        // The "peer": expresses interest, requests (piece 5, chunk 0), reads back the Piece.
        let peer = tokio::spawn(async move {
            let mut p = PeerSession::new(client);
            p.send(&PeerMessage::Interested).await.unwrap();
            p.send(&chunk_request(5, 0)).await.unwrap();
            loop {
                match p.read_message().await.unwrap() {
                    m @ PeerMessage::Piece { .. } => {
                        return LiveChunk::from_message(&m).unwrap();
                    }
                    _ => continue, // skip Unchoke / advertisements
                }
            }
        });

        // Our seeder serves the peer until it closes.
        let mut us = PeerSession::new(server);
        let identity = Identity::generate();
        // Run the serve loop in the background; it exits on its own once the peer drops `client`.
        let serve_task = tokio::spawn(async move {
            let _ = SeederSession::serve(
                &mut us,
                Some(store),
                None,
                [0u8; 8],
                &identity,
                [127, 0, 0, 1],
                None,
            )
            .await;
        });
        let got = peer.await.unwrap();
        assert_eq!(
            got,
            LiveChunk {
                piece: 5,
                piece_header: [0u8; 8],
                chunk: 0,
                data: vec![9, 9, 9, 9]
            }
        );
        serve_task.abort(); // stop the loop if it hasn't already returned
    }

    #[tokio::test]
    async fn serves_the_piece_specific_header_from_the_store() {
        let store = Arc::new(Mutex::new(PieceStore::new(4, 4, 1024)));
        let header = [0x41, 0xda, 0x91, 0x52, 0x26, 0x34, 0xc2, 0xee];
        store
            .lock()
            .await
            .put_chunk_with_header(5, 0, header, &[9, 9, 9, 9]);

        let (client, server) = tokio::io::duplex(64 * 1024);
        let peer = tokio::spawn(async move {
            let mut p = PeerSession::new(client);
            p.send(&PeerMessage::Interested).await.unwrap();
            p.send(&chunk_request(5, 0)).await.unwrap();
            loop {
                match p.read_message().await.unwrap() {
                    m @ PeerMessage::Piece { .. } => {
                        return LiveChunk::from_message(&m).unwrap();
                    }
                    _ => continue,
                }
            }
        });

        let mut us = PeerSession::new(server);
        let identity = Identity::generate();
        let serve_task = tokio::spawn(async move {
            let _ = SeederSession::serve(
                &mut us,
                Some(store),
                None,
                [0u8; 8],
                &identity,
                [127, 0, 0, 1],
                None,
            )
            .await;
        });
        let got = peer.await.unwrap();

        assert_eq!(got.piece_header, header);
        serve_task.abort();
    }
}
