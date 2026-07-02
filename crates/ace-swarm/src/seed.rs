//! Serving peers: a pure unchoke policy (`Choker`) and the `SeederSession` serve loop.
use crate::store::PieceStore;
use ace_peer::session::PeerSession;
use ace_peer::Result;
use ace_wire::bencode::Bencode;
use ace_wire::extended::{ExtendedHandshake, LivePosition, NodeFields, OutgoingExtendedHandshake};
use ace_wire::identity::Identity;
use ace_wire::live_codec::{build_piece, live_bitfield};
use ace_wire::message::PeerMessage;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Mutex;

pub struct SeederSession;

impl SeederSession {
    /// Serve one already-connected peer from `store`: send our signed extended handshake
    /// advertising the store's current live window (`mi`) — **required** before a real leech
    /// client (including our own `AceProvider`, which waits for this as the peer's first
    /// message) will proceed at all — then advertise held pieces with Acestream's live
    /// bitfield, then answer each Acestream chunk-request (id=6
    /// `[stream u32][piece u32][chunk u16]`) with a `Piece` built from the store, after
    /// unchoking on the peer's first `Interested`. `piece_header` is the fallback 8-byte
    /// per-piece timestamp header used only when the store has no header for that piece
    /// (note 33). Returns on close.
    ///
    /// Upload accounting (bytes/peers served) is not tracked here; the S1 reciprocal seeder in
    /// `ace_provider::follow_one_peer` inlines this loop and counts via atomics. A standalone
    /// seeder built on this method (S2) will need its own counters.
    #[allow(clippy::too_many_arguments)]
    pub async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
        session: &mut PeerSession<S>,
        store: Arc<Mutex<PieceStore>>,
        piece_header: [u8; 8],
        identity: &Identity,
        peer_ip: [u8; 4],
    ) -> Result<()> {
        let debug = std::env::var_os("OUTPACE_SEED_DEBUG").is_some();
        // Advertise our identity + current live window using the profile observed from an
        // official local source node (note 32). SeederSession backs the standalone inbound
        // listener (S2) today; S1's reciprocal path inlines its own serve loop instead.
        let (min, max) = {
            let guard = store.lock().await;
            complete_piece_window(&guard).unwrap_or((0, 0))
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
        };
        session
            .send_signed_extended_handshake(&hs, identity)
            .await?;

        let mut unchoked = false;
        let mut advertised_bitfields = false;
        loop {
            let msg = session.read_message().await?;
            if debug {
                crate::swarm_log!("[seed-session] <- {}", seed_message_summary(&msg));
            }
            match msg {
                PeerMessage::Extended { ext_id: 0, .. } if !advertised_bitfields => {
                    advertise_live_bitfields(session, &store, debug).await?;
                    advertised_bitfields = true;
                }
                PeerMessage::Interested if !unchoked => {
                    session.send(&PeerMessage::Unchoke).await?;
                    unchoked = true;
                    if debug {
                        crate::swarm_log!("[seed-session] -> Unchoke");
                    }
                }
                PeerMessage::Unknown { id: 6, payload } if payload.len() >= 10 => {
                    // payload: [stream u32 @0..4][piece u32 @4..8][chunk u16 @8..10]
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
            let _ = SeederSession::serve(&mut us, serve_store, [0u8; 8], &identity, [127, 0, 0, 1])
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
            let _ = SeederSession::serve(&mut us, serve_store, [0u8; 8], &identity, [127, 0, 0, 1])
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
            let _ = SeederSession::serve(&mut us, serve_store, [0u8; 8], &identity, [127, 0, 0, 1])
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
            let _ = SeederSession::serve(&mut us, store, [0u8; 8], &identity, [127, 0, 0, 1]).await;
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
            let _ = SeederSession::serve(&mut us, store, [0u8; 8], &identity, [127, 0, 0, 1]).await;
        });
        let got = peer.await.unwrap();

        assert_eq!(got.piece_header, header);
        serve_task.abort();
    }
}
