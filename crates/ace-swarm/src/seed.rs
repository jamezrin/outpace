//! Serving peers: a pure unchoke policy (`Choker`) and the `SeederSession` serve loop.
use crate::store::PieceStore;
use ace_peer::session::PeerSession;
use ace_peer::Result;
use ace_wire::live_codec::build_piece;
use ace_wire::message::PeerMessage;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Mutex;

pub struct SeederSession;

impl SeederSession {
    /// Serve one already-connected peer from `store`: advertise held pieces, then answer each
    /// Acestream chunk-request (id=6 `[stream u32][piece u32][chunk u16]`) with a `Piece` built
    /// from the store, after unchoking on the peer's first `Interested`. `piece_header` is the
    /// 8-byte per-chunk header (pinned to engine ground truth in note 21). Returns on close.
    pub async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
        session: &mut PeerSession<S>,
        store: Arc<Mutex<PieceStore>>,
        piece_header: [u8; 8],
    ) -> Result<()> {
        // Advertise what we currently hold (one Have per complete piece).
        for piece in store.lock().await.have_pieces() {
            session.send(&PeerMessage::Have(piece as u32)).await?;
        }
        let mut unchoked = false;
        loop {
            let msg = session.read_message().await?;
            match msg {
                PeerMessage::Interested if !unchoked => {
                    session.send(&PeerMessage::Unchoke).await?;
                    unchoked = true;
                }
                PeerMessage::Unknown { id: 6, payload } if payload.len() >= 10 => {
                    let piece = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
                    let chunk = u16::from_be_bytes([payload[8], payload[9]]);
                    let data = store.lock().await.chunk(piece as u64, chunk).map(|d| d.to_vec());
                    if let Some(data) = data {
                        let reply = build_piece(0, piece, chunk, piece_header, &data);
                        session.send(&reply).await?;
                    }
                    // Missing/evicted chunk: silently skip (a future task may send a reject).
                }
                _ => {}
            }
        }
    }
}

/// Decides which interested peers to unchoke. Live-appropriate: unchoke up to `max_unchoked`
/// interested peers (stable order) plus one rotating "optimistic" peer so newcomers get a turn.
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

    use crate::store::PieceStore;
    use ace_peer::session::PeerSession;
    use ace_wire::live_codec::{chunk_request, LiveChunk};
    use ace_wire::message::PeerMessage;
    use std::sync::Arc;
    use tokio::sync::Mutex;

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
        // Run the serve loop in the background; it exits on its own once the peer drops `client`.
        let serve_task = tokio::spawn(async move {
            let _ = SeederSession::serve(&mut us, store, [0u8; 8]).await;
        });
        let got = peer.await.unwrap();
        assert_eq!(got, LiveChunk { piece: 5, chunk: 0, data: vec![9, 9, 9, 9] });
        serve_task.abort(); // stop the loop if it hasn't already returned
    }
}
