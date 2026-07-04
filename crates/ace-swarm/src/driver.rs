//! Single-peer live download loop (async I/O).
//!
//! Assumes the peer handshake + **signed** extended handshake are already done. Drives:
//! wait for unchoke → schedule piece requests ([`crate::scheduler::Scheduler`]) → request
//! each piece's chunk blocks → collect `Piece` messages into a
//! [`PieceReassembler`] → emit the contiguous byte stream. Generalises to many peers by
//! running one of these per connection sharing a scheduler; this is the per-peer core.

use crate::scheduler::{PeerView, Scheduler};
use ace_peer::session::PeerSession;
use ace_peer::{PeerError, Result};
use ace_wire::message::PeerMessage;
use ace_wire::reassembly::PieceReassembler;
use std::collections::BTreeMap;
use tokio::io::{AsyncRead, AsyncWrite};

/// What/how much to pull from the peer.
#[derive(Debug, Clone, Copy)]
pub struct DownloadParams {
    pub piece_length: u64,
    pub chunk_length: u64,
    pub start_piece: u64,
    /// Last piece index to fetch (inclusive).
    pub head: u64,
    pub max_in_flight: usize,
}

/// Download pieces `[start_piece, head]` from a single peer advertising `[peer_min,
/// peer_max]`, returning the reassembled in-order byte stream. Stops when everything
/// through `head` has been emitted, or returns what was gathered if the peer closes.
pub async fn download_from_peer<S: AsyncRead + AsyncWrite + Unpin>(
    session: &mut PeerSession<S>,
    p: DownloadParams,
    peer_min: u64,
    peer_max: u64,
) -> Result<Vec<u8>> {
    let mut sched = Scheduler::new(p.max_in_flight);
    // We only ever request `[start_piece, head]`; reject peer chunks for indices beyond that
    // window rather than buffering unsolicited far-future pieces (#13).
    let mut reasm = PieceReassembler::new(p.piece_length, p.start_piece)
        .with_max_pieces_ahead(p.head.saturating_sub(p.start_piece).saturating_add(1));
    let mut received: BTreeMap<u64, u64> = BTreeMap::new();
    let mut out = Vec::new();
    let mut unchoked = false;

    loop {
        if unchoked {
            let next = reasm.next_needed();
            let pv = PeerView {
                id: 0,
                min_piece: peer_min,
                max_piece: peer_max,
                unchoked: true,
                in_flight: sched.in_flight_count(),
            };
            for (_id, piece) in sched.assign(next, p.head, &[pv]) {
                // Request the piece as chunk_length blocks (BT request granularity).
                let mut begin = 0u64;
                while begin < p.piece_length {
                    let len = (p.piece_length - begin).min(p.chunk_length);
                    session
                        .send(&PeerMessage::Request {
                            index: piece as u32,
                            begin: begin as u32,
                            length: len as u32,
                        })
                        .await?;
                    begin += len;
                }
            }
        }

        match session.read_message().await {
            Ok(PeerMessage::Unchoke) => unchoked = true,
            Ok(PeerMessage::Choke) => unchoked = false,
            Ok(PeerMessage::Piece {
                index,
                begin,
                block,
            }) => {
                let idx = index as u64;
                let n = block.len() as u64;
                reasm.add_block(idx, begin as u64, &block)?;
                *received.entry(idx).or_insert(0) += n;
                if received.get(&idx).copied().unwrap_or(0) >= p.piece_length {
                    sched.on_complete(idx);
                    received.remove(&idx);
                }
                out.extend(reasm.take_ready());
                if reasm.next_needed() > p.head {
                    return Ok(out);
                }
            }
            Ok(_) => {} // bitfield / have / extended — not needed here
            Err(PeerError::Closed) => return Ok(out),
            Err(e) => return Err(e),
        }
    }
}
