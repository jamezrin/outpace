//! Continuous single-peer live download: request chunks for consecutive pieces, reassemble
//! contiguous MPEG-TS, and push it to an mpsc channel. Multi-peer scheduling layers on
//! later via [`crate::scheduler`].

use crate::types::StreamInfo;
use ace_peer::session::PeerSession;
use ace_peer::{PeerError, Result};
use ace_wire::identity::Identity;
use ace_wire::live_codec::{chunk_request, LiveChunk};
use ace_wire::message::PeerMessage;
use ace_wire::reassembly::PieceReassembler;
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

/// Parameters for a live download.
pub struct LiveConfig {
    pub start_piece: u64,
    /// Last piece to fetch (inclusive).
    pub head: u64,
    pub identity: Identity,
    /// Our view of the peer's IP for `yourip` (None when unknown, e.g. duplex tests).
    pub peer_ip: Option<[u8; 4]>,
}

pub struct LiveSession;

impl LiveSession {
    /// Drive the download on an already-BT-handshaken `session`: send our signed extended
    /// handshake, wait for unchoke, request every chunk of pieces `[start, head]`, and push
    /// reassembled contiguous TS to `out`. Returns when `head` is fully emitted, all
    /// receivers drop, or the peer closes.
    pub async fn run<S: AsyncRead + AsyncWrite + Unpin>(
        mut session: PeerSession<S>,
        info: StreamInfo,
        cfg: LiveConfig,
        out: mpsc::Sender<Bytes>,
    ) -> Result<()> {
        use ace_wire::extended::{LivePosition, NodeFields, OutgoingExtendedHandshake};
        let hs = OutgoingExtendedHandshake {
            ace_metadata_version: 1,
            ut_metadata_id: 2,
            mi: Some(LivePosition {
                min_piece: cfg.start_piece as i64,
                max_piece: cfg.head as i64,
                position: -1,
                distance_from_source: 1,
            }),
            node: NodeFields {
                ts: 5000,
                ..NodeFields::default()
            },
            peer_ip: cfg.peer_ip,
            metadata_size: None,
        };
        session
            .send_signed_extended_handshake(&hs, &cfg.identity)
            .await?;

        let chunks_per_piece = info.chunks_per_piece();
        // Strip each piece's signature tail, and — when the transport gave us the source's
        // pubkey — verify the piece's in-band RSA signature before emitting it (issue #10).
        let mut reasm = PieceReassembler::new(info.piece_length, cfg.start_piece)
            .with_piece_trailer(info.sig_len as u64)
            .with_source_pubkey(info.source_pubkey.clone());
        let mut unchoked = false;
        let mut requested = false;

        loop {
            if unchoked && !requested {
                for piece in cfg.start_piece..=cfg.head {
                    for chunk in 0..chunks_per_piece {
                        session.send(&chunk_request(piece as u32, chunk)).await?;
                    }
                }
                requested = true;
            }
            match session.read_message().await {
                Ok(PeerMessage::Unchoke) => unchoked = true,
                Ok(PeerMessage::Choke) => unchoked = false,
                Ok(msg @ PeerMessage::Piece { .. }) => {
                    if let Some(lc) = LiveChunk::from_message(&msg) {
                        let begin = lc.chunk as u64 * info.chunk_length;
                        reasm.add_block(lc.piece as u64, begin, &lc.data)?;
                        let ready = reasm.take_ready();
                        if !ready.is_empty() && out.send(Bytes::from(ready)).await.is_err() {
                            return Ok(()); // all receivers dropped
                        }
                        if reasm.next_needed() > cfg.head {
                            return Ok(());
                        }
                    }
                }
                Ok(_) => {}
                Err(PeerError::Closed) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }
}
