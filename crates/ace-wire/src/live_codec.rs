//! Acestream live chunk request/piece wire helpers (note 19).
//!
//! Request: peer message id=6 with payload `[stream u32=0][piece u32][chunk u16]`.
//! Piece:   peer message id=7 with payload `[stream u32][piece u32][8B piece hdr][chunk u16]
//!          [16384 data]`, which our decoder surfaces as
//!          `PeerMessage::Piece { index=stream, begin=piece, block }`.

use crate::message::PeerMessage;

/// Build the Acestream chunk-request message for `(piece, chunk)`.
pub fn chunk_request(piece: u32, chunk: u16) -> PeerMessage {
    let mut payload = vec![0u8, 0, 0, 0]; // stream index = 0
    payload.extend_from_slice(&piece.to_be_bytes());
    payload.extend_from_slice(&chunk.to_be_bytes());
    PeerMessage::Unknown { id: 6, payload }
}

/// Build the Acestream live `Piece` message (id=7) to SEND, the inverse of [`LiveChunk`]:
/// payload `[stream u32][piece u32][8B piece header][chunk u16][data]`. The 8-byte
/// `piece_header` is the engine's per-chunk header (pinned to ground truth in note 21; for a
/// broadcast source it is synthesized in B0/B1).
pub fn build_piece(stream: u32, piece: u32, chunk: u16, piece_header: [u8; 8], data: &[u8]) -> PeerMessage {
    let mut block = Vec::with_capacity(8 + 2 + data.len());
    block.extend_from_slice(&piece_header);
    block.extend_from_slice(&chunk.to_be_bytes());
    block.extend_from_slice(data);
    PeerMessage::Piece { index: stream, begin: piece, block }
}

/// A received live chunk: its piece/chunk coordinates and the TS payload.
#[derive(Debug, PartialEq, Eq)]
pub struct LiveChunk {
    pub piece: u32,
    pub chunk: u16,
    pub data: Vec<u8>,
}

impl LiveChunk {
    /// Interpret a decoded `Piece` message as a live chunk: `begin` = piece, `block` =
    /// `[8B piece hdr][chunk u16][data]`. Returns None if not a piece / block too short.
    pub fn from_message(msg: &PeerMessage) -> Option<LiveChunk> {
        if let PeerMessage::Piece { begin, block, .. } = msg {
            if block.len() < 10 {
                return None;
            }
            let chunk = u16::from_be_bytes([block[8], block[9]]);
            Some(LiveChunk { piece: *begin, chunk, data: block[10..].to_vec() })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_has_acestream_10_byte_payload() {
        let m = chunk_request(0x005067f8, 3);
        match m {
            PeerMessage::Unknown { id, payload } => {
                assert_eq!(id, 6);
                assert_eq!(payload, vec![0, 0, 0, 0, 0x00, 0x50, 0x67, 0xf8, 0x00, 0x03]);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn parse_piece_into_live_chunk() {
        let mut block = vec![0xAAu8; 8]; // 8-byte piece header
        block.extend_from_slice(&7u16.to_be_bytes()); // chunk 7
        block.extend_from_slice(&[1, 2, 3, 4]); // data
        let msg = PeerMessage::Piece { index: 0, begin: 5269621, block };
        let lc = LiveChunk::from_message(&msg).unwrap();
        assert_eq!(lc, LiveChunk { piece: 5269621, chunk: 7, data: vec![1, 2, 3, 4] });
    }

    #[test]
    fn non_piece_message_is_none() {
        assert!(LiveChunk::from_message(&PeerMessage::Unchoke).is_none());
    }

    #[test]
    fn build_piece_roundtrips_through_live_chunk() {
        // What we SEND must decode back to the same (piece, chunk, data) a peer would read.
        let data = [1u8, 2, 3, 4];
        let msg = build_piece(0, 5269621, 7, [0xAB; 8], &data);
        match &msg {
            PeerMessage::Piece { index, begin, block } => {
                assert_eq!(*index, 0); // stream
                assert_eq!(*begin, 5269621); // piece
                assert_eq!(&block[..8], &[0xAB; 8]); // 8-byte piece header
                assert_eq!(&block[8..10], &7u16.to_be_bytes()); // chunk
                assert_eq!(&block[10..], &data); // payload
            }
            _ => panic!("expected Piece"),
        }
        let lc = LiveChunk::from_message(&msg).unwrap();
        assert_eq!(lc, LiveChunk { piece: 5269621, chunk: 7, data: data.to_vec() });
    }
}
