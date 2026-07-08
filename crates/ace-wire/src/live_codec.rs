//! Acestream live chunk request/piece wire helpers (note 19).
//!
//! Request: peer message id=6 with payload `[stream u32=0][piece u32][chunk u16]`.
//! Piece:   peer message id=7 with payload `[stream u32][piece u32][8B piece hdr][chunk u16]
//!          [16384 data]`, which our decoder surfaces as
//!          `PeerMessage::Piece { index=stream, begin=piece, block }`.

use crate::message::PeerMessage;

/// Build Acestream's 8-byte live piece header from a Unix timestamp. Official source nodes
/// encode this as a big-endian IEEE-754 `f64` (note 33).
pub fn piece_header_from_unix_seconds(seconds: f64) -> [u8; 8] {
    seconds.to_be_bytes()
}

/// Build the Acestream chunk-request message for `(piece, chunk)`.
pub fn chunk_request(piece: u32, chunk: u16) -> PeerMessage {
    let mut payload = vec![0u8, 0, 0, 0]; // stream index = 0
    payload.extend_from_slice(&piece.to_be_bytes());
    payload.extend_from_slice(&chunk.to_be_bytes());
    PeerMessage::Unknown { id: 6, payload }
}

/// Build Acestream's live request rejection for `(stream, piece, chunk)`.
///
/// BEP-6 `reject_request` uses message id 16. Acestream live requests use a custom
/// 10-byte stream/piece/chunk payload rather than the standard 12-byte BitTorrent
/// piece/begin/length tuple, so this stays a raw peer message.
pub fn reject_chunk_request(stream: u32, piece: u32, chunk: u16) -> PeerMessage {
    let mut payload = Vec::with_capacity(10);
    payload.extend_from_slice(&stream.to_be_bytes());
    payload.extend_from_slice(&piece.to_be_bytes());
    payload.extend_from_slice(&chunk.to_be_bytes());
    PeerMessage::Unknown { id: 16, payload }
}

/// Build Acestream's custom live availability signal (id=4):
/// payload `[stream u32=0][piece u32]`. This is distinct from standard BT `Have`, whose
/// payload is only `[piece u32]`.
pub fn live_have(piece: u32) -> PeerMessage {
    let mut payload = vec![0u8, 0, 0, 0]; // stream index = 0
    payload.extend_from_slice(&piece.to_be_bytes());
    PeerMessage::Unknown { id: 4, payload }
}

/// Build Acestream's live bitfield bootstrap (id=5):
/// payload `[stream u32=0][first_piece u32][bit_count u32][MSB-first bits]`.
pub fn live_bitfield(first_piece: u32, bit_count: u32) -> PeerMessage {
    let mut payload = vec![0u8, 0, 0, 0]; // stream index = 0
    payload.extend_from_slice(&first_piece.to_be_bytes());
    payload.extend_from_slice(&bit_count.to_be_bytes());
    let byte_count = bit_count.div_ceil(8) as usize;
    let mut bits = vec![0u8; byte_count];
    for bit in 0..bit_count {
        bits[(bit / 8) as usize] |= 0x80 >> (bit % 8);
    }
    payload.extend_from_slice(&bits);
    PeerMessage::Bitfield(payload)
}

/// Build the Acestream live `Piece` message (id=7) to SEND, the inverse of [`LiveChunk`]:
/// payload `[stream u32][piece u32][8B piece header][chunk u16][data]`. The 8-byte
/// `piece_header` is constant for all chunks of a piece; official source nodes encode it as
/// a big-endian `f64` Unix timestamp (note 33).
pub fn build_piece(
    stream: u32,
    piece: u32,
    chunk: u16,
    piece_header: [u8; 8],
    data: &[u8],
) -> PeerMessage {
    let mut block = Vec::with_capacity(8 + 2 + data.len());
    block.extend_from_slice(&piece_header);
    block.extend_from_slice(&chunk.to_be_bytes());
    block.extend_from_slice(data);
    PeerMessage::Piece {
        index: stream,
        begin: piece,
        block,
    }
}

/// A received live chunk: its piece/chunk coordinates and the TS payload.
#[derive(Debug, PartialEq, Eq)]
pub struct LiveChunk {
    pub piece: u32,
    pub piece_header: [u8; 8],
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
            let piece_header: [u8; 8] = block[..8].try_into().expect("length checked");
            Some(LiveChunk {
                piece: *begin,
                piece_header,
                chunk,
                data: block[10..].to_vec(),
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn piece_header_is_big_endian_unix_time_double() {
        let header = piece_header_from_unix_seconds(1_782_925_464.8243976);
        assert_eq!(header, [0x41, 0xda, 0x91, 0x52, 0x26, 0x34, 0xc2, 0xee]);
    }

    #[test]
    fn request_has_acestream_10_byte_payload() {
        let m = chunk_request(0x005067f8, 3);
        match m {
            PeerMessage::Unknown { id, payload } => {
                assert_eq!(id, 6);
                assert_eq!(
                    payload,
                    vec![0, 0, 0, 0, 0x00, 0x50, 0x67, 0xf8, 0x00, 0x03]
                );
            }
            _ => panic!(),
        }
    }

    #[test]
    fn reject_request_has_acestream_10_byte_payload_on_bep6_id() {
        let m = reject_chunk_request(0, 0x005067f8, 3);
        match &m {
            PeerMessage::Unknown { id, payload } => {
                assert_eq!(*id, 16);
                assert_eq!(
                    payload.as_slice(),
                    &[0, 0, 0, 0, 0x00, 0x50, 0x67, 0xf8, 0x00, 0x03]
                );
            }
            _ => panic!(),
        }

        let encoded = m.encode();
        assert_eq!(
            PeerMessage::decode(&encoded).unwrap(),
            Some((m, encoded.len()))
        );
    }

    #[test]
    fn live_have_has_acestream_8_byte_payload() {
        let m = live_have(0x005067f8);
        match m {
            PeerMessage::Unknown { id, payload } => {
                assert_eq!(id, 4);
                assert_eq!(payload, vec![0, 0, 0, 0, 0x00, 0x50, 0x67, 0xf8]);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn live_bitfield_uses_acestream_window_payload_and_msb_bits() {
        let m = live_bitfield(0x005067f8, 9);
        assert_eq!(
            m,
            PeerMessage::Bitfield(vec![
                0, 0, 0, 0, // stream
                0x00, 0x50, 0x67, 0xf8, // first piece
                0, 0, 0, 9, // bit count
                0xff, 0x80, // 9 MSB-first one bits
            ])
        );
    }

    #[test]
    fn parse_piece_into_live_chunk() {
        let mut block = vec![0xAAu8; 8]; // 8-byte piece header
        block.extend_from_slice(&7u16.to_be_bytes()); // chunk 7
        block.extend_from_slice(&[1, 2, 3, 4]); // data
        let msg = PeerMessage::Piece {
            index: 0,
            begin: 5269621,
            block,
        };
        let lc = LiveChunk::from_message(&msg).unwrap();
        assert_eq!(
            lc,
            LiveChunk {
                piece: 5269621,
                piece_header: [0xAA; 8],
                chunk: 7,
                data: vec![1, 2, 3, 4]
            }
        );
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
            PeerMessage::Piece {
                index,
                begin,
                block,
            } => {
                assert_eq!(*index, 0); // stream
                assert_eq!(*begin, 5269621); // piece
                assert_eq!(&block[..8], &[0xAB; 8]); // 8-byte piece header
                assert_eq!(&block[8..10], &7u16.to_be_bytes()); // chunk
                assert_eq!(&block[10..], &data); // payload
            }
            _ => panic!("expected Piece"),
        }
        let lc = LiveChunk::from_message(&msg).unwrap();
        assert_eq!(
            lc,
            LiveChunk {
                piece: 5269621,
                piece_header: [0xAB; 8],
                chunk: 7,
                data: data.to_vec()
            }
        );
    }
}
