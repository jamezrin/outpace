//! Combines [`crate::chunker::TsChunker`] with per-piece live-source signing (B0/B1):
//! buffers raw TS bytes up to the signable payload capacity (`piece_length -
//! signature_len`), signs that payload, appends the signature to fill out the full
//! `piece_length`, then hands the result to an ordinary `TsChunker` for wire chunking.
//!
//! This is the one piece of B1 that needed B0's signing crack (`live_auth`) — everything
//! else about origination (minting, registering, serving) was already wire-compatible
//! because a piece's bytes already *are* `payload || signature` end to end (see
//! `live_auth`'s module docs); only the *source* needs to actually produce a valid
//! signature instead of a placeholder tail.

use crate::chunker::{OutChunk, TsChunker};
use crate::live_auth::LiveSourceAuth;

pub struct SigningChunker {
    payload_capacity: u64,
    buf: Vec<u8>,
    chunker: TsChunker,
}

impl SigningChunker {
    /// `piece_length` must exceed `sig_len` (the signer's `signature_len()`) — the signable
    /// payload capacity is `piece_length - sig_len`.
    pub fn new(piece_length: u64, chunk_length: u64, start_piece: u64, sig_len: u64) -> Self {
        assert!(
            piece_length > sig_len,
            "piece_length must leave room for the signature"
        );
        SigningChunker {
            payload_capacity: piece_length - sig_len,
            buf: Vec::new(),
            chunker: TsChunker::new(piece_length, chunk_length, start_piece),
        }
    }

    /// Append `bytes`; return every full, **signed** piece's chunks now available, in order.
    pub fn push(&mut self, bytes: &[u8], auth: &LiveSourceAuth) -> Vec<OutChunk> {
        self.buf.extend_from_slice(bytes);
        let mut out = Vec::new();
        while self.buf.len() as u64 >= self.payload_capacity {
            let payload: Vec<u8> = self.buf.drain(..self.payload_capacity as usize).collect();
            let sig = auth.sign(&payload);
            let mut piece_bytes = payload;
            piece_bytes.extend_from_slice(&sig);
            out.extend(self.chunker.push(&piece_bytes));
        }
        out
    }

    /// Flush any remaining buffered bytes as a final signed (possibly short) piece. Returns
    /// an empty vec if nothing is buffered. Note: a short final piece still gets a signature
    /// sized for the *configured* key, so its payload capacity is smaller than usual pieces
    /// only in that it may be shorter than `payload_capacity`.
    pub fn flush(&mut self, auth: &LiveSourceAuth) -> Vec<OutChunk> {
        if self.buf.is_empty() {
            return Vec::new();
        }
        let payload = std::mem::take(&mut self.buf);
        let sig = auth.sign(&payload);
        let mut piece_bytes = payload;
        piece_bytes.extend_from_slice(&sig);
        let mut out = self.chunker.push(&piece_bytes);
        // The pushed bytes are very unlikely to be an exact multiple of chunk_length, so the
        // inner chunker's own flush() is needed to emit its trailing partial chunk too.
        if let Some(tail) = self.chunker.flush() {
            out.push(tail);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live_auth::{split_piece, verify_piece};
    use crate::reassembly::PieceReassembler;

    #[test]
    fn chunked_pieces_reassemble_and_verify() {
        let auth = LiveSourceAuth::generate();
        let sig_len = auth.signature_len() as u64; // 96 for the 768-bit generated key
        let piece_length = 128u64; // must exceed sig_len; small enough to exercise quickly
        let chunk_length = 8u64;
        let mut sc = SigningChunker::new(piece_length, chunk_length, 0, sig_len);

        // Enough input for exactly 2 full pieces' worth of PAYLOAD (piece_length - sig_len
        // each), so both pieces complete without needing flush().
        let payload_capacity = (piece_length - sig_len) as usize;
        let input: Vec<u8> = (0..(payload_capacity * 2) as u32)
            .map(|i| (i % 256) as u8)
            .collect();
        let chunks = sc.push(&input, &auth);
        assert!(
            sc.flush(&auth).is_empty(),
            "exact multiple of payload_capacity -> nothing to flush"
        );

        // Reassemble via the ordinary reassembler, exactly like a real leech client would.
        let mut r = PieceReassembler::new(piece_length, 0);
        for c in &chunks {
            r.add_block(c.piece, c.begin, &c.data).unwrap();
        }
        let assembled = r.take_ready();
        assert_eq!(assembled.len(), piece_length as usize * 2);

        // Each piece must verify against the signer's own pubkey, and the payload recovered
        // must be exactly the original bytes (not the signature tail).
        let pubkey_der = auth.pubkey_der();
        for (i, piece_bytes) in assembled.chunks(piece_length as usize).enumerate() {
            let (payload, sig) = split_piece(piece_bytes, sig_len as usize).unwrap();
            assert!(
                verify_piece(&pubkey_der, payload, sig),
                "piece {i} must verify"
            );
            let expected_payload = &input[i * payload_capacity..(i + 1) * payload_capacity];
            assert_eq!(
                payload, expected_payload,
                "piece {i} payload must be the original bytes"
            );
        }
    }

    #[test]
    fn flush_signs_and_verifies_a_short_final_piece() {
        // A short final piece (shorter than the configured piece_length) is a VOD/finite-
        // content edge case, not something a true unbounded live stream produces — and
        // `PieceReassembler` deliberately only completes a piece once it's fully
        // `piece_length` bytes, so it can't be used to reassemble this short one. Verify
        // directly from the emitted chunks (ordered by `begin`) instead.
        let auth = LiveSourceAuth::generate();
        let sig_len = auth.signature_len() as u64; // 96 for the 768-bit generated key
        let piece_length = 128u64;
        let mut sc = SigningChunker::new(piece_length, 8, 0, sig_len);
        let input = b"short tail data";
        assert!(
            sc.push(input, &auth).is_empty(),
            "not enough for a full piece yet"
        );
        let mut chunks = sc.flush(&auth);
        assert!(!chunks.is_empty());
        chunks.sort_by_key(|c| c.begin);

        let assembled: Vec<u8> = chunks.iter().flat_map(|c| c.data.clone()).collect();
        let (payload, sig) = split_piece(&assembled, sig_len as usize).unwrap();
        assert_eq!(payload, input);
        assert!(verify_piece(&auth.pubkey_der(), payload, sig));
    }
}
