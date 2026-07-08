//! Live piece reassembly (pure, no I/O).
//!
//! Peers deliver `piece` messages as `chunk_length`-sized blocks (`index, begin, block`).
//! This reassembles those blocks into complete `piece_length` pieces and emits the
//! pieces' bytes **in contiguous order** from a starting index — the continuous byte
//! stream the media layer (MPEG-TS) consumes. Pure logic; pairs with [`crate::live`].

use crate::{Result, WireError};
use std::collections::BTreeMap;

struct Partial {
    buf: Vec<u8>,
    filled: u64,
}

/// Reassembles chunk blocks into ordered piece bytes for a live download.
pub struct PieceReassembler {
    piece_length: u64,
    next_emit: u64,
    /// Trailing bytes of every piece that are NOT media and must be dropped before emitting
    /// (Acestream live pieces carry a per-piece RSA source signature as their last `sig_len`
    /// bytes; emitting it would inject non-TS bytes at every piece boundary — see B0/note 27).
    piece_trailer: u64,
    /// Broadcaster's DER SubjectPublicKeyInfo (the transport descriptor's `pubkey` field).
    /// When set, every completed piece's in-band RSA source signature — its trailing
    /// `piece_trailer` bytes — is verified before the piece is marked complete; a piece that
    /// fails is dropped and never emitted, so unauthenticated peer bytes can't reach the
    /// consumer (B0/note 27). `None` (a bare-infohash stream with no known source key) leaves
    /// verification off, stripping the trailer without authenticating it.
    verify_pubkey: Option<Vec<u8>>,
    /// Largest distance ahead of `next_emit` an incoming piece index may have before it is
    /// rejected. Peers deliver `piece` blocks for untrusted indices, and every new index
    /// allocates a full `piece_length` buffer retained until the intervening pieces arrive; a
    /// hostile peer streaming far-future indices could otherwise force unbounded allocation
    /// (issue #13). Defaults to `u64::MAX` (unbounded — legacy/trusted callers); live callers
    /// set a window that comfortably covers what they actually request.
    max_ahead: u64,
    partial: BTreeMap<u64, Partial>,
    complete: BTreeMap<u64, Vec<u8>>,
}

impl PieceReassembler {
    /// `start_piece` is the first piece index to emit (the live start; earlier pieces
    /// are ignored). `piece_length` must be > 0.
    pub fn new(piece_length: u64, start_piece: u64) -> Self {
        PieceReassembler {
            piece_length,
            next_emit: start_piece,
            piece_trailer: 0,
            verify_pubkey: None,
            max_ahead: u64::MAX,
            partial: BTreeMap::new(),
            complete: BTreeMap::new(),
        }
    }

    /// Set the number of trailing bytes to drop from each emitted piece — the live-source
    /// signature length (`sig_len`, the RSA modulus's byte length). Blocks are still received
    /// over the full `piece_length`; only the emitted media stream has the tail removed, so
    /// the pieces byte-chain into a clean, packet-aligned MPEG-TS stream. `0` (the default)
    /// emits pieces verbatim.
    pub fn with_piece_trailer(mut self, sig_len: u64) -> Self {
        self.piece_trailer = sig_len.min(self.piece_length);
        self
    }

    /// Authenticate every completed piece against the broadcast source's `pubkey_der` (the
    /// transport descriptor's DER SubjectPublicKeyInfo). The trailing signature length is
    /// derived from the key's own modulus, so this also sets the piece trailer — no separate
    /// [`Self::with_piece_trailer`] call is needed, and callers can't accidentally verify over
    /// a differently-sized tail than they strip. A completed piece whose in-band RSASSA-
    /// PKCS1-v1_5/SHA1 signature (B0/note 27) does not verify is dropped and never emitted.
    ///
    /// An empty or unparseable `pubkey_der` is a no-op (verification stays off) — a bare
    /// infohash carries no source key, so its pieces can only be stripped, not authenticated.
    pub fn with_source_pubkey(mut self, pubkey_der: Vec<u8>) -> Self {
        if let Some(sig_len) = crate::live_auth::signature_len_from_pubkey_der(&pubkey_der) {
            self.piece_trailer = (sig_len as u64).min(self.piece_length);
            self.verify_pubkey = Some(pubkey_der);
        }
        self
    }

    /// Bound how far ahead of the emit cursor a piece index may be before [`Self::add_block`]
    /// rejects it. Blocks are received for untrusted peer-supplied indices and each new index
    /// allocates a full `piece_length` buffer held until the intervening pieces arrive, so a
    /// peer streaming far-future indices could otherwise force unbounded allocation (issue #13).
    /// `max_pieces_ahead` must comfortably exceed the largest distance the caller ever legitimately
    /// requests ahead of `next_emit`; a piece at `next_emit + max_pieces_ahead` or beyond is
    /// rejected. The default is unbounded.
    pub fn with_max_pieces_ahead(mut self, max_pieces_ahead: u64) -> Self {
        self.max_ahead = max_pieces_ahead;
        self
    }

    /// Place a received block at `begin` within piece `index`. Blocks for already-emitted
    /// pieces (`index < start/next_emit`) are dropped; blocks for indices more than
    /// [`Self::with_max_pieces_ahead`] beyond `next_emit` are rejected (an unsolicited
    /// far-future index that would otherwise pin a `piece_length` buffer — issue #13).
    /// Assumes non-overlapping blocks that together cover `[0, piece_length)`.
    pub fn add_block(&mut self, index: u64, begin: u64, block: &[u8]) -> Result<()> {
        if index < self.next_emit || self.complete.contains_key(&index) {
            return Ok(()); // stale or already complete
        }
        if index >= self.next_emit.saturating_add(self.max_ahead) {
            // Outside the accept window: reject before allocating a piece buffer. The caller
            // drops the block (and re-requests in-window pieces from the pool), so a peer
            // streaming far-future indices can't force unbounded reassembly allocation.
            return Err(WireError::Invalid(
                "piece index too far ahead of emit cursor",
            ));
        }
        let end = begin
            .checked_add(block.len() as u64)
            .ok_or(WireError::Invalid("block offset overflow"))?;
        if end > self.piece_length {
            return Err(WireError::Invalid("block exceeds piece length"));
        }
        let p = self.partial.entry(index).or_insert_with(|| Partial {
            buf: vec![0u8; self.piece_length as usize],
            filled: 0,
        });
        p.buf[begin as usize..end as usize].copy_from_slice(block);
        p.filled += block.len() as u64;
        if p.filled >= self.piece_length {
            let done = self.partial.remove(&index).unwrap();
            if let Some(pubkey) = &self.verify_pubkey {
                // Authenticate before the piece can ever be emitted. A forged/corrupt piece is
                // already removed from `partial` above, so returning an error simply drops it —
                // the cursor doesn't advance and the caller re-requests, rather than serving
                // unauthenticated bytes to the consumer (see the module docs / B0/note 27).
                let verified =
                    crate::live_auth::split_piece(&done.buf, self.piece_trailer as usize)
                        .is_some_and(|(payload, sig)| {
                            crate::live_auth::verify_piece(pubkey, payload, sig)
                        });
                if !verified {
                    return Err(WireError::Invalid(
                        "live-source piece signature verification failed",
                    ));
                }
            }
            self.complete.insert(index, done.buf);
        }
        Ok(())
    }

    /// Pull all contiguous completed pieces from `next_emit` onward as one byte buffer,
    /// advancing the emit cursor. Returns empty if the next needed piece isn't ready yet.
    pub fn take_ready(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(bytes) = self.complete.remove(&self.next_emit) {
            let media_end = bytes.len().saturating_sub(self.piece_trailer as usize);
            out.extend_from_slice(&bytes[..media_end]);
            self.next_emit += 1;
        }
        out
    }

    /// Next piece index still needed to make progress (for the picker to target).
    pub fn next_needed(&self) -> u64 {
        self.next_emit
    }

    /// Force the emit cursor forward to `piece`, discarding any buffered data strictly
    /// before it. For recovering from a genuine, unrecoverable gap (e.g. a peer reconnect
    /// whose live window has already evicted the piece we still needed) — without this,
    /// `take_ready` would wait forever for a piece index that will never arrive, silently
    /// stalling the stream. No-ops if `piece` doesn't actually move the cursor forward, so
    /// callers can call it unconditionally on every reconnect.
    pub fn skip_to(&mut self, piece: u64) {
        if piece <= self.next_emit {
            return;
        }
        self.next_emit = piece;
        self.partial.retain(|&idx, _| idx >= piece);
        self.complete.retain(|&idx, _| idx >= piece);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assembles_single_piece_from_in_order_chunks() {
        let mut r = PieceReassembler::new(4, 0);
        r.add_block(0, 0, &[1, 2]).unwrap();
        r.add_block(0, 2, &[3, 4]).unwrap();
        assert_eq!(r.take_ready(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn handles_out_of_order_chunks_within_a_piece() {
        let mut r = PieceReassembler::new(4, 0);
        r.add_block(0, 2, &[3, 4]).unwrap();
        r.add_block(0, 0, &[1, 2]).unwrap();
        assert_eq!(r.take_ready(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn emits_pieces_in_contiguous_order_only() {
        let mut r = PieceReassembler::new(2, 0);
        // Complete piece 1 before piece 0 — must NOT emit yet (would create a gap).
        r.add_block(1, 0, &[9, 9]).unwrap();
        assert_eq!(r.take_ready(), Vec::<u8>::new());
        // Now complete piece 0 → both emit in order.
        r.add_block(0, 0, &[1, 1]).unwrap();
        assert_eq!(r.take_ready(), vec![1, 1, 9, 9]);
    }

    #[test]
    fn partial_piece_is_not_emitted() {
        let mut r = PieceReassembler::new(4, 0);
        r.add_block(0, 0, &[1, 2]).unwrap();
        assert_eq!(r.take_ready(), Vec::<u8>::new());
        assert_eq!(r.next_needed(), 0);
    }

    #[test]
    fn drops_stale_pieces_before_start() {
        let mut r = PieceReassembler::new(2, 5);
        r.add_block(3, 0, &[1, 1]).unwrap(); // before start → dropped
        assert_eq!(r.take_ready(), Vec::<u8>::new());
        assert_eq!(r.next_needed(), 5);
    }

    #[test]
    fn rejects_block_past_piece_length() {
        let mut r = PieceReassembler::new(4, 0);
        assert!(r.add_block(0, 2, &[1, 2, 3]).is_err());
    }

    #[test]
    fn skip_to_advances_cursor_past_an_unrecoverable_gap() {
        let mut r = PieceReassembler::new(2, 0);
        r.skip_to(5);
        assert_eq!(r.next_needed(), 5);
        // The reassembler now accepts and emits piece 5 onward, not the old gap.
        r.add_block(5, 0, &[9, 9]).unwrap();
        assert_eq!(r.take_ready(), vec![9, 9]);
    }

    #[test]
    fn skip_to_drops_stale_buffered_data_below_the_new_cursor() {
        let mut r = PieceReassembler::new(2, 0);
        r.add_block(1, 0, &[9, 9]).unwrap(); // completed, but before the skip target
        r.add_block(2, 0, &[1]).unwrap(); // partial, also before the skip target
        r.skip_to(3);
        r.add_block(3, 0, &[7, 7]).unwrap();
        // Only piece 3 onward emits; the stale piece 1/2 data must not leak out.
        assert_eq!(r.take_ready(), vec![7, 7]);
    }

    #[test]
    fn piece_trailer_is_dropped_from_each_emitted_piece() {
        // piece_length 6, 2-byte signature trailer: only the first 4 media bytes of each
        // piece are emitted, and consecutive pieces byte-chain with no trailer bytes between.
        let mut r = PieceReassembler::new(6, 0).with_piece_trailer(2);
        r.add_block(0, 0, &[1, 2, 3, 4, 0xAA, 0xBB]).unwrap(); // 0xAA,0xBB = signature
        r.add_block(1, 0, &[5, 6, 7, 8, 0xCC, 0xDD]).unwrap();
        assert_eq!(r.take_ready(), vec![1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn piece_trailer_defaults_to_zero() {
        let mut r = PieceReassembler::new(4, 0);
        r.add_block(0, 0, &[1, 2, 3, 4]).unwrap();
        assert_eq!(r.take_ready(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn skip_to_never_moves_the_cursor_backward() {
        let mut r = PieceReassembler::new(2, 10);
        r.skip_to(3); // behind next_emit -> no-op
        assert_eq!(r.next_needed(), 10);
    }

    /// Build a wire piece (`payload || signature`) of exactly `piece_length` bytes, signed by
    /// `auth` — the same layout `SigningChunker` and a real Acestream source produce.
    fn signed_piece(
        auth: &crate::live_auth::LiveSourceAuth,
        piece_length: usize,
        fill: u8,
    ) -> (Vec<u8>, Vec<u8>) {
        let payload = vec![fill; piece_length - auth.signature_len()];
        let mut piece = payload.clone();
        piece.extend_from_slice(&auth.sign(&payload));
        assert_eq!(piece.len(), piece_length);
        (piece, payload)
    }

    #[test]
    fn verifies_and_emits_a_valid_signed_piece() {
        let auth = crate::live_auth::LiveSourceAuth::generate();
        let piece_length = 128; // > 96-byte signature of the generated 768-bit key
        let (piece, payload) = signed_piece(&auth, piece_length, 0x5A);

        let mut r =
            PieceReassembler::new(piece_length as u64, 0).with_source_pubkey(auth.pubkey_der());
        // Deliver as two blocks to exercise reassembly + verification of the whole piece.
        r.add_block(0, 0, &piece[..64]).unwrap();
        r.add_block(0, 64, &piece[64..]).unwrap();
        assert_eq!(
            r.take_ready(),
            payload,
            "a valid piece emits its media payload with the signature trailer stripped"
        );
    }

    #[test]
    fn rejects_a_piece_whose_signature_does_not_verify() {
        let auth = crate::live_auth::LiveSourceAuth::generate();
        let piece_length = 128;
        let (mut piece, _) = signed_piece(&auth, piece_length, 0x11);
        piece[0] ^= 0xFF; // tamper a media byte after signing -> signature no longer matches

        let mut r =
            PieceReassembler::new(piece_length as u64, 0).with_source_pubkey(auth.pubkey_der());
        r.add_block(0, 0, &piece[..64]).unwrap();
        let err = r.add_block(0, 64, &piece[64..]).unwrap_err();
        assert!(
            matches!(err, WireError::Invalid(_)),
            "the block completing a forged piece is rejected"
        );
        assert_eq!(
            r.take_ready(),
            Vec::<u8>::new(),
            "a forged piece is never emitted"
        );
        assert_eq!(
            r.next_needed(),
            0,
            "the cursor stays put; the forged piece was never marked complete"
        );
    }

    #[test]
    fn a_forged_piece_can_be_replaced_by_a_valid_re_request() {
        let auth = crate::live_auth::LiveSourceAuth::generate();
        let piece_length = 128;
        let (good, payload) = signed_piece(&auth, piece_length, 0x33);
        let mut forged = good.clone();
        forged[10] ^= 0xAA;

        let mut r =
            PieceReassembler::new(piece_length as u64, 0).with_source_pubkey(auth.pubkey_der());
        assert!(
            r.add_block(0, 0, &forged).is_err(),
            "forged piece rejected on completion"
        );
        assert_eq!(r.take_ready(), Vec::<u8>::new());
        // The honest re-request rebuilds piece 0 from scratch and now verifies + emits.
        r.add_block(0, 0, &good).unwrap();
        assert_eq!(r.take_ready(), payload);
    }

    #[test]
    fn a_piece_signed_by_the_wrong_key_is_rejected() {
        let source = crate::live_auth::LiveSourceAuth::generate();
        let attacker = crate::live_auth::LiveSourceAuth::generate();
        let piece_length = 128;
        // Signed by the attacker's key but presented against the real source's pubkey.
        let (piece, _) = signed_piece(&attacker, piece_length, 0x77);

        let mut r =
            PieceReassembler::new(piece_length as u64, 0).with_source_pubkey(source.pubkey_der());
        assert!(r.add_block(0, 0, &piece).is_err());
        assert_eq!(r.take_ready(), Vec::<u8>::new());
    }

    #[test]
    fn without_a_pubkey_pieces_are_stripped_but_not_verified() {
        // A bare-infohash stream knows the signature length but not the source key, so it can
        // only strip the trailer — an unverifiable (here arbitrary) tail still passes through.
        let mut r = PieceReassembler::new(6, 0).with_piece_trailer(2);
        r.add_block(0, 0, &[1, 2, 3, 4, 0xAA, 0xBB]).unwrap();
        assert_eq!(r.take_ready(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn an_empty_or_unparseable_pubkey_leaves_verification_off() {
        let mut r = PieceReassembler::new(4, 0).with_source_pubkey(Vec::new());
        r.add_block(0, 0, &[1, 2, 3, 4]).unwrap();
        assert_eq!(r.take_ready(), vec![1, 2, 3, 4], "empty pubkey is a no-op");

        let mut r = PieceReassembler::new(4, 0).with_source_pubkey(vec![0xDE, 0xAD]);
        r.add_block(0, 0, &[5, 6, 7, 8]).unwrap();
        assert_eq!(
            r.take_ready(),
            vec![5, 6, 7, 8],
            "garbage that isn't an RSA key is a no-op, not a reject-everything trap"
        );
    }

    #[test]
    fn rejects_pieces_too_far_ahead_of_the_emit_cursor() {
        // A hostile peer sends blocks for far-future piece indices. Each new index would
        // allocate a full piece_length buffer held until the intervening pieces arrive, so
        // out-of-window indices must be rejected, not buffered (issue #13).
        let mut r = PieceReassembler::new(4, 0).with_max_pieces_ahead(8);
        assert!(
            r.add_block(7, 0, &[1, 2, 3, 4]).is_ok(),
            "within window [0,8)"
        );
        assert!(
            r.add_block(8, 0, &[1, 2, 3, 4]).is_err(),
            "at the window edge -> rejected"
        );
        assert!(
            r.add_block(9999, 0, &[1, 2, 3, 4]).is_err(),
            "far beyond the window -> rejected"
        );
    }

    #[test]
    fn accept_window_slides_forward_with_the_emit_cursor() {
        // As pieces emit and next_emit advances, the window slides so pieces just ahead of
        // the new cursor are accepted again — a legitimately-requested piece is never
        // permanently rejected just because playback started further back.
        let mut r = PieceReassembler::new(2, 0).with_max_pieces_ahead(4);
        assert!(
            r.add_block(4, 0, &[9, 9]).is_err(),
            "outside [0,4) at first"
        );
        r.add_block(0, 0, &[1, 1]).unwrap();
        r.add_block(1, 0, &[2, 2]).unwrap();
        assert_eq!(r.take_ready(), vec![1, 1, 2, 2]);
        assert_eq!(r.next_needed(), 2);
        assert!(
            r.add_block(4, 0, &[9, 9]).is_ok(),
            "now within [2,6) -> accepted"
        );
    }

    #[test]
    fn accepts_arbitrarily_far_pieces_when_window_is_unbounded() {
        // Default (no window set): behavior is unchanged for legacy/trusted callers.
        let mut r = PieceReassembler::new(2, 0);
        assert!(r.add_block(1_000_000, 0, &[9, 9]).is_ok());
    }
}
