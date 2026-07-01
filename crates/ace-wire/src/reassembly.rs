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
            partial: BTreeMap::new(),
            complete: BTreeMap::new(),
        }
    }

    /// Place a received block at `begin` within piece `index`. Blocks for already-emitted
    /// pieces (`index < start/next_emit`) are dropped. Assumes non-overlapping blocks that
    /// together cover `[0, piece_length)`.
    pub fn add_block(&mut self, index: u64, begin: u64, block: &[u8]) -> Result<()> {
        if index < self.next_emit || self.complete.contains_key(&index) {
            return Ok(()); // stale or already complete
        }
        let end = begin
            .checked_add(block.len() as u64)
            .ok_or(WireError::Invalid("block offset overflow"))?;
        if end > self.piece_length {
            return Err(WireError::Invalid("block exceeds piece length"));
        }
        let p = self
            .partial
            .entry(index)
            .or_insert_with(|| Partial { buf: vec![0u8; self.piece_length as usize], filled: 0 });
        p.buf[begin as usize..end as usize].copy_from_slice(block);
        p.filled += block.len() as u64;
        if p.filled >= self.piece_length {
            let done = self.partial.remove(&index).unwrap();
            self.complete.insert(index, done.buf);
        }
        Ok(())
    }

    /// Pull all contiguous completed pieces from `next_emit` onward as one byte buffer,
    /// advancing the emit cursor. Returns empty if the next needed piece isn't ready yet.
    pub fn take_ready(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(bytes) = self.complete.remove(&self.next_emit) {
            out.extend_from_slice(&bytes);
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
    fn skip_to_never_moves_the_cursor_backward() {
        let mut r = PieceReassembler::new(2, 10);
        r.skip_to(3); // behind next_emit -> no-op
        assert_eq!(r.next_needed(), 10);
    }
}
