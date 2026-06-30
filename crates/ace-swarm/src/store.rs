//! A bounded, rolling store of downloaded (or broadcast) piece data, keyed by piece then chunk.
//! Feeds the seeder: we serve chunks we still hold. Pure (no I/O); eviction is FIFO by lowest
//! piece index once the byte budget is exceeded.
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct PieceStore {
    piece_length: u64,
    chunk_length: u64,
    max_bytes: u64,
    cur_bytes: u64,
    /// piece index -> (chunk index -> TS payload bytes)
    pieces: BTreeMap<u64, BTreeMap<u16, Vec<u8>>>,
}

impl PieceStore {
    /// # Panics
    /// `chunk_length` must be > 0 (otherwise [`chunks_per_piece`](Self::chunks_per_piece) divides
    /// by zero), and `piece_length` should be a multiple of `chunk_length`. Domain inputs are
    /// always 1 MiB / 16 KiB, so this is documented rather than asserted at runtime.
    pub fn new(piece_length: u64, chunk_length: u64, max_bytes: u64) -> Self {
        PieceStore { piece_length, chunk_length, max_bytes, cur_bytes: 0, pieces: BTreeMap::new() }
    }

    /// Chunks per piece (`piece_length / chunk_length`).
    pub fn chunks_per_piece(&self) -> u16 {
        (self.piece_length / self.chunk_length) as u16
    }

    /// Store a chunk's TS payload. Replacing an existing chunk adjusts the byte total. After the
    /// insert, evict the lowest-index pieces until within `max_bytes`. If `max_bytes` is smaller
    /// than `data.len()`, the chunk is evicted immediately after insertion (a no-op write).
    pub fn put_chunk(&mut self, piece: u64, chunk: u16, data: &[u8]) {
        let entry = self.pieces.entry(piece).or_default();
        if let Some(old) = entry.insert(chunk, data.to_vec()) {
            self.cur_bytes -= old.len() as u64;
        }
        self.cur_bytes += data.len() as u64;
        while self.cur_bytes > self.max_bytes {
            let Some((&lowest, _)) = self.pieces.iter().next() else { break };
            if let Some(removed) = self.pieces.remove(&lowest) {
                self.cur_bytes -= removed.values().map(|d| d.len() as u64).sum::<u64>();
            }
        }
    }

    /// The TS payload of `(piece, chunk)` if still held.
    pub fn chunk(&self, piece: u64, chunk: u16) -> Option<&[u8]> {
        self.pieces.get(&piece)?.get(&chunk).map(|v| v.as_slice())
    }

    /// True iff every chunk of `piece` is present.
    pub fn has_piece(&self, piece: u64) -> bool {
        self.pieces.get(&piece).is_some_and(|c| c.len() as u16 == self.chunks_per_piece())
    }

    /// Sorted indices of fully-held pieces (for `Have` advertisement).
    pub fn have_pieces(&self) -> Vec<u64> {
        self.pieces.keys().copied().filter(|&p| self.has_piece(p)).collect()
    }

    /// `(min, max)` stored piece indices, or None if empty.
    pub fn window(&self) -> Option<(u64, u64)> {
        let min = *self.pieces.keys().next()?;
        let max = *self.pieces.keys().next_back()?;
        Some((min, max))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 4-byte chunks, 2 chunks/piece, tiny budget for eviction tests.
    fn store(max_bytes: u64) -> PieceStore {
        PieceStore::new(8, 4, max_bytes)
    }

    #[test]
    fn stores_and_returns_a_chunk() {
        let mut s = store(1024);
        s.put_chunk(10, 0, &[1, 2, 3, 4]);
        assert_eq!(s.chunk(10, 0), Some(&[1, 2, 3, 4][..]));
        assert_eq!(s.chunk(10, 1), None);
        assert_eq!(s.chunk(11, 0), None);
    }

    #[test]
    fn has_piece_true_only_when_all_chunks_present() {
        let mut s = store(1024);
        assert_eq!(s.chunks_per_piece(), 2);
        s.put_chunk(5, 0, &[1, 2, 3, 4]);
        assert!(!s.has_piece(5));
        s.put_chunk(5, 1, &[5, 6, 7, 8]);
        assert!(s.has_piece(5));
        assert_eq!(s.have_pieces(), vec![5]);
    }

    #[test]
    fn window_reflects_min_and_max() {
        let mut s = store(1024);
        assert_eq!(s.window(), None);
        s.put_chunk(7, 0, &[0; 4]);
        s.put_chunk(9, 0, &[0; 4]);
        assert_eq!(s.window(), Some((7, 9)));
    }

    #[test]
    fn evicts_lowest_piece_when_over_budget() {
        // budget = 8 bytes = exactly two 4-byte chunks. A third chunk evicts the lowest piece.
        let mut s = store(8);
        s.put_chunk(1, 0, &[0; 4]);
        s.put_chunk(2, 0, &[0; 4]);
        assert_eq!(s.window(), Some((1, 2)));
        s.put_chunk(3, 0, &[0; 4]); // over budget -> drop piece 1
        assert_eq!(s.chunk(1, 0), None, "lowest piece evicted");
        assert_eq!(s.window(), Some((2, 3)));
    }

    #[test]
    fn replacing_a_chunk_does_not_double_count_bytes() {
        let mut s = store(8);
        s.put_chunk(1, 0, &[0; 4]);
        s.put_chunk(1, 0, &[9; 4]); // replace, not add
        s.put_chunk(1, 1, &[0; 4]);
        // Still one piece (1) with two chunks = 8 bytes, nothing evicted.
        assert_eq!(s.chunk(1, 0), Some(&[9, 9, 9, 9][..]));
        assert!(s.has_piece(1));
    }

    #[test]
    fn budget_smaller_than_one_chunk_discards_gracefully() {
        let mut s = PieceStore::new(8, 4, 3); // max_bytes < chunk size
        s.put_chunk(1, 0, &[0; 4]);
        assert_eq!(s.chunk(1, 0), None); // evicted immediately, no panic
        assert_eq!(s.window(), None);
    }

    #[test]
    fn eviction_runs_multiple_iterations_if_needed() {
        let mut s = store(4); // 1-chunk budget
        s.put_chunk(1, 0, &[0; 4]);
        s.put_chunk(2, 0, &[0; 4]); // evicts piece 1
        s.put_chunk(3, 0, &[0; 4]); // evicts piece 2
        assert_eq!(s.chunk(1, 0), None);
        assert_eq!(s.chunk(2, 0), None);
        assert_eq!(s.window(), Some((3, 3)));
    }

    #[test]
    fn have_pieces_excludes_partial_pieces() {
        let mut s = store(1024); // 2 chunks/piece
        s.put_chunk(5, 0, &[0; 4]); // piece 5 partial
        s.put_chunk(6, 0, &[0; 4]);
        s.put_chunk(6, 1, &[0; 4]); // piece 6 complete
        assert_eq!(s.have_pieces(), vec![6]);
    }

    #[test]
    fn has_piece_flips_true_on_the_chunk_that_completes_it() {
        // Mirrors the Have-advertisement use case in ace-engine's follow_one_peer: check
        // has_piece() right after each put_chunk to know exactly when to fire a Have.
        let mut s = store(1024); // 2 chunks/piece (per the existing `store()` test helper)
        assert!(!s.has_piece(9));
        s.put_chunk(9, 0, &[0; 4]);
        assert!(!s.has_piece(9), "still missing chunk 1");
        s.put_chunk(9, 1, &[0; 4]);
        assert!(s.has_piece(9), "both chunks present now");
    }
}
