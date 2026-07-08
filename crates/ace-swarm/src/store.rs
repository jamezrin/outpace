//! A bounded, rolling store of downloaded (or broadcast) piece data, keyed by piece then chunk.
//! Feeds the seeder: we serve chunks we still hold. Eviction is FIFO by lowest piece index once
//! the byte budget is exceeded.
//!
//! The store's internals sit behind a [`Backend`]: the default `Memory` backend keeps piece data
//! in RAM (pure, no I/O), while the optional `Disk` backend spills chunk payloads to one file per
//! piece so operators can retain far more reseed data without paying RAM. Both share the same
//! `max_bytes` budget and public API; only the disk backend touches the filesystem, and only in
//! [`chunk`](PieceStore::chunk) / the `put_chunk*` writers.
use std::borrow::Cow;
use std::collections::BTreeMap;

mod disk;
use disk::DiskBackend;

#[derive(Debug)]
pub struct PieceStore {
    piece_length: u64,
    chunk_length: u64,
    max_bytes: u64,
    cur_bytes: u64,
    backend: Backend,
}

/// How a [`PieceStore`] keeps its piece data. Selected once at construction.
pub enum BackendKind {
    /// Keep piece data in RAM (default; pure, no I/O).
    Memory,
    /// Spill chunk payloads to one file per piece under `dir`. The directory is treated as
    /// ephemeral: any stale contents from a prior run are wiped when the store is created.
    Disk { dir: std::path::PathBuf },
}

/// The storage implementation backing a [`PieceStore`]. Byte accounting and eviction policy live
/// on `PieceStore`; a backend only owns the piece maps and reports how many bytes an insert
/// replaced or an eviction freed so the accounting stays backend-agnostic.
#[derive(Debug)]
enum Backend {
    Memory(MemoryBackend),
    Disk(DiskBackend),
}

/// Byte-accounting deltas from a single insert, so `PieceStore` can keep `cur_bytes` exact
/// without knowing which backend actually stored the data. On the disk backend a failed write
/// reports `added: 0` so a dropped chunk never inflates the running total.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Stored {
    /// Bytes newly written and now counted toward the budget.
    added: u64,
    /// Bytes freed by replacing an existing chunk at the same `(piece, chunk)` slot.
    removed: u64,
}

impl Backend {
    /// Insert `(piece, chunk)`, applying the header-upgrade rule. Returns the byte deltas the
    /// caller should apply to its running total.
    fn put(&mut self, piece: u64, chunk: u16, header: [u8; 8], data: &[u8]) -> Stored {
        match self {
            Backend::Memory(m) => m.put(piece, chunk, header, data),
            Backend::Disk(d) => d.put(piece, chunk, header, data),
        }
    }

    fn get(&self, piece: u64, chunk: u16) -> Option<Cow<'_, [u8]>> {
        match self {
            Backend::Memory(m) => m.get(piece, chunk),
            Backend::Disk(d) => d.get(piece, chunk),
        }
    }

    fn header(&self, piece: u64) -> Option<[u8; 8]> {
        match self {
            Backend::Memory(m) => m.header(piece),
            Backend::Disk(d) => d.header(piece),
        }
    }

    fn has_piece(&self, piece: u64, chunks_per_piece: u16) -> bool {
        match self {
            Backend::Memory(m) => m.has_piece(piece, chunks_per_piece),
            Backend::Disk(d) => d.has_piece(piece, chunks_per_piece),
        }
    }

    fn have_pieces(&self, chunks_per_piece: u16) -> Vec<u64> {
        match self {
            Backend::Memory(m) => m.have_pieces(chunks_per_piece),
            Backend::Disk(d) => d.have_pieces(chunks_per_piece),
        }
    }

    fn window(&self) -> Option<(u64, u64)> {
        match self {
            Backend::Memory(m) => m.window(),
            Backend::Disk(d) => d.window(),
        }
    }

    /// Evict the lowest-index piece; return the byte total it freed, or `None` if the store was
    /// already empty. `Some(0)` (a zero-byte piece) is distinct from `None` so the eviction loop
    /// keeps going until the store is actually empty rather than halting on a zero-byte piece.
    fn evict_lowest(&mut self) -> Option<u64> {
        match self {
            Backend::Memory(m) => m.evict_lowest(),
            Backend::Disk(d) => d.evict_lowest(),
        }
    }
}

/// In-RAM backend: today's `BTreeMap` piece store, moved verbatim behind the backend boundary.
#[derive(Debug, Default)]
struct MemoryBackend {
    /// piece index -> (chunk index -> TS payload bytes)
    pieces: BTreeMap<u64, BTreeMap<u16, Vec<u8>>>,
    /// piece index -> Acestream's 8-byte live piece header.
    headers: BTreeMap<u64, [u8; 8]>,
}

impl MemoryBackend {
    fn put(&mut self, piece: u64, chunk: u16, header: [u8; 8], data: &[u8]) -> Stored {
        self.headers
            .entry(piece)
            .and_modify(|stored| {
                if *stored == [0u8; 8] && header != [0u8; 8] {
                    *stored = header;
                }
            })
            .or_insert(header);
        let entry = self.pieces.entry(piece).or_default();
        let removed = entry
            .insert(chunk, data.to_vec())
            .map_or(0, |old| old.len() as u64);
        Stored {
            added: data.len() as u64,
            removed,
        }
    }

    fn get(&self, piece: u64, chunk: u16) -> Option<Cow<'_, [u8]>> {
        self.pieces
            .get(&piece)?
            .get(&chunk)
            .map(|v| Cow::Borrowed(v.as_slice()))
    }

    fn header(&self, piece: u64) -> Option<[u8; 8]> {
        self.headers.get(&piece).copied()
    }

    fn has_piece(&self, piece: u64, chunks_per_piece: u16) -> bool {
        self.pieces
            .get(&piece)
            .is_some_and(|c| c.len() as u16 == chunks_per_piece)
    }

    fn have_pieces(&self, chunks_per_piece: u16) -> Vec<u64> {
        self.pieces
            .keys()
            .copied()
            .filter(|&p| self.has_piece(p, chunks_per_piece))
            .collect()
    }

    fn window(&self) -> Option<(u64, u64)> {
        let min = *self.pieces.keys().next()?;
        let max = *self.pieces.keys().next_back()?;
        Some((min, max))
    }

    fn evict_lowest(&mut self) -> Option<u64> {
        let (&lowest, _) = self.pieces.iter().next()?;
        let removed = self.pieces.remove(&lowest);
        self.headers.remove(&lowest);
        Some(
            removed
                .map(|c| c.values().map(|d| d.len() as u64).sum())
                .unwrap_or(0),
        )
    }
}

impl PieceStore {
    /// # Panics
    /// `chunk_length` must be > 0 (otherwise [`chunks_per_piece`](Self::chunks_per_piece) divides
    /// by zero), and `piece_length` should be a multiple of `chunk_length`. Domain inputs are
    /// always 1 MiB / 16 KiB, so this is documented rather than asserted at runtime.
    pub fn new(piece_length: u64, chunk_length: u64, max_bytes: u64) -> Self {
        // Memory backend never fails to build, so this stays infallible for back-compat.
        Self::with_backend(piece_length, chunk_length, max_bytes, BackendKind::Memory)
            .expect("memory backend is infallible")
    }

    /// Build a store with an explicitly chosen [`BackendKind`]. Fails only when a `Disk` backend
    /// cannot prepare its cache directory.
    pub fn with_backend(
        piece_length: u64,
        chunk_length: u64,
        max_bytes: u64,
        kind: BackendKind,
    ) -> std::io::Result<Self> {
        let backend = match kind {
            BackendKind::Memory => Backend::Memory(MemoryBackend::default()),
            BackendKind::Disk { dir } => Backend::Disk(DiskBackend::new(dir, chunk_length)?),
        };
        Ok(PieceStore {
            piece_length,
            chunk_length,
            max_bytes,
            cur_bytes: 0,
            backend,
        })
    }

    /// Convenience: a disk-backed store rooted at `dir` (its contents are wiped on creation).
    pub fn new_disk(
        piece_length: u64,
        chunk_length: u64,
        max_bytes: u64,
        dir: std::path::PathBuf,
    ) -> std::io::Result<Self> {
        Self::with_backend(
            piece_length,
            chunk_length,
            max_bytes,
            BackendKind::Disk { dir },
        )
    }

    /// Chunks per piece (`piece_length / chunk_length`).
    pub fn chunks_per_piece(&self) -> u16 {
        (self.piece_length / self.chunk_length) as u16
    }

    /// Store a chunk's TS payload. Replacing an existing chunk adjusts the byte total. After the
    /// insert, evict the lowest-index pieces until within `max_bytes`. If `max_bytes` is smaller
    /// than `data.len()`, the chunk is evicted immediately after insertion (a no-op write).
    pub fn put_chunk(&mut self, piece: u64, chunk: u16, data: &[u8]) {
        self.put_chunk_with_header(piece, chunk, [0u8; 8], data);
    }

    /// Store a chunk plus the piece-scoped 8-byte live header observed on the wire. The header is
    /// stable for every chunk in a piece; a later nonzero header replaces an earlier zero
    /// placeholder so old call sites remain compatible while relay/source paths preserve real
    /// headers.
    pub fn put_chunk_with_header(&mut self, piece: u64, chunk: u16, header: [u8; 8], data: &[u8]) {
        let stored = self.backend.put(piece, chunk, header, data);
        self.cur_bytes -= stored.removed;
        self.cur_bytes += stored.added;
        while self.cur_bytes > self.max_bytes {
            // Stop only when the store is empty (`None`); a zero-byte piece (`Some(0)`) must not
            // halt eviction while real over-budget bytes remain in higher pieces.
            let Some(freed) = self.backend.evict_lowest() else {
                break;
            };
            self.cur_bytes -= freed;
        }
    }

    /// The Acestream live piece header for `piece`, if known.
    pub fn piece_header(&self, piece: u64) -> Option<[u8; 8]> {
        self.backend.header(piece)
    }

    /// The TS payload of `(piece, chunk)` if still held. Borrowed from RAM on the memory backend;
    /// read from disk (and owned) on the disk backend — hence [`Cow`].
    pub fn chunk(&self, piece: u64, chunk: u16) -> Option<Cow<'_, [u8]>> {
        self.backend.get(piece, chunk)
    }

    /// True iff every chunk of `piece` is present.
    pub fn has_piece(&self, piece: u64) -> bool {
        self.backend.has_piece(piece, self.chunks_per_piece())
    }

    /// Sorted indices of fully-held pieces (for `Have` advertisement).
    pub fn have_pieces(&self) -> Vec<u64> {
        self.backend.have_pieces(self.chunks_per_piece())
    }

    /// `(min, max)` stored piece indices, or None if empty.
    pub fn window(&self) -> Option<(u64, u64)> {
        self.backend.window()
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
        assert_eq!(s.chunk(10, 0).as_deref(), Some(&[1, 2, 3, 4][..]));
        assert_eq!(s.chunk(10, 1).as_deref(), None);
        assert_eq!(s.chunk(11, 0).as_deref(), None);
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
        assert_eq!(s.chunk(1, 0).as_deref(), None, "lowest piece evicted");
        assert_eq!(s.window(), Some((2, 3)));
    }

    #[test]
    fn replacing_a_chunk_does_not_double_count_bytes() {
        let mut s = store(8);
        s.put_chunk(1, 0, &[0; 4]);
        s.put_chunk(1, 0, &[9; 4]); // replace, not add
        s.put_chunk(1, 1, &[0; 4]);
        // Still one piece (1) with two chunks = 8 bytes, nothing evicted.
        assert_eq!(s.chunk(1, 0).as_deref(), Some(&[9, 9, 9, 9][..]));
        assert!(s.has_piece(1));
    }

    #[test]
    fn budget_smaller_than_one_chunk_discards_gracefully() {
        let mut s = PieceStore::new(8, 4, 3); // max_bytes < chunk size
        s.put_chunk(1, 0, &[0; 4]);
        assert_eq!(s.chunk(1, 0).as_deref(), None); // evicted immediately, no panic
        assert_eq!(s.window(), None);
    }

    #[test]
    fn eviction_runs_multiple_iterations_if_needed() {
        let mut s = store(4); // 1-chunk budget
        s.put_chunk(1, 0, &[0; 4]);
        s.put_chunk(2, 0, &[0; 4]); // evicts piece 1
        s.put_chunk(3, 0, &[0; 4]); // evicts piece 2
        assert_eq!(s.chunk(1, 0).as_deref(), None);
        assert_eq!(s.chunk(2, 0).as_deref(), None);
        assert_eq!(s.window(), Some((3, 3)));
    }

    #[test]
    fn eviction_does_not_halt_on_a_zero_byte_lowest_piece() {
        // budget = 4 bytes. Piece 1 holds a zero-length chunk (0 bytes); pieces 2 and 3 each hold
        // a real 4-byte chunk. Adding piece 3 pushes over budget; eviction must skip past the
        // zero-byte piece 1 and keep evicting real bytes, not stop at the Some(0) piece.
        let mut s = store(4);
        s.put_chunk(1, 0, &[]); // 0 bytes, creates piece 1
        s.put_chunk(2, 0, &[0; 4]); // cur = 4 (at budget)
        s.put_chunk(3, 0, &[0; 4]); // cur = 8 > 4 -> must evict down to <= 4
        assert!(!s.has_piece(1), "zero-byte piece evicted, not skipped");
        assert_eq!(s.chunk(1, 0).as_deref(), None);
        assert_eq!(
            s.chunk(2, 0).as_deref(),
            None,
            "real bytes above it also evicted"
        );
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

    #[test]
    fn stores_a_piece_header_with_its_chunks() {
        let mut s = store(1024);
        let header = [0x41, 0xda, 0x91, 0x52, 0x26, 0x34, 0xc2, 0xee];

        s.put_chunk_with_header(9, 0, header, &[0; 4]);
        s.put_chunk_with_header(9, 1, header, &[1; 4]);

        assert_eq!(s.piece_header(9), Some(header));
    }

    #[test]
    fn evicts_piece_header_with_piece_data() {
        let mut s = store(8);

        s.put_chunk_with_header(1, 0, [1; 8], &[0; 4]);
        s.put_chunk_with_header(1, 1, [1; 8], &[0; 4]);
        s.put_chunk_with_header(2, 0, [2; 8], &[0; 4]);

        assert_eq!(s.piece_header(1), None);
        assert_eq!(s.piece_header(2), Some([2; 8]));
    }

    // ---- Disk backend ----

    #[test]
    fn disk_backend_round_trips_and_evicts() {
        let dir = tempfile::tempdir().unwrap();
        // piece_length 8, chunk_length 4, budget 8 bytes -> holds 1 piece (2 chunks).
        let mut s = PieceStore::new_disk(8, 4, 8, dir.path().join("ih")).unwrap();
        s.put_chunk_with_header(10, 0, [7u8; 8], &[1, 2, 3, 4]);
        s.put_chunk(10, 1, &[5, 6, 7, 8]);
        assert_eq!(s.chunk(10, 0).as_deref(), Some(&[1, 2, 3, 4][..]));
        assert_eq!(s.chunk(10, 1).as_deref(), Some(&[5, 6, 7, 8][..]));
        assert_eq!(s.piece_header(10), Some([7u8; 8]));
        assert!(s.has_piece(10));
        assert_eq!(s.have_pieces(), vec![10]);
        assert_eq!(s.window(), Some((10, 10)));
        assert!(dir.path().join("ih/10.piece").exists());

        // Writing a second piece exceeds the 8-byte budget -> evict piece 10 (lowest).
        s.put_chunk(11, 0, &[9, 9, 9, 9]);
        assert!(!s.has_piece(10), "lowest piece evicted");
        assert_eq!(s.chunk(10, 0).as_deref(), None, "evicted chunk unreadable");
        assert!(
            !dir.path().join("ih/10.piece").exists(),
            "evicted file deleted"
        );
        assert!(dir.path().join("ih/11.piece").exists());
    }

    #[test]
    fn disk_backend_wipes_stale_dir_on_creation() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("ih");
        // A prior run left a piece file behind.
        std::fs::create_dir_all(&cache).unwrap();
        std::fs::write(cache.join("99.piece"), b"stale").unwrap();

        let s = PieceStore::new_disk(8, 4, 1024, cache.clone()).unwrap();
        assert!(!cache.join("99.piece").exists(), "stale file wiped");
        assert_eq!(s.window(), None, "index starts empty");
    }

    #[test]
    fn disk_backend_fails_fast_on_unwritable_dir() {
        // A path *under a file* (not a directory) cannot be created.
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let bad = tmp.path().join("cannot/exist");
        assert!(PieceStore::new_disk(8, 4, 8, bad).is_err());
    }

    #[test]
    fn disk_backend_replacing_a_chunk_does_not_double_count_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = PieceStore::new_disk(8, 4, 8, dir.path().join("ih")).unwrap();
        s.put_chunk(1, 0, &[0; 4]);
        s.put_chunk(1, 0, &[9; 4]); // replace, not add
        s.put_chunk(1, 1, &[0; 4]);
        // Still one piece (1) with two chunks = 8 bytes, nothing evicted.
        assert_eq!(s.chunk(1, 0).as_deref(), Some(&[9, 9, 9, 9][..]));
        assert!(s.has_piece(1));
    }

    #[test]
    fn with_backend_disk_matches_memory_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = PieceStore::with_backend(
            8,
            4,
            1024,
            BackendKind::Disk {
                dir: dir.path().join("ih"),
            },
        )
        .unwrap();
        s.put_chunk(3, 0, &[1, 2, 3, 4]);
        assert_eq!(s.chunk(3, 0).as_deref(), Some(&[1, 2, 3, 4][..]));
        assert!(
            !s.has_piece(3),
            "partial piece (1 of 2 chunks) not complete"
        );
    }
}
