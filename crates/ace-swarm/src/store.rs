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
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Maximum request block accepted from a standard BitTorrent peer (BEP 3 convention).
pub const MAX_BT_REQUEST_LEN: u32 = 16 * 1024;

mod disk;
use disk::DiskBackend;

#[derive(Debug)]
pub struct PieceStore {
    piece_length: u64,
    chunk_length: u64,
    max_bytes: u64,
    cur_bytes: u64,
    backend: Backend,
    /// Optional age bound: pieces first seen longer ago than this are evicted even while under
    /// the byte budget. For a live stream this makes retained reseed data track *time* (window ×
    /// bitrate) instead of a fixed byte cap, so RAM scales with the stream rather than always
    /// filling `max_bytes`. `max_bytes` remains a hard safety ceiling. `None` preserves the
    /// original byte-only behavior (used by VOD/broadcast stores).
    retention: Option<Duration>,
    /// First-seen instant per retained piece index, kept in lockstep with the backend's piece set:
    /// every retained piece has an entry, and eviction removes it. Only populated when `retention`
    /// is set, to avoid the bookkeeping cost on byte-only stores.
    first_seen: BTreeMap<u64, Instant>,
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
    /// Read a chunk from a shared store without running disk I/O on a Tokio worker thread.
    /// Memory stores take the ordinary async lock and preserve their zero-copy internal path;
    /// disk stores perform the synchronous compatibility operation on the blocking pool.
    pub async fn shared_chunk(
        store: &Arc<Mutex<Self>>,
        piece: u64,
        chunk: u16,
    ) -> Option<(Vec<u8>, [u8; 8])> {
        let handle = {
            let guard = store.lock().await;
            match &guard.backend {
                Backend::Memory(_) => {
                    return guard.chunk(piece, chunk).map(|data| {
                        (
                            data.into_owned(),
                            guard.piece_header(piece).unwrap_or([0; 8]),
                        )
                    });
                }
                Backend::Disk(disk) => disk.handle(),
            }
        };
        handle.get(piece, chunk).await
    }

    /// Read a standard BitTorrent byte block from a piece stored as request-sized chunks.
    /// Returns `None` unless every byte in the requested range is retained.
    pub async fn shared_block(
        store: &Arc<Mutex<Self>>,
        piece: u64,
        begin: u32,
        length: u32,
    ) -> Option<Vec<u8>> {
        if length == 0 || length > MAX_BT_REQUEST_LEN {
            return None;
        }
        let (chunk_length, piece_length) = {
            let guard = store.lock().await;
            (guard.chunk_length, guard.piece_length)
        };
        let end = u64::from(begin).checked_add(u64::from(length))?;
        // Bound hostile request geometry before looping or allocating. BEP 3 blocks are normally
        // 16 KiB; permit larger compatible requests up to one piece, but never beyond it.
        if end > piece_length {
            return None;
        }
        let first = u64::from(begin) / chunk_length;
        let last = end.div_ceil(chunk_length);
        let mut piece_bytes = Vec::new();
        for chunk in first..last {
            let (data, _) = Self::shared_chunk(store, piece, u16::try_from(chunk).ok()?).await?;
            piece_bytes.extend_from_slice(&data);
        }
        let offset = (u64::from(begin) % chunk_length) as usize;
        let requested_end = offset.checked_add(length as usize)?;
        Some(piece_bytes.get(offset..requested_end)?.to_vec())
    }

    /// Write a chunk to a shared store without running disk I/O on a Tokio worker thread.
    pub async fn shared_put_chunk_with_header(
        store: &Arc<Mutex<Self>>,
        piece: u64,
        chunk: u16,
        header: [u8; 8],
        data: &[u8],
    ) {
        let (handle, max_bytes) = {
            let mut guard = store.lock().await;
            if guard.max_bytes == 0 || (chunk as u64) >= guard.piece_length / guard.chunk_length {
                return;
            }
            match &guard.backend {
                Backend::Memory(_) => {
                    guard.put_chunk_with_header(piece, chunk, header, data);
                    return;
                }
                Backend::Disk(disk) => (disk.handle(), guard.max_bytes),
            }
        };
        handle
            .put(piece, chunk, header, data.to_vec(), max_bytes)
            .await;
    }

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
            retention: None,
            first_seen: BTreeMap::new(),
        })
    }

    /// Bound retained pieces by age as well as bytes: on each write, pieces first seen more than
    /// `retention` ago are evicted (oldest first), so a live seed store holds ~`retention` × bitrate
    /// of recent data instead of always growing to `max_bytes`. `max_bytes` still caps the store as
    /// a hard ceiling. A zero `retention` is treated as "no age bound".
    pub fn with_retention(mut self, retention: Duration) -> Self {
        self.retention = (!retention.is_zero()).then_some(retention);
        self
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
        self.put_chunk_with_header_at(Instant::now(), piece, chunk, header, data);
    }

    /// [`put_chunk_with_header`](Self::put_chunk_with_header) with an explicit "now", so age-based
    /// eviction is deterministic under test. Production callers use the wall-clock wrapper.
    fn put_chunk_with_header_at(
        &mut self,
        now: Instant,
        piece: u64,
        chunk: u16,
        header: [u8; 8],
        data: &[u8],
    ) {
        // A zero budget is the explicit no-retention policy used when disk-mode construction
        // fails. Do not even touch backend metadata: empty payloads add zero accounted bytes and
        // would otherwise accumulate piece/chunk/header entries without triggering eviction.
        if self.max_bytes == 0 {
            return;
        }
        // Chunk indices arrive straight off the wire (id=7). Drop any index outside the piece's
        // geometry so a stray value can't sit in the chunk map — inflating its length so
        // `has_piece` never counts the piece complete (it would then never be reseeded) and
        // wasting byte budget that should hold real reseed data (issue #91). A valid chunk index
        // is always < chunks_per_piece, even for a short final piece (which has fewer chunks).
        // Compared as u64 to avoid the u16 truncation in `chunks_per_piece`.
        if (chunk as u64) >= self.piece_length / self.chunk_length {
            return;
        }
        let stored = self.backend.put(piece, chunk, header, data);
        if self.retention.is_some() {
            self.first_seen.entry(piece).or_insert(now);
        }
        self.cur_bytes -= stored.removed;
        self.cur_bytes += stored.added;
        while self.cur_bytes > self.max_bytes {
            // Stop only when the store is empty (`None`); a zero-byte piece (`Some(0)`) must not
            // halt eviction while real over-budget bytes remain in higher pieces.
            let Some(freed) = self.evict_lowest_tracked() else {
                break;
            };
            self.cur_bytes -= freed;
        }
        // Age eviction: drop the oldest pieces once they fall outside the retention window. Pieces
        // arrive in increasing index/time order on the live path, so the lowest-index piece is also
        // the oldest — the same order the byte-budget loop evicts in.
        if let Some(retention) = self.retention {
            while let Some((_, &seen)) = self.first_seen.iter().next() {
                if now.saturating_duration_since(seen) < retention {
                    break;
                }
                let Some(freed) = self.evict_lowest_tracked() else {
                    break;
                };
                self.cur_bytes -= freed;
            }
        }
    }

    /// Evict the lowest-index (oldest) piece from the backend and drop its `first_seen` entry so the
    /// age index stays in lockstep with the stored piece set. Returns the bytes freed.
    fn evict_lowest_tracked(&mut self) -> Option<u64> {
        let freed = self.backend.evict_lowest()?;
        self.first_seen.pop_first();
        Some(freed)
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

    #[tokio::test]
    async fn standard_block_validates_request_geometry_and_short_final_piece() {
        let store = Arc::new(Mutex::new(PieceStore::new(32_768, 16_384, 65_536)));
        PieceStore::shared_put_chunk_with_header(&store, 0, 0, [0; 8], &vec![1; 16_384]).await;
        PieceStore::shared_put_chunk_with_header(&store, 0, 1, [0; 8], &vec![2; 16_384]).await;
        PieceStore::shared_put_chunk_with_header(&store, 1, 0, [0; 8], &[3; 100]).await;

        assert_eq!(
            PieceStore::shared_block(&store, 0, 16_380, 8)
                .await
                .unwrap(),
            [vec![1; 4], vec![2; 4]].concat()
        );
        assert!(PieceStore::shared_block(&store, 0, 0, 0).await.is_none());
        assert!(
            PieceStore::shared_block(&store, 0, 0, MAX_BT_REQUEST_LEN + 1)
                .await
                .is_none()
        );
        assert!(PieceStore::shared_block(&store, 0, u32::MAX, 1)
            .await
            .is_none());
        assert!(
            PieceStore::shared_block(&store, 1, 96, 8).await.is_none(),
            "request crossing the retained short final piece is rejected"
        );
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

    // Fill piece `p` (both 4-byte chunks) at instant `t`.
    fn put_full_at(s: &mut PieceStore, t: Instant, p: u64) {
        s.put_chunk_with_header_at(t, p, 0, [0; 8], &[9; 4]);
        s.put_chunk_with_header_at(t, p, 1, [0; 8], &[9; 4]);
    }

    #[test]
    fn retention_evicts_pieces_older_than_window() {
        let mut s = store(1 << 20).with_retention(Duration::from_secs(30));
        let t0 = Instant::now();
        for p in 0..3 {
            put_full_at(&mut s, t0, p);
        }
        assert_eq!(s.window(), Some((0, 2)));
        // A write 31s later pushes the t0 pieces outside the 30s window, evicting them.
        put_full_at(&mut s, t0 + Duration::from_secs(31), 3);
        assert_eq!(
            s.window(),
            Some((3, 3)),
            "pieces older than the retention window are evicted even under the byte budget"
        );
    }

    #[test]
    fn retention_keeps_pieces_within_window() {
        let mut s = store(1 << 20).with_retention(Duration::from_secs(30));
        let t0 = Instant::now();
        put_full_at(&mut s, t0, 0);
        put_full_at(&mut s, t0 + Duration::from_secs(10), 1);
        assert_eq!(
            s.window(),
            Some((0, 1)),
            "both pieces are within the 30s window and are retained"
        );
    }

    #[test]
    fn retention_respects_byte_ceiling() {
        // Generous window, but the byte ceiling only holds two full 8-byte pieces.
        let mut s = store(16).with_retention(Duration::from_secs(3600));
        let t0 = Instant::now();
        for p in 0..4 {
            put_full_at(&mut s, t0, p);
        }
        assert_eq!(
            s.window(),
            Some((2, 3)),
            "byte ceiling still caps the store when everything is inside the window"
        );
    }

    #[test]
    fn without_retention_skips_age_bookkeeping() {
        let mut s = store(1 << 20);
        s.put_chunk(0, 0, &[1, 2, 3, 4]);
        assert!(
            s.first_seen.is_empty(),
            "byte-only stores must not pay the per-piece age-tracking cost"
        );
    }

    #[test]
    fn ignores_a_chunk_index_outside_the_piece_geometry() {
        // Chunk indices come straight off the wire (id=7). A stray out-of-range index must not
        // enter the piece's chunk map: otherwise it inflates the map length so `has_piece`
        // never counts the piece complete (it would never be reseeded) and it wastes byte
        // budget that should hold real reseed data (issue #91).
        let mut s = store(1024);
        assert_eq!(s.chunks_per_piece(), 2); // valid chunk indices are 0 and 1
        s.put_chunk(5, 0, &[1, 2, 3, 4]);
        s.put_chunk(5, 99, &[9, 9, 9, 9]); // bogus index from a hostile/buggy peer
        s.put_chunk(5, 1, &[5, 6, 7, 8]);
        assert!(
            s.has_piece(5),
            "a stray chunk index must not block the piece from completing"
        );
        assert_eq!(s.have_pieces(), vec![5]);
        assert_eq!(
            s.chunk(5, 99),
            None,
            "the out-of-range chunk must never be stored"
        );
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
    fn zero_budget_retains_no_payload_or_metadata_even_for_empty_chunks() {
        let mut store = PieceStore::new(8, 4, 0);
        for piece in 0..10_000 {
            store.put_chunk_with_header(piece, 0, [0x55; 8], &[]);
            store.put_chunk_with_header(piece, 1, [0xaa; 8], &[1, 2, 3, 4]);
        }

        assert!(store.chunk(0, 0).is_none());
        assert!(store.chunk(9_999, 1).is_none());
        assert!(store.piece_header(0).is_none());
        assert!(store.piece_header(9_999).is_none());
        assert!(store.have_pieces().is_empty());
        assert_eq!(store.window(), None);
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

    #[tokio::test]
    async fn cancelled_disk_put_still_commits_index_and_budget_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Mutex::new(
            PieceStore::new_disk(4, 4, 4, dir.path().join("cache")).unwrap(),
        ));
        PieceStore::shared_put_chunk_with_header(&store, 1, 0, [1; 8], b"1111").await;
        let task = tokio::spawn({
            let store = Arc::clone(&store);
            async move {
                PieceStore::shared_put_chunk_with_header(&store, 2, 0, [2; 8], b"2222").await;
            }
        });
        tokio::task::yield_now().await; // command has been submitted; cancellation drops its reply
        task.abort();
        for _ in 0..100 {
            if store.lock().await.has_piece(2) {
                break;
            }
            tokio::task::yield_now().await;
        }
        let guard = store.lock().await;
        assert!(
            guard.has_piece(2),
            "actor commits after the caller is cancelled"
        );
        assert!(
            !guard.has_piece(1),
            "the same actor transaction enforces the budget"
        );
        assert_eq!(guard.chunk(2, 0).unwrap().as_ref(), b"2222");
    }
}
