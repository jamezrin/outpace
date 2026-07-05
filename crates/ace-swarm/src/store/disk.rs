//! Disk backend for [`PieceStore`](super::PieceStore): chunk payloads spill to one file per
//! piece under a per-store directory, keeping RAM use flat while retaining far more reseed data.
//!
//! A small in-RAM index answers every metadata query (`header`, `has_piece`, `have_pieces`,
//! `window`) so only [`get`](DiskBackend::get) and [`put`](DiskBackend::put) touch the
//! filesystem. Within a piece file, chunk `i` lives at byte offset `i * chunk_length`; the
//! 8-byte live header is kept only in the index (like the memory backend), never on disk.
//!
//! The cache is **ephemeral**: the directory is wiped on creation and never reloaded across
//! restarts, since live piece data goes stale — this also sidesteps serving evicted-stale data.
//!
//! I/O is synchronous. Chunk writes are ~16 KiB and reads ≤1 MiB, hitting page cache almost
//! immediately on the live path; if profiling ever shows reactor stalls under disk mode, move
//! these ops to `spawn_blocking` — the backend boundary keeps that localized.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::store::Stored;

/// RAM-side record for one piece: its header plus the byte length of each present chunk (chunks
/// can be short at a piece tail, so lengths are tracked rather than assumed `chunk_length`).
#[derive(Debug)]
struct PieceIndex {
    header: [u8; 8],
    /// present chunk index -> stored byte length.
    present: BTreeMap<u16, u32>,
}

#[derive(Debug)]
pub(super) struct DiskBackend {
    dir: PathBuf,
    chunk_length: u64,
    index: BTreeMap<u64, PieceIndex>,
}

impl DiskBackend {
    /// Prepare `dir` as an empty cache root, wiping any stale contents from a prior run. Fails
    /// (so the caller can fail fast at startup) if the directory cannot be created — e.g. the
    /// path is unwritable or sits under a non-directory.
    pub(super) fn new(dir: PathBuf, chunk_length: u64) -> std::io::Result<Self> {
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        std::fs::create_dir_all(&dir)?;
        Ok(DiskBackend {
            dir,
            chunk_length,
            index: BTreeMap::new(),
        })
    }

    fn piece_path(&self, piece: u64) -> PathBuf {
        self.dir.join(format!("{piece}.piece"))
    }

    pub(super) fn put(&mut self, piece: u64, chunk: u16, header: [u8; 8], data: &[u8]) -> Stored {
        let off = chunk as u64 * self.chunk_length;
        let write_res = std::fs::OpenOptions::new()
            .create(true)
            // Never truncate: chunks share the piece file at fixed offsets, so an existing file
            // must keep the chunks already written to it.
            .truncate(false)
            .write(true)
            .open(self.piece_path(piece))
            .and_then(|mut f| {
                f.seek(SeekFrom::Start(off))?;
                f.write_all(data)
            });
        if let Err(e) = write_res {
            // Best-effort, non-fatal: drop the chunk and leave accounting untouched. The index is
            // not updated, so the chunk simply reads back as absent.
            crate::swarm_log!("[cache] disk write failed for piece {piece} chunk {chunk}: {e}");
            return Stored::default();
        }
        let entry = self.index.entry(piece).or_insert_with(|| PieceIndex {
            header,
            present: BTreeMap::new(),
        });
        // Header-upgrade rule, mirroring the memory backend: a nonzero header replaces an earlier
        // zero placeholder, but a later zero never overwrites a known header.
        if entry.header == [0u8; 8] && header != [0u8; 8] {
            entry.header = header;
        }
        let removed = entry
            .present
            .insert(chunk, data.len() as u32)
            .map_or(0, |old| old as u64);
        Stored {
            added: data.len() as u64,
            removed,
        }
    }

    pub(super) fn get(&self, piece: u64, chunk: u16) -> Option<Cow<'_, [u8]>> {
        let len = *self.index.get(&piece)?.present.get(&chunk)? as usize;
        let off = chunk as u64 * self.chunk_length;
        let mut f = std::fs::File::open(self.piece_path(piece)).ok()?;
        f.seek(SeekFrom::Start(off)).ok()?;
        let mut buf = vec![0u8; len];
        f.read_exact(&mut buf).ok()?;
        Some(Cow::Owned(buf))
    }

    pub(super) fn header(&self, piece: u64) -> Option<[u8; 8]> {
        self.index.get(&piece).map(|e| e.header)
    }

    pub(super) fn has_piece(&self, piece: u64, chunks_per_piece: u16) -> bool {
        self.index
            .get(&piece)
            .is_some_and(|e| e.present.len() as u16 == chunks_per_piece)
    }

    pub(super) fn have_pieces(&self, chunks_per_piece: u16) -> Vec<u64> {
        self.index
            .iter()
            .filter(|(_, e)| e.present.len() as u16 == chunks_per_piece)
            .map(|(&p, _)| p)
            .collect()
    }

    pub(super) fn window(&self) -> Option<(u64, u64)> {
        let min = *self.index.keys().next()?;
        let max = *self.index.keys().next_back()?;
        Some((min, max))
    }

    pub(super) fn evict_lowest(&mut self) -> u64 {
        let Some((&lowest, _)) = self.index.iter().next() else {
            return 0;
        };
        let entry = self.index.remove(&lowest).expect("key just observed");
        let _ = std::fs::remove_file(self.piece_path(lowest));
        entry.present.values().map(|&len| len as u64).sum()
    }
}
