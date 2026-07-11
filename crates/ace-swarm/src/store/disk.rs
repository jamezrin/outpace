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
//! I/O is synchronous at this compatibility boundary; async serve/download callers use
//! `PieceStore::shared_chunk` and `shared_put_chunk_with_header` to dispatch it to Tokio's blocking
//! pool. Each indexed piece owns one open handle and uses positioned reads/writes, avoiding both
//! per-chunk opens and a shared seek cursor.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;

use crate::store::Stored;

/// RAM-side record for one piece: its header plus the byte length of each present chunk (chunks
/// can be short at a piece tail, so lengths are tracked rather than assumed `chunk_length`).
#[derive(Debug)]
struct PieceIndex {
    header: [u8; 8],
    /// Kept open for the lifetime of the cached piece. Positioned I/O makes the handle safe to
    /// reuse for every chunk without a shared seek cursor.
    file: File,
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
        let new_file = if self.index.contains_key(&piece) {
            None
        } else {
            match std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(self.piece_path(piece))
            {
                Ok(file) => Some(file),
                Err(e) => {
                    crate::swarm_log!(
                        "[cache] disk open failed for piece {piece} chunk {chunk}: {e}"
                    );
                    return Stored::default();
                }
            }
        };
        let file = self
            .index
            .get(&piece)
            .map(|entry| &entry.file)
            .or(new_file.as_ref())
            .expect("existing or newly opened piece file");
        let write_res = write_all_at(file, data, off);
        if let Err(e) = write_res {
            // Best-effort, non-fatal: drop the chunk and leave accounting untouched. The index is
            // not updated, so the chunk simply reads back as absent.
            crate::swarm_log!("[cache] disk write failed for piece {piece} chunk {chunk}: {e}");
            // `.create(true)` may have made the file before the write failed. If this piece has no
            // index entry yet, that file is untracked — eviction (index-driven) would never reclaim
            // it — so remove it now. A piece already in the index keeps its file (it holds valid
            // earlier chunks).
            if !self.index.contains_key(&piece) {
                let _ = std::fs::remove_file(self.piece_path(piece));
            }
            return Stored::default();
        }
        let entry = self.index.entry(piece).or_insert_with(|| PieceIndex {
            header,
            file: new_file.expect("new piece has its newly opened file"),
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
        // A chunk absent from the index is a normal miss (evicted / never held) — stay quiet.
        let len = *self.index.get(&piece)?.present.get(&chunk)? as usize;
        let off = chunk as u64 * self.chunk_length;
        let read = || -> std::io::Result<Vec<u8>> {
            let mut buf = vec![0u8; len];
            read_exact_at(
                &self.index.get(&piece).expect("checked above").file,
                &mut buf,
                off,
            )?;
            Ok(buf)
        };
        match read() {
            Ok(buf) => Some(Cow::Owned(buf)),
            // The index says this chunk is present (so it may already be advertised via `Have`),
            // yet the disk read failed. Log it — a silent `None` here looks like an ordinary miss
            // to the seeder but is really an unservable advertised piece.
            Err(e) => {
                crate::swarm_log!(
                    "[cache] disk read failed for present piece {piece} chunk {chunk}: {e}"
                );
                None
            }
        }
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

    pub(super) fn evict_lowest(&mut self) -> Option<u64> {
        let (&lowest, _) = self.index.iter().next()?;
        let entry = self.index.remove(&lowest).expect("key just observed");
        if let Err(e) = std::fs::remove_file(self.piece_path(lowest)) {
            // The bytes are dropped from the budget regardless (the index entry is gone), but a
            // failed unlink means they still occupy disk — surface it rather than leaking silently.
            if e.kind() != std::io::ErrorKind::NotFound {
                crate::swarm_log!("[cache] disk evict failed to remove piece {lowest} file: {e}");
            }
        }
        Some(entry.present.values().map(|&len| len as u64).sum())
    }
}

#[cfg(unix)]
fn write_all_at(file: &File, mut data: &[u8], mut offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    while !data.is_empty() {
        let written = file.write_at(data, offset)?;
        if written == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        data = &data[written..];
        offset += written as u64;
    }
    Ok(())
}

#[cfg(unix)]
fn read_exact_at(file: &File, mut data: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    while !data.is_empty() {
        let read = file.read_at(data, offset)?;
        if read == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        data = &mut data[read..];
        offset += read as u64;
    }
    Ok(())
}

#[cfg(windows)]
fn write_all_at(file: &File, data: &[u8], offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    file.seek_write(data, offset).and_then(|n| {
        (n == data.len())
            .then_some(())
            .ok_or_else(|| std::io::ErrorKind::WriteZero.into())
    })
}

#[cfg(windows)]
fn read_exact_at(file: &File, data: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    file.seek_read(data, offset).and_then(|n| {
        (n == data.len())
            .then_some(())
            .ok_or_else(|| std::io::ErrorKind::UnexpectedEof.into())
    })
}

impl Drop for DiskBackend {
    /// Remove this store's private directory. Because the owning `PieceStore` is `Arc`-shared,
    /// this runs only when the last holder (registry entry, in-flight seed peers, ingest) releases
    /// it — so a live writer can never resurrect a dir after teardown, and the per-instance
    /// `-<generation>` name means we only ever delete our own data. Best-effort: a failed unlink is
    /// logged, not fatal. Sync I/O (v1 tradeoff, see module header / #37).
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir_all(&self.dir) {
            if e.kind() != std::io::ErrorKind::NotFound {
                crate::swarm_log!(
                    "[cache] disk cleanup failed for {}: {e}",
                    self.dir.display()
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn chunks_reuse_one_open_piece_handle() {
        use std::os::fd::AsRawFd;

        let tmp = tempfile::tempdir().unwrap();
        let mut backend = DiskBackend::new(tmp.path().join("cache"), 4).unwrap();
        backend.put(7, 0, [0; 8], b"aaaa");
        let fd = backend.index.get(&7).unwrap().file.as_raw_fd();
        backend.put(7, 1, [0; 8], b"bbbb");
        assert_eq!(backend.index.get(&7).unwrap().file.as_raw_fd(), fd);
        assert_eq!(backend.get(7, 0).unwrap().as_ref(), b"aaaa");
        assert_eq!(backend.index.get(&7).unwrap().file.as_raw_fd(), fd);
    }

    #[test]
    fn dropping_backend_removes_its_directory() {
        let tmp = std::env::temp_dir().join(format!("outpace-diskdrop-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let dir = tmp.join("aabb-0");
        {
            let mut b = DiskBackend::new(dir.clone(), 4).unwrap();
            b.put(0, 0, [0u8; 8], b"data");
            assert!(dir.exists(), "dir exists while backend is alive");
        }
        assert!(!dir.exists(), "dir is removed when the backend drops");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn dropping_one_backend_does_not_touch_another_infohash_generation() {
        let tmp = std::env::temp_dir().join(format!("outpace-diskdrop2-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let dir_a = tmp.join("aabb-0");
        let dir_b = tmp.join("aabb-1"); // same infohash, newer generation
        let b_new = DiskBackend::new(dir_b.clone(), 4).unwrap();
        {
            let _b_old = DiskBackend::new(dir_a.clone(), 4).unwrap();
        } // old drops here
        assert!(
            dir_b.exists(),
            "the newer instance's dir survives the old one's Drop"
        );
        drop(b_new);
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
