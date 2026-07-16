//! Disk-backed piece storage. A dedicated per-store actor exclusively owns filesystem handles and
//! serializes open/positioned-read/positioned-write/unlink operations. The small metadata index is
//! shared separately and is locked only after I/O completes, never across a filesystem syscall.

use std::borrow::Cow;
use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex};

use crate::store::Stored;

const MAX_OPEN_PIECES: usize = 64;

#[derive(Debug, Default)]
struct PieceIndex {
    header: [u8; 8],
    present: BTreeMap<u16, u32>,
}

#[derive(Debug, Default)]
struct State {
    index: BTreeMap<u64, PieceIndex>,
    cur_bytes: u64,
}

enum Command {
    Put {
        piece: u64,
        chunk: u16,
        header: [u8; 8],
        data: Vec<u8>,
        max_bytes: u64,
        reply: tokio::sync::oneshot::Sender<()>,
    },
    Get {
        piece: u64,
        chunk: u16,
        reply: tokio::sync::oneshot::Sender<Option<(Vec<u8>, [u8; 8])>>,
    },
    SyncPut {
        piece: u64,
        chunk: u16,
        header: [u8; 8],
        data: Vec<u8>,
        max_bytes: u64,
        reply: mpsc::SyncSender<Stored>,
    },
    SyncGet {
        piece: u64,
        chunk: u16,
        reply: mpsc::SyncSender<Option<Vec<u8>>>,
    },
    SyncEvict {
        reply: mpsc::SyncSender<Option<u64>>,
    },
    #[cfg(test)]
    InspectHandles { reply: mpsc::SyncSender<usize> },
}

#[derive(Debug)]
pub(super) struct DiskBackend {
    tx: mpsc::Sender<Command>,
    state: Arc<Mutex<State>>,
}

#[derive(Clone)]
pub(super) struct DiskHandle {
    tx: mpsc::Sender<Command>,
}

impl DiskBackend {
    pub(super) fn handle(&self) -> DiskHandle {
        DiskHandle {
            tx: self.tx.clone(),
        }
    }
    pub(super) fn new(dir: PathBuf, chunk_length: u64) -> std::io::Result<Self> {
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        std::fs::create_dir_all(&dir)?;
        let state = Arc::new(Mutex::new(State::default()));
        let (tx, rx) = mpsc::channel();
        let actor_state = Arc::clone(&state);
        std::thread::Builder::new()
            .name("outpace-disk-cache".into())
            .spawn(move || Actor::new(dir, chunk_length, actor_state).run(rx))?;
        Ok(Self { tx, state })
    }

    pub(super) fn put(&mut self, piece: u64, chunk: u16, header: [u8; 8], data: &[u8]) -> Stored {
        let (reply, done) = mpsc::sync_channel(1);
        if self
            .tx
            .send(Command::SyncPut {
                piece,
                chunk,
                header,
                data: data.to_vec(),
                max_bytes: u64::MAX,
                reply,
            })
            .is_err()
        {
            return Stored::default();
        }
        done.recv().unwrap_or_default()
    }

    pub(super) fn get(&self, piece: u64, chunk: u16) -> Option<Cow<'_, [u8]>> {
        let (reply, done) = mpsc::sync_channel(1);
        self.tx
            .send(Command::SyncGet {
                piece,
                chunk,
                reply,
            })
            .ok()?;
        done.recv().ok().flatten().map(Cow::Owned)
    }

    pub(super) fn header(&self, piece: u64) -> Option<[u8; 8]> {
        self.state
            .lock()
            .unwrap()
            .index
            .get(&piece)
            .map(|e| e.header)
    }

    pub(super) fn has_piece(&self, piece: u64, chunks: u16) -> bool {
        self.state
            .lock()
            .unwrap()
            .index
            .get(&piece)
            .is_some_and(|e| e.present.len() as u16 == chunks)
    }

    pub(super) fn have_pieces(&self, chunks: u16) -> Vec<u64> {
        self.state
            .lock()
            .unwrap()
            .index
            .iter()
            .filter(|(_, e)| e.present.len() as u16 == chunks)
            .map(|(&p, _)| p)
            .collect()
    }

    pub(super) fn window(&self) -> Option<(u64, u64)> {
        let state = self.state.lock().unwrap();
        Some((
            *state.index.keys().next()?,
            *state.index.keys().next_back()?,
        ))
    }

    pub(super) fn evict_lowest(&mut self) -> Option<u64> {
        let (reply, done) = mpsc::sync_channel(1);
        self.tx.send(Command::SyncEvict { reply }).ok()?;
        done.recv().ok().flatten()
    }

    #[cfg(test)]
    fn open_handles(&self) -> usize {
        let (reply, done) = mpsc::sync_channel(1);
        self.tx.send(Command::InspectHandles { reply }).unwrap();
        done.recv().unwrap()
    }
}

impl DiskHandle {
    pub(super) async fn put(
        &self,
        piece: u64,
        chunk: u16,
        header: [u8; 8],
        data: Vec<u8>,
        max_bytes: u64,
    ) {
        let (reply, done) = tokio::sync::oneshot::channel();
        if self
            .tx
            .send(Command::Put {
                piece,
                chunk,
                header,
                data,
                max_bytes,
                reply,
            })
            .is_ok()
        {
            let _ = done.await;
        }
    }
    pub(super) async fn get(&self, piece: u64, chunk: u16) -> Option<(Vec<u8>, [u8; 8])> {
        let (reply, done) = tokio::sync::oneshot::channel();
        self.tx
            .send(Command::Get {
                piece,
                chunk,
                reply,
            })
            .ok()?;
        done.await.ok().flatten()
    }
}

struct Actor {
    dir: PathBuf,
    chunk_length: u64,
    state: Arc<Mutex<State>>,
    files: BTreeMap<u64, File>,
    lru: VecDeque<u64>,
}

impl Actor {
    fn new(dir: PathBuf, chunk_length: u64, state: Arc<Mutex<State>>) -> Self {
        Self {
            dir,
            chunk_length,
            state,
            files: BTreeMap::new(),
            lru: VecDeque::new(),
        }
    }

    fn run(mut self, rx: mpsc::Receiver<Command>) {
        while let Ok(command) = rx.recv() {
            match command {
                Command::Put {
                    piece,
                    chunk,
                    header,
                    data,
                    max_bytes,
                    reply,
                } => {
                    let _ = self.put(piece, chunk, header, &data, max_bytes);
                    let _ = reply.send(());
                }
                Command::Get {
                    piece,
                    chunk,
                    reply,
                } => {
                    let _ = reply.send(self.get(piece, chunk));
                }
                Command::SyncPut {
                    piece,
                    chunk,
                    header,
                    data,
                    max_bytes,
                    reply,
                } => {
                    let _ = reply.send(self.put(piece, chunk, header, &data, max_bytes));
                }
                Command::SyncGet {
                    piece,
                    chunk,
                    reply,
                } => {
                    let _ = reply.send(self.get(piece, chunk).map(|v| v.0));
                }
                Command::SyncEvict { reply } => {
                    let _ = reply.send(self.evict_lowest());
                }
                #[cfg(test)]
                Command::InspectHandles { reply } => {
                    let _ = reply.send(self.files.len());
                }
            }
        }
        self.files.clear();
        if let Err(e) = std::fs::remove_dir_all(&self.dir) {
            if e.kind() != std::io::ErrorKind::NotFound {
                crate::alog!(
                    "[cache] disk cleanup failed for {}: {e}",
                    self.dir.display()
                );
            }
        }
    }

    fn path(&self, piece: u64) -> PathBuf {
        self.dir.join(format!("{piece}.piece"))
    }

    fn file(&mut self, piece: u64, create: bool) -> std::io::Result<&File> {
        if !self.files.contains_key(&piece) {
            let file = std::fs::OpenOptions::new()
                .create(create)
                .truncate(false)
                .read(true)
                .write(create)
                .open(self.path(piece))?;
            if self.files.len() == MAX_OPEN_PIECES {
                if let Some(oldest) = self.lru.pop_front() {
                    self.files.remove(&oldest);
                }
            }
            self.files.insert(piece, file);
        }
        if let Some(pos) = self.lru.iter().position(|&p| p == piece) {
            self.lru.remove(pos);
        }
        self.lru.push_back(piece);
        Ok(self.files.get(&piece).expect("inserted"))
    }

    fn put(&mut self, piece: u64, chunk: u16, header: [u8; 8], data: &[u8], max: u64) -> Stored {
        let offset = chunk as u64 * self.chunk_length;
        if let Err(e) = self
            .file(piece, true)
            .and_then(|f| write_all_at(f, data, offset))
        {
            crate::alog!("[cache] disk write failed for piece {piece} chunk {chunk}: {e}");
            return Stored::default();
        }
        let (stored, evicted) = {
            let mut state = self.state.lock().unwrap();
            let entry = state.index.entry(piece).or_insert_with(|| PieceIndex {
                header,
                present: BTreeMap::new(),
            });
            if entry.header == [0; 8] && header != [0; 8] {
                entry.header = header;
            }
            let removed = entry
                .present
                .insert(chunk, data.len() as u32)
                .map_or(0, u64::from);
            let stored = Stored {
                added: data.len() as u64,
                removed,
            };
            state.cur_bytes = state.cur_bytes - removed + data.len() as u64;
            let mut evicted = Vec::new();
            while state.cur_bytes > max {
                let Some((&lowest, _)) = state.index.iter().next() else {
                    break;
                };
                let entry = state.index.remove(&lowest).unwrap();
                let freed = entry.present.values().map(|&n| n as u64).sum::<u64>();
                state.cur_bytes -= freed;
                evicted.push(lowest);
            }
            (stored, evicted)
        };
        for piece in evicted {
            self.remove(piece);
        }
        stored
    }

    fn get(&mut self, piece: u64, chunk: u16) -> Option<(Vec<u8>, [u8; 8])> {
        let (len, header) = {
            let state = self.state.lock().unwrap();
            let entry = state.index.get(&piece)?;
            (*entry.present.get(&chunk)? as usize, entry.header)
        };
        let offset = chunk as u64 * self.chunk_length;
        let mut data = vec![0; len];
        read_exact_at(self.file(piece, false).ok()?, &mut data, offset).ok()?;
        Some((data, header))
    }

    fn remove(&mut self, piece: u64) {
        self.files.remove(&piece);
        if let Some(pos) = self.lru.iter().position(|&p| p == piece) {
            self.lru.remove(pos);
        }
        if let Err(e) = std::fs::remove_file(self.path(piece)) {
            if e.kind() != std::io::ErrorKind::NotFound {
                crate::alog!("[cache] disk evict failed to remove piece {piece}: {e}");
            }
        }
    }

    fn evict_lowest(&mut self) -> Option<u64> {
        let (piece, freed) = {
            let mut state = self.state.lock().unwrap();
            let (&piece, _) = state.index.iter().next()?;
            let entry = state.index.remove(&piece)?;
            let freed = entry.present.values().map(|&n| n as u64).sum();
            state.cur_bytes = state.cur_bytes.saturating_sub(freed);
            (piece, freed)
        };
        self.remove(piece);
        Some(freed)
    }
}

#[cfg(unix)]
fn write_all_at(file: &File, mut data: &[u8], mut offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    while !data.is_empty() {
        let n = file.write_at(data, offset)?;
        if n == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        data = &data[n..];
        offset += n as u64;
    }
    Ok(())
}
#[cfg(unix)]
fn read_exact_at(file: &File, mut data: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    while !data.is_empty() {
        let n = file.read_at(data, offset)?;
        if n == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        data = &mut data[n..];
        offset += n as u64;
    }
    Ok(())
}
#[cfg(windows)]
fn write_all_at(file: &File, data: &[u8], offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    (file.seek_write(data, offset)? == data.len())
        .then_some(())
        .ok_or_else(|| std::io::ErrorKind::WriteZero.into())
}
#[cfg(windows)]
fn read_exact_at(file: &File, data: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    (file.seek_read(data, offset)? == data.len())
        .then_some(())
        .ok_or_else(|| std::io::ErrorKind::UnexpectedEof.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actor_fd_lru_is_bounded_and_eviction_closes_handles() {
        let temp = tempfile::tempdir().unwrap();
        let mut backend = DiskBackend::new(temp.path().join("cache"), 4).unwrap();
        for piece in 0..(MAX_OPEN_PIECES as u64 + 8) {
            backend.put(piece, 0, [0; 8], b"data");
        }
        assert_eq!(backend.open_handles(), MAX_OPEN_PIECES);
        while backend.evict_lowest().is_some() {}
        assert_eq!(backend.open_handles(), 0);
    }
}
