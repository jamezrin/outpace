//! Disk-backed piece storage. A dedicated per-store actor exclusively owns filesystem handles and
//! serializes open/positioned-read/positioned-write/unlink operations. The small metadata index is
//! shared separately and is locked only after I/O completes, never across a filesystem syscall.

use std::borrow::Cow;
use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex};

use crate::store::{CompletePut, Stored};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

// The command owns admission through commit/failure, even if its reply is dropped.
struct PendingWire {
    data: Vec<u8>,
    pending: Arc<AtomicUsize>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Drop for PendingWire {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.data));
        self.pending.store(0, Ordering::Release);
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
struct PublicationHooks {
    // Actor takes this gate outside every metadata/store lock.
    pause: Option<(Arc<tokio::sync::Notify>, mpsc::Receiver<()>)>,
    fail_next_write: bool,
    admission_pending: Option<Arc<tokio::sync::Notify>>,
    read_pause: Option<(Arc<tokio::sync::Notify>, mpsc::Receiver<()>)>,
}

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
    first_seen: BTreeMap<u64, Instant>,
}

enum Command {
    PutComplete {
        piece: u64,
        header: [u8; 8],
        wire: PendingWire,
        max_bytes: u64,
        chunks: u16,
        retention: Option<Duration>,
        reply: tokio::sync::oneshot::Sender<std::io::Result<CompletePut>>,
    },
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
    ReciprocalGet {
        piece: u64,
        chunk: u16,
        permit: tokio::sync::OwnedSemaphorePermit,
        reply: tokio::sync::oneshot::Sender<Option<super::ReciprocalChunk>>,
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
    publication: Arc<tokio::sync::Semaphore>,
    reciprocal: Arc<tokio::sync::Semaphore>,
    pending: Arc<AtomicUsize>,
    #[cfg(test)]
    hooks: Arc<Mutex<PublicationHooks>>,
}

#[derive(Clone)]
pub(crate) struct DiskHandle {
    #[cfg(test)]
    hooks: Arc<Mutex<PublicationHooks>>,
    tx: mpsc::Sender<Command>,
    publication: Arc<tokio::sync::Semaphore>,
    reciprocal: Arc<tokio::sync::Semaphore>,
    pending: Arc<AtomicUsize>,
}

impl DiskBackend {
    #[cfg(test)]
    pub(super) fn pause_publication(
        &self,
        reached: Arc<tokio::sync::Notify>,
        gate: mpsc::Receiver<()>,
    ) {
        self.hooks.lock().unwrap().pause = Some((reached, gate));
    }

    #[cfg(test)]
    pub(super) fn pause_read(&self, reached: Arc<tokio::sync::Notify>, gate: mpsc::Receiver<()>) {
        self.hooks.lock().unwrap().read_pause = Some((reached, gate));
    }

    #[cfg(test)]
    pub(super) fn publication_budget(&self) -> (usize, usize) {
        (
            self.pending.load(Ordering::Acquire),
            self.publication.available_permits(),
        )
    }

    pub(super) fn handle(&self) -> DiskHandle {
        #[cfg(test)]
        let _ = &self.hooks; // Kept on the backend for colocated test configuration.
        DiskHandle {
            #[cfg(test)]
            hooks: self.hooks.clone(),
            tx: self.tx.clone(),
            publication: Arc::clone(&self.publication),
            reciprocal: Arc::clone(&self.reciprocal),
            pending: Arc::clone(&self.pending),
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
        #[cfg(test)]
        let hooks = Arc::new(Mutex::new(PublicationHooks::default()));
        #[cfg(test)]
        let actor_hooks = Arc::clone(&hooks);
        std::thread::Builder::new()
            .name("outpace-disk-cache".into())
            .spawn(move || {
                let actor = Actor::new(dir, chunk_length, actor_state);
                #[cfg(test)]
                let actor = Actor {
                    hooks: actor_hooks,
                    ..actor
                };
                actor.run(rx);
            })?;
        Ok(Self {
            tx,
            state,
            publication: Arc::new(tokio::sync::Semaphore::new(1)),
            reciprocal: Arc::new(tokio::sync::Semaphore::new(1)),
            pending: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            hooks,
        })
    }

    #[cfg(test)]
    pub(super) fn admission_witness(&self, notify: Arc<tokio::sync::Notify>) {
        self.hooks.lock().unwrap().admission_pending = Some(notify);
    }
    #[cfg(test)]
    pub(super) fn reciprocal_available(&self) -> usize {
        self.reciprocal.available_permits()
    }

    pub(super) fn retained_bytes(&self) -> u64 {
        self.state.lock().unwrap().cur_bytes
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
    #[cfg(test)]
    pub(super) async fn put_complete(
        &self,
        piece: u64,
        header: [u8; 8],
        data: &[u8],
        max_bytes: u64,
        chunks: u16,
        retention: Option<Duration>,
    ) -> std::io::Result<CompletePut> {
        let permit = self.reserve_complete().await?;
        self.submit_complete(permit, piece, header, data, max_bytes, chunks, retention)?
            .await
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::BrokenPipe))?
    }

    pub(super) async fn reserve_complete(
        &self,
    ) -> std::io::Result<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&self.publication)
            .acquire_owned()
            .await
            .map_err(|_| std::io::ErrorKind::BrokenPipe.into())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn submit_complete(
        &self,
        permit: tokio::sync::OwnedSemaphorePermit,
        piece: u64,
        header: [u8; 8],
        data: &[u8],
        max_bytes: u64,
        chunks: u16,
        retention: Option<Duration>,
    ) -> std::io::Result<tokio::sync::oneshot::Receiver<std::io::Result<CompletePut>>> {
        let data = data.to_vec();
        self.pending.store(data.len(), Ordering::Release);
        let wire = PendingWire {
            data,
            pending: Arc::clone(&self.pending),
            _permit: permit,
        };
        let (reply, done) = tokio::sync::oneshot::channel();
        self.tx
            .send(Command::PutComplete {
                piece,
                header,
                wire,
                max_bytes,
                chunks,
                retention,
                reply,
            })
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::BrokenPipe))?;
        Ok(done)
    }

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
    pub(super) async fn reciprocal_get(
        &self,
        piece: u64,
        chunk: u16,
    ) -> Option<super::ReciprocalChunk> {
        #[cfg(not(test))]
        let permit = self.reciprocal.clone().acquire_owned().await.ok()?;
        #[cfg(test)]
        let permit = {
            use std::future::Future;
            let mut acquire = std::pin::pin!(self.reciprocal.clone().acquire_owned());
            std::future::poll_fn(|cx| {
                let result = acquire.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(notify) = self.hooks.lock().unwrap().admission_pending.take() {
                        notify.notify_one();
                    }
                }
                result
            })
            .await
            .ok()?
        };
        let (reply, done) = tokio::sync::oneshot::channel();
        self.tx
            .send(Command::ReciprocalGet {
                piece,
                chunk,
                permit,
                reply,
            })
            .ok()?;
        done.await.ok().flatten()
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
    #[cfg(test)]
    hooks: Arc<Mutex<PublicationHooks>>,
}

impl Actor {
    fn pause_read(&self) {
        #[cfg(test)]
        {
            let pause = self.hooks.lock().unwrap().read_pause.take();
            if let Some((reached, release)) = pause {
                reached.notify_one();
                let _ = release.recv();
            }
        }
    }

    fn new(dir: PathBuf, chunk_length: u64, state: Arc<Mutex<State>>) -> Self {
        Self {
            dir,
            chunk_length,
            state,
            files: BTreeMap::new(),
            lru: VecDeque::new(),
            #[cfg(test)]
            hooks: Arc::new(Mutex::new(PublicationHooks::default())),
        }
    }

    fn run(mut self, rx: mpsc::Receiver<Command>) {
        while let Ok(command) = rx.recv() {
            match command {
                Command::PutComplete {
                    piece,
                    header,
                    wire,
                    max_bytes,
                    chunks,
                    retention,
                    reply,
                } => {
                    let result =
                        self.put_complete(piece, header, &wire.data, max_bytes, chunks, retention);
                    drop(wire);
                    let _ = reply.send(result);
                }
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
                    self.pause_read();
                    let _ = reply.send(self.get(piece, chunk));
                }
                Command::ReciprocalGet {
                    piece,
                    chunk,
                    permit,
                    reply,
                } => {
                    self.pause_read();
                    let result =
                        self.get(piece, chunk)
                            .map(|(data, header)| super::ReciprocalChunk {
                                data,
                                header,
                                _permit: Some(permit),
                            });
                    let _ = reply.send(result);
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
                state.first_seen.remove(&lowest);
                evicted.push(lowest);
            }
            (stored, evicted)
        };
        for piece in evicted {
            self.remove(piece);
        }
        stored
    }

    fn put_complete(
        &mut self,
        piece: u64,
        header: [u8; 8],
        wire: &[u8],
        max: u64,
        chunks: u16,
        retention: Option<Duration>,
    ) -> std::io::Result<CompletePut> {
        if self
            .state
            .lock()
            .unwrap()
            .index
            .get(&piece)
            .is_some_and(|entry| entry.present.len() == usize::from(chunks))
        {
            return Ok(CompletePut::AlreadyPresent);
        }
        #[cfg(test)]
        let fail = {
            let (pause, fail) = {
                let mut hooks = self.hooks.lock().unwrap();
                (
                    hooks.pause.take(),
                    std::mem::take(&mut hooks.fail_next_write),
                )
            };
            if let Some((reached, release)) = pause {
                reached.notify_one();
                let _ = release.recv();
            }
            fail
        };
        let temp = self.dir.join(format!("{piece}.pending"));
        let result = (|| {
            let file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temp)?;
            #[cfg(test)]
            if fail {
                return Err(std::io::Error::other("injected whole-piece write failure"));
            }
            write_all_at(&file, wire, 0)?;
            drop(file);
            std::fs::rename(&temp, self.path(piece))?;
            Ok::<(), std::io::Error>(())
        })();
        if let Err(error) = result {
            let _ = std::fs::remove_file(&temp);
            return Err(error);
        }
        // Drop any descriptor for the replaced trusted partial generation.
        self.files.remove(&piece);
        self.lru.retain(|&p| p != piece);
        let (evicted, retained) = {
            let mut state = self.state.lock().unwrap();
            let present = (0..chunks)
                .map(|chunk| (chunk, self.chunk_length as u32))
                .collect();
            let removed = state
                .index
                .insert(piece, PieceIndex { header, present })
                .map_or(0, |old| old.present.values().map(|&n| u64::from(n)).sum());
            state.cur_bytes = state.cur_bytes - removed + wire.len() as u64;
            let now = Instant::now();
            state.first_seen.entry(piece).or_insert(now);
            let mut evicted = Vec::new();
            while let Some((&lowest, _)) = state.index.first_key_value() {
                let expired = retention.is_some_and(|age| {
                    state
                        .first_seen
                        .get(&lowest)
                        .is_some_and(|seen| now.saturating_duration_since(*seen) >= age)
                });
                if state.cur_bytes <= max && !expired {
                    break;
                }
                let old = state.index.remove(&lowest).unwrap();
                state.cur_bytes -= old.present.values().map(|&n| u64::from(n)).sum::<u64>();
                state.first_seen.remove(&lowest);
                evicted.push(lowest);
            }
            (evicted, state.index.contains_key(&piece))
        };
        for old in evicted {
            self.remove(old);
        }
        Ok(if retained {
            CompletePut::Published
        } else {
            CompletePut::NotRetained
        })
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
            state.first_seen.remove(&piece);
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

    #[tokio::test]
    async fn reciprocal_admission_survives_execution_and_buffered_cancellation() {
        use std::future::Future;
        use std::task::{Context, Waker};
        let dir = tempfile::tempdir().unwrap();
        let backend = DiskBackend::new(dir.path().join("cache"), 4).unwrap();
        let first = backend.handle();
        let next_generation = backend.handle();
        first
            .put_complete(1, [0; 8], b"abcdefgh", 64, 16, None)
            .await
            .unwrap();
        let reached = Arc::new(tokio::sync::Notify::new());
        let (release, gate) = mpsc::channel();
        backend.hooks.lock().unwrap().read_pause = Some((reached.clone(), gate));
        let mut read = Box::pin(first.reciprocal_get(1, 1));
        let pending = read
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending();
        let reached_read = tokio::time::timeout(Duration::from_secs(5), reached.notified())
            .await
            .is_ok();
        let mut waiting = Box::pin(next_generation.reciprocal_get(1, 0));
        let waits = waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending();
        drop(waiting); // Before-admission cancellation.
        drop(read); // Executing command still owns admission.
        let held = backend.reciprocal.available_permits();
        let publication_free = backend.publication.available_permits();
        drop(release);
        let drain = first.get(1, 0).await;
        let restored = backend.reciprocal.available_permits();
        // Hold the worker until admission has explicitly returned Pending.
        let buffered_reached = Arc::new(tokio::sync::Notify::new());
        let (buffered_release, buffered_gate) = mpsc::channel();
        backend.hooks.lock().unwrap().read_pause = Some((buffered_reached.clone(), buffered_gate));
        let mut buffered = Box::pin(first.reciprocal_get(1, 1));
        let buffered_pending = buffered
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending();
        let buffered_executing =
            tokio::time::timeout(Duration::from_secs(5), buffered_reached.notified())
                .await
                .is_ok();
        // Release before any assertion can unwind.
        drop(buffered_release);
        // FIFO drain proves the oneshot is ready; leave that result unconsumed.
        let drain2 = first.get(1, 0).await;
        let buffered_held = backend.reciprocal.available_permits();
        let mut successor = Box::pin(next_generation.reciprocal_get(1, 0));
        let successor_waits = successor
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending();
        // Publication admission is independent even with a buffered read result.
        let publication = first.reserve_complete().await;
        drop(publication);
        drop(buffered);
        let reply = successor.await;
        let bytes = reply.as_ref().map(|r| (r.data.clone(), r.header));
        let result_held = backend.reciprocal.available_permits();
        drop(reply);
        assert!(buffered_pending && buffered_executing);
        assert!(pending && reached_read && waits && successor_waits);
        assert_eq!(
            (held, publication_free, restored, buffered_held, result_held),
            (0, 1, 1, 0, 0)
        );
        assert_eq!(drain, Some((b"abcd".to_vec(), [0; 8])));
        assert_eq!(drain2, drain);
        assert_eq!(bytes, drain);
        assert_eq!(backend.reciprocal.available_permits(), 1);
    }

    #[tokio::test]
    async fn contract_whole_cancel_admission_keeps_one_owned_buffer_and_atomic_budget() {
        use std::future::Future;
        use std::task::{Context, Waker};
        let dir = tempfile::tempdir().unwrap();
        let backend = DiskBackend::new(dir.path().join("cache"), 4).unwrap();
        let handle = backend.handle();
        handle
            .put_complete(1, [1; 8], b"11112222", 8, 2, None)
            .await
            .unwrap();
        let reached = Arc::new(tokio::sync::Notify::new());
        let (release, gate) = mpsc::channel();
        backend.hooks.lock().unwrap().pause = Some((reached.clone(), gate));
        let mut admitted = Box::pin(handle.put_complete(2, [2; 8], b"abcdefgh", 8, 2, None));
        assert!(admitted
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        tokio::time::timeout(Duration::from_secs(5), reached.notified())
            .await
            .unwrap();
        // Capture observations before releasing the actor, then assert after drain.
        let before = {
            let state = backend.state.lock().unwrap();
            (
                state.index.contains_key(&2),
                state.cur_bytes,
                state.index.len(),
            )
        };
        let pending_before = backend.pending.load(Ordering::Acquire);
        let mut waiting = Box::pin(handle.put_complete(3, [3; 8], b"XXXXXXXX", 8, 2, None));
        let waiting_pending = waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending();
        drop(waiting); // Cancellation before admission must not copy or enqueue.
        drop(admitted); // Admitted command, rather than caller, owns capacity.
        let held = (
            backend.pending.load(Ordering::Acquire),
            backend.publication.available_permits(),
        );
        drop(release); // Disconnection unconditionally releases actor even on unwind.
        let committed = tokio::time::timeout(Duration::from_secs(5), handle.get(2, 1))
            .await
            .unwrap();
        let absent = tokio::time::timeout(Duration::from_secs(5), handle.get(3, 0))
            .await
            .unwrap();
        assert_eq!(before, (false, 8, 1));
        assert_eq!(pending_before, 8);
        assert!(waiting_pending);
        assert_eq!(held, (8, 0));
        assert_eq!(committed, Some((b"efgh".to_vec(), [2; 8])));
        assert!(absent.is_none());
        assert_eq!(backend.pending.load(Ordering::Acquire), 0);
        assert_eq!(backend.publication.available_permits(), 1);
        let state = backend.state.lock().unwrap();
        assert_eq!(state.cur_bytes, 8);
        assert_eq!(state.index.keys().copied().collect::<Vec<_>>(), vec![2]);
    }

    #[tokio::test]
    async fn contract_whole_io_failure_cleans_temp_and_allows_valid_retry() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let backend = DiskBackend::new(cache.clone(), 4).unwrap();
        let handle = backend.handle();
        backend.hooks.lock().unwrap().fail_next_write = true;
        let failed = handle
            .put_complete(7, [7; 8], b"abcdefgh", 8, 2, None)
            .await;
        assert!(failed.is_err());
        assert!(handle.get(7, 0).await.is_none());
        assert_eq!(backend.retained_bytes(), 0);
        assert!(backend.state.lock().unwrap().index.is_empty());
        assert_eq!(std::fs::read_dir(&cache).unwrap().count(), 0);
        assert_eq!(backend.pending.load(Ordering::Acquire), 0);
        assert_eq!(backend.publication.available_permits(), 1);
        assert_eq!(
            handle
                .put_complete(7, [8; 8], b"ijklmnop", 8, 2, None)
                .await
                .unwrap(),
            CompletePut::Published
        );
        assert_eq!(handle.get(7, 1).await, Some((b"mnop".to_vec(), [8; 8])));
        assert_eq!(
            handle
                .put_complete(7, [9; 8], b"XXXXXXXX", 8, 2, None)
                .await
                .unwrap(),
            CompletePut::AlreadyPresent
        );
        assert_eq!(handle.get(7, 0).await, Some((b"ijkl".to_vec(), [8; 8])));
        assert_eq!(backend.retained_bytes(), 8);
    }

    #[tokio::test]
    async fn cancelled_disk_put_still_commits_index_and_budget_atomically() {
        use crate::store::{Backend, PieceStore};
        use std::future::Future;
        use std::task::{Context, Waker};

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(tokio::sync::Mutex::new(
            PieceStore::new_disk(4, 4, 4, dir.path().join("cache")).unwrap(),
        ));
        PieceStore::shared_put_chunk_with_header(&store, 1, 0, [1; 8], b"1111").await;
        let actor_state = {
            let guard = store.lock().await;
            let Backend::Disk(disk) = &guard.backend else {
                panic!("expected disk backend");
            };
            Arc::clone(&disk.state)
        };

        // Hold the actor's commit lock while polling the caller once. This proves
        // the command is queued and its reply is pending before cancellation.
        {
            let pause_commit = actor_state.lock().unwrap();
            let mut write = Box::pin(PieceStore::shared_put_chunk_with_header(
                &store, 2, 0, [2; 8], b"2222",
            ));
            let mut context = Context::from_waker(Waker::noop());
            assert!(write.as_mut().poll(&mut context).is_pending());
            drop(write);
            drop(pause_commit);
        }

        // The read is queued behind the cancelled write, so its reply is a
        // completion barrier rather than a guess about thread scheduling.
        let chunk = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            PieceStore::shared_chunk(&store, 2, 0),
        )
        .await
        .expect("disk actor completes queued commands");
        assert_eq!(chunk, Some((b"2222".to_vec(), [2; 8])));
        let guard = store.lock().await;
        assert!(
            guard.has_piece(2),
            "actor commits after caller cancellation"
        );
        assert!(
            !guard.has_piece(1),
            "the same transaction enforces the budget"
        );
    }

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
