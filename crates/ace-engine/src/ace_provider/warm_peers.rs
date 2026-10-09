//! Bounded, private hints from authenticated contiguous producers. Never descriptor authority.
use super::CandidateKind;
use std::collections::{BTreeMap, HashMap};
use std::io::{self, Read, Write};
use std::net::SocketAddrV4;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const TTL: u64 = 300;
const MAX_BYTES: usize = 256 * 1024;
const MAX_IDENTITIES: usize = 16;
const HEADER: &str = "OUTPACE-WARM-1\n";
const FILE: &str = "recent-peers-v1";
#[derive(Clone, Copy)]
struct Entry {
    addr: SocketAddrV4,
    source: bool,
    at: u64,
}
#[derive(Default)]
struct State {
    entries: BTreeMap<[u8; 20], Vec<Entry>>,
    revision: u64,
    persisted: u64,
    loaded: bool,
    flush: bool,
    stop: bool,
}
struct Shared {
    state: Mutex<State>,
    wake: Condvar,
    changed: tokio::sync::Notify,
    allow_loopback: bool,
}
struct Owner {
    shared: Arc<Shared>,
    identity: Option<PathBuf>,
    worker: Mutex<Option<JoinHandle<()>>>,
}
struct Slot {
    owner: Weak<Owner>,
    retired: Option<JoinHandle<()>>,
}
fn registry() -> &'static Mutex<HashMap<PathBuf, Slot>> {
    static REGISTRY: OnceLock<Mutex<HashMap<PathBuf, Slot>>> = OnceLock::new();
    REGISTRY.get_or_init(Mutex::default)
}
impl Drop for Owner {
    fn drop(&mut self) {
        {
            let mut state = self.shared.state.lock().unwrap();
            state.flush = true;
            state.stop = true;
        }
        self.shared.wake.notify_one();
        if let Some(identity) = &self.identity {
            let handle = self.worker.lock().unwrap().take();
            if let Some(slot) = registry().lock().unwrap().get_mut(identity) {
                slot.retired = handle;
            }
        }
    }
}
#[derive(Clone)]
pub(super) struct WarmPeerCache(Arc<Owner>);
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn normalize(path: &Path) -> Option<PathBuf> {
    let path = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir().ok()?.join(path)
    };
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir => result.push("/"),
            Component::Normal(p) => result.push(p),
            Component::CurDir => {}
            Component::ParentDir => {
                if !result.pop() {
                    return None;
                }
            }
            _ => return None,
        }
    }
    Some(result)
}
fn endpoint_allowed(addr: SocketAddrV4, allow_loopback: bool) -> bool {
    let ip = addr.ip();
    let octets = ip.octets();
    addr.port() != 0
        && ((allow_loopback && ip.is_loopback())
            || !(ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_broadcast()
                || ip.is_documentation()
                || ip.is_unspecified()
                || ip.is_multicast()
                || octets[0] == 0
                || octets[0] >= 240
                || (octets[0] == 100 && (64..=127).contains(&octets[1]))
                || (octets[0] == 198 && (18..=19).contains(&octets[1]))))
}
fn sweep(slots: &mut HashMap<PathBuf, Slot>) {
    // Join only known-finished handles: a filesystem stall never blocks async teardown.
    slots.retain(|_, slot| {
        if slot.owner.strong_count() > 0 {
            return true;
        }
        match slot.retired.as_ref() {
            None => true,
            Some(handle) if !handle.is_finished() => true,
            _ => {
                if let Some(handle) = slot.retired.take() {
                    let _ = handle.join();
                }
                false
            }
        }
    });
}
impl WarmPeerCache {
    pub(super) fn memory() -> Self {
        Self::new(Path::new(""), false)
    }
    pub(super) fn new(path: &Path, allow_loopback: bool) -> Self {
        Self::new_inner(
            path,
            allow_loopback,
            #[cfg(test)]
            None,
        )
    }
    fn new_inner(
        path: &Path,
        allow_loopback: bool,
        #[cfg(test)] pause: Option<Arc<(Mutex<bool>, Condvar)>>,
    ) -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            wake: Condvar::new(),
            changed: tokio::sync::Notify::new(),
            allow_loopback,
        });
        let memory = || {
            Self(Arc::new(Owner {
                shared: shared.clone(),
                identity: None,
                worker: Mutex::new(None),
            }))
        };
        if path.as_os_str().is_empty() {
            shared.state.lock().unwrap().loaded = true;
            return memory();
        }
        let Some(identity) = normalize(path) else {
            shared.state.lock().unwrap().loaded = true;
            return memory();
        };
        let mut slots = registry().lock().unwrap();
        sweep(&mut slots);
        if let Some(slot) = slots.get(&identity) {
            if let Some(owner) = slot.owner.upgrade() {
                return Self(owner);
            }
            // An unfinished retired writer owns this path until its actual terminal.
            shared.state.lock().unwrap().loaded = true;
            return memory();
        }
        if slots.len() >= MAX_IDENTITIES {
            shared.state.lock().unwrap().loaded = true;
            return memory();
        }
        let owner = Arc::new(Owner {
            shared: shared.clone(),
            identity: Some(identity.clone()),
            worker: Mutex::new(None),
        });
        let worker_path = identity.clone();
        match std::thread::Builder::new()
            .name("warm-peer-cache".into())
            .spawn(move || {
                #[cfg(test)]
                if let Some(pause) = pause {
                    let mut released = pause.0.lock().unwrap();
                    while !*released {
                        released = pause.1.wait(released).unwrap();
                    }
                }
                worker(shared, worker_path)
            }) {
            Ok(handle) => {
                *owner.worker.lock().unwrap() = Some(handle);
                slots.insert(
                    identity,
                    Slot {
                        owner: Arc::downgrade(&owner),
                        retired: None,
                    },
                );
                Self(owner)
            }
            Err(_) => {
                owner.shared.state.lock().unwrap().loaded = true;
                Self(owner)
            }
        }
    }
    pub(super) async fn initialized(&self) {
        let _ = tokio::time::timeout(Duration::from_millis(100), async {
            loop {
                let notified = self.0.shared.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.0.shared.state.lock().unwrap().loaded {
                    break;
                }
                notified.await;
            }
        })
        .await;
    }
    pub(super) fn hints(&self, hash: &[u8; 20]) -> Vec<(SocketAddrV4, CandidateKind)> {
        let mut state = self.0.shared.state.lock().unwrap();
        prune(&mut state.entries, now());
        let mut entries = state.entries.get(hash).cloned().unwrap_or_default();
        entries.sort_by_key(|entry| (!entry.source, std::cmp::Reverse(entry.at)));
        entries
            .into_iter()
            .map(|entry| {
                (
                    entry.addr,
                    if entry.source {
                        CandidateKind::Source
                    } else {
                        CandidateKind::Discovered
                    },
                )
            })
            .collect()
    }
    pub(super) fn record_productive(
        &self,
        hash: [u8; 20],
        addr: SocketAddrV4,
        kind: CandidateKind,
    ) {
        if !endpoint_allowed(addr, self.0.shared.allow_loopback) {
            return;
        }
        let mut state = self.0.shared.state.lock().unwrap();
        insert(
            &mut state.entries,
            hash,
            Entry {
                addr,
                source: kind == CandidateKind::Source,
                at: now(),
            },
        );
        state.revision = state.revision.wrapping_add(1);
        drop(state);
        self.0.shared.wake.notify_one();
    }
    pub(super) fn request_flush(&self) {
        self.0.shared.state.lock().unwrap().flush = true;
        self.0.shared.wake.notify_one();
    }
    #[cfg(test)]
    pub(super) async fn flush(&self) -> bool {
        let revision = {
            let mut state = self.0.shared.state.lock().unwrap();
            state.flush = true;
            state.revision
        };
        self.0.shared.wake.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let notified = self.0.shared.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.0.shared.state.lock().unwrap().persisted >= revision {
                    break;
                }
                notified.await;
            }
        })
        .await
        .is_ok()
    }
}
fn prune(entries: &mut BTreeMap<[u8; 20], Vec<Entry>>, time: u64) {
    entries.retain(|_, peers| {
        peers.retain(|peer| peer.at <= time && time - peer.at <= TTL);
        !peers.is_empty()
    });
}
fn insert(entries: &mut BTreeMap<[u8; 20], Vec<Entry>>, hash: [u8; 20], mut entry: Entry) {
    prune(entries, now());
    let previous = entries
        .get(&hash)
        .and_then(|peers| peers.iter().find(|old| old.addr == entry.addr))
        .copied();
    if let Some(old) = previous {
        if old.at > entry.at {
            return;
        }
        entry.source |= old.source;
    }
    if !entries.contains_key(&hash) && entries.len() >= 256 {
        if let Some(old) = entries
            .iter()
            .min_by_key(|(_, peers)| peers.iter().map(|p| p.at).max().unwrap_or(0))
            .map(|(hash, _)| *hash)
        {
            entries.remove(&old);
        }
    }
    if previous.is_none() && entries.values().map(Vec::len).sum::<usize>() >= 3000 {
        if let Some((old_hash, index)) = entries
            .iter()
            .flat_map(|(hash, peers)| {
                peers
                    .iter()
                    .enumerate()
                    .map(move |(index, entry)| (*hash, index, entry.at))
            })
            .min_by_key(|(_, _, at)| *at)
            .map(|(hash, index, _)| (hash, index))
        {
            entries.get_mut(&old_hash).unwrap().remove(index);
            if entries[&old_hash].is_empty() {
                entries.remove(&old_hash);
            }
        }
    }
    let peers = entries.entry(hash).or_default();
    if let Some(index) = peers.iter().position(|old| old.addr == entry.addr) {
        // Refreshes do not consume another row. Apply source upgrades through the same cap.
        peers.remove(index);
    }
    if entry.source && peers.iter().filter(|peer| peer.source).count() >= 8 {
        if let Some(index) = peers
            .iter()
            .enumerate()
            .filter(|(_, peer)| peer.source)
            .min_by_key(|(_, peer)| peer.at)
            .map(|(i, _)| i)
        {
            peers.remove(index);
        }
    }
    if peers.len() >= 16 {
        let index = peers
            .iter()
            .enumerate()
            .min_by_key(|(_, peer)| peer.at)
            .unwrap()
            .0;
        peers.remove(index);
    }
    peers.push(entry);
}
fn encode(entries: &BTreeMap<[u8; 20], Vec<Entry>>) -> Vec<u8> {
    let mut bytes = HEADER.as_bytes().to_vec();
    for (hash, peers) in entries {
        for peer in peers {
            writeln!(
                &mut bytes,
                "{} {} {} {} {}",
                hex::encode(hash),
                peer.at,
                if peer.source { "S" } else { "D" },
                peer.addr.ip(),
                peer.addr.port()
            )
            .unwrap();
        }
    }
    bytes
}
fn parse(bytes: &[u8], allow_loopback: bool) -> io::Result<BTreeMap<[u8; 20], Vec<Entry>>> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid warm cache");
    if bytes.len() > MAX_BYTES || !bytes.starts_with(HEADER.as_bytes()) {
        return Err(invalid());
    }
    let mut entries = BTreeMap::<[u8; 20], Vec<Entry>>::new();
    let mut rows = 0;
    let time = now();
    for line in bytes[HEADER.len()..].split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        if line.len() > 128 || rows >= 3000 {
            return Err(invalid());
        }
        rows += 1;
        let row = std::str::from_utf8(line).map_err(|_| invalid())?;
        let mut fields = row.split(' ');
        let mut hash = [0; 20];
        let encoded = fields.next().ok_or_else(invalid)?;
        if encoded.len() != 40 || hex::decode_to_slice(encoded, &mut hash).is_err() {
            return Err(invalid());
        }
        let at = fields
            .next()
            .ok_or_else(invalid)?
            .parse::<u64>()
            .map_err(|_| invalid())?;
        let source = match fields.next() {
            Some("S") => true,
            Some("D") => false,
            _ => return Err(invalid()),
        };
        let ip = fields
            .next()
            .ok_or_else(invalid)?
            .parse()
            .map_err(|_| invalid())?;
        let port = fields
            .next()
            .ok_or_else(invalid)?
            .parse()
            .map_err(|_| invalid())?;
        let addr = SocketAddrV4::new(ip, port);
        if fields.next().is_some()
            || at > time
            || time - at > TTL
            || !endpoint_allowed(addr, allow_loopback)
        {
            return Err(invalid());
        }
        if !entries.contains_key(&hash) && entries.len() >= 256 {
            return Err(invalid());
        }
        let peers = entries.entry(hash).or_default();
        if peers.len() >= 16
            || peers.iter().any(|entry| entry.addr == addr)
            || (source && peers.iter().filter(|entry| entry.source).count() >= 8)
        {
            return Err(invalid());
        }
        peers.push(Entry { addr, source, at });
    }
    Ok(entries)
}
fn worker(shared: Arc<Shared>, path: PathBuf) {
    let directory = secure::Directory::open(&path);
    let loaded = directory
        .as_ref()
        .ok()
        .and_then(|directory| directory.read().ok())
        .and_then(|bytes| parse(&bytes, shared.allow_loopback).ok());
    {
        let mut state = shared.state.lock().unwrap();
        if let Some(entries) = loaded {
            for (hash, peers) in entries {
                for entry in peers {
                    insert(&mut state.entries, hash, entry);
                }
            }
        }
        state.loaded = true;
    }
    shared.changed.notify_waiters();
    let mut last_write = Instant::now()
        .checked_sub(Duration::from_secs(5))
        .unwrap_or_else(Instant::now);
    loop {
        let (snapshot, revision, stop) = {
            let mut state = shared.state.lock().unwrap();
            while !state.stop
                && (state.revision == state.persisted
                    || (!state.flush && last_write.elapsed() < Duration::from_secs(5)))
            {
                let delay = Duration::from_secs(5)
                    .saturating_sub(last_write.elapsed())
                    .max(Duration::from_millis(10));
                state = shared.wake.wait_timeout(state, delay).unwrap().0;
            }
            prune(&mut state.entries, now());
            state.flush = false;
            (encode(&state.entries), state.revision, state.stop)
        };
        let success = directory
            .as_ref()
            .is_ok_and(|directory| directory.write(&snapshot).is_ok());
        last_write = Instant::now();
        if success {
            shared.state.lock().unwrap().persisted = revision;
            shared.changed.notify_waiters();
        } else {
            crate::alog!(
                "[ace] warm peer cache I/O failed; streaming continues without durable hints"
            );
        }
        if stop {
            break;
        }
        if !success {
            // Failed writes retry only after coalescing interval, never a hot spin.
            let state = shared.state.lock().unwrap();
            if !state.stop {
                let _ = shared.wake.wait_timeout(state, Duration::from_secs(5));
            }
        }
    }
}

#[cfg(unix)]
mod secure {
    use super::*;
    use std::ffi::CString;
    use std::fs::File;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;
    pub(super) struct Directory {
        dir: File,
        _lock: File,
    }
    fn openat(dir: i32, name: &std::ffi::OsStr, flags: i32, mode: u32) -> io::Result<File> {
        let name = CString::new(name.as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL path"))?;
        // SAFETY: the CString outlives the call; successful fd ownership transfers once.
        let fd = unsafe {
            libc::openat(
                dir,
                name.as_ptr(),
                flags | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
                mode,
            )
        };
        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(unsafe { File::from_raw_fd(fd) })
        }
    }
    fn validate(file: &File) -> io::Result<()> {
        let metadata = file.metadata()?;
        if !metadata.is_file()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.nlink() != 1
            || metadata.mode() & 0o7777 != 0o600
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "unsafe cache inode",
            ));
        }
        Ok(())
    }
    impl Directory {
        pub(super) fn open(path: &Path) -> io::Result<Self> {
            let mut dir = openat(
                libc::AT_FDCWD,
                Path::new("/").as_os_str(),
                libc::O_RDONLY | libc::O_DIRECTORY,
                0,
            )?;
            for component in path.components() {
                if let Component::Normal(name) = component {
                    dir = match openat(dir.as_raw_fd(), name, libc::O_RDONLY | libc::O_DIRECTORY, 0)
                    {
                        Ok(next) => next,
                        Err(error) if error.kind() == io::ErrorKind::NotFound => {
                            let name_c = CString::new(name.as_bytes()).map_err(|_| {
                                io::Error::new(io::ErrorKind::InvalidInput, "NUL path")
                            })?;
                            if unsafe { libc::mkdirat(dir.as_raw_fd(), name_c.as_ptr(), 0o700) } < 0
                                && io::Error::last_os_error().kind() != io::ErrorKind::AlreadyExists
                            {
                                return Err(io::Error::last_os_error());
                            }
                            openat(dir.as_raw_fd(), name, libc::O_RDONLY | libc::O_DIRECTORY, 0)?
                        }
                        Err(error) => return Err(error),
                    };
                }
            }
            let metadata = dir.metadata()?;
            if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o022 != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "unsafe cache directory",
                ));
            }
            let lock = openat(
                dir.as_raw_fd(),
                std::ffi::OsStr::new("recent-peers-v1.lock"),
                libc::O_RDWR | libc::O_CREAT,
                0o600,
            )?;
            validate(&lock)?;
            // This inode lock also prevents directory aliases from creating concurrent writers.
            if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(Self { dir, _lock: lock })
        }
        pub(super) fn read(&self) -> io::Result<Vec<u8>> {
            let file = openat(
                self.dir.as_raw_fd(),
                std::ffi::OsStr::new(FILE),
                libc::O_RDONLY,
                0,
            )?;
            validate(&file)?;
            if file.metadata()?.len() > MAX_BYTES as u64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "oversized cache",
                ));
            }
            let mut bytes = Vec::new();
            file.take(MAX_BYTES as u64 + 1).read_to_end(&mut bytes)?;
            if bytes.len() > MAX_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "oversized cache",
                ));
            }
            Ok(bytes)
        }
        pub(super) fn write(&self, bytes: &[u8]) -> io::Result<()> {
            if bytes.len() > MAX_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "oversized snapshot",
                ));
            }
            // Reject a poisoned existing target rather than replacing its untrusted inode.
            match openat(
                self.dir.as_raw_fd(),
                std::ffi::OsStr::new(FILE),
                libc::O_RDONLY,
                0,
            ) {
                Ok(file) => validate(&file)?,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let temp = format!(
                ".recent-peers-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            );
            let temp_c = CString::new(temp.as_bytes()).unwrap();
            let target = CString::new(FILE).unwrap();
            let mut file = openat(
                self.dir.as_raw_fd(),
                std::ffi::OsStr::new(&temp),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
                0o600,
            )?;
            let result = (|| {
                validate(&file)?;
                file.write_all(bytes)?;
                file.sync_all()?;
                if unsafe {
                    libc::renameat(
                        self.dir.as_raw_fd(),
                        temp_c.as_ptr(),
                        self.dir.as_raw_fd(),
                        target.as_ptr(),
                    )
                } < 0
                {
                    return Err(io::Error::last_os_error());
                }
                self.dir.sync_all()
            })();
            if result.is_err() {
                unsafe { libc::unlinkat(self.dir.as_raw_fd(), temp_c.as_ptr(), 0) };
            }
            result
        }
    }
}
#[cfg(not(unix))]
mod secure {
    use super::*;
    pub(super) struct Directory;
    impl Directory {
        pub(super) fn open(_: &Path) -> io::Result<Self> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "private cache requires Unix inode validation",
            ))
        }
        pub(super) fn read(&self) -> io::Result<Vec<u8>> {
            unreachable!()
        }
        pub(super) fn write(&self, _: &[u8]) -> io::Result<()> {
            unreachable!()
        }
    }
}

#[cfg(test)]
pub(super) async fn retired_terminal(path: &Path) -> bool {
    let identity = normalize(path).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            {
                let mut slots = registry().lock().unwrap();
                sweep(&mut slots);
                if !slots.contains_key(&identity) {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}

#[cfg(test)]
pub(super) async fn test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    static GUARD: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    GUARD
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::{symlink, PermissionsExt};
    fn temp() -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "outpace-warm-file-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ))
    }
    fn peer(port: u16) -> SocketAddrV4 {
        SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, port)
    }
    fn valid() -> Vec<u8> {
        encode(&BTreeMap::from([(
            [0; 20],
            vec![Entry {
                addr: peer(23456),
                source: true,
                at: now(),
            }],
        )]))
    }
    fn private_file(path: &Path, bytes: &[u8]) {
        std::fs::write(path, bytes).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    #[test]
    fn bounded_parser_rejects_corrupt_expired_future_poison_and_excess_rows() {
        assert_eq!(parse(&valid(), true).unwrap()[&[0; 20]].len(), 1);
        assert!(
            parse(&valid(), false).is_err(),
            "default policy must reject loopback hints"
        );
        let mut malformed = vec![b'x'; MAX_BYTES + 1];
        assert!(parse(&malformed, true).is_err());
        malformed = valid();
        malformed[0] = b'x';
        assert!(parse(&malformed, true).is_err());
        let row =
            |at: u64, port: u16| format!("{} {at} S 127.0.0.1 {port}\n", hex::encode([0; 20]));
        for suffix in [
            row(now() + 1, 23456),
            row(now() - TTL - 1, 23456),
            row(now(), 0),
            "x".repeat(129),
        ] {
            assert!(parse(format!("{HEADER}{suffix}").as_bytes(), true).is_err());
        }
        let duplicate = format!("{HEADER}{}{}", row(now(), 23456), row(now(), 23456));
        assert!(parse(duplicate.as_bytes(), true).is_err());
        let sources = format!(
            "{HEADER}{}",
            (1..=9).map(|port| row(now(), port)).collect::<String>()
        );
        assert!(parse(sources.as_bytes(), true).is_err());
        let peers = format!(
            "{HEADER}{}",
            (1..=17)
                .map(|port| row(now(), port).replace(" S ", " D "))
                .collect::<String>()
        );
        assert!(parse(peers.as_bytes(), true).is_err());
        let mut swarms = BTreeMap::new();
        for hash in 0..257u16 {
            let mut key = [0; 20];
            key[..2].copy_from_slice(&hash.to_be_bytes());
            swarms.insert(
                key,
                vec![Entry {
                    addr: peer(23456),
                    source: false,
                    at: now(),
                }],
            );
        }
        assert!(
            parse(&encode(&swarms), true).is_err(),
            "257 swarms must fail before allocating another row"
        );
        assert!(
            parse(
                valid().as_slice().strip_prefix(HEADER.as_bytes()).unwrap(),
                true
            )
            .is_err(),
            "header/version is required"
        );
    }
    #[test]
    fn cache_inode_and_directory_rejections_are_nonblocking() {
        let root = temp();
        let directory = secure::Directory::open(&root).unwrap();
        let target = root.join(FILE);
        let outside = root.join("outside");
        private_file(&outside, b"private original");
        symlink(&outside, &target).unwrap();
        assert!(directory.read().is_err());
        assert!(directory.write(&valid()).is_err());
        assert_eq!(std::fs::read(&outside).unwrap(), b"private original");
        std::fs::remove_file(&target).unwrap();
        std::fs::hard_link(&outside, &target).unwrap();
        assert!(directory.read().is_err());
        assert!(directory.write(&valid()).is_err());
        std::fs::remove_file(&target).unwrap();
        private_file(&target, &valid());
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(directory.read().is_err());
        assert!(directory.write(&valid()).is_err());
        std::fs::remove_file(&target).unwrap();
        let name = std::ffi::CString::new(target.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        let start = Instant::now();
        assert!(directory.read().is_err());
        assert!(directory.write(&valid()).is_err());
        assert!(
            start.elapsed() < Duration::from_millis(100),
            "FIFO must never block cache access"
        );
        std::fs::remove_file(&target).unwrap();
        private_file(&target, &vec![0; MAX_BYTES + 1]);
        assert!(directory.read().is_err());
        drop(directory);
        let alias = root.with_extension("alias");
        symlink(&root, &alias).unwrap();
        assert!(secure::Directory::open(&alias).is_err());
        std::fs::remove_file(alias).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o777)).unwrap();
        assert!(secure::Directory::open(&root).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn atomic_private_replacement_and_alias_writer_lock() {
        let root = temp();
        let directory = secure::Directory::open(&root).unwrap();
        assert!(
            secure::Directory::open(&root).is_err(),
            "directory inode lock must reject alias writers"
        );
        directory.write(&valid()).unwrap();
        assert_eq!(directory.read().unwrap(), valid());
        assert_eq!(
            std::fs::metadata(root.join(FILE))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::read_dir(&root).unwrap().count(),
            2,
            "only completed cache and owned lock remain"
        );
        drop(directory);
        assert!(secure::Directory::open(&root).is_ok());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn clone_updates_merge_with_initial_load_and_persist_actual_revision() {
        let _guard = test_guard().await;
        let root = temp();
        let directory = secure::Directory::open(&root).unwrap();
        directory.write(&valid()).unwrap();
        drop(directory);
        let cache = WarmPeerCache::new(&root, true);
        let clone = WarmPeerCache::new(&root.join("."), true);
        assert!(Arc::ptr_eq(&cache.0, &clone.0));
        // New progress can arrive before the old file finishes loading.
        clone.record_productive([0; 20], peer(23457), CandidateKind::Source);
        cache.initialized().await;
        assert_eq!(cache.hints(&[0; 20]).len(), 2);
        assert!(cache.flush().await);
        drop(clone);
        drop(cache);
        assert!(retired_terminal(&root).await);
        let fresh = WarmPeerCache::new(&root, true);
        fresh.initialized().await;
        assert_eq!(fresh.hints(&[0; 20]).len(), 2);
        assert!(fresh
            .hints(&[0; 20])
            .iter()
            .all(|(_, kind)| *kind == CandidateKind::Source));
        drop(fresh);
        assert!(retired_terminal(&root).await);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn paused_io_keeps_load_and_drop_bounded_and_retired_workers_owned() {
        let _guard = test_guard().await;
        let pause = Arc::new((Mutex::new(false), Condvar::new()));
        let mut owners = Vec::new();
        let mut roots = Vec::new();
        for _ in 0..MAX_IDENTITIES {
            let path = temp();
            owners.push(WarmPeerCache::new_inner(&path, true, Some(pause.clone())));
            roots.push(path);
        }
        assert!(owners
            .iter()
            .all(|cache| cache.0.worker.lock().unwrap().is_some()));
        let start = Instant::now();
        owners[0].initialized().await;
        assert!(start.elapsed() < Duration::from_millis(250));
        let extra = WarmPeerCache::new(&temp(), true);
        assert!(extra.0.worker.lock().unwrap().is_none());
        let start = Instant::now();
        drop(owners);
        assert!(start.elapsed() < Duration::from_millis(100));
        let same = WarmPeerCache::new(&roots[0], true);
        assert!(
            same.0.worker.lock().unwrap().is_none(),
            "retired stalled writer still owns path"
        );
        assert_eq!(
            registry().lock().unwrap().len(),
            MAX_IDENTITIES,
            "retired handles stay bounded and observable"
        );
        *pause.0.lock().unwrap() = true;
        pause.1.notify_all();
        for root in roots {
            assert!(retired_terminal(&root).await);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
    #[test]
    fn productive_provenance_upgrades_preserve_eight_source_cache_cap() {
        let cache = WarmPeerCache::new(Path::new(""), true);
        for port in 1..=16 {
            cache.record_productive([0; 20], peer(port), CandidateKind::Discovered);
        }
        for port in 1..=16 {
            cache.record_productive([0; 20], peer(port), CandidateKind::Source);
            let hints = cache.hints(&[0; 20]);
            assert!(
                hints
                    .iter()
                    .filter(|(_, kind)| *kind == CandidateKind::Source)
                    .count()
                    <= 8,
                "productive source upgrade bypassed source cap"
            );
            assert!(hints.len() <= 16);
        }
    }

    #[test]
    fn existing_productive_update_at_total_row_cap_does_not_evict_another_record() {
        let mut entries = BTreeMap::new();
        for hash in 0..250u16 {
            let mut key = [0; 20];
            key[..2].copy_from_slice(&hash.to_be_bytes());
            for port in 1..=12 {
                insert(
                    &mut entries,
                    key,
                    Entry {
                        addr: peer(port),
                        source: false,
                        at: now(),
                    },
                );
            }
        }
        assert_eq!(entries.values().map(Vec::len).sum::<usize>(), 3000);
        let before = entries
            .iter()
            .flat_map(|(key, peers)| peers.iter().map(move |entry| (*key, entry.addr)))
            .collect::<std::collections::BTreeSet<_>>();
        insert(
            &mut entries,
            [0; 20],
            Entry {
                addr: peer(12),
                source: true,
                at: now(),
            },
        );
        let after = entries
            .iter()
            .flat_map(|(key, peers)| peers.iter().map(move |entry| (*key, entry.addr)))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            before, after,
            "refreshing an existing row evicted unrelated productive evidence"
        );
    }

    #[test]
    fn cache_loader_rejects_total_row_budget_before_another_entry_allocation() {
        let mut entries = BTreeMap::new();
        for hash in 0..251u16 {
            let mut key = [0; 20];
            key[..2].copy_from_slice(&hash.to_be_bytes());
            entries.insert(
                key,
                (1..=12)
                    .map(|port| Entry {
                        addr: peer(port),
                        source: false,
                        at: now(),
                    })
                    .collect(),
            );
        }
        let bytes = encode(&entries);
        assert!(bytes.len() < MAX_BYTES);
        assert!(
            parse(&bytes, true).is_err(),
            "loader accepted more than 3000 rows within byte/per-swarm limits"
        );
    }

    #[test]
    fn productive_memory_bounds_limit_sources_swarms_and_snapshot_size() {
        let cache = WarmPeerCache::memory();
        for hash in 0..300u16 {
            let mut key = [0; 20];
            key[..2].copy_from_slice(&hash.to_be_bytes());
            for port in 1..=30 {
                cache.record_productive(key, peer(port), CandidateKind::Source);
            }
        }
        // Default endpoint policy rejected loopback, including process-local hints.
        assert!(cache.hints(&[0; 20]).is_empty());
        let cache = WarmPeerCache::new(Path::new(""), true);
        for hash in 0..300u16 {
            let mut key = [0; 20];
            key[..2].copy_from_slice(&hash.to_be_bytes());
            for port in 1..=30 {
                cache.record_productive(
                    key,
                    peer(port),
                    if port <= 10 {
                        CandidateKind::Source
                    } else {
                        CandidateKind::Discovered
                    },
                );
            }
        }
        let state = cache.0.shared.state.lock().unwrap();
        assert!(state.entries.len() <= 256);
        assert!(
            state
                .entries
                .values()
                .all(|peers| peers.len() <= 16
                    && peers.iter().filter(|peer| peer.source).count() <= 8)
        );
        assert!(state.entries.values().map(Vec::len).sum::<usize>() <= 3000);
        assert!(encode(&state.entries).len() <= MAX_BYTES);
        let mut expired = state.entries.clone();
        prune(&mut expired, now() + TTL + 1);
        assert!(
            expired.is_empty(),
            "expired productive hints must be evicted"
        );
    }
}
