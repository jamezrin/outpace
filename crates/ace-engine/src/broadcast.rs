//! Exclusive broadcast producers, durable piece reservations and owned entry lifetime.

use crate::ace_provider::build_piece_store;
use crate::broadcast_ingest::{BroadcastIngest, IngestError};
use crate::broadcast_options::{
    validate_broadcast_options, BroadcastOptions, BroadcastOptionsError, ResolvedBroadcastOptions,
};
use crate::broadcast_persist::{BroadcastPersist, PersistedBroadcast};
use crate::broadcast_pull::{run_source, SourceStatus, SourceStatusWriter, ValidatedSourceConfig};
use crate::config::{CacheType, Config};
use ace_swarm::http_fetch::FetchPolicy;
use ace_swarm::listen::{SeedLease, SeedRegistry};
use ace_swarm::store::PieceStore;
use ace_wire::bencode::Bencode;
use ace_wire::infohash::{infohash_of_descriptor, infohash_of_transport, transport_file_hash};
use ace_wire::live_auth::LiveSourceAuth;
use ace_wire::transport::{decode_transport, encode_transport, TransportDescriptor};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::{watch, Mutex, OwnedMutexGuard};
use tokio::task::{AbortHandle, JoinHandle};

pub const PIECE_LENGTH: u64 = 65_536;
pub const CHUNK_LENGTH: u64 = 16_384;
pub const DEFAULT_BROADCAST_BITRATE: i64 = 8375;
pub const CURSOR_PERSIST_INTERVAL: u64 = 128;
pub const CLEANUP_GRACE: Duration = Duration::from_secs(5);

pub fn valid_broadcast_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProducerKind {
    RawPut,
    Rtmp,
    Pull,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum BroadcastError {
    InvalidName,
    ProducerConflict,
    SourceConflict,
    OptionsConflict,
    Stopping,
    ServiceStopping,
    PersistenceFailed,
    StateInvalid,
    PieceIndexExhausted,
    CleanupTimeout,
}

impl BroadcastError {
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidName => "invalid_name",
            Self::ProducerConflict => "producer_conflict",
            Self::SourceConflict => "source_conflict",
            Self::OptionsConflict => "broadcast_options_conflict",
            Self::Stopping => "broadcast_stopping",
            Self::ServiceStopping => "broadcast_service_stopping",
            Self::PersistenceFailed => "broadcast_persistence_failed",
            Self::StateInvalid => "broadcast_state_invalid",
            Self::PieceIndexExhausted => "piece_index_exhausted",
            Self::CleanupTimeout => "broadcast_cleanup_timeout",
        }
    }
}
// Runtime/CLI may box this error directly after actual reload. Termination uses
// Debug, so every presentation must retain the fixed public code.
impl std::fmt::Debug for BroadcastError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for BroadcastError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for BroadcastError {}

type Completion = watch::Receiver<Option<Result<(), BroadcastError>>>;

async fn completed(mut rx: Completion) -> Result<(), BroadcastError> {
    loop {
        if let Some(result) = *rx.borrow_and_update() {
            return result;
        }
        if rx.changed().await.is_err() {
            return Err(BroadcastError::StateInvalid);
        }
    }
}

pub(crate) async fn cancelled(rx: &mut watch::Receiver<bool>) {
    loop {
        if *rx.borrow_and_update() {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}

#[derive(Clone)]
struct CursorSink {
    persist: BroadcastPersist,
    name: String,
    transport: Vec<u8>,
    key_pkcs1_pem: String,
}

struct PoisonOnFailure {
    poisoned: Arc<AtomicBool>,
    succeeded: bool,
}
impl Drop for PoisonOnFailure {
    fn drop(&mut self) {
        if !self.succeeded {
            self.poisoned.store(true, Ordering::SeqCst);
        }
    }
}

/// Actual consumed next index is independent of the exclusive durable high-water mark.
/// The I/O guard is moved into the blocking syscall job, never owned by its waiting caller.
pub struct BroadcastCursor {
    next: AtomicU64,
    reserved: AtomicU64,
    poisoned: Arc<AtomicBool>,
    closed: AtomicBool,
    service_closed: Arc<AtomicBool>,
    cancel: watch::Receiver<bool>,
    sink: Option<CursorSink>,
    io: Arc<Mutex<()>>,
    pub(crate) publication: Arc<Mutex<()>>,
    io_jobs: StdMutex<Vec<JoinHandle<()>>>,
}

impl BroadcastCursor {
    pub fn start_piece(&self) -> u64 {
        self.next.load(Ordering::SeqCst)
    }
    pub fn reserved_until(&self) -> u64 {
        self.reserved.load(Ordering::SeqCst)
    }

    pub(crate) fn check(&self) -> Result<(), BroadcastError> {
        if self.poisoned.load(Ordering::SeqCst) {
            return Err(BroadcastError::PersistenceFailed);
        }
        if self.closed.load(Ordering::SeqCst)
            || self.service_closed.load(Ordering::SeqCst)
            || *self.cancel.borrow()
        {
            return Err(BroadcastError::Stopping);
        }
        Ok(())
    }

    fn record(&self, reserved_until: u64) -> Option<PersistedBroadcast> {
        self.sink.as_ref().map(|sink| PersistedBroadcast {
            transport: sink.transport.clone(),
            key_pkcs1_pem: sink.key_pkcs1_pem.clone(),
            next_piece: self.start_piece(),
            reserved_until: Some(reserved_until),
        })
    }

    // Called only under publication, so a diagnostic flush cannot race an extension and
    // write an older reservation after it. The blocking guard also fences deletion.
    async fn save(&self, reservation: u64) -> Result<(), BroadcastError> {
        self.check()?;
        let Some(rec) = self.record(reservation) else {
            return Ok(());
        };
        let sink = self.sink.as_ref().unwrap().clone();
        let io = self.io.clone().lock_owned().await;
        self.check()?;
        let poison = self.poisoned.clone();
        let (tx, rx) = watch::channel(None);
        let job = tokio::task::spawn_blocking(move || {
            let _io = io;
            let mut fence = PoisonOnFailure {
                poisoned: poison,
                succeeded: false,
            };
            let result = sink
                .persist
                .save(&sink.name, &rec)
                .map_err(|_| BroadcastError::PersistenceFailed);
            fence.succeeded = result.is_ok();
            tx.send_replace(Some(result));
        });
        {
            let mut jobs = self.io_jobs.lock().unwrap();
            jobs.retain(|job| !job.is_finished());
            jobs.push(job);
        }
        if completed(rx).await.is_err() {
            self.poisoned.store(true, Ordering::SeqCst);
            return Err(BroadcastError::PersistenceFailed);
        }
        // Never let a successful write authorize publication after cancellation.
        self.check()?;
        self.reserved.fetch_max(reservation, Ordering::SeqCst);
        Ok(())
    }

    pub(crate) async fn consume(&self, piece: u64) -> Result<(), BroadcastError> {
        self.check()?;
        let next = piece
            .checked_add(1)
            .ok_or(BroadcastError::PieceIndexExhausted)?;
        let current = self.reserved_until();
        if piece >= current {
            let blocks = (piece - current) / CURSOR_PERSIST_INTERVAL;
            let extension = blocks
                .checked_add(1)
                .and_then(|n| n.checked_mul(CURSOR_PERSIST_INTERVAL))
                .and_then(|n| current.checked_add(n))
                .ok_or(BroadcastError::PieceIndexExhausted)?;
            self.save(extension).await?;
            self.reserved.fetch_max(extension, Ordering::SeqCst);
        }
        self.next.fetch_max(next, Ordering::SeqCst);
        self.check()
    }

    pub(crate) async fn checkpoint(&self) -> Result<(), BroadcastError> {
        self.save(self.reserved_until()).await
    }

    pub async fn flush(&self) -> Result<(), BroadcastError> {
        let _publication = self.publication.lock().await;
        self.save(self.reserved_until()).await
    }
}

#[derive(Clone)]
pub struct Broadcast {
    pub infohash: [u8; 20],
    pub content_id: [u8; 20],
    pub transport_bytes: Arc<Vec<u8>>,
    pub geometry: BroadcastGeometry,
    pub trackers: Arc<Vec<String>>,
    pub store: Arc<Mutex<PieceStore>>,
    pub auth: Arc<LiveSourceAuth>,
    pub cursor: Arc<BroadcastCursor>,
}

pub(crate) struct ProducerAuthority {
    generation: u64,
    pub(crate) cancel: watch::Sender<bool>,
    owner_alive: AtomicBool,
    pub(crate) ingest_busy: Arc<AtomicBool>,
    pub(crate) operation: Arc<Mutex<()>>,
    // Publication jobs are deliberately not abortable: an already queued store write must
    // settle while still holding publication authority. Jobs hold no authority/controller.
    pub(crate) jobs: StdMutex<Vec<JoinHandle<()>>>,
    ingress: StdMutex<Option<IngressTask>>,
    kind: ProducerKind,
}

/// Completion observer for entry-owned ingress. It owns no controller/task handle, so a
/// source attachment or test may observe completion without taking join ownership away.
#[derive(Clone)]
pub struct IngressTask {
    completion: watch::Receiver<bool>,
}
impl IngressTask {
    pub async fn wait(&self) -> Result<(), BroadcastError> {
        let mut completion = self.completion.clone();
        loop {
            if *completion.borrow_and_update() {
                return Ok(());
            }
            if completion.changed().await.is_err() {
                return Err(BroadcastError::StateInvalid);
            }
        }
    }
}

/// Noncloneable producer claim. An ingest holds its own noncloneable attempt permit.
/// Dropping the session cancels all attempts; pending operations still own their fences.
pub struct ProducerSession {
    broadcast: Broadcast,
    pub(crate) authority: Arc<ProducerAuthority>,
}

impl ProducerSession {
    pub fn open_ingest(&mut self) -> Result<BroadcastIngest, IngestError> {
        self.check().map_err(IngestError::from)?;
        if self
            .authority
            .ingest_busy
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(IngestError::ActiveIngest);
        }
        Ok(BroadcastIngest::authorized(
            self.broadcast.clone(),
            self.authority.clone(),
        ))
    }

    pub fn kind(&self) -> ProducerKind {
        self.authority.kind
    }
    pub fn generation(&self) -> u64 {
        self.authority.generation
    }
    pub fn cancellation(&self) -> watch::Receiver<bool> {
        self.authority.cancel.subscribe()
    }
    pub(crate) fn generation_cancellation(&self) -> watch::Receiver<bool> {
        self.broadcast.cursor.cancel.clone()
    }
    pub fn check(&self) -> Result<(), BroadcastError> {
        self.broadcast.cursor.check()?;
        if *self.authority.cancel.borrow() {
            return Err(BroadcastError::Stopping);
        }
        Ok(())
    }
    /// RTMP holds this barrier across mux, ingest and finite finish. Cleanup waits for it
    /// without requiring the enclosing dependency-owned connection task to terminate.
    pub(crate) fn operation_barrier(&self) -> Arc<Mutex<()>> {
        self.authority.operation.clone()
    }
}
impl Drop for ProducerSession {
    fn drop(&mut self) {
        self.authority.owner_alive.store(false, Ordering::SeqCst);
        self.authority.cancel.send_replace(true);
    }
}

struct EntryData {
    broadcast: Option<Broadcast>,
    lease: Option<SeedLease>,
    invalid: Option<BroadcastError>,
    stopping: bool,
    closed: bool,
    producer: Option<Arc<ProducerAuthority>>,
    announcing: bool,
    source: Option<SourceAttachment>,
    source_detach: Option<(Completion, tokio::time::Instant)>,
}
struct SourceAttachment {
    config: ValidatedSourceConfig,
    status: SourceStatusWriter,
    authority: Arc<ProducerAuthority>,
    worker_abort: AbortHandle,
    stopping: bool,
}

struct EntryState {
    capacity_invalid: AtomicBool,
    data: StdMutex<EntryData>,
    cancel: watch::Sender<bool>,
    service_closed: Arc<AtomicBool>,
    io: Arc<Mutex<()>>,
    publication: Arc<Mutex<()>>,
}
struct StopJob {
    completion: Completion,
    deadline: tokio::time::Instant,
    _join: JoinHandle<()>,
}
struct BroadcastEntry {
    state: Arc<EntryState>,
    lifecycle: Arc<Mutex<()>>,
    source_gate: Arc<Mutex<()>>,
    initializers: StdMutex<Vec<JoinHandle<()>>>,
    tasks: StdMutex<Vec<JoinHandle<()>>>,
    aborters: StdMutex<Vec<AbortHandle>>,
    stop: StdMutex<Option<StopJob>>,
}
impl BroadcastEntry {
    fn new(service_closed: Arc<AtomicBool>) -> Self {
        Self {
            state: Arc::new(EntryState {
                capacity_invalid: AtomicBool::new(false),
                data: StdMutex::new(EntryData {
                    broadcast: None,
                    lease: None,
                    invalid: None,
                    stopping: false,
                    closed: false,
                    producer: None,
                    announcing: false,
                    source: None,
                    source_detach: None,
                }),
                cancel: watch::channel(false).0,
                service_closed,
                io: Arc::new(Mutex::new(())),
                publication: Arc::new(Mutex::new(())),
            }),
            lifecycle: Arc::new(Mutex::new(())),
            source_gate: Arc::new(Mutex::new(())),
            initializers: StdMutex::new(Vec::new()),
            tasks: StdMutex::new(Vec::new()),
            aborters: StdMutex::new(Vec::new()),
            stop: StdMutex::new(None),
        }
    }
    fn stop_admissions(&self) {
        let mut data = self.state.data.lock().unwrap();
        data.stopping = true;
        if let Some(source) = &data.source {
            source.status.stopping();
        }
        self.state.cancel.send_replace(true);
        if let Some(producer) = &data.producer {
            producer.cancel.send_replace(true);
        }
    }
    fn abort_async(&self) {
        for abort in self.aborters.lock().unwrap().iter() {
            abort.abort();
        }
    }
}

/// Registry map locks only locate controllers; joins and all filesystem work are per entry.
pub struct BroadcastRegistry {
    options: BTreeMap<String, BroadcastOptions>,
    by_name: Arc<StdMutex<BTreeMap<String, Arc<BroadcastEntry>>>>,
    closed: Arc<AtomicBool>,
    service_cancel: watch::Sender<bool>,
    next_generation: AtomicU64,
    persist: Option<BroadcastPersist>,
    cache_type: CacheType,
    cache_dir: PathBuf,
    shutdown_deadline: StdMutex<Option<tokio::time::Instant>>,
    source_policy: StdMutex<FetchPolicy>,
    #[cfg(test)]
    source_hooks: StdMutex<crate::broadcast_pull::SourceTestHooks>,
    #[cfg(test)]
    announcer: StdMutex<Option<AnnouncerFactory>>,
}

type AnnounceFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
#[cfg(test)]
pub(crate) type AnnouncerFactory =
    Arc<dyn Fn([u8; 20], Vec<String>) -> AnnounceFuture + Send + Sync>;

impl BroadcastRegistry {
    pub fn new() -> Arc<Self> {
        Self::configured(None, CacheType::Memory, PathBuf::new())
    }
    pub fn with_persist(data_dir: &Path, cache_type: CacheType, cache_dir: PathBuf) -> Arc<Self> {
        Self::configured(Some(BroadcastPersist::new(data_dir)), cache_type, cache_dir)
    }
    fn configured(
        persist: Option<BroadcastPersist>,
        cache_type: CacheType,
        cache_dir: PathBuf,
    ) -> Arc<Self> {
        Self::configured_with_options(persist, cache_type, cache_dir, BTreeMap::new())
    }
    pub fn new_with_options(
        options: BTreeMap<String, BroadcastOptions>,
    ) -> Result<Arc<Self>, BroadcastOptionsError> {
        validate_broadcast_options(&options)?;
        Ok(Self::configured_with_options(
            None,
            CacheType::Memory,
            PathBuf::new(),
            options,
        ))
    }
    pub fn with_persist_and_options(
        data_dir: &Path,
        cache_type: CacheType,
        cache_dir: PathBuf,
        options: BTreeMap<String, BroadcastOptions>,
    ) -> Result<Arc<Self>, BroadcastOptionsError> {
        validate_broadcast_options(&options)?;
        Ok(Self::configured_with_options(
            Some(BroadcastPersist::new(data_dir)),
            cache_type,
            cache_dir,
            options,
        ))
    }
    fn configured_with_options(
        persist: Option<BroadcastPersist>,
        cache_type: CacheType,
        cache_dir: PathBuf,
        options: BTreeMap<String, BroadcastOptions>,
    ) -> Arc<Self> {
        Arc::new(Self {
            options,
            by_name: Arc::new(StdMutex::new(BTreeMap::new())),
            closed: Arc::new(AtomicBool::new(false)),
            service_cancel: watch::channel(false).0,
            next_generation: AtomicU64::new(1),
            persist,
            cache_type,
            cache_dir,
            shutdown_deadline: StdMutex::new(None),
            source_policy: StdMutex::new(FetchPolicy::default()),
            #[cfg(test)]
            source_hooks: StdMutex::new(crate::broadcast_pull::SourceTestHooks::default()),
            #[cfg(test)]
            announcer: StdMutex::new(None),
        })
    }
    #[cfg(test)]
    pub(crate) fn with_test_persist(persist: BroadcastPersist) -> Arc<Self> {
        Self::configured(Some(persist), CacheType::Memory, PathBuf::new())
    }
    #[cfg(test)]
    pub(crate) fn set_test_announcer(&self, factory: AnnouncerFactory) {
        *self.announcer.lock().unwrap() = Some(factory);
    }

    pub fn is_service_stopping(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
    pub(crate) fn service_cancellation(&self) -> watch::Receiver<bool> {
        self.service_cancel.subscribe()
    }

    fn entry(&self, name: &str) -> Result<Arc<BroadcastEntry>, BroadcastError> {
        if !valid_broadcast_name(name) {
            return Err(BroadcastError::InvalidName);
        }
        let mut map = self.by_name.lock().unwrap();
        if self.closed.load(Ordering::SeqCst) {
            return Err(BroadcastError::ServiceStopping);
        }
        Ok(map
            .entry(name.to_owned())
            .or_insert_with(|| Arc::new(BroadcastEntry::new(self.closed.clone())))
            .clone())
    }
    fn check_entry(state: &EntryState, data: &EntryData) -> Result<(), BroadcastError> {
        if state.service_closed.load(Ordering::SeqCst) {
            return Err(BroadcastError::ServiceStopping);
        }
        if data.stopping || data.closed {
            return Err(BroadcastError::Stopping);
        }
        if let Some(error) = data.invalid {
            return Err(error);
        }
        Ok(())
    }
    pub async fn get(&self, name: &str) -> Option<Broadcast> {
        let entry = self.by_name.lock().unwrap().get(name).cloned()?;
        let data = entry.state.data.lock().unwrap();
        if data.stopping || data.closed {
            None
        } else {
            data.broadcast.clone()
        }
    }
    pub async fn names(&self) -> Vec<String> {
        self.by_name
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(name, entry)| {
                let data = entry.state.data.lock().unwrap();
                (data.broadcast.is_some() && !data.stopping && !data.closed).then(|| name.clone())
            })
            .collect()
    }

    async fn lock_lifecycle(entry: &BroadcastEntry) -> Result<OwnedMutexGuard<()>, BroadcastError> {
        {
            let data = entry.state.data.lock().unwrap();
            Self::check_entry(&entry.state, &data)?;
        }
        let mut cancel = entry.state.cancel.subscribe();
        tokio::select! {
            biased;
            () = cancelled(&mut cancel) => {
                if entry.state.service_closed.load(Ordering::SeqCst) {
                    Err(BroadcastError::ServiceStopping)
                } else { Err(BroadcastError::Stopping) }
            }
            guard = entry.lifecycle.clone().lock_owned() => Ok(guard),
        }
    }

    async fn prepare(
        &self,
        name: &str,
        trackers: &[String],
        seed_registry: &SeedRegistry,
        store_bytes: u64,
        require_persisted: bool,
    ) -> Result<(Arc<BroadcastEntry>, Broadcast), BroadcastError> {
        let entry = self.entry(name)?;
        let lifecycle = Self::lock_lifecycle(&entry).await?;
        {
            let data = entry.state.data.lock().unwrap();
            Self::check_entry(&entry.state, &data)?;
            if let Some(bc) = &data.broadcast {
                check_store_capacity(bc.geometry, store_bytes, self.cache_type)?;
                return Ok((entry.clone(), bc.clone()));
            }
        }
        let state = entry.state.clone();
        let name = name.to_owned();
        let options = self.options.get(&name).cloned().unwrap_or_default();
        let trackers = trackers.to_vec();
        let seed_registry = seed_registry.clone();
        let persist = self.persist.clone();
        let cache_type = self.cache_type;
        let cache_dir = self.cache_dir.clone();
        let (tx, rx) = watch::channel(None);
        // Ownership is committed before waiting: cancellation of the HTTP/CLI caller cannot
        // drop an in-flight initialization or free its lifecycle/I/O fence.
        let job = tokio::spawn(async move {
            let _lifecycle = lifecycle;
            let io = state.io.clone().lock_owned().await;
            let init_state = state.clone();
            let result = tokio::task::spawn_blocking(move || {
                let _io = io;
                initialize(
                    &name,
                    &options,
                    &trackers,
                    &seed_registry,
                    store_bytes,
                    persist,
                    cache_type,
                    cache_dir,
                    &init_state,
                    require_persisted,
                )
            })
            .await
            .unwrap_or(Err(BroadcastError::StateInvalid));
            let mut data = state.data.lock().unwrap();
            let outcome = match result {
                Ok((bc, lease)) => {
                    data.broadcast = Some(bc);
                    data.lease = Some(lease);
                    Self::check_entry(&state, &data)
                }
                Err(error) => {
                    data.invalid = Some(error);
                    Err(error)
                }
            };
            tx.send_replace(Some(outcome));
        });
        entry.initializers.lock().unwrap().push(job);
        let mut cancel = entry.state.cancel.subscribe();
        tokio::select! {
            biased;
            () = cancelled(&mut cancel) => {
                return Err(if entry.state.service_closed.load(Ordering::SeqCst) {
                    BroadcastError::ServiceStopping
                } else { BroadcastError::Stopping });
            }
            result = completed(rx) => result?,
        }
        let bc = entry
            .state
            .data
            .lock()
            .unwrap()
            .broadcast
            .clone()
            .ok_or(BroadcastError::StateInvalid)?;
        Ok((entry, bc))
    }

    pub async fn mint(
        &self,
        name: &str,
        trackers: &[String],
        seed_registry: &SeedRegistry,
        store_bytes: u64,
    ) -> Result<Broadcast, BroadcastError> {
        let (_, bc) = self
            .prepare(name, trackers, seed_registry, store_bytes, false)
            .await?;
        bc.cursor.check()?;
        Ok(bc)
    }

    pub async fn acquire_producer(
        &self,
        name: &str,
        trackers: &[String],
        seed_registry: &SeedRegistry,
        store_bytes: u64,
        kind: ProducerKind,
    ) -> Result<(Broadcast, ProducerSession), BroadcastError> {
        self.source_preflight(name, true)?;
        let (entry, bc) = self
            .prepare(name, trackers, seed_registry, store_bytes, false)
            .await?;
        let _lifecycle = Self::lock_lifecycle(&entry).await?;
        let mut data = entry.state.data.lock().unwrap();
        Self::check_entry(&entry.state, &data)?;
        if data.source_detach.is_some() {
            return Err(BroadcastError::Stopping);
        }
        if let Some(source) = &data.source {
            return Err(if source.stopping {
                BroadcastError::Stopping
            } else {
                BroadcastError::ProducerConflict
            });
        }
        if let Some(old) = &data.producer {
            if old.owner_alive.load(Ordering::SeqCst)
                || old.ingest_busy.load(Ordering::SeqCst)
                || old.operation.try_lock().is_err()
                || bc.cursor.publication.try_lock().is_err()
                || bc.cursor.io.try_lock().is_err()
            {
                return Err(BroadcastError::ProducerConflict);
            }
        }
        bc.cursor.check()?;
        let generation = self
            .next_generation
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |generation| {
                generation.checked_add(1)
            })
            .map_err(|_| BroadcastError::StateInvalid)?;
        let authority = Arc::new(ProducerAuthority {
            generation,
            cancel: watch::channel(false).0,
            owner_alive: AtomicBool::new(true),
            ingest_busy: Arc::new(AtomicBool::new(false)),
            operation: Arc::new(Mutex::new(())),
            jobs: StdMutex::new(Vec::new()),
            ingress: StdMutex::new(None),
            kind,
        });
        data.producer = Some(authority.clone());
        Ok((
            bc.clone(),
            ProducerSession {
                broadcast: bc,
                authority,
            },
        ))
    }

    pub(crate) fn set_source_policy(&self, policy: FetchPolicy) {
        *self.source_policy.lock().unwrap() = policy;
    }
    pub(crate) fn source_policy(&self) -> FetchPolicy {
        *self.source_policy.lock().unwrap()
    }

    #[cfg(test)]
    pub(crate) fn set_source_test_hooks(&self, hooks: crate::broadcast_pull::SourceTestHooks) {
        *self.source_hooks.lock().unwrap() = hooks;
    }

    /// Read-only route preflight. Unknown names never mint an entry or identity. GET may
    /// inspect a stopping source, while mutation admission observes its stopping fence.
    pub(crate) fn source_preflight(
        &self,
        name: &str,
        attaching: bool,
    ) -> Result<(), BroadcastError> {
        if !valid_broadcast_name(name) {
            return Err(BroadcastError::InvalidName);
        }
        if self.is_service_stopping() {
            return Err(BroadcastError::ServiceStopping);
        }
        if let Some(entry) = self.by_name.lock().unwrap().get(name) {
            let data = entry.state.data.lock().unwrap();
            if attaching
                && (data.stopping
                    || data.closed
                    || data.source_detach.is_some()
                    || data.source.as_ref().is_some_and(|s| s.stopping))
            {
                return Err(BroadcastError::Stopping);
            }
        }
        Ok(())
    }

    /// Admission itself is an entry-owned task before its first await. An HTTP future can
    /// disappear at any point without dropping an admitted claim between acquire and spawn.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn attach_source(
        self: &Arc<Self>,
        name: &str,
        config: ValidatedSourceConfig,
        seeds: &SeedRegistry,
        trackers: &[String],
        store_bytes: u64,
        port: watch::Receiver<Option<u16>>,
    ) -> Result<(Broadcast, SourceStatus), BroadcastError> {
        self.source_preflight(name, true)?;
        let entry = self.entry(name)?;
        let registry = self.clone();
        let name = name.to_owned();
        let seeds = seeds.clone();
        let trackers = trackers.to_vec();
        let owned_entry = entry.clone();
        let (tx, mut rx) = watch::channel(None);
        {
            // start_stop first closes this data gate before taking owned initializers.
            let data = entry.state.data.lock().unwrap();
            Self::check_entry(&entry.state, &data)?;
            let task = tokio::spawn(async move {
                let result = registry
                    .commit_source(
                        &owned_entry,
                        &name,
                        config,
                        &seeds,
                        &trackers,
                        store_bytes,
                        port,
                    )
                    .await;
                tx.send_replace(Some(result));
            });
            let mut initializers = entry.initializers.lock().unwrap();
            initializers.retain(|job| !job.is_finished());
            initializers.push(task);
        }
        let mut service_cancel = self.service_cancellation();
        // Cancel only this request's observer. The initializer and any persistence work
        // stay entry-owned even when shutdown releases the HTTP request before they settle.
        tokio::select! {
            biased;
            () = cancelled(&mut service_cancel) => Err(BroadcastError::ServiceStopping),
            result = async {
                loop {
                    let result = rx.borrow_and_update().clone();
                    if let Some(result) = result {
                        #[cfg(test)]
                        if result.is_ok() {
                            let gate = self.source_hooks.lock().unwrap().after_commit.clone();
                            if let Some(gate) = gate { gate().await; }
                        }
                        return result;
                    }
                    if rx.changed().await.is_err() { return Err(BroadcastError::StateInvalid); }
                }
            } => {
                if self.is_service_stopping() { Err(BroadcastError::ServiceStopping) }
                else { result }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_source(
        &self,
        entry: &BroadcastEntry,
        name: &str,
        config: ValidatedSourceConfig,
        seeds: &SeedRegistry,
        trackers: &[String],
        store_bytes: u64,
        port: watch::Receiver<Option<u16>>,
    ) -> Result<(Broadcast, SourceStatus), BroadcastError> {
        #[cfg(test)]
        {
            let hook = self.source_hooks.lock().unwrap().before_source_gate.clone();
            if let Some(hook) = hook {
                hook().await;
            }
        }
        let mut service_cancel = self.service_cancellation();
        let mut entry_cancel = entry.state.cancel.subscribe();
        let _gate = tokio::select! {
            biased;
            () = cancelled(&mut service_cancel) => return Err(BroadcastError::ServiceStopping),
            () = cancelled(&mut entry_cancel) => return Err(if self.is_service_stopping() {
                BroadcastError::ServiceStopping
            } else { BroadcastError::Stopping }),
            gate = entry.source_gate.lock() => gate,
        };
        {
            let data = entry.state.data.lock().unwrap();
            // Cancellation may race lock readiness; recheck before inspecting or changing
            // the attachment, even when the lock branch won the selection.
            Self::check_entry(&entry.state, &data)?;
            if data.source_detach.is_some() {
                return Err(BroadcastError::Stopping);
            }
            if let Some(source) = &data.source {
                if source.stopping {
                    return Err(BroadcastError::Stopping);
                }
                if source.config != config {
                    return Err(BroadcastError::SourceConflict);
                }
                let bc = data.broadcast.clone().ok_or(BroadcastError::StateInvalid)?;
                check_store_capacity(bc.geometry, store_bytes, self.cache_type)?;
                return Ok((bc, source.status.snapshot()));
            }
        }
        let (bc, session) = self
            .acquire_producer(name, trackers, seeds, store_bytes, ProducerKind::Pull)
            .await?;
        self.ensure_announcing(name, port).await?;
        let status = SourceStatusWriter::new(name);
        #[cfg(test)]
        let status = {
            let mut status = status;
            status.hooks = self.source_hooks.lock().unwrap().clone();
            status
        };
        // Worker/status/handle visibility commits together below, with no await.
        let mut data = entry.state.data.lock().unwrap();
        Self::check_entry(&entry.state, &data)?;
        if data.source_detach.is_some() {
            return Err(BroadcastError::Stopping);
        }
        session.check()?;
        let authority = session.authority.clone();
        let policy = self.source_policy();
        let worker_config = config.clone();
        let worker_status = status.clone();
        let (done, completion) = watch::channel(false);
        let worker = IngressTask { completion };
        *authority.ingress.lock().unwrap() = Some(worker.clone());
        let task = tokio::spawn(async move {
            run_source(session, worker_config, policy, worker_status).await;
            done.send_replace(true);
        });
        let worker_abort = task.abort_handle();
        let mut aborters = entry.aborters.lock().unwrap();
        aborters.retain(|abort| !abort.is_finished());
        aborters.push(worker_abort.clone());
        let mut tasks = entry.tasks.lock().unwrap();
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
        data.source = Some(SourceAttachment {
            config,
            status: status.clone(),
            authority,
            worker_abort,
            stopping: false,
        });
        Ok((bc, status.snapshot()))
    }

    pub(crate) async fn source_status(
        &self,
        name: &str,
    ) -> Result<Option<SourceStatus>, BroadcastError> {
        self.source_preflight(name, false)?;
        let entry = self.by_name.lock().unwrap().get(name).cloned();
        Ok(entry.and_then(|entry| {
            entry
                .state
                .data
                .lock()
                .unwrap()
                .source
                .as_ref()
                .map(|source| source.status.snapshot())
        }))
    }

    pub(crate) async fn detach_source(self: &Arc<Self>, name: &str) -> Result<(), BroadcastError> {
        self.source_preflight(name, false)?;
        let Some(entry) = self.by_name.lock().unwrap().get(name).cloned() else {
            return Ok(());
        };
        let stop = Self::queue_source_detach(&entry)?;
        let Some((completion, deadline)) = stop else {
            return Ok(());
        };
        match tokio::time::timeout_at(deadline, completed(completion)).await {
            Ok(result) => result,
            Err(_) => {
                if let Some(source) = &entry.state.data.lock().unwrap().source {
                    source.worker_abort.abort();
                }
                Err(BroadcastError::CleanupTimeout)
            }
        }
    }

    fn queue_source_detach(
        entry: &BroadcastEntry,
    ) -> Result<Option<(Completion, tokio::time::Instant)>, BroadcastError> {
        let mut data = entry.state.data.lock().unwrap();
        if entry.state.service_closed.load(Ordering::SeqCst) {
            return Err(BroadcastError::ServiceStopping);
        }
        if data.stopping || data.closed {
            return Err(BroadcastError::Stopping);
        }
        if let Some(stop) = &data.source_detach {
            return Ok(Some(stop.clone()));
        }
        // No attachment and no admission occupying its gate: raw/RTMP is an immediate no-op.
        if data.source.is_none() && entry.source_gate.try_lock().is_ok() {
            return Ok(None);
        }
        if let Some(source) = data.source.as_mut() {
            source.stopping = true;
            source.status.stopping();
            source.authority.cancel.send_replace(true);
        }
        let state = entry.state.clone();
        let source_gate = entry.source_gate.clone();
        // Only the task list is shared, not the entry/controller; the continuation cannot
        // own the registry. Taking the worker handle happens under the admission data gate.
        let worker = if let Some(source) = &data.source {
            let mut tasks = entry.tasks.lock().unwrap();
            let index = tasks
                .iter()
                .position(|task| task.id() == source.worker_abort.id())
                .ok_or(BroadcastError::StateInvalid)?;
            Some((tasks.swap_remove(index), source.authority.clone()))
        } else {
            None
        };
        let (tx, rx) = watch::channel(None);
        let deadline = tokio::time::Instant::now() + CLEANUP_GRACE;
        data.source_detach = Some((rx.clone(), deadline));
        // Owned before waiting on a pending admission. That admission rechecks the detach
        // fence before claiming/spawning; an HTTP timeout cannot later resurrect a source.
        let job = tokio::spawn(async move {
            let _gate = source_gate.lock().await;
            if let Some((worker, authority)) = worker {
                let _ = worker.await;
                let _operation = authority.operation.clone().lock_owned().await;
                let jobs = std::mem::take(&mut *authority.jobs.lock().unwrap());
                for job in jobs {
                    let _ = job.await;
                }
                let _publication = state.publication.clone().lock_owned().await;
                let _io = state.io.clone().lock_owned().await;
                let mut data = state.data.lock().unwrap();
                if data
                    .source
                    .as_ref()
                    .is_some_and(|s| Arc::ptr_eq(&s.authority, &authority))
                {
                    data.source.take();
                    if data
                        .producer
                        .as_ref()
                        .is_some_and(|p| Arc::ptr_eq(p, &authority))
                    {
                        data.producer.take();
                    }
                }
                data.source_detach.take();
            } else {
                // A still-running admission may own a noncancelable initialization save.
                // Its source gate is released only after that admission has settled.
                state.data.lock().unwrap().source_detach.take();
            }
            tx.send_replace(Some(Ok(())));
        });
        let mut initializers = entry.initializers.lock().unwrap();
        initializers.retain(|job| !job.is_finished());
        initializers.push(job);
        Ok(Some((rx, deadline)))
    }

    /// Startup reconstructs/reserves identities but starts no network work. Invalid names
    /// retain their tombstones; a directory-enumeration failure is a startup error.
    pub async fn reload_persisted(
        &self,
        seed_registry: &SeedRegistry,
        store_bytes: u64,
        configured_sources: &BTreeSet<String>,
    ) -> Result<Vec<Broadcast>, BroadcastError> {
        let Some(persist) = &self.persist else {
            return Ok(Vec::new());
        };
        let persist = persist.clone();
        let records = tokio::task::spawn_blocking(move || persist.load_all())
            .await
            .map_err(|_| BroadcastError::StateInvalid)?
            .map_err(|_| BroadcastError::PersistenceFailed)?;
        let mut out = Vec::new();
        for (name, _record) in records {
            if !valid_broadcast_name(&name) {
                let mut map = self.by_name.lock().unwrap();
                let entry = map
                    .entry(name)
                    .or_insert_with(|| Arc::new(BroadcastEntry::new(self.closed.clone())));
                entry.state.data.lock().unwrap().invalid = Some(BroadcastError::StateInvalid);
                crate::alog!("[broadcast] persisted state rejected: broadcast_state_invalid");
                continue;
            }
            // Re-read and validate inside prepare's owned lifecycle/I/O fences. The
            // enumeration snapshot cannot authorize a later, replaced record.
            let restored = self
                .prepare(&name, &[], seed_registry, store_bytes, true)
                .await
                .and_then(|(_, bc)| {
                    bc.cursor.check()?;
                    Ok(bc)
                });
            match restored {
                Ok(bc) => out.push(bc),
                Err(error) => {
                    let capacity_invalid =
                        self.by_name
                            .lock()
                            .unwrap()
                            .get(&name)
                            .is_some_and(|entry| {
                                entry.state.capacity_invalid.load(Ordering::SeqCst)
                                    || entry
                                        .state
                                        .data
                                        .lock()
                                        .unwrap()
                                        .broadcast
                                        .as_ref()
                                        .is_some_and(|bc| {
                                            check_store_capacity(
                                                bc.geometry,
                                                store_bytes,
                                                self.cache_type,
                                            )
                                            .is_err()
                                        })
                            });
                    if capacity_invalid
                        || !matches!(
                            error,
                            BroadcastError::StateInvalid | BroadcastError::PieceIndexExhausted
                        )
                        || self.options.contains_key(&name)
                        || configured_sources.contains(&name)
                    {
                        return Err(error);
                    }
                    crate::alog!("[broadcast] persisted state rejected: {}", error.code());
                }
            }
        }
        Ok(out)
    }

    /// Atomically register a task while the generation is open. The future owns only its
    /// producer/session data; the entry owns the handle, avoiding controller/worker cycles.
    pub fn spawn_ingress<F, Run>(
        &self,
        name: &str,
        session: ProducerSession,
        run: Run,
    ) -> Result<IngressTask, BroadcastError>
    where
        F: Future<Output = ()> + Send + 'static,
        Run: FnOnce(ProducerSession) -> F,
    {
        let entry = self
            .by_name
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or(BroadcastError::Stopping)?;
        let data = entry.state.data.lock().unwrap();
        Self::check_entry(&entry.state, &data)?;
        if !data
            .producer
            .as_ref()
            .is_some_and(|p| Arc::ptr_eq(p, &session.authority))
        {
            return Err(BroadcastError::Stopping);
        }
        session.check()?;
        let mut tasks = entry.tasks.lock().unwrap();
        // Reap completed handles; completed tasks have no outstanding work to detach.
        tasks.retain(|task| !task.is_finished());
        let authority = session.authority.clone();
        let future = run(session);
        let (done, completion) = watch::channel(false);
        let observer = IngressTask { completion };
        *authority.ingress.lock().unwrap() = Some(observer.clone());
        let task = tokio::spawn(async move {
            future.await;
            done.send_replace(true);
        });
        let mut aborters = entry.aborters.lock().unwrap();
        aborters.retain(|abort| !abort.is_finished());
        aborters.push(task.abort_handle());
        tasks.push(task);
        Ok(observer)
    }

    pub async fn ensure_announcing(
        &self,
        name: &str,
        port: watch::Receiver<Option<u16>>,
    ) -> Result<(), BroadcastError> {
        let entry = self
            .by_name
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or(BroadcastError::StateInvalid)?;
        let mut data = entry.state.data.lock().unwrap();
        Self::check_entry(&entry.state, &data)?;
        if data.announcing || port.borrow().is_none() {
            return Ok(());
        }
        let bc = data
            .broadcast
            .as_ref()
            .ok_or(BroadcastError::StateInvalid)?;
        bc.cursor.check()?;
        let mut tasks = entry.tasks.lock().unwrap();
        let trackers = bc.trackers.as_ref();
        for id in [bc.infohash, bc.content_id] {
            #[cfg(test)]
            let factory = self.announcer.lock().unwrap().clone();
            #[cfg(test)]
            let announce: AnnounceFuture = match factory {
                Some(factory) => factory(id, trackers.to_vec()),
                None => Box::pin(crate::ace_provider::announce_infohash_periodically_dynamic(
                    trackers.to_vec(),
                    id,
                    port.clone(),
                )),
            };
            #[cfg(not(test))]
            let announce: AnnounceFuture =
                Box::pin(crate::ace_provider::announce_infohash_periodically_dynamic(
                    trackers.to_vec(),
                    id,
                    port.clone(),
                ));
            let mut cancel = entry.state.cancel.subscribe();
            let task = tokio::spawn(async move {
                tokio::select! { biased; () = cancelled(&mut cancel) => {}, () = announce => {} }
            });
            entry.aborters.lock().unwrap().push(task.abort_handle());
            tasks.push(task);
        }
        data.announcing = true;
        Ok(())
    }

    pub fn cleanup_deadline(&self) -> tokio::time::Instant {
        *self
            .shutdown_deadline
            .lock()
            .unwrap()
            .get_or_insert_with(|| tokio::time::Instant::now() + CLEANUP_GRACE)
    }

    pub fn close_admissions(&self) {
        self.cleanup_deadline();
        let map = self.by_name.lock().unwrap();
        self.closed.store(true, Ordering::SeqCst);
        self.service_cancel.send_replace(true);
        for entry in map.values() {
            entry.stop_admissions();
        }
    }

    fn start_stop(
        &self,
        name: &str,
        entry: &BroadcastEntry,
        delete: bool,
        deadline: tokio::time::Instant,
    ) -> (Completion, tokio::time::Instant) {
        let mut stop = entry.stop.lock().unwrap();
        if let Some(job) = stop.as_ref() {
            let result = *job.completion.borrow();
            if result.is_none() || result == Some(Ok(())) {
                return (job.completion.clone(), job.deadline.min(deadline));
            }
            // Only an explicit subsequent call retries a settled failed deletion.
        }
        entry.stop_admissions();
        let state = entry.state.clone();
        let lifecycle = entry.lifecycle.clone();
        let tasks = std::mem::take(&mut *entry.tasks.lock().unwrap());
        let initializers = std::mem::take(&mut *entry.initializers.lock().unwrap());
        let persist = self.persist.clone();
        let name = name.to_owned();
        let map = Arc::downgrade(&self.by_name);
        let (tx, rx) = watch::channel(None);
        let job = tokio::spawn(async move {
            let _lifecycle = lifecycle.lock_owned().await;
            for init in initializers {
                let _ = init.await;
            }
            for task in tasks {
                let _ = task.await;
            }
            let producer = state.data.lock().unwrap().producer.clone();
            let _operation = if let Some(producer) = &producer {
                Some(producer.operation.clone().lock_owned().await)
            } else {
                None
            };
            if let Some(producer) = &producer {
                let jobs = std::mem::take(&mut *producer.jobs.lock().unwrap());
                for job in jobs {
                    let _ = job.await;
                }
            }
            let _publication = state.publication.clone().lock_owned().await;
            let io = state.io.clone().lock_owned().await;
            let bc = state.data.lock().unwrap().broadcast.clone();
            if let Some(bc) = &bc {
                bc.cursor.closed.store(true, Ordering::SeqCst);
                let jobs = std::mem::take(&mut *bc.cursor.io_jobs.lock().unwrap());
                for job in jobs {
                    let _ = job.await;
                }
            }
            // Keep the guard and blocking handle in this non-abortable stop continuation.
            let result = if delete {
                if let Some(persist) = persist {
                    tokio::task::spawn_blocking(move || {
                        let _io = io;
                        persist
                            .delete(&name)
                            .map_err(|_| BroadcastError::PersistenceFailed)
                    })
                    .await
                    .unwrap_or(Err(BroadcastError::StateInvalid))
                } else {
                    drop(io);
                    Ok(())
                }
            } else {
                drop(io);
                Ok(())
            };
            if result.is_ok() {
                let mut data = state.data.lock().unwrap();
                data.lease.take();
                data.broadcast.take();
                data.producer.take();
                data.source.take();
                data.closed = true;
                drop(data);
                if delete {
                    if let Some(map) = map.upgrade() {
                        let mut map = map.lock().unwrap();
                        // Locate by state identity; never remove a replacement controller.
                        map.retain(|_, current| !Arc::ptr_eq(&current.state, &state));
                    }
                }
            }
            tx.send_replace(Some(result));
        });
        *stop = Some(StopJob {
            completion: rx.clone(),
            deadline,
            _join: job,
        });
        (rx, deadline)
    }

    pub async fn delete(&self, name: &str) -> Result<(), BroadcastError> {
        if !valid_broadcast_name(name) {
            return Err(BroadcastError::InvalidName);
        }
        let entry = {
            let mut map = self.by_name.lock().unwrap();
            if self.closed.load(Ordering::SeqCst) {
                return Err(BroadcastError::ServiceStopping);
            }
            map.entry(name.to_owned())
                .or_insert_with(|| Arc::new(BroadcastEntry::new(self.closed.clone())))
                .clone()
        };
        let (rx, deadline) = self.start_stop(
            name,
            &entry,
            true,
            tokio::time::Instant::now() + CLEANUP_GRACE,
        );
        match tokio::time::timeout_at(deadline, completed(rx)).await {
            Ok(result) => result,
            Err(_) => {
                entry.abort_async();
                Err(BroadcastError::CleanupTimeout)
            }
        }
    }

    pub async fn shutdown(&self) -> Result<(), BroadcastError> {
        self.close_admissions();
        let entries: Vec<_> = self
            .by_name
            .lock()
            .unwrap()
            .iter()
            .map(|(name, entry)| (name.clone(), entry.clone()))
            .collect();
        let deadline = self.cleanup_deadline();
        let stops: Vec<_> = entries
            .iter()
            .map(|(name, entry)| self.start_stop(name, entry, false, deadline))
            .collect();
        let mut outcome = Ok(());
        for (stop, stop_deadline) in stops {
            match tokio::time::timeout_at(stop_deadline, completed(stop)).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    outcome = Err(error);
                }
                Err(_) => {
                    for (_, entry) in &entries {
                        entry.abort_async();
                    }
                    outcome = Err(BroadcastError::CleanupTimeout);
                }
            }
        }
        outcome
    }
}

impl Drop for BroadcastRegistry {
    fn drop(&mut self) {
        // Async shutdown is the normal joined path. This backstop also fences publication
        // when a serving caller is canceled or drops its final registry owner unexpectedly.
        self.close_admissions();
        for entry in self.by_name.lock().unwrap().values() {
            entry.abort_async();
        }
    }
}

/// Geometry frozen from a validated transport, including consumer-valid legacy sizes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BroadcastGeometry {
    pub piece_length: u64,
    pub chunk_length: u64,
}
impl BroadcastGeometry {
    pub fn from_transport(
        descriptor: &TransportDescriptor,
        signature_len: usize,
    ) -> Result<Self, BroadcastError> {
        let piece = descriptor.piece_length;
        let chunk = descriptor.chunk_length;
        let signature = u64::try_from(signature_len).map_err(|_| BroadcastError::StateInvalid)?;
        if chunk != CHUNK_LENGTH
            || piece == 0
            || piece > ace_swarm::resolve::MAX_PIECE_LENGTH
            || !piece.is_multiple_of(chunk)
            || u16::try_from(piece / chunk).is_err()
            || signature == 0
            || signature >= piece
            || i64::try_from(piece).is_err()
            || i64::try_from(chunk).is_err()
        {
            return Err(BroadcastError::StateInvalid);
        }
        for length in [piece, chunk, piece - signature] {
            let length = usize::try_from(length).map_err(|_| BroadcastError::StateInvalid)?;
            if length > isize::MAX as usize {
                return Err(BroadcastError::StateInvalid);
            }
        }
        Ok(Self {
            piece_length: piece,
            chunk_length: chunk,
        })
    }
}

fn check_store_capacity(
    geometry: BroadcastGeometry,
    store_bytes: u64,
    cache_type: CacheType,
) -> Result<(), BroadcastError> {
    if store_bytes < geometry.piece_length
        || (cache_type == CacheType::Memory
            && store_bytes > crate::config::safe_in_memory_pool_limit(isize::MAX as u64))
    {
        return Err(BroadcastError::StateInvalid);
    }
    Ok(())
}

struct ValidatedPersistedBroadcast {
    auth: LiveSourceAuth,
    geometry: BroadcastGeometry,
    trackers: Vec<String>,
    start: u64,
}
fn validate_persisted(
    name: &str,
    record: &PersistedBroadcast,
    options: &BroadcastOptions,
) -> Result<ValidatedPersistedBroadcast, BroadcastError> {
    let auth = LiveSourceAuth::from_pkcs1_pem(&record.key_pkcs1_pem)
        .map_err(|_| BroadcastError::StateInvalid)?;
    let decoded = decode_transport(&record.transport).map_err(|_| BroadcastError::StateInvalid)?;
    let geometry = BroadcastGeometry::from_transport(&decoded, auth.signature_len())?;
    if !valid_broadcast_name(name)
        || decoded.pubkey != auth.pubkey_der()
        || !decoded.is_live
        || decoded.raw.get(b"pieces").is_some()
        || decoded
            .raw
            .get(b"name")
            .and_then(Bencode::as_bytes)
            .is_none()
        || decoded.bitrate.is_none_or(|bitrate| bitrate <= 0)
        || decoded.raw.get(b"authmethod").and_then(Bencode::as_bytes) != Some(b"RSA".as_slice())
        || decoded
            .raw
            .get(b"outpace_name")
            .is_some_and(|value| value.as_bytes() != Some(name.as_bytes()))
    {
        return Err(BroadcastError::StateInvalid);
    }
    infohash_of_descriptor(&decoded.raw).map_err(|_| BroadcastError::StateInvalid)?;
    let start = match record.reserved_until {
        Some(reserved) if record.next_piece <= reserved => reserved,
        Some(_) => return Err(BroadcastError::StateInvalid),
        None => record
            .next_piece
            .checked_add(CURSOR_PERSIST_INTERVAL)
            .ok_or(BroadcastError::PieceIndexExhausted)?,
    };
    start
        .checked_add(CURSOR_PERSIST_INTERVAL)
        .ok_or(BroadcastError::PieceIndexExhausted)?;
    if !options.matches_descriptor(&decoded.raw) {
        return Err(BroadcastError::OptionsConflict);
    }
    Ok(ValidatedPersistedBroadcast {
        auth,
        geometry,
        trackers: decoded.trackers,
        start,
    })
}

pub(crate) enum BroadcastStartupError {
    StoreTooSmall,
    Broadcast(BroadcastError),
}
// Boxed startup errors reach Result's Termination through Debug, including
// profile conflicts and capacity failures. Keep that presentation sanitized too.
impl std::fmt::Debug for BroadcastStartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for BroadcastStartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StoreTooSmall => f.write_str("broadcast_store_too_small"),
            Self::Broadcast(error) => f.write_str(error.code()),
        }
    }
}
impl std::error::Error for BroadcastStartupError {}

/// Read-only startup policy: never generates keys, creates stores or advances reservations.
pub(crate) fn preflight_broadcasts(config: &Config) -> Result<(), BroadcastStartupError> {
    let enough = |piece_length| {
        if config.seed_store_bytes < piece_length {
            Err(BroadcastStartupError::StoreTooSmall)
        } else {
            Ok(())
        }
    };
    enough(PIECE_LENGTH)?;
    for (name, options) in &config.broadcast_options {
        let options = options
            .resolve_fresh(name)
            .map_err(|_| BroadcastStartupError::Broadcast(BroadcastError::StateInvalid))?;
        enough(options.piece_length)?;
    }
    let records = BroadcastPersist::new(&config.data_dir)
        .load_all()
        .map_err(|_| BroadcastStartupError::Broadcast(BroadcastError::PersistenceFailed))?;
    for (name, record) in records {
        let options = config
            .broadcast_options
            .get(&name)
            .cloned()
            .unwrap_or_default();
        let validated = record
            .map_err(|_| BroadcastError::StateInvalid)
            .and_then(|record| validate_persisted(&name, &record, &options));
        match validated {
            Ok(validated) => enough(validated.geometry.piece_length)?,
            Err(error)
                if error == BroadcastError::OptionsConflict
                    || config.broadcast_options.contains_key(&name)
                    || config.broadcast_sources.contains_key(&name) =>
            {
                return Err(BroadcastStartupError::Broadcast(error));
            }
            Err(_) => {} // Unrelated malformed records retain their tombstones on actual reload.
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn initialize(
    name: &str,
    options: &BroadcastOptions,
    trackers: &[String],
    seeds: &SeedRegistry,
    store_bytes: u64,
    persist: Option<BroadcastPersist>,
    cache_type: CacheType,
    cache_dir: PathBuf,
    state: &EntryState,
    require_persisted: bool,
) -> Result<(Broadcast, SeedLease), BroadcastError> {
    let loaded = match &persist {
        Some(persist) => persist
            .load(name)
            .map_err(|_| BroadcastError::StateInvalid)?,
        None => None,
    };
    let capacity = |geometry| {
        check_store_capacity(geometry, store_bytes, cache_type).inspect_err(|_| {
            state.capacity_invalid.store(true, Ordering::SeqCst);
        })
    };
    let (auth, transport, key_pkcs1_pem, geometry, trackers, start) = match loaded {
        Some(rec) => {
            let validated = validate_persisted(name, &rec, options)?;
            capacity(validated.geometry)?;
            (
                validated.auth,
                rec.transport,
                rec.key_pkcs1_pem,
                validated.geometry,
                validated.trackers,
                validated.start,
            )
        }
        None => {
            if require_persisted {
                return Err(BroadcastError::StateInvalid);
            }
            let resolved = options
                .resolve_fresh(name)
                .map_err(|_| BroadcastError::StateInvalid)?;
            // Reject a bad direct-call budget before generating an identity or saving anything.
            capacity(BroadcastGeometry {
                piece_length: resolved.piece_length,
                chunk_length: CHUNK_LENGTH,
            })?;
            let auth = LiveSourceAuth::generate();
            let descriptor = build_descriptor(name, &resolved, trackers, &auth.pubkey_der());
            infohash_of_descriptor(&descriptor).map_err(|_| BroadcastError::StateInvalid)?;
            let transport = encode_transport(&descriptor);
            let decoded = decode_transport(&transport).map_err(|_| BroadcastError::StateInvalid)?;
            let geometry = BroadcastGeometry::from_transport(&decoded, auth.signature_len())?;
            let key = auth.to_pkcs1_pem();
            (auth, transport, key, geometry, decoded.trackers, 0)
        }
    };
    let reserved = start
        .checked_add(CURSOR_PERSIST_INTERVAL)
        .ok_or(BroadcastError::PieceIndexExhausted)?;
    if let Some(persist) = &persist {
        persist
            .save(
                name,
                &PersistedBroadcast {
                    transport: transport.clone(),
                    key_pkcs1_pem: key_pkcs1_pem.clone(),
                    next_piece: start,
                    reserved_until: Some(reserved),
                },
            )
            .map_err(|_| BroadcastError::PersistenceFailed)?;
    }
    if state.service_closed.load(Ordering::SeqCst) || *state.cancel.borrow() {
        return Err(BroadcastError::Stopping);
    }
    let infohash = infohash_of_transport(&transport);
    let content_id = transport_file_hash(&transport);
    let (store, lease) = seeds.lease_broadcast(infohash, content_id, transport.clone(), || {
        build_piece_store(
            geometry.piece_length,
            geometry.chunk_length,
            store_bytes,
            None,
            cache_type,
            &cache_dir,
            &infohash,
        )
    });
    let cursor = Arc::new(BroadcastCursor {
        next: AtomicU64::new(start),
        reserved: AtomicU64::new(reserved),
        poisoned: Arc::new(AtomicBool::new(false)),
        closed: AtomicBool::new(false),
        service_closed: state.service_closed.clone(),
        cancel: state.cancel.subscribe(),
        sink: persist.map(|persist| CursorSink {
            persist,
            name: name.to_owned(),
            transport: transport.clone(),
            key_pkcs1_pem,
        }),
        io: state.io.clone(),
        publication: state.publication.clone(),
        io_jobs: StdMutex::new(Vec::new()),
    });
    Ok((
        Broadcast {
            infohash,
            content_id,
            transport_bytes: Arc::new(transport),
            geometry,
            trackers: Arc::new(trackers),
            store,
            auth: Arc::new(auth),
            cursor,
        },
        lease,
    ))
}

/// Build the bencode descriptor dict for a fresh MPEG-TS live broadcast. Field set matches a
/// real engine-produced source-node transport (note 25): `name`, `piece_length`,
/// `chunk_length`, `bitrate`, `quality`, `categories`, `authmethod`, `pubkey`, `trackers`,
/// `allow_public_trackers`. No `pieces` key — live, matching `TransportDescriptor::is_live`.
fn build_descriptor(
    name: &str,
    options: &ResolvedBroadcastOptions,
    trackers: &[String],
    pubkey_der: &[u8],
) -> Bencode {
    let mut d: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
    d.insert(
        b"name".to_vec(),
        Bencode::Bytes(options.title.as_bytes().to_vec()),
    );
    d.insert(
        b"piece_length".to_vec(),
        Bencode::Int(options.piece_length as i64),
    );
    d.insert(b"chunk_length".to_vec(), Bencode::Int(CHUNK_LENGTH as i64));
    d.insert(
        b"bitrate".to_vec(),
        Bencode::Int(options.bitrate_bytes_per_sec as i64),
    );
    d.insert(
        b"quality".to_vec(),
        Bencode::Bytes(options.quality.as_bytes().to_vec()),
    );
    d.insert(
        b"categories".to_vec(),
        Bencode::List(
            options
                .categories
                .iter()
                .map(|s| Bencode::Bytes(s.as_bytes().to_vec()))
                .collect(),
        ),
    );
    d.insert(b"authmethod".to_vec(), Bencode::Bytes(b"RSA".to_vec()));
    d.insert(b"pubkey".to_vec(), Bencode::Bytes(pubkey_der.to_vec()));
    d.insert(
        b"trackers".to_vec(),
        Bencode::List(
            trackers
                .iter()
                .map(|t| Bencode::Bytes(t.as_bytes().to_vec()))
                .collect(),
        ),
    );
    d.insert(b"allow_public_trackers".to_vec(), Bencode::Int(1));
    // `name` above is actually reused for outpace's internal identifier too; keep the
    // ingest path segment discoverable via a dedicated key for operator tooling.
    d.insert(
        b"outpace_name".to_vec(),
        Bencode::Bytes(name.as_bytes().to_vec()),
    );
    Bencode::Dict(d)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> (Arc<BroadcastRegistry>, SeedRegistry) {
        (BroadcastRegistry::new(), SeedRegistry::new())
    }

    fn tmp_dir() -> PathBuf {
        let path = std::env::temp_dir().join(format!("outpace-bc-test-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn minted_broadcast_decodes_to_a_live_descriptor_with_our_pubkey() {
        let options = std::collections::BTreeMap::from([(
            "test".to_owned(),
            crate::broadcast_options::BroadcastOptions {
                title: Some("Test Stream".to_owned()),
                ..Default::default()
            },
        )]);
        let registry = BroadcastRegistry::new_with_options(options).unwrap();
        let seed = SeedRegistry::new();
        let bc = registry
            .mint(
                "test",
                &["udp://t.example:1/announce".into()],
                &seed,
                1 << 20,
            )
            .await
            .unwrap();
        let decoded = decode_transport(&bc.transport_bytes).unwrap();
        assert_eq!(decoded.name, "Test Stream");
        assert_eq!(decoded.piece_length, PIECE_LENGTH);
        assert_eq!(decoded.chunk_length, CHUNK_LENGTH);
        assert_eq!(decoded.bitrate, Some(8375));
        assert!(decoded.is_live);
        assert!((120..=128).contains(&decoded.pubkey.len()));
        assert_eq!(decoded.trackers, vec!["udp://t.example:1/announce"]);
    }

    #[tokio::test]
    async fn minted_descriptor_hash_and_seed_metadata_match() {
        let (registry, seed) = registry();
        let bc = registry
            .mint(
                "channel",
                &["udp://tracker.example:2710/announce".into()],
                &seed,
                1 << 20,
            )
            .await
            .unwrap();
        assert_eq!(bc.infohash, infohash_of_transport(&bc.transport_bytes));
        assert_eq!(bc.content_id, transport_file_hash(&bc.transport_bytes));
        assert_eq!(
            seed.metadata(&bc.content_id).as_deref().map(Vec::as_slice),
            Some(bc.transport_bytes.as_slice())
        );
        assert!(seed.serves(&bc.infohash));
        assert!(seed.serves(&bc.content_id));
    }

    #[tokio::test]
    async fn delete_releases_seed_lease_and_allows_a_fresh_identity() {
        let dir = tmp_dir();
        let seed = SeedRegistry::new();
        let registry = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
        let old = registry.mint("channel", &[], &seed, 1 << 20).await.unwrap();
        assert!(seed.serves(&old.infohash));
        registry.delete("channel").await.unwrap();
        assert!(BroadcastPersist::new(&dir)
            .load("channel")
            .unwrap()
            .is_none());
        assert!(!seed.serves(&old.infohash));
        assert!(!seed.serves(&old.content_id));
        let new = registry.mint("channel", &[], &seed, 1 << 20).await.unwrap();
        assert_ne!(old.infohash, new.infohash);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn persisted_identity_reloads_with_exclusive_reservation() {
        let dir = tmp_dir();
        let seed1 = SeedRegistry::new();
        let first = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new())
            .mint("news", &[], &seed1, 1 << 20)
            .await
            .unwrap();
        let seed2 = SeedRegistry::new();
        let second_registry =
            BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
        let loaded = second_registry
            .reload_persisted(&seed2, 1 << 20, &std::collections::BTreeSet::new())
            .await
            .unwrap();
        let second = loaded
            .into_iter()
            .find(|bc| bc.infohash == first.infohash)
            .unwrap();
        assert_eq!(second.transport_bytes, first.transport_bytes);
        assert_eq!(second.auth.pubkey_der(), first.auth.pubkey_der());
        assert_eq!(second.cursor.start_piece(), CURSOR_PERSIST_INTERVAL);
        assert_eq!(second.cursor.reserved_until(), CURSOR_PERSIST_INTERVAL * 2);
        assert!(seed2.serves(&second.infohash));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn different_names_get_different_infohashes() {
        let (registry, seed) = registry();
        let a = registry.mint("a", &[], &seed, 1 << 20).await.unwrap();
        let b = registry.mint("b", &[], &seed, 1 << 20).await.unwrap();
        assert_ne!(a.infohash, b.infohash);
    }
}

#[cfg(test)]
mod tests_options51;
#[cfg(test)]
mod tests_pull206;
