//! Registry of live sessions keyed by `(network, id)`. One shared session per key; lazy
//! start via the provider; teardown after the last subscriber leaves + grace.

use crate::config::HlsConfig;
use crate::hls::HlsPackager;
use crate::provider::{ProviderError, ProviderRegistry, TsSource, VodContent};
use crate::session::{StreamSession, Subscription};
use ace_swarm::types::{StreamInfo, StreamMetadata};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;

/// A resolved VOD plus its last-access time (for idle expiry), keyed by `(network, id)`.
type VodCacheEntry = (Arc<dyn VodContent>, Instant);
type StreamKey = (String, String);

#[derive(Default)]
struct HlsLifecycleState {
    next_generation: u64,
    pending: HashMap<StreamKey, HashSet<u64>>,
    cancelled: HashSet<(StreamKey, u64)>,
}

impl HlsLifecycleState {
    fn finish(&mut self, key: &StreamKey, generation: u64) -> bool {
        let cancelled = self.cancelled.remove(&(key.clone(), generation));
        if let Some(generations) = self.pending.get_mut(key) {
            generations.remove(&generation);
            if generations.is_empty() {
                self.pending.remove(key);
            }
        }
        cancelled
    }
}

struct HlsTransition {
    state: Arc<StdMutex<HlsLifecycleState>>,
    key: StreamKey,
    generation: u64,
    active: bool,
}

impl HlsTransition {
    fn begin(state: Arc<StdMutex<HlsLifecycleState>>, key: StreamKey) -> Self {
        let generation = {
            let mut lifecycle = state.lock().unwrap();
            lifecycle.next_generation = lifecycle
                .next_generation
                .checked_add(1)
                .expect("HLS transition generation overflow");
            let generation = lifecycle.next_generation;
            lifecycle
                .pending
                .entry(key.clone())
                .or_default()
                .insert(generation);
            generation
        };
        Self {
            state,
            key,
            generation,
            active: true,
        }
    }

    fn is_cancelled(&self) -> bool {
        self.state
            .lock()
            .unwrap()
            .cancelled
            .contains(&(self.key.clone(), self.generation))
    }

    fn finish(&mut self) -> bool {
        let cancelled = self
            .state
            .lock()
            .unwrap()
            .finish(&self.key, self.generation);
        self.active = false;
        cancelled
    }
}

impl Drop for HlsTransition {
    fn drop(&mut self) {
        if self.active {
            self.state
                .lock()
                .unwrap()
                .finish(&self.key, self.generation);
        }
    }
}

/// Readable media never owns the cancellation or join lifetime.
struct HlsEntry {
    media: Arc<HlsPackager>,
    control: WorkerControl,
}

#[derive(Debug)]
enum CleanupJob {
    Join(JoinHandle<()>, watch::Sender<bool>),
    Close,
}

struct CleanupCoordinator {
    sender: tokio::sync::mpsc::UnboundedSender<CleanupJob>,
    // Shared polling keeps the coordinator join owned if a shutdown caller is cancelled.
    join: futures::future::Shared<futures::future::BoxFuture<'static, ()>>,
}

pub(crate) struct WorkerControl {
    cancel: watch::Sender<bool>,
    join: Option<JoinHandle<()>>,
    completed: watch::Receiver<bool>,
    done: Option<watch::Sender<bool>>,
    cleanup: tokio::sync::mpsc::UnboundedSender<CleanupJob>,
}

impl WorkerControl {
    fn cancel(&self) {
        self.cancel.send_replace(true);
    }

    fn transfer(&mut self) {
        if let Some(join) = self.join.take() {
            // The coordinator is runtime-owned and exits only after every sender closes and
            // every transferred join completes. Neither HTTP cancellation nor Drop aborts it.
            self.cleanup
                .send(CleanupJob::Join(join, self.done.take().unwrap()))
                .expect("cleanup coordinator must live while controls exist");
        }
    }

    async fn join(mut self) {
        self.transfer();
        while !*self.completed.borrow_and_update() {
            if self.completed.changed().await.is_err() {
                break;
            }
        }
    }
}

impl Drop for WorkerControl {
    fn drop(&mut self) {
        self.cancel();
        self.transfer();
    }
}

/// Completion is registered synchronously before spawn/publication. A single coordinator per
/// manager owns transferred joins, rather than adding a monitor task to every worker. Draining
/// borrows registered completion receivers, so cancelling its caller never abandons cleanup.
#[derive(Default)]
pub(crate) struct CleanupTracker {
    pending: StdMutex<Vec<watch::Receiver<bool>>>,
    coordinator: StdMutex<Option<CleanupCoordinator>>,
}

impl CleanupTracker {
    fn coordinator(&self) -> tokio::sync::mpsc::UnboundedSender<CleanupJob> {
        use futures::future::BoxFuture;
        use futures::stream::{FuturesUnordered, StreamExt};
        use futures::FutureExt;
        let mut coordinator = self.coordinator.lock().unwrap();
        coordinator
            .get_or_insert_with(|| {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<CleanupJob>();
                let task = tokio::spawn(async move {
                    let mut joins: FuturesUnordered<BoxFuture<'static, ()>> =
                        FuturesUnordered::new();
                    let mut closed = false;
                    loop {
                        tokio::select! {
                            job = rx.recv(), if !closed => match job {
                                Some(CleanupJob::Join(join, done)) => joins.push(Box::pin(async move {
                                    let _ = join.await;
                                    done.send_replace(true);
                                })),
                                Some(CleanupJob::Close) => rx.close(),
                                None => closed = true,
                            },
                            _ = joins.next(), if !joins.is_empty() => {},
                            else => break,
                        }
                    }
                });
                CleanupCoordinator {
                    sender: tx,
                    join: async move { let _ = task.await; }.boxed().shared(),
                }
            })
            .sender.clone()
    }

    pub(crate) fn spawn<F, Fut>(&self, run: F) -> WorkerControl
    where
        F: FnOnce(watch::Receiver<bool>) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let cleanup = self.coordinator();
        let (cancel, rx) = watch::channel(false);
        let (done, completed) = watch::channel(false);
        {
            let mut pending = self.pending.lock().unwrap();
            pending.retain(|rx| !*rx.borrow());
            pending.push(completed.clone());
        }
        let join = Some(tokio::spawn(run(rx)));
        WorkerControl {
            cancel,
            join,
            done: Some(done),
            completed,
            cleanup,
        }
    }

    pub(crate) async fn drain(&self) {
        loop {
            let pending = self.pending.lock().unwrap().clone();
            for mut done in pending {
                while !*done.borrow_and_update() {
                    if done.changed().await.is_err() {
                        break;
                    }
                }
            }
            let mut pending = self.pending.lock().unwrap();
            pending.retain(|done| !*done.borrow() && done.has_changed().is_ok());
            if pending.is_empty() {
                break;
            }
        }
        let join = self
            .coordinator
            .lock()
            .unwrap()
            .as_ref()
            .map(|coordinator| {
                let _ = coordinator.sender.send(CleanupJob::Close);
                coordinator.join.clone()
            });
        if let Some(join) = join {
            join.await;
        }
    }
}

pub struct StreamManager {
    registry: ProviderRegistry,
    sessions: Mutex<HashMap<(String, String), Arc<StreamSession>>>,
    packagers: Mutex<HashMap<(String, String), HlsEntry>>,
    /// Synchronous pending/cancellation bookkeeping so a dropped future cleans up in `Drop`.
    hls_transitions: Arc<StdMutex<HlsLifecycleState>>,
    /// Serializes only short HLS publication/reap/stop map operations, never provider work.
    hls_finalization: Mutex<()>,
    /// Weak per-key session-start mutexes shared by direct TS and HLS paths. Different keys open
    /// concurrently; same-key callers perform exactly one provider open.
    session_start_locks: StdMutex<HashMap<StreamKey, Weak<Mutex<()>>>>,
    /// Resolved single-file VODs, shared across a playback's many range reads (the HLS manifest
    /// plus every segment). Each entry keeps its provider-resolved handle (and, for the ace
    /// provider, its bounded piece cache + discovered peers) alive so segments resolve the
    /// transport descriptor once and reuse downloaded pieces instead of re-fetching per request.
    /// Entries expire after `grace` of no access. Value: (handle, last-access time).
    vod_cache: Mutex<HashMap<(String, String), VodCacheEntry>>,
    /// Serializes VOD *resolution* so concurrent first reads for the same id (a manifest fetch
    /// racing an eager segment fetch) trigger exactly one `provider.resolve_vod()`.
    vod_resolve_lock: Mutex<()>,
    buffer: usize,
    hls: HlsConfig,
    normalizer: Option<crate::hls_audio::PreparedNormalizer>,
    grace: Duration,
    cleanup: CleanupTracker,
    reaper: StdMutex<Option<WorkerControl>>,
    shutting_down: std::sync::atomic::AtomicBool,
}

impl StreamManager {
    pub fn new(registry: ProviderRegistry) -> Arc<StreamManager> {
        Self::with_buffer(registry, 256)
    }

    pub fn with_buffer(registry: ProviderRegistry, buffer: usize) -> Arc<StreamManager> {
        Self::with_config(registry, buffer, HlsConfig::default())
    }

    pub fn with_config(
        registry: ProviderRegistry,
        buffer: usize,
        hls: HlsConfig,
    ) -> Arc<StreamManager> {
        Self::with_normalizer(registry, buffer, hls, None)
    }

    pub fn with_normalizer(
        registry: ProviderRegistry,
        buffer: usize,
        hls: HlsConfig,
        normalizer: Option<crate::hls_audio::PreparedNormalizer>,
    ) -> Arc<StreamManager> {
        Arc::new(StreamManager {
            registry,
            sessions: Mutex::new(HashMap::new()),
            packagers: Mutex::new(HashMap::new()),
            hls_transitions: Arc::new(StdMutex::new(HlsLifecycleState::default())),
            hls_finalization: Mutex::new(()),
            session_start_locks: StdMutex::new(HashMap::new()),
            vod_cache: Mutex::new(HashMap::new()),
            vod_resolve_lock: Mutex::new(()),
            buffer,
            hls,
            normalizer,
            grace: Duration::from_secs(30),
            cleanup: CleanupTracker::default(),
            reaper: StdMutex::new(None),
            shutting_down: std::sync::atomic::AtomicBool::new(false),
        })
    }

    #[cfg(test)]
    pub(crate) fn buffer(&self) -> usize {
        self.buffer
    }

    pub(crate) fn hls_config(&self) -> HlsConfig {
        self.hls
    }

    pub(crate) fn hls_startup_timeout(&self) -> Duration {
        Duration::from_millis(self.hls.startup_timeout_ms)
    }

    /// Offline pre-check that `id` can be opened on `network`; see
    /// [`StreamProvider::check_openable`](crate::provider::StreamProvider::check_openable).
    /// `NotFound` if the network is unregistered.
    pub fn check_openable(&self, network: &str, id: &str) -> Result<(), ProviderError> {
        self.registry
            .get(network)
            .ok_or(ProviderError::NotFound)?
            .check_openable(id)
    }

    /// Hand a live descriptor resolved outside `open` to `network`'s provider; see
    /// [`StreamProvider::remember_live_descriptor`](crate::provider::StreamProvider::remember_live_descriptor).
    /// Callers must pass only descriptors resolved through the signed catalog. A no-op for an
    /// unregistered network.
    pub fn remember_live_descriptor(&self, network: &str, info: &StreamInfo) {
        if let Some(provider) = self.registry.get(network) {
            provider.remember_live_descriptor(info);
        }
    }

    /// Resolve `id` to a single-file VOD, caching the resolved handle per `(network, id)` so a
    /// whole playback — the HLS manifest plus every segment read — shares one resolution (and, for
    /// the ace provider, one piece cache + one peer discovery) instead of re-resolving per request.
    /// Returns `NotFound` for an unregistered network.
    pub async fn resolve_vod(
        &self,
        network: &str,
        id: &str,
    ) -> Result<Arc<dyn VodContent>, ProviderError> {
        let key = (network.to_string(), id.to_string());
        if let Some(v) = self.cached_vod(&key).await {
            return Ok(v);
        }
        // Serialize resolution and re-check: a concurrent request may have resolved this id while
        // we waited, so we must not resolve (and discover) a second time.
        let _resolving = self.vod_resolve_lock.lock().await;
        if let Some(v) = self.cached_vod(&key).await {
            return Ok(v);
        }
        let provider = self.registry.get(network).ok_or(ProviderError::NotFound)?;
        let vod: Arc<dyn VodContent> = Arc::from(provider.resolve_vod(id).await?);
        self.vod_cache
            .lock()
            .await
            .insert(key, (vod.clone(), Instant::now()));
        Ok(vod)
    }

    /// A live (non-expired) cached VOD for `key`, refreshing its access time. Sweeps entries idle
    /// longer than `grace` on the way, so the cache is bounded to VODs read within the last `grace`.
    async fn cached_vod(&self, key: &(String, String)) -> Option<Arc<dyn VodContent>> {
        let mut map = self.vod_cache.lock().await;
        let now = Instant::now();
        map.retain(|_, (_, at)| now.duration_since(*at) < self.grace);
        map.get_mut(key).map(|(v, at)| {
            *at = now;
            v.clone()
        })
    }

    fn session_start_lock(&self, key: &StreamKey) -> Arc<Mutex<()>> {
        let mut locks = self.session_start_locks.lock().unwrap();
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(key).and_then(Weak::upgrade) {
            return lock;
        }

        let lock = Arc::new(Mutex::new(()));
        locks.insert(key.clone(), Arc::downgrade(&lock));
        lock
    }

    /// Get the running session for `(network, id)` or start one via the provider. Returns
    /// `NotFound` if the network is unregistered.
    pub async fn get_or_start(
        self: &Arc<Self>,
        network: &str,
        id: &str,
    ) -> Result<Arc<StreamSession>, ProviderError> {
        let key = (network.to_string(), id.to_string());
        {
            let map = self.sessions.lock().await;
            if let Some(s) = map.get(&key) {
                return Ok(s.clone());
            }
        }
        // Serialize same-key starts and re-check: direct and HLS callers share this lock, while
        // unrelated provider opens remain concurrent.
        let start_lock = self.session_start_lock(&key);
        let _starting = start_lock.lock().await;
        {
            let map = self.sessions.lock().await;
            if let Some(s) = map.get(&key) {
                return Ok(s.clone());
            }
        }
        let provider = self.registry.get(network).ok_or(ProviderError::NotFound)?;
        let source = provider.open(id).await?;
        let session = StreamSession::start(source, self.buffer);
        let mut map = self.sessions.lock().await;
        Ok(map.entry(key).or_insert(session).clone())
    }

    /// Peek at an already-running session without starting one.
    pub async fn get(&self, network: &str, id: &str) -> Option<Arc<StreamSession>> {
        self.sessions
            .lock()
            .await
            .get(&(network.to_string(), id.to_string()))
            .cloned()
    }

    /// Get (or lazily start) a native HLS packager, recording native demand before it is returned.
    pub async fn get_or_start_hls(
        self: &Arc<Self>,
        network: &str,
        id: &str,
    ) -> Result<Arc<HlsPackager>, ProviderError> {
        let (pkg, _session, _transition_pin) =
            self.get_or_start_pinned_hls(network, id, true).await?;
        Ok(pkg)
    }

    /// Get (or lazily start) a compatibility HLS packager without recording native activity.
    /// The returned subscription pins the session across creation and must be transferred
    /// directly to the compatibility lease owner.
    pub async fn get_or_start_compatibility_hls(
        self: &Arc<Self>,
        network: &str,
        id: &str,
    ) -> Result<(Arc<HlsPackager>, Arc<StreamSession>, Subscription), ProviderError> {
        self.get_or_start_pinned_hls(network, id, false).await
    }

    async fn get_or_start_pinned_hls(
        self: &Arc<Self>,
        network: &str,
        id: &str,
        native: bool,
    ) -> Result<(Arc<HlsPackager>, Arc<StreamSession>, Subscription), ProviderError> {
        let key = (network.to_string(), id.to_string());
        if self
            .shutting_down
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(ProviderError::NotFound);
        }
        let mut transition = HlsTransition::begin(self.hls_transitions.clone(), key.clone());
        let start_lock = self.session_start_lock(&key);

        // Serialize only starts for this key. Unrelated provider opens, reap, and stop proceed.
        let _starting = start_lock.lock().await;
        if transition.is_cancelled() {
            return Err(ProviderError::NotFound);
        }

        let existing_session = self.sessions.lock().await.get(&key).cloned();
        let mut source: Option<Box<dyn TsSource>> = if existing_session.is_none() {
            let provider = self.registry.get(network).ok_or(ProviderError::NotFound)?;
            Some(provider.open(id).await?)
        } else {
            None
        };

        // Finalization is atomic with reap/stop, but contains no provider or other long I/O.
        let _finalizing = self.hls_finalization.lock().await;
        if transition.finish()
            || self
                .shutting_down
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(ProviderError::NotFound);
        }

        let session = {
            let mut sessions = self.sessions.lock().await;
            if let Some(session) = sessions.get(&key) {
                session.clone()
            } else {
                let session = StreamSession::start(
                    source
                        .take()
                        .expect("an absent HLS session must have an opened source"),
                    self.buffer,
                );
                sessions.insert(key.clone(), session.clone());
                session
            }
        };
        let transition_pin = session.subscribe();
        let pkg = {
            let mut packagers = self.packagers.lock().await;
            packagers
                .entry(key)
                .or_insert_with(|| {
                    let media = HlsPackager::create(&session, self.hls, self.normalizer.is_some());
                    let control = crate::hls_audio::spawn_worker(
                        &session,
                        media.clone(),
                        self.hls,
                        self.normalizer.clone(),
                        &self.cleanup,
                    );
                    HlsEntry { media, control }
                })
                .media
                .clone()
        };
        if native {
            pkg.record_native_access();
        }
        Ok((pkg, session, transition_pin))
    }

    /// Peek at an already-running HLS packager without starting a session or packager.
    pub async fn get_hls(&self, network: &str, id: &str) -> Option<Arc<HlsPackager>> {
        self.packagers
            .lock()
            .await
            .get(&(network.to_string(), id.to_string()))
            .map(|entry| entry.media.clone())
    }

    /// Force-stop a session: remove it (and any HLS packager) so the shared download is torn
    /// down — the session's `Drop` aborts its background pull task. Returns `true` if a session
    /// for `(network, id)` existed. Connected clients see the stream end.
    pub async fn stop(&self, network: &str, id: &str) -> bool {
        let key = (network.to_string(), id.to_string());
        let (entry, existed) = {
            let _finalizing = self.hls_finalization.lock().await;
            {
                let mut lifecycle = self.hls_transitions.lock().unwrap();
                let pending = lifecycle
                    .pending
                    .get(&key)
                    .into_iter()
                    .flatten()
                    .copied()
                    .collect::<Vec<_>>();
                for generation in pending {
                    lifecycle.cancelled.insert((key.clone(), generation));
                }
            }
            let entry = self.packagers.lock().await.remove(&key);
            if let Some(entry) = &entry {
                entry.media.stop();
                entry.control.cancel();
            }
            let existed = self.sessions.lock().await.remove(&key).is_some();
            (entry, existed)
        };
        if let Some(entry) = entry {
            entry.control.join().await;
        }
        existed
    }

    /// Active sessions as `(network, id, subscriber_count, metadata)`.
    pub async fn list(&self) -> Vec<(String, String, u64, StreamMetadata)> {
        self.sessions
            .lock()
            .await
            .iter()
            .map(|((n, i), s)| {
                (
                    n.clone(),
                    i.clone(),
                    s.subscriber_count(),
                    s.metadata().clone(),
                )
            })
            .collect()
    }

    async fn reap_idle_at(&self, now: Instant) {
        let controls = {
            let _finalizing = self.hls_finalization.lock().await;
            let pending_hls: HashSet<_> = self
                .hls_transitions
                .lock()
                .unwrap()
                .pending
                .keys()
                .cloned()
                .collect();
            let active_hls: HashSet<_> = {
                let packagers = self.packagers.lock().await;
                packagers
                    .iter()
                    .filter(|(_, pkg)| pkg.media.was_accessed_within(now, self.grace))
                    .map(|(key, _)| key.clone())
                    .collect()
            };

            let retained_sessions: HashSet<_> = {
                let mut sessions = self.sessions.lock().await;
                sessions.retain(|key, session| {
                    session.subscriber_count() > 0
                        || active_hls.contains(key)
                        || pending_hls.contains(key)
                });
                sessions.keys().cloned().collect()
            };

            let mut entries = self.packagers.lock().await;
            let removed: Vec<_> = entries
                .keys()
                .filter(|key| !retained_sessions.contains(*key))
                .cloned()
                .collect();
            let mut controls = Vec::new();
            for key in removed {
                if let Some(entry) = entries.remove(&key) {
                    entry.media.stop();
                    entry.control.cancel();
                    controls.push(entry.control);
                }
            }
            controls
        };
        for control in controls {
            control.join().await;
        }
    }

    /// The reaper holds only a Weak manager, including while sleeping.
    pub fn spawn_reaper(self: &Arc<Self>) {
        let mut reaper = self.reaper.lock().unwrap();
        if reaper.is_some()
            || self
                .shutting_down
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }
        let manager = Arc::downgrade(self);
        let grace = self.grace;
        *reaper = Some(self.cleanup.spawn(move |mut cancel| async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancel.changed() => break,
                    _ = tokio::time::sleep(grace) => {}
                }
                let Some(manager) = manager.upgrade() else {
                    break;
                };
                manager.reap_idle_at(Instant::now()).await;
            }
        }));
    }

    /// Fence publication and transfer cancellation under locks; join outside every lifecycle lock.
    pub async fn shutdown(&self) {
        let mut controls = Vec::new();
        {
            let _finalizing = self.hls_finalization.lock().await;
            self.shutting_down
                .store(true, std::sync::atomic::Ordering::Release);
            if let Some(reaper) = self.reaper.lock().unwrap().take() {
                reaper.cancel();
                controls.push(reaper);
            }
            let mut entries = self.packagers.lock().await;
            for entry in entries.values() {
                entry.media.stop();
                entry.control.cancel();
            }
            controls.extend(entries.drain().map(|(_, entry)| entry.control));
            self.sessions.lock().await.clear();
        }
        for control in controls {
            control.join().await;
        }
        self.cleanup.drain().await;
    }
}

impl Drop for StreamManager {
    fn drop(&mut self) {
        if let Some(reaper) = self.reaper.get_mut().unwrap().as_ref() {
            reaper.cancel();
        }
        for entry in self.packagers.get_mut().values() {
            entry.media.stop();
            entry.control.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{SourceStats, StreamProvider, TsSource};
    use crate::testprovider::TestProvider;
    use async_trait::async_trait;
    use tokio::sync::Notify;

    struct IdleSource;

    #[async_trait]
    impl TsSource for IdleSource {
        async fn next(&mut self) -> Option<bytes::Bytes> {
            std::future::pending().await
        }

        fn stats(&self) -> SourceStats {
            SourceStats::default()
        }
    }

    struct BlockingProvider {
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl StreamProvider for BlockingProvider {
        fn network(&self) -> &'static str {
            "blocking"
        }

        async fn open(&self, id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
            if id == "fast" {
                return Ok(Box::new(IdleSource));
            }
            self.started.notify_one();
            self.release.notified().await;
            Ok(Box::new(IdleSource))
        }
    }

    fn blocking_manager() -> (Arc<StreamManager>, Arc<Notify>, Arc<Notify>) {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(BlockingProvider {
            started: started.clone(),
            release: release.clone(),
        }));
        (StreamManager::new(registry), started, release)
    }

    fn registry() -> ProviderRegistry {
        let mut r = ProviderRegistry::new();
        r.register(Arc::new(TestProvider { chunks: 1000 }));
        r
    }

    struct FeedSource {
        rx: tokio::sync::mpsc::UnboundedReceiver<(bool, bytes::Bytes)>,
        discontinuity: bool,
    }

    #[async_trait]
    impl TsSource for FeedSource {
        async fn next(&mut self) -> Option<bytes::Bytes> {
            let (discontinuity, bytes) = self.rx.recv().await?;
            self.discontinuity = discontinuity;
            Some(bytes)
        }
        fn take_discontinuity(&mut self) -> bool {
            std::mem::take(&mut self.discontinuity)
        }
        fn stats(&self) -> SourceStats {
            SourceStats::default()
        }
    }

    struct FeedProvider(
        StdMutex<Option<tokio::sync::mpsc::UnboundedReceiver<(bool, bytes::Bytes)>>>,
    );

    #[async_trait]
    impl StreamProvider for FeedProvider {
        fn network(&self) -> &'static str {
            "normalization-test"
        }
        async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
            Ok(Box::new(FeedSource {
                rx: self.0.lock().unwrap().take().expect("one source open"),
                discontinuity: false,
            }))
        }
    }

    fn task4_input(video_only: bool) -> Vec<u8> {
        let fixture = crate::hls_audio::process::test_fixtures::transport_prefix();
        let mut input = fixture[..2 * 188].to_vec();
        if video_only {
            let mut pmt = vec![
                0x02, 0xb0, 0x00, 0x00, 0x09, 0xc1, 0, 0, 0xe1, 0x00, 0xf0, 0, 0x1b, 0xe1, 0x00,
                0xf0, 0,
            ];
            let section_length = pmt.len() - 3 + 4;
            pmt[1] = 0xb0 | ((section_length >> 8) as u8 & 0x0f);
            pmt[2] = section_length as u8;
            let crc = ace_media::mpegts::mpeg_crc32(&pmt);
            pmt.extend_from_slice(&crc.to_be_bytes());
            input[188..376].copy_from_slice(&crate::hls_audio::process::test_fixtures::psi_packet(
                4096, 2, &pmt,
            ));
        }
        input
            .extend_from_slice(&crate::hls_audio::process::test_fixtures::video_access_packet(256));
        for pcr in [0, 108_000] {
            input.extend_from_slice(&crate::hls_audio::process::test_fixtures::pcr_packet(
                256, pcr,
            ));
        }
        input
    }

    fn task4_mpeg_audio_input() -> Vec<u8> {
        let mut input = crate::hls_audio::process::test_fixtures::transport_prefix();
        let section =
            crate::hls_audio::process::test_fixtures::pmt_section(9, 256, 256, 0x1b, 257, 0x03);
        input[188..376].copy_from_slice(&crate::hls_audio::process::test_fixtures::psi_packet(
            4096, 2, &section,
        ));
        for pcr in [0, 108_000, 216_000] {
            input.extend_from_slice(&crate::hls_audio::process::test_fixtures::pcr_packet(
                256, pcr,
            ));
        }
        input
    }

    #[tokio::test]
    async fn hls_audio_native_aac_and_video_only_skip_child_even_with_invalid_prepared_path() {
        let _serial = crate::hls_audio::process::test_support::process_test_lock().await;
        for (id, video_only) in [("native-aac", false), ("video-only", true)] {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let mut providers = ProviderRegistry::new();
            providers.register(Arc::new(FeedProvider(StdMutex::new(Some(rx)))));
            let hls = HlsConfig {
                segment_packets: 128,
                window_segments: 4,
                segment_duration_ms: 1000,
                startup_segments: 1,
                startup_timeout_ms: 5000,
            };
            let manager = StreamManager::with_normalizer(
                providers,
                32,
                hls,
                Some(crate::hls_audio::PreparedNormalizer {
                    executable: std::path::PathBuf::from("__missing_task4_ffmpeg_binary__"),
                }),
            );
            let media = manager
                .get_or_start_hls("normalization-test", id)
                .await
                .unwrap();
            tx.send((false, bytes::Bytes::from(task4_input(video_only))))
                .unwrap();
            let status = tokio::time::timeout(
                Duration::from_secs(4),
                media.wait_available(Duration::from_secs(4)),
            )
            .await
            .expect("native source should become ready without invoking FFmpeg");
            assert_eq!(status, crate::hls::HlsStatus::Ready, "case {id}");
            manager.shutdown().await;
        }
    }

    #[tokio::test]
    async fn hls_audio_manager_source_recovery_retains_old_segment_and_emits_one_new_boundary() {
        let _serial = crate::hls_audio::process::test_support::process_test_lock().await;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut providers = ProviderRegistry::new();
        providers.register(Arc::new(FeedProvider(StdMutex::new(Some(rx)))));
        let manager = StreamManager::with_normalizer(
            providers,
            32,
            HlsConfig {
                segment_packets: 128,
                window_segments: 4,
                segment_duration_ms: 1000,
                startup_segments: 1,
                startup_timeout_ms: 5000,
            },
            Some(crate::hls_audio::PreparedNormalizer {
                executable: std::path::PathBuf::from("__missing_task4_ffmpeg_binary__"),
            }),
        );
        let media = manager
            .get_or_start_hls("normalization-test", "recovering")
            .await
            .unwrap();
        tx.send((false, bytes::Bytes::from(task4_input(false))))
            .unwrap();
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(4),
                media.wait_available(Duration::from_secs(4))
            )
            .await
            .unwrap(),
            crate::hls::HlsStatus::Ready,
            "first native segment is available",
        );
        let old = media.segment(0).expect("segment retained before recovery");

        let mut marker = [0xff; 188];
        marker[..6].copy_from_slice(&[0x47, 0x40, 0x11, 0x30, 1, 0x80]);
        tx.send((true, bytes::Bytes::copy_from_slice(&marker)))
            .unwrap(); // paired marker on filtered SDT PID before opening
        tokio::time::timeout(Duration::from_secs(2), async {
            while media.status() != crate::hls::HlsStatus::Recovering {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("source discontinuity reaches the worker before resumed packets");
        tx.send((false, bytes::Bytes::from(task4_input(false))))
            .unwrap();

        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(4),
                media.wait_available(Duration::from_secs(4))
            )
            .await
            .unwrap(),
            crate::hls::HlsStatus::Ready,
            "recovered generation becomes ready",
        );
        assert_eq!(
            media.segment(0).as_deref(),
            Some(old.as_ref()),
            "old retained sequence remains byte-identical"
        );
        let new = media.segment(1).expect("new generation segment");
        assert_eq!(
            ace_media::mpegts::ts_pid(&new[..188]),
            0,
            "new segment starts with fresh PAT"
        );
        assert_eq!(
            ace_media::mpegts::ts_pid(&new[188..376]),
            4096,
            "new segment follows with fresh PMT"
        );
        let playlist = media.playlist_with_segment_prefix("/segments");
        assert_eq!(playlist.matches("#EXT-X-DISCONTINUITY\n").count(), 1);
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn hls_audio_enabled_worker_ignores_version_repeat_then_recovers_semantic_layout_change()
    {
        let _serial = crate::hls_audio::process::test_support::process_test_lock().await;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut providers = ProviderRegistry::new();
        providers.register(Arc::new(FeedProvider(StdMutex::new(Some(rx)))));
        let manager = StreamManager::with_normalizer(
            providers,
            32,
            HlsConfig {
                segment_packets: 128,
                window_segments: 4,
                segment_duration_ms: 1000,
                startup_segments: 1,
                startup_timeout_ms: 5000,
            },
            Some(crate::hls_audio::PreparedNormalizer {
                executable: std::path::PathBuf::from("__missing_task4_ffmpeg_binary__"),
            }),
        );
        let media = manager
            .get_or_start_hls("normalization-test", "layout-worker")
            .await
            .unwrap();
        tx.send((false, bytes::Bytes::from(task4_input(false))))
            .unwrap();
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(3),
                media.wait_available(Duration::from_secs(3))
            )
            .await
            .unwrap(),
            crate::hls::HlsStatus::Ready,
        );

        let mut version_repeat = task4_input(false);
        version_repeat[188 + 10] = 0xc3; // PMT version 1; selected streams unchanged.
        let pmt_packet: &mut [u8; 188] = (&mut version_repeat[188..376]).try_into().unwrap();
        crate::hls_audio::process::test_fixtures::set_psi_crc(pmt_packet);
        tx.send((false, bytes::Bytes::from(version_repeat)))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(
            media.status(),
            crate::hls::HlsStatus::Ready,
            "version-only PAT repeat must not invalidate the active worker generation"
        );

        let mut semantic_change = crate::hls_audio::process::test_fixtures::transport_prefix();
        let changed_pmt =
            crate::hls_audio::process::test_fixtures::pmt_section(9, 256, 256, 0x1b, 258, 0x0f);
        semantic_change[188..376].copy_from_slice(
            &crate::hls_audio::process::test_fixtures::psi_packet(4096, 2, &changed_pmt),
        );
        tx.send((false, bytes::Bytes::from(semantic_change)))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while media.status() != crate::hls::HlsStatus::Recovering {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("semantic selected-audio PID change invalidates the active generation");
        assert_eq!(media.status(), crate::hls::HlsStatus::Recovering);
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn hls_audio_post_opening_filtered_pid_marker_is_independent_after_paired_source_recovery(
    ) {
        let _serial = crate::hls_audio::process::test_support::process_test_lock().await;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut providers = ProviderRegistry::new();
        providers.register(Arc::new(FeedProvider(StdMutex::new(Some(rx)))));
        let manager = StreamManager::with_normalizer(
            providers,
            32,
            HlsConfig {
                segment_packets: 128,
                window_segments: 4,
                segment_duration_ms: 1000,
                startup_segments: 1,
                startup_timeout_ms: 5000,
            },
            Some(crate::hls_audio::PreparedNormalizer {
                executable: std::path::PathBuf::from("__missing_task4_ffmpeg_binary__"),
            }),
        );
        let media = manager
            .get_or_start_hls("normalization-test", "post-opening-marker")
            .await
            .unwrap();
        tx.send((false, bytes::Bytes::from(task4_input(false))))
            .unwrap();
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(3),
                media.wait_available(Duration::from_secs(3))
            )
            .await
            .unwrap(),
            crate::hls::HlsStatus::Ready,
        );
        let old = media.segment(0).expect("first generation segment");

        let marker = filtered_sdt_discontinuity_packet();
        tx.send((true, bytes::Bytes::copy_from_slice(&marker)))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while media.status() != crate::hls::HlsStatus::Recovering {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("paired source event begins one replacement generation");
        tx.send((false, bytes::Bytes::from(task4_input(false))))
            .unwrap();
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(3),
                media.wait_available(Duration::from_secs(3))
            )
            .await
            .unwrap(),
            crate::hls::HlsStatus::Ready,
        );
        assert_eq!(media.segment(0).as_deref(), Some(old.as_ref()));

        // The paired marker token is consumed at the accepted opening point.
        // A later marker on filtered SDT PID 0x11 must independently invalidate.
        let marker = filtered_sdt_discontinuity_packet();
        tx.send((false, bytes::Bytes::copy_from_slice(&marker)))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while media.status() != crate::hls::HlsStatus::Recovering {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("post-opening filtered-PID marker is independent");
        tx.send((false, bytes::Bytes::from(task4_input(false))))
            .unwrap();
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(3),
                media.wait_available(Duration::from_secs(3))
            )
            .await
            .unwrap(),
            crate::hls::HlsStatus::Ready,
        );
        assert_eq!(media.segment(0).as_deref(), Some(old.as_ref()));
        assert!(
            media.segment(2).is_some(),
            "independent post-opening recovery publishes a third generation segment"
        );
        assert_eq!(
            media
                .playlist_with_segment_prefix("/segments")
                .matches("#EXT-X-DISCONTINUITY\n")
                .count(),
            2
        );
        manager.shutdown().await;
    }

    fn filtered_sdt_discontinuity_packet() -> [u8; 188] {
        let mut marker = [0xff; 188];
        marker[..6].copy_from_slice(&[0x47, 0x40, 0x11, 0x30, 1, 0x80]);
        marker
    }

    #[tokio::test]
    async fn hls_audio_transform_recovery_limit_fences_before_fourth_child_reap() {
        let _serial = crate::hls_audio::process::test_support::process_test_lock().await;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut providers = ProviderRegistry::new();
        providers.register(Arc::new(FeedProvider(StdMutex::new(Some(rx)))));
        let (prepared, counters) = crate::hls_audio::process::test_support::fake_prepared_child(
            crate::hls_audio::process::test_support::ChildMode::ReadyThenStall,
        );
        let manager = StreamManager::with_normalizer(
            providers,
            32,
            HlsConfig {
                segment_packets: 128,
                window_segments: 4,
                segment_duration_ms: 1000,
                startup_segments: 1,
                startup_timeout_ms: 5000,
            },
            Some(prepared),
        );
        let media = manager
            .get_or_start_hls("normalization-test", "live-transform-recovery-limit")
            .await
            .unwrap();
        let input = bytes::Bytes::from(task4_mpeg_audio_input());
        tx.send((false, input.clone())).unwrap();
        let wait_ready = || async {
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if media.status() == crate::hls::HlsStatus::Ready {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("fake transform generation becomes Ready");
        };
        wait_ready().await;
        assert_eq!(counters.snapshot().children_started, 1);

        for generation_child in 2..=4 {
            tx.send((true, bytes::Bytes::new())).unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                while counters.snapshot().children_reaped < generation_child - 1 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("previous transform child is reaped before replacement");
            assert_eq!(counters.snapshot().children_started, generation_child - 1);
            tx.send((false, input.clone())).unwrap();
            wait_ready().await;
            assert_eq!(counters.snapshot().children_started, generation_child);
        }

        // This is the fourth invalidation (the initial generation is not counted).
        // The fourth child is held in ReadyThenStall while Failed must publish before
        // its cancellation/reap completes.
        tx.send((true, bytes::Bytes::new())).unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let snapshot = counters.snapshot();
                if media.status()
                    == crate::hls::HlsStatus::Failed(crate::hls::HlsFailure::RecoveryLimit)
                    && snapshot.children_started == 4
                    && snapshot.children_reaped == 3
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("fourth recovery fences Ready media before old child reap");
        manager.shutdown().await;
        let snapshot = counters.snapshot();
        assert_eq!(snapshot.children_started, 4);
        assert_eq!(snapshot.children_reaped, 4);
        assert_eq!(snapshot.pumps_started, snapshot.pumps_joined);
        assert_eq!(snapshot.active_tasks, 0);
    }

    #[tokio::test]
    async fn hls_audio_manager_terminal_transform_failure_is_not_restarted_by_retry() {
        let _serial = crate::hls_audio::process::test_support::process_test_lock().await;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut providers = ProviderRegistry::new();
        providers.register(Arc::new(FeedProvider(StdMutex::new(Some(rx)))));
        let manager = StreamManager::with_normalizer(
            providers,
            32,
            HlsConfig {
                startup_timeout_ms: 5000,
                ..HlsConfig::default()
            },
            Some(crate::hls_audio::PreparedNormalizer {
                executable: std::path::PathBuf::from("__missing_task4_ffmpeg_binary__"),
            }),
        );
        let media = manager
            .get_or_start_hls("normalization-test", "transform")
            .await
            .unwrap();
        tx.send((false, bytes::Bytes::from(task4_mpeg_audio_input())))
            .unwrap();
        let status = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let status = media.status();
                if matches!(status, crate::hls::HlsStatus::Failed(_)) {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("invalid transform executable should fail terminally");
        assert!(matches!(status, crate::hls::HlsStatus::Failed(_)));
        let retry = manager
            .get_or_start_hls("normalization-test", "transform")
            .await
            .unwrap();
        assert!(
            Arc::ptr_eq(&media, &retry),
            "retry shares the terminal manager entry"
        );
        assert_eq!(
            retry.status(),
            status,
            "an HTTP-style retry cannot restart a failed worker"
        );
        manager.shutdown().await;
    }

    async fn manager_with_blocking_worker_for_test(
    ) -> (Arc<StreamManager>, Arc<Notify>, Arc<Notify>) {
        let manager = StreamManager::new(registry());
        manager
            .get_or_start_hls("test", "slow-cleanup")
            .await
            .unwrap();
        let cleanup_started = Arc::new(Notify::new());
        let release_cleanup = Arc::new(Notify::new());
        let started = cleanup_started.clone();
        let release = release_cleanup.clone();
        let control = manager.cleanup.spawn(move |mut cancel| async move {
            while cancel.changed().await.is_ok() && !*cancel.borrow() {}
            started.notify_one();
            release.notified().await;
        });
        let media = manager.get_hls("test", "slow-cleanup").await.unwrap();
        let old = manager.packagers.lock().await.insert(
            ("test".into(), "slow-cleanup".into()),
            HlsEntry { media, control },
        );
        drop(old);
        (manager, cleanup_started, release_cleanup)
    }

    #[test]
    fn with_buffer_sets_the_fanout_depth() {
        let reg = ProviderRegistry::new();
        let mgr = StreamManager::with_buffer(reg, 4);
        assert_eq!(mgr.buffer(), 4);
    }

    #[test]
    fn with_config_sets_buffer_and_hls_config() {
        let reg = ProviderRegistry::new();
        let hls = HlsConfig {
            segment_packets: 64,
            window_segments: 4,
            segment_duration_ms: 1500,
            ..HlsConfig::default()
        };
        let mgr = StreamManager::with_config(reg, 8, hls);

        assert_eq!(mgr.buffer(), 8);
        assert_eq!(mgr.hls_config(), hls);
        assert_eq!(
            mgr.hls_startup_timeout(),
            Duration::from_millis(hls.startup_timeout_ms)
        );
    }

    #[tokio::test]
    async fn same_key_returns_same_session() {
        let m = StreamManager::new(registry());
        let s1 = m.get_or_start("test", "abc").await.unwrap();
        let s2 = m.get_or_start("test", "abc").await.unwrap();
        assert!(Arc::ptr_eq(&s1, &s2));
        let s3 = m.get_or_start("test", "different").await.unwrap();
        assert!(!Arc::ptr_eq(&s1, &s3));
    }

    #[tokio::test]
    async fn concurrent_first_requests_start_the_session_once() {
        // A provider that counts how many times `open` is called.
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingProvider(Arc<AtomicUsize>);
        #[async_trait]
        impl StreamProvider for CountingProvider {
            fn network(&self) -> &'static str {
                "count"
            }
            async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                // A small yield so concurrent callers genuinely overlap inside `open`.
                tokio::task::yield_now().await;
                Ok(Box::new(IdleSource))
            }
        }

        let opens = Arc::new(AtomicUsize::new(0));
        let mut r = ProviderRegistry::new();
        r.register(Arc::new(CountingProvider(opens.clone())));
        let m = StreamManager::new(r);

        // Fire many concurrent first requests for the same id.
        let mut handles = Vec::new();
        for _ in 0..16 {
            let m = m.clone();
            handles.push(tokio::spawn(async move {
                m.get_or_start("count", "x")
                    .await
                    .map(|s| Arc::as_ptr(&s) as usize)
            }));
        }
        let mut ptrs = Vec::new();
        for h in handles {
            ptrs.push(h.await.unwrap().unwrap());
        }
        // Exactly one open, and everyone got the same session.
        assert_eq!(
            opens.load(Ordering::SeqCst),
            1,
            "session must be started exactly once"
        );
        assert!(
            ptrs.iter().all(|p| *p == ptrs[0]),
            "all callers share one session"
        );
    }

    #[tokio::test]
    async fn resolve_vod_is_cached_per_key_and_shared() {
        // A provider that counts how many times `resolve_vod` runs, so we can prove one playback's
        // manifest + segment reads resolve the descriptor once and share one resolved VOD.
        use crate::provider::{ProviderError, StreamProvider, TsSource, VodByteSource, VodContent};
        use async_trait::async_trait;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingVodProvider(Arc<AtomicUsize>);
        struct EmptyVod;
        #[async_trait]
        impl VodContent for EmptyVod {
            fn content_length(&self) -> u64 {
                0
            }
            async fn open_range(
                &self,
                _start: u64,
                _end: u64,
            ) -> Result<Box<dyn VodByteSource>, ProviderError> {
                Err(ProviderError::NotFound)
            }
        }
        #[async_trait]
        impl StreamProvider for CountingVodProvider {
            fn network(&self) -> &'static str {
                "cvod"
            }
            async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
                Err(ProviderError::NotFound)
            }
            async fn resolve_vod(&self, _id: &str) -> Result<Box<dyn VodContent>, ProviderError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(EmptyVod))
            }
        }

        let resolves = Arc::new(AtomicUsize::new(0));
        let mut r = ProviderRegistry::new();
        r.register(Arc::new(CountingVodProvider(resolves.clone())));
        let m = StreamManager::new(r);

        let a = m.resolve_vod("cvod", "x").await.unwrap();
        let b = m.resolve_vod("cvod", "x").await.unwrap();
        assert!(Arc::ptr_eq(&a, &b), "same key shares one resolved VOD");
        assert_eq!(
            resolves.load(Ordering::SeqCst),
            1,
            "resolved once for one id"
        );

        // A different id resolves independently.
        let _c = m.resolve_vod("cvod", "y").await.unwrap();
        assert_eq!(resolves.load(Ordering::SeqCst), 2);

        // Unknown network is NotFound, not a cache entry.
        assert!(matches!(
            m.resolve_vod("nope", "x").await,
            Err(ProviderError::NotFound)
        ));
    }

    #[tokio::test]
    async fn get_hls_only_returns_an_already_running_packager() {
        let m = StreamManager::new(registry());
        // Nothing running yet: peeking must not start a session or packager.
        assert!(m.get_hls("test", "a").await.is_none());
        assert!(
            m.get("test", "a").await.is_none(),
            "peeking must not have started a session"
        );
        // Once the packager exists (e.g. after a playlist fetch), peeking finds it.
        m.get_or_start_hls("test", "a").await.unwrap();
        assert!(m.get_hls("test", "a").await.is_some());
    }

    #[tokio::test]
    async fn unknown_network_is_not_found() {
        let m = StreamManager::new(registry());
        assert!(matches!(
            m.get_or_start("nope", "x").await,
            Err(ProviderError::NotFound)
        ));
    }

    #[tokio::test]
    async fn check_openable_is_not_found_for_an_unregistered_network() {
        let manager = StreamManager::new(ProviderRegistry::new());
        assert!(matches!(
            manager.check_openable("nope", "x"),
            Err(ProviderError::NotFound)
        ));
    }

    #[tokio::test]
    async fn check_openable_delegates_to_the_provider_default() {
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(TestProvider { chunks: 1 }));
        let manager = StreamManager::new(registry);
        assert!(manager.check_openable("test", "anything").is_ok());
    }

    #[tokio::test]
    async fn list_reports_active_sessions() {
        let m = StreamManager::new(registry());
        m.get_or_start("test", "a").await.unwrap();
        let list = m.list().await;
        assert!(list.iter().any(|(n, i, _, metadata)| {
            n == "test" && i == "a" && metadata == &ace_swarm::types::StreamMetadata::default()
        }));
    }

    #[tokio::test]
    async fn hls_owned_stale_media_arc_does_not_keep_worker_alive_after_stop_and_reap() {
        let manager = StreamManager::new(registry());
        let stale_media = manager
            .get_or_start_hls("test", "owned-stale")
            .await
            .unwrap();
        manager.stop("test", "owned-stale").await;

        assert_eq!(stale_media.status(), crate::hls::HlsStatus::Stopped);
        tokio::time::timeout(Duration::from_secs(1), manager.shutdown())
            .await
            .expect("stale media references must not own worker cleanup");
        assert!(manager.get_hls("test", "owned-stale").await.is_none());
    }

    #[tokio::test]
    async fn hls_owned_reap_detaches_worker_even_with_stale_media_arc() {
        let manager = StreamManager::new(registry());
        let stale_media = manager
            .get_or_start_hls("test", "reap-stale")
            .await
            .unwrap();
        manager
            .reap_idle_at(Instant::now() + manager.grace + Duration::from_secs(1))
            .await;
        assert_eq!(stale_media.status(), crate::hls::HlsStatus::Stopped);
        assert!(manager.get_hls("test", "reap-stale").await.is_none());
    }

    #[tokio::test]
    async fn hls_owned_drop_releases_weak_reaper_reference() {
        let manager = StreamManager::new(registry());
        manager.spawn_reaper();
        let weak = Arc::downgrade(&manager);
        drop(manager);
        tokio::task::yield_now().await;
        assert!(
            weak.upgrade().is_none(),
            "reaper must not keep manager alive while sleeping"
        );
    }

    #[tokio::test]
    async fn hls_owned_shutdown_fences_pending_initializer_before_publication() {
        let (manager, started, release) = blocking_manager();
        let initializing = {
            let manager = manager.clone();
            tokio::spawn(async move {
                manager
                    .get_or_start_hls("blocking", "shutdown-pending")
                    .await
            })
        };
        started.notified().await;
        manager.shutdown().await;
        release.notify_one();
        assert!(initializing.await.unwrap().is_err());
        assert!(manager
            .get_hls("blocking", "shutdown-pending")
            .await
            .is_none());
    }

    #[tokio::test]
    async fn hls_owned_native_and_compatibility_starts_share_one_entry() {
        let manager = StreamManager::new(registry());
        let native_start = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.get_or_start_hls("test", "shared-hls").await })
        };
        let compatibility_start = {
            let manager = manager.clone();
            tokio::spawn(async move {
                manager
                    .get_or_start_compatibility_hls("test", "shared-hls")
                    .await
            })
        };
        let native = native_start.await.unwrap().unwrap();
        let (compat, _session, _pin) = compatibility_start.await.unwrap().unwrap();
        assert!(Arc::ptr_eq(&native, &compat));
    }

    #[tokio::test]
    async fn hls_owned_stop_during_delayed_initialization_never_publishes_entry() {
        let (manager, started, release) = blocking_manager();
        let initializing = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.get_or_start_hls("blocking", "delayed").await })
        };
        started.notified().await;
        assert!(!manager.stop("blocking", "delayed").await);
        release.notify_one();
        assert!(initializing.await.unwrap().is_err());
        assert!(manager.get_hls("blocking", "delayed").await.is_none());
    }

    #[tokio::test]
    async fn hls_owned_aborted_initializer_transfers_cleanup_to_manager_tracker() {
        let (manager, started, release) = blocking_manager();
        let initializing = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.get_or_start_hls("blocking", "abort-owned").await })
        };
        started.notified().await;
        initializing.abort();
        assert!(matches!(initializing.await, Err(error) if error.is_cancelled()));
        release.notify_one();

        tokio::time::timeout(Duration::from_secs(1), manager.shutdown())
            .await
            .expect("shutdown must drain cleanup transferred from an aborted initializer");
        assert!(manager.get_hls("blocking", "abort-owned").await.is_none());
    }

    #[tokio::test]
    async fn hls_owned_shutdown_joins_reaper_and_workers_before_returning() {
        let manager = StreamManager::new(registry());
        manager.spawn_reaper();
        manager
            .get_or_start_hls("test", "shutdown-worker")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), manager.shutdown())
            .await
            .expect("shutdown must join the reaper and all owned workers");
        assert!(manager.get_hls("test", "shutdown-worker").await.is_none());
    }

    #[tokio::test]
    async fn hls_owned_shutdown_closes_cleanup_coordinator() {
        let manager = StreamManager::new(registry());
        manager
            .get_or_start_hls("test", "shutdown-coordinator")
            .await
            .unwrap();
        manager.spawn_reaper();
        let coordinator = manager.cleanup.coordinator();

        tokio::time::timeout(Duration::from_secs(1), manager.shutdown())
            .await
            .expect("shutdown must finish before checking coordinator closure");

        assert!(
            coordinator.is_closed(),
            "cleanup coordinator channel must close after shutdown"
        );
    }

    #[tokio::test]
    async fn hls_owned_unrelated_start_progresses_while_cleanup_waits() {
        let (manager, cleanup_started, release_cleanup) =
            manager_with_blocking_worker_for_test().await;
        let stopping = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.stop("test", "slow-cleanup").await })
        };
        cleanup_started.notified().await;
        tokio::task::yield_now().await;
        assert!(
            !stopping.is_finished(),
            "stop must await the detached worker join"
        );
        tokio::time::timeout(
            Duration::from_millis(100),
            manager.get_or_start_hls("test", "unrelated-fast"),
        )
        .await
        .expect("unrelated-key start must proceed outside cleanup waits")
        .unwrap();
        release_cleanup.notify_one();
        stopping.await.unwrap();
    }

    #[tokio::test]
    async fn hls_owned_aborted_stop_leaves_join_with_manager_tracker() {
        let (manager, cleanup_started, release_cleanup) =
            manager_with_blocking_worker_for_test().await;
        let stopping = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.stop("test", "slow-cleanup").await })
        };
        cleanup_started.notified().await;
        assert!(
            !stopping.is_finished(),
            "stop should still be joining cleanup"
        );
        stopping.abort();
        assert!(matches!(stopping.await, Err(error) if error.is_cancelled()));
        release_cleanup.notify_one();
        tokio::time::timeout(Duration::from_secs(1), manager.shutdown())
            .await
            .expect("aborting stop must not abandon the manager-tracked join");
    }

    #[tokio::test]
    async fn stop_removes_session_and_is_idempotent() {
        let m = StreamManager::new(registry());
        m.get_or_start("test", "a").await.unwrap();
        assert!(
            m.stop("test", "a").await,
            "first stop removes the running session"
        );
        assert!(
            m.get("test", "a").await.is_none(),
            "session is gone afterwards"
        );
        assert!(
            !m.stop("test", "a").await,
            "stopping a missing session is a no-op"
        );
    }

    #[tokio::test]
    async fn stop_cancels_pending_hls_without_waiting_for_provider() {
        let (m, started, release) = blocking_manager();
        let start = {
            let m = m.clone();
            tokio::spawn(async move { m.get_or_start_hls("blocking", "stopped").await })
        };
        started.notified().await;

        let stopped =
            tokio::time::timeout(Duration::from_millis(50), m.stop("blocking", "stopped"))
                .await
                .expect("stop must not wait for pending provider work");
        assert!(!stopped);

        release.notify_one();
        assert!(start.await.unwrap().is_err());

        assert!(m.get("blocking", "stopped").await.is_none());
        assert!(m.get_hls("blocking", "stopped").await.is_none());
    }

    #[tokio::test]
    async fn stop_cancels_only_hls_generations_already_pending() {
        let (m, started, release) = blocking_manager();
        let old_start = {
            let m = m.clone();
            tokio::spawn(async move { m.get_or_start_hls("blocking", "generation").await })
        };
        started.notified().await;
        assert!(!m.stop("blocking", "generation").await);

        let new_start = {
            let m = m.clone();
            tokio::spawn(async move { m.get_or_start_hls("blocking", "generation").await })
        };
        release.notify_one();
        assert!(old_start.await.unwrap().is_err());

        started.notified().await;
        release.notify_one();
        new_start.await.unwrap().unwrap();

        assert!(m.get("blocking", "generation").await.is_some());
        assert!(m.get_hls("blocking", "generation").await.is_some());
    }

    #[tokio::test]
    async fn aborted_hls_start_does_not_leave_the_key_permanently_pending() {
        let (m, started, release) = blocking_manager();
        let start = {
            let m = m.clone();
            tokio::spawn(async move { m.get_or_start_hls("blocking", "aborted").await })
        };
        started.notified().await;
        start.abort();
        assert!(matches!(start.await, Err(error) if error.is_cancelled()));

        release.notify_one();
        let pkg = m.get_or_start_hls("blocking", "aborted").await.unwrap();
        pkg.set_last_access_for_test(Instant::now() - m.grace - Duration::from_secs(1));
        m.reap_idle_at(Instant::now()).await;

        assert!(m.get("blocking", "aborted").await.is_none());
        assert!(m.get_hls("blocking", "aborted").await.is_none());
    }

    #[tokio::test]
    async fn direct_and_hls_first_start_share_one_provider_open_and_session() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingBlockingProvider {
            opens: Arc<AtomicUsize>,
            first_open: Arc<Notify>,
            second_open: Arc<Notify>,
            release: Arc<Notify>,
        }

        #[async_trait]
        impl StreamProvider for CountingBlockingProvider {
            fn network(&self) -> &'static str {
                "shared-start"
            }

            async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
                let open = self.opens.fetch_add(1, Ordering::SeqCst) + 1;
                if open == 1 {
                    self.first_open.notify_one();
                } else {
                    self.second_open.notify_one();
                }
                self.release.notified().await;
                Ok(Box::new(IdleSource))
            }
        }

        let opens = Arc::new(AtomicUsize::new(0));
        let first_open = Arc::new(Notify::new());
        let second_open = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(CountingBlockingProvider {
            opens: opens.clone(),
            first_open: first_open.clone(),
            second_open: second_open.clone(),
            release: release.clone(),
        }));
        let m = StreamManager::new(registry);

        let direct = {
            let m = m.clone();
            tokio::spawn(async move { m.get_or_start("shared-start", "same").await })
        };
        first_open.notified().await;

        let hls_entered = Arc::new(Notify::new());
        let hls = {
            let m = m.clone();
            let hls_entered = hls_entered.clone();
            tokio::spawn(async move {
                hls_entered.notify_one();
                m.get_or_start_hls("shared-start", "same").await
            })
        };
        hls_entered.notified().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(50), second_open.notified())
                .await
                .is_err(),
            "same-key direct and HLS starts must not open the provider twice"
        );

        release.notify_one();
        release.notify_one();
        let direct_session = direct.await.unwrap().unwrap();
        hls.await.unwrap().unwrap();
        let managed_session = m.get("shared-start", "same").await.unwrap();

        assert_eq!(opens.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&direct_session, &managed_session));
    }

    #[tokio::test]
    async fn recent_hls_playlist_access_survives_idle_reap() {
        let m = StreamManager::new(registry());
        let pkg = m.get_or_start_hls("test", "active-hls").await.unwrap();
        let before_access = Instant::now();
        pkg.set_last_access_for_test(before_access - m.grace);
        let _playlist = pkg.playlist("test", "active-hls");

        m.reap_idle_at(before_access + m.grace / 2).await;

        assert!(m.get("test", "active-hls").await.is_some());
        assert!(m.get_hls("test", "active-hls").await.is_some());
    }

    #[tokio::test]
    async fn native_hls_creation_survives_reap_before_playlist_rendering() {
        let m = StreamManager::new(registry());
        m.get_or_start_hls("test", "new-native-hls").await.unwrap();

        m.reap_idle_at(Instant::now()).await;

        assert!(m.get("test", "new-native-hls").await.is_some());
        assert!(m.get_hls("test", "new-native-hls").await.is_some());
    }

    #[tokio::test]
    async fn unrelated_hls_start_does_not_wait_for_pending_provider() {
        let (m, started, release) = blocking_manager();
        let blocked = {
            let m = m.clone();
            tokio::spawn(async move { m.get_or_start_hls("blocking", "blocked").await })
        };
        started.notified().await;

        tokio::time::timeout(
            Duration::from_millis(50),
            m.get_or_start_hls("blocking", "fast"),
        )
        .await
        .expect("an unrelated HLS start must not wait for pending provider work")
        .unwrap();

        release.notify_one();
        blocked.await.unwrap().unwrap();
        assert!(m.get_hls("blocking", "blocked").await.is_some());
        assert!(m.get_hls("blocking", "fast").await.is_some());
    }

    #[tokio::test]
    async fn reap_returns_while_native_hls_is_pending_then_observes_activity() {
        let (m, started, release) = blocking_manager();
        let start = {
            let m = m.clone();
            tokio::spawn(async move { m.get_or_start_hls("blocking", "native").await })
        };
        started.notified().await;

        tokio::time::timeout(Duration::from_millis(50), m.reap_idle_at(Instant::now()))
            .await
            .expect("reap must not wait for pending native provider work");

        release.notify_one();
        start.await.unwrap().unwrap();

        assert!(m.get("blocking", "native").await.is_some());
        assert!(m.get_hls("blocking", "native").await.is_some());
    }

    #[tokio::test]
    async fn compatibility_hls_is_retained_only_by_returned_pin() {
        let m = StreamManager::new(registry());
        let (_pkg, _session, pin) = m
            .get_or_start_compatibility_hls("test", "compatibility-only-hls")
            .await
            .unwrap();

        m.reap_idle_at(Instant::now()).await;
        assert!(m.get("test", "compatibility-only-hls").await.is_some());
        assert!(m.get_hls("test", "compatibility-only-hls").await.is_some());

        drop(pin);
        m.reap_idle_at(Instant::now()).await;

        assert!(m.get("test", "compatibility-only-hls").await.is_none());
        assert!(m.get_hls("test", "compatibility-only-hls").await.is_none());
    }

    #[tokio::test]
    async fn reap_returns_while_compatibility_hls_is_pending_then_observes_pin() {
        let (m, started, release) = blocking_manager();
        let start = {
            let m = m.clone();
            tokio::spawn(async move {
                m.get_or_start_compatibility_hls("blocking", "compatibility")
                    .await
            })
        };
        started.notified().await;

        tokio::time::timeout(Duration::from_millis(50), m.reap_idle_at(Instant::now()))
            .await
            .expect("reap must not wait for pending compatibility provider work");

        release.notify_one();
        let (_pkg, _session, pin) = start.await.unwrap().unwrap();

        assert!(m.get("blocking", "compatibility").await.is_some());
        assert!(m.get_hls("blocking", "compatibility").await.is_some());

        drop(pin);
        m.reap_idle_at(Instant::now()).await;
        assert!(m.get("blocking", "compatibility").await.is_none());
        assert!(m.get_hls("blocking", "compatibility").await.is_none());
    }

    #[tokio::test]
    async fn inactive_hls_session_and_packager_are_reaped_together() {
        let m = StreamManager::new(registry());
        m.get_or_start_hls("test", "stale-hls").await.unwrap();

        m.reap_idle_at(Instant::now() + m.grace + Duration::from_secs(1))
            .await;

        assert!(m.get("test", "stale-hls").await.is_none());
        assert!(m.get_hls("test", "stale-hls").await.is_none());
    }
}
