//! Registry of live sessions keyed by `(network, id)`. One shared session per key; lazy
//! start via the provider; teardown after the last subscriber leaves + grace.

use crate::config::HlsConfig;
use crate::hls::HlsPackager;
use crate::provider::{ProviderError, ProviderRegistry, TsSource, VodContent};
use crate::session::{StreamSession, Subscription};
use ace_swarm::types::StreamMetadata;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

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

pub struct StreamManager {
    registry: ProviderRegistry,
    sessions: Mutex<HashMap<(String, String), Arc<StreamSession>>>,
    packagers: Mutex<HashMap<(String, String), Arc<HlsPackager>>>,
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
    grace: Duration,
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
            grace: Duration::from_secs(30),
        })
    }

    #[cfg(test)]
    pub(crate) fn buffer(&self) -> usize {
        self.buffer
    }

    pub(crate) fn hls_config(&self) -> HlsConfig {
        self.hls
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
        if transition.finish() {
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
                .or_insert_with(|| HlsPackager::start(&session, self.hls))
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
            .cloned()
    }

    /// Force-stop a session: remove it (and any HLS packager) so the shared download is torn
    /// down — the session's `Drop` aborts its background pull task. Returns `true` if a session
    /// for `(network, id)` existed. Connected clients see the stream end.
    pub async fn stop(&self, network: &str, id: &str) -> bool {
        let key = (network.to_string(), id.to_string());
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
        self.packagers.lock().await.remove(&key);
        self.sessions.lock().await.remove(&key).is_some()
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
                .filter(|(_, pkg)| pkg.was_accessed_within(now, self.grace))
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

        self.packagers
            .lock()
            .await
            .retain(|key, _| retained_sessions.contains(key));
    }

    /// Spawn the idle-teardown watcher: drops sessions with 0 subscribers after `grace`, while
    /// recent native HLS activity retains a zero-subscriber session during that grace period.
    pub fn spawn_reaper(self: &Arc<Self>) {
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(me.grace).await;
                me.reap_idle_at(Instant::now()).await;
            }
        });
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
    async fn list_reports_active_sessions() {
        let m = StreamManager::new(registry());
        m.get_or_start("test", "a").await.unwrap();
        let list = m.list().await;
        assert!(list.iter().any(|(n, i, _, metadata)| {
            n == "test" && i == "a" && metadata == &ace_swarm::types::StreamMetadata::default()
        }));
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
