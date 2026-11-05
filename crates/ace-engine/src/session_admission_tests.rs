use super::*;
use crate::provider::{SourceStats, StreamProvider, VodByteSource};
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

struct Provider {
    calls: AtomicUsize,
    entered: Notify,
    finish: Notify,
}
struct Live;
#[async_trait]
impl TsSource for Live {
    async fn next(&mut self) -> Option<bytes::Bytes> {
        std::future::pending().await
    }
    fn stats(&self) -> SourceStats {
        SourceStats::default()
    }
}
struct Content;
struct Range;
#[async_trait]
impl VodContent for Content {
    fn content_length(&self) -> u64 {
        10
    }
    async fn open_range(&self, _: u64, _: u64) -> Result<Box<dyn VodByteSource>, ProviderError> {
        Ok(Box::new(Range))
    }
}
#[async_trait]
impl VodByteSource for Range {
    fn content_length(&self) -> u64 {
        3
    }
    async fn next(&mut self) -> Option<bytes::Bytes> {
        Some(bytes::Bytes::from_static(b"abc"))
    }
}
#[async_trait]
impl StreamProvider for Provider {
    fn network(&self) -> &'static str {
        "synthetic"
    }
    async fn open(&self, id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match id {
            "fail" => Err(ProviderError::NotFound),
            "complete" => {
                crate::testprovider::TestProvider { chunks: 0 }
                    .open(id)
                    .await
            }
            "pending" => {
                self.entered.notify_one();
                self.finish.notified().await;
                Ok(Box::new(Live))
            }
            _ => Ok(Box::new(Live)),
        }
    }
    async fn resolve_vod(&self, id: &str) -> Result<Box<dyn VodContent>, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if id == "pending" {
            self.entered.notify_one();
            self.finish.notified().await;
        }
        if id == "fail" {
            return Err(ProviderError::NotFound);
        }
        Ok(Box::new(Content))
    }
}
fn setup(max: usize) -> (Arc<StreamManager>, Arc<Provider>) {
    let provider = Arc::new(Provider {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        finish: Notify::new(),
    });
    let mut registry = ProviderRegistry::new();
    registry.register(provider.clone());
    (StreamManager::with_session_limit(registry, max), provider)
}
async fn empty(m: &StreamManager) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while m.admission_status().0 != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn support_abandoned_pending_open_is_disposed_without_publishing_pin() {
    let (manager, provider) = setup(1);
    let caller = tokio::spawn({
        let manager = manager.clone();
        async move { manager.acquire_support("synthetic", "pending").await }
    });
    tokio::time::timeout(Duration::from_secs(1), provider.entered.notified())
        .await
        .unwrap();
    caller.abort();
    let _ = caller.await;
    assert_eq!(manager.admission_status().0, 1);
    provider.finish.notify_one();
    empty(&manager).await;
    assert!(manager.get("synthetic", "pending").await.is_none());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(manager.admission_status().0, 0);
    assert!(manager.shutdown().await);
}

#[tokio::test]
async fn support_force_stop_fences_pending_open_before_pin_delivery() {
    let (manager, provider) = setup(1);
    let caller = tokio::spawn({
        let manager = manager.clone();
        async move { manager.acquire_support("synthetic", "pending").await }
    });
    tokio::time::timeout(Duration::from_secs(1), provider.entered.notified())
        .await
        .unwrap();
    // Poll the actual stop through its synchronous pending-generation fence.
    let mut stopping = Box::pin(manager.stop_checked("synthetic", "pending"));
    let _ = futures::poll!(stopping.as_mut());
    assert!(!manager.hls_transitions.lock().unwrap().cancelled.is_empty());
    assert_eq!(manager.admission_status().0, 1);
    provider.finish.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(1), caller)
        .await
        .unwrap()
        .unwrap();
    let refused = matches!(result, Err(ProviderError::NotFound));
    drop(result);
    drop(stopping);
    assert!(manager.shutdown().await);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(manager.admission_status().0, 0);
    assert!(manager.get("synthetic", "pending").await.is_none());
    assert!(
        refused,
        "force-stopped initialization must not publish a support pin"
    );
}

#[tokio::test]
async fn support_existing_session_acquisition_rechecks_stop_after_map_wait() {
    let (manager, provider) = setup(1);
    let retained = manager.get_or_start("synthetic", "live").await.unwrap();
    let map = manager.sessions.lock().await;
    let transition = HlsTransition::begin(
        manager.hls_transitions.clone(),
        ("synthetic".into(), "live".into()),
    );
    let (_cancel, cancel) = watch::channel(false);
    let mut acquiring = Box::pin(manager.get_or_start_owned(
        "synthetic",
        "live",
        cancel,
        transition,
        None,
        |session| session.support_pin(),
    ));
    assert!(futures::poll!(acquiring.as_mut()).is_pending());
    let mut stopping = Box::pin(manager.stop_checked("synthetic", "live"));
    assert!(futures::poll!(stopping.as_mut()).is_pending());
    assert!(!manager.hls_transitions.lock().unwrap().cancelled.is_empty());
    drop(map);
    let (acquired, stopped) = tokio::join!(acquiring, stopping);
    let refused =
        matches!(&acquired, Err(failure) if matches!(failure.error, ProviderError::NotFound));
    drop(acquired);
    assert!(stopped.unwrap());
    assert!(manager.shutdown().await);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(retained.support_pin_count(), 0);
    assert!(
        refused,
        "existing-session acquisition ignored cancellation while awaiting map access"
    );
}

#[tokio::test]
async fn admission_live_fanout_hls_stop_retained_pump_and_completion() {
    let (m, p) = setup(1);
    let first = m.get_or_start("synthetic", "live").await.unwrap();
    let second = m.get_or_start("synthetic", "live").await.unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    let (_, hls_session, pin) = m
        .get_or_start_compatibility_hls("synthetic", "live")
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&first, &hls_session));
    assert!(matches!(
        m.resolve_vod("synthetic", "other").await,
        Err(ProviderError::CapacityExhausted)
    ));
    assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    assert!(m.stop("synthetic", "live").await);
    // Stop now joins actual source destruction even while media handles remain.
    // Paused cleanup retention is independently pinned by paused_live_shutdown_*.
    assert_eq!(m.admission_status(), (0, Some(1)));
    drop((first, second, hls_session, pin));
    empty(&m).await;
    let completed = m.get_or_start("synthetic", "complete").await.unwrap();
    empty(&m).await;
    assert!(m.get("synthetic", "complete").await.is_some());
    drop(completed);
    m.shutdown().await;
}

#[tokio::test]
async fn admission_vod_ranges_hold_owner_after_expiry_and_shutdown() {
    let (m, _) = setup(1);
    let content = m.resolve_vod("synthetic", "vod").await.unwrap();
    let cached = m.resolve_vod("synthetic", "vod").await.unwrap();
    assert!(Arc::ptr_eq(&content, &cached));
    let mut first = content.open_range(0, 2).await.unwrap();
    let second = content.open_range(3, 5).await.unwrap();
    assert_eq!(first.content_length(), 3);
    assert_eq!(
        first.next().await.unwrap(),
        bytes::Bytes::from_static(b"abc")
    );
    {
        let mut cache = m.vod_cache.lock().await;
        cache
            .values_mut()
            .for_each(|(_, at)| *at = Instant::now() - Duration::from_secs(60));
    }
    assert!(matches!(
        m.resolve_vod("synthetic", "vod").await,
        Err(ProviderError::CapacityExhausted)
    ));
    drop((content, cached));
    m.shutdown().await;
    assert_eq!(m.admission_status().0, 1);
    drop(first);
    assert_eq!(m.admission_status().0, 1);
    drop(second);
    assert_eq!(m.admission_status().0, 0);
}

#[tokio::test]
async fn admission_pending_vod_is_shared_with_live_and_unrelated_opens_are_concurrent() {
    let (m, p) = setup(2);
    let pending = tokio::spawn({
        let m = m.clone();
        async move { m.resolve_vod("synthetic", "pending").await }
    });
    p.entered.notified().await;
    let live = tokio::time::timeout(Duration::from_secs(1), m.get_or_start("synthetic", "live"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.admission_status(), (2, Some(2)));
    assert!(matches!(
        m.resolve_vod("synthetic", "extra").await,
        Err(ProviderError::CapacityExhausted)
    ));
    assert_eq!(p.calls.load(Ordering::SeqCst), 2);
    pending.abort();
    let _ = pending.await;
    assert_eq!(m.admission_status().0, 1);
    drop(live);
    m.shutdown().await;
    empty(&m).await;
}

#[tokio::test]
async fn admission_failure_unlimited_and_concurrent_cap() {
    let (m, _) = setup(1);
    assert!(m.get_or_start("synthetic", "fail").await.is_err());
    assert!(m.resolve_vod("synthetic", "fail").await.is_err());
    assert_eq!(m.admission_status().0, 0);
    let mut tasks = Vec::new();
    for n in 0..16 {
        let m = m.clone();
        tasks.push(tokio::spawn(async move {
            m.get_or_start("synthetic", &format!("key{n}")).await
        }));
    }
    let mut retained = Vec::new();
    for task in tasks {
        if let Ok(s) = task.await.unwrap() {
            retained.push(s);
        }
    }
    assert_eq!(retained.len(), 1);
    drop(retained);
    m.shutdown().await;
    empty(&m).await;
    let (unlimited, _) = setup(0);
    for n in 0..10 {
        unlimited
            .get_or_start("synthetic", &format!("key{n}"))
            .await
            .unwrap();
    }
    assert_eq!(unlimited.admission_status(), (10, None));
    unlimited.shutdown().await;
    empty(&unlimited).await;
}

#[tokio::test]
async fn admission_shutdown_fences_pending_live_and_vod_publication() {
    for vod in [false, true] {
        let (m, p) = setup(1);
        let pending = tokio::spawn({
            let m = m.clone();
            async move {
                if vod {
                    m.resolve_vod("synthetic", "pending").await.map(|_| ())
                } else {
                    m.get_or_start("synthetic", "pending").await.map(|_| ())
                }
            }
        });
        p.entered.notified().await;
        if vod {
            // VOD resolve ownership is separate from live initializer teardown.
            m.shutdown().await;
            assert_eq!(m.admission_status().0, 1);
            p.finish.notify_one();
        } else {
            let mut shutting_down = Box::pin(m.shutdown());
            assert!(futures::poll!(shutting_down.as_mut()).is_pending());
            assert_eq!(
                m.admission_status().0,
                1,
                "pending provider retains capacity"
            );
            p.finish.notify_one();
            tokio::time::timeout(Duration::from_secs(1), shutting_down)
                .await
                .unwrap();
            assert_eq!(
                m.admission_status().0,
                0,
                "shutdown joins owned live initialization"
            );
        }
        assert!(
            pending.await.unwrap().is_err(),
            "shutdown must fence publication"
        );
        empty(&m).await;
        assert!(m.get_or_start("synthetic", "later").await.is_err());
        assert!(m.resolve_vod("synthetic", "later").await.is_err());
    }
}

#[tokio::test]
async fn admission_stop_fences_pending_direct_open() {
    let (m, p) = setup(1);
    let pending = tokio::spawn({
        let m = m.clone();
        async move { m.get_or_start("synthetic", "pending").await }
    });
    p.entered.notified().await;
    m.stop("synthetic", "pending").await;
    assert_eq!(m.admission_status().0, 1);
    p.finish.notify_one();
    assert!(pending.await.unwrap().is_err());
    empty(&m).await;
}

#[tokio::test]
async fn admission_hls_failure_cancellation_and_reap_retention() {
    let (m, p) = setup(1);
    assert!(m.get_or_start_hls("synthetic", "fail").await.is_err());
    assert_eq!(m.admission_status().0, 0);
    let pending = tokio::spawn({
        let m = m.clone();
        async move { m.get_or_start_hls("synthetic", "pending").await }
    });
    p.entered.notified().await;
    assert!(matches!(
        m.get_or_start("synthetic", "other").await,
        Err(ProviderError::CapacityExhausted)
    ));
    pending.abort();
    let _ = pending.await;
    assert_eq!(
        m.admission_status().0,
        1,
        "canceled pending open stays owned"
    );
    p.finish.notify_one();
    empty(&m).await;
    let session = m.get_or_start("synthetic", "live").await.unwrap();
    m.reap_idle_at(Instant::now() + Duration::from_secs(60))
        .await;
    assert!(m.get("synthetic", "live").await.is_none());
    // Reaping now joins actual source teardown, not just map removal.
    assert_eq!(m.admission_status().0, 0);
    drop(session);
    empty(&m).await;
    m.get_or_start("synthetic", "other").await.unwrap();
    m.shutdown().await;
    empty(&m).await;
}

#[tokio::test]
async fn admission_queued_hls_does_not_open_after_shutdown() {
    for native in [true, false] {
        let (manager, provider) = setup(1);
        let first = tokio::spawn({
            let manager = manager.clone();
            async move { manager.get_or_start("synthetic", "pending").await }
        });
        tokio::time::timeout(Duration::from_secs(2), provider.entered.notified())
            .await
            .unwrap();
        let outcome = {
            let mut queued = std::pin::pin!(async {
                if native {
                    manager
                        .get_or_start_hls("synthetic", "pending")
                        .await
                        .map(|_| ())
                } else {
                    manager
                        .get_or_start_compatibility_hls("synthetic", "pending")
                        .await
                        .map(|_| ())
                }
            });
            assert!(futures::poll!(queued.as_mut()).is_pending());
            assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
            let mut shutting_down = Box::pin(manager.shutdown());
            assert!(futures::poll!(shutting_down.as_mut()).is_pending());
            first.abort();
            assert!(matches!(first.await, Err(error) if error.is_cancelled()));
            provider.finish.notify_one();
            tokio::time::timeout(Duration::from_secs(1), shutting_down)
                .await
                .unwrap();
            empty(&manager).await;
            tokio::time::timeout(Duration::from_millis(100), queued.as_mut()).await
        };
        empty(&manager).await;
        assert!(
            matches!(outcome, Ok(Err(ProviderError::NotFound))),
            "queued HLS must reject shutdown without new provider work (native={native})"
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(manager.admission_status(), (0, Some(1)));
    }
}

#[tokio::test]
async fn admission_periodic_reap_expires_vod_without_releasing_retained_ranges() {
    for retain_range in [false, true] {
        let (manager, provider) = setup(1);
        let content = manager.resolve_vod("synthetic", "vod").await.unwrap();
        let range = if retain_range {
            Some(content.open_range(0, 2).await.unwrap())
        } else {
            None
        };
        drop(content);
        let at = manager.vod_cache.lock().await.values().next().unwrap().1;
        manager
            .reap_idle_at(at + manager.grace - Duration::from_nanos(1))
            .await;
        assert_eq!(manager.vod_cache.lock().await.len(), 1);
        assert_eq!(manager.admission_status().0, 1);
        manager.reap_idle_at(at + manager.grace).await;
        let removed = manager.vod_cache.lock().await.is_empty();
        let occupied = manager.admission_status().0;
        if retain_range {
            assert!(matches!(
                manager.get_or_start("synthetic", "live").await,
                Err(ProviderError::CapacityExhausted)
            ));
        }
        drop(range);
        assert!(
            removed,
            "periodic reap must expire VOD without another lookup"
        );
        assert_eq!(occupied, usize::from(retain_range));
        let live = manager.get_or_start("synthetic", "live").await.unwrap();
        let (_, shared, pin) = manager
            .get_or_start_compatibility_hls("synthetic", "live")
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&live, &shared));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        manager.stop("synthetic", "live").await;
        drop((live, shared, pin));
        empty(&manager).await;
        manager.shutdown().await;
    }
}

#[tokio::test]
async fn stop_retained_live_handle_terminates_source_before_completion() {
    let (manager, _) = setup(1);
    let held = manager.get_or_start("synthetic", "live").await.unwrap();
    let mut receiver = held.raw_receiver();
    let stopped = tokio::time::timeout(
        Duration::from_millis(100),
        manager.stop("synthetic", "live"),
    )
    .await;
    let occupancy_at_stop = manager.admission_status().0;
    let terminal = tokio::time::timeout(Duration::from_millis(50), receiver.recv()).await;
    // Restore all fixture ownership before asserting the new stop contract.
    drop(receiver);
    drop(held);
    manager.shutdown().await;
    empty(&manager).await;
    assert!(
        matches!(stopped, Ok(true)),
        "bounded explicit stop: {stopped:?}"
    );
    assert_eq!(
        occupancy_at_stop, 0,
        "completed stop retained a provider pump through its media handle"
    );
    assert!(
        matches!(
            terminal,
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed))
        ),
        "waiting subscriber did not see terminal output: {terminal:?}"
    );
}

#[tokio::test]
async fn completed_live_source_closes_late_receiver_with_retained_handle() {
    let (manager, _) = setup(1);
    let held = manager.get_or_start("synthetic", "complete").await.unwrap();
    empty(&manager).await;
    let mut late = held.raw_receiver();
    let terminal = tokio::time::timeout(Duration::from_millis(50), late.recv()).await;
    drop(late);
    drop(held);
    manager.shutdown().await;
    assert!(
        matches!(
            terminal,
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed))
        ),
        "completed source leaves late subscriber pending: {terminal:?}"
    );
}

#[derive(Default)]
struct ClosingState {
    entered: Notify,
    release: Notify,
    calls: AtomicUsize,
    dropped: std::sync::atomic::AtomicBool,
}
struct ClosingLive(Arc<ClosingState>);
impl Drop for ClosingLive {
    fn drop(&mut self) {
        self.0.dropped.store(true, Ordering::SeqCst);
    }
}
#[async_trait]
impl TsSource for ClosingLive {
    async fn next(&mut self) -> Option<bytes::Bytes> {
        std::future::pending().await
    }
    async fn shutdown(&mut self) {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        self.0.entered.notify_one();
        self.0.release.notified().await;
    }
    fn stats(&self) -> SourceStats {
        SourceStats::default()
    }
}
struct ClosingProvider(Arc<ClosingState>);
#[async_trait]
impl StreamProvider for ClosingProvider {
    fn network(&self) -> &'static str {
        "synthetic"
    }
    async fn open(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        Ok(crate::startup_buffer::StartupBufferedSource::new(
            Box::new(ClosingLive(self.0.clone())),
            crate::config::StartupBufferConfig {
                target_ms: 0,
                ..Default::default()
            },
            None,
        ))
    }
}

#[tokio::test]
async fn paused_live_shutdown_retains_capacity_through_cancelled_stop_waiter() {
    paused_live_cleanup(false).await;
}

#[tokio::test]
async fn support_release_waits_for_already_retired_generation_cleanup() {
    let state = Arc::new(ClosingState::default());
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(ClosingProvider(state.clone())));
    let manager = StreamManager::with_session_limit(registry, 1);
    let pin = manager.acquire_support("synthetic", "live").await.unwrap();
    let stopping = tokio::spawn({
        let manager = manager.clone();
        async move { manager.stop_checked("synthetic", "live").await }
    });
    tokio::time::timeout(Duration::from_secs(1), state.entered.notified())
        .await
        .unwrap();
    let mut releasing = Box::pin(manager.release_support("synthetic", "live", pin));
    let waited = futures::poll!(releasing.as_mut()).is_pending();
    let occupied = manager.admission_status().0;
    stopping.abort();
    let _ = stopping.await;
    state.release.notify_one();
    if waited {
        assert!(!tokio::time::timeout(Duration::from_secs(1), releasing)
            .await
            .unwrap()
            .unwrap());
    } else {
        drop(releasing);
    }
    assert!(
        tokio::time::timeout(Duration::from_secs(1), manager.shutdown())
            .await
            .unwrap()
    );
    assert_eq!(occupied, 1);
    assert!(state.dropped.load(Ordering::SeqCst));
    assert_eq!(manager.admission_status().0, 0);
    assert!(
        waited,
        "release must not finish while its retired source cleanup remains pending"
    );
}

#[tokio::test]
async fn support_cancelled_release_keeps_cleanup_and_capacity_owned() {
    let state = Arc::new(ClosingState::default());
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(ClosingProvider(state.clone())));
    let manager = StreamManager::with_session_limit(registry, 1);
    let pin = manager.acquire_support("synthetic", "live").await.unwrap();
    let releasing = tokio::spawn({
        let manager = manager.clone();
        async move { manager.release_support("synthetic", "live", pin).await }
    });
    tokio::time::timeout(Duration::from_secs(1), state.entered.notified())
        .await
        .unwrap();
    assert_eq!(manager.admission_status().0, 1);
    assert!(!releasing.is_finished());
    releasing.abort();
    let _ = releasing.await;
    let refused = matches!(
        manager.acquire_support("synthetic", "other").await,
        Err(ProviderError::CapacityExhausted)
    );
    state.release.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), manager.shutdown())
            .await
            .unwrap()
    );
    assert!(state.dropped.load(Ordering::SeqCst));
    assert_eq!(manager.admission_status().0, 0);
    assert!(
        refused,
        "canceling release must not release pending source capacity"
    );
}

#[tokio::test]
async fn paused_live_reap_retains_capacity_through_cancelled_waiter() {
    paused_live_cleanup(true).await;
}

async fn paused_live_cleanup(reap: bool) {
    let state = Arc::new(ClosingState::default());
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(ClosingProvider(state.clone())));
    let manager = StreamManager::with_session_limit(registry, 1);
    let held = manager.get_or_start("synthetic", "live").await.unwrap();
    let mut waiting = held.raw_receiver();
    let mut stopping = tokio::spawn({
        let manager = manager.clone();
        async move {
            if reap {
                manager.reap_idle_at(Instant::now()).await;
            } else {
                manager.stop("synthetic", "live").await;
            }
        }
    });
    let entered = tokio::time::timeout(Duration::from_millis(100), state.entered.notified())
        .await
        .is_ok();
    let occupancy_paused = manager.admission_status().0;
    let waiting_closed = matches!(
        waiting.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Closed)
    );
    let late_closed = matches!(
        held.raw_receiver().try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Closed)
    );
    let waiter_pending = !stopping.is_finished();
    stopping.abort();
    let _ = (&mut stopping).await;
    state.release.notify_one();
    let joined = tokio::time::timeout(Duration::from_secs(1), manager.shutdown()).await;
    let occupancy_joined = manager.admission_status().0;
    let source_destroyed = state.dropped.load(Ordering::SeqCst);
    // Even an old implementation must release its fixture source before assertions.
    drop(held);
    empty(&manager).await;
    assert!(entered, "explicit stop did not invoke source shutdown");
    assert!(
        waiting_closed && late_closed,
        "output EOF must precede paused source cleanup"
    );
    assert!(
        waiter_pending,
        "stop reported completion while source cleanup was paused"
    );
    assert_eq!(
        occupancy_paused, 1,
        "capacity released before actual source destruction"
    );
    assert!(
        joined.is_ok(),
        "manager lost the canceled waiter's cleanup join"
    );
    assert!(
        source_destroyed,
        "retained media handle kept source alive after joined shutdown"
    );
    assert_eq!(occupancy_joined, 0);
    assert_eq!(state.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn canceled_live_initializer_retains_capacity_and_same_key_lock() {
    for hls in [false, true] {
        let (manager, provider) = setup(2);
        let first = tokio::spawn({
            let manager = manager.clone();
            async move {
                if hls {
                    manager
                        .get_or_start_hls("synthetic", "pending")
                        .await
                        .map(|_| ())
                } else {
                    manager
                        .get_or_start("synthetic", "pending")
                        .await
                        .map(|_| ())
                }
            }
        });
        provider.entered.notified().await;
        first.abort();
        let _ = first.await;
        let occupied_after_cancel = manager.admission_status().0;
        let replacement = tokio::spawn({
            let manager = manager.clone();
            async move { manager.get_or_start("synthetic", "pending").await }
        });
        let duplicate_before_release =
            tokio::time::timeout(Duration::from_millis(50), provider.entered.notified())
                .await
                .is_ok();
        let calls_before_release = provider.calls.load(Ordering::SeqCst);
        // Release and reap every initializer before checking the captured observations.
        provider.finish.notify_one();
        if !duplicate_before_release {
            tokio::time::timeout(Duration::from_secs(1), provider.entered.notified())
                .await
                .unwrap();
            provider.finish.notify_one();
        }
        let session = tokio::time::timeout(Duration::from_secs(1), replacement)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        manager.stop("synthetic", "pending").await;
        drop(session);
        manager.shutdown().await;
        empty(&manager).await;
        assert_eq!(
            occupied_after_cancel, 1,
            "hls={hls}: canceled caller discarded a pending provider reservation"
        );
        assert!(
            !duplicate_before_release,
            "hls={hls}: replacement provider overlapped the owned initializer"
        );
        assert_eq!(
            calls_before_release, 1,
            "hls={hls}: same-key lock escaped the initializer lifetime"
        );
    }
}

struct OpeningClosingProvider {
    entered: Notify,
    release: Notify,
    closing: Arc<ClosingState>,
}
#[async_trait]
impl StreamProvider for OpeningClosingProvider {
    fn network(&self) -> &'static str {
        "synthetic"
    }
    async fn open(&self, id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.entered.notify_one();
        self.release.notified().await;
        ClosingProvider(self.closing.clone()).open(id).await
    }
}

#[tokio::test]
async fn canceled_initializer_joins_paused_source_shutdown_before_capacity_release() {
    for hls in [false, true] {
        let state = Arc::new(ClosingState::default());
        let provider = Arc::new(OpeningClosingProvider {
            entered: Notify::new(),
            release: Notify::new(),
            closing: state.clone(),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider.clone());
        let manager = StreamManager::with_session_limit(registry, 1);
        let pending = tokio::spawn({
            let manager = manager.clone();
            async move {
                if hls {
                    manager
                        .get_or_start_hls("synthetic", "opening")
                        .await
                        .map(|_| ())
                } else {
                    manager
                        .get_or_start("synthetic", "opening")
                        .await
                        .map(|_| ())
                }
            }
        });
        provider.entered.notified().await;
        pending.abort();
        let _ = pending.await;
        provider.release.notify_one();
        let closing_entered =
            tokio::time::timeout(Duration::from_secs(1), state.entered.notified())
                .await
                .is_ok();
        let occupied_while_closing = manager.admission_status().0;
        let mut shutdown = Box::pin(manager.shutdown());
        let shutdown_pending = futures::poll!(shutdown.as_mut()).is_pending();
        let dropped_early = state.dropped.load(Ordering::SeqCst);
        state.release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), shutdown)
            .await
            .unwrap();
        assert!(
            closing_entered,
            "hls={hls}: canceled open bypassed source shutdown"
        );
        assert!(
            shutdown_pending && !dropped_early,
            "hls={hls}: closing source was abandoned"
        );
        assert_eq!(occupied_while_closing, 1);
        assert!(state.dropped.load(Ordering::SeqCst));
        assert_eq!(state.calls.load(Ordering::SeqCst), 1);
        assert_eq!(manager.admission_status().0, 0);
    }
}

#[tokio::test]
async fn stop_fences_registered_initializer_before_its_first_poll() {
    for hls in [false, true] {
        let (manager, provider) = setup(1);
        let mut opening = Box::pin(async {
            if hls {
                manager
                    .get_or_start_hls("synthetic", "live")
                    .await
                    .map(|_| ())
            } else {
                manager.get_or_start("synthetic", "live").await.map(|_| ())
            }
        });
        assert!(futures::poll!(opening.as_mut()).is_pending());
        manager.stop("synthetic", "live").await;
        let result = tokio::time::timeout(Duration::from_secs(1), opening)
            .await
            .unwrap();
        let rejected = result.is_err();
        let calls = provider.calls.load(Ordering::SeqCst);
        manager.stop("synthetic", "live").await;
        manager.shutdown().await;
        assert!(
            rejected,
            "hls={hls}: stop missed a registered but unpolled initializer"
        );
        assert_eq!(
            calls, 0,
            "hls={hls}: stopped generation started provider work"
        );
    }
}

#[tokio::test]
async fn concurrent_stops_share_paused_generation_completion() {
    shared_stop_cleanup(false).await;
}

#[tokio::test]
async fn stop_joins_generation_already_removed_by_reaper() {
    shared_stop_cleanup(true).await;
}

async fn shared_stop_cleanup(reap: bool) {
    let state = Arc::new(ClosingState::default());
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(ClosingProvider(state.clone())));
    let manager = StreamManager::with_session_limit(registry, 1);
    let held = manager.get_or_start("synthetic", "live").await.unwrap();
    let first = tokio::spawn({
        let manager = manager.clone();
        async move {
            if reap {
                manager
                    .reap_idle_at(Instant::now() + Duration::from_secs(60))
                    .await;
                true
            } else {
                manager.stop("synthetic", "live").await
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(1), state.entered.notified())
        .await
        .unwrap();
    let first_pending = !first.is_finished();
    let mut second = tokio::spawn({
        let manager = manager.clone();
        async move { manager.stop("synthetic", "live").await }
    });
    let early_second = tokio::time::timeout(Duration::from_millis(20), &mut second).await;
    let second_pending = early_second.is_err();
    first.abort();
    let _ = first.await;
    state.release.notify_one();
    let second_result = match early_second {
        Ok(result) => result.unwrap(),
        Err(_) => tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .unwrap()
            .unwrap(),
    };
    manager.shutdown().await;
    drop(held);
    assert!(
        first_pending && second_pending,
        "second stop forgot the removed generation's pending cleanup"
    );
    assert!(second_result);
    assert!(state.dropped.load(Ordering::SeqCst));
    assert_eq!(state.calls.load(Ordering::SeqCst), 1);
    assert_eq!(manager.admission_status().0, 0);
}

struct PanickingSource(ClosingLive, Option<tokio::task::JoinHandle<()>>);
impl Drop for PanickingSource {
    fn drop(&mut self) {
        if let Some(worker) = &self.1 {
            worker.abort();
        }
    }
}
struct PanicChildExit(Arc<std::sync::atomic::AtomicBool>);
impl Drop for PanicChildExit {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
#[async_trait]
impl TsSource for PanickingSource {
    async fn next(&mut self) -> Option<bytes::Bytes> {
        panic!("synthetic pull failure")
    }
    async fn shutdown(&mut self) {
        self.0.shutdown().await;
        if let Some(worker) = self.1.as_mut() {
            worker.abort();
            let _ = worker.await;
        }
        self.1.take();
    }
    fn stats(&self) -> SourceStats {
        SourceStats::default()
    }
}
struct PanickingProvider(Arc<ClosingState>, Arc<std::sync::atomic::AtomicBool>);
#[async_trait]
impl StreamProvider for PanickingProvider {
    fn network(&self) -> &'static str {
        "synthetic"
    }
    async fn open(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        let exit = PanicChildExit(self.1.clone());
        let worker = tokio::spawn(async move {
            let _exit = exit;
            std::future::pending::<()>().await;
        });
        Ok(Box::new(PanickingSource(
            ClosingLive(self.0.clone()),
            Some(worker),
        )))
    }
}

#[tokio::test]
async fn source_pull_panic_preserves_owned_shutdown_and_capacity() {
    let state = Arc::new(ClosingState::default());
    let child_destroyed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(PanickingProvider(
        state.clone(),
        child_destroyed.clone(),
    )));
    let manager = StreamManager::with_session_limit(registry, 1);
    let held = manager.get_or_start("synthetic", "panic").await.unwrap();
    let shutdown_entered =
        tokio::time::timeout(Duration::from_millis(100), state.entered.notified())
            .await
            .is_ok();
    let occupancy = manager.admission_status().0;
    let child_retained = !child_destroyed.load(Ordering::SeqCst);
    let mut stopping = Box::pin(manager.stop("synthetic", "panic"));
    let pending = futures::poll!(stopping.as_mut()).is_pending();
    state.release.notify_one();
    if pending {
        tokio::time::timeout(Duration::from_secs(1), stopping)
            .await
            .unwrap();
    }
    manager.shutdown().await;
    drop(held);
    assert!(
        child_retained && child_destroyed.load(Ordering::SeqCst),
        "owned child must remain until joined cleanup"
    );
    assert!(
        shutdown_entered,
        "pull panic bypassed explicit source shutdown"
    );
    assert!(pending, "pull panic falsely completed cleanup");
    assert_eq!(
        occupancy, 1,
        "pull panic released capacity before owned cleanup"
    );
    assert!(state.dropped.load(Ordering::SeqCst));
    assert_eq!(state.calls.load(Ordering::SeqCst), 1);
    assert_eq!(manager.admission_status().0, 0);
}

#[tokio::test]
async fn panicked_worker_does_not_publish_successful_cleanup() {
    let cleanup = CleanupTracker::default();
    let control = cleanup.spawn(|_| async { panic!("synthetic cleanup owner failure") });
    let mut completed = control.completion_receiver();
    drop(control);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !*completed.borrow_and_update() {
            if completed.changed().await.is_err() {
                break;
            }
        }
    })
    .await
    .unwrap();
    let success = *completed.borrow();
    let failed = completed.has_changed().is_err() && !success;
    let first_drain = cleanup.drain().await;
    let repeated_drain = cleanup.drain().await;
    assert!(
        !first_drain && !repeated_drain,
        "cleanup failure must remain latched after receiver pruning"
    );
    assert!(
        !success && failed,
        "leader panic was mislabeled as successful cleanup"
    );
}

struct FailedShutdownSource(ClosingLive);
#[async_trait]
impl TsSource for FailedShutdownSource {
    async fn next(&mut self) -> Option<bytes::Bytes> {
        std::future::pending().await
    }
    async fn shutdown(&mut self) {
        panic!("synthetic source cleanup failure")
    }
    fn stats(&self) -> SourceStats {
        self.0.stats()
    }
}
struct FailedShutdownProvider(Arc<ClosingState>);
#[async_trait]
impl StreamProvider for FailedShutdownProvider {
    fn network(&self) -> &'static str {
        "synthetic"
    }
    async fn open(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        Ok(Box::new(FailedShutdownSource(ClosingLive(self.0.clone()))))
    }
}

#[tokio::test]
async fn support_retired_cleanup_failure_is_not_success_or_reusable_capacity() {
    let state = Arc::new(ClosingState::default());
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(FailedShutdownProvider(state.clone())));
    let manager = StreamManager::with_session_limit(registry, 1);
    let pin = manager.acquire_support("synthetic", "live").await.unwrap();
    assert_eq!(
        manager.stop_checked("synthetic", "live").await,
        Err(StopError::CleanupUnconfirmed)
    );
    assert_eq!(
        manager.release_support("synthetic", "live", pin).await,
        Err(StopError::CleanupUnconfirmed)
    );
    assert_eq!(manager.admission_status().0, 1);
    assert!(!manager.shutdown().await);
    assert!(!state.dropped.load(Ordering::SeqCst));
    drop(manager);
    assert!(state.dropped.load(Ordering::SeqCst));
}
#[tokio::test]
async fn failed_source_shutdown_retains_source_and_admission_until_owner_destruction() {
    let state = Arc::new(ClosingState::default());
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(FailedShutdownProvider(state.clone())));
    let manager = StreamManager::with_session_limit(registry, 1);
    let held = manager.get_or_start("synthetic", "failure").await.unwrap();
    let stopped = manager.stop("synthetic", "failure").await;
    let occupied_after_failure = manager.admission_status().0;
    let destroyed_early = state.dropped.load(Ordering::SeqCst);
    let shutdown_succeeded = manager.shutdown().await;
    drop(held);
    drop(manager);
    assert!(
        !stopped && !shutdown_succeeded,
        "unknown cleanup must not report success"
    );
    assert_eq!(
        occupied_after_failure, 1,
        "cleanup panic released uncertain source capacity"
    );
    assert!(
        !destroyed_early,
        "cleanup panic discarded the failed resource owner"
    );
    assert!(state.dropped.load(Ordering::SeqCst));
}

struct OpeningFailedShutdownProvider(Arc<OpeningClosingProvider>);
#[async_trait]
impl StreamProvider for OpeningFailedShutdownProvider {
    fn network(&self) -> &'static str {
        "synthetic"
    }
    async fn open(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.0.entered.notify_one();
        self.0.release.notified().await;
        Ok(Box::new(FailedShutdownSource(ClosingLive(
            self.0.closing.clone(),
        ))))
    }
}
#[tokio::test]
async fn canceled_initializer_retains_failed_cleanup_source() {
    for hls in [false, true] {
        let state = Arc::new(ClosingState::default());
        let opening = Arc::new(OpeningClosingProvider {
            entered: Notify::new(),
            release: Notify::new(),
            closing: state.clone(),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(OpeningFailedShutdownProvider(opening.clone())));
        let manager = StreamManager::with_session_limit(registry, 1);
        let request = tokio::spawn({
            let manager = manager.clone();
            async move {
                if hls {
                    manager
                        .get_or_start_hls("synthetic", "failure")
                        .await
                        .map(|_| ())
                } else {
                    manager
                        .get_or_start("synthetic", "failure")
                        .await
                        .map(|_| ())
                }
            }
        });
        opening.entered.notified().await;
        request.abort();
        let _ = request.await;
        opening.release.notify_one();
        let shutdown = manager.shutdown().await;
        let occupancy = manager.admission_status().0;
        let dropped_early = state.dropped.load(Ordering::SeqCst);
        drop(manager);
        assert!(!shutdown);
        assert_eq!(
            occupancy, 1,
            "hls={hls}: canceled initializer lost failed source reservation"
        );
        assert!(!dropped_early && state.dropped.load(Ordering::SeqCst));
    }
}

struct GenerationProvider {
    calls: AtomicUsize,
    old: Arc<ClosingState>,
    replacement: Arc<ClosingState>,
}
#[async_trait]
impl StreamProvider for GenerationProvider {
    fn network(&self) -> &'static str {
        "synthetic"
    }
    async fn open(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        let state = if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            &self.old
        } else {
            &self.replacement
        };
        Ok(Box::new(ClosingLive(state.clone())))
    }
}
#[tokio::test]
async fn retired_generation_completion_does_not_remove_or_cancel_replacement() {
    for reap in [false, true] {
        let old = Arc::new(ClosingState::default());
        let replacement_state = Arc::new(ClosingState::default());
        let provider = Arc::new(GenerationProvider {
            calls: AtomicUsize::new(0),
            old: old.clone(),
            replacement: replacement_state.clone(),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider.clone());
        // Spare capacity is intentional: old cleanup must remain admitted while
        // a new fenced generation can start without conflating their ownership.
        let manager = StreamManager::with_session_limit(registry, 2);
        let held_old = manager
            .get_or_start("synthetic", "generation")
            .await
            .unwrap();
        let mut retiring = tokio::spawn({
            let manager = manager.clone();
            async move {
                if reap {
                    manager
                        .reap_idle_at(Instant::now() + Duration::from_secs(60))
                        .await;
                } else {
                    manager.stop("synthetic", "generation").await;
                }
            }
        });
        tokio::time::timeout(Duration::from_secs(1), old.entered.notified())
            .await
            .unwrap();
        let replacement = manager
            .get_or_start("synthetic", "generation")
            .await
            .unwrap();
        let pin = replacement.subscribe();
        let overlap = manager.admission_status().0;
        old.release.notify_one();
        let joined = tokio::time::timeout(Duration::from_secs(1), &mut retiring).await;
        let still_current = manager
            .get("synthetic", "generation")
            .await
            .is_some_and(|current| Arc::ptr_eq(&current, &replacement));
        let replacement_untouched = replacement_state.calls.load(Ordering::SeqCst) == 0;
        let occupied_after_old = manager.admission_status().0;
        replacement_state.release.notify_one();
        let stopped = manager.stop("synthetic", "generation").await;
        drop((held_old, replacement, pin));
        let shutdown = manager.shutdown().await;
        assert!(joined.is_ok_and(|result| result.is_ok()));
        assert!(
            still_current && replacement_untouched,
            "reap={reap}: retired completion affected replacement"
        );
        assert_eq!(overlap, 2);
        assert_eq!(occupied_after_old, 1);
        assert!(stopped && shutdown);
        assert_eq!(old.calls.load(Ordering::SeqCst), 1);
        assert_eq!(replacement_state.calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(manager.admission_status().0, 0);
    }
}

#[tokio::test]
async fn support_stale_open_preserves_newer_desired_intent() {
    let (manager, provider) = setup(1);
    let control = manager
        .register_support("synthetic", "pending")
        .await
        .unwrap();
    let initial = control.intent();
    let opening = tokio::spawn({
        let manager = manager.clone();
        let control = control.clone();
        async move {
            manager
                .acquire_declared_support(&control, initial.epoch)
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(1), provider.entered.notified())
        .await
        .unwrap();
    manager.set_support_running(&control, false).await.unwrap();
    let newer = manager.set_support_running(&control, true).await.unwrap();
    provider.finish.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(1), opening)
        .await
        .unwrap()
        .unwrap();
    let refused =
        matches!(&result, Err(failure) if matches!(failure.error, ProviderError::NotFound));
    drop(result);
    let not_published = manager.get("synthetic", "pending").await.is_none();
    let preserved = control.intent() == newer;
    manager.reap_idle_at(Instant::now()).await;
    provider.finish.notify_one();
    let current = manager
        .acquire_declared_support(&control, newer.epoch)
        .await
        .unwrap();
    manager
        .release_support("synthetic", "pending", current)
        .await
        .unwrap();
    assert!(manager.shutdown().await);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(manager.admission_status().0, 0);
    assert!(refused && not_published && preserved);
}

#[tokio::test]
async fn support_stale_existing_map_attempt_cannot_join_after_restart() {
    let (manager, provider) = setup(1);
    let held = manager.acquire_support("synthetic", "live").await.unwrap();
    let control = manager.register_support("synthetic", "live").await.unwrap();
    let attempt = crate::support_control::SupportAttempt {
        control: control.clone(),
        epoch: control.intent().epoch,
    };
    let map = manager.sessions.lock().await;
    let transition = HlsTransition::begin(
        manager.hls_transitions.clone(),
        ("synthetic".into(), "live".into()),
    );
    let (_cancel, cancel) = watch::channel(false);
    let mut acquiring = Box::pin(manager.get_or_start_owned(
        "synthetic",
        "live",
        cancel,
        transition,
        Some(attempt),
        |session| session.support_pin(),
    ));
    assert!(futures::poll!(acquiring.as_mut()).is_pending());
    manager.set_support_running(&control, false).await.unwrap();
    let newer = manager.set_support_running(&control, true).await.unwrap();
    drop(map);
    let result = acquiring.await;
    let refused =
        matches!(&result, Err(failure) if matches!(failure.error, ProviderError::NotFound));
    drop(result);
    let fresh = manager
        .acquire_declared_support(&control, newer.epoch)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(fresh.session(), held.session()));
    assert_eq!(held.session().support_pin_count(), 2);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    manager
        .release_support("synthetic", "live", fresh)
        .await
        .unwrap();
    manager
        .release_support("synthetic", "live", held)
        .await
        .unwrap();
    assert!(manager.shutdown().await);
    assert!(refused);
}

struct PendingFailedProvider {
    source: Arc<ClosingState>,
    entered: Notify,
    release: Notify,
}
#[async_trait]
impl StreamProvider for PendingFailedProvider {
    fn network(&self) -> &'static str {
        "synthetic"
    }
    async fn open(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(Box::new(FailedShutdownSource(ClosingLive(
            self.source.clone(),
        ))))
    }
}
#[tokio::test]
async fn declared_cancelled_open_cleanup_failure_is_terminal_and_keeps_capacity() {
    let state = Arc::new(ClosingState::default());
    let provider = Arc::new(PendingFailedProvider {
        source: state.clone(),
        entered: Notify::new(),
        release: Notify::new(),
    });
    let mut registry = ProviderRegistry::new();
    registry.register(provider.clone());
    let manager = StreamManager::with_session_limit(registry, 1);
    let control = manager.register_support("synthetic", "live").await.unwrap();
    let opening = tokio::spawn({
        let manager = manager.clone();
        let control = control.clone();
        async move {
            manager
                .acquire_declared_support(&control, control.intent().epoch)
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(1), provider.entered.notified())
        .await
        .unwrap();
    manager.set_support_running(&control, false).await.unwrap();
    provider.release.notify_one();
    let error = tokio::time::timeout(Duration::from_secs(1), opening)
        .await
        .unwrap()
        .unwrap()
        .err()
        .unwrap();
    let occupied = manager.admission_status().0;
    assert!(!manager.shutdown().await);
    assert!(!state.dropped.load(Ordering::SeqCst));
    drop(manager);
    assert!(state.dropped.load(Ordering::SeqCst));
    assert_eq!(occupied, 1);
    assert_eq!(
        error.kind,
        crate::start_failure::StartFailureKind::CleanupUnconfirmed
    );
    assert!(!error.kind.retryable());
}

#[tokio::test]
async fn declared_start_session_capacity_is_typed_and_does_not_open_provider() {
    let (manager, provider) = setup(1);
    let pin = manager.acquire_support("synthetic", "live").await.unwrap();
    let control = manager
        .register_support("synthetic", "other")
        .await
        .unwrap();
    let error = manager
        .acquire_declared_support(&control, control.intent().epoch)
        .await
        .err()
        .unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    manager
        .release_support("synthetic", "live", pin)
        .await
        .unwrap();
    assert!(manager.shutdown().await);
    assert_eq!(error.kind, crate::start_failure::StartFailureKind::Capacity);
    assert!(error.kind.retryable());
    assert_eq!(error.error.to_string(), "session capacity reached");
}

#[tokio::test]
async fn completed_support_generation_retires_despite_old_viewers_and_pins() {
    let (manager, provider) = setup(1);
    let first = manager
        .acquire_support("synthetic", "complete")
        .await
        .unwrap();
    let second = manager
        .acquire_support("synthetic", "complete")
        .await
        .unwrap();
    let viewer = first.session().subscribe();
    let old = first.session().clone();
    let mut completed = old.completion_receiver();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), wait_cleanup(&mut completed))
            .await
            .unwrap()
    );
    assert_eq!(old.subscriber_count(), 1);
    assert_eq!(old.support_pin_count(), 2);
    let retired = manager
        .release_support("synthetic", "complete", first)
        .await
        .unwrap();
    let absent = manager.get("synthetic", "complete").await.is_none();
    // Restore all owners even against the old release policy before asserting.
    if !absent {
        manager.stop_checked("synthetic", "complete").await.unwrap();
    }
    let fresh = manager
        .acquire_support("synthetic", "complete")
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&old, fresh.session()));
    assert!(!manager
        .release_support("synthetic", "complete", second)
        .await
        .unwrap());
    assert!(Arc::ptr_eq(
        &manager.get("synthetic", "complete").await.unwrap(),
        fresh.session()
    ));
    manager
        .release_support("synthetic", "complete", fresh)
        .await
        .unwrap();
    drop(viewer);
    assert!(manager.shutdown().await);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(manager.admission_status().0, 0);
    assert!(
        retired && absent,
        "completed generation must leave the map without waiting for old media handles"
    );
}
