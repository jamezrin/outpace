use super::*;
use crate::provider::{ProviderRegistry, StreamProvider, TsSource};
use crate::start_failure::{ProviderStartFailure, StartFailureKind};
use async_trait::async_trait;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
struct Scripted {
    calls: AtomicUsize,
    permanent: bool,
}
#[async_trait]
impl StreamProvider for Scripted {
    fn network(&self) -> &'static str {
        "ace"
    }
    async fn open(&self, id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.open_classified(id).await.map_err(ProviderError::from)
    }
    async fn open_classified(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderStartFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(ProviderStartFailure::new(
            if self.permanent {
                StartFailureKind::Resolution(
                    ace_swarm::resolve::ResolveFailureKind::InvalidDescriptor,
                )
            } else {
                StartFailureKind::Discovery
            },
            ProviderError::Backend("private diagnostic must not escape".into()),
        ))
    }
}
async fn setup(permanent: bool) -> (Arc<SupportService>, Arc<StreamManager>, Arc<Scripted>) {
    let provider = Arc::new(Scripted {
        calls: AtomicUsize::new(0),
        permanent,
    });
    let mut registry = ProviderRegistry::new();
    registry.register(provider.clone());
    let manager = StreamManager::with_session_limit(registry, 1);
    let sources = SupportSources::parse(Some(r#"{"synthetic":"local:synthetic"}"#)).unwrap();
    let service = SupportService::spawn(manager.clone(), &sources)
        .await
        .unwrap();
    (service, manager, provider)
}
async fn settle(service: &SupportService, state: SupportState, attempts: u64) -> bool {
    for _ in 0..1000 {
        let status = &service.status()[0];
        if status.state == state && status.attempts == attempts && !status.transition_pending {
            return true;
        }
        tokio::task::yield_now().await;
    }
    false
}
#[tokio::test(start_paused = true)]
async fn retries_are_bounded_and_stop_prevents_further_attempts() {
    let (service, manager, provider) = setup(false).await;
    let first = settle(&service, SupportState::Retrying, 1).await;
    if !first {
        assert!(service.shutdown().await);
        assert!(manager.shutdown().await);
    }
    assert!(first, "configured source must run without a viewer");
    let encoded = serde_json::to_string(&service.status()).unwrap();
    assert!(!encoded.contains("private diagnostic"));
    assert!(!encoded.contains("local:synthetic"));
    assert_eq!(service.status()[0].reason, Some("discovery"));
    let mut attempts = 1;
    for seconds in [1, 2, 4, 8, 16, 30, 30] {
        tokio::time::advance(Duration::from_millis(seconds * 1000 - 1)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), attempts as usize);
        tokio::time::advance(Duration::from_millis(1)).await;
        attempts += 1;
        assert!(settle(&service, SupportState::Retrying, attempts).await);
    }
    service.stop("synthetic").await.unwrap();
    assert!(settle(&service, SupportState::Stopped, attempts).await);
    tokio::time::advance(Duration::from_secs(100)).await;
    assert_eq!(provider.calls.load(Ordering::SeqCst), attempts as usize);
    service.start("synthetic").await.unwrap();
    assert!(settle(&service, SupportState::Retrying, attempts + 1).await);
    assert!(service.shutdown().await);
    assert!(manager.shutdown().await);
    assert!(!serde_json::to_string(&service.status())
        .unwrap()
        .contains("private diagnostic"));
}
#[tokio::test(start_paused = true)]
async fn permanent_failure_waits_for_explicit_restart() {
    let (service, manager, provider) = setup(true).await;
    let failed = settle(&service, SupportState::Error, 1).await;
    if !failed {
        assert!(service.shutdown().await);
        assert!(manager.shutdown().await);
    }
    assert!(failed);
    let encoded = serde_json::to_string(&service.status()).unwrap();
    assert!(!encoded.contains("private diagnostic"));
    assert!(!encoded.contains("local:synthetic"));
    assert_eq!(service.status()[0].reason, Some("invalid_descriptor"));
    tokio::time::advance(Duration::from_secs(120)).await;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    service.start("synthetic").await.unwrap();
    assert!(settle(&service, SupportState::Error, 2).await);
    assert!(service.shutdown().await);
    assert!(manager.shutdown().await);
}

struct LiveProvider {
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
}
struct OwnedLive(Arc<AtomicUsize>);
impl Drop for OwnedLive {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl TsSource for OwnedLive {
    async fn next(&mut self) -> Option<bytes::Bytes> {
        std::future::pending().await
    }
    fn stats(&self) -> crate::provider::SourceStats {
        Default::default()
    }
}
#[async_trait]
impl StreamProvider for LiveProvider {
    fn network(&self) -> &'static str {
        "ace"
    }
    async fn open(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.active.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(OwnedLive(self.active.clone())))
    }
}
#[tokio::test]
async fn zero_viewer_source_is_owned_through_stop_restart_and_concurrent_shutdown() {
    let active = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(LiveProvider {
        calls: 0.into(),
        active: active.clone(),
    });
    let mut registry = ProviderRegistry::new();
    registry.register(provider.clone());
    let manager = StreamManager::with_session_limit(registry, 1);
    let sources = SupportSources::parse(Some(r#"{"synthetic":"local:synthetic"}"#)).unwrap();
    let service = SupportService::spawn(manager.clone(), &sources)
        .await
        .unwrap();
    assert!(settle(&service, SupportState::Running, 1).await);
    assert_eq!(manager.list().await[0].2, 0);
    assert_eq!(active.load(Ordering::SeqCst), 1);
    assert_eq!(manager.admission_status().0, 1);
    service.stop("synthetic").await.unwrap();
    assert!(settle(&service, SupportState::Stopped, 1).await);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(manager.admission_status().0, 0);
    service.start("synthetic").await.unwrap();
    assert!(settle(&service, SupportState::Running, 2).await);
    let (first, second) = tokio::join!(service.shutdown(), service.shutdown());
    assert!(first && second);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(manager.admission_status().0, 0);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert!(service.start("synthetic").await.is_err());
    assert!(manager.shutdown().await);
}

struct PanicStats;
impl crate::provider::StatsReader for PanicStats {
    fn snapshot(&self) -> crate::provider::SourceStats {
        panic!("synthetic stats fault")
    }
}
struct PanicStatsSource(OwnedLive);
#[async_trait]
impl TsSource for PanicStatsSource {
    async fn next(&mut self) -> Option<bytes::Bytes> {
        self.0.next().await
    }
    fn stats(&self) -> crate::provider::SourceStats {
        Default::default()
    }
    fn stats_handle(&self) -> Option<crate::provider::StatsHandle> {
        Some(Arc::new(PanicStats))
    }
}
struct PanicStatsProvider(Arc<AtomicUsize>);
#[async_trait]
impl StreamProvider for PanicStatsProvider {
    fn network(&self) -> &'static str {
        "ace"
    }
    async fn open(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(PanicStatsSource(OwnedLive(self.0.clone()))))
    }
}
#[tokio::test(start_paused = true)]
async fn supervisor_panic_is_visible_terminal_and_shutdown_is_not_success() {
    let active = Arc::new(AtomicUsize::new(0));
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(PanicStatsProvider(active.clone())));
    let manager = StreamManager::with_session_limit(registry, 1);
    let sources = SupportSources::parse(Some(r#"{"synthetic":"local:synthetic"}"#)).unwrap();
    let service = SupportService::spawn(manager.clone(), &sources)
        .await
        .unwrap();
    assert!(settle(&service, SupportState::Running, 1).await);
    tokio::time::advance(Duration::from_secs(1)).await;
    let visible = settle(&service, SupportState::Error, 1).await;
    let rejected = service.start("synthetic").await.is_err();
    assert!(!service.shutdown().await);
    assert!(manager.shutdown().await);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(
        visible && rejected,
        "dead worker must not remain Running or accept restart"
    );
}

struct ProgressProvider {
    calls: AtomicUsize,
    source: StdMutex<Option<tokio::sync::mpsc::Receiver<bytes::Bytes>>>,
}
struct ProgressSource {
    rx: tokio::sync::mpsc::Receiver<bytes::Bytes>,
    downloaded: u64,
}
#[async_trait]
impl TsSource for ProgressSource {
    async fn next(&mut self) -> Option<bytes::Bytes> {
        let chunk = self.rx.recv().await?;
        self.downloaded += chunk.len() as u64;
        Some(chunk)
    }
    fn stats(&self) -> crate::provider::SourceStats {
        crate::provider::SourceStats {
            downloaded: self.downloaded,
            ..Default::default()
        }
    }
}
#[async_trait]
impl StreamProvider for ProgressProvider {
    fn network(&self) -> &'static str {
        "ace"
    }
    async fn open(&self, id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.open_classified(id).await.map_err(ProviderError::from)
    }
    async fn open_classified(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderStartFailure> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call > 2 {
            if let Some(rx) = self.source.lock().unwrap().take() {
                return Ok(Box::new(ProgressSource { rx, downloaded: 0 }));
            }
        }
        Err(ProviderStartFailure::new(
            StartFailureKind::Discovery,
            ProviderError::Backend("synthetic discovery failure".into()),
        ))
    }
}
async fn progress_case(progress_steps: u64) {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let provider = Arc::new(ProgressProvider {
        calls: 0.into(),
        source: StdMutex::new(Some(rx)),
    });
    let mut registry = ProviderRegistry::new();
    registry.register(provider.clone());
    let manager = StreamManager::with_session_limit(registry, 1);
    let sources = SupportSources::parse(Some(r#"{"synthetic":"local:synthetic"}"#)).unwrap();
    let service = SupportService::spawn(manager.clone(), &sources)
        .await
        .unwrap();
    assert!(settle(&service, SupportState::Retrying, 1).await);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(settle(&service, SupportState::Retrying, 2).await);
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(settle(&service, SupportState::Running, 3).await);
    if progress_steps > 0 {
        let source = manager.get("ace", "local:synthetic").await.unwrap();
        for step in 1..=progress_steps {
            tx.send(bytes::Bytes::from(vec![0x47; 188])).await.unwrap();
            let mut consumed = false;
            for _ in 0..1000 {
                if source.stats().await.downloaded == step * 188 {
                    consumed = true;
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert!(consumed, "real source pump must consume the fixture packet");
            tokio::time::advance(Duration::from_secs(1)).await;
            let mut sampled = false;
            for _ in 0..1000 {
                if service.status()[0].downloaded == step * 188 {
                    sampled = true;
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert!(
                sampled,
                "supervisor must observe real emitted-byte progress"
            );
        }
    } else {
        tokio::time::advance(Duration::from_secs(10)).await;
    }
    drop(tx);
    assert!(settle(&service, SupportState::Retrying, 3).await);
    let delay = service.status()[0].retry_ms;
    let expected = if progress_steps >= 31 { 1 } else { 4 };
    tokio::time::advance(Duration::from_secs(expected)).await;
    let attempted = settle(&service, SupportState::Retrying, 4).await;
    let reset_snapshot = service.status()[0].downloaded == 0;
    assert!(service.shutdown().await);
    assert!(manager.shutdown().await);
    assert_eq!(manager.admission_status().0, 0);
    assert_eq!(
        delay,
        expected * 1000,
        "opening alone must not reset retry delay; sustained actual progress must"
    );
    assert!(attempted);
    assert!(
        reset_snapshot,
        "new attempt retained the prior generation's downloaded sample"
    );
}
#[tokio::test(start_paused = true)]
async fn successful_open_without_progress_does_not_reset_backoff() {
    progress_case(0).await;
}
#[tokio::test(start_paused = true)]
async fn sustained_emitted_progress_resets_backoff_and_new_attempt_statistics() {
    progress_case(31).await;
}

#[tokio::test(start_paused = true)]
async fn progress_short_of_thirty_observed_seconds_does_not_reset_backoff() {
    progress_case(30).await;
}

#[derive(Default)]
struct PauseState {
    opening: tokio::sync::Notify,
    open_release: tokio::sync::Notify,
    cleaning: tokio::sync::Notify,
    cleanup_release: tokio::sync::Notify,
    active: AtomicUsize,
    calls: AtomicUsize,
}
struct PausedProvider(Arc<PauseState>);
struct PausedSource(Arc<PauseState>);
impl Drop for PausedSource {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl TsSource for PausedSource {
    async fn next(&mut self) -> Option<bytes::Bytes> {
        std::future::pending().await
    }
    fn stats(&self) -> crate::provider::SourceStats {
        Default::default()
    }
    async fn shutdown(&mut self) {
        self.0.cleaning.notify_one();
        self.0.cleanup_release.notified().await;
    }
}
#[async_trait]
impl StreamProvider for PausedProvider {
    fn network(&self) -> &'static str {
        "ace"
    }
    async fn open(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        self.0.opening.notify_one();
        self.0.open_release.notified().await;
        self.0.active.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(PausedSource(self.0.clone())))
    }
}
#[tokio::test]
async fn cancelled_shutdown_waiter_preserves_pending_open_and_paused_source_cleanup() {
    let state = Arc::new(PauseState::default());
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(PausedProvider(state.clone())));
    let manager = StreamManager::with_session_limit(registry, 1);
    let sources = SupportSources::parse(Some(r#"{"synthetic":"local:synthetic"}"#)).unwrap();
    let service = SupportService::spawn(manager.clone(), &sources)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), state.opening.notified())
        .await
        .unwrap();
    let waiter = tokio::spawn({
        let service = service.clone();
        async move { service.shutdown().await }
    });
    for _ in 0..1000 {
        if service.closed.load(Ordering::Acquire) && service.workers.lock().unwrap().is_empty() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(service.closed.load(Ordering::Acquire));
    assert!(!waiter.is_finished());
    assert_eq!(manager.admission_status().0, 1);
    waiter.abort();
    let _ = waiter.await;
    state.open_release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), state.cleaning.notified())
        .await
        .unwrap();
    assert_eq!(state.active.load(Ordering::SeqCst), 1);
    assert_eq!(manager.admission_status().0, 1);
    let mut repeated = Box::pin(service.shutdown());
    assert!(futures::poll!(repeated.as_mut()).is_pending());
    state.cleanup_release.notify_one();
    assert!(tokio::time::timeout(Duration::from_secs(1), repeated)
        .await
        .unwrap());
    assert_eq!(state.active.load(Ordering::SeqCst), 0);
    assert_eq!(state.calls.load(Ordering::SeqCst), 1);
    assert_eq!(manager.admission_status().0, 0);
    assert!(manager.shutdown().await);
}

struct FailedCleanupSource {
    end: Arc<tokio::sync::Notify>,
    active: Arc<AtomicUsize>,
}
impl Drop for FailedCleanupSource {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl TsSource for FailedCleanupSource {
    async fn next(&mut self) -> Option<bytes::Bytes> {
        self.end.notified().await;
        None
    }
    fn stats(&self) -> crate::provider::SourceStats {
        Default::default()
    }
    async fn shutdown(&mut self) {
        panic!("synthetic cleanup failure")
    }
}
struct FailedCleanupProvider {
    end: Arc<tokio::sync::Notify>,
    active: Arc<AtomicUsize>,
    calls: AtomicUsize,
}
#[async_trait]
impl StreamProvider for FailedCleanupProvider {
    fn network(&self) -> &'static str {
        "ace"
    }
    async fn open(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.active.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(FailedCleanupSource {
            end: self.end.clone(),
            active: self.active.clone(),
        }))
    }
}
#[tokio::test(start_paused = true)]
async fn source_cleanup_fault_cannot_retry_or_explicitly_restart() {
    let active = Arc::new(AtomicUsize::new(0));
    let end = Arc::new(tokio::sync::Notify::new());
    let provider = Arc::new(FailedCleanupProvider {
        end: end.clone(),
        active: active.clone(),
        calls: 0.into(),
    });
    let mut registry = ProviderRegistry::new();
    registry.register(provider.clone());
    let manager = StreamManager::with_session_limit(registry, 1);
    let sources = SupportSources::parse(Some(r#"{"synthetic":"local:synthetic"}"#)).unwrap();
    let service = SupportService::spawn(manager.clone(), &sources)
        .await
        .unwrap();
    assert!(settle(&service, SupportState::Running, 1).await);
    end.notify_one();
    assert!(settle(&service, SupportState::Error, 1).await);
    tokio::time::advance(Duration::from_secs(120)).await;
    assert!(service.start("synthetic").await.is_err());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert!(!service.shutdown().await);
    assert!(!manager.shutdown().await);
    assert_eq!(manager.admission_status().0, 1);
    assert_eq!(active.load(Ordering::SeqCst), 1);
    drop(service);
    drop(manager);
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn duplicate_declarations_viewer_and_hls_share_one_source_at_capacity() {
    let active = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(LiveProvider {
        calls: 0.into(),
        active: active.clone(),
    });
    let mut registry = ProviderRegistry::new();
    registry.register(provider.clone());
    let manager = StreamManager::with_session_limit(registry, 1);
    let sources =
        SupportSources::parse(Some(r#"{"one":"local:synthetic","two":"local:synthetic"}"#))
            .unwrap();
    let service = SupportService::spawn(manager.clone(), &sources)
        .await
        .unwrap();
    let mut ready = false;
    for _ in 0..1000 {
        if service
            .status()
            .iter()
            .all(|status| status.state == SupportState::Running)
        {
            ready = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(ready);
    let session = manager
        .get_or_start("ace", "local:synthetic")
        .await
        .unwrap();
    let viewer = session.subscribe();
    let hls = manager
        .get_or_start_hls("ace", "local:synthetic")
        .await
        .unwrap();
    assert_eq!(session.subscriber_count(), 1);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    service.stop("one").await.unwrap();
    let mut stopped = false;
    for _ in 0..1000 {
        let status = service.status();
        if status[0].state == SupportState::Stopped
            && !status[0].transition_pending
            && status[1].state == SupportState::Running
        {
            stopped = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(stopped);
    assert_eq!(active.load(Ordering::SeqCst), 1);
    assert!(service.shutdown().await);
    assert_eq!(manager.admission_status().0, 1);
    assert_eq!(session.subscriber_count(), 1);
    assert_eq!(active.load(Ordering::SeqCst), 1);
    assert!(manager.shutdown().await);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    drop(viewer);
    drop(hls);
}

struct GenerationProvider {
    calls: AtomicUsize,
    sources: StdMutex<std::collections::VecDeque<tokio::sync::mpsc::Receiver<bytes::Bytes>>>,
}
#[async_trait]
impl StreamProvider for GenerationProvider {
    fn network(&self) -> &'static str {
        "ace"
    }
    async fn open(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let rx = self
            .sources
            .lock()
            .unwrap()
            .pop_front()
            .ok_or(ProviderError::NotFound)?;
        Ok(Box::new(ProgressSource { rx, downloaded: 0 }))
    }
}
#[tokio::test(start_paused = true)]
async fn confirmed_eof_recovers_autonomously_with_old_viewer_retained() {
    let (first_tx, first_rx) = tokio::sync::mpsc::channel(1);
    let (next_tx, next_rx) = tokio::sync::mpsc::channel(1);
    let provider = Arc::new(GenerationProvider {
        calls: 0.into(),
        sources: StdMutex::new([first_rx, next_rx].into()),
    });
    let mut registry = ProviderRegistry::new();
    registry.register(provider.clone());
    let manager = StreamManager::with_session_limit(registry, 1);
    let sources = SupportSources::parse(Some(r#"{"synthetic":"local:synthetic"}"#)).unwrap();
    let service = SupportService::spawn(manager.clone(), &sources)
        .await
        .unwrap();
    assert!(settle(&service, SupportState::Running, 1).await);
    let old = manager.get("ace", "local:synthetic").await.unwrap();
    let mut viewer = old.subscribe();
    first_tx
        .send(bytes::Bytes::from_static(b"synthetic first generation"))
        .await
        .unwrap();
    assert_eq!(
        viewer.rx.recv().await.unwrap(),
        crate::session::StreamEvent::Data(bytes::Bytes::from_static(b"synthetic first generation"))
    );
    drop(first_tx);
    assert!(settle(&service, SupportState::Retrying, 1).await);
    assert_eq!(old.subscriber_count(), 1);
    assert!(matches!(
        viewer.rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Closed)
    ));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(settle(&service, SupportState::Running, 2).await);
    let current = manager.get("ace", "local:synthetic").await.unwrap();
    assert!(!Arc::ptr_eq(&old, &current));
    assert_eq!(current.subscriber_count(), 0);
    next_tx
        .send(bytes::Bytes::from_static(b"synthetic next generation"))
        .await
        .unwrap();
    let mut progressed = false;
    for _ in 0..1000 {
        if current.stats().await.downloaded == 25 {
            progressed = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        progressed,
        "replacement must consume media without a new viewer"
    );
    assert!(matches!(
        viewer.rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Closed)
    ));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert!(service.shutdown().await);
    assert!(manager.shutdown().await);
    assert_eq!(manager.admission_status().0, 0);
    drop(viewer);
    drop(next_tx);
}

struct RejectingProvider(AtomicUsize);
#[async_trait]
impl StreamProvider for RejectingProvider {
    fn network(&self) -> &'static str {
        "ace"
    }
    fn check_openable(&self, id: &str) -> Result<(), ProviderError> {
        if id == "local:reject" {
            Err(ProviderError::Unresolvable(
                "synthetic rejected declaration".into(),
            ))
        } else {
            Ok(())
        }
    }
    async fn open(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(ProviderError::NotFound)
    }
}
#[tokio::test]
async fn registration_failure_rolls_back_before_any_worker_or_source_starts() {
    let provider = Arc::new(RejectingProvider(0.into()));
    let mut registry = ProviderRegistry::new();
    registry.register(provider.clone());
    let manager = StreamManager::with_session_limit(registry, 1);
    let sources =
        SupportSources::parse(Some(r#"{"one":"local:first","two":"local:reject"}"#)).unwrap();
    assert!(SupportService::spawn(manager.clone(), &sources)
        .await
        .is_err());
    let mut controls = Vec::new();
    for _ in 0..8 {
        controls.push(
            manager
                .register_support("ace", "local:first")
                .await
                .unwrap(),
        );
    }
    assert_eq!(provider.0.load(Ordering::SeqCst), 0);
    assert_eq!(manager.admission_status().0, 0);
    assert!(manager.shutdown().await);
}

struct StaleFailureProvider {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
}
#[async_trait]
impl StreamProvider for StaleFailureProvider {
    fn network(&self) -> &'static str {
        "ace"
    }
    async fn open(&self, id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.open_classified(id).await.map_err(ProviderError::from)
    }
    async fn open_classified(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderStartFailure> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.entered.notify_one();
            self.release.notified().await;
            Err(ProviderStartFailure::new(
                StartFailureKind::Resolution(
                    ace_swarm::resolve::ResolveFailureKind::InvalidDescriptor,
                ),
                ProviderError::Backend("synthetic late rejection".into()),
            ))
        } else {
            self.active.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(OwnedLive(self.active.clone())))
        }
    }
}
#[tokio::test]
async fn stale_permanent_error_does_not_disable_newer_service_start() {
    let active = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(StaleFailureProvider {
        entered: Default::default(),
        release: Default::default(),
        calls: 0.into(),
        active: active.clone(),
    });
    let mut registry = ProviderRegistry::new();
    registry.register(provider.clone());
    let manager = StreamManager::with_session_limit(registry, 1);
    let sources = SupportSources::parse(Some(r#"{"synthetic":"local:synthetic"}"#)).unwrap();
    let service = SupportService::spawn(manager.clone(), &sources)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), provider.entered.notified())
        .await
        .unwrap();
    service.stop("synthetic").await.unwrap();
    let newer = service.start("synthetic").await.unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    provider.release.notify_one();
    assert!(settle(&service, SupportState::Running, 2).await);
    assert_eq!(service.status()[0].epoch, newer.epoch);
    assert_eq!(service.status()[0].reason, None);
    assert_eq!(active.load(Ordering::SeqCst), 1);
    assert!(service.shutdown().await);
    assert!(manager.shutdown().await);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
}
