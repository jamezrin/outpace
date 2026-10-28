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
    assert_eq!(m.admission_status(), (1, Some(1)));
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
        m.shutdown().await;
        assert_eq!(
            m.admission_status().0,
            1,
            "provider future still owns reservation"
        );
        p.finish.notify_one();
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
    assert_eq!(m.admission_status().0, 0);
    let session = m.get_or_start("synthetic", "live").await.unwrap();
    m.reap_idle_at(Instant::now() + Duration::from_secs(60))
        .await;
    assert!(m.get("synthetic", "live").await.is_none());
    assert_eq!(m.admission_status().0, 1);
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
            manager.shutdown().await;
            first.abort();
            assert!(matches!(first.await, Err(error) if error.is_cancelled()));
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
