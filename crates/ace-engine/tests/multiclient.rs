//! acexy-parity: many clients share one download per stream; distinct streams are
//! independent; one client leaving doesn't disturb the others.

use ace_engine::manager::StreamManager;
use ace_engine::provider::{
    ProviderError, ProviderRegistry, SourceStats, StreamProvider, TsSource,
};
use ace_engine::testprovider::TestProvider;
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;

struct GatedProvider {
    gate: Arc<Semaphore>,
    calls: AtomicUsize,
}
struct GatedSource {
    gate: Arc<Semaphore>,
    started: bool,
    inner: Box<dyn TsSource>,
}
#[async_trait]
impl TsSource for GatedSource {
    async fn next(&mut self) -> Option<bytes::Bytes> {
        if !self.started {
            self.gate.acquire().await.unwrap().forget();
            self.started = true;
        }
        self.inner.next().await
    }
    async fn shutdown(&mut self) {
        self.inner.shutdown().await;
    }
    fn stats(&self) -> SourceStats {
        self.inner.stats()
    }
}
#[async_trait]
impl StreamProvider for GatedProvider {
    fn network(&self) -> &'static str {
        "test"
    }
    async fn open(&self, id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(GatedSource {
            gate: self.gate.clone(),
            started: false,
            inner: TestProvider { chunks: 100 }.open(id).await?,
        }))
    }
}

#[tokio::test]
async fn n_clients_one_source_independent_lifecycles() {
    let mut r = ProviderRegistry::new();
    let provider = Arc::new(GatedProvider {
        gate: Arc::new(Semaphore::new(0)),
        calls: AtomicUsize::new(0),
    });
    r.register(provider.clone());
    let m = StreamManager::new(r);
    let s = m.get_or_start("test", "chan").await.unwrap();

    let mut subs: Vec<_> = (0..4).map(|_| s.subscribe()).collect();
    assert_eq!(s.subscriber_count(), 4);
    let shared = m.get_or_start("test", "chan").await.unwrap();
    assert!(Arc::ptr_eq(&s, &shared));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    // Stage source publication explicitly; finite fixtures may otherwise finish
    // before callers subscribe under cancellation-safe owned initialization.
    provider.gate.add_permits(1);
    // all receive the same first chunk
    let first = subs[0].rx.recv().await.unwrap();
    for sub in subs.iter_mut().skip(1) {
        assert_eq!(sub.rx.recv().await.unwrap(), first);
    }
    // one client leaves; others keep receiving
    subs.pop();
    assert_eq!(s.subscriber_count(), 3);
    let _ = subs[0].rx.recv().await.unwrap();

    // distinct stream gets a distinct session
    let s2 = m.get_or_start("test", "other").await.unwrap();
    assert!(!Arc::ptr_eq(&s, &s2));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert!(m.shutdown().await);
}
