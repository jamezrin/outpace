//! acexy-parity: many clients share one download per stream; distinct streams are
//! independent; one client leaving doesn't disturb the others.

use ace_engine::manager::StreamManager;
use ace_engine::provider::ProviderRegistry;
use ace_engine::testprovider::TestProvider;
use std::sync::Arc;

#[tokio::test]
async fn n_clients_one_source_independent_lifecycles() {
    let mut r = ProviderRegistry::new();
    r.register(Arc::new(TestProvider { chunks: 100 }));
    let m = StreamManager::new(r);
    let s = m.get_or_start("test", "chan").await.unwrap();

    let mut subs: Vec<_> = (0..4).map(|_| s.subscribe()).collect();
    assert_eq!(s.subscriber_count(), 4);
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
}
