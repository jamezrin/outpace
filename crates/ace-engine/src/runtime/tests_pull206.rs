//! Entry-owned announcement lifetimes exercised through BroadcastState/Registry.

use crate::broadcast::{AnnouncerFactory, BroadcastRegistry};
use crate::broadcast_persist::BroadcastPersist;
use crate::http::BroadcastState;
use ace_swarm::listen::SeedRegistry;
use std::future::pending;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::time::{timeout, Duration};

fn state(
    registry: Arc<BroadcastRegistry>,
    seed_registry: SeedRegistry,
    inbound: Option<u16>,
) -> BroadcastState {
    BroadcastState {
        registry,
        seed_registry,
        trackers: vec![],
        store_bytes: 2 << 20,
        inbound_peer_port: watch::channel(inbound).1,
    }
}

fn temp_dir() -> PathBuf {
    let path = std::env::temp_dir().join(format!("outpace-runtime206-{}", rand::random::<u64>()));
    std::fs::create_dir_all(&path).unwrap();
    path
}

#[tokio::test]
async fn pull206_announcements_are_entry_owned_idempotent_and_canceled_on_shutdown() {
    let dir = temp_dir();
    let seed = SeedRegistry::new();
    let registry =
        BroadcastRegistry::with_persist(&dir, crate::config::CacheType::Memory, PathBuf::new());
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (dropped_tx, mut dropped_rx) = mpsc::unbounded_channel();
    let starts = Arc::new(AtomicUsize::new(0));
    let start_counter = starts.clone();
    let factory: AnnouncerFactory = Arc::new(move |id| {
        let started = started_tx.clone();
        let dropped = dropped_tx.clone();
        let starts = start_counter.clone();
        Box::pin(async move {
            struct DropNotice(mpsc::UnboundedSender<()>);
            impl Drop for DropNotice {
                fn drop(&mut self) {
                    let _ = self.0.send(());
                }
            }
            starts.fetch_add(1, Ordering::SeqCst);
            let _guard = DropNotice(dropped);
            let _ = started.send(id);
            pending::<()>().await;
        })
    });
    registry.set_test_announcer(factory);
    let runtime_state = state(registry.clone(), seed.clone(), Some(4661));
    let original = runtime_state.mint("owned").await.unwrap();

    runtime_state.ensure_announcing("owned").await.unwrap();
    runtime_state.ensure_announcing("owned").await.unwrap();
    let first = timeout(Duration::from_secs(2), started_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let second = timeout(Duration::from_secs(2), started_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        first, second,
        "the real registry starts one announce task for each identity hash"
    );
    assert_eq!(starts.load(Ordering::SeqCst), 2);

    runtime_state.close_admissions();
    runtime_state.shutdown().await.unwrap();
    timeout(Duration::from_secs(2), dropped_rx.recv())
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(2), dropped_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        BroadcastPersist::new(&dir)
            .load("owned")
            .unwrap()
            .unwrap()
            .reserved_until,
        Some(128)
    );
    assert!(
        !seed.serves(&original.infohash),
        "shutdown releases the real seed lease"
    );
    assert!(!seed.serves(&original.content_id));

    let quiet_registry = BroadcastRegistry::new();
    let quiet_starts = Arc::new(AtomicUsize::new(0));
    let quiet_counter = quiet_starts.clone();
    quiet_registry.set_test_announcer(Arc::new(move |_| {
        quiet_counter.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { pending::<()>().await })
    }));
    let quiet_state = state(quiet_registry.clone(), SeedRegistry::new(), None);
    quiet_state.mint("quiet").await.unwrap();
    quiet_state.ensure_announcing("quiet").await.unwrap();
    assert_eq!(
        quiet_starts.load(Ordering::SeqCst),
        0,
        "no inbound listener means no announce tasks"
    );
    quiet_state.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn pull206_real_serving_wrapper_stops_owned_listeners_and_preserves_broadcast_record() {
    let dir = temp_dir();
    let runtime = crate::runtime::build_runtime(
        crate::config::Config {
            bind: "127.0.0.1:0".parse().unwrap(),
            rtmp_bind: "127.0.0.1:0".parse().unwrap(),
            data_dir: dir.clone(),
            networks: vec![],
            enable_inbound: false,
            seed_ttl_secs: 0,
            ..crate::config::Config::default()
        },
        vec![],
    )
    .await
    .unwrap();
    let registry = runtime.broadcasts.registry.clone();
    let seed = runtime.broadcasts.seed_registry.clone();
    let broadcast = runtime.broadcasts.mint("served").await.unwrap();

    crate::runtime::serve_http_until(runtime, async {})
        .await
        .unwrap();
    assert!(registry
        .mint("after-shutdown", "After", &[], &seed, 1 << 20)
        .await
        .is_err());
    assert!(BroadcastPersist::new(&dir)
        .load("served")
        .unwrap()
        .is_some());
    assert!(!seed.serves(&broadcast.infohash));
    let _ = std::fs::remove_dir_all(dir);
}
