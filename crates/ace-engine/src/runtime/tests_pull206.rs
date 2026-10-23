//! Entry-owned announcement lifetimes exercised through BroadcastState/Registry.

use crate::broadcast::{AnnouncerFactory, BroadcastRegistry};
use crate::broadcast_persist::BroadcastPersist;
use crate::broadcast_persist::{PersistOperation, TestPersistIo};
use crate::broadcast_pull::{
    BroadcastSourceConfig, SourceTestHooks, TestSourceBody, TestSourceOpen,
};
use crate::http::BroadcastState;
use ace_swarm::http_fetch::{FetchError, FetchPolicy};
use ace_swarm::listen::SeedRegistry;
use async_trait::async_trait;
use bytes::Bytes;
use std::future::pending;
use std::io;
use std::path::Path;
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

#[tokio::test]
async fn pull206_startup_validation_and_unwind() {
    let dir = temp_dir();
    let mut invalid = crate::config::Config {
        data_dir: dir.clone(),
        networks: vec![],
        enable_inbound: false,
        seed_ttl_secs: 0,
        ..crate::config::Config::default()
    };
    invalid.broadcast_sources.insert(
        "invalid-source".into(),
        crate::broadcast_pull::BroadcastSourceConfig {
            url: "https://source.invalid/live".into(),
            connect_timeout_secs: 5,
            read_timeout_secs: 15,
            reconnect_initial_secs: 31,
            reconnect_max_secs: 30,
        },
    );

    let result = crate::runtime::build_runtime(invalid, vec![]).await;
    assert!(result.is_err());
    assert!(
        !dir.join("identity.seed").exists(),
        "invalid programmatic declarations must fail before identity creation"
    );
    assert!(
        !dir.join("broadcasts").exists(),
        "invalid programmatic declarations must not create persisted broadcast state"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn pull206_build_runtime_validates_sources_without_starting_workers_or_announcers() {
    let dir = temp_dir();
    let mut config = crate::config::Config {
        data_dir: dir.clone(),
        networks: vec![],
        enable_inbound: false,
        seed_ttl_secs: 0,
        ..crate::config::Config::default()
    };
    let defaults = crate::config::Config::default();
    assert!(defaults.broadcast_sources.is_empty());
    assert!(!defaults.allow_non_global_broadcast_source);
    config.broadcast_sources.insert(
        "declaration".into(),
        crate::broadcast_pull::BroadcastSourceConfig {
            url: "https://source.invalid/live?token=SYNTHETIC_RUNTIME_MARKER".into(),
            connect_timeout_secs: 5,
            read_timeout_secs: 15,
            reconnect_initial_secs: 1,
            reconnect_max_secs: 30,
        },
    );

    let runtime = crate::runtime::build_runtime(config, vec![]).await.unwrap();
    assert!(runtime
        .broadcasts
        .registry
        .get("declaration")
        .await
        .is_none());
    assert!(
        !std::fs::read_dir(dir.join("broadcasts"))
            .map(|mut entries| entries.next().is_some())
            .unwrap_or(false),
        "build_runtime validates declarations but defers readers and attachments to serving"
    );
    runtime.broadcasts.shutdown().await.unwrap();
    runtime.manager.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}

struct IdleSource {
    dropped: Option<mpsc::UnboundedSender<()>>,
}

#[async_trait]
impl TestSourceBody for IdleSource {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>, FetchError> {
        pending().await
    }
}

impl Drop for IdleSource {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(());
        }
    }
}

fn local_source_config(path: &str) -> BroadcastSourceConfig {
    BroadcastSourceConfig {
        url: format!("http://source.invalid/{path}"),
        connect_timeout_secs: 5,
        read_timeout_secs: 15,
        reconnect_initial_secs: 1,
        reconnect_max_secs: 2,
    }
}

fn idle_source_hooks(
    opened: mpsc::UnboundedSender<()>,
    dropped: Option<mpsc::UnboundedSender<()>>,
) -> SourceTestHooks {
    let open: TestSourceOpen = Arc::new(move |_, _, _, _| {
        let opened = opened.clone();
        let dropped = dropped.clone();
        Box::pin(async move {
            let _ = opened.send(());
            Ok(Box::new(IdleSource { dropped }) as Box<dyn TestSourceBody>)
        })
    });
    SourceTestHooks {
        open: Some(open),
        ..SourceTestHooks::default()
    }
}

fn serving_config(dir: PathBuf, declarations: bool) -> crate::config::Config {
    let mut config = crate::config::Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        rtmp_bind: "127.0.0.1:0".parse().unwrap(),
        data_dir: dir,
        networks: vec![],
        enable_inbound: false,
        seed_ttl_secs: 0,
        allow_non_global_broadcast_source: true,
        ..crate::config::Config::default()
    };
    if declarations {
        config
            .broadcast_sources
            .insert("declared".into(), local_source_config("declared"));
    }
    config
}

#[tokio::test]
async fn pull206_restart_attachment_contract_declaration_and_api_only_sources() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = temp_dir();
            let (opened_tx, mut opened_rx) = mpsc::unbounded_channel();

            // A declaration is attached only when serving starts, and detaching it suppresses it only
            // for that process. The same current declaration reattaches after a fresh runtime build.
            let runtime = crate::runtime::build_runtime(serving_config(dir.clone(), true), vec![])
                .await
                .unwrap();
            runtime
                .broadcasts
                .registry
                .set_source_test_hooks(idle_source_hooks(opened_tx.clone(), None));
            assert!(runtime
                .broadcasts
                .source_status("declared")
                .await
                .unwrap()
                .is_none());
            let broadcasts = runtime.broadcasts.clone();
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
            let serving = tokio::task::spawn_local(async move {
                crate::runtime::serve_http_until(runtime, async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .map_err(|error| error.to_string())
            });
            timeout(Duration::from_secs(4), opened_rx.recv())
                .await
                .expect("declaration worker should start during serving startup")
                .unwrap();
            broadcasts.detach_source("declared").await.unwrap();
            let _ = shutdown_tx.send(());
            timeout(Duration::from_secs(5), serving)
                .await
                .expect("serving cleanup should finish")
                .unwrap()
                .unwrap();

            let runtime = crate::runtime::build_runtime(serving_config(dir.clone(), true), vec![])
                .await
                .unwrap();
            runtime
                .broadcasts
                .registry
                .set_source_test_hooks(idle_source_hooks(opened_tx.clone(), None));
            let broadcasts = runtime.broadcasts.clone();
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
            let serving = tokio::task::spawn_local(async move {
                crate::runtime::serve_http_until(runtime, async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .map_err(|error| error.to_string())
            });
            timeout(Duration::from_secs(4), opened_rx.recv())
                .await
                .expect("current declaration should reattach after restart")
                .unwrap();

            // API attachments use the same registry/worker, but no URL is persisted as restart state.
            let api_config = crate::broadcast_pull::validate_source(
                local_source_config("api-only"),
                FetchPolicy {
                    allow_non_global: true,
                },
            )
            .unwrap();
            broadcasts
                .attach_source("api-only", api_config)
                .await
                .unwrap();
            timeout(Duration::from_secs(4), opened_rx.recv())
                .await
                .expect("API worker should start in its current process")
                .unwrap();
            let api_identity = broadcasts.registry.get("api-only").await.unwrap();
            let _ = shutdown_tx.send(());
            timeout(Duration::from_secs(5), serving)
                .await
                .expect("serving cleanup should finish")
                .unwrap()
                .unwrap();

            let runtime = crate::runtime::build_runtime(serving_config(dir.clone(), true), vec![])
                .await
                .unwrap();
            assert!(runtime.broadcasts.registry.get("api-only").await.is_some());
            assert!(runtime
                .broadcasts
                .source_status("api-only")
                .await
                .unwrap()
                .is_none());
            assert_eq!(
                runtime
                    .broadcasts
                    .registry
                    .get("api-only")
                    .await
                    .unwrap()
                    .infohash,
                api_identity.infohash
            );
            assert!(runtime
                .broadcasts
                .source_status("declared")
                .await
                .unwrap()
                .is_none());
            runtime.broadcasts.shutdown().await.unwrap();
            runtime.manager.shutdown().await;

            // Removing a declaration leaves it absent on the next rebuild while preserving its
            // descriptor identity under the documented detach/source restart contract.
            let runtime = crate::runtime::build_runtime(serving_config(dir.clone(), false), vec![])
                .await
                .unwrap();
            assert!(runtime
                .broadcasts
                .source_status("declared")
                .await
                .unwrap()
                .is_none());
            assert!(runtime.broadcasts.registry.get("declared").await.is_some());
            runtime.broadcasts.shutdown().await.unwrap();
            runtime.manager.shutdown().await;
            let _ = std::fs::remove_dir_all(dir);
        })
        .await;
}

struct FailSecondReservation {
    gate: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
}

impl TestPersistIo for FailSecondReservation {
    fn before(
        &self,
        operation: PersistOperation,
        _path: &Path,
        other: Option<&Path>,
    ) -> io::Result<()> {
        let is_second_reservation = operation == PersistOperation::Replace
            && other
                .and_then(Path::file_name)
                .is_some_and(|name| name == "b-declaration.json");
        if is_second_reservation {
            let (ready, condition) = &*self.gate;
            let mut ready = ready.lock().unwrap();
            while !*ready {
                ready = condition.wait(ready).unwrap();
            }
            return Err(io::Error::other("synthetic reservation failure"));
        }
        Ok(())
    }
}

#[tokio::test]
async fn pull206_startup_failure_unwinds_prior_worker_and_releases_http_listener() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = temp_dir();
            let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let http_addr = probe.local_addr().unwrap();
            drop(probe);
            let mut config = serving_config(dir.clone(), false);
            config.bind = http_addr;
            config
                .broadcast_sources
                .insert("a-declaration".into(), local_source_config("a"));
            config
                .broadcast_sources
                .insert("b-declaration".into(), local_source_config("b"));
            let mut runtime = crate::runtime::build_runtime(config, vec![]).await.unwrap();
            let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
            runtime.broadcasts.registry =
                BroadcastRegistry::with_test_persist(BroadcastPersist::with_test_io(
                    &dir,
                    Arc::new(FailSecondReservation { gate: gate.clone() }),
                ));
            runtime.broadcasts.registry.set_source_policy(FetchPolicy {
                allow_non_global: true,
            });
            let (opened_tx, mut opened_rx) = mpsc::unbounded_channel();
            let (dropped_tx, mut dropped_rx) = mpsc::unbounded_channel();
            runtime
                .broadcasts
                .registry
                .set_source_test_hooks(idle_source_hooks(opened_tx, Some(dropped_tx)));
            let registry = runtime.broadcasts.registry.clone();
            let http_addr = runtime.config.bind;
            let serving = tokio::task::spawn_local(async move {
                crate::runtime::serve_http_until(runtime, pending::<()>())
                    .await
                    .map_err(|error| error.to_string())
            });

            timeout(Duration::from_secs(4), opened_rx.recv())
                .await
                .expect("first sorted declaration should own a live worker before second admission")
                .unwrap();
            {
                let (ready, condition) = &*gate;
                *ready.lock().unwrap() = true;
                condition.notify_all();
            }
            let error = timeout(Duration::from_secs(5), serving)
                .await
                .expect("startup failure cleanup should finish")
                .unwrap()
                .expect_err("second declaration reservation should fail startup");
            assert!(error.contains("broadcast_persistence_failed"));
            timeout(Duration::from_secs(3), dropped_rx.recv())
                .await
                .expect("startup unwind must drop the earlier source body")
                .expect("body drop notification should be sent");
            assert!(matches!(
                registry.source_status("a-declaration").await,
                Err(crate::broadcast::BroadcastError::ServiceStopping)
            ));

            // serve_http_until has returned only after its owned HTTP listener was dropped.
            let rebound = tokio::net::TcpListener::bind(http_addr).await;
            assert!(
                rebound.is_ok(),
                "failed startup must release its HTTP listener"
            );
            drop(rebound);
            let _ = std::fs::remove_dir_all(dir);
        })
        .await;
}
