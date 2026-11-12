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
        peer_policy: Default::default(),
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
    let factory: AnnouncerFactory = Arc::new(move |id, _trackers| {
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
    quiet_registry.set_test_announcer(Arc::new(move |_, _trackers| {
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
        .mint("after-shutdown", &[], &seed, 1 << 20)
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

#[tokio::test]
async fn private_runtime_serves_without_binding_peer_port_and_joins_on_shutdown() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    tokio::task::LocalSet::new().run_until(async {
        let dir = temp_dir();
        // Holding this port makes accidental inbound startup observable.
        let peer_guard = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer_guard.local_addr().unwrap();
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = probe.local_addr().unwrap();
        drop(probe);
        let mut config = serving_config(dir.clone(), false);
        config.bind = http_addr;
        config.networks = vec!["ace".into()];
        config.enable_port_mapping = false;
        config.peer_listen = peer_addr;
        config.peer_policy = ace_swarm::peer_policy::PeerPolicy::from_private_peers(
            Some(&peer_addr.to_string()),
        ).unwrap();
        let runtime = crate::runtime::build_runtime(config, vec![]).await.unwrap();
        assert!(runtime.reachability.is_none());
        assert!(runtime.broadcasts.trackers.is_empty());
        assert!(runtime.broadcasts.inbound_peer_port.borrow().is_none());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let serving = tokio::task::spawn_local(async move {
            crate::runtime::serve_http_until(runtime, async { let _ = stopped.await; })
                .await.map_err(|error| error.to_string())
        });
        let observation = timeout(Duration::from_secs(3), async {
            let mut socket = loop {
                match tokio::net::TcpStream::connect(http_addr).await {
                    Ok(socket) => break socket,
                    Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            };
            socket.write_all(b"GET /network-policy HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").await.unwrap();
            let mut bytes = Vec::new();
            socket.take(8192).read_to_end(&mut bytes).await.unwrap();
            bytes
        }).await;
        // Always stop and join before inspecting the request result.
        let _ = stop.send(());
        let joined = timeout(Duration::from_secs(5), serving).await;
        let rebound = tokio::net::TcpListener::bind(http_addr).await;
        drop(peer_guard);
        std::fs::remove_dir_all(dir).unwrap();
        joined.expect("private runtime shutdown deadline").unwrap().unwrap();
        assert!(rebound.is_ok(), "HTTP listener must be released after shutdown");
        let bytes = observation.expect("private runtime HTTP startup deadline");
        let response = std::str::from_utf8(&bytes).unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        let (_, body) = response.split_once("\r\n\r\n").unwrap();
        let policy: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(policy["mode"], "private");
        assert_eq!(policy["allowed_peer_count"], 1);
        assert_eq!(policy["inbound_peer_connections"], false);
        assert_eq!(policy["public_discovery"], false);
        assert_eq!(policy["public_advertising"], false);
        assert!(!body.contains(&peer_addr.to_string()));
    }).await;
}

#[cfg(unix)]
#[tokio::test]
async fn private_runtime_local_vod_alias_delivers_allowed_peer_bytes_over_native_http() {
    use ace_wire::message::PeerMessage;
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use sha1::{Digest, Sha1};
    use std::os::unix::fs::PermissionsExt;
    use tower::ServiceExt;

    let dir = temp_dir();
    let expected = b"synthetic private fixture";
    let mut descriptor = format!(
        "d4:infod6:lengthi{}e4:name9:synthetic12:piece lengthi16384e6:pieces20:",
        expected.len()
    )
    .into_bytes();
    descriptor.extend_from_slice(&Sha1::digest(expected));
    descriptor.extend_from_slice(b"ee");
    let info = ace_swarm::resolve::vod_info_from_transport(&descriptor).unwrap();
    let path = dir.join("fixture.torrent");
    std::fs::write(&path, descriptor).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let mut server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        let mut peer =
            ace_peer::session::PeerSession::new(socket).with_protocol(info.peer_protocol);
        peer.accept_handshake(ace_wire::handshake::random_peer_id(), |hash| {
            hash == &info.infohash
        })
        .await?;
        peer.send(&PeerMessage::Bitfield(vec![0x80])).await?;
        peer.send(&PeerMessage::Unchoke).await?;
        loop {
            match peer.read_message().await {
                Ok(PeerMessage::Request {
                    index,
                    begin,
                    length,
                }) => {
                    if index != 0 || begin as usize + length as usize > expected.len() {
                        return Err(ace_peer::PeerError::Protocol("invalid fixture request"));
                    }
                    peer.send(&PeerMessage::Piece {
                        index,
                        begin,
                        block: expected[begin as usize..(begin + length) as usize].to_vec(),
                    })
                    .await?;
                }
                Ok(_) => {}
                Err(ace_peer::PeerError::Closed) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
    });
    let mut config = serving_config(dir.clone(), false);
    config.networks = vec!["ace".into()];
    config.enable_port_mapping = false;
    config.cache_dir = PathBuf::new();
    config.peer_policy =
        ace_swarm::peer_policy::PeerPolicy::from_private_peers(Some(&endpoint.to_string()))
            .unwrap();
    config.private_descriptors = crate::local_descriptors::DescriptorPaths::from_config(Some(
        &serde_json::json!({"fixture": path}).to_string(),
    ))
    .unwrap();
    let runtime = crate::runtime::build_runtime(config, vec![]).await.unwrap();
    // Startup snapshots the file; requests must not reopen it.
    std::fs::remove_file(&path).unwrap();
    let manager = runtime.manager.clone();
    let broadcasts = runtime.broadcasts.clone();
    let state = crate::http::AppState {
        peer_policy: runtime.config.peer_policy,
        manager: manager.clone(),
        networks: runtime.networks,
        resolve_content_ids_in_getstream: true,
        ace_sessions: Arc::new(crate::http::AceSessionStore::default()),
        experimental_ace_compat: false,
        origin_hls: None,
        broadcasts: Some(broadcasts.clone()),
    };
    let observed = timeout(Duration::from_secs(5), async {
        let response = crate::http::router(state)
            .oneshot(
                Request::builder()
                    .uri("/vod/ace/local:fixture")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        (status, body)
    })
    .await;
    broadcasts.shutdown().await.unwrap();
    manager.shutdown().await;
    let closed = timeout(Duration::from_secs(2), &mut server).await;
    if closed.is_err() {
        server.abort();
        let _ = server.await;
    }
    std::fs::remove_dir_all(dir).unwrap();
    assert!(
        matches!(closed, Ok(Ok(Ok(())))),
        "owned peer cleanup: {closed:?}"
    );
    let (status, body) = observed.expect("bounded local alias playback");
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(body.as_ref(), expected);
    assert_eq!(manager.admission_status().0, 0);
}

#[cfg(unix)]
#[tokio::test]
async fn private_runtime_local_live_alias_delivers_authenticated_source_and_cancels() {
    use ace_wire::bencode::Bencode;
    use axum::{body::Body, http::Request};
    use futures::StreamExt;
    use std::os::unix::fs::PermissionsExt;
    use tower::ServiceExt;

    let dir = temp_dir();
    let auth = ace_wire::live_auth::LiveSourceAuth::generate();
    let mut fields = std::collections::BTreeMap::new();
    fields.insert(b"name".to_vec(), Bencode::Bytes(b"synthetic".to_vec()));
    fields.insert(b"piece_length".to_vec(), Bencode::Int(32768));
    fields.insert(b"chunk_length".to_vec(), Bencode::Int(16384));
    fields.insert(b"bitrate".to_vec(), Bencode::Int(1000000));
    fields.insert(b"authmethod".to_vec(), Bencode::Bytes(b"RSA".to_vec()));
    fields.insert(b"pubkey".to_vec(), Bencode::Bytes(auth.pubkey_der()));
    let descriptor = ace_wire::transport::encode_transport(&Bencode::Dict(fields));
    let info = ace_swarm::resolve::stream_info_from_transport(&descriptor).unwrap();
    let fixture = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/vectors/media/h264-keyframes.ts"
    ));
    let payload: Vec<u8> = fixture
        .iter()
        .copied()
        .cycle()
        .take(32768 - auth.signature_len())
        .collect();
    let mut wire = payload.clone();
    wire.extend(auth.sign(&payload));
    let registry = SeedRegistry::new();
    let (store, lease) = registry.lease_store(info.infohash, || {
        ace_swarm::store::PieceStore::new(32768, 16384, 32768 * 4)
    });
    let header = ace_wire::live_codec::piece_header_from_unix_seconds(1700000000.0);
    for piece in [7, 8] {
        ace_swarm::store::PieceStore::shared_put_complete_piece(&store, piece, header, &wire)
            .await
            .unwrap();
    }
    let path = dir.join("fixture.acelive");
    std::fs::write(&path, descriptor).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let admission = ace_swarm::listen::InboundPolicy {
        max_inbound: 1,
        max_per_stream: 1,
        max_unchoked: 1,
    }
    .admission();
    let mut source = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        admission
            .accept(
                socket,
                registry,
                ace_wire::handshake::random_peer_id(),
                [0; 8],
                Arc::new(ace_wire::identity::Identity::generate()),
                [127, 0, 0, 1],
            )
            .unwrap()
            .await
    });
    let mut config = serving_config(dir.clone(), false);
    config.networks = vec!["ace".into()];
    config.enable_port_mapping = false;
    config.cache_dir = PathBuf::new();
    config.prefetch_pieces = Some(1);
    config.startup_buffer.target_ms = 0;
    config.peer_policy =
        ace_swarm::peer_policy::PeerPolicy::from_private_peers(Some(&endpoint.to_string()))
            .unwrap();
    config.private_descriptors = crate::local_descriptors::DescriptorPaths::from_config(Some(
        &serde_json::json!({"fixture": path}).to_string(),
    ))
    .unwrap();
    let runtime = crate::runtime::build_runtime(config, vec![]).await.unwrap();
    let manager = runtime.manager.clone();
    let broadcasts = runtime.broadcasts.clone();
    let state = crate::http::AppState {
        peer_policy: runtime.config.peer_policy,
        manager: manager.clone(),
        networks: runtime.networks,
        resolve_content_ids_in_getstream: true,
        ace_sessions: Arc::new(crate::http::AceSessionStore::default()),
        experimental_ace_compat: false,
        origin_hls: None,
        broadcasts: Some(broadcasts.clone()),
    };
    let observed = timeout(Duration::from_secs(8), async {
        let response = crate::http::router(state)
            .oneshot(
                Request::builder()
                    .uri("/streams/ace/local:fixture.ts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let mut body = response.into_body().into_data_stream();
        let first = body.next().await;
        drop(body);
        (status, first)
    })
    .await;
    broadcasts.shutdown().await.unwrap();
    manager.shutdown().await;
    let closed = timeout(Duration::from_secs(2), &mut source).await;
    if closed.is_err() {
        source.abort();
        let _ = source.await;
    }
    drop(lease);
    std::fs::remove_dir_all(dir).unwrap();
    assert!(
        matches!(closed, Ok(Ok(Err(ace_peer::PeerError::Closed)))),
        "signed source cleanup: {closed:?}"
    );
    let (status, first) = observed.expect("bounded signed live local alias");
    assert_eq!(status, axum::http::StatusCode::OK);
    let first = first.expect("live output").unwrap();
    assert!(!first.is_empty());
    assert_eq!(first.len() % 188, 0);
    assert!(first
        .as_chunks::<188>()
        .0
        .iter()
        .all(|packet| packet[0] == 0x47));
    assert_eq!(manager.admission_status().0, 0);
}

#[tokio::test]
async fn serving_shutdown_reports_manager_cleanup_failure() {
    use crate::provider::{ProviderError, ProviderRegistry, SourceStats, StreamProvider, TsSource};
    use async_trait::async_trait;
    struct FailedSource;
    #[async_trait]
    impl TsSource for FailedSource {
        async fn next(&mut self) -> Option<bytes::Bytes> {
            std::future::pending().await
        }
        async fn shutdown(&mut self) {
            panic!("synthetic daemon cleanup failure")
        }
        fn stats(&self) -> SourceStats {
            SourceStats::default()
        }
    }
    struct Provider;
    #[async_trait]
    impl StreamProvider for Provider {
        fn network(&self) -> &'static str {
            "cleanup-test"
        }
        async fn open(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderError> {
            Ok(Box::new(FailedSource))
        }
    }
    let dir = temp_dir();
    let mut runtime = crate::runtime::build_runtime(serving_config(dir.clone(), false), vec![])
        .await
        .unwrap();
    assert!(runtime.manager.shutdown().await);
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(Provider));
    let manager = crate::manager::StreamManager::with_session_limit(registry, 1);
    let held = manager
        .get_or_start("cleanup-test", "synthetic")
        .await
        .unwrap();
    runtime.manager = manager.clone();
    let result = timeout(
        Duration::from_secs(3),
        crate::runtime::serve_http_until(runtime, async {}),
    )
    .await;
    let occupancy = manager.admission_status().0;
    drop(held);
    drop(manager);
    std::fs::remove_dir_all(dir).unwrap();
    let error = result
        .expect("bounded daemon cleanup")
        .expect_err("daemon must not exit successfully after unconfirmed manager cleanup");
    assert_eq!(error.to_string(), "stream manager cleanup failed");
    assert_eq!(occupancy, 1);
}
