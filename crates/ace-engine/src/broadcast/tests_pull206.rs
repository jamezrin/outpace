//! Reservation and producer invariants exercised through the real registry and piece store.

use super::{
    BroadcastError, BroadcastRegistry, ProducerKind, CURSOR_PERSIST_INTERVAL, PIECE_LENGTH,
};
use crate::broadcast_persist::{
    tests_pull206::persistence_observer, BroadcastPersist, PersistOperation, PersistedBroadcast,
};
use crate::broadcast_pull::{
    validate_source, BroadcastSourceConfig, SourceErrorCode, SourceState, SourceTestHooks,
    TestSourceBody, TestSourceOpen,
};
use crate::config::CacheType;
use ace_swarm::http_fetch::{FetchError, FetchPolicy};
use ace_swarm::listen::SeedRegistry;
use async_trait::async_trait;
use bytes::Bytes;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tower::ServiceExt;

const POLICY: FetchPolicy = FetchPolicy {
    allow_non_global: true,
};

struct PullFrames(mpsc::UnboundedReceiver<Result<Option<Bytes>, FetchError>>);

#[async_trait]
impl TestSourceBody for PullFrames {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>, FetchError> {
        self.0.recv().await.unwrap_or(Err(FetchError::Cancelled))
    }
}

fn source_config(url: String) -> BroadcastSourceConfig {
    BroadcastSourceConfig {
        url,
        connect_timeout_secs: 5,
        read_timeout_secs: 15,
        reconnect_initial_secs: 1,
        reconnect_max_secs: 30,
    }
}

fn source(url: &str) -> BroadcastSourceConfig {
    source_config(url.to_owned())
}

fn ts_packets(count: usize, marker: u8) -> Bytes {
    let mut body = Vec::with_capacity(count * 188);
    for _ in 0..count {
        body.push(0x47);
        body.extend(std::iter::repeat_n(marker, 187));
    }
    Bytes::from(body)
}

fn test_broadcast_state(
    registry: Arc<BroadcastRegistry>,
    policy: FetchPolicy,
) -> crate::http::BroadcastState {
    registry.set_source_policy(policy);
    crate::http::BroadcastState {
        registry,
        seed_registry: SeedRegistry::new(),
        trackers: vec![],
        store_bytes: 4 << 20,
        inbound_peer_port: watch::channel(None).1,
    }
}

async fn attach_test_source(
    state: &crate::http::BroadcastState,
    name: &str,
    config: BroadcastSourceConfig,
    policy: FetchPolicy,
) -> (super::Broadcast, crate::broadcast_pull::SourceStatus) {
    state
        .attach_source(name, validate_source(config, policy).unwrap())
        .await
        .unwrap()
}

async fn wait_status_with_state(
    state: &crate::http::BroadcastState,
    name: &str,
    predicate: impl Fn(&crate::broadcast_pull::SourceStatus) -> bool,
) -> crate::broadcast_pull::SourceStatus {
    for _ in 0..10_000 {
        let status = state.source_status(name).await.unwrap().unwrap();
        if predicate(&status) {
            return status;
        }
        tokio::task::yield_now().await;
    }
    panic!("source status did not reach the expected transition");
}

struct ReleaseDiskGate(Option<std::sync::mpsc::SyncSender<()>>);

impl ReleaseDiskGate {
    fn release(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

impl Drop for ReleaseDiskGate {
    fn drop(&mut self) {
        self.release();
    }
}

fn temp_dir() -> PathBuf {
    let path = std::env::temp_dir().join(format!("outpace-pull206-{}", rand::random::<u64>()));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn ts(bytes: usize) -> Vec<u8> {
    let packets = bytes.div_ceil(188);
    let mut body = Vec::with_capacity(packets * 188);
    for packet in 0..packets {
        body.push(0x47);
        body.extend(std::iter::repeat_n((packet % 251) as u8, 187));
    }
    body
}

fn diskless_registry() -> (Arc<BroadcastRegistry>, SeedRegistry) {
    (BroadcastRegistry::new(), SeedRegistry::new())
}

#[tokio::test]
async fn pull206_reservations_survive_repeated_restart() {
    let dir = temp_dir();
    let seed1 = SeedRegistry::new();
    let reg1 = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
    let (first, mut owner1) = reg1
        .acquire_producer("alpha", "Alpha", &[], &seed1, 8 << 20, ProducerKind::Pull)
        .await
        .unwrap();
    let mut ingest = owner1.open_ingest().unwrap();
    let progress = ingest
        .push_bytes(&ts((PIECE_LENGTH * 3) as usize))
        .await
        .unwrap();
    assert_eq!(progress.committed_pieces, 3);
    drop(ingest); // A retry drops partial state without finishing a finite tail.
    drop(owner1);

    let seed2 = SeedRegistry::new();
    let reg2 = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
    let (second, owner2) = reg2
        .acquire_producer("alpha", "Alpha", &[], &seed2, 8 << 20, ProducerKind::Pull)
        .await
        .unwrap();
    assert_eq!(second.infohash, first.infohash);
    assert_eq!(second.content_id, first.content_id);
    assert_eq!(second.cursor.start_piece(), CURSOR_PERSIST_INTERVAL);
    assert_eq!(second.cursor.reserved_until(), CURSOR_PERSIST_INTERVAL * 2);
    drop(owner2);

    let seed3 = SeedRegistry::new();
    let reg3 = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
    let (third, _owner3) = reg3
        .acquire_producer("alpha", "Alpha", &[], &seed3, 8 << 20, ProducerKind::RawPut)
        .await
        .unwrap();
    assert_eq!(third.infohash, first.infohash);
    assert_eq!(third.cursor.start_piece(), CURSOR_PERSIST_INTERVAL * 2);
    assert_eq!(third.cursor.reserved_until(), CURSOR_PERSIST_INTERVAL * 3);

    third.cursor.flush().await.unwrap();
    let rec = BroadcastPersist::new(&dir).load("alpha").unwrap().unwrap();
    assert_eq!(rec.reserved_until, Some(CURSOR_PERSIST_INTERVAL * 3));
    assert_eq!(rec.next_piece, CURSOR_PERSIST_INTERVAL * 2);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn pull206_legacy_and_invalid_records() {
    let dir = temp_dir();
    let registry = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
    let seed1 = SeedRegistry::new();
    let (bc, _owner) = registry
        .acquire_producer(
            "legacy",
            "Legacy",
            &[],
            &seed1,
            1 << 20,
            ProducerKind::RawPut,
        )
        .await
        .unwrap();

    // A pre-reservation-format record keeps its committed cursor. Admission migrates from
    // checked n+128 and immediately persists one further exclusive block.
    let path = dir.join("broadcasts/legacy.json");
    let mut old: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    old.as_object_mut().unwrap().remove("reserved_until");
    old["next_piece"] = serde_json::json!(17);
    std::fs::write(&path, serde_json::to_vec(&old).unwrap()).unwrap();
    let seed2 = SeedRegistry::new();
    let resumed_registry = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
    let (resumed, _owner2) = resumed_registry
        .acquire_producer("legacy", "Legacy", &[], &seed2, 1 << 20, ProducerKind::Pull)
        .await
        .unwrap();
    assert_eq!(resumed.infohash, bc.infohash);
    assert_eq!(resumed.cursor.start_piece(), 17 + CURSOR_PERSIST_INTERVAL);
    assert_eq!(
        resumed.cursor.reserved_until(),
        17 + 2 * CURSOR_PERSIST_INTERVAL
    );

    std::fs::write(dir.join("broadcasts/broken.json"), b"{not-json").unwrap();
    let persist = BroadcastPersist::new(&dir);
    assert!(matches!(persist.load("absent"), Ok(None)));
    assert!(persist.load("broken").is_err());
    let seed3 = SeedRegistry::new();
    let invalid_registry = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
    assert!(invalid_registry
        .acquire_producer("broken", "Broken", &[], &seed3, 1 << 20, ProducerKind::Pull)
        .await
        .is_err());
    assert!(
        !seed3.serves(&bc.infohash),
        "invalid record must not create seed state"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn pull206_reserve_before_publish() {
    let dir = temp_dir();
    let observer = persistence_observer();
    let persist = BroadcastPersist::with_test_io(&dir, observer.clone());
    let registry = BroadcastRegistry::with_test_persist(persist);
    let seed = SeedRegistry::new();
    let (broadcast, mut session) = registry
        .acquire_producer(
            "boundary",
            "Boundary",
            &[],
            &seed,
            4 << 20,
            ProducerKind::Pull,
        )
        .await
        .unwrap();
    assert_eq!(broadcast.cursor.reserved_until(), CURSOR_PERSIST_INTERVAL);

    // Position the real allocator at the last covered index. One piece can publish; the next
    // must first extend the durable range and must fail closed when that save fails.
    broadcast
        .cursor
        .next
        .store(CURSOR_PERSIST_INTERVAL - 1, Ordering::SeqCst);
    let mut ingest = session.open_ingest().unwrap();
    observer.fail_next(PersistOperation::FileSync);
    assert!(ingest
        .push_bytes(&ts((PIECE_LENGTH * 2) as usize))
        .await
        .is_err());
    let store = broadcast.store.lock().await;
    assert!(store.chunk(CURSOR_PERSIST_INTERVAL - 1, 0).is_some());
    assert!(store.chunk(CURSOR_PERSIST_INTERVAL, 0).is_none());
    drop(store);
    assert_eq!(broadcast.cursor.reserved_until(), CURSOR_PERSIST_INTERVAL);
    assert!(
        broadcast.cursor.check().is_err(),
        "failed reservation poisons the producer generation"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn pull206_overflow_and_identity_mismatch_fail_closed() {
    let dir = temp_dir();
    let registry = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
    let seed = SeedRegistry::new();
    let broadcast = registry
        .mint("overflow", "Overflow", &[], &seed, 1 << 20)
        .await
        .unwrap();
    let persist = BroadcastPersist::new(&dir);

    let near_end = u64::MAX - (CURSOR_PERSIST_INTERVAL - 1);
    persist
        .save(
            "overflow",
            &PersistedBroadcast {
                transport: (*broadcast.transport_bytes).clone(),
                key_pkcs1_pem: broadcast.auth.to_pkcs1_pem(),
                next_piece: near_end,
                reserved_until: Some(near_end),
            },
        )
        .unwrap();
    let overflow_seed = SeedRegistry::new();
    let overflow_registry =
        BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
    assert!(overflow_registry
        .acquire_producer(
            "overflow",
            "Overflow",
            &[],
            &overflow_seed,
            1 << 20,
            ProducerKind::Pull
        )
        .await
        .is_err());
    assert!(!overflow_seed.serves(&broadcast.infohash));

    let mismatch_identity = registry
        .mint("mismatch", "Mismatch", &[], &seed, 1 << 20)
        .await
        .unwrap();
    let mismatched_key = ace_wire::live_auth::LiveSourceAuth::generate();
    persist
        .save(
            "mismatch",
            &PersistedBroadcast {
                transport: (*mismatch_identity.transport_bytes).clone(),
                key_pkcs1_pem: mismatched_key.to_pkcs1_pem(),
                next_piece: 0,
                reserved_until: Some(CURSOR_PERSIST_INTERVAL),
            },
        )
        .unwrap();
    let mismatch_seed = SeedRegistry::new();
    let mismatch_registry =
        BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
    assert!(mismatch_registry
        .acquire_producer(
            "mismatch",
            "Mismatch",
            &[],
            &mismatch_seed,
            1 << 20,
            ProducerKind::Pull
        )
        .await
        .is_err());
    assert!(!mismatch_seed.serves(&broadcast.infohash));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn pull206_exclusive_finite_ingress() {
    let (registry, seed) = diskless_registry();
    let (broadcast, mut session) = registry
        .acquire_producer(
            "exclusive",
            "Exclusive",
            &[],
            &seed,
            2 << 20,
            ProducerKind::RawPut,
        )
        .await
        .unwrap();

    let mut first = session.open_ingest().unwrap();
    assert!(
        session.open_ingest().is_err(),
        "one producer may have one active ingest"
    );
    assert!(registry
        .acquire_producer(
            "exclusive",
            "Exclusive",
            &[],
            &seed,
            2 << 20,
            ProducerKind::Rtmp
        )
        .await
        .is_err());

    first.push_bytes(&ts(188 * 5)).await.unwrap();
    assert_eq!(
        broadcast.cursor.start_piece(),
        0,
        "short tail remains buffered until EOF"
    );
    first.finish().await.unwrap();
    assert_eq!(
        broadcast.cursor.start_piece(),
        1,
        "finite EOF publishes one signed short tail"
    );
    first.finish().await.unwrap();
    assert_eq!(
        broadcast.cursor.start_piece(),
        1,
        "repeated finish cannot publish twice"
    );

    drop(first);
    let _retry = session.open_ingest().unwrap();
    assert!(
        registry
            .acquire_producer(
                "exclusive",
                "Exclusive",
                &[],
                &seed,
                2 << 20,
                ProducerKind::Pull
            )
            .await
            .is_err(),
        "dropping an attempt releases only its ingest permit"
    );
}

#[tokio::test(start_paused = true)]
async fn pull206_blocked_save_is_fenced_by_delete_and_recreate() {
    let dir = temp_dir();
    let observer = persistence_observer();
    let persist = BroadcastPersist::with_test_io(&dir, observer.clone());
    let registry = BroadcastRegistry::with_test_persist(persist);
    let seed = SeedRegistry::new();
    let (broadcast, mut session) = registry
        .acquire_producer("fenced", "Fenced", &[], &seed, 2 << 20, ProducerKind::Pull)
        .await
        .unwrap();
    let identity = broadcast.infohash;

    // Commit the last covered piece, then block the actual durable extension needed before
    // the next piece can publish. The observer's real watchdog is independent of paused time.
    broadcast
        .cursor
        .next
        .store(CURSOR_PERSIST_INTERVAL - 1, Ordering::SeqCst);
    let ingest = session.open_ingest().unwrap();
    let (entered, release) = observer.gate_with_watchdog(
        PersistOperation::FileSync,
        std::time::Duration::from_secs(30),
    );
    let waiter = tokio::spawn(async move {
        let mut ingest = ingest;
        ingest.push_bytes(&ts((PIECE_LENGTH * 2) as usize)).await
    });
    tokio::task::spawn_blocking(move || {
        entered
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap()
    })
    .await
    .unwrap();

    waiter.abort();
    assert!(
        waiter.await.unwrap_err().is_cancelled(),
        "the ingest waiter is canceled while the reservation save remains owned"
    );
    drop(session);

    let delete1 = registry.delete("fenced");
    tokio::pin!(delete1);
    assert!(futures::poll!(delete1.as_mut()).is_pending());
    let entry = registry
        .by_name
        .lock()
        .unwrap()
        .get("fenced")
        .unwrap()
        .clone();
    let (first_completion, first_deadline) = {
        let stop = entry.stop.lock().unwrap();
        let stop = stop.as_ref().unwrap();
        (stop.completion.clone(), stop.deadline)
    };

    let delete2 = registry.delete("fenced");
    tokio::pin!(delete2);
    assert!(futures::poll!(delete2.as_mut()).is_pending());
    assert!(registry.get("fenced").await.is_none());

    let (completion, deadline) = {
        let stop = entry.stop.lock().unwrap();
        let stop = stop.as_ref().unwrap();
        (stop.completion.clone(), stop.deadline)
    };
    assert!(
        first_completion.same_channel(&completion),
        "the second DELETE joins the first DELETE's stop completion"
    );
    assert_eq!(
        first_deadline, deadline,
        "concurrent DELETE calls retain the shared cleanup deadline"
    );

    tokio::time::advance(super::CLEANUP_GRACE).await;
    assert_eq!(delete1.await, Err(BroadcastError::CleanupTimeout));
    assert_eq!(delete2.await, Err(BroadcastError::CleanupTimeout));
    assert!(
        entry.state.data.lock().unwrap().stopping,
        "timeout retains the stopping entry"
    );
    assert!(
        registry
            .acquire_producer("fenced", "Fenced", &[], &seed, 2 << 20, ProducerKind::Pull)
            .await
            .is_err(),
        "admission remains closed while the filesystem gate is held"
    );
    assert!(
        observer
            .events()
            .iter()
            .all(|event| event.operation != PersistOperation::Delete),
        "delete has not crossed the blocked real save"
    );
    assert!(
        completion.borrow().is_none(),
        "owned cleanup continues after both callers time out"
    );

    release.send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            super::completed(completion.clone()),
        )
        .await
        .expect("the owned stop continuation settles after gate release"),
        Ok(())
    );
    assert_eq!(
        observer
            .events()
            .iter()
            .filter(|event| event.operation == PersistOperation::Delete)
            .count(),
        1,
        "the shared stop performs one durable delete after releasing its gate"
    );
    assert!(BroadcastPersist::new(&dir)
        .load("fenced")
        .unwrap()
        .is_none());
    let (recreated, _new_session) = registry
        .acquire_producer("fenced", "Fenced", &[], &seed, 2 << 20, ProducerKind::Pull)
        .await
        .unwrap();
    assert_ne!(
        recreated.infohash, identity,
        "fresh identity is admitted only after settled deletion"
    );
    assert_eq!(recreated.cursor.start_piece(), 0);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn pull206_settled_delete_failure_requires_explicit_retry() {
    let dir = temp_dir();
    let observer = persistence_observer();
    let persist = BroadcastPersist::with_test_io(&dir, observer.clone());
    let registry = BroadcastRegistry::with_test_persist(persist);
    let seed = SeedRegistry::new();
    let broadcast = registry
        .mint("retry-delete", "Retry delete", &[], &seed, 2 << 20)
        .await
        .unwrap();
    observer.fail_at(PersistOperation::Delete, 1);

    assert_eq!(
        registry.delete("retry-delete").await,
        Err(BroadcastError::PersistenceFailed)
    );
    assert!(registry.get("retry-delete").await.is_none());
    assert!(
        registry
            .acquire_producer(
                "retry-delete",
                "Retry delete",
                &[],
                &seed,
                2 << 20,
                ProducerKind::Pull
            )
            .await
            .is_err(),
        "a settled delete error leaves the entry stopping and nonadmitting"
    );
    assert!(BroadcastPersist::new(&dir)
        .load("retry-delete")
        .unwrap()
        .is_some());
    assert!(
        seed.serves(&broadcast.infohash),
        "the seed lease remains until durable deletion succeeds"
    );
    assert_eq!(
        observer
            .events()
            .iter()
            .filter(|event| event.operation == PersistOperation::Delete)
            .count(),
        1,
        "the failed stop does not retry itself"
    );

    registry.delete("retry-delete").await.unwrap();
    assert_eq!(
        observer
            .events()
            .iter()
            .filter(|event| event.operation == PersistOperation::Delete)
            .count(),
        2
    );
    assert!(BroadcastPersist::new(&dir)
        .load("retry-delete")
        .unwrap()
        .is_none());
    assert!(!seed.serves(&broadcast.infohash));
    let recreated = registry
        .mint("retry-delete", "Retry delete", &[], &seed, 2 << 20)
        .await
        .unwrap();
    assert_ne!(
        recreated.infohash, broadcast.infohash,
        "new identity follows explicit successful retry"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(start_paused = true)]
async fn pull206_actual_persistence_failure_is_terminal_and_keeps_failed_claim() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let dir = std::env::temp_dir().join(format!(
        "outpace-pull206-terminal-{}",
        rand::random::<u64>()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let observer = persistence_observer();
    let registry = BroadcastRegistry::with_test_persist(BroadcastPersist::with_test_io(
        &dir,
        observer.clone(),
    ));
    let (frames_tx, frames_rx) = mpsc::unbounded_channel();
    let frames_rx = Arc::new(Mutex::new(Some(frames_rx)));
    let frames_rx_hook = frames_rx.clone();
    let (opened_tx, mut opened_rx) = mpsc::unbounded_channel();
    let (release_open_tx, release_open_rx) = tokio::sync::oneshot::channel();
    let release_open_rx = Arc::new(Mutex::new(Some(release_open_rx)));
    let release_open_hook = release_open_rx.clone();
    let open: TestSourceOpen = Arc::new(move |_, _, _, _| {
        let receiver = frames_rx_hook.clone();
        let opened = opened_tx.clone();
        let release = release_open_hook.clone();
        Box::pin(async move {
            let _ = opened.send(());
            let release = release.lock().unwrap().take().unwrap();
            release.await.map_err(|_| FetchError::Cancelled)?;
            Ok(
                Box::new(PullFrames(receiver.lock().unwrap().take().unwrap()))
                    as Box<dyn TestSourceBody>,
            )
        })
    });
    let logs = Arc::new(AtomicUsize::new(0));
    let logs_hook = logs.clone();
    registry.set_source_test_hooks(SourceTestHooks {
        open: Some(open),
        on_log: Some(Arc::new(move |_| {
            logs_hook.fetch_add(1, Ordering::SeqCst);
        })),
        ..SourceTestHooks::default()
    });
    let state = test_broadcast_state(registry.clone(), POLICY);
    let config = source("http://127.0.0.1:1/persistence-failure");
    let (broadcast, _) =
        attach_test_source(&state, "persist-failure", config.clone(), POLICY).await;
    tokio::time::timeout(Duration::from_secs(2), opened_rx.recv())
        .await
        .expect("source worker should reach the open boundary")
        .expect("open notification should arrive");

    // The actual HTTP source and worker continue normally; force its next complete signed
    // piece across the real reservation boundary and fail BroadcastPersist::save's FileSync.
    broadcast.cursor.next.store(
        broadcast.cursor.reserved_until(),
        std::sync::atomic::Ordering::SeqCst,
    );
    observer.fail_next(PersistOperation::FileSync);
    release_open_tx.send(()).unwrap();
    let packets = (65_536usize - broadcast.auth.signature_len()).div_ceil(188) + 8;
    frames_tx.send(Ok(Some(ts_packets(packets, 0x42)))).unwrap();
    let failed = tokio::time::timeout(
        Duration::from_secs(5),
        wait_status_with_state(&state, "persist-failure", |status| {
            status.state == SourceState::Failed
        }),
    )
    .await
    .expect("worker should classify real persistence failure");
    assert_eq!(failed.attempts, 1);
    assert_eq!(failed.last_error, Some(SourceErrorCode::PersistenceFailed));
    assert_eq!(failed.retry_in_ms, None);
    assert_eq!(logs.load(Ordering::SeqCst), 1);

    let (same, same_status) = attach_test_source(&state, "persist-failure", config, POLICY).await;
    assert_eq!(same.infohash, broadcast.infohash);
    assert_eq!(same_status.attempts, 1);
    for kind in [ProducerKind::RawPut, ProducerKind::Rtmp] {
        assert_eq!(
            registry
                .acquire_producer(
                    "persist-failure",
                    "conflict",
                    &[],
                    &state.seed_registry,
                    state.store_bytes,
                    kind,
                )
                .await
                .err()
                .unwrap(),
            BroadcastError::ProducerConflict
        );
    }
    tokio::time::advance(Duration::from_secs(300)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        state
            .source_status("persist-failure")
            .await
            .unwrap()
            .unwrap()
            .attempts,
        1
    );
    assert_eq!(logs.load(Ordering::SeqCst), 1);
    state.detach_source("persist-failure").await.unwrap();
    assert!(state
        .source_status("persist-failure")
        .await
        .unwrap()
        .is_none());
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(start_paused = true)]
async fn pull206_detach_timeout_keeps_claim_and_io_fence_until_real_disk_gate_releases() {
    let dir = std::env::temp_dir().join(format!(
        "outpace-pull206-detach-gate-{}",
        rand::random::<u64>()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let observer = persistence_observer();
    let registry = BroadcastRegistry::with_test_persist(BroadcastPersist::with_test_io(
        &dir,
        observer.clone(),
    ));
    let (frames_tx, frames_rx) = mpsc::unbounded_channel();
    let frames_rx = Arc::new(Mutex::new(Some(frames_rx)));
    let frames_rx_hook = frames_rx.clone();
    let (opened_tx, mut opened_rx) = mpsc::unbounded_channel();
    let (release_open_tx, release_open_rx) = tokio::sync::oneshot::channel();
    let release_open_rx = Arc::new(Mutex::new(Some(release_open_rx)));
    let release_open_hook = release_open_rx.clone();
    let open: TestSourceOpen = Arc::new(move |_, _, _, _| {
        let receiver = frames_rx_hook.clone();
        let opened = opened_tx.clone();
        let release = release_open_hook.clone();
        Box::pin(async move {
            let _ = opened.send(());
            let release = release.lock().unwrap().take().unwrap();
            release.await.map_err(|_| FetchError::Cancelled)?;
            Ok(
                Box::new(PullFrames(receiver.lock().unwrap().take().unwrap()))
                    as Box<dyn TestSourceBody>,
            )
        })
    });
    registry.set_source_test_hooks(SourceTestHooks {
        open: Some(open),
        ..SourceTestHooks::default()
    });
    let state = test_broadcast_state(registry.clone(), POLICY);
    let (broadcast, _) = attach_test_source(
        &state,
        "disk-gated-detach",
        source("http://127.0.0.1:1/disk-gated-detach"),
        POLICY,
    )
    .await;
    tokio::time::timeout(Duration::from_secs(2), opened_rx.recv())
        .await
        .expect("worker should pause before opening its real ingest")
        .expect("open notification should arrive");

    broadcast.cursor.next.store(
        broadcast.cursor.reserved_until(),
        std::sync::atomic::Ordering::SeqCst,
    );
    let (disk_entered, release_disk) =
        observer.gate_with_watchdog(PersistOperation::FileSync, Duration::from_secs(30));
    release_open_tx.send(()).unwrap();
    let packets = (65_536usize - broadcast.auth.signature_len()).div_ceil(188) + 8;
    frames_tx.send(Ok(Some(ts_packets(packets, 0x43)))).unwrap();
    tokio::task::spawn_blocking(move || {
        disk_entered
            .recv_timeout(std::time::Duration::from_secs(3))
            .expect("real reservation save should block at FileSync")
    })
    .await
    .unwrap();

    let detaching = {
        let state = state.clone();
        tokio::spawn(async move { state.detach_source("disk-gated-detach").await })
    };
    let stopping = wait_status_with_state(&state, "disk-gated-detach", |status| {
        status.state == SourceState::Stopping
    })
    .await;
    assert_eq!(stopping.attempts, 1);
    tokio::time::advance(crate::broadcast::CLEANUP_GRACE).await;
    assert_eq!(
        detaching.await.unwrap(),
        Err(BroadcastError::CleanupTimeout)
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            state.source_status("disk-gated-detach")
        )
        .await
        .expect("status remains readable while cleanup is disk-fenced")
        .unwrap()
        .is_some(),
        "timed-out detach must retain the source attachment until the blocking operation settles"
    );
    for kind in [ProducerKind::RawPut, ProducerKind::Rtmp] {
        assert_eq!(
            registry
                .acquire_producer(
                    "disk-gated-detach",
                    "conflict",
                    &[],
                    &state.seed_registry,
                    state.store_bytes,
                    kind,
                )
                .await
                .err()
                .unwrap(),
            BroadcastError::Stopping
        );
    }

    release_disk.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if state
                .source_status("disk-gated-detach")
                .await
                .unwrap()
                .is_none()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owned detach cleanup should settle after the actual disk call returns");
    let (_, raw_session) = registry
        .acquire_producer(
            "disk-gated-detach",
            "after detach",
            &[],
            &state.seed_registry,
            state.store_bytes,
            ProducerKind::RawPut,
        )
        .await
        .expect("the producer claim is released only after disk cleanup settles");
    drop(raw_session);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(start_paused = true)]
async fn pull206_queued_http_attach_returns_503_while_detach_holds_real_disk_fence() {
    let dir = temp_dir();
    let observer = persistence_observer();
    let registry = BroadcastRegistry::with_test_persist(BroadcastPersist::with_test_io(
        &dir,
        observer.clone(),
    ));
    let state = test_broadcast_state(registry.clone(), POLICY);

    let (frames_tx, frames_rx) = mpsc::unbounded_channel();
    let frames_rx = Arc::new(Mutex::new(Some(frames_rx)));
    let frames_rx_hook = frames_rx.clone();
    let (opened_tx, mut opened_rx) = mpsc::unbounded_channel();
    let (release_open_tx, release_open_rx) = tokio::sync::oneshot::channel();
    let release_open_rx = Arc::new(Mutex::new(Some(release_open_rx)));
    let release_open_hook = release_open_rx.clone();
    let open: TestSourceOpen = Arc::new(move |_, _, _, _| {
        let receiver = frames_rx_hook.clone();
        let opened = opened_tx.clone();
        let release = release_open_hook.clone();
        Box::pin(async move {
            let _ = opened.send(());
            let release = release.lock().unwrap().take().unwrap();
            release.await.map_err(|_| FetchError::Cancelled)?;
            Ok(
                Box::new(PullFrames(receiver.lock().unwrap().take().unwrap()))
                    as Box<dyn TestSourceBody>,
            )
        })
    });
    registry.set_source_test_hooks(SourceTestHooks {
        open: Some(open),
        ..SourceTestHooks::default()
    });

    let config = source("http://127.0.0.1:1/queued-http-attach");
    let (broadcast, _) =
        attach_test_source(&state, "queued-http-attach", config.clone(), POLICY).await;
    tokio::time::timeout(Duration::from_secs(2), opened_rx.recv())
        .await
        .expect("attached source worker should reach the body boundary")
        .expect("open notification should arrive");

    // Drive one full signed piece through the real worker, store, and persistence path.
    broadcast
        .cursor
        .next
        .store(broadcast.cursor.reserved_until(), Ordering::SeqCst);
    let (disk_entered, disk_release) =
        observer.gate_with_watchdog(PersistOperation::FileSync, Duration::from_secs(30));
    let mut disk_release = ReleaseDiskGate(Some(disk_release));
    release_open_tx.send(()).unwrap();
    let packets = (PIECE_LENGTH as usize - broadcast.auth.signature_len()).div_ceil(188) + 8;
    frames_tx.send(Ok(Some(ts_packets(packets, 0x54)))).unwrap();
    tokio::task::spawn_blocking(move || {
        disk_entered
            .recv_timeout(std::time::Duration::from_secs(3))
            .expect("actual reservation FileSync should be blocked")
    })
    .await
    .unwrap();

    let entry = registry
        .by_name
        .lock()
        .unwrap()
        .get("queued-http-attach")
        .unwrap()
        .clone();
    let (initializer_entered_tx, mut initializer_entered_rx) = mpsc::unbounded_channel();
    let (release_initializer_tx, release_initializer_rx) = tokio::sync::oneshot::channel();
    let release_initializer_rx = Arc::new(Mutex::new(Some(release_initializer_rx)));
    let release_initializer_hook = release_initializer_rx.clone();
    let before_source_gate = Arc::new(move || {
        let entered = initializer_entered_tx.clone();
        let release = release_initializer_hook.clone();
        Box::pin(async move {
            let _ = entered.send(());
            let release = release.lock().unwrap().take().unwrap();
            let _ = release.await;
        }) as futures::future::BoxFuture<'static, ()>
    });
    registry.set_source_test_hooks(SourceTestHooks {
        before_source_gate: Some(before_source_gate),
        ..SourceTestHooks::default()
    });

    let manager = crate::manager::StreamManager::new(crate::provider::ProviderRegistry::new());
    let app = crate::http::router(crate::http::AppState {
        manager,
        networks: vec![],
        resolve_content_ids_in_getstream: false,
        ace_sessions: Arc::new(crate::http::AceSessionStore::default()),
        experimental_ace_compat: false,
        broadcasts: Some(state.clone()),
    });
    let put = app.oneshot(
        axum::http::Request::put("/broadcast/queued-http-attach/source")
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(serde_json::to_vec(&config).unwrap()))
            .unwrap(),
    );
    let put = tokio::spawn(put);
    tokio::pin!(put);
    tokio::time::timeout(Duration::from_secs(2), initializer_entered_rx.recv())
        .await
        .expect("real owned initializer should stop before source_gate")
        .expect("pre-gate hook notification should arrive");

    // With the initializer paused before its first lock poll, the only possible source-gate
    // owner is the real detach continuation. Bound this exact lock observation with wall time.
    let gate_deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    let detach = {
        let state = state.clone();
        tokio::spawn(async move { state.detach_source("queued-http-attach").await })
    };
    let detach_owns_source_gate = loop {
        if entry.source_gate.try_lock().is_err() {
            break true;
        }
        if std::time::Instant::now() >= gate_deadline {
            break false;
        }
        tokio::task::yield_now().await;
    };

    // Record all pre-release facts, then release the actual blocking syscall before asserting
    // anything that may fail. This keeps the expected RED from leaving runtime work wedged.
    let retained_fences = entry.state.io.try_lock().is_err()
        && entry.state.data.lock().unwrap().source.is_some()
        && entry.state.data.lock().unwrap().producer.is_some();
    let _ = release_initializer_tx.send(());
    registry.close_admissions();
    let mut response_before_disk_release = None;
    for _ in 0..64 {
        if let std::task::Poll::Ready(joined) = futures::poll!(put.as_mut()) {
            response_before_disk_release = Some(joined);
            break;
        }
        tokio::task::yield_now().await;
    }
    if response_before_disk_release.is_none() {
        tokio::time::advance(Duration::from_millis(250)).await;
        for _ in 0..64 {
            if let std::task::Poll::Ready(joined) = futures::poll!(put.as_mut()) {
                response_before_disk_release = Some(joined);
                break;
            }
            tokio::task::yield_now().await;
        }
    }
    let fixed_503_before_disk_release = response_before_disk_release
        .as_ref()
        .and_then(|joined| joined.as_ref().ok())
        .and_then(|response| response.as_ref().ok())
        .is_some_and(|response| response.status() == axum::http::StatusCode::SERVICE_UNAVAILABLE);

    tokio::time::advance(crate::broadcast::CLEANUP_GRACE).await;
    let detach_result = tokio::time::timeout(Duration::from_secs(2), detach)
        .await
        .expect("detach should hit cleanup grace while FileSync remains blocked")
        .unwrap();
    let retained_after_detach_timeout = entry.state.data.lock().unwrap().source.is_some()
        && entry.state.data.lock().unwrap().producer.is_some()
        && entry.state.io.try_lock().is_err();

    disk_release.release();
    let response = match response_before_disk_release {
        Some(Ok(Ok(response))) => response,
        Some(Ok(Err(never))) => match never {},
        Some(Err(error)) => panic!("HTTP PUT task failed before disk release: {error}"),
        None => tokio::time::timeout(Duration::from_secs(10), put)
            .await
            .expect("HTTP PUT must settle after the real source gate is released")
            .expect("HTTP PUT task should return a response")
            .expect("router response is infallible"),
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let settled = {
                let data = entry.state.data.lock().unwrap();
                data.source.is_none() && data.source_detach.is_none()
            };
            if settled {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owned detach cleanup should settle after the real FileSync release");
    let response_status = response.status();
    let response_body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let response_json: serde_json::Value = serde_json::from_slice(&response_body).unwrap();
    let _ = std::fs::remove_dir_all(dir);

    assert!(
        detach_owns_source_gate,
        "detach must own the real source gate before PUT resumes"
    );
    assert!(
        retained_fences,
        "source claim and actual I/O fence remain held before disk release"
    );
    assert_eq!(detach_result, Err(BroadcastError::CleanupTimeout));
    assert!(
        retained_after_detach_timeout,
        "cleanup timeout must retain source and producer claims plus the I/O fence"
    );
    assert!(
        fixed_503_before_disk_release,
        "queued source PUT must return fixed 503 before the blocked FileSync is released"
    );
    assert_eq!(response_status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response_json,
        serde_json::json!({"error":{"code":"broadcast_service_stopping"}})
    );
}
