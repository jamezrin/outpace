//! Reservation and producer invariants exercised through the real registry and piece store.

use super::{
    BroadcastError, BroadcastRegistry, ProducerKind, CURSOR_PERSIST_INTERVAL, PIECE_LENGTH,
};
use crate::broadcast_persist::{
    tests_pull206::persistence_observer, BroadcastPersist, PersistOperation, PersistedBroadcast,
};
use crate::config::CacheType;
use ace_swarm::listen::SeedRegistry;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

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
