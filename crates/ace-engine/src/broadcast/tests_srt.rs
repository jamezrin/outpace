//! Transport retirement must fence producer replacement and deletion, not just task lifetime.
use super::*;

#[tokio::test]
async fn transport_retirement_blocks_replacement_after_producer_drop() {
    let registry = BroadcastRegistry::new();
    let seeds = SeedRegistry::new();
    let (confirm, retirement) = watch::channel(false);
    let (original, producer) = registry
        .acquire_producer_with_retirement(
            "retirement",
            &[],
            &seeds,
            1 << 20,
            ProducerKind::RawPut,
            retirement,
        )
        .await
        .unwrap();
    drop(producer);
    let blocked = registry
        .acquire_producer("retirement", &[], &seeds, 1 << 20, ProducerKind::Rtmp)
        .await;
    assert!(
        matches!(blocked, Err(BroadcastError::ProducerConflict)),
        "native cleanup must retain the exclusive producer claim"
    );
    confirm.send_replace(true);
    let (replacement, producer) = registry
        .acquire_producer("retirement", &[], &seeds, 1 << 20, ProducerKind::Rtmp)
        .await
        .unwrap();
    assert_eq!(original.transport_bytes, replacement.transport_bytes);
    drop(producer);
    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn closed_unconfirmed_transport_cannot_release_producer() {
    let registry = BroadcastRegistry::new();
    let seeds = SeedRegistry::new();
    let (confirm, retirement) = watch::channel(false);
    let (_, producer) = registry
        .acquire_producer_with_retirement(
            "uncertain",
            &[],
            &seeds,
            1 << 20,
            ProducerKind::RawPut,
            retirement,
        )
        .await
        .unwrap();
    drop(confirm);
    drop(producer);
    assert!(matches!(
        registry
            .acquire_producer("uncertain", &[], &seeds, 1 << 20, ProducerKind::Rtmp)
            .await,
        Err(BroadcastError::ProducerConflict)
    ));
}

#[tokio::test]
async fn canceled_delete_waiter_retains_seed_until_transport_confirmation() {
    let registry = BroadcastRegistry::new();
    let seeds = SeedRegistry::new();
    let (confirm, retirement) = watch::channel(false);
    let (broadcast, producer) = registry
        .acquire_producer_with_retirement(
            "delete-retirement",
            &[],
            &seeds,
            1 << 20,
            ProducerKind::RawPut,
            retirement,
        )
        .await
        .unwrap();
    drop(producer);
    let first = tokio::time::timeout(
        Duration::from_millis(20),
        registry.delete("delete-retirement"),
    )
    .await;
    let retained = seeds.serves(&broadcast.infohash);
    confirm.send_replace(true);
    registry.delete("delete-retirement").await.unwrap();
    assert!(
        first.is_err(),
        "unconfirmed transport must keep deletion pending"
    );
    assert!(retained, "canceled waiter must not release the seed lease");
    assert!(!seeds.serves(&broadcast.infohash));
}

#[tokio::test]
async fn uncertain_transport_cleanup_reports_failure_without_releasing_seed() {
    let registry = BroadcastRegistry::new();
    let seeds = SeedRegistry::new();
    let (confirm, retirement) = watch::channel(false);
    let (broadcast, producer) = registry
        .acquire_producer_with_retirement(
            "failed-retirement",
            &[],
            &seeds,
            1 << 20,
            ProducerKind::RawPut,
            retirement,
        )
        .await
        .unwrap();
    drop(confirm);
    drop(producer);
    assert!(matches!(
        registry.delete("failed-retirement").await,
        Err(BroadcastError::CleanupTimeout)
    ));
    assert!(seeds.serves(&broadcast.infohash));
}
