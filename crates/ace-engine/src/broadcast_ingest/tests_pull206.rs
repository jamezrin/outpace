//! Session-owned ingest permits and finite-tail semantics.

use crate::broadcast::{BroadcastRegistry, ProducerKind};
use ace_swarm::listen::SeedRegistry;

fn ts_packets(count: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(count * 188);
    for packet in 0..count {
        bytes.push(0x47);
        bytes.extend(std::iter::repeat_n((packet % 251) as u8, 187));
    }
    bytes
}

#[tokio::test]
async fn pull206_one_active_ingest_per_producer_and_drop_keeps_claim() {
    let registry = BroadcastRegistry::new();
    let seed = SeedRegistry::new();
    let (_broadcast, mut producer) = registry
        .acquire_producer("permit", "Permit", &[], &seed, 2 << 20, ProducerKind::Pull)
        .await
        .unwrap();

    let first = producer.open_ingest().unwrap();
    assert!(producer.open_ingest().is_err());
    drop(first);

    // Dropping a retry attempt releases only its permit; the producer still owns the name.
    assert!(registry
        .acquire_producer("permit", "Permit", &[], &seed, 2 << 20, ProducerKind::Rtmp)
        .await
        .is_err());
    let _next_attempt = producer.open_ingest().unwrap();
}

#[tokio::test]
async fn pull206_finish_publishes_a_signed_short_tail_once() {
    let registry = BroadcastRegistry::new();
    let seed = SeedRegistry::new();
    let (broadcast, mut producer) = registry
        .acquire_producer(
            "finite",
            "Finite",
            &[],
            &seed,
            2 << 20,
            ProducerKind::RawPut,
        )
        .await
        .unwrap();
    let mut ingest = producer.open_ingest().unwrap();

    ingest.push_bytes(&ts_packets(5)).await.unwrap();
    assert_eq!(
        broadcast.cursor.start_piece(),
        0,
        "the short tail is buffered until EOF"
    );
    ingest.finish().await.unwrap();
    assert_eq!(broadcast.cursor.start_piece(), 1);
    assert!(broadcast.store.lock().await.chunk(0, 0).is_some());

    // A repeated finish must not create or advance another piece.
    ingest.finish().await.unwrap();
    assert_eq!(broadcast.cursor.start_piece(), 1);
    assert!(broadcast.store.lock().await.chunk(1, 0).is_none());
}

#[tokio::test]
async fn pull206_drop_retry_discards_tail_and_reopens_same_identity() {
    let registry = BroadcastRegistry::new();
    let seed = SeedRegistry::new();
    let (broadcast, mut producer) = registry
        .acquire_producer("retry", "Retry", &[], &seed, 2 << 20, ProducerKind::Pull)
        .await
        .unwrap();
    let identity = broadcast.infohash;

    let mut abandoned = producer.open_ingest().unwrap();
    abandoned.push_bytes(&ts_packets(5)).await.unwrap();
    drop(abandoned); // Pull retry/cancel does not call finite finish.
    assert_eq!(broadcast.cursor.start_piece(), 0);
    assert!(broadcast.store.lock().await.chunk(0, 0).is_none());

    let mut retry = producer.open_ingest().unwrap();
    retry.push_bytes(&ts_packets(5)).await.unwrap();
    retry.finish().await.unwrap();
    assert_eq!(broadcast.infohash, identity);
    assert_eq!(broadcast.cursor.start_piece(), 1);
    assert!(broadcast.store.lock().await.chunk(0, 0).is_some());
}
