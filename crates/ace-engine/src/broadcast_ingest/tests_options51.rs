//! Effective geometry must drive real signing, storage, and full-piece progress.

use crate::broadcast::{BroadcastRegistry, ProducerKind};
use crate::broadcast_options::BroadcastOptions;
use ace_swarm::listen::SeedRegistry;
use ace_wire::live_auth::{split_piece, verify_piece};
use ace_wire::reassembly::PieceReassembler;
use std::collections::BTreeMap;

fn transport_stream(bytes: usize) -> Vec<u8> {
    let packets = bytes.div_ceil(188);
    let mut out = Vec::with_capacity(packets * 188);
    for packet in 0..packets {
        out.push(0x47);
        out.push(0x00);
        out.push(0x10);
        out.push(0x10);
        out.extend(std::iter::repeat_n((packet % 251) as u8, 184));
    }
    out
}

async fn signed_geometry_case(expected_piece_length: u64) {
    let options = if expected_piece_length == 65_536 {
        BTreeMap::new()
    } else {
        BTreeMap::from([(
            "synthetic-origin".into(),
            BroadcastOptions {
                piece_length: Some(expected_piece_length),
                ..Default::default()
            },
        )])
    };
    let registry = BroadcastRegistry::new_with_options(options).unwrap();
    let seed = SeedRegistry::new();
    let (broadcast, mut producer) = registry
        .acquire_producer(
            "synthetic-origin",
            &[],
            &seed,
            1 << 20,
            ProducerKind::RawPut,
        )
        .await
        .unwrap();
    let piece_length = broadcast.geometry.piece_length;
    let chunk_length = broadcast.geometry.chunk_length;
    let signature_len = broadcast.auth.signature_len();
    assert_eq!(piece_length, expected_piece_length);
    assert_eq!(chunk_length, 16_384);
    let payload_capacity = piece_length as usize - signature_len;
    // TsResync may discard an initial sync-search window; leave enough aligned bytes
    // after it to complete both signed pieces and retain a short tail.
    let body = transport_stream(payload_capacity * 2 + 1_024);
    let mut ingest = producer.open_ingest().unwrap();
    let progress = ingest.push_bytes(&body).await.unwrap();
    assert_eq!(progress.committed_pieces, 2);
    ingest.finish().await.unwrap();
    assert_eq!(broadcast.cursor.start_piece(), 3);

    let mut reassembler =
        PieceReassembler::new(piece_length, 0).with_source_pubkey(broadcast.auth.pubkey_der());
    {
        let store = broadcast.store.lock().await;
        for piece in 0..2 {
            for chunk in 0..store.chunks_per_piece() {
                let block = store.chunk(piece, chunk).unwrap().into_owned();
                reassembler
                    .add_block(piece, u64::from(chunk) * chunk_length, &block)
                    .unwrap();
            }
        }
    }
    let media = reassembler.take_ready();
    assert_eq!(media.len(), payload_capacity * 2);
    assert_eq!(media.as_slice(), &body[..payload_capacity * 2]);

    // Reassembly intentionally yields only full pieces. Verify the finite signed tail directly.
    let tail = broadcast
        .store
        .lock()
        .await
        .chunk(2, 0)
        .unwrap()
        .into_owned();
    let (payload, signature) = split_piece(&tail, signature_len).unwrap();
    assert!(payload.len() < payload_capacity);
    assert!(verify_piece(
        &broadcast.auth.pubkey_der(),
        payload,
        signature
    ));

    drop(ingest);
    let mut abandoned = producer.open_ingest().unwrap();
    abandoned.push_bytes(&transport_stream(128)).await.unwrap();
    drop(abandoned);
    assert_eq!(broadcast.cursor.start_piece(), 3);
    assert!(broadcast.store.lock().await.chunk(3, 0).is_none());

    // A fresh signing chunker must rebase its local zero to the actual global cursor.
    let mut retry = producer.open_ingest().unwrap();
    let retry_body = transport_stream(payload_capacity + 1_024);
    let retry_progress = retry.push_bytes(&retry_body).await.unwrap();
    assert_eq!(retry_progress.committed_pieces, 1);
    retry.finish().await.unwrap();
    let mut blocks = Vec::new();
    {
        let store = broadcast.store.lock().await;
        for chunk in 0..store.chunks_per_piece() {
            let block = store.chunk(3, chunk).unwrap().into_owned();
            blocks.extend_from_slice(&block);
        }
    }
    let (payload, signature) = split_piece(&blocks, signature_len).unwrap();
    assert!(verify_piece(
        &broadcast.auth.pubkey_der(),
        payload,
        signature
    ));
    assert_eq!(payload.len(), payload_capacity);
    assert_eq!(broadcast.cursor.start_piece(), 5);
}

#[tokio::test]
async fn options51_signed_geometry_progress_and_rebase() {
    signed_geometry_case(65_536).await;
    signed_geometry_case(131_072).await;
}

#[tokio::test]
async fn options51_128k_restart_rebases_piece_progress_after_reservation_gap() {
    let dir = std::env::temp_dir().join(format!(
        "outpace-options51-restart-{}",
        rand::random::<u64>()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let options = BTreeMap::from([(
        "synthetic-origin".to_owned(),
        BroadcastOptions {
            piece_length: Some(131_072),
            ..Default::default()
        },
    )]);
    let first_registry = crate::broadcast::BroadcastRegistry::with_persist_and_options(
        &dir,
        crate::config::CacheType::Memory,
        std::path::PathBuf::new(),
        options.clone(),
    )
    .unwrap();
    let first_seed = SeedRegistry::new();
    let (first, mut first_producer) = first_registry
        .acquire_producer(
            "synthetic-origin",
            &[],
            &first_seed,
            1 << 20,
            ProducerKind::Pull,
        )
        .await
        .unwrap();
    let capacity = first.geometry.piece_length as usize - first.auth.signature_len();
    let mut first_ingest = first_producer.open_ingest().unwrap();
    let first_progress = first_ingest
        .push_bytes(&transport_stream(capacity + 1_024))
        .await
        .unwrap();
    assert_eq!(first_progress.committed_pieces, 1);
    first_ingest.finish().await.unwrap();
    first_registry.shutdown().await.unwrap();

    let resumed_registry = crate::broadcast::BroadcastRegistry::with_persist_and_options(
        &dir,
        crate::config::CacheType::Memory,
        std::path::PathBuf::new(),
        options,
    )
    .unwrap();
    let resumed_seed = SeedRegistry::new();
    let (resumed, mut producer) = resumed_registry
        .acquire_producer(
            "synthetic-origin",
            &[],
            &resumed_seed,
            1 << 20,
            ProducerKind::Pull,
        )
        .await
        .unwrap();
    assert_eq!(resumed.geometry.piece_length, 131_072);
    assert_eq!(resumed.cursor.start_piece(), 128);
    let capacity = resumed.geometry.piece_length as usize - resumed.auth.signature_len();
    let mut ingest = producer.open_ingest().unwrap();
    let progress = ingest
        .push_bytes(&transport_stream(capacity + 1_024))
        .await
        .unwrap();
    assert_eq!(progress.committed_pieces, 1);
    ingest.finish().await.unwrap();
    assert_eq!(resumed.cursor.start_piece(), 130);

    let mut reassembler =
        PieceReassembler::new(131_072, 128).with_source_pubkey(resumed.auth.pubkey_der());
    let store = resumed.store.lock().await;
    for chunk in 0..store.chunks_per_piece() {
        let block = store.chunk(128, chunk).unwrap().into_owned();
        reassembler
            .add_block(128, u64::from(chunk) * 16_384, &block)
            .unwrap();
    }
    drop(store);
    assert_eq!(reassembler.take_ready().len(), capacity);
    resumed_registry.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(dir);
}
