use super::*;

async fn cancel_at_pending(future: impl Future<Output = bool>) {
    tokio::pin!(future);
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn peerless_drain_survives_cancel_between_segments_and_closes_promptly() {
    let info = StreamInfo {
        infohash: [0; 20],
        piece_length: 4,
        chunk_length: 2,
        trackers: vec![],
        metadata: Default::default(),
        sig_len: 0,
        source_pubkey: vec![],
    };
    let seed = SeedConfig {
        metrics: None,
        registry: SeedRegistry::new(),
        store_factory: test_store_factory(4096, None),
        enabled: true,
        prefetch_pieces: 0,
        live_recovery: LiveRecoveryConfig::default(),
        warm_peers: WarmPeerCache::memory(),
    };
    let store = Arc::new(tokio::sync::Mutex::new(PieceStore::new(4, 2, 4096)));
    let (mut c, _) = Continuity::fresh(&info, 0, 0, 0, seed.live_recovery);
    let mut candidates = SessionCandidates::default();
    let downloaded = Arc::new(AtomicU64::new(0));
    let (tx, mut rx) = mpsc::channel(1);
    for bytes in [b"one".as_slice(), b"two", b"three"] {
        c.pending_output.push_back(LiveOutput {
            bytes: Bytes::copy_from_slice(bytes),
            discontinuity: false,
        });
        c.pending_credit.push_back(vec![]);
    }
    // Cancel an owned operation after its first commit, without losing pending bytes.
    cancel_at_pending(drain_retained(
        &mut c,
        &mut candidates,
        &info,
        &seed,
        &store,
        &downloaded,
        &tx,
    ))
    .await;
    assert_eq!(rx.recv().await.unwrap().bytes.as_ref(), b"one");
    assert_eq!(c.pending_output.len(), 2);
    assert_eq!(c.first_output, FirstOutput::Done);
    let fence = c.output_ceiling;
    cancel_at_pending(drain_retained(
        &mut c,
        &mut candidates,
        &info,
        &seed,
        &store,
        &downloaded,
        &tx,
    ))
    .await;
    assert_eq!(c.output_ceiling, fence);
    assert_eq!(rx.recv().await.unwrap().bytes.as_ref(), b"two");
    assert!(
        drain_retained(
            &mut c,
            &mut candidates,
            &info,
            &seed,
            &store,
            &downloaded,
            &tx
        )
        .await
    );
    assert_eq!(rx.recv().await.unwrap().bytes.as_ref(), b"three");
    assert_eq!(downloaded.load(Ordering::Relaxed), 11);
    assert!(c.output_ceiling.is_none());
    // Idle drainage returns without reserving capacity or requiring a peer.
    assert!(
        drain_retained(
            &mut c,
            &mut candidates,
            &info,
            &seed,
            &store,
            &downloaded,
            &tx
        )
        .await
    );
    c.pending_output.push_back(LiveOutput {
        bytes: Bytes::from_static(b"closed"),
        discontinuity: false,
    });
    drop(rx);
    assert!(!tokio::time::timeout(
        Duration::from_millis(250),
        drain_retained(
            &mut c,
            &mut candidates,
            &info,
            &seed,
            &store,
            &downloaded,
            &tx
        )
    )
    .await
    .unwrap());
    assert_eq!(c.pending_output.len(), 1);
}

#[tokio::test]
async fn peerless_publication_before_extraction_and_withheld_does_not_complete_first_output() {
    let info = StreamInfo {
        infohash: [0; 20],
        piece_length: 4,
        chunk_length: 2,
        trackers: vec![],
        metadata: Default::default(),
        sig_len: 0,
        source_pubkey: vec![],
    };
    let seed = SeedConfig {
        metrics: None,
        registry: SeedRegistry::new(),
        store_factory: test_store_factory(4096, None),
        enabled: true,
        prefetch_pieces: 0,
        live_recovery: LiveRecoveryConfig::default(),
        warm_peers: WarmPeerCache::memory(),
    };
    let store = Arc::new(tokio::sync::Mutex::new(PieceStore::new(4, 2, 4096)));
    let (mut c, _) = Continuity::fresh(&info, 0, 0, 0, seed.live_recovery);
    c.reasm.add_live_chunk(0, 0, 2, [1; 8], b"ab").unwrap();
    c.reasm.add_live_chunk(0, 1, 2, [1; 8], b"cd").unwrap();
    c.publication.insert(0, PublicationStatus::Needed);
    let mut candidates = SessionCandidates::default();
    let downloaded = Arc::new(AtomicU64::new(0));
    let (tx, mut rx) = mpsc::channel(1);
    tokio::time::timeout(
        Duration::from_secs(1),
        drain_retained(
            &mut c,
            &mut candidates,
            &info,
            &seed,
            &store,
            &downloaded,
            &tx,
        ),
    )
    .await
    .unwrap();
    assert_eq!(c.reasm.next_needed(), 1);
    assert!(store.lock().await.has_piece(0));
    assert!(matches!(c.first_output, FirstOutput::Armed(_)));
    assert_eq!(downloaded.load(Ordering::Relaxed), 0);
    assert!(rx.try_recv().is_err());
    assert!(c.output_ceiling.is_none());
}

#[tokio::test]
async fn peerless_cancel_before_extract_preserves_wire_and_older_pending_bypasses_publication() {
    let info = StreamInfo {
        infohash: [0; 20],
        piece_length: 4,
        chunk_length: 2,
        trackers: vec![],
        metadata: Default::default(),
        sig_len: 0,
        source_pubkey: vec![],
    };
    let seed = SeedConfig {
        metrics: None,
        registry: SeedRegistry::new(),
        store_factory: test_store_factory(4096, None),
        enabled: true,
        prefetch_pieces: 0,
        live_recovery: LiveRecoveryConfig::default(),
        warm_peers: WarmPeerCache::memory(),
    };
    let store = Arc::new(tokio::sync::Mutex::new(PieceStore::new(4, 2, 4096)));
    let (mut c, _) = Continuity::fresh(&info, 0, 0, 0, seed.live_recovery);
    c.reasm.add_live_chunk(0, 0, 2, [1; 8], b"ab").unwrap();
    c.reasm.add_live_chunk(0, 1, 2, [1; 8], b"cd").unwrap();
    let mut candidates = SessionCandidates::default();
    let downloaded = Arc::new(AtomicU64::new(0));
    let (tx, mut rx) = mpsc::channel(1);
    tx.send(LiveOutput {
        bytes: Bytes::from_static(b"full"),
        discontinuity: false,
    })
    .await
    .unwrap();
    // A generation was dropped with an active hold and real corroboration due.
    let now = Instant::now();
    c.output_clock.begin(now);
    c.output_ceiling = Some(c.reasm.next_needed() + seed.live_recovery.max_reasm_pieces_ahead);
    c.startup_deadline = Some(now);
    assert_eq!(c.first_output, FirstOutput::AwaitCorroboration);
    cancel_at_pending(drain_retained(
        &mut c,
        &mut candidates,
        &info,
        &seed,
        &store,
        &downloaded,
        &tx,
    ))
    .await;
    assert_eq!(c.reasm.next_needed(), 0);
    assert_eq!(c.reasm.ready_chunks().next().unwrap(), b"abcd");
    assert!(matches!(c.first_output, FirstOutput::Armed(_)));
    let frozen = c.output_clock.now(Instant::now());
    assert_eq!(
        c.output_clock
            .now(Instant::now() + Duration::from_secs(600)),
        frozen
    );
    rx.recv().await.unwrap();
    c.pending_output.push_back(LiveOutput {
        bytes: Bytes::from_static(b"older"),
        discontinuity: true,
    });
    c.pending_credit.push_back(vec![]);
    c.publication.insert(0, PublicationStatus::Needed);
    let guard = store.lock().await;
    cancel_at_pending(drain_retained(
        &mut c,
        &mut candidates,
        &info,
        &seed,
        &store,
        &downloaded,
        &tx,
    ))
    .await;
    let older = rx.recv().await.unwrap();
    assert_eq!(older.bytes.as_ref(), b"older");
    assert!(older.discontinuity);
    assert_eq!(c.first_output, FirstOutput::Done);
    assert_eq!(c.reasm.next_needed(), 0);
    drop(guard);
    assert!(tokio::time::timeout(
        Duration::from_secs(1),
        drain_retained(
            &mut c,
            &mut candidates,
            &info,
            &seed,
            &store,
            &downloaded,
            &tx
        )
    )
    .await
    .unwrap());
    assert_eq!(c.reasm.next_needed(), 1);
}
