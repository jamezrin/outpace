use super::*;
#[tokio::test]
async fn rejected_signed_piece_retries_through_full_peer_pool() {
    let auth = ace_wire::live_auth::LiveSourceAuth::generate();
    let payload: Vec<u8> = (0..4)
        .flat_map(|cc| {
            let mut packet = vec![0x55; 188];
            packet[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10 | cc]);
            packet
        })
        .collect();
    let mut authentic = payload.clone();
    authentic.extend(auth.sign(&payload));
    let mut corrupt = authentic.clone();
    corrupt[0] ^= 1;
    let info = StreamInfo {
        infohash: [0; 20],
        piece_length: authentic.len() as u64,
        chunk_length: authentic.len() as u64 / 2,
        trackers: vec![],
        metadata: StreamMetadata::default(),
        sig_len: auth.signature_len(),
        source_pubkey: auth.pubkey_der(),
    };
    let policy = LiveRecoveryConfig {
        max_active_upstreams: 1,
        max_piece_advance: 1,
        request_timeout_ms: 5_000,
        request_check_interval_ms: 10,
        ..default_live_recovery()
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(addr) => addr,
        _ => unreachable!(),
    };
    let client = TcpStream::connect(addr).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    let (retry_tx, retry_rx) = tokio::sync::oneshot::channel();
    let (allow_tx, allow_rx) = tokio::sync::oneshot::channel();
    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
    let chunk_length = info.chunk_length as usize;
    let server = tokio::spawn(async move {
        let mut retry_tx = Some(retry_tx);
        let mut allow_rx = Some(allow_rx);
        let mut finish_rx = Some(finish_rx);
        let mut session = PeerSession::new(server);
        session.send(&PeerMessage::Unchoke).await.unwrap();
        for attempt in 0..2 {
            let mut chunks = Vec::new();
            while chunks.len() < 2 {
                let msg = session.read_message().await.unwrap();
                if let PeerMessage::Unknown { id: 6, payload } = msg {
                    assert_eq!(u32::from_be_bytes(payload[4..8].try_into().unwrap()), 7);
                    chunks.push(u16::from_be_bytes(payload[8..10].try_into().unwrap()));
                }
            }
            assert_eq!(chunks, vec![0, 1]);
            if attempt == 0 {
                for chunk in [0, 0, 1] {
                    let begin = chunk as usize * chunk_length;
                    session
                        .send(&build_piece(
                            0,
                            7,
                            chunk,
                            [0; 8],
                            &corrupt[begin..begin + chunk_length],
                        ))
                        .await
                        .unwrap();
                }
            } else {
                retry_tx.take().unwrap().send(()).unwrap();
                allow_rx.take().unwrap().await.unwrap();
                // Deliver the previously rejected last chunk first. Stale chunk counts
                // must not free the slot and start a third request before completion.
                session
                    .send(&build_piece(0, 7, 1, [0; 8], &authentic[chunk_length..]))
                    .await
                    .unwrap();
                assert!(
                    tokio::time::timeout(Duration::from_millis(100), session.read_message())
                        .await
                        .is_err(),
                    "partial retry must stay in flight"
                );
                session
                    .send(&build_piece(0, 7, 0, [0; 8], &authentic[..chunk_length]))
                    .await
                    .unwrap();
                // Late copies must not emit the authenticated piece twice.
                for chunk in 0..2 {
                    let begin = chunk as usize * chunk_length;
                    session
                        .send(&build_piece(
                            0,
                            7,
                            chunk,
                            [0; 8],
                            &authentic[begin..begin + chunk_length],
                        ))
                        .await
                        .unwrap();
                }
                finish_rx.take().unwrap().await.unwrap();
            }
        }
    });
    let store = Arc::new(tokio::sync::Mutex::new(PieceStore::new(
        info.piece_length,
        info.chunk_length,
        4096,
    )));
    let recovery_observer = TestObserver::new(
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        Arc::new(AtomicU32::new(0)),
    );
    let seed = SeedConfig {
        metrics: Some(recovery_observer.clone()),
        registry: SeedRegistry::new(),
        store_factory: test_store_factory(4096, None),
        enabled: false,
        prefetch_pieces: 0,
        live_recovery: policy,

        warm_peers: WarmPeerCache::memory(),
    };
    let (tx, mut rx) = mpsc::channel(4);
    let pool = tokio::spawn(async move {
        let mut continuity = None;
        follow_peer_pool(
            vec![ConnectedUpstream {
                session: PeerSession::new(client),
                addr,
                window: live_pos(7, 7),
                unchoked: false,
                yourip: None,
            }],
            &info,
            &Identity::generate(),
            2,
            &tx,
            &Arc::new(AtomicU64::new(0)),
            &Arc::new(AtomicU64::new(0)),
            &Arc::new(AtomicU32::new(0)),
            &seed,
            &store,
            &mut continuity,
            vec![],
            vec![],
            completed_discovery(|_| Box::pin(async { vec![] })),
            &mut SessionCandidates::default(),
            &Arc::new(AtomicU32::new(0)),
            None,
        )
        .await;
    });
    tokio::time::timeout(Duration::from_secs(2), retry_rx)
        .await
        .expect("pool must re-request the rejected piece without waiting for timeout")
        .unwrap();
    assert!(rx.try_recv().is_err(), "corrupt piece must emit nothing");
    allow_tx.send(()).unwrap();
    let output = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    // The existing TS resynchronizer retains one packet for its next-sync check.
    assert_eq!(output.bytes.as_ref(), &payload[..564]);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .is_err(),
        "late duplicate blocks must not emit another piece"
    );
    finish_tx.send(()).unwrap();
    server.await.unwrap();
    pool.await.unwrap();
    assert_eq!(
        recovery_observer.snapshot().recovery_events,
        vec![RecoveryEvent::SignatureReject],
        "one assembled piece failing source verification counts once; the valid retry does not"
    );
}
