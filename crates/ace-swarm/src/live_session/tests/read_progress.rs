use super::*;

#[tokio::test]
async fn paused_reciprocal_read_allows_unrelated_actor_control() {
    diagnostic(false).await;
}

#[tokio::test]
async fn paused_reciprocal_read_allows_consumer_close() {
    diagnostic(true).await;
}

async fn pair() -> (TcpStream, PeerSession<TcpStream>, SocketAddrV4) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    let client = TcpStream::connect(addr).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    (client, PeerSession::new(server), addr)
}

async fn diagnostic(close: bool) {
    let dir = tempfile::tempdir().unwrap();
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
        max_active_upstreams: 2,
        max_piece_advance: 1,
        request_timeout_ms: 5_000,
        request_check_interval_ms: 10,
        ..default_live_recovery()
    };

    let wire_len = authentic.len();
    let (client, mut source, addr) = pair().await;
    let (healthy_client, mut healthy, healthy_addr) = pair().await;
    let store = Arc::new(tokio::sync::Mutex::new(
        PieceStore::with_backend(
            info.piece_length,
            info.chunk_length,
            4096,
            crate::store::BackendKind::Disk {
                dir: dir.path().join("cache"),
            },
        )
        .unwrap(),
    ));

    let reached = Arc::new(tokio::sync::Notify::new());
    let (release, gate) = std::sync::mpsc::channel();
    // Synthetic authenticated wire is published through the accepted-piece API before reading.
    PieceStore::shared_put_complete_piece(&store, 7, [3; 8], &authentic)
        .await
        .unwrap();
    store.lock().await.pause_read(reached.clone(), gate);
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
        enabled: true,
        prefetch_pieces: 0,
        live_recovery: policy,

        warm_peers: WarmPeerCache::memory(),
    };
    let (tx, mut rx) = mpsc::channel(4);
    let observed_store = store.clone();
    let pool = tokio::spawn(async move {
        let mut continuity = None;
        follow_peer_pool(
            vec![
                ConnectedUpstream {
                    session: PeerSession::new(client),
                    addr,
                    window: live_pos(7, 7),
                    unchoked: false,
                    yourip: None,
                },
                ConnectedUpstream {
                    session: PeerSession::new(healthy_client),
                    addr: healthy_addr,
                    window: live_pos(7, 7),
                    unchoked: false,
                    yourip: None,
                },
            ],
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

    // No detached remote tasks: this test owns both socket sessions.
    let mut payload = 123u32.to_be_bytes().to_vec();
    payload.extend_from_slice(&7u32.to_be_bytes());
    payload.extend_from_slice(&0u16.to_be_bytes());
    let requested = source.send(&PeerMessage::Unknown { id: 6, payload }).await;
    let paused = tokio::time::timeout(Duration::from_secs(5), reached.notified())
        .await
        .is_ok();
    let before = {
        let stored = observed_store.lock().await;
        (
            stored.has_piece(7),
            stored.have_pieces().contains(&7),
            stored.publication_budget(),
        )
    };
    let mut pool = pool;
    let mut exit = None;
    let mut control = false;
    if paused {
        if close {
            rx.close();
            exit = tokio::time::timeout(Duration::from_millis(250), &mut pool)
                .await
                .ok();
        } else {
            healthy.send(&PeerMessage::Interested).await.unwrap();
            control = tokio::time::timeout(Duration::from_millis(250), async {
                loop {
                    if matches!(healthy.read_message().await.unwrap(), PeerMessage::Unchoke) {
                        break;
                    }
                }
            })
            .await
            .is_ok();
        }
    }
    let held = observed_store.lock().await.publication_budget();
    // Sender drop also releases the blocking disk thread on any earlier panic.
    drop(release);
    let committed = tokio::time::timeout(
        Duration::from_secs(5),
        PieceStore::shared_chunk(&observed_store, 7, 0),
    )
    .await;
    let positive = if close {
        true
    } else {
        if !control {
            control_after_release(&mut healthy).await
        } else {
            true
        }
    };
    let reply = if close {
        true
    } else {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let PeerMessage::Piece {
                    index,
                    begin,
                    block,
                } = source.read_message().await.unwrap()
                {
                    break index == 0
                        && begin == 7
                        && block[..8] == [3; 8]
                        && block[8..10] == [0, 0]
                        && block[10..] == authentic[..wire_len / 2];
                }
            }
        })
        .await
        .unwrap_or(false)
    };
    rx.close();
    let cleanup = if let Some(result) = exit.as_ref() {
        result.is_ok()
    } else {
        match tokio::time::timeout(Duration::from_secs(5), &mut pool).await {
            Ok(result) => result.is_ok(),
            Err(_) => {
                pool.abort();
                let _ = pool.await;
                false
            }
        }
    };
    // Pool shutdown aborts its private workers; EOF witnesses socket teardown.
    let sockets_closed = tokio::time::timeout(Duration::from_secs(5), async {
        while source.read_message().await.is_ok() {}
        while healthy.read_message().await.is_ok() {}
    })
    .await
    .is_ok();
    drop(source);
    drop(healthy);
    let after = {
        let stored = observed_store.lock().await;
        (
            stored.has_piece(7),
            stored.have_pieces().contains(&7),
            stored.publication_budget(),
        )
    };
    eprintln!("issue287 reciprocal-read close={close} paused={paused} before={before:?} held={held:?} actor_control_before={control} exit_before={} release_control={positive} reply={reply} after={after:?} cleanup={cleanup}", exit.is_some());
    assert!(
        requested.is_ok() && paused && cleanup && sockets_closed,
        "fixture phases and task cleanup"
    );
    assert_eq!(before, (true, true, (0, 1)));
    assert_eq!(
        held,
        (0, 1),
        "read does not own a publication buffer or permit"
    );
    assert!(committed.is_ok_and(|v| v.is_some()));
    assert_eq!(after, (true, true, (0, 1)));
    assert!(positive && reply, "release positive control");
    assert!(
        if close { exit.is_some() } else { control },
        "desired actor progress while reciprocal disk read remains paused"
    );
}
async fn control_after_release(healthy: &mut PeerSession<TcpStream>) -> bool {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(healthy.read_message().await.unwrap(), PeerMessage::Unchoke) {
                break;
            }
        }
    })
    .await
    .is_ok()
}
