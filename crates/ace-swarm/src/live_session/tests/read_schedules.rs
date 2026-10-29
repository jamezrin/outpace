use super::*;

#[tokio::test]
async fn mutex_waiting_reciprocal_read_allows_unrelated_actor_control() {
    diagnostic(false).await;
}

#[tokio::test]
async fn mutex_waiting_reciprocal_read_allows_consumer_close() {
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

    // Synthetic authenticated wire is published through the accepted-piece API before reading.
    PieceStore::shared_put_complete_piece(&store, 7, [3; 8], &authentic)
        .await
        .unwrap();

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
    let (_witness, waiting) = crate::store::reciprocal_lock_witness::register(&observed_store);
    let release = observed_store.lock().await;
    let requested = source.send(&PeerMessage::Unknown { id: 6, payload }).await;
    // This notification is emitted only after the actual shared reciprocal lock
    // future has been polled Pending by the actor-owned job.
    let paused = tokio::time::timeout(Duration::from_secs(5), waiting.notified())
        .await
        .is_ok();
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
    assert!(
        requested.is_ok() && paused && cleanup && sockets_closed,
        "fixture phases and task cleanup"
    );
    assert!(committed.is_ok_and(|v| v.is_some()));
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

fn generation(
    clients: (TcpStream, SocketAddrV4, TcpStream, SocketAddrV4),
    store: Arc<tokio::sync::Mutex<PieceStore>>,
    info: StreamInfo,
) -> (
    tokio::task::JoinHandle<()>,
    mpsc::Receiver<LiveOutput>,
    Arc<AtomicU64>,
    Arc<AtomicU32>,
) {
    let (client, addr, healthy_client, healthy_addr) = clients;
    let uploaded = Arc::new(AtomicU64::new(0));
    let served = Arc::new(AtomicU32::new(0));
    let observed_uploaded = uploaded.clone();
    let observed_served = served.clone();
    let seed = SeedConfig {
        metrics: None,
        registry: SeedRegistry::new(),
        store_factory: test_store_factory(4096, None),
        enabled: true,
        prefetch_pieces: 0,
        live_recovery: default_live_recovery(),
        warm_peers: WarmPeerCache::memory(),
    };
    let (tx, rx) = mpsc::channel(4);
    let task = tokio::spawn(async move {
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
            1,
            &tx,
            &Arc::new(AtomicU64::new(0)),
            &uploaded,
            &served,
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
    (task, rx, observed_uploaded, observed_served)
}

#[tokio::test]
async fn two_full_generations_share_cancelled_disk_admission() {
    let dir = tempfile::tempdir().unwrap();
    let auth = ace_wire::live_auth::LiveSourceAuth::generate();
    let mut wire = vec![0x47; 752];
    wire.extend(auth.sign(&wire));
    let info = StreamInfo {
        infohash: [0; 20],
        piece_length: wire.len() as u64,
        chunk_length: wire.len() as u64,
        trackers: vec![],
        metadata: Default::default(),
        sig_len: auth.signature_len(),
        source_pubkey: auth.pubkey_der(),
    };
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
    PieceStore::shared_put_complete_piece(&store, 7, [3; 8], &wire)
        .await
        .unwrap();
    let reached = Arc::new(tokio::sync::Notify::new());
    let (release, gate) = std::sync::mpsc::channel();
    store.lock().await.pause_read(reached.clone(), gate);
    let (client, mut source, addr) = pair().await;
    let (healthy_client, mut healthy, healthy_addr) = pair().await;
    let (mut old, mut old_rx, old_uploaded, old_served) = generation(
        (client, addr, healthy_client, healthy_addr),
        store.clone(),
        info.clone(),
    );
    let request = || PeerMessage::Unknown {
        id: 6,
        payload: [
            0u32.to_be_bytes().as_slice(),
            7u32.to_be_bytes().as_slice(),
            &[0, 0],
        ]
        .concat(),
    };
    let sent = source.send(&request()).await.is_ok();
    let paused = tokio::time::timeout(Duration::from_secs(5), reached.notified())
        .await
        .is_ok();
    old_rx.close();
    let old_exit = tokio::time::timeout(Duration::from_secs(5), &mut old).await;
    let old_joined = old_exit.is_ok();
    let held = store.lock().await.reciprocal_available() == 0;
    let pending = Arc::new(tokio::sync::Notify::new());
    store
        .lock()
        .await
        .reciprocal_admission_witness(pending.clone());
    let (client, mut next_source, addr) = pair().await;
    let (healthy_client, mut next_healthy, healthy_addr) = pair().await;
    let (mut next, mut next_rx, uploaded, served) = generation(
        (client, addr, healthy_client, healthy_addr),
        store.clone(),
        info,
    );
    let next_sent = next_source.send(&request()).await.is_ok();
    let waiting = tokio::time::timeout(Duration::from_secs(5), pending.notified())
        .await
        .is_ok();
    let control_sent = next_healthy.send(&PeerMessage::Interested).await.is_ok();
    let control = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match next_healthy.read_message().await {
                Ok(PeerMessage::Unchoke) => break true,
                Ok(_) => {}
                Err(_) => break false,
            }
        }
    })
    .await;
    drop(release);
    let reply = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match next_source.read_message().await {
                Ok(PeerMessage::Piece {
                    index,
                    begin,
                    block,
                }) => break Some((index, begin, block)),
                Ok(_) => {}
                Err(_) => break None,
            }
        }
    })
    .await
    .ok()
    .flatten();
    next_rx.close();
    let next_joined = tokio::time::timeout(Duration::from_secs(5), &mut next)
        .await
        .is_ok();
    if !old_joined {
        old.abort();
        let _ = old.await;
    }
    if !next_joined {
        next.abort();
        let _ = next.await;
    }
    let drained = tokio::time::timeout(Duration::from_secs(5), async {
        while source.read_message().await.is_ok() {}
        while healthy.read_message().await.is_ok() {}
        while next_source.read_message().await.is_ok() {}
        while next_healthy.read_message().await.is_ok() {}
        PieceStore::shared_chunk(&store, 7, 0).await
    })
    .await
    .is_ok();
    let restored = store.lock().await.reciprocal_available() == 1;
    assert!(sent && paused && old_joined && held && next_sent && waiting && control_sent);
    assert!(control.unwrap_or(false) && next_joined && drained && restored);
    assert_eq!(old_uploaded.load(Ordering::Relaxed), 0);
    assert_eq!(old_served.load(Ordering::Relaxed), 0);
    assert_eq!(uploaded.load(Ordering::Relaxed), wire.len() as u64);
    assert!(served.load(Ordering::Relaxed) > 0);
    assert_eq!(reply, Some((0, 7, [vec![3; 8], vec![0, 0], wire].concat())));
}
