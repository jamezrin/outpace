use super::*;

async fn pair() -> (ConnectedUpstream, PeerSession<TcpStream>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    let client = TcpStream::connect(addr).await.unwrap();
    let (remote, _) = listener.accept().await.unwrap();
    (
        ConnectedUpstream {
            session: PeerSession::new(client),
            addr,
            window: live_pos(7, 8),
            unchoked: true,
            yourip: None,
        },
        PeerSession::new(remote),
    )
}

fn run(
    upstream: ConnectedUpstream,
    info: StreamInfo,
    store: Arc<tokio::sync::Mutex<PieceStore>>,
    continuity: Continuity,
) -> (
    mpsc::Receiver<LiveOutput>,
    tokio::task::JoinHandle<Continuity>,
) {
    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(async move {
        let seed = SeedConfig {
            metrics: None,
            registry: SeedRegistry::new(),
            store_factory: test_store_factory(16384, None),
            enabled: true,
            prefetch_pieces: 1,
            live_recovery: continuity.live_recovery,
            warm_peers: WarmPeerCache::memory(),
        };
        let mut continuity = Some(continuity);
        follow_peer_pool(
            vec![upstream],
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
        continuity.unwrap()
    });
    (rx, task)
}

async fn control(remote: &mut PeerSession<TcpStream>) {
    remote.send(&PeerMessage::Interested).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(remote.read_message().await.unwrap(), PeerMessage::Unchoke) {
                break;
            }
        }
    })
    .await
    .unwrap();
}

async fn stop(
    rx: &mut mpsc::Receiver<LiveOutput>,
    task: tokio::task::JoinHandle<Continuity>,
    remote: &mut PeerSession<TcpStream>,
) -> Continuity {
    rx.close();
    let continuity = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while remote.read_message().await.is_ok() {}
    })
    .await
    .unwrap();
    continuity
}

#[tokio::test]
async fn signed_out_of_order_batch_holds_until_disk_commit() {
    schedule(false).await;
}

#[tokio::test]
async fn replacement_pool_waits_without_copy_and_retries_lost_ack() {
    schedule(true).await;
}

async fn schedule(replace: bool) {
    let dir = tempfile::tempdir().unwrap();
    let auth = ace_wire::live_auth::LiveSourceAuth::generate();
    let payloads: Vec<Vec<u8>> = (0..2)
        .map(|piece| {
            (0..4)
                .flat_map(|cc| {
                    let mut p = vec![0x55 + piece; 188];
                    p[..4].copy_from_slice(&[0x47, 1, 0, 0x10 | (piece * 4 + cc)]);
                    p
                })
                .collect()
        })
        .collect();
    let wires: Vec<Vec<u8>> = payloads
        .iter()
        .map(|p| {
            let mut w = p.clone();
            w.extend(auth.sign(p));
            w
        })
        .collect();
    let info = StreamInfo {
        infohash: [0; 20],
        piece_length: wires[0].len() as u64,
        chunk_length: (wires[0].len() / 2) as u64,
        trackers: vec![],
        metadata: StreamMetadata::default(),
        sig_len: auth.signature_len(),
        source_pubkey: auth.pubkey_der(),
    };
    let policy = LiveRecoveryConfig {
        max_active_upstreams: 1,
        max_piece_advance: 2,
        request_timeout_ms: 5000,
        ..default_live_recovery()
    };
    let (mut continuity, _) = Continuity::fresh(&info, 7, 8, 1, policy);
    let floor = continuity.request_floor.clone();
    let (events, mut observed) = mpsc::unbounded_channel();
    continuity.contribution_observer = Some(events);
    let store = Arc::new(tokio::sync::Mutex::new(
        PieceStore::with_backend(
            info.piece_length,
            info.chunk_length,
            16384,
            crate::store::BackendKind::Disk {
                dir: dir.path().join("cache"),
            },
        )
        .unwrap(),
    ));
    let reached = Arc::new(tokio::sync::Notify::new());
    let (release, gate) = std::sync::mpsc::channel();
    store.lock().await.pause_publication(reached.clone(), gate);
    let (upstream, mut remote) = pair().await;
    let (mut rx, task) = run(upstream, info.clone(), store.clone(), continuity);
    for piece in [8, 7] {
        if piece == 7 {
            remote
                .send_extended_handshake(&OutgoingExtendedHandshake {
                    mi: Some(live_pos(7, 100)),
                    ace_metadata_version: 1,
                    ut_metadata_id: 2,
                    node: NodeFields::default(),
                    peer_ip: None,
                    metadata_size: None,
                })
                .await
                .unwrap();
        }
        for chunk in [1, 0] {
            let begin = chunk * wires[0].len() / 2;
            remote
                .send(&build_piece(
                    0,
                    piece,
                    chunk as u16,
                    [piece as u8; 8],
                    &wires[(piece - 7) as usize][begin..begin + wires[0].len() / 2],
                ))
                .await
                .unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), observed.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .0,
                piece as u64
            );
        }
        if piece == 8 {
            tokio::time::timeout(Duration::from_secs(2), reached.notified())
                .await
                .unwrap();
        }
    }
    // Completed malformed duplicates are immutable no-ops in the real actor.
    for piece in [7, 8] {
        for chunk in [0, 1] {
            remote
                .send(&build_piece(
                    0,
                    piece,
                    chunk,
                    [99; 8],
                    &vec![0; wires[0].len() / 2],
                ))
                .await
                .unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), observed.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .0,
                piece as u64
            );
        }
    }
    control(&mut remote).await;
    assert!(rx.try_recv().is_err());
    assert_eq!(floor.load(Ordering::Relaxed), 7);
    {
        let s = store.lock().await;
        assert!(!s.has_piece(7) && !s.has_piece(8));
        assert_eq!(s.publication_budget(), (wires[0].len(), 0));
    }
    let (mut rx, task, mut remote) = if replace {
        let continuity = stop(&mut rx, task, &mut remote).await;
        assert_eq!(continuity.reasm.next_needed(), 7);
        assert_eq!(continuity.head, 100);
        assert_eq!(continuity.request_floor.load(Ordering::Relaxed), 7);
        assert_eq!(continuity.publication.len(), 2);
        assert!(continuity
            .publication
            .values()
            .all(|v| matches!(v, PublicationStatus::Needed)));
        let (upstream, mut remote) = pair().await;
        let (mut rx, task) = run(upstream, info.clone(), store.clone(), continuity);
        control(&mut remote).await;
        assert!(rx.try_recv().is_err());
        assert_eq!(store.lock().await.publication_budget(), (wires[0].len(), 0));
        // A third producer is polled against the same store semaphore, then cancelled.
        let mut third = Box::pin(PieceStore::begin_complete_admission(
            store.clone(),
            9,
            wires[0].len(),
        ));
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut third)
            .await
            .is_err());
        assert_eq!(store.lock().await.publication_budget(), (wires[0].len(), 0));
        drop(third);
        let continuity = stop(&mut rx, task, &mut remote).await;
        assert_eq!(continuity.publication.len(), 2);
        drop(release);
        assert!(tokio::time::timeout(
            Duration::from_secs(2),
            PieceStore::shared_chunk(&store, 8, 0)
        )
        .await
        .unwrap()
        .is_some());
        assert_eq!(
            PieceStore::shared_put_complete_piece(&store, 8, [99; 8], &vec![0; wires[0].len()])
                .await
                .unwrap(),
            crate::store::CompletePut::AlreadyPresent
        );
        let (upstream, remote) = pair().await;
        let (rx, task) = run(upstream, info.clone(), store.clone(), continuity);
        (rx, task, remote)
    } else {
        drop(release);
        (rx, task, remote)
    };
    let mut output = Vec::new();
    while output.len() < 7 * 188 {
        output.extend_from_slice(
            &tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .unwrap()
                .unwrap()
                .bytes,
        );
    }
    let expected: Vec<u8> = payloads.concat();
    assert_eq!(
        output,
        expected[..7 * 188],
        "resync retains the eighth packet; signature bytes never enter media"
    );
    let continuity = stop(&mut rx, task, &mut remote).await;
    assert_eq!(
        continuity.reasm.next_needed(),
        99,
        "normal live recovery resumes only after ordered drain"
    );
    assert!(continuity.publication.is_empty());
    assert!(rx.try_recv().is_err());
    for piece in [7, 8] {
        let mut raw = Vec::new();
        for chunk in 0..2 {
            let (bytes, header) = PieceStore::shared_chunk(&store, piece, chunk)
                .await
                .unwrap();
            assert_eq!(header, [piece as u8; 8]);
            raw.extend(bytes);
        }
        assert_eq!(raw, wires[(piece - 7) as usize]);
    }
    assert_eq!(store.lock().await.publication_budget(), (0, 1));
}
