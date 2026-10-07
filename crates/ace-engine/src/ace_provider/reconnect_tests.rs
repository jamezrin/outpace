//! Loopback protocol regressions exercise the production session and pool implementation.
use super::*;

fn default_live_recovery() -> LiveRecoveryConfig {
    LiveRecoveryConfig::default()
}
fn live_pos(min_piece: i64, max_piece: i64) -> LivePosition {
    LivePosition {
        min_piece,
        max_piece,
        position: -1,
        distance_from_source: 1,
    }
}

#[tokio::test]
async fn stale_gossip_harvests_each_peer_with_one_total_deadline() {
    let mut upstreams = Vec::new();
    let mut servers = Vec::new();
    let announced = SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 23456);
    for gossip in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        upstreams.push(ConnectedUpstream {
            session: PeerSession::new(client),
            addr,
            window: live_pos(1, 1),
            yourip: None,
        });
        servers.push(tokio::spawn(async move {
            let mut session = PeerSession::new(server);
            let handshake = session.read_message().await.unwrap();
            assert!(matches!(handshake, PeerMessage::Extended { ext_id: 0, .. }));
            if gossip {
                let mut payload = vec![0; 14];
                payload[8..12].copy_from_slice(&announced.ip().octets());
                payload[12..14].copy_from_slice(&announced.port().to_be_bytes());
                session.send(&PeerMessage::Have(u32::MAX)).await.unwrap();
                session.send(&PeerMessage::Unchoke).await.unwrap();
                session
                    .send(&PeerMessage::Unknown { id: 36, payload })
                    .await
                    .unwrap();
            }
            while let Ok(msg) = session.read_message().await {
                assert!(
                    !matches!(
                        msg,
                        PeerMessage::Interested | PeerMessage::Unknown { id: 6, .. }
                    ),
                    "gossip-only connection must not schedule stale media"
                );
            }
        }));
    }
    let mut candidates = SessionCandidates::default();
    let started = Instant::now();
    harvest_stale_gossip(upstreams, &Identity::generate(), &mut candidates).await;
    assert!(
        started.elapsed() < Duration::from_millis(1200),
        "the batch shares a single 750ms budget"
    );
    assert_eq!(
        candidates.eligible(Instant::now()),
        vec![announced],
        "silent first peer must not prevent harvesting the second"
    );
    for server in servers {
        server.await.unwrap();
    }
}

#[tokio::test]
async fn slow_preferred_source_does_not_starve_working_pex_fallback() {
    let slow = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let good = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(slow_addr) = slow.local_addr().unwrap() else {
        unreachable!()
    };
    let std::net::SocketAddr::V4(good_addr) = good.local_addr().unwrap() else {
        unreachable!()
    };
    let server = tokio::spawn(async move {
        let (stream, _) = good.accept().await.unwrap();
        let mut session = PeerSession::new(stream);
        session.accept_handshake([0; 20], |_| true).await.unwrap();
        session
            .send_extended_handshake(&OutgoingExtendedHandshake {
                ace_metadata_version: 1,
                ut_metadata_id: 2,
                mi: Some(live_pos(7, 8)),
                node: NodeFields::default(),
                peer_ip: None,
                metadata_size: None,
            })
            .await
            .unwrap();
        while session.read_message().await.is_ok() {}
    });
    let mut candidates = SessionCandidates::default();
    candidates.learn(slow_addr, CandidateKind::Source);
    candidates.learn(good_addr, CandidateKind::Pex);
    candidates.failed(slow_addr, Instant::now() - Duration::from_secs(2));
    // A repeat source announcement does not erase failure history. Its old cooldown
    // already expired; the connect snapshot still visits the healthy fallback once.
    candidates.learn(slow_addr, CandidateKind::Source);
    let ready = candidates.eligible(Instant::now());
    let started = Instant::now();
    let pool = tokio::time::timeout(
        Duration::from_secs(5),
        connect_pool(
            &ready,
            [0; 20],
            &mut candidates,
            LiveRecoveryConfig {
                max_parallel_connect: 1,
                ..default_live_recovery()
            },
        ),
    )
    .await
    .expect("source timeout must leave time for fallback");
    assert_eq!(pool.len(), 1);
    assert_eq!(pool[0].addr, good_addr);
    assert!(started.elapsed() >= CONNECT_TIMEOUT);
    assert!(!candidates.eligible(Instant::now()).contains(&slow_addr));
    drop(pool);
    drop(slow);
    server.await.unwrap();
}

#[tokio::test]
async fn learned_connects_deduplicate_bound_and_close_on_pool_drop() {
    let first = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(first_addr) = first.local_addr().unwrap() else {
        unreachable!()
    };
    let std::net::SocketAddr::V4(second_addr) = second.local_addr().unwrap() else {
        unreachable!()
    };
    let mut candidates = SessionCandidates::default();
    candidates.learn(first_addr, CandidateKind::Source);
    candidates.learn(second_addr, CandidateKind::Pex);
    let mut tasks = tokio::task::JoinSet::new();
    let mut pending = HashSet::new();
    let (tx, _rx) = mpsc::channel(1);
    spawn_learned_connects(
        &mut candidates,
        &BTreeMap::new(),
        [0; 20],
        1,
        &tx,
        &mut tasks,
        &mut pending,
    );
    let (stream, _) = first.accept().await.unwrap();
    candidates.attempting(first_addr, Instant::now() - Duration::from_secs(1));
    for _ in 0..5 {
        candidates.learn(first_addr, CandidateKind::Source);
        spawn_learned_connects(
            &mut candidates,
            &BTreeMap::new(),
            [0; 20],
            1,
            &tx,
            &mut tasks,
            &mut pending,
        );
    }
    assert_eq!(tasks.len(), 1);
    assert_eq!(pending.len(), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), second.accept())
            .await
            .is_err()
    );
    drop(tasks);
    let mut stream = stream;
    // A pending outbound BT handshake is followed by EOF once its owned task drops.
    let mut bytes = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(1),
        tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut bytes),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!bytes.is_empty());
}

async fn learned_source_recovers_after_outage(disconnect: bool, pex: bool, full_pool: bool) {
    let source_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let v4 = |addr| match addr {
        std::net::SocketAddr::V4(addr) => addr,
        _ => unreachable!(),
    };
    let source_addr = v4(source_listener.local_addr().unwrap());
    let relay_addr = v4(relay_listener.local_addr().unwrap());
    let source_connections = Arc::new(AtomicU32::new(0));
    let stale_requests = Arc::new(AtomicU32::new(0));
    let payload: Vec<u8> = (0..4)
        .flat_map(|cc| {
            let mut packet = vec![0x55; 188];
            packet[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10 | cc]);
            packet
        })
        .collect();
    let source_payload = payload.clone();
    let connection_count = source_connections.clone();
    let (produced_tx, produced_rx) = tokio::sync::watch::channel(false);
    let source = tokio::spawn(async move {
        let ready_at = Arc::new(std::sync::Mutex::new(None::<Instant>));
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let (stream, _) = source_listener.accept().await.unwrap();
            let number = connection_count.fetch_add(1, Ordering::Relaxed);
            let payload = source_payload.clone();
            let produced = produced_tx.clone();
            let ready_at = ready_at.clone();
            connections.spawn(async move {
                let mut session = PeerSession::new(stream);
                session.accept_handshake([0; 20], |_| true).await.unwrap();
                let hs = OutgoingExtendedHandshake {
                    mi: Some(live_pos(7, 8)),
                    ace_metadata_version: 1,
                    ut_metadata_id: 2,
                    node: NodeFields::default(),
                    peer_ip: None,
                    metadata_size: None,
                };
                session.send_extended_handshake(&hs).await.unwrap();
                session.send(&PeerMessage::Unchoke).await.unwrap();
                while let Ok(msg) = session.read_message().await {
                    if let PeerMessage::Unknown {
                        id: 6,
                        payload: request,
                    } = msg
                    {
                        let piece = u32::from_be_bytes(request[4..8].try_into().unwrap());
                        if piece == 7
                            || (piece == 8
                                && ready_at
                                    .lock()
                                    .unwrap()
                                    .is_some_and(|deadline| Instant::now() >= deadline))
                        {
                            let mut fresh = payload.clone();
                            if piece == 8 {
                                for packet in fresh.chunks_mut(188) {
                                    packet[4..].fill(0x66);
                                }
                            }
                            session
                                .send(&build_piece(0, piece, 0, [0; 8], &fresh))
                                .await
                                .unwrap();
                            if piece == 7 {
                                ready_at.lock().unwrap().get_or_insert_with(|| {
                                    Instant::now() + Duration::from_millis(1600)
                                });
                            }
                            produced.send_replace(true);
                            if disconnect && number == 0 {
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                return;
                            }
                        }
                    }
                }
            });
        }
    });
    let requests = stale_requests.clone();
    let relay = tokio::spawn(async move {
        let mut number = 0;
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let (stream, _) = relay_listener.accept().await.unwrap();
            let first = number == 0;
            number += 1;
            let requests = requests.clone();
            let mut produced = produced_rx.clone();
            connections.spawn(async move {
                    let mut session = PeerSession::new(stream);
                    session.accept_handshake([0;20], |_| true).await.unwrap();
                    let hs = OutgoingExtendedHandshake { mi:Some(if first {live_pos(7,7)} else {live_pos(1,1)}), ace_metadata_version:1,ut_metadata_id:2,node:NodeFields::default(),peer_ip:None,metadata_size:None };
                    session.send_extended_handshake(&hs).await.unwrap();
                    // Gossip is sent only on the initial connection: recovery must use the
                    // address remembered by the real session, not another announcement.
                    if first {
                        let mut announce = vec![0;14];
                        announce[8..12].copy_from_slice(&source_addr.ip().octets());
                        announce[12..14].copy_from_slice(&source_addr.port().to_be_bytes());
                        if pex {
                            let mut exchange = vec![0;16+108];
                            exchange[1..3].copy_from_slice(&1u16.to_be_bytes());
                            exchange[3..7].copy_from_slice(&17u32.to_be_bytes());
                            exchange[7..11].copy_from_slice(&108u32.to_be_bytes());
                            exchange[27..33].copy_from_slice(&announce[8..14]);
                            session.send(&PeerMessage::Unknown {id:12,payload:exchange}).await.unwrap();
                        } else {session.send(&PeerMessage::Unknown {id:36,payload:announce}).await.unwrap();}
                    }
                    loop {
                        tokio::select! {
                            msg = session.read_message() => match msg {
                                Ok(PeerMessage::Unknown {id:6,..}) if !first => {requests.fetch_add(1,Ordering::Relaxed);},
                                Ok(_) => {}, Err(_) => return,
                            },
                            _ = produced.changed(), if disconnect => {
                                tokio::time::sleep(Duration::from_millis(150)).await;
                                return;
                            }
                        }
                    }
                });
        }
    });
    let info = StreamInfo {
        infohash: [0; 20],
        piece_length: payload.len() as u64,
        chunk_length: payload.len() as u64,
        trackers: vec![],
        metadata: StreamMetadata::default(),
        sig_len: 0,
        source_pubkey: vec![],
    };
    let seed = SeedConfig {
        registry: SeedRegistry::new(),
        store_bytes: 4096,
        store_retention: None,
        enabled: false,
        prefetch_pieces: 0,
        live_recovery: LiveRecoveryConfig {
            request_timeout_ms: 100,
            request_check_interval_ms: 10,
            stale_upstream_timeout_ms: if disconnect { 800 } else { 250 },
            max_piece_advance: 1,
            max_active_upstreams: if full_pool { 1 } else { 4 },
            ..default_live_recovery()
        },
        cache_type: CacheType::Memory,
        cache_dir: PathBuf::new(),
    };
    let (tx, mut rx) = mpsc::channel(16);
    let peer_count = Arc::new(AtomicU32::new(0));
    let count = peer_count.clone();
    let discovery_calls = Arc::new(AtomicU32::new(0));
    let calls = discovery_calls.clone();
    let session = tokio::spawn(follow_live_session(
        info,
        vec![relay_addr],
        Arc::new(Identity::generate()),
        tx,
        count,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        seed,
        Arc::new(move |_| {
            calls.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { vec![] })
        }),
        None,
    ));
    let result = tokio::time::timeout(Duration::from_secs(8), async {
        let first = rx.recv().await.expect("initial source output");
        assert_eq!(first.bytes.as_ref(), &payload[..564]);
        let resumed = rx.recv().await.expect("resumed source output");
        assert!(
            resumed.bytes.contains(&0x66),
            "resumption must deliver new media, not replay the old piece"
        );
    })
    .await;
    drop(rx);
    let stopped = tokio::time::timeout(Duration::from_secs(1), session).await;
    source.abort();
    relay.abort();
    let _ = source.await;
    let _ = relay.await;
    assert!(
        stopped.is_ok(),
        "consumer close must stop the live session promptly"
    );
    assert_eq!(
        peer_count.load(Ordering::Relaxed),
        0,
        "shutdown must clear active-peer statistics"
    );
    assert!(
        result.is_ok(),
        "learned source must reconnect and resume after silence beyond stale timeout"
    );
    assert_eq!(
        discovery_calls.load(Ordering::Relaxed),
        0,
        "learned source must be retried before rediscovery"
    );
    assert!(
        source_connections.load(Ordering::Relaxed) >= 2,
        "source must be retried after teardown"
    );
    assert_eq!(
        stale_requests.load(Ordering::Relaxed),
        0,
        "stale relay must never receive media requests"
    );
}

#[tokio::test]
async fn learned_source_recovers_after_pool_stale() {
    learned_source_recovers_after_outage(false, false, false).await;
}

#[tokio::test]
async fn learned_source_recovers_after_peer_lost() {
    learned_source_recovers_after_outage(true, false, false).await;
}

#[tokio::test]
async fn pex_source_recovers_after_pool_stale() {
    learned_source_recovers_after_outage(false, true, false).await;
}

#[tokio::test]
async fn pex_source_recovers_after_peer_lost() {
    learned_source_recovers_after_outage(true, true, false).await;
}

#[tokio::test]
async fn full_pool_remembers_source_without_connecting_until_rebuild() {
    learned_source_recovers_after_outage(false, false, true).await;
}

#[tokio::test]
async fn full_pool_retains_pex_until_rebuild() {
    learned_source_recovers_after_outage(false, true, true).await;
}

fn cancellation_fixture() -> (StreamInfo, SeedConfig) {
    let info = StreamInfo {
        infohash: [0; 20],
        piece_length: 752,
        chunk_length: 752,
        trackers: vec![],
        metadata: StreamMetadata::default(),
        sig_len: 0,
        source_pubkey: vec![],
    };
    let seed = SeedConfig {
        registry: SeedRegistry::new(),
        store_bytes: 4096,
        store_retention: None,
        enabled: false,
        prefetch_pieces: 0,
        live_recovery: LiveRecoveryConfig::default(),
        cache_type: CacheType::Memory,
        cache_dir: PathBuf::new(),
    };
    (info, seed)
}

#[tokio::test]
async fn consumer_close_cancels_pending_connect() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let (info, seed) = cancellation_fixture();
    let (tx, rx) = mpsc::channel(1);
    let count = Arc::new(AtomicU32::new(0));
    let session = tokio::spawn(follow_live_session(
        info,
        vec![addr],
        Arc::new(Identity::generate()),
        tx,
        count.clone(),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        seed,
        Arc::new(|_| Box::pin(async { panic!("connect cancellation must not reach discovery") })),
        None,
    ));
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut handshake = [0; 66];
    tokio::io::AsyncReadExt::read_exact(&mut stream, &mut handshake)
        .await
        .unwrap();
    drop(rx);
    tokio::time::timeout(Duration::from_secs(1), session)
        .await
        .unwrap()
        .unwrap();
    let mut remaining = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(1),
        tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut remaining),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(count.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn consumer_close_cancels_owned_rediscovery() {
    struct Dropped(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            let _ = self.0.take().unwrap().send(());
        }
    }
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let signals = Arc::new(std::sync::Mutex::new(Some((started_tx, dropped_tx))));
    let discovery: PeerDiscovery = Arc::new(move |_| {
        let (started, dropped) = signals
            .lock()
            .unwrap()
            .take()
            .expect("one owned discovery at a time");
        Box::pin(async move {
            let _guard = Dropped(Some(dropped));
            started.send(()).unwrap();
            std::future::pending().await
        })
    });
    let (info, seed) = cancellation_fixture();
    let (tx, rx) = mpsc::channel(1);
    let session = tokio::spawn(follow_live_session(
        info,
        vec![],
        Arc::new(Identity::generate()),
        tx,
        Arc::new(AtomicU32::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        seed,
        discovery,
        None,
    ));
    tokio::time::timeout(Duration::from_secs(1), started_rx)
        .await
        .unwrap()
        .unwrap();
    drop(rx);
    tokio::time::timeout(Duration::from_secs(1), session)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), dropped_rx)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn active_source_loss_enters_cooldown_before_another_announcement() {
    let source_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(source_addr) = source_listener.local_addr().unwrap() else {
        unreachable!()
    };
    let std::net::SocketAddr::V4(relay_addr) = relay_listener.local_addr().unwrap() else {
        unreachable!()
    };
    let source_client = TcpStream::connect(source_addr).await.unwrap();
    let (source_server, _) = source_listener.accept().await.unwrap();
    let relay_client = TcpStream::connect(relay_addr).await.unwrap();
    let (relay_server, _) = relay_listener.accept().await.unwrap();
    let (lost_tx, lost_rx) = tokio::sync::oneshot::channel();
    let (announce_tx, announce_rx) = tokio::sync::oneshot::channel();
    let source = tokio::spawn(async move {
        let mut session = PeerSession::new(source_server);
        while !matches!(
            session.read_message().await.unwrap(),
            PeerMessage::Interested
        ) {}
        drop(session);
        lost_tx.send(()).unwrap();
    });
    let relay = tokio::spawn(async move {
        let mut session = PeerSession::new(relay_server);
        lost_rx.await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut payload = vec![0; 14];
        payload[8..12].copy_from_slice(&source_addr.ip().octets());
        payload[12..14].copy_from_slice(&source_addr.port().to_be_bytes());
        session
            .send(&PeerMessage::Unknown { id: 36, payload })
            .await
            .unwrap();
        announce_tx.send(()).unwrap();
        while session.read_message().await.is_ok() {}
    });
    let (info, seed) = cancellation_fixture();
    let (tx, rx) = mpsc::channel(1);
    let pool = tokio::spawn(async move {
        let mut candidates = SessionCandidates::default();
        candidates.learn(source_addr, CandidateKind::Source);
        candidates.learn(relay_addr, CandidateKind::Discovered);
        let store = Arc::new(tokio::sync::Mutex::new(PieceStore::new(
            info.piece_length,
            info.chunk_length,
            4096,
        )));
        let end = follow_peer_pool(
            vec![
                ConnectedUpstream {
                    session: PeerSession::new(relay_client),
                    addr: relay_addr,
                    window: live_pos(7, 8),
                    yourip: None,
                },
                ConnectedUpstream {
                    session: PeerSession::new(source_client),
                    addr: source_addr,
                    window: live_pos(7, 8),
                    yourip: None,
                },
            ],
            &info,
            &Identity::generate(),
            1,
            &tx,
            &Arc::new(AtomicU64::new(0)),
            &Arc::new(AtomicU64::new(0)),
            &Arc::new(AtomicU32::new(0)),
            &seed,
            &store,
            &mut None,
            vec![],
            vec![relay_addr, source_addr],
            Arc::new(|_| Box::pin(async { panic!("no discovery required") })),
            &mut candidates,
            &Arc::new(AtomicU32::new(0)),
            None,
        )
        .await;
        (end, candidates)
    });
    announce_rx.await.unwrap();
    let reconnected =
        tokio::time::timeout(Duration::from_millis(200), source_listener.accept()).await;
    drop(rx);
    let (end, candidates) = tokio::time::timeout(Duration::from_secs(1), pool)
        .await
        .unwrap()
        .unwrap();
    source.await.unwrap();
    relay.await.unwrap();
    assert!(matches!(end, FollowEnd::ConsumerGone));
    assert!(
        reconnected.is_err(),
        "a lost active source must cool down before a repeated announcement retries it"
    );
    assert!(!candidates.eligible(Instant::now()).contains(&source_addr));
}
