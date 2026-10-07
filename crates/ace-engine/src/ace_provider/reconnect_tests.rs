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

// Real-session admission fairness regression under the default recovery settings.
async fn fast_source_fairness(return_source: bool) {
    let mut listeners = Vec::new();
    for _ in 0..10 {
        listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let addrs: Vec<_> = listeners
        .iter()
        .map(|l| match l.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!(),
        })
        .collect();
    let payload: Vec<u8> = (0..4)
        .flat_map(|cc| {
            let mut packet = vec![0x55; 188];
            packet[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10 | cc]);
            packet
        })
        .collect();
    let attempts = Arc::new(AtomicU32::new(0));
    let admitted = Arc::new(AtomicU32::new(0));
    let source_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_healthy = Arc::new(tokio::sync::Notify::new());
    let source_admissions = Arc::new(AtomicU32::new(0));
    let mut servers = tokio::task::JoinSet::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let all = addrs.clone();
        let payload = payload.clone();
        let attempts = attempts.clone();
        let admitted = admitted.clone();
        let source_ready = source_ready.clone();
        let stop_healthy = stop_healthy.clone();
        let source_admissions = source_admissions.clone();
        servers.spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let all = all.clone();
                let payload = payload.clone();
                let attempts = attempts.clone();
                let admitted = admitted.clone();
                let source_ready = source_ready.clone();
                let stop_healthy = stop_healthy.clone();
                let source_admissions = source_admissions.clone();
                connections.spawn(async move {
                    if index == 9 {
                        attempts.fetch_add(1, Ordering::Relaxed);
                    }
                    let mut session = PeerSession::new(stream);
                    if index == 9 {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    if session.accept_handshake([0; 20], |_| true).await.is_err() {
                        return;
                    }
                    let resumed_source = index == 1 && source_ready.load(Ordering::Relaxed);
                    let hs = OutgoingExtendedHandshake {
                        mi: Some(live_pos(7, if resumed_source { 16 } else { 8 })),
                        ace_metadata_version: 1,
                        ut_metadata_id: 2,
                        node: NodeFields::default(),
                        peer_ip: None,
                        metadata_size: None,
                    };
                    if session.send_extended_handshake(&hs).await.is_err() {
                        return;
                    }
                    if index == 0 {
                        for source in &all[1..9] {
                            let mut announcement = vec![0; 14];
                            announcement[8..12].copy_from_slice(&source.ip().octets());
                            announcement[12..14].copy_from_slice(&source.port().to_be_bytes());
                            if session
                                .send(&PeerMessage::Unknown {
                                    id: 36,
                                    payload: announcement,
                                })
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        let mut exchange = vec![0; 124];
                        exchange[1..3].copy_from_slice(&1u16.to_be_bytes());
                        exchange[3..7].copy_from_slice(&17u32.to_be_bytes());
                        exchange[7..11].copy_from_slice(&108u32.to_be_bytes());
                        exchange[27..31].copy_from_slice(&all[9].ip().octets());
                        exchange[31..33].copy_from_slice(&all[9].port().to_be_bytes());
                        if session
                            .send(&PeerMessage::Unknown {
                                id: 12,
                                payload: exchange,
                            })
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    if (index == 9 || resumed_source)
                        && session.send(&PeerMessage::Unchoke).await.is_err()
                    {
                        return;
                    }
                    let mut first_request = true;
                    loop {
                        let msg = if index == 9 {
                            tokio::select! {
                                _ = stop_healthy.notified() => return,
                                msg = session.read_message() => msg,
                            }
                        } else {
                            session.read_message().await
                        };
                        let Ok(msg) = msg else {
                            return;
                        };
                        if index == 9 || resumed_source {
                            if matches!(msg, PeerMessage::Interested) {
                                if index == 9 {
                                    admitted.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    source_admissions.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            if let PeerMessage::Unknown {
                                id: 6,
                                payload: request,
                            } = msg
                            {
                                if return_source && index == 9 && first_request {
                                    // Let spare slots activate before productive output resets history.
                                    tokio::time::sleep(Duration::from_millis(200)).await;
                                    first_request = false;
                                }
                                let piece = u32::from_be_bytes(request[4..8].try_into().unwrap());
                                let mut payload = payload.clone();
                                if resumed_source {
                                    for packet in payload.chunks_mut(188) {
                                        packet[4..].fill(0x66);
                                    }
                                }
                                if session
                                    .send(&build_piece(0, piece, 0, [0; 8], &payload))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                    }
                });
            }
        });
    }
    let run = |bootstrap: Vec<SocketAddrV4>| {
        let (info, mut seed) = cancellation_fixture();
        seed.live_recovery = LiveRecoveryConfig::default();
        let (tx, rx) = mpsc::channel(16);
        let peer_count = Arc::new(AtomicU32::new(0));
        let count = peer_count.clone();
        let session = tokio::spawn(follow_live_session(
            info,
            bootstrap,
            Arc::new(Identity::generate()),
            tx,
            count,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            seed,
            Arc::new(|_| Box::pin(async { vec![] })),
            None,
        ));
        (rx, session, peer_count)
    };
    // Positive control: the same healthy peer supplies contiguous media promptly.
    let (mut direct_rx, direct_session, _) = run(vec![addrs[9]]);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), direct_rx.recv())
            .await
            .unwrap()
            .is_some()
    );
    drop(direct_rx);
    tokio::time::timeout(Duration::from_secs(1), direct_session)
        .await
        .unwrap()
        .unwrap();
    attempts.store(0, Ordering::Relaxed);
    admitted.store(0, Ordering::Relaxed);
    let (mut rx, session, peer_count) = run(vec![addrs[0]]);
    let count = peer_count.clone();
    let bounds = tokio::spawn(async move {
        loop {
            assert!(
                count.load(Ordering::Relaxed) <= 4,
                "active pool exceeds default bound"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    let started = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(30), rx.recv()).await;
    let first_ms = started.elapsed().as_millis();
    let mut returned = !return_source;
    if return_source && matches!(result, Ok(Some(_))) {
        source_ready.store(true, Ordering::Relaxed);
        let active = peer_count.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            peer_count.load(Ordering::Relaxed),
            active,
            "returning source must not preempt active media"
        );
        assert_eq!(source_admissions.load(Ordering::Relaxed), 0);
        stop_healthy.notify_waiters();
        returned = tokio::time::timeout(Duration::from_secs(16), async {
            while let Some(output) = rx.recv().await {
                if output.bytes.contains(&0x66) {
                    return true;
                }
            }
            false
        })
        .await
        .unwrap_or(false);
    }
    drop(rx);
    tokio::time::timeout(Duration::from_secs(1), session)
        .await
        .unwrap()
        .unwrap();
    bounds.abort();
    assert!(bounds.await.unwrap_err().is_cancelled());
    assert_eq!(peer_count.load(Ordering::Relaxed), 0);
    let attempts = attempts.load(Ordering::Relaxed);
    let admitted = admitted.load(Ordering::Relaxed);
    drop(servers);
    assert!(
        matches!(result, Ok(Some(_))),
        "healthy PEX starved for 30s: attempts={attempts}, admitted={admitted}"
    );
    assert!(admitted > 0 && attempts > 0);
    assert!(
        returned,
        "productive alternative must restore returning source preference"
    );
    if return_source {
        assert!(source_admissions.load(Ordering::Relaxed) > 0);
    }
    eprintln!("fairness control: first_ms={first_ms}, attempts={attempts}, admitted={admitted}, returning_source={return_source}");
}

#[tokio::test]
async fn fast_unproductive_sources_must_not_starve_healthy_pex() {
    // Two twelve-second nonproductive pools plus bounded handshakes fit within30s.
    fast_source_fairness(false).await;
}

#[tokio::test]
async fn productive_alternative_restores_returning_source_preference() {
    fast_source_fairness(true).await;
}

#[tokio::test]
async fn ready_learned_connections_share_pending_cap_and_close_on_cancel() {
    let mut candidates = SessionCandidates::default();
    let mut servers = tokio::task::JoinSet::new();
    let accepted = Arc::new(AtomicU32::new(0));
    let closed = Arc::new(AtomicU32::new(0));
    for _ in 0..3 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        candidates.learn(addr, CandidateKind::Pex);
        let accepted = accepted.clone();
        let closed = closed.clone();
        servers.spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                accepted.fetch_add(1, Ordering::Relaxed);
                let mut session = PeerSession::new(stream);
                session.accept_handshake([0; 20], |_| true).await.unwrap();
                session
                    .send_extended_handshake(&OutgoingExtendedHandshake {
                        mi: Some(live_pos(7, 8)),
                        ace_metadata_version: 1,
                        ut_metadata_id: 2,
                        node: NodeFields::default(),
                        peer_ip: None,
                        metadata_size: None,
                    })
                    .await
                    .unwrap();
                while session.read_message().await.is_ok() {}
                closed.fetch_add(1, Ordering::Relaxed);
            }
        });
    }
    let (tx, rx) = mpsc::channel(2);
    let mut tasks = tokio::task::JoinSet::new();
    let mut pending = HashSet::new();
    spawn_learned_connects(
        &mut candidates,
        &BTreeMap::new(),
        [0; 20],
        2,
        &tx,
        &mut tasks,
        &mut pending,
    );
    while let Some(joined) = tasks.join_next().await {
        let (_, failure, queued) = joined.unwrap();
        assert!(failure.is_none() && queued);
    }
    assert_eq!(rx.len(), 2);
    assert_eq!(pending.len(), 2);
    // These are completed transports, not running attempts. Cooldown expiry and repeated
    // gossip still cannot grow another bucket of sockets while both remain ready/owned.
    for addr in candidates.all() {
        candidates.attempting(addr, Instant::now() - Duration::from_secs(1));
    }
    for _ in 0..5 {
        spawn_learned_connects(
            &mut candidates,
            &BTreeMap::new(),
            [0; 20],
            2,
            &tx,
            &mut tasks,
            &mut pending,
        );
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(tasks.is_empty());
    assert_eq!(accepted.load(Ordering::Relaxed), 2);
    assert_eq!(pending.len(), 2);
    drop(tasks);
    drop(rx); // Pool cancellation drops owned ready transports as well as running tasks.
    tokio::time::timeout(Duration::from_secs(1), async {
        while closed.load(Ordering::Relaxed) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(servers);
}

#[tokio::test]
async fn silent_bt_total_deadline_is_not_a_window_failure() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let connect = tokio::spawn(connect_upstream(addr, [0; 20]));
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut bt = [0; 66];
    tokio::io::AsyncReadExt::read_exact(&mut stream, &mut bt)
        .await
        .unwrap();
    let attempt = connect.await.unwrap();
    let PeerConnectAttempt::Failed(failure) = attempt else {
        panic!("silent BT must time out")
    };
    assert_ne!(
        failure.stage,
        PeerConnectStage::Window,
        "total deadline expired before any BT reply/window stage"
    );
    let mut stats = PeerConnectStats::default();
    stats.record_failure(failure);
    assert!(stats.summary().contains("total_timeout=1"));
}

async fn failed_round_discovery_control(healthy_delay: Duration, output_bound: Duration) {
    let mut early = Vec::new();
    let mut servers = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        early.push(addr);
        servers.spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                // Real TCP accepts the BT request but never returns a BT handshake.
                let _ = tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut bytes).await;
            }
        });
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(healthy) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let payload: Vec<_> = (0..4)
        .flat_map(|cc| {
            let mut packet = vec![0x66; 188];
            packet[..4].copy_from_slice(&[0x47, 1, 0, 0x10 | cc]);
            packet
        })
        .collect();
    servers.spawn(async move {
        let mut clients = tokio::task::JoinSet::new();
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let payload = payload.clone();
            clients.spawn(async move {
                let mut peer = PeerSession::new(stream);
                if peer.accept_handshake([0; 20], |_| true).await.is_err() {
                    return;
                }
                if peer
                    .send_extended_handshake(&OutgoingExtendedHandshake {
                        mi: Some(live_pos(7, 8)),
                        ace_metadata_version: 1,
                        ut_metadata_id: 2,
                        node: NodeFields::default(),
                        peer_ip: None,
                        metadata_size: None,
                    })
                    .await
                    .is_err()
                {
                    return;
                }
                if peer.send(&PeerMessage::Unchoke).await.is_err() {
                    return;
                }
                while let Ok(msg) = peer.read_message().await {
                    if let PeerMessage::Unknown {
                        id: 6,
                        payload: request,
                    } = msg
                    {
                        let piece = u32::from_be_bytes(request[4..8].try_into().unwrap());
                        if peer
                            .send(&build_piece(0, piece, 0, [0; 8], &payload))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
            });
        }
    });
    let run = |bootstrap: Vec<SocketAddrV4>, discovery: PeerDiscovery| {
        let (info, seed) = cancellation_fixture();
        let (tx, rx) = mpsc::channel(16);
        let count = Arc::new(AtomicU32::new(0));
        let peers = count.clone();
        let session = tokio::spawn(follow_live_session(
            info,
            bootstrap,
            Arc::new(Identity::generate()),
            tx,
            peers,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            seed,
            discovery,
            None,
        ));
        (rx, session, count)
    };
    let (mut rx, direct, _) = run(vec![healthy], Arc::new(|_| Box::pin(async { vec![] })));
    assert!(tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .is_some());
    drop(rx);
    tokio::time::timeout(Duration::from_secs(1), direct)
        .await
        .unwrap()
        .unwrap();
    let known = early.clone();
    let discovery: PeerDiscovery = Arc::new(move |options| {
        let early = known.clone();
        Box::pin(async move {
            // Discovery I/O only: preserve ace-swarm's actual first_peer_source_with_target
            // race/cardinality semantics, independently checked in private exact-source evidence.
            // Both sources always return the same endpoints; no target-specific healthy shortcut.
            let a = async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                early
            };
            let b = async move {
                tokio::time::sleep(healthy_delay).await;
                vec![healthy]
            };
            tokio::pin!(a, b);
            tokio::select! {
                mut peers=&mut a=>{
                    if peers.len()<options.peer_target.max(1){peers.extend(b.await);}
                    peers.sort();peers.dedup();peers
                }
                mut peers=&mut b=>{
                    if peers.len()<options.peer_target.max(1){peers.extend(a.await);}
                    peers.sort();peers.dedup();peers
                }
            }
        })
    });
    let (mut rx, session, count) = run(early, discovery);
    let started = Instant::now();
    let output = tokio::time::timeout(output_bound, rx.recv()).await;
    let elapsed_ms = started.elapsed().as_millis();
    drop(rx);
    tokio::time::timeout(Duration::from_secs(1), session)
        .await
        .unwrap()
        .unwrap();
    drop(servers);
    assert_eq!(count.load(Ordering::Relaxed), 0);
    assert!(
        matches!(output, Ok(Some(_))),
        "delayed healthy discovery alternative must recover actual media within its bound"
    );
    eprintln!("failed-round discovery control: first_ms={elapsed_ms}");
}

#[tokio::test]
async fn failed_round_recovery_waits_for_delayed_discovery_alternative() {
    failed_round_discovery_control(Duration::from_millis(400), Duration::from_secs(10)).await;
}

#[tokio::test]
async fn recovery_preserves_discovery_alternative_after_dht_budget() {
    failed_round_discovery_control(Duration::from_millis(8500), Duration::from_secs(20)).await;
}
