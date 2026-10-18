use super::*;

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
                                    // The live floor now skips to head16, requiring the real
                                    // fresh discontinuity gate. Supply synthetic current PSI
                                    // and a video access point rather than bypassing that gate.
                                    payload = include_bytes!(
                                        "../../../../../tests/vectors/media/transport-resync.ts"
                                    )[..752]
                                        .to_vec();
                                    for packet in payload
                                        .chunks_mut(188)
                                        .filter(|p| ace_media::mpegts::ts_pid(p) == 0x101)
                                    {
                                        packet[6..].fill(0x66);
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
            completed_discovery(|_| Box::pin(async { vec![] })),
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
    let (mut rx, direct, _) = run(
        vec![healthy],
        completed_discovery(|_| Box::pin(async { vec![] })),
    );
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
    let discovery: PeerDiscovery = completed_discovery(move |options| {
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
