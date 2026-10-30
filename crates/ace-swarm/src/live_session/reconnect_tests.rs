//! Loopback protocol regressions exercise the production session and pool implementation.
use super::*;

fn default_live_recovery() -> LiveRecoveryConfig {
    LiveRecoveryConfig::default()
}
fn live_pos(min_piece: i64, max_piece: i64) -> WireLivePosition {
    WireLivePosition {
        min_piece,
        max_piece,
        position: -1,
        distance_from_source: 1,
    }
}

struct SelectionPeer {
    addr: SocketAddrV4,
    accepted: tokio::sync::oneshot::Receiver<Instant>,
    server: tokio::task::JoinHandle<SelectionPeerOutcome>,
}

#[derive(Default)]
struct SelectionPeerOutcome {
    accepted_at: Option<Instant>,
    handshake_accepted: bool,
    window_sent: bool,
    window_sent_at: Option<Instant>,
    socket_closed: bool,
}

async fn selection_peer(delay: Duration, window: WireLivePosition) -> SelectionPeer {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let (accepted_tx, accepted) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let accepted_at = Instant::now();
        let _ = accepted_tx.send(accepted_at);
        let mut session = PeerSession::new(stream);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        if session.accept_handshake([0; 20], |_| true).await.is_err() {
            return SelectionPeerOutcome {
                accepted_at: Some(accepted_at),
                socket_closed: true,
                ..SelectionPeerOutcome::default()
            };
        }
        if session
            .send_extended_handshake(&OutgoingExtendedHandshake {
                ace_metadata_version: 1,
                ut_metadata_id: 2,
                mi: Some(window),
                node: NodeFields::default(),
                peer_ip: None,
                metadata_size: None,
            })
            .await
            .is_err()
        {
            return SelectionPeerOutcome {
                accepted_at: Some(accepted_at),
                handshake_accepted: true,
                socket_closed: true,
                ..SelectionPeerOutcome::default()
            };
        }
        let window_sent_at = Instant::now();
        while session.read_message().await.is_ok() {}
        SelectionPeerOutcome {
            accepted_at: Some(accepted_at),
            handshake_accepted: true,
            window_sent: true,
            window_sent_at: Some(window_sent_at),
            socket_closed: true,
        }
    });
    SelectionPeer {
        addr,
        accepted,
        server,
    }
}

async fn reap_selection_peer(
    server: &mut tokio::task::JoinHandle<SelectionPeerOutcome>,
    budget: Duration,
) -> Option<SelectionPeerOutcome> {
    match tokio::time::timeout(budget, &mut *server).await {
        Ok(Ok(outcome)) => Some(outcome),
        Ok(Err(_)) => None,
        Err(_) => {
            server.abort();
            let _ = (&mut *server).await;
            None
        }
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
            unchoked: false,
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
    let started = Instant::now();
    let pool = tokio::time::timeout(
        Duration::from_secs(5),
        connect_pool(
            [0; 20],
            &mut candidates,
            LiveRecoveryConfig {
                max_parallel_connect: 1,
                ..default_live_recovery()
            },
            &mut None,
            None,
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
    let source_connections_at_announcement = Arc::new(AtomicU32::new(u32::MAX));
    let stale_requests = Arc::new(AtomicU32::new(0));
    let gossip_sends = Arc::new(AtomicU32::new(0));
    let payload: Vec<u8> = (0..4)
        .flat_map(|cc| {
            let mut packet = vec![0x55; 188];
            packet[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10 | cc]);
            packet
        })
        .collect();
    let source_payload = payload.clone();
    let connection_count = source_connections.clone();
    let piece8_ready = Arc::new(std::sync::Mutex::new(None::<Instant>));
    let source_piece8_ready = piece8_ready.clone();
    let (produced_tx, produced_rx) = tokio::sync::watch::channel(false);
    let source = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let (stream, _) = source_listener.accept().await.unwrap();
            let number = connection_count.fetch_add(1, Ordering::Relaxed);
            let payload = source_payload.clone();
            let produced = produced_tx.clone();
            let ready_at = source_piece8_ready.clone();
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
                                    .is_some_and(|ready| Instant::now() >= ready))
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
                            produced.send_replace(true);
                            if disconnect && number == 0 {
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                *ready_at.lock().unwrap() = Some(Instant::now());
                                return;
                            }
                        }
                    }
                }
            });
        }
    });
    let requests = stale_requests.clone();
    let announced_source_connections = source_connections_at_announcement.clone();
    let source_connections_for_relay = source_connections.clone();
    let sent_gossip = gossip_sends.clone();
    let relay_piece8_ready = piece8_ready.clone();
    let relay = tokio::spawn(async move {
        let mut number = 0;
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let (stream, _) = relay_listener.accept().await.unwrap();
            let first = number == 0;
            number += 1;
            if !first {
                // The relay's second transport marks the actual pool rebuild. Let piece 8
                // answer immediately on the source connection recovered into that pool.
                *relay_piece8_ready.lock().unwrap() = Some(Instant::now());
            }
            let requests = requests.clone();
            let announced_source_connections = announced_source_connections.clone();
            let source_connections_for_relay = source_connections_for_relay.clone();
            let sent_gossip = sent_gossip.clone();
            let mut produced = produced_rx.clone();
            connections.spawn(async move {
                    let mut session = PeerSession::new(stream);
                    session.accept_handshake([0;20], |_| true).await.unwrap();
                    let hs = OutgoingExtendedHandshake { mi:Some(if first {live_pos(7,7)} else {live_pos(1,1)}), ace_metadata_version:1,ut_metadata_id:2,node:NodeFields::default(),peer_ip:None,metadata_size:None };
                    session.send_extended_handshake(&hs).await.unwrap();
                    // Gossip is sent only on the initial connection: recovery must use the
                    // address remembered by the real session, not another announcement.
                    if first {
                        announced_source_connections.store(
                            source_connections_for_relay.load(Ordering::Relaxed),
                            Ordering::Relaxed,
                        );
                        let mut announce = vec![0;14];
                        announce[8..12].copy_from_slice(&source_addr.ip().octets());
                        announce[12..14].copy_from_slice(&source_addr.port().to_be_bytes());
                        if pex {
                            sent_gossip.fetch_add(1, Ordering::Relaxed);
                            let mut exchange = vec![0;16+108];
                            exchange[1..3].copy_from_slice(&1u16.to_be_bytes());
                            exchange[3..7].copy_from_slice(&17u32.to_be_bytes());
                            exchange[7..11].copy_from_slice(&108u32.to_be_bytes());
                            exchange[27..33].copy_from_slice(&announce[8..14]);
                            session.send(&PeerMessage::Unknown {id:12,payload:exchange}).await.unwrap();
                        } else {
                            sent_gossip.fetch_add(1, Ordering::Relaxed);
                            session.send(&PeerMessage::Unknown {id:36,payload:announce}).await.unwrap();
                        }
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
    let observed_media_bytes = Arc::new(AtomicU64::new(0));
    let recovery_observer = TestObserver::new(
        Arc::new(AtomicU64::new(0)),
        observed_media_bytes.clone(),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        Arc::new(AtomicU32::new(0)),
    );
    let mut seed = SeedConfig {
        metrics: Some(recovery_observer.clone()),
        registry: SeedRegistry::new(),
        store_factory: {
            let piece_length = info.piece_length;
            let chunk_length = info.chunk_length;
            Arc::new(move |_| PieceStore::new(piece_length, chunk_length, 4096))
        },
        enabled: false,
        // The fixture emits piece7 before its advertised head8.
        prefetch_pieces: 1,
        live_recovery: LiveRecoveryConfig {
            request_timeout_ms: 100,
            request_check_interval_ms: 10,
            stale_upstream_timeout_ms: if disconnect { 800 } else { 250 },
            max_piece_advance: 1,
            max_active_upstreams: if full_pool { 1 } else { 4 },
            ..default_live_recovery()
        },

        warm_peers: WarmPeerCache::memory(),
    };
    let registry = seed.registry.clone();
    let infohash = info.infohash;
    let creations = Arc::new(AtomicU32::new(0));
    let factory = seed.store_factory.clone();
    let created = creations.clone();
    seed.store_factory = Arc::new(move |hash| {
        created.fetch_add(1, Ordering::Relaxed);
        factory(hash)
    });
    let (tx, mut rx) = mpsc::channel(16);
    let peer_count = Arc::new(AtomicU32::new(0));
    let count = peer_count.clone();
    let discovery_calls = Arc::new(AtomicU32::new(0));
    let calls = discovery_calls.clone();
    let discovery_evidence = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded_evidence = discovery_evidence.clone();
    let source_count_at_discovery = source_connections.clone();
    let media_bytes_at_discovery = observed_media_bytes.clone();
    let observer_at_discovery = recovery_observer.clone();
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
        completed_discovery(move |_| {
            calls.fetch_add(1, Ordering::Relaxed);
            let observed = observer_at_discovery.snapshot();
            recorded_evidence.lock().unwrap().push((
                source_count_at_discovery.load(Ordering::Relaxed),
                media_bytes_at_discovery.load(Ordering::Relaxed),
                observed.recovery_events,
            ));
            Box::pin(async { vec![] })
        }),
        None,
    ));
    let result = tokio::time::timeout(Duration::from_secs(8), async {
        let first = rx.recv().await.expect("initial source output");
        assert_eq!(first.bytes.as_ref(), &payload[..564]);
        let inbound_store = registry
            .get(&infohash)
            .expect("producer store visible to inbound consumers");
        let resumed = rx.recv().await.expect("resumed source output");
        assert!(Arc::ptr_eq(
            &inbound_store,
            &registry
                .get(&infohash)
                .expect("same store remains published after reconnect")
        ));
        assert_eq!(
            creations.load(Ordering::Relaxed),
            1,
            "one producer store for the whole session"
        );
        assert!(
            resumed.bytes.contains(&0x66),
            "resumption must deliver new media, not replay the old piece"
        );
    })
    .await;
    drop(rx);
    let stopped = tokio::time::timeout(Duration::from_secs(1), session).await;
    assert!(
        !registry.serves(&infohash),
        "session teardown releases its producer lease"
    );
    assert_eq!(creations.load(Ordering::Relaxed), 1);
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
    let observed = recovery_observer.snapshot();
    assert_eq!(
        observed.active_upstreams, 0,
        "teardown clears the upstream gauge"
    );
    let expected_events = if disconnect {
        vec![RecoveryEvent::UpstreamReconnect]
    } else {
        vec![
            RecoveryEvent::StaleTeardown,
            RecoveryEvent::UpstreamReconnect,
        ]
    };
    assert_eq!(
        observed.recovery_events, expected_events,
        "pool loss is counted once, and the successful learned retry is counted once"
    );
    assert!(
        result.is_ok(),
        "learned source must reconnect and resume after silence beyond stale timeout"
    );
    let expected_discovery_calls = if full_pool && !disconnect { 1 } else { 0 };
    assert_eq!(
        discovery_calls.load(Ordering::Relaxed),
        expected_discovery_calls,
        "discovery follows learned-source progress and stale teardown when the sole eligible relay is stale"
    );
    let discovery_evidence = discovery_evidence.lock().unwrap();
    assert_eq!(discovery_evidence.len(), expected_discovery_calls as usize);
    for (index, (source_count, media_bytes, recovery_events)) in
        discovery_evidence.iter().enumerate()
    {
        eprintln!(
            "rediscovery evidence: call={} source_connections={} output_bytes={} recovery_events={recovery_events:?}",
            index + 1,
            source_count,
            media_bytes
        );
        assert!(
            *source_count > 0,
            "rediscovery must follow a learned-source connection"
        );
        assert!(
            *media_bytes > 0,
            "rediscovery must follow actual first output"
        );
        assert!(
            recovery_events.contains(&RecoveryEvent::StaleTeardown),
            "rediscovery must follow the observed stale teardown"
        );
    }
    assert_eq!(
        source_connections_at_announcement.load(Ordering::Relaxed),
        0,
        "the remembered source must not be connected before its real relay announcement"
    );
    assert_eq!(
        gossip_sends.load(Ordering::Relaxed),
        1,
        "the relay gossips only once"
    );
    eprintln!(
        "learned-source fixture: full_pool={full_pool} pex={pex} source_connections={} discovery_calls={} source_connections_at_announcement={} gossip_sends={} stale_requests={}",
        source_connections.load(Ordering::Relaxed),
        discovery_calls.load(Ordering::Relaxed),
        source_connections_at_announcement.load(Ordering::Relaxed),
        gossip_sends.load(Ordering::Relaxed),
        stale_requests.load(Ordering::Relaxed)
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
async fn idle_pool_turn_replenishes_request_credit_without_inbound_peer_frames() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let client = TcpStream::connect(addr).await.unwrap();
    let (server_stream, _) = listener.accept().await.unwrap();

    let (mut info, mut seed) = cancellation_fixture();
    info.piece_length = 752;
    info.chunk_length = 752;
    let policy = LiveRecoveryConfig {
        request_timeout_ms: 5_000,
        stale_upstream_timeout_ms: 10_000,
        request_check_interval_ms: 10,
        max_piece_advance: 256,
        ..default_live_recovery()
    };
    seed.live_recovery = policy;
    seed.prefetch_pieces = 256;
    let (continuity, _) = Continuity::fresh(&info, 1, 256, 256, policy);
    let (output_tx, output_rx) = mpsc::channel(4);
    let downloaded = Arc::new(AtomicU64::new(0));
    let uploaded = Arc::new(AtomicU64::new(0));
    let peers_served = Arc::new(AtomicU32::new(0));
    let peer_count = Arc::new(AtomicU32::new(0));
    let identity = Identity::generate();
    let store = Arc::new(tokio::sync::Mutex::new(PieceStore::new(
        info.piece_length,
        info.chunk_length,
        4096,
    )));
    let mut candidates = SessionCandidates::default();
    let started_at = Instant::now();
    let mut server = tokio::spawn(async move {
        let mut session = PeerSession::new(server_stream);
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            if !matches!(
                session.read_message().await,
                Ok(PeerMessage::Extended { ext_id: 0, .. })
            ) {
                return Err("pool did not send its extended handshake".to_string());
            }
            if !matches!(session.read_message().await, Ok(PeerMessage::Interested)) {
                return Err("pool did not send Interested".to_string());
            }
            let mut pieces = Vec::with_capacity(128);
            while pieces.len() < 128 {
                let message = session
                    .read_message()
                    .await
                    .map_err(|error| format!("peer socket ended early: {error:?}"))?;
                match message {
                    PeerMessage::Unknown { id: 6, payload } if payload.len() == 10 => {
                        pieces.push(u32::from_be_bytes(payload[4..8].try_into().unwrap()) as u64);
                    }
                    other => return Err(format!("unexpected outbound message: {other:?}")),
                }
            }
            Ok(pieces)
        })
        .await;
        result
            .unwrap_or_else(|_| Err("peer did not receive 128 requests within two seconds".into()))
    });
    let mut pool = tokio::spawn(async move {
        let mut continuity = Some(continuity);
        follow_peer_pool(
            vec![ConnectedUpstream {
                session: PeerSession::new(client),
                addr,
                window: live_pos(1, 256),
                unchoked: true,
                yourip: None,
            }],
            &info,
            &identity,
            1,
            &output_tx,
            &downloaded,
            &uploaded,
            &peers_served,
            &seed,
            &store,
            &mut continuity,
            vec![],
            vec![],
            completed_discovery(|_| Box::pin(async { vec![] })),
            &mut candidates,
            &peer_count,
            None,
        )
        .await
    });

    let result = tokio::time::timeout(Duration::from_secs(3), async {
        let server_result = (&mut server).await;
        let pool_result = (&mut pool).await;
        (server_result, pool_result)
    })
    .await;
    let timed_out_cleanup = if result.is_err() {
        server.abort();
        pool.abort();
        tokio::time::timeout(Duration::from_secs(1), async {
            let _ = (&mut server).await;
            let _ = (&mut pool).await;
        })
        .await
        .is_ok()
    } else {
        true
    };
    drop(output_rx);
    let (server_result, pool_result) = result.expect("pool/local peer exceeded three seconds");
    let pieces = server_result
        .expect("local peer task panicked")
        .expect("local peer failed before receiving 128 requests");
    let end = pool_result.expect("pool task panicked");
    let elapsed = started_at.elapsed();

    assert!(timed_out_cleanup);
    assert_eq!(pieces, (1..=128).collect::<Vec<_>>());
    assert!(
        elapsed < policy.request_timeout(),
        "no request retransmit deadline should fire during idle refill: {elapsed:?}"
    );
    assert!(matches!(end, FollowEnd::PeerLost(1)));
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
        metrics: None,
        registry: SeedRegistry::new(),
        store_factory: test_store_factory(4096, None),
        enabled: false,
        prefetch_pieces: 0,
        live_recovery: LiveRecoveryConfig::default(),

        warm_peers: WarmPeerCache::memory(),
    };
    (info, seed)
}

#[tokio::test]
async fn retired_same_address_worker_events_cannot_target_fresh_source_runtime() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let (info, seed) = cancellation_fixture();
    let (mut continuity, _) = Continuity::fresh(&info, 7, 10, 1, seed.live_recovery);
    let identity = Identity::generate();
    let (events, mut receiver) = mpsc::channel(8);
    let mut peers = BTreeMap::new();
    for peer_id in [41, 42] {
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let server = tokio::spawn(async move {
            let mut session = PeerSession::new(server);
            assert!(matches!(
                session.read_message().await.unwrap(),
                PeerMessage::Extended { .. }
            ));
            assert!(matches!(
                session.read_message().await.unwrap(),
                PeerMessage::Interested
            ));
            session
                .send(&build_piece(
                    0,
                    9,
                    0,
                    [0; 8],
                    &[if peer_id == 41 { 0x55 } else { 0x66 }; 752],
                ))
                .await
                .unwrap();
            if peer_id == 42 {
                while session.read_message().await.is_ok() {}
            }
        });
        let (mut runtime, _) = activate_upstream_peer(ActivationInputs {
            peer_id,
            upstream: ConnectedUpstream {
                session: PeerSession::new(client),
                addr,
                window: live_pos(7, 10),
                unchoked: false,
                yourip: None,
            },
            start: 9,
            identity: &identity,
            continuity: &mut continuity,
            event_tx: &events,
            reachability: None,
            replacing: None,
        })
        .await
        .unwrap();
        if peer_id == 41 {
            // Both the actual old block and EOF event are queued before its transport
            // is retired and a fresh runtime at the identical endpoint is admitted.
            server.await.unwrap();
            tokio::time::timeout(Duration::from_secs(1), &mut runtime.worker)
                .await
                .unwrap()
                .unwrap();
            peers.insert(peer_id, runtime);
            assert_eq!(
                drop_peer_runtime(peer_id, &mut peers, &mut continuity),
                Some(addr)
            );
        } else {
            peers.insert(peer_id, runtime);
            let old_block = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap();
            let old_loss = receiver.recv().await.unwrap();
            assert!(matches!(
                old_block,
                PeerEvent::Message {
                    peer_id: 41,
                    msg: PeerMessage::Piece { .. },
                    ..
                }
            ));
            assert!(matches!(old_loss, PeerEvent::Lost { peer_id: 41, .. }));
            assert!(!old_block.is_current(&peers));
            assert!(!old_loss.is_current(&peers));
            let fresh_block = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(fresh_block.is_current(&peers));
            assert!(
                matches!(fresh_block, PeerEvent::Message { peer_id: 42, msg: PeerMessage::Piece {ref block,..},..} if block.contains(&0x66))
            );
            let runtime = peers.get_mut(&peer_id).unwrap();
            runtime.commands.send(PeerCommand::Stop).await.unwrap();
            tokio::time::timeout(Duration::from_secs(1), &mut runtime.worker)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                drop_peer_runtime(peer_id, &mut peers, &mut continuity),
                Some(addr)
            );
            tokio::time::timeout(Duration::from_secs(1), server)
                .await
                .unwrap()
                .unwrap();
        }
    }
    assert!(peers.is_empty());
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
        completed_discovery(|_| {
            Box::pin(async { panic!("connect cancellation must not reach discovery") })
        }),
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
    let discovery: PeerDiscovery = completed_discovery(move |_| {
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
                    unchoked: false,
                    yourip: None,
                },
                ConnectedUpstream {
                    session: PeerSession::new(source_client),
                    addr: source_addr,
                    window: live_pos(7, 8),
                    unchoked: false,
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
            completed_discovery(|_| Box::pin(async { panic!("no discovery required") })),
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
#[tokio::test]
async fn stale_zero_admission_cohort_must_not_lock_out_returning_source() {
    let mut listeners = Vec::new();
    for _ in 0..73 {
        listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let addrs: Vec<_> = listeners
        .iter()
        .map(|l| match l.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!(),
        })
        .collect();
    let available = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let attempts = Arc::new(AtomicU32::new(0));
    let mut servers = tokio::task::JoinSet::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let all = addrs.clone();
        let available = available.clone();
        let attempts = attempts.clone();
        servers.spawn(async move {
            let mut clients = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let all = all.clone();
                let available = available.clone();
                let attempts = attempts.clone();
                clients.spawn(async move {
                    let ready = index == 1 && available.load(Ordering::Relaxed);
                    if ready {
                        attempts.fetch_add(1, Ordering::Relaxed);
                    }
                    let mut peer = PeerSession::new(stream);
                    if peer.accept_handshake([0; 20], |_| true).await.is_err() {
                        return;
                    }
                    if peer
                        .send_extended_handshake(&OutgoingExtendedHandshake {
                            mi: Some(if index < 9 {
                                live_pos(7, 8)
                            } else {
                                live_pos(1, 1)
                            }),
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
                    if index == 0 {
                        for addr in &all[1..9] {
                            let mut payload = vec![0; 14];
                            payload[8..12].copy_from_slice(&addr.ip().octets());
                            payload[12..14].copy_from_slice(&addr.port().to_be_bytes());
                            if peer
                                .send(&PeerMessage::Unknown { id: 36, payload })
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        for addr in &all[9..] {
                            let mut payload = vec![0; 124];
                            payload[1..3].copy_from_slice(&1u16.to_be_bytes());
                            payload[3..7].copy_from_slice(&17u32.to_be_bytes());
                            payload[7..11].copy_from_slice(&108u32.to_be_bytes());
                            payload[27..31].copy_from_slice(&addr.ip().octets());
                            payload[31..33].copy_from_slice(&addr.port().to_be_bytes());
                            if peer
                                .send(&PeerMessage::Unknown { id: 12, payload })
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                    if ready && peer.send(&PeerMessage::Unchoke).await.is_err() {
                        return;
                    }
                    while let Ok(msg) = peer.read_message().await {
                        if ready {
                            if let PeerMessage::Unknown {
                                id: 6,
                                payload: req,
                            } = msg
                            {
                                let piece = u32::from_be_bytes(req[4..8].try_into().unwrap());
                                let payload: Vec<_> = (0..4)
                                    .flat_map(|cc| {
                                        let mut p = vec![0x66; 188];
                                        p[..4].copy_from_slice(&[0x47, 1, 0, 0x10 | cc]);
                                        p
                                    })
                                    .collect();
                                if peer
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
        seed.live_recovery.stale_upstream_timeout_ms = 5000;
        let (tx, rx) = mpsc::channel(16);
        let task = tokio::spawn(follow_live_session(
            info,
            bootstrap,
            Arc::new(Identity::generate()),
            tx,
            Arc::new(AtomicU32::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            seed,
            completed_discovery(|_| Box::pin(async { vec![] })),
            None,
        ));
        (rx, task)
    };
    let (mut rx, direct) = run(vec![addrs[1]]);
    assert!(tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .is_some());
    drop(rx);
    tokio::time::timeout(Duration::from_secs(1), direct)
        .await
        .unwrap()
        .unwrap();
    available.store(false, Ordering::Relaxed);
    attempts.store(0, Ordering::Relaxed);
    let started = Instant::now();
    let (mut rx, session) = run(vec![addrs[0]]);
    let ready = available.clone();
    let wake = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(20)).await;
        ready.store(true, Ordering::Relaxed);
    });
    let output = tokio::time::timeout(Duration::from_secs(30), rx.recv()).await;
    let elapsed = started.elapsed();
    drop(rx);
    tokio::time::timeout(Duration::from_secs(1), session)
        .await
        .unwrap()
        .unwrap();
    wake.await.unwrap();
    drop(servers);
    eprintln!(
        "stale-cohort control: first_ms={}, healthy_attempts={}",
        elapsed.as_millis(),
        attempts.load(Ordering::Relaxed)
    );
    assert!(
        attempts.load(Ordering::Relaxed) > 0,
        "returning source received no healthy attempt"
    );
    assert!(
        matches!(output, Ok(Some(_))),
        "returning source starved by stale cohort: healthy attempts={}",
        attempts.load(Ordering::Relaxed)
    );
}

#[tokio::test]
async fn discovered_refill_preserves_overlapping_learned_reservation() {
    let a = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let c = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = |listener: &tokio::net::TcpListener| match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    let addrs = [addr(&a), addr(&b), addr(&c)];
    let gate = Arc::new(tokio::sync::Notify::new());
    let waiting = Arc::new(AtomicU32::new(0));
    let closed = Arc::new(AtomicU32::new(0));
    let mut servers = tokio::task::JoinSet::new();
    for listener in [a, b] {
        let gate = gate.clone();
        let waiting = waiting.clone();
        let closed = closed.clone();
        servers.spawn(async move {
            let mut clients = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let gate = gate.clone();
                let waiting = waiting.clone();
                let closed = closed.clone();
                clients.spawn(async move {
                    let mut peer = PeerSession::new(stream);
                    peer.accept_handshake([0; 20], |_| true).await.unwrap();
                    waiting.fetch_add(1, Ordering::Relaxed);
                    gate.notified().await;
                    peer.send_extended_handshake(&OutgoingExtendedHandshake {
                        mi: Some(live_pos(7, 8)),
                        ace_metadata_version: 1,
                        ut_metadata_id: 2,
                        node: NodeFields::default(),
                        peer_ip: None,
                        metadata_size: None,
                    })
                    .await
                    .unwrap();
                    while peer.read_message().await.is_ok() {}
                    closed.fetch_add(1, Ordering::Relaxed);
                });
            }
        });
    }
    let (tx, rx) = mpsc::channel(2);
    let mut candidates = SessionCandidates::default();
    candidates.learn(addrs[0], CandidateKind::Source);
    candidates.learn(addrs[1], CandidateKind::Discovered);
    candidates.learn(addrs[2], CandidateKind::Discovered);
    let mut tasks = tokio::task::JoinSet::new();
    let mut pending = HashSet::new();
    spawn_candidate_connects(CandidateConnectInputs {
        held: None,
        candidates: &mut candidates,
        peers: &BTreeMap::new(),
        infohash: [0; 20],
        max_parallel: 2,
        tx: &tx,
        tasks: &mut tasks,
        pending: &mut pending,
        include_discovered: true,
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while waiting.load(Ordering::Relaxed) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // A duplicate discovered hint arrives while the source reservation still owns A.
    let duplicate = completed_discovery(move |_| async move { vec![addrs[0]] });
    let mut run = Some(DiscoveryRun::start(&duplicate, DiscoveryOptions::default()));
    let learned = next_discovery_peer(&mut run).await.unwrap();
    candidates.learn(learned, CandidateKind::Discovered);
    assert!(
        pending.contains(&addrs[0]),
        "discovered hint cleared source reservation"
    );
    gate.notify_waiters();
    while let Some(joined) = tasks.join_next().await {
        let (_, failure, queued) = joined.unwrap();
        assert!(failure.is_none() && queued);
    }
    assert_eq!(rx.len(), 2); // Both actual learned connections are ready and owned.
    spawn_candidate_connects(CandidateConnectInputs {
        held: None,
        candidates: &mut candidates,
        peers: &BTreeMap::new(),
        infohash: [0; 20],
        max_parallel: 2,
        tx: &tx,
        tasks: &mut tasks,
        pending: &mut pending,
        include_discovered: true,
    });
    let unexpected = tokio::time::timeout(Duration::from_millis(100), c.accept()).await;
    let exceeds_cap = unexpected.is_ok();
    drop(unexpected);
    drop(tasks);
    drop(rx);
    tokio::time::timeout(Duration::from_secs(1), async {
        while closed.load(Ordering::Relaxed) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(servers);
    assert!(
        !exceeds_cap,
        "duplicate discovered A hint released source A, allowing C alongside ready A/B under cap2"
    );
}

#[tokio::test]
async fn discovered_refill_reports_completed_transport_failure() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        drop(socket);
    });
    let (tx, _rx) = mpsc::channel(1);
    let mut candidates = SessionCandidates::default();
    candidates.learn(addr, CandidateKind::Discovered);
    let mut pending = HashSet::new();
    let mut tasks = tokio::task::JoinSet::new();
    spawn_candidate_connects(CandidateConnectInputs {
        held: None,
        candidates: &mut candidates,
        peers: &BTreeMap::new(),
        infohash: [0; 20],
        max_parallel: 1,
        tx: &tx,
        tasks: &mut tasks,
        pending: &mut pending,
        include_discovered: true,
    });
    assert!(pending.contains(&addr));
    let completion = tokio::time::timeout(Duration::from_secs(1), tasks.join_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    server.await.unwrap();
    assert!(
        completion.1.is_some(),
        "real completed discovered failure required"
    );
    assert!(finish_candidate_connect(
        completion,
        &mut pending,
        &mut candidates
    ));
    assert!(!pending.contains(&addr));
    assert_eq!(candidates.explorations(addr), 1);
    assert!(candidates.eligible(Instant::now()).is_empty());
    candidates.learn(addr, CandidateKind::Source);
    assert_eq!(
        candidates.explorations(addr),
        1,
        "announcement reset completed opportunity"
    );
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RetainedWindowUpdate {
    Id4,
    StructuredExtended,
    StructuredUnknown,
    DuplicateExtended,
    DuplicateUnknown,
    UnknownInvalidSequence,
}

// Real pool/worker control for per-peer window updates behind another peer's shared head.
async fn retained_producer_window_control(
    producer_first: bool,
    head_only_max: u32,
    producer_max_piece: u32,
    update: RetainedWindowUpdate,
) {
    const INVALID_WINDOW_PAYLOADS: [&[u8]; 6] = [
        b"not-bencode",
        b"d1:ai1ee",
        b"d9:max_piece1:xe",
        b"d9:max_piecei-1ee",
        b"d9:max_piecei4294967296ee",
        b"d9:max_piecei0ee",
    ];
    let received = Arc::new(AtomicU64::new(0));
    let mut upstreams = Vec::new();
    let mut servers = tokio::task::JoinSet::new();
    let mut commands = Vec::new();
    let mut requested = Vec::new();
    let mut requests = Vec::new();
    for healthy in [true, false] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        let client = TcpStream::connect(addr).await.unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        upstreams.push(ConnectedUpstream {
            session: PeerSession::new(client),
            addr,
            window: live_pos(7, 8),
            unchoked: false,
            yourip: None,
        });
        let (command, mut control) = mpsc::channel::<()>(1);
        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        requests.push(seen.clone());
        commands.push(command);
        requested.push(Some(request_rx));
        servers.spawn(async move {
            let mut session = PeerSession::new(stream);
            let mut request_tx = Some(request_tx);
            let mut update_count = 0_u8;
            loop {
                tokio::select! {
                    command = control.recv() => {
                        if command.is_none() { break; }
                        let max_piece = if healthy { producer_max_piece } else { head_only_max };
                        if healthy
                            && matches!(update, RetainedWindowUpdate::UnknownInvalidSequence)
                        {
                            for payload in INVALID_WINDOW_PAYLOADS {
                                session.send(&PeerMessage::Unknown {
                                    id: 99,
                                    payload: payload.to_vec(),
                                }).await.unwrap();
                            }
                            // This same-max marker is an ordered observer barrier. It changes
                            // preliminary coverage stats, but cannot extend B's runtime window.
                            let marker = OutgoingExtendedHandshake {
                                ace_metadata_version: 1,
                                ut_metadata_id: 2,
                                mi: Some(live_pos(6, 8)),
                                node: NodeFields::default(),
                                peer_ip: None,
                                metadata_size: None,
                            }.encode_payload();
                            session.send(&PeerMessage::Unknown {
                                id: 99,
                                payload: marker,
                            }).await.unwrap();
                        } else if healthy
                            && update_count > 0
                            && matches!(
                                update,
                                RetainedWindowUpdate::DuplicateExtended
                                    | RetainedWindowUpdate::DuplicateUnknown
                            )
                        {
                            let duplicate_payload = OutgoingExtendedHandshake {
                                ace_metadata_version: 1,
                                ut_metadata_id: 2,
                                mi: Some(live_pos(6, i64::from(max_piece))),
                                node: NodeFields::default(),
                                peer_ip: None,
                                metadata_size: None,
                            }
                            .encode_payload();
                            let marker_payload = OutgoingExtendedHandshake {
                                ace_metadata_version: 1,
                                ut_metadata_id: 2,
                                mi: Some(live_pos(6 - i64::from(update_count), i64::from(max_piece))),
                                node: NodeFields::default(),
                                peer_ip: None,
                                metadata_size: None,
                            }
                            .encode_payload();
                            let msg = |payload| match update {
                                RetainedWindowUpdate::DuplicateExtended => {
                                    PeerMessage::Extended { ext_id: 0, payload }
                                }
                                RetainedWindowUpdate::DuplicateUnknown => {
                                    PeerMessage::Unknown { id: 99, payload }
                                }
                                _ => unreachable!(),
                            };
                            // The first frame is an identical max update; the following
                            // distinct minimum is an event barrier on the same stream.
                            session.send(&msg(duplicate_payload)).await.unwrap();
                            session.send(&msg(marker_payload)).await.unwrap();
                        } else if healthy
                            && matches!(
                                update,
                                RetainedWindowUpdate::StructuredExtended
                                    | RetainedWindowUpdate::StructuredUnknown
                                    | RetainedWindowUpdate::DuplicateExtended
                                    | RetainedWindowUpdate::DuplicateUnknown
                            )
                        {
                            // min_piece=6 distinguishes pool observation of this update from
                            // the other peer's already-established 7..=10 window.
                            let payload = OutgoingExtendedHandshake {
                                ace_metadata_version: 1,
                                ut_metadata_id: 2,
                                mi: Some(live_pos(6, i64::from(max_piece))),
                                node: NodeFields::default(),
                                peer_ip: None,
                                metadata_size: None,
                            }
                            .encode_payload();
                            let message = match update {
                                RetainedWindowUpdate::StructuredExtended
                                | RetainedWindowUpdate::DuplicateExtended => {
                                    PeerMessage::Extended { ext_id: 0, payload }
                                }
                                RetainedWindowUpdate::StructuredUnknown
                                | RetainedWindowUpdate::DuplicateUnknown => {
                                    PeerMessage::Unknown { id: 99, payload }
                                }
                                RetainedWindowUpdate::Id4
                                | RetainedWindowUpdate::UnknownInvalidSequence => unreachable!(),
                            };
                            session.send(&message).await.unwrap();
                        } else {
                            let mut payload = vec![0; 8];
                            payload[4..].copy_from_slice(&max_piece.to_be_bytes());
                            session.send(&PeerMessage::Unknown { id: 4, payload }).await.unwrap();
                        }
                        update_count = update_count.saturating_add(1);
                    }
                    message = session.read_message() => {
                        let Ok(message) = message else { break; };
                        if healthy && matches!(message, PeerMessage::Interested) {
                            session.send(&PeerMessage::Unchoke).await.unwrap();
                        }
                        if let PeerMessage::Unknown { id: 6, payload } = message {
                            let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                            seen.lock().unwrap().push(piece);
                            if piece == 9 {
                                if let Some(sender) = request_tx.take() {
                                    let _ = sender.send(());
                                }
                                if !healthy {
                                    // The head-only peer makes request ordering observable, then
                                    // stops receiving useful data. Its reservation expires normally.
                                    session.send(&PeerMessage::Choke).await.unwrap();
                                }
                            }
                            if healthy
                                && (!matches!(
                                    update,
                                    RetainedWindowUpdate::DuplicateExtended
                                        | RetainedWindowUpdate::DuplicateUnknown
                                ) || piece <= 8 || piece == 9)
                            {
                                let marker = if piece <= 8 { 0x44 } else { 0x66 };
                                let media: Vec<_> = (0..4).flat_map(|cc| {
                                    let mut packet = vec![marker; 188];
                                    packet[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10 | cc]);
                                    packet
                                }).collect();
                                if session.send(&build_piece(0, piece, 0, [0; 8], &media)).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });
    }
    let (info, mut seed) = cancellation_fixture();
    seed.prefetch_pieces = u64::from(head_only_max.max(producer_max_piece) - 9);
    seed.live_recovery.max_active_upstreams = 2;
    let duplicate_chatter = matches!(
        update,
        RetainedWindowUpdate::DuplicateExtended | RetainedWindowUpdate::DuplicateUnknown
    );
    if duplicate_chatter {
        seed.live_recovery.stale_upstream_timeout_ms = 5_000;
        seed.live_recovery.request_check_interval_ms = 100;
    }
    let downloaded = Arc::new(AtomicU64::new(0));
    let observer = TestObserver::new(
        downloaded.clone(),
        received.clone(),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        Arc::new(AtomicU32::new(0)),
    );
    seed.metrics = Some(observer.clone());
    let (tx, rx) = mpsc::channel(16);
    let mut rx = Some(rx);
    let addresses: Vec<_> = upstreams.iter().map(|peer| peer.addr).collect();
    let mut pool = tokio::spawn(async move {
        let mut candidates = SessionCandidates::default();
        candidates.learn(addresses[0], CandidateKind::Pex);
        candidates.learn(addresses[1], CandidateKind::Source);
        let store = Arc::new(tokio::sync::Mutex::new(PieceStore::new(752, 752, 4096)));
        let mut continuity = None;
        let end = follow_peer_pool(
            upstreams,
            &info,
            &Identity::generate(),
            1,
            &tx,
            &Arc::new(AtomicU64::new(0)),
            &Arc::new(AtomicU64::new(0)),
            &Arc::new(AtomicU32::new(0)),
            &seed,
            &store,
            &mut continuity,
            vec![],
            addresses,
            completed_discovery(|_| Box::pin(async { panic!("no discovery required") })),
            &mut candidates,
            &Arc::new(AtomicU32::new(0)),
            None,
        )
        .await;
        let continuity = continuity.unwrap();
        (end, continuity.head, continuity.reasm.next_needed())
    });
    // Require both original pieces through the actual media path before advancing either head.
    let initial_output = tokio::time::timeout(Duration::from_secs(2), async {
        let mut bytes = 0;
        while bytes < 7 * 188 {
            let Some(output) = rx.as_mut().unwrap().recv().await else {
                return false;
            };
            if !output.bytes.contains(&0x44) {
                return false;
            }
            bytes += output.bytes.len();
        }
        true
    })
    .await
    .unwrap_or(false);
    let first = if producer_first { 0 } else { 1 };
    let first_barrier = if initial_output && commands[first].send(()).await.is_ok() {
        if first == 1 {
            // The head-only peer is deliberately choked: observe its accepted head update
            // after the initial producer has advanced the cursor to piece 9.
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let position = observer.snapshot().livepos;
                    if position.as_ref().and_then(|p| p.last) == Some(u64::from(head_only_max))
                        && position.as_ref().and_then(|p| p.pos) == Some(9)
                    {
                        return true;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap_or(false)
        } else {
            tokio::time::timeout(Duration::from_secs(2), requested[first].take().unwrap())
                .await
                .is_ok_and(|result| result.is_ok())
        }
    } else {
        false
    };
    let update_sent = if first_barrier {
        commands[1 - first].send(()).await.is_ok()
    } else {
        false
    };
    let observed_window_update = if update_sent && update != RetainedWindowUpdate::Id4 {
        let expected_min = match update {
            RetainedWindowUpdate::UnknownInvalidSequence => 6,
            _ => 6,
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if observer
                    .snapshot()
                    .livepos
                    .as_ref()
                    .and_then(|p| p.live_first)
                    == Some(expected_min)
                {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or(false)
    } else {
        update_sent
    };
    let healthy_requested_piece9 = if observed_window_update {
        if first == 0 {
            // The healthy producer's request was the first ordering barrier.
            first_barrier
        } else {
            tokio::time::timeout(Duration::from_millis(750), requested[0].take().unwrap())
                .await
                .is_ok_and(|result| result.is_ok())
        }
    } else {
        false
    };
    let output = tokio::time::timeout(Duration::from_secs(1), async {
        while let Some(output) = rx.as_mut().unwrap().recv().await {
            if output.bytes.contains(&0x66) {
                return true;
            }
        }
        false
    })
    .await;
    let output_at = Instant::now();
    let duplicate_barriers = if duplicate_chatter && matches!(output, Ok(true)) {
        let mut all_observed = true;
        for expected_min in [5, 4, 3, 2] {
            if commands[0].send(()).await.is_err() {
                all_observed = false;
                break;
            }
            let observed = tokio::time::timeout(Duration::from_millis(250), async {
                loop {
                    if observer
                        .snapshot()
                        .livepos
                        .as_ref()
                        .and_then(|p| p.live_first)
                        == Some(expected_min)
                    {
                        return true;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap_or(false);
            if !observed {
                all_observed = false;
                break;
            }
        }
        all_observed
    } else {
        !duplicate_chatter
    };
    if !duplicate_chatter {
        drop(rx.take());
    }
    let pool_outcome = match tokio::time::timeout(
        if duplicate_chatter {
            Duration::from_secs(7)
        } else {
            Duration::from_secs(1)
        },
        &mut pool,
    )
    .await
    {
        Ok(Ok(outcome)) => Some(outcome),
        _ => None,
    };
    if pool_outcome.is_none() {
        drop(rx.take());
        pool.abort();
        let _ = pool.await;
    } else {
        drop(rx.take());
    }
    drop(commands);
    let stale_elapsed = output_at.elapsed();
    servers.abort_all();
    while tokio::time::timeout(Duration::from_secs(1), servers.join_next())
        .await
        .is_ok_and(|joined| joined.is_some())
    {}
    let (end, head, cursor) = pool_outcome.expect("pool worker must stop after consumer closes");
    eprintln!(
        "window control: producer_first={producer_first} head={head} cursor={cursor} healthy_requests={:?} head_only_requests={:?}",
        requests[0].lock().unwrap(),
        requests[1].lock().unwrap()
    );
    assert!(initial_output, "healthy initial producer fixture");
    assert!(
        first_barrier,
        "head/cursor ordering barrier must be reached"
    );
    assert!(
        observed_window_update,
        "peer update bytes must reach the pool reader"
    );
    assert_eq!(
        head,
        u64::from(head_only_max.max(producer_max_piece)),
        "shared head must stay monotonic"
    );
    if matches!(update, RetainedWindowUpdate::UnknownInvalidSequence) {
        assert!(matches!(end, FollowEnd::ConsumerGone));
        assert!(
            !healthy_requested_piece9,
            "malformed, negative, over-bound, and max-zero updates must not request piece9"
        );
        assert!(
            !matches!(output, Ok(true)),
            "invalid windows must supply no new output"
        );
        assert_eq!(
            cursor, 9,
            "invalid windows must not advance the media cursor"
        );
    } else {
        assert!(
            healthy_requested_piece9,
            "healthy producer must receive actual piece9 request"
        );
        assert!(
            matches!(output, Ok(true)),
            "retained healthy producer's window update must supply usable output"
        );
        assert!(
            cursor > 9,
            "new producer media must advance the actual cursor"
        );
        if duplicate_chatter {
            assert!(
                duplicate_barriers,
                "duplicate frames must be accepted in order"
            );
            assert!(matches!(end, FollowEnd::PoolStale { .. }));
            assert_eq!(
                cursor, 10,
                "duplicate window chatter must not produce more media"
            );
            assert!(
                stale_elapsed >= Duration::from_secs(4)
                    && stale_elapsed < Duration::from_secs(7),
                "duplicate max updates must not renew the post-output stale deadline: {stale_elapsed:?}"
            );
        } else {
            assert!(matches!(end, FollowEnd::ConsumerGone));
        }
    }
}

#[tokio::test]
async fn retained_producer_same_head_window_update_supplies_media() {
    retained_producer_window_control(false, 10, 10, RetainedWindowUpdate::Id4).await;
}

#[tokio::test]
async fn retained_producer_first_window_update_positive_control() {
    retained_producer_window_control(true, 10, 10, RetainedWindowUpdate::Id4).await;
}

#[tokio::test]
async fn retained_producer_lower_than_shared_head_window_update_supplies_media() {
    // Shared head11/cursor9; producer head10 remains usable inside prefetch2.
    retained_producer_window_control(false, 11, 10, RetainedWindowUpdate::Id4).await;
}

#[tokio::test]
async fn retained_producer_structured_same_head_window_update_supplies_media() {
    retained_producer_window_control(false, 10, 10, RetainedWindowUpdate::StructuredExtended).await;
}

#[tokio::test]
async fn retained_producer_structured_new_head_window_update_supplies_media() {
    retained_producer_window_control(false, 10, 11, RetainedWindowUpdate::StructuredExtended).await;
}

#[tokio::test]
async fn retained_producer_unknown_structured_same_head_window_update_supplies_media() {
    retained_producer_window_control(false, 10, 10, RetainedWindowUpdate::StructuredUnknown).await;
}

#[tokio::test]
async fn retained_producer_unknown_structured_lower_head_window_update_supplies_media() {
    retained_producer_window_control(false, 11, 10, RetainedWindowUpdate::StructuredUnknown).await;
}

#[tokio::test]
async fn retained_producer_extended_structured_lower_head_window_update_supplies_media() {
    retained_producer_window_control(false, 11, 10, RetainedWindowUpdate::StructuredExtended).await;
}

#[tokio::test]
async fn retained_producer_extended_duplicate_updates_do_not_renew_stale_deadline() {
    retained_producer_window_control(false, 10, 10, RetainedWindowUpdate::DuplicateExtended).await;
}

#[tokio::test]
async fn retained_producer_unknown_duplicate_updates_do_not_renew_stale_deadline() {
    retained_producer_window_control(false, 10, 10, RetainedWindowUpdate::DuplicateUnknown).await;
}

#[tokio::test]
async fn retained_producer_unknown_invalid_windows_preserve_global_head_and_coverage() {
    retained_producer_window_control(false, 10, 10, RetainedWindowUpdate::UnknownInvalidSequence)
        .await;
}

#[test]
fn read_window_max_preserves_live_window_parser_compatibility() {
    assert_eq!(read_window_max(b"d9:max_piecei0ee"), Some(0));
    assert_eq!(read_window_max(b"d2:mid9:max_piecei0eee"), Some(0));
    assert_eq!(read_window_max(b"d9:max_piecei10ee"), Some(10));
    assert_eq!(read_window_max(b"d9:max_piecei10ee trailing"), Some(10));
    assert_eq!(read_window_max(b"d1:ai1ee"), None);
    assert_eq!(read_window_max(b"d9:max_piece1:xe"), None);
    assert_eq!(read_window_max(b"d9:max_piecei-1ee"), None);
    assert_eq!(read_window_max(b"not-bencode"), None);
    assert_eq!(read_window_max(b"d9:max_piecei4294967296ee"), None);
}

#[tokio::test]
async fn initial_selection_keeps_first_protocol_success_grace() {
    let (info, seed) = cancellation_fixture();
    let first = selection_peer(Duration::from_millis(50), live_pos(1, 8)).await;
    let delayed = selection_peer(Duration::from_millis(400), live_pos(1, 9)).await;
    let first_addr = first.addr;
    let delayed_addr = delayed.addr;
    let mut candidates = SessionCandidates::default();
    candidates.learn(first_addr, CandidateKind::Source);
    candidates.learn(delayed_addr, CandidateKind::Pex);
    let started = Instant::now();
    let selected = tokio::time::timeout(
        Duration::from_secs(2),
        connect_pool(
            info.infohash,
            &mut candidates,
            LiveRecoveryConfig {
                max_parallel_connect: 2,
                max_active_upstreams: 2,
                ..seed.live_recovery
            },
            &mut None,
            None,
        ),
    )
    .await
    .expect("initial peer selection must be bounded");
    let elapsed = started.elapsed();
    let selected_addr = selected.first().map(|peer| peer.addr);
    drop(selected);
    tokio::time::timeout(Duration::from_secs(1), first.accepted)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), delayed.accepted)
        .await
        .unwrap()
        .unwrap();
    let first_outcome = tokio::time::timeout(Duration::from_secs(1), first.server)
        .await
        .unwrap()
        .unwrap();
    let delayed_outcome = tokio::time::timeout(Duration::from_secs(1), delayed.server)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(selected_addr, Some(first_addr));
    assert!((Duration::from_millis(250)..Duration::from_millis(390)).contains(&elapsed));
    assert!(first_outcome.handshake_accepted && first_outcome.window_sent);
    assert!(delayed_outcome.socket_closed);
    assert!(candidates.eligible(Instant::now()).contains(&delayed_addr));
}

#[tokio::test]
async fn reconnect_selection_grace_can_cancel_later_covering_peer() {
    let cursor = 9;
    let (info, seed) = cancellation_fixture();
    let (mut continuity, _) = Continuity::fresh(&info, 7, 10, 1, seed.live_recovery);
    continuity.reasm.skip_to(cursor);
    // Establish prior output through the real TS resynchronizer before reconnecting.
    let packet: Vec<u8> = (0..4)
        .flat_map(|cc| {
            let mut packet = vec![0x55; 188];
            packet[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10 | cc]);
            packet
        })
        .collect();
    let output = continuity.resync_output(&packet);
    assert!(!output.is_empty());
    continuity.emitted = output.iter().map(|chunk| chunk.bytes.len() as u64).sum();
    assert!(continuity.emitted > 0);
    assert_eq!(continuity.reasm.next_needed(), cursor);

    let stale = selection_peer(Duration::from_millis(50), live_pos(1, cursor as i64 - 1)).await;
    let covering = selection_peer(Duration::from_millis(400), live_pos(1, cursor as i64)).await;
    let stale_addr = stale.addr;
    let covering_addr = covering.addr;
    let mut candidates = SessionCandidates::default();
    candidates.learn(stale_addr, CandidateKind::Source);
    candidates.learn(covering_addr, CandidateKind::Pex);
    let selected = tokio::time::timeout(
        Duration::from_secs(2),
        connect_pool(
            info.infohash,
            &mut candidates,
            LiveRecoveryConfig {
                max_parallel_connect: 2,
                max_active_upstreams: 2,
                ..seed.live_recovery
            },
            &mut None,
            Some(continuity.resume_cursor()),
        ),
    )
    .await
    .expect("reconnect selection must be bounded");
    let selected_addr = selected.first().map(|peer| peer.addr);
    // Exercise the same post-selection coverage predicate the production follower applies.
    let selected_contains_covering_peer = selected
        .iter()
        .any(|peer| continuity.window_can_resume(&peer.window));
    drop(selected);
    let covering_window = live_pos(1, cursor as i64);
    let covering_window_can_resume = continuity.window_can_resume(&covering_window);
    let retryable = candidates.eligible(Instant::now());
    let stale_outcome = tokio::time::timeout(Duration::from_secs(1), stale.server)
        .await
        .unwrap()
        .unwrap();
    let covering_outcome = tokio::time::timeout(Duration::from_secs(1), covering.server)
        .await
        .unwrap()
        .unwrap();

    assert!(
        selected_addr == Some(covering_addr),
        "the covering candidate must be selected in this reconnect round"
    );
    assert!(selected_contains_covering_peer);
    assert!(
        covering_window_can_resume,
        "a cursor-covering window is accepted"
    );
    assert!(
        !retryable.contains(&stale_addr),
        "the rejected stale candidate enters cooldown"
    );
    assert_eq!(candidates.explorations(stale_addr), 1);
    assert_eq!(candidates.explorations(covering_addr), 0);
    assert!(
        stale_outcome.handshake_accepted
            && stale_outcome.window_sent
            && stale_outcome.socket_closed,
        "a protocol-valid stale candidate is rejected and closed"
    );
    assert!(
        covering_outcome.handshake_accepted
            && covering_outcome.window_sent
            && covering_outcome.socket_closed,
        "selected covering candidate closes only when the selected pool is dropped"
    );

    // Positive control: the same 400 ms covering peer succeeds when no earlier stale
    // handshake starts the selection grace clock.
    let alone_peer = selection_peer(Duration::from_millis(400), covering_window).await;
    let alone_addr = alone_peer.addr;
    let mut alone_candidates = SessionCandidates::default();
    alone_candidates.learn(alone_addr, CandidateKind::Pex);
    let alone = tokio::time::timeout(
        Duration::from_secs(2),
        connect_pool(
            info.infohash,
            &mut alone_candidates,
            LiveRecoveryConfig {
                max_parallel_connect: 2,
                max_active_upstreams: 2,
                ..seed.live_recovery
            },
            &mut None,
            Some(continuity.resume_cursor()),
        ),
    )
    .await
    .expect("covering peer must succeed on its own");
    let alone_selected_covers_cursor = alone
        .first()
        .is_some_and(|peer| peer.addr == alone_addr && continuity.window_can_resume(&peer.window));
    drop(alone);
    let alone_outcome = tokio::time::timeout(Duration::from_secs(1), alone_peer.server)
        .await
        .unwrap()
        .unwrap();
    assert!(alone_selected_covers_cursor);
    assert!(alone_outcome.handshake_accepted && alone_outcome.window_sent);

    assert!(
        selected_contains_covering_peer,
        "reconnect selection must include a peer whose window can resume the current cursor"
    );
}

#[tokio::test]
async fn reconnect_stale_only_candidates_cool_down_across_cohorts() {
    let (info, seed) = cancellation_fixture();
    let (mut continuity, _) = Continuity::fresh(&info, 7, 10, 1, seed.live_recovery);
    continuity.reasm.skip_to(9);
    let first = selection_peer(Duration::ZERO, live_pos(1, 8)).await;
    let later = selection_peer(Duration::ZERO, live_pos(1, 7)).await;
    let first_addr = first.addr;
    let later_addr = later.addr;
    let mut candidates = SessionCandidates::default();
    candidates.learn(first_addr, CandidateKind::Source);
    candidates.learn(later_addr, CandidateKind::Pex);
    candidates.explored(later_addr);

    let selected = tokio::time::timeout(
        Duration::from_secs(2),
        connect_pool(
            info.infohash,
            &mut candidates,
            LiveRecoveryConfig {
                max_parallel_connect: 2,
                max_active_upstreams: 2,
                ..seed.live_recovery
            },
            &mut None,
            Some(continuity.resume_cursor()),
        ),
    )
    .await
    .expect("finite stale cohorts must exhaust promptly");
    let first_outcome = first.server.await.unwrap();
    let later_outcome = later.server.await.unwrap();

    assert!(selected.is_empty());
    assert_eq!(candidates.explorations(first_addr), 1);
    assert_eq!(candidates.explorations(later_addr), 2);
    assert!(!candidates.eligible(Instant::now()).contains(&first_addr));
    assert!(!candidates.eligible(Instant::now()).contains(&later_addr));
    assert!(first_outcome.handshake_accepted && first_outcome.window_sent);
    assert!(later_outcome.handshake_accepted && later_outcome.window_sent);
    assert!(first_outcome.socket_closed && later_outcome.socket_closed);
}

#[tokio::test]
async fn reconnect_grace_starts_at_first_usable_candidate() {
    let (info, seed) = cancellation_fixture();
    let (mut continuity, _) = Continuity::fresh(&info, 7, 10, 1, seed.live_recovery);
    continuity.reasm.skip_to(9);
    let stale = selection_peer(Duration::from_millis(50), live_pos(1, 8)).await;
    let usable = selection_peer(Duration::from_millis(400), live_pos(1, 9)).await;
    let pending = selection_peer(Duration::from_millis(750), live_pos(1, 10)).await;
    let stale_addr = stale.addr;
    let usable_addr = usable.addr;
    let pending_addr = pending.addr;
    let mut candidates = SessionCandidates::default();
    candidates.learn(stale_addr, CandidateKind::Source);
    candidates.learn(usable_addr, CandidateKind::Pex);
    candidates.learn(pending_addr, CandidateKind::Discovered);
    let started = Instant::now();
    let selected = tokio::time::timeout(
        Duration::from_secs(2),
        connect_pool(
            info.infohash,
            &mut candidates,
            LiveRecoveryConfig {
                max_parallel_connect: 3,
                max_active_upstreams: 2,
                ..seed.live_recovery
            },
            &mut None,
            Some(continuity.resume_cursor()),
        ),
    )
    .await
    .expect("usable candidate grace must remain bounded");
    let elapsed = started.elapsed();
    let selected_len = selected.len();
    let selected_addr = selected.first().map(|peer| peer.addr);
    drop(selected);
    tokio::time::timeout(Duration::from_secs(1), pending.accepted)
        .await
        .unwrap()
        .unwrap();
    let stale_outcome = stale.server.await.unwrap();
    let usable_outcome = usable.server.await.unwrap();
    let pending_outcome = pending.server.await.unwrap();

    assert_eq!(selected_len, 1);
    assert_eq!(selected_addr, Some(usable_addr));
    assert!((Duration::from_millis(550)..Duration::from_millis(1050)).contains(&elapsed));
    assert_eq!(candidates.explorations(stale_addr), 1);
    assert!(candidates.explorations(pending_addr) == 0);
    assert!(stale_outcome.handshake_accepted && stale_outcome.window_sent);
    assert!(usable_outcome.handshake_accepted && usable_outcome.window_sent);
    assert!(pending_outcome.socket_closed);
    assert!(candidates.eligible(Instant::now()).contains(&pending_addr));
}

#[tokio::test]
async fn rejected_stale_candidate_does_not_extend_usable_grace() {
    let (info, seed) = cancellation_fixture();
    let (mut continuity, _) = Continuity::fresh(&info, 7, 10, 1, seed.live_recovery);
    continuity.reasm.skip_to(9);
    let mut usable = selection_peer(Duration::from_millis(40), live_pos(1, 9)).await;
    let mut stale = selection_peer(Duration::from_millis(210), live_pos(1, 8)).await;
    let mut pending = selection_peer(Duration::from_secs(2), live_pos(1, 10)).await;
    let stale_addr = stale.addr;
    let usable_addr = usable.addr;
    let pending_addr = pending.addr;
    let mut candidates = SessionCandidates::default();
    candidates.learn(stale_addr, CandidateKind::Source);
    candidates.learn(usable_addr, CandidateKind::Pex);
    candidates.learn(pending_addr, CandidateKind::Discovered);

    // Keep a third attempt active past the original 250 ms grace deadline. The stale peer's
    // valid response arrives during that grace, so a reset deadline would extend this round.
    let selected_result = tokio::time::timeout(
        Duration::from_secs(2),
        connect_pool(
            info.infohash,
            &mut candidates,
            LiveRecoveryConfig {
                max_parallel_connect: 3,
                max_active_upstreams: 2,
                ..seed.live_recovery
            },
            &mut None,
            Some(continuity.resume_cursor()),
        ),
    )
    .await;
    let selected_at = Instant::now();
    let (selection_timed_out, selected_addrs) = match selected_result {
        Ok(selected) => {
            let addresses = selected.iter().map(|peer| peer.addr).collect();
            drop(selected);
            (false, addresses)
        }
        Err(_) => (true, Vec::new()),
    };
    let stale_explorations_at_selection = candidates.explorations(stale_addr);
    let stale_cooled_at_selection = !candidates.eligible(selected_at).contains(&stale_addr);
    let pending_explorations_at_selection = candidates.explorations(pending_addr);
    let pending_retryable_at_selection = candidates.eligible(selected_at).contains(&pending_addr);

    // Join all fixture servers before making assertions, with a bounded abort fallback so
    // both successful and failing cases close every owned loopback task.
    let server_outcomes = (
        reap_selection_peer(&mut pending.server, Duration::from_millis(2500)).await,
        reap_selection_peer(&mut stale.server, Duration::from_millis(500)).await,
        reap_selection_peer(&mut usable.server, Duration::from_millis(500)).await,
    );

    assert!(
        !selection_timed_out,
        "selection must finish within its bound"
    );
    let pending_outcome = server_outcomes
        .0
        .expect("pending fixture task must be reaped");
    let usable_outcome = server_outcomes
        .2
        .expect("usable fixture task must be reaped");
    let stale_outcome = server_outcomes
        .1
        .expect("stale fixture task must be reaped");
    let usable_sent_at = usable_outcome
        .window_sent_at
        .expect("usable peer must send its protocol-valid window");
    let stale_sent_at = stale_outcome
        .window_sent_at
        .expect("stale peer must send its protocol-valid window during grace");
    let elapsed_from_usable = selected_at.duration_since(usable_sent_at);
    eprintln!(
        "in-grace stale control: usable_to_stale={:?} usable_to_return={elapsed_from_usable:?} stale_explorations={} stale_cooled={} pending_explorations={} pending_retryable={} pending_accepted_before_return={}",
        stale_sent_at.duration_since(usable_sent_at),
        stale_explorations_at_selection,
        stale_cooled_at_selection,
        pending_explorations_at_selection,
        pending_retryable_at_selection,
        pending_outcome
            .accepted_at
            .is_some_and(|at| at < selected_at),
    );

    assert_eq!(selected_addrs, vec![usable_addr]);
    assert!(usable_sent_at < stale_sent_at && stale_sent_at < selected_at);
    assert!(
        (Duration::from_millis(210)..Duration::from_millis(380)).contains(&elapsed_from_usable),
        "selection should end near the original usable-peer grace deadline, elapsed={elapsed_from_usable:?}"
    );
    assert_eq!(stale_explorations_at_selection, 1);
    assert!(
        stale_cooled_at_selection,
        "the completed stale candidate must be cooled down and excluded"
    );
    assert_eq!(pending_explorations_at_selection, 0);
    assert!(
        pending_retryable_at_selection,
        "the cancelled pending candidate remains retryable"
    );
    assert!(stale_outcome.handshake_accepted && stale_outcome.window_sent);
    assert!(usable_outcome.handshake_accepted && usable_outcome.window_sent);
    assert!(pending_outcome
        .accepted_at
        .is_some_and(|at| at < selected_at));
    assert!(!pending_outcome.window_sent);
    assert!(pending_outcome.socket_closed);
}

#[tokio::test]
async fn cancelling_reconnect_selection_closes_pending_socket_without_advancing_it() {
    let (info, seed) = cancellation_fixture();
    let (mut continuity, _) = Continuity::fresh(&info, 7, 10, 1, seed.live_recovery);
    continuity.reasm.skip_to(9);
    let stale = selection_peer(Duration::ZERO, live_pos(1, 8)).await;
    let pending = selection_peer(Duration::from_millis(400), live_pos(1, 9)).await;
    let stale_addr = stale.addr;
    let pending_addr = pending.addr;
    let mut candidates = SessionCandidates::default();
    candidates.learn(stale_addr, CandidateKind::Source);
    candidates.learn(pending_addr, CandidateKind::Pex);
    let mut discovery = None;
    let mut round = Box::pin(connect_pool(
        info.infohash,
        &mut candidates,
        LiveRecoveryConfig {
            max_parallel_connect: 2,
            max_active_upstreams: 2,
            ..seed.live_recovery
        },
        &mut discovery,
        Some(continuity.resume_cursor()),
    ));
    let mut stale_server = stale.server;
    let mut pending_accepted = pending.accepted;
    let mut stale_outcome = None;
    let mut saw_pending_accept = false;
    tokio::time::timeout(Duration::from_secs(2), async {
        while stale_outcome.is_none() || !saw_pending_accept {
            tokio::select! {
                result = &mut round => panic!("round completed while a peer was pending: {} selected", result.len()),
                result = &mut stale_server, if stale_outcome.is_none() => {
                    stale_outcome = Some(result.unwrap());
                }
                result = &mut pending_accepted, if !saw_pending_accept => {
                    result.unwrap();
                    saw_pending_accept = true;
                }
            }
        }
    })
    .await
    .expect("observe stale rejection and pending TCP before cancelling round");
    drop(round);
    let pending_outcome = tokio::time::timeout(Duration::from_secs(1), pending.server)
        .await
        .unwrap()
        .unwrap();

    assert!(stale_outcome.unwrap().handshake_accepted);
    assert!(pending_outcome.socket_closed);
    assert_eq!(candidates.explorations(stale_addr), 1);
    assert_eq!(candidates.explorations(pending_addr), 0);
    assert!(!candidates.eligible(Instant::now()).is_empty());
}

mod fairness;

#[tokio::test]
async fn owned_gossip_limits_messages_and_retained_pex() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let client = TcpStream::connect(addr).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    let server = tokio::spawn(async move {
        let mut session = PeerSession::new(server);
        assert!(matches!(
            session.read_message().await.unwrap(),
            PeerMessage::Extended { ext_id: 0, .. }
        ));
        let mut payload = vec![0; 16 + 1100 * 108];
        payload[1..3].copy_from_slice(&1100u16.to_be_bytes());
        payload[7..11].copy_from_slice(&108u32.to_be_bytes());
        for i in 0..1100usize {
            let offset = 16 + i * 108 + 11;
            payload[offset..offset + 4].copy_from_slice(&[127, 0, 0, 1]);
            payload[offset + 4..offset + 6].copy_from_slice(&(1000 + i as u16).to_be_bytes());
        }
        session
            .send(&PeerMessage::Unknown { id: 12, payload })
            .await
            .unwrap();
        for _ in 1..32 {
            session.send(&PeerMessage::KeepAlive).await.unwrap();
        }
        // This source is the 33rd message: it must not be learned.
        let mut payload = vec![0; 14];
        payload[8..12].copy_from_slice(&[127, 0, 0, 1]);
        payload[12..14].copy_from_slice(&30000u16.to_be_bytes());
        let _ = session
            .send(&PeerMessage::Unknown { id: 36, payload })
            .await;
        while let Ok(message) = session.read_message().await {
            assert!(!matches!(
                message,
                PeerMessage::Interested | PeerMessage::Unknown { id: 6, .. }
            ));
        }
    });
    let learned = pool_support::stale_gossip_job(
        ConnectedUpstream {
            session: PeerSession::new(client),
            addr,
            window: live_pos(1, 1),
            unchoked: false,
            yourip: None,
        },
        &Identity::generate(),
        tokio::time::Instant::now() + STALE_GOSSIP_BUDGET,
    )
    .await;
    let learned = learned.into_learned();
    assert_eq!(learned.len(), 1024);
    assert!(learned
        .iter()
        .all(|(addr, kind)| addr.port() < 2100 && *kind == CandidateKind::Pex));
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn owned_gossip_drop_before_poll_closes_socket() {
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let client = TcpStream::connect(addr).await.unwrap();
    let (mut server, _) = listener.accept().await.unwrap();
    let job = pool_support::stale_gossip_job(
        ConnectedUpstream {
            session: PeerSession::new(client),
            addr,
            window: live_pos(1, 1),
            unchoked: false,
            yourip: None,
        },
        &Identity::generate(),
        tokio::time::Instant::now() + STALE_GOSSIP_BUDGET,
    );
    drop(job);
    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), server.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn owned_gossip_chatter_does_not_renew_absolute_deadline() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let client = TcpStream::connect(addr).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    let server = tokio::spawn(async move {
        let mut session = PeerSession::new(server);
        session.read_message().await.unwrap();
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if session.send(&PeerMessage::KeepAlive).await.is_err() {
                break;
            }
        }
    });
    let start = tokio::time::Instant::now();
    let deadline = start + Duration::from_millis(80);
    pool_support::stale_gossip_job(
        ConnectedUpstream {
            session: PeerSession::new(client),
            addr,
            window: live_pos(1, 1),
            unchoked: false,
            yourip: None,
        },
        &Identity::generate(),
        deadline,
    )
    .await;
    assert!(start.elapsed() >= Duration::from_millis(80));
    assert!(start.elapsed() < Duration::from_millis(250));
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn owned_gossip_deadline_starts_at_admission_not_first_poll() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let client = TcpStream::connect(addr).await.unwrap();
    let (_server, _) = listener.accept().await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(20);
    let job = pool_support::stale_gossip_job(
        ConnectedUpstream {
            session: PeerSession::new(client),
            addr,
            window: live_pos(1, 1),
            unchoked: false,
            yourip: None,
        },
        &Identity::generate(),
        deadline,
    );
    tokio::time::advance(Duration::from_millis(30)).await;
    let before = tokio::time::Instant::now();
    assert!(job.await.into_learned().is_empty());
    assert_eq!(tokio::time::Instant::now(), before);
}
