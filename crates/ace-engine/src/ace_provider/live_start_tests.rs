//! Production live-start protocol regressions with entirely synthetic loopback media.
use super::*;

fn window(min: i64, max: i64) -> LivePosition {
    LivePosition {
        min_piece: min,
        max_piece: max,
        position: -1,
        distance_from_source: 1,
    }
}
fn packet_piece(marker: u8) -> Vec<u8> {
    (0..4)
        .flat_map(|cc| {
            let mut packet = vec![marker; 188];
            packet[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10 | cc]);
            packet
        })
        .collect()
}
fn fixture(max_active: usize) -> (StreamInfo, SeedConfig) {
    (
        StreamInfo {
            infohash: [0; 20],
            piece_length: 752,
            chunk_length: 752,
            trackers: vec![],
            metadata: StreamMetadata::default(),
            sig_len: 0,
            source_pubkey: vec![],
        },
        SeedConfig {
            registry: SeedRegistry::new(),
            store_bytes: 4096,
            store_retention: None,
            enabled: false,
            prefetch_pieces: 1,
            live_recovery: LiveRecoveryConfig {
                max_active_upstreams: max_active,
                request_check_interval_ms: 10,
                ..LiveRecoveryConfig::default()
            },
            cache_type: CacheType::Memory,
            cache_dir: PathBuf::new(),
            warm_peers: WarmPeerCache::memory(),
        },
    )
}
fn v4(addr: std::net::SocketAddr) -> SocketAddrV4 {
    match addr {
        std::net::SocketAddr::V4(addr) => addr,
        _ => unreachable!(),
    }
}
async fn stale_then_source(max_active: usize, matching_window: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source_addr = v4(listener.local_addr().unwrap());
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = requests.clone();
    let source = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut session = PeerSession::new(stream);
        session.accept_handshake([0; 20], |_| true).await.unwrap();
        session
            .send_extended_handshake(&OutgoingExtendedHandshake {
                ace_metadata_version: 1,
                ut_metadata_id: 2,
                mi: Some(if matching_window {
                    window(7, 8)
                } else {
                    window(1000, 1001)
                }),
                node: NodeFields::default(),
                peer_ip: None,
                metadata_size: None,
            })
            .await
            .unwrap();
        session.send(&PeerMessage::Unchoke).await.unwrap();
        while let Ok(msg) = session.read_message().await {
            if let PeerMessage::Unknown { id: 6, payload } = msg {
                let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                observed.lock().unwrap().push(piece);
                session
                    .send(&build_piece(0, piece, 0, [0; 8], &packet_piece(0x66)))
                    .await
                    .unwrap();
            }
        }
    });
    let relay_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_addr = v4(relay_listener.local_addr().unwrap());
    let client = TcpStream::connect(relay_addr).await.unwrap();
    let (relay_stream, _) = relay_listener.accept().await.unwrap();
    let observed = requests.clone();
    let relay = tokio::spawn(async move {
        let mut session = PeerSession::new(relay_stream);
        session.send(&PeerMessage::Unchoke).await.unwrap();
        let announce_at = tokio::time::Instant::now() + Duration::from_millis(75);
        let mut announced = false;
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(announce_at), if !announced => {
                    let mut payload = vec![0;14];
                    payload[8..12].copy_from_slice(&source_addr.ip().octets());
                    payload[12..14].copy_from_slice(&source_addr.port().to_be_bytes());
                    if session.send(&PeerMessage::Unknown {id:36,payload}).await.is_err() { break; }
                    announced=true;
                }
                msg=session.read_message() => match msg {
                    Ok(PeerMessage::Unknown {id:6,payload}) => {
                        let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                        observed.lock().unwrap().push(piece);
                        if session.send(&build_piece(0,piece,0,[0;8],&packet_piece(0x55))).await.is_err() {break;}
                    }
                    Ok(_) => {}, Err(_) => break,
                }
            }
        }
    });
    let (info, seed) = fixture(max_active);
    let (tx, mut rx) = mpsc::channel(16);
    let count = Arc::new(AtomicU32::new(1));
    let pool_count = count.clone();
    let pool = tokio::spawn(async move {
        let store = Arc::new(tokio::sync::Mutex::new(PieceStore::new(752, 752, 4096)));
        let mut candidates = SessionCandidates::default();
        candidates.learn(relay_addr, CandidateKind::Discovered);
        follow_peer_pool(
            vec![ConnectedUpstream {
                session: PeerSession::new(client),
                addr: relay_addr,
                window: window(7, 8),
                yourip: None,
            }],
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
            vec![relay_addr],
            completed_discovery(|_| Box::pin(async { vec![] })),
            &mut candidates,
            &pool_count,
            None,
        )
        .await
    });
    let output = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
    drop(rx);
    tokio::time::timeout(Duration::from_secs(1), pool)
        .await
        .unwrap()
        .unwrap();
    relay.await.unwrap();
    source.abort();
    let _ = source.await;
    let output = output
        .expect("source should complete inside startup budget")
        .expect("live output");
    assert!(
        output
            .bytes
            .as_chunks::<188>().0.iter()
            .all(|packet| packet[4] == if matching_window {0x55} else {0x66}),
        "initial output must use the best-window transport and retain the healthy peer when windows match"
    );
    let requests = requests.lock().unwrap();
    assert!(!requests.is_empty());
    assert!(
        requests
            .iter()
            .all(|piece| *piece >= if matching_window { 7 } else { 1000 }),
        "no piece below live head minus prefetch may be requested: {requests:?}"
    );
    assert!(count.load(Ordering::Relaxed) <= max_active as u32);
}

#[tokio::test]
async fn stale_first_window_waits_for_actual_source_before_request_or_output() {
    stale_then_source(4, false).await;
}
#[tokio::test]
async fn stale_first_window_replaced_with_actual_source_at_single_active_limit() {
    stale_then_source(1, false).await;
}

#[tokio::test]
async fn known_live_floor_prunes_buffered_and_inflight_stale_pieces_before_request() {
    let (info, seed) = fixture(1);
    let (mut c, _) = Continuity::fresh(&info, 7, 8, 1, seed.live_recovery);
    let addr = "127.0.0.1:1".parse().unwrap();
    c.register_active_peer(1, addr, window(7, 10));
    c.set_peer_unchoked(1, true);
    let (commands, mut rx) = mpsc::channel(32);
    let worker = tokio::spawn(async {});
    let mut peers = BTreeMap::from([(
        1,
        PeerRuntime {
            addr,
            min_piece: 7,
            max_piece: 10,
            unchoked_peer: false,
            produced_output: false,
            seen_ids: HashSet::new(),
            commands,
            worker,
        },
    )]);
    advance_pool_requests(&mut peers, &mut c, 1).await;
    while rx.try_recv().is_ok() {}
    c.reasm.add_block(8, 0, &packet_piece(0x55)).unwrap();
    c.head = 10; // Floor9 is inside the old512-piece lag allowance.
    c.reasm.add_block(7, 0, &packet_piece(0x55)).unwrap();
    advance_pool_requests(&mut peers, &mut c, 1).await;
    let mut actual = Vec::new();
    while let Ok(PeerCommand::RequestPiece { piece, .. }) = rx.try_recv() {
        actual.push(piece);
    }
    assert_eq!(
        actual,
        vec![9, 10],
        "known-head floor must clear stale request capacity and request current pieces"
    );
    assert!(
        c.reasm.take_ready().is_empty(),
        "buffered stale prefix must be discarded before ready extraction"
    );
    assert_eq!(c.reasm.next_needed(), 9);
}

// A known live edge does not license throwing away useful current buffered pieces.
#[tokio::test]
async fn known_live_floor_preserves_current_buffer_and_partial_piece() {
    let (info, seed) = fixture(1);
    let (mut c, _) = Continuity::fresh(&info, 7, 8, 1, seed.live_recovery);
    c.reasm.add_block(9, 0, &packet_piece(0x66)).unwrap();
    c.reasm
        .add_block(10, 0, &packet_piece(0x77)[..376])
        .unwrap();
    c.head = 10;
    advance_pool_requests(&mut BTreeMap::new(), &mut c, 1).await;
    // Late stale data cannot revive pieces removed by the floor.
    c.reasm.add_block(7, 0, &packet_piece(0x55)).unwrap();
    assert_eq!(c.reasm.take_ready(), packet_piece(0x66));
    c.reasm
        .add_block(10, 376, &packet_piece(0x77)[376..])
        .unwrap();
    assert_eq!(c.reasm.take_ready(), packet_piece(0x77));
}

async fn single_peer_with_hint_or_head(
    pending_source: bool,
    head_advance: bool,
    cancel_early: bool,
    stale_window_delay: bool,
) {
    use tokio::io::AsyncReadExt;
    let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let silent_addr = v4(silent.local_addr().unwrap());
    let (connected_tx, connected_rx) = tokio::sync::oneshot::channel();
    let pending = tokio::spawn(async move {
        let (mut socket, _) = silent.accept().await.unwrap();
        let mut handshake = [0; 66];
        socket.read_exact(&mut handshake).await.unwrap();
        connected_tx.send(()).unwrap();
        if stale_window_delay {
            use tokio::io::AsyncWriteExt;
            socket
                .write_all(&ace_wire::handshake::Handshake::new([0; 20], [0; 20]).encode())
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(600)).await;
            let mut session = PeerSession::new(socket);
            session
                .send_extended_handshake(&OutgoingExtendedHandshake {
                    ace_metadata_version: 1,
                    ut_metadata_id: 2,
                    mi: Some(window(1, 1)),
                    node: NodeFields::default(),
                    peer_ip: None,
                    metadata_size: None,
                })
                .await
                .unwrap();
            while session.read_message().await.is_ok() {}
        } else {
            let mut tail = Vec::new();
            socket.read_to_end(&mut tail).await.unwrap();
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = v4(listener.local_addr().unwrap());
    let client = TcpStream::connect(addr).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    let (first_tx, first_rx) = tokio::sync::oneshot::channel();
    let requests = Arc::new(AtomicU32::new(0));
    let observed = requests.clone();
    let server = tokio::spawn(async move {
        let started = Instant::now();
        let mut first = Some(first_tx);
        let mut session = PeerSession::new(server);
        session.send(&PeerMessage::Unchoke).await.unwrap();
        if pending_source {
            let mut payload = vec![0; 14];
            payload[8..12].copy_from_slice(&silent_addr.ip().octets());
            payload[12..14].copy_from_slice(&silent_addr.port().to_be_bytes());
            session
                .send(&PeerMessage::Unknown { id: 36, payload })
                .await
                .unwrap();
        }
        if head_advance {
            let mut payload = vec![0; 8];
            payload[4..8].copy_from_slice(&10u32.to_be_bytes());
            session
                .send(&PeerMessage::Unknown { id: 4, payload })
                .await
                .unwrap();
        }
        while let Ok(msg) = session.read_message().await {
            if let PeerMessage::Unknown { id: 6, payload } = msg {
                observed.fetch_add(1, Ordering::Relaxed);
                if let Some(first) = first.take() {
                    let _ = first.send(started.elapsed());
                }
                let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                if head_advance {
                    assert!(piece >= 9, "knownhead10/prefetch1 must not request below9");
                    // A late authenticated-looking stale piece must never reach output/cache.
                    if session
                        .send(&build_piece(0, 8, 0, [0; 8], &packet_piece(0x55)))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                if session
                    .send(&build_piece(0, piece, 0, [0; 8], &packet_piece(0x66)))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    });
    let (info, mut seed) = fixture(1);
    // A coarse retry sweep must not extend the independent startup deadline.
    seed.live_recovery.request_check_interval_ms = 2000;
    seed.live_recovery.request_timeout_ms = 3000;
    let (tx, mut rx) = mpsc::channel(16);
    let store = Arc::new(tokio::sync::Mutex::new(PieceStore::new(752, 752, 4096)));
    let actual_store = store.clone();
    let pool = tokio::spawn(async move {
        follow_peer_pool(
            vec![ConnectedUpstream {
                session: PeerSession::new(client),
                addr,
                window: window(7, 8),
                yourip: None,
            }],
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
            vec![addr],
            completed_discovery(|_| Box::pin(async { vec![] })),
            &mut SessionCandidates::default(),
            &Arc::new(AtomicU32::new(0)),
            None,
        )
        .await
    });
    if cancel_early {
        tokio::time::timeout(Duration::from_millis(500), connected_rx)
            .await
            .unwrap()
            .unwrap();
        drop(rx);
        tokio::time::timeout(Duration::from_secs(1), pool)
            .await
            .unwrap()
            .unwrap();
        server.await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            requests.load(Ordering::Relaxed),
            0,
            "closing during corroboration must cancel without media requests"
        );
        return;
    }
    let first = tokio::time::timeout(Duration::from_millis(1600), first_rx)
        .await
        .unwrap()
        .unwrap();
    let output = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    drop(rx);
    pool.await.unwrap();
    server.await.unwrap();
    if pending_source {
        tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .unwrap()
            .unwrap();
    } else {
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
    }
    assert!(
        first >= Duration::from_millis(900),
        "single peer must receive bounded opportunity for corroboration"
    );
    assert!(
        first < Duration::from_millis(if stale_window_delay { 1200 } else { 1500 }),
        "gossip and coarse sweeps must not extend the one-second media decision budget: {first:?}"
    );
    assert!(
        output
            .bytes
            .as_chunks::<188>()
            .0
            .iter()
            .all(|p| p[4] == 0x66),
        "late stale completion must not publish"
    );
    if head_advance {
        assert!(
            PieceStore::shared_chunk(&actual_store, 8, 0)
                .await
                .is_none(),
            "late stale data must not populate the serving cache"
        );
    }
}

#[tokio::test]
async fn lone_healthy_peer_starts_after_bounded_uncorroborated_fallback() {
    single_peer_with_hint_or_head(false, false, false, false).await;
}
#[tokio::test]
async fn silent_announced_source_does_not_extend_the_start_deadline() {
    single_peer_with_hint_or_head(true, false, false, false).await;
}
#[tokio::test]
async fn known_head_prevents_late_stale_output_and_cache_population() {
    single_peer_with_hint_or_head(false, true, false, false).await;
}
#[tokio::test]
async fn consumer_close_during_corroboration_cancels_pending_source() {
    single_peer_with_hint_or_head(true, false, true, false).await;
}

#[tokio::test]
async fn invalid_or_wrapping_handshake_windows_never_become_live_evidence() {
    for (min, max, valid) in [
        (-1, 10, false),
        (20, 10, false),
        (0, u32::MAX as i64 + 1, false),
        (u32::MAX as i64 - 5, 2, false),
        (u32::MAX as i64 - 5, u32::MAX as i64, true),
        (0, 0, true),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let server = tokio::spawn(async move {
            PeerSession::new(server)
                .send_extended_handshake(&OutgoingExtendedHandshake {
                    ace_metadata_version: 1,
                    ut_metadata_id: 2,
                    mi: Some(window(min, max)),
                    node: NodeFields::default(),
                    peer_ip: None,
                    metadata_size: None,
                })
                .await
                .unwrap();
        });
        assert_eq!(
            read_peer_window(&mut PeerSession::new(client))
                .await
                .is_some(),
            valid,
            "window {min}..{max}"
        );
        server.await.unwrap();
    }
    let (info, seed) = fixture(1);
    let (mut c, _) = Continuity::fresh(
        &info,
        u32::MAX as u64 - 5,
        u32::MAX as u64 - 1,
        1,
        seed.live_recovery,
    );
    c.head = u32::MAX as u64;
    advance_pool_requests(&mut BTreeMap::new(), &mut c, 1).await;
    assert_eq!(c.reasm.next_needed(), u32::MAX as u64 - 1);
    c.head = 1; // An older observation cannot pull the cursor backward.
    advance_pool_requests(&mut BTreeMap::new(), &mut c, 1).await;
    assert_eq!(c.reasm.next_needed(), u32::MAX as u64 - 1);
}

#[tokio::test]
async fn failed_candidate_writes_preserve_the_only_provisional_peer() {
    use tokio::io::AsyncWriteExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = v4(listener.local_addr().unwrap());
    let client = TcpStream::connect(addr).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    let (info, seed) = fixture(1);
    let (mut c, _) = Continuity::fresh(&info, 7, 8, 1, seed.live_recovery);
    let identity = Identity::generate();
    let (events, mut event_rx) = mpsc::channel(16);
    let (runtime, _) = activate_upstream_peer(
        1,
        ConnectedUpstream {
            session: PeerSession::new(client),
            addr,
            window: window(7, 8),
            yourip: None,
        },
        7,
        &identity,
        &mut c,
        &events,
        None,
        None,
    )
    .await
    .unwrap();
    let mut peers = BTreeMap::from([(1, runtime)]);
    let mut server = PeerSession::new(server);
    assert!(matches!(
        server.read_message().await.unwrap(),
        PeerMessage::Extended { .. }
    ));
    assert!(matches!(
        server.read_message().await.unwrap(),
        PeerMessage::Interested
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bad_addr = v4(listener.local_addr().unwrap());
    let mut client = TcpStream::connect(bad_addr).await.unwrap();
    let (_bad_server, _) = listener.accept().await.unwrap();
    // This half-close deterministically rejects candidate writes without TCP timing races.
    client.shutdown().await.unwrap();
    let result = activate_upstream_peer(
        2,
        ConnectedUpstream {
            session: PeerSession::new(client),
            addr: bad_addr,
            window: window(7, 8),
            yourip: None,
        },
        7,
        &identity,
        &mut c,
        &events,
        None,
        Some((&mut peers, 1)),
    )
    .await;
    assert!(result.is_err());
    assert!(
        peers.contains_key(&1),
        "failed activation must retain the sole valid provisional runtime"
    );
    assert_eq!(peers.len(), 1);
    server.send(&PeerMessage::Unchoke).await.unwrap();
    assert!(matches!(
        event_rx.recv().await.unwrap(),
        PeerEvent::Message {
            peer_id: 1,
            msg: PeerMessage::Unchoke,
            ..
        }
    ));
    c.set_peer_unchoked(1, true);
    advance_pool_requests(&mut peers, &mut c, 1).await;
    let request = tokio::time::timeout(Duration::from_secs(1), server.read_message())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(request, PeerMessage::Unknown { id: 6, .. }),
        "retained runtime must still serve real wire requests"
    );
    shutdown_peer_runtimes(&mut peers);
}

#[tokio::test]
async fn queued_chunk_requests_stop_when_known_floor_advances() {
    // One frame of capacity leaves a request write in progress, unlike a timer-only race.
    let (client, server) = tokio::io::duplex(15);
    let (commands, rx) = mpsc::channel(4);
    let (events, _event_rx) = mpsc::channel(4);
    let (info, seed) = fixture(1);
    let (mut c, _) = Continuity::fresh(&info, 7, 8, 1, seed.live_recovery);
    let addr = "127.0.0.1:1".parse().unwrap();
    let worker = tokio::spawn(peer_worker(
        1,
        addr,
        PeerSession::new(client),
        rx,
        events,
        c.request_floor.clone(),
    ));
    commands
        .send(PeerCommand::RequestPiece {
            piece: 7,
            chunks_per_piece: 4,
        })
        .await
        .unwrap();
    commands
        .send(PeerCommand::RequestPiece {
            piece: 9,
            chunks_per_piece: 1,
        })
        .await
        .unwrap();
    let mut server = PeerSession::new(server);
    assert!(matches!(
        server.read_message().await.unwrap(),
        PeerMessage::Unknown { id: 6, .. }
    ));
    c.head = 10;
    advance_pool_requests(&mut BTreeMap::new(), &mut c, 1).await;
    let mut stale_after_observation = 0;
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(1), server.read_message())
            .await
            .unwrap()
            .unwrap();
        let PeerMessage::Unknown { id: 6, payload } = msg else {
            panic!("request expected")
        };
        let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
        if piece == 9 {
            break;
        }
        assert_eq!(piece, 7);
        stale_after_observation += 1;
    }
    drop(commands);
    worker.await.unwrap();
    assert!(stale_after_observation<=1,"only a write already underway before the new head may complete; queued stale chunk writes must stop, got {stale_after_observation}");
}

#[tokio::test]
async fn stale_refill_gossip_shares_the_start_decision_deadline() {
    single_peer_with_hint_or_head(true, false, false, true).await;
}

#[tokio::test]
async fn matching_independent_window_keeps_the_single_healthy_provisional_peer() {
    stale_then_source(1, true).await;
}
