//! Real provider/discovery/BT cold-start regressions, synthetic loopback addresses only.
use super::*;
use ace_swarm::discover::discover_peers_from_sources_incremental;
use ace_swarm::discover::MAX_DISCOVERY_PEERS;

fn info() -> StreamInfo {
    StreamInfo {
        infohash: [0; 20],
        piece_length: 752,
        chunk_length: 752,
        trackers: Vec::new(),
        metadata: StreamMetadata::default(),
        sig_len: 0,
        source_pubkey: Vec::new(),
    }
}

async fn assert_first_attempt(slow_second: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let (attempt_tx, attempt_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut session = PeerSession::new(stream);
        session.accept_handshake([0; 20], |_| true).await.unwrap();
        attempt_tx.send(Instant::now()).unwrap();
    });
    let available = Arc::new(std::sync::Mutex::new(None));
    let observed = available.clone();
    let discovery: PeerDiscovery = Arc::new(move |_options, sender| {
        let observed = observed.clone();
        Box::pin(async move {
            let early = async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                *observed.lock().unwrap() = Some(Instant::now());
                vec![addr]
            };
            let other = async move {
                if slow_second {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                Vec::new()
            };
            discover_peers_from_sources_incremental(early, other, sender).await
        })
    });
    let start = Instant::now();
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0);
    let open =
        tokio::spawn(async move { provider.open_resolved("synthetic", info(), discovery).await });
    let attempt = tokio::time::timeout(Duration::from_secs(2), attempt_rx).await;
    // Always clean owned operations before asserting, including the original failing path.
    open.abort();
    if let Ok(Ok(source)) = open.await {
        drop(source);
    }
    server.abort();
    let _ = server.await;
    let candidate_at = available.lock().unwrap().expect("early source ran");
    assert!(candidate_at.duration_since(start) < Duration::from_millis(500));
    crate::alog!(
        "[test] candidate available after {} ms; first BT attempt within 2s={}",
        candidate_at.duration_since(start).as_millis(),
        attempt.is_ok()
    );
    assert!(
        attempt.is_ok(),
        "production resolved-open waited for the slow aggregate despite a candidate at {:?}",
        candidate_at.duration_since(start),
    );
    let attempt_at = attempt.unwrap().unwrap();
    assert!(attempt_at.duration_since(start) < Duration::from_secs(2));
}

#[tokio::test]
async fn provider_first_bt_attempt_does_not_wait_for_slow_second_source() {
    assert_first_attempt(true).await;
}

#[tokio::test]
async fn provider_immediate_complete_discovery_positive_control() {
    assert_first_attempt(false).await;
}

fn media_piece(marker: u8) -> Vec<u8> {
    (0..4)
        .flat_map(|cc| {
            let mut packet = vec![marker; 188];
            packet[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10 | cc]);
            packet
        })
        .collect()
}

async fn late_candidate_media_control(late: bool, silent_slot: bool, fail_first: bool) {
    let mut servers = tokio::task::JoinSet::new();
    let mut addrs = Vec::new();
    let healthy_attempts = Arc::new(AtomicU32::new(0));
    for healthy in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        addrs.push(addr);
        let attempts = healthy_attempts.clone();
        servers.spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let attempts = attempts.clone();
                connections.spawn(async move {
                    if healthy {
                        let attempt = attempts.fetch_add(1, Ordering::Relaxed);
                        if fail_first && attempt == 0 {
                            return;
                        }
                    }
                    let mut session = PeerSession::new(stream);
                    if session.accept_handshake([0; 20], |_| true).await.is_err() {
                        return;
                    }
                    if session
                        .send_extended_handshake(&OutgoingExtendedHandshake {
                            ace_metadata_version: 1,
                            ut_metadata_id: 2,
                            mi: Some(LivePosition {
                                min_piece: 7,
                                max_piece: if healthy { 10 } else { 8 },
                                position: -1,
                                distance_from_source: 1,
                            }),
                            node: NodeFields::default(),
                            peer_ip: None,
                            metadata_size: None,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                    let _ = session.send(&PeerMessage::Unchoke).await;
                    while let Ok(msg) = session.read_message().await {
                        if healthy {
                            if let PeerMessage::Unknown { id: 6, payload } = msg {
                                let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                                if session
                                    .send(&build_piece(0, piece, 0, [0; 8], &media_piece(0x66)))
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
    let relay = addrs[0];
    let healthy = addrs[1];
    let silent = if silent_slot {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        servers.spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut byte = [0];
            while tokio::io::AsyncReadExt::read(&mut stream, &mut byte)
                .await
                .unwrap()
                > 0
            {}
        });
        Some(addr)
    } else {
        None
    };
    let calls = Arc::new(AtomicU32::new(0));
    let called = calls.clone();
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        called.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move {
            sender
                .send(if late { relay } else { healthy })
                .await
                .unwrap();
            if late {
                if let Some(silent) = silent {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    sender.send(silent).await.unwrap();
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                sender.send(healthy).await.unwrap();
            }
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_prefetch_pieces(Some(1))
        .with_live_recovery(LiveRecoveryConfig {
            max_parallel_connect: if silent_slot { 1 } else { 12 },
            ..LiveRecoveryConfig::default()
        })
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let mut source = provider
        .open_resolved("synthetic", info(), discovery)
        .await
        .unwrap();
    let output = tokio::time::timeout(
        Duration::from_secs(if silent_slot { 5 } else { 3 }),
        source.next(),
    )
    .await;
    drop(source);
    drop(servers);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "cold discovery must remain the sole generation"
    );
    assert!(
        healthy_attempts.load(Ordering::Relaxed) > 0,
        "late candidate was never connected by the active pool"
    );
    assert!(
        matches!(output,Ok(Some(ref bytes)) if bytes.contains(&0x66)),
        "late source must supply actual current media"
    );
}

#[tokio::test]
async fn late_discovery_candidate_enters_existing_active_pool() {
    late_candidate_media_control(true, false, false).await;
}

#[tokio::test]
async fn late_discovery_same_healthy_peer_positive_control() {
    late_candidate_media_control(false, false, false).await;
}

#[tokio::test]
async fn late_discovery_refills_after_silent_reserved_attempt_completes() {
    late_candidate_media_control(true, true, false).await;
}

#[tokio::test]
async fn quiet_candidate_cooldown_expiry_retries_without_new_discovery_or_gossip() {
    late_candidate_media_control(true, false, true).await;
}

// Reconstruct the entire provider: process-local candidate memory cannot satisfy this.
struct DiscoveryDropProbe(Arc<AtomicU32>);
impl Drop for DiscoveryDropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}
#[derive(Clone, Copy)]
enum CancellationStage {
    EmptyFirst,
    SilentConnect,
    FullChannel,
}
async fn discovery_cancellation_control(stage: CancellationStage) {
    let dropped = Arc::new(AtomicU32::new(0));
    let calls = Arc::new(AtomicU32::new(0));
    let full = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let (connected_tx, connected_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::io::AsyncReadExt::read(&mut stream, &mut byte)
                .await
                .unwrap(),
            1
        );
        let _ = connected_tx.send(());
        while tokio::io::AsyncReadExt::read(&mut stream, &mut byte)
            .await
            .unwrap()
            > 0
        {}
    });
    let probe = dropped.clone();
    let called = calls.clone();
    let observed_full = full.clone();
    let factory: PeerDiscovery = Arc::new(move |_, sender| {
        let probe = probe.clone();
        let observed_full = observed_full.clone();
        let called = called.clone();
        Box::pin(async move {
            let _owned = DiscoveryDropProbe(probe);
            called.fetch_add(1, Ordering::Relaxed);
            match stage {
                CancellationStage::EmptyFirst => {}
                CancellationStage::SilentConnect => {
                    sender.send(addr).await.unwrap();
                }
                CancellationStage::FullChannel => {
                    for port in 0..MAX_DISCOVERY_PEERS as u16 {
                        sender
                            .try_send(SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, port))
                            .unwrap();
                    }
                    let extra = SocketAddrV4::new(
                        std::net::Ipv4Addr::LOCALHOST,
                        MAX_DISCOVERY_PEERS as u16,
                    );
                    assert!(matches!(
                        sender.try_send(extra),
                        Err(mpsc::error::TrySendError::Full(_))
                    ));
                    observed_full.store(true, Ordering::Relaxed);
                    if sender.send(extra).await.is_err() {
                        return;
                    }
                }
            }
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0);
    let open =
        tokio::spawn(async move { provider.open_resolved("synthetic", info(), factory).await });
    match stage {
        CancellationStage::SilentConnect => {
            tokio::time::timeout(Duration::from_secs(1), connected_rx)
                .await
                .unwrap()
                .unwrap();
        }
        CancellationStage::FullChannel => {
            tokio::time::timeout(Duration::from_secs(1), async {
                while !full.load(Ordering::Relaxed) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        CancellationStage::EmptyFirst => {
            tokio::time::timeout(Duration::from_secs(1), async {
                while calls.load(Ordering::Relaxed) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
    }
    open.abort();
    if let Ok(Ok(source)) = open.await {
        drop(source);
    }
    let canceled = tokio::time::timeout(Duration::from_secs(1), async {
        while dropped.load(Ordering::Relaxed) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    if matches!(stage, CancellationStage::SilentConnect) {
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    } else {
        server.abort();
        let _ = server.await;
    }
    assert!(
        canceled.is_ok(),
        "owned discovery future remained alive after consumer cancellation"
    );
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "observation timeout restarted discovery"
    );
}
#[tokio::test]
async fn cancellation_during_empty_first_batch_collects_owned_discovery() {
    discovery_cancellation_control(CancellationStage::EmptyFirst).await;
}
#[tokio::test]
async fn cancellation_during_silent_first_connect_closes_socket_and_discovery() {
    discovery_cancellation_control(CancellationStage::SilentConnect).await;
}
#[tokio::test]
async fn cancellation_with_actual_full_discovery_channel_collects_owner() {
    discovery_cancellation_control(CancellationStage::FullChannel).await;
}

#[tokio::test]
async fn late_candidate_at_full_active_cap_survives_for_pool_rebuild() {
    let mut servers = tokio::task::JoinSet::new();
    let mut addrs = Vec::new();
    let later_attempts = Arc::new(AtomicU32::new(0));
    let close_first = Arc::new(tokio::sync::Notify::new());
    for index in 0..2 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        addrs.push(addr);
        let later_attempts = later_attempts.clone();
        let close = close_first.clone();
        servers.spawn(async move {
            let mut clients=tokio::task::JoinSet::new();
            loop {
                let (stream,_)=listener.accept().await.unwrap();if index==1 {later_attempts.fetch_add(1,Ordering::Relaxed);}
                let close=close.clone();clients.spawn(async move {
                    let mut session=PeerSession::new(stream);if session.accept_handshake([0;20],|_|true).await.is_err(){return;}
                    if session.send_extended_handshake(&OutgoingExtendedHandshake {
                        mi:Some(LivePosition {min_piece:7,max_piece:if index==0 {10}else {12},position:-1,distance_from_source:1}),
                        ace_metadata_version:1,ut_metadata_id:2,node:NodeFields::default(),peer_ip:None,metadata_size:None,
                    }).await.is_err(){return;}
                    let _=session.send(&PeerMessage::Unchoke).await;
                    loop {
                        let message=tokio::select! {_=close.notified(),if index==0=>return,message=session.read_message()=>message};
                        let Ok(message)=message else {return;};
                        if let PeerMessage::Unknown {id:6,payload}=message {
                            let piece=u32::from_be_bytes(payload[4..8].try_into().unwrap());
                            if session.send(&build_piece(0,piece,0,[0;8],&media_piece(if index==0 {0x55}else {0x66}))).await.is_err(){return;}
                        }
                    }
                });
            }
        });
    }
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let release = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));
    let calls = Arc::new(AtomicU32::new(0));
    let called = calls.clone();
    let generation_addrs = addrs.clone();
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        called.fetch_add(1, Ordering::Relaxed);
        let release = release.clone();
        let addrs = generation_addrs.clone();
        Box::pin(async move {
            sender.send(addrs[0]).await.unwrap();
            release.lock().await.take().unwrap().await.unwrap();
            sender.send(addrs[1]).await.unwrap();
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_prefetch_pieces(Some(1))
        .with_live_recovery(LiveRecoveryConfig {
            max_active_upstreams: 1,
            max_parallel_connect: 1,
            ..LiveRecoveryConfig::default()
        })
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let mut source = provider
        .open_resolved("synthetic", info(), discovery)
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), source.next())
        .await
        .unwrap()
        .unwrap();
    assert!(first.contains(&0x55));
    release_tx.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        later_attempts.load(Ordering::Relaxed),
        0,
        "full current pool must retain rather than dial an unnecessary candidate"
    );
    close_first.notify_waiters();
    let replacement = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(bytes) = source.next().await {
                if bytes.contains(&0x66) {
                    return;
                }
            } else {
                panic!("replacement source ended");
            }
        }
    })
    .await;
    drop(source);
    servers.abort_all();
    while servers.join_next().await.is_some() {}
    assert!(
        replacement.is_ok(),
        "retained late candidate was lost during pool rebuild"
    );
    assert!(later_attempts.load(Ordering::Relaxed) > 0);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "rebuild discarded the existing discovery generation"
    );
}

#[derive(Clone, Copy)]
enum InitialControlOrder {
    NoControl,
    Unchoke,
    UnchokeThenChoke,
    ChokeThenUnchoke,
    WindowThenUnchoke,
    LaterUnchoke,
}

#[derive(Debug, PartialEq, Eq)]
enum RequestObservation {
    Request(Vec<u8>),
    NoRequestWithinBound,
    PeerExited,
}

async fn observe_request(
    session: &mut PeerSession<TcpStream>,
    bound: Duration,
) -> RequestObservation {
    match tokio::time::timeout(bound, async {
        loop {
            match session.read_message().await {
                Ok(PeerMessage::Unknown { id: 6, payload }) => {
                    return RequestObservation::Request(payload);
                }
                Ok(_) => {}
                Err(_) => return RequestObservation::PeerExited,
            }
        }
    })
    .await
    {
        Ok(observation) => observation,
        Err(_) => RequestObservation::NoRequestWithinBound,
    }
}

fn assert_live_piece_request(observation: RequestObservation, context: &str) {
    assert_live_piece_request_in_range(observation, 9..=9, context);
}

fn assert_live_piece_request_in_range(
    observation: RequestObservation,
    expected_pieces: std::ops::RangeInclusive<u32>,
    context: &str,
) {
    let RequestObservation::Request(payload) = observation else {
        panic!("{context}: expected an id-6 request, observed {observation:?}");
    };
    assert_eq!(payload.len(), 10, "{context}: request payload length");
    assert_eq!(&payload[..4], &[0; 4], "{context}: stream identifier");
    let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
    assert!(
        expected_pieces.contains(&piece),
        "{context}: requested piece {piece} outside {expected_pieces:?}"
    );
    assert_eq!(
        u16::from_be_bytes(payload[8..10].try_into().unwrap()),
        0,
        "{context}: requested chunk"
    );
}

async fn unchoke_order_control(order: InitialControlOrder) -> RequestObservation {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let (activated_tx, activated_rx) = tokio::sync::oneshot::channel();
    let (request_tx, request_rx) = tokio::sync::oneshot::channel();
    let mut server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut session = PeerSession::new(stream);
        session.accept_handshake([0; 20], |_| true).await.unwrap();

        match order {
            InitialControlOrder::Unchoke => {
                session.send(&PeerMessage::Unchoke).await.unwrap();
            }
            InitialControlOrder::UnchokeThenChoke => {
                session.send(&PeerMessage::Unchoke).await.unwrap();
                session.send(&PeerMessage::Choke).await.unwrap();
            }
            InitialControlOrder::ChokeThenUnchoke => {
                session.send(&PeerMessage::Choke).await.unwrap();
                session.send(&PeerMessage::Unchoke).await.unwrap();
            }
            InitialControlOrder::NoControl
            | InitialControlOrder::WindowThenUnchoke
            | InitialControlOrder::LaterUnchoke => {}
        }
        session
            .send_extended_handshake(&OutgoingExtendedHandshake {
                ace_metadata_version: 1,
                ut_metadata_id: 2,
                mi: Some(LivePosition {
                    min_piece: 7,
                    max_piece: 10,
                    position: -1,
                    distance_from_source: 1,
                }),
                node: NodeFields::default(),
                peer_ip: None,
                metadata_size: None,
            })
            .await
            .unwrap();
        if matches!(order, InitialControlOrder::WindowThenUnchoke) {
            session.send(&PeerMessage::Unchoke).await.unwrap();
        }

        assert!(matches!(
            session.read_message().await.unwrap(),
            PeerMessage::Extended { ext_id: 0, .. }
        ));
        assert_eq!(
            session.read_message().await.unwrap(),
            PeerMessage::Interested
        );
        activated_tx.send(()).unwrap();
        if matches!(order, InitialControlOrder::LaterUnchoke) {
            session.send(&PeerMessage::Unchoke).await.unwrap();
        }

        let observation = observe_request(&mut session, Duration::from_secs(4)).await;
        let _ = request_tx.send(observation);
    });

    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        Box::pin(async move {
            sender.send(addr).await.unwrap();
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_prefetch_pieces(Some(1))
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let source = match provider.open_resolved("synthetic", info(), discovery).await {
        Ok(source) => source,
        Err(error) => {
            server.abort();
            let _ = server.await;
            panic!("synthetic resolved open failed: {error}");
        }
    };

    let activated = tokio::time::timeout(Duration::from_secs(2), activated_rx).await;
    let request = if matches!(&activated, Ok(Ok(()))) {
        match tokio::time::timeout(Duration::from_secs(5), request_rx).await {
            Ok(Ok(observation)) => observation,
            Ok(Err(_)) => RequestObservation::PeerExited,
            Err(_) => RequestObservation::PeerExited,
        }
    } else {
        RequestObservation::PeerExited
    };

    drop(source);
    let server_done = tokio::time::timeout(Duration::from_secs(1), &mut server).await;
    if server_done.is_err() {
        server.abort();
        let _ = server.await;
    }
    assert!(
        matches!(&activated, Ok(Ok(()))),
        "peer activation must reach the signed-extension and Interested barrier"
    );
    server_done
        .expect("fake peer must stop after source cleanup")
        .expect("fake peer task must not panic");
    request
}

#[tokio::test]
async fn unchoke_ordering_window_then_unchoke_requests_a_live_piece() {
    assert_live_piece_request(
        unchoke_order_control(InitialControlOrder::WindowThenUnchoke).await,
        "window-first Unchoke must reach the real chunk-request path",
    );
}

#[tokio::test]
async fn unchoke_ordering_early_unchoke_is_retained_for_live_piece_requests() {
    assert_live_piece_request(
        unchoke_order_control(InitialControlOrder::Unchoke).await,
        "window-reader must preserve an earlier Unchoke through peer activation",
    );
}

#[tokio::test]
async fn unchoke_ordering_default_peer_does_not_request_without_control() {
    assert_eq!(
        unchoke_order_control(InitialControlOrder::NoControl).await,
        RequestObservation::NoRequestWithinBound,
        "no pre-window control must leave the peer choked and observable"
    );
}

#[tokio::test]
async fn unchoke_ordering_pre_window_unchoke_then_choke_remains_choked() {
    assert_eq!(
        unchoke_order_control(InitialControlOrder::UnchokeThenChoke).await,
        RequestObservation::NoRequestWithinBound,
        "the last pre-window Choke must override Unchoke while observable"
    );
}

#[tokio::test]
async fn unchoke_ordering_pre_window_choke_then_unchoke_requests_a_live_piece() {
    assert_live_piece_request(
        unchoke_order_control(InitialControlOrder::ChokeThenUnchoke).await,
        "the last pre-window Unchoke must enable the real chunk-request path",
    );
}

#[tokio::test]
async fn unchoke_ordering_later_control_uses_the_active_worker_path() {
    assert_live_piece_request(
        unchoke_order_control(InitialControlOrder::LaterUnchoke).await,
        "a post-activation Unchoke must reach the existing worker/event path",
    );
}

#[tokio::test]
async fn unchoke_ordering_initial_two_early_peers_schedule_without_later_control() {
    let mut servers = tokio::task::JoinSet::new();
    let (ready_tx, ready_rx) = tokio::sync::mpsc::channel(2);
    let mut ready_rx = ready_rx;
    let (activated_tx, activated_rx) = tokio::sync::mpsc::channel(2);
    let mut activated_rx = activated_rx;
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let mut addrs = Vec::new();
    let mut request_receivers = Vec::new();

    for _ in 0..2 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        addrs.push(addr);
        let ready_tx = ready_tx.clone();
        let activated_tx = activated_tx.clone();
        let gate = gate.clone();
        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        request_receivers.push(request_rx);
        servers.spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut session = PeerSession::new(stream);
            session.accept_handshake([0; 20], |_| true).await.unwrap();
            ready_tx.send(()).await.unwrap();
            gate.wait().await;

            // Both connected candidates receive exactly one early Unchoke and the same
            // window together; neither sends any later control or advancing window.
            session.send(&PeerMessage::Unchoke).await.unwrap();
            session
                .send_extended_handshake(&OutgoingExtendedHandshake {
                    ace_metadata_version: 1,
                    ut_metadata_id: 2,
                    mi: Some(LivePosition {
                        min_piece: 7,
                        max_piece: 10,
                        position: -1,
                        distance_from_source: 1,
                    }),
                    node: NodeFields::default(),
                    peer_ip: None,
                    metadata_size: None,
                })
                .await
                .unwrap();
            assert!(matches!(
                session.read_message().await.unwrap(),
                PeerMessage::Extended { ext_id: 0, .. }
            ));
            assert_eq!(
                session.read_message().await.unwrap(),
                PeerMessage::Interested
            );
            activated_tx.send(()).await.unwrap();

            let observation = observe_request(&mut session, Duration::from_secs(4)).await;
            let _ = request_tx.send(observation);
        });
    }
    drop(ready_tx);
    drop(activated_tx);

    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        let addrs = addrs.clone();
        Box::pin(async move {
            for addr in addrs {
                sender.send(addr).await.unwrap();
            }
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_prefetch_pieces(Some(1))
        .with_live_recovery(LiveRecoveryConfig {
            max_active_upstreams: 2,
            max_parallel_connect: 2,
            ..LiveRecoveryConfig::default()
        })
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let source = match provider.open_resolved("synthetic", info(), discovery).await {
        Ok(source) => source,
        Err(error) => {
            servers.abort_all();
            while servers.join_next().await.is_some() {}
            panic!("synthetic resolved open failed: {error}");
        }
    };

    let both_connected = tokio::time::timeout(Duration::from_secs(2), async {
        ready_rx.recv().await?;
        ready_rx.recv().await
    })
    .await;
    // Each post-activation barrier is emitted only after the real initial upstream
    // selection has admitted that connection.
    let both_activated = tokio::time::timeout(Duration::from_secs(2), async {
        activated_rx.recv().await?;
        activated_rx.recv().await
    })
    .await;
    let mut request_receivers = request_receivers.into_iter();
    let mut first_request_rx = request_receivers.next().unwrap();
    let mut second_request_rx = request_receivers.next().unwrap();
    let (first_observation, second_observation) = if matches!(&both_activated, Ok(Some(()))) {
        let first = async {
            match tokio::time::timeout(Duration::from_secs(5), &mut first_request_rx).await {
                Ok(Ok(observation)) => observation,
                Ok(Err(_)) | Err(_) => RequestObservation::PeerExited,
            }
        };
        let second = async {
            match tokio::time::timeout(Duration::from_secs(5), &mut second_request_rx).await {
                Ok(Ok(observation)) => observation,
                Ok(Err(_)) | Err(_) => RequestObservation::PeerExited,
            }
        };
        tokio::join!(first, second)
    } else {
        (
            RequestObservation::PeerExited,
            RequestObservation::PeerExited,
        )
    };

    drop(source);
    let servers_done = tokio::time::timeout(Duration::from_secs(1), async {
        let mut all_succeeded = true;
        while let Some(result) = servers.join_next().await {
            all_succeeded &= result.is_ok();
        }
        all_succeeded
    })
    .await;
    if servers_done.is_err() {
        servers.abort_all();
        while servers.join_next().await.is_some() {}
    }

    assert!(
        matches!(both_connected, Ok(Some(()))),
        "both loopback peers must connect before either sends its early control"
    );
    assert!(
        matches!(both_activated, Ok(Some(()))),
        "both peers must be admitted by the initial selected pool"
    );
    assert!(
        matches!(servers_done, Ok(true)),
        "fake peers must stop cleanly after their bounded observation and source cleanup"
    );
    let mut request_count = 0;
    for observation in [first_observation, second_observation] {
        match observation {
            RequestObservation::Request(payload) => {
                assert_live_piece_request_in_range(
                    RequestObservation::Request(payload),
                    9..=10,
                    "initial multi-peer retained Unchoke must request an in-window live piece",
                );
                request_count += 1;
            }
            RequestObservation::NoRequestWithinBound => {}
            RequestObservation::PeerExited => {
                panic!("peer exited before completing its bounded request observation")
            }
        }
    }
    assert!(
        request_count > 0,
        "two initially admitted early-Unchoke peers must schedule a request without a later control"
    );
}

#[tokio::test]
async fn unchoke_ordering_active_peer_does_not_change_another_peers_default() {
    let mut servers = tokio::task::JoinSet::new();
    let (activated_tx, mut activated_rx) = tokio::sync::mpsc::channel(2);
    let mut addrs = Vec::new();
    let mut request_receivers = Vec::new();

    for unchoke_after_activation in [true, false] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        addrs.push(addr);
        let activated_tx = activated_tx.clone();
        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        request_receivers.push(request_rx);
        servers.spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut session = PeerSession::new(stream);
            session.accept_handshake([0; 20], |_| true).await.unwrap();
            session
                .send_extended_handshake(&OutgoingExtendedHandshake {
                    ace_metadata_version: 1,
                    ut_metadata_id: 2,
                    mi: Some(LivePosition {
                        min_piece: 7,
                        max_piece: 10,
                        position: -1,
                        distance_from_source: 1,
                    }),
                    node: NodeFields::default(),
                    peer_ip: None,
                    metadata_size: None,
                })
                .await
                .unwrap();
            assert!(matches!(
                session.read_message().await.unwrap(),
                PeerMessage::Extended { ext_id: 0, .. }
            ));
            assert_eq!(
                session.read_message().await.unwrap(),
                PeerMessage::Interested
            );
            activated_tx.send(unchoke_after_activation).await.unwrap();
            if unchoke_after_activation {
                session.send(&PeerMessage::Unchoke).await.unwrap();
            }
            let observation = observe_request(&mut session, Duration::from_secs(4)).await;
            let _ = request_tx.send(observation);
        });
    }
    drop(activated_tx);

    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        let addrs = addrs.clone();
        Box::pin(async move {
            for addr in addrs {
                sender.send(addr).await.unwrap();
            }
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_prefetch_pieces(Some(1))
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let source = match provider.open_resolved("synthetic", info(), discovery).await {
        Ok(source) => source,
        Err(error) => {
            servers.abort_all();
            while servers.join_next().await.is_some() {}
            panic!("synthetic resolved open failed: {error}");
        }
    };
    let activated = tokio::time::timeout(Duration::from_secs(2), async {
        let mut roles = Vec::new();
        for _ in 0..2 {
            roles.push(activated_rx.recv().await?);
        }
        Some(roles)
    })
    .await;
    let mut request_receivers = request_receivers.into_iter();
    let mut unchoked_request_rx = request_receivers.next().unwrap();
    let mut choked_request_rx = request_receivers.next().unwrap();
    let (unchoked_request, choked_request) = if matches!(&activated, Ok(Some(_))) {
        let unchoked = async {
            match tokio::time::timeout(Duration::from_secs(5), &mut unchoked_request_rx).await {
                Ok(Ok(observation)) => observation,
                Ok(Err(_)) | Err(_) => RequestObservation::PeerExited,
            }
        };
        let choked = async {
            match tokio::time::timeout(Duration::from_secs(5), &mut choked_request_rx).await {
                Ok(Ok(observation)) => observation,
                Ok(Err(_)) | Err(_) => RequestObservation::PeerExited,
            }
        };
        tokio::join!(unchoked, choked)
    } else {
        (
            RequestObservation::PeerExited,
            RequestObservation::PeerExited,
        )
    };

    drop(source);
    let servers_done = tokio::time::timeout(Duration::from_secs(1), async {
        let mut all_succeeded = true;
        while let Some(result) = servers.join_next().await {
            all_succeeded &= result.is_ok();
        }
        all_succeeded
    })
    .await;
    if servers_done.is_err() {
        servers.abort_all();
        while servers.join_next().await.is_some() {}
    }
    assert!(
        matches!(activated, Ok(Some(_))),
        "both peer activations must reach their barriers"
    );
    assert!(
        matches!(servers_done, Ok(true)),
        "fake peers must stop cleanly after source cleanup"
    );
    assert_live_piece_request(
        unchoked_request,
        "the unchoked peer must receive a valid request",
    );
    assert!(
        choked_request == RequestObservation::NoRequestWithinBound,
        "the default-choked peer must remain observably request-free; got {choked_request:?}"
    );
}

#[tokio::test]
async fn unchoke_ordering_early_incoming_does_not_suppress_reciprocal_seed_unchoke() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let (activated_tx, activated_rx) = tokio::sync::oneshot::channel();
    let (reciprocal_tx, reciprocal_rx) = tokio::sync::oneshot::channel();
    let mut server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut session = PeerSession::new(stream).with_timeout(Duration::from_secs(3));
        session.accept_handshake([0; 20], |_| true).await.unwrap();
        session.send(&PeerMessage::Unchoke).await.unwrap();
        session
            .send_extended_handshake(&OutgoingExtendedHandshake {
                ace_metadata_version: 1,
                ut_metadata_id: 2,
                mi: Some(LivePosition {
                    min_piece: 7,
                    max_piece: 10,
                    position: -1,
                    distance_from_source: 1,
                }),
                node: NodeFields::default(),
                peer_ip: None,
                metadata_size: None,
            })
            .await
            .unwrap();
        assert!(matches!(
            session.read_message().await.unwrap(),
            PeerMessage::Extended { ext_id: 0, .. }
        ));
        assert_eq!(
            session.read_message().await.unwrap(),
            PeerMessage::Interested
        );
        session.send(&PeerMessage::Interested).await.unwrap();
        activated_tx.send(()).unwrap();
        let reciprocal = tokio::time::timeout(Duration::from_secs(2), session.read_message())
            .await
            .ok()
            .and_then(Result::ok);
        let _ = reciprocal_tx.send(matches!(reciprocal, Some(PeerMessage::Unchoke)));
    });

    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        Box::pin(async move {
            sender.send(addr).await.unwrap();
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_seeding_enabled(true)
        .with_prefetch_pieces(Some(1))
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let source = match provider.open_resolved("synthetic", info(), discovery).await {
        Ok(source) => source,
        Err(error) => {
            server.abort();
            let _ = server.await;
            panic!("synthetic resolved open failed: {error}");
        }
    };

    let activated = tokio::time::timeout(Duration::from_secs(2), activated_rx).await;
    let reciprocal = if matches!(&activated, Ok(Ok(()))) {
        tokio::time::timeout(Duration::from_secs(3), reciprocal_rx)
            .await
            .ok()
            .and_then(Result::ok)
    } else {
        None
    };

    drop(source);
    let server_done = tokio::time::timeout(Duration::from_secs(1), &mut server).await;
    if server_done.is_err() {
        server.abort();
        let _ = server.await;
    }
    assert!(
        matches!(&activated, Ok(Ok(()))),
        "fake peer must observe our signed extension and Interested"
    );
    server_done
        .expect("fake peer must stop after source cleanup")
        .expect("fake peer task must not panic");
    assert_eq!(
        reciprocal,
        Some(true),
        "retaining incoming Unchoke must not imply we already sent reciprocal Unchoke"
    );
}

async fn surviving_peer_capacity_control(active_cap: usize) {
    let mut servers = tokio::task::JoinSet::new();
    let mut addrs = Vec::new();
    let later_attempts = Arc::new(AtomicU32::new(0));
    let close_first = Arc::new(tokio::sync::Notify::new());
    for index in 0..3 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        addrs.push(addr);
        let later_attempts = later_attempts.clone();
        let close = close_first.clone();
        servers.spawn(async move {
            let mut clients = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                if index == 2 {
                    later_attempts.fetch_add(1, Ordering::Relaxed);
                }
                let close = close.clone();
                clients.spawn(async move {
                    let mut session = PeerSession::new(stream);
                    if session.accept_handshake([0; 20], |_| true).await.is_err() {
                        return;
                    }
                    if session
                        .send_extended_handshake(&OutgoingExtendedHandshake {
                            mi: Some(LivePosition {
                                min_piece: 7,
                                max_piece: if index == 2 { 12 } else { 10 },
                                position: -1,
                                distance_from_source: 1,
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
                    let _ = session.send(&PeerMessage::Unchoke).await;
                    loop {
                        let message = tokio::select! {
                            _ = close.notified(), if index == 0 => return,
                            message = session.read_message() => message,
                        };
                        let Ok(message) = message else {
                            return;
                        };
                        if let PeerMessage::Unknown { id: 6, payload } = message {
                            let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                            if session
                                .send(&build_piece(
                                    0,
                                    piece,
                                    0,
                                    [0; 8],
                                    &media_piece(if index == 2 { 0x66 } else { 0x55 }),
                                ))
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
    }
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let release = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));
    let calls = Arc::new(AtomicU32::new(0));
    let called = calls.clone();
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        called.fetch_add(1, Ordering::Relaxed);
        let release = release.clone();
        let addrs = addrs.clone();
        Box::pin(async move {
            // Both initial peers enter the same actual connect_pool selection batch.
            sender.try_send(addrs[0]).unwrap();
            sender.try_send(addrs[1]).unwrap();
            release.lock().await.take().unwrap().await.unwrap();
            sender.send(addrs[2]).await.unwrap();
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_prefetch_pieces(Some(1))
        .with_live_recovery(LiveRecoveryConfig {
            max_active_upstreams: active_cap,
            max_parallel_connect: 2,
            ..LiveRecoveryConfig::default()
        })
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let mut source = provider
        .open_resolved("synthetic", info(), discovery)
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), source.next())
        .await
        .unwrap()
        .unwrap();
    assert!(first.contains(&0x55));
    // Publish both pieces through the initial head before raising it. Otherwise the
    // strict one-piece floor correctly skips the outstanding old piece and arms the
    // decoder recovery gate, which this scheduling-only fixture does not model.
    let mut initial_bytes = first.len();
    tokio::time::timeout(Duration::from_secs(2), async {
        // TsResync retains the final packet until another sync byte confirms it.
        while initial_bytes < 7 * 188 {
            let bytes = source.next().await.expect("initial producer ended");
            assert!(bytes.contains(&0x55));
            initial_bytes += bytes.len();
        }
    })
    .await
    .unwrap();
    release_tx.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    if active_cap == 2 {
        assert_eq!(
            later_attempts.load(Ordering::Relaxed),
            0,
            "full active pool dialed a late candidate"
        );
        close_first.notify_waiters();
    }
    let replacement = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(bytes) = source.next().await {
                if bytes.contains(&0x66) {
                    return;
                }
            } else {
                panic!("surviving pool ended");
            }
        }
    })
    .await;
    drop(source);
    servers.abort_all();
    while servers.join_next().await.is_some() {}
    assert!(
        replacement.is_ok(),
        "freed active slot stranded a retained peer while another upstream survived"
    );
    assert!(later_attempts.load(Ordering::Relaxed) > 0);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn retained_candidate_enters_after_peer_loss_with_surviving_upstream() {
    surviving_peer_capacity_control(2).await;
}

#[tokio::test]
async fn surviving_peer_late_candidate_positive_control() {
    surviving_peer_capacity_control(3).await;
}

async fn prepared_peer_after_output_control(newer_window: bool) {
    let mut servers = tokio::task::JoinSet::new();
    let mut addrs = Vec::new();
    for delayed in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        addrs.push(addr);
        servers.spawn(async move {
            let mut clients = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                clients.spawn(async move {
                    // Complete after the real one-second lone-window gate, so the
                    // initial producer publishes before this pending transport is ready.
                    if delayed {
                        tokio::time::sleep(Duration::from_millis(1500)).await;
                    }
                    let mut session = PeerSession::new(stream);
                    if session.accept_handshake([0; 20], |_| true).await.is_err() {
                        return;
                    }
                    if session
                        .send_extended_handshake(&OutgoingExtendedHandshake {
                            mi: Some(LivePosition {
                                min_piece: 7,
                                max_piece: if delayed && newer_window { 12 } else { 10 },
                                position: -1,
                                distance_from_source: 1,
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
                    let _ = session.send(&PeerMessage::Unchoke).await;
                    while let Ok(msg) = session.read_message().await {
                        if let PeerMessage::Unknown { id: 6, payload } = msg {
                            let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                            if !delayed && piece != 9 {
                                continue;
                            }
                            let mut media = include_bytes!(
                                "../../../../tests/vectors/media/transport-resync.ts"
                            )[..752]
                                .to_vec();
                            for packet in media
                                .chunks_mut(188)
                                .filter(|p| ace_media::mpegts::ts_pid(p) == 0x101)
                            {
                                packet[6..].fill(if delayed { 0x66 } else { 0x55 });
                            }
                            if session
                                .send(&build_piece(0, piece, 0, [0; 8], &media))
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
    }
    let calls = Arc::new(AtomicU32::new(0));
    let called = calls.clone();
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        called.fetch_add(1, Ordering::Relaxed);
        let addrs = addrs.clone();
        Box::pin(async move {
            for addr in addrs {
                sender.try_send(addr).unwrap();
            }
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_prefetch_pieces(Some(1))
        .with_live_recovery(LiveRecoveryConfig {
            max_active_upstreams: 2,
            max_parallel_connect: 1,
            ..LiveRecoveryConfig::default()
        })
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let mut source = provider
        .open_resolved("synthetic", info(), discovery)
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), source.next())
        .await
        .unwrap()
        .unwrap();
    assert!(
        first.contains(&0x55),
        "initial peer must publish before the prepared transport completes"
    );
    let later = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(bytes) = source.next().await {
                if bytes.contains(&0x66) {
                    return;
                }
            } else {
                panic!("prepared producer lost");
            }
        }
    })
    .await;
    drop(source);
    servers.abort_all();
    while servers.join_next().await.is_some() {}
    assert!(
        later.is_ok(),
        "productive history reset discarded a useful prepared producer"
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn prepared_within_floor_peer_survives_productive_history_reset() {
    prepared_peer_after_output_control(false).await;
}

#[tokio::test]
async fn prepared_newer_window_peer_survives_productive_history_reset() {
    prepared_peer_after_output_control(true).await;
}

// Exercise the production resolved-open path before any upstream has a live window.
async fn initial_silent_candidate_control(parallel: usize, cancel: bool) {
    struct DiscoveryDropped(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for DiscoveryDropped {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let healthy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = |listener: &tokio::net::TcpListener| match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(addr) => addr,
        _ => unreachable!(),
    };
    let silent_addr = addr(&silent);
    let healthy_addr = addr(&healthy);
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
    let (healthy_tx, mut healthy_rx) = tokio::sync::oneshot::channel();
    let mut servers = tokio::task::JoinSet::new();
    servers.spawn(async move {
        let (mut stream, _) = silent.accept().await.unwrap();
        let _ = accepted_tx.send(());
        tokio::select! {
            _ = release_rx => {},
            _ = async {
                let mut bytes = [0; 1024];
                while tokio::io::AsyncReadExt::read(&mut stream, &mut bytes).await.unwrap() != 0 {}
                let _ = closed_tx.send(());
            } => {},
        }
    });
    servers.spawn(async move {
        let (stream, _) = healthy.accept().await.unwrap();
        let _ = healthy_tx.send(());
        let mut session = PeerSession::new(stream);
        session.accept_handshake([0; 20], |_| true).await.unwrap();
        session
            .send_extended_handshake(&OutgoingExtendedHandshake {
                mi: Some(LivePosition {
                    min_piece: 7,
                    max_piece: 10,
                    position: -1,
                    distance_from_source: 0,
                }),
                ace_metadata_version: 1,
                ut_metadata_id: 2,
                node: NodeFields::default(),
                peer_ip: None,
                metadata_size: None,
            })
            .await
            .unwrap();
        session.send(&PeerMessage::Unchoke).await.unwrap();
        while let Ok(message) = session.read_message().await {
            if let PeerMessage::Unknown { id: 6, payload } = message {
                let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                if session
                    .send(&build_piece(0, piece, 0, [0; 8], &media_piece(0x66)))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    });
    let (published_tx, published_rx) = tokio::sync::oneshot::channel();
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let state = Arc::new(std::sync::Mutex::new(Some((
        accepted_rx,
        published_tx,
        dropped_tx,
    ))));
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        let (accepted, published, dropped) = state.lock().unwrap().take().unwrap();
        Box::pin(async move {
            let _dropped = DiscoveryDropped(Some(dropped));
            sender.send(silent_addr).await.unwrap();
            accepted.await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            sender.send(healthy_addr).await.unwrap();
            sender.send(healthy_addr).await.unwrap(); // Repeated batches must not duplicate attempts.
            let _ = published.send(Instant::now());
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_prefetch_pieces(Some(1))
        .with_live_recovery(LiveRecoveryConfig {
            max_parallel_connect: parallel,
            ..LiveRecoveryConfig::default()
        })
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let mut source = provider
        .open_resolved("synthetic", info(), discovery)
        .await
        .unwrap();
    let published = tokio::time::timeout(Duration::from_secs(2), published_rx)
        .await
        .unwrap()
        .unwrap();
    if cancel {
        drop(source);
        tokio::time::timeout(Duration::from_secs(1), closed_rx)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), dropped_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), healthy_rx)
                .await
                .is_err(),
            "consumer cancellation must not admit the queued peer"
        );
        return;
    }
    if parallel == 1 {
        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut healthy_rx)
                .await
                .is_err(),
            "late candidate must not exceed the occupied connection limit"
        );
        release_tx.send(()).unwrap();
    }
    let connected = tokio::time::timeout(Duration::from_secs(1), healthy_rx).await;
    let output = tokio::time::timeout(Duration::from_secs(2), source.next()).await;
    drop(source);
    assert!(
        matches!(connected, Ok(Ok(()))),
        "late candidate did not use available capacity after {:?}",
        published.elapsed()
    );
    assert!(
        matches!(output, Ok(Some(ref bytes)) if bytes.contains(&0x66)),
        "late initial candidate must deliver actual media"
    );
}

#[tokio::test]
async fn late_initial_candidate_uses_spare_slot_during_silent_handshake() {
    initial_silent_candidate_control(2, false).await;
}

#[tokio::test]
async fn late_initial_candidate_waits_for_full_connection_capacity() {
    initial_silent_candidate_control(1, false).await;
}

#[tokio::test]
async fn dropping_initial_connect_cancels_attempts_and_discovery() {
    initial_silent_candidate_control(1, true).await;
}
