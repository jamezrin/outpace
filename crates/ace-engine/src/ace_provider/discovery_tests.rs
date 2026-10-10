//! Real provider/discovery/BT cold-start regressions, synthetic loopback addresses only.
use super::*;
use ace_swarm::discover::discover_peers_from_sources_incremental;

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
#[derive(Clone, Copy, PartialEq, Eq)]
enum CacheProducer {
    Authenticated,
    Unsigned,
    Rejected,
    Incomplete,
    AnnounceOnly,
    Mixed,
    Stale,
}
async fn durable_provider_cache_control(mode: CacheProducer) {
    let _guard = warm_peers::test_guard().await;
    let auth = ace_wire::live_auth::LiveSourceAuth::generate();
    let payload = media_piece(0x5a);
    let mut signed = payload.clone();
    if mode != CacheProducer::Unsigned {
        signed.extend(auth.sign(&payload));
    }
    if mode == CacheProducer::Rejected {
        signed[0] ^= 1;
    }
    let relay_signed = signed.clone();
    let descriptor = StreamInfo {
        piece_length: signed.len() as u64,
        chunk_length: if mode == CacheProducer::Mixed {
            signed.len() as u64 / 2
        } else {
            signed.len() as u64
        },
        sig_len: if mode == CacheProducer::Unsigned {
            0
        } else {
            auth.signature_len()
        },
        source_pubkey: if mode == CacheProducer::Unsigned {
            Vec::new()
        } else {
            auth.pubkey_der()
        },
        ..info()
    };
    let healthy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = |listener: &tokio::net::TcpListener| match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(addr) => addr,
        _ => unreachable!(),
    };
    let source_addr = addr(&healthy);
    let relay_addr = addr(&relay);
    let attempts = Arc::new(AtomicU32::new(0));
    let counted = attempts.clone();
    let mut servers = tokio::task::JoinSet::new();
    servers.spawn(async move {
        let mut clients = tokio::task::JoinSet::new();
        loop {
            let (stream, _) = healthy.accept().await.unwrap();
            counted.fetch_add(1, Ordering::Relaxed);
            let signed = signed.clone();
            clients.spawn(async move {
                let mut session = PeerSession::new(stream);
                if session.accept_handshake([0; 20], |_| true).await.is_err() {
                    return;
                }
                if session
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
                    .is_err()
                {
                    return;
                }
                let _ = session.send(&PeerMessage::Unchoke).await;
                let mut rejected_sent = false;
                while let Ok(message) = session.read_message().await {
                    if let PeerMessage::Unknown { id: 6, payload } = message {
                        let piece = if mode == CacheProducer::Stale {
                            7
                        } else {
                            u32::from_be_bytes(payload[4..8].try_into().unwrap())
                        };
                        if mode == CacheProducer::AnnounceOnly
                            || (mode == CacheProducer::Rejected && rejected_sent)
                        {
                            continue;
                        }
                        rejected_sent = mode == CacheProducer::Rejected;
                        let data =
                            if matches!(mode, CacheProducer::Mixed | CacheProducer::Incomplete) {
                                &signed[..signed.len() / 2]
                            } else {
                                &signed
                            };
                        if session
                            .send(&build_piece(0, piece, 0, [0; 8], data))
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
    servers.spawn(async move {
        let (stream, _) = relay.accept().await.unwrap();
        let mut session = PeerSession::new(stream);
        session.accept_handshake([0; 20], |_| true).await.unwrap();
        session
            .send_extended_handshake(&OutgoingExtendedHandshake {
                mi: Some(LivePosition {
                    min_piece: 7,
                    max_piece: 10,
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
            .unwrap();
        session.send(&PeerMessage::Unchoke).await.unwrap();
        let mut announce = vec![0; 14];
        announce[8..12].copy_from_slice(&source_addr.ip().octets());
        announce[12..14].copy_from_slice(&source_addr.port().to_be_bytes());
        session
            .send(&PeerMessage::Unknown {
                id: 36,
                payload: announce,
            })
            .await
            .unwrap();
        while let Ok(message) = session.read_message().await {
            if mode == CacheProducer::Mixed {
                if let PeerMessage::Unknown { id: 6, payload } = message {
                    let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                    if session
                        .send(&build_piece(
                            0,
                            piece,
                            1,
                            [0; 8],
                            &relay_signed[relay_signed.len() / 2..],
                        ))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
    });
    static NEXT_CACHE: AtomicU64 = AtomicU64::new(0);
    let cache_dir = std::env::temp_dir().join(format!(
        "outpace-warm-provider-{}-{}",
        std::process::id(),
        NEXT_CACHE.fetch_add(1, Ordering::Relaxed)
    ));
    let make_provider = || {
        AceProvider::new(Arc::new(Identity::generate()), 0)
            .with_warm_cache_loopback_policy(cache_dir.clone())
            .with_prefetch_pieces(Some(1))
            .with_startup_buffer(StartupBufferConfig {
                target_ms: 0,
                ..StartupBufferConfig::default()
            })
    };
    let first = make_provider();
    let mut source = first
        .open_resolved(
            "synthetic",
            descriptor.clone(),
            completed_discovery(move |_| async move { vec![relay_addr] }),
        )
        .await
        .unwrap();
    let output = tokio::time::timeout(
        Duration::from_secs(if mode == CacheProducer::Mixed { 5 } else { 3 }),
        source.next(),
    )
    .await;
    if matches!(
        mode,
        CacheProducer::Authenticated | CacheProducer::Unsigned | CacheProducer::Mixed
    ) {
        assert!(
            matches!(output, Ok(Some(ref bytes)) if bytes.contains(&0x5a)),
            "positive fixture must emit its actual media"
        );
    } else {
        assert!(
            output.is_err(),
            "rejected/incomplete/announce-only producer emitted media"
        );
    }
    if mode == CacheProducer::Authenticated {
        assert!(
            first.warm_peers.hints(&[0; 20]) == vec![(source_addr, CandidateKind::Source)],
            "credit belongs to the complete authenticated source, not its announcing relay"
        );
    } else {
        assert!(
            first.warm_peers.hints(&[0; 20]).is_empty(),
            "unverified or mixed producer acquired productive cache credit"
        );
    }
    let before = attempts.load(Ordering::Relaxed);
    assert!(before > 0);
    assert!(
        first.warm_peers.flush().await,
        "normal productive cache revision must actually persist"
    );
    drop(source);
    drop(first);
    assert!(
        warm_peers::retired_terminal(&cache_dir).await,
        "old cache owner/writer must be terminal before fresh reconstruction"
    );
    let fresh = make_provider();
    fresh.warm_peers.initialized().await;
    assert!(
        fresh.open(&infohash_hex(&[0; 20])).await.is_err(),
        "peer hints must never grant bare-infohash descriptor authority"
    );
    if mode != CacheProducer::Authenticated {
        assert!(
            fresh.warm_peers.hints(&[0; 20]).is_empty(),
            "negative credit persisted across provider restart"
        );
        drop(fresh);
        assert!(warm_peers::retired_terminal(&cache_dir).await);
        servers.abort_all();
        while servers.join_next().await.is_some() {}
        std::fs::remove_dir_all(cache_dir).unwrap();
        return;
    }
    let slow = completed_discovery(|_| async {
        tokio::time::sleep(Duration::from_secs(5)).await;
        Vec::new()
    });
    let open =
        tokio::spawn(async move { fresh.open_resolved("synthetic", descriptor, slow).await });
    let warm_attempt = tokio::time::timeout(Duration::from_secs(2), async {
        while attempts.load(Ordering::Relaxed) == before {
            tokio::task::yield_now().await;
        }
    })
    .await;
    open.abort();
    if let Ok(Ok(source)) = open.await {
        drop(source);
    }
    servers.abort_all();
    while servers.join_next().await.is_some() {}
    assert!(warm_peers::retired_terminal(&cache_dir).await);
    let _ = std::fs::remove_dir_all(cache_dir);
    assert!(warm_attempt.is_ok(), "fresh provider never dialed its previously authenticated id36 source while discovery was still pending");
}

#[tokio::test]
async fn authenticated_source_is_durable_for_fresh_provider_warm_start() {
    durable_provider_cache_control(CacheProducer::Authenticated).await;
}
#[tokio::test]
async fn unsigned_media_never_creates_durable_peer_credit() {
    durable_provider_cache_control(CacheProducer::Unsigned).await;
}
#[tokio::test]
async fn rejected_signature_never_creates_durable_peer_credit() {
    durable_provider_cache_control(CacheProducer::Rejected).await;
}
#[tokio::test]
async fn incomplete_signed_piece_never_creates_durable_peer_credit() {
    durable_provider_cache_control(CacheProducer::Incomplete).await;
}
#[tokio::test]
async fn source_announcement_without_media_never_creates_durable_peer_credit() {
    durable_provider_cache_control(CacheProducer::AnnounceOnly).await;
}
#[tokio::test]
async fn mixed_producer_authenticated_piece_never_creates_durable_peer_credit() {
    durable_provider_cache_control(CacheProducer::Mixed).await;
}

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

#[tokio::test]
async fn stale_signed_piece_never_creates_durable_peer_credit() {
    durable_provider_cache_control(CacheProducer::Stale).await;
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum ResumeCredit {
    MixedPartial,
    CompleteDuplicate,
    SameProducer,
}

struct SignedResumePool {
    info: StreamInfo,
    seed: SeedConfig,
    store: Arc<tokio::sync::Mutex<PieceStore>>,
    continuity: Option<Continuity>,
    candidates: SessionCandidates,
}
impl SignedResumePool {
    async fn run(
        &mut self,
        addresses: Vec<SocketAddrV4>,
        tx: &mpsc::Sender<LiveOutput>,
    ) -> FollowEnd {
        let mut upstreams = Vec::new();
        for addr in addresses {
            match connect_upstream(addr, self.info.infohash).await {
                PeerConnectAttempt::Connected(upstream) => upstreams.push(upstream),
                PeerConnectAttempt::Failed(_) => panic!("signed reconnect fixture must connect"),
            }
        }
        follow_peer_pool_with_discovery(
            upstreams,
            &self.info,
            &Identity::generate(),
            self.info.chunks_per_piece(),
            tx,
            &Arc::new(AtomicU64::new(0)),
            &Arc::new(AtomicU64::new(0)),
            &Arc::new(AtomicU32::new(0)),
            &self.seed,
            &self.store,
            &mut self.continuity,
            vec![],
            vec![],
            completed_discovery(|_| async { Vec::new() }),
            &mut self.candidates,
            &Arc::new(AtomicU32::new(0)),
            None,
            &mut None,
        )
        .await
    }
}

async fn signed_resume_session(
    stream: tokio::net::TcpStream,
    pieces: Arc<Vec<Vec<u8>>>,
    blocks: &[(u32, u16)],
    initial: bool,
    close: bool,
    ready: Option<tokio::sync::oneshot::Receiver<()>>,
) {
    let mut session = PeerSession::new(stream);
    session.accept_handshake([0; 20], |_| true).await.unwrap();
    session
        .send_extended_handshake(&OutgoingExtendedHandshake {
            mi: Some(LivePosition {
                min_piece: 7,
                max_piece: 8,
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
        if matches!(message, PeerMessage::Unknown { id: 6, .. }) {
            break;
        }
    }
    if initial {
        // Preserve the actual one-second lone-window gate; send accepted chunks only afterward.
        tokio::time::sleep(Duration::from_millis(1100)).await;
    }
    if let Some(ready) = ready {
        let _ = ready.await;
    }
    for &(piece, chunk) in blocks {
        let signed = &pieces[(piece - 7) as usize];
        let half = signed.len() / 2;
        session
            .send(&build_piece(
                0,
                piece,
                chunk,
                [0; 8],
                &signed[chunk as usize * half..(chunk as usize + 1) * half],
            ))
            .await
            .unwrap();
    }
    if close {
        tokio::time::sleep(Duration::from_millis(100)).await;
    } else {
        while session.read_message().await.is_ok() {}
    }
}

async fn retained_signed_resume_credit_control(mode: ResumeCredit) {
    let _guard = warm_peers::test_guard().await;
    let auth = ace_wire::live_auth::LiveSourceAuth::generate();
    let pieces = Arc::new(
        [0xa7, 0xa8]
            .into_iter()
            .map(|marker| {
                let mut bytes = media_piece(marker);
                bytes.extend(auth.sign(&bytes));
                bytes
            })
            .collect::<Vec<_>>(),
    );
    let descriptor = StreamInfo {
        piece_length: pieces[0].len() as u64,
        chunk_length: pieces[0].len() as u64 / 2,
        sig_len: auth.signature_len(),
        source_pubkey: auth.pubkey_der(),
        ..info()
    };
    let a = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let c = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = |listener: &tokio::net::TcpListener| match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(addr) => addr,
        _ => unreachable!(),
    };
    let a_addr = address(&a);
    let b_addr = address(&b);
    let c_addr = address(&c);
    let (permit_gap, gap_ready) = tokio::sync::oneshot::channel();
    let mut servers = tokio::task::JoinSet::new();
    servers.spawn({
        let pieces = pieces.clone();
        async move {
            let (stream, _) = a.accept().await.unwrap();
            let blocks: &[(u32, u16)] = if mode == ResumeCredit::CompleteDuplicate {
                &[(8, 0), (8, 1)]
            } else {
                &[(7, 0)]
            };
            signed_resume_session(stream, pieces.clone(), blocks, true, true, None).await;
            if mode == ResumeCredit::SameProducer {
                let (stream, _) = a.accept().await.unwrap();
                signed_resume_session(stream, pieces, &[(7, 1)], false, false, None).await;
            }
        }
    });
    if mode != ResumeCredit::SameProducer {
        servers.spawn({
            let pieces = pieces.clone();
            async move {
                let (stream, _) = b.accept().await.unwrap();
                let blocks: &[(u32, u16)] = if mode == ResumeCredit::CompleteDuplicate {
                    &[(8, 0)]
                } else {
                    &[(7, 1)]
                };
                signed_resume_session(stream, pieces, blocks, false, false, None).await;
            }
        });
    }
    if mode == ResumeCredit::CompleteDuplicate {
        servers.spawn({
            let pieces = pieces.clone();
            async move {
                let (stream, _) = c.accept().await.unwrap();
                signed_resume_session(
                    stream,
                    pieces,
                    &[(7, 0), (7, 1)],
                    false,
                    false,
                    Some(gap_ready),
                )
                .await;
            }
        });
    } else {
        drop(gap_ready);
    }
    static SERIAL: AtomicU64 = AtomicU64::new(0);
    let cache_dir = std::env::temp_dir().join(format!(
        "outpace-resume-credit-{}-{}",
        std::process::id(),
        SERIAL.fetch_add(1, Ordering::Relaxed)
    ));
    let make_provider = || {
        AceProvider::new(Arc::new(Identity::generate()), 0)
            .with_warm_cache_loopback_policy(cache_dir.clone())
    };
    let first = make_provider();
    let seed = SeedConfig {
        registry: SeedRegistry::new(),
        store_bytes: 16384,
        store_retention: None,
        enabled: false,
        prefetch_pieces: 1,
        live_recovery: LiveRecoveryConfig {
            max_active_upstreams: 2,
            max_parallel_connect: 2,
            ..LiveRecoveryConfig::default()
        },
        cache_type: CacheType::Memory,
        cache_dir: PathBuf::new(),
        warm_peers: first.warm_peers.clone(),
    };
    let mut fixture = SignedResumePool {
        info: descriptor.clone(),
        store: Arc::new(tokio::sync::Mutex::new(PieceStore::new(
            descriptor.piece_length,
            descriptor.chunk_length,
            16384,
        ))),
        seed,
        continuity: None,
        candidates: SessionCandidates::default(),
    };
    fixture.candidates.learn(a_addr, CandidateKind::Source);
    let (tx, mut output) = mpsc::channel(16);
    let ended = tokio::time::timeout(Duration::from_secs(3), fixture.run(vec![a_addr], &tx))
        .await
        .unwrap();
    assert!(
        matches!(ended, FollowEnd::PeerLost(_)),
        "first pool must actually tear down"
    );
    let retained_piece = if mode == ResumeCredit::CompleteDuplicate {
        8
    } else {
        7
    };
    let continuity = fixture.continuity.as_mut().unwrap();
    assert_eq!(
        continuity.reasm.next_needed(),
        7,
        "reconnect must retain the earlier gap cursor"
    );
    assert_eq!(
        continuity.piece_producers.get(&retained_piece),
        Some(&Some(a_addr)),
        "actual initial signed chunks must have reached reassembly"
    );
    assert!(
        output.try_recv().is_err(),
        "partial/ahead data must not emit before its gap closes"
    );
    let (observed, mut contributions) = mpsc::unbounded_channel();
    continuity.contribution_observer = Some(observed);
    let replacement = if mode == ResumeCredit::SameProducer {
        a_addr
    } else {
        b_addr
    };
    fixture.candidates.learn(replacement, CandidateKind::Source);
    let mut replacements = vec![replacement];
    if mode == ResumeCredit::CompleteDuplicate {
        fixture.candidates.learn(c_addr, CandidateKind::Pex);
        replacements.push(c_addr);
    }
    let pool = tokio::spawn(async move {
        let end = fixture.run(replacements, &tx).await;
        (end, fixture)
    });
    if mode == ResumeCredit::CompleteDuplicate {
        tokio::time::timeout(Duration::from_secs(2), async {
            while let Some((piece, addr)) = contributions.recv().await {
                if piece == 8 && addr == b_addr {
                    break;
                }
            }
        })
        .await
        .unwrap();
        // This signal comes from the actual handler after B's completed-piece no-op, not its server write.
        permit_gap.send(()).unwrap();
    } else {
        drop(permit_gap);
    }
    let wanted = if mode == ResumeCredit::CompleteDuplicate {
        0xa8
    } else {
        0xa7
    };
    let emitted = tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(bytes) = output.recv().await {
            if bytes.bytes.contains(&wanted) {
                return true;
            }
        }
        false
    })
    .await;
    drop(output);
    let (end, fixture) = tokio::time::timeout(Duration::from_secs(1), pool)
        .await
        .unwrap()
        .unwrap();
    while let Some(server) = tokio::time::timeout(Duration::from_secs(1), servers.join_next())
        .await
        .unwrap()
    {
        server.unwrap();
    }
    let cursor = fixture.continuity.as_ref().unwrap().reasm.next_needed();
    assert!(first.warm_peers.flush().await);
    drop(fixture);
    drop(first);
    assert!(
        warm_peers::retired_terminal(&cache_dir).await,
        "prior cache writer must actually terminate before reconstruction"
    );
    let fresh = make_provider();
    fresh.warm_peers.initialized().await;
    let hints = fresh.warm_peers.hints(&descriptor.infohash);
    drop(fresh);
    assert!(warm_peers::retired_terminal(&cache_dir).await);
    std::fs::remove_dir_all(cache_dir).unwrap();
    assert!(matches!(end, FollowEnd::ConsumerGone));
    assert!(
        matches!(emitted, Ok(true)),
        "useful retained authenticated media was discarded or stranded"
    );
    assert!(
        cursor > retained_piece,
        "actual retained piece must become contiguous"
    );
    if mode == ResumeCredit::SameProducer {
        assert!(
            hints.contains(&(a_addr, CandidateKind::Source)),
            "legitimate single-producer signed reconnect lost durable credit"
        );
    } else {
        assert!(
            !hints.iter().any(|(addr, _)| *addr == b_addr),
            "replacement B acquired false durable source credit for retained bytes"
        );
    }
    if mode == ResumeCredit::CompleteDuplicate {
        assert!(
            hints.contains(&(a_addr, CandidateKind::Source)),
            "completed retained A provenance was lost to B's no-op"
        );
    }
}

#[tokio::test]
async fn retained_partial_signed_reconnect_does_not_credit_only_the_last_producer() {
    retained_signed_resume_credit_control(ResumeCredit::MixedPartial).await;
}
#[tokio::test]
async fn retained_complete_signed_reconnect_ignores_duplicate_producer_credit() {
    retained_signed_resume_credit_control(ResumeCredit::CompleteDuplicate).await;
}
#[tokio::test]
async fn retained_single_producer_signed_reconnect_preserves_media_and_durable_credit() {
    retained_signed_resume_credit_control(ResumeCredit::SameProducer).await;
}
