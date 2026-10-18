use super::*;

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
                        mi: Some(WireLivePosition {
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
                mi: Some(WireLivePosition {
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
    let make_cache = || WarmPeerCache::new(&cache_dir, true);
    let first = make_cache();
    let mut source = open_cached_session(
        first.clone(),
        descriptor.clone(),
        completed_discovery(move |_| async move { vec![relay_addr] }),
    )
    .await;
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
            first.hints(&[0; 20]) == vec![(source_addr, CandidateKind::Source)],
            "credit belongs to the complete authenticated source, not its announcing relay"
        );
    } else {
        assert!(
            first.hints(&[0; 20]).is_empty(),
            "unverified or mixed producer acquired productive cache credit"
        );
    }
    let before = attempts.load(Ordering::Relaxed);
    assert!(before > 0);
    assert!(
        first.flush().await,
        "normal productive cache revision must actually persist"
    );
    drop(source);
    drop(first);
    assert!(
        warm_peers::retired_terminal(&cache_dir).await,
        "old cache owner/writer must be terminal before fresh reconstruction"
    );
    let fresh = make_cache();
    fresh.initialized().await;
    if mode != CacheProducer::Authenticated {
        assert!(
            fresh.hints(&[0; 20]).is_empty(),
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
    let open = tokio::spawn(async move { open_cached_session(fresh, descriptor, slow).await });
    let warm_attempt = tokio::time::timeout(Duration::from_secs(2), async {
        while attempts.load(Ordering::Relaxed) == before {
            tokio::task::yield_now().await;
        }
    })
    .await;
    open.abort();
    if let Ok(source) = open.await {
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

#[tokio::test]
async fn stale_signed_piece_never_creates_durable_peer_credit() {
    durable_provider_cache_control(CacheProducer::Stale).await;
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
            mi: Some(WireLivePosition {
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
    let make_cache = || WarmPeerCache::new(&cache_dir, true);
    let first = make_cache();
    let seed = SeedConfig {
        metrics: None,
        registry: SeedRegistry::new(),
        store_factory: test_store_factory(16384, None),
        enabled: false,
        prefetch_pieces: 1,
        live_recovery: LiveRecoveryConfig {
            max_active_upstreams: 2,
            max_parallel_connect: 2,
            ..LiveRecoveryConfig::default()
        },

        warm_peers: first.clone(),
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
    assert!(first.flush().await);
    drop(fixture);
    drop(first);
    assert!(
        warm_peers::retired_terminal(&cache_dir).await,
        "prior cache writer must actually terminate before reconstruction"
    );
    let fresh = make_cache();
    fresh.initialized().await;
    let hints = fresh.hints(&descriptor.infohash);
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

fn media_piece(marker: u8) -> Vec<u8> {
    (0..4)
        .flat_map(|cc| {
            let mut packet = vec![marker; 188];
            packet[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10 | cc]);
            packet
        })
        .collect()
}
struct CachedSource {
    rx: mpsc::Receiver<LiveOutput>,
    _task: OwnedTask,
}
impl CachedSource {
    async fn next(&mut self) -> Option<Bytes> {
        self.rx.recv().await.map(|output| output.bytes)
    }
}
async fn open_cached_session(
    cache: WarmPeerCache,
    info: StreamInfo,
    discovery: PeerDiscovery,
) -> CachedSource {
    let mut run = DiscoveryRun::start(&discovery, crate::discover::DiscoveryOptions::default());
    cache.initialized().await;
    let mut peers = cache.candidate_addresses(&info.infohash);
    if peers.is_empty() {
        peers.extend(run.next_candidate().await);
    }
    while let Ok(peer) = run.try_next_candidate() {
        peers.push(peer);
    }
    let piece_length = info.piece_length;
    let chunk_length = info.chunk_length;
    let seed = LiveSeedOptions {
        metrics: None,
        registry: SeedRegistry::new(),
        store_factory: Arc::new(move |_| {
            PieceStore::new(piece_length, chunk_length, 128 * 1024 * 1024)
                .with_retention(Duration::from_secs(45))
        }),
        enabled: true,
        prefetch_pieces: 1,
        live_recovery: LiveRecoveryOptions::default(),
        warm_peers: cache,
    };
    let (output, rx) = mpsc::channel(256);
    let task = tokio::spawn(follow_live(LiveSessionInputs {
        info,
        peers,
        identity: Arc::new(Identity::generate()),
        output,
        counters: LiveCounters {
            peers: Arc::new(AtomicU32::new(0)),
            downloaded: Arc::new(AtomicU64::new(0)),
            uploaded: Arc::new(AtomicU64::new(0)),
            peers_served: Arc::new(AtomicU32::new(0)),
        },
        seed,
        discovery,
        reachability: None,
        initial_run: Some(run),
    }));
    CachedSource {
        rx,
        _task: OwnedTask(task),
    }
}
