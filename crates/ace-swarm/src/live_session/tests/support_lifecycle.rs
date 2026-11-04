//! Finite source/support/leecher composition, not an autonomous service implementation.
use super::*;
use crate::listen::InboundPolicy;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

async fn admitted_peer(registry: SeedRegistry) -> (SocketAddrV4, JoinHandle<ace_peer::Result<()>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let admission = InboundPolicy {
        max_inbound: 1,
        max_per_stream: 1,
        max_unchoked: 1,
    }
    .admission();
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        admission
            .accept(
                socket,
                registry,
                random_peer_id(),
                [0; 8],
                Arc::new(Identity::generate()),
                [127, 0, 0, 1],
            )
            .expect("one connection fits the fixture admission")
            .await
    });
    (addr, task)
}

async fn join_or_reap<T: Send + 'static>(task: &mut JoinHandle<T>) -> Option<T> {
    match tokio::time::timeout(Duration::from_secs(2), &mut *task).await {
        Ok(result) => result.ok(),
        Err(_) => {
            task.abort();
            let _ = task.await;
            None
        }
    }
}

#[tokio::test]
async fn signed_support_reseeds_without_playback_and_releases_its_producer() {
    signed_support_lifecycle(false).await;
}

#[tokio::test]
async fn private_follower_uses_allowlist_without_public_discovery_or_warm_cache() {
    signed_support_lifecycle(true).await;
}

async fn signed_support_lifecycle(private: bool) {
    let auth = ace_wire::live_auth::LiveSourceAuth::generate();
    let payload: Vec<u8> = (0..4)
        .flat_map(|cc| {
            let mut packet = vec![0x55; 188];
            packet[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10 | cc]);
            packet
        })
        .collect();
    let mut wire = payload.clone();
    wire.extend(auth.sign(&payload));
    let piece_length = wire.len() as u64;
    let chunk_length = piece_length / 2;
    let header = ace_wire::live_codec::piece_header_from_unix_seconds(1_700_000_000.0);
    let source_registry = SeedRegistry::new();
    let (source_store, source_lease) = source_registry.lease_store([0; 20], || {
        PieceStore::new(piece_length, chunk_length, piece_length * 4)
    });
    for piece in [7, 8] {
        PieceStore::shared_put_complete_piece(&source_store, piece, header, &wire)
            .await
            .unwrap();
    }
    let (source_addr, mut source) = admitted_peer(source_registry.clone()).await;
    let support_registry = SeedRegistry::new();
    let (support_addr, mut inbound) = admitted_peer(support_registry.clone()).await;
    let info = StreamInfo {
        protection: None,
        infohash: [0; 20],
        piece_length,
        chunk_length,
        trackers: vec![],
        metadata: StreamMetadata::default(),
        sig_len: auth.signature_len(),
        source_pubkey: auth.pubkey_der(),
    };
    let warm = WarmPeerCache::new(std::path::Path::new(""), true);
    warm.record_productive(
        [0; 20],
        "127.0.0.1:1".parse().unwrap(),
        CandidateKind::Discovered,
    );
    let initial_hints = warm.hints(&[0; 20]);
    let discovery_calls = Arc::new(AtomicU32::new(0));
    let calls = discovery_calls.clone();
    let seed = SeedConfig {
        peer_policy: if private {
            crate::peer_policy::PeerPolicy::from_private_peers(Some(&source_addr.to_string()))
                .unwrap()
        } else {
            Default::default()
        },
        metrics: None,
        registry: support_registry.clone(),
        store_factory: Arc::new(move |_| {
            PieceStore::new(piece_length, chunk_length, piece_length * 4)
        }),
        enabled: true,
        prefetch_pieces: 1,
        live_recovery: LiveRecoveryConfig {
            max_active_upstreams: 1,
            max_parallel_connect: 1,
            max_piece_advance: 1,
            request_timeout_ms: 1_000,
            request_check_interval_ms: 10,
            stale_upstream_timeout_ms: 30_000,
            ..default_live_recovery()
        },
        warm_peers: warm.clone(),
    };
    // The service owns this bounded receiver. No player or drain task consumes it.
    let (tx, rx) = mpsc::channel(1);
    let peers = Arc::new(AtomicU32::new(0));
    let downloaded = Arc::new(AtomicU64::new(0));
    let mut follower = tokio::spawn(follow_live(LiveSessionInputs {
        info,
        peers: if private { vec![] } else { vec![source_addr] },
        identity: Arc::new(Identity::generate()),
        output: tx,
        counters: LiveCounters {
            peers: peers.clone(),
            downloaded: downloaded.clone(),
            uploaded: Arc::new(AtomicU64::new(0)),
            peers_served: Arc::new(AtomicU32::new(0)),
        },
        seed,
        discovery: completed_discovery(move |_| {
            calls.fetch_add(1, Ordering::Relaxed);
            async move { vec![source_addr] }
        }),
        reachability: None,
        initial_run: None,
    }));
    let observation = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Some(store) = support_registry.get(&[0; 20]) {
                if store
                    .lock()
                    .await
                    .window()
                    .is_some_and(|(min, max)| min <= 7 && max >= 7)
                {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        while rx.is_empty() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let mut leecher = PeerSession::new(TcpStream::connect(support_addr).await?);
        leecher.perform_handshake([0; 20], random_peer_id()).await?;
        leecher
            .send_signed_extended_handshake(
                &OutgoingExtendedHandshake {
                    ace_metadata_version: 1,
                    ut_metadata_id: 2,
                    mi: None,
                    node: NodeFields::default(),
                    peer_ip: None,
                    metadata_size: None,
                },
                &Identity::generate(),
            )
            .await?;
        leecher.send(&PeerMessage::Interested).await?;
        while !matches!(leecher.read_message().await?, PeerMessage::Unchoke) {}
        let mut received = Vec::new();
        for chunk in [0, 1] {
            leecher.send(&chunk_request(7, chunk)).await?;
            loop {
                let message = leecher.read_message().await?;
                if let Some(reply) = LiveChunk::from_message(&message) {
                    if reply.piece != 7 || reply.chunk != chunk || reply.piece_header != header {
                        return Ok::<_, ace_peer::PeerError>(false);
                    }
                    received.extend(reply.data);
                    break;
                }
            }
        }
        Ok(received == wire
            && ace_wire::live_auth::verify_piece(
                &auth.pubkey_der(),
                &received[..payload.len()],
                &received[payload.len()..],
            ))
    })
    .await;
    // Always release the consumer and join all owned tasks before reporting an observation failure.
    let follower_running = !follower.is_finished();
    let producer_registered = support_registry.serves(&[0; 20]);
    drop(rx);
    let follower_joined = join_or_reap(&mut follower).await;
    let inbound_joined = join_or_reap(&mut inbound).await;
    let source_joined = join_or_reap(&mut source).await;
    let support_removed = !support_registry.serves(&[0; 20]);
    drop(source_lease);
    assert!(
        follower_running && producer_registered,
        "service must still own the producer before receiver close"
    );
    assert!(
        follower_joined.is_some(),
        "receiver close must join the real follower"
    );
    assert!(
        matches!(inbound_joined, Some(Err(ace_peer::PeerError::Closed)))
            && matches!(source_joined, Some(Err(ace_peer::PeerError::Closed))),
        "both admitted peers must close cleanly: inbound={inbound_joined:?}, source={source_joined:?}"
    );
    assert!(support_removed, "follower must release its producer lease");
    assert!(!source_registry.serves(&[0; 20]));
    assert_eq!(peers.load(Ordering::Relaxed), 0);
    if private {
        assert_eq!(
            discovery_calls.load(Ordering::Relaxed),
            0,
            "private follower invoked public discovery factory"
        );
        assert!(
            warm.hints(&[0; 20]) == initial_hints,
            "private output persisted productive peer hints"
        );
    }
    // Playback-output accounting is not a wire-acquisition counter. Exact reseeding below
    // proves acquisition independently of the undrained playback receiver.
    assert!(
        matches!(observation, Ok(Ok(true))),
        "signed reseeding: {observation:?}"
    );
}
