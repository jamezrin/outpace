use super::*;

#[tokio::test]
async fn queued_live_outputs_keep_each_discontinuity_with_its_chunk() {
    let (tx, rx) = mpsc::channel(8);
    for (bytes, discontinuity) in [
        (b"pre".as_slice(), false),
        (b"post-one".as_slice(), true),
        (b"between".as_slice(), false),
        (b"post-two".as_slice(), true),
    ] {
        tx.send(LiveOutput {
            bytes: Bytes::copy_from_slice(bytes),
            discontinuity,
        })
        .await
        .unwrap();
    }
    drop(tx);
    let mut source = AceSource {
        metrics: None,
        sampler: None,
        worker: None,
        rx,
        discontinuity: false,
        peers: Arc::new(AtomicU32::new(0)),
        downloaded: Arc::new(AtomicU64::new(0)),
        uploaded: Arc::new(AtomicU64::new(0)),
        peers_served: Arc::new(AtomicU32::new(0)),
        metadata: StreamMetadata::default(),
    };

    for (expected, gap) in [
        (b"pre".as_slice(), false),
        (b"post-one".as_slice(), true),
        (b"between".as_slice(), false),
        (b"post-two".as_slice(), true),
    ] {
        assert_eq!(source.next().await.unwrap().as_ref(), expected);
        assert_eq!(source.take_discontinuity(), gap);
    }
}

#[tokio::test]
async fn ace_source_shutdown_waits_for_owned_worker_cleanup() {
    owned_shutdown_case(false).await;
}

#[tokio::test]
async fn ace_source_shutdown_resumes_after_cancelled_waiter() {
    owned_shutdown_case(true).await;
}

async fn owned_shutdown_case(cancel_waiter: bool) {
    let (tx, rx) = mpsc::channel(1);
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let worker = tokio::spawn({
        let entered = entered.clone();
        let release = release.clone();
        async move {
            tx.closed().await;
            entered.notify_one();
            release.notified().await;
        }
    });
    let mut source = AceSource {
        metrics: None,
        sampler: None,
        worker: Some(worker),
        rx,
        discontinuity: false,
        peers: Arc::new(AtomicU32::new(0)),
        downloaded: Arc::new(AtomicU64::new(0)),
        uploaded: Arc::new(AtomicU64::new(0)),
        peers_served: Arc::new(AtomicU32::new(0)),
        metadata: StreamMetadata::default(),
    };
    let mut closing = Box::pin(source.shutdown());
    let pending = futures::poll!(closing.as_mut()).is_pending();
    let entered = tokio::time::timeout(Duration::from_millis(100), entered.notified())
        .await
        .is_ok();
    let joined = if pending && cancel_waiter {
        drop(closing);
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), source.shutdown())
            .await
            .is_ok()
    } else if pending {
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), closing)
            .await
            .is_ok()
    } else {
        drop(closing);
        true
    };
    let owned_join_released = source.worker.is_none();
    // Independent cleanup preserves the fixture even with the original no-op shutdown.
    if let Some(worker) = source.worker.take() {
        worker.abort();
        let _ = worker.await;
    }
    assert!(
        pending,
        "source shutdown returned before its owned worker cleanup"
    );
    assert!(
        entered,
        "source receiver was not closed to initiate worker cleanup"
    );
    assert!(
        joined && owned_join_released,
        "source did not reap its worker"
    );
}

#[tokio::test]
async fn manager_stop_joins_signed_ace_source_with_retained_media_handle() {
    use crate::provider::ProviderRegistry;
    let auth = ace_wire::live_auth::LiveSourceAuth::generate();
    let fixture = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/vectors/media/h264-keyframes.ts"
    ));
    let payload: Vec<u8> = fixture
        .iter()
        .copied()
        .cycle()
        .take(32768 - auth.signature_len())
        .collect();
    let mut wire = payload.clone();
    wire.extend(auth.sign(&payload));
    let piece_length = wire.len() as u64;
    let chunk_length = piece_length / 2;
    use ace_wire::bencode::Bencode;
    let fields = std::collections::BTreeMap::from([
        (b"name".to_vec(), Bencode::Bytes(b"synthetic".to_vec())),
        (b"piece_length".to_vec(), Bencode::Int(piece_length as i64)),
        (b"chunk_length".to_vec(), Bencode::Int(chunk_length as i64)),
        (b"bitrate".to_vec(), Bencode::Int(1_000_000)),
        (b"authmethod".to_vec(), Bencode::Bytes(b"RSA".to_vec())),
        (b"pubkey".to_vec(), Bencode::Bytes(auth.pubkey_der())),
    ]);
    let descriptor = ace_wire::transport::encode_transport(&Bencode::Dict(fields));
    let info = ace_swarm::resolve::stream_info_from_transport(&descriptor).unwrap();
    let descriptors =
        crate::local_descriptors::LocalDescriptors::from_test_bytes("fixture", &descriptor)
            .unwrap();
    let upstream_registry = SeedRegistry::new();
    let (store, lease) = upstream_registry.lease_store(info.infohash, || {
        ace_swarm::store::PieceStore::new(piece_length, chunk_length, piece_length * 4)
    });
    let header = ace_wire::live_codec::piece_header_from_unix_seconds(1_700_000_000.0);
    for piece in [7, 8] {
        ace_swarm::store::PieceStore::shared_put_complete_piece(&store, piece, header, &wire)
            .await
            .unwrap();
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(endpoint) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let admission = ace_swarm::listen::InboundPolicy {
        max_inbound: 1,
        max_per_stream: 1,
        max_unchoked: 1,
    }
    .admission();
    let mut server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        admission
            .accept(
                socket,
                upstream_registry,
                ace_wire::handshake::random_peer_id(),
                [0; 8],
                Arc::new(Identity::generate()),
                [127, 0, 0, 1],
            )
            .unwrap()
            .await
    });
    let local_registry = SeedRegistry::new();
    // Strict private endpoints keep this synthetic integration entirely loopback;
    // no catalog lookup, discovery, tracker announcement or port mapping is allowed.
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_peer_policy(
            ace_swarm::peer_policy::PeerPolicy::from_private_peers(Some(&endpoint.to_string()))
                .unwrap(),
        )
        .with_local_descriptors(descriptors)
        .with_seed_registry(local_registry.clone())
        .with_prefetch_pieces(Some(1))
        .with_startup_buffer(crate::config::StartupBufferConfig {
            target_ms: 0,
            ..Default::default()
        });
    let id = "local:fixture";
    let mut providers = ProviderRegistry::new();
    providers.register(Arc::new(provider));
    let manager = crate::manager::StreamManager::with_session_limit(providers, 1);
    let held = match manager.get_or_start("ace", id).await {
        Ok(held) => held,
        Err(error) => {
            server.abort();
            let _ = server.await;
            manager.shutdown().await;
            panic!("synthetic signed source open failed: {error}");
        }
    };
    let mut receiver = held.raw_receiver();
    let output = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match receiver.recv().await {
                Ok(crate::session::StreamEvent::Data(bytes)) => break bytes,
                Err(_) => break bytes::Bytes::new(),
                _ => {}
            }
        }
    })
    .await;
    let snapshot = local_registry.get(&info.infohash);
    // This listener and its admission are deliberately fixture-owned, not manager-owned.
    let inbound_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let inbound_endpoint = inbound_listener.local_addr().unwrap();
    let inbound_admission = ace_swarm::listen::InboundPolicy {
        max_inbound: 1,
        max_per_stream: 1,
        max_unchoked: 1,
    }
    .admission();
    let reader_registry = local_registry.clone();
    let mut inbound = tokio::spawn(async move {
        let (socket, _) = inbound_listener.accept().await?;
        inbound_admission
            .accept(
                socket,
                reader_registry,
                ace_wire::handshake::random_peer_id(),
                [0; 8],
                Arc::new(Identity::generate()),
                [127, 0, 0, 1],
            )
            .unwrap()
            .await
    });
    let admitted_reader = tokio::time::timeout(Duration::from_secs(2), async {
        let socket = tokio::net::TcpStream::connect(inbound_endpoint).await?;
        let mut reader = ace_peer::session::PeerSession::new(socket);
        reader
            .perform_handshake(info.infohash, ace_wire::handshake::random_peer_id())
            .await?;
        reader
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
        reader
            .send(&ace_wire::message::PeerMessage::Interested)
            .await?;
        while !matches!(
            reader.read_message().await?,
            ace_wire::message::PeerMessage::Unchoke
        ) {}
        Ok::<_, ace_peer::PeerError>(reader)
    })
    .await;
    let mut reader = admitted_reader.ok().and_then(Result::ok);
    let reader_admitted = reader.is_some();
    let producer_before = local_registry.serves(&info.infohash);
    let stopped = tokio::time::timeout(Duration::from_secs(2), manager.stop("ace", id)).await;
    let occupied_with_handle = manager.admission_status().0;
    let producer_after = local_registry.serves(&info.infohash);
    let terminal = matches!(
        held.raw_receiver().try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Closed)
    );
    let retained_read = tokio::time::timeout(Duration::from_secs(2), async {
        let Some(reader) = reader.as_mut() else {
            return Ok::<_, ace_peer::PeerError>(false);
        };
        let mut received = Vec::new();
        for chunk in [0, 1] {
            reader
                .send(&ace_wire::live_codec::chunk_request(7, chunk))
                .await?;
            loop {
                let message = reader.read_message().await?;
                if let Some(reply) = ace_wire::live_codec::LiveChunk::from_message(&message) {
                    if reply.piece != 7 || reply.chunk != chunk || reply.piece_header != header {
                        return Ok(false);
                    }
                    received.extend(reply.data);
                    break;
                }
            }
        }
        Ok(received == wire)
    })
    .await;
    drop(reader);
    let inbound_closed = tokio::time::timeout(Duration::from_secs(2), &mut inbound).await;
    if inbound_closed.is_err() {
        inbound.abort();
        let _ = inbound.await;
    }
    let source_closed = tokio::time::timeout(Duration::from_secs(2), &mut server).await;
    if source_closed.is_err() {
        server.abort();
        let _ = server.await;
    }
    let snapshot_retained = match snapshot {
        Some(store) => store.lock().await.window().is_some(),
        None => false,
    };
    drop((receiver, held, lease));
    let shutdown = manager.shutdown().await;
    assert!(output.is_ok_and(|bytes| !bytes.is_empty() && bytes.len().is_multiple_of(188)));
    assert!(
        producer_before && !producer_after && snapshot_retained,
        "source lease ends independently of retained verified-store snapshot"
    );
    assert!(matches!(stopped, Ok(true)) && shutdown);
    assert_eq!(
        occupied_with_handle, 0,
        "retained media handle must not retain the ACE resource"
    );
    assert!(terminal);
    assert!(reader_admitted && matches!(retained_read, Ok(Ok(true))), "independently admitted reader must retain its verified snapshot after producer removal: {retained_read:?}");
    assert!(
        matches!(inbound_closed, Ok(Ok(Err(ace_peer::PeerError::Closed)))),
        "fixture must independently close/join its admitted inbound reader: {inbound_closed:?}"
    );
    assert!(
        matches!(source_closed, Ok(Ok(Err(ace_peer::PeerError::Closed)))),
        "real signed source socket must terminate: {source_closed:?}"
    );
}

#[tokio::test]
async fn failed_follower_cannot_confirm_source_cleanup_or_release_capacity() {
    use crate::provider::{ProviderRegistry, StreamProvider};
    struct FailedFollowerProvider;
    #[async_trait]
    impl StreamProvider for FailedFollowerProvider {
        fn network(&self) -> &'static str {
            "follower-test"
        }
        async fn open(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderError> {
            let (tx, rx) = mpsc::channel(1);
            let worker = tokio::spawn(async move {
                let _tx = tx;
                panic!("synthetic follower failure");
            });
            Ok(Box::new(AceSource {
                metrics: None,
                sampler: None,
                worker: Some(worker),
                rx,
                discontinuity: false,
                peers: Arc::new(AtomicU32::new(0)),
                downloaded: Arc::new(AtomicU64::new(0)),
                uploaded: Arc::new(AtomicU64::new(0)),
                peers_served: Arc::new(AtomicU32::new(0)),
                metadata: StreamMetadata::default(),
            }))
        }
    }
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(FailedFollowerProvider));
    let manager = crate::manager::StreamManager::with_session_limit(registry, 1);
    let held = manager
        .get_or_start("follower-test", "synthetic")
        .await
        .unwrap();
    let stopped = manager.stop("follower-test", "synthetic").await;
    let occupied = manager.admission_status().0;
    let shutdown = manager.shutdown().await;
    drop(held);
    drop(manager);
    assert!(
        !stopped && !shutdown,
        "failed follower was mislabeled as confirmed cleanup"
    );
    assert_eq!(
        occupied, 1,
        "uncertain follower cleanup freed the admitted source"
    );
}
