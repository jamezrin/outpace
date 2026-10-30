use super::*;
use tokio::io::AsyncReadExt;

// Each pool has its own observer; this never replaces or blocks the real send.
tokio::task_local! {
    pub(crate) static OUTPUT_SEND_PENDING: Arc<tokio::sync::Notify>;
}

async fn pair() -> (ConnectedUpstream, PeerSession<TcpStream>, TcpStream) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    let client = TcpStream::connect(addr).await.unwrap();
    let (remote, _) = listener.accept().await.unwrap();
    let remote = remote.into_std().unwrap();
    let raw = TcpStream::from_std(remote.try_clone().unwrap()).unwrap();
    (
        ConnectedUpstream {
            session: PeerSession::new(client),
            addr,
            window: live_pos(7, 8),
            unchoked: true,
            yourip: None,
        },
        PeerSession::new(TcpStream::from_std(remote).unwrap()),
        raw,
    )
}

fn run(
    upstream: ConnectedUpstream,
    healthy: ConnectedUpstream,
    pending: Arc<tokio::sync::Notify>,
    prefill: bool,
    info: StreamInfo,
    store: Arc<tokio::sync::Mutex<PieceStore>>,
    continuity: Continuity,
) -> (
    mpsc::Receiver<LiveOutput>,
    tokio::task::JoinHandle<Continuity>,
) {
    let (tx, rx) = mpsc::channel(if prefill { 1 } else { 16 });
    if prefill {
        tx.try_send(LiveOutput {
            bytes: bytes::Bytes::from_static(b"output-marker"),
            discontinuity: false,
        })
        .unwrap();
    }
    let task = tokio::spawn(OUTPUT_SEND_PENDING.scope(pending, async move {
        let seed = SeedConfig {
            metrics: None,
            registry: SeedRegistry::new(),
            store_factory: test_store_factory(16384, None),
            enabled: true,
            prefetch_pieces: 1,
            live_recovery: continuity.live_recovery,
            warm_peers: WarmPeerCache::memory(),
        };
        let mut continuity = Some(continuity);
        follow_peer_pool(
            vec![upstream, healthy],
            &info,
            &Identity::generate(),
            2,
            &tx,
            &Arc::new(AtomicU64::new(0)),
            &Arc::new(AtomicU64::new(0)),
            &Arc::new(AtomicU32::new(0)),
            &seed,
            &store,
            &mut continuity,
            vec![],
            vec![],
            completed_discovery(|_| Box::pin(async { vec![] })),
            &mut SessionCandidates::default(),
            &Arc::new(AtomicU32::new(0)),
            None,
        )
        .await;
        continuity.unwrap()
    }));
    (rx, task)
}

async fn unchoke(remote: &mut PeerSession<TcpStream>) -> bool {
    tokio::time::timeout(Duration::from_millis(250), async {
        loop {
            match remote.read_message().await {
                Ok(PeerMessage::Unchoke) => return true,
                Ok(_) => {}
                Err(_) => return false,
            }
        }
    })
    .await
    .unwrap_or(false)
}

#[tokio::test]
async fn pending_output_allows_healthy_control() {
    diagnostic(0).await;
}
#[tokio::test]
async fn pending_output_consumer_close_cancels_pool() {
    diagnostic(1).await;
}
#[tokio::test]
async fn unblocked_output_preserves_order_and_control() {
    diagnostic(2).await;
}

async fn diagnostic(mode: u8) {
    let dir = tempfile::tempdir().unwrap();
    let auth = ace_wire::live_auth::LiveSourceAuth::generate();
    let payloads: Vec<Vec<u8>> = (0..2)
        .map(|piece| {
            (0..4)
                .flat_map(|cc| {
                    let mut p = vec![0x55 + piece; 188];
                    p[..4].copy_from_slice(&[0x47, 1, 0, 0x10 | (piece * 4 + cc)]);
                    p
                })
                .collect()
        })
        .collect();
    let wires: Vec<Vec<u8>> = payloads
        .iter()
        .map(|p| {
            let mut w = p.clone();
            w.extend(auth.sign(p));
            w
        })
        .collect();
    let info = StreamInfo {
        infohash: [0; 20],
        piece_length: wires[0].len() as u64,
        chunk_length: (wires[0].len() / 2) as u64,
        trackers: vec![],
        metadata: StreamMetadata::default(),
        sig_len: auth.signature_len(),
        source_pubkey: auth.pubkey_der(),
    };
    let policy = LiveRecoveryConfig {
        max_active_upstreams: 2,
        max_piece_advance: 2,
        request_timeout_ms: 5000,
        ..default_live_recovery()
    };
    let (continuity, _) = Continuity::fresh(&info, 7, 8, 1, policy);

    let store = Arc::new(tokio::sync::Mutex::new(
        PieceStore::with_backend(
            info.piece_length,
            info.chunk_length,
            16384,
            crate::store::BackendKind::Disk {
                dir: dir.path().join("cache"),
            },
        )
        .unwrap(),
    ));
    let (upstream, mut source, mut raw_source) = pair().await;
    let (healthy, mut remote, mut raw_healthy) = pair().await;
    let pending = Arc::new(tokio::sync::Notify::new());
    let (mut rx, mut task) = run(
        upstream,
        healthy,
        pending.clone(),
        mode != 2,
        info,
        store.clone(),
        continuity,
    );
    let mut fixture = true;
    for piece in [7, 8] {
        for chunk in [0, 1] {
            let begin = chunk * wires[0].len() / 2;
            fixture &= tokio::time::timeout(
                Duration::from_secs(2),
                source.send(&build_piece(
                    0,
                    piece,
                    chunk as u16,
                    [piece as u8; 8],
                    &wires[(piece - 7) as usize][begin..begin + wires[0].len() / 2],
                )),
            )
            .await
            .is_ok_and(|r| r.is_ok());
        }
    }
    let witnessed = if mode != 2 {
        tokio::time::timeout(Duration::from_secs(2), pending.notified())
            .await
            .is_ok()
    } else {
        false
    };
    let mut control = false;
    let mut joined = None;
    if mode == 1 {
        rx.close();
        joined = tokio::time::timeout(Duration::from_millis(250), &mut task)
            .await
            .ok();
    } else {
        fixture &= remote.send(&PeerMessage::Interested).await.is_ok();
        control = unchoke(&mut remote).await;
    }
    // Release before any diagnostic assertion, including the intentional RED.
    let mut marker = mode == 2;
    let mut output = Vec::new();
    if mode != 1 {
        if mode == 0 {
            marker = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .ok()
                .flatten()
                .is_some_and(|v| v.bytes.as_ref() == b"output-marker");
        }
        let drained = tokio::time::timeout(Duration::from_secs(2), async {
            while output.len() < 7 * 188 {
                match rx.recv().await {
                    Some(v) => output.extend_from_slice(&v.bytes),
                    None => break,
                }
            }
        })
        .await
        .is_ok();
        fixture &= drained;
        if !control {
            fixture &= unchoke(&mut remote).await;
        }
    }
    rx.close();
    let cleanup = if let Some(result) = joined.as_ref() {
        result.is_ok()
    } else {
        match tokio::time::timeout(Duration::from_secs(2), &mut task).await {
            Ok(result) => result.is_ok(),
            Err(_) => {
                task.abort();
                let _ = task.await;
                false
            }
        }
    };
    drop(source);
    drop(remote);
    // Ignore parser state: only a successful zero-byte raw TCP read proves EOF.
    let eof = tokio::time::timeout(Duration::from_secs(2), async {
        let mut buf = [0; 4096];
        for socket in [&mut raw_source, &mut raw_healthy] {
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(_) => return false,
                }
            }
        }
        true
    })
    .await
    .unwrap_or(false);
    let stored = store.lock().await;
    let verified = stored.has_piece(7) && (mode == 1 || stored.has_piece(8));
    drop(stored);
    assert!(
        fixture && (mode == 2 || witnessed) && cleanup && eof && verified,
        "fixture and cleanup"
    );
    if mode != 1 {
        assert!(marker, "prefill accounted separately");
        assert_eq!(
            output,
            payloads.concat()[..7 * 188],
            "exact ordered media; final TS packet retained by resync"
        );
    }
    assert!(
        if mode == 1 { joined.is_some() } else { control },
        "desired progress during actual output-send Pending (or unblocked control)"
    );
}
