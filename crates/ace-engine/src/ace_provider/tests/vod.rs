use super::*;

#[tokio::test]
async fn resolve_vod_rejects_bare_infohash() {
    // A bare infohash carries no transport descriptor, so it has no VOD piece hashes.
    // This resolves synchronously (no network) to a clear error.
    let p = AceProvider::new(Arc::new(Identity::generate()), 6878);
    let err = p
        .resolve_vod("00112233445566778899aabbccddeeff00112233")
        .await
        .err();
    assert!(err.is_some(), "a bare infohash is not a VOD target");
}

#[tokio::test]
async fn vod_source_trims_received_pieces_to_the_requested_byte_range() {
    // Three whole "pieces" of 4 bytes arrive in order. The source is opened for a range that
    // starts 2 bytes into the first covering piece and spans 5 bytes total, so it must drop
    // the leading 2 bytes and stop after 5 emitted bytes — never leaking bytes outside the
    // range even though the covering pieces carry more.
    let (tx, rx) = mpsc::channel::<Bytes>(4);
    tx.send(Bytes::from_static(b"AAAA")).await.unwrap();
    tx.send(Bytes::from_static(b"BBBB")).await.unwrap();
    tx.send(Bytes::from_static(b"CCCC")).await.unwrap();
    drop(tx);
    let mut src = VodSource {
        prefix: VecDeque::new(),
        rx,
        skip: 2,
        remaining: 5,
        emit_len: 5,
    };
    assert_eq!(src.content_length(), 5);
    let mut got = Vec::new();
    while let Some(chunk) = src.next().await {
        got.extend_from_slice(&chunk);
    }
    assert_eq!(&got, b"AABBB");
}

#[tokio::test]
async fn vod_source_emits_cached_prefix_before_downloaded_suffix() {
    // First covering piece "AAAA" is cached (prefix); the suffix "BBBB","CCCC" streams in.
    // The range starts 2 bytes into the first piece and spans 5 bytes, so trimming must apply
    // uniformly across the prefix/stream boundary: "AA" + "BBB".
    let (tx, rx) = mpsc::channel::<Bytes>(4);
    tx.send(Bytes::from_static(b"BBBB")).await.unwrap();
    tx.send(Bytes::from_static(b"CCCC")).await.unwrap();
    drop(tx);
    let mut src = VodSource {
        prefix: VecDeque::from(vec![Bytes::from_static(b"AAAA")]),
        rx,
        skip: 2,
        remaining: 5,
        emit_len: 5,
    };
    let mut got = Vec::new();
    while let Some(chunk) = src.next().await {
        got.extend_from_slice(&chunk);
    }
    assert_eq!(&got, b"AABBB");
}

// Build `total_len` bytes plus a matching VodInfo (SHA-1 piece hashes over that content).
fn make_vod_content(piece_length: u64, chunk_length: u64, total_len: u64) -> (Vec<u8>, VodInfo) {
    use sha1::{Digest, Sha1};
    let content: Vec<u8> = (0..total_len).map(|i| (i % 251) as u8).collect();
    let piece_count = total_len.div_ceil(piece_length);
    let mut piece_hashes = Vec::new();
    for p in 0..piece_count {
        let start = (p * piece_length) as usize;
        let end = ((p + 1) * piece_length).min(total_len) as usize;
        let h: [u8; 20] = Sha1::digest(&content[start..end]).into();
        piece_hashes.push(h);
    }
    let info = VodInfo {
        infohash: [0x42; 20],
        peer_protocol: Default::default(),
        piece_length,
        chunk_length,
        trackers: vec![],
        piece_hashes,
        total_length: total_len,
    };
    (content, info)
}

// A minimal standard-BitTorrent VOD seeder that tallies how many block bytes it serves, so a
// test can assert a piece was fetched from the swarm at most once.
async fn spawn_counting_vod_seeder(
    content: Vec<u8>,
    info: VodInfo,
    served: Arc<AtomicU64>,
) -> SocketAddrV4 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            let content = content.clone();
            let info = info.clone();
            let served = served.clone();
            tokio::spawn(async move {
                let mut hs = [0u8; ace_wire::handshake::HANDSHAKE_LEN];
                if sock.read_exact(&mut hs).await.is_err() {
                    return;
                }
                let reply =
                    ace_wire::handshake::Handshake::new(info.infohash, random_peer_id()).encode();
                if sock.write_all(&reply).await.is_err() {
                    return;
                }
                let nbytes = (info.piece_count() as usize).div_ceil(8);
                let mut bits = vec![0u8; nbytes];
                for p in 0..info.piece_count() as usize {
                    bits[p / 8] |= 0x80 >> (p % 8);
                }
                let _ = sock.write_all(&PeerMessage::Bitfield(bits).encode()).await;
                let _ = sock.write_all(&PeerMessage::Unchoke.encode()).await;
                let mut buf: Vec<u8> = Vec::new();
                let mut tmp = [0u8; 4096];
                loop {
                    loop {
                        match PeerMessage::decode(&buf) {
                            Ok(Some((msg, used))) => {
                                buf.drain(..used);
                                if let PeerMessage::Request {
                                    index,
                                    begin,
                                    length,
                                } = msg
                                {
                                    let start =
                                        (index as u64 * info.piece_length + begin as u64) as usize;
                                    let end = start + length as usize;
                                    let block = content[start..end].to_vec();
                                    served.fetch_add(block.len() as u64, Ordering::SeqCst);
                                    let piece = PeerMessage::Piece {
                                        index,
                                        begin,
                                        block,
                                    }
                                    .encode();
                                    if sock.write_all(&piece).await.is_err() {
                                        return;
                                    }
                                }
                            }
                            Ok(None) => break,
                            Err(_) => return,
                        }
                    }
                    let n = match sock.read(&mut tmp).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    buf.extend_from_slice(&tmp[..n]);
                }
            });
        }
    });
    addr
}

async fn read_all(mut src: Box<dyn VodByteSource>) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(chunk) = src.next().await {
        out.extend_from_slice(&chunk);
    }
    out
}

#[tokio::test]
async fn open_range_downloads_each_covering_piece_at_most_once() {
    // 3 pieces of 16 bytes (one 16-byte block per piece). Two reads that both fall inside
    // piece 0: the second must be served from the cache the first populated, not re-downloaded.
    let (content, info) = make_vod_content(16, 16, 48);
    assert_eq!(info.piece_count(), 3);
    let served = Arc::new(AtomicU64::new(0));
    let addr = spawn_counting_vod_seeder(content.clone(), info.clone(), served.clone()).await;
    let registry = SeedRegistry::new();
    let store = registry.lease_store(info.infohash, || PieceStore::new(16, 16, 128));
    let vod = AceVodContent {
        info,
        bootstrap_peers: vec![addr],
        announce_peer_port: tokio::sync::watch::channel(None).1,
        store,
        peers: Arc::new(tokio::sync::Mutex::new(None)),
        range_lock: Arc::new(tokio::sync::Mutex::new(())),
    };

    let first = read_all(vod.open_range(0, 3).await.unwrap()).await;
    assert_eq!(first, content[0..=3]);
    let second = read_all(vod.open_range(8, 11).await.unwrap()).await;
    assert_eq!(second, content[8..=11]);

    // Piece 0 is 16 bytes; both reads cover it, but the swarm served it exactly once.
    assert_eq!(
        served.load(Ordering::SeqCst),
        16,
        "the covering piece is downloaded once, then reused from cache"
    );
}

#[tokio::test]
async fn open_range_spanning_a_new_piece_downloads_only_the_missing_suffix() {
    // Read piece 0 first (caches it), then read [0, 20] which spans pieces 0 and 1: piece 0 is
    // served from cache, only piece 1 is fetched. The assembled bytes must still be correct.
    let (content, info) = make_vod_content(16, 16, 48);
    let served = Arc::new(AtomicU64::new(0));
    let addr = spawn_counting_vod_seeder(content.clone(), info.clone(), served.clone()).await;
    let registry = SeedRegistry::new();
    let store = registry.lease_store(info.infohash, || PieceStore::new(16, 16, 128));
    let vod = AceVodContent {
        info,
        bootstrap_peers: vec![addr],
        announce_peer_port: tokio::sync::watch::channel(None).1,
        store,
        peers: Arc::new(tokio::sync::Mutex::new(None)),
        range_lock: Arc::new(tokio::sync::Mutex::new(())),
    };

    let _ = read_all(vod.open_range(0, 15).await.unwrap()).await; // piece 0
    let spanning = read_all(vod.open_range(0, 20).await.unwrap()).await;
    assert_eq!(spanning, content[0..=20]);
    // Piece 0 (16) downloaded once for the first read; piece 1 (16) for the spanning read.
    assert_eq!(served.load(Ordering::SeqCst), 32);
}

#[tokio::test]
async fn downloaded_vod_piece_is_served_to_an_inbound_bittorrent_peer() {
    let (content, info) = make_vod_content(16, 8, 32);
    let upstream =
        spawn_counting_vod_seeder(content.clone(), info.clone(), Arc::new(AtomicU64::new(0))).await;
    let registry = SeedRegistry::new();
    let store = registry.lease_store(info.infohash, || PieceStore::new(16, 8, 128));
    let vod = AceVodContent {
        info: info.clone(),
        bootstrap_peers: vec![upstream],
        announce_peer_port: tokio::sync::watch::channel(None).1,
        store,
        peers: Arc::new(tokio::sync::Mutex::new(None)),
        range_lock: Arc::new(tokio::sync::Mutex::new(())),
    };
    assert_eq!(
        read_all(vod.open_range(0, 15).await.unwrap()).await,
        content[..16]
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(ace_swarm::listen::PeerListener::serve(
        listener,
        registry,
        random_peer_id(),
        [0; 8],
        8,
        Arc::new(Identity::generate()),
        8,
        None,
    ));
    let mut peer = PeerSession::new(TcpStream::connect(addr).await.unwrap());
    peer.perform_handshake(info.infohash, random_peer_id())
        .await
        .unwrap();
    peer.send(&PeerMessage::Interested).await.unwrap();
    loop {
        if matches!(peer.read_message().await.unwrap(), PeerMessage::Unchoke) {
            break;
        }
    }
    peer.send(&PeerMessage::Request {
        index: 0,
        begin: 4,
        length: 8,
    })
    .await
    .unwrap();
    loop {
        if let PeerMessage::Piece {
            index,
            begin,
            block,
        } = peer.read_message().await.unwrap()
        {
            assert_eq!((index, begin), (0, 4));
            assert_eq!(block, content[4..12]);
            break;
        }
    }
}

#[tokio::test]
async fn cancelled_range_finishes_background_transaction_before_overlap_downloads() {
    let (content, info) = make_vod_content(16, 16, 32);
    let served = Arc::new(AtomicU64::new(0));
    let upstream = spawn_counting_vod_seeder(content.clone(), info.clone(), served.clone()).await;
    let registry = SeedRegistry::new();
    let store = registry.lease_store(info.infohash, || PieceStore::new(16, 16, 128));
    let range_lock = Arc::new(tokio::sync::Mutex::new(()));
    let vod = AceVodContent {
        info,
        bootstrap_peers: vec![upstream],
        announce_peer_port: tokio::sync::watch::channel(None).1,
        store,
        peers: Arc::new(tokio::sync::Mutex::new(None)),
        range_lock: range_lock.clone(),
    };

    let cancelled = vod.open_range(0, 15).await.unwrap();
    drop(cancelled);
    // The supervisor owns the lock until its downloader observes cancellation and exits.
    let guard = tokio::time::timeout(Duration::from_secs(1), range_lock.clone().lock_owned())
        .await
        .expect("cancelled background transaction shuts down");
    drop(guard);

    assert_eq!(
        read_all(vod.open_range(0, 15).await.unwrap()).await,
        content[..16]
    );
    assert_eq!(
        served.load(Ordering::SeqCst),
        16,
        "cancelled overlap cannot race a duplicate piece download/write"
    );
}

struct ScriptedRequest {
    index: u32,
    begin: u32,
    length: u32,
    reply: tokio::sync::oneshot::Sender<Vec<u8>>,
}

struct ScriptedSeeder {
    addr: SocketAddrV4,
    requests: mpsc::Receiver<ScriptedRequest>,
    shutdown: tokio::sync::oneshot::Sender<()>,
    join: tokio::task::JoinHandle<()>,
    closed: Arc<AtomicU64>,
}

impl ScriptedSeeder {
    async fn stop(self) {
        let _ = self.shutdown.send(());
        self.join.await.unwrap();
    }
}

// A test seeder whose request replies are controlled by the test. The listener owns all
// connection tasks and aborts/joins them on shutdown; client cancellation is observable through
// `closed` while an unanswered request is pending.
async fn spawn_scripted_vod_seeder(info: VodInfo) -> ScriptedSeeder {
    use ace_wire::handshake::Handshake;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::task::JoinSet;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(addr) => addr,
        _ => unreachable!(),
    };
    let (request_tx, requests) = mpsc::channel(32);
    let (shutdown, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let closed = Arc::new(AtomicU64::new(0));
    let closed_for_task = closed.clone();
    let join = tokio::spawn(async move {
        let mut sessions = JoinSet::new();
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                accepted = listener.accept() => {
                    let Ok((mut socket, _)) = accepted else { break };
                    let info = info.clone();
                    let request_tx = request_tx.clone();
                    let closed = closed_for_task.clone();
                    sessions.spawn(async move {
                        let mut handshake = [0; ace_wire::handshake::HANDSHAKE_LEN];
                        if socket.read_exact(&mut handshake).await.is_err() {
                            return;
                        }
                        let response = Handshake::new(info.infohash, random_peer_id()).encode();
                        if socket.write_all(&response).await.is_err() {
                            return;
                        }
                        let count = info.piece_count() as usize;
                        let mut bits = vec![0; count.div_ceil(8)];
                        for piece in 0..count {
                            bits[piece / 8] |= 0x80 >> (piece % 8);
                        }
                        if socket.write_all(&PeerMessage::Bitfield(bits).encode()).await.is_err()
                            || socket.write_all(&PeerMessage::Unchoke.encode()).await.is_err()
                        {
                            return;
                        }

                        let mut buffer = Vec::new();
                        let mut scratch = [0; 4096];
                        'session: loop {
                            loop {
                                match PeerMessage::decode(&buffer) {
                                    Ok(Some((message, used))) => {
                                        buffer.drain(..used);
                                        if let PeerMessage::Request { index, begin, length } = message {
                                            let (reply, response_rx) = tokio::sync::oneshot::channel();
                                            if request_tx.send(ScriptedRequest { index, begin, length, reply }).await.is_err() {
                                                break 'session;
                                            }
                                            tokio::select! {
                                                response = response_rx => {
                                                    let Ok(block) = response else { break 'session };
                                                    let message = PeerMessage::Piece { index, begin, block }.encode();
                                                    if socket.write_all(&message).await.is_err() {
                                                        break 'session;
                                                    }
                                                }
                                                read = socket.read(&mut scratch) => {
                                                    match read {
                                                        Ok(0) | Err(_) => break 'session,
                                                        Ok(n) => buffer.extend_from_slice(&scratch[..n]),
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Ok(None) => break,
                                    Err(_) => break 'session,
                                }
                            }
                            match socket.read(&mut scratch).await {
                                Ok(0) | Err(_) => break,
                                Ok(n) => buffer.extend_from_slice(&scratch[..n]),
                            }
                        }
                        closed.fetch_add(1, Ordering::SeqCst);
                    });
                }
                Some(_) = sessions.join_next(), if !sessions.is_empty() => {}
            }
        }
        sessions.abort_all();
        while sessions.join_next().await.is_some() {}
    });
    ScriptedSeeder {
        addr,
        requests,
        shutdown,
        join,
        closed,
    }
}

async fn next_scripted_request(seeders: &mut [ScriptedSeeder]) -> (usize, ScriptedRequest) {
    loop {
        for (index, seeder) in seeders.iter_mut().enumerate() {
            if let Ok(request) = seeder.requests.try_recv() {
                return (index, request);
            }
        }
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn cancelling_multi_peer_range_is_followed_by_correct_overlapping_range() {
    let (content, info) = make_vod_content(16, 16, 32);
    let mut seeders = vec![
        spawn_scripted_vod_seeder(info.clone()).await,
        spawn_scripted_vod_seeder(info.clone()).await,
    ];
    let registry = SeedRegistry::new();
    let store = registry.lease_store(info.infohash, || PieceStore::new(16, 16, 128));
    let shared_store = store.0.clone();
    let range_lock = Arc::new(tokio::sync::Mutex::new(()));
    let vod = Arc::new(AceVodContent {
        info: info.clone(),
        bootstrap_peers: seeders.iter().map(|s| s.addr).collect(),
        announce_peer_port: tokio::sync::watch::channel(None).1,
        store,
        peers: Arc::new(tokio::sync::Mutex::new(None)),
        range_lock: range_lock.clone(),
    });

    let cancelled = vod.open_range(0, 31).await.unwrap();
    let mut active = [false; 2];
    let mut pending = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !active.iter().all(|active| *active) {
            let (peer, request) = next_scripted_request(&mut seeders).await;
            active[peer] = true;
            pending.push(request);
        }
    })
    .await
    .expect("both peer sockets receive active piece requests");

    // Queue the overlap while the first transaction still owns the lock.
    let next_vod = vod.clone();
    let overlapping = tokio::spawn(async move { next_vod.open_range(0, 31).await });
    drop(cancelled);
    tokio::time::timeout(Duration::from_secs(3), async {
        while seeders
            .iter()
            .any(|seeder| seeder.closed.load(Ordering::SeqCst) == 0)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancellation closes every active upstream socket");
    drop(pending);

    // Both old sockets report EOF before the queued overlap is awaited for correct completion.
    // This observes socket shutdown followed by overlap correctness, not exact remote-EOF versus
    // lock-release timing.
    let second = tokio::time::timeout(Duration::from_secs(3), overlapping)
        .await
        .expect("the cancelled range releases its lock")
        .unwrap()
        .unwrap();
    let reader = tokio::spawn(read_all(second));
    let mut requested = Vec::new();
    let bytes = tokio::time::timeout(Duration::from_secs(3), async {
        while requested.len() < 2 {
            let (_, request) = next_scripted_request(&mut seeders).await;
            let start =
                request.index as usize * info.piece_length as usize + request.begin as usize;
            let end = start + request.length as usize;
            request.reply.send(content[start..end].to_vec()).unwrap();
            requested.push(request.index);
        }
        reader.await.unwrap()
    })
    .await
    .expect("the overlapping range completes");
    assert_eq!(bytes, content);
    assert!(seeders.iter().all(|s| s.closed.load(Ordering::SeqCst) >= 1));
    assert!(PieceStore::shared_block(&shared_store, 0, 0, 16)
        .await
        .is_some());

    for seeder in seeders {
        seeder.stop().await;
    }
}

#[tokio::test]
async fn out_of_order_and_corrupt_upstream_never_reseed_unverified_bytes() {
    let (content, info) = make_vod_content(16, 16, 32);
    let mut seeders = vec![
        spawn_scripted_vod_seeder(info.clone()).await,
        spawn_scripted_vod_seeder(info.clone()).await,
    ];
    let registry = SeedRegistry::new();
    let store = registry.lease_store(info.infohash, || PieceStore::new(16, 16, 128));
    let shared_store = store.0.clone();
    let vod = AceVodContent {
        info: info.clone(),
        bootstrap_peers: seeders.iter().map(|s| s.addr).collect(),
        announce_peer_port: tokio::sync::watch::channel(None).1,
        store,
        peers: Arc::new(tokio::sync::Mutex::new(None)),
        range_lock: Arc::new(tokio::sync::Mutex::new(())),
    };

    let source = vod.open_range(0, 31).await.unwrap();
    let mut first = None;
    let mut later = None;
    tokio::time::timeout(Duration::from_secs(3), async {
        while first.is_none() || later.is_none() {
            let (peer, request) = next_scripted_request(&mut seeders).await;
            if request.index == 0 {
                first = Some((peer, request));
            } else if request.index == 1 {
                later = Some((peer, request));
            } else {
                panic!("unexpected piece request: {}", request.index);
            }
        }
    })
    .await
    .expect("both peers are assigned concurrent pieces");

    let (_, later) = later.unwrap();
    let later_start = info.piece_length as usize;
    let later_end = later_start + later.length as usize;
    later
        .reply
        .send(content[later_start..later_end].to_vec())
        .unwrap();
    tokio::task::yield_now().await;
    assert!(PieceStore::shared_block(&shared_store, 1, 0, 16)
        .await
        .is_none());

    let (bad_peer, first) = first.unwrap();
    let mut corrupted = content[..first.length as usize].to_vec();
    corrupted[0] ^= 0xff;
    first.reply.send(corrupted).unwrap();
    let (retry_peer, retry) = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (peer, request) = next_scripted_request(&mut seeders).await;
            if request.index == 0 {
                break (peer, request);
            }
        }
    })
    .await
    .expect("corrupt piece is retried");
    assert_ne!(retry_peer, bad_peer, "a hash-failing peer is not reused");
    retry
        .reply
        .send(content[..retry.length as usize].to_vec())
        .unwrap();

    let got = tokio::time::timeout(Duration::from_secs(3), read_all(source))
        .await
        .expect("verified ordered pieces reach the source");
    assert_eq!(got, content);
    assert_eq!(
        PieceStore::shared_block(&shared_store, 0, 0, 16)
            .await
            .unwrap(),
        content[..16]
    );
    assert_eq!(
        PieceStore::shared_block(&shared_store, 1, 0, 16)
            .await
            .unwrap(),
        content[16..]
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let listener_task = tokio::spawn(ace_swarm::listen::PeerListener::serve(
        listener,
        registry,
        random_peer_id(),
        [0; 8],
        8,
        Arc::new(Identity::generate()),
        8,
        None,
    ));
    let mut peer = PeerSession::new(TcpStream::connect(addr).await.unwrap());
    peer.perform_handshake(info.infohash, random_peer_id())
        .await
        .unwrap();
    peer.send(&PeerMessage::Interested).await.unwrap();
    loop {
        if matches!(peer.read_message().await.unwrap(), PeerMessage::Unchoke) {
            break;
        }
    }
    for (index, expected) in [content[..16].as_ref(), content[16..].as_ref()]
        .into_iter()
        .enumerate()
    {
        peer.send(&PeerMessage::Request {
            index: index as u32,
            begin: 0,
            length: 16,
        })
        .await
        .unwrap();
        loop {
            if let PeerMessage::Piece {
                index: got_index,
                begin,
                block,
            } = peer.read_message().await.unwrap()
            {
                assert_eq!((got_index, begin), (index as u32, 0));
                assert_eq!(block, expected);
                break;
            }
        }
    }

    listener_task.abort();
    let _ = listener_task.await;

    for seeder in seeders {
        seeder.stop().await;
    }
}
