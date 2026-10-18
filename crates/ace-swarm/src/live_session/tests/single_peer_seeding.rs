use super::*;
use std::net::Shutdown;

const HEADER: [u8; 8] = *b"headr123";
const DATA: &[u8] = b"da";

struct Caller {
    peer: PeerSession<TcpStream>,
    task: tokio::task::JoinHandle<FollowEnd>,
    shutdown_write: std::net::TcpStream,
    downloaded: Arc<AtomicU64>,
    uploaded: Arc<AtomicU64>,
    peers_served: Arc<AtomicU32>,
}

async fn start_single_peer(enabled: bool) -> Caller {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = listener.local_addr().unwrap();
    let client = TcpStream::connect(target).await.unwrap();
    let peer_addr = match client.local_addr().unwrap() {
        std::net::SocketAddr::V4(addr) => addr,
        _ => unreachable!(),
    };
    let client = client.into_std().unwrap();
    client.set_nonblocking(true).unwrap();
    let shutdown_write = client.try_clone().unwrap();
    let client = TcpStream::from_std(client).unwrap();
    let (server, _) = listener.accept().await.unwrap();

    let info = StreamInfo {
        infohash: [0; 20],
        piece_length: 4,
        chunk_length: 2,
        trackers: vec![],
        metadata: Default::default(),
        sig_len: 0,
        source_pubkey: vec![],
    };
    let store = Arc::new(tokio::sync::Mutex::new(PieceStore::new(4, 2, 1024)));
    store.lock().await.put_chunk_with_header(7, 0, HEADER, DATA);
    let registry = SeedRegistry::new();
    registry.register(info.infohash, store);
    let seed = LiveSeedOptions {
        metrics: None,
        registry,
        store_factory: test_store_factory(4096, None),
        enabled,
        prefetch_pieces: 0,
        live_recovery: LiveRecoveryConfig::default(),
        warm_peers: WarmPeerCache::memory(),
    };
    let downloaded = Arc::new(AtomicU64::new(0));
    let uploaded = Arc::new(AtomicU64::new(0));
    let peers_served = Arc::new(AtomicU32::new(0));
    let task_downloaded = downloaded.clone();
    let task_uploaded = uploaded.clone();
    let task_peers_served = peers_served.clone();
    let (tx, _rx) = mpsc::channel(1);
    let task = tokio::spawn(async move {
        let mut session = PeerSession::new(client);
        let mut continuity = None;
        follow_one_peer(
            &mut session,
            &info,
            &Identity::generate(),
            peer_addr,
            WireLivePosition {
                min_piece: 7,
                max_piece: 7,
                position: 7,
                distance_from_source: 1,
            },
            2,
            &tx,
            &task_downloaded,
            &task_uploaded,
            &task_peers_served,
            &seed,
            &mut continuity,
        )
        .await
    });
    let mut peer = PeerSession::new(server).with_timeout(Duration::from_secs(1));
    assert!(matches!(
        peer.read_message().await.unwrap(),
        PeerMessage::Extended { ext_id: 0, .. }
    ));
    assert_eq!(peer.read_message().await.unwrap(), PeerMessage::Interested);
    Caller {
        peer,
        task,
        shutdown_write,
        downloaded,
        uploaded,
        peers_served,
    }
}

fn id6_request() -> PeerMessage {
    let mut payload = 0x1234_5678u32.to_be_bytes().to_vec();
    payload.extend_from_slice(&7u32.to_be_bytes());
    payload.extend_from_slice(&0u16.to_be_bytes());
    PeerMessage::Unknown { id: 6, payload }
}

async fn assert_reciprocal_reply(peer: &mut PeerSession<TcpStream>) {
    let PeerMessage::Piece {
        index,
        begin,
        block,
    } = peer.read_message().await.unwrap()
    else {
        panic!("id6 request should receive a Piece reply")
    };
    assert_eq!(index, 0);
    assert_eq!(begin, 7);
    assert_eq!(&block[..8], &HEADER);
    assert_eq!(&block[8..10], &[0, 0]);
    assert_eq!(&block[10..], DATA);
}

async fn stop_caller(caller: Caller) {
    caller.task.abort();
    let _ = caller.task.await;
}

#[tokio::test]
async fn enabled_reciprocal_seed_serves_id6_before_remote_interest() {
    let mut caller = start_single_peer(true).await;
    caller.peer.send(&id6_request()).await.unwrap();
    assert_reciprocal_reply(&mut caller.peer).await;
    assert_eq!(caller.uploaded.load(Ordering::Relaxed), DATA.len() as u64);
    assert_eq!(caller.peers_served.load(Ordering::Relaxed), 1);
    stop_caller(caller).await;
}

#[tokio::test]
async fn enabled_reciprocal_seed_unchokes_after_first_interest_then_serves_id6() {
    let mut caller = start_single_peer(true).await;
    caller.peer.send(&PeerMessage::Interested).await.unwrap();
    assert_eq!(
        caller.peer.read_message().await.unwrap(),
        PeerMessage::Unchoke
    );
    caller.peer.send(&id6_request()).await.unwrap();
    assert_reciprocal_reply(&mut caller.peer).await;
    assert_eq!(caller.uploaded.load(Ordering::Relaxed), DATA.len() as u64);
    assert_eq!(caller.peers_served.load(Ordering::Relaxed), 1);
    stop_caller(caller).await;
}

#[tokio::test]
async fn disabled_reciprocal_seed_does_not_unchoke_or_serve_id6() {
    let mut caller = start_single_peer(false).await;
    caller.peer.send(&PeerMessage::Interested).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), caller.peer.read_message())
            .await
            .is_err()
    );
    caller.peer.send(&id6_request()).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), caller.peer.read_message())
            .await
            .is_err()
    );
    assert_eq!(caller.uploaded.load(Ordering::Relaxed), 0);
    assert_eq!(caller.peers_served.load(Ordering::Relaxed), 0);
    stop_caller(caller).await;
}

#[tokio::test]
async fn failed_legacy_socket_send_does_not_increment_counters() {
    let mut caller = start_single_peer(true).await;
    caller.downloaded.store(13, Ordering::Relaxed);
    caller.uploaded.store(17, Ordering::Relaxed);
    caller.peers_served.store(19, Ordering::Relaxed);

    // Shut down the local session's write half through its duplicated socket. Its read half
    // remains live, so it can receive the request but cannot send the prepared reply.
    caller.shutdown_write.shutdown(Shutdown::Write).unwrap();
    caller.peer.send(&id6_request()).await.unwrap();
    drop(caller.peer);

    let end = tokio::time::timeout(Duration::from_secs(2), &mut caller.task)
        .await
        .expect("closed remote peer should end the legacy loop")
        .unwrap();
    assert!(matches!(end, FollowEnd::PeerLost(_)));
    assert_eq!(caller.downloaded.load(Ordering::Relaxed), 13);
    assert_eq!(caller.uploaded.load(Ordering::Relaxed), 17);
    assert_eq!(caller.peers_served.load(Ordering::Relaxed), 19);
}
