use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const HEADER: [u8; 8] = *b"headr123";
const DATA: &[u8] = b"da";

struct PoolCaller {
    peer: PeerSession<TcpStream>,
    control: TcpStream,
    task: tokio::task::JoinHandle<FollowEnd>,
    rx: mpsc::Receiver<LiveOutput>,
    store: Arc<tokio::sync::Mutex<PieceStore>>,
    downloaded: Arc<AtomicU64>,
    uploaded: Arc<AtomicU64>,
    peers_served: Arc<AtomicU32>,
}

fn duplicate_tokio_stream(stream: TcpStream) -> (TcpStream, TcpStream) {
    let stream = stream.into_std().unwrap();
    stream.set_nonblocking(true).unwrap();
    let control = stream.try_clone().unwrap();
    (
        TcpStream::from_std(stream).unwrap(),
        TcpStream::from_std(control).unwrap(),
    )
}

async fn start_pool(enabled: bool) -> PoolCaller {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = listener.local_addr().unwrap();
    let client = TcpStream::connect(target).await.unwrap();
    let addr = match client.local_addr().unwrap() {
        std::net::SocketAddr::V4(addr) => addr,
        _ => unreachable!(),
    };
    let (server, _) = listener.accept().await.unwrap();
    let (server, control) = duplicate_tokio_stream(server);

    let info = info();
    let store = Arc::new(tokio::sync::Mutex::new(PieceStore::new(4, 2, 1024)));
    store.lock().await.put_chunk_with_header(7, 0, HEADER, DATA);
    let seed = LiveSeedOptions {
        metrics: None,
        registry: SeedRegistry::new(),
        store_factory: test_store_factory(4096, None),
        enabled,
        prefetch_pieces: 0,
        live_recovery: default_live_recovery(),
        warm_peers: WarmPeerCache::memory(),
    };
    let downloaded = Arc::new(AtomicU64::new(0));
    let uploaded = Arc::new(AtomicU64::new(0));
    let peers_served = Arc::new(AtomicU32::new(0));
    let task_downloaded = downloaded.clone();
    let task_uploaded = uploaded.clone();
    let task_peers_served = peers_served.clone();
    let task_store = store.clone();
    let (tx, rx) = mpsc::channel(1);
    let task = tokio::spawn(async move {
        let mut continuity = None;
        let mut candidates = SessionCandidates::default();
        follow_peer_pool(
            vec![ConnectedUpstream {
                session: PeerSession::new(client),
                addr,
                window: live_pos(7, 7),
                unchoked: false,
                yourip: None,
            }],
            &info,
            &Identity::generate(),
            2,
            &tx,
            &task_downloaded,
            &task_uploaded,
            &task_peers_served,
            &seed,
            &task_store,
            &mut continuity,
            vec![],
            vec![],
            completed_discovery(|_| Box::pin(async { vec![] })),
            &mut candidates,
            &Arc::new(AtomicU32::new(0)),
            None,
        )
        .await
    });
    let mut peer = PeerSession::new(server).with_timeout(Duration::from_secs(1));
    assert!(matches!(
        peer.read_message().await.unwrap(),
        PeerMessage::Extended { ext_id: 0, .. }
    ));
    assert_eq!(peer.read_message().await.unwrap(), PeerMessage::Interested);
    PoolCaller {
        peer,
        control,
        task,
        rx,
        store,
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

async fn stop_pool(caller: PoolCaller) {
    let PoolCaller {
        peer,
        control,
        task,
        rx,
        ..
    } = caller;
    drop(peer);
    drop(control);
    let result = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("closed remote peer should end the pool loop")
        .unwrap();
    assert!(matches!(result, FollowEnd::PeerLost(_)));
    drop(rx);
}

#[tokio::test]
async fn enabled_pool_reciprocal_seed_serves_id6_before_remote_interest() {
    let mut caller = start_pool(true).await;
    caller.peer.send(&id6_request()).await.unwrap();
    assert_reciprocal_reply(&mut caller.peer).await;
    assert_eq!(caller.uploaded.load(Ordering::Relaxed), DATA.len() as u64);
    assert_eq!(caller.peers_served.load(Ordering::Relaxed), 1);
    stop_pool(caller).await;
}

#[tokio::test]
async fn enabled_pool_reciprocal_seed_unchokes_after_first_interest_then_serves_id6() {
    let mut caller = start_pool(true).await;
    caller.peer.send(&PeerMessage::Interested).await.unwrap();
    assert_eq!(
        caller.peer.read_message().await.unwrap(),
        PeerMessage::Unchoke
    );
    caller.peer.send(&id6_request()).await.unwrap();
    assert_reciprocal_reply(&mut caller.peer).await;
    assert_eq!(caller.uploaded.load(Ordering::Relaxed), DATA.len() as u64);
    assert_eq!(caller.peers_served.load(Ordering::Relaxed), 1);
    stop_pool(caller).await;
}

#[tokio::test]
async fn disabled_pool_reciprocal_seed_does_not_unchoke_or_serve_id6() {
    let mut caller = start_pool(false).await;
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
    stop_pool(caller).await;
}

#[tokio::test]
async fn failed_pool_reply_enqueue_does_not_increment_counters() {
    let mut caller = start_pool(true).await;
    let store = caller.store.clone();
    let guard = store.lock().await;
    caller.peer.send(&id6_request()).await.unwrap();
    // A remote write-half close makes the real peer worker see EOF after its queued id6
    // event. The held store lock lets that worker drop its command receiver first.
    caller.control.shutdown().await.unwrap();
    let mut byte = [0u8; 1];
    let n = tokio::time::timeout(Duration::from_secs(2), caller.control.read(&mut byte))
        .await
        .expect("the local worker should close its socket after receiving EOF")
        .unwrap();
    assert_eq!(n, 0, "the remote read half should observe worker EOF");
    assert!(
        !caller.task.is_finished(),
        "reply preparation should wait on the held store lock"
    );
    drop(guard);

    let result = tokio::time::timeout(Duration::from_secs(2), &mut caller.task)
        .await
        .expect("the failed enqueue should retire the closed peer")
        .unwrap();
    assert!(matches!(result, FollowEnd::PeerLost(1)));
    assert_eq!(caller.downloaded.load(Ordering::Relaxed), 0);
    assert_eq!(caller.uploaded.load(Ordering::Relaxed), 0);
    assert_eq!(caller.peers_served.load(Ordering::Relaxed), 0);
    drop(caller.peer);
    drop(caller.control);
    drop(caller.rx);
}
