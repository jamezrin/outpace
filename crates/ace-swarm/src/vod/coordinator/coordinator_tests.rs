use super::*;
use crate::types::VodInfo;
use ace_peer::session::PeerSession;
use ace_wire::handshake::{Handshake, HANDSHAKE_LEN};
use ace_wire::message::PeerMessage;
use sha1::{Digest, Sha1};
use std::{
    net::SocketAddrV4,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{duplex, AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
    task::JoinHandle,
};

struct ScriptedPeer {
    address: SocketAddrV4,
    owner: JoinHandle<()>,
}

impl Drop for ScriptedPeer {
    fn drop(&mut self) {
        self.owner.abort();
    }
}

#[allow(clippy::too_many_arguments)]
async fn spawn_scripted_peer(
    peer_id: usize,
    content: Vec<u8>,
    info: VodInfo,
    bitfield: Vec<u8>,
    late_have: Option<(u32, Duration)>,
    active_sessions: Arc<AtomicUsize>,
    maximum_sessions: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<(usize, u32)>>>,
) -> ScriptedPeer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(address) => address,
        _ => unreachable!(),
    };
    let owner = tokio::spawn(async move {
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(connection) => connection,
                Err(_) => return,
            };
            let active = active_sessions.fetch_add(1, Ordering::SeqCst) + 1;
            maximum_sessions.fetch_max(active, Ordering::SeqCst);
            let _active_guard = ActiveSession(active_sessions.clone());
            let mut incoming = [0; HANDSHAKE_LEN];
            if socket.read_exact(&mut incoming).await.is_err()
                || socket
                    .write_all(&Handshake::new(info.infohash, [peer_id as u8; 20]).encode())
                    .await
                    .is_err()
                || socket
                    .write_all(&PeerMessage::Bitfield(bitfield.clone()).encode())
                    .await
                    .is_err()
                || socket
                    .write_all(&PeerMessage::Unchoke.encode())
                    .await
                    .is_err()
            {
                continue;
            }
            if let Some((piece, delay)) = late_have {
                tokio::time::sleep(delay).await;
                if socket
                    .write_all(&PeerMessage::Have(piece).encode())
                    .await
                    .is_err()
                {
                    continue;
                }
            }
            let mut buffered = Vec::new();
            let mut scratch = [0; 4096];
            loop {
                loop {
                    match PeerMessage::decode(&buffered) {
                        Ok(Some((message, consumed))) => {
                            buffered.drain(..consumed);
                            if let PeerMessage::Request {
                                index,
                                begin,
                                length,
                            } = message
                            {
                                requests.lock().unwrap().push((peer_id, index));
                                let start =
                                    index as usize * info.piece_length as usize + begin as usize;
                                let end = start + length as usize;
                                let response = PeerMessage::Piece {
                                    index,
                                    begin,
                                    block: content[start..end].to_vec(),
                                };
                                if socket.write_all(&response.encode()).await.is_err() {
                                    break;
                                }
                            }
                        }
                        Ok(None) => break,
                        Err(_) => return,
                    }
                }
                match socket.read(&mut scratch).await {
                    Ok(0) | Err(_) => break,
                    Ok(read) => buffered.extend_from_slice(&scratch[..read]),
                }
            }
        }
    });
    ScriptedPeer { address, owner }
}

struct ActiveSession(Arc<AtomicUsize>);

impl Drop for ActiveSession {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn test_info(piece_count: usize, piece_length: usize) -> (Vec<u8>, VodInfo) {
    let content: Vec<u8> = (0..piece_count * piece_length)
        .map(|index| (index % 251) as u8)
        .collect();
    let piece_hashes = content
        .chunks(piece_length)
        .map(|piece| <[u8; 20]>::from(Sha1::digest(piece)))
        .collect();
    let info = VodInfo {
        infohash: [0x7c; 20],
        peer_protocol: Default::default(),
        piece_length: piece_length as u64,
        chunk_length: piece_length as u64,
        trackers: Vec::new(),
        piece_hashes,
        total_length: content.len() as u64,
    };
    (content, info)
}

fn test_timeouts(unchoke: Duration) -> VodTimeouts {
    VodTimeouts {
        connect: Duration::from_secs(2),
        handshake: Duration::from_secs(2),
        unchoke,
        block_progress: Duration::from_secs(2),
        write: Duration::from_secs(2),
    }
}

#[tokio::test]
async fn fetch_piece_keeps_at_most_eight_requests_outstanding_and_requests_short_tail() {
    const CHUNK: usize = 32;
    const BLOCKS: usize = REQUESTS + 3;
    let content: Vec<u8> = (0..CHUNK * BLOCKS - 25)
        .map(|index| (index % 251) as u8)
        .collect();
    let info = VodInfo {
        infohash: [0x5a; 20],
        peer_protocol: Default::default(),
        piece_length: content.len() as u64,
        chunk_length: CHUNK as u64,
        trackers: Vec::new(),
        piece_hashes: vec![<[u8; 20]>::from(Sha1::digest(&content))],
        total_length: content.len() as u64,
    };
    let (client, server) = duplex(4096);
    let mut session = PeerSession::new(client);
    let server_content = content.clone();
    let state = Mutex::new(Availability {
        unchoked: true,
        bits: None,
    });
    let server_info = info.clone();
    let server_task = tokio::spawn(async move {
        let mut peer = PeerSession::new(server);
        let blocks = server_info.piece_size(0).div_ceil(server_info.chunk_length) as usize;
        let mut requested = 0usize;
        let mut served = 0usize;
        let mut pending = Vec::<(u32, u32)>::new();
        let mut high_water = 0usize;
        let mut final_request_len = 0;
        let mut refills = 0usize;
        while pending.len() < REQUESTS {
            let PeerMessage::Request {
                index,
                begin,
                length,
            } = peer.read_message().await.unwrap()
            else {
                panic!("client should send only block requests before any response");
            };
            assert_eq!(index, 0);
            pending.push((begin, length));
            requested += 1;
            high_water = high_water.max(pending.len());
        }
        // The client must stop after filling the eight request credits and wait for a
        // complete response. An eager writer that sends every block before reading would
        // make this readiness probe complete.
        tokio::select! {
            biased;
            message = peer.read_message() => panic!("request credit exceeded before any response: {message:?}"),
            _ = tokio::task::yield_now() => {},
        }

        let (begin, length) = pending[0];
        peer.send(&PeerMessage::Piece {
            index: 0,
            begin,
            block: server_content[begin as usize..begin as usize + 1].to_vec(),
        })
        .await
        .unwrap();
        // A partial response adds coverage, but does not complete the requested block and
        // therefore must not free a credit or trigger a refill.
        tokio::select! {
            biased;
            message = peer.read_message() => panic!("partial response freed a request credit: {message:?}"),
            _ = tokio::task::yield_now() => {},
        }
        peer.send(&PeerMessage::Piece {
            index: 0,
            begin,
            block: server_content[begin as usize..begin as usize + 1].to_vec(),
        })
        .await
        .unwrap();
        tokio::select! {
            biased;
            message = peer.read_message() => panic!("duplicate response freed a request credit: {message:?}"),
            _ = tokio::task::yield_now() => {},
        }
        peer.send(&PeerMessage::Piece {
            index: 0,
            begin,
            block: server_content[begin as usize..begin as usize + length as usize].to_vec(),
        })
        .await
        .unwrap();
        pending.remove(0);
        served += 1;
        let PeerMessage::Request {
            index,
            begin,
            length,
        } = peer.read_message().await.unwrap()
        else {
            panic!("new coverage should refill one request credit");
        };
        assert_eq!(index, 0);
        pending.push((begin, length));
        requested += 1;
        refills += 1;
        high_water = high_water.max(pending.len());
        while served < blocks {
            let (begin, length) = pending.remove(0);
            let start = begin as usize;
            let end = start + length as usize;
            peer.send(&PeerMessage::Piece {
                index: 0,
                begin,
                block: server_content[start..end].to_vec(),
            })
            .await
            .unwrap();
            served += 1;
            if requested < blocks {
                let PeerMessage::Request {
                    index,
                    begin,
                    length,
                } = peer.read_message().await.unwrap()
                else {
                    panic!("new coverage should refill one request credit");
                };
                assert_eq!(index, 0);
                pending.push((begin, length));
                requested += 1;
                refills += 1;
                if requested == blocks {
                    final_request_len = length;
                }
                high_water = high_water.max(pending.len());
            }
        }
        (high_water, final_request_len, requested, refills)
    });

    let timeouts = VodTimeouts {
        connect: Duration::from_secs(2),
        handshake: Duration::from_secs(2),
        unchoke: Duration::from_secs(2),
        block_progress: Duration::from_secs(2),
        write: Duration::from_secs(2),
    };
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        fetch_piece(&mut session, &info, 0, &state, timeouts),
    )
    .await
    .expect("the duplex peer should serve each refill");
    let bytes = match result {
        Ok(bytes) => bytes,
        Err(_) => panic!("piece assembly should complete in duplex"),
    };
    assert_eq!(bytes.as_ref(), content);
    let (high_water, final_request_len, requested, refills) =
        tokio::time::timeout(Duration::from_secs(3), server_task)
            .await
            .expect("the mock peer should see the complete request sequence")
            .unwrap();
    assert_eq!(
        high_water, REQUESTS,
        "initial pipeline fills exactly eight slots"
    );
    assert!(high_water <= REQUESTS, "request credit never exceeds eight");
    assert_eq!(final_request_len, 7, "last request covers the short tail");
    assert_eq!(requested, BLOCKS);
    assert_eq!(
        refills,
        BLOCKS - REQUESTS,
        "new coverage replenishes request credit"
    );
}

#[tokio::test]
async fn conflicting_partial_overlap_fails_piece_hash_verification() {
    let content: Vec<u8> = (0..32).collect();
    let info = VodInfo {
        infohash: [0x5b; 20],
        peer_protocol: Default::default(),
        piece_length: 32,
        chunk_length: 32,
        trackers: Vec::new(),
        piece_hashes: vec![<[u8; 20]>::from(Sha1::digest(&content))],
        total_length: 32,
    };
    let (client, server) = duplex(4096);
    let mut session = PeerSession::new(client);
    let state = Mutex::new(Availability {
        unchoked: true,
        bits: None,
    });
    let fetch = tokio::spawn(async move {
        fetch_piece(
            &mut session,
            &info,
            0,
            &state,
            test_timeouts(Duration::from_secs(2)),
        )
        .await
    });
    let mut peer = PeerSession::new(server);
    assert!(matches!(
        peer.read_message().await.unwrap(),
        PeerMessage::Request { .. }
    ));
    peer.send(&PeerMessage::Piece {
        index: 0,
        begin: 0,
        block: content[..16].to_vec(),
    })
    .await
    .unwrap();
    let mut conflict = content[4..12].to_vec();
    conflict[0] ^= 0xff;
    peer.send(&PeerMessage::Piece {
        index: 0,
        begin: 4,
        block: conflict,
    })
    .await
    .unwrap();
    peer.send(&PeerMessage::Piece {
        index: 0,
        begin: 16,
        block: content[16..].to_vec(),
    })
    .await
    .unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), fetch)
            .await
            .expect("assembly must finish after full coverage")
            .unwrap(),
        Err(Failure::Hash)
    ));
}

#[tokio::test]
async fn cancelling_production_request_writer_drops_backpressured_session() {
    let (_content, mut info) = test_info(1, 32 * 100);
    info.chunk_length = 32;
    let (client, mut server) = duplex(1);
    let mut session = PeerSession::new(client);
    let state = Mutex::new(Availability {
        unchoked: true,
        bits: None,
    });
    let write = tokio::spawn(async move {
        assemble_piece(
            &mut session,
            &info,
            0,
            &state,
            test_timeouts(Duration::from_secs(10)),
        )
        .await
    });

    // Drain only two bytes of the first encoded request. The real production request writer
    // then cannot finish its frame and remains backpressured until its owning future is dropped.
    let mut prefix = [0; 2];
    tokio::time::timeout(Duration::from_secs(2), server.read_exact(&mut prefix))
        .await
        .expect("request writer should begin writing")
        .unwrap();
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(
        !write.is_finished(),
        "request write should still be backpressured"
    );
    write.abort();
    match write.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("cancelled production request writer unexpectedly completed"),
    }

    tokio::time::timeout(Duration::from_secs(2), async {
        let mut remaining = [0; 64];
        loop {
            if server.read(&mut remaining).await.unwrap() == 0 {
                break;
            }
        }
    })
    .await
    .expect("cancelling the production writer must close its session");
}

#[tokio::test(start_paused = true)]
async fn duplicate_bytes_and_choke_chatter_do_not_extend_progress_deadline() {
    let content: Vec<u8> = (0..32).collect();
    let info = VodInfo {
        infohash: [0x6c; 20],
        peer_protocol: Default::default(),
        piece_length: 32,
        chunk_length: 32,
        trackers: Vec::new(),
        piece_hashes: vec![<[u8; 20]>::from(Sha1::digest(&content))],
        total_length: 32,
    };
    let (client, server) = duplex(4096);
    let mut session = PeerSession::new(client);
    let state = Mutex::new(Availability {
        unchoked: true,
        bits: None,
    });
    let task_info = info.clone();
    let task_state = state;
    let fetch = tokio::spawn(async move {
        fetch_piece(
            &mut session,
            &task_info,
            0,
            &task_state,
            VodTimeouts {
                connect: Duration::from_secs(2),
                handshake: Duration::from_secs(2),
                unchoke: Duration::from_secs(20),
                block_progress: Duration::from_secs(10),
                write: Duration::from_secs(2),
            },
        )
        .await
    });
    let mut peer = PeerSession::new(server);
    assert!(matches!(
        peer.read_message().await.unwrap(),
        PeerMessage::Request { .. }
    ));
    peer.send(&PeerMessage::Piece {
        index: 0,
        begin: 0,
        block: content[..16].to_vec(),
    })
    .await
    .unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(9)).await;
    peer.send(&PeerMessage::Piece {
        index: 0,
        begin: 0,
        block: content[..16].to_vec(),
    })
    .await
    .unwrap();
    peer.send(&PeerMessage::Choke).await.unwrap();
    peer.send(&PeerMessage::Unchoke).await.unwrap();
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert!(
        !fetch.is_finished(),
        "non-progress traffic cannot expire early"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    let timed_out = fetch.is_finished();
    if timed_out {
        assert!(
            fetch.await.unwrap().is_err(),
            "partial assembly is discarded on timeout"
        );
    } else {
        fetch.abort();
        let _ = fetch.await;
    }
    assert!(
        timed_out,
        "duplicate and choke/unchoke messages do not reset the deadline"
    );
}

#[tokio::test(start_paused = true)]
async fn fetch_piece_absolute_deadline_stops_unique_byte_dribble_at_120_seconds() {
    let content: Vec<u8> = (0..13).collect();
    let info = VodInfo {
        infohash: [0x6b; 20],
        peer_protocol: Default::default(),
        piece_length: content.len() as u64,
        chunk_length: content.len() as u64,
        trackers: Vec::new(),
        piece_hashes: vec![<[u8; 20]>::from(Sha1::digest(&content))],
        total_length: content.len() as u64,
    };
    let (client, server) = duplex(4096);
    let mut session = PeerSession::new(client);
    let state = Mutex::new(Availability {
        unchoked: true,
        bits: None,
    });
    let task_info = info.clone();
    let task_state = state;
    let fetch = tokio::spawn(async move {
        fetch_piece(
            &mut session,
            &task_info,
            0,
            &task_state,
            VodTimeouts {
                connect: Duration::from_secs(2),
                handshake: Duration::from_secs(2),
                unchoke: Duration::from_secs(2),
                block_progress: Duration::from_secs(20),
                write: Duration::from_secs(2),
            },
        )
        .await
    });
    let mut peer = PeerSession::new(server);
    assert!(matches!(
        peer.read_message().await.unwrap(),
        PeerMessage::Request {
            index: 0,
            begin: 0,
            length: 13
        }
    ));

    // Every ten virtual seconds, cover one new byte of the outstanding request. This keeps
    // the unique-progress deadline alive while crossing the fixed 120-second attempt limit.
    for offset in 0..11u32 {
        tokio::time::advance(Duration::from_secs(10)).await;
        peer.send(&PeerMessage::Piece {
            index: 0,
            begin: offset,
            block: vec![content[offset as usize]],
        })
        .await
        .unwrap();
        tokio::task::yield_now().await;
        assert!(
            !fetch.is_finished(),
            "unique byte progress keeps the attempt alive before 120s"
        );
    }
    tokio::time::advance(Duration::from_secs(10)).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    let stopped_at_deadline = fetch.is_finished();
    if !stopped_at_deadline {
        fetch.abort();
        let _ = fetch.await;
    } else {
        assert!(
            fetch.await.unwrap().is_err(),
            "absolute timeout discards partial coverage"
        );
    }
    assert!(
        stopped_at_deadline,
        "unique dribbled bytes cannot extend the 120s absolute limit"
    );
}

#[test]
fn availability_validates_msb_length_tail_and_late_have() {
    let mut availability = Availability::default();
    assert!(availability
        .control(&PeerMessage::Bitfield(vec![0b1000_0001, 0b0100_0000]), 10)
        .is_ok());
    assert!(availability.has(0));
    assert!(availability.has(7));
    assert!(!availability.has(8));
    assert!(availability.has(9));

    assert!(availability.control(&PeerMessage::Have(8), 10).is_ok());
    assert!(availability.control(&PeerMessage::Have(8), 10).is_ok());
    assert!(
        availability.has(8),
        "a late HAVE updates known availability idempotently"
    );
    assert!(availability.control(&PeerMessage::Have(10), 10).is_err());
    assert!(availability
        .control(&PeerMessage::Bitfield(vec![0xff]), 10)
        .is_err());
    assert!(
        availability
            .control(&PeerMessage::Bitfield(vec![0x80, 0b0100_0001]), 10)
            .is_err(),
        "unused low tail bits must be zero"
    );

    let unknown = Availability::default();
    assert!(unknown.has(9), "omitted bitfields remain optimistic");
}

#[tokio::test]
async fn complementary_bitfields_assign_each_piece_only_to_a_holder() {
    let (content, info) = test_info(4, 16);
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let peer0 = spawn_scripted_peer(
        0,
        content.clone(),
        info.clone(),
        vec![0b1010_0000],
        None,
        active.clone(),
        maximum.clone(),
        requests.clone(),
    )
    .await;
    let peer1 = spawn_scripted_peer(
        1,
        content.clone(),
        info.clone(),
        vec![0b0101_0000],
        None,
        active.clone(),
        maximum.clone(),
        requests.clone(),
    )
    .await;
    let (tx, mut rx) = mpsc::channel::<Bytes>(4);
    let download = tokio::spawn(download_vod_pieces_with_policy(
        info,
        vec![peer0.address, peer1.address],
        tx,
        0,
        4,
        test_timeouts(Duration::from_secs(2)),
        2,
    ));
    let mut got = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(piece) = rx.recv().await {
            got.extend_from_slice(&piece);
        }
    })
    .await
    .expect("complementary availability should cover the range");
    download.await.unwrap().unwrap();
    assert_eq!(got, content);
    let served = requests.lock().unwrap().clone();
    assert_eq!(served.len(), 4);
    assert!(served.iter().all(|(peer, piece)| match piece {
        0 | 2 => *peer == 0,
        1 | 3 => *peer == 1,
        _ => false,
    }));
    assert!(maximum.load(Ordering::SeqCst) <= 4);
    drop((peer0, peer1));
}

#[tokio::test]
async fn late_have_unblocks_frontier_and_absent_frontier_exhausts_boundedly() {
    let (content, info) = test_info(1, 16);
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let peer = spawn_scripted_peer(
        0,
        content.clone(),
        info.clone(),
        vec![0],
        Some((0, Duration::from_millis(30))),
        active.clone(),
        maximum.clone(),
        requests.clone(),
    )
    .await;
    let (tx, mut rx) = mpsc::channel::<Bytes>(1);
    let download = tokio::spawn(download_vod_pieces_with_policy(
        info.clone(),
        vec![peer.address],
        tx,
        0,
        1,
        test_timeouts(Duration::from_millis(250)),
        1,
    ));
    let bytes = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("late HAVE should arrive before the bounded availability wait")
        .expect("the eligible peer returns the piece");
    assert_eq!(bytes.as_ref(), content);
    drop(rx);
    download.await.unwrap().unwrap();
    assert_eq!(requests.lock().unwrap().as_slice(), &[(0, 0)]);
    drop(peer);

    let (content, info) = test_info(1, 16);
    let absent = spawn_scripted_peer(
        0,
        content,
        info.clone(),
        vec![0],
        None,
        active,
        maximum,
        requests,
    )
    .await;
    let (tx, _rx) = mpsc::channel::<Bytes>(1);
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        download_vod_pieces_with_policy(
            info,
            vec![absent.address; 6],
            tx,
            0,
            1,
            test_timeouts(Duration::from_millis(40)),
            1,
        ),
    )
    .await
    .expect("known absence must exhaust within the retry budget");
    assert!(matches!(result, Err(VodError::Unrecoverable(0))));
    drop(absent);
}

#[tokio::test]
async fn fifth_address_rotates_into_a_slot_after_four_unavailable_peers() {
    let (content, info) = test_info(1, 16);
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut peers = Vec::new();
    for index in 0..5 {
        peers.push(
            spawn_scripted_peer(
                index,
                content.clone(),
                info.clone(),
                if index == 4 { vec![0x80] } else { vec![0] },
                None,
                active.clone(),
                maximum.clone(),
                requests.clone(),
            )
            .await,
        );
    }
    let addresses = peers.iter().map(|peer| peer.address).collect();
    let (tx, mut rx) = mpsc::channel::<Bytes>(1);
    let download = tokio::spawn(download_vod_pieces_with_policy(
        info,
        addresses,
        tx,
        0,
        1,
        test_timeouts(Duration::from_millis(80)),
        4,
    ));
    let bytes = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .expect("the fifth address should enter a freed worker slot")
        .expect("fifth peer serves the frontier");
    assert_eq!(bytes.as_ref(), content);
    drop(rx);
    download.await.unwrap().unwrap();
    assert!(requests
        .lock()
        .unwrap()
        .iter()
        .any(|(peer, piece)| *peer == 4 && *piece == 0));
    assert!(
        maximum.load(Ordering::SeqCst) <= 4,
        "active sessions stay within the worker cap"
    );
    drop(peers);
}

#[tokio::test]
async fn dropping_backpressured_consumer_joins_worker_and_closes_session() {
    let (content, info) = test_info(8, 16);
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let peer = spawn_scripted_peer(
        0,
        content,
        info.clone(),
        vec![0xff],
        None,
        active.clone(),
        maximum.clone(),
        requests.clone(),
    )
    .await;
    let (tx, rx) = mpsc::channel::<Bytes>(1);
    let download = tokio::spawn(download_vod_pieces_with_policy(
        info,
        vec![peer.address],
        tx,
        0,
        8,
        test_timeouts(Duration::from_secs(2)),
        1,
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if requests.lock().unwrap().len() >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("one piece should fill the caller queue and a second should block on send");
    drop(rx);
    let result = tokio::time::timeout(Duration::from_secs(2), download)
        .await
        .expect("consumer closure must stop and join the worker")
        .unwrap();
    assert!(matches!(result, Err(VodError::ConsumerGone)));
    tokio::time::timeout(Duration::from_secs(2), async {
        while active.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owned worker shutdown closes its socket before returning");
    assert!(maximum.load(Ordering::SeqCst) <= 4);
    drop(peer);
}
