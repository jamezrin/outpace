//! Deterministic integration tests for the private VOD piece-concurrency policy.
//!
//! The parent `vod.rs` module declares this file once the production state machine is ready.

use super::{download_vod_pieces_with_policy, VodTimeouts};
use crate::types::VodInfo;
use ace_wire::{
    handshake::{Handshake, HANDSHAKE_LEN},
    message::PeerMessage,
};
use bytes::Bytes;
use sha1::{Digest, Sha1};
use std::{net::SocketAddrV4, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

const PIECES: usize = 8;
const PIECE_LEN: usize = 32;

struct Request {
    peer: usize,
    piece: u32,
    release: oneshot::Sender<()>,
}

struct Seeders {
    addresses: Vec<SocketAddrV4>,
    owners: Vec<JoinHandle<()>>,
}

impl Drop for Seeders {
    fn drop(&mut self) {
        for owner in &self.owners {
            owner.abort();
        }
    }
}

async fn spawn_seeders(
    content: Vec<u8>,
    info: VodInfo,
    events: mpsc::UnboundedSender<Request>,
    corrupt_first_peer: bool,
) -> Seeders {
    let mut addresses = Vec::new();
    let mut owners = Vec::new();
    for peer in 0..2 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = match listener.local_addr().unwrap() {
            std::net::SocketAddr::V4(address) => address,
            _ => unreachable!(),
        };
        addresses.push(address);
        let content = content.clone();
        let info = info.clone();
        let events = events.clone();
        owners.push(tokio::spawn(async move {
            let mut children = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => match accepted {
                        Ok((socket, _)) => {
                            let content = content.clone();
                            let info = info.clone();
                            let events = events.clone();
                            children.spawn(async move {
                                serve_controlled(
                                    socket,
                                    peer,
                                    content,
                                    info,
                                    events,
                                    corrupt_first_peer,
                                )
                                .await;
                            });
                        }
                        Err(_) => break,
                    },
                    Some(_) = children.join_next(), if !children.is_empty() => {}
                }
            }
            children.abort_all();
        }));
    }
    Seeders { addresses, owners }
}

async fn serve_controlled(
    mut socket: TcpStream,
    peer: usize,
    content: Vec<u8>,
    info: VodInfo,
    events: mpsc::UnboundedSender<Request>,
    corrupt_first_peer: bool,
) {
    let mut incoming_handshake = [0; HANDSHAKE_LEN];
    if socket.read_exact(&mut incoming_handshake).await.is_err() {
        return;
    }
    if socket
        .write_all(&Handshake::new(info.infohash, [peer as u8; 20]).encode())
        .await
        .is_err()
    {
        return;
    }
    let mut bitfield = vec![0xff; info.piece_count().div_ceil(8) as usize];
    if let Some(last) = bitfield.last_mut() {
        let used = info.piece_count() as usize % 8;
        if used != 0 {
            *last = 0xff << (8 - used);
        }
    }
    if socket
        .write_all(&PeerMessage::Bitfield(bitfield).encode())
        .await
        .is_err()
        || socket
            .write_all(&PeerMessage::Unchoke.encode())
            .await
            .is_err()
    {
        return;
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
                        let (release, released) = oneshot::channel();
                        if events
                            .send(Request {
                                peer,
                                piece: index,
                                release,
                            })
                            .is_err()
                        {
                            return;
                        }
                        if released.await.is_err() {
                            return;
                        }
                        let start = index as usize * info.piece_length as usize + begin as usize;
                        let end = start + length as usize;
                        let mut block = content[start..end].to_vec();
                        if corrupt_first_peer && peer == 0 && index == 0 && begin == 0 {
                            block[0] ^= 0xff;
                        }
                        let response = PeerMessage::Piece {
                            index,
                            begin,
                            block,
                        };
                        if socket.write_all(&response.encode()).await.is_err() {
                            return;
                        }
                    }
                }
                Ok(None) => break,
                Err(_) => return,
            }
        }
        match socket.read(&mut scratch).await {
            Ok(0) | Err(_) => return,
            Ok(read) => buffered.extend_from_slice(&scratch[..read]),
        }
    }
}

fn content_and_info() -> (Vec<u8>, VodInfo) {
    content_and_info_with_length(PIECES * PIECE_LEN)
}

fn content_and_info_with_length(total_len: usize) -> (Vec<u8>, VodInfo) {
    let content: Vec<u8> = (0..total_len).map(|byte| (byte % 251) as u8).collect();
    let piece_hashes = content
        .chunks(PIECE_LEN)
        .map(|piece| <[u8; 20]>::from(Sha1::digest(piece)))
        .collect();
    (
        content,
        VodInfo {
            infohash: [0x4d; 20],
            piece_length: PIECE_LEN as u64,
            chunk_length: PIECE_LEN as u64,
            trackers: vec![],
            piece_hashes,
            total_length: total_len as u64,
        },
    )
}

fn short_timeouts() -> VodTimeouts {
    VodTimeouts {
        connect: Duration::from_secs(2),
        handshake: Duration::from_secs(2),
        unchoke: Duration::from_secs(2),
        block_progress: Duration::from_secs(2),
        write: Duration::from_secs(2),
    }
}

fn patient_timeouts() -> VodTimeouts {
    VodTimeouts {
        connect: Duration::from_secs(20),
        handshake: Duration::from_secs(20),
        unchoke: Duration::from_secs(20),
        block_progress: Duration::from_secs(20),
        write: Duration::from_secs(20),
    }
}

async fn run_service_rounds(
    concurrency: usize,
) -> (Vec<u8>, usize, Vec<usize>, usize, Vec<(usize, usize, u32)>) {
    let (content, info) = content_and_info();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let seeders = spawn_seeders(content.clone(), info.clone(), event_tx, false).await;
    let (tx, mut rx) = mpsc::channel::<Bytes>(PIECES);
    let download = tokio::spawn(download_vod_pieces_with_policy(
        info,
        seeders.addresses.clone(),
        tx,
        0,
        PIECES as u64,
        short_timeouts(),
        concurrency,
    ));

    let mut rounds = 0;
    let mut peer_piece_counts = vec![0; 2];
    let mut released_pieces = 0;
    let mut first_emit_round = None;
    let mut request_trace = Vec::new();
    let mut completed = Vec::with_capacity(PIECES * PIECE_LEN);
    while released_pieces < PIECES {
        let mut requests = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            requests.push(
                tokio::time::timeout(Duration::from_secs(10), event_rx.recv())
                    .await
                    .expect("controlled seeder should observe an assigned request")
                    .expect("seeders remain owned for the full download"),
            );
        }
        let peers: std::collections::HashSet<_> =
            requests.iter().map(|request| request.peer).collect();
        let pieces: std::collections::HashSet<_> =
            requests.iter().map(|request| request.piece).collect();
        assert_eq!(
            pieces.len(),
            concurrency,
            "a service round must serve distinct pieces"
        );
        if concurrency == 2 {
            assert_eq!(
                peers.len(),
                2,
                "both seeders must be assigned before the round is released"
            );
        }
        for request in requests {
            peer_piece_counts[request.peer] += 1;
            released_pieces += 1;
            request_trace.push((rounds + 1, request.peer, request.piece));
            request
                .release
                .send(())
                .expect("request is still waiting for its service round");
        }
        rounds += 1;
        if first_emit_round.is_none() {
            let piece = tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .expect("the frontier should emit after its first service round")
                .expect("the output receiver stays open");
            completed.extend_from_slice(&piece);
            first_emit_round = Some(rounds);
        }
    }
    while let Some(piece) = rx.recv().await {
        completed.extend_from_slice(&piece);
    }
    drop(rx);
    let result = tokio::time::timeout(Duration::from_secs(10), download)
        .await
        .expect("download must terminate after all controlled service rounds")
        .unwrap();
    assert!(
        result.is_ok(),
        "valid controlled seeders should complete: {result:?}"
    );
    drop(seeders);
    (
        completed,
        rounds,
        peer_piece_counts,
        first_emit_round.expect("at least one service round runs"),
        request_trace,
    )
}

async fn next_controlled_request(events: &mut mpsc::UnboundedReceiver<Request>) -> Request {
    tokio::time::timeout(Duration::from_secs(3), events.recv())
        .await
        .expect("workers should reach a controlled request")
        .expect("test-owned seeders stay alive")
}

#[tokio::test]
async fn two_peer_concurrency_halves_controlled_service_rounds() {
    let (serial_bytes, serial_rounds, _, serial_first_emit_round, serial_trace) =
        run_service_rounds(1).await;
    let (
        parallel_bytes,
        parallel_rounds,
        peer_piece_counts,
        parallel_first_emit_round,
        parallel_trace,
    ) = run_service_rounds(2).await;

    let expected = content_and_info().0;
    assert_eq!(serial_bytes, expected);
    assert_eq!(
        parallel_bytes, expected,
        "verified pieces must still emit in index order"
    );
    assert_eq!(
        serial_rounds, PIECES,
        "one-worker baseline serves one piece each round"
    );
    assert_eq!(
        parallel_rounds,
        PIECES / 2,
        "two workers should serve two pieces each round"
    );
    assert_eq!(serial_first_emit_round, 1);
    assert_eq!(parallel_first_emit_round, 1);
    assert_eq!(expected.len() / serial_rounds, PIECE_LEN);
    assert_eq!(expected.len() / parallel_rounds, 2 * PIECE_LEN);
    assert_eq!(serial_trace.len(), PIECES);
    assert_eq!(parallel_trace.len(), PIECES);
    assert!(serial_trace
        .iter()
        .all(|(round, _, _)| *round <= serial_rounds));
    assert!(parallel_trace
        .iter()
        .all(|(round, _, _)| *round <= parallel_rounds));
    let mut serial_piece_ids: Vec<_> = serial_trace.iter().map(|(_, _, piece)| *piece).collect();
    let mut parallel_piece_ids: Vec<_> =
        parallel_trace.iter().map(|(_, _, piece)| *piece).collect();
    serial_piece_ids.sort_unstable();
    parallel_piece_ids.sort_unstable();
    assert_eq!(serial_piece_ids, (0..PIECES as u32).collect::<Vec<_>>());
    assert_eq!(parallel_piece_ids, (0..PIECES as u32).collect::<Vec<_>>());
    assert!(
        peer_piece_counts.iter().all(|count| *count > 0),
        "both peers must contribute"
    );
}

#[tokio::test]
async fn stalled_frontier_bounds_window_and_emits_subrange_in_order_with_short_tail() {
    let (content, info) = content_and_info_with_length(PIECES * PIECE_LEN - 7);
    assert_eq!(info.piece_count(), PIECES as u64);
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let seeders = spawn_seeders(content.clone(), info.clone(), event_tx, false).await;
    let (tx, mut rx) = mpsc::channel::<Bytes>(1);
    let download = tokio::spawn(download_vod_pieces_with_policy(
        info,
        seeders.addresses.clone(),
        tx,
        1,
        PIECES as u64,
        patient_timeouts(),
        2,
    ));

    let mut frontier = None;
    let mut seen = std::collections::HashSet::new();
    while seen.len() < 4 {
        let request = next_controlled_request(&mut event_rx).await;
        let piece = request.piece;
        assert!((1..=4).contains(&piece));
        if piece == 1 {
            assert!(
                frontier.replace(request).is_none(),
                "piece one has one owner"
            );
        } else {
            request.release.send(()).unwrap();
        }
        seen.insert(piece);
    }
    let frontier = frontier.expect("requested range frontier must be assigned");
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        event_rx.try_recv().is_err(),
        "the four-credit window must not assign piece five while piece one stalls"
    );
    assert!(rx.try_recv().is_err(), "later verified pieces stay private");

    frontier.release.send(()).unwrap();
    let mut got = Vec::new();
    let expected_len = content.len() - PIECE_LEN;
    tokio::time::timeout(Duration::from_secs(5), async {
        while got.len() < expected_len {
            tokio::select! {
                Some(piece) = rx.recv() => got.extend_from_slice(&piece),
                Some(request) = event_rx.recv() => {
                    assert!((1..PIECES as u32).contains(&request.piece));
                    request.release.send(()).expect("later request remains owned");
                }
            }
        }
    })
    .await
    .expect("controller must release every later request while draining output");
    assert_eq!(got.len(), expected_len);
    drop(rx);
    tokio::time::timeout(Duration::from_secs(3), download)
        .await
        .expect("download should finish after the frontier is released")
        .unwrap()
        .unwrap();
    assert_eq!(got, content[PIECE_LEN..]);
    assert_eq!(got.len(), content.len() - PIECE_LEN);
    drop(seeders);
}

#[tokio::test]
async fn corrupt_peer_is_retired_and_verified_retry_emits_only_good_bytes() {
    let (content, info) = content_and_info_with_length(PIECE_LEN);
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let seeders = spawn_seeders(content.clone(), info.clone(), event_tx, true).await;
    let (tx, mut rx) = mpsc::channel::<Bytes>(1);
    let download = tokio::spawn(download_vod_pieces_with_policy(
        info,
        seeders.addresses.clone(),
        tx,
        0,
        1,
        patient_timeouts(),
        1,
    ));

    let mut peers = Vec::new();
    let mut got = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while got.len() < content.len() {
            tokio::select! {
                Some(request) = event_rx.recv() => {
                    peers.push(request.peer);
                    request.release.send(()).expect("request remains owned");
                }
                Some(piece) = rx.recv() => got.extend_from_slice(&piece),
            }
        }
    })
    .await
    .expect("healthy peer should verify and complete the retry");
    drop(rx);
    tokio::time::timeout(Duration::from_secs(3), download)
        .await
        .expect("successful retry should finish")
        .unwrap()
        .unwrap();

    assert_eq!(got, content, "the corrupt payload must never reach output");
    assert!(
        peers.contains(&0),
        "the first peer supplies the corrupt attempt"
    );
    assert!(
        peers.contains(&1),
        "the hash-failing peer is replaced by the healthy peer"
    );
    drop(seeders);
}
