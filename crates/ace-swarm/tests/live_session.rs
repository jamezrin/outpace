//! LiveSession single-peer continuous download: a mock peer accepts our handshake,
//! unchokes, and serves Acestream chunk requests with TS-bearing Piece messages; assert
//! the session emits the original contiguous TS.

use ace_peer::session::PeerSession;
use ace_swarm::live::{LiveConfig, LiveSession};
use ace_swarm::types::StreamInfo;
use ace_wire::handshake::Handshake;
use ace_wire::message::PeerMessage;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn ts_byte(i: usize) -> u8 {
    if i % 188 == 0 { 0x47 } else { (i % 251) as u8 }
}

#[tokio::test]
async fn live_session_emits_contiguous_ts_from_one_peer() {
    let infohash = [0x42u8; 20];
    // Tiny geometry for the test: 2 chunks/piece, 4 bytes/chunk.
    let info = StreamInfo { infohash, piece_length: 8, chunk_length: 4, trackers: vec![] };
    let start_piece = 10u32;
    let pieces = 3u32;
    let content: Vec<u8> = (0..(pieces as usize * 8)).map(ts_byte).collect();
    let content_peer = content.clone();

    let (client, mut server) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        let mut hs = [0u8; 66];
        server.read_exact(&mut hs).await.unwrap();
        server
            .write_all(&Handshake::new(infohash, *b"R30-MOCKLIVE-1234567").encode())
            .await
            .unwrap();
        let mut sess = PeerSession::new(server);
        // our signed extended handshake
        let _ = sess.read_message().await.unwrap();
        sess.send(&PeerMessage::Unchoke).await.unwrap();
        // serve every chunk request from the contiguous content
        loop {
            match sess.read_message().await {
                Ok(PeerMessage::Unknown { id: 6, payload }) => {
                    let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                    let chunk = u16::from_be_bytes(payload[8..10].try_into().unwrap());
                    let off = ((piece - start_piece) as usize) * 8 + (chunk as usize) * 4;
                    let mut block = vec![0u8; 8]; // 8-byte piece header
                    block.extend_from_slice(&chunk.to_be_bytes());
                    block.extend_from_slice(&content_peer[off..off + 4]);
                    sess.send(&PeerMessage::Piece { index: 0, begin: piece, block }).await.unwrap();
                }
                _ => break,
            }
        }
    });

    let mut session = PeerSession::new(client);
    session
        .perform_handshake(infohash, ace_wire::handshake::random_peer_id())
        .await
        .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(64);
    let cfg = LiveConfig {
        start_piece: start_piece as u64,
        head: (start_piece + pieces - 1) as u64,
        identity: ace_wire::identity::Identity::generate(),
        peer_ip: None,
    };
    tokio::spawn(async move {
        let _ = LiveSession::run(session, info, cfg, tx).await;
    });

    let mut got = Vec::new();
    while got.len() < content.len() {
        match rx.recv().await {
            Some(b) => got.extend_from_slice(&b),
            None => break,
        }
    }
    assert_eq!(got, content);
}
