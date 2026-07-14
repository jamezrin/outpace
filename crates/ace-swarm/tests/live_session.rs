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
    if i.is_multiple_of(188) {
        0x47
    } else {
        (i % 251) as u8
    }
}

#[tokio::test]
async fn live_session_emits_contiguous_ts_from_one_peer() {
    let infohash = [0x42u8; 20];
    // Tiny geometry for the test: 2 chunks/piece, 4 bytes/chunk.
    let info = StreamInfo {
        infohash,
        piece_length: 8,
        chunk_length: 4,
        trackers: vec![],
        metadata: Default::default(),
        sig_len: 0,
        source_pubkey: vec![],
    };
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
        while let Ok(PeerMessage::Unknown { id: 6, payload }) = sess.read_message().await {
            let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
            let chunk = u16::from_be_bytes(payload[8..10].try_into().unwrap());
            let off = ((piece - start_piece) as usize) * 8 + (chunk as usize) * 4;
            let mut block = vec![0u8; 8]; // 8-byte piece header
            block.extend_from_slice(&chunk.to_be_bytes());
            block.extend_from_slice(&content_peer[off..off + 4]);
            sess.send(&PeerMessage::Piece {
                index: 0,
                begin: piece,
                block,
            })
            .await
            .unwrap();
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

/// End-to-end #10: a source signs each piece (payload || RSA signature), a peer serves the
/// signed wire pieces, and the session — given the source's `pubkey` — verifies every piece's
/// in-band signature and emits only the (trailer-stripped) media payload. Proves the whole
/// `StreamInfo.source_pubkey -> reassembler verify -> emit` plumbing with real crypto.
#[tokio::test]
async fn live_session_verifies_signed_pieces_and_emits_only_media() {
    use ace_wire::live_auth::LiveSourceAuth;

    let infohash = [0x24u8; 20];
    let auth = LiveSourceAuth::generate();
    let sig_len = auth.signature_len(); // 96 for the generated 768-bit key
    let piece_length = 128usize; // > sig_len; media payload capacity is 32 bytes
    let chunk_length = 32usize; // 4 chunks/piece
    let media_per_piece = piece_length - sig_len;
    let start_piece = 10u32;
    let pieces = 3usize;

    // The original media (what a verified consumer must receive), and the signed wire pieces
    // (payload || signature) the peer actually serves.
    let media: Vec<u8> = (0..pieces * media_per_piece).map(ts_byte).collect();
    let signed_pieces: Vec<Vec<u8>> = (0..pieces)
        .map(|i| {
            let payload = &media[i * media_per_piece..(i + 1) * media_per_piece];
            let mut piece = payload.to_vec();
            piece.extend_from_slice(&auth.sign(payload));
            assert_eq!(piece.len(), piece_length);
            piece
        })
        .collect();

    let info = StreamInfo {
        infohash,
        piece_length: piece_length as u64,
        chunk_length: chunk_length as u64,
        trackers: vec![],
        metadata: Default::default(),
        sig_len,
        source_pubkey: auth.pubkey_der(),
    };

    let (client, mut server) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        let mut hs = [0u8; 66];
        server.read_exact(&mut hs).await.unwrap();
        server
            .write_all(&Handshake::new(infohash, *b"R30-MOCKLIVE-1234567").encode())
            .await
            .unwrap();
        let mut sess = PeerSession::new(server);
        let _ = sess.read_message().await.unwrap(); // our signed extended handshake
        sess.send(&PeerMessage::Unchoke).await.unwrap();
        while let Ok(PeerMessage::Unknown { id: 6, payload }) = sess.read_message().await {
            let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
            let chunk = u16::from_be_bytes(payload[8..10].try_into().unwrap());
            let off = chunk as usize * chunk_length;
            let data = &signed_pieces[(piece - start_piece) as usize][off..off + chunk_length];
            let mut block = vec![0u8; 8]; // 8-byte piece header
            block.extend_from_slice(&chunk.to_be_bytes());
            block.extend_from_slice(data);
            sess.send(&PeerMessage::Piece {
                index: 0,
                begin: piece,
                block,
            })
            .await
            .unwrap();
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
        head: start_piece as u64 + pieces as u64 - 1,
        identity: ace_wire::identity::Identity::generate(),
        peer_ip: None,
    };
    tokio::spawn(async move {
        let _ = LiveSession::run(session, info, cfg, tx).await;
    });

    let mut got = Vec::new();
    while got.len() < media.len() {
        match rx.recv().await {
            Some(b) => got.extend_from_slice(&b),
            None => break,
        }
    }
    assert_eq!(
        got, media,
        "verified session emits exactly the signed media payload, signatures stripped"
    );
}
