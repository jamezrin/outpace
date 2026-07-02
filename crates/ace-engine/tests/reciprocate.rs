//! A mock peer connects; we SERVE it a chunk we hold — proving reciprocal upload via the
//! public seeder API (the same logic wired into the download loop in ace_provider).
use ace_peer::session::PeerSession;
use ace_swarm::seed::SeederSession;
use ace_swarm::store::PieceStore;
use ace_wire::identity::Identity;
use ace_wire::live_codec::{chunk_request, LiveChunk};
use ace_wire::message::PeerMessage;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::test]
async fn peer_downloads_a_chunk_from_us() {
    let store = Arc::new(Mutex::new(PieceStore::new(4, 4, 1024)));
    store.lock().await.put_chunk(42, 0, &[7, 7, 7, 7]);

    let (client, server) = tokio::io::duplex(64 * 1024);
    let peer = tokio::spawn(async move {
        let mut p = PeerSession::new(client);
        p.send(&PeerMessage::Interested).await.unwrap();
        p.send(&chunk_request(42, 0)).await.unwrap();
        loop {
            if let m @ PeerMessage::Piece { .. } = p.read_message().await.unwrap() {
                return LiveChunk::from_message(&m).unwrap();
            }
        }
    });

    let mut us = PeerSession::new(server);
    let identity = Identity::generate();
    let serve_task = tokio::spawn(async move {
        let _ = SeederSession::serve(&mut us, store, [0u8; 8], &identity, [127, 0, 0, 1]).await;
    });
    let got = peer.await.unwrap();
    assert_eq!(
        got,
        LiveChunk {
            piece: 42,
            piece_header: [0u8; 8],
            chunk: 0,
            data: vec![7, 7, 7, 7]
        }
    );
    serve_task.abort();
}
