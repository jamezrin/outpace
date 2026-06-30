//! Real-TCP loopback proof: a client connects to our PeerListener, handshakes, requests a
//! chunk we hold, and receives it back via SeederSession — the inbound half of reciprocal
//! seeding (S1 proved the outbound half).
use ace_peer::session::PeerSession;
use ace_swarm::listen::{PeerListener, SeedRegistry};
use ace_swarm::store::PieceStore;
use ace_wire::handshake::random_peer_id;
use ace_wire::live_codec::{chunk_request, LiveChunk};
use ace_wire::message::PeerMessage;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

#[tokio::test]
async fn peer_connects_handshakes_and_downloads_a_chunk_from_us() {
    let infohash = [0x77u8; 20];
    let store = Arc::new(Mutex::new(PieceStore::new(4, 4, 1024)));
    store.lock().await.put_chunk(9, 0, &[5, 5, 5, 5]);

    let registry = SeedRegistry::new();
    registry.register(infohash, store);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let our_id = random_peer_id();
    tokio::spawn(PeerListener::serve(listener, registry, our_id, [0u8; 8], 8));

    let stream = TcpStream::connect(addr).await.unwrap();
    let mut client = PeerSession::new(stream);
    client.perform_handshake(infohash, random_peer_id()).await.unwrap();
    client.send(&PeerMessage::Interested).await.unwrap();
    client.send(&chunk_request(9, 0)).await.unwrap();

    loop {
        if let m @ PeerMessage::Piece { .. } = client.read_message().await.unwrap() {
            let lc = LiveChunk::from_message(&m).unwrap();
            assert_eq!(lc, LiveChunk { piece: 9, chunk: 0, data: vec![5, 5, 5, 5] });
            break;
        }
    }
}

#[tokio::test]
async fn unknown_infohash_is_refused_not_served() {
    let registry = SeedRegistry::new(); // empty — serves nothing
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(PeerListener::serve(listener, registry, random_peer_id(), [0u8; 8], 8));

    let stream = TcpStream::connect(addr).await.unwrap();
    // `with_timeout` is a safety net, not the expected failure mode: on refusal,
    // `handle_inbound` returns immediately without replying, dropping the stream — so the
    // client's read sees an immediate EOF, not a stalled connection.
    let mut client = PeerSession::new(stream).with_timeout(std::time::Duration::from_millis(300));
    let result = client.perform_handshake([0xAAu8; 20], random_peer_id()).await;
    assert!(result.is_err(), "must not reply to an infohash we don't serve");
}
