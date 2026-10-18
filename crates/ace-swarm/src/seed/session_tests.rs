use super::*;

#[tokio::test]
async fn choked_peer_id6_and_bt_request_are_unanswered() {
    let store = Arc::new(Mutex::new(PieceStore::new(8, 4, 1024)));
    store.lock().await.put_chunk(7, 0, b"data");
    let (client, server) = tokio::io::duplex(4096);
    let identity = Identity::generate();
    let coordinator = ServeCoordinator::new(0);
    let mut serving = PeerSession::new(server);
    let task = tokio::spawn(async move {
        SeederSession::serve(
            &mut serving,
            Some(store),
            None,
            [0; 8],
            &identity,
            [127, 0, 0, 1],
            Some(coordinator),
        )
        .await
    });
    let mut peer = PeerSession::new(client).with_timeout(std::time::Duration::from_millis(100));
    let _ = peer.read_message().await.unwrap(); // signed handshake
    let mut id6 = 0u32.to_be_bytes().to_vec();
    id6.extend_from_slice(&7u32.to_be_bytes());
    id6.extend_from_slice(&0u16.to_be_bytes());
    peer.send(&PeerMessage::Unknown {
        id: 6,
        payload: id6,
    })
    .await
    .unwrap();
    peer.send(&PeerMessage::Request {
        index: 7,
        begin: 0,
        length: 4,
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(80), peer.read_message())
            .await
            .is_err()
    );
    task.abort();
}

#[tokio::test]
async fn legacy_serve_unchokes_only_after_first_interested() {
    let (client, server) = tokio::io::duplex(4096);
    let identity = Identity::generate();
    let mut serving = PeerSession::new(server);
    let task = tokio::spawn(async move {
        SeederSession::serve(
            &mut serving,
            None,
            None,
            [0; 8],
            &identity,
            [127, 0, 0, 1],
            None,
        )
        .await
    });
    let mut peer = PeerSession::new(client).with_timeout(std::time::Duration::from_millis(250));
    let _ = peer.read_message().await.unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), peer.read_message())
            .await
            .is_err()
    );
    peer.send(&PeerMessage::Interested).await.unwrap();
    assert_eq!(peer.read_message().await.unwrap(), PeerMessage::Unchoke);
    task.abort();
}
