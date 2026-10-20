use super::*;

#[tokio::test]
async fn seeder_announce_never_fires_without_an_inbound_port() {
    // `None` (no inbound listener to back an advertisable endpoint) must never even attempt
    // a tracker announce, which would otherwise invite peers to dial a port nobody serves.
    let res = tokio::time::timeout(
        std::time::Duration::from_millis(50),
        announce_seeder_periodically(info(), tokio::sync::watch::channel(None).1),
    )
    .await;
    assert!(res.is_err(), "must never resolve without an inbound port");
}

#[tokio::test]
async fn closed_inbound_port_watch_keeps_self_announce_pending() {
    let (port_tx, port_rx) = tokio::sync::watch::channel(None);
    drop(port_tx);

    let res = tokio::time::timeout(
        std::time::Duration::from_millis(50),
        announce_infohash_periodically_dynamic(vec![], [0; 20], port_rx),
    )
    .await;

    assert!(
        res.is_err(),
        "a closed disabled-port watch must not cancel the live follower"
    );
}

#[test]
fn leech_self_announce_uses_the_peer_port_never_the_http_port() {
    // Regression for #21: a leeching provider configured the way `build_runtime` wires it
    // (peer_listen 8621, HTTP/bind 6878, inbound enabled) must advertise the PEER port on
    // its seeder self-announce — dialing the HTTP port would speak the wrong protocol.
    const PEER_PORT: u16 = 8621;
    const HTTP_PORT: u16 = 6878;
    let p = AceProvider::new(Arc::new(Identity::generate()), PEER_PORT)
        .with_inbound_announce_port(Some(PEER_PORT));
    assert_eq!(p.seeder_announce_port(), Some(PEER_PORT));
    assert_ne!(
        p.seeder_announce_port(),
        Some(HTTP_PORT),
        "must never advertise the HTTP API port"
    );
    // Discovery announces (which also register us as a peer) use the same resolved port.
    assert_eq!(p.discovery_announce_port(), PEER_PORT);

    // Without an inbound listener (the default, and `enable_inbound = false`), we advertise
    // no dial-able endpoint at all — matching the broadcast path's `inbound_peer_port`.
    let leech_only = AceProvider::new(Arc::new(Identity::generate()), PEER_PORT);
    assert_eq!(leech_only.seeder_announce_port(), None);
    assert_eq!(leech_only.discovery_announce_port(), 0);
}

#[tokio::test]
async fn resolved_or_disabled_port_is_encoded_in_the_tracker_discovery_announce() {
    use tokio::net::UdpSocket;
    const LOCAL_PORT: u16 = 8621;
    const MAPPED_PORT: u16 = 48621;
    let (_port_tx, port_rx) = tokio::sync::watch::channel(Some(MAPPED_PORT));
    let provider = AceProvider::new(Arc::new(Identity::generate()), LOCAL_PORT)
        .with_inbound_announce_port_receiver(port_rx);
    assert_eq!(provider.discovery_announce_port(), MAPPED_PORT);

    async fn capture_wire_port(port: u16) -> u16 {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = match server.local_addr().unwrap() {
            std::net::SocketAddr::V4(addr) => addr,
            _ => unreachable!(),
        };
        let capture = tokio::spawn(async move {
            let mut buf = [0_u8; 2048];
            let (_, peer) = server.recv_from(&mut buf).await.unwrap();
            let txid = u32::from_be_bytes(buf[12..16].try_into().unwrap());
            let mut response = Vec::new();
            response.extend_from_slice(&0_u32.to_be_bytes());
            response.extend_from_slice(&txid.to_be_bytes());
            response.extend_from_slice(&42_u64.to_be_bytes());
            server.send_to(&response, peer).await.unwrap();
            let (len, peer) = server.recv_from(&mut buf).await.unwrap();
            assert_eq!(len, 98);
            let announced_port = u16::from_be_bytes(buf[96..98].try_into().unwrap());
            let txid = u32::from_be_bytes(buf[12..16].try_into().unwrap());
            let mut response = Vec::new();
            response.extend_from_slice(&1_u32.to_be_bytes());
            response.extend_from_slice(&txid.to_be_bytes());
            response.extend_from_slice(&1800_u32.to_be_bytes());
            response.extend_from_slice(&0_u32.to_be_bytes());
            response.extend_from_slice(&0_u32.to_be_bytes());
            server.send_to(&response, peer).await.unwrap();
            announced_port
        });
        ace_tracker::client::announce(
            addr,
            &[1_u8; 20],
            &[2_u8; 20],
            port,
            50,
            ace_tracker::codec::TransferState::default(),
            ace_tracker::codec::AnnounceEvent::Started,
        )
        .await
        .unwrap();
        capture.await.unwrap()
    }
    assert_eq!(
        capture_wire_port(provider.discovery_announce_port()).await,
        MAPPED_PORT
    );
    assert_ne!(MAPPED_PORT, LOCAL_PORT);
    let inbound_disabled = AceProvider::new(Arc::new(Identity::generate()), LOCAL_PORT);
    assert_eq!(
        capture_wire_port(inbound_disabled.discovery_announce_port()).await,
        0
    );
}
