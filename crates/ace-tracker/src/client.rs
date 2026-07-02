//! Async BEP-15 announce: connect then announce over one UdpSocket, with timeout.
use crate::codec::{
    build_announce_request, build_connect_request, parse_announce_response, parse_connect_response,
    AnnounceEvent, TransferState,
};
use crate::{Result, TrackerError};
use std::net::{SocketAddr, SocketAddrV4};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::timeout;

const RECV_TIMEOUT: Duration = Duration::from_secs(8);

/// Announce to a UDP tracker and return its peer list.
#[allow(clippy::too_many_arguments)]
pub async fn announce(
    tracker: SocketAddrV4,
    infohash: &[u8; 20],
    peer_id: &[u8; 20],
    port: u16,
    num_want: i32,
    transfer: TransferState,
    event: AnnounceEvent,
) -> Result<Vec<SocketAddrV4>> {
    let sock = UdpSocket::bind("0.0.0.0:0").await?;
    sock.connect(SocketAddr::V4(tracker)).await?;

    // connect
    let ctxid: u32 = rand::random();
    sock.send(&build_connect_request(ctxid)).await?;
    let mut buf = [0u8; 4096];
    let n = recv(&sock, &mut buf).await?;
    let connection_id = parse_connect_response(&buf[..n], ctxid)?;

    // announce
    let atxid: u32 = rand::random();
    let req = build_announce_request(
        connection_id,
        atxid,
        infohash,
        peer_id,
        port,
        num_want,
        &transfer,
        event,
    );
    sock.send(&req).await?;
    let n = recv(&sock, &mut buf).await?;
    let (_interval, peers) = parse_announce_response(&buf[..n], atxid)?;
    Ok(peers)
}

async fn recv(sock: &UdpSocket, buf: &mut [u8]) -> Result<usize> {
    match timeout(RECV_TIMEOUT, sock.recv(buf)).await {
        Ok(r) => Ok(r?),
        Err(_) => Err(TrackerError::Timeout),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Deterministic: a local fake "tracker" that answers one connect + one announce.
    #[tokio::test]
    async fn announce_against_local_fake_tracker() {
        use tokio::net::UdpSocket;
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = server.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            // connect
            let (n, peer) = server.recv_from(&mut buf).await.unwrap();
            let txid = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
            assert_eq!(n, 16);
            let mut resp = Vec::new();
            resp.extend_from_slice(&0u32.to_be_bytes());
            resp.extend_from_slice(&txid.to_be_bytes());
            resp.extend_from_slice(&42u64.to_be_bytes()); // conn id
            server.send_to(&resp, peer).await.unwrap();
            // announce
            let (_n, peer) = server.recv_from(&mut buf).await.unwrap();
            let atxid = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
            let mut ar = Vec::new();
            ar.extend_from_slice(&1u32.to_be_bytes());
            ar.extend_from_slice(&atxid.to_be_bytes());
            ar.extend_from_slice(&1800u32.to_be_bytes());
            ar.extend_from_slice(&0u32.to_be_bytes());
            ar.extend_from_slice(&1u32.to_be_bytes());
            ar.extend_from_slice(&[9, 9, 9, 9]);
            ar.extend_from_slice(&1234u16.to_be_bytes());
            server.send_to(&ar, peer).await.unwrap();
        });

        let server_v4 = match server_addr {
            std::net::SocketAddr::V4(a) => a,
            _ => panic!("want v4"),
        };
        let peers = announce(
            server_v4,
            &[1u8; 20],
            &[2u8; 20],
            6881,
            50,
            TransferState::default(),
            AnnounceEvent::Started,
        )
        .await
        .unwrap();
        assert_eq!(peers, vec!["9.9.9.9:1234".parse().unwrap()]);
        handle.await.unwrap();
    }

    #[tokio::test]
    #[ignore] // live network: hits the real Acestream tracker
    async fn announce_against_real_tracker() {
        use tokio::net::lookup_host;
        let addr = lookup_host("t1.torrentstream.org:2710")
            .await
            .unwrap()
            .next()
            .unwrap();
        let v4 = match addr {
            std::net::SocketAddr::V4(a) => a,
            _ => panic!("want v4"),
        };
        let infohash = hex::decode("50e93529d3eb46a50506b14464185a15292d6e47").unwrap();
        let mut ih = [0u8; 20];
        ih.copy_from_slice(&infohash);
        let peers = announce(
            v4,
            &ih,
            &[7u8; 20],
            6881,
            50,
            TransferState::default(),
            AnnounceEvent::Started,
        )
        .await
        .unwrap();
        println!("live tracker returned {} peers", peers.len());
    }
}
