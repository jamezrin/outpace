//! Async peer session over any AsyncRead+AsyncWrite stream.
use crate::{PeerError, Result};
use ace_wire::handshake::{Handshake, HANDSHAKE_LEN};
use ace_wire::message::PeerMessage;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Default ceiling for any single peer I/O operation (connect, handshake, read).
///
/// Without this, a stalled peer that holds the connection open but sends nothing
/// would block a session task indefinitely — a real hazard once swarm orchestration
/// fans out across many flaky peers.
pub const DEFAULT_PEER_TIMEOUT: Duration = Duration::from_secs(20);

pub struct PeerSession<S> {
    stream: S,
    /// Bytes read from the stream but not yet consumed into a message.
    buf: Vec<u8>,
    /// Ceiling applied to each individual I/O await (handshake, send, read).
    timeout: Duration,
}

impl<S: AsyncRead + AsyncWrite + Unpin> PeerSession<S> {
    pub fn new(stream: S) -> Self {
        PeerSession {
            stream,
            buf: Vec::with_capacity(32 * 1024),
            timeout: DEFAULT_PEER_TIMEOUT,
        }
    }

    /// Override the per-operation I/O timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Send our handshake, read the peer's, and verify the infohash matches.
    pub async fn perform_handshake(
        &mut self, infohash: [u8; 20], peer_id: [u8; 20],
    ) -> Result<Handshake> {
        let ours = Handshake::new(infohash, peer_id);
        let timeout = self.timeout;
        with_timeout(timeout, self.stream.write_all(&ours.encode())).await??;
        let mut hs = [0u8; HANDSHAKE_LEN];
        with_timeout(timeout, self.stream.read_exact(&mut hs)).await??;
        let peer = Handshake::decode(&hs)?;
        if peer.infohash != infohash {
            return Err(PeerError::InfohashMismatch);
        }
        Ok(peer)
    }

    /// Send a peer message.
    pub async fn send(&mut self, msg: &PeerMessage) -> Result<()> {
        with_timeout(self.timeout, self.stream.write_all(&msg.encode())).await??;
        Ok(())
    }

    /// Read exactly one peer message, buffering until a full frame is available.
    pub async fn read_message(&mut self) -> Result<PeerMessage> {
        loop {
            if let Some((msg, consumed)) = PeerMessage::decode(&self.buf)? {
                self.buf.drain(..consumed);
                return Ok(msg);
            }
            let mut chunk = [0u8; 16 * 1024];
            let n = with_timeout(self.timeout, self.stream.read(&mut chunk)).await??;
            if n == 0 {
                return Err(PeerError::Closed);
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }
}

/// Apply `dur` as a ceiling to `fut`, mapping elapsed time to `PeerError::Timeout`.
async fn with_timeout<F, T>(dur: Duration, fut: F) -> Result<T>
where
    F: std::future::Future<Output = T>,
{
    tokio::time::timeout(dur, fut)
        .await
        .map_err(|_| PeerError::Timeout)
}

use tokio::net::TcpStream;

/// Connect a TCP peer session to `addr` (e.g. "1.2.3.4:8621").
///
/// The TCP connect is bounded by [`DEFAULT_PEER_TIMEOUT`] so an unreachable peer
/// can't stall a connecting task indefinitely.
pub async fn connect(addr: &str) -> Result<PeerSession<TcpStream>> {
    let stream = with_timeout(DEFAULT_PEER_TIMEOUT, TcpStream::connect(addr)).await??;
    Ok(PeerSession::new(stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ace_wire::handshake::Handshake;
    use ace_wire::message::PeerMessage;

    #[tokio::test]
    async fn handshake_then_read_extended_over_duplex() {
        let (client, mut server) = tokio::io::duplex(4096);
        let infohash = [0x11u8; 20];

        // The "server" side acts like a real peer: read our handshake, reply with its
        // own (same infohash), then send an extended-handshake message.
        let srv = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut hs = [0u8; 66];
            server.read_exact(&mut hs).await.unwrap();
            let peer_hs = Handshake::new(infohash, *b"R30------SERVERPEERx");
            server.write_all(&peer_hs.encode()).await.unwrap();
            let ext = PeerMessage::Extended { ext_id: 0, payload: b"d1:md11:ut_metadatai2eee".to_vec() };
            server.write_all(&ext.encode()).await.unwrap();
        });

        let mut session = PeerSession::new(client);
        let got = session.perform_handshake(infohash, *b"R30------CLIENTPEERy").await.unwrap();
        assert_eq!(got.infohash, infohash);

        let msg = session.read_message().await.unwrap();
        match msg {
            PeerMessage::Extended { ext_id, payload } => {
                assert_eq!(ext_id, 0);
                assert_eq!(payload, b"d1:md11:ut_metadatai2eee");
            }
            other => panic!("unexpected message: {other:?}"),
        }
        srv.await.unwrap();
    }

    #[tokio::test]
    #[ignore] // live network: connect to a real Acestream peer and exchange handshakes
    async fn live_interop_handshake() {
        // Provide a current peer + infohash via env, since live peers churn:
        //   ACE_PEER=82.213.234.240:8623 ACE_INFOHASH=47eda3..afa022 cargo test -p ace-peer live_interop -- --ignored --nocapture
        let peer = std::env::var("ACE_PEER").expect("set ACE_PEER=ip:port");
        let ih_hex = std::env::var("ACE_INFOHASH").expect("set ACE_INFOHASH=40hex");
        let mut ih = [0u8; 20];
        ih.copy_from_slice(&hex::decode(ih_hex).unwrap());
        let mut session = connect(&peer).await.unwrap();
        let hs = session
            .perform_handshake(ih, ace_wire::handshake::random_peer_id())
            .await
            .unwrap();
        assert_eq!(hs.infohash, ih);
        // Read the next message the peer sends (typically the extended handshake).
        let msg = session.read_message().await.unwrap();
        println!("live peer accepted handshake; first message: {msg:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn read_message_times_out_on_silent_peer() {
        // Keep `_server` alive so the read pends (open but idle) rather than EOF-ing.
        let (client, _server) = tokio::io::duplex(4096);
        let mut session =
            PeerSession::new(client).with_timeout(std::time::Duration::from_secs(5));
        let res = session.read_message().await;
        assert!(matches!(res, Err(PeerError::Timeout)));
    }

    #[tokio::test(start_paused = true)]
    async fn perform_handshake_times_out_when_peer_never_replies() {
        let (client, _server) = tokio::io::duplex(4096);
        let mut session =
            PeerSession::new(client).with_timeout(std::time::Duration::from_secs(5));
        let res = session
            .perform_handshake([0x11u8; 20], *b"R30------CLIENTPEERy")
            .await;
        assert!(matches!(res, Err(PeerError::Timeout)));
    }

    #[tokio::test]
    async fn handshake_rejects_wrong_infohash() {
        let (client, mut server) = tokio::io::duplex(4096);
        let srv = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut hs = [0u8; 66];
            server.read_exact(&mut hs).await.unwrap();
            // reply with a DIFFERENT infohash
            let peer_hs = Handshake::new([0x22u8; 20], *b"R30------SERVERPEERx");
            server.write_all(&peer_hs.encode()).await.unwrap();
        });
        let mut session = PeerSession::new(client);
        let res = session.perform_handshake([0x11u8; 20], *b"R30------CLIENTPEERy").await;
        assert!(matches!(res, Err(PeerError::InfohashMismatch)));
        srv.await.unwrap();
    }
}
