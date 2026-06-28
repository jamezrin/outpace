//! Async peer session over any AsyncRead+AsyncWrite stream.
use crate::{PeerError, Result};
use ace_wire::handshake::{Handshake, HANDSHAKE_LEN};
use ace_wire::message::PeerMessage;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub struct PeerSession<S> {
    stream: S,
    /// Bytes read from the stream but not yet consumed into a message.
    buf: Vec<u8>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> PeerSession<S> {
    pub fn new(stream: S) -> Self {
        PeerSession { stream, buf: Vec::with_capacity(32 * 1024) }
    }

    /// Send our handshake, read the peer's, and verify the infohash matches.
    pub async fn perform_handshake(
        &mut self, infohash: [u8; 20], peer_id: [u8; 20],
    ) -> Result<Handshake> {
        let ours = Handshake::new(infohash, peer_id);
        self.stream.write_all(&ours.encode()).await?;
        let mut hs = [0u8; HANDSHAKE_LEN];
        self.stream.read_exact(&mut hs).await?;
        let peer = Handshake::decode(&hs)?;
        if peer.infohash != infohash {
            return Err(PeerError::InfohashMismatch);
        }
        Ok(peer)
    }

    /// Send a peer message.
    pub async fn send(&mut self, msg: &PeerMessage) -> Result<()> {
        self.stream.write_all(&msg.encode()).await?;
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
            let n = self.stream.read(&mut chunk).await?;
            if n == 0 {
                return Err(PeerError::Closed);
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }
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
