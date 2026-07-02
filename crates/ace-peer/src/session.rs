//! Async peer session over any AsyncRead+AsyncWrite stream.
use crate::{PeerError, Result};
use ace_wire::handshake::{Handshake, HANDSHAKE_LEN, HANDSHAKE_PREFIX_LEN};
use ace_wire::message::PeerMessage;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Default ceiling for any single peer I/O operation (connect, handshake, read).
///
/// Without this, a stalled peer that holds the connection open but sends nothing
/// would block a session task indefinitely — a real hazard once swarm orchestration
/// fans out across many flaky peers.
pub const DEFAULT_PEER_TIMEOUT: Duration = Duration::from_secs(20);
const INBOUND_PEER_ID_GRACE: Duration = Duration::from_millis(20);

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
        &mut self,
        infohash: [u8; 20],
        peer_id: [u8; 20],
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

    /// Inbound handshake: read the peer's 66-byte handshake first, then — only if `serves`
    /// returns true for the requested infohash — reply with ours. Used by the seeder listener
    /// to avoid replying (and thus leaking which infohashes we serve) to a peer asking for one
    /// we don't have. Returns the peer's handshake on success.
    pub async fn accept_handshake<F>(
        &mut self,
        our_peer_id: [u8; 20],
        serves: F,
    ) -> Result<Handshake>
    where
        F: FnOnce(&[u8; 20]) -> bool,
    {
        let timeout = self.timeout;
        let mut prefix = [0u8; HANDSHAKE_PREFIX_LEN];
        with_timeout(timeout, self.stream.read_exact(&mut prefix)).await??;
        let mut peer = Handshake::decode_prefix(&prefix)?;
        if !serves(&peer.infohash) {
            return Err(PeerError::InfohashMismatch);
        }
        let mut peer_id_seen = false;
        if let Some(peer_id) = self.read_optional_inbound_peer_id().await? {
            peer.peer_id = peer_id;
            peer_id_seen = true;
        }
        let ours = Handshake::new(peer.infohash, our_peer_id);
        with_timeout(timeout, self.stream.write_all(&ours.encode())).await??;
        if !peer_id_seen {
            if let Some(peer_id) = self.read_optional_deferred_peer_id().await? {
                peer.peer_id = peer_id;
            }
        }
        Ok(peer)
    }

    async fn read_optional_inbound_peer_id(&mut self) -> Result<Option<[u8; 20]>> {
        let mut peer_id = [0u8; 20];
        let n = match tokio::time::timeout(INBOUND_PEER_ID_GRACE, self.stream.read(&mut peer_id))
            .await
        {
            Ok(r) => r?,
            Err(_) => return Ok(None),
        };
        if n == 0 {
            return Ok(None);
        }
        if n < peer_id.len() {
            with_timeout(self.timeout, self.stream.read_exact(&mut peer_id[n..])).await??;
        }
        Ok(Some(peer_id))
    }

    async fn read_optional_deferred_peer_id(&mut self) -> Result<Option<[u8; 20]>> {
        let mut peer_id = [0u8; 20];
        let n = match tokio::time::timeout(INBOUND_PEER_ID_GRACE, self.stream.read(&mut peer_id))
            .await
        {
            Ok(r) => r?,
            Err(_) => return Ok(None),
        };
        if n == 0 {
            return Ok(None);
        }
        if !looks_like_acestream_peer_id_prefix(&peer_id[..n]) {
            self.buf.extend_from_slice(&peer_id[..n]);
            return Ok(None);
        }
        if n < peer_id.len() {
            with_timeout(self.timeout, self.stream.read_exact(&mut peer_id[n..])).await??;
        }
        Ok(Some(peer_id))
    }

    /// Send a peer message.
    pub async fn send(&mut self, msg: &PeerMessage) -> Result<()> {
        with_timeout(self.timeout, self.stream.write_all(&msg.encode())).await??;
        Ok(())
    }

    /// Send our BEP-10 extended handshake (id 20, sub-id 0), unsigned (no node identity).
    /// Real peers require a signed identity — see [`send_signed_extended_handshake`].
    ///
    /// [`send_signed_extended_handshake`]: Self::send_signed_extended_handshake
    pub async fn send_extended_handshake(
        &mut self,
        hs: &ace_wire::extended::OutgoingExtendedHandshake,
    ) -> Result<()> {
        let msg = PeerMessage::Extended {
            ext_id: 0,
            payload: hs.encode_payload(),
        };
        self.send(&msg).await
    }

    /// Send our extended handshake carrying our node identity + a valid signature
    /// (note 17), which is what peers require before they will unchoke us.
    pub async fn send_signed_extended_handshake(
        &mut self,
        hs: &ace_wire::extended::OutgoingExtendedHandshake,
        identity: &ace_wire::identity::Identity,
    ) -> Result<()> {
        let msg = PeerMessage::Extended {
            ext_id: 0,
            payload: hs.sign_and_encode(identity),
        };
        self.send(&msg).await
    }

    /// Fetch the BEP-9 `ut_metadata` blob (for Acestream, the `AceStreamTransport` file):
    /// request every 16 KiB piece from the peer and assemble the data blocks into a single
    /// buffer of `metadata_size` bytes.
    ///
    /// `peer_ut_metadata_id` is the ext id the peer assigned to `ut_metadata` in its extended
    /// handshake `m` dict; the BEP-10 handshake must already have been exchanged. Unrelated
    /// messages are ignored; a `Reject` or an early close is an error.
    pub async fn fetch_metadata(
        &mut self,
        peer_ut_metadata_id: u8,
        metadata_size: usize,
    ) -> Result<Vec<u8>> {
        use ace_wire::ut_metadata::{
            piece_count, request_piece, MetadataMessage, METADATA_BLOCK_LEN,
        };
        let pieces = piece_count(metadata_size);
        if pieces == 0 {
            return Ok(Vec::new());
        }
        for i in 0..pieces {
            let payload = request_piece(i as i64);
            self.send(&PeerMessage::Extended {
                ext_id: peer_ut_metadata_id,
                payload,
            })
            .await?;
        }
        let mut blob = vec![0u8; metadata_size];
        let mut have = vec![false; pieces];
        let mut remaining = pieces;
        while remaining > 0 {
            match self.read_message().await? {
                PeerMessage::Extended { ext_id, payload } if ext_id != 0 => {
                    match MetadataMessage::parse(&payload) {
                        Some(MetadataMessage::Data { piece, data, .. }) => {
                            let idx = piece as usize;
                            if piece < 0 || idx >= pieces {
                                continue;
                            }
                            let off = idx * METADATA_BLOCK_LEN;
                            let end = (off + data.len()).min(metadata_size);
                            if off < end {
                                blob[off..end].copy_from_slice(&data[..end - off]);
                            }
                            if !have[idx] {
                                have[idx] = true;
                                remaining -= 1;
                            }
                        }
                        Some(MetadataMessage::Reject { .. }) => {
                            return Err(PeerError::Protocol("metadata request rejected"));
                        }
                        _ => {}
                    }
                }
                _ => {} // ignore non-metadata traffic during the fetch
            }
        }
        Ok(blob)
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

fn looks_like_acestream_peer_id_prefix(bytes: &[u8]) -> bool {
    const PREFIX: &[u8] = b"R30------";
    bytes.len() <= PREFIX.len() && PREFIX.starts_with(bytes)
        || bytes.len() > PREFIX.len() && bytes.starts_with(PREFIX)
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
            let ext = PeerMessage::Extended {
                ext_id: 0,
                payload: b"d1:md11:ut_metadatai2eee".to_vec(),
            };
            server.write_all(&ext.encode()).await.unwrap();
        });

        let mut session = PeerSession::new(client);
        let got = session
            .perform_handshake(infohash, *b"R30------CLIENTPEERy")
            .await
            .unwrap();
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

    #[tokio::test]
    async fn send_extended_handshake_frames_a_decodable_bep10_message() {
        use ace_wire::extended::{ExtendedHandshake, OutgoingExtendedHandshake};

        let (client, mut server) = tokio::io::duplex(4096);
        let srv = tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            // Read one full peer frame the way a real peer would.
            let mut hdr = [0u8; 4];
            server.read_exact(&mut hdr).await.unwrap();
            let len = u32::from_be_bytes(hdr) as usize;
            let mut body = vec![0u8; len];
            server.read_exact(&mut body).await.unwrap();
            let mut frame = hdr.to_vec();
            frame.extend_from_slice(&body);
            PeerMessage::decode(&frame).unwrap().unwrap().0
        });

        let mut session = PeerSession::new(client);
        let hs = OutgoingExtendedHandshake {
            ace_metadata_version: 1,
            ut_metadata_id: 2,
            mi: None,
            node: Default::default(),
            peer_ip: None,
            metadata_size: None,
        };
        session.send_extended_handshake(&hs).await.unwrap();

        match srv.await.unwrap() {
            PeerMessage::Extended { ext_id, payload } => {
                assert_eq!(ext_id, 0); // BEP-10 handshake sub-id
                let parsed = ExtendedHandshake::parse(&payload).unwrap();
                assert_eq!(parsed.ace_metadata_version, Some(1));
                assert_eq!(parsed.ut_metadata_id(), Some(2));
            }
            other => panic!("expected Extended, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn read_message_times_out_on_silent_peer() {
        // Keep `_server` alive so the read pends (open but idle) rather than EOF-ing.
        let (client, _server) = tokio::io::duplex(4096);
        let mut session = PeerSession::new(client).with_timeout(std::time::Duration::from_secs(5));
        let res = session.read_message().await;
        assert!(matches!(res, Err(PeerError::Timeout)));
    }

    #[tokio::test(start_paused = true)]
    async fn perform_handshake_times_out_when_peer_never_replies() {
        let (client, _server) = tokio::io::duplex(4096);
        let mut session = PeerSession::new(client).with_timeout(std::time::Duration::from_secs(5));
        let res = session
            .perform_handshake([0x11u8; 20], *b"R30------CLIENTPEERy")
            .await;
        assert!(matches!(res, Err(PeerError::Timeout)));
    }

    #[tokio::test]
    #[ignore] // live network: drive past the handshake and observe unchoke/piece behavior
    async fn live_recon_unchoke() {
        // Phase 3.2 recon. Capture hot peers from the engine namespace, then e.g.:
        //   ACE_PEER=82.213.234.240:8623 ACE_INFOHASH=47eda3..afa022 \
        //     cargo test -p ace-peer live_recon -- --ignored --nocapture
        use ace_wire::extended::{ExtendedHandshake, LivePosition, OutgoingExtendedHandshake};
        use ace_wire::live_codec::chunk_request;

        let peer = std::env::var("ACE_PEER").expect("set ACE_PEER=ip:port");
        let ih_hex = std::env::var("ACE_INFOHASH").expect("set ACE_INFOHASH=40hex");
        let mut ih = [0u8; 20];
        ih.copy_from_slice(&hex::decode(ih_hex).unwrap());

        let mut session = connect(&peer)
            .await
            .unwrap()
            .with_timeout(Duration::from_secs(10));
        session
            .perform_handshake(ih, ace_wire::handshake::random_peer_id())
            .await
            .unwrap();
        println!("[recon {peer}] handshake accepted");

        // The peer's first message is its extended handshake — read its live window.
        let first = session.read_message().await.unwrap();
        let mut their_pos: Option<LivePosition> = None;
        if let PeerMessage::Extended {
            ext_id: 0,
            ref payload,
        } = first
        {
            if let Ok(eh) = ExtendedHandshake::parse(payload) {
                let top = |k: &[u8]| eh.raw.get(k).and_then(|v| v.as_int());
                println!(
                    "[recon {peer}] their identity: ts={:?} v={:?} pv={:?} p={:?} nt={:?} platform={:?}",
                    top(b"ts"), top(b"v"), top(b"pv"), top(b"p"), top(b"nt"), top(b"platform"),
                );
                let mi = eh.raw.get(b"mi");
                let get = |k: &[u8]| mi.and_then(|m| m.get(k)).and_then(|v| v.as_int());
                println!(
                    "[recon {peer}] mi: min={:?} max={:?} pos={:?} dist={:?}",
                    get(b"min_piece"),
                    get(b"max_piece"),
                    get(b"position"),
                    get(b"distance_from_source"),
                );
                if let (Some(min), Some(max), Some(pos)) =
                    (get(b"min_piece"), get(b"max_piece"), get(b"position"))
                {
                    their_pos = Some(LivePosition {
                        min_piece: min,
                        max_piece: max,
                        position: pos,
                        distance_from_source: get(b"distance_from_source").unwrap_or(99),
                    });
                }
            }
        } else {
            println!("[recon {peer}] first message was not an extended handshake: {first:?}");
        }

        // Experiment knobs (env): ACE_NO_MI=1 sends no mi; ACE_DIST=N overrides the
        // advertised distance_from_source; ACE_NO_INTERESTED=1 holds without interest.
        let env = |k: &str| std::env::var(k).ok();
        let mi = if env("ACE_NO_MI").is_some() {
            None
        } else {
            their_pos.map(|mut p| {
                if let Some(d) = env("ACE_DIST").and_then(|v| v.parse().ok()) {
                    p.distance_from_source = d;
                }
                p
            })
        };
        let has_mi = mi.is_some();
        // Mint our own Ed25519 identity (node_id = our pubkey).
        let identity = ace_wire::identity::Identity::generate();
        let ts: i64 = env("ACE_TS").and_then(|v| v.parse().ok()).unwrap_or(1);

        // The peer's IP (for `yourip`), parsed from the ip:port we connected to.
        let peer_ip: Option<[u8; 4]> = {
            let host = peer.rsplit_once(':').map(|(h, _)| h).unwrap_or(&peer);
            let octets: Vec<u8> = host
                .split('.')
                .filter_map(|o| o.parse::<u8>().ok())
                .collect();
            <[u8; 4]>::try_from(octets.as_slice()).ok()
        };

        // Build + send the full signed handshake via the promoted library API.
        let hs = OutgoingExtendedHandshake {
            ace_metadata_version: 1,
            ut_metadata_id: 2,
            mi,
            node: ace_wire::extended::NodeFields {
                ts,
                ..Default::default()
            },
            peer_ip,
            metadata_size: None,
        };
        session
            .send_signed_extended_handshake(&hs, &identity)
            .await
            .unwrap();
        println!(
            "[recon {peer}] sent FULL SIGNED handshake (mi={}, node_id={})",
            if has_mi { "yes" } else { "no" },
            hex::encode(identity.node_id())
        );
        if env("ACE_NO_INTERESTED").is_none() {
            session.send(&PeerMessage::Interested).await.unwrap();
            println!("[recon {peer}] sent interested");
        }

        // Observe what the peer does: unchoke? bitfield? have? piece?
        let mut unchoked = false;
        loop {
            match session.read_message().await {
                Ok(msg) => {
                    match &msg {
                        PeerMessage::Unchoke => {
                            unchoked = true;
                            println!("[recon {peer}] >>> UNCHOKE");
                        }
                        PeerMessage::Bitfield(b) => {
                            println!("[recon {peer}] bitfield ({} bytes)", b.len())
                        }
                        PeerMessage::Have(i) => println!("[recon {peer}] have {i}"),
                        PeerMessage::Piece {
                            index,
                            begin,
                            block,
                        } => {
                            println!("[recon {peer}] >>> PIECE idx={index} begin={begin} ({} bytes) head={}",
                                block.len(), hex::encode(&block[..block.len().min(24)]));
                            if let Ok(path) = std::env::var("ACE_DUMP") {
                                use std::io::Write;
                                // Structured record so we can reassemble by (piece, chunk)
                                // offline: [piece u32 BE][chunk u16 BE][len u32 BE][data].
                                // piece = `begin`; chunk = block[8..10]; data = block[10..].
                                if block.len() > 10 {
                                    let chunk = ((block[8] as u16) << 8) | block[9] as u16;
                                    let data = &block[10..];
                                    let mut rec = Vec::new();
                                    rec.extend_from_slice(&begin.to_be_bytes());
                                    rec.extend_from_slice(&chunk.to_be_bytes());
                                    rec.extend_from_slice(&(data.len() as u32).to_be_bytes());
                                    rec.extend_from_slice(data);
                                    let mut fh = std::fs::OpenOptions::new()
                                        .create(true)
                                        .append(true)
                                        .open(&path)
                                        .unwrap();
                                    fh.write_all(&rec).unwrap();
                                }
                            }
                        }
                        PeerMessage::Unknown { id, payload } => println!(
                            "[recon {peer}] >>> msg id={id} ({} bytes) head={}",
                            payload.len(),
                            hex::encode(&payload[..payload.len().min(16)])
                        ),
                        other => println!("[recon {peer}] {other:?}"),
                    }
                    if unchoked {
                        if let Some(p) = their_pos {
                            // Acestream request (id=6, 10-byte payload): [stream u32=0]
                            // [piece u32][chunk u16]. Pull ACE_CHUNKS chunks each across
                            // ACE_PIECES consecutive complete pieces ending below the head.
                            let chunks: u16 =
                                env("ACE_CHUNKS").and_then(|v| v.parse().ok()).unwrap_or(4);
                            let pieces: u32 =
                                env("ACE_PIECES").and_then(|v| v.parse().ok()).unwrap_or(1);
                            let base = (p.max_piece as u32).saturating_sub(pieces);
                            for piece in base..base + pieces {
                                for chunk in 0..chunks {
                                    session.send(&chunk_request(piece, chunk)).await.unwrap();
                                }
                            }
                            println!(
                                "[recon {peer}] requested pieces {base}..{} x {chunks} chunks",
                                base + pieces
                            );
                            unchoked = false; // request once; keep reading for the data
                        }
                    }
                }
                Err(e) => {
                    println!("[recon {peer}] read ended: {e:?}");
                    break;
                }
            }
        }
    }

    #[tokio::test]
    async fn fetch_metadata_assembles_multi_piece_blob() {
        use ace_wire::ut_metadata::{data_piece, MetadataMessage, METADATA_BLOCK_LEN};
        // A 2-piece blob: one full 16 KiB block + a 100-byte remainder.
        let metadata: Vec<u8> = (0..(METADATA_BLOCK_LEN + 100))
            .map(|i| (i % 251) as u8)
            .collect();
        let total = metadata.len();
        let meta_peer = metadata.clone();
        let peer_ut_id = 2u8;

        let (client, server) = tokio::io::duplex(64 * 1024);
        // Mock peer: answer every ut_metadata request with the matching data block.
        let srv = tokio::spawn(async move {
            let mut sess = PeerSession::new(server);
            let pieces = total.div_ceil(METADATA_BLOCK_LEN);
            let mut served = 0;
            while served < pieces {
                match sess.read_message().await {
                    Ok(PeerMessage::Extended { ext_id, payload }) if ext_id == peer_ut_id => {
                        if let Some(MetadataMessage::Request { piece }) =
                            MetadataMessage::parse(&payload)
                        {
                            let off = piece as usize * METADATA_BLOCK_LEN;
                            let end = (off + METADATA_BLOCK_LEN).min(total);
                            let resp = data_piece(piece, total as i64, &meta_peer[off..end]);
                            sess.send(&PeerMessage::Extended {
                                ext_id: peer_ut_id,
                                payload: resp,
                            })
                            .await
                            .unwrap();
                            served += 1;
                        }
                    }
                    _ => break,
                }
            }
        });

        let mut session = PeerSession::new(client);
        let got = session.fetch_metadata(peer_ut_id, total).await.unwrap();
        assert_eq!(got, metadata);
        srv.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_metadata_errors_on_reject() {
        let peer_ut_id = 2u8;
        let (client, server) = tokio::io::duplex(8 * 1024);
        let srv = tokio::spawn(async move {
            let mut sess = PeerSession::new(server);
            if let Ok(PeerMessage::Extended { .. }) = sess.read_message().await {
                // Reject piece 0: {msg_type: 2, piece: 0}.
                let reject = b"d8:msg_typei2e5:piecei0ee".to_vec();
                let _ = sess
                    .send(&PeerMessage::Extended {
                        ext_id: peer_ut_id,
                        payload: reject,
                    })
                    .await;
            }
        });
        let mut session = PeerSession::new(client);
        let res = session.fetch_metadata(peer_ut_id, 100).await;
        assert!(matches!(res, Err(PeerError::Protocol(_))));
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
        let res = session
            .perform_handshake([0x11u8; 20], *b"R30------CLIENTPEERy")
            .await;
        assert!(matches!(res, Err(PeerError::InfohashMismatch)));
        srv.await.unwrap();
    }

    #[tokio::test]
    async fn accept_handshake_replies_when_we_serve_the_infohash() {
        let (client, server) = tokio::io::duplex(256);
        let ih = [0x42u8; 20];
        let client_id = *b"R30------CLIENTPEERy";
        let server_id = *b"R30------SERVERPEERy";

        let client_task = tokio::spawn(async move {
            let mut p = PeerSession::new(client);
            p.perform_handshake(ih, client_id).await
        });

        let mut server_session = PeerSession::new(server);
        let got = server_session
            .accept_handshake(server_id, |peer_ih| *peer_ih == ih)
            .await
            .unwrap();
        assert_eq!(got.infohash, ih);
        assert_eq!(got.peer_id, client_id);

        let client_got = client_task.await.unwrap().unwrap();
        assert_eq!(client_got.infohash, ih);
        assert_eq!(client_got.peer_id, server_id);
    }

    #[tokio::test]
    async fn accept_handshake_replies_to_official_short_inbound_handshake() {
        let (mut client, server) = tokio::io::duplex(256);
        let ih = [0x43u8; 20];
        let client_id = *b"R30------CLIENTPEERy";
        let server_id = *b"R30------SERVERPEERy";

        let short = Handshake::new(ih, client_id).encode()[..46].to_vec();
        let client_task = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            client.write_all(&short).await.unwrap();
            let mut reply = [0u8; HANDSHAKE_LEN];
            client.read_exact(&mut reply).await.unwrap();
            Handshake::decode(&reply)
        });

        let mut server_session = PeerSession::new(server).with_timeout(Duration::from_millis(100));
        let got = server_session
            .accept_handshake(server_id, |peer_ih| *peer_ih == ih)
            .await
            .unwrap();
        assert_eq!(got.infohash, ih);
        assert_eq!(
            got.peer_id, [0u8; 20],
            "the official short inbound handshake omits peer_id; keep that explicit"
        );

        let client_got = client_task.await.unwrap().unwrap();
        assert_eq!(client_got.infohash, ih);
        assert_eq!(client_got.peer_id, server_id);
    }

    #[tokio::test]
    async fn accept_handshake_drains_deferred_peer_id_after_official_short_handshake() {
        let (mut client, server) = tokio::io::duplex(4096);
        let ih = [0x44u8; 20];
        let client_id = *b"R30------CLIENTPEERy";
        let server_id = *b"R30------SERVERPEERy";
        let ext = PeerMessage::Extended {
            ext_id: 0,
            payload: b"d1:md11:ut_metadatai2eee".to_vec(),
        };
        let ext_wire = ext.encode();

        let short = Handshake::new(ih, client_id).encode()[..46].to_vec();
        let client_task = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            client.write_all(&short).await.unwrap();
            let mut reply = [0u8; HANDSHAKE_LEN];
            client.read_exact(&mut reply).await.unwrap();
            client.write_all(&client_id).await.unwrap();
            client.write_all(&ext_wire).await.unwrap();
            Handshake::decode(&reply)
        });

        let mut server_session = PeerSession::new(server).with_timeout(Duration::from_millis(200));
        let got = server_session
            .accept_handshake(server_id, |peer_ih| *peer_ih == ih)
            .await
            .unwrap();
        assert_eq!(got.infohash, ih);
        assert_eq!(got.peer_id, client_id);
        assert_eq!(server_session.read_message().await.unwrap(), ext);

        let client_got = client_task.await.unwrap().unwrap();
        assert_eq!(client_got.infohash, ih);
        assert_eq!(client_got.peer_id, server_id);
    }

    #[tokio::test]
    async fn accept_handshake_refuses_an_infohash_we_dont_serve() {
        let (client, server) = tokio::io::duplex(256);
        let ih = [0x42u8; 20];

        let client_task = tokio::spawn(async move {
            let mut p = PeerSession::new(client).with_timeout(Duration::from_millis(200));
            // The client performs its handshake write+read; if the server never replies,
            // perform_handshake's own read will time out.
            p.perform_handshake(ih, *b"R30------CLIENTPEERy").await
        });

        let mut server_session = PeerSession::new(server);
        let result = server_session
            .accept_handshake(*b"R30------SERVERPEERy", |_| false)
            .await;
        assert!(result.is_err(), "must refuse an infohash we don't serve");

        // The client never got a reply, so its own handshake read times out / errors.
        assert!(client_task.await.unwrap().is_err());
    }
}
