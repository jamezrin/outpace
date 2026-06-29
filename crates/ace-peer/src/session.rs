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

    /// Send our BEP-10 extended handshake (id 20, sub-id 0), unsigned (no node identity).
    /// Real peers require a signed identity — see [`send_signed_extended_handshake`].
    ///
    /// [`send_signed_extended_handshake`]: Self::send_signed_extended_handshake
    pub async fn send_extended_handshake(
        &mut self, hs: &ace_wire::extended::OutgoingExtendedHandshake,
    ) -> Result<()> {
        let msg = PeerMessage::Extended { ext_id: 0, payload: hs.encode_payload() };
        self.send(&msg).await
    }

    /// Send our extended handshake carrying our node identity + a valid signature
    /// (note 17), which is what peers require before they will unchoke us.
    pub async fn send_signed_extended_handshake(
        &mut self,
        hs: &ace_wire::extended::OutgoingExtendedHandshake,
        identity: &ace_wire::identity::Identity,
    ) -> Result<()> {
        let msg = PeerMessage::Extended { ext_id: 0, payload: hs.sign_and_encode(identity) };
        self.send(&msg).await
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
    #[ignore] // live network: drive past the handshake and observe unchoke/piece behavior
    async fn live_recon_unchoke() {
        // Phase 3.2 recon. Capture hot peers from the engine namespace, then e.g.:
        //   ACE_PEER=82.213.234.240:8623 ACE_INFOHASH=47eda3..afa022 \
        //     cargo test -p ace-peer live_recon -- --ignored --nocapture
        use ace_wire::extended::{ExtendedHandshake, LivePosition};

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
        if let PeerMessage::Extended { ext_id: 0, ref payload } = first {
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
                    get(b"min_piece"), get(b"max_piece"), get(b"position"),
                    get(b"distance_from_source"),
                );
                if let (Some(min), Some(max), Some(pos)) =
                    (get(b"min_piece"), get(b"max_piece"), get(b"position"))
                {
                    their_pos = Some(LivePosition {
                        min_piece: min, max_piece: max, position: pos,
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

        // Build the FULL client handshake the engine sends (note 19): the complete field
        // set peers require, signed over the whole dict. `yourip` = the peer's IP (4 raw
        // bytes), proving we really connected to them.
        use ace_wire::bencode::Bencode;
        use std::collections::BTreeMap;
        let bi = |n: i64| Bencode::Int(n);
        let bb = |b: &[u8]| Bencode::Bytes(b.to_vec());
        let peer_ip: Vec<u8> = peer
            .rsplit_once(':').map(|(h, _)| h).unwrap_or(&peer)
            .split('.').filter_map(|o| o.parse::<u8>().ok()).collect();

        let mut m: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        m.insert(b"ut_metadata".to_vec(), bi(2));

        let p = mi.unwrap_or(LivePosition { min_piece: 0, max_piece: 0, position: -1, distance_from_source: -1 });
        let mut midict: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        for (k, v) in [
            ("distance_from_source", p.distance_from_source), ("down_rate", 0),
            ("download_window_end", -1), ("is_accessible", 0),
            ("live_window_size", (p.max_piece - p.min_piece + 1).max(0)), ("lsp", -1),
            ("mam", -1), ("max_piece", p.max_piece), ("min_piece", p.min_piece),
            ("peer_type", 0), ("ping_from_source", -1), ("position", -1),
            ("time_from_source", -1), ("top_session_up_rate", 0), ("top_up_rate", 0),
            ("up_rate", 0), ("upload_rating", 0),
        ] { midict.insert(k.as_bytes().to_vec(), bi(v)); }

        let mut f: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        f.insert(b"ace_metadata_version".to_vec(), bi(1));
        f.insert(b"asn".to_vec(), bi(0));
        f.insert(b"asn_country".to_vec(), bb(b""));
        f.insert(b"geoip_country".to_vec(), bb(b""));
        f.insert(b"lsp".to_vec(), bi(-1));
        f.insert(b"m".to_vec(), Bencode::Dict(m));
        if has_mi { f.insert(b"mi".to_vec(), Bencode::Dict(midict)); }
        f.insert(b"node_id".to_vec(), bb(&identity.node_id()));
        f.insert(b"nt".to_vec(), bi(1));
        f.insert(b"p".to_vec(), bi(8621));
        f.insert(b"platform".to_vec(), bi(2));
        f.insert(b"pv".to_vec(), bi(2));
        f.insert(b"stream_statuses".to_vec(), Bencode::Dict(BTreeMap::new()));
        f.insert(b"ts".to_vec(), bi(ts));
        f.insert(b"tt".to_vec(), bb(b"bt"));
        f.insert(b"v".to_vec(), bi(3021100));
        if peer_ip.len() == 4 { f.insert(b"yourip".to_vec(), bb(&peer_ip)); }

        let digest = ace_wire::identity::handshake_digest(&f);
        f.insert(b"signature".to_vec(), bb(&identity.sign(&digest)));
        let payload = Bencode::Dict(f).encode();
        session.send(&PeerMessage::Extended { ext_id: 0, payload }).await.unwrap();
        println!("[recon {peer}] sent FULL SIGNED handshake (mi={}, node_id={})",
                 if has_mi { "yes" } else { "no" }, hex::encode(identity.node_id()));
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
                        PeerMessage::Unchoke => { unchoked = true; println!("[recon {peer}] >>> UNCHOKE"); }
                        PeerMessage::Bitfield(b) => println!("[recon {peer}] bitfield ({} bytes)", b.len()),
                        PeerMessage::Have(i) => println!("[recon {peer}] have {i}"),
                        PeerMessage::Piece { index, begin, block } => {
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
                                    let mut fh = std::fs::OpenOptions::new().create(true).append(true).open(&path).unwrap();
                                    fh.write_all(&rec).unwrap();
                                }
                            }
                        }
                        PeerMessage::Unknown { id, payload } => println!(
                            "[recon {peer}] >>> msg id={id} ({} bytes) head={}",
                            payload.len(), hex::encode(&payload[..payload.len().min(16)])),
                        other => println!("[recon {peer}] {other:?}"),
                    }
                    if unchoked {
                        if let Some(p) = their_pos {
                            // Acestream request (id=6, 10-byte payload): [stream u32=0]
                            // [piece u32][chunk u16]. Pull ACE_CHUNKS chunks each across
                            // ACE_PIECES consecutive complete pieces ending below the head.
                            let chunks: u16 = env("ACE_CHUNKS").and_then(|v| v.parse().ok()).unwrap_or(4);
                            let pieces: u32 = env("ACE_PIECES").and_then(|v| v.parse().ok()).unwrap_or(1);
                            let base = (p.max_piece as u32).saturating_sub(pieces);
                            for piece in base..base + pieces {
                                for chunk in 0..chunks {
                                    let mut payload = vec![0u8, 0, 0, 0];
                                    payload.extend_from_slice(&piece.to_be_bytes());
                                    payload.extend_from_slice(&chunk.to_be_bytes());
                                    session.send(&PeerMessage::Unknown { id: 6, payload }).await.unwrap();
                                }
                            }
                            println!("[recon {peer}] requested pieces {base}..{} x {chunks} chunks", base + pieces);
                            unchoked = false; // request once; keep reading for the data
                        }
                    }
                }
                Err(e) => { println!("[recon {peer}] read ended: {e:?}"); break; }
            }
        }
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
