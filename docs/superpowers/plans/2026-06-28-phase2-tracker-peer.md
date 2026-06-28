# Phase 2 — `ace-tracker` + `ace-peer` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn an infohash into a live peer session: `ace-tracker` (BEP-15 UDP tracker client → peer list) and `ace-peer` (async TCP session that does the `AceStreamProtocol` handshake + BEP-10 extended handshake and reads peer messages), reusing `ace-wire`.

**Architecture:** Two new workspace crates. `ace-tracker` is split into a pure codec (build/parse request/response bytes — fully unit-testable) and a thin async `announce()` over `UdpSocket`. `ace-peer` provides `PeerSession<S>` generic over any `AsyncRead+AsyncWrite` so the handshake/read logic is tested deterministically with an in-memory duplex; a `connect()` helper wraps `TcpStream`. Network-dependent tests are `#[ignore]`d (run manually) so the default `cargo test` stays deterministic and offline.

**Tech Stack:** Rust 2021, `tokio` (net, io-util, rt, macros, time), `rand`, `ace-wire` (path dep). Reuses Phase-0 facts: tracker `t1.torrentstream.org:2710`; peer handshake = `AceStreamProtocol` (see `docs/protocol/wire-protocol.md`).

**Out of scope (Phase 3):** piece download/verification (needs the OPEN transport-body layout), Mainline DHT, live piece picker.

---

## File / Directory Map

| Path | Responsibility |
|---|---|
| `Cargo.toml` (root) | add `crates/ace-tracker`, `crates/ace-peer` to workspace members |
| `crates/ace-tracker/Cargo.toml` | deps |
| `crates/ace-tracker/src/lib.rs` | `TrackerError` + re-exports |
| `crates/ace-tracker/src/codec.rs` | BEP-15 build/parse (pure) |
| `crates/ace-tracker/src/client.rs` | async `announce()` over UDP |
| `crates/ace-peer/Cargo.toml` | deps incl. `ace-wire` path dep |
| `crates/ace-peer/src/lib.rs` | `PeerError` + re-exports |
| `crates/ace-peer/src/session.rs` | `PeerSession<S>`: handshake + read/send |

---

## Task 1: Add `ace-tracker` to the workspace (skeleton)

**Files:**
- Modify: `Cargo.toml` (root)
- Create: `crates/ace-tracker/Cargo.toml`, `crates/ace-tracker/src/lib.rs`, `crates/ace-tracker/src/codec.rs`, `crates/ace-tracker/src/client.rs`

- [ ] **Step 1: Add the crate to the workspace members**

Edit root `Cargo.toml` so `members` reads:
```toml
[workspace]
members = ["crates/ace-wire", "crates/ace-tracker"]
resolver = "2"
```

- [ ] **Step 2: Create the crate manifest**

Create `crates/ace-tracker/Cargo.toml`:
```toml
[package]
name = "ace-tracker"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { version = "1", features = ["net", "rt", "macros", "time"] }
rand = "0.8"

[dev-dependencies]
hex = "0.4"
tokio = { version = "1", features = ["net", "rt", "macros", "time", "rt-multi-thread"] }
```

- [ ] **Step 3: Create lib + empty modules**

Create `crates/ace-tracker/src/lib.rs`:
```rust
//! ace-tracker: BitTorrent UDP tracker client (BEP-15) for Acestream infohashes.
pub mod client;
pub mod codec;

#[derive(Debug)]
pub enum TrackerError {
    /// Response too short / malformed.
    Malformed(&'static str),
    /// Transaction id in the response did not match the request.
    TransactionMismatch,
    /// Tracker returned an error action (3) with this message.
    Tracker(String),
    /// Underlying I/O or timeout.
    Io(std::io::Error),
    /// Operation timed out.
    Timeout,
}

impl From<std::io::Error> for TrackerError {
    fn from(e: std::io::Error) -> Self { TrackerError::Io(e) }
}

pub type Result<T> = std::result::Result<T, TrackerError>;
```

Create `crates/ace-tracker/src/codec.rs` and `crates/ace-tracker/src/client.rs` each with a single `// placeholder` line.

- [ ] **Step 4: Verify build**

Run: `cargo build -p ace-tracker`
Expected: compiles (with unused warnings).

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/ace-tracker/
git commit -m "ace-tracker: workspace member + skeleton"
```

---

## Task 2: BEP-15 codec (pure build/parse)

**Files:**
- Modify: `crates/ace-tracker/src/codec.rs`
- Test: `crates/ace-tracker/src/codec.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Write the failing tests (inline)**

Put at the bottom of `crates/ace-tracker/src/codec.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddrV4;

    #[test]
    fn connect_request_layout() {
        let req = build_connect_request(0x1122_3344);
        assert_eq!(&req[0..8], &0x41727101980u64.to_be_bytes()); // magic protocol id
        assert_eq!(&req[8..12], &0u32.to_be_bytes());            // action = connect
        assert_eq!(&req[12..16], &0x1122_3344u32.to_be_bytes()); // txid
    }

    #[test]
    fn parse_connect_roundtrip() {
        let txid = 0xAABB_CCDD;
        let mut resp = Vec::new();
        resp.extend_from_slice(&0u32.to_be_bytes());          // action connect
        resp.extend_from_slice(&txid.to_be_bytes());          // txid
        resp.extend_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes()); // conn id
        assert_eq!(parse_connect_response(&resp, txid).unwrap(), 0x0102_0304_0506_0708);
        // wrong txid rejected
        assert!(parse_connect_response(&resp, txid ^ 1).is_err());
    }

    #[test]
    fn announce_request_layout() {
        let req = build_announce_request(0x0102_0304_0506_0708, 0x1111_2222,
            &[0xAB; 20], &[0xCD; 20], 6881, 50);
        assert_eq!(req.len(), 98);
        assert_eq!(&req[0..8], &0x0102_0304_0506_0708u64.to_be_bytes()); // conn id
        assert_eq!(&req[8..12], &1u32.to_be_bytes());                    // action announce
        assert_eq!(&req[16..36], &[0xABu8; 20]);                         // infohash
        assert_eq!(&req[36..56], &[0xCDu8; 20]);                         // peer id
        assert_eq!(&req[96..98], &6881u16.to_be_bytes());                // port
    }

    #[test]
    fn parse_announce_peers() {
        let txid = 0x1111_2222;
        let mut resp = Vec::new();
        resp.extend_from_slice(&1u32.to_be_bytes());   // action announce
        resp.extend_from_slice(&txid.to_be_bytes());   // txid
        resp.extend_from_slice(&1800u32.to_be_bytes()); // interval
        resp.extend_from_slice(&0u32.to_be_bytes());    // leechers
        resp.extend_from_slice(&2u32.to_be_bytes());    // seeders
        resp.extend_from_slice(&[5, 252, 161, 218]); resp.extend_from_slice(&2710u16.to_be_bytes());
        resp.extend_from_slice(&[1, 2, 3, 4]);          resp.extend_from_slice(&8621u16.to_be_bytes());
        let (interval, peers) = parse_announce_response(&resp, txid).unwrap();
        assert_eq!(interval, 1800);
        assert_eq!(peers, vec![
            "5.252.161.218:2710".parse::<SocketAddrV4>().unwrap(),
            "1.2.3.4:8621".parse::<SocketAddrV4>().unwrap(),
        ]);
    }

    #[test]
    fn parse_tracker_error_action() {
        let txid = 7;
        let mut resp = Vec::new();
        resp.extend_from_slice(&3u32.to_be_bytes()); // action = error
        resp.extend_from_slice(&txid.to_be_bytes());
        resp.extend_from_slice(b"nope");
        assert!(matches!(parse_announce_response(&resp, txid), Err(crate::TrackerError::Tracker(_))));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p ace-tracker codec`
Expected: FAIL — functions not found.

- [ ] **Step 3: Implement**

Replace `crates/ace-tracker/src/codec.rs` (keep the test module at the bottom):
```rust
//! BEP-15 UDP tracker wire codec. Pure build/parse, no I/O.
use crate::{Result, TrackerError};
use std::net::SocketAddrV4;

/// Magic protocol id for the initial connect handshake (BEP-15).
pub const PROTOCOL_ID: u64 = 0x41727101980;
pub const ACTION_CONNECT: u32 = 0;
pub const ACTION_ANNOUNCE: u32 = 1;
pub const ACTION_ERROR: u32 = 3;
pub const EVENT_STARTED: u32 = 2;

pub fn build_connect_request(txid: u32) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[0..8].copy_from_slice(&PROTOCOL_ID.to_be_bytes());
    b[8..12].copy_from_slice(&ACTION_CONNECT.to_be_bytes());
    b[12..16].copy_from_slice(&txid.to_be_bytes());
    b
}

pub fn parse_connect_response(buf: &[u8], txid: u32) -> Result<u64> {
    if buf.len() < 16 { return Err(TrackerError::Malformed("connect resp < 16")); }
    let action = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let rtxid = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if rtxid != txid { return Err(TrackerError::TransactionMismatch); }
    if action != ACTION_CONNECT { return Err(TrackerError::Malformed("not a connect action")); }
    let mut c = [0u8; 8];
    c.copy_from_slice(&buf[8..16]);
    Ok(u64::from_be_bytes(c))
}

#[allow(clippy::too_many_arguments)]
pub fn build_announce_request(
    connection_id: u64, txid: u32, infohash: &[u8; 20], peer_id: &[u8; 20],
    port: u16, num_want: i32,
) -> Vec<u8> {
    let mut b = Vec::with_capacity(98);
    b.extend_from_slice(&connection_id.to_be_bytes());
    b.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
    b.extend_from_slice(&txid.to_be_bytes());
    b.extend_from_slice(infohash);
    b.extend_from_slice(peer_id);
    b.extend_from_slice(&0u64.to_be_bytes()); // downloaded
    b.extend_from_slice(&0u64.to_be_bytes()); // left
    b.extend_from_slice(&0u64.to_be_bytes()); // uploaded
    b.extend_from_slice(&EVENT_STARTED.to_be_bytes());
    b.extend_from_slice(&0u32.to_be_bytes()); // ip (default)
    b.extend_from_slice(&0u32.to_be_bytes()); // key
    b.extend_from_slice(&num_want.to_be_bytes());
    b.extend_from_slice(&port.to_be_bytes());
    b
}

/// Returns (interval_seconds, peers).
pub fn parse_announce_response(buf: &[u8], txid: u32) -> Result<(u32, Vec<SocketAddrV4>)> {
    if buf.len() < 8 { return Err(TrackerError::Malformed("announce resp < 8")); }
    let action = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let rtxid = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if rtxid != txid { return Err(TrackerError::TransactionMismatch); }
    if action == ACTION_ERROR {
        let msg = String::from_utf8_lossy(&buf[8..]).into_owned();
        return Err(TrackerError::Tracker(msg));
    }
    if action != ACTION_ANNOUNCE { return Err(TrackerError::Malformed("not an announce action")); }
    if buf.len() < 20 { return Err(TrackerError::Malformed("announce header < 20")); }
    let interval = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
    let mut peers = Vec::new();
    let mut i = 20;
    while i + 6 <= buf.len() {
        let ip = std::net::Ipv4Addr::new(buf[i], buf[i + 1], buf[i + 2], buf[i + 3]);
        let port = u16::from_be_bytes([buf[i + 4], buf[i + 5]]);
        peers.push(SocketAddrV4::new(ip, port));
        i += 6;
    }
    Ok((interval, peers))
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p ace-tracker codec`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/ace-tracker/src/codec.rs
git commit -m "ace-tracker: BEP-15 connect/announce codec with tests"
```

---

## Task 3: async `announce()` over UDP

**Files:**
- Modify: `crates/ace-tracker/src/client.rs`
- Test: `crates/ace-tracker/src/client.rs` (inline `#[cfg(test)]`, with one `#[ignore]` live test)

- [ ] **Step 1: Write the failing tests (inline)**

Put at the bottom of `crates/ace-tracker/src/client.rs`:
```rust
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
            ar.extend_from_slice(&[9, 9, 9, 9]); ar.extend_from_slice(&1234u16.to_be_bytes());
            server.send_to(&ar, peer).await.unwrap();
        });

        let peers = announce(server_addr, &[1u8; 20], &[2u8; 20], 6881, 50)
            .await.unwrap();
        assert_eq!(peers, vec!["9.9.9.9:1234".parse().unwrap()]);
        handle.await.unwrap();
    }

    #[tokio::test]
    #[ignore] // live network: hits the real Acestream tracker
    async fn announce_against_real_tracker() {
        use tokio::net::lookup_host;
        let addr = lookup_host("t1.torrentstream.org:2710").await.unwrap().next().unwrap();
        let v4 = match addr { std::net::SocketAddr::V4(a) => a, _ => panic!("want v4") };
        let infohash = hex::decode("50e93529d3eb46a50506b14464185a15292d6e47").unwrap();
        let mut ih = [0u8; 20]; ih.copy_from_slice(&infohash);
        let peers = announce(v4, &ih, &[7u8; 20], 6881, 50).await.unwrap();
        println!("live tracker returned {} peers", peers.len());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p ace-tracker client`
Expected: FAIL — `announce` not found.

- [ ] **Step 3: Implement**

Replace `crates/ace-tracker/src/client.rs` (keep the test module at the bottom):
```rust
//! Async BEP-15 announce: connect then announce over one UdpSocket, with timeout.
use crate::codec::{
    build_announce_request, build_connect_request, parse_announce_response, parse_connect_response,
};
use crate::{Result, TrackerError};
use std::net::{SocketAddr, SocketAddrV4};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::timeout;

const RECV_TIMEOUT: Duration = Duration::from_secs(8);

/// Announce to a UDP tracker and return its peer list.
pub async fn announce(
    tracker: SocketAddrV4, infohash: &[u8; 20], peer_id: &[u8; 20], port: u16, num_want: i32,
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
    let req = build_announce_request(connection_id, atxid, infohash, peer_id, port, num_want);
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
```

- [ ] **Step 4: Run to verify it passes (deterministic test only)**

Run: `cargo test -p ace-tracker client`
Expected: PASS — `announce_against_local_fake_tracker` passes; the live test is ignored.

- [ ] **Step 5: Commit**

```bash
git add crates/ace-tracker/src/client.rs
git commit -m "ace-tracker: async UDP announce (deterministic fake-tracker test + ignored live test)"
```

---

## Task 4: Add `ace-peer` to the workspace (skeleton)

**Files:**
- Modify: `Cargo.toml` (root)
- Create: `crates/ace-peer/Cargo.toml`, `crates/ace-peer/src/lib.rs`, `crates/ace-peer/src/session.rs`

- [ ] **Step 1: Add to workspace members**

Root `Cargo.toml` members:
```toml
members = ["crates/ace-wire", "crates/ace-tracker", "crates/ace-peer"]
```

- [ ] **Step 2: Crate manifest with the ace-wire path dep**

Create `crates/ace-peer/Cargo.toml`:
```toml
[package]
name = "ace-peer"
version = "0.1.0"
edition = "2021"

[dependencies]
ace-wire = { path = "../ace-wire" }
tokio = { version = "1", features = ["net", "io-util", "rt", "macros", "time"] }

[dev-dependencies]
hex = "0.4"
tokio = { version = "1", features = ["net", "io-util", "rt", "macros", "time", "rt-multi-thread"] }
```

- [ ] **Step 3: lib + empty session module**

Create `crates/ace-peer/src/lib.rs`:
```rust
//! ace-peer: async Acestream peer session built on ace-wire codecs.
pub mod session;

#[derive(Debug)]
pub enum PeerError {
    /// A protocol decode failed.
    Wire(ace_wire::WireError),
    /// Peer presented a handshake for a different infohash.
    InfohashMismatch,
    /// Connection closed before a full structure arrived.
    Closed,
    /// Underlying I/O.
    Io(std::io::Error),
}

impl From<std::io::Error> for PeerError {
    fn from(e: std::io::Error) -> Self { PeerError::Io(e) }
}
impl From<ace_wire::WireError> for PeerError {
    fn from(e: ace_wire::WireError) -> Self { PeerError::Wire(e) }
}

pub type Result<T> = std::result::Result<T, PeerError>;
```

Create `crates/ace-peer/src/session.rs` with a single `// placeholder` line.

- [ ] **Step 4: Verify build**

Run: `cargo build -p ace-peer`
Expected: compiles.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/ace-peer/
git commit -m "ace-peer: workspace member + skeleton"
```

---

## Task 5: `PeerSession<S>` handshake + message reader

**Files:**
- Modify: `crates/ace-peer/src/session.rs`
- Test: `crates/ace-peer/src/session.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Write the failing test (inline) — uses an in-memory duplex (no network)**

Put at the bottom of `crates/ace-peer/src/session.rs`:
```rust
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
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p ace-peer session`
Expected: FAIL — `PeerSession` not found.

- [ ] **Step 3: Implement**

Replace `crates/ace-peer/src/session.rs` (keep the test module at the bottom):
```rust
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
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p ace-peer session`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/ace-peer/src/session.rs
git commit -m "ace-peer: PeerSession handshake + buffered message reader (duplex-tested)"
```

---

## Task 6: `connect()` helper + live interop test

**Files:**
- Modify: `crates/ace-peer/src/session.rs`
- Test: `crates/ace-peer/src/session.rs` (append one `#[ignore]` live test)

- [ ] **Step 1: Write the failing test (append to the existing test module)**

Add inside the `#[cfg(test)] mod tests { ... }` block in `session.rs`:
```rust
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
```

- [ ] **Step 2: Run to verify it fails to compile (no `connect`)**

Run: `cargo test -p ace-peer session`
Expected: FAIL — `connect` not found.

- [ ] **Step 3: Implement the `connect` helper**

Add to `crates/ace-peer/src/session.rs` (after the `impl` block, before the tests):
```rust
use tokio::net::TcpStream;

/// Connect a TCP peer session to `addr` (e.g. "1.2.3.4:8621").
pub async fn connect(addr: &str) -> Result<PeerSession<TcpStream>> {
    let stream = TcpStream::connect(addr).await?;
    Ok(PeerSession::new(stream))
}
```

- [ ] **Step 4: Run to verify the suite still passes (live test stays ignored)**

Run: `cargo test -p ace-peer`
Expected: PASS — the 2 duplex tests pass; `live_interop_handshake` is ignored.

- [ ] **Step 5: Run the whole workspace + commit**

Run: `cargo test`
Expected: all crates green (`ace-wire`, `ace-tracker`, `ace-peer`).

```bash
git add crates/ace-peer/src/session.rs
git commit -m "ace-peer: TcpStream connect() helper + ignored live interop test"
```

- [ ] **Step 6 (optional, manual verification): run the live tests**

With the official engine running and a current infohash/peer (from a capture), run:
```bash
cargo test -p ace-tracker -- --ignored --nocapture
ACE_PEER=<ip:port> ACE_INFOHASH=<40hex> cargo test -p ace-peer live_interop -- --ignored --nocapture
```
Expected: the peer accepts our handshake and prints its first message (the extended handshake). This reproduces the Phase-0 interop spike as reusable library code.

---

## Self-Review Notes (coverage vs scope)

- `wire-protocol.md` §2 discovery → Task 2/3 (UDP tracker; DHT/LSD deferred to a later
  sub-phase, noted out of scope here).
- §3.1 handshake exchange as a live session → Task 5 (`perform_handshake`), reusing
  `ace-wire::handshake`.
- §3.2 message read loop → Task 5 (`read_message` driving `PeerMessage::decode`).
- §3.3 extended handshake is exercised as the first message in the duplex test (Task 5)
  and the live test (Task 6); deeper extended-message parsing already lives in `ace-wire`.
- Deterministic by default: only Task 3's fake-tracker test and Task 5's duplex tests run
  in CI; all live-network tests are `#[ignore]`. No test depends on external state.
- Type/name consistency: `TrackerError`/`PeerError`/`PeerSession`/`announce`/`connect`/
  `perform_handshake`/`read_message`/`send` are used identically across tasks. `ace-peer`
  depends on `ace-wire` (Phase 1) via path; `HANDSHAKE_LEN` and `PeerMessage::decode`
  come from `ace-wire`.
- Out of scope (Phase 3): piece request/download/verification (needs the OPEN
  transport-body layout), Mainline DHT, live piece picker.
```
