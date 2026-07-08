# VOD Transport and Playback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a single-file VOD (video-on-demand) download → SHA-1-verify → serve path alongside the existing live path, without regressing live.

**Architecture:** VOD is vanilla BitTorrent (standard `Request`/`Piece`/`Bitfield`/`Have` in `ace_wire::message::PeerMessage`), so the path is fully parallel to live and shares only connect/handshake primitives. `ace-wire` surfaces the VOD file layout; `ace-swarm` adds `VodInfo` + a `download_vod` loop that verifies each piece against the transport's `pieces` hashes before emitting ordered bytes; `ace-engine` adds an `open_vod` provider seam, a `/vod` HTTP route, and CLI dispatch. Multi-file is rejected with a clear error. Determinism comes from a local mock BitTorrent seeder — no live swarm.

**Tech Stack:** Rust, tokio, sha1, axum, clap. Existing crates: `ace-wire`, `ace-peer`, `ace-swarm`, `ace-engine`.

---

## File Structure

- `crates/ace-wire/src/transport.rs` (modify) — parse VOD `length` + detect multi-file `files`.
- `crates/ace-swarm/src/types.rs` (modify) — `VodInfo` struct + geometry helpers.
- `crates/ace-swarm/src/resolve.rs` (modify) — `vod_info_from_transport`; expose catalog transport bytes.
- `crates/ace-swarm/src/vod.rs` (create) — `verify_piece`, piece geometry, `download_vod` loop + mock-seeder tests.
- `crates/ace-swarm/src/lib.rs` (modify) — `pub mod vod;`.
- `crates/ace-engine/src/provider.rs` (modify) — `VodByteSource` trait + `VodOpen` on registry seam (kept minimal).
- `crates/ace-engine/src/ace_provider.rs` (modify) — `AceProvider::open_vod`.
- `crates/ace-engine/src/http.rs` (modify) — `GET /vod/:network/:id`.
- `crates/ace-engine/src/cli.rs` (modify) — resolve-then-dispatch VOD in `play`.

Dependencies: `sha1` is already used in the workspace (infohash); confirm it is a dependency of `ace-swarm` (add if missing).

---

### Task 1: VOD file layout in the transport descriptor

**Files:**
- Modify: `crates/ace-wire/src/transport.rs`
- Test: `crates/ace-wire/src/transport.rs` (`#[cfg(test)]`)

- [ ] **Step 1: Write failing tests**

Add to the `tests` module in `transport.rs`:

```rust
#[test]
fn single_file_vod_exposes_length_and_pieces() {
    // 2 pieces worth of SHA-1 hashes (40 bytes) + a length key => single-file VOD.
    let pieces = vec![0u8; 40];
    let mut d = std::collections::BTreeMap::new();
    d.insert(b"name".to_vec(), Bencode::Bytes(b"movie".to_vec()));
    d.insert(b"piece_length".to_vec(), Bencode::Int(131072));
    d.insert(b"chunk_length".to_vec(), Bencode::Int(16384));
    d.insert(b"length".to_vec(), Bencode::Int(200000));
    d.insert(b"pieces".to_vec(), Bencode::Bytes(pieces));
    let tf = encode_transport(&Bencode::Dict(d));
    let got = decode_transport(&tf).unwrap();
    assert!(!got.is_live);
    assert_eq!(got.pieces.len(), 2);
    assert_eq!(got.vod_total_length(), Some(200000));
    assert!(!got.is_multifile());
}

#[test]
fn files_key_marks_multifile() {
    let mut d = std::collections::BTreeMap::new();
    d.insert(b"piece_length".to_vec(), Bencode::Int(131072));
    d.insert(b"chunk_length".to_vec(), Bencode::Int(16384));
    d.insert(b"pieces".to_vec(), Bencode::Bytes(vec![0u8; 20]));
    d.insert(b"files".to_vec(), Bencode::List(vec![]));
    let tf = encode_transport(&Bencode::Dict(d));
    let got = decode_transport(&tf).unwrap();
    assert!(got.is_multifile());
    assert_eq!(got.vod_total_length(), None);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p ace-wire vod`
Expected: FAIL — `no method named vod_total_length`.

- [ ] **Step 3: Implement**

In `transport.rs`, add methods on `TransportDescriptor` (after the struct). These read from `raw`, which is already retained:

```rust
impl TransportDescriptor {
    /// Total content byte length for a single-file VOD (`length` key), if present.
    ///
    /// SYNTHESIZED SCHEMA: no public VOD fixture is available, so the single-file `length`
    /// key is assumed from standard BitTorrent conventions. Reconcile against a real capture.
    pub fn vod_total_length(&self) -> Option<u64> {
        self.raw
            .get(b"length")
            .and_then(|v| v.as_int())
            .filter(|&i| i > 0)
            .map(|i| i as u64)
    }

    /// Whether this descriptor advertises a multi-file layout (`files` list). Multi-file VOD is
    /// intentionally unsupported; callers reject it. SYNTHESIZED SCHEMA (see `vod_total_length`).
    pub fn is_multifile(&self) -> bool {
        matches!(self.raw.get(b"files"), Some(crate::bencode::Bencode::List(_)))
    }
}
```

Add `use crate::bencode::Bencode;` is already present at top; the test uses `Bencode` and `encode_transport`, already in scope in tests.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p ace-wire vod`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/ace-wire/src/transport.rs
git commit -m "feat(wire): surface VOD length and multi-file layout (#47)"
```

---

### Task 2: `VodInfo` type + construction from a descriptor

**Files:**
- Modify: `crates/ace-swarm/src/types.rs`
- Modify: `crates/ace-swarm/src/resolve.rs`
- Modify: `crates/ace-swarm/Cargo.toml` (ensure `sha1` dep)
- Test: `crates/ace-swarm/src/resolve.rs` (`#[cfg(test)]`)

- [ ] **Step 1: Add `VodInfo` to `types.rs`**

```rust
/// What a single-file VOD stream needs to be downloaded and verified, from its transport
/// descriptor. Unlike [`StreamInfo`] (live), integrity is the transport's SHA-1 `pieces`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VodInfo {
    /// 20-byte BitTorrent infohash of the VOD swarm.
    pub infohash: [u8; 20],
    pub piece_length: u64,
    pub chunk_length: u64,
    pub trackers: Vec<String>,
    /// Per-piece SHA-1 hashes (20 bytes each) from the transport `pieces` key.
    pub piece_hashes: Vec<[u8; 20]>,
    /// Total content length in bytes (the final piece is truncated to this).
    pub total_length: u64,
}

impl VodInfo {
    pub fn chunks_per_piece(&self) -> u16 {
        (self.piece_length / self.chunk_length) as u16
    }
    /// Number of pieces (== `piece_hashes.len()`).
    pub fn piece_count(&self) -> u64 {
        self.piece_hashes.len() as u64
    }
    /// Byte length of piece `index` (the last piece is truncated to `total_length`).
    pub fn piece_size(&self, index: u64) -> u64 {
        let start = index * self.piece_length;
        (self.total_length - start).min(self.piece_length)
    }
}
```

- [ ] **Step 2: Write failing test in `resolve.rs`**

```rust
#[test]
fn vod_info_from_single_file_transport() {
    use ace_wire::bencode::Bencode;
    use std::collections::BTreeMap;
    let mut d = BTreeMap::new();
    d.insert(b"name".to_vec(), Bencode::Bytes(b"movie".to_vec()));
    d.insert(b"piece_length".to_vec(), Bencode::Int(131072));
    d.insert(b"chunk_length".to_vec(), Bencode::Int(16384));
    d.insert(b"length".to_vec(), Bencode::Int(300000));
    d.insert(b"pieces".to_vec(), Bencode::Bytes(vec![7u8; 60])); // 3 pieces
    let tf = ace_wire::transport::encode_transport(&Bencode::Dict(d));
    let info = vod_info_from_transport(&tf).unwrap();
    assert_eq!(info.piece_hashes.len(), 3);
    assert_eq!(info.total_length, 300000);
    assert_eq!(info.piece_size(2), 300000 - 2 * 131072);
}

#[test]
fn vod_info_rejects_live_transport() {
    // No `pieces` key => live => not a VOD target.
    use ace_wire::bencode::Bencode;
    use std::collections::BTreeMap;
    let mut d = BTreeMap::new();
    d.insert(b"piece_length".to_vec(), Bencode::Int(131072));
    d.insert(b"chunk_length".to_vec(), Bencode::Int(16384));
    let tf = ace_wire::transport::encode_transport(&Bencode::Dict(d));
    assert!(matches!(vod_info_from_transport(&tf), Err(ResolveError::Transport(_))));
}

#[test]
fn vod_info_rejects_multifile() {
    use ace_wire::bencode::Bencode;
    use std::collections::BTreeMap;
    let mut d = BTreeMap::new();
    d.insert(b"piece_length".to_vec(), Bencode::Int(131072));
    d.insert(b"chunk_length".to_vec(), Bencode::Int(16384));
    d.insert(b"length".to_vec(), Bencode::Int(1000));
    d.insert(b"pieces".to_vec(), Bencode::Bytes(vec![0u8; 20]));
    d.insert(b"files".to_vec(), Bencode::List(vec![]));
    let tf = ace_wire::transport::encode_transport(&Bencode::Dict(d));
    assert!(matches!(vod_info_from_transport(&tf), Err(ResolveError::Transport(_))));
}

#[test]
fn vod_info_rejects_missing_length() {
    use ace_wire::bencode::Bencode;
    use std::collections::BTreeMap;
    let mut d = BTreeMap::new();
    d.insert(b"piece_length".to_vec(), Bencode::Int(131072));
    d.insert(b"chunk_length".to_vec(), Bencode::Int(16384));
    d.insert(b"pieces".to_vec(), Bencode::Bytes(vec![0u8; 20]));
    let tf = ace_wire::transport::encode_transport(&Bencode::Dict(d));
    assert!(matches!(vod_info_from_transport(&tf), Err(ResolveError::Transport(_))));
}
```

- [ ] **Step 3: Run to verify failure**

Run: `cargo test -p ace-swarm vod_info`
Expected: FAIL — `vod_info_from_transport` not found.

- [ ] **Step 4: Implement `vod_info_from_transport` in `resolve.rs`**

Add near `stream_info_from_transport`. It reuses `validate_geometry` and `infohash_of_descriptor`:

```rust
use crate::types::VodInfo;

/// Build a [`VodInfo`] from raw `AceStreamTransport` bytes. Errors if the descriptor is live
/// (no `pieces`), multi-file (`files` present), or missing a single-file `length`.
pub fn vod_info_from_transport(bytes: &[u8]) -> Result<VodInfo, ResolveError> {
    let d = decode_transport(bytes).map_err(|_| ResolveError::Transport("decode failed"))?;
    if d.is_live {
        return Err(ResolveError::Transport("not a VOD transport (no pieces)"));
    }
    if d.is_multifile() {
        return Err(ResolveError::Transport("multi-file VOD is not supported"));
    }
    validate_geometry(d.piece_length, d.chunk_length)?;
    let total_length = d
        .vod_total_length()
        .ok_or(ResolveError::Transport("VOD descriptor missing length"))?;
    Ok(VodInfo {
        infohash: infohash_of_descriptor(&d.raw)
            .map_err(|_| ResolveError::Transport("infohash failed"))?,
        piece_length: d.piece_length,
        chunk_length: d.chunk_length,
        trackers: d.trackers,
        piece_hashes: d.pieces,
        total_length,
    })
}
```

Confirm `sha1` is available to `ace-swarm` (used in Task 3). Check `crates/ace-swarm/Cargo.toml`; if `sha1` is absent, add it: `sha1 = "0.10"` (match the version already in `ace-wire`).

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p ace-swarm vod_info`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add crates/ace-swarm/src/types.rs crates/ace-swarm/src/resolve.rs crates/ace-swarm/Cargo.toml
git commit -m "feat(swarm): VodInfo and vod_info_from_transport (#47)"
```

---

### Task 3: Piece verification (pure function)

**Files:**
- Create: `crates/ace-swarm/src/vod.rs`
- Modify: `crates/ace-swarm/src/lib.rs` (add `pub mod vod;`)
- Test: `crates/ace-swarm/src/vod.rs` (`#[cfg(test)]`)

- [ ] **Step 1: Create `vod.rs` with a failing test**

```rust
//! Single-file VOD download over vanilla BitTorrent: request blocks in order, verify each
//! assembled piece against the transport's SHA-1 `pieces`, and emit verified bytes in order.

use sha1::{Digest, Sha1};

/// True iff `bytes` hashes (SHA-1) to `expected` — standard BitTorrent piece integrity.
pub fn verify_piece(expected: &[u8; 20], bytes: &[u8]) -> bool {
    let digest = Sha1::digest(bytes);
    digest.as_slice() == expected
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_piece_accepts_matching_and_rejects_tampered() {
        let data = b"hello vod piece";
        let hash: [u8; 20] = Sha1::digest(data).into();
        assert!(verify_piece(&hash, data));
        let mut bad = data.to_vec();
        bad[0] ^= 0xff;
        assert!(!verify_piece(&hash, &bad));
    }
}
```

- [ ] **Step 2: Add module** to `crates/ace-swarm/src/lib.rs`:

```rust
pub mod vod;
```

- [ ] **Step 3: Run**

Run: `cargo test -p ace-swarm verify_piece`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/ace-swarm/src/vod.rs crates/ace-swarm/src/lib.rs
git commit -m "feat(swarm): SHA-1 VOD piece verification (#47)"
```

---

### Task 4: `download_vod` loop + mock-seeder integration test

**Files:**
- Modify: `crates/ace-swarm/src/vod.rs`
- Test: `crates/ace-swarm/src/vod.rs` (`#[cfg(test)]`)

- [ ] **Step 1: Write the failing integration test (mock seeder)**

Add to `vod.rs` tests. The mock seeder speaks standard BitTorrent: accept handshake, echo a handshake, send a full `Bitfield`, `Unchoke`, then answer each `Request` with the exact block. `optionally_tamper` lets a variant corrupt one block.

```rust
#[cfg(test)]
mod seeder_tests {
    use super::*;
    use crate::types::VodInfo;
    use ace_wire::handshake::Handshake;
    use ace_wire::message::PeerMessage;
    use std::net::SocketAddrV4;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    // Build content of `total_len` bytes and a VodInfo whose piece_hashes match it.
    fn make_content(piece_length: u64, chunk_length: u64, total_len: u64) -> (Vec<u8>, VodInfo) {
        let content: Vec<u8> = (0..total_len).map(|i| (i % 251) as u8).collect();
        let piece_count = total_len.div_ceil(piece_length);
        let mut piece_hashes = Vec::new();
        for p in 0..piece_count {
            let start = (p * piece_length) as usize;
            let end = ((p + 1) * piece_length).min(total_len) as usize;
            let h: [u8; 20] = sha1::Sha1::digest(&content[start..end]).into();
            piece_hashes.push(h);
        }
        let info = VodInfo {
            infohash: [0x42; 20],
            piece_length,
            chunk_length,
            trackers: vec![],
            piece_hashes,
            total_length: total_len,
        };
        (content, info)
    }

    async fn spawn_seeder(content: Vec<u8>, info: VodInfo, tamper: bool) -> SocketAddrV4 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = match listener.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!(),
        };
        // Accept connections in a loop so a client that abandons a peer (e.g. after a failed
        // verification) can immediately reconnect — keeps the tamper test fast/deterministic
        // instead of waiting on handshake timeouts.
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await { Ok(v) => v, Err(_) => break };
                let content = content.clone();
                let info = info.clone();
                tokio::spawn(async move {
                    // Read + reply handshake (66 bytes).
                    let mut hs = [0u8; ace_wire::handshake::HANDSHAKE_LEN];
                    if sock.read_exact(&mut hs).await.is_err() { return; }
                    let reply = Handshake::new(info.infohash, ace_wire::handshake::random_peer_id()).encode();
                    if sock.write_all(&reply).await.is_err() { return; }
                    // Full bitfield: piece_count bits set, MSB-first.
                    let nbytes = (info.piece_count() as usize).div_ceil(8);
                    let mut bits = vec![0u8; nbytes];
                    for p in 0..info.piece_count() as usize {
                        bits[p / 8] |= 0x80 >> (p % 8);
                    }
                    let _ = sock.write_all(&PeerMessage::Bitfield(bits).encode()).await;
                    let _ = sock.write_all(&PeerMessage::Unchoke.encode()).await;
                    // Serve requests until the peer disconnects.
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 4096];
                    loop {
                        loop {
                            match PeerMessage::decode(&buf) {
                                Ok(Some((msg, used))) => {
                                    buf.drain(..used);
                                    if let PeerMessage::Request { index, begin, length } = msg {
                                        let start = (index as u64 * info.piece_length + begin as u64) as usize;
                                        let end = start + length as usize;
                                        let mut block = content[start..end].to_vec();
                                        if tamper && index == 0 && begin == 0 {
                                            block[0] ^= 0xff;
                                        }
                                        let piece = PeerMessage::Piece { index, begin, block }.encode();
                                        if sock.write_all(&piece).await.is_err() { return; }
                                    }
                                }
                                Ok(None) => break,
                                Err(_) => return,
                            }
                        }
                        let n = match sock.read(&mut tmp).await { Ok(0) | Err(_) => return, Ok(n) => n };
                        buf.extend_from_slice(&tmp[..n]);
                    }
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn downloads_and_verifies_single_file_vod() {
        // 3 pieces, last one partial: piece_length 32 KiB, chunk 16 KiB, total 80000.
        let (content, info) = make_content(32768, 16384, 80000);
        let addr = spawn_seeder(content.clone(), info.clone(), false).await;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(64);
        let handle = tokio::spawn(async move { download_vod(info, vec![addr], tx).await });
        let mut got = Vec::new();
        while let Some(chunk) = rx.recv().await { got.extend_from_slice(&chunk); }
        handle.await.unwrap().unwrap();
        assert_eq!(got, content);
    }

    #[tokio::test]
    async fn tampered_piece_is_rejected() {
        let (content, info) = make_content(32768, 16384, 80000);
        let addr = spawn_seeder(content, info.clone(), true).await;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(64);
        let handle = tokio::spawn(async move { download_vod(info, vec![addr], tx).await });
        // Drain whatever (nothing verified should be emitted for piece 0).
        while rx.recv().await.is_some() {}
        let result = handle.await.unwrap();
        assert!(result.is_err(), "a tampered, unrecoverable piece must fail the download");
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p ace-swarm downloads_and_verifies`
Expected: FAIL — `download_vod` not found.

- [ ] **Step 3: Implement `download_vod`**

Add to `vod.rs`. In-order, single-peer-at-a-time with reconnect-to-next-peer on failure; per-piece SHA-1 gate; last piece/last block truncated to `total_length`. This is deliberately simple (sequential) — rarest-first / multi-peer parallelism is a follow-up.

```rust
use crate::types::VodInfo;
use ace_peer::session::{connect, PeerSession};
use ace_wire::handshake::random_peer_id;
use ace_wire::message::PeerMessage;
use bytes::Bytes;
use std::net::SocketAddrV4;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

/// Error from a VOD download.
#[derive(Debug)]
pub enum VodError {
    /// No peer could supply a verifying copy of a needed piece.
    Unrecoverable(u64),
    /// The consumer dropped the receiver.
    ConsumerGone,
}

/// Download a single-file VOD described by `info` from `peers`, verifying every piece against
/// its SHA-1 hash before sending its bytes (in order) to `tx`. Tries peers in turn; on any
/// per-peer failure (connect, verify mismatch, disconnect) moves to the next peer. Fails with
/// `Unrecoverable` if the piece list cannot be completed.
pub async fn download_vod(
    info: VodInfo,
    peers: Vec<SocketAddrV4>,
    tx: mpsc::Sender<Bytes>,
) -> Result<(), VodError> {
    let mut next_piece: u64 = 0;
    let piece_count = info.piece_count();
    let mut peer_idx = 0usize;
    // Each pass tries the peers in order starting from where we left off, resuming at next_piece.
    let mut attempts = 0usize;
    let max_attempts = peers.len().max(1) * 3;
    while next_piece < piece_count {
        if peers.is_empty() || attempts >= max_attempts {
            return Err(VodError::Unrecoverable(next_piece));
        }
        attempts += 1;
        let addr = peers[peer_idx % peers.len()];
        peer_idx += 1;
        let mut session = match connect(&addr.to_string()).await {
            Ok(s) => s,
            Err(_) => continue,
        };
        if session.perform_handshake(info.infohash, random_peer_id()).await.is_err() {
            continue;
        }
        match drain_from_peer(&mut session, &info, &mut next_piece, &tx).await {
            Ok(()) => {}            // peer exhausted its usefulness or we finished
            Err(VodError::ConsumerGone) => return Err(VodError::ConsumerGone),
            Err(VodError::Unrecoverable(_)) => {} // try another peer from the same cursor
        }
    }
    Ok(())
}

/// Pull pieces in order from a single connected peer, verifying and emitting each. Returns when
/// the peer stops being useful (disconnect / choke without progress) or all pieces are done.
async fn drain_from_peer<S: AsyncRead + AsyncWrite + Unpin>(
    session: &mut PeerSession<S>,
    info: &VodInfo,
    next_piece: &mut u64,
    tx: &mpsc::Sender<Bytes>,
) -> Result<(), VodError> {
    session.send(&PeerMessage::Interested).await.map_err(|_| VodError::Unrecoverable(*next_piece))?;
    let piece_count = info.piece_count();
    let mut unchoked = false;
    // Assemble one piece at a time.
    while *next_piece < piece_count {
        let idx = *next_piece;
        let piece_len = info.piece_size(idx) as usize;
        // Request every block of this piece up front (bounded: chunks_per_piece <= u16::MAX).
        let mut assembled = vec![0u8; piece_len];
        let mut have = 0usize;
        if unchoked {
            request_piece_blocks(session, info, idx, piece_len).await
                .map_err(|_| VodError::Unrecoverable(idx))?;
        }
        // Read messages until the piece is complete or the peer stops.
        loop {
            let msg = match session.read_message().await {
                Ok(m) => m,
                Err(_) => return Err(VodError::Unrecoverable(idx)),
            };
            match msg {
                PeerMessage::Unchoke => {
                    if !unchoked {
                        unchoked = true;
                        request_piece_blocks(session, info, idx, piece_len).await
                            .map_err(|_| VodError::Unrecoverable(idx))?;
                    }
                }
                PeerMessage::Choke => { unchoked = false; }
                PeerMessage::Piece { index, begin, block } if index == idx as u32 => {
                    let start = begin as usize;
                    let end = (start + block.len()).min(piece_len);
                    if start < piece_len {
                        assembled[start..end].copy_from_slice(&block[..end - start]);
                        have += end - start;
                    }
                    if have >= piece_len {
                        break;
                    }
                }
                // Ignore Have/Bitfield/other; keep reading.
                _ => {}
            }
        }
        if !verify_piece(&info.piece_hashes[idx as usize], &assembled) {
            // This peer served a bad piece; abandon it, keep the cursor for another peer.
            return Err(VodError::Unrecoverable(idx));
        }
        tx.send(Bytes::from(assembled)).await.map_err(|_| VodError::ConsumerGone)?;
        *next_piece += 1;
    }
    Ok(())
}

/// Send `Request` messages covering `[0, piece_len)` of piece `idx` in `chunk_length` blocks.
async fn request_piece_blocks<S: AsyncRead + AsyncWrite + Unpin>(
    session: &mut PeerSession<S>,
    info: &VodInfo,
    idx: u64,
    piece_len: usize,
) -> ace_peer::Result<()> {
    let block = info.chunk_length as usize;
    let mut begin = 0usize;
    while begin < piece_len {
        let length = block.min(piece_len - begin);
        session
            .send(&PeerMessage::Request {
                index: idx as u32,
                begin: begin as u32,
                length: length as u32,
            })
            .await?;
        begin += length;
    }
    Ok(())
}
```

Notes for the implementer:
- `PeerSession::send` and `read_message` are `pub` on `ace_peer::session::PeerSession`.
- The tampered-piece test relies on `verify_piece` failing piece 0, then the seeder disconnecting on the next reconnect attempt (only one connection accepted), so attempts exhaust and `Unrecoverable` is returned. This is the intended "never emit unverified bytes; fail cleanly" behavior.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p ace-swarm vod`
Expected: PASS (download + tamper + verify + earlier vod_info tests).

- [ ] **Step 5: Commit**

```bash
git add crates/ace-swarm/src/vod.rs
git commit -m "feat(swarm): download_vod verified single-file download loop (#47)"
```

---

### Task 5: Provider seam — `open_vod`

**Files:**
- Modify: `crates/ace-engine/src/provider.rs`
- Modify: `crates/ace-engine/src/ace_provider.rs`
- Test: `crates/ace-engine/src/ace_provider.rs` (`#[cfg(test)]`)

- [ ] **Step 1: Add a VOD source abstraction to `provider.rs`**

```rust
/// A finite, ordered VOD byte stream with a known total length.
#[async_trait]
pub trait VodByteSource: Send {
    /// Total content length in bytes (for `Content-Length`).
    fn content_length(&self) -> u64;
    /// Next verified, ordered chunk, or None at end-of-content.
    async fn next(&mut self) -> Option<Bytes>;
}
```

- [ ] **Step 2: Write a failing provider test**

In `ace_provider.rs` tests, drive `open_vod` end-to-end against an in-process mock seeder (reuse the pattern from Task 4 via a small local helper, or resolve from a synthesized transport served over `ut_metadata`). To keep this test deterministic and focused, assert the *dispatch contract*: `open_vod` on a bare infohash (no descriptor) returns `ProviderError::NotFound`/`Backend` with a clear message (a bare infohash cannot be a VOD target).

```rust
#[tokio::test]
async fn open_vod_rejects_bare_infohash() {
    let identity = std::sync::Arc::new(ace_wire::identity::Identity::generate());
    let provider = AceProvider::new(identity, 0);
    let err = provider.open_vod("00112233445566778899aabbccddeeff00112233").await.err();
    assert!(err.is_some(), "a bare infohash has no VOD descriptor");
}
```

(Adjust `Identity::generate` to the actual constructor used elsewhere in these tests — search `Identity::` usage in `ace_provider.rs` tests.)

- [ ] **Step 3: Implement `open_vod` on `AceProvider`**

Add a method (not on the `StreamProvider` trait, to avoid disturbing live callers):

```rust
impl AceProvider {
    /// Resolve `id` to a single-file VOD and start a verified download, returning a
    /// [`VodByteSource`]. Errors if the id is live, multi-file, or has no VOD descriptor.
    pub async fn open_vod(
        &self,
        id: &str,
    ) -> Result<Box<dyn crate::provider::VodByteSource>, ProviderError> {
        // Resolve to transport bytes, then VodInfo. A bare infohash carries no descriptor.
        let vod = self.resolve_vod_info(id).await?; // returns VodInfo or ProviderError
        let peers = self.discover_vod_peers(&vod).await; // trackers + DHT (reuse discover_peers)
        if peers.is_empty() {
            return Err(ProviderError::Backend("no VOD peers discovered".into()));
        }
        let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(64);
        let total = vod.total_length;
        tokio::spawn(async move {
            let _ = ace_swarm::vod::download_vod(vod, peers, tx).await;
        });
        Ok(Box::new(VodSource { rx, total, produced: 0 }))
    }
}

struct VodSource {
    rx: tokio::sync::mpsc::Receiver<Bytes>,
    total: u64,
    produced: u64,
}

#[async_trait]
impl crate::provider::VodByteSource for VodSource {
    fn content_length(&self) -> u64 { self.total }
    async fn next(&mut self) -> Option<Bytes> {
        let b = self.rx.recv().await?;
        self.produced += b.len() as u64;
        Some(b)
    }
}
```

Implement the two private helpers:
- `resolve_vod_info(&self, id) -> Result<VodInfo, ProviderError>`: For a `cid:`-prefixed id, fetch transport bytes via the catalog/peer path (reuse the existing content-id resolution to obtain the transport bytes, then `ace_swarm::resolve::vod_info_from_transport`). For a bare 40-hex infohash, return `ProviderError::Backend("bare infohash is not a VOD target".into())`. To obtain raw transport bytes without duplicating the catalog loop, add a small public `pub async fn catalog_transport_bytes(content_id: &str) -> Result<Vec<u8>, ResolveError>` to `resolve.rs` (extract the host loop from `resolve_via_catalog`, and have `resolve_via_catalog` call it then `stream_info_from_transport`). Map `ResolveError` → `ProviderError::Backend(format!("{e:?}"))`.
- `discover_vod_peers(&self, vod) -> Vec<SocketAddrV4>`: reuse `ace_swarm::discover::discover_peers` with `vod.trackers`, `vod.infohash`, a random peer id, and the provider's discovery port (mirror how `follow_live` calls `discover_peers`). Include `self.bootstrap_peers` if set.

- [ ] **Step 4: Run**

Run: `cargo test -p ace-engine open_vod`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/ace-engine/src/provider.rs crates/ace-engine/src/ace_provider.rs crates/ace-swarm/src/resolve.rs
git commit -m "feat(engine): AceProvider::open_vod VOD source seam (#47)"
```

---

### Task 6: HTTP `/vod/:network/:id` route

**Files:**
- Modify: `crates/ace-engine/src/http.rs`
- Test: `crates/ace-engine/src/http.rs` (`#[cfg(test)]`)

- [ ] **Step 1: Write a failing route test**

Use a test double: since `open_vod` needs the ace provider + network, gate the test on a small in-process `VodByteSource` returned by a test hook, OR assert routing + headers via a `VodSource`-like fake. Concretely, add a handler that, given an `AceProvider` in `AppState`, streams the body. Test that the route exists and returns 200 + `Content-Length` for a fake source by constructing the response body helper directly:

```rust
#[tokio::test]
async fn vod_body_response_sets_content_length() {
    // Unit-test the response builder rather than the full network path.
    let chunks = vec![Bytes::from_static(b"abc"), Bytes::from_static(b"de")];
    let resp = super::vod_response_from_chunks(5, chunks).await;
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    assert_eq!(resp.headers()[axum::http::header::CONTENT_LENGTH], "5");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p ace-engine vod_body_response`
Expected: FAIL — helper not found.

- [ ] **Step 3: Implement the route + helper**

Add the route to `router()`:

```rust
.route("/vod/:network/:id", get(vod_stream))
```

Handler + helper (`vod_stream` resolves via the ace provider; the helper builds the body):

```rust
use axum::body::Body;
use axum::http::{header, StatusCode};

async fn vod_stream(
    State(s): State<AppState>,
    Path((network, id)): Path<(String, String)>,
) -> Response {
    let Some(provider) = ace_network_provider(&s, &network) else {
        return (StatusCode::NOT_FOUND, "unknown network").into_response();
    };
    match provider.open_vod(&id).await {
        Ok(source) => vod_response_from_source(source).await,
        Err(e) => (StatusCode::BAD_GATEWAY, format!("{e:?}")).into_response(),
    }
}

/// Stream a `VodByteSource` as an HTTP body with a `Content-Length`.
async fn vod_response_from_source(mut source: Box<dyn crate::provider::VodByteSource>) -> Response {
    let total = source.content_length();
    let stream = async_stream::stream! {
        while let Some(chunk) = source.next().await {
            yield Ok::<_, std::io::Error>(chunk);
        }
    };
    axum::http::Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_LENGTH, total)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from_stream(stream))
        .unwrap()
        .into_response()
}

// Test-only convenience mirroring the body builder for a fixed chunk list.
#[cfg(test)]
async fn vod_response_from_chunks(total: u64, chunks: Vec<Bytes>) -> Response {
    let stream = futures_util::stream::iter(
        chunks.into_iter().map(|c| Ok::<_, std::io::Error>(c)),
    );
    axum::http::Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_LENGTH, total)
        .body(Body::from_stream(stream))
        .unwrap()
        .into_response()
}
```

Implementer notes:
- `ace_network_provider(&s, &network)` — add a small accessor returning the concrete `Arc<AceProvider>` from `AppState` (mirror the existing `ace_network(&s)` helper used by `/ace/*`). If `AppState` stores providers only as `Arc<dyn StreamProvider>`, thread the concrete `Arc<AceProvider>` into `AppState` (or downcast). Search how `ace_getstream` gets its provider and follow that pattern.
- `async-stream` and `futures-util` are already transitive deps via axum; if not direct deps of `ace-engine`, add `async-stream` and `futures-util` to `crates/ace-engine/Cargo.toml`.

- [ ] **Step 4: Run**

Run: `cargo test -p ace-engine vod`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/ace-engine/src/http.rs crates/ace-engine/Cargo.toml
git commit -m "feat(engine): GET /vod/:network/:id serves verified VOD bytes (#47)"
```

---

### Task 7: CLI `play` VOD dispatch

**Files:**
- Modify: `crates/ace-engine/src/cli.rs`
- Test: `crates/ace-engine/src/cli.rs` (`#[cfg(test)]`)

- [ ] **Step 1: Add a `--vod` flag / auto-detect + failing test**

Simplest deterministic contract: add a `--vod` boolean to `PlayArgs`. When set, `run_play` calls `open_vod` and writes verified bytes to stdout; otherwise unchanged (live). Test that the flag parses:

```rust
#[test]
fn play_accepts_vod_flag() {
    let cli = Cli::try_parse_from(["outpace", "play", "--vod",
        "acestream://0123456789abcdef0123456789abcdef01234567"]).unwrap();
    match cli.command {
        Command::Play(args) => assert!(args.vod),
        _ => panic!("expected play"),
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p ace-engine play_accepts_vod_flag`
Expected: FAIL — no field `vod`.

- [ ] **Step 3: Implement**

Add to `PlayArgs`:

```rust
/// Treat the target as a single-file VOD (download + SHA-1 verify to stdout) instead of live.
#[arg(long)]
pub vod: bool,
```

In `run_play`, after building `provider` and `target`:

```rust
if args.vod {
    use crate::provider::VodByteSource;
    let mut source = provider
        .open_vod(&target.provider_id)
        .await
        .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
    let mut stdout = tokio::io::stdout();
    while let Some(chunk) = source.next().await {
        stdout.write_all(&chunk).await?;
        stdout.flush().await?;
    }
    return Ok(());
}
```

- [ ] **Step 4: Run**

Run: `cargo test -p ace-engine play`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/ace-engine/src/cli.rs
git commit -m "feat(cli): outpace play --vod verified VOD download (#47)"
```

---

### Task 8: Docs, README surface, and full verification

**Files:**
- Modify: `docs/protocol/transport-file.md` (record the synthesized VOD `length`/`files` schema).
- Create: `docs/protocol/notes/54-vod-transport-and-playback.md` (implementation note).
- Modify: `README.md` if it documents the CLI/HTTP surface (add `--vod` / `/vod`).

- [ ] **Step 1: Update `transport-file.md`** — under the OPEN section, replace the "needs a VOD fixture" note with the implemented behavior: single-file `length` key, `files` ⇒ multi-file (rejected), SHA-1 `pieces` verified before serving; mark `length`/`files` as synthesized pending a real capture.

- [ ] **Step 2: Write `notes/54-vod-transport-and-playback.md`** summarizing: VOD = vanilla BitTorrent, the `download_vod` loop, verification gate, single-file scope, and the deferred follow-ups (HLS, range, multi-file, reseeding, live validation).

- [ ] **Step 3: Update README** CLI/API tables if present (search for `/streams` and `play` in `README.md`).

- [ ] **Step 4: Full workspace verification**

Run: `cargo build && cargo test`
Expected: builds clean; all tests pass (baseline + new). Note the pre-existing ignored/known ace-media fixture behavior is unchanged.

Run: `cargo clippy --all-targets 2>&1 | rg -n "warning|error" | head` and address new warnings in touched files.

- [ ] **Step 5: Commit**

```bash
git add docs/ README.md
git commit -m "docs: document VOD transport, playback, and follow-ups (#47)"
```

---

## Follow-ups (out of scope; create issues)

- VOD HLS packaging; HTTP byte-range/seek; multi-peer rarest-first VOD scheduling; reseeding downloaded VOD pieces via the existing `SeedRegistry`; multi-file / selected-file playback; live-swarm validation against a real public VOD transport (reconcile the synthesized `length`/`files` schema).
