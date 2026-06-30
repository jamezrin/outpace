# S1 — Compliance & Reciprocal Upload Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the daemon a good P2P citizen — retain downloaded pieces and **serve them back** to peers over connections we already hold (reciprocal upload), instead of being a pure leecher.

**Architecture:** A pure `PieceStore` (chunk-granular, rolling, bounded) holds what we download; a pure `Choker` decides who to unchoke; a `SeederSession` serve loop answers a peer's `Interested`/chunk-request with a `Piece` built from the store. The existing download loop (`AceProvider::follow_one_peer`) feeds the store and gains the serve arms, so we upload on the very connections we download from. No new sockets (that's S2). Wire-byte exactness vs the engine is pinned in the final ground-truth task.

**Tech Stack:** Rust, tokio, `ace-wire` (PeerMessage/live_codec), `ace-peer` (PeerSession), `ace-swarm`. TDD throughout.

**Spec:** `docs/superpowers/specs/2026-06-30-compliance-seeding-broadcasting-design.md` (phase S1).

---

## File structure

- Create `crates/ace-swarm/src/store.rs` — `PieceStore` (pure: chunk storage, bitfield, rolling eviction).
- Modify `crates/ace-wire/src/live_codec.rs` — add `build_piece` (the send side of the piece codec).
- Create `crates/ace-swarm/src/seed.rs` — `Choker` (pure policy) + `SeederSession` (serve loop).
- Modify `crates/ace-swarm/src/lib.rs` — `pub mod store; pub mod seed;`.
- Modify `crates/ace-engine/src/provider.rs` — extend `SourceStats` with `uploaded`/`peers_served`.
- Modify `crates/ace-engine/src/ace_provider.rs` — feed the store + serve arms in `follow_one_peer`; report new stats.
- Create `docs/protocol/notes/21-seeder-ground-truth.md` — engine-as-seeder capture (Task 7).

---

## Task 1: `PieceStore` (pure, chunk-granular, rolling)

**Files:**
- Create: `crates/ace-swarm/src/store.rs`
- Modify: `crates/ace-swarm/src/lib.rs`

- [ ] **Step 1: Add the module declaration** — in `crates/ace-swarm/src/lib.rs`, add after `pub mod scheduler;`:

```rust
pub mod seed;
pub mod store;
```

- [ ] **Step 2: Write the failing tests** — create `crates/ace-swarm/src/store.rs`:

```rust
//! A bounded, rolling store of downloaded (or broadcast) piece data, keyed by piece then chunk.
//! Feeds the seeder: we serve chunks we still hold. Pure (no I/O); eviction is FIFO by lowest
//! piece index once the byte budget is exceeded.
use std::collections::BTreeMap;

pub struct PieceStore {
    piece_length: u64,
    chunk_length: u64,
    max_bytes: u64,
    cur_bytes: u64,
    /// piece index -> (chunk index -> TS payload bytes)
    pieces: BTreeMap<u64, BTreeMap<u16, Vec<u8>>>,
}

impl PieceStore {
    pub fn new(piece_length: u64, chunk_length: u64, max_bytes: u64) -> Self {
        PieceStore { piece_length, chunk_length, max_bytes, cur_bytes: 0, pieces: BTreeMap::new() }
    }

    /// Chunks per piece (`piece_length / chunk_length`).
    pub fn chunks_per_piece(&self) -> u16 {
        (self.piece_length / self.chunk_length) as u16
    }

    /// Store a chunk's TS payload. Replacing an existing chunk adjusts the byte total. After the
    /// insert, evict the lowest-index pieces until within `max_bytes`.
    pub fn put_chunk(&mut self, piece: u64, chunk: u16, data: &[u8]) {
        let entry = self.pieces.entry(piece).or_default();
        if let Some(old) = entry.insert(chunk, data.to_vec()) {
            self.cur_bytes -= old.len() as u64;
        }
        self.cur_bytes += data.len() as u64;
        while self.cur_bytes > self.max_bytes {
            let Some((&lowest, _)) = self.pieces.iter().next() else { break };
            if let Some(removed) = self.pieces.remove(&lowest) {
                self.cur_bytes -= removed.values().map(|d| d.len() as u64).sum::<u64>();
            }
        }
    }

    /// The TS payload of `(piece, chunk)` if still held.
    pub fn chunk(&self, piece: u64, chunk: u16) -> Option<&[u8]> {
        self.pieces.get(&piece)?.get(&chunk).map(|v| v.as_slice())
    }

    /// True iff every chunk of `piece` is present.
    pub fn has_piece(&self, piece: u64) -> bool {
        self.pieces.get(&piece).is_some_and(|c| c.len() as u16 == self.chunks_per_piece())
    }

    /// Sorted indices of fully-held pieces (for `Have` advertisement).
    pub fn have_pieces(&self) -> Vec<u64> {
        self.pieces.keys().copied().filter(|&p| self.has_piece(p)).collect()
    }

    /// `(min, max)` stored piece indices, or None if empty.
    pub fn window(&self) -> Option<(u64, u64)> {
        let min = *self.pieces.keys().next()?;
        let max = *self.pieces.keys().next_back()?;
        Some((min, max))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 4-byte chunks, 2 chunks/piece, tiny budget for eviction tests.
    fn store(max_bytes: u64) -> PieceStore {
        PieceStore::new(8, 4, max_bytes)
    }

    #[test]
    fn stores_and_returns_a_chunk() {
        let mut s = store(1024);
        s.put_chunk(10, 0, &[1, 2, 3, 4]);
        assert_eq!(s.chunk(10, 0), Some(&[1, 2, 3, 4][..]));
        assert_eq!(s.chunk(10, 1), None);
        assert_eq!(s.chunk(11, 0), None);
    }

    #[test]
    fn has_piece_true_only_when_all_chunks_present() {
        let mut s = store(1024);
        assert_eq!(s.chunks_per_piece(), 2);
        s.put_chunk(5, 0, &[1, 2, 3, 4]);
        assert!(!s.has_piece(5));
        s.put_chunk(5, 1, &[5, 6, 7, 8]);
        assert!(s.has_piece(5));
        assert_eq!(s.have_pieces(), vec![5]);
    }

    #[test]
    fn window_reflects_min_and_max() {
        let mut s = store(1024);
        assert_eq!(s.window(), None);
        s.put_chunk(7, 0, &[0; 4]);
        s.put_chunk(9, 0, &[0; 4]);
        assert_eq!(s.window(), Some((7, 9)));
    }

    #[test]
    fn evicts_lowest_piece_when_over_budget() {
        // budget = 8 bytes = exactly two 4-byte chunks. A third chunk evicts the lowest piece.
        let mut s = store(8);
        s.put_chunk(1, 0, &[0; 4]);
        s.put_chunk(2, 0, &[0; 4]);
        assert_eq!(s.window(), Some((1, 2)));
        s.put_chunk(3, 0, &[0; 4]); // over budget -> drop piece 1
        assert_eq!(s.chunk(1, 0), None, "lowest piece evicted");
        assert_eq!(s.window(), Some((2, 3)));
    }

    #[test]
    fn replacing_a_chunk_does_not_double_count_bytes() {
        let mut s = store(8);
        s.put_chunk(1, 0, &[0; 4]);
        s.put_chunk(1, 0, &[9; 4]); // replace, not add
        s.put_chunk(1, 1, &[0; 4]);
        // Still one piece (1) with two chunks = 8 bytes, nothing evicted.
        assert_eq!(s.chunk(1, 0), Some(&[9, 9, 9, 9][..]));
        assert!(s.has_piece(1));
    }
}
```

- [ ] **Step 3: Run the tests, verify they pass**

Run: `cargo test -p ace-swarm --lib store 2>&1 | tail -8`
Expected: PASS (5 tests). (The module is small and the impl is included; if you prefer strict red-first, stub each method body with `unimplemented!()` first and watch one test fail.)

- [ ] **Step 4: Clippy**

Run: `cargo clippy -p ace-swarm --all-targets 2>&1 | grep -E "warning|error" | head`
Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add crates/ace-swarm/src/store.rs crates/ace-swarm/src/lib.rs
git commit -m "ace-swarm: PieceStore (rolling chunk store for seeding)"
```

---

## Task 2: `build_piece` — the send side of the live piece codec

**Files:**
- Modify: `crates/ace-wire/src/live_codec.rs`

- [ ] **Step 1: Write the failing test** — append to the `tests` module in `crates/ace-wire/src/live_codec.rs`:

```rust
    #[test]
    fn build_piece_roundtrips_through_live_chunk() {
        // What we SEND must decode back to the same (piece, chunk, data) a peer would read.
        let data = [1u8, 2, 3, 4];
        let msg = build_piece(0, 5269621, 7, [0xAB; 8], &data);
        match &msg {
            PeerMessage::Piece { index, begin, block } => {
                assert_eq!(*index, 0); // stream
                assert_eq!(*begin, 5269621); // piece
                assert_eq!(&block[..8], &[0xAB; 8]); // 8-byte piece header
                assert_eq!(&block[8..10], &7u16.to_be_bytes()); // chunk
                assert_eq!(&block[10..], &data); // payload
            }
            _ => panic!("expected Piece"),
        }
        let lc = LiveChunk::from_message(&msg).unwrap();
        assert_eq!(lc, LiveChunk { piece: 5269621, chunk: 7, data: data.to_vec() });
    }
```

- [ ] **Step 2: Run it, verify it fails to compile**

Run: `cargo test -p ace-wire --lib build_piece 2>&1 | tail -5`
Expected: error — `cannot find function build_piece`.

- [ ] **Step 3: Add `build_piece`** — in `crates/ace-wire/src/live_codec.rs`, after `chunk_request`:

```rust
/// Build the Acestream live `Piece` message (id=7) to SEND, the inverse of [`LiveChunk`]:
/// payload `[stream u32][piece u32][8B piece header][chunk u16][data]`. The 8-byte
/// `piece_header` is the engine's per-chunk header (pinned to ground truth in note 21; for a
/// broadcast source it is synthesized in B0/B1).
pub fn build_piece(stream: u32, piece: u32, chunk: u16, piece_header: [u8; 8], data: &[u8]) -> PeerMessage {
    let mut block = Vec::with_capacity(8 + 2 + data.len());
    block.extend_from_slice(&piece_header);
    block.extend_from_slice(&chunk.to_be_bytes());
    block.extend_from_slice(data);
    PeerMessage::Piece { index: stream, begin: piece, block }
}
```

- [ ] **Step 4: Run it, verify it passes**

Run: `cargo test -p ace-wire --lib build_piece 2>&1 | tail -5`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/ace-wire/src/live_codec.rs
git commit -m "ace-wire: build_piece (send side of the live piece codec)"
```

---

## Task 3: `Choker` (pure unchoke policy)

**Files:**
- Create: `crates/ace-swarm/src/seed.rs`

- [ ] **Step 1: Write the failing tests** — create `crates/ace-swarm/src/seed.rs`:

```rust
//! Serving peers: a pure unchoke policy (`Choker`) and the `SeederSession` serve loop.

/// Decides which interested peers to unchoke. Live-appropriate: unchoke up to `max_unchoked`
/// interested peers (stable order) plus one rotating "optimistic" peer so newcomers get a turn.
pub struct Choker {
    max_unchoked: usize,
}

impl Choker {
    pub fn new(max_unchoked: usize) -> Self {
        Choker { max_unchoked }
    }

    /// Peers to unchoke now. `interested` is the current interested set (caller-stable order);
    /// `tick` rotates the optimistic slot over time.
    pub fn choose(&self, interested: &[u64], tick: u64) -> Vec<u64> {
        let mut out: Vec<u64> = interested.iter().take(self.max_unchoked).copied().collect();
        let rest = &interested[out.len()..];
        if !rest.is_empty() {
            out.push(rest[(tick as usize) % rest.len()]);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unchokes_up_to_max_plus_one_optimistic() {
        let c = Choker::new(2);
        // first 2 always unchoked; the 3rd slot rotates through the remainder by tick.
        assert_eq!(c.choose(&[10, 20, 30, 40], 0), vec![10, 20, 30]);
        assert_eq!(c.choose(&[10, 20, 30, 40], 1), vec![10, 20, 40]);
        assert_eq!(c.choose(&[10, 20, 30, 40], 2), vec![10, 20, 30]); // wraps
    }

    #[test]
    fn fewer_interested_than_max_unchokes_all() {
        let c = Choker::new(4);
        assert_eq!(c.choose(&[10, 20], 0), vec![10, 20]);
        assert_eq!(c.choose(&[], 0), Vec::<u64>::new());
    }
}
```

- [ ] **Step 2: Run the tests, verify they pass**

Run: `cargo test -p ace-swarm --lib seed::tests::choker 2>&1 | tail -6` (or `seed::tests` — both choker tests).
Expected: PASS (2 tests).

- [ ] **Step 3: Commit**

```bash
git add crates/ace-swarm/src/seed.rs
git commit -m "ace-swarm: Choker unchoke policy (pure)"
```

---

## Task 4: `SeederSession` serve loop (mock-peer duplex)

**Files:**
- Modify: `crates/ace-swarm/src/seed.rs`
- Modify: `crates/ace-swarm/Cargo.toml` (ensure dev-dep `tokio` has `io-util`,`macros`,`rt`,`sync`)

This serves ONE peer over an existing connection: advertise our held pieces, then on a peer's
chunk-request reply with a `Piece` from the store (when unchoked). It returns when the peer
closes. The 8-byte piece header is a parameter here (pinned in Task 7).

- [ ] **Step 1: Write the failing test** — append to the `tests` module in `crates/ace-swarm/src/seed.rs`:

```rust
    use crate::store::PieceStore;
    use ace_peer::session::PeerSession;
    use ace_wire::live_codec::{chunk_request, LiveChunk};
    use ace_wire::message::PeerMessage;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn serves_a_requested_chunk_from_the_store() {
        // Store holds piece 5, chunk 0 = [9,9,9,9] (geometry: 4-byte chunks, 1 chunk/piece).
        let store = Arc::new(Mutex::new(PieceStore::new(4, 4, 1024)));
        store.lock().await.put_chunk(5, 0, &[9, 9, 9, 9]);

        let (client, server) = tokio::io::duplex(64 * 1024);

        // The "peer": expresses interest, requests (piece 5, chunk 0), reads back the Piece.
        let peer = tokio::spawn(async move {
            let mut p = PeerSession::new(client);
            p.send(&PeerMessage::Interested).await.unwrap();
            p.send(&chunk_request(5, 0)).await.unwrap();
            loop {
                match p.read_message().await.unwrap() {
                    m @ PeerMessage::Piece { .. } => {
                        return LiveChunk::from_message(&m).unwrap();
                    }
                    _ => continue, // skip Unchoke / advertisements
                }
            }
        });

        // Our seeder serves the peer until it closes.
        let mut us = PeerSession::new(server);
        let serve = SeederSession::serve(&mut us, store, [0u8; 8]);
        // Run the serve loop until the peer has its chunk, then drop.
        let got = tokio::select! {
            r = peer => r.unwrap(),
            _ = serve => panic!("serve ended before peer got its chunk"),
        };
        assert_eq!(got, LiveChunk { piece: 5, chunk: 0, data: vec![9, 9, 9, 9] });
    }
```

- [ ] **Step 2: Run it, verify it fails to compile**

Run: `cargo test -p ace-swarm --lib seed::tests::serves_a_requested 2>&1 | tail -5`
Expected: error — `cannot find ... SeederSession`.

- [ ] **Step 3: Implement `SeederSession`** — in `crates/ace-swarm/src/seed.rs`, above the tests, add:

```rust
use crate::store::PieceStore;
use ace_peer::session::PeerSession;
use ace_peer::Result;
use ace_wire::live_codec::build_piece;
use ace_wire::message::PeerMessage;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Mutex;

pub struct SeederSession;

impl SeederSession {
    /// Serve one already-connected peer from `store`: advertise held pieces, then answer each
    /// Acestream chunk-request (id=6 `[stream u32][piece u32][chunk u16]`) with a `Piece` built
    /// from the store, after unchoking on the peer's first `Interested`. `piece_header` is the
    /// 8-byte per-chunk header (pinned to engine ground truth in note 21). Returns on close.
    pub async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
        session: &mut PeerSession<S>,
        store: Arc<Mutex<PieceStore>>,
        piece_header: [u8; 8],
    ) -> Result<()> {
        // Advertise what we currently hold (one Have per complete piece).
        for piece in store.lock().await.have_pieces() {
            session.send(&PeerMessage::Have(piece as u32)).await?;
        }
        let mut unchoked = false;
        loop {
            let msg = session.read_message().await?;
            match msg {
                PeerMessage::Interested if !unchoked => {
                    session.send(&PeerMessage::Unchoke).await?;
                    unchoked = true;
                }
                PeerMessage::Unknown { id: 6, payload } if payload.len() >= 10 => {
                    let piece = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
                    let chunk = u16::from_be_bytes([payload[8], payload[9]]);
                    let data = store.lock().await.chunk(piece as u64, chunk).map(|d| d.to_vec());
                    if let Some(data) = data {
                        let reply = build_piece(0, piece, chunk, piece_header, &data);
                        session.send(&reply).await?;
                    }
                    // Missing/evicted chunk: silently skip (a future task may send a reject).
                }
                _ => {}
            }
        }
    }
}
```

- [ ] **Step 4: Run it, verify it passes**

Run: `cargo test -p ace-swarm --lib seed 2>&1 | tail -8`
Expected: PASS (Choker tests + `serves_a_requested_chunk_from_the_store`).

- [ ] **Step 5: Clippy**

Run: `cargo clippy -p ace-swarm --all-targets 2>&1 | grep -E "warning|error" | head`
Expected: no output.

- [ ] **Step 6: Commit**

```bash
git add crates/ace-swarm/src/seed.rs crates/ace-swarm/Cargo.toml
git commit -m "ace-swarm: SeederSession serve loop (advertise + answer chunk-requests)"
```

---

## Task 5: Extend `SourceStats` with upload counters

**Files:**
- Modify: `crates/ace-engine/src/provider.rs`

- [ ] **Step 1: Write the failing test** — append to the `tests` module in `crates/ace-engine/src/provider.rs`:

```rust
    #[test]
    fn source_stats_has_upload_counters() {
        let s = SourceStats { peers: 1, bitrate: 0, buffer_ms: 0, uploaded: 4096, peers_served: 2 };
        assert_eq!(s.uploaded, 4096);
        assert_eq!(s.peers_served, 2);
    }
```

- [ ] **Step 2: Run it, verify it fails to compile**

Run: `cargo test -p ace-engine --lib source_stats_has_upload 2>&1 | tail -5`
Expected: error — `struct SourceStats has no field named uploaded`.

- [ ] **Step 3: Add the fields** — in `crates/ace-engine/src/provider.rs`, extend the struct:

```rust
pub struct SourceStats {
    pub peers: u32,
    pub bitrate: u64,   // bits/sec
    pub buffer_ms: u64, // buffered duration estimate
    /// Bytes we have uploaded (served to peers) on this source.
    pub uploaded: u64,
    /// Distinct peers we have served at least one chunk to.
    pub peers_served: u32,
}
```

- [ ] **Step 4: Fix the other `SourceStats { .. }` constructors** — they now miss two fields. Find them:

Run: `grep -rn "SourceStats {" crates/ --include=*.rs`
For each literal (in `testprovider.rs`, `ace_provider.rs`, and any test), add `uploaded: 0, peers_served: 0,`. (`SourceStats::default()` usages are unaffected — `Default` fills the new `u64`/`u32` with 0.)

- [ ] **Step 5: Run it, verify it passes**

Run: `cargo test -p ace-engine --lib source_stats_has_upload 2>&1 | tail -5 && cargo build -p ace-engine 2>&1 | tail -2`
Expected: test PASS, build OK.

- [ ] **Step 6: Surface them in `/status`** — in `crates/ace-engine/src/http.rs`, in `stream_status`, add to the JSON after `"buffer_ms": stats.buffer_ms,`:

```rust
                "uploaded": stats.uploaded,
                "peers_served": stats.peers_served,
```

- [ ] **Step 7: Run the engine tests + clippy**

Run: `cargo test -p ace-engine 2>&1 | grep "test result" && cargo clippy -p ace-engine --all-targets 2>&1 | grep -E "warning|error" | head`
Expected: all PASS, clippy clean.

- [ ] **Step 8: Commit**

```bash
git add crates/ace-engine/src/provider.rs crates/ace-engine/src/http.rs crates/ace-engine/src/testprovider.rs crates/ace-engine/src/ace_provider.rs
git commit -m "ace-engine: SourceStats upload counters + /status fields"
```

---

## Task 6: Reciprocate — feed the store and serve in `follow_one_peer`

**Files:**
- Modify: `crates/ace-engine/src/ace_provider.rs`

The download loop already receives `LiveChunk`s and reassembles TS. Now also (a) put each chunk
into a shared `PieceStore`, and (b) answer the peer's `Interested`/chunk-request from it, so we
upload on the connection we download from. This reuses the exact serve logic from Task 4.

- [ ] **Step 1: Add imports + a store to the follow path** — at the top of `crates/ace-engine/src/ace_provider.rs` add:

```rust
use ace_swarm::store::PieceStore;
use ace_wire::live_codec::build_piece;
use std::sync::Mutex as StdMutex;
```

In `follow_one_peer`, where `reasm` and `resync` are created, add a store sized from config
(use a constant for now):

```rust
    // Retain recently-downloaded pieces so we can serve (reseed) peers on this connection.
    const SEED_STORE_BYTES: u64 = 128 * 1024 * 1024;
    let store = StdMutex::new(PieceStore::new(info.piece_length, info.chunk_length, SEED_STORE_BYTES));
    let mut served_bytes: u64 = 0;
    let mut unchoked_peer = false;
```

- [ ] **Step 2: Write the failing integration test** — create `crates/ace-engine/tests/reciprocate.rs`:

```rust
//! A mock peer connects, we (the downloader) also SERVE it a chunk we hold — proving reciprocal
//! upload on the same connection. Drives the serve arms directly via a PieceStore + the seeder.
use ace_peer::session::PeerSession;
use ace_swarm::seed::SeederSession;
use ace_swarm::store::PieceStore;
use ace_wire::live_codec::{chunk_request, LiveChunk};
use ace_wire::message::PeerMessage;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::test]
async fn peer_downloads_a_chunk_from_us() {
    let store = Arc::new(Mutex::new(PieceStore::new(4, 4, 1024)));
    store.lock().await.put_chunk(42, 0, &[7, 7, 7, 7]);

    let (client, server) = tokio::io::duplex(64 * 1024);
    let peer = tokio::spawn(async move {
        let mut p = PeerSession::new(client);
        p.send(&PeerMessage::Interested).await.unwrap();
        p.send(&chunk_request(42, 0)).await.unwrap();
        loop {
            if let m @ PeerMessage::Piece { .. } = p.read_message().await.unwrap() {
                return LiveChunk::from_message(&m).unwrap();
            }
        }
    });

    let mut us = PeerSession::new(server);
    let got = tokio::select! {
        r = peer => r.unwrap(),
        _ = SeederSession::serve(&mut us, store, [0u8; 8]) => panic!("ended early"),
    };
    assert_eq!(got, LiveChunk { piece: 42, chunk: 0, data: vec![7, 7, 7, 7] });
}
```

- [ ] **Step 3: Run it, verify it passes**

Run: `cargo test -p ace-engine --test reciprocate 2>&1 | tail -6`
Expected: PASS. (This proves the seam end-to-end via the public `ace-swarm` API; it is the offline contract for the in-loop wiring.)

- [ ] **Step 4: Wire the serve arms into the live `Piece` handler** — in `follow_one_peer`, inside the `m @ PeerMessage::Piece { .. }` arm, after `reasm.add_block(...)` succeeds and the existing `resync`/`tx.send` block, also feed the store:

```rust
                if let Some(lc) = LiveChunk::from_message(&m) {
                    store.lock().unwrap().put_chunk(lc.piece as u64, lc.chunk as u64 as u16, &lc.data);
                }
```

And add serve arms to the message `match` (alongside `Unchoke`/`Have`/`Piece`):

```rust
            PeerMessage::Interested => {
                if !unchoked_peer {
                    let _ = session.send(&PeerMessage::Unchoke).await;
                    unchoked_peer = true;
                }
            }
            PeerMessage::Unknown { id: 6, ref payload } if payload.len() >= 10 => {
                let p = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
                let c = u16::from_be_bytes([payload[8], payload[9]]);
                let data = store.lock().unwrap().chunk(p as u64, c).map(|d| d.to_vec());
                if let Some(data) = data {
                    // piece_header [0u8;8] until note 21 pins the engine's bytes.
                    if session.send(&build_piece(0, p, c, [0u8; 8], &data)).await.is_ok() {
                        served_bytes += data.len() as u64;
                    }
                }
            }
```

- [ ] **Step 5: Report upload stats** — in the `AceSource`/stats path, surface `served_bytes`. In the `SourceStats` returned by `AceSource::stats`, set `uploaded` from a shared `AtomicU64` updated by the follow loop (mirror the existing `peers` `AtomicU32` pattern: add `uploaded: Arc<AtomicU64>`, `fetch_add(data.len())` after a successful serve, read it in `stats()`).

- [ ] **Step 6: Run engine tests + clippy**

Run: `cargo test -p ace-engine 2>&1 | grep "test result" && cargo clippy -p ace-engine --all-targets 2>&1 | grep -E "warning|error" | head`
Expected: all PASS, clippy clean.

- [ ] **Step 7: Commit**

```bash
git add crates/ace-engine/src/ace_provider.rs crates/ace-engine/tests/reciprocate.rs
git commit -m "ace-engine: reciprocal upload — feed PieceStore + serve peers in the follow loop"
```

---

## Task 7: Pin the wire bytes against engine-as-seeder ground truth (RE / sandbox)

**Files:**
- Create: `docs/protocol/notes/21-seeder-ground-truth.md`
- Create: `tests/vectors/seed/` (committed capture artifacts)
- Modify: `crates/ace-wire/src/live_codec.rs` (pin the 8-byte piece header), `crates/ace-swarm/src/seed.rs` (advertisement form if it differs)

This is RE/operator-run (needs the sandbox engine + a non-WARP network), like the v1 live tasks.
It makes our serve bytes **byte-identical** to the engine so an Acestream peer can't distinguish us.

- [ ] **Step 1: Capture the engine serving a peer.** Start the sandbox engine (`re/sandbox/docker-compose.yml`) on a live channel it is seeding. With our `live_recon` harness acting as a *downloader*, capture the engine's serve-side bytes: the advertisement after the extended handshake (is it `Bitfield`, or BEP-6 `HAVE_ALL`/`HAVE_NONE`/`ALLOWED_FAST`?), the exact `Piece` reply for a known `(piece, chunk)` (record the 8-byte header), and unchoke timing. Save raw frames under `tests/vectors/seed/`.

- [ ] **Step 2: Determine the 8-byte piece header.** From the captured `Piece` frames, derive what the header encodes (constant? piece index? a per-chunk field?). Document it in note 21.

- [ ] **Step 3: Write a byte-exact test** — in `crates/ace-wire/src/live_codec.rs`, add a test that `build_piece(stream, piece, chunk, <derived header>, data)` reproduces a captured engine `Piece` frame byte-for-byte (load the vector from `tests/vectors/seed/`). If the header is non-constant, replace the `[u8;8]` parameter with a small `piece_header(piece, chunk) -> [u8;8]` derived per the finding, and update Task 4/6 call sites.

- [ ] **Step 4: Align the advertisement** — if the engine advertises with `HAVE_ALL`/`HAVE_NONE`/`ALLOWED_FAST` rather than per-piece `Have`/`Bitfield`, update `SeederSession::serve` (and the follow-loop serve arm) to emit the same messages in the same order, asserted against the captured fixture.

- [ ] **Step 5: Interop check (live).** Point the sandbox engine at our infohash as a *peer* and confirm it **downloads a piece from us** (the inverse of v1's proven download). Record the result in note 21.

- [ ] **Step 6: Run the full suite + clippy, then commit**

```bash
cargo test --workspace 2>&1 | grep "test result"
cargo clippy --workspace --all-targets 2>&1 | grep -E "warning|error" | head
git add crates/ace-wire/src/live_codec.rs crates/ace-swarm/src/seed.rs tests/vectors/seed docs/protocol/notes/21-seeder-ground-truth.md
git commit -m "seed: pin serve-side wire bytes to engine ground truth (note 21); engine downloads from us"
```

---

## Self-review notes
- **Spec coverage (S1):** PieceStore (Task 1), build_piece (Task 2), Choker (Task 3), SeederSession serve loop (Task 4), reciprocation in the download loop (Task 6), `uploaded`/`peers_served` stats (Tasks 5–6), engine-as-seeder ground-truth + byte-exactness + bidirectional interop (Task 7). All S1 spec bullets map to a task.
- **Wire compatibility:** Tasks 1–6 build and unit/integration-test the mechanism with synthetic data and a placeholder `[0u8;8]` header; Task 7 pins the real bytes against captured engine behavior and proves the engine downloads from us — the spec's hard constraint.
- **Deferred to S2:** inbound TCP listener + seeder announce (no new sockets in S1).
- **Type consistency:** `PieceStore::{new,put_chunk,chunk,has_piece,have_pieces,window,chunks_per_piece}`, `Choker::{new,choose}`, `SeederSession::serve(session, store: Arc<Mutex<PieceStore>>, piece_header: [u8;8])`, `build_piece(stream,piece,chunk,piece_header,data)`, `SourceStats{..,uploaded:u64,peers_served:u32}` — used consistently across tasks.
