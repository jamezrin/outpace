# outpace daemon (v1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the proven Acestream live-download path into a multi-client daemon that plays live streams in VLC via a clean, provider-abstracted HTTP API (`/streams/{network}/{id}.ts`).

**Architecture:** A provider-agnostic streaming engine (`ace-engine`) fans one shared MPEG-TS source out to many HTTP clients. All Acestream protocol lives behind a `StreamProvider` adapter (`ace`), which uses promoted library code in `ace-wire` (full signed handshake + live chunk codec) and `ace-swarm` (`LiveSession`, tracker discovery, reassembly). Spec: `docs/superpowers/specs/2026-06-29-outpace-daemon-design.md`.

**Tech Stack:** Rust, tokio, axum 0.7, bytes, async-trait. Existing crates: ace-wire, ace-tracker, ace-peer, ace-media, ace-swarm, ace-engine. Protocol facts: `docs/protocol/notes/17-19`.

**Hard constraints (`memory/outpace-api-constraints.md`):** public API (paths, JSON keys, binary, config) contains no `ace`/`acestream` token **except** the `{network}` value `"ace"`; never call any Acestream HTTP/index API; one shared download per `(network,id)` fanned out to many clients.

---

## Phase 1 — Promote the live protocol into the library

### Task 1: Full signed client handshake in `ace-wire`

Promote the complete accepted handshake (note 19) into `OutgoingExtendedHandshake::sign_and_encode`, which today emits only a minimal field set.

**Files:**
- Modify: `crates/ace-wire/src/extended.rs`
- Test: `crates/ace-wire/src/extended.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Write the failing test** — append to the `tests` module in `crates/ace-wire/src/extended.rs`:

```rust
#[test]
fn full_signed_handshake_has_all_accepted_fields_and_verifies() {
    use crate::bencode::Bencode;
    use crate::identity::{verify_handshake, Identity};
    let id = Identity::generate();
    let hs = OutgoingExtendedHandshake {
        ace_metadata_version: 1,
        ut_metadata_id: 2,
        mi: Some(LivePosition { min_piece: 100, max_piece: 163, position: -1, distance_from_source: -1 }),
        node: NodeFields { ts: 5000, ..NodeFields::default() },
        peer_ip: Some([95, 17, 44, 10]),
    };
    let payload = hs.sign_and_encode(&id);
    let dict = match Bencode::parse(&payload).unwrap() { Bencode::Dict(d) => d, _ => panic!() };
    for k in [b"ace_metadata_version".as_slice(), b"asn", b"asn_country", b"geoip_country",
              b"lsp", b"m", b"mi", b"node_id", b"nt", b"p", b"platform", b"pv",
              b"signature", b"stream_statuses", b"ts", b"tt", b"v", b"yourip"] {
        assert!(dict.contains_key(k), "missing key {:?}", std::str::from_utf8(k));
    }
    assert_eq!(dict[b"tt".as_slice()].as_bytes(), Some(b"bt".as_slice()));
    assert_eq!(dict[b"yourip".as_slice()].as_bytes(), Some([95u8,17,44,10].as_slice()));
    let mi = match &dict[b"mi".as_slice()] { Bencode::Dict(d) => d, _ => panic!() };
    assert_eq!(mi[b"min_piece".as_slice()].as_int(), Some(100));
    assert_eq!(mi[b"max_piece".as_slice()].as_int(), Some(163));
    let node_id: [u8;32] = dict[b"node_id".as_slice()].as_bytes().unwrap().try_into().unwrap();
    let sig: [u8;64] = dict[b"signature".as_slice()].as_bytes().unwrap().try_into().unwrap();
    assert!(verify_handshake(&node_id, &sig, &dict));
}
```

- [ ] **Step 2: Run it, verify it fails to compile** — `peer_ip` field doesn't exist yet.

Run: `cargo test -p ace-wire --lib full_signed_handshake 2>&1 | tail -5`
Expected: compile error `struct OutgoingExtendedHandshake has no field named peer_ip`.

- [ ] **Step 3: Add the `peer_ip` field** to `OutgoingExtendedHandshake` (in `crates/ace-wire/src/extended.rs`):

```rust
    /// Node identity/announce fields signed into the handshake.
    pub node: NodeFields,
    /// The recipient peer's IP as 4 bytes (the `yourip` field; anti-spoof). None to omit.
    pub peer_ip: Option<[u8; 4]>,
}
```

- [ ] **Step 4: Replace `sign_and_encode`** body with the full field set (note 19). Replace the existing `sign_and_encode` method:

```rust
    /// Build the payload carrying our node identity and a valid signature over the FULL
    /// accepted field set (note 19): identity + announce fields + rich `mi` + `yourip`.
    pub fn sign_and_encode(&self, id: &Identity) -> Vec<u8> {
        use crate::identity::handshake_digest;
        let bi = |n: i64| Bencode::Int(n);
        let bb = |b: &[u8]| Bencode::Bytes(b.to_vec());
        let mut f = self.base_fields(); // ace_metadata_version, m, mi(min/max/pos/dist)
        // Promote mi to the full live-position dict if present.
        if let Some(p) = self.mi {
            let mut mi: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
            for (k, v) in [
                ("distance_from_source", p.distance_from_source), ("down_rate", 0),
                ("download_window_end", -1), ("is_accessible", 0),
                ("live_window_size", (p.max_piece - p.min_piece + 1).max(0)), ("lsp", -1),
                ("mam", -1), ("max_piece", p.max_piece), ("min_piece", p.min_piece),
                ("peer_type", 0), ("ping_from_source", -1), ("position", p.position),
                ("time_from_source", -1), ("top_session_up_rate", 0), ("top_up_rate", 0),
                ("up_rate", 0), ("upload_rating", 0),
            ] { mi.insert(k.as_bytes().to_vec(), bi(v)); }
            f.insert(b"mi".to_vec(), Bencode::Dict(mi));
        }
        f.insert(b"asn".to_vec(), bi(0));
        f.insert(b"asn_country".to_vec(), bb(b""));
        f.insert(b"geoip_country".to_vec(), bb(b""));
        f.insert(b"lsp".to_vec(), bi(-1));
        f.insert(b"node_id".to_vec(), bb(&id.node_id()));
        f.insert(b"nt".to_vec(), bi(self.node.nt));
        f.insert(b"p".to_vec(), bi(self.node.p));
        f.insert(b"platform".to_vec(), bi(self.node.platform));
        f.insert(b"pv".to_vec(), bi(self.node.pv));
        f.insert(b"stream_statuses".to_vec(), Bencode::Dict(BTreeMap::new()));
        f.insert(b"ts".to_vec(), bi(self.node.ts));
        f.insert(b"tt".to_vec(), bb(b"bt"));
        f.insert(b"v".to_vec(), bi(self.node.v));
        if let Some(ip) = self.peer_ip { f.insert(b"yourip".to_vec(), bb(&ip)); }
        let digest = handshake_digest(&f);
        f.insert(b"signature".to_vec(), Bencode::Bytes(id.sign(&digest).to_vec()));
        Bencode::Dict(f).encode()
    }
```

- [ ] **Step 5: Update existing constructors** in the same file's test module and any callers to add `peer_ip: None`. Find them:

Run: `grep -rn "OutgoingExtendedHandshake {" crates/`
For each occurrence (two in `extended.rs` tests, two in `ace-peer/src/session.rs`), add `peer_ip: None,` after the `node:` line.

- [ ] **Step 6: Run tests** — `cargo test -p ace-wire --lib extended 2>&1 | tail -3`
Expected: PASS (incl. `full_signed_handshake_has_all_accepted_fields_and_verifies`).

- [ ] **Step 7: Commit**

```bash
git add crates/ace-wire/src/extended.rs crates/ace-peer/src/session.rs
git commit -m "ace-wire: full signed client handshake (complete accepted field set, note 19)"
```

### Task 2: Live chunk request/piece codec in `ace-wire`

**Files:**
- Create: `crates/ace-wire/src/live_codec.rs`
- Modify: `crates/ace-wire/src/lib.rs` (add `pub mod live_codec;`)
- Test: in `live_codec.rs`

- [ ] **Step 1: Write the failing test** — create `crates/ace-wire/src/live_codec.rs`:

```rust
//! Acestream live chunk request/piece wire helpers (note 19).
//! Request: peer message id=6 with payload `[stream u32=0][piece u32][chunk u16]`.
//! Piece:   peer message id=7 with payload `[stream u32][piece u32][8B piece hdr][chunk u16][16384 data]`,
//!          which our decoder surfaces as PeerMessage::Piece { index=stream, begin=piece, block }.
use crate::message::PeerMessage;

/// Build the Acestream chunk-request message for (piece, chunk).
pub fn chunk_request(piece: u32, chunk: u16) -> PeerMessage {
    let mut payload = vec![0u8, 0, 0, 0]; // stream index = 0
    payload.extend_from_slice(&piece.to_be_bytes());
    payload.extend_from_slice(&chunk.to_be_bytes());
    PeerMessage::Unknown { id: 6, payload }
}

/// A received live chunk: its piece/chunk coordinates and the 16384-byte TS payload.
#[derive(Debug, PartialEq, Eq)]
pub struct LiveChunk {
    pub piece: u32,
    pub chunk: u16,
    pub data: Vec<u8>,
}

impl LiveChunk {
    /// Interpret a decoded `Piece` message as a live chunk: `begin`=piece, `block`=
    /// `[8B piece hdr][chunk u16][data]`. Returns None if the block is too short.
    pub fn from_message(msg: &PeerMessage) -> Option<LiveChunk> {
        if let PeerMessage::Piece { begin, block, .. } = msg {
            if block.len() < 10 { return None; }
            let chunk = u16::from_be_bytes([block[8], block[9]]);
            Some(LiveChunk { piece: *begin, chunk, data: block[10..].to_vec() })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_has_acestream_10_byte_payload() {
        let m = chunk_request(0x005067f8, 3);
        match m {
            PeerMessage::Unknown { id, payload } => {
                assert_eq!(id, 6);
                assert_eq!(payload, vec![0,0,0,0, 0x00,0x50,0x67,0xf8, 0x00,0x03]);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn parse_piece_into_live_chunk() {
        let mut block = vec![0xAAu8; 8];          // 8-byte piece header
        block.extend_from_slice(&7u16.to_be_bytes()); // chunk 7
        block.extend_from_slice(&[1, 2, 3, 4]);   // data
        let msg = PeerMessage::Piece { index: 0, begin: 5269621, block };
        let lc = LiveChunk::from_message(&msg).unwrap();
        assert_eq!(lc, LiveChunk { piece: 5269621, chunk: 7, data: vec![1,2,3,4] });
    }

    #[test]
    fn non_piece_message_is_none() {
        assert!(LiveChunk::from_message(&PeerMessage::Unchoke).is_none());
    }
}
```

- [ ] **Step 2: Add module** — in `crates/ace-wire/src/lib.rs` add `pub mod live_codec;` near the other `pub mod` lines.

- [ ] **Step 3: Run tests** — `cargo test -p ace-wire --lib live_codec 2>&1 | tail -3`
Expected: PASS (3 tests).

- [ ] **Step 4: Commit**

```bash
git add crates/ace-wire/src/live_codec.rs crates/ace-wire/src/lib.rs
git commit -m "ace-wire: live chunk request/piece codec (note 19)"
```

### Task 3: `StreamInfo` + `LiveParams` types in `ace-swarm`

**Files:**
- Create: `crates/ace-swarm/src/types.rs`
- Modify: `crates/ace-swarm/src/lib.rs` (add `pub mod types;`)

- [ ] **Step 1: Write the failing test** — create `crates/ace-swarm/src/types.rs`:

```rust
//! Stream descriptors shared across resolution and download.

/// What a stream needs to be downloaded, from the transport file (or known directly).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamInfo {
    /// 20-byte BitTorrent infohash of the live stream.
    pub infohash: [u8; 20],
    /// Bytes per piece (e.g. 1_048_576).
    pub piece_length: u64,
    /// Bytes per chunk (e.g. 16_384).
    pub chunk_length: u64,
    /// Tracker URLs from the transport file (UDP `udp://host:port` entries used).
    pub trackers: Vec<String>,
}

impl StreamInfo {
    /// Number of chunks per piece (piece_length / chunk_length).
    pub fn chunks_per_piece(&self) -> u16 {
        (self.piece_length / self.chunk_length) as u16
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn chunks_per_piece_is_64_for_1mib_pieces() {
        let si = StreamInfo { infohash: [0;20], piece_length: 1_048_576, chunk_length: 16_384, trackers: vec![] };
        assert_eq!(si.chunks_per_piece(), 64);
    }
}
```

- [ ] **Step 2: Add module** — `pub mod types;` in `crates/ace-swarm/src/lib.rs`.
- [ ] **Step 3: Run** — `cargo test -p ace-swarm --lib types 2>&1 | tail -3` → PASS.
- [ ] **Step 4: Commit** — `git add -A && git commit -m "ace-swarm: StreamInfo/LiveParams types"`

### Task 4: `LiveSession` single-peer continuous download in `ace-swarm`

A continuous live driver: given a connected+handshaken peer session and `StreamInfo`, request chunks for consecutive pieces starting at the peer's live head, reassemble into contiguous TS, and push TS to an `mpsc` channel until told to stop. (Multi-peer comes in Phase 5; single-peer is the proven MVP.)

**Files:**
- Create: `crates/ace-swarm/src/live.rs`
- Modify: `crates/ace-swarm/src/lib.rs` (add `pub mod live;`), `crates/ace-swarm/Cargo.toml` (deps: `bytes = "1"`, `tracing = "0.1"`)
- Test: integration `crates/ace-swarm/tests/live_session.rs` (mock peer)

- [ ] **Step 1: Write the failing integration test** — create `crates/ace-swarm/tests/live_session.rs`. A mock peer accepts our handshake, unchokes, and serves `chunk_request`s with TS-bearing `Piece` messages; assert the LiveSession emits contiguous TS.

```rust
use ace_peer::session::PeerSession;
use ace_wire::handshake::Handshake;
use ace_wire::live_codec::chunk_request;
use ace_wire::message::PeerMessage;
use ace_swarm::live::{LiveSession, LiveConfig};
use ace_swarm::types::StreamInfo;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn ts_byte(i: usize) -> u8 { if i % 188 == 0 { 0x47 } else { (i % 251) as u8 } }

#[tokio::test]
async fn live_session_emits_contiguous_ts_from_one_peer() {
    let infohash = [0x42u8; 20];
    // Tiny geometry for the test: piece=2 chunks, chunk=4 bytes.
    let info = StreamInfo { infohash, piece_length: 8, chunk_length: 4, trackers: vec![] };
    let start_piece = 10u32;
    let pieces = 3u32;
    // Build the contiguous TS the peer will serve for pieces [10,13): 3*8 = 24 bytes.
    let content: Vec<u8> = (0..(pieces as usize * 8)).map(ts_byte).collect();
    let content_peer = content.clone();

    let (client, mut server) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        let mut hs = [0u8; 66];
        server.read_exact(&mut hs).await.unwrap();
        server.write_all(&Handshake::new(infohash, *b"R30------MOCKLIVE0001"[..20].try_into().unwrap()).encode()).await.unwrap();
        let mut sess = PeerSession::new(server);
        // our extended handshake
        let _ = sess.read_message().await.unwrap();
        // advertise live window via a minimal extended handshake then unchoke
        sess.send(&PeerMessage::Extended { ext_id: 0, payload:
            format!("d2:mid e2:mid2:mie", ).into_bytes() }).await.ok(); // placeholder; head supplied via LiveConfig
        sess.send(&PeerMessage::Unchoke).await.unwrap();
        // Serve every chunk request from the contiguous content.
        loop {
            match sess.read_message().await {
                Ok(PeerMessage::Unknown { id: 6, payload }) => {
                    let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                    let chunk = u16::from_be_bytes(payload[8..10].try_into().unwrap());
                    let off = ((piece - start_piece) as usize) * 8 + (chunk as usize) * 4;
                    let mut block = vec![0u8; 8];               // 8-byte piece header
                    block.extend_from_slice(&chunk.to_be_bytes());
                    block.extend_from_slice(&content_peer[off..off + 4]);
                    sess.send(&PeerMessage::Piece { index: 0, begin: piece, block }).await.unwrap();
                }
                _ => break,
            }
        }
    });

    let mut session = PeerSession::new(client);
    session.perform_handshake(infohash, ace_wire::handshake::random_peer_id()).await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(64);
    let cfg = LiveConfig { start_piece: start_piece as u64, head: (start_piece + pieces - 1) as u64,
                           identity: ace_wire::identity::Identity::generate(), peer_ip: None };
    tokio::spawn(async move { let _ = LiveSession::run(session, info, cfg, tx).await; });

    let mut got = Vec::new();
    while got.len() < content.len() {
        match rx.recv().await { Some(b) => got.extend_from_slice(&b), None => break }
    }
    assert_eq!(got, content);
}
```

- [ ] **Step 2: Run it, verify it fails** — `cargo test -p ace-swarm --test live_session 2>&1 | tail -5`
Expected: compile error (`ace_swarm::live` missing).

- [ ] **Step 3: Implement `LiveSession`** — create `crates/ace-swarm/src/live.rs`:

```rust
//! Continuous single-peer live download: request chunks for consecutive pieces, reassemble
//! contiguous MPEG-TS, push to an mpsc channel. Multi-peer scheduling layers on later.
use crate::types::StreamInfo;
use ace_peer::session::PeerSession;
use ace_peer::Result;
use ace_wire::identity::Identity;
use ace_wire::live_codec::{chunk_request, LiveChunk};
use ace_wire::message::PeerMessage;
use ace_wire::reassembly::PieceReassembler;
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

/// Parameters for a live download (identity, where to start, how far).
pub struct LiveConfig {
    pub start_piece: u64,
    /// Last piece to fetch (inclusive). For unbounded live use a large value / update later.
    pub head: u64,
    pub identity: Identity,
    /// Our view of the peer's IP for `yourip` (None when unknown, e.g. duplex tests).
    pub peer_ip: Option<[u8; 4]>,
}

pub struct LiveSession;

impl LiveSession {
    /// Drive the download on an already-BT-handshaken `session`. Sends our signed extended
    /// handshake, waits for unchoke, requests every chunk of pieces [start, head], and pushes
    /// reassembled contiguous TS to `out`. Returns when `head` is fully emitted or peer closes.
    pub async fn run<S: AsyncRead + AsyncWrite + Unpin>(
        mut session: PeerSession<S>,
        info: StreamInfo,
        cfg: LiveConfig,
        out: mpsc::Sender<Bytes>,
    ) -> Result<()> {
        use ace_wire::extended::{LivePosition, NodeFields, OutgoingExtendedHandshake};
        let hs = OutgoingExtendedHandshake {
            ace_metadata_version: 1,
            ut_metadata_id: 2,
            mi: Some(LivePosition {
                min_piece: cfg.start_piece as i64,
                max_piece: cfg.head as i64,
                position: -1,
                distance_from_source: 1,
            }),
            node: NodeFields { ts: 5000, ..NodeFields::default() },
            peer_ip: cfg.peer_ip,
        };
        session.send_signed_extended_handshake(&hs, &cfg.identity).await?;

        let chunks_per_piece = info.chunks_per_piece();
        let mut reasm = PieceReassembler::new(info.piece_length, cfg.start_piece);
        let mut unchoked = false;
        let mut requested_through: Option<u64> = None;

        loop {
            if unchoked {
                // Request all chunks for pieces we haven't requested yet, up to head.
                let from = requested_through.map(|p| p + 1).unwrap_or(cfg.start_piece);
                for piece in from..=cfg.head {
                    for chunk in 0..chunks_per_piece {
                        session.send(&chunk_request(piece as u32, chunk)).await?;
                    }
                }
                requested_through = Some(cfg.head);
            }
            match session.read_message().await {
                Ok(PeerMessage::Unchoke) => unchoked = true,
                Ok(PeerMessage::Choke) => unchoked = false,
                Ok(msg @ PeerMessage::Piece { .. }) => {
                    if let Some(lc) = LiveChunk::from_message(&msg) {
                        let begin = lc.chunk as u64 * info.chunk_length;
                        reasm.add_block(lc.piece as u64, begin, &lc.data)?;
                        let ready = reasm.take_ready();
                        if !ready.is_empty() && out.send(Bytes::from(ready)).await.is_err() {
                            return Ok(()); // all receivers dropped
                        }
                        if reasm.next_needed() > cfg.head { return Ok(()); }
                    }
                }
                Ok(_) => {}
                Err(ace_peer::PeerError::Closed) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }
}
```

- [ ] **Step 4: Add deps + module** — in `crates/ace-swarm/Cargo.toml` add to `[dependencies]`: `bytes = "1"`. Add `pub mod live;` to `crates/ace-swarm/src/lib.rs`. (Remove the placeholder extended-handshake line in the test's mock — the head is supplied via `LiveConfig`, so delete the `sess.send(&PeerMessage::Extended ...)` placeholder line from Step 1's test.)

- [ ] **Step 5: Run** — `cargo test -p ace-swarm --test live_session 2>&1 | tail -5`
Expected: PASS.

- [ ] **Step 6: Commit** — `git add -A && git commit -m "ace-swarm: LiveSession single-peer continuous TS download"`

### Task 5: Delete the inline recon protocol from `ace-peer`

The full handshake + request/piece logic now lives in the library; keep `live_recon_unchoke` only as a thin live smoke-test using the promoted APIs, or delete it.

**Files:** Modify: `crates/ace-peer/src/session.rs`

- [ ] **Step 1:** Replace the body of the `live_recon_unchoke` test so it builds the handshake via `OutgoingExtendedHandshake { .., peer_ip: .. }.sign_and_encode` and requests via `ace_wire::live_codec::chunk_request` (delete the inline `BTreeMap`/`f.insert`/manual `midict` block). Keep it `#[ignore]`.
- [ ] **Step 2: Run** — `cargo test -p ace-peer 2>&1 | tail -3` → PASS (ignored test still compiles).
- [ ] **Step 3: Commit** — `git add -A && git commit -m "ace-peer: use promoted handshake/codec APIs in recon; drop inline protocol"`

---

## Phase 2 — Provider abstraction + in-memory test provider

### Task 6: `StreamProvider` / `TsSource` traits + `ProviderRegistry`

**Files:**
- Create: `crates/ace-engine/src/provider.rs`
- Modify: `crates/ace-engine/src/lib.rs` (`pub mod provider;`), `crates/ace-engine/Cargo.toml` (deps: `tokio = { version="1", features=["full"] }`, `bytes="1"`, `async-trait="0.1"`)
- Test: in `provider.rs`

- [ ] **Step 1: Write the failing test** — create `crates/ace-engine/src/provider.rs`:

```rust
//! Provider abstraction: the single seam between the generic engine and a network's
//! protocol. `{network}` in the URL selects a `StreamProvider` via `ProviderRegistry`.
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;

/// Stats snapshot for `/status` (clean field names only).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourceStats {
    pub peers: u32,
    pub bitrate: u64,   // bits/sec
    pub buffer_ms: u64, // buffered duration estimate
}

/// A live MPEG-TS byte source for one stream.
#[async_trait]
pub trait TsSource: Send {
    /// Next contiguous MPEG-TS chunk, or None at end-of-stream.
    async fn next(&mut self) -> Option<Bytes>;
    fn stats(&self) -> SourceStats;
}

/// Adapter for one network (e.g. "ace").
#[async_trait]
pub trait StreamProvider: Send + Sync {
    fn network(&self) -> &'static str;
    /// Open a live TS source for `id` (provider resolves/discovers internally).
    async fn open(&self, id: &str) -> Result<Box<dyn TsSource>, ProviderError>;
}

#[derive(Debug)]
pub enum ProviderError {
    NotFound,
    Backend(String),
}

/// Maps network name → provider.
#[derive(Default, Clone)]
pub struct ProviderRegistry {
    providers: HashMap<&'static str, Arc<dyn StreamProvider>>,
}

impl ProviderRegistry {
    pub fn new() -> Self { Self::default() }
    pub fn register(&mut self, p: Arc<dyn StreamProvider>) { self.providers.insert(p.network(), p); }
    pub fn get(&self, network: &str) -> Option<Arc<dyn StreamProvider>> { self.providers.get(network).cloned() }
    pub fn networks(&self) -> Vec<&'static str> { let mut v: Vec<_> = self.providers.keys().copied().collect(); v.sort(); v }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyProvider;
    #[async_trait]
    impl StreamProvider for DummyProvider {
        fn network(&self) -> &'static str { "dummy" }
        async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> { Err(ProviderError::NotFound) }
    }

    #[test]
    fn registry_registers_and_looks_up() {
        let mut r = ProviderRegistry::new();
        r.register(Arc::new(DummyProvider));
        assert!(r.get("dummy").is_some());
        assert!(r.get("nope").is_none());
        assert_eq!(r.networks(), vec!["dummy"]);
    }
}
```

- [ ] **Step 2: Wire deps + module** — update `crates/ace-engine/Cargo.toml` `[dependencies]` (add the three deps above and keep `ace-wire`, `ace-media`); add `pub mod provider;` to `crates/ace-engine/src/lib.rs`.
- [ ] **Step 3: Run** — `cargo test -p ace-engine --lib provider 2>&1 | tail -3` → PASS.
- [ ] **Step 4: Commit** — `git add -A && git commit -m "ace-engine: StreamProvider/TsSource traits + ProviderRegistry"`

### Task 7: In-memory test provider

**Files:** Create: `crates/ace-engine/src/testprovider.rs`; Modify: `crates/ace-engine/src/lib.rs` (`#[cfg(any(test, feature = "testprovider"))] pub mod testprovider;` — simplest: plain `pub mod testprovider;`).

- [ ] **Step 1: Write the test + provider** — create `crates/ace-engine/src/testprovider.rs`:

```rust
//! A network-free `StreamProvider` for exercising the engine in tests: serves a fixed
//! sequence of TS-looking chunks for any id.
use crate::provider::{ProviderError, SourceStats, StreamProvider, TsSource};
use async_trait::async_trait;
use bytes::Bytes;

pub struct TestProvider { pub chunks: usize }

struct TestSource { remaining: usize, idx: usize }

#[async_trait]
impl TsSource for TestSource {
    async fn next(&mut self) -> Option<Bytes> {
        if self.remaining == 0 { return None; }
        self.remaining -= 1;
        let mut b = vec![0u8; 188];
        b[0] = 0x47; b[1] = self.idx as u8; self.idx += 1;
        Some(Bytes::from(b))
    }
    fn stats(&self) -> SourceStats { SourceStats { peers: 1, bitrate: 0, buffer_ms: 0 } }
}

#[async_trait]
impl StreamProvider for TestProvider {
    fn network(&self) -> &'static str { "test" }
    async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        Ok(Box::new(TestSource { remaining: self.chunks, idx: 0 }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::StreamProvider;
    #[tokio::test]
    async fn test_source_yields_n_ts_packets_then_ends() {
        let p = TestProvider { chunks: 3 };
        let mut s = p.open("anything").await.unwrap();
        let mut n = 0;
        while let Some(b) = s.next().await { assert_eq!(b[0], 0x47); n += 1; }
        assert_eq!(n, 3);
    }
}
```

- [ ] **Step 2: Module** — add `pub mod testprovider;` to `crates/ace-engine/src/lib.rs`.
- [ ] **Step 3: Run** — `cargo test -p ace-engine --lib testprovider 2>&1 | tail -3` → PASS.
- [ ] **Step 4: Commit** — `git add -A && git commit -m "ace-engine: in-memory test provider"`

---

## Phase 3 — Streaming engine (fan-out + lifecycle), provider-agnostic

### Task 8: `StreamSession` with broadcast fan-out

**Files:** Create: `crates/ace-engine/src/session.rs`; Modify: `crates/ace-engine/src/lib.rs`.

- [ ] **Step 1: Write the failing test** — create `crates/ace-engine/src/session.rs`:

```rust
//! One shared download per stream, fanned out to many subscribers via a broadcast channel.
use crate::provider::{SourceStats, StreamProvider, TsSource};
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};

/// A live session: pulls from one TsSource and broadcasts TS chunks to subscribers.
pub struct StreamSession {
    tx: broadcast::Sender<Bytes>,
    subscribers: Arc<AtomicU64>,
    stats: Arc<Mutex<SourceStats>>,
}

/// A subscription that decrements the live subscriber count on drop.
pub struct Subscription {
    pub rx: broadcast::Receiver<Bytes>,
    count: Arc<AtomicU64>,
}
impl Drop for Subscription {
    fn drop(&mut self) { self.count.fetch_sub(1, Ordering::SeqCst); }
}

impl StreamSession {
    /// Start pulling from `source` in a background task, broadcasting chunks. The pump stops
    /// when the source ends. `buffer` is the broadcast backlog (chunks a late joiner can lag).
    pub fn start(mut source: Box<dyn TsSource>, buffer: usize) -> Arc<StreamSession> {
        let (tx, _rx) = broadcast::channel(buffer);
        let stats = Arc::new(Mutex::new(SourceStats::default()));
        let session = Arc::new(StreamSession { tx: tx.clone(), subscribers: Arc::new(AtomicU64::new(0)), stats: stats.clone() });
        tokio::spawn(async move {
            while let Some(chunk) = source.next().await {
                { *stats.lock().await = source.stats(); }
                if tx.send(chunk).is_err() { /* no live receivers right now; keep pulling */ }
            }
        });
        session
    }

    pub fn subscribe(&self) -> Subscription {
        self.subscribers.fetch_add(1, Ordering::SeqCst);
        Subscription { rx: self.tx.subscribe(), count: self.subscribers.clone() }
    }
    pub fn subscriber_count(&self) -> u64 { self.subscribers.load(Ordering::SeqCst) }
    pub async fn stats(&self) -> SourceStats { self.stats.lock().await.clone() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testprovider::TestProvider;
    use crate::provider::StreamProvider;

    #[tokio::test]
    async fn two_subscribers_share_one_source() {
        let source = TestProvider { chunks: 5 }.open("x").await.unwrap();
        let session = StreamSession::start(source, 16);
        let mut a = session.subscribe();
        let mut b = session.subscribe();
        assert_eq!(session.subscriber_count(), 2);
        // both receive the same first chunk
        let ca = a.rx.recv().await.unwrap();
        let cb = b.rx.recv().await.unwrap();
        assert_eq!(ca, cb);
        assert_eq!(ca[0], 0x47);
        drop(a);
        assert_eq!(session.subscriber_count(), 1);
    }
}
```

- [ ] **Step 2: Module** — `pub mod session;` in `crates/ace-engine/src/lib.rs`.
- [ ] **Step 3: Run** — `cargo test -p ace-engine --lib session 2>&1 | tail -3` → PASS.
- [ ] **Step 4: Commit** — `git add -A && git commit -m "ace-engine: StreamSession broadcast fan-out + subscriber counting"`

### Task 9: `StreamManager` (get-or-start, keyed by (network,id), idle teardown)

**Files:** Create: `crates/ace-engine/src/manager.rs`; Modify: `crates/ace-engine/src/lib.rs`.

- [ ] **Step 1: Write the failing test** — create `crates/ace-engine/src/manager.rs`:

```rust
//! Registry of live sessions keyed by (network, id). One shared session per key; lazy
//! start; teardown after the last subscriber leaves + grace.
use crate::provider::{ProviderError, ProviderRegistry};
use crate::session::StreamSession;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct StreamManager {
    registry: ProviderRegistry,
    sessions: Mutex<HashMap<(String, String), Arc<StreamSession>>>,
    buffer: usize,
    grace: Duration,
}

impl StreamManager {
    pub fn new(registry: ProviderRegistry) -> Arc<StreamManager> {
        Arc::new(StreamManager { registry, sessions: Mutex::new(HashMap::new()), buffer: 256, grace: Duration::from_secs(30) })
    }

    /// Get the running session for (network,id) or start one via the provider. Returns
    /// NotFound if the network is unregistered.
    pub async fn get_or_start(self: &Arc<Self>, network: &str, id: &str) -> Result<Arc<StreamSession>, ProviderError> {
        let key = (network.to_string(), id.to_string());
        {
            let map = self.sessions.lock().await;
            if let Some(s) = map.get(&key) { return Ok(s.clone()); }
        }
        let provider = self.registry.get(network).ok_or(ProviderError::NotFound)?;
        let source = provider.open(id).await?;
        let session = StreamSession::start(source, self.buffer);
        let mut map = self.sessions.lock().await;
        // Double-check: another task may have started it concurrently.
        Ok(map.entry(key).or_insert(session).clone())
    }

    /// Spawn the idle-teardown watcher: drops sessions with 0 subscribers after `grace`.
    pub fn spawn_reaper(self: &Arc<Self>) {
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(me.grace).await;
                let mut map = me.sessions.lock().await;
                map.retain(|_, s| s.subscriber_count() > 0);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::StreamProvider;
    use crate::testprovider::TestProvider;
    use std::sync::Arc;

    fn registry() -> ProviderRegistry {
        let mut r = ProviderRegistry::new();
        r.register(Arc::new(TestProvider { chunks: 1000 }));
        r
    }

    #[tokio::test]
    async fn same_key_returns_same_session() {
        let m = StreamManager::new(registry());
        let s1 = m.get_or_start("test", "abc").await.unwrap();
        let s2 = m.get_or_start("test", "abc").await.unwrap();
        assert!(Arc::ptr_eq(&s1, &s2));
        let s3 = m.get_or_start("test", "different").await.unwrap();
        assert!(!Arc::ptr_eq(&s1, &s3));
    }

    #[tokio::test]
    async fn unknown_network_is_not_found() {
        let m = StreamManager::new(registry());
        assert!(matches!(m.get_or_start("nope", "x").await, Err(ProviderError::NotFound)));
    }
}
```

- [ ] **Step 2: Module** — `pub mod manager;` in `crates/ace-engine/src/lib.rs`.
- [ ] **Step 3: Run** — `cargo test -p ace-engine --lib manager 2>&1 | tail -3` → PASS.
- [ ] **Step 4: Commit** — `git add -A && git commit -m "ace-engine: StreamManager get-or-start + idle reaper"`

### Task 10: Multi-client integration test (acexy parity)

**Files:** Create: `crates/ace-engine/tests/multiclient.rs`.

- [ ] **Step 1: Write the test** — drive several subscribers off one manager-started session and assert one source feeds all; a dropped subscriber doesn't disturb others.

```rust
use ace_engine::manager::StreamManager;
use ace_engine::provider::{ProviderRegistry, StreamProvider};
use ace_engine::testprovider::TestProvider;
use std::sync::Arc;

#[tokio::test]
async fn n_clients_one_source_independent_lifecycles() {
    let mut r = ProviderRegistry::new();
    r.register(Arc::new(TestProvider { chunks: 100 }));
    let m = StreamManager::new(r);
    let s = m.get_or_start("test", "chan").await.unwrap();

    let mut subs: Vec<_> = (0..4).map(|_| s.subscribe()).collect();
    assert_eq!(s.subscriber_count(), 4);
    // all receive the same first chunk
    let first = subs[0].rx.recv().await.unwrap();
    for sub in subs.iter_mut().skip(1) {
        assert_eq!(sub.rx.recv().await.unwrap(), first);
    }
    // one client leaves; others keep receiving
    subs.pop();
    assert_eq!(s.subscriber_count(), 3);
    let _ = subs[0].rx.recv().await.unwrap();

    // distinct stream gets a distinct session
    let s2 = m.get_or_start("test", "other").await.unwrap();
    assert!(!Arc::ptr_eq(&s, &s2));
}
```

- [ ] **Step 2: Run** — `cargo test -p ace-engine --test multiclient 2>&1 | tail -3` → PASS.
- [ ] **Step 3: Commit** — `git add -A && git commit -m "ace-engine: multi-client (acexy-parity) integration test"`

---

## Phase 4 — Clean HTTP API (axum)

### Task 11: HTTP server skeleton + `/healthz` + `/networks`

**Files:** Create: `crates/ace-engine/src/http.rs`; Modify: `crates/ace-engine/src/lib.rs`, `crates/ace-engine/src/main.rs`, `crates/ace-engine/Cargo.toml` (add `axum = "0.7"`, `tower = "0.4"`).

- [ ] **Step 1: Write the failing test** — create `crates/ace-engine/src/http.rs` with a `router(manager, networks)` builder and a test using `tower::ServiceExt::oneshot`:

```rust
//! Clean HTTP API (axum). No ace/acestream tokens in paths/JSON except the {network} value.
use crate::manager::StreamManager;
use axum::{routing::get, Router, Json};
use serde_json::json;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub manager: Arc<StreamManager>,
    pub networks: Vec<String>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/networks", get(networks))
        .with_state(state)
}

async fn networks(axum::extract::State(s): axum::extract::State<AppState>) -> Json<serde_json::Value> {
    Json(json!({ "networks": s.networks }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{ProviderRegistry, StreamProvider};
    use crate::testprovider::TestProvider;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn state() -> AppState {
        let mut r = ProviderRegistry::new();
        r.register(std::sync::Arc::new(TestProvider { chunks: 10 }));
        AppState { manager: StreamManager::new(r), networks: vec!["test".into()] }
    }

    #[tokio::test]
    async fn healthz_ok() {
        let resp = router(state()).oneshot(Request::get("/healthz").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn networks_lists_registered() {
        let resp = router(state()).oneshot(Request::get("/networks").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("\"test\""));
    }
}
```

- [ ] **Step 2: Deps + modules** — add `axum = "0.7"`, `tower = "0.4"`, `serde_json = "1"`, `serde = { version="1", features=["derive"] }` to `crates/ace-engine/Cargo.toml`; add `pub mod http;` to `lib.rs`.
- [ ] **Step 3: Run** — `cargo test -p ace-engine --lib http 2>&1 | tail -4` → PASS.
- [ ] **Step 4: Commit** — `git add -A && git commit -m "ace-engine: axum HTTP skeleton (/healthz, /networks)"`

### Task 12: `GET /streams/{network}/{id}.ts` — continuous MPEG-TS

**Files:** Modify: `crates/ace-engine/src/http.rs`.

- [ ] **Step 1: Write the failing test** — add to `http.rs` tests: request `/streams/test/abc.ts`, assert 200 and that the body stream starts with a TS sync byte.

```rust
    #[tokio::test]
    async fn streams_ts_returns_mpegts_body() {
        let resp = router(state()).oneshot(
            Request::get("/streams/test/abc.ts").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()["content-type"], "video/mp2t");
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        assert_eq!(body[0], 0x47); // first TS packet
    }

    #[tokio::test]
    async fn unknown_network_404() {
        let resp = router(state()).oneshot(
            Request::get("/streams/nope/abc.ts").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
```

- [ ] **Step 2: Implement the route + handler** — add to `router()`:
  `.route("/streams/:network/:file", get(stream_ts))`
and the handler (splits the `.ts` extension; subscribes; streams the broadcast as the body):

```rust
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::body::Body;
use futures::StreamExt;

async fn stream_ts(State(s): State<AppState>, Path((network, file)): Path<(String, String)>) -> Response {
    let id = match file.strip_suffix(".ts") {
        Some(id) => id.to_string(),
        None => return StatusCode::NOT_FOUND.into_response(),
    };
    let session = match s.manager.get_or_start(&network, &id).await {
        Ok(sess) => sess,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    let sub = session.subscribe();
    // Bridge broadcast receiver -> HTTP body stream; keep `sub` alive for its lifecycle drop.
    let stream = futures::stream::unfold(sub, |mut sub| async move {
        loop {
            match sub.rx.recv().await {
                Ok(chunk) => return Some((Ok::<_, std::io::Error>(chunk), sub)),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
            }
        }
    });
    Response::builder()
        .header(header::CONTENT_TYPE, "video/mp2t")
        .body(Body::from_stream(stream))
        .unwrap()
}
```

- [ ] **Step 3: Deps** — add `futures = "0.3"` to `crates/ace-engine/Cargo.toml`.
- [ ] **Step 4: Run** — `cargo test -p ace-engine --lib http 2>&1 | tail -5` → PASS.
- [ ] **Step 5: Commit** — `git add -A && git commit -m "ace-engine: GET /streams/{network}/{id}.ts continuous MPEG-TS"`

### Task 13: `/streams/{network}/{id}/status`, `GET /streams`

**Files:** Modify: `crates/ace-engine/src/http.rs`, `crates/ace-engine/src/manager.rs` (add `list()` returning active keys + counts).

- [ ] **Step 1:** Add `StreamManager::list(&self) -> Vec<(String,String,u64)>` (network, id, subscriber_count) with a unit test in `manager.rs`:

```rust
    pub async fn list(&self) -> Vec<(String, String, u64)> {
        self.sessions.lock().await.iter()
            .map(|((n, i), s)| (n.clone(), i.clone(), s.subscriber_count())).collect()
    }
```
Test: after `get_or_start("test","a")`, `list()` contains `("test","a",_)`.

- [ ] **Step 2:** Add routes `/streams/:network/:id/status` and `/streams` to `router()`, returning clean JSON (`{ network, id, clients, peers, bitrate, buffer }` and a list). Handler test: `GET /streams/test/abc/status` after starting returns 200 JSON containing `"network":"test"` and `"clients"`.

```rust
async fn stream_status(State(s): State<AppState>, Path((network, id)): Path<(String,String)>) -> Response {
    match s.manager.get_or_start(&network, &id).await {
        Ok(sess) => {
            let st = sess.stats().await;
            Json(json!({ "network": network, "id": id, "clients": sess.subscriber_count(),
                         "peers": st.peers, "bitrate": st.bitrate, "buffer": st.buffer_ms })).into_response()
        }
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}
async fn list_streams(State(s): State<AppState>) -> Json<serde_json::Value> {
    let items: Vec<_> = s.manager.list().await.into_iter()
        .map(|(n,i,c)| json!({ "network": n, "id": i, "clients": c })).collect();
    Json(json!({ "streams": items }))
}
```

- [ ] **Step 3: Run** — `cargo test -p ace-engine 2>&1 | grep 'test result'` → all PASS.
- [ ] **Step 4: Commit** — `git add -A && git commit -m "ace-engine: /status and /streams listing endpoints"`

### Task 14: HLS endpoints `.m3u8` + `seg/{n}.ts`

**Files:** Modify: `crates/ace-engine/src/session.rs` (retain a rolling window of recent TS for segmenting), `crates/ace-engine/src/http.rs`.

- [ ] **Step 1:** Add to `StreamSession` a shared rolling buffer of the last N seconds of TS (a `Mutex<VecDeque<Bytes>>` appended by the pump) plus a monotonically increasing media sequence. Unit test: after pumping, `recent_ts()` returns concatenated TS that `ace_media::mpegts::find_sync_offset` accepts.

- [ ] **Step 2:** Add routes `/streams/:network/:file` already matches `.ts`; add `.m3u8` handling in the same handler (branch on suffix) producing `ace_media::hls::media_playlist(...)` with segment URLs `seg/{n}.ts`, and a route `/streams/:network/:id/seg/:seg` returning `ace_media::hls::segment(...)` slices as `video/mp2t`. Tests: `.m3u8` returns 200 `application/vnd.apple.mpegurl` starting `#EXTM3U`; a segment returns 200 TS.

(Implementation detail: segment by `packets_per_segment` using `ace_media::hls::segment` over `recent_ts()`; map `{n}` to the rolling media-sequence index.)

- [ ] **Step 3: Run** — `cargo test -p ace-engine 2>&1 | grep 'test result'` → PASS.
- [ ] **Step 4: Commit** — `git add -A && git commit -m "ace-engine: HLS .m3u8 + segment endpoints"`

### Task 15: `main.rs` daemon wiring + config + identity persistence

**Files:** Modify: `crates/ace-engine/src/main.rs`; Modify `Cargo.toml` (`dirs = "5"`); Create: `crates/ace-engine/src/config.rs`.

- [ ] **Step 1:** `config.rs`: `Config { listen: SocketAddr (default 127.0.0.1:8000), idle_grace_secs, log_level }` from env vars with defaults; unit test for defaults.
- [ ] **Step 2:** Identity persistence helper in `config.rs`: `load_or_create_identity(dir) -> Identity` writing/reading 32 random seed bytes at `dir/identity.key`; unit test (temp dir → two loads yield same node_id).
- [ ] **Step 3:** `main.rs`: build `ProviderRegistry` (register `AceProvider::new(identity)` — added in Phase 5; until then register `TestProvider` behind a `--demo` flag), construct `StreamManager`, `spawn_reaper()`, serve `http::router` via `axum::serve` on the configured address. Print the listen URL.
- [ ] **Step 4: Run** — `cargo run -p ace-engine &` then `curl -s localhost:8000/healthz` → `ok`; `curl -s localhost:8000/networks`. Kill it.
- [ ] **Step 5: Commit** — `git add -A && git commit -m "ace-engine: daemon main (config, identity persistence, axum serve)"`

---

## Phase 5 — `AceProvider` (wire the real swarm) + content-id resolution + muxing + VLC

### Task 16: Tracker discovery helper in `ace-swarm`

**Files:** Create: `crates/ace-swarm/src/discover.rs`; Modify: `Cargo.toml` (dep `ace-tracker`), `lib.rs`.

- [ ] **Step 1:** `discover_peers(trackers: &[String], infohash: &[u8;20]) -> Vec<SocketAddrV4>`: for each `udp://host:port` tracker, resolve + call `ace_tracker::client::announce(addr, infohash, peer_id, port, num_want, TransferState::default())`, collect/dedup peers. Default tracker `t1.torrentstream.org:2710` if none. Test: against a local fake UDP tracker (mirror `ace-tracker`'s existing `announce_against_local_fake_tracker` test harness) returns the injected peer.
- [ ] **Step 2: Run** — `cargo test -p ace-swarm --lib discover 2>&1 | tail -3` → PASS.
- [ ] **Step 3: Commit** — `git add -A && git commit -m "ace-swarm: tracker peer discovery helper"`

### Task 17: `AceProvider` (`ace`) in `ace-engine`

**Files:** Create: `crates/ace-engine/src/ace_provider.rs`; Modify: `Cargo.toml` (deps `ace-swarm`, `ace-tracker`, `ace-peer`), `lib.rs`, `main.rs` (register it).

- [ ] **Step 1:** Implement `StreamProvider for AceProvider` (`network()=="ace"`). `open(id)`:
  1. `let info = resolve(id, &self.identity).await?;` (Task 18; for now, if `id` is a 40-hex infohash, build `StreamInfo` with known geometry `piece_length=1_048_576, chunk_length=16_384` and default trackers — a stopgap until resolution lands).
  2. `let peers = ace_swarm::discover::discover_peers(&info.trackers, &info.infohash).await;`
  3. Connect to the first responsive peer (`ace_peer::session::connect`), `perform_handshake`, then spawn `LiveSession::run(session, info, cfg, tx)`.
  4. Return a `TsSource` wrapping the `mpsc::Receiver<Bytes>` (its `next()` awaits `rx.recv()`; `stats()` reads a shared counter).
- [ ] **Step 2:** Unit-test the `TsSource` adapter over an `mpsc` channel (no network): feed bytes into the sender, assert `next()` yields them. (The full live path is covered by the live E2E in Task 20.)
- [ ] **Step 3: Run** — `cargo test -p ace-engine --lib ace_provider 2>&1 | tail -3` → PASS.
- [ ] **Step 4: Commit** — `git add -A && git commit -m "ace-engine: AceProvider wiring discovery->LiveSession->TsSource"`

### Task 18: Network content-id resolution (ut_metadata + transport decode) — ✅ DONE

**Implemented:** pure BEP-9 codec `ace_wire::ut_metadata` (`request_piece`/`MetadataMessage`),
`PeerSession::fetch_metadata` (mock-peer tested), `ace_swarm::resolve::resolve_via_peer` +
`ResolveCache` (TTL), and an offline end-to-end mock-peer integration test
(`crates/ace-swarm/tests/resolve_metadata.rs`: handshake → extended handshake → ut_metadata →
transport decode → `StreamInfo` with the real infohash). Wired into `AceProvider::open` behind
a `cid:<40hex>` id prefix (bare `<40hex>` stays infohash-direct). The live discovery half shares
the same environment gate as the download path.

**Files:** Create: `crates/ace-swarm/src/resolve.rs`; Modify: `lib.rs`. Reuses `ace-wire`'s transport decoder.

- [ ] **Step 1:** Confirm/implement `ut_metadata` (BEP-9) fetch on `PeerSession`: send the extended handshake advertising `ut_metadata`, then request metadata pieces (`{ msg_type:0, piece:n }` bencoded under the peer's `ut_metadata` ext id), concatenate `msg_type:1` data pieces into the raw transport bytes. Add a `fetch_metadata(&mut session, peer_ut_id) -> Vec<u8>` in `ace-peer` with a mock-peer test that serves a 2-piece metadata blob.
- [ ] **Step 2:** `resolve(content_id_or_infohash, identity) -> StreamInfo`: discover metadata peers (tracker on the content-id; **if the tracker yields none, log and return a clear error — DHT is the documented follow-up**), fetch metadata, decode with `ace_wire::transport` → `StreamInfo`. Cache results in a `Mutex<HashMap<String, (StreamInfo, Instant)>>` with TTL. Test: a mock peer serving a fixture `AceStreamTransport` blob (build one with the existing transport encoder/test vectors) resolves to the expected `StreamInfo`.
- [ ] **Step 3:** Replace the Task-17 stopgap in `AceProvider::open` with a real `resolve(id)` call.
- [ ] **Step 4: Run** — `cargo test -p ace-swarm --lib resolve 2>&1 | tail -3` and `cargo test -p ace-peer fetch_metadata 2>&1 | tail -3` → PASS.
- [ ] **Step 5: Commit** — `git add -A && git commit -m "content-id resolution via ut_metadata + transport decode (no Acestream API)"`

### Task 19: Gapless cross-piece muxing — ✅ DONE

**Implemented** as `ace_media::mpegts::TsResync` (the −4 B/piece drift is a partial packet of
junk per piece boundary; the resync filter drops it and re-locks). Plus per-client
start-on-keyframe (`KeyframeGate`) so players begin on a decodable picture. Daemon output
decodes to 1280×720 50fps H.264.

**Files:** Modify: `crates/ace-wire/src/reassembly.rs` (or a new `ace-media` pass); add a fixture-based test using captured pieces under `tests/vectors/` if available.

- [ ] **Step 1:** Investigate the −4 B/piece drift (note 19) against captured pieces: determine whether each piece carries a small per-piece header/trailer that must be stripped for byte-clean continuity (hypothesis: ~96 bytes/piece). Write a test that concatenates two adjacent captured pieces and asserts a single 0x47 alignment holds across the boundary after the correct strip.
- [ ] **Step 2:** Implement the fix in reassembly (strip the identified per-piece bytes before emitting). If no clean structure is found, document that and keep demuxer-resync (already tolerated) — the test then asserts ffmpeg-style resync rather than byte-perfect continuity.
- [ ] **Step 3: Run** — `cargo test -p ace-wire reassembly 2>&1 | tail -3` → PASS.
- [ ] **Step 4: Commit** — `git add -A && git commit -m "gapless cross-piece TS continuity (resolve -4B/piece drift)"`

### Task 20: Live end-to-end verification (the deliverable) — ✅ DONE (2026-06-30)

**Verified live** against the real swarm (WARP off) — see `docs/protocol/notes/20-vlc-playback.md`:
the daemon autonomously discovered 25 peers for content-id cid1, connected + handshaked,
got UNCHOKE, and downloaded **8.3 MB of live MPEG-TS**; ffprobe reports **1280×720 H.264 + 48 kHz
AAC**; ffmpeg decoded a **720p frame** from the served output (Step 3); the served stream starts
on a **keyframe** (`KeyframeGate`); and **two concurrent clients shared one session**
(`GET /streams` → `clients:2`, Step 4). The literal VLC-GUI watch (Step 4's "open two VLC
windows") is the only human-visual action; its shared-download behavior is what the two-client
check proves. Content-id `cid:` finding: peers accept the content-id as the swarm key and
advertise `ut_metadata` but send no `metadata_size` (the bare-id path drives playback) — a
characterized live-RE follow-up that does not block playback.

**Files:** none (manual/scripted verification); results recorded in `docs/protocol/notes/20-vlc-playback.md`.

- [ ] **Step 1:** Start the daemon: `cargo run -p ace-engine`. Ensure WARP is off; the sandbox engine need not run.
- [ ] **Step 2:** Resolve a known-live id (e.g. `acestream://cid2`). Confirm `curl -s localhost:8000/streams/ace/cid2/status` shows `peers>0` after a few seconds.
- [ ] **Step 3:** Pull the TS and decode a frame to prove real video end-to-end through the daemon:

```bash
curl -s --max-time 30 http://localhost:8000/streams/ace/cid2.ts \
  | ffmpeg -hide_banner -err_detect ignore_err -fflags +discardcorrupt -i - -frames:v 1 -update 1 -y shot.jpg
ffprobe -v error -show_entries stream=codec_name,width,height shot.jpg
```
Expected: a 1920×1080 frame written (mjpeg).

- [ ] **Step 4:** Open `http://localhost:8000/streams/ace/<id>.ts` in **VLC** and confirm playback; open a second VLC simultaneously and confirm `GET /streams` shows one session with `clients: 2` (one shared download).
- [ ] **Step 5:** Write `docs/protocol/notes/20-vlc-playback.md` with the outcome; update `README.md` Phase 3 to ✅. Commit.

```bash
git add docs/ && git commit -m "docs: Phase 3 complete — VLC plays from the outpace daemon (multi-client verified)"
```

---

## Self-review notes
- **Spec coverage:** Protocol promotion (T1–5), provider abstraction (T6–7), engine+fan-out+lifecycle (T8–10), clean API incl. {network} (T11–14), daemon/config/identity (T15), AceProvider+discovery (T16–17), content-id resolution (T18), gapless muxing (T19), VLC E2E (T20). Search/aggregate intentionally absent (later spec).
- **Naming:** all HTTP paths/JSON keys are clean; the only `ace` token on the surface is the `{network}` value, registered via `AceProvider::network()`.
- **Risks carried from spec:** content-id metadata peer source (tracker vs DHT) handled in T18 Step 2 with a clear error + DHT-follow-up note; −4 B/piece drift handled in T19 with a documented fallback.
