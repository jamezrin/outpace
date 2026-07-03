# v2 offline foundations — S2 listener plumbing + B1 pure codecs

Branch: `v2-offline-foundations`. Executes the parts of the v2 spec
(`docs/superpowers/specs/2026-06-30-compliance-seeding-broadcasting-design.md`) that are
**buildable and fully testable without the RE/sandbox/live-swarm environment**. The hard-gated
remainder (B0 RSA-signed pieces, S1 Task 7 piece_header ground truth, and all real-peer interop /
broadcast-origination verification) stays deferred — see README.md.

**Compatibility guard:** because the served `piece_header` is still `[0u8;8]` (Task 7 will pin the
engine's real bytes) and the inbound advertisement is unvalidated against engine ground truth, the
inbound seeder is **built + loopback-tested but defaults OFF** (`enable_inbound=false`). Turning it
on in production before Task 7 risks serving non-compliant pieces to the real swarm, violating the
project's transparent-interop constraint. Flip the default to `true` as part of closing Task 7.

Each task: TDD (test first), `cargo clippy` clean, no `cargo fmt` (repo is hand-formatted), commit.

---

## T1 — `encode_transport` (ace-wire) — production transport encoder
**File:** `crates/ace-wire/src/transport.rs`.
The inverse of `decode_transport`: bencode a descriptor dict → PKCS#7 + AES-128-CBC under the
global key/IV → prepend `"AceStreamTransport\x00\x02"`. The test module already has the `Enc`
encryptor used to build vectors; promote that to a production function.

Add (production, not test-only):
```rust
use cbc::cipher::{BlockModeEncrypt, block_padding::Pkcs7};
type Enc = cbc::Encryptor<aes::Aes128>;

/// Encode a transport file from a bencode descriptor dict, the inverse of [`decode_transport`]:
/// PKCS#7 + AES-128-CBC under the global key/IV, prefixed with the magic + version. Round-trips
/// with the decoder. `descriptor` must be a `Bencode::Dict`.
pub fn encode_transport(descriptor: &Bencode) -> Vec<u8> {
    encode_transport_with_key(descriptor, &TRANSPORT_KEY, &TRANSPORT_IV)
}

pub fn encode_transport_with_key(descriptor: &Bencode, key: &[u8; 16], iv: &[u8; 16]) -> Vec<u8> {
    let ct = Enc::new_from_slices(key, iv)
        .expect("16-byte key/iv")
        .encrypt_padded_vec::<Pkcs7>(&descriptor.encode());
    let mut out = b"AceStreamTransport\x00\x02".to_vec();
    out.extend_from_slice(&ct);
    out
}
```
Tests (in the existing `#[cfg(test)] mod tests`, can drop the now-redundant `make_transport` or
keep it delegating to `encode_transport_with_key`):
- `encode_then_decode_roundtrips`: build a `Bencode::Dict` with `name`, `piece_length`(1048576),
  `chunk_length`(16384), `trackers` (a 1-item list), encode, `decode_transport`, assert the
  descriptor fields match.
- `infohash_is_sha1_of_encoded_bytes`: `encode_transport(&d)` then assert
  `crate::infohash::is_transport_file(&bytes)` is true and the bytes are 20 (magic) + a multiple of
  16. (If a SHA1 helper exists in `infohash`, also assert it returns 20 bytes.)
- Keep the existing decode tests green.

## T2 — `TsChunker` (ace-wire) — pure MPEG-TS → pieces/chunks (inverse of `PieceReassembler`)
**File:** new `crates/ace-wire/src/chunker.rs`; add `pub mod chunker;` to `lib.rs`.
Accumulate TS bytes; emit fixed `chunk_length` chunks grouped into `piece_length` pieces with
ascending indices from a start epoch. The defining property: chunk-then-reassemble is the identity.

```rust
/// Splits a contiguous live byte stream into `(piece, chunk, begin, bytes)` units sized to the
/// transport geometry, the inverse of [`crate::reassembly::PieceReassembler`]. Pure: feed bytes,
/// drain ready chunks. The final partial chunk is only emitted on `flush`.
pub struct TsChunker {
    piece_length: u64,
    chunk_length: u64,
    start_piece: u64,
    buf: Vec<u8>,
    abs: u64, // absolute byte offset of buf[0] from the epoch
}

pub struct OutChunk { pub piece: u64, pub chunk: u16, pub begin: u64, pub data: Vec<u8> }

impl TsChunker {
    pub fn new(piece_length: u64, chunk_length: u64, start_piece: u64) -> Self { ... }
    /// Append bytes; return every full `chunk_length` chunk now available.
    pub fn push(&mut self, bytes: &[u8]) -> Vec<OutChunk> { ... }
    /// Emit any remaining buffered bytes as a final (possibly short) chunk.
    pub fn flush(&mut self) -> Option<OutChunk> { ... }
}
```
`piece = start_piece + (abs / piece_length)`, `chunk = (abs % piece_length) / chunk_length`,
`begin = (abs % piece_length)`. Tests:
- `chunks_are_chunk_length_sized_with_ascending_indices`
- `roundtrip_through_reassembler_is_identity`: chunk a synthetic buffer covering ≥2 pieces, feed
  each `OutChunk` into a `PieceReassembler::new(piece_length, start_piece)` via `add_block(piece,
  begin, data)`, assert the concatenated `take_ready()` output equals the original buffer.
- `flush_emits_trailing_partial_chunk`
- `empty_push_emits_nothing`

## T3 — `accept_handshake` (ace-peer) — inbound handshake
**File:** `crates/ace-peer/src/session.rs`.
`perform_handshake` is initiator-order (write ours, read theirs). The inbound side must **read the
peer's handshake first**, decide whether we serve that infohash, then reply ours (or drop).

```rust
/// Inbound handshake: read the peer's 66-byte handshake first, then — if `serves(infohash)` —
/// reply with ours and return the peer's handshake. Used by the seeder listener. Rejects with
/// `InfohashMismatch` if we don't serve the requested infohash.
pub async fn accept_handshake<F>(&mut self, our_peer_id: [u8; 20], serves: F) -> Result<Handshake>
where F: FnOnce(&[u8; 20]) -> bool {
    let mut hs = [0u8; HANDSHAKE_LEN];
    with_timeout(self.timeout, self.stream.read_exact(&mut hs)).await??;
    let peer = Handshake::decode(&hs)?;
    if !serves(&peer.infohash) { return Err(PeerError::InfohashMismatch); }
    let ours = Handshake::new(peer.infohash, our_peer_id);
    with_timeout(self.timeout, self.stream.write_all(&ours.encode())).await??;
    Ok(peer)
}
```
Test over `tokio::io::duplex`: one side drives `perform_handshake(ih, ...)`, the other
`accept_handshake(our_id, |h| *h == ih)`; both succeed and agree on infohash. A second test:
`accept_handshake` with a `serves` predicate returning false yields `InfohashMismatch` (and does
not write a reply — assert the initiator's subsequent read times out or sees EOF).

## T4 — seeder announce event (ace-tracker + ace-swarm) — announce as a seeder
**Files:** `crates/ace-tracker/src/codec.rs`, `crates/ace-tracker/src/client.rs`,
`crates/ace-swarm/src/discover.rs`.
`build_announce_request` hardcodes `EVENT_STARTED`. Parameterize it so we can announce
`completed` (seeder) with `left=0`.
- In codec: add `pub enum AnnounceEvent { None=0, Completed=1, Started=2, Stopped=3 }` (values per
  BEP-15) and take an `event: AnnounceEvent` param in `build_announce_request` (replace the
  hardcoded `EVENT_STARTED`). Update existing callers/tests.
- In client: add `event: AnnounceEvent` param to `announce` (thread it to the codec). Keep the
  existing leecher call working (pass `Started`).
- In discover: add `pub async fn announce_seeder(trackers, infohash, peer_id, port)` that announces
  with `TransferState { downloaded: ..., left: 0, uploaded: ... }` and `AnnounceEvent::Completed`
  (best-effort, mirrors `discover_peers`' error handling). Return the peers found (a seeder still
  learns peers).
Tests: codec byte test asserting the event u32 at offset [80..84] changes with the enum; a
`announce_seeder` smoke test against a local non-responding tracker (returns empty, no panic) in
the style of the existing discover test.

## T5 — config additions (ace-engine) — peer/seed knobs
**Files:** `crates/ace-engine/src/config.rs`, `crates/ace-engine/src/main.rs`.
Add to `Config` (with `#[serde(default)]` already on the struct; provide `Default`):
```rust
pub peer_listen: SocketAddr,      // default 0.0.0.0:8621
pub seed_store_bytes: u64,        // default 128*1024*1024
pub max_unchoked: usize,          // default 8
pub max_inbound_peers: usize,     // default 64
pub enable_seeding: bool,         // default true (reciprocal upload over existing conns; S1)
pub enable_inbound: bool,         // default FALSE until Task 7 pins piece_header (see plan note)
```
Env overrides in main.rs (v1 pattern): `OUTPACE_PEER_LISTEN`, `OUTPACE_SEED_STORE_BYTES`,
`OUTPACE_MAX_UNCHOKED`, `OUTPACE_MAX_INBOUND`, `OUTPACE_ENABLE_INBOUND` (parse
"1"/"true"), `OUTPACE_ENABLE_SEEDING`. Test: `Config::default()` has the documented defaults
(`peer_listen.port()==8621`, `enable_inbound==false`, `enable_seeding==true`).

## T6 — `SeedRegistry` + `PeerListener` (ace-swarm) — inbound seeder plumbing (loopback-tested)
**Files:** new `crates/ace-swarm/src/listen.rs`; `pub mod listen;` in lib.rs.
- `SeedRegistry`: `Arc`-shareable map `infohash([u8;20]) -> Arc<Mutex<PieceStore>>` with
  `register(infohash, store)`, `get(&infohash) -> Option<Arc<Mutex<PieceStore>>>`, `serves(&infohash) -> bool`.
- `PeerListener::serve(listener: TcpListener, registry: SeedRegistry, our_peer_id, piece_header,
  max_inbound: usize)`: accept loop, a `tokio::sync::Semaphore(max_inbound)` bounding concurrent
  peers; per connection spawn: `PeerSession::new(stream).accept_handshake(our_peer_id, |ih|
  registry.serves(ih))`, then look up the store and run `SeederSession::serve(&mut session, store,
  piece_header)`. Errors per-connection are logged, never fatal to the accept loop.
- **Loopback integration test** (`crates/ace-swarm/tests/inbound_seeder.rs`): bind a `TcpListener`
  on `127.0.0.1:0`, register an infohash whose store holds `put_chunk(0,0,[..])`, spawn
  `PeerListener::serve`, then a client `TcpStream::connect` → `perform_handshake(ih, client_id)` →
  send `Interested` + `chunk_request(0,0)` → assert it reads back the served `Piece`. Then abort.

## T7 — wire the registry + listener into the engine (gated)
**Files:** `crates/ace-engine/src/ace_provider.rs`, `crates/ace-engine/src/main.rs` (+ provider/
manager seams as needed).
- Give `AceProvider` an `Arc<SeedRegistry>` (constructor/with_ method). In `follow_one_peer`, instead
  of a private `PieceStore`, obtain/create the shared `Arc<Mutex<PieceStore>>` for `info.infohash`
  from the registry (register on first use); feed it as today. This makes downloaded pieces
  serveable by the listener. Keep S1's serve arms working against the shared store
  (`store.lock()...`). Preserve download behavior; keep all tests green.
- In main.rs: build a `SeedRegistry`, hand it to the provider, and **if `config.enable_inbound`**
  bind `config.peer_listen` and spawn `PeerListener::serve(...)` with `config.max_inbound_peers`
  and `[0u8;8]` piece_header. Default-off, so production behavior is unchanged until Task 7.
- No new live test (the real-peer path is environment-gated); the T6 loopback test plus existing
  suite cover the plumbing. Verify the whole workspace builds + tests + clippy clean.

---

## After all tasks
Final whole-branch review, update README.md (S2 plumbing + B1 pure codecs landed; remaining =
B0 RSA signing, Task 7 ground truth, live/interop verification — all RE/sandbox-gated), then finish
the branch.
