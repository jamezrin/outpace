# outpace — P2P compliance, seeding & broadcasting (v2) — design

**Date:** 2026-06-30
**Status:** DRAFT — awaiting user review
**Builds on:** the completed v1 daemon (`2026-06-29-outpace-daemon-design.md`, all tasks +
live VLC E2E done) and the cracked live protocol (`docs/protocol/notes/17–20`).

## Goal

Make outpace a **full, well-behaved P2P participant** that not only downloads but **uploads
(seeds)**, and then a **broadcaster** that ingests a live video feed over HTTP (e.g. from OBS)
and originates its own Acestream swarm — with the **official Acestream engine/app able to play
that broadcast** (interop), not just our own daemons.

Today the daemon is a pure leecher: it never accepts inbound peer connections, never answers a
peer's `Request`, never advertises `Have`/`Bitfield`, and never serves a `Piece`. The only
`TcpListener` is the HTTP API. This is poor swarm citizenship and makes broadcasting impossible
(a source must be reachable). v2 closes both gaps.

## Scope (v2)

Delivered as four phases in dependency order:

1. **S1 — Compliance & reciprocal upload.** Retain downloaded pieces; advertise what we have;
   serve pieces to peers over connections we already hold; basic unchoke policy.
2. **S2 — Inbound seeder.** Accept inbound peer connections on the peer port; serve as a full
   peer; announce to tracker + DHT as a seeder. We become a reachable swarm node.
3. **B0 — Live-source authentication (RE spike, interop linchpin).** Recover and implement how
   a broadcaster signs live pieces so consumers verify the source (the transport descriptor's
   RSA pubkey + per-piece/segment signatures). Proven when the official engine verifies our
   signatures. De-risks interop **before** building broadcast plumbing.
4. **B1 — Broadcast ingest → swarm.** `PUT /broadcast/{name}` accepts a chunked MPEG-TS body;
   chunk it into pieces; mint a transport file + infohash; announce; originate the swarm as the
   signed source. Validate outpace→outpace (loopback) **and** official-engine-plays-our-
   infohash.

### Hard constraints (carried from v1)

- Public API (paths, JSON keys, binary, config) carries **no `ace`/`acestream` token** except
  the `{network}` value `ace`. Broadcast ingest path `/broadcast/{name}` is clean.
- **Never call any Acestream HTTP/index API.** Discovery and announce are P2P (tracker/DHT).
- One shared download per `(network,id)`, fanned out to many clients (unchanged).

### Wire compatibility (first-class, cross-cutting)

**outpace and official Acestream peers must interoperate transparently — an Acestream peer
must not be able to tell it's talking to outpace, and vice versa.** Everything we *send* on
the serve side (extended handshake fields, advertisement, request/piece framing, choke/unchoke,
live-source signatures) must match the engine's exact wire format and sequencing, exactly as the
*receive* side already does in v1. Concretely:

- **Validate against ground truth, not assumptions.** Before implementing any serve-side
  message, capture how the **engine-as-seeder/source** actually behaves (Frida/pcap in the
  sandbox), commit fixtures, and assert our bytes match. The download side was built this way
  (notes 11–19); the serve side gets the same rigor.
- **Match the engine's choice of messages.** e.g. Acestream peers use BEP-6 fast-extension
  variants (`HAVE_ALL`/`HAVE_NONE`/`ALLOWED_FAST`) and a non-standard `Piece` header; we must
  advertise/serve with whatever the engine actually uses, not a textbook BitTorrent guess
  (`message.rs` already preserves these as `Unknown` on decode — the serve side must emit the
  real ones).
- **Bidirectional interop is a test gate, not an afterthought:** official engine downloads a
  piece *from outpace*; outpace downloads *from the engine* (already proven); two
  outpace peers; and (B-phases) the engine plays a outpace broadcast. Any divergence that
  lets a peer distinguish us is a bug.

This principle applies to **every** component below; per-component notes call out the specific
ground-truth capture each one needs.

## Non-goals (explicitly out of THIS spec)

- **RTMP / SRT ingest.** OBS speaks these natively and they are **planned for a future spec**,
  but v2 ingest is **HTTP push only**. The ingest layer is designed behind a small seam
  (`BroadcastIngest` → `TsChunker`) so RTMP/SRT slot in later without touching the swarm/source
  code.
- **Minting official `acestream://` content-ids.** Those are registered with Acestream's index,
  which we do not control. Interop is **by infohash + transport-file distribution** (hand the
  official engine our infohash; it joins our swarm). Content-id↔infohash registry is theirs.
- **UPnP / NAT-PMP automatic port mapping.** Inbound reachability is an operator concern in v2
  (port-forward / reachable host); auto-mapping is a later enhancement.
- **Tit-for-tat / rarest-first / endgame.** v2 uses a simple live-appropriate unchoke; classic
  BitTorrent choking refinements are future polish.
- **Premium/encrypted (DRM) content.** Public swarm only (`is_encrypted=0`), as in v1.
- **Transcoding.** The broadcast passes through the ingested MPEG-TS as-is.

## Architecture

```
                         ace-engine (bin: outpace)
  OBS / ffmpeg ──HTTP chunked MPEG-TS──►  PUT /broadcast/{name}
                                              │  BroadcastIngest
                                              ▼
                                          TsResync ─► TsChunker ──┐ (chunk→piece, ascending live idx)
                                                                  ▼
   AceProvider (download) ─► PieceReassembler ─────────────►  PieceStore  (rolling window + bitfield)
                                  ▲                               │
                                  │ reciprocate                   │  serve
                                  │                ┌──────────────┼───────────────┬──────────────────┐
                                  │                ▼              ▼               ▼                  ▼
                            follow_one_peer   SeederSession  inbound listener  tracker/DHT      transport file
                            (download loop)  (serve/Unchoke/  (accept :8621)    announce         (encode + ut_metadata
                             + serve back     Have/Piece,                      seeder/source      serve later)
                                              Choker)         + LiveSourceAuth signs pieces (B0)
```

The generic streaming engine (HTTP fan-out, `StreamManager`, lifecycle, HLS) is unchanged. New
swarm/source capability lives in `ace-swarm`/`ace-wire`; the broadcast ingest lives in
`ace-engine` behind a clean seam.

## Components

### 1. `ace-swarm::store::PieceStore` (pure) — the heart of seeding & broadcasting
One owner of completed piece data, feeding both "seed what we downloaded" and "serve what we
broadcast."

```rust
pub struct PieceStore { /* piece_length, chunk_length, map<piece_idx, PieceBuf>, window bounds */ }
impl PieceStore {
    pub fn new(piece_length: u64, chunk_length: u64, max_bytes: u64) -> Self;
    /// Insert a fully-assembled piece (from download or the chunker). Evicts oldest past `max_bytes`.
    pub fn put_piece(&mut self, index: u64, data: Vec<u8>);
    /// A chunk of a piece for serving (the 16 KiB slice), or None if missing/evicted.
    pub fn chunk(&self, piece: u64, chunk: u16) -> Option<&[u8]>;
    pub fn has(&self, index: u64) -> bool;
    /// Current contiguous live window we can serve: (min_piece, max_piece, position).
    pub fn window(&self) -> LiveWindow;
    /// Bitfield / have-set for the handshake/advertisement.
    pub fn have_set(&self) -> &BTreeSet<u64>;
}
```

- **Retention:** rolling, bounded by `max_bytes` (default ~128 MiB, configurable), FIFO eviction
  of the lowest indices. Bounds memory while covering the live window.
- **Sources:** `AceProvider` calls `put_piece` for each reassembled piece (currently dropped
  after forwarding); `TsChunker` calls it for broadcast pieces.
- **Pure & fully unit-testable** (no I/O).

### 2. `ace-swarm::seed` — serving peers
- `SeederSession<S>`: drives one peer in the serve direction over any `AsyncRead+AsyncWrite`.
  Sends our **signed** extended handshake advertising the `PieceStore` window (`mi`,
  `distance_from_source` = 0 for a broadcast source, ≥1 when reseeding downloaded content),
  advertises availability, and on a peer's `Interested`+`Request`/chunk-request replies — if the
  `Choker` has unchoked it — with a `Piece` built from the store. For a broadcast source it signs
  each piece via `LiveSourceAuth` (B0). **Wire compatibility:** the exact advertisement messages
  (BEP-6 `HAVE_ALL`/`HAVE_NONE`/`ALLOWED_FAST` vs `Bitfield`/`Have`), their ordering relative to
  the extended handshake/unchoke, and the `Piece` header layout must match what the engine emits
  as a seeder — captured as ground-truth fixtures first, then asserted byte-for-byte.
- `Choker` (**pure**): given the set of interested peers + current unchoked set, decide who to
  unchoke. Policy: up to `max_unchoked` (default 8) interested peers unchoked, plus one
  rotating optimistic slot; the rest choked. Deterministic given inputs → unit-testable.
- **Reciprocation (S1):** `follow_one_peer` (the download loop) gains the serve arms — when a
  peer we're downloading from sends `Interested`/`Request`, we answer from the store using the
  same serve logic. No new sockets; immediate compliance.

### 3. `ace-swarm::listen` — inbound peers (S2)
A `PeerListener` binds a configurable TCP port (default the advertised `p`=8621), accepts
connections, performs the **inbound** handshake (read peer's first, reply ours; verify infohash
is one we serve), and hands the socket to a `SeederSession`. Bounded concurrency
(`max_inbound_peers`). Announce changes from leecher to **seeder** (`left=0`, `event=completed`/
`started`) via the existing `ace_tracker::announce` + DHT.

### 4. `ace-wire` additions
- `transport::encode_transport(&TransportDescriptor) -> Vec<u8>` — production encoder (the
  inverse of `decode_transport`): bencode the descriptor, PKCS#7 + AES-128-CBC under the global
  key/IV, prepend `"AceStreamTransport\x00\x02"`. Round-trips with the decoder.
- `live_codec`: add `build_piece(piece, chunk, data) -> PeerMessage::Piece` (the send side of
  the request/piece codec we currently only parse).
- `live_auth::LiveSourceAuth` (from B0): RSA keypair generate/load, `sign(...)`/`verify(...)`
  for the live-source scheme, plus the transport `pubkey` derivation.

### 5. `ace-engine::broadcast` — ingest → swarm (B1)
- `BroadcastIngest`: the `PUT /broadcast/{name}` handler. Reads the chunked MPEG-TS request
  body, runs it through `TsResync` (clean alignment), then `TsChunker`.
- `TsChunker` (**pure**): accumulate TS bytes, emit fixed `chunk_length` (16 KiB) chunks grouped
  into `piece_length` (1 MiB) pieces with ascending live indices from an epoch; push completed
  pieces into a `PieceStore`. Inverse of `PieceReassembler` — a chunk-then-reassemble round-trip
  is the identity, which is the core test.
- **Minting:** build a `TransportDescriptor` (`name`, `piece_length`, `chunk_length`,
  `bitrate`, `authmethod`, trackers, our RSA `pubkey`), `encode_transport` → bytes; compute
  the official swarm infohash from the descriptor selected-field hash (see
  `docs/protocol/notes/29-infohash-formula-cracked.md`); persist the transport file under
  the data dir (so consumers/ut_metadata can fetch it).
- **Source:** announce the infohash; run `SeederSession`s as `distance_from_source=0`, signing
  pieces. The broadcast registers as a normal stream: listable in `/streams`, playable at
  `/streams/ace/<infohash>.ts` on any daemon.
- A small `BroadcastRegistry` maps `{name}` → `{infohash, PieceStore, transport bytes}` and is
  reachable from both the ingest handler and the seeder/listener.

### 6. Config & identity additions
- New config: `peer_listen` (default `0.0.0.0:8621`), `seed_store_bytes` (default 128 MiB),
  `max_unchoked` (8), `max_inbound_peers` (64), `enable_seeding` (default true),
  `enable_inbound` (default true). All env-var overridable with defaults (v1 pattern).
- Persist the broadcaster **RSA** key alongside the existing Ed25519 identity key.

## Phase detail & build order

Each phase is **its own implementation plan → TDD cycle** (the phases are sequenced sub-projects
of this one spec, not a single plan). Start with S1; S2 depends on S1; B1 depends on S2 + B0; B0
can run in parallel as a research spike. Recommended first plan: **S1**.

### S1 — Compliance & reciprocal upload
0. **Capture engine-as-seeder ground truth** (sandbox + Frida/pcap): the exact advertisement
   messages, their ordering, the `Piece` reply header, and choke/unchoke timing the engine uses
   when *serving* a peer. Commit fixtures under `tests/vectors/seed/`. Everything below asserts
   against these so an Acestream peer can't distinguish us.
1. `PieceStore` (pure, TDD): put/chunk/has/window/have_set + eviction.
2. `live_codec::build_piece` (pure, TDD) — header layout matches the captured engine `Piece`.
3. `Choker` (pure, TDD).
4. `SeederSession` serve loop (mock-peer duplex test: peer sends Interested+Request → we
   Unchoke + send the correct Piece; request for an evicted piece → no/empty serve). Assert the
   emitted advertisement/`Piece` bytes equal the captured engine fixtures.
5. `AceProvider`: feed reassembled pieces into a `PieceStore`; add the serve arms to
   `follow_one_peer` (reciprocate). Integration test: a mock peer downloads a piece *from us*
   while we download from it.
6. `/status` + `SourceStats`: add `uploaded`, `peers_served`.

### S2 — Inbound seeder
1. `PeerListener` (bind, accept, inbound handshake, dispatch to `SeederSession`); bounded
   concurrency. Integration test: a client connects to our listener, handshakes, requests a
   piece we hold, receives it.
2. Seeder announce (`left=0`, event) wired into the manager/session lifecycle.
3. `main.rs`: start the listener when `enable_inbound`.

### B0 — Live-source authentication (RE spike, go/no-go gate)
1. Sandbox the official engine **as a broadcaster** (it has a "start broadcast"/source mode);
   feed it a test TS.
2. Frida-hook its signing path (the transport RSA key + per-piece/segment signing — RESUME notes
   `LiveSourceAuth.sign`); recover the exact preimage + algorithm; capture **verify-only**
   vectors (committed under `tests/vectors/live-source-auth/`).
3. Implement `ace_wire::live_auth::LiveSourceAuth`; prove `verify` passes on captured engine
   signatures (N/N), mirroring the node-identity crack (note 17).
4. **Gate:** sign with our key and confirm the **official engine verifies + plays** a minimal
   broadcast by infohash. If signatures don't interop, document the blocker; S1/S2 + outpace-
   only broadcast still ship.

### B1 — Broadcast ingest → swarm
1. `transport::encode_transport` (pure, round-trip test with decoder + a captured engine
   transport vector).
2. `TsChunker` (pure, TDD): chunk→reassemble identity; live-window advance.
3. `BroadcastIngest` `PUT /broadcast/{name}`: body → resync → chunker → store; mint transport +
   infohash; persist; announce; start source seeding (signed per B0).
4. **Loopback E2E (offline, the reachable milestone):** `PUT` a fixture TS into daemon A; daemon
   B plays `/streams/ace/<infohash>.ts` via the existing `AceProvider` and ffmpeg decodes a
   frame. No official engine, both ends ours.
5. **Interop E2E (live):** hand the infohash/transport to the official engine; confirm it joins
   our swarm and plays. (Requires B0 passing + a reachable host.)
6. Expose broadcast metadata: it appears in `/streams`; `GET /broadcast/{name}` (or
   `/streams/ace/<infohash>/status`) reports the infohash + ingest state.

## Testing & verification

- **Pure unit (offline):** `PieceStore` (put/chunk/has/window/evict), `Choker` policy,
  `build_piece`, `TsChunker`↔`PieceReassembler` identity round-trip, `encode_transport`↔
  `decode_transport` round-trip (+ against a captured engine transport vector).
- **Integration (offline, mock/loopback):** serve-a-piece over duplex; reciprocate-while-
  downloading; inbound listener accept+serve; **broadcast loopback** (daemon A `PUT` → daemon B
  plays by infohash, ffmpeg decodes a frame).
- **Wire-compat (ground-truth fixtures):** every serve-side message (advertisement, `Piece`
  header, unchoke sequencing) asserted byte-for-byte against captured engine-as-seeder behavior.
- **Bidirectional interop (live gate):** the **official engine downloads a piece FROM
  outpace** (S1/S2 — the inverse of v1's proven download); two outpace peers exchange; an
  Acestream peer cannot distinguish us from a peer engine.
- **Live / RE-gated:** `LiveSourceAuth` verify vectors (N/N on engine signatures); official
  engine verifies our signature; official engine plays our broadcast by infohash; inbound seeder
  reachable from a second host.

## Risks / to-confirm

1. **Live-source auth (B0) is the interop linchpin.** If the live-source signing can't be
   matched (or requires keys we can't mint), official-engine interop is blocked; the spec still
   ships S1/S2 compliance + outpace↔outpace broadcast. Mitigated by spiking it first.
2. **NAT reachability** for inbound/broadcast — operator port-forward in v2; UPnP later.
3. **Retention sizing** — `seed_store_bytes` vs serving the live window; default 128 MiB,
   tunable; broadcasting a high-bitrate feed may need more.
4. **Ingest backpressure** — as the source we hold the live window and evict; a consumer that
   can't keep up just lags within the window (same as any live swarm).
5. **No official content-id** — interop is infohash/transport-based; `acestream://` links for
   our broadcasts are out of scope.

## Out-of-scope follow-ups (named for later specs)

- RTMP/SRT ingest (native OBS transports) behind the same `BroadcastIngest` seam.
- UPnP/NAT-PMP automatic port mapping.
- Tit-for-tat / rarest-first / endgame choking.
- Serving the broadcast transport over `ut_metadata` so consumers resolve it from the infohash
  alone (pairs with v1's resolve path).
