# VOD transport and playback support (single-file)

Design for issue #47 (child of the parity epic #46). Adds a VOD (video-on-demand)
download/verify/serve path alongside the existing live path, without regressing live.

## Context

Outpace currently targets public **live** Acestream swarms. The transport-file decoder
(`ace-wire/src/transport.rs`) already distinguishes live from VOD: it surfaces the `pieces`
key (a concatenated list of 20-byte SHA-1 piece hashes) and an `is_live` flag (`true` when no
`pieces` key is present). Everything downstream of resolution, however, is live-shaped:

- `StreamInfo` (`ace-swarm/src/types.rs`) carries no `pieces`/`is_live`/total length.
- Piece integrity on the live path is per-piece **in-band RSA signatures**
  (`sig_len` + `source_pubkey`), not the transport's SHA-1 `pieces`.
- The scheduler follows a sliding **live window** (peers advertise `min_piece`/`max_piece`).
- `TsSource` / the HTTP serve path emit an unbounded live MPEG-TS stream.

### Key protocol fact

Per `docs/protocol/wire-protocol.md`, **VOD is vanilla BitTorrent**: standard
`Bitfield`/`Have`/`Request{index,begin,length}`/`Piece{index,begin,block}` messages (all
already modeled in `ace_wire::message::PeerMessage`), with integrity provided by the
transport's `pieces` SHA-1 list. The **live** path reuses those same message IDs with *custom*
`[stream u32]` payloads plus an 8-byte per-piece header (a separate `live_codec` layer). The
VOD path is therefore fully parallel to live and shares only the low-level connect/handshake
and `PieceStore` primitives — it never touches live message handling.

Because VOD is standard BitTorrent, the whole path is **deterministically testable offline**
with a local mock seeder; no live swarm is required (unlike the live path, whose real-network
behavior is only exercised by `#[ignore]`d tests).

## Scope

In scope:

- Single-file VOD: resolve → download → **SHA-1-verify each piece before emitting** → serve.
- Multi-file VOD: **explicitly rejected with a clear error** (acceptance criterion allows this).
- Native HTTP route and CLI path for VOD playback/download.
- Deterministic fixtures + unit tests + a mock-seeder integration test.

Out of scope (tracked as follow-ups):

- VOD HLS packaging.
- HTTP byte-range / seek requests (basic progressive streaming only).
- Reseeding of downloaded VOD data to inbound peers.
- Multi-file / selected-file index playback.
- Live-network CI (validation is left to the operator, as with the live path).

Live playback and origination behavior is unchanged.

## The one real assumption

No public VOD transport fixture is available, and the original engine cannot be queried from
this environment. The exact encoding of a VOD descriptor's **total content length** and
**multi-file** layout is therefore *synthesized from standard-BitTorrent conventions*:

- Single-file: a `length` key holding total content bytes; no `files` key.
- Multi-file: presence of a `files` key (a bencoded list) — detected and rejected.

This assumption is confined to descriptor parsing and is clearly documented in code as
"synthesized / reconcile against a real capture." Everything downstream (block requests,
reassembly, SHA-1 verification) is standard BitTorrent and does not depend on it.

## Components

### 1. `ace-wire/src/transport.rs` — VOD file layout

- Parse an optional `length` (total content bytes, single-file) and detect a `files` key.
- Add `TransportDescriptor::vod_total_length() -> Option<u64>` and `is_multifile() -> bool`.
- Document the synthesized schema. Round-trip test via `encode_transport`.

### 2. `ace-swarm` — VOD resolution + download (`src/vod.rs`, `src/types.rs`)

- `VodInfo { infohash, piece_length, chunk_length, trackers, piece_hashes: Vec<[u8;20]>,
  total_length: u64 }`, produced from a non-live descriptor. Multi-file → typed error.
- `download_vod(info, peers, sink)`:
  - Reuse the existing connect/handshake pool machinery.
  - Send `Interested`; on `Unchoke`, request blocks (`chunk_length`-sized) for needed pieces
    **in ascending order**, with a bounded in-flight request count.
  - Reassemble each piece from its blocks (a small in-order assembler — not the live
    reassembler), then **SHA-1-verify the assembled piece against `piece_hashes[index]`**.
  - On verification success, emit the verified piece bytes in order (last piece truncated to
    `total_length`). On mismatch, discard and re-request the piece from another peer.
  - On unrecoverable failure (no peer can supply a verifying piece), fail with a clear error.
    **Never emit unverified bytes.**
- Verification is a pure, unit-testable function: `verify_piece(hash, bytes) -> bool`.

### 3. `ace-engine` — provider seam, HTTP, CLI

- Provider: add `AceProvider::open_vod(id) -> Result<VodDownload, ProviderError>`, where
  `VodDownload` exposes `content_length()` and an ordered byte stream. The existing
  live `open()` returns a clear error if the id resolves to VOD, and `open_vod` errors on a
  live id. Resolution is shared/cached (`ResolveCache`), so dispatch is cheap.
- HTTP: `GET /vod/:network/:id` resolves and streams verified bytes with a `Content-Length`
  header. Live routes are untouched.
- CLI: `play` / `serve` resolve first and dispatch to the VOD path when the descriptor is not
  live; multi-file yields a clear error. `outpace play <vod-target>` writes verified ordered
  bytes to stdout (mirroring the live one-shot leech).

### 4. Fixtures + tests

- **Fixture builder**: synthesize a small single-file VOD transport (~3 pieces) with correct
  per-piece SHA-1 hashes and `length`, encoded under `TRANSPORT_KEY` via `encode_transport`.
  Also a multi-file variant (with a `files` key) for the rejection test.
- **Unit tests**: descriptor parsing (`pieces`, `length`, multi-file detection); `verify_piece`
  accepts the correct hash and rejects a tampered piece; multi-file `VodInfo` construction errors.
- **Integration test (mock seeder)**: a local `tokio` `TcpListener` speaking standard
  BitTorrent (66-byte handshake → full `Bitfield` → `Unchoke` → serve requested blocks) that
  serves the synthesized single-file content. Assert `download_vod` reproduces the exact
  original bytes; a tampered-block seeder is rejected; a multi-file descriptor errors.
- **Engine test**: exercise the `/vod` route (using a test VOD source) and assert the full
  body is returned with a correct `Content-Length`.

## Error handling

- Multi-file descriptor → typed error, surfaced as a clear CLI message / HTTP error status.
- SHA-1 mismatch → re-request the piece from another peer; unrecoverable → hard error.
  Unverified bytes are never emitted.
- Missing `length` on a VOD descriptor → error (cannot size or verify the final piece).

## Acceptance criteria mapping (issue #47)

- *A public/free VOD transport can be resolved, downloaded, verified, and served* — implemented
  and covered by the mock-seeder integration test; live-swarm verification left to the operator.
- *Piece hashes from the transport are verified before bytes are emitted* — `verify_piece`
  gates every emitted piece.
- *Multi-file or selected-file behavior is implemented or explicitly rejected* — rejected with
  a clear error.
- *Tests cover descriptor parsing, piece verification, and provider behavior using deterministic
  fixtures* — unit + integration tests above.
- *Live playback remains unchanged* — VOD path is parallel; no live code is modified beyond
  additive dispatch.
