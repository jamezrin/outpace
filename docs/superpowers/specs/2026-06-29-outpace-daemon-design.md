# outpace daemon — productization design (v1)

**Date:** 2026-06-29
**Status:** DRAFT — awaiting user review
**Supersedes nothing; builds on** `2026-06-28-acestream-engine-reimplementation-design.md`
and the cracked live protocol (`docs/protocol/notes/17–19`).

## Goal

Turn the proven live-download path (signed handshake → unchoke → Acestream piece
requests → MPEG-TS, verified end-to-end against the real swarm, 1080p H.264 decoded) into
a polished, multi-client daemon that plays live streams in VLC out of the box, with a
clean public API that carries **no ace/acestream naming**.

## Scope (v1)

1. Promote the live protocol from `ace-peer`'s `live_recon_unchoke` test into the library
   (`ace-wire` + `ace-swarm`).
2. **Provider abstraction** (strategy/adapter): a `StreamProvider` trait behind the
   `{network}` URL segment; the generic engine is provider-agnostic. Only the `ace`
   provider is implemented in v1; the architecture supports adding more with no engine
   changes.
3. Multi-client streaming engine: one shared download per stream, fanned out to many
   concurrent clients; lazy start, reference-counted, idle teardown.
4. Network-based **content-id resolution** (no Acestream APIs): fetch the
   `AceStreamTransport` metadata over the BitTorrent network and decode it.
5. Clean HTTP API exposing **MPEG-TS and HLS** playback plus status/listing.
6. Gapless cross-piece MPEG-TS muxing.
7. End-to-end verification: **VLC plays `http://localhost:PORT/streams/ace/{id}.ts`**.

## Non-goals (explicitly later / never)

- **Stream search & aggregate playlist** — a *later* spec, and it must discover streams
  **over the P2P network (DHT)**, NOT by calling Acestream's index/search APIs. The API
  reserves `GET /search` and `GET /playlist.m3u8` so they slot in without rework.
- **No dependency on any Acestream HTTP API**, ever (resolution and discovery are P2P).
- Premium/encrypted (DRM) content — public swarm only.
- Transcoding — pass through the source MPEG-TS as-is.

## Architecture

```
                    ┌───────────────────────── ace-engine (bin: outpace) ─────────────────────────┐
   VLC / Jellyfin → │  axum HTTP API  →  StreamManager  →  StreamSession (one per (network,id))        │
   dispatcharr      │ /streams/{network}/..  (registry)        │  (generic: fan-out, lifecycle, HLS)   │
                    │                                          │                                       │
                    │                            ProviderRegistry: network → dyn StreamProvider        │
                    │                                          │                                       │
                    │                            ┌── StreamProvider trait (strategy/adapter) ──┐        │
                    │                            │  AceProvider ("ace")  [only provider in v1]  │        │
                    │                            │   resolve → discover → LiveSession → TS      │        │
                    │                            └──────────────────────────────────────────────┘       │
                    └────────────────────────────────────────────────────────────────────────────────┘
   AceProvider wraps:  ace-swarm (scheduler/driver/LiveSession)   ace-tracker (BEP-15)   ace-media (TS/HLS)   ace-wire (protocol/identity/transport/codec)
```

The generic engine (HTTP API, `StreamManager`, fan-out ring buffer, lifecycle, HLS
segmenting) is **provider-agnostic** — it consumes an opaque live MPEG-TS byte stream. All
Acestream-specific protocol lives behind one `StreamProvider` adapter.

Internal crate names stay `ace-*` (they implement the Acestream protocol — accurate for
implementation crates). The **public surface** (HTTP paths, JSON field names, the binary
name `outpace`, config keys) contains zero `ace`/`acestream` tokens **except the
`{network}` selector value `ace`**, which is a deliberate, first-class provider identifier
(not leaked baggage) — the entry point to the provider abstraction.

## Components

### 0. Provider abstraction (strategy / adapter) — the core of the clean architecture
A `StreamProvider` trait is the single seam between the generic engine and any
network's protocol. `{network}` in the URL selects the adapter via a `ProviderRegistry`.

```rust
#[async_trait]
pub trait StreamProvider: Send + Sync {
    /// Network selector used in the URL, e.g. "ace".
    fn network(&self) -> &'static str;
    /// Open a live MPEG-TS source for `id` (provider resolves/discovers internally).
    async fn open(&self, id: &str) -> Result<Box<dyn TsSource>>;
}

#[async_trait]
pub trait TsSource: Send {
    /// Next contiguous MPEG-TS chunk, or None at end. The engine fans this out.
    async fn next(&mut self) -> Option<Bytes>;
    /// Snapshot for /status (peers, bitrate, buffer).
    fn stats(&self) -> SourceStats;
}
```

- **`AceProvider` ("ace") is the only provider in v1.** Its `open` does the full
  Acestream pipeline (resolve → tracker discovery → `LiveSession` → reassemble → TS),
  wrapping `ace-wire`/`ace-swarm`/`ace-tracker`/`ace-media`.
- The engine never names a network: it looks up `registry.get(network)` and consumes
  `TsSource`. Adding a future provider = implement the trait + register it; **zero changes**
  to the HTTP layer, fan-out, lifecycle, or HLS.
- Unknown `{network}` → `404`. The registry is constructed at startup.

### 1. Protocol promotion (`ace-wire`, `ace-swarm`)
- `ace-wire`: a full signed extended-handshake builder (the complete field set from
  note 19: `node_id`, `signature`, `ts`, `v`, `pv`, `p`, `nt`, `platform`, `asn`,
  `asn_country`, `geoip_country`, `lsp`, `tt="bt"`, `yourip`, `stream_statuses`, rich `mi`),
  and the Acestream piece request/piece codec (request id=6 `[stream u32=0][piece u32]
  [chunk u16]`; piece id=7 `[stream][piece][8-byte piece hdr][chunk u16][16384 data]`).
- `ace-swarm`: a `LiveSession<S>` that performs the signed handshake, waits for unchoke,
  drives the existing `Scheduler`/`PieceReassembler`, and yields contiguous TS bytes. The
  inline recon logic in `ace-peer` is deleted once covered.

### 2. Content-id resolution (`ace-swarm::resolve`)
`resolve(content_id) -> StreamInfo { infohash, piece_length, chunk_length, trackers }`:
1. Find peers holding the content-id's metadata (tracker first via `ace-tracker`; **DHT is
   the documented fallback/risk** — see Risks).
2. Fetch the `AceStreamTransport` file via `ut_metadata` (BEP-9; our handshake already
   advertises `ut_metadata`).
3. Decode it with the existing `ace-wire` transport decoder → `StreamInfo`.
Results are cached (content-id → StreamInfo) with a TTL. A raw infohash is accepted
directly; piece/chunk sizing still comes from the transport metadata.

### 3. Streaming engine (`ace-engine`) — provider-agnostic
- `StreamManager`: `HashMap<(Network, Id), Arc<StreamSession>>` behind a lock;
  `get_or_start(network, id)` resolves the provider via the `ProviderRegistry`.
- `StreamSession`: calls `provider.open(id)` to get a `TsSource`, then publishes its TS
  chunks to a **live ring buffer**. Clients subscribe at the current live edge (a
  `tokio::sync::broadcast` of `Arc<[u8]>` TS chunks, or a bounded ring with per-subscriber
  cursors). Exactly one `TsSource` (one download) regardless of client count. The session
  contains **no Acestream-specific logic** — that all lives in the provider.
- Lifecycle: lazy start on first request; `Arc` ref-count + a grace timer; teardown (stop
  swarm, drop buffers) after the last client disconnects + grace (default 30 s).

**Out-of-the-box / acexy parity (first-class requirement).** acexy exists only because the
official engine doesn't cleanly multiplex clients or manage stream lifecycle; this daemon
does both natively, so **no proxy/wrapper is ever needed** — players point directly at
`http://daemon/streams/{network}/{id}.ts`. Required behaviors:
- **N clients → 1 download** for the same `(network, id)` (fan-out from the shared ring
  buffer); never start a second download for a stream already running.
- **Concurrent distinct streams** each run their own independent session in parallel.
- **Independent client lifecycles:** any client connecting, disconnecting, or stalling
  (VLC closed, tab closed, network drop) must not interrupt other clients on that stream;
  the session persists while ≥1 client is attached.
- **Mid-stream join:** a new client starts at the current live edge (no rebuffer of old
  data, no restart of the download).
- **Last-client teardown:** session stops after the last client leaves + grace, freeing
  swarm connections and buffers.
These behaviors get explicit integration tests (multiple concurrent subscribers; one
subscriber dropping mid-stream; join-after-start) — see Testing.

### 4. Clean HTTP API (axum)
`{network}` selects the provider (only `ace` exists in v1); `{id}` is a content-id or
infohash for that network.

| Method & path | Purpose |
|---|---|
| `GET /streams/{network}/{id}.ts` | Continuous MPEG-TS (primary playback). |
| `GET /streams/{network}/{id}.m3u8` | HLS media playlist (live sliding window). |
| `GET /streams/{network}/{id}/seg/{n}.ts` | HLS segment (from `ace-media` segmenter). |
| `GET /streams/{network}/{id}/status` | JSON: `{ network, state, peers, bitrate, buffer, clients }`. |
| `GET /streams` | JSON list of active sessions (each tagged with its `network`). |
| `GET /networks` | JSON list of registered providers (v1: `["ace"]`). |
| `DELETE /streams/{network}/{id}` | Force-stop a session (optional admin). |
| `GET /healthz` | Liveness. |
| *(reserved, later spec)* `GET /search?q=&network=`, `GET /playlist.m3u8` | Network discovery + aggregate. |

Example: `http://localhost:PORT/streams/ace/cid2.ts`.
JSON uses clean names only (`network`, `stream`, `peers`, `bitrate`, `buffer`, `clients`) —
no `infohash`/`ace*` field names; the opaque `{id}` is the only hex value exposed.

### 5. Gapless muxing (`ace-media` / reassembly)
Pin the exact per-piece byte layout to resolve the observed **−4 B/piece alignment drift**
(≈96 bytes/piece that don't raw-chain). Likely a small per-piece structure in the
transport/piece framing; implementation will confirm against captured pieces. Fallback:
rely on TS demuxer resync (ffmpeg already decodes with `-err_detect ignore_err`), but the
goal is byte-clean continuity.

### 6. Identity & config
Generate an Ed25519 identity once and persist it (our own "device key") under the OS
config dir (e.g. `~/.config/outpace/identity.key`); reuse across runs. Config:
listen address/port, idle-grace, cache TTL, log level. Sensible defaults so it runs with
no flags.

## Peer discovery
`ace-tracker` (BEP-15 UDP, proven live in Phase 2) announces the live infohash to
`t1.torrentstream.org:2710` and returns peers; the swarm download proceeds as proven. DHT
is a **future enhancement** (borrow a vetted crate) and the fallback for content-id
metadata peers if the tracker doesn't serve them.

## Testing & verification
- Unit: handshake builder, request/piece codec, resolution (mock `ut_metadata` peer +
  fixture transport), scheduler, ring-buffer fan-out.
- Integration: multiple concurrent subscribers off one mock provider/`StreamSession`
  receive the same TS; idle teardown fires; HLS manifest/segments validate. A trivial
  in-memory test `StreamProvider` exercises the engine with no network, proving the
  provider seam.
- **Live E2E (the deliverable):** start the daemon, point VLC at
  `http://localhost:PORT/streams/ace/{id}.ts` for a known-live channel, confirm playback;
  also confirm two simultaneous clients share a single swarm download.

## Risks / to-confirm during implementation
1. **Content-id metadata peer source** — does the BEP-15 tracker return peers for the
   content-id metadata, or is DHT required? If DHT is required, content-id resolution
   pulls a DHT crate into v1 scope (infohash-direct streaming still works without it).
2. **−4 B/piece drift** — exact cause; clean fix vs demuxer-resync fallback.
3. **Tracker peer yield** — enough peers for smooth live playback from a cold start.

## Build order
1. Promote protocol (handshake + codec) into `ace-wire`; `LiveSession` into `ace-swarm`;
   delete inline recon.
2. `StreamProvider`/`TsSource` traits + `ProviderRegistry` + an in-memory test provider.
3. `StreamSession` + ring-buffer fan-out + `StreamManager` (provider-agnostic), validated
   against the test provider.
4. axum API `/streams/{network}/...` (TS first, then HLS, then status/list/networks).
5. `AceProvider`: wire resolve → tracker discovery → `LiveSession` → TS behind the trait.
6. Content-id resolution; gapless muxing; live VLC verification.
