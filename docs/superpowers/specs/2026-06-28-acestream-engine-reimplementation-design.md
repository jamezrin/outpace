# Open-Source Acestream Engine — Reimplementation Plan

**Date:** 2026-06-28
**Status:** Design / viability plan (pre-implementation)
**Codename:** `outpace` (working title)

## 1. Goal & Scope

Build a clean, open-source reimplementation of the **Acestream engine** — the
component that joins the Acestream P2P network, downloads a live/VOD stream from
peers, and re-exposes it over HTTP so any standard player can consume it. The
deliverable is a **CLI daemon** that drops into existing setups (Jellyfin, VLC,
`dispatcharr`, `acexy`-style proxies) without any closed-source binary blobs.

### In scope
- Join the **free public P2P swarm** and play content addressed by
  `content_id`, `infohash`, `magnet`, or transport-file `url`.
- Live **and** VOD streams.
- Output as **MPEG-TS** (`/ace/getstream`) and **HLS** (`/ace/manifest.m3u8`),
  plus **m3u playlist export** for player integration.
- Optional **transcoding** (audio to AAC/MP3/AC3, remux) via FFmpeg.
- A **6878-compatible HTTP API subset** so existing clients work unmodified.
- Single static binary, CLI-driven, container-friendly.

### Out of scope
- The **Ace Network DAO / Stellar blockchain** layer (content registry,
  subscriptions, payments, oracles, DMCA) — `ace-network-node` and
  `ace-network-explorer` are a Stellar fork and are a separate economic system.
- **Premium / authenticated / encrypted-premium** content, sign-in, ads.
- **Broadcasting as a source/support node** (injecting a stream into the swarm)
  — deferred; revisit after playback works.
- **Android apps and player UIs** — explicitly excluded.

### Decisions locked with stakeholder
- **Language:** implementer's recommendation → **Rust** (§4).
- **RE methodology:** **Full reverse-engineering** — decompile the closed
  binaries *and* capture live traffic.
- **Network scope:** **free public swarm only.**

## 2. What the references told us

### 2.1 Two different "networks" — only one is the engine
| System | Repos | Nature | Relevance |
|---|---|---|---|
| **P2P streaming swarm** | the closed engine (`Core.so`, `node.so`, `Transport.so`, `live.so`, `streamer.so`) | BitTorrent-derived swarm | **This is what we reimplement.** |
| **Ace Network DAO** | `ace-network-node`, `ace-network-explorer` | A **Stellar blockchain** fork | Out of scope. |

The Cython symbols recovered from `node.so`/`Transport.so` confirm a
BitTorrent-derived design: `infohash`, `get_trackers` / `get_meta_trackers` /
`allow_public_trackers` / `add_trackers`, `get_peers`, `get_piece_length`,
`TransportDescriptor` / `MultiTransportDescriptor`, and piece pickers
`PiecePickerSource` / `PiecePickerClient` / `PiecePickerClientDASH`
(live source, live client, and DASH/VOD variants).

### 2.2 Form of the closed engine
The engine is the **same codebase across Linux, Windows, and Android**:
a **Python application whose core is Cython-compiled to native `.so`**
(not plain `.pyc`):
`Core`, `CoreApp`, `Transport`, `node`, `live`, `streamer`, `pysegmenter`.
- Compiled with **Cython 0.29.22** — symbol names and docstrings survive in the
  binary, which materially helps reverse-engineering.
- Runtime deps: **`pynacl`** (NaCl: Curve25519 / Ed25519 / `crypto_box`),
  **`pycryptodome`** (AES — streams expose an `is_encrypted` flag),
  `aiohttp`, `apsw` (SQLite), `lxml`.
- Bundles **FFmpeg `.so`** (`libavcodec`, `libavformat`, `libswscale`,
  `libx264`, `libfdk-aac`, `libmp3lame`, …) for transcode/segmenting, and
  **Streamlink plugins** for direct-URL (non-P2P) stream extraction.
- Android core (`AceStreamCore-3.1.80`) embeds `libpython38.so` + a Python 3.8
  stdlib zip + the engine modules — confirming the shared lineage.

### 2.3 Documented vs. undocumented
- **Documented (consumer side):** the HTTP API on `:6878`
  (`/ace/getstream`, `/ace/manifest.m3u8`, `/server/api`, playback sessions,
  `stat`/`cmd`, playlists). Full reference exists in `ace-network-docs`.
  Every open tool we have — `acexy`, `acestream-http-proxy`, `aceplay` — is a
  **client of this API around the closed engine**, not a reimplementation.
- **Undocumented (the hard part):** the **P2P wire protocol** — peer handshake,
  encryption / key exchange, tracker & supernode bootstrap, `content_id` →
  `infohash` → transport-file resolution, and live piece exchange. **This must
  be reverse-engineered. It is the entire viability question.**

### 2.4 Key engine facts for the implementation
- P2P node port default **`8621`** (`--port`); HTTP API **`6878`**
  (`--http-port`); deprecated legacy API `62062`. State dir `~/.ACEStream`.
- Engine acts as a gateway: player tells it *what* to play → engine downloads
  from peers → reassembles to MPEG-TS / HLS → player reads over HTTP.
- `format=json` playback returns a session with `playback_url`, `stat_url`,
  `command_url`, `playback_session_id`, `is_live`, `is_encrypted`.
- Live streams track position (`livepos`: `live_first`/`live_last`/`pos`/
  `buffer_pieces`).

## 3. Viability Assessment

**Verdict: plausible but RE-gated.** The architecture is a known quantity
(BitTorrent-derived swarm + HTTP gateway), and mature Rust references exist
(`rqbit`, `cratetorrent`). The risk is concentrated almost entirely in
**recovering the proprietary protocol details**, especially the crypto
handshake and swarm-entry requirements.

**The make-or-break unknowns (must be resolved in Phase 0):**
1. **Does joining the public swarm require an acestream-issued identity/key?**
   `pynacl` `crypto_box` handshakes can be tied to server-signed node keys. If
   public peers reject unsigned/unknown clients, transparent interop is hard.
2. **Tracker / supernode bootstrap:** endpoints, protocol (HTTP/UDP/custom),
   and whether they fingerprint or rate-limit non-official clients.
3. **`content_id` ↔ `infohash` resolution:** how a bare `content_id` is mapped
   to a transport file / infohash (likely a hosted lookup service).
4. **Per-stream encryption:** when `is_encrypted=1`, where the key comes from
   (swarm vs. account/DAO). Public free streams appear to be largely
   unencrypted, which is favorable.

**Recommendation:** run a **timeboxed Phase-0 spike** to answer 1–4 before
committing to full implementation. Phase 0 produces a go/no-go decision and a
written protocol spec; everything downstream depends on it.

## 4. Language Choice — Rust (recommended)

| Factor | Rust | Go |
|---|---|---|
| Long-running daemon, live buffers | No GC pauses; predictable latency | GC pauses possible mid-playback |
| Parsing untrusted peer binary data | Strong types, `nom`, fuzzing; memory-safe | Safe but more manual |
| Crypto parity with `pynacl`/`pycryptodome` | `crypto_box`, `*-dalek`, RustCrypto | `golang.org/x/crypto`, `nacl` |
| BitTorrent reference to crib | **`rqbit`**, `cratetorrent` (production) | `anacrolix/torrent` (also strong) |
| Distribution | static musl binary | static binary |
| Iteration speed | slower | faster (precedent: `aceplay`) |

**Pick Rust.** The dominant cost is protocol RE, not language ergonomics, so
Rust's correctness/perf advantages for a security-sensitive network daemon
outweigh Go's faster iteration. **Go remains a viable fallback** if the team's
velocity in Rust proves limiting; the architecture below is language-agnostic.

## 5. Architecture

A daemon exposing the documented `:6878` HTTP API on the front and speaking the
Acestream P2P protocol on the back. Cargo **workspace**, built bottom-up so each
layer is independently testable against captured vectors.

```
player ──HTTP──> ace-engine ──> ace-swarm ──> ace-tracker (peer discovery)
  (VLC/Jellyfin)   (:6878 API)      │     └──> ace-peer  (per-peer protocol)
       ▲                            │              └──> ace-wire (framing/crypto)
       └────── MPEG-TS / HLS ◄── ace-media ◄── reassembled pieces
```

| Crate | Responsibility | Depends on |
|---|---|---|
| `ace-wire` | bencode; transport-file (`TransportDescriptor`/`Multi…`) parse/build; `content_id`/`infohash`/`magnet` parsing & hash math; message framing; NaCl handshake + stream cipher. **Fuzz-tested, pure.** | crypto crates |
| `ace-tracker` | tracker / meta-tracker / supernode discovery; announce/scrape; returns peer lists; bootstrap config | `ace-wire` |
| `ace-peer` | per-peer connection state machine: handshake, bitfield/have, request/piece, choke/unchoke, upload slots; live vs VOD | `ace-wire` |
| `ace-swarm` | orchestrate trackers+peers for one content; piece picker (`Source`/`Client`/DASH); disk+mem cache; live-position tracking; stats | `ace-tracker`, `ace-peer` |
| `ace-media` | reassemble pieces → MPEG-TS; HLS segmenter (`pysegmenter` analogue); optional FFmpeg transcode (subprocess first, libav bindings later) | — |
| `ace-engine` (bin) | the daemon: 6878-compatible HTTP subset, session manager, **m3u/playlist export**, config, CLI, logging | all above |

### Component contracts (one purpose each)
- `ace-wire`: given bytes → typed messages / descriptors, and back; no I/O.
- `ace-tracker`: given a content identity → a stream of candidate peers.
- `ace-peer`: given a peer socket + content → a stream of received pieces.
- `ace-swarm`: given a content identity → an ordered byte stream + live stats.
- `ace-media`: given an ordered byte stream → TS bytes or HLS segments/manifest.
- `ace-engine`: given an HTTP request → a playing session backed by a swarm.

### HTTP API surface (MVP subset)
- `GET /ace/getstream` (TS), `GET /ace/manifest.m3u8` (HLS), `/ace/c/.../N.ts`
  segments, `/ace/m/...`, `/ace/stat/...`, `/ace/cmd/...?method=stop`.
- `GET /server/api` (`get_version`, `get_status`, `get_media_files`,
  `get_content_id`, `analyze_content`) — enough for clients to negotiate.
- `format=json` playback-session responses matching the documented shape.

## 6. Reverse-Engineering Methodology (Phase 0)

The RE workstream produces **our own clean protocol spec + test vectors** that
the implementation is written against (clean-room style separation: the spec is
the artifact, not the decompiled source).

1. **Static lift of Cython `.so`** in Ghidra. Order: `node.so` (peer/tracker),
   `Transport.so` (transport file / hashing), `live.so` (live piece picker),
   then `Core.so`/`streamer.so`. Cython's preserved symbol names + docstrings
   anchor the analysis.
2. **Recover the algorithmic skeleton from older pure-`.pyc` releases** of the
   same lineage (early ACE 3.x / TorrentStream), decompiled with
   `decompyle3`/`uncompyle6`. These predate Cython and are far more readable;
   the protocol math (hashing, bencode layout, message IDs) is largely stable
   across versions.
3. **Dynamic capture & instrumentation.** Run the **official Linux engine in
   Docker** (`acestream_3.2.11_ubuntu_22.04` tarball), play known public
   infohashes, and capture: `:8621` peer traffic, tracker HTTP/UDP, and DNS for
   bootstrap hosts. Hook **pre-encryption** buffers and session keys via
   `frida` / `LD_PRELOAD` to obtain plaintext message vectors and confirm the
   handshake.
4. **Resolve the swarm-entry question (highest priority):** determine whether
   handshakes require acestream-signed keys, and how `content_id` resolves to a
   transport file. This answer decides go/no-go.
5. **Write `docs/protocol/` spec** + binary **test vectors** committed to the
   repo. All later crates are validated against these vectors and against live
   interop with official peers.

## 7. Phased Roadmap (the plan)

Each phase ends with a concrete, verifiable milestone. Phase 0 is the gate.

- **Phase 0 — Protocol recovery & viability spike (timeboxed).**
  Deliverables: written protocol spec (handshake, framing, tracker, piece
  exchange, transport-file format, `content_id`/`infohash` math), test vectors,
  and a **go/no-go memo** answering the four unknowns in §3.
  *Exit:* we can explain, on paper, how to fetch one public stream's pieces.

- **Phase 1 — `ace-wire` foundation.**
  bencode, transport-file parse/build, identifier/hash math, message framing,
  crypto handshake primitives. Validated against Phase-0 vectors + fuzzing.
  *Exit:* round-trip every captured message/transport file.

- **Phase 2 — Discovery + single-peer fetch.**
  `ace-tracker` (bootstrap → peers) and `ace-peer` (handshake → receive pieces
  from one real official peer). *Exit:* download and verify a known piece from
  the live network.

- **Phase 3 — `ace-swarm` (VOD first).**
  Multi-peer orchestration, piece picker, cache, integrity. *Exit:* fully
  download a public VOD transport and verify the assembled file.

- **Phase 4 — `ace-media` + `ace-engine` MVP.**
  Reassemble → MPEG-TS, serve `GET /ace/getstream`, minimal `/server/api` and
  session/`stat`/`cmd`. *Exit:* **play a public VOD stream in VLC** via our
  daemon.

- **Phase 5 — Live streaming.**
  Live piece picker (`Source`/`Client`), live-position/buffer management, HLS
  segmenter, `/ace/manifest.m3u8`. *Exit:* **play a public live channel** in VLC
  and Jellyfin.

- **Phase 6 — Integration polish.**
  m3u playlist export, transcoding (FFmpeg), config/CLI ergonomics, container
  image, docs, and drop-in compatibility with `dispatcharr`/`acexy`-style
  consumers. *Exit:* a documented, packaged single binary.

- **Phase 7 (deferred / optional) — Source-node broadcasting.**
  Inject a stream into the swarm. Re-scoped only after playback is solid.

## 8. Risks & Mitigations
- **Signed-identity swarm entry** → Phase-0 blocker; mitigation: instrument the
  official client to extract/observe handshake; if keys are server-issued,
  evaluate whether they can be obtained transparently (go/no-go input).
- **Bootstrap fingerprinting / bans of non-official clients** → mirror the
  official client's announce shape; keep a configurable bootstrap list.
- **Protocol drift between versions** → pin to the 3.1.80 / 3.2.11 captures;
  keep the spec + vectors versioned.
- **Legal exposure** — the plan derives from decompiled closed binaries
  (stakeholder accepted Full RE). Keep the *spec* as the clean artifact;
  document provenance. (Not legal advice.)
- **Effort** — multi-month, RE-heavy. The Phase-0 timebox prevents
  over-investing before viability is proven.

## 9. Open Questions (to resolve during Phase 0)
- Exact transport-file / `TransportDescriptor` binary layout and how
  `MultiTransportDescriptor` (multi-stream) is encoded.
- Tracker protocol: HTTP vs UDP vs custom; announce cadence; supernode role.
- Whether DHT participation is required or trackers suffice for public content.
- Resolution path for bare `content_id` (hosted lookup vs derivable).
- Minimum viable handshake to be accepted by official peers.
```
