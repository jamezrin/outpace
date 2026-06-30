# RESUME — start here in a fresh session

This is the single entry point for picking up the **outpace** project (an
open-source, from-scratch reimplementation of the **Acestream P2P streaming
engine**) in a new Claude Code session with no prior context.

## 30-second orientation
- **Goal:** a CLI daemon that joins the Acestream network, pulls a stream, and
  re-exposes it (MPEG-TS / HLS / m3u) so VLC/Jellyfin/dispatcharr can play it.
  No closed-source blobs. Engine only — Android/player apps out of scope.
- **Viability:** PROVEN (GO). An independent client built here was accepted by live
  swarm peers. See `docs/superpowers/specs/2026-06-28-phase0-findings.md`.
- **Read next, in order:** this file → `docs/superpowers/specs/2026-06-28-acestream-engine-reimplementation-design.md`
  (design) → `docs/protocol/wire-protocol.md` (the protocol) →
  `docs/protocol/transport-file.md`. The `docs/protocol/notes/` folder has the
  detailed reverse-engineering findings (numbered 00–13).

## Current state (what's on `main`, all committed + tests green)
Run `cargo test` — should be all green (live-network tests are `#[ignore]`d).

| Phase | Status | Deliverable |
|---|---|---|
| 0 Protocol recovery | ✅ done | Specs + vectors + go/no-go memo (GO) |
| 1 `ace-wire` | ✅ done | infohash, bencode, handshake, msg framing, extended HS, **transport decoder** |
| 2 `ace-tracker` + `ace-peer` | ✅ done | BEP-15 UDP tracker; async peer session (handshake + read) — both verified live |
| 3 piece download → media → engine | ✅ done | **live HD video downloaded from the real swarm** (note 19: full signed handshake → UNCHOKE → Acestream piece requests → MPEG-TS → ffmpeg decodes 1920×1080 H.264+AAC). Protocol promoted into `ace-wire`/`ace-swarm` |
| 4 productization (outpace daemon) | ✅ built + **live-proven** | **Autonomous DHT discovery → connect → 9.4 MB live MPEG-TS downloaded from just an infohash (verified 2026-06-29).** Clean provider-abstracted daemon: `StreamProvider`/`TsSource` + `ProviderRegistry`, shared `StreamSession` broadcast fan-out (one download → many clients, **acexy-parity out of the box**), `StreamManager`, axum HTTP API, live HLS, config + persistent identity, tracker discovery, transport→`StreamInfo` resolve, `AceProvider`. All tests green + clippy clean; daemon boots & serves the clean API. Muxing drift (Task 19) fixed (`TsResync`); per-client start-on-keyframe done (`KeyframeGate`); content-id→infohash resolution over `ut_metadata` done (Task 18 — `ace_wire::ut_metadata`, `PeerSession::fetch_metadata`, `ace_swarm::resolve::resolve_via_peer`, offline mock-peer integration test); DELETE force-stop + abort-pump-on-teardown. **Spec complete, incl. live E2E (Task 20, verified 2026-06-30 — see `docs/protocol/notes/20-vlc-playback.md`):** daemon autonomously discovered 25 peers, downloaded 8.3 MB live MPEG-TS, ffmpeg decoded a 1280×720 H.264 frame from the served output, the served stream starts on a keyframe, and two clients shared one session (`/streams` → `clients:2`). Plan: `docs/superpowers/plans/2026-06-29-outpace-daemon.md`; spec: `docs/superpowers/specs/2026-06-29-outpace-daemon-design.md` |

Crates: `crates/{ace-wire,ace-tracker,ace-peer,ace-media,ace-engine}`. Workspace root `Cargo.toml`.
**Pure Phase 3/4 logic done (no live data needed):** `ace_wire::live` (LiveWindow/LivePicker),
`ace_wire::reassembly` (PieceReassembler: chunks→ordered bytes), `ace_media::{mpegts,hls}`
(TS align + HLS segment/manifest), `ace_engine::routes` (6878 URL surface). What's left needs
the live byte path: peer download loop / `ace-swarm`, then wire `ace-media`+`ace-engine` to it.

## Next: v2 — P2P compliance, seeding & broadcasting
The v1 daemon was **download-only (a pure leecher)**. v2 fixes that and adds origination. Spec:
**`docs/superpowers/specs/2026-06-30-compliance-seeding-broadcasting-design.md`**. Four phases:
**S1** reciprocal upload, **S2** inbound seeder, **B0** live-source-auth RE spike, **B1** HTTP
broadcast ingest. **Hard constraint throughout:** full **wire compatibility** with the official
engine — outpace and Acestream peers must interoperate transparently (an Acestream peer can't
tell it's talking to outpace). RTMP/SRT ingest is a documented future follow-up, out of scope.

### S1 — DONE (merged to main 2026-06-30, branch `seeding-s1-compliance`, plan `docs/superpowers/plans/2026-06-30-seeding-s1-compliance.md`)
Reciprocal upload over connections we already hold. Built + reviewed (spec + code-quality + a
final whole-branch pass; all green, clippy clean):
- `ace-swarm::store::PieceStore` — pure bounded rolling chunk store (FIFO evict lowest piece).
- `ace-wire::live_codec::build_piece` — send side of the live piece codec (inverse of `LiveChunk`).
- `ace-swarm::seed::SeederSession::serve` (advertise Haves → unchoke on `Interested` → answer
  `Unknown{id:6}` chunk-requests with `build_piece`).
- `ace-engine::ace_provider::follow_one_peer` — feeds a per-connection `PieceStore` from
  downloaded pieces and serves the peer's chunk-requests inline; `uploaded`/`peers_served`
  atomics surfaced at `/status`.

### v2 offline foundations — DONE (merged to main, plan
`docs/superpowers/plans/2026-06-30-v2-offline-foundations.md`)
Everything from S2/B1 that's buildable and testable WITHOUT a live Acestream swarm or RE sandbox.
All green, clippy clean, full workspace:
- `ace-wire::transport::encode_transport` — production transport-file encoder (inverse of
  `decode_transport`); unblocks B1 minting. Round-trips with the decoder.
- `ace-wire::chunker::TsChunker` — pure live-stream chunker (inverse of `PieceReassembler`);
  unblocks B1's `PUT /broadcast` ingest. Chunk-then-reassemble proven as the identity.
- `ace-peer::session::PeerSession::accept_handshake` — inbound/responder-side BT handshake
  (reads peer's handshake first, replies only if we serve the requested infohash).
- `ace-tracker`/`ace-swarm::discover::announce_seeder` — parameterized the tracker announce
  `event` field; added a seeder-announce helper (`left=0`, `Completed`). **No production caller
  yet** — wiring into the manager/session lifecycle is the remaining S2 item.
- `ace-engine::config::Config` gained inbound-seeder knobs: `peer_listen`, `seed_store_bytes`,
  `max_unchoked`, `max_inbound_peers`, `enable_seeding`, `enable_inbound`. `enable_seeding` is
  now wired (gates the three S1 serve arms in `follow_one_peer` — `Interested`, the chunk-serve
  arm, and `Have`-on-completion; `false` makes the provider a pure leecher). **`max_unchoked` is
  still accepted but not wired** — it configures `Choker`, which has no production caller until
  S2's multi-peer serve coordinator exists (a single outbound connection or a single inbound
  `SeederSession::serve` call has nobody to choose between).
- `ace-swarm::listen::{SeedRegistry, PeerListener}` — inbound seeder plumbing: a registry
  mapping infohash → shared `PieceStore`, and a bounded-concurrency TCP accept loop handing
  accepted peers to `SeederSession::serve`. Real-TCP loopback-tested (accept+handshake+serve,
  and refusal of an infohash we don't serve). **Known gap:** `SeedRegistry` entries are never
  evicted — unbounded by infohash count (each store is itself byte-bounded) for the process's
  lifetime; add eviction (e.g. tied to `StreamSession` teardown) if this becomes a real concern.
- Wired into the live engine: `follow_one_peer`'s store now comes from the shared registry (so
  downloaded pieces become servable to inbound peers too); `main.rs` optionally starts
  `PeerListener` — **gated OFF by default** (`enable_inbound=false`) because the served
  `piece_header` is still a placeholder `[0u8;8]`. Verified live: a default `cargo run` does
  NOT start the inbound listener; `OUTPACE_ENABLE_INBOUND=1` does.
- **Do not flip `enable_inbound`'s default to `true` before Task 7 (below) lands** — serving
  non-compliant pieces to the real swarm would violate the wire-compatibility constraint.

### Task 7 — SUBSTANTIALLY ADVANCED, not closed (`docs/protocol/notes/21-seeder-ground-truth.md`)
With Docker + a non-WARP host (confirmed available 2026-06-30), ran real verification against
both the live swarm and the official engine itself (sandboxed):
- **Captured real piece-header structure** from a live peer: the 8-byte header is constant
  across every chunk of one piece (confirms it's per-piece, matching our `piece_header:[u8;8]`
  assumption) — `header[0..4]` is constant across pieces (session-wide), `header[4..8]` varies
  per piece with irregular deltas (hash? fine timestamp? undetermined).
- **Proved the official engine accepts outpace's full protocol stack** — signed handshake,
  Ed25519 identity, BEP-10 extended handshake, Acestream request/piece codec — by connecting
  outpace directly to the engine's peer port inside its own Docker sandbox and downloading
  9 MB of real live video from it. This is the harder, more rigorous half of wire-compatibility
  (the *reference implementation* treats outpace indistinguishably from a real peer).
- **Tried, broke, and reverted a fix for the reverse direction (engine downloads FROM
  outpace).** Added proactive `Have`-advertisement to `follow_one_peer` to give peers a
  signal to request from us — **live testing (prompted by VLC showing no video) found this
  makes a real swarm peer go silent**, confirmed by bisection (works before the commit, hangs
  with it, works again reverted). Reverted outright (no demonstrated benefit either way — see
  note 21's "⚠️ Update" section for the full account). **Lesson banked: any new behavior on
  the outbound leecher path needs a live download smoke test, not just mock/duplex tests,
  before being trusted.** The reverse direction is open again; needs getting the engine to
  dial outpace directly so the *inbound* `SeederSession::serve` path (untouched, unaffected
  by this regression) is what gets exercised — via DHT/tracker self-announce (`announce_seeder`
  exists but is unwired), or the engine's I2I instance-coordination API on port 62062, briefly
  probed, not HTTP, would need Frida/binary RE comparable to the node-identity crack.
- `header[4..8]`'s exact semantics remain open; the served `piece_header` is still `[0u8;8]`.
- **Next session, in order of leverage:** (1) try the reverse-direction proof again with a
  deliberately staggered start time between outpace and a test client, to get a genuine
  window-lead without needing S2; (2) wire `announce_seeder` into the manager lifecycle so
  outpace becomes organically discoverable, then retry engine-dials-us; (3) only if those
  don't converge, invest in I2I/binary RE for direct peer injection. Once the engine is shown
  downloading from outpace (any route), flip `enable_inbound`'s default to `true`.

### Remaining: B0, B1 (RE/sandbox-gated where noted)
- **B0** live-source-auth RE spike (RSA-signed pieces, like the node-identity crack) — no plan
  written yet; can run in parallel as a research spike.
- **B1 minting/origination + live verification**: the pure codecs (`encode_transport`,
  `TsChunker`) are done; remaining is the `PUT /broadcast/{name}` HTTP handler, the
  `BroadcastRegistry`, signing pieces per B0, and live verification that an official Acestream
  client can discover and play a outpace-originated broadcast. Depends on S2 (done) + B0.

## Run the daemon + verify VLC (live, operator-run)
The daemon binary is `outpace` (`cargo run -p ace-engine --bin outpace`). The clean API:
`GET /healthz`, `/networks`, `/streams`, `/streams/{network}/{id}.ts` (live MPEG-TS),
`/streams/{network}/{id}.m3u8` (+ `/seg/{n}.ts`), `/streams/{network}/{id}/status`,
`DELETE /streams/{network}/{id}` (force-stop a session). `{network}` is the provider key
(`ace` today). **No `ace`/`acestream` baggage** elsewhere. (Session teardown — idle-reaper or
force-stop — now aborts the background pull task, so a stopped stream stops downloading.)

Default bind is `127.0.0.1:6878` (collides with a running official engine — override with
`OUTPACE_BIND`). **Peer discovery is autonomous via mainline DHT** (`ace_swarm::dht`) +
the UDP tracker — no peer list needed. **VERIFIED LIVE (2026-06-29):** from just an infohash
the daemon discovers ~44 peers, connects, and downloads 9.4 MB of live MPEG-TS (188-aligned,
H.264) with no hand-fed peers.

```sh
# Fully autonomous — just an infohash:
OUTPACE_BIND=127.0.0.1:6900 cargo run -p ace-engine --bin outpace
vlc http://127.0.0.1:6900/streams/ace/<40-hex-infohash>.ts
```

The served stream is **clean, packet-aligned MPEG-TS** — the −4 B/piece drift (Task 19) is
fixed by `ace_media::mpegts::TsResync` (Acestream's 1 MiB live pieces are each internally
188-aligned but don't byte-chain; the resync filter drops the ~1 partial packet of junk per
piece boundary and re-locks). **VERIFIED:** `ffmpeg` decodes the daemon's output to
1280×720 50 fps H.264.

**Start-on-keyframe (done).** Each `.ts` client is fed through a per-client
`ace_media::mpegts::KeyframeGate`: a player joining mid-GOP is held until the first clean
keyframe — it parses PAT→PMT to find the video PID, detects the random-access point (the
adaptation-field RAI bit or an H.264 IDR/SPS NAL), then emits the cached PAT+PMT followed by
the keyframe and passes everything through after. A safety budget falls back to passthrough
if no keyframe appears. So players start on a decodable picture instead of garbage. Validated
against a committed real-encoder fixture (`tests/vectors/media/h264-keyframes.ts`): joining
mid-GOP, the gate locks exactly on the real keyframe and ffmpeg decodes the output I-frame
first. Applied per-client (the broadcast is unchanged), so every joiner benefits, not just the
first.

**Content-id → infohash resolution is now wired (network-native, no Acestream API).** Pass an
`acestream://` content-id as `cid:<40hex>`:
```sh
vlc http://127.0.0.1:6900/streams/ace/cid:cid1.ts
```
The daemon announces the content-id to the tracker/DHT, connects to a metadata-swarm peer,
fetches the `AceStreamTransport` file over **BEP-9 ut_metadata** (`ace_wire::ut_metadata` +
`PeerSession::fetch_metadata`), and decodes it to the real infohash + geometry + trackers
(`ace_swarm::resolve::resolve_via_peer`, TTL-cached). A bare `<40hex>` is still treated as an
infohash directly (proven path). The full resolve flow is offline-proven by an in-memory
mock-peer integration test (`crates/ace-swarm/tests/resolve_metadata.rs`); the live discovery
half shares the same environment gate as the download path. (The old fallback — asking a
running engine `getstream?content_id=…` for `.response.infohash` — is no longer needed.)

`OUTPACE_ACE_PEERS=ip:port,…` still exists as an optional manual-peer override.

Multi-client check: open the same URL from two players — `GET /streams` shows ONE session
with `clients: 2` (single shared swarm download).

## The protocol in one screen (all reverse-engineered + validated)
- Acestream is a **BitTornado (BitTorrent) fork**.
- **Discovery:** standard BitTorrent — UDP tracker (`t1.torrentstream.org:2710`) +
  Mainline DHT + LSD. (DHT not yet implemented; plan is to borrow a vetted DHT crate.)
- **Peer handshake (66 bytes, plaintext):** `0x11` + `"AceStreamProtocol"` + 8 zero
  reserved + infohash(20) + peer_id(20). peer_id = `R30------` + 11 random (ephemeral).
- **After handshake:** standard BT framing `<u32 len><u8 id><payload>` + a BEP-10
  extended handshake whose `mi` dict carries the **live piece window**
  (`min_piece`/`max_piece`/`position`/`live_window_size`).
- **Identifiers:** `infohash = SHA1(whole transport file)`. `content_id` is what
  `acestream://` links carry; the engine resolves content_id→infohash.
- **Transport file:** `"AceStreamTransport"` + `00 02` + **AES-128-CBC(fixed key/IV) →
  PKCS#7 → bencode** descriptor (name, piece_length, chunk_length, trackers, RSA
  pubkey; VOD adds a `pieces` SHA1 list). Key/IV are embedded constants in
  `ace-wire::transport`.
- **No account/identity needed** for the public swarm. Public content is
  `is_encrypted=0` (no DRM); the AES `Encrypter` is only for premium (out of scope).

## THE NEXT STEP (Phase 3.2 — piece download) — BLOCKER LOCATED
The blocker to "play in VLC" is now **precisely characterized** (see
`docs/protocol/notes/14-live-unchoke-recon.md`). `ace-peer` can now build + send its own
BEP-10 extended handshake (`ace_wire::extended::OutgoingExtendedHandshake`,
`PeerSession::send_extended_handshake`) and a recon harness (`live_recon_unchoke`,
`#[ignore]`d, with env knobs) drove the full exchange against live peers.

**Finding:** live peers accept our 66-byte handshake and send their extended handshake,
then **insta-close (~0.1 s) the moment we send ours** — regardless of `mi`/`interested`/
distance, and even with the full key set if `node_id`/`signature` are dummies. The same
peers serve the official engine. So the gate is a **valid node identity**: `node_id`
(32 B) + `signature` (64 B) — Ed25519-shaped. We must mint/sign a real one.

**Identity scheme CRACKED (notes 15–16):** `node_id` = an **Ed25519 public key** derived
from `/root/.ACEStream/device.key` (a 32-byte seed in hex) — **confirmed by exact pubkey
match**; `signature` = a 64-byte **Ed25519** signature. The identity is **self-generated**,
so we can mint our own keypair — no engine key needed. Signer = **`LiveSourceAuth.sign`**
(`core/src/live/LiveSourceAuth.pyx`), a Python orchestrator that delegates the actual sign.

1. **Preimage — SOLVED & verified (note 17).** The signature is computed **per-connection**:
   ```
   digest32  = SHA256( bencode(handshake_dict, signature := 64 × 0x00) )   # canonical sorted-key bencode
   signature = Ed25519_detached_sign(secret_key, digest32)
   ```
   `node_id` = the Ed25519 public key (self-generated; mint our own). Confirmed by
   `Ed25519_verify(node_id, signature, digest32)` passing **6/6** on live engine handshakes.
   (Note 16's "once at startup" was wrong — an attach-timing artifact; cracked by
   `docker restart` + **race-attach during boot** so `crypto_sign_detached` is hooked before
   the node signs.) Vectors: committed verify-only set in `tests/vectors/node-identity/`;
   full RE artifacts in `re/captures/node-identity/` (gitignored).
2. **Mint + sign:** Ed25519 identity in `ace_wire` + canonical bencode encoder; extend
   `OutgoingExtendedHandshake` to carry `node_id`/`signature`/`ts`/`v`/`pv`/`p`/`platform`/`nt`
   and sign (zero-sig → SHA256 → detached sign) before send; re-run `live_recon_unchoke`
   → expect acceptance + unchoke.
3. Then: live piece loop (request within `[min_piece, max_piece]`), `ace-swarm` → wire
   `ace-media` (MPEG-TS/HLS — built) + `ace-engine` (routes built) to it. Verify in VLC.

The engine listens for peers on **TCP 8621** (container IP `172.23.0.2`): connecting there
with our client + the current infohash makes it reply as a peer with its full signed
handshake — the ground-truth probe used to crack the above.

**Identity gate is now PASSED (notes 17–18).** Hooking the engine's
`crypto_sign_verify_detached` while our signed client connects shows `ret=0` with
`pk = our node_id` and `m = the digest the engine recomputed` — the official engine accepts
our minted identity. The engine-as-peer still closes us afterward, but that is **not**
identity rejection: in the current sandbox the channel is `status=prebuf, downloaded=0`
(the engine itself is pulling no data), so there is nothing to transact. **Validating live
unchoke + piece flow needs a live channel that is actually delivering data** (and likely
real swarm peers reachable from the host with WARP off) — an environmental gate, not a code
one. A VOD acestream id (static piece-hash list) would let the piece loop be validated
offline.

This step is RE-ish (a live experiment + binary RE), like the handshake/transport spikes —
not clean coding. A **VOD acestream id** would also help (static piece-hash list to
validate against; we've only had live fixtures).

## Environment & gotchas (HARD-WON — don't relearn these)
- **Cloudflare WARP breaks P2P** (drops inbound UDP, exits in-country). Keep it OFF for
  swarm testing. The Spain network itself does NOT block the swarm once WARP is off.
- **Engine sandbox:** the official closed engine runs in Docker for capture/RE:
  `docker compose -f re/sandbox/docker-compose.yml up -d acestream` (HTTP API on
  `127.0.0.1:6878`; `frida-tools` installed inside the container; SYS_PTRACE+seccomp
  unconfined so Frida attaches). `re/sandbox/` is git-ignored.
- **API quirks:** read `/ace/stat` fields under **`.response`** (not top-level — all
  top-level keys are null). Use **`content_id=`** for `acestream://` ids, not
  `infohash=`. A `status=prebuf, peers=0` forever = a **dead live channel** (the old
  docs' promo `685e…6067` is dead — don't use it).
- **Known-good LIVE test:** content_id `cid1`
  (resolve to the current infohash via `analyze_content` — it rotates).
- **`re/` is git-ignored and local-only** (NOT in version control): the extracted
  engine, Ghidra decompilation (`re/decompiled/ghidra/*.so.c`), Frida scripts +
  captures (`re/harness/`, `re/captures/`), the transport key file, the throwaway
  Python probes, and the interop spike. These persist on THIS machine but won't exist
  in a fresh clone elsewhere. The AES key/IV are already baked into `ace-wire` (committed),
  so the code doesn't depend on `re/`.
- **References:** original engine binaries + related repos are in `references/`
  (git-ignored). The Linux engine tarball + an Android APK + a Windows exe are there.

## Workflow notes
- This project followed the superpowers skills: brainstorm → writing-plans →
  subagent-driven-development. Plans are in `docs/superpowers/plans/`. The pattern that
  worked: do live recon/RE inline (controller), then hand clean, fully-specified TDD
  tasks to subagents; validate everything against captured ground-truth vectors in
  `tests/vectors/`.
- RE spikes (Frida/Ghidra) need the Docker engine running and a non-WARP network.
