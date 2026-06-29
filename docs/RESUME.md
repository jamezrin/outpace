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
| 3 piece download → media → engine | 🔜 CORE PROVEN | **live HD video downloaded from the real swarm** (note 19: full signed handshake → UNCHOKE → Acestream piece requests → MPEG-TS → ffmpeg decodes 1920×1080 H.264+AAC). Remaining: promote inline recon protocol into `ace-wire`/`ace-swarm`, glitch-free cross-piece muxing, wire `ace-media`+`ace-engine`, point VLC at it |

Crates: `crates/{ace-wire,ace-tracker,ace-peer,ace-media,ace-engine}`. Workspace root `Cargo.toml`.
**Pure Phase 3/4 logic done (no live data needed):** `ace_wire::live` (LiveWindow/LivePicker),
`ace_wire::reassembly` (PieceReassembler: chunks→ordered bytes), `ace_media::{mpegts,hls}`
(TS align + HLS segment/manifest), `ace_engine::routes` (6878 URL surface). What's left needs
the live byte path: peer download loop / `ace-swarm`, then wire `ace-media`+`ace-engine` to it.

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
