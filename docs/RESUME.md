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
| 3 piece download → media → engine | 🔜 IN PROGRESS | transport decoder (3.1); **deterministic domain logic built** (live picker, piece reassembler, ace-media TS/HLS, ace-engine routes); **blocked on live piece bytes** = node-identity preimage |

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

1. **Finish the preimage (last mile) — only unknown left = the exact bytes signed.**
   Ruled out (don't repeat): brute force (node_id/infohash/ts, all encodings), bencode-dict
   hypotheses, and hooking `_sodium crypto_sign*` + OpenSSL `EVP_DigestSign`/SHA-512 — none
   matched / none fired during handshake builds. `live.so` has **no** ed25519/sha512
   constants. Conclusion: the signature is computed **at engine startup / on a timer and
   cached** (peers just get the cached value), so every attach-after-boot hook missed it.
   CONCLUSIVE (note 16): a 400s window with hooks *definitively* installed + triggers caught
   ZERO crypto calls — the signature is computed **once at engine startup** (engine-global
   identity); it only changed across my restarts. **Capture procedure (disruptive):** stop
   the running engine, then `frida.spawn` a clean instance with `env LD_LIBRARY_PATH=/app/lib`
   and the lazy per-module hook (`scratchpad/spawn_driver2.py` + `hook_lazy.js`), resume, and
   read `crypto_sign`/`EVP_DigestSign` `m`/`mlen` at startup = the preimage. (A port-conflicting
   spawn alongside the running engine does NOT reach the sign — confirmed.) Ghidra scripts in
   `tools/ghidra/`.
2. **Mint + sign:** new `ace-identity` (Ed25519) + extend `OutgoingExtendedHandshake` to
   carry `node_id`/`signature`/`ts`/`v`/`pv`/`p`/`platform`/`nt`; re-run `live_recon_unchoke`
   → expect acceptance + unchoke.
3. Then: live piece loop (request within `[min_piece, max_piece]`), `ace-swarm` →
   `ace-media` (MPEG-TS) → `ace-engine` (`:6878` `/ace/getstream`). Verify in VLC.

The engine listens for peers on **TCP 8621** (container IP `172.23.0.2`): connecting there
with our client + the current infohash makes it reply as a peer with its full signed
handshake — the ground-truth probe used to crack the above.

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
