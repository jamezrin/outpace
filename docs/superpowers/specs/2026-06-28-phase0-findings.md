# Phase 0 — Findings & Go/No-Go (FINAL)

**Date:** 2026-06-28
**Status:** FINAL — all Phase-0 tasks complete; viability proven by live interop.

## Decision: **GO** (proven)

Demonstrated end-to-end: the protocol is a documented BitTorrent/BitTornado
derivative, **public-swarm entry needs no account/server identity** (identity-wipe
test), the public peer link is **plaintext** (no encryption to crack), and — the
capstone — an **independent from-scratch client was accepted by the real swarm**.

### Capstone proof (Task 10, independently reproduced)
A hand-built `AceStreamProtocol` handshake (no Acestream code, no blobs) was sent
to live swarm peers for infohash `50e935…2d6e47`:
- Subagent's Rust client: accepted by `82.213.234.240:8623` and `188.171.2.171:8621`.
- **Controller's independent Python probe: 7/8 connectable peers accepted it**, each
  replying with the `AceStreamProtocol` handshake + a BEP-10 extended handshake
  (`d20:ace_metadata…`). This confirms an independent client can join the swarm.
- Not done (expected): downloading a live piece as a fresh, unproven, choked leecher
  on a sliding-window live stream — that's normal BitTorrent leecher behaviour, not a
  protocol blocker. VOD piece-fetch is a Phase-2 milestone.

### Peer handshake (CAPTURED — public content is plaintext)
Public (`is_encrypted=0`) peers speak **plaintext BitTorrent** with a custom pstr:
`0x11 "AceStreamProtocol" + 8 reserved + infohash(20) + peer_id(20)`, peer_id
`R30------`+random (ephemeral), then a BEP-10 extended handshake carrying
Acestream live metadata (`ut_metadata`, `distance_from_source`, `down_rate`, …).
**No encryption to crack for the in-scope case.** The `pubkey`/`vc`/AES `Encrypter`
applies only to premium `is_encrypted=1` content (out of scope). See `notes/05-crypto.md`.

## The four unknowns

| # | Unknown | Status | Evidence |
|---|---|---|---|
| 1 | Signed identity required to join swarm? | **RESOLVED — NO** | Proven empirically: deleted the local identity (`device.key`), restarted with **no account/network registration** → engine **regenerated `device.key` locally** and **still streamed the public channel unauthenticated** (`check_auth.auth_level=null`, `status=dl`, peers 26, data flowing). `analytics.key` is just a telemetry UUID. The peer identity is locally generated and ephemeral; no acestream-issued/account key gates public-swarm entry. |
| 2 | Bootstrap/tracker endpoints & protocol | **RESOLVED** | UDP BitTorrent tracker `t1.torrentstream.org:2710` + Mainline DHT (bencode KRPC) + LSD multicast `239.255.17.18`. See `notes/03-discovery-and-transport.md`. |
| 3 | content_id → infohash → transport resolution | **MOSTLY RESOLVED** | **infohash = SHA1(whole transport file)** — validated byte-for-byte against engine ground truth via a Rust harness (2 vectors, `notes/08-idmath-validation.md`, `transport-file.md`). content_id↔infohash resolves via the engine API; native content_id derivation + inner transport-body layout remain OPEN (non-blocking — engine API resolves today). |
| 4 | Stream encryption usage & key origin | **RESOLVED** | Frida socket capture proves the peer link is **plaintext BitTorrent** for `is_encrypted=0` (public) content: handshake `0x11 "AceStreamProtocol" + reserved + infohash + peer_id`, then plaintext BEP-10 extended messages. The AES `Encrypter` is only used for premium `is_encrypted=1` (out of scope). See `notes/05-crypto.md`. |

## What is now known (high confidence)
- Engine = **Cython-compiled BitTornado fork**. Peer wire code in `live.so`
  (`Connecter`/`Encrypter`/`Downloader`/`Rerequester`/`handshake`).
- **Discovery is 100% standard BitTorrent** (tracker + DHT + LSD) — no bespoke
  protocol to reverse there; use existing BT libraries/specs.
- **Piece transfer is TCP**; pieces for public content are not DRM-encrypted.
- Environment works **from Spain without any VPN**; WARP actively breaks P2P.

## Phase-0 task status — COMPLETE
- ✅ 0 Inventory · 1 Sandbox · 2 Live stream · 3 Capture · 4 Bootstrap · 5 Handshake
  (Frida) · 7 Ghidra · 8 Id-math (Rust, validated) · 9 Wire spec · 10 Interop (proven)
  · 11 This memo.
- ❌ 6 Legacy pyc — dropped (BitTornado upstream source supersedes it).

## Carry-forward OPEN items for Phase 1+ (none block viability)
- Inner transport-body layout: piece length, piece SHA1 list, file list, live params
  (needed for `ace-wire` piece verification + `ut_metadata`).
- Native `content_id` derivation (engine API resolves it today).
- Acestream live-extension message semantics beyond the handshake (`mi` metrics,
  live piece picker) — for `ace-swarm` live mode.
- Premium `is_encrypted=1` AES `Encrypter` — intentionally out of scope.

## Effort read for Phases 1–6
- `ace-tracker` + DHT + discovery: mostly **library integration** (existing BT
  crates), low risk.
- `ace-wire`/`ace-peer`: BitTorrent baseline is straightforward; **the `Encrypter`
  handshake is the critical-path risk** — once cracked, the rest is conventional.
- `ace-media` (TS/HLS) + `ace-engine` (6878 API): conventional engineering.
- Net: viability is **proven**; remaining risk is concentrated in the `Encrypter`,
  not in whether the network can be joined at all.
