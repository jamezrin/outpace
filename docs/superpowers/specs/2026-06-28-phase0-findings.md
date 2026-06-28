# Phase 0 — Findings & Go/No-Go (PRELIMINARY)

**Date:** 2026-06-28
**Status:** Preliminary — viability gate effectively passed; some RE depth remains.

## Decision: **GO** (high confidence)

The official engine runs on a normal machine, the network is reachable, the
protocol is a documented BitTorrent/BitTornado derivative, **public-swarm entry
requires no account/server-issued identity** (proven by identity-wipe test), and
the only hard reverse-engineering delta (the peer-link `Encrypter`) is bounded
and tractable.

### Encrypter handshake shape (from `live.so` symbols)
Custom scheme: peers exchange a **`pubkey`** (`pubkeyobj`) plus a **`vc`**
(verification constant), then encrypt the channel with **AES**
(`m2_AES_encrypt/decrypt`, `block_encrypt/decrypt`) with some `xor_encrypt` use.
This is a public-key handshake variant (NOT vanilla BitTorrent MSE). Exact byte
layout + key derivation = remaining capture work (Frida on `live.so`, Task 5b).

## The four unknowns

| # | Unknown | Status | Evidence |
|---|---|---|---|
| 1 | Signed identity required to join swarm? | **RESOLVED — NO** | Proven empirically: deleted the local identity (`device.key`), restarted with **no account/network registration** → engine **regenerated `device.key` locally** and **still streamed the public channel unauthenticated** (`check_auth.auth_level=null`, `status=dl`, peers 26, data flowing). `analytics.key` is just a telemetry UUID. The peer identity is locally generated and ephemeral; no acestream-issued/account key gates public-swarm entry. |
| 2 | Bootstrap/tracker endpoints & protocol | **RESOLVED** | UDP BitTorrent tracker `t1.torrentstream.org:2710` + Mainline DHT (bencode KRPC) + LSD multicast `239.255.17.18`. See `notes/03-discovery-and-transport.md`. |
| 3 | content_id → infohash → transport resolution | **PARTIAL** | content_id `f8b0…` resolves to infohash `47eda3…`; `TorrentDef.finalize()` computes SHA1 infohash (+md5+crc32). Exact SHA1 input bytes OPEN (Task 8). |
| 4 | Stream encryption usage & key origin | **PARTIAL** | Public channels tested are `is_encrypted=0` (no content DRM). The only encryption is the peer-link `Encrypter` (`m2_AES`/`xor_encrypt`/`block_encrypt`). Key origin: Frida (Task 5). |

## What is now known (high confidence)
- Engine = **Cython-compiled BitTornado fork**. Peer wire code in `live.so`
  (`Connecter`/`Encrypter`/`Downloader`/`Rerequester`/`handshake`).
- **Discovery is 100% standard BitTorrent** (tracker + DHT + LSD) — no bespoke
  protocol to reverse there; use existing BT libraries/specs.
- **Piece transfer is TCP**; pieces for public content are not DRM-encrypted.
- Environment works **from Spain without any VPN**; WARP actively breaks P2P.

## Remaining Phase-0 work (none are viability blockers)
- **Task 5 (Frida):** dump the `Encrypter` handshake + keys → close Unknown #1/#4
  definitively. This is the highest-value remaining RE item.
- **Task 8 (id math):** reproduce infohash/content_id from a transport file.
- **Task 9 (wire spec):** document handshake + message framing = BitTorrent baseline
  + the `Encrypter` delta.
- **Task 10 (interop):** fetch one piece with our own code (needs `Encrypter` solved).
- **Task 6 (legacy pyc):** DEMOTED — BitTornado upstream source is the reference.

## Effort read for Phases 1–6
- `ace-tracker` + DHT + discovery: mostly **library integration** (existing BT
  crates), low risk.
- `ace-wire`/`ace-peer`: BitTorrent baseline is straightforward; **the `Encrypter`
  handshake is the critical-path risk** — once cracked, the rest is conventional.
- `ace-media` (TS/HLS) + `ace-engine` (6878 API): conventional engineering.
- Net: viability is **proven**; remaining risk is concentrated in the `Encrypter`,
  not in whether the network can be joined at all.
