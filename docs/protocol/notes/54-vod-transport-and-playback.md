# 54 — VOD transport and playback (single-file)

Implements issue #47 (child of parity epic #46).

## Key finding

**VOD is vanilla BitTorrent.** Per `wire-protocol.md`, a VOD swarm uses the standard
peer-wire messages — `Bitfield`/`Have`/`Request{index,begin,length}`/`Piece{index,begin,block}`
(all already modeled in `ace_wire::message::PeerMessage`) — and piece integrity is the
transport descriptor's `pieces` SHA1 list. This is unlike the **live** path, which reuses those
same message IDs with custom `[stream u32]` payloads plus an 8-byte per-piece header
(`live_codec`) and verifies pieces with in-band RSA signatures.

Consequently the VOD path is fully parallel to live and shares only the low-level
connect/handshake and `PieceStore` primitives. It is deterministically testable offline against
a local mock BitTorrent seeder — no live swarm required.

## What was added

- `ace-wire` `transport.rs`: `TransportDescriptor::vod_total_length()` (single-file `length`)
  and `is_multifile()` (`files` list).
- `ace-swarm`:
  - `types::VodInfo` (infohash, geometry, `piece_hashes`, `total_length`).
  - `resolve::vod_info_from_transport` (rejects live / multi-file / missing-length), plus
    byte-returning resolver variants (`catalog_transport_bytes`, `transport_bytes_via_peer`,
    `transport_bytes_from_url`) so VOD reuses the existing catalog/peer/url fetch paths.
  - `vod::verify_piece` (SHA1) and `vod::download_vod`: sequential, verified, single-file
    download loop. Pulls pieces in order from one peer at a time, re-requesting from the next
    peer on any failure, and **never emits a piece whose SHA1 does not match**.
- `ace-engine`:
  - `provider::VodByteSource` trait; `StreamProvider::open_vod` (default: unsupported) overridden
    by `AceProvider`.
  - `GET /vod/:network/:id` — streams verified bytes with a `Content-Length`.
  - `outpace play --vod <target>` — verified VOD download to stdout.

## Scope and follow-ups

Single-file only; multi-file is rejected with a clear error. Deliberately deferred:
VOD HLS packaging, HTTP byte-range/seek, multi-peer rarest-first VOD scheduling, reseeding
downloaded VOD pieces via `SeedRegistry`, and live-swarm validation against a real public VOD
transport (which would also confirm/adjust the synthesized `length`/`files` schema — see
`transport-file.md`).

## Verification

`cargo test` across the workspace is green. The authoritative VOD test is the mock-seeder
integration test in `ace-swarm/src/vod.rs` (`downloads_and_verifies_single_file_vod`,
`tampered_piece_is_rejected`). Live-network verification is left to the operator, matching how
the live path is validated (`#[ignore]`d live tests).
