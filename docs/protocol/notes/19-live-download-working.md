# 19 — LIVE PIECE DOWNLOAD WORKING (end-to-end from the real swarm)

**Milestone:** our from-scratch client connects to **real Acestream swarm peers**, gets
**unchoked**, requests pieces, and **receives live MPEG-TS data**. Phase 3's core data
path is proven against the live network.

Test channel: `acestream://cid2` (Synthetic Live Channel), infohash
`c123456789abcdef0123456789abcdef01234567`, swarm peers `203.0.113.10:8621`,
`84.122.160.176:8621`.

## What it took (the two remaining gates after the signature)

### 1. The FULL client handshake field set
A valid signature alone is **not** enough — peers also require the complete field set the
engine sends. Captured the engine's own outbound handshake (Frida hook on
`send`/`sendmsg`/`writev` filtering for `7:node_id`) and replicated it exactly:

```
ace_metadata_version, asn, asn_country, geoip_country, lsp(-1), m{ut_metadata},
mi{distance_from_source, down_rate, download_window_end, is_accessible, live_window_size,
   lsp, mam, max_piece, min_piece, peer_type, ping_from_source, position,
   time_from_source, top_session_up_rate, top_up_rate, up_rate, upload_rating},
node_id, nt(1), p(8621), platform(2), pv(2), signature, stream_statuses{}, ts,
tt("bt"), v(3021100), yourip(=peer IP, 4 raw bytes)
```

`yourip` = the peer's own IP (anti-spoof: prove we really reached them). Signature is over
the whole dict with `signature` zeroed (note 17). With this, both live peers send
`>>> UNCHOKE`.

### 2. The Acestream piece-request format (NOT standard BT)
Standard BT `Request` (id=6, 12-byte payload) gets us closed. Captured the engine's real
request via the `send` hook:

```
id=6, 10-byte payload:  [stream u32 = 0][piece u32][chunk u16]
```

i.e. per-CHUNK requests within a piece (chunk = 0,1,2,… up to chunks-per-piece). Sending
this, peers reply with **`Piece` (id=7)** messages: payload
`[stream u32=0][piece u32][block]`, block ≈ 16394 B = `[~10-byte header][16384-byte chunk]`.

### Verified it is real video
Dumped 4 chunks (65 576 B) and found **40/40** `0x47` MPEG-TS sync hits at 188-byte spacing
(alignment offset ~74 into the dump) → the payload is genuine MPEG-TS. (Exact chunk-header
size / cross-chunk TS reassembly is the last parsing refinement before piping to
`ace-media` → VLC.)

## Code changes
- `ace_wire::message`: `PeerMessage::Unknown { id, payload }`; decoder now tolerates
  unmodelled ids and length-variant standard ids (keeps them as `Unknown` instead of
  erroring) so Acestream's custom messages (ids 10, 11, 34, 36, …) don't tear down the
  read loop. `MAX_FRAME_LEN` still guards against abuse.
- `ace-peer` `live_recon_unchoke`: builds + signs the full handshake, requests pieces in
  Acestream format, dumps received blocks (`ACE_DUMP`).

## PROVEN: decodes to real 1080p video

Pulled 30 complete pieces (31 MB) from peer `84.122.160.176`, reassembled by `(piece,
chunk)` order (each chunk's 16384-byte payload, headers stripped), and **ffmpeg decoded an
actual frame**: `1920×1080` H.264 + AAC stereo 48 kHz (saved `shot.jpg`, a real Synthetic Live Channel
frame). `ffprobe` reports a valid TS program (H.264 video + AAC audio). End-to-end with a
from-scratch client, no closed-source blobs.

- chunks-per-piece = **64**, `piece_length = 1 MiB` (64 × 16384), `chunk_length = 16384`.
- Each piece is internally 100% TS-aligned. Across pieces the alignment drifts exactly
  **−4 bytes/piece** (≈96 bytes/piece don't raw-chain) → ~1 broken packet per piece
  boundary, which TS demuxers resync past (ffmpeg decodes fine with
  `-err_detect ignore_err -fflags +discardcorrupt`). Glitch-free continuity is a muxing
  refinement, not a blocker.

## Next (the last mile to VLC)
1. Pin the exact chunk-block header layout; strip it; reassemble contiguous MPEG-TS
   (`PieceReassembler`).
2. Promote the full handshake + request/piece codec from the recon into `ace-wire` /
   `ace-swarm` proper (currently inline in the recon test).
3. Pipe reassembled TS through `ace-media` and serve via `ace-engine` `/ace/getstream`;
   confirm playback in VLC.
