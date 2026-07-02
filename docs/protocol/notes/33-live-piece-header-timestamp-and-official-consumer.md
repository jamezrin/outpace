# 33 — Live piece header is a timestamp; official consumer accepts outpace pieces

Follow-up to note 32. The remaining blocker was "official requests outpace pieces but
does not accept/release them." That blocker is resolved: the 8-byte `id=7` live piece header
is a big-endian IEEE-754 `f64` Unix timestamp, constant across all chunks of a piece. Once
outpace preserves/generates that header instead of serving `[0u8;8]`, the official engine
accepts outpace-originated pieces and returns media bytes.

## Ground truth from the official source node

The local official `--stream-source-node` from note 25 was still running:

- descriptor: `/tmp/ace-source-node-pub/scproof.acelive`
- source peer: `172.23.0.3:7764`
- infohash: `cafe0b6789abcdef0123456789abcdef01234567`
- geometry: `piece_length=65536`, `chunk_length=16384`, `bitrate=8375`

The existing ignored recon harness connected directly to that source node, sent a signed
extended handshake, `Interested`, and `id=6` requests:

```bash
ACE_PEER=172.23.0.3:7764 \
ACE_INFOHASH=cafe0b6789abcdef0123456789abcdef01234567 \
ACE_PIECES=6 ACE_CHUNKS=4 ACE_DUMP=/tmp/source-node-recon-payloads.bin \
cargo test -p ace-peer live_recon_unchoke -- --ignored --nocapture
```

Captured official `id=7` piece headers:

| piece | header hex | decoded BE f64 Unix time |
|---:|---|---:|
| 110 | `41da91522634c2ee` | `1782925464.8243976` |
| 111 | `41da915228a7f55b` | `1782925474.6243503` |
| 112 | `41da91522b1b2805` | `1782925484.4243176` |
| 113 | `41da91522d8e590f` | `1782925494.2241857` |
| 114 | `41da9152300187f6` | `1782925504.0239234` |
| 115 | `41da915232818873` | `1782925514.0239532` |

The header is identical for chunks `0..3` of the same piece and advances by roughly one
piece duration between pieces. It is not a hash or checksum.

## Code changes

- `ace_wire::live_codec::LiveChunk` now exposes `piece_header`.
- `piece_header_from_unix_seconds(seconds)` encodes the header as `seconds.to_be_bytes()`.
- `PieceStore` stores one header per piece, evicts it with the piece, and replaces an earlier
  zero placeholder with a later nonzero header.
- Relay paths preserve upstream official headers:
  - `ace_provider` stores `LiveChunk.piece_header` with downloaded chunks.
  - reciprocal serving and `SeederSession::serve` reply with the store's per-piece header.
- Broadcast ingest generates a timestamp header for outpace-originated pieces and stores
  the same header for every chunk in that piece.

## Official consumer proof

Runtime setup:

- outpace: HTTP `0.0.0.0:6900`, inbound peer listener `0.0.0.0:8621`,
  `OUTPACE_ENABLE_INBOUND=1`, `OUTPACE_SEED_DEBUG=1`
- local UDP tracker: `0.0.0.0:7001`, returning compact peer `172.23.0.1:8621`
- patched descriptor: only `trackers` changed to `udp://172.23.0.1:7001/announce`
- broadcast name: `proof-header-1`
- body: `tests/vectors/media/h264-keyframes.ts` repeated 400 times (`6,768,000` bytes)
- infohash: `cafe026789abcdef0123456789abcdef01234567`

Official `/ace/getstream?url=http://172.23.0.1:6901/proof-header-1-patched.acelive`
returned the exact outpace infohash. The local tracker saw the official engine announce,
and outpace saw the expected peer-wire path:

```text
[seed-session] peer=172.23.0.2 advertise window min=0 max=102 position=102 distance=-1
[seed-session] <- ExtendedHandshake ... mi[min=-1 max=-1 pos=-1 dist=-1]
[seed-session] -> live Bitfield for 103 complete piece(s)
[seed-session] <- Interested
[seed-session] -> Unchoke
[seed-session] <- ACE Request stream=0 piece=100 chunk=0 bytes=10
[seed-session] -> Piece stream=0 piece=100 chunk=0 bytes=16384
...
[seed-session] <- ACE Request stream=0 piece=102 chunk=3 bytes=10
[seed-session] -> Piece stream=0 piece=102 chunk=3 bytes=16384
```

The decisive difference from note 32: the official stat endpoint moved to `status="dl"`
instead of staying `prebuf`, with `peers=1` and `downloaded=196608`:

```json
{
  "status": "dl",
  "peers": 1,
  "downloaded": 196608,
  "infohash": "cafe026789abcdef0123456789abcdef01234567",
  "is_live": 1
}
```

Following the official playback URL returned `HTTP 200` and `196228` bytes before the
35-second curl timeout. The output began on MPEG-TS sync bytes (`0x47` every 188 bytes at the
start), `ffprobe` identified H.264 video (`128x96`), and `ffmpeg -frames:v 1 -f null -`
exited successfully on the short capture.

## Current status

The reverse-direction official-engine-as-consumer path is now proven through discovery,
handshake, live bitfield, interest, unchoke, chunk requests, piece replies, official piece
acceptance, and HTTP media output.

Still open:

- ingest-resume continuity: piece numbering restarts at 0 on each new ingest task;
- transport persistence / `ut_metadata` serving for minted broadcasts;
- whether `enable_inbound` should default to true is now a product/exposure decision, not a
  known wire-compatibility blocker.
