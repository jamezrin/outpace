# 50 - Live robustness soak on the healthy public target

Date: 2026-07-02

## Target

Content id:

```text
cid3
```

`/ace/getstream?content_id=...` resolved it through the catalog path to:

```text
d123456789abcdef0123456789abcdef01234567
```

The daemon was run with isolated state:

```sh
OUTPACE_BIND=127.0.0.1:6900 \
OUTPACE_DATA_DIR=/tmp/outpace-soak/data \
cargo run -p ace-engine --bin outpace
```

## Short pre-soak probe

A short probe before the main soak produced media bytes and exercised the note-48 retransmit
path once:

```text
16:52:02.499 [ace] re-requesting 1 piece(s) outstanding > 4s (from 7430365)
```

The same session continued serving and recovered without a pool teardown.

## Main 180-second soak

Command:

```sh
curl -v --max-time 180 \
  -o /tmp/outpace-soak/capture180.ts \
  http://127.0.0.1:6900/ace/r/d123456789abcdef0123456789abcdef01234567/outpace
```

Result:

- `HTTP 200 OK`, `content-type: video/mp2t`.
- Ended by the intentional curl timeout after `180.002 s`.
- Downloaded `157,092,800` bytes.
- MPEG-TS packet alignment: `157,092,800 % 188 == 0`.
- Total TS packets: `835,600`.

Daemon evidence:

- Initial discovery found 23 peers.
- Initial upstream pool connected 3 handshaked peers:
  `5.231.25.139:10026`, `87.217.156.180:8621`, `37.11.110.121:8621`.
- PEX from `5.231.25.139:10026` advertised 3 new peers.
- `90.173.16.56:8621` was added to the active upstream pool.
- Background discovery found 59 peers and added 36 candidates.
- The source-node peer was lost once and replaced without a visible stall.
- Progress markers advanced steadily through `served 156 MiB`.

Event counts across the full daemon log for this run window plus the short probe:

```text
re-requesting=1
skip_evicted=0
pool_stale=0
peer_exchange=2
upstream_peer_lost=2
pool_added=4
```

During the main 180-second soak specifically, there was no `re-requesting`, no pool-stale
teardown, and no evicted-gap skip.

## Media checks

`ffprobe` identified:

- Video: H.264, PID `0x100`, 1920x1080, 25 fps.
- Audio: AAC, PID `0x101`, 48 kHz.

PTS scan over video frames:

```text
frames=4732
backward=0
gaps_gt_0_1=1
min_gap=0.040000
max_gap=0.160000
```

`ffmpeg -v warning -map 0:v:0 -f null -` decoded the capture. Warnings were limited to the
expected mid-stream join/startup SPS/PPS misses plus one later H.264 macroblock decode error:

```text
cabac decode of qscale diff failed at 41 67
error while decoding MB 41 67, bytestream -7
```

The MPEG-TS continuity-counter scan was clean:

```text
pid=0x0   packets=19886  payloads=19886  cc_disc=0 cc_dups=0 tei=0
pid=0x100 packets=774951 payloads=774951 cc_disc=0 cc_dups=0 tei=0
pid=0x101 packets=16908  payloads=16908  cc_disc=0 cc_dups=0 tei=0
```

## Conclusion

This target is healthy enough for outpace robustness work. The 180-second soak showed
continuous forward progress, clean TS continuity counters, no backward PTS, and no pool-stale
teardown. PEX and background discovery both supplied peers, and a source-node loss recovered
without interrupting the output stream.

Remaining useful validation:

- Longer VLC/user-player soak, because this run was curl/ffmpeg-based.
- Capture a true field stall where `Continuity::skip_evicted_gap` fires; this path is still
  unit-tested but not live-observed.
- Parse PEX record live-window fields and prefer peers that already cover `next_needed`.
