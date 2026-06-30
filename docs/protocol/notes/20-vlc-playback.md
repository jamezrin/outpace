# 20 — Live end-to-end playback through the outpace daemon (Task 20)

**Date:** 2026-06-30. **Network:** Spain, **WARP off** (mandatory — see RESUME gotchas).
**Daemon:** `cargo run -p ace-engine --bin outpace` on `127.0.0.1:696x`.
**Test id:** content-id `cid1` (RESUME known-good live).

This is the spec's deliverable: real live video pulled from the Acestream swarm and served
through the clean daemon API, verified end-to-end. All steps were run against the **live
swarm** from this host.

## Result: ✅ live video served end-to-end

### Discovery → connect → download (autonomous, just an id)
```
[dht] seeded 4 bootstrap node(s)
[ace] open cid1: discovered 25 peer(s)
[ace] 85.87.156.75:8621: connected + handshaked
[ace] 85.87.156.75:8621: window min=14718220 max=14718269 -> start=14718261 head=14718269
[ace] 85.87.156.75:8621: UNCHOKE -> requesting pieces 14718261..=14718269
```
Pulled **8.3 MB of live MPEG-TS** from `GET /streams/ace/cid1.ts` (188-aligned; first
bytes `47 40 00 10`). curl exits 124 (timeout) because the live stream is unbounded — expected.

### Decode proves real video through the daemon (plan Task 20, Step 3)
```
$ ffprobe … direct.ts
codec_name=h264  codec_type=video  width=1280 height=720
codec_name=aac   codec_type=audio  sample_rate=48000
$ ffmpeg … -frames:v 1 shot.jpg   →   mjpeg 1280x720 (45 KB) written
```
A real **1280×720 H.264 + 48 kHz AAC** stream; a frame decodes cleanly. (The leading
`non-existing SPS/PPS 0` / `no frame!` lines are the normal pre-lock transient; the decoder
recovers and writes the 720p frame.)

### Start-on-keyframe verified live (`KeyframeGate`)
The first **served** video frame is a keyframe:
```
$ ffprobe -select_streams v -show_entries frame=key_frame,pict_type … direct.ts
1,I
0,B
0,P
```
The per-client gate begins the stream on an I-frame instead of mid-GOP. ✓

### Multi-client / acexy parity (plan Task 20, Step 4)
Two concurrent clients on the same id → **one shared session**:
```
$ curl …/streams
{"streams":[{"clients":2,"id":"cid1","network":"ace"}]}
$ curl …/streams/ace/cid1/status
{"clients":2,"network":"ace","id":"cid1", …}
```
One download fanned out to both clients — no proxy/wrapper needed. ✓

## Note on content-id resolution (spec Risk #1, now characterized)
The `cid:<40hex>` ut_metadata path discovers metadata peers (25 found) and the peers **accept
the content-id as the BT swarm key** and advertise the `ut_metadata` extension — but they send
**no `metadata_size`** in the extended handshake, so the `AceStreamTransport` blob is *not*
served via vanilla BEP-9 `metadata_size`. Crucially, the content-id **works directly as the
swarm key for the live data**: the bare-id (infohash-direct) path handshakes with the same
peers, gets UNCHOKE, and downloads the stream — which is what playback uses here. So live
playback does not depend on the transport fetch. Pinning exactly how the engine obtains the
transport descriptor for a content-id (a non-`metadata_size` channel) remains a small live-RE
follow-up; it does not block playback.

## What a human still does manually
Opening the URL in the **VLC GUI** and watching it render is a visual confirmation a headless
agent can't perform; the programmatic equivalents above (decode a 720p frame from the daemon's
live output + two-client shared-session check) all pass against the live swarm. To watch it:
```sh
OUTPACE_BIND=127.0.0.1:6900 cargo run -p ace-engine --bin outpace
vlc http://127.0.0.1:6900/streams/ace/cid1.ts
```
(The id rotates; if dead, pick a current live content-id.)
