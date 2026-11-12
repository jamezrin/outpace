# HLS Output

Outpace packages live streams into HLS itself. No FFmpeg is needed for this. A player opens
`/streams/ace/<id>.m3u8` (or the compatibility `/ace/manifest.m3u8`), and Outpace cuts the
incoming MPEG-TS into segments, keeps a sliding window of them and serves a live playlist.

All HLS viewers of one source share one packager, and that packager shares the download with
any direct MPEG-TS viewers.

## When to use HLS instead of MPEG-TS

| Use HLS when... | Use direct MPEG-TS when... |
| --- | --- |
| the player or device only does HLS (browsers, many TVs, Apple devices) | the player handles a continuous stream (VLC, mpv, Jellyfin, ffmpeg) |
| clients reconnect often, since HLS tolerates short drops | you want the lowest delay to the live edge |
| you need [timeshift](Timeshift) | you are recording or piping to another tool |

## How segments are made

- Segments target `OUTPACE_HLS_SEGMENT_DURATION_MS` (default 5 seconds) on the stream's own
  clock, and cut on the **next keyframe** after that. H.264 and HEVC keyframes are detected.
  Segments are therefore usually a little longer than the target, and each one starts where a
  player can begin decoding.
- A segment can never exceed `OUTPACE_HLS_SEGMENT_PACKETS` 188-byte packets (default 65536,
  about 12.3 MB). This is a memory safety limit. A stream that hits it gets a forced cut that
  doesn't start on a keyframe. Raise the limit only for very high bitrates, such as 4K HEVC.
- Streams without usable timing are cut by packet count instead.
- The playlist keeps the last `OUTPACE_HLS_WINDOW_SEGMENTS` (default 8) segments. Older segments
  return `404`. Worst-case memory per HLS source is about (window + 1) × the segment ceiling.

## Startup

A new playlist request waits until `OUTPACE_HLS_STARTUP_SEGMENTS` (default 6) segments are
complete, so the player starts with a healthy buffer. With the defaults that is about 30 seconds
of media after the swarm starts delivering.

- If that doesn't happen within `OUTPACE_HLS_STARTUP_TIMEOUT_MS` (default 45 seconds), the
  request returns **`503` with `Retry-After: 1`**. Most players retry on their own.
- The playlist's `EXT-X-START` points near the beginning of the startup window. Players may
  choose a different position.
- `0` startup segments behaves as `1`.

For faster starts at the cost of resilience:

```bash
export OUTPACE_HLS_STARTUP_SEGMENTS=2
export OUTPACE_HLS_SEGMENT_DURATION_MS=4000
```

Then start (or restart) Outpace from the same shell. With `docker run`, pass each setting
as `-e NAME=value`; with Compose, see [Deployment](Deployment#passing-other-settings).

Constraints the daemon checks at startup:

- startup segments ≤ window segments;
- startup timeout ≥ segment duration;
- segment ceiling ≥ 3 packets;
- the whole window must fit the platform's memory limit (relevant on 32-bit).

The [startup prebuffer](Configuration#playback-and-startup-buffering) applies to the shared
live source, so HLS goes through it as well. When the prebuffer releases, its queued media
arrives in one burst, and several segments complete almost at once. With the defaults, an HLS
start is roughly discovery time plus up to 15 seconds of prebuffering. Setting
`OUTPACE_PREBUFFER_MS=0` removes the prebuffer stage, and the startup segment count alone then
decides how long HLS waits.

## Recovery and errors

| Situation | Playlist response |
| --- | --- |
| Still starting, before the timeout | The request waits. |
| Not ready at the timeout | `503`, `Retry-After: 1` |
| Recovering from a source gap | The playlist waits for fresh segments after the gap; completed segments stay available. |
| Packaging failed (for example audio conversion failed) | `503`, `Retry-After: 1`, until the stream is stopped |
| Source ended | Closed playlist with `#EXT-X-ENDLIST` (see below) |

Segment requests never start a stream. A segment that isn't produced yet, or has left the
window, returns `404`.

## When a source ends

> **Unreleased:** this behaviour is in the `edge` image and builds from `main`. On v0.9.0 an
> ended stream keeps answering `503` for as long as a player keeps polling it.

When a live source stops (the broadcaster ends, or the swarm reaches the end):

- the completed segments stay available;
- the playlist ends with `#EXT-X-ENDLIST`, so players play out what they have and stop instead
  of polling forever;
- the unfinished last segment is dropped;
- about 30 seconds after the last request the stream is cleaned up. The next request to the URL
  starts a fresh session.

For [support sources](Support-Sources-and-Operations), which restart on their own, HLS clients
must reopen the playlist URL to join the new run.

## Audio normalisation

By default Outpace doesn't touch audio. HLS takes the first AAC (ADTS) audio track; streams
without one keep their audio as it is. Many streams carry AC3 or MPEG audio (MP2/MP3), which
browsers, Apple devices and many TVs can't play over HLS. Normalisation fixes that.

| Setting | Converts | Avail. |
| --- | --- | --- |
| `OUTPACE_HLS_AUDIO_NORMALIZE=1` | Live **HLS** only. Direct MPEG-TS stays untouched. | all |
| `OUTPACE_LIVE_AUDIO_NORMALIZE=1` | The shared live source, once, before it goes to MPEG-TS viewers, HLS and support sources. | edge |

With both on, only the shared conversion runs. VOD is never converted, whichever switch is on.

### What conversion does

- Selected AC3, MP2 or MP3 audio becomes **AAC-LC stereo, 48 kHz, 128 kbit/s**.
- Video is copied, never re-encoded.
- AAC audio passes through without conversion, and video-only streams start no converter.
- Everyone watching a source gets the same converted audio. There is no per-viewer track or
  language choice.
- Stereo downmix loses surround channels, and re-encoding costs some quality.
- At most **four** conversions run at once across the daemon.

### What it requires

- FFmpeg installed (see [Installation](Installation#ffmpeg)), and `OUTPACE_FFMPEG_PATH` if it
  isn't on `PATH`. Outpace tests it at startup and refuses to start if the test fails.
- An unencrypted 188-byte MPEG-TS stream with:
  - one program;
  - one MPEG-2, H.264 or HEVC video stream;
  - valid PCR timing;
  - simple (single-packet) PAT/PMT tables.

Audio-only streams, multiple programs or video streams, fragmented tables, MPEG Layer I, LATM,
E-AC3 and DTS are rejected. Turning normalisation on can therefore refuse streams that play fine
with it off.

### Errors with normalisation on

| Response | Meaning |
| --- | --- |
| `422` | The stream's layout isn't supported for conversion. |
| `503` + `Retry-After: 1` | All four conversion slots are busy. |
| `504` | The conversion didn't become ready within `OUTPACE_HLS_STARTUP_TIMEOUT_MS`. |
| `502` | The conversion failed. |
| Body ends with an error | A failure after playback started (direct TS). |

A conversion failure is final for that session. HLS keeps answering `503` and nothing retries
on its own. Stop the stream (`DELETE /streams/ace/<id>`) or let it go idle, and the next request
starts fresh. The stream's `/status` shows the state under `normalization`, for example
`{"generation":2,"state":"failed","failure":"child_exit"}`.

## Compatibility HLS

`/ace/manifest.m3u8` uses the same packager and settings as `/streams/.../.m3u8`. It adds
tokenised playlist and segment URLs (`/ace/m/...`, `/ace/c/...`) tied to a
[lease](Playing-Streams#leases), and [timeshift](Timeshift) controls. Segment URLs in its
playlists are absolute and built from the request's `Host` header.

Native playlists use root-relative paths (`/streams/ace/<id>/seg/<n>.ts`). They work through a
reverse proxy on the same host name, but not under a sub-path.

## Related pages

- [Timeshift](Timeshift): rewind HLS viewers.
- [Managed HLS Origins](Managed-HLS-Origins): descriptors that already point at HLS.
- [Configuration](Configuration#hls): all HLS settings.
