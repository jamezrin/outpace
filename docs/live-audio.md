# Live audio normalization

Live HLS uses its existing native selection by default: the first ADTS AAC
track wins; without ADTS AAC, audio passes through unchanged. Live TS stays raw
by default, and VOD byte ranges are unaffected by either audio option.

| Setting | Default | Effect |
| --- | --- | --- |
| `OUTPACE_HLS_AUDIO_NORMALIZE` | `false` | Convert selected audio for live HLS only. |
| `OUTPACE_LIVE_AUDIO_NORMALIZE` | `false` | Normalize one shared live source before TS, HLS and support fanout. |
| `OUTPACE_FFMPEG_PATH` | `ffmpeg` | Direct executable path or name resolved through PATH once at startup. |

Both boolean settings accept `true`/`false` or `1`/`0`. The fixed conversion target
is selected AC3 or MPEG Layer II/III to AAC-LC stereo, 48 kHz, 128 kbit/s. All
viewers of a shared source receive the same operator-selected representation;
there is no per-viewer codec or language override. When both switches are enabled,
HLS consumes the normalized TS without a second classifier or conversion child.
With both disabled, FFmpeg is never looked up or probed. Native ADTS AAC and
video-only streams start no conversion child; AAC passthrough does not resample.
The TS passthrough case retains the program's other tracks; HLS keeps its native
track selection. This is not a per-viewer track or language-selection API.

Enabled mode requires unencrypted 188-byte TS, one program, one MPEG-2/H.264/HEVC
video stream, valid PCR and current CRC-valid single-packet PAT/PMT sections.
Audio-only, multiple video/programs, fragmented PSI, unidentified private streams
and unsupported selected codecs fail closed. MPEG PMT types do not identify a
layer: bounded elementary-stream frame checks admit Layers II/III and reject
Layer I, malformed, free-format or unaligned MPEG framing. LATM, EAC3 and DTS
cannot be selected. The first ADTS AAC wins, otherwise the first recognized
MPEG audio or AC3 wins; this does not search for a different track after rejection.
Enabled strictness can reject inputs that disabled mode historically passes through.

Conversion copies video without encoding it; player compatibility (including
Safari) is not guaranteed. Stereo conversion can discard surround channels and
lossy re-encoding reduces quality. At most four conversion children run globally;
this is a process bound, not an RSS/CPU capacity promise. Recovery retains completed
segments but playlists wait for fresh-generation readiness. A process, codec,
resource or output failure is terminal for that session. HLS returns HTTP 503 with
`Retry-After: 1`; this does not restart a failed worker. TS responses wait for
validated tables and a clean video opening before headers. Unsupported layouts
return 422, child-capacity exhaustion 503 with `Retry-After: 1`, timeouts 504 and
other conversion failures 502. After headers, failure ends the body with an I/O
error and remains visible in native stream status. Stop/idle teardown and a fresh
session are required. Authorization and expiry still take precedence.

Shared TS startup and recovery use `OUTPACE_HLS_STARTUP_TIMEOUT_MS` (default
45,000 ms). A recovery during startup keeps the original deadline; a ready source
gets a new recovery deadline. Three invalidations per minute are allowed. Old
queued output is fenced before replacement. Input/replay/writer storage shares
one 1 MiB budget; output has four slots with chunks below 32 KiB, plus fixed
stdout/framing and existing provider, fanout and client buffers. Full output
backpressures the worker; cancellation still collects owned work. Native status
counters describe the upstream source, not transformed byte counts.

Operators supply and maintain FFmpeg with MPEG-TS mux/demux, native AAC encoding,
MP2/MP3/AC3 decoding and pipe support. Startup checks capabilities and performs a
bounded AC3 conversion before opening listeners. Enabled containers need an
operator-built image containing this binary; the base Docker/compose setup does
not install it. Wrappers launching descendants are unsupported. Embedders must
await manager shutdown before destroying the runtime so children and pumps are
reaped; Drop only signals cancellation.

Synthetic codec acceptance is optional locally: run
`OUTPACE_TEST_HLS_AUDIO_FFMPEG=1 cargo test -p ace-engine --test hls_audio_ffmpeg -- --nocapture`.
Without opt-in it prints an explicit skip. Opt-in requires FFmpeg/ffprobe,
synthetic video/sine inputs and AC3/MP2/MP3/AAC encoders; a missing prerequisite
fails the suite. The designated CI job always opts in. No public media is used.

The additional shared-TS acceptance test is ignored by default. With a direct
FFmpeg binary and FFmpeg/ffprobe available, run:

```sh
OUTPACE_TEST_HLS_AUDIO_FFMPEG=1 OUTPACE_TEST_NORMALIZER_FFMPEG_PATH=/usr/bin/ffmpeg \
  cargo test -p ace-engine --test hls_audio_ffmpeg \
  shared_live_ts_normalizes_ac3_and_mp2_for_two_viewers_and_native_hls -- --ignored --exact
```

Adjust the binary path for your installation. This generates AC3/MP2 media and
checks decoded AAC, copied video and service metadata through two TS viewers and
native HLS; it does not qualify visual playback quality or every player.
