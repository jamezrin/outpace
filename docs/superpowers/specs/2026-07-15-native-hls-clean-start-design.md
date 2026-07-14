# Native HLS Clean-Start Design

## Problem

A cold native HLS request currently returns `200 OK` before the packager has completed any media
segment. The response is a syntactically valid but empty media playlist. Some players treat that
as terminal and never reload it.

The packager also begins its first segment at whichever TS packet arrives first and chooses later
boundaries from transport random-access flags without first identifying the video PID. It does not
place cached PAT and PMT packets before the video access point at each segment boundary. On the
reported live stream, FFmpeg repeatedly reports `non-existing PPS 0 referenced` for the first HLS
segment while the direct TS endpoint from the same session decodes immediately. A cold
reproduction returned an empty 73-byte playlist, then exposed segment zero roughly 0.8 seconds
later; that segment was not independently decodable.

The `icy-name` response header is present in FFmpeg's parsed HLS metadata and is not the cause.

## Goals

- Never return a successful native media playlist with zero completed segments.
- Make the first retained segment and every later segment start at a detected video access point.
- Put the latest PAT and PMT packets before that access point so a client joining any retained
  segment can identify and decode the video immediately.
- Reuse one MPEG-TS access-point implementation for direct TS and HLS instead of allowing their
  PAT/PMT, video-PID, and keyframe rules to diverge.
- Preserve the existing HLS URLs, `icy-name` response metadata, segment duration target, memory
  ceiling, activity lease, compatibility token pins, and no-transcoding behavior.

## Non-goals

- Transcoding, codec conversion, elementary-stream rewriting, or adaptive-bitrate output.
- Synthesizing SPS/PPS NAL units or changing encoded audio/video payloads.
- Advertising `#EXT-X-INDEPENDENT-SEGMENTS` until all supported codecs have equivalent verified
  access-point handling.
- Changing VOD HLS byte-range behavior.

## Design

### Shared MPEG-TS access-point state

`ace-media` will expose a small stateful MPEG-TS access-point component extracted from the logic
already used by `KeyframeGate`. It will consume aligned 188-byte packets, cache the most recent PAT
and PMT, learn the H.264 video PID from the PMT, and report whether a packet is a video access
point. The access-point rule remains a video-PID packet with PUSI and either the adaptation-field
random-access indicator or an Annex-B H.264 IDR/SPS NAL.

`KeyframeGate` will use this component without changing its public behavior. `HlsPackager` will use
the same component to avoid treating random-access flags on unrelated PIDs as video boundaries and
to obtain the cached PAT/PMT prefix.

### Segment construction

Before the first segment, HLS will discard the partial startup GOP until the shared component finds
a video access point with PAT/PMT context. Each timed segment cut will occur immediately before a
later detected video access point. The new segment will start with cloned cached PAT and PMT TS
packets followed by the original access-point packet and subsequent unmodified TS packets.

The cached table packets count against the existing per-segment byte ceiling. PCR timing remains
the media clock, and the existing twice-target and hard-packet fallbacks remain bounded escape
paths for malformed streams. A fallback segment is not advertised as independently decodable.
Transport discontinuities clear partial media and access-point/table state, then require a new
clean start before media is published again.

This is packet-level HLS packaging only: encoded payloads are neither transcoded nor rewritten.

### Playlist readiness

`HlsPackager` will notify waiters when its first completed segment becomes available. The native
`.m3u8` handler will wait for readiness before rendering its first successful playlist response.
The wait is bounded. If no segment is produced within the startup deadline, the handler returns
`503 Service Unavailable` with `Retry-After: 1` rather than `200` with an empty playlist. A later
request can reuse the still-running packager and succeed once media is ready.

Already-ready packagers return immediately. Request cancellation drops only that waiter and does
not stop the shared session. Compatibility playlist routes will use the same readiness guarantee
while retaining their explicit token pin as their sole lifecycle authority.

## Error and concurrency behavior

- Readiness uses a loop around `tokio::sync::Notify` plus a state check, preventing lost wakeups.
- Segment publication updates state before notifying waiters.
- A timeout or cancelled HTTP request does not create an extra subscriber and does not alter the
  manager's HLS transition generations.
- Missing PAT/PMT or video access points remain subject to the existing bounded fallback instead of
  buffering without limit.
- Invalid, future, and evicted segment requests remain `404` and do not refresh activity.

## Testing

Test-driven implementation will cover:

- shared access-point state learns PAT/PMT/video PID and rejects access flags on other PIDs;
- the first HLS segment drops a partial GOP and begins with cached PAT, PMT, and a video access
  point;
- subsequent segments repeat the table prefix and begin on video access points;
- discontinuity clears stale table/access-point state;
- a cold native playlist request does not return an empty successful playlist;
- readiness wakes after first publication, returns immediately when already ready, and times out
  with retryable `503` when media never arrives;
- compatibility HLS readiness does not refresh native activity or replace its token pin;
- existing duration, ceiling, lifecycle, title, and segment-parity regressions remain green.

Live verification will cold-stop the reported content ID, request `.m3u8`, assert that its first
`200` response already contains a segment, fetch that segment, and verify with FFmpeg that video
frames decode without missing-PPS startup errors. The full workspace tests, formatting, and Clippy
with warnings denied will run before the daemon is restarted for user testing.
