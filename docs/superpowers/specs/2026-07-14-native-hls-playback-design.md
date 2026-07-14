# Native HLS Playback Reliability Design

## Problem

The native `GET /streams/{network}/{id}.m3u8` endpoint starts an HLS packager over an uncounted
session receiver. Playlist and segment requests therefore leave the session subscriber count at
zero. The manager's fixed 30-second reaper removes that session and its packager even while an HLS
player is actively refreshing the playlist, forcing playback to restart repeatedly.

The default HLS hard ceiling is also too small for ordinary live video. At 256 MPEG-TS packets, a
segment can contain at most 48,128 bytes. PCR/keyframe-aware packaging is forced to cut at that
ceiling even when the configured one-second media target has not elapsed. On a multi-megabit
stream this produces sub-second, often mid-GOP segments and a retained media window shorter than
the playlist reload cadence.

Neither native endpoint transcodes or remuxes Ace live playback. The `.ts` endpoint forwards the
received MPEG-TS after its startup keyframe gate, while the `.m3u8` endpoint groups the same TS
packets into HLS segments. The fix remains limited to HLS lifecycle and segment geometry.

## Goals

- Preserve the existing `/streams/{network}/{id}.m3u8` URL and response shape.
- Keep a native HLS session alive while its playlist or retained segments are actively requested.
- Reclaim a native HLS session after 30 seconds with no direct TS subscribers and no HLS access.
- Let the default packager reach PCR/keyframe boundaries on ordinary multi-megabit live streams.
- Retain a bounded hard byte ceiling for malformed or unusually high-bitrate streams.
- Keep explicit `OUTPACE_HLS_SEGMENT_PACKETS` overrides authoritative.

## Non-goals

- Transcoding, codec conversion, remuxing, or adaptive-bitrate variants.
- Changing the tokenized Ace compatibility HLS endpoints or their explicit playback leases.
- Changing the continuous MPEG-TS response path.
- Adding client identity, cookies, tokens, or explicit stop commands to the native shorthand URL.

## Design

### HLS activity lease

`HlsPackager` will store the instant of its most recent native media access. Rendering either a
playlist or a retained segment refreshes this instant. Internal inspection helpers that do not
serve media will not refresh it.

The manager's reaper will make one retention decision per session:

1. Retain a session when `subscriber_count() > 0`.
2. Otherwise, retain it when its matching HLS packager was accessed less than the manager grace
   period ago.
3. Otherwise remove both the session and its matching packager.

The existing grace remains 30 seconds in production. Tests may construct a manager with a shorter
grace or invoke one deterministic reap pass; production configuration does not gain a new knob.
The reaper will snapshot recent packager activity, release that map lock, update the session map,
and then prune packagers in a separate lock scope. It will never hold both manager map locks at
once, avoiding the inverse lock order currently used by explicit stop.

A manifest request refreshes activity before returning its playlist. A valid retained segment
request refreshes activity when it returns bytes. Invalid, future, or already-evicted segment
numbers return `404` and do not keep a stream alive.

### Default segment ceiling

The default `HlsConfig.segment_packets` value will increase from 256 to 16,384 packets. This is a
3,080,192-byte hard ceiling per segment. With the default six-segment window, configuration
accounting bounds completed segments plus the current segment to 21,561,344 payload bytes.

PCR/random-access timing remains the preferred boundary. The packager still targets 1,000 ms,
cuts at the first random-access packet after the target, falls back at twice the target on a
PCR-bearing packet, and never exceeds the packet ceiling. Streams without usable PCR continue to
use packet-count segmentation. Explicit `OUTPACE_HLS_SEGMENT_PACKETS` values retain their current
meaning and are not silently raised.

The larger default is intentionally a ceiling rather than a nominal segment size: ordinary live
streams should cut earlier at a timed keyframe, while malformed inputs remain memory-bounded on
64-bit and ARMv7 targets through the existing checked configuration validation.

### Documentation

The README will describe `OUTPACE_HLS_SEGMENT_PACKETS` as the hard ceiling/fallback packet count,
state the new 16,384 default, and describe `OUTPACE_HLS_SEGMENT_DURATION_MS` as the requested PCR
media duration rather than merely an advertised value. The Linux portability note will continue
to document that the configured packet count is a hard memory ceiling.

## Error and concurrency behavior

- Activity tracking uses a monotonic `Instant`; wall-clock changes cannot expire active playback.
- Poisoned synchronous state follows the packager's existing mutex policy and is not given a
  separate recovery path.
- The reaper evaluates the most recent completed access at its monotonic cutoff. A request that
  arrives only after an already-idle session is removed starts a new session through the existing
  lazy-start path.
- Explicit stop continues to remove the session and packager immediately, regardless of recent
  activity.

## Testing

Regression tests will establish red/green behavior for:

- an HLS-only session surviving a reap pass after recent playlist access;
- segment access refreshing the same activity lease;
- an HLS-only session and packager being removed after genuine inactivity;
- invalid segment probes not extending activity;
- the default configuration using 16,384 packets in configuration and runtime parsing tests;
- timed input exceeding 256 packets reaching its later random-access boundary under defaults;
- explicit small packet ceilings still forcing bounded fallback segmentation;
- existing tokenized compatibility HLS lease behavior remaining unchanged.

Final verification will run formatting, the full workspace test suite, and Clippy with warnings
denied. Socket-binding tests require execution outside the restricted sandbox, as demonstrated by
the clean baseline.
