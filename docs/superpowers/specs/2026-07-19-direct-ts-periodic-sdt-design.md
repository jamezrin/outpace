# Direct MPEG-TS Periodic SDT Design

## Problem

Outpace synthesizes an MPEG-TS SDT `service_name` from resolved stream metadata so players can
show the channel title. Native HLS repeats the synthesized tables at segment access points, but a
continuous direct MPEG-TS response currently emits the titled SDT only when its per-client
`KeyframeGate` first locks. VLC can miss or discard that one-shot announcement and then displays no
title for direct playback even though the equivalent HLS stream is titled correctly.

## Scope

Change only titled continuous MPEG-TS output produced by `KeyframeGate`. After initial lock, repeat
the synthesized `[PAT][PMT][SDT]` table prefix at each subsequently detected clean video access
point. Continue filtering upstream PID `0x0011` while a synthesized title is configured, so Outpace
remains authoritative. Untitled streams retain their existing passthrough behavior.

HLS packaging remains unchanged: it already emits its table prefix at segment access points. HTTP
title headers, timer-based PSI scheduling, configuration variables, and unrelated transport
rewriting are out of scope.

## Behavior

- The initial titled access point remains `[PAT][PMT][SDT][video access packet]`.
- Each later detectable video access point in the same continuous response is preceded by the same
  synthesized `[PAT][PMT][SDT]` prefix.
- Ordinary non-access-point packets pass through unchanged after lock, except that upstream SDT is
  still filtered for titled streams.
- Untitled streams do not gain synthesized SDT or additional table prefixes.
- Reset and discontinuity recovery preserve the configured title and resume the same behavior after
  reacquiring a clean access point.
- Scan-budget fallback remains bounded and retains its existing discontinuity semantics; periodic
  title injection does not create a new fallback policy.

## Implementation Boundary

Keep periodic access-point recognition inside `ace-media::mpegts::KeyframeGate`, where PAT, PMT,
video PID, access-point detection, title sanitization, SDT construction, and upstream-SDT filtering
already live. Do not add HTTP-layer state or duplicate MPEG-TS parsing in `ace-engine`.

After initial lock, retain enough `VideoAccessPointState` parsing state to recognize later video
access points and request its existing table prefix. Emitting a repeated prefix must not re-buffer
or drop the access-point packet.

## Verification

Use test-driven development:

1. A titled gate emits a synthesized SDT at initial lock and again before a later access point.
2. Intervening ordinary packets remain byte-identical and do not receive repeated tables.
3. An untitled gate preserves existing passthrough and upstream-SDT behavior.
4. Reset/discontinuity reacquisition retains the title and periodic behavior.
5. Existing MPEG-TS, HTTP direct-stream, HLS, workspace, formatting, Clippy, identifier-hygiene,
   and release-build checks remain green.

Manual validation uses VLC against the direct `.ts` endpoint after rebuilding and restarting the
existing test daemon. VLC should display the same resolved title as native HLS.
