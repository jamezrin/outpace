# 70 — Controlled startup acceptance for #167

Date: 2026-10-18 (Europe/Madrid). Baseline: merged `main` at `2578e88`,
with the existing optimized binary built using Rust 1.99.1. This follow-up
changes documentation only; the discovery, connection, freshness and buffering
policies are unchanged.

## Method

One fresh-state, simultaneous 180-second comparison used cid20, supplied manually
through the environment, and original engine 3.2.11. The user retained only cid20
in the private registry, so this current public source replaces the historical
cid9 named in the issue. Neither the harness nor an environment helper read the
registry. Both engines used compat playback/stat routes, one capture client each,
default playback settings, isolated storage and listeners. Native status polling
created no extra playback subscriber.

This was a controlled local baseline with WARP off. Routing was checked before,
during and after the run, and the host WARP route was restored during cleanup.
Only the pre-existing BuildKit container remained after owned clients stopped.
No retry, target reduction, timeout adjustment or freshness bypass was used.

The extracted numeric report, raw media, logs, tokens, binary identity and
routing notes remain private under
`/tmp/outpace-startup-167-baseline-20261017T222735Z`.

## Startup results

Both arrival files use the comparison harness's shared monotonic capture origin.
The first positive completed body reads were:

| Observation | Seconds from capture start |
| --- | ---: |
| Reference first media body | 1.194421 |
| Candidate first media body | 2.737019 |
| Candidate minus reference | 1.542598 |

Candidate operational log timestamps provide a separate stage timeline relative
to resolution. These are millisecond wall-log differences, not wire timings; the
log clock is not precisely aligned to the capture clock.

| Candidate stage | Seconds after resolution |
| --- | ---: |
| First TCP attempt | 0.082 |
| First TCP connection | 0.086 |
| First BT handshake | 0.173 |
| Initial handshaked pool, one peer | 0.424 |
| Authenticated contiguous media | 1.609 |
| First source output | 1.609 |

The reservoir released by `TargetDuration` with 30,300 ms of buffered media
duration. That duration describes media history, not wall-clock startup delay.
The default 30-second target and 15-second timeout were preserved.

Both remaining live timing conditions pass in this run: first connection activity
within three seconds of resolution, and first media within five seconds of the
reference. Existing deterministic fast-connect coverage and the independently
reviewed fixes in notes 61 and 66 remain the implementation evidence. This
observation completes the scoped controlled-baseline acceptance for #167 without
an additional production change.

## Sustained activity and limits

The predefined seconds 60–180 observation retained all 120 speed samples per
engine, with no invalid or zero samples. Reference and candidate mean reported
download speeds were 715.917 and 720.572 KiB/s. Captured bytes in that interval
were 91,209,408 and 91,269,864 respectively. These observations establish usable
activity during the comparison; they do not prove byte identity, exact live
position, decode quality or sub-poll playback smoothness.

Earlier cid20 captures from the statistics validation also used defaults. Their
first-body differences were +4.752788 seconds with WARP off and +5.746747 seconds
with WARP on. The latter **missed** the five-second criterion by 0.746747 seconds;
it is not relabeled as passing. Both connected within about 81 ms of resolution
and released by target duration. These separate observations are not a controlled
WARP causation experiment, a latency distribution or a guarantee for every route
and source. The fresh controlled baseline above was run once after inspecting
both earlier outcomes, with the same criteria and no playback tuning.

The result resolves the issue's controlled local startup gate. It does not
establish universal startup reliability, the full paced TS/HLS/lead matrix, or
the cause of prior slow or no-media sessions. Historical negative results in
notes 61 and 66 remain valid observations of those runs.
