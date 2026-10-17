# 65 — Paced startup validation attempt for #166

Date: 2026-10-17. Candidate runtime: `0398462`, optimized build; the following
branch commit only added note 64. Source: cid9 resolved manually from the private
registry. Raw artifacts remain private outside the repository. No harness read
the registry, and no runtime changes were made for these observations.

The explicit numeric startup checkbox in #166 is supported by the controlled
samples in note 64. This follow-up tests the broader playback-design requirements;
invalid trials here do not erase those startup observations or establish full
design acceptance.

## Qualification requirements

The prebuffer design's broader acceptance requires three valid ten-minute
reference trials, then three direct-TS and three native-HLS trials at each target
in ascending order, stopping after the first complete passing target. Each valid
trial needs at least 599 seconds of normally terminated decode, bounded advancing
media gaps, measured initial and early/middle/late lead, component memory, and
HLS readiness/start/retrieval evidence. Startup body timing alone cannot satisfy
these requirements.

The experimental paced harness from the older startup-history worktree was copied
to private temporary storage without modifying that worktree or importing its
unfinished runtime changes. Its offline suite completed 314 tests with 47 skips.
A read-only review found that it cannot qualify the current binary unchanged:

- Candidate component memory requires reservoir/HLS gauges absent from current
  `main`. Process RSS cannot substitute for those component observations.
- HLS identity/readiness checks require a private observation socket protocol not
  present in the current runtime.
- Lead acceptance remains pending until independent evidence-bound review; the
  runner does not establish three-reference provenance or orchestrate the matrix.
- Provider retransmissions, fan-out lag and live skips are not fully collected.

Missing observations remain invalid. No trial is promoted to an acceptance pass
by dropping those gates. A diagnostic direct pair and native-HLS trial use a
10-second target with other playback defaults, fresh process/storage state,
controlled WARP-off routing and cleanup/restoration. The decoder drains stderr
continuously, applies input and presentation pacing, and enforces per-track
progress and terminal lifetime watchdogs. Invalid diagnostic trials do not count
toward the matrix.

## Direct-TS diagnostic

A simultaneous original-engine 3.2.11 / candidate pair requested 600 seconds.
Reference first body was 1.027797 seconds; candidate first body was 2.132954
seconds. Each decoded 36.64 seconds of contiguous video; decoded audio was
37.883333 seconds reference and 37.030667 seconds candidate. Both hit the
progress watchdog and were terminated, with maximum observed post-start gaps
about 15 seconds. Neither completed the required interval or normal exit.

The saved direct captures both contain an analyzer-reported PCR jump of about
95,292.598 seconds. This may interact with timestamp-based pacing, but does not
establish source-wide failure or the origin of the discontinuity.

Both passive TCP captures and flow attribution completed without reported capture
or attribution errors. Required timed lead observations were missing, candidate
component memory was unverified, and the direct trials were invalid. The matching
short decode duration does not establish a source outage, engine defect or
source-wide condition. It requires reconciliation with a real-player observation.

## Native-HLS diagnostic

A fresh candidate-only native-HLS trial also requested 600 seconds with the
10-second target. First media body was observed at 16.461758 seconds. The harness
reported 658.84 seconds of contiguous video and 658.336667 seconds of decoded
audio, with a maximum post-start advancement gap of 0.563089 seconds. These are
harness coverage counters, not proof that the requested interval was correctly
bounded or presented.

The initial manifest contained six segments, matching the configured readiness
count, and its advisory start was valid. The harness observed the selected first
segment and decoded identity. Of 113 requested segments, 112 completed; the final
requested body was incomplete. These narrower successful checks do not replace
the failed complete-interval certificate.

Despite sustained decode, the process reached its absolute lifetime after about
675.84 seconds and exited 255 rather than ending normally. Required timed lead,
component memory and the HTTP-complete requested-interval identity certificate
were unavailable. The trial was invalid. Sustained sampled decode does not
repair the missing normal-exit and evidence requirements; the harness's terminal
behavior needs investigation before repeating the matrix.

Reported candidate process peak RSS was 153,018,368 bytes in direct TS and
175,370,240 bytes in HLS. Neither value identifies reservoir or HLS component
usage, establishes an allocation bound, or proves absence of growth.

The two-phase wrapper restored the WARP route after both diagnostic phases and
cleaned up its candidate/reference processes. A separate fresh-state VLC pair
then began its own controlled WARP-off window to reconcile the direct watchdog
result, using native content-id routing for the candidate and the reference's
compatibility route, with one-second caching and five-second counter sampling.

## VLC reconciliation

The fresh-state simultaneous VLC pair completed its bounded 600-second window
with audio/video decode progress on both engines and no counter resets. Both used
dummy audio/video outputs, one-second network caching and five-second polling.
Only the private copy's candidate playlist route was changed to native content-id
playback; the checked-in runner and runtime were unchanged.

| Final sampled counter | Reference | Candidate |
| --- | ---: | ---: |
| Decoded video | 17,362 | 17,322 |
| Decoded audio | 55,750 | 55,666 |
| Lost pictures | 465 | 465 |
| Lost audio buffers | 23,991 | 23,918 |
| Maximum sampled video/audio progress gap | 5.002 s | 5.004 s |
| Control errors | 1 | 1 |
| Counter resets | 0 | 0 |

The first positive audio/video decode counters were observed at 5.029 seconds
reference and 5.052 seconds candidate. These are polling upper bounds, including
launch offsets, not precise startup measurements. Each client had 119 successful samples, all reporting `playing`, and one
control error. The failed request
time was not retained separately, so its precise placement is not established.
Drop counters are material observations and do not support a flawless-playback
claim. Dummy outputs cannot establish visual presentation or audio-device quality.

The approximately 37-second terminal stop from the FFmpeg direct diagnostic did
not reproduce in this VLC observation. This does not identify the PCR jump's
origin or prove all clients behave alike. Five-second sampled progress cannot
exclude shorter stalls, establish packet identity/live position, or replace the
paced design's normal completion, lead and memory proof.

The bounded synthetic harness follow-up did not reproduce a generic endless-input
termination bug: its exact filter construction with two-second trims exited
normally. Coverage counters can span backward-timestamp epochs while timestamp-
based trims have a different endpoint; that remains a possible explanation for
the HLS totals exceeding 600 seconds, not a proven cause of this run's timeout.
The forced final segment interruption is not independently a product defect.

Both VLC clients and the wrapper's candidate/reference processes were cleaned up.
Final route inspection selected `wgcf-profile` again, and only the unrelated
pre-existing BuildKit container remained. No larger target or invalid-trial repeat
was run: the qualification prerequisites were not satisfied. The startup checkbox
is supported, while broader playback acceptance remains unverified.

## Offline verification

The unchanged runtime passed `cargo test --workspace --locked`: 961 passed,
seven ignored. `cargo clippy --workspace --all-targets --locked -- -D warnings`,
`cargo fmt --all --check`, identifier hygiene and whitespace checks passed.

All 16 existing startup-buffer fixtures passed in a separate run. They exercise
byte-cap release with one incoming-chunk overhead, deadline release while the
source is stalled, queue bounds, resets and dropping the wrapped source. Captured
operational output reported `ByteLimit` and `Deadline` with duration/queued-byte
fields, without source identifiers. These deterministic fixture results establish
narrow buffer invariants, not measured whole-daemon or HLS memory bounds in live
playback.
