# 64 — Controlled startup revalidation for #166

Date: 2026-10-17. Candidate: current `main` at `0398462`, built with
`cargo build --release -p ace-engine --bin outpace`. Reference: original engine
3.2.11 from the local `swarmtest-engine:latest` image, using its default command.

## Method

One fresh-state, simultaneous 180-second comparison used cid9 resolved manually
from the gitignored registry. The candidate used native content-id playback; the
reference used its compatibility route. No code or harness read the registry.
All inherited `OUTPACE_*` tuning overrides were removed from the child environment;
the runner isolated storage and listeners. Candidate defaults included the
30-second startup target and 15-second reservoir timeout. There was one candidate
process and one reference process, with no pre-existing engine clients observed.

The host's WARP tunnel was temporarily disabled with operator authorization.
Routing was checked before, during and after the run; public traffic used the
ordinary gateway during the comparison, and the original WARP route was restored
after cleanup. Two subsequent fresh-state 90-second pairs tested explicit
10- and 20-second targets under the same WARP-off conditions, with restoration
after both pairs. This was a controlled local comparison, not a reproduction of an
existing deployment through WARP. Neither runner changed global networking itself.

With the content id supplied privately through `ACE_CONTENT_ID`, the existing
runner command was:

```sh
READ_TIMEOUT=60 python3 tools/abcompare/run.py \
  --outpace "$PWD/target/release/outpace" --seconds 180
```

For the follow-up pairs, the same command used `--seconds 90` and respectively
`OUTPACE_PREBUFFER_MS=10000` and `OUTPACE_PREBUFFER_MS=20000`. All other inherited
`OUTPACE_*` overrides were removed.

Raw media, logs, settings, image identity and routing observations remain private
outside the repository. All six full captures were probed and decoded with FFprobe
and FFmpeg after the runner finished and cleaned up its owned processes.

## Startup observations

- Reference first completed body read: **1.224394 seconds**.
- Candidate first completed body read: **3.358224 seconds**, or **2.133830 seconds**
  after the reference.
- Candidate reservoir release: `TargetDuration`, with **30,960 ms** of selected-PCR
  duration and **15,201,116 bytes** queued.
- The initial selected peer's window allowed the derived **42-piece** history
  depth without clamping it to the peer minimum. The reported API metadata rate
  was 5,580,696 bits/s, corresponding to the raw 697,587 bytes/s descriptor hint.

Relative to the first candidate TCP-attempt log, candidate operational timestamps
showed TCP connection at 32 ms, BT handshake at 96 ms, initial pool selection at
347 ms, authenticated contiguous bytes at 1,684 ms, first source output at
1,685 ms, and reservoir release at 3,016 ms. Pool selection reported three
attempts: one connection and two TCP failures. These are millisecond wall-log
differences, not independently captured wire timings. Body-read times use the
comparison harness's monotonic clock; the two clocks were not precisely aligned.

This observation meets the original #166 startup numbers: release by target
duration and first body no later than 14.6 seconds. It also falls within the
reference-plus-five-second startup criterion shared with #167. It required no
runtime change beyond the fixes already on `main`.

## Follow-up startup target samples

Each target below had one fresh-state simultaneous pair; these are individual
observations, not latency distributions. First-body timing is measured by the
same harness as the default run.

| Target | Reference first body | Candidate first body | Candidate minus reference | Release reason | Selected-PCR duration | Queued bytes |
| --- | --- | --- | --- | --- | --- | --- |
| 10 seconds | 1.565187 s | 2.054633 s | 0.489446 s | `TargetDuration` | 10,480 ms | 5,241,628 |
| 20 seconds | 1.030809 s | 2.126275 s | 1.095466 s | `TargetDuration` | 20,160 ms | 9,959,300 |
| 30 seconds (default) | 1.224394 s | 3.358224 s | 2.133830 s | `TargetDuration` | 30,960 ms | 15,201,116 |

All three sampled targets released by target duration and met the original
first-body threshold. The follow-up pools each reported four attempts, one
connection, two TCP failures and one handshake failure. Authenticated contiguous
bytes arrived about 1.54–1.57 seconds after the first TCP attempt, and reservoir
release followed at about 1.67–1.71 seconds.

The 10-second pair decoded 2,601 reference and 2,651 candidate video frames;
the 20-second pair decoded 2,591 and 2,991 respectively. All four probes and
full-file audio/video decodes exited zero. Candidate continuity-error totals
were 67 and 69, while each reference reported zero. The analyzer reported one
PCR jump in each 10-second output and no PCR jumps in either 20-second output.
These diagnostics preserve the same limits on quality and player-behavior claims
as the default run below.

## Media evidence and limits

The reference delivered 92,257,736 bytes and the candidate 105,730,636 bytes.
Both FFprobe and full-file audio/video FFmpeg decode commands exited zero.
FFmpeg progress reported 4,869 video frames for the reference and 5,619 for the
candidate. Decoder diagnostic counts were 149 and two lines respectively; these
are not comparable frame-error rates, and exit zero is not a quality guarantee.

The analyzer reported the same approximately 95,292-second forward PCR jump in
both outputs around 50 seconds into capture. Its three-second-buffer player model
therefore did not start until about 52 seconds. That model is not an observed
player, and its zero reported stalls cannot establish smooth initial playback.
The later common PCR interval had 129.04 seconds of analyzed overlap and 100%
bidirectional media-packet digest membership in that interval. This set-membership
check excludes PSI/SI, PMT and null packets; it does not establish packet order or
full-transport byte identity, nor validate earlier history or the full wall-clock
observation. The candidate's unmatched prefix and
ambiguous packet exclusions remain measurement limits.

Candidate continuity errors numbered 190, all on SDT PID 17; reference continuity
errors numbered zero. Candidate media-packet duplication and transport sync loss
were zero. The reference had one sync loss. This startup observation does not
resolve the SDT behavior tracked by #172 or the robustness work in #1.

The prior slow starts used different source windows, earlier builds and WARP.
These three samples cannot attribute the difference to a particular change,
establish WARP causation, or prove the older failure can no longer occur. They cover
startup release at the three configured targets, but not the full paced TS/HLS matrix,
real-player behavior, warm starts, ten-minute duration, memory bounds or the full
lead/recovery acceptance. Those results must not be inferred from first-body timing.

## Separate connector diagnostic

An offline loopback probe against the current production resolved-open path
exposed a remaining initial-connection scheduling delay. Its first discovered
peer accepted TCP but did not answer the BT handshake. A healthy second address
was published 100 ms after that acceptance. Despite spare parallel capacity, the
healthy listener was not dialed within the next two seconds. The initial
`connect_pool` consumes a fixed candidate snapshot; later discoveries wait until
that snapshot finishes, including the silent attempt's three-second timeout.

This delay was not exercised by the fast live pool above and does not explain
its reservoir timing. It belongs with #167's connection acquisition work. A fix
would need to admit incremental candidates while preserving bounded connection
capacity, exploration ordering and owned cancellation. The diagnostic test was
temporary and left no production or tracked test changes.

## Verification

The unchanged candidate's offline workspace baseline completed with 961 tests
passed and seven ignored. The optimized build succeeded. All three comparison
runners, six probes and six decodes completed. Owned candidate processes and reference
containers were cleaned up; the final public route again used `wgcf-profile`.
Document whitespace and identifier-hygiene checks passed. No claim of complete
issue or milestone acceptance follows from these samples.
