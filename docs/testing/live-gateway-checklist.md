# Live gateway validation checklist

Use this checklist to record a bounded observation, not to infer reliability from
a successful startup. API field meanings are in [native-api.md](../native-api.md#live-statistics).

## Setup and identity

- Decide whether this is a controlled local build or an existing deployment.
  Keep Cloudflare WARP off for controlled local validation. For deployment
  comparisons, preserve the deployed routing and record client/server WARP
  states, routes and forwarding privately; restore any authorized local changes
  in cleanup, including on failure.
- Record exact build/version, binary digest and manifest, owned PID identity,
  cold/warm state, existing viewers, API route, player, buffering, cache and
  network settings. Pending documentation edits do not change a candidate binary;
  record them explicitly rather than rebuilding an otherwise identical candidate.
- Use the same current public, unencrypted source and simultaneous window for
  comparisons. Resolve a registry identifier manually into a private environment
  variable such as `ACE_CONTENT_ID`. The gitignored registry is LLM context only:
  no code, test or harness may read it. Keep values, derived infohashes, stream
  names, URLs, raw logs and media out of tracked files and shared output.
- Use fresh private artifacts, bounded logs and HTTP deadlines, an absolute
  7,500-second budget including cleanup, and terminate only owned children whose
  identity is verified. Do not attach to an unrelated daemon.

## Two-hour decode observation

- Require a first decoded frame within 90 seconds, then a full 7,200 seconds of
  active observation with continuing decoded-frame progress. A 60-second frame
  watchdog detects stalls; bytes or FFmpeg timestamps alone do not establish
  decoded-frame progress. Keep sufficient valid status coverage across the whole
  active window and its tail; startup samples do not substitute for active time.
- Poll `GET /streams/<network>/<id>/status` every five seconds. Save every
  attempt before classifying or aborting: index/elapsed time, HTTP status when
  available, parse success, and an allowlist of finite numeric observations.
  Rejections retain fixed field paths, reason codes and types, never raw bodies
  or arbitrary strings. Preserve attempted/valid counts and covered active
  seconds on every exit. Unavailable status is a coverage limitation, not a pass.
- Validate the actual API schema. Present nullable position bounds are valid
  during recovery; missing fields are distinct from null. Pool rebuilds clear
  active coverage while retaining historical recovery counters. Do not replace
  unavailable measurements with invented zeros. Require meaningful measured
  statistics over the observation, rather than treating placeholder snapshots as
  evidence of playback.
- Interpret `buffer_ms` as the engine's computed reservoir/reassembler coverage,
  not a player buffer. After draining, zero `buffer_ms` and `buffer_pieces` can be
  correct; positive steady-state buffer is not an acceptance requirement.
- Report complete only with the full decode window, valid coverage and successful
  cleanup. Use CLI exit 0 for complete, 1 for failure, and 2 for inconclusive or
  internal error. Report measured decode progress, gaps and errors explicitly.
  Five-second samples cannot prove sub-poll smoothness, exact startup timing or
  exact live position. An FFmpeg observation does not establish deployed VLC
  behavior; use [the VLC comparison workflow](../../tools/abcompare/README.md#compare-deployed-instances-with-vlc)
  for that question, with matching routes and buffering settings.

## Separate recovery and memory evidence

Keep deterministic offline gap/recovery tests separate from natural live logs
and sampled counters. The four offline gap tests exercise controlled cases;
zero sampled event counters do not exclude an event after the last saved sample.
Record the observation window and counter semantics before attributing a recovery
or an outage.

Run memory churn as a separate, explicitly budgeted procedure: fix the build,
cache/retention limits, source count, viewers, start/stop cycle count, dwell and
settling times, and total duration before starting. Record baseline, per-cycle
and settled RSS plus `GET /debug/memstats` live-heap/resident observations, and
verify session/child cleanup. Compare repeated settled measurements under the
same workload. A bounded trend is evidence for that budget, not proof of no leak;
a decode soak without memory measurements makes no memory claim.

## Current evidence and acceptance

The cid20 attempt decoded 93,045 frames over roughly 62 minutes and retained 741
status samples: one active upstream and zero sampled recovery event counters.
All recorded engine buffer values were zero. It stopped inconclusively before
the two-hour window; the rejected sample was discarded, so its cause is unknown.
Private logs show a stale event and reconnect after the last saved sample and
before the runner abort. The runner required integer bounds where the API allows
null during cleared coverage: this demonstrates a harness contract mismatch,
not the proven cause of that rejection. Neither an engine buffer bug nor a
source-wide outage was established. The two-hour acceptance remains pending;
offline harness tests and a corrected runner do not close it.

A subsequent schema-correct run failed the 60-second video-frame watchdog after
roughly 6.5 minutes, having decoded 17,248 frames with 80 valid active status
samples. One stale teardown and reconnect occurred. This establishes a
video-frame progress failure; the timing and cause of the media/recovery
interaction remain under investigation in
[#246](https://github.com/jamezrin/outpace/issues/246).

A separate simultaneous 480-second comparison against AceStream 3.2.11 did not
reproduce a stale teardown, reconnect, or video stall. Both saved transports
decoded video and audio; every compared packet in their shared PCR window
matched. Offline decoding and packet identity in that bounded window do not
explain the earlier failure or establish live presentation quality. WARP and
owned processes were restored/cleaned up after each run. No RSS or memory-churn
result is established by these decode observations.

Issue #195 remains open pending a full two-hour decode observation and adequate
recovery evidence. No live evicted-gap skip was observed in these runs; the
focused offline gap tests remain separate evidence.
