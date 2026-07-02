# 48 - Stale-pool retry and per-piece request retransmit

Date: 2026-07-02

## Symptom

After note 47, streams played cleanly but could still freeze intermittently after the initial
window. Connected peers kept the TCP session alive with telemetry (`id=11`, `id=12`, `id=34`)
but stopped delivering the next contiguous piece. The 12 s stale guard tore the whole pool down,
and reacquisition sometimes landed on batches where other known peers were unreachable
(`no usable upstreams: tcp=22`).

## Changes

### Timestamped logs

All `[ace]`, `[dht]`, `[seed]`, and `[listen]` stderr lines now include a dependency-free
`HH:MM:SS.mmm` UTC timestamp via the `logts` module and `alog!` / `swarm_log!` macros. This
makes stall duration and recovery gaps directly measurable in operator logs.

### Stale pool is not a bad-peer signal

`FollowEnd::PoolStale` distinguishes a visibly stalled but reachable pool from a genuinely
failed peer. A stalled pool's peers are retried rather than added to the cumulative excluded
set. Recovery is bounded by an emitted-byte watermark (`retry_stalled_pool`) so a truly frozen
source still gives up.

Before this, the recovery loop treated a stall like a peer failure, skipped the only reachable
peers, and spent time on dead addresses.

### Per-piece request timeout and mid-session skip

The pool loop sweeps every <= 1 s (`REQUEST_CHECK_INTERVAL`):

- A piece outstanding longer than `REQUEST_TIMEOUT` (4 s) is requeued and requested again.
- The timed-out piece is dropped from the scheduler's global in-flight set, while the slow peer
  keeps its own in-flight slot, so `ActivePeers::assign` tends to route the retry to a peer with
  more spare capacity.
- If the cursor is stuck longer than `REQUEST_TIMEOUT` on a piece that no upstream window still
  covers, `Continuity::skip_evicted_gap` skips forward to the lowest covered piece without a
  full pool teardown.

New pure primitives:

- `ActivePeers::{complete_everywhere, prune_below, any_unchoked_covers, lowest_covered_piece}`
- `Continuity::{timed_out_requests, skip_evicted_gap}`

All are unit-tested.

## Verification

A healthy live regression run produced a 90 s / 81 MB capture with 0 continuity-counter
discontinuities. The retransmit/skip paths stayed dormant on that healthy stream, which is the
expected behavior.

## Follow-up

Observe the retransmit and skip paths during a real field stall using the timestamped logs.
Those paths are unit-tested and reasoned through, but still need live evidence from an actual
stall where they fire.
