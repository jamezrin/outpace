# 66 — Admit discovery during initial connection selection

Date: 2026-10-17. Follow-up for #167, based on merged `main` at `cd68af8`.

## Remaining delay and change

Incremental discovery already started the first connection before discovery
completed. Initial pool selection still consumed a fixed candidate snapshot,
however. A peer accepting TCP but withholding the BT handshake could hold that
snapshot until the three-second connection timeout, even after a healthy address
arrived with spare connection capacity.

Initial pool selection now consumes the owned discovery receiver while pending
connections run. Newly eligible addresses enter spare slots without cancelling
existing attempts. Pending attempts plus connected candidates stay within the
configured initial parallel limit. Eligible ordering still uses exploration
history and learned-source priority. More-explored candidates wait while the
current cohort has pending attempts; new less-explored candidates can join spare
slots. Selection may still cancel pending attempts after its grace period.
Each address is attempted once per selection round, and actual failures preserve
cooldown history. Repeated discovery does not reset history or restart attempts.

The first usable handshake starts the existing 250-ms selection grace. Later
updates cannot reset that deadline. Window preference and active-pool truncation
remain in place. Dropping the source cancels the selection future and its owned
connection set, and drops the owned discovery worker. The active-pool refill
policy, exposed configuration and defaults are unchanged.

## Offline evidence

The original production resolved-open diagnostic failed on the fixed snapshot:
a healthy address published after a silent TCP connection was not dialed within
two seconds, despite spare capacity. That diagnostic was synthetic and used only
loopback addresses.

The retained regressions exercise the production resolved-open path:

- With two slots and the first peer silent, a late healthy peer connects within
  one second of publication and supplies actual synthetic TS media.
- With one occupied slot, the late peer remains undialed for the controlled
  200-ms observation, then connects and supplies media after the first slot is
  released. Duplicate discovery batches do not reset candidate history.
- Dropping the consumer with one occupied slot closes the silent socket and
  drops discovery within one second, without admitting the queued peer.

All 176 provider regressions passed, including existing priority, exploration,
cooldown and reconnect cases. The full workspace run passed 964 tests with seven
ignored. The release build, Clippy with warnings denied, formatting, identifier
hygiene and whitespace checks passed. Independent code review found no actionable
issues.

## Controlled live comparison

One fresh-state simultaneous 180-second comparison used cid9 supplied manually
through the environment, the optimized candidate built from this patch, and the
original 3.2.11 engine with its default image command. Candidate playback used
default settings, including the 30-second reservoir. Storage/listeners were
isolated; no other engine client was present. The registry was not read by any
harness. Private run notes retain the binary identity and source diff because the
release was built before the final commit. Raw logs and media remain private.

WARP was temporarily disabled under the existing operator authorization. Public
routing used the ordinary gateway during capture and returned to `wgcf-profile`
after owned-client cleanup. Only the pre-existing BuildKit container remained.

- First candidate TCP attempt: **66 ms after resolution**, meeting the under-
  three-second first-connect criterion.
- Reference first body: **1.229224 seconds**; candidate: **7.703294 seconds**.
  The **6.474070-second** difference **fails** #167's reference-plus-five-second
  criterion. It remains below #166's separate 14.6-second threshold.
- Reservoir release: `TargetDuration`, **30,240 ms** selected-PCR span and
  **25,160,792 bytes** queued.

Operational timestamps relative to resolution placed the first successful BT
handshake at 3.210 seconds, pool selection at 3.461 seconds, authenticated
contiguous bytes at 4.705 seconds and reservoir release at 7.378 seconds. Pool
selection reported 17 completed attempts: one connection, 14 TCP failures and two
handshake failures. Multiple initial attempts occupied the configured connection
slots, and another group began around the three-second timeout boundary. This
is consistent with bounded capacity delaying later candidates, but logs do not
establish each candidate's publication time or queue residence. There is no claim
that the new incremental-admission defect was exercised by this live run.

Both saved captures passed probing and full-file audio/video decode with exit
zero. Video-frame counts were 4,689 reference and 5,014 candidate; nonempty decoder
diagnostics remain a limit. Both captures had zero analyzer-reported PCR jumps
and sync losses. Candidate continuity errors numbered 166, all on SDT PID 17;
reference errors numbered zero. This does not resolve #172 or prove smooth
real-time playback.

No repeat was run to obtain a passing startup number. The deterministic scheduling
fix is independently supported by the loopback regressions; this one live result
does not establish a latency improvement over earlier source windows. Merge this
as bounded progress, keeping #167 open because its complete live criterion is
unmet. Connection-capacity/timeout policy and the broader playback/soak acceptance
remain separate follow-up work, not silently changed defaults.
