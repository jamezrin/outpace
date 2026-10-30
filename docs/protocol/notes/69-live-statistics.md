# 69 — Independently observable live statistics

Date: 2026-10-17. Issue #171. Runtime build: `c311f4d`, release profile,
Rust 1.99.1. Source: cid9 supplied manually through the environment. No harness
read the registry; raw status, media, tokens and logs remain private.

## Implemented behavior

The session can read an independent source snapshot while its next output is
pending. Ace measures contiguous reassembled payload before output gating,
separately from the existing delivered-MPEG-TS byte total. Fixed-capacity history
supports nominal five-second download/upload rates, sampled once per second;
reads do not alter the measurement. Sampler shutdown records a final counter
snapshot so retained readers expire correctly.

The startup reader overlays reservoir duration while collecting/draining, then
exposes the provider's contiguous-ready observation. Partial pieces and pieces
beyond a hole do not count. Handoff/socket/broadcast/HLS/client queues are excluded;
this is neither player lead nor a whole-daemon buffering measurement.

Compat statistics include `speed_down`/`speed_up` in fractional KiB/s and nullable
`livepos`; native status includes the same position object. Positions describe
provider cursors and advertised windows. Unknown timestamps stay null. A sliding
window updates its reported bounds independently of the existing scheduler's
coverage policy, including updates whose upper bound has not advanced.

## Deterministic verification

Red-to-green tests drove the new interfaces, startup visibility and JSON fields.
The focused and boundary tests cover measured rates, idle decay, reset/overflow handling, bounded history, repeated-read purity,
sampler lifetime, ready-prefix holes/trailers, payload accounting and positions.
Fake sources prove that bitrate and reservoir duration are visible before the
first outer output, that draining decreases the duration, and that discontinuity,
cancellation, EOF and sources without readers retain their intended behavior.
HTTP tests verify active/idle schema, units, unknown timestamps and token handling.

Independent review identified an outdated lower window bound after subsequent
peer advertisements. A regression test preceded the fix, which separates observed
bounds from scheduler updates and preserves scheduling behavior.

Final local gates passed:

```sh
cargo +1.99.1 test --workspace --locked
cargo +1.99.1 clippy --workspace --all-targets --locked -- -D warnings
cargo +1.99.1 fmt --all --check
cargo +1.99.1 build --release --locked -p ace-engine --bin outpace
python3 -m unittest discover -s tools/abcompare
python3 tools/hygiene/check_identifiers.py
git diff --check
```

Workspace results: 990 passed, seven ignored. Comparison-harness results: 34
passed. No dependency was added; the existing Tokio test-util feature is enabled
only through the engine's development dependency for deterministic clock tests.

## One bounded live attempt

A single 90-second simultaneous local capture used fresh candidate/reference
state, original engine 3.2.11, default playback settings and one capture client
per engine. The candidate's experimental compat endpoint was enabled for this
diagnostic. A private adapter of the existing comparison runner selected compat
playback/stat routes for both engines, polled once per second, and additionally
observed native status without creating another playback subscriber.

The predefined comparison interval was seconds 30 through 90. All numeric speed
samples, including zeros, were retained; there was no retry or sample trimming.

| Final 60-second observation | Reference | Candidate |
| --- | ---: | ---: |
| Speed samples / invalid samples | 60 / 0 | 60 / 0 |
| Mean reported download speed, KiB/s | 126.167 | 0 |
| Zero-speed samples | 32 | 60 |
| Captured media bytes in that interval | 10,483,824 | 0 |
| Captured media bytes in the full run | 10,483,824 | 0 |

Native observation returned 89 usable snapshots, including 88 with known
positions and up to two peers. No positive measured bitrate or reservoir duration
was observed. The candidate logged no first-source output, no rejected pieces,
and one stale-pool event. These observations show that the endpoint remains
readable during a no-media session; they do not establish positive-rate accuracy
or explain why this candidate session received no media.

Because the candidate had no media activity, this is an invalid rate comparison.
There is no accepted candidate/reference ratio and no ±20% pass. The reference's
successful delivery does not establish a source-wide outage or identify an outpace
regression. Issue #171 must remain open for valid live throughput acceptance;
offline tests do not substitute for that criterion.

Cleanup restored the host WARP route and stopped owned processes/containers.
Only the pre-existing BuildKit container remained. Private artifacts and the
reviewed numeric report are under `/tmp/outpace-stats-171`.

## Follow-up live acceptance with cid20

On 2026-10-18 (Europe/Madrid), the merged implementation from PR #229 was tested
with cid20 on the same controlled local setup: original engine 3.2.11, fresh state
for both engines, default playback settings, compat playback/stat routes, one
capture client per engine, and an auxiliary native status observer. The release
build used Rust 1.99.1. WARP was off during the run and restored afterward.

The simultaneous capture lasted 180 seconds. The comparison interval was fixed
in advance at seconds 60 through 180, with one-second polling and all numeric
samples retained, including zeros. Neither engine had a zero-speed sample in
that interval.

| Final 120-second observation | Reference | Candidate |
| --- | ---: | ---: |
| Speed samples / invalid samples | 120 / 0 | 120 / 0 |
| Mean reported download speed, KiB/s | 778.842 | 748.297 |
| Zero-speed samples | 0 | 0 |
| Captured media bytes in that interval | 93,830,368 | 93,894,720 |
| Captured media bytes in the full run | 148,346,176 | 161,490,872 |

The candidate/reference mean reported rate ratio was 0.96078: the candidate was
3.92% lower, satisfying the issue's ±20% live rate criterion for this observation.
Both clients had usable media and status samples across the measurement window;
the largest status-sample gap was below 1.004 seconds. This compares the interval
means, not each instantaneous sample, byte identity, or exact live position.

All 179 native status observations succeeded. The first positive bitrate was
observed at approximately 3.002 seconds from capture start; the maximum observed
buffer duration was 29,500 ms. Together with the deterministic startup-buffer and
schema tests above, this completes the scoped acceptance for issue #171.

Earlier follow-ups with cid9 and cid1 delivered no media on either engine. A
cid17 attempt delivered media initially, but the candidate delivered none during
the fixed comparison window. Those attempts remain invalid comparisons; their
windows were not moved or their zero samples discarded. They do not identify the
cause of those sessions' delivery failures. This successful run does not establish
reliability across all sources or resolve the separate startup/recovery work.

Cleanup stopped the owned candidate and reference container, restored the host
WARP route, and left only the pre-existing BuildKit container running. Raw media,
status, tokens and logs remain private under `/tmp/outpace-stats-171-cid20-wqa4__m3`.


### Incremental ready-buffer measurements

Native status adds `livepos.buffer_complete`. When true, `buffer_ms` and
`buffer_pieces` describe one completed coherent measurement at the reported cursor.
When false, both numeric measurements are zero and unavailable; this does not
establish that the buffer is empty. Cursor movement and completed-piece additions
invalidate measurements synchronously. A fast-moving cursor may prevent a scan
from completing. These metrics never gate media admission or readiness.

The compatibility position object retains its existing keys and numeric types,
without `buffer_complete`; its zero values therefore cannot distinguish unavailable
from empty. Non-live provider statistics are unchanged.

The actor scans at most 2048 complete TS packets and 64 piece transitions per turn,
using borrowed per-piece media slices and the existing selected-PCR clock and
bitrate fallback. Publication and extraction callbacks do not scan media. Useful
unfinished work yields cooperatively; completed scans and absent observers do not
schedule stats continuations. Output capacity reservation is a separate dependency:
the existing blocking send still prevents progress while that send is pending.

Output backpressure uses a retained playback clock: time waiting for consumer
capacity does not consume first-output, stale-progress, request or cursor-age
budgets. Independent startup corroboration and network deadlines continue in
real time. Rebuilding a peer generation drains already accepted media before
new discovery or connection work; only successful enqueue counts as output
progress. Publication waiting alone retains the existing timeout policy.
