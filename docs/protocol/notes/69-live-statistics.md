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
