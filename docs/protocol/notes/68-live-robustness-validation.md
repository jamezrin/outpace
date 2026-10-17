# 68 — Longer live-player soak and bounded recovery probes

Date: 2026-10-17. Candidate runtime: `7d6a7ab`, release build with pinned Rust
1.99.1. Source: cid9 supplied manually through the environment from the private
registry. This change records validation; it does not change runtime behavior.

## Setup and scope

The controlled local run used fresh candidate storage and a disposable original
AceStream 3.2.11 container. Host/client/server WARP was off during observation and
restored afterward. No other outpace process or stream-engine container was
running beforehand; an unrelated BuildKit container was left alone. Playback
settings were defaults, including the 30-second candidate prebuffer target.

Each engine had one local dummy-output VLC client and one HTTP capture client.
The candidate clients shared its provider session. Both VLC clients used
one-second network caching and five-second status polling. A private copy of
`tools/abcompare/vlc_compare.py` changed only the candidate playlist URL to the
native content-id route, as in note 65. The capture used the checked-in
`ab_capture.Capture` with native content-id routing for outpace, the reference's
JSON playback lookup, and a 180-second read timeout. The harness did not read the
registry. Logs, playlists, settings, status samples and media remain private.

A bounded private wrapper requested 1,200 seconds. After 900 seconds it suspended
only its candidate process with SIGSTOP for 75 seconds, resumed it with SIGCONT,
and observed the remaining approximately 225 seconds. Cleanup always resumed a
suspended candidate before stopping owned processes and restoring WARP. The
first phase includes startup; it is not 900 seconds of certified presentation.

## Candidate player observation

VLC produced 239 valid samples, all reporting `playing`, with one control error
and no counter resets. Its first positive audio/video decode counters were
observed at 5.054 seconds, a polling upper bound including launch overhead.

| Candidate observation | Before pause | During pause | After resume |
| --- | ---: | ---: | ---: |
| Valid samples | 179 | 15 | 45 |
| Video counter increase between phase samples | 44,496 | 3,488 | 10,998 |
| Audio counter increase between phase samples | 83,434 | 6,530 | 20,708 |
| Maximum sampled audio/video progress gap | 5.002 s | 5.002 s | 5.002 s |

At the final sample VLC reported 59,615 decoded video frames and 111,914 decoded
audio buffers. Lost pictures, lost audio buffers, demux corruption and demux
discontinuity counters were all zero. Phase increases exclude the intervals
before the first and after the last sample in each phase. Five-second sampling
cannot exclude shorter stalls or establish visual smoothness, exact startup,
content identity, live position or audio-device behavior.

Decode continued during the process suspension, while the independent capture
had no arriving media for 76.789 seconds. Client-side buffered media therefore
separates decode progress from upstream delivery. Continued VLC progress does
not establish that the engine recovered without a delivery interruption or
quantify a player's actual live lead.

## Capture and pool recovery

| Complete-window capture | Reference | Candidate |
| --- | ---: | ---: |
| Bytes recorded | 355,402,012 | 368,351,032 |
| First body read | 0.838 s | 1.394 s |
| Payload continuity errors | 0 | 0 |
| Sync losses / malformed adaptations / transport errors | 0 / 0 / 0 | 0 / 0 / 0 |
| PCR jumps under the existing analyzer rule | 0 | 0 |
| Exact duplicate TS packets | 0 | 1,574 |
| Playback HTTP connections | 1 | 1 |

The reference ended with 44 partial trailing bytes; the candidate was packet
aligned. Both captures were cut at the fixed deadline, with no earlier EOF or
playback reconnect. Each recorded one read error at 1,200 seconds as the deadline
closed the active reader. HTTP status-poll responses are not counted as playback
connections. Duplicate packets remain reported; zero continuity errors does not
mean byte-for-byte identity or an absence of duplication.

Full-file FFmpeg audio/video decodes ended normally with exit 0 for both
captures. The reference decoded 30,638 video frames and produced 75 diagnostic
stderr lines; the candidate decoded 31,864 frames with one diagnostic line.
Reported output timelines were 1,225.84 and 1,274.80 seconds respectively. These
are offline decoder timelines, not wall-clock coverage certificates, realtime
playback durations or comparable throughput scores. Different retained history
and diagnostic counts do not establish relative playback quality.

The candidate logged one upstream-pool stale event and two connection rounds,
with only one provider open. It stayed alive and resumed delivery on the existing
HTTP response after the suspension. No evicted-window skip or live-edge resync
was logged. This establishes an injected pool-recovery observation, not the
specific mid-session eviction path.

The default request timeout is 1.5 seconds and pool-stale timeout is 12 seconds.
The pool loop checks staleness before the request/gap recovery helper, so a
75-second process suspension predictably exercises pool reconnect. It cannot
isolate `Continuity::skip_evicted_gap`.

## Short eviction-probe attempt

Because the long observation contained no natural evicted-window skip, one
separate bounded probe requested 120 seconds with fresh state and the same
defaults. It planned an eight-second candidate suspension after 40 seconds,
between the 1.5-second request timeout and the 12-second pool-stale budget. The
wrapper required recent candidate capture arrival and VLC decode progress before
injecting the pause, so an already stalled startup could not count as recovery.

That prerequisite failed: neither capture had received media and both VLC clients
still had zero decoded audio/video counters. The wrapper aborted at approximately
40 seconds without injecting the pause, stopped its clients and servers, and
restored WARP. This probe is invalid as a recovery observation. The shared lack
of decode does not establish a source-wide outage or its cause. No retry was run
to turn the failed prerequisite into an apparent success.

No field call to `skip_evicted_gap` was observed. The long injected outage reached
pool staleness before mid-session repair; the shorter attempt lacked the healthy
pre-pause baseline needed to interpret it. Even a healthy short suspension would
freeze window processing as well as requests, might be shorter than upstream
retention, and could be superseded by live-edge resync. These are the bounded
attempts and nonreproduction explanation for issue #1's explicit alternative;
the specific field eviction path remains unverified.

## Reference-player limitation

The reference VLC input received HTTP 500 during opening, remained stopped, and
returned no valid decode samples. Its 240 control/sample errors include statuses
without decoder statistics. Its capture client successfully obtained a playback
URL through the JSON route and recorded media, but that does not repair the
failed VLC input. The comparison runner exited 2 and marked the paired player
observation incomplete. The candidate observation above is usable on its own;
this run establishes no comparative VLC-quality result or source-wide failure.

## Deterministic recovery coverage

Existing tests independently verify that an evicted cursor skips only after the
request timeout, does not skip while an unchoked peer still covers it, advances
to the lowest covered piece, and arms the output keyframe gate. The relevant
tests are `skip_evicted_gap_jumps_to_lowest_covered_when_cursor_is_stranded`,
`skip_evicted_gap_does_not_skip_while_a_peer_still_covers_the_cursor`, and
`skip_evicted_gap_arms_output_keyframe_gate`. These synthetic results do not
substitute for a field observation. The separate live-edge resync runs before
the eviction helper and can supersede it when a newly learned head is far ahead.

## Verification and retained evidence

The private wrapper and numeric report are under `/tmp/outpace-robustness-1`;
VLC samples are retained in its separately reported private artifact directory.
The aborted short probe is retained separately under `/tmp/outpace-gap-1`.
Only reviewed numeric observations are copied into this note. The long media
files were parsed with the checked-in TS parser over a memory map, omitting only
its per-packet identity index. The numeric result matched the original parser
on seven synthetic continuity/alignment cases and each capture's bounded prefix.
No packet-identity comparison was performed.

Commands run with the repository's pinned toolchain:

```sh
cargo +1.99.1 build --release --locked -p ace-engine --bin outpace
cargo +1.99.1 test --workspace --locked
cargo +1.99.1 clippy --workspace --all-targets --locked -- -D warnings
cargo +1.99.1 fmt --all --check
python3 -m unittest discover -s tools/abcompare
python3 tools/hygiene/check_identifiers.py
git diff --check
```

The workspace suite passed 968 tests with seven ignored; the comparison harness
passed 33 tests. Clippy, formatting and identifier hygiene passed. No new exposed
defaults, runtime dependencies, Docker images or configuration were introduced.
