# Live Playback Prebuffer Design

Date: 2026-07-18

## Problem

Outpace can download a healthy public live swarm at the required average bitrate while still
delivering MPEG-TS in visible bursts. In a ten-minute direct-TS observation, contiguous media
periodically stopped advancing and then caught up, despite multiple active upstream peers, no
decoder overload, and almost no explicit piece retransmission. A reference-engine A/B observation
on the same live source showed similar bursty piece delivery but gave the player roughly 35 seconds
of playable media immediately, while outpace supplied roughly 10 seconds and subsequently ran with
only a 2–5 second lead.

The defect is therefore not average swarm throughput. Outpace publishes its small historical
piece window immediately and gives the player too little media to absorb normal piece-arrival
jitter. Native HLS makes this worse by retaining only six approximately one-second segments, so it
discards most of any larger startup burst.

## Goals

- Give direct MPEG-TS players an AceStream-like startup media cushion before live bytes are
  published.
- Give native and compatibility HLS clients a coherently retained startup cushion.
- Keep direct TS and HLS on one shared provider download and one startup gate.
- Bound startup memory and startup delay even when PCR or descriptor metadata is absent or bad.
- Preserve immediate runtime recovery: a discontinuity must not trigger another long prebuffer.
- Make the principal policy operator-configurable and allow the old immediate-publication behavior.
- Preserve all existing live recovery, keyframe gating, session sharing, lease, and idle-reaping
  behavior.

## Non-goals

- Transcoding, remuxing, or rewriting encoded timestamps.
- Claiming that server-side statistics can observe a player's decoder buffer.
- Eliminating public-swarm jitter or replacing the piece scheduler.
- Changing VOD behavior.
- Guaranteeing that every HLS client honors advisory `EXT-X-START` guidance.

## Configuration

Add the following validated settings:

| Environment variable | Default | Meaning |
|---|---:|---|
| `OUTPACE_PREBUFFER_MS` | `30000` | Target clean MPEG-TS duration accumulated before the first publication. `0` disables startup prebuffering. |
| `OUTPACE_PREBUFFER_BYTES` | `134217728` | Hard per-session byte ceiling for the startup reservoir. Must be at least one TS packet. |
| `OUTPACE_PREBUFFER_TIMEOUT_MS` | `15000` | Maximum wall-clock wait after the first clean TS byte before degraded release. Must be at least `1` when prebuffering is enabled. |
| `OUTPACE_HLS_STARTUP_SEGMENTS` | `6` | Number of completed HLS segments required before a cold playlist becomes ready. `0` means one segment, preserving the current minimum-ready behavior. |
| `OUTPACE_HLS_STARTUP_TIMEOUT_MS` | `45000` | Maximum cold-playlist wait for the configured completed-segment count. |

Change the default `OUTPACE_HLS_SEGMENT_DURATION_MS` from `1000` to `5000` and
`OUTPACE_HLS_WINDOW_SEGMENTS` from `6` to `8`. This gives a normal retained duration of roughly
40 seconds while keeping the conservative hard-ceiling memory class close to today's value:
`(8 + 1) * 65,536 * 188` is about 106 MiB rather than the roughly 455 MiB that a 36-by-one-second
window would reserve. Validate
`effective_startup_segments <= window_segments`, where the effective value is
`max(1, OUTPACE_HLS_STARTUP_SEGMENTS)`. The HLS startup timeout must be at least one configured
segment duration.

`OUTPACE_PREFETCH_PIECES` remains supported. Configuration must retain whether the variable was
explicitly supplied:

- explicit value: use that exact requested history depth, subject only to the upstream's real live
  window and existing safety bounds;
- absent value: derive a history depth for `OUTPACE_PREBUFFER_MS` from descriptor bitrate and
  piece payload geometry when bitrate is available, with two extra pieces for rounding/jitter;
- missing/untrusted bitrate: use a conservative fallback of 32 pieces;
- prebuffer disabled: preserve the existing fallback of 8 pieces.

The derived or explicit depth is always clamped to the peer's advertised minimum piece and to the
existing scheduler/reassembly limits. A short upstream window degrades to its oldest available
piece; it never makes startup impossible.

The 30-second target is based on the observed reference behavior, not protocol dogma. Live
acceptance must also measure 10- and 20-second overrides. Documentation should recommend the
smallest target that remains stable on the operator's sources.

## Architecture

### Startup source decorator

Add a narrowly scoped `StartupBufferedSource` in `ace-engine`. It implements `TsSource`, wraps the
Ace provider's live source, and is installed by the runtime before the source enters
`StreamSession`. It is not built into generic swarm reassembly and is not silently applied to test,
broadcast, or future provider types.

The decorator has two phases:

1. **Collecting:** receive clean contiguous `Bytes` from the wrapped source into a bounded
   `VecDeque<Bytes>`, observe complete 188-byte TS packets, and measure the reservoir.
2. **Released:** drain the queued chunks in original order as quickly as the session pump accepts
   them, then pass every subsequent source chunk through without buffering.

The gate runs exactly once per source instance. `take_discontinuity()` during collection clears all
pre-gap queued bytes and duration state, then resumes collection from the first post-gap chunk. A
discontinuity after release is passed through immediately and does not re-arm the startup gate.

Dropping the session or all owning manager state drops the decorator and its queued bytes without a
background task or detached lifetime.

### Duration measurement

The primary clock is PCR carried by MPEG-TS adaptation fields:

- lock to the first PCR PID in the current clean run;
- ignore PCR values from other PIDs;
- handle the 33-bit PCR base wrap as forward progress;
- treat a non-wrap backward jump or a transport discontinuity as a new clean run during startup;
- retain partial TS packets between source chunks without counting them as media duration;
- never count malformed packets or raw byte length as PCR duration.

The implementation should reuse or extract the clock-selection and wrap helpers already exercised
by the HLS packager instead of creating subtly different timestamp rules.

Readiness occurs on the first of:

1. selected-PCR span reaches `OUTPACE_PREBUFFER_MS`;
2. reservoir reaches `OUTPACE_PREBUFFER_BYTES`;
3. `OUTPACE_PREBUFFER_TIMEOUT_MS` elapses after the first clean packet.

If PCR is unavailable, a trustworthy positive descriptor bitrate may estimate queued duration as
`queued_bytes * 8 / bitrate`. This estimate may satisfy the duration target. Without PCR or usable
bitrate, only the byte ceiling or startup deadline releases the reservoir. Every degraded release
uses `alog!` with the `[prebuffer]` tag and states the reason without including a content id,
infohash, or stream name.

`SourceStats.buffer_ms` means only the currently server-resident contiguous media duration while
collecting/draining. It approaches zero after the initial burst and must not be documented as
player lead. Status JSON may add explicit startup state and target fields if that is necessary to
avoid ambiguity, but existing fields remain compatible.

### Session and provider wiring

The runtime constructs the Ace provider with a prefetch policy that distinguishes explicit from
derived depth. Within `AceProvider::open`, after metadata resolution and live-source creation, the
Ace path wraps that source once with `StartupBufferedSource` before returning it to
`StreamSession::start`.

Direct TS subscriptions and HLS's raw receiver therefore observe the identical ordered startup
burst. Multiple clients never create multiple reservoirs or provider downloads. A late direct
subscriber keeps current live-edge semantics and receives only future broadcast events.

A late-created HLS packager also receives only future broadcast events. It must build its own HLS
startup window from that point; it must not attempt to replay the already-drained source reservoir.

### HLS readiness, retention, and start position

The HLS packager retains `window_segments` completed segments as today but does not report a cold
playlist ready until `effective_startup_segments` clean completed segments exist. The existing
retryable startup failure remains the contract, but its deadline becomes
`OUTPACE_HLS_STARTUP_TIMEOUT_MS`: if the configured count cannot be reached in time, HTTP returns
the existing retryable failure rather than an empty or falsely buffered playlist. The 45-second
default allows a late-created HLS packager to build six five-second segments even though broadcast
subscribers receive no earlier direct-session history.

Render standard advisory guidance:

```text
#EXT-X-START:TIME-OFFSET=-<seconds>,PRECISE=NO
```

Compute the offset from retained segment durations, not `segment_count * target_duration`. Clamp
the chosen start so that:

- it does not select the oldest segment when that segment is at immediate eviction risk;
- at least three target durations remain between the advised start and the live edge;
- short or irregular windows still produce a valid in-window offset;
- omit the tag when the retained duration cannot satisfy those constraints.

The tag is advisory. A client that ignores it retains its normal HLS live-start behavior; server
correctness cannot depend on compliance.

### HLS discontinuities

Keep completed pre-gap segments in the sliding window. Clear only the incomplete current run and
mark the first complete post-gap segment with `#EXT-X-DISCONTINUITY`, matching current behavior.

Add a `discontinuity_sequence` counter to packager state. Whenever window eviction removes a
segment whose `discontinuity` flag is true, increment the counter. Render
`#EXT-X-DISCONTINUITY-SEQUENCE:<n>` whenever the counter is nonzero. Native and compatibility
playlist prefixes must render the same media sequence and discontinuity sequence.

## Memory and failure behavior

- Startup source memory is bounded by `OUTPACE_PREBUFFER_BYTES` plus at most one incoming source
  chunk and one partial TS packet.
- The existing provider `mpsc(256)` and session broadcast capacity are not treated as memory
  bounds for the reservoir.
- HLS retained-memory validation continues to use checked arithmetic over
  `(window_segments + 1) * segment_packets * 188`; the new defaults must pass the existing
  conservative target-specific limit.
- A short live window, unavailable PCR, bad bitrate, slow swarm, or early discontinuity causes a
  logged degraded release, not an infinite wait or unbounded allocation.
- Source EOF during collection drains any clean queued bytes, then ends normally.
- Zero prebuffer bypasses collection without changing byte ordering or discontinuity behavior.

## Testing

All implementation follows red-green-refactor.

### Startup buffer unit tests

- withhold chunks until a PCR span reaches the target, then release them in exact order;
- `0` target passes the first chunk immediately;
- PCR wrap counts forward;
- unrelated PCR PIDs are ignored;
- backward PCR resets the startup run;
- PCR arriving late measures only the selected clean span;
- no-PCR data releases through bitrate estimation when available;
- no-PCR/no-bitrate data releases at byte cap and at deadline;
- memory never exceeds the configured cap plus documented overhead;
- startup discontinuity discards every pre-gap byte;
- runtime discontinuity does not re-arm the gate;
- source EOF drains queued clean data;
- dropping the consumer while collecting drops the wrapped source and queue.

### Prefetch tests

- descriptor bitrate and piece payload derive the expected rounded piece count;
- fallback is 32 pieces with prebuffering and 8 pieces without it;
- explicit `OUTPACE_PREFETCH_PIECES` remains exact;
- derived depth clamps to a short peer window and scheduler/reassembly bounds;
- narrow windows still start and degrade rather than waiting forever.

### Shared-session tests

- direct and HLS consumers share one provider open and one startup gate;
- multiple direct consumers do not multiply reservoir memory;
- a late HLS packager waits for its own configured completed-segment count;
- session teardown during collection leaves no live background download.

### HLS tests

- readiness waits for the configured number of segments;
- startup count greater than the retained window is rejected;
- startup timeout shorter than one segment duration is rejected;
- `EXT-X-START` uses real irregular durations and remains safely inside the window;
- short windows omit or clamp the tag correctly;
- a client-visible playlist retains the configured startup duration;
- repeated discontinuities and evictions advance `EXT-X-DISCONTINUITY-SEQUENCE` exactly once per
  evicted marker;
- native and compatibility playlists share sequence state;
- completed pre-gap segments survive a runtime discontinuity.

### Configuration and compatibility tests

- defaults and every environment override parse and validate;
- zero prebuffer preserves immediate publication;
- checked arithmetic rejects impossible HLS memory settings;
- existing direct TS keyframe start, lag reset, HLS lifecycle, leases, and idle reaping remain
  unchanged.

## Live acceptance

Use a current public content id resolved from the gitignored registry at runtime. Never put its
value, derived infohash, or stream name in tracked files, commands saved to the repository, logs
committed to the repository, issue text, commit messages, or PR text.

Run the reference engine and Outpace under overlapping or interleaved network conditions without
running competing trials that interfere with one another. Evaluate direct TS and native HLS with
the same corrected real-time-paced decoder harness. The harness must continuously drain FFmpeg
stderr, enforce a terminal watchdog, and exclude intentional cold-start withholding from gap
scoring.

First benchmark the reference engine with three valid trials and use the maximum post-start
advancing-media gap across them as the comparative baseline. A candidate trial passes the gap
criterion when its maximum gap is no greater than `reference maximum + 0.5 seconds`.

Then test `OUTPACE_PREBUFFER_MS` in ascending order: 10000, 20000, 30000. For each candidate, run
three ten-minute direct-TS trials followed by three ten-minute native-HLS trials. Stop at the first
candidate whose six trials all pass; do not run larger candidates after a full pass. Invalid
trials do not count and must be rerun.

Every valid trial must decode at least 599 seconds, exit normally, and have no terminal freeze.
For each path record only redacted diagnostics:

- time to first playable media;
- initial playable media depth;
- minimum steady-state lead and whether it progressively depletes;
- maximum post-start advancing-media gap and comparison with the reference allowance;
- discontinuities, fan-out lag, live skips, and piece retransmissions;
- peak process/reservoir/HLS memory.

Lead must not show progressive trial-long depletion: compare early, middle, and late steady-state
windows and reject a sustained downward trend that ends without recovery. HLS additionally
requires readiness with the configured startup-segment count, a valid `EXT-X-START`, and complete
retrieval/decoding through the requested duration. Run the controlled degraded fixture to prove
byte-limit/deadline release, bounded memory, and redacted degraded logging.

The accepted target is the smallest ascending candidate whose three direct and three HLS trials
all satisfy these comparative and completion criteria.

Before completion, run:

```text
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
python tools/hygiene/check_identifiers.py
git diff --check
```

## Documentation impact

Update `README.md` and `docs/native-api.md` with the new startup behavior, exact environment
variables, memory/latency trade-offs, the server-resident meaning of `buffer_ms`, HLS advisory-start
limitations, and the `0` compatibility setting. Do not include any real identifier, infohash, or
stream name.
