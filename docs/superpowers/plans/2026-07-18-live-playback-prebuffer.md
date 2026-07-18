# Live Playback Prebuffer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give direct MPEG-TS and HLS playback a bounded AceStream-like startup cushion so normal live-piece jitter does not exhaust the player buffer.

**Architecture:** Add a startup-only `TsSource` decorator that measures clean MPEG-TS duration, releases one ordered burst, and never re-gates runtime discontinuities. Coordinate its target with Ace live-piece history selection, then make HLS retain and advertise a comparable multi-segment cushion with correct discontinuity sequence accounting.

**Tech Stack:** Rust 2024, Tokio, Axum, MPEG-TS PCR parsing, native/compatibility HLS, existing workspace tests and live FFmpeg acceptance tooling.

## Global Constraints

- Real content ids, infohashes, and stream names never enter tracked files, commit messages, issues, PRs, or saved repository logs.
- `ace-log`'s `alog!` is the only operational logging macro; use `[prebuffer]` for startup-buffer events.
- The startup reservoir is bounded to the configured bytes plus one incoming chunk and one partial 188-byte packet.
- The startup gate runs once; a runtime discontinuity never causes another long prebuffer.
- Direct TS and HLS share one provider download and one startup source decorator.
- An explicit `OUTPACE_PREFETCH_PIECES` value remains exact before upstream-window clamping.
- HLS startup segments never exceed the retained window.
- VOD behavior remains unchanged.
- No new runtime dependency is required.

---

## File map

- `crates/ace-media/src/mpegts.rs` — reusable selected-PCR clock and TS timing parser.
- `crates/ace-engine/src/config.rs` — startup-buffer and expanded HLS policy types/defaults/validation.
- `crates/ace-engine/src/runtime.rs` — environment parsing and runtime wiring.
- `crates/ace-engine/src/startup_buffer.rs` — bounded startup-only `TsSource` decorator.
- `crates/ace-engine/src/lib.rs` — module declaration.
- `crates/ace-engine/src/ace_provider.rs` — derived historical piece depth and source wrapping.
- `crates/ace-engine/src/hls.rs` — startup-segment readiness, advisory start, and discontinuity sequence.
- `crates/ace-engine/src/http.rs` — configured playlist startup timeout.
- `crates/ace-engine/src/manager.rs` — pass complete HLS configuration unchanged to packagers.
- `README.md`, `docs/native-api.md` — configuration and behavior documentation.

### Task 1: Share a correct selected-PCR media clock

**Files:**
- Modify: `crates/ace-media/src/mpegts.rs`
- Modify: `crates/ace-engine/src/hls.rs`

**Interfaces:**
- Produces: `pub struct TsTiming { pub pid: u16, pub pcr: Option<u64>, pub discontinuity: bool }`.
- Produces: `pub fn ts_timing(packet: &[u8; 188]) -> TsTiming`.
- Produces: `pub struct SelectedPcrClock` with `new`, `reset`, `observe`, and `elapsed`.
- `SelectedPcrClock::observe(&mut self, timing: TsTiming) -> ClockObservation` returns `Ignored`, `Advanced(Duration)`, or `Reset`.
- Consumes: HLS replaces its private PCR PID/start/last selection logic with this shared clock without changing segment behavior.

- [ ] **Step 1: Write failing media-clock tests**

Add focused tests beside `ace-media::mpegts`:

```rust
#[test]
fn selected_pcr_clock_ignores_other_pids_and_handles_wrap() {
    let mut clock = SelectedPcrClock::new();
    assert_eq!(clock.observe(timing(0x100, Some(PCR_MODULUS - 45_000))), ClockObservation::Advanced(Duration::ZERO));
    assert_eq!(clock.observe(timing(0x101, Some(90_000))), ClockObservation::Ignored);
    assert_eq!(clock.observe(timing(0x100, Some(45_000))), ClockObservation::Advanced(Duration::from_secs(1)));
}

#[test]
fn selected_pcr_clock_resets_on_backward_jump_or_discontinuity() {
    let mut clock = SelectedPcrClock::new();
    clock.observe(timing(0x100, Some(180_000)));
    assert_eq!(clock.observe(timing(0x100, Some(90_000))), ClockObservation::Reset);
    clock.observe(timing(0x100, Some(180_000)));
    assert_eq!(clock.observe(TsTiming { discontinuity: true, ..timing(0x100, Some(270_000)) }), ClockObservation::Reset);
}
```

- [ ] **Step 2: Verify RED**

Run: `cargo test -p ace-media selected_pcr_clock -- --nocapture`

Expected: compilation fails because `SelectedPcrClock`, `ClockObservation`, and public `TsTiming` do not exist.

- [ ] **Step 3: Implement the minimal shared clock**

Move the packet adaptation-field parser and PCR wrap arithmetic from `ace-engine/src/hls.rs` into `ace-media/src/mpegts.rs`. Implement state equivalent to:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockObservation {
    Ignored,
    Advanced(Duration),
    Reset,
}

#[derive(Debug, Default)]
pub struct SelectedPcrClock {
    pid: Option<u16>,
    start: Option<u64>,
    last: Option<u64>,
}

impl SelectedPcrClock {
    pub fn new() -> Self { Self::default() }
    pub fn reset(&mut self) { *self = Self::default(); }
    pub fn elapsed(&self) -> Option<Duration> {
        Some(Duration::from_secs_f64(pcr_delta(self.start?, self.last?)? as f64 / 90_000.0))
    }
    pub fn observe(&mut self, timing: TsTiming) -> ClockObservation {
        if timing.discontinuity { self.reset(); return ClockObservation::Reset; }
        let Some(pcr) = timing.pcr else { return ClockObservation::Ignored; };
        match self.pid {
            Some(pid) if pid != timing.pid => return ClockObservation::Ignored,
            None => self.pid = Some(timing.pid),
            _ => {}
        }
        if self.last.is_some_and(|last| pcr_delta(last, pcr).is_none()) {
            self.reset();
            return ClockObservation::Reset;
        }
        self.start.get_or_insert(pcr);
        self.last = Some(pcr);
        ClockObservation::Advanced(self.elapsed().unwrap_or_default())
    }
}
```

- [ ] **Step 4: Migrate HLS to the shared parser/clock**

Replace private `TsTiming`, `ts_timing`, `pcr_delta`, `pcr_went_backwards`, and PID-selection state with imports from `ace_media::mpegts`. Preserve the existing segment start/last PCR values where boundary carry-over is required; use the shared parser and wrap decision as the sole interpretation rule.

- [ ] **Step 5: Verify GREEN and unchanged HLS semantics**

Run:

```text
cargo test -p ace-media selected_pcr_clock
cargo test -p ace-engine --lib hls::tests
```

Expected: all selected-clock tests and all existing HLS tests pass.

- [ ] **Step 6: Commit**

```text
git add crates/ace-media/src/mpegts.rs crates/ace-engine/src/hls.rs
git commit -m "refactor(ace-media): share selected pcr clock"
```

### Task 2: Add validated startup and HLS policy configuration

**Files:**
- Modify: `crates/ace-engine/src/config.rs`
- Modify: `crates/ace-engine/src/runtime.rs`

**Interfaces:**
- Produces: `StartupBufferConfig { target_ms: u64, max_bytes: usize, timeout_ms: u64 }` with `Default`, `target()`, `timeout()`, and `validate()`.
- Extends: `HlsConfig` with `startup_segments: usize` and `startup_timeout_ms: u64`.
- Changes: `Config.prefetch_pieces` from `u64` to `Option<u64>`; `None` means derived policy, `Some(n)` means exact operator value.
- Runtime passes `StartupBufferConfig` and optional prefetch depth into `AceProvider`.

- [ ] **Step 1: Write failing configuration tests**

Add tests proving exact defaults and invalid combinations:

```rust
#[test]
fn playback_buffer_defaults_match_reference_candidate() {
    let c = Config::default();
    assert_eq!(c.startup_buffer.target_ms, 30_000);
    assert_eq!(c.startup_buffer.max_bytes, 134_217_728);
    assert_eq!(c.startup_buffer.timeout_ms, 15_000);
    assert_eq!(c.prefetch_pieces, None);
    assert_eq!(c.hls.segment_duration_ms, 5_000);
    assert_eq!(c.hls.window_segments, 8);
    assert_eq!(c.hls.startup_segments, 6);
    assert_eq!(c.hls.startup_timeout_ms, 45_000);
}

#[test]
fn hls_rejects_startup_count_past_window_and_short_timeout() {
    let mut hls = HlsConfig::default();
    hls.startup_segments = hls.window_segments + 1;
    assert!(hls.validate().unwrap_err().contains("OUTPACE_HLS_STARTUP_SEGMENTS"));
    hls = HlsConfig::default();
    hls.startup_timeout_ms = hls.segment_duration_ms - 1;
    assert!(hls.validate().unwrap_err().contains("OUTPACE_HLS_STARTUP_TIMEOUT_MS"));
}
```

Add serialized environment tests using the existing environment-lock pattern for every new variable and an assertion that absent versus explicit `OUTPACE_PREFETCH_PIECES=8` yields `None` versus `Some(8)`.

- [ ] **Step 2: Verify RED**

Run: `cargo test -p ace-engine --lib config::tests runtime::tests -- --nocapture`

Expected: compilation fails on the missing fields and policy type.

- [ ] **Step 3: Implement configuration structs and validation**

Add the exact defaults from the spec. Validation rules:

```rust
if self.target_ms > 0 && self.timeout_ms == 0 {
    return Err("OUTPACE_PREBUFFER_TIMEOUT_MS must be >= 1 when OUTPACE_PREBUFFER_MS > 0".into());
}
if self.max_bytes < 188 {
    return Err("OUTPACE_PREBUFFER_BYTES must be >= 188".into());
}
let effective_startup = self.startup_segments.max(1);
if effective_startup > self.window_segments {
    return Err("OUTPACE_HLS_STARTUP_SEGMENTS must be <= OUTPACE_HLS_WINDOW_SEGMENTS".into());
}
if self.startup_timeout_ms < self.segment_duration_ms {
    return Err("OUTPACE_HLS_STARTUP_TIMEOUT_MS must be >= OUTPACE_HLS_SEGMENT_DURATION_MS".into());
}
```

Retain all existing checked HLS allocation validation.

- [ ] **Step 4: Parse and wire environment values**

Parse:

```text
OUTPACE_PREBUFFER_MS
OUTPACE_PREBUFFER_BYTES
OUTPACE_PREBUFFER_TIMEOUT_MS
OUTPACE_HLS_STARTUP_SEGMENTS
OUTPACE_HLS_STARTUP_TIMEOUT_MS
```

Set `config.prefetch_pieces = Some(v.parse()?)` only when `OUTPACE_PREFETCH_PIECES` exists. Call both startup-buffer and HLS validation before runtime construction.

- [ ] **Step 5: Verify GREEN**

Run: `cargo test -p ace-engine --lib config::tests runtime::tests`

Expected: all configuration/runtime tests pass.

- [ ] **Step 6: Commit**

```text
git add crates/ace-engine/src/config.rs crates/ace-engine/src/runtime.rs
git commit -m "feat(ace-engine): configure live startup buffering"
```

### Task 3: Implement the bounded startup-only source decorator

**Files:**
- Create: `crates/ace-engine/src/startup_buffer.rs`
- Modify: `crates/ace-engine/src/lib.rs`
- Modify: `crates/ace-engine/src/provider.rs`

**Interfaces:**
- Produces: `StartupBufferedSource::new(inner: Box<dyn TsSource>, config: StartupBufferConfig, bitrate: Option<u64>) -> Box<dyn TsSource>`.
- Produces: internal `StartupReservoir` with deterministic `push`, `discontinuity`, `ready_reason`, and ordered drain behavior.
- Preserves: inner `metadata`, `stats`, and discontinuities; only `buffer_ms` is replaced with server-resident queued duration while nonzero.

- [ ] **Step 1: Write failing reservoir tests**

Use synthetic 188-byte PCR packets and a test `TsSource` to cover one behavior per test:

```rust
#[tokio::test(start_paused = true)]
async fn withholds_until_target_then_releases_exact_bytes_in_order() {
    let source = chunks_with_pcr(&[(0, b'a'), (90_000, b'b'), (180_000, b'c')]);
    let mut buffered = StartupBufferedSource::new(source, config(2_000, 4096, 10_000), None);
    let first = buffered.next().await.unwrap();
    assert_eq!(first, packet_bytes(b'a'));
    assert_eq!(buffered.next().await.unwrap(), packet_bytes(b'b'));
    assert_eq!(buffered.next().await.unwrap(), packet_bytes(b'c'));
}

#[tokio::test]
async fn startup_discontinuity_discards_pre_gap_but_runtime_gap_never_regates() {
    let source = discontinuous_fixture_source();
    let mut buffered = StartupBufferedSource::new(source, config(1_000, 4096, 10_000), None);
    assert_eq!(buffered.next().await.unwrap(), post_gap_first_chunk());
    assert!(buffered.take_discontinuity());
    assert_eq!(buffered.next().await.unwrap(), runtime_post_gap_chunk());
}
```

Add separate tests for zero target, PCR wrap, other PID, backward jump, late PCR, bitrate fallback, byte-cap release, paused-time deadline release, EOF drain, memory bound, and drop propagation.

- [ ] **Step 2: Verify RED**

Run: `cargo test -p ace-engine --lib startup_buffer::tests -- --nocapture`

Expected: compilation fails because the module and decorator do not exist.

- [ ] **Step 3: Implement `StartupReservoir`**

Use `VecDeque<Bytes>`, a `SelectedPcrClock`, a `[u8; 188]` partial-packet buffer, queued-byte accounting, first-clean-byte `tokio::time::Instant`, and a release enum:

```rust
enum ReleaseReason { TargetDuration, ByteLimit, Deadline, EndOfStream, Disabled }
enum Phase { Collecting, Draining(ReleaseReason), Released }
```

Before appending a chunk that would cross `max_bytes`, transition to draining and retain the new chunk as the documented single-chunk overhead. On a startup clock reset/discontinuity, clear the queue, byte estimate, partial packet, and deadline origin. Log only degraded `ByteLimit`, `Deadline`, or `EndOfStream` releases via `alog!("[prebuffer] ...")` without stream identifiers.

- [ ] **Step 4: Implement the `TsSource` decorator**

`next()` loops while collecting, drains queued chunks in FIFO order, then delegates. `take_discontinuity()` consumes inner discontinuities during collection and exposes exactly one discontinuity before the first post-gap released chunk; after release it delegates immediately. `stats()` clones inner stats and sets `buffer_ms` from PCR or bitrate-estimated queued duration. `metadata()` delegates unchanged.

- [ ] **Step 5: Verify GREEN and source compatibility**

Run:

```text
cargo test -p ace-engine --lib startup_buffer::tests
cargo test -p ace-engine --lib session::tests provider::tests
```

Expected: all decorator, session, and provider tests pass.

- [ ] **Step 6: Commit**

```text
git add crates/ace-engine/src/startup_buffer.rs crates/ace-engine/src/lib.rs crates/ace-engine/src/provider.rs
git commit -m "feat(ace-engine): buffer clean live startup media"
```

### Task 4: Coordinate Ace historical prefetch and install one decorator

**Files:**
- Modify: `crates/ace-engine/src/ace_provider.rs`
- Modify: `crates/ace-engine/src/runtime.rs`

**Interfaces:**
- Produces: `fn derived_prefetch_pieces(target_ms: u64, bitrate: Option<u64>, piece_length: u64, sig_len: usize) -> u64`.
- Changes: `AceProvider::with_prefetch_pieces` accepts `Option<u64>` and preserves explicitness.
- Adds: `AceProvider::with_startup_buffer(StartupBufferConfig)`.
- `AceProvider::open` computes the resolved policy, uses it in `SeedConfig`, and returns one `StartupBufferedSource` around `AceSource`.

- [ ] **Step 1: Write failing prefetch-policy tests**

```rust
#[test]
fn derives_history_from_target_bitrate_and_payload_with_two_piece_margin() {
    assert_eq!(derived_prefetch_pieces(30_000, Some(8_000_000), 1_048_576, 96), 31);
}

#[test]
fn prefetch_fallback_depends_on_whether_prebuffer_is_enabled() {
    assert_eq!(derived_prefetch_pieces(30_000, None, 1_048_576, 96), 32);
    assert_eq!(derived_prefetch_pieces(0, None, 1_048_576, 96), 8);
}

#[test]
fn explicit_prefetch_override_is_not_reinterpreted() {
    let provider = test_provider().with_prefetch_pieces(Some(3));
    assert_eq!(provider.prefetch_policy_for(&info()), 3);
}
```

The arithmetic is `ceil(target_ms * bitrate / 8000 / media_payload_per_piece) + 2`, using checked `u128`; payload is `piece_length - sig_len`, at least one byte.

- [ ] **Step 2: Verify RED**

Run: `cargo test -p ace-engine --lib ace_provider::tests::prefetch -- --nocapture`

Expected: tests fail because the derived policy and optional override do not exist.

- [ ] **Step 3: Implement derived/exact prefetch selection**

Store `prefetch_pieces: Option<u64>` and `startup_buffer: StartupBufferConfig` on `AceProvider`. After resolving `StreamInfo`, select the explicit value or call `derived_prefetch_pieces`. Keep `LivePicker`/`Continuity::fresh` as the final clamp to the advertised upstream minimum. Clamp the selected depth to `live_recovery.max_reasm_pieces_ahead` before constructing `SeedConfig`.

- [ ] **Step 4: Wrap the source once**

Construct `AceSource`, then return:

```rust
Ok(StartupBufferedSource::new(
    Box::new(source),
    self.startup_buffer,
    info.metadata.bitrate,
))
```

Ensure the spawned follow task receives the already-selected prefetch depth and no second decorator exists in `StreamManager` or HLS.

- [ ] **Step 5: Add shared-session integration coverage**

Extend provider/manager tests to assert one provider open when direct and HLS start concurrently, one startup release, exact byte order for both receivers, late-HLS independent segment accumulation, and prompt source drop when the session is stopped during collection.

- [ ] **Step 6: Verify GREEN**

Run:

```text
cargo test -p ace-engine --lib ace_provider::tests
cargo test -p ace-engine --lib manager::tests session::tests
```

Expected: all targeted tests pass.

- [ ] **Step 7: Commit**

```text
git add crates/ace-engine/src/ace_provider.rs crates/ace-engine/src/runtime.rs crates/ace-engine/src/manager.rs
git commit -m "feat(ace-engine): coordinate live history with prebuffer"
```

### Task 5: Retain and advertise the HLS startup cushion

**Files:**
- Modify: `crates/ace-engine/src/hls.rs`
- Modify: `crates/ace-engine/src/http.rs`
- Modify: `crates/ace-engine/src/manager.rs`

**Interfaces:**
- `HlsPackager` stores `startup_segments` and notifies readiness only when `segments.len() >= max(1, startup_segments)`.
- `HlsState` adds `discontinuity_sequence: u64`.
- `HlsPackager::startup_timeout() -> Duration` exposes the configured HTTP wait.
- Playlist rendering adds optional `EXT-X-START` and nonzero `EXT-X-DISCONTINUITY-SEQUENCE`.

- [ ] **Step 1: Write failing readiness and playlist tests**

Add tests:

```rust
#[tokio::test]
async fn readiness_waits_for_configured_completed_segment_count() {
    let p = pkg_with(HlsConfig { startup_segments: 3, ..small_hls_config() });
    feed_complete_segments(&p, 2);
    assert!(!p.is_ready());
    feed_complete_segments(&p, 1);
    assert!(p.wait_ready(Duration::from_millis(20)).await);
}

#[test]
fn playlist_start_uses_irregular_retained_durations_inside_safe_edge() {
    let p = pkg_with_durations(&[4.0, 6.0, 5.0, 7.0, 4.0, 6.0, 5.0, 7.0]);
    let playlist = p.playlist("ace", "synthetic");
    assert!(playlist.contains("#EXT-X-START:TIME-OFFSET=-29.000,PRECISE=NO"));
}

#[test]
fn evicting_marked_segments_advances_discontinuity_sequence_once() {
    let p = discontinuous_sliding_pkg();
    assert!(p.playlist("ace", "synthetic").contains("#EXT-X-DISCONTINUITY-SEQUENCE:1"));
}
```

Use only obviously synthetic ids in test URLs.

- [ ] **Step 2: Verify RED**

Run: `cargo test -p ace-engine --lib hls::tests -- --nocapture`

Expected: new readiness/start/discontinuity-sequence assertions fail.

- [ ] **Step 3: Implement segment-count readiness**

Store the effective startup count. In `emit`, notify when pushing a segment changes readiness from false to true. `is_ready` checks the count. Preserve the notify-before-wait race protection in `wait_ready`.

- [ ] **Step 4: Implement safe advisory start calculation**

From retained `HlsSegment.duration` values, choose the earliest segment boundary that leaves at least `3 * target_duration` before the edge while skipping the oldest boundary when more than one candidate exists. Return its negative suffix duration. Omit `EXT-X-START` when no interior boundary satisfies the safety distance. Render the tag after target duration and before media sequence.

- [ ] **Step 5: Implement discontinuity sequence accounting**

When `emit` evicts a front segment, increment `discontinuity_sequence` if its flag is true, then increment `media_seq`. Render the nonzero sequence immediately after media sequence. Keep completed pre-gap segments and mark only the first post-gap completed segment as today.

- [ ] **Step 6: Use configured HTTP startup timeout**

Remove the fixed `HLS_PLAYLIST_STARTUP_TIMEOUT`. Native and compatibility handlers pass `manager.hls_config().startup_timeout()` (or `pkg.startup_timeout()`) into the existing readiness helper/select branch. Preserve `404` cancellation and retryable `503` timeout behavior.

- [ ] **Step 7: Verify GREEN**

Run:

```text
cargo test -p ace-engine --lib hls::tests
cargo test -p ace-engine --lib http::tests manager::tests
```

Expected: all HLS, HTTP, lifecycle, native/compatibility sharing, and cancellation tests pass.

- [ ] **Step 8: Commit**

```text
git add crates/ace-engine/src/hls.rs crates/ace-engine/src/http.rs crates/ace-engine/src/manager.rs
git commit -m "fix(hls): retain the live startup cushion"
```

### Task 6: Document, run gates, and perform live acceptance

**Files:**
- Modify: `README.md`
- Modify: `docs/native-api.md`
- Optional ignored diagnostics only: `/tmp/outpace-prebuffer-*`

**Interfaces:**
- Documents all new variables, changed HLS defaults, startup delay/memory trade-offs, `buffer_ms` semantics, degraded release, and `0` compatibility behavior.

- [ ] **Step 1: Write documentation**

Document exact defaults:

```text
OUTPACE_PREBUFFER_MS=30000
OUTPACE_PREBUFFER_BYTES=134217728
OUTPACE_PREBUFFER_TIMEOUT_MS=15000
OUTPACE_HLS_SEGMENT_DURATION_MS=5000
OUTPACE_HLS_WINDOW_SEGMENTS=8
OUTPACE_HLS_STARTUP_SEGMENTS=6
OUTPACE_HLS_STARTUP_TIMEOUT_MS=45000
```

State that `buffer_ms` is server-resident queued duration, not decoder/player lead, and that HLS `EXT-X-START` is advisory.

- [ ] **Step 2: Run focused and workspace gates**

Run:

```text
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
python tools/hygiene/check_identifiers.py
git diff --check
```

Expected: every command exits 0; only documented live-network tests are ignored.

- [ ] **Step 3: Build the acceptance binary**

Run: `cargo build --release -p ace-engine --bin outpace`

Expected: release build exits 0.

- [ ] **Step 4: Run direct-TS live trials**

Resolve the current registry entry only into an interactive environment variable; never place it in a saved command or tracked file. Run three ten-minute real-time-paced decoder trials for each `OUTPACE_PREBUFFER_MS` value `10000`, `20000`, and `30000`. Save redacted timing/progress and engine logs under `/tmp`, record startup depth, stalls >=2 seconds after first media, minimum lead, re-requests, skips, discontinuities, and peak RSS.

Expected acceptance: choose the smallest target with zero post-start stalls >=2 seconds in all three trials and no progressive lead depletion.

- [ ] **Step 5: Run HLS live trials**

Repeat three ten-minute trials against `.m3u8` with the chosen direct target. Confirm playlists contain six retained startup segments before readiness, `EXT-X-START` is valid, segment downloads remain ahead of real-time playback, and no post-start media-clock stall >=2 seconds occurs.

- [ ] **Step 6: Run degraded-window acceptance**

Using a controlled fixture or currently short advertised live window, verify the byte/deadline path releases within configured bounds, memory stays below the reservoir ceiling plus documented overhead, and logs show one redacted `[prebuffer]` degraded-release reason.

- [ ] **Step 7: Commit documentation and any acceptance-driven default adjustment**

If 10 or 20 seconds passes the full acceptance matrix, first update defaults/tests/docs/spec rationale consistently. Then:

```text
git add README.md docs/native-api.md crates/ace-engine/src/config.rs crates/ace-engine/src/runtime.rs
git commit -m "docs: explain live playback prebuffering"
```

- [ ] **Step 8: Final branch verification**

Run:

```text
git merge-base --is-ancestor main HEAD
git status --short --branch
git log --oneline main..HEAD
python tools/hygiene/check_identifiers.py
```

Expected: base check exits 0, worktree is clean, branch name is `fix/live-playback-prebuffer`, and hygiene exits 0.
