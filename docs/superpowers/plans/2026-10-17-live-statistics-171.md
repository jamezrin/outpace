# Live Statistics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace placeholder live rates and stale buffer snapshots with observable statistics for #171.

**Architecture:** An optional independent stats reader keeps the session observable while `next()` is pending. Ace publishes bounded rate history and position observations; the startup decorator overlays reservoir duration. HTTP maps snapshots into documented native and compat fields.

**Tech Stack:** Rust 1.99.1, existing Tokio/axum primitives, standard-library synchronization; existing Python comparison tools.

**Spec:** [Approved design](../specs/2026-10-17-live-statistics-171-design.md).

## Global Constraints

- Preserve playback, discovery, buffering-limit and listener defaults; add no dependencies.
- Use a nominal five-second rolling window sampled at one-second intervals, independently of HTTP reads.
- Keep existing byte-total meanings; measured download payload excludes signatures, framing and injected tables.
- `buffer_ms` is stage-specific contiguous media, never player lead or total daemon buffering.
- Unknown timestamps are null; before position discovery, `livepos` is null.
- Use synthetic identifiers in tracked artifacts. Supply live sources manually through the environment; scripts never read the registry.
- Run controlled local validation with WARP off and restore it afterward; retain raw evidence privately.

## Review Focus

- Multiple simultaneous status clients must not change rate history or measurements: Task 1 read-purity test.
- Cancellation during collection must not leave a sampler running: Task 3 cancellation test.
- A later piece beyond a hole must not inflate playable buffer: Task 2 readiness test.
- Reset or wrapping counters must not become huge rates: Task 1 reset test.
- No-data or missing-reference live observations must not become acceptance passes: Task 5 validity checks.

## File responsibilities

`provider.rs` owns the reader/snapshot contract. New `live_stats.rs` owns rate history, Ace snapshot publication and sampler lifetime. `lib.rs` registers that module. `ace_provider.rs` accounts media and publishes positions; `ace-wire/src/reassembly.rs` provides a read-only contiguous-ready iterator. `startup_buffer.rs` owns its overlay; `session.rs` selects the reader. `http.rs` owns JSON mapping. API/compat docs and a short protocol note describe semantics and evidence.

### Task 1: Independent reader and bounded rate measurements

**Files:** Modify `crates/ace-engine/src/provider.rs`, `crates/ace-engine/src/lib.rs`; create `crates/ace-engine/src/live_stats.rs`. Tests live beside their implementation. Update existing `SourceStats` literals as required, without changing their prior values.

**Interfaces:** In `provider.rs`, add `StatsReader: Send + Sync` with `fn snapshot(&self) -> SourceStats`, alias `StatsHandle = Arc<dyn StatsReader>`, and default `TsSource::stats_handle(&self) -> Option<StatsHandle>` returning `None`. Extend `SourceStats` with integer `download_rate`, `upload_rate` (bytes/second) and `livepos: Option<LivePositionStats>`. Define `LivePositionStats` with optional unsigned `pos`, `last`, `live_first`, `live_last`, and unsigned `buffer_pieces`; derive the existing snapshot traits. Timestamp JSON is owned by Task 4, not synthesized here.

**Interfaces:** `live_stats.rs` defines `RateMeter::new(now: tokio::time::Instant, total: u64)`, `observe(&mut self, now: Instant, total: u64)`, and pure `rate(&self, now: Instant, total: u64) -> u64`. Keep at most six timestamped cumulative samples, including the boundary sample. Define `LiveMetrics::new(downloaded: Arc<AtomicU64>, received: Arc<AtomicU64>, uploaded: Arc<AtomicU64>, peers: Arc<AtomicU32>, peers_served: Arc<AtomicU32>) -> Arc<Self>`, implementing `StatsReader`, and `publish_position(&self, position: Option<LivePositionStats>, buffer_ms: u64)`. `spawn_sampler(metrics: Arc<LiveMetrics>) -> MetricsSampler` returns an owner whose Drop aborts its timer task; use short synchronous metrics locks only.

- [ ] Write tests `rate_measures_flow_and_expires`, `rate_reads_are_pure`, `rate_reset_is_not_traffic`, and `rate_history_is_bounded`. With manually advanced instants, assert 1,024 bytes/second for sustained 1,024-byte one-second increments, `assert_eq!(meter.rate(now, total), 1024)` during steady flow, zero after five silent seconds, identical repeated reads, zero reset spikes, and history length at most six after 10,000 observations. Cover early-window and upload-only snapshots, saturating bits/second conversion and retained-reader idle decay.
- [ ] Run `cargo +1.99.1 test -p ace-engine live_stats --locked`; confirm the new assertions fail before implementation.
- [ ] Implement these interfaces. Read current atomics without mutating history on a status request. Use monotonic elapsed times, guard counter decreases, and expire the window even when sampling has stopped. Preserve total counters independently of measured payload rates.
- [ ] Run the focused tests and provider tests; require all to pass. Ensure the default reader is absent for unchanged providers.
- [ ] Commit as `feat(ace-engine): add independent live statistics snapshots`.

### Task 2: Ace payload accounting, ready media and piece positions

**Files:** Modify `crates/ace-wire/src/reassembly.rs` and `crates/ace-engine/src/ace_provider.rs`; place focused provider tests in new `crates/ace-engine/src/ace_provider/stats_tests.rs`.

**Consumes:** Task 1's `LiveMetrics`, `MetricsSampler`, `StatsHandle` and `LivePositionStats`.

**Produces:** `PieceReassembler::ready_chunks(&self) -> impl Iterator<Item = &[u8]>` walks complete consecutive pieces starting at `next_needed`, excluding trailers, without consuming anything. Engine helper `ready_buffer_snapshot(continuity: &Continuity, bitrate: Option<u64>) -> (u64, u64)` returns contiguous duration milliseconds and complete-piece count. Use existing PCR helpers when present and descriptor-rate estimation otherwise.

- [ ] Write readiness assertions: complete pieces at cursor and cursor+2 yield one ready piece until cursor+1 completes; partial pieces yield none; trailers are excluded; inspection asserts `assert_eq!(reasm.ready_chunks().count(), 1)` and `assert_eq!(reasm.next_needed(), cursor)`. Provider tests must assert that duplicate blocks do not inflate received payload, injected table bytes do not count, and both pooled and single-peer paths publish the same accounting boundary. Check unknown/negative positions and recovery clearing old buffer observations.
- [ ] Run `cargo +1.99.1 test -p ace-wire reassembly --locked` and `cargo +1.99.1 test -p ace-engine stats_tests --locked`; observe the new failures.
- [ ] Implement the read-only iterator and provider integration. Increment a separate received-media counter after contiguous verified reassembly and before gating/backpressure; leave delivered `downloaded` totals untouched. Capture the handle in `AceSource`, own its sampler there, and delegate `stats()` to the same reader. Publish cursor, observed head, active window bounds and ready-buffer observations together; refresh before/after extraction and clear obsolete observations on skip/reconnect. Unknown active bounds remain unavailable. Do not modify scheduling or media output.
- [ ] Run both focused suites; require pass, including authenticating/retransmission fixtures relevant to the new counter boundary.
- [ ] Commit as `feat(ace-engine): measure live payload rates and positions`.

### Task 3: Observable startup buffering and session lifecycle

**Files:** Modify `crates/ace-engine/src/startup_buffer.rs` and `crates/ace-engine/src/session.rs`.

**Consumes:** Task 1's reader and Task 2's Ace handle.

**Produces:** Private `StartupStatsReader` combining the inner reader (or last cached source snapshot) with shared reservoir observations. `StartupBufferedSource::stats_handle()` exposes it; `StreamSession::stats()` reads an independent handle when available and otherwise retains its existing cached behavior.

- [ ] Add a fake source with an independently writable reader and a pending first outer `next()`. Before that call returns, assert `assert!(session.stats().await.bitrate > 0)` and `assert!(session.stats().await.buffer_ms > 0)`. Assert dequeue reduces the reservoir duration, discontinuity removes the old timing epoch, release returns to provider-ready estimates, and status readers do not consume chunks. Add EOF/retained-reader, cancellation-during-collection and no-reader fallback tests; explicitly verify dropping the source stops its sampler.
- [ ] Run `cargo +1.99.1 test -p ace-engine startup_buffer --locked` and `cargo +1.99.1 test -p ace-engine session --locked`; confirm the new behavior fails first.
- [ ] Implement the composite reader and short shared-state updates after enqueue, dequeue, release and discontinuity. Reuse current reservoir duration/PCR/fallback behavior. Capture the session's reader before moving the source into the pump. Never lock the source across its pending `next()` or introduce an owning reference cycle.
- [ ] Run both focused suites; require pass with unchanged chunk order, discontinuity behavior, bounds and cancellation.
- [ ] Commit as `fix(ace-engine): expose statistics while startup buffering`.

### Task 4: Native and compat contracts

**Files:** Modify `crates/ace-engine/src/http.rs`, `docs/native-api.md` and `docs/protocol/compat-matrix.md`.

**Consumes:** `SourceStats.download_rate`, `upload_rate`, `livepos` and `bitrate` from Tasks 1–3.

**Produces:** Private `live_position_json(position: Option<&LivePositionStats>) -> serde_json::Value`; native status gains `livepos`. Active and idle compat responses gain `speed_down`, `speed_up` and `livepos`.

- [ ] Add schema tests asserting rates 1,536 and 512 bytes/second serialize as 1.5 and 0.5 KiB/s (`assert_eq!(json["response"]["speed_down"], 1.5)`), unknown position serializes null, and known positions have null `first_ts`/`last_ts` with `is_live: 1`. Assert native bitrate remains bits/second, idle responses have zero speeds/null position, and invalid-token reads do not start a provider or refresh a lease.
- [ ] Run `cargo +1.99.1 test -p ace-engine http::tests --locked`; observe new assertion failures before mapping the fields.
- [ ] Implement the JSON mapping and document the spec's exact piece-position meanings, null timestamps, sampling, counter/rate difference and buffer exclusions. Keep unrelated engine parity fields out of scope.
- [ ] Run HTTP tests; require pass with existing selector, token and session semantics unchanged.
- [ ] Commit as `feat(ace-engine): expose measured compat live statistics`.

### Task 5: Review, bounded live validation and integration

**Files:** Add a concise evidence note under `docs/protocol/notes/`; keep raw captures, helper scripts and reports private beneath `/tmp`. Update comparison tooling only if a concrete collection gap requires it, with a focused offline test.

**Consumes:** The complete implementation and existing `tools/abcompare` capture/status artifacts.

- [ ] Run `cargo +1.99.1 test --workspace --locked`, `cargo +1.99.1 clippy --workspace --all-targets --locked -- -D warnings`, `cargo +1.99.1 fmt --all --check`, the relevant Python harness tests, identifier hygiene and whitespace checks. Inspect exits and summaries; repair actual failures before broad reruns.
- [ ] Request one independent whole-branch review against the approved spec. Resolve actionable findings and rerun only affected checks, broadening when the change warrants it.
- [ ] Run one unattended 90-second simultaneous reference/candidate comparison, using manually supplied current public source, fresh state, default playback settings and controlled WARP-off routing. Restore WARP and stop only owned resources in cleanup. Collect candidate and reference statistics through their appropriate routes; preserve both media/activity evidence and the numeric report privately.
- [ ] Evaluate exactly the predefined final 60-second common window; require usable activity from both engines. Compute candidate/reference mean download-speed ratio with consistent KiB/s units. Record startup buffer visibility and sample uncertainty. Ratio 0.8 through 1.2 meets the live rate criterion; invalid/missing/zero reference activity does not. Do not omit inconvenient samples or repeat merely to pass. If live acceptance remains unverified or fails, leave #171 open and explain the remaining gap in the PR.
- [ ] Record reviewed numeric results and limits in the evidence note, commit, push and open a PR. Link it to this thread. Monitor through T3; merge the reviewed head when required CI passes, then verify issue state and unwatch the PR. Never claim full #171 acceptance from offline checks alone.

## Execution recommendation

Native execution is recommended: these five tasks depend on shared interfaces, and one implementer plus one final independent review minimizes repeated context. Subagent-driven execution remains available if the user prefers per-task independent review. Written-plan review and execution selection precede product edits.
