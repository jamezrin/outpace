# Shared live audio normalization implementation plan

> **For agentic workers:** Use superpowers:executing-plans inline, task by task.
> User waived routine design/plan approvals; independent review remains mandatory.

**Goal:** Complete #49's optional fixed AAC live-TS target with raw defaults and
bounded shared ownership, not HLS-only behavior or full transcoding parity.
**Architecture:** One source decorator before shared fanout, existing supervised
process core reused through narrow HLS/TS output adapters, latest-value playback
state for readiness and terminal failure. Keep VOD ranges unchanged.
**Tech Stack:** Pinned Rust/Tokio, existing FFmpeg subprocess and MPEG-TS selectors;
no new library binding/dependency. Design: sibling specification dated 2026-11-06.

## Current status

The first output-boundary slice passes workspace tests and independent static
review. It does not implement the live-TS option. The copy microbenchmark covers
only the extracted validator against debug media dependencies; whole-pipeline
performance and all later feature acceptance remain outstanding.

## 1. Bounded process-output extraction (first reviewable slice)

Files: `hls_audio/process.rs`, then focused `audio_normalize` module only when the
neutral process responsibility can actually move. Preserve existing HLS policy.

- [x] Add failing pure packet-batch tests: no publication before tables/opening;
  opening emits exactly PAT, PMT, video in order; later packets emit at most one;
  malformed output emits no batch. Literal fixture expectations, not a new helper
  used as its own oracle.
- [x] Run targeted tests and retain missing-interface/behavior RED accurately.
- [x] Implement fixed 3-packet batch without allocation and use it at stdout and
  capability validation boundaries. Preserve HLS SDT suppression/readiness,
  progress clock updates, publication fencing and existing failure tests.
- [x] Extract a narrow process-output adapter and neutral failure mapping with
  HLS as the sole production implementation first. No generic async framework.
- [x] Verify HLS process/classification/generation tests and independent equivalence
  review; retain source pins and measured copy/allocation bounds. Commit/PR only
  once this slice is coherent and reviewable. #49 remains open.

## 2. TS output and failure state

Files: `audio_normalize` process/output modules, `provider.rs`, `session.rs`.

- [ ] RED tests for bounded asynchronous TS sink and readiness distinct from HLS,
  queue-full cancellation, epoch fencing, terminal failure retention and source
  default-state None. Validate accepted batch before publication; never await under
  publication mutex. Include held allocation owners in byte accounting.
- [ ] Implement HLS/TS adapters; chunks <=32 KiB, queue four chunks, stable state
  watch. Preserve existing HLS completed-segment readiness and 45-second real
  stall detection while accounting intentional TS backpressure.
- [ ] Verify adversarial framing/backing-storage, failure/reap/slot reuse and HLS
  regressions. Independently review lifetime/error semantics.

## 3. Shared normalized source and bounded recovery

Files: new `live_audio` adapter/coordinator, shared strict classifier/probe seams.

- [ ] RED tests for supported layout, opening, discontinuity, replay storage,
  cancellation/Drop/shutdown and old-generation output rejection.
- [ ] Implement owning source worker under CleanupTracker, source reservation held
  through child/pump/source cleanup, original stats/metadata delegation, epoch
  output and bounded generation replacement. No wait inside `TsSource::next`
  owns an otherwise untracked child.
- [ ] Use actual synthetic MP2 and AC3 conversion, video-copy/decoded-AAC witness;
  AAC/video-only no child; unsupported layout and child faults stay observable.

## 4. Manager, HTTP and configuration integration

Files: `manager.rs`, native/compat TS and HLS HTTP paths, `config.rs`, CLI/env load,
status serialization and operator docs.

- [ ] RED separate-default-disabled live switch; raw identity/no allocation/probe,
  live/HLS option combinations, concurrent direct/HLS/support starts with one
  source reservation/child, and VOD byte/range preservation.
- [ ] Decorate through one helper in both manager creation paths. Share preflight,
  admission and tracker ownership. Preserve existing HLS-only scope.
- [ ] RED native/compat pre-header errors and midstream body errors, source-state
  HLS failure, terminal entry retry/stop/reap. Subscribe/pin before readiness wait.
- [ ] Implement explicit status/error propagation without broad provider trait
  rewrites or a new session-history map. Document selected-codec/layout limits.

## 5. Performance, final gates and issue completion

- [ ] Run bounded synthetic before/after throughput/latency/resource experiments,
  including one/multiple viewers and cancellation under pressure. Report actual
  measurements separately from source bounds; no speculative optimization.
- [ ] Full appropriate workspace tests, all-target Clippy, fmt and identifier
  hygiene/diff checks; independent architecture/performance/test-fidelity review.
- [ ] Address concrete findings, normal-hook commit/PR, handle exact-head CI and
  review, auto-merge green reviewed head. Run synthetic FFmpeg/manual-player smoke
  where required; no automatic live matrix. Update #49 only for proved criteria.
