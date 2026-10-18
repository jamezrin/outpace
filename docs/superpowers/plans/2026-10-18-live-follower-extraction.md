# Production Live Follower Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the production live session into `ace-swarm` while preserving behavior and engine API compatibility.

**Architecture:** Add `ace_swarm::live_session` with a grouped session runner and narrow observation/store adapters. Move existing algorithms and their tests intact; keep engine configuration, provider adaptation, startup buffering, and statistics presentation in engine.

**Tech Stack:** Rust workspace, Tokio, existing peer/wire/swarm/media crates, ace-log.

**Spec:** `docs/superpowers/specs/2026-10-18-live-follower-extraction-design.md`

## Global Constraints

- Preserve the post-v0.3.0 behavior, compatibility APIs, defaults, limits, deadlines, authentication, and output ordering.
- Keep #192 seed consolidation and #193 legacy API retirement separate.
- Keep production files approximately below 1,500 lines, excluding tests.
- No engine or HTTP dependency in swarm; promote existing `ace-media` dependency from development-only where needed.
- Operational logging uses `ace-log::alog!`.
- No real identifiers, stream names, captures, or local sandbox state in tracked changes or PR text.
- GPT-6.1 Sol implements and reviews; GPT-6 Luna runs every test/build/verification command.
- Branch `fix/live-follower-190` is based on current main; root checkout remains untouched.

## Review Focus

- Consumer cancellation during connects/refill: no surviving workers or active peer counters (Task 2).
- Discovery yields a candidate after the first handshake: retain late-candidate admission and fairness (Task 2).
- Reconnect with inbound consumers: retain one producer lease and shared store (Task 2).
- Session termination with a published position: clear active coverage without losing historical fields (Task 1).
- Nondefault recovery/cache configuration: map every field and retain disk fallback behavior (Tasks 1–2).

## File Structure

- New `crates/ace-swarm/src/live_session/{mod,options,observation,discovery,reconnect,warm_peers,peer,pool,pool_support,continuity,single_peer}.rs`: public boundary plus relocated private machinery.
- New `crates/ace-swarm/src/live_session/tests/`: relocated private session tests, split by existing concerns.
- Modify `crates/ace-swarm/{Cargo.toml,src/lib.rs}` and lockfile only if Cargo requires dependency bookkeeping.
- Modify `crates/ace-engine/src/ace_provider.rs` and `ace_provider/live/mod.rs`: replace orchestration with swarm runner adaptation.
- Modify `crates/ace-engine/src/live_stats.rs`: implement swarm observation interface.
- Modify engine `ace_provider` test modules: retain provider-facing tests and move direct machinery tests into swarm.
- Remove relocated engine implementation files once imports/callers move; retain engine announce, resolution, and VOD modules.

## Task 1: Introduce the swarm boundary and engine adapters

**Interfaces:**

- `LiveOutput { pub bytes: Bytes, pub discontinuity: bool }`.
- `LiveRecoveryOptions`: copyable seven-field settings matching existing recovery fields; duration accessors and validation preserve existing follower checks.
- `LiveObserver: Send + Sync`: `received_bytes(&self, count: u64)`, `publish_position(&self, position: Option<LivePosition>, buffer_ms: u64)`, `clear_active_coverage(&self)`. `LivePosition` contains the same fields/types as engine `LivePositionStats` without engine dependencies.
- `LiveCounters`: cloned shared atomic handles for peers, downloaded, uploaded, and peers_served.
- `StoreFactory`: `Arc<dyn Fn([u8; 20]) -> PieceStore + Send + Sync>`; engine closure captures existing cache/size/retention policy and delegates to `build_piece_store`.
- `LiveSeedOptions`: registry, enabled flag, store factory, prefetch pieces, recovery options, warm-cache handle, optional observer. Keep existing retention/capacity inputs needed for registry leasing.

- [ ] Read spec and existing configuration/statistics/store definitions; record exact field types in boundary modules without changing meanings.
- [ ] Add engine mapping tests using deliberately distinct valid nondefault values for all seven recovery fields; assert each mapped field equals input and defaults still map to 1500/12000/1000 ms, 4 active, 12 parallel, 256 advance, 512 ahead.
- [ ] Add observer adapter tests: received bytes affect received counter only; all position fields round-trip; clearing coverage sets live_first/live_last to None, buffer_pieces/buffer_ms to zero, and preserves other position fields.
- [ ] Have Luna run the focused new tests and confirm failure before adding missing adapters.
- [ ] Implement boundary types, explicit engine mapping, metrics adapter, and store closure using current validation and construction logic. Keep existing production runner active until Task 2.
- [ ] Have Luna run affected crate tests and formatting; expect all pass. Commit `refactor(ace-swarm): define production live session boundary`.

## Task 2: Relocate and connect the production follower

**Consumes:** Task 1 boundary, unchanged peer/wire/scheduler/store APIs.

**Produces:** `pub async fn follow_live(session: LiveSessionInputs)` in `live_session/mod.rs`. `LiveSessionInputs` groups StreamInfo, initial peers, Arc<Identity>, output sender, LiveCounters, LiveSeedOptions, PeerDiscovery, optional ReachabilityMonitor, and optional DiscoveryRun. Preserve all existing inner helper signatures wherever possible.

`PeerDiscovery` keeps its existing Arc callback signature. `DiscoveryRun` exposes construction, next-candidate reception, and nonblocking drain needed by engine bootstrap; it owns and aborts its worker. `WarmPeerCache` exposes only existing construction/initialization/hints operations needed by provider setup; candidate policy remains private.

- [ ] Before relocation, have Luna save the workspace test inventory (`cargo test --workspace --offline -- --list`) for post-move comparison.
- [ ] Move production peer/pool/continuity/session, discovery, reconnect and warm-cache machinery into the planned swarm files. Keep algorithms, event order, timeouts, constants, worker guards, and protocol checks unchanged.
- [ ] Replace engine-specific metrics/config/store dependencies with Task 1 adapters. Retain the single session-wide seed lease and existing cache-flush guards. Move legacy engine single-peer helper intact with its tests; leave existing swarm legacy APIs intact.
- [ ] Move direct machinery tests with implementation. Split mixed test files so engine policy/startup-buffer/source/statistics adaptation tests remain in engine. Replace old private access paths rather than exporting internals for tests.
- [ ] Ensure moved tests cover cancellation, candidate admission during initial connection, fairness, reconnect continuity, and lease/store lifetime; add focused missing ownership regressions only where existing tests do not exercise those paths.
- [ ] Replace engine runner invocation with `ace_swarm::live_session::follow_live(LiveSessionInputs { ... })`; retain source channel capacity 256, startup-buffer wrapper, announce select, and provider error conversion.
- [ ] Export module and promote `ace-media` dependency. Document production versus legacy live entrypoints in crate/module docs.
- [ ] Have Luna run affected crate tests, then full offline workspace suite. Compare inventories, accounting for moved/renamed tests; expect no lost behavioral coverage or new failures. Commit `refactor(ace-swarm): own production multi-upstream live follower`.

## Task 3: Review, validate, and integrate

**Consumes:** compiled extraction and preserved test inventory.

- [ ] Request independent GPT-6.1 Sol review of the whole branch against spec and diff. Specifically inspect cancellation guards, discovery handoff, seed lease acquisition count/lifetime, observer teardown, and config/store mapping.
- [ ] Resolve substantiated findings with focused changes and Luna regression tests; obtain fresh independent review if material changes require it.
- [ ] Have Luna run `cargo test --workspace --offline`, `cargo clippy --workspace --all-targets --offline -- -D warnings`, `cargo fmt --all --check`, and `python3 tools/hygiene/check_identifiers.py`. Each must exit zero.
- [ ] Have Luna verify no live peer read/write loops remain in the engine's Ace live path, no engine dependency entered swarm, and affected production file sizes remain reviewable.
- [ ] Arrange one bounded local live smoke with current public registry content supplied via environment, WARP off, logs private. Ask before host routing changes if needed. Record authenticated MPEG-TS progress and measurement limits; do not expand the matrix without a specific failure.
- [ ] Open/link a concise PR for #190 with behavior boundary, validation, dependency impact, and live observation. Review final diff, await required CI, then merge under existing user authorization. Unwatch the PR before returning completion.

## Execution

Use the user's established subagent workflow: GPT-6.1 Sol implementation and independent review, GPT-6 Luna validation. Tasks 1 and 2 are sequential because the relocation consumes the boundary; avoid parallel edits to shared modules. Preserve the existing clean baseline result: 1,002 passed, zero failed, seven ignored.
