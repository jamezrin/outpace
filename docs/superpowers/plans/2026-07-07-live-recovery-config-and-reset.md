# Live Recovery Configuration and Reset Behavior Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement issue #68 by making live lag-recovery and HLS packaging knobs configurable, validating unsafe combinations, and regating output after lag/skips.

**Architecture:** Add focused config structs under `Config`, validate env overrides in `runtime::config_from_env`, and thread the resulting policy through existing builder/manager seams. Keep session messages as raw `Bytes`; use `KeyframeGate` resets at direct-client and provider-output boundaries for clean recovery.

**Tech Stack:** Rust 2024 workspace, Tokio, Axum, ace-engine, ace-media MPEG-TS keyframe gate, existing cargo unit/integration tests.

---

## File Structure

- Modify: `crates/ace-engine/src/config.rs` - add `LiveRecoveryConfig` and `HlsConfig`.
- Modify: `crates/ace-engine/src/runtime.rs` - parse env vars, validate relationships, and wire config.
- Modify: `crates/ace-engine/src/ace_provider.rs` - replace runtime constants with policy and regate after skips.
- Modify: `crates/ace-engine/src/manager.rs` - store `HlsConfig`.
- Modify: `crates/ace-engine/src/hls.rs` - start packagers from `HlsConfig`.
- Modify: `crates/ace-engine/src/http.rs` - reset direct TS `KeyframeGate` on fan-out lag.
- Modify: `README.md` - document all knobs and validation relationships.

### Task 1: Config Structs and Validation

**Files:**
- Modify: `crates/ace-engine/src/config.rs`
- Test: `crates/ace-engine/src/runtime.rs`

- [ ] **Step 1: Write failing tests**

Add `runtime::tests::default_config_has_live_recovery_and_hls_defaults`, `runtime::tests::parses_live_recovery_and_hls_knobs`, and `runtime::tests::rejects_invalid_live_recovery_relationships`.

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p ace-engine live_recovery`

Expected: compile failure until `Config::live_recovery` and `Config::hls` exist.

- [ ] **Step 3: Implement config structs**

Add `LiveRecoveryConfig` with `request_timeout_ms`, `stale_upstream_timeout_ms`, `request_check_interval_ms`, `max_active_upstreams`, `max_parallel_connect`, `max_piece_advance`, and `max_reasm_pieces_ahead`. Add `HlsConfig` with `segment_packets`, `window_segments`, and `segment_duration_ms`. Implement defaults and validation methods exactly as listed in the design doc.

- [ ] **Step 4: Parse env vars**

In `runtime::config_from_env`, parse every new env var into the nested config fields, then call `config.live_recovery.validate()?` and `config.hls.validate()?`.

- [ ] **Step 5: Run targeted tests**

Run: `cargo test -p ace-engine live_recovery`

Expected: the three new config tests pass.

### Task 2: Provider Policy Threading

**Files:**
- Modify: `crates/ace-engine/src/ace_provider.rs`
- Modify: `crates/ace-engine/src/runtime.rs`

- [ ] **Step 1: Write failing provider test**

Add `ace_provider::tests::continuity_uses_configured_live_recovery_bounds`, proving `Continuity::fresh` uses configured scheduler and reassembler bounds.

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test -p ace-engine ace_provider::tests::continuity_uses_configured_live_recovery_bounds`

Expected: compile failure until `Continuity::fresh` accepts the policy.

- [ ] **Step 3: Implement policy threading**

Store `LiveRecoveryConfig` in `AceProvider`, add `with_live_recovery`, pass it from `build_runtime`, and replace runtime uses of active-upstream, parallel-connect, piece-advance, request-timeout, request-check, and stale-upstream constants with policy values.

- [ ] **Step 4: Run provider tests**

Run: `cargo test -p ace-engine ace_provider::tests::continuity_uses_configured_live_recovery_bounds ace_provider::tests::stale_upstream_budget_expires_after_no_forward_progress ace_provider::tests::timed_out_requests_returns_only_aged_pieces_and_prunes_passed_ones`

Expected: pass.

### Task 3: HLS Config Threading

**Files:**
- Modify: `crates/ace-engine/src/hls.rs`
- Modify: `crates/ace-engine/src/manager.rs`
- Modify: `crates/ace-engine/src/runtime.rs`

- [ ] **Step 1: Write failing HLS test**

Add `hls::tests::configured_hls_settings_control_playlist_and_segments`, proving configured packet count, window length, and duration affect generated segments and playlist metadata.

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test -p ace-engine hls::tests::configured_hls_settings_control_playlist_and_segments`

Expected: compile failure until `HlsPackager::new` accepts `HlsConfig`.

- [ ] **Step 3: Implement HLS threading**

Change `HlsPackager::start(session)` to `HlsPackager::start(session, config)`, store `HlsConfig` in `StreamManager`, and construct the manager from `StreamManager::with_config(registry, config.session_buffer, config.hls)`.

- [ ] **Step 4: Run HLS tests**

Run: `cargo test -p ace-engine hls::tests manager::tests::get_hls_only_returns_an_already_running_packager`

Expected: pass.

### Task 4: Direct Client and Source Skip Regating

**Files:**
- Modify: `crates/ace-engine/src/http.rs`
- Modify: `crates/ace-engine/src/ace_provider.rs`

- [ ] **Step 1: Write failing reset tests**

Add `ace_provider::tests::resume_gap_arms_output_keyframe_gate` and `ace_provider::tests::skip_evicted_gap_arms_output_keyframe_gate`. Add a small helper in `http.rs` if needed so direct lag reset can be unit-tested without a real HTTP body.

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p ace-engine output_keyframe_gate`

Expected: compile failure until the gate helpers exist.

- [ ] **Step 3: Implement resets**

Reset the direct TS gate on `RecvError::Lagged(_)`. Add an optional provider-output `KeyframeGate` to `Continuity`, arm it on reconnect gaps and evicted-gap skips, and filter aligned output through it before sending bytes to the session.

- [ ] **Step 4: Run reset tests**

Run: `cargo test -p ace-engine output_keyframe_gate`

Expected: pass.

### Task 5: Documentation and Verification

**Files:**
- Modify: `README.md`
- Modify: docs created for this issue

- [ ] **Step 1: Update README**

Document all new env vars, defaults, and validation relationships under `Configuration`.

- [ ] **Step 2: Run checks**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Expected: all commands exit 0, with ignored live tests unchanged.

- [ ] **Step 3: Commit**

Run:

```bash
git add README.md docs/superpowers/specs/2026-07-07-live-recovery-config-and-reset-design.md docs/superpowers/plans/2026-07-07-live-recovery-config-and-reset.md crates/ace-engine/src/config.rs crates/ace-engine/src/runtime.rs crates/ace-engine/src/ace_provider.rs crates/ace-engine/src/manager.rs crates/ace-engine/src/hls.rs crates/ace-engine/src/http.rs
git commit -m "Make live recovery policy configurable"
```
