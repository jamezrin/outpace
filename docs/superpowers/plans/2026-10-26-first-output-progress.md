# First Output Progress Implementation Plan

> **For agentic workers:** Sol may implement this single task only after root accepts the design. Use superpowers:executing-plans then; this assignment permits documentation only and no delegation, source/test edits, checks, builds, git operations or network work.

**Goal:** Recover a full unproductive startup pool despite advancing peer heads, using the existing stale budget.
**Architecture:** One local non-sliding deadline and a private budget helper integrate into the existing pool loop/teardown. Established corroboration and post-output sliding recovery remain intact.
**Tech Stack:** Rust 1.99.1, current std Instant, Tokio, existing signed loopback fixtures.
**Spec:** ../specs/2026-10-26-first-output-progress-design.md

## Global Constraints

Reuse stale_upstream_timeout without changing its production value or adding configuration. Preserve one-second corroboration, initial scheduling, authenticated media/chunk limits, active/parallel caps, cooldown/exploration fairness, post-output stale policy and cleanup. No HTTP/decode/global-open budget, new queue/framework, per-operation timeout, production test hook or persistent Continuity field. Preserve Luna's uncommitted discovery tests. No public root-cause or performance claim.

## Review Focus

- T below one second must leave a full request opportunity after the corroboration gate.
- Protocol/partial-piece activity and additional admissions cannot move an armed deadline.
- Existing emitted continuity must retain post-output recovery, while never-emitted rebuilds receive a fresh per-pool opportunity.
- A cap-4 spare-slot control cannot substitute for a four-A full-pool regression.
- Both video and audio-only authenticated TS count; consumer cancellation must not leave workers, discovery or a new timer alive.

### Task 1: Bound startup pool progress by first usable output

**Production files:** `crates/ace-swarm/src/live_session/support.rs` (pool_progress_budget), `mod.rs` (import only), `pool.rs` (local deadline, budget integration, fresh timestamp after scheduling).
**Tests:** existing `crates/ace-engine/src/ace_provider/discovery_tests.rs`; `crates/ace-swarm/src/live_session/tests/continuity.rs`; `live_start_tests.rs` for pool-level near-bound/resume controls if needed. Keep helpers adjacent to these tests; no separate harness/framework. Docs are the spec and this plan.
**Interfaces:** implement exactly pool_progress_budget's six-argument signature and None/remaining semantics from the spec. Consume existing stale_upstream_budget, Continuity.emitted/startup_deadline, made_output, existing LiveOutput and FollowEnd::PoolStale. Preserve should_refresh_stale_deadline and production ownership types. No new public API.

- [x] **1. Preserve and establish RED evidence before implementation.** Read `/tmp/outpace-overnight/first-output-red.md` and the actual tests. Retain `startup_progress_advancing_heads_do_not_starve_healthy_late_peer` as the original behavioral RED. Its current assertion must fail because media/B attempt is absent, not due to setup or invalid recovery options. Keep the three reported positive controls and their signed output assertions. Record no new result without executing it in the later implementation phase.

- [x] **2. Add the missing full-capacity behavioral RED.** Name it `startup_progress_four_advancing_peers_full_pool_recovers_late_healthy_peer`. Extend the local fixture with four distinct A listeners, barrier-completed handshakes/windows and retained Unchokes; publish them together so all join the initial selection within its existing grace. Observe four successful signed-extension/Interested activations and production peer count == 4 before publishing B. At least one A must consume real requests; do not make the barrier depend on four piece assignments. Keep all four connected and advancing heads without media. Publish healthy signed-media B 1.2 seconds after that activation/request barrier and assert B is not attempted while the still-live pool is full before expiry. With T=2000 ms, request timeout=900 ms and check interval=100 ms, require eventual teardown, B attempt and authenticated contiguous B output, without exceeding caps. Initial RED should demonstrate all four active, B retained/unattempted, no output. This is default-capacity evidence, not a measurement of default timing.

- [x] **3. Add pure policy controls using explicit Instant arithmetic.** Tests in continuity.rs call the new helper; function-not-found failure is preparatory, never a substitute for the behavioral RED. Use a base Instant and Duration offsets; no sleeps/time framework:

  - `pool_progress_budget_waits_for_corroboration_then_grants_full_interval`: pending gate gives its remainder with no first-output deadline; clear it after the release scheduling point and get a full T, including T=200 ms.
  - `pool_progress_budget_startup_activity_cannot_slide_deadline`: repeatedly update last_progress using the existing true activity helper, and pass new head/peer activity times; original deadline remains identical, positive just before expiry and None at equality/afterward.
  - `pool_progress_budget_first_output_restores_existing_stale_policy`: emitted > 0 clears the first deadline and returns exactly stale_upstream_budget; non-output chatter afterward cannot refresh last_progress.
  - `pool_progress_budget_new_unemitted_pool_gets_one_fresh_interval`: new local Option for a rebuilt pool arms once without changing the prior pool's result or adding corroboration.
  - `pool_progress_budget_resumed_emitted_continuity_never_arms`: previous emitted bytes keep the local deadline None and use existing sliding-budget results.

  Retain `stale_deadline_refreshes_only_for_output_after_playback_starts` unchanged. Its startup=true assertion is not contradictory once the separate bound is tested.

- [x] **4. Implement the single policy change.** Add/import pool_progress_budget; keep a local `first_output_deadline = None` after successful initial admissions/scheduling. In the existing control-loop stale calculation, run corroboration release and its scheduling first, then sample a fresh now and query the helper. Use that remaining budget in the existing stale/wait/tick path. Keep all normal stale teardown, candidate accounting and observer behavior. Do not reset the deadline in matching corroboration, replacement, later admission, message or retransmission branches. No parser/auth/scheduler/cap/timeout changes. Use current emitted/made_output path to transition after successful nonempty source output; do not manufacture output from verified-but-withheld bytes.

- [x] **5. Add request-opportunity and near-bound production-path positives.** Extend fixture options rather than copying an entire test harness:

  - `startup_progress_healthy_video_just_before_bound_is_not_rebuilt` and `startup_progress_healthy_audio_just_before_bound_is_not_rebuilt`: A withholds actual signed TS until 200 ms before the end of a 2-second post-request opportunity (the accepted CI scheduling margin; exact expiry is tested by explicit Instant arithmetic), then outputs and remains the producer beyond its former deadline; B must stay unattempted and no stale teardown occur. Observe first request and release with owned signals. Exact boundary policy is established by pure tests; allow documented bounded scheduling slack in loopback tests without moving production T.
  - Video payload uses existing `tests/vectors/media/h264-keyframes.ts`, packed/signed into normal test pieces. There is no standalone audio-only media vector in this checkout: construct a test-local PAT/PMT selecting only AAC (stream type 0x0f) plus audio PES/transport payload, following the existing pure table construction/CRC patterns in ace-media/src/audio.rs tests. Assert audio PID payload reaches output, with no video access point prerequisite. This tests transport/signature delivery, not AAC/video decoding. Do not alter the production keyframe gate or add FFmpeg generation/network probes.
  - `startup_progress_short_stale_interval_starts_after_corroboration`: valid request=100 ms, stale=200 ms, check=20 ms. A reaches request scheduling after the existing lone-peer gate and delivers promptly within its full opportunity; no pre-request stale teardown. Treat these as explicit test settings only. Preserve existing `lone_healthy_peer_starts_after_bounded_uncorroborated_fallback` and early-Unchoke multi-peer scheduling controls.
  - `startup_progress_chatter_and_late_admission_do_not_reset_bound`: record request/gate barrier, send continuing head/Unchoke activity, make B available after the gate; a new peer/control event before expiry cannot renew the original pool interval. Use lower-level pool observation if needed to identify teardown rather than inferring timing solely from B bytes.

- [x] **6. Cover continuity and cleanup conflicts.** Add `startup_progress_resumed_output_uses_existing_post_output_stale` using a genuinely previously emitted Continuity; head-only activity must still trigger the existing stale result with no new corroboration or first-output interval. Keep existing signed reconnect/provenance, rejected-signature retry and resync controls. Add `startup_progress_consumer_drop_before_first_output_cleans_up`: drop the source while the chatter pool and late discovery candidate exist; assert discovery owner drop and A sockets close, B is not newly admitted afterward, then abort/await all fixture JoinSet tasks before assertions. Preserve the existing cancellation test. Every new fixture uses bounded watchdogs and cleanup on failure; no detached timer is introduced.

- [ ] **7. Verify once after GREEN and review the bounded claim.** Future commands, not run for this design:

```bash
RUSTUP_TOOLCHAIN=1.99.1 cargo test --offline -p ace-engine startup_progress_ -- --nocapture
RUSTUP_TOOLCHAIN=1.99.1 cargo test --offline -p ace-swarm pool_progress_budget_
RUSTUP_TOOLCHAIN=1.99.1 cargo test --offline -p ace-swarm stale_deadline_refreshes_only_for_output_after_playback_starts
RUSTUP_TOOLCHAIN=1.99.1 cargo test --offline -p ace-swarm live_start_tests
RUSTUP_TOOLCHAIN=1.99.1 cargo test --offline -p ace-engine unchoke_ordering_
RUSTUP_TOOLCHAIN=1.99.1 cargo test --offline -p ace-swarm rejected_signed_piece_retries_through_full_peer_pool
RUSTUP_TOOLCHAIN=1.99.1 cargo test --offline -p ace-swarm retained_single_producer_signed_reconnect_preserves_media_and_durable_credit
RUSTUP_TOOLCHAIN=1.99.1 cargo test --offline -p ace-swarm resync_tests
RUSTUP_TOOLCHAIN=1.99.1 cargo test --offline -p ace-engine dropping_initial_connect_cancels_attempts_and_discovery
RUSTUP_TOOLCHAIN=1.99.1 cargo test --offline --workspace
RUSTUP_TOOLCHAIN=1.99.1 cargo clippy --offline --workspace --all-targets -- -D warnings
RUSTUP_TOOLCHAIN=1.99.1 cargo fmt --all --check
python3 tools/hygiene/check_identifiers.py
```

Expected: retained cap-1 and added full-cap-4 RED become GREEN; positive media/cancellation/continuity controls pass; ordinary offline workspace, lint, formatting and hygiene pass. Review all timestamp writes and every startup_deadline-clear branch; verify no new timeout wraps activation/channel I/O and no claim of preemptive deadline enforcement. Distinguish first aligned source output from HTTP/prebuffer/decode, and default capacity from accelerated timing. Root accepted the design before implementation. Return verification evidence to root; root retains CI and PR ownership, while the explicit handoff authorizes only the reviewed topic commit and merge from current origin/main. No push or publication is authorized.

Self-review: every review-focus item has named controls, all three corroboration release paths are covered by the common first-loop budget check, and the conflicting legacy unit expectation remains explicitly preserved. One coherent task; no unrelated liveness work.
## Execution status

- [x] Implementation complete.
- [x] Test coverage complete.
- [x] Review complete, including the approved stale-teardown cause assertion.
- [ ] External CI and PR merge pending.
