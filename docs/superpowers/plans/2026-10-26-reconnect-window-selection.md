# Reconnect Window Selection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans for this single task. No delegation is authorized by this design assignment. Checkboxes describe future implementation; nothing below was executed here.

**Goal:** Prevent stale reconnect handshakes from ending selection before an in-flight usable peer completes.

**Architecture:** Copy the current cursor/head into selection and share Continuity's exact eligibility predicate. Reject stale successes before usable accounting/grace, reusing current exploration/cooldown handling.

**Tech Stack:** Rust 1.99.1, Tokio, existing loopback peer fixtures; no new dependency.

**Spec:** `docs/superpowers/specs/2026-10-26-reconnect-window-selection-design.md`.

## Global constraints

- Initial implementation work used the supplied `82e4d6a` base. Root later authorized committing and merging the reviewed change with latest `origin/main` in this worktree. Others' test edits are owned work; accommodate them.
- Initial acquisition has no criterion; retained continuity supplies one regardless of prior output.
- Preserve 3-second per-attempt timeout, 250-ms first-USABLE grace, parallel/active caps, cohorts/cooldowns, wire/authentication and buffering defaults.
- No all-stale policy overhaul, overall recovery deadline, primary-selection change, identifier/payload logging, live probe, deployment or network qualification.

## Review focus

Pin these inputs in the task's tests: a future window with min above cursor; caught-up cursor beyond head; stale successes completing during usable grace; finite all-stale candidates across cohorts; cancellation before delayed usable completion. Eligibility means the existing predicate, not strict cursor containment.

### Task 1: Apply reconnect eligibility before usable selection

**Files:**
- Modify `crates/ace-swarm/src/live_session/continuity.rs`: shared private ResumeCursor and delegating Continuity methods.
- Modify `crates/ace-swarm/src/live_session/peer.rs`: appended optional snapshot and Connected-arm ordering.
- Modify `crates/ace-swarm/src/live_session/mod.rs`: capture/pass snapshot before the existing cancellation select.
- Test `crates/ace-swarm/src/live_session/reconnect_tests.rs` and `tests/continuity.rs` beneath that directory. Existing `reconnect_tests/fairness.rs` remains a regression suite; do not redesign it.

**Interfaces:** Consume existing `SessionCandidates::{explored,failed}`, `PeerConnectStats::record_connected`, and wire-window validation. Produce exactly `ResumeCursor`, `Continuity::resume_cursor`, `ResumeCursor::window_can_resume`, delegating `Continuity::window_can_resume`, and the five-argument `connect_pool` specified in the spec; return type stays `Vec<ConnectedUpstream>`.

- [x] **1. Preserve behavioral RED.** Read both private RED reports and the existing modified test. Retain four-packet prior-output setup, cursor 9, A=50 ms/max8, B=400 ms/max9, B-alone control and cleanup before the final `selected.iter().any(|peer| continuity.window_can_resume(&peer.window))` assertion with its existing failure text. The supplied baseline already failed that exact assertion. Before implementation, separate old-behavior assertions (A wins, <390 ms, B cancelled) into the initial-None control; reconnect should instead require B selected and A excluded. Remove manual stale accounting from the future GREEN path. Run the focused test on the baseline API and retain behavioral failure evidence. Do not change the base. For any baseline comparison after signature migration, adapt only five arguments back to the old four; keep normative assertion/setup intact, never substitute None as a reconnect GREEN workaround.

- [x] **2. Implement only the specified interface/branch.** Move predicate logic once, preserving conversions and both branches. Capture the snapshot without cloning Continuity or holding its borrow across await. Record valid Connected timing, reject stale with explored/failed/drop, then use existing push/deadline logic only for accepted candidates. Keep attempted/cohort ordering and cancellation ownership. Migrate all four existing call sites: production uses its optional snapshot; startup fallback test uses None; both calls in the normative reconnect fixture use Some. Retain the outer defensive filter without double accounting. No new log/schema/error class.

- [x] **3. Complete focused controls.** Use the existing candidate helper with owned, bounded socket/task cleanup; prefer barriers to narrow wall-clock assertions.
  - `reconnect_selection_grace_can_cancel_later_covering_peer`: final normative assertion passes, B selected, A excluded/cooled exactly once, B-alone passes; no assertion of a permanent baseline stall.
  - `reconnect_stale_only_returns_empty_with_cooldown`: A alone, then two stale peers across exploration cohorts; finite completion, empty result, each explored once/cooled, sockets close, no admission and no synthetic wire failure. All-stale uses ordinary caller retry semantics.
  - `initial_selection_keeps_first_protocol_success_grace`: None with A50/B400 retains A and cancels B; cancelled B stays eligible. Move baseline-specific mechanism assertions here.
  - `reconnect_grace_starts_at_first_usable_window`: parallel cap 3, active cap 2, A50 stale, B400 usable, C750 usable; B selected, C cancelled/eligible at the existing B+250 deadline. Repeat with A500 stale to show rejection during usable grace cannot extend it. Use server completion/closure barriers and a generous outer timeout, not an exact scheduler-time equality; preserve cap checks.
  - `resume_cursor_matches_continuity_window_policy`: ordinary cover/stale; N11/H10/max10 accepted, max9 rejected; N9/H10/window[12,15] accepted; preserve saturated conversion behavior.
  - `reconnect_selection_cancellation_closes_pending_peers`: drop the selection future after A rejects and B accepts TCP but before its delayed reply; B socket closes, no B exploration/failure/admission. Retain existing consumer-close, fairness and timeout-fallback tests.

- [x] **4. Verify and review the one change.** Root-authorized integration is complete; the clean merge result passed the requested gates using the pinned toolchain:
  ```sh
  cargo test --offline -p ace-swarm reconnect_selection_grace_can_cancel_later_covering_peer --lib -- --nocapture
  cargo test --offline -p ace-swarm live_session:: --lib
  cargo test --offline --workspace
  cargo clippy --offline --workspace --all-targets -- -D warnings
  cargo fmt --all --check
  python3 tools/hygiene/check_identifiers.py
  git diff --check
  ```
  Require focused GREEN, broader suites passing with documented ignored live tests, clean lint/format/hygiene/whitespace. Review the actual diff against the spec and verify no test weakens the normative assertion or changes initial behavior. No live reliability claim follows.

- [x] **5. Handoff one reviewable change.** Record baseline RED, GREEN commands/results and limitations. Root authorized and received the local commit and main integration in this test worktree; no push/PR/publication was performed.

## Root-authorized integration status

Fresh independent reviews approved the production diff and accepted the in-grace stale completion control. Implementation and focused-control tasks 1–3 are complete. Root committed `a05c27b`, merged `origin/main` `f1b6390` at `9b1e047`, and passed the focused, reconnect, cursor, workspace, lint, formatting, hygiene and release compatibility gates on the clean merge source. The release candidate and final gate/commit records are in `/tmp/outpace-overnight/reconnect-commit-build.md` and `.superpowers/sdd/2026-10-26-reconnect-window-selection/reconnect-commit-build.md`. Local publication remains with root; no push or PR was created.
