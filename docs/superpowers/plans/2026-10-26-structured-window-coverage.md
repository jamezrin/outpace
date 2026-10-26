# Structured Window Coverage Implementation Plan

> **For agentic workers:** This bounded implementation is authorized in the handoff. Keep Luna's tests untouched; Luna owns GREEN and additional controls.

**Recovery status:** The prior capacity-failed task left production changes and both documents. Preserve those changes and finish only remaining issues. No prior implementation report was present at recovery. The user has accepted the design and execution method; no approval gate or open material choices remain.

**Goal:** Let a valid structured window update advance a producer's scheduler coverage even when the shared head is already as high or higher.

**Architecture:** Extract a validated maximum helper from the existing parser seam, retaining the existing head-advance helper API. Use that maximum in both live-pool structured message branches to drive the same runtime-window, request, and loss path used for genuine progress.

**Tech Stack:** Rust, `ace-wire::live::LiveWindow`, `ace-swarm` live-session pool.

**Spec:** `docs/superpowers/specs/2026-10-26-structured-window-coverage-design.md`

## Global Constraints

- Preserve the current parser, `u32::MAX` bound, and monotonic shared head.
- Do not change minimum-window acceptance, message IDs, Bitfield, timers, defaults, authentication, fairness, caps, gates, or log schema.
- Leave all test files and Luna's concurrent edits untouched; do not run tests, builds, formatting, checks, network commands, or Git mutations in this handoff.

## Review Focus

- Same-head and lower-than-shared-head updates must advance a producer's own coverage and request the next piece when eligible.
- Repeated or lower-than-peer updates must not trigger request advancement or activity.
- Negative, malformed, and over-`u32::MAX` maxima must not affect scheduling; valid zero and trailing-byte payloads retain existing parsing semantics.
- Both `Extended` and otherwise-unhandled `Unknown` paths must behave consistently; existing `Unknown` logging should retain its meaning.
- Shared head must remain monotonic; no minimum-window or deadline policy changes.

### Task 1: Parse once through the existing structured-window seam

**Files:** Modify `crates/ace-swarm/src/live_session/support.rs` and its import in `mod.rs`.

**Interfaces:** Add `read_window_max(payload: &[u8]) -> Option<u64>` using `LiveWindow::from_myinfo_payload` and the existing maximum bound. Retain `advance_head_from_window(payload: &[u8], head: u64) -> Option<u64>` by filtering the new helper against `head`; make the wrapper and its import test-only because production callers now use the independent maximum reader.

- [x] Recover the existing helper extraction and preserve the old helper's behavior.
- [x] Scope the compatibility wrapper and import to tests and retain the pool's maximum-reader import.

### Task 2: Advance peer coverage in both structured-message paths

**Files:** Modify `crates/ace-swarm/src/live_session/pool.rs`.

**Interfaces:** Consume `read_window_max` in the `Extended` and otherwise-unhandled `Unknown` arms. When `max > continuity.head` or `max > peers[peer_id].max_piece`, assign `continuity.head = continuity.head.max(max)`, call `update_runtime_window`, mark activity, then call `advance_pool_requests` and `record_pool_losses`. Log the existing `Unknown` head transition only when the shared head truly increases; keep its one-time unhandled log for unrecognized or non-progressing messages.

- [x] Recover the same progress condition and request path in both arms.
- [x] Self-review duplicate and lower maxima, monotonicity, parser handling, and logging without running verification commands.
- [x] Write implementation report and hand off GREEN plus any additional controls to Luna.

Implementation, regression tests, repository gates, and fresh independent review are complete. CI remains pending.

### Final validation status (2026-10-26)

- [x] Implementation: both structured message paths use the existing validated parser and per-peer progress path while preserving monotonic global head behavior.
- [x] Tests: final fresh retained-producer run passed 11/11; the five original controls passed. Focused reconnect, startup-progress, parser, and deadline-helper suites passed. Workspace offline tests passed 1,460 with 7 ignored and no failures.
- [x] Review: `/tmp/outpace-overnight/structured-window-review.md` and the companion SDD review approve the seven-file branch scope with no P1/P2 findings.
- [ ] CI: pending; no CI result is claimed here.

Validation evidence and fresh-binary provenance are recorded in `/tmp/outpace-overnight/structured-window-current-green.md` and `/tmp/outpace-overnight/structured-window-final-provenance-run.log`. The local regression establishes the handler behavior only; it does not establish public or deployed plateau causality.
