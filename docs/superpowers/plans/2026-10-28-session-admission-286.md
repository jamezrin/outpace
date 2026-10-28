# Provider Session Admission Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development or superpowers:executing-plans. Follow the approved design and test-first steps below.

**Status:** Implementation and independent whole-branch review complete, including the queued HLS shutdown correction. Final integration gates and publication are root-owned; CI, merge and deployment remain pending.

**Goal:** Optional atomic provider-session admission, preserving the unlimited daemon default.

**Architecture:** Manager-wide admission reservations follow live source and shared VOD/range ownership. Same-key cache hits and HLS wrappers reuse existing resources. Saturation refuses new starts without queueing.

**Tech Stack:** Existing Rust/Tokio provider and manager traits; no new dependencies.

**Spec:** docs/superpowers/specs/2026-10-28-session-admission-286-design.md (approved by user).

## Constraints

Unset/0 OUTPACE_MAX_SESSIONS is unlimited. Positive values cap pending plus retained provider ownership. Preserve CLI play, same-key fan-out, provider metadata/stats/discontinuity, trusted writers and network defaults. No aggregate byte budget, release, deployment or live tests. Synthetic identifiers only. Root owns commits/publication and final review.

## Task 1: Complete admission contract

Ownership: new private admission module under crates/ace-engine/src (plus its module declaration), manager.rs, provider error variant, configuration/environment wiring, native and compatibility HTTP mappings/status, focused tests and deployment/config documentation. Inspect existing environment loader and manager construction before choosing exact wiring locations. Keep changes focused; avoid unrelated refactors.

- [x] Verify clean main-derived branch baseline. Reuse the prior worktree target only with explicit provenance; record commands and exit statuses under ignored re/sdd/.
- [x] Write and run failing contract tests before implementation: N+1 refusal before provider work; concurrent pending starts; same-key direct/HLS fan-out at capacity; failed/cancelled open releases; completion and retained owners; VOD ranges surviving cache expiry; saturated HTTP errors and count reporting; default/zero/positive/invalid configuration. Core pending-capacity and lifecycle contracts were observed behavioral RED before enforcement; supplementary regressions were added around implementation and were not each independently observed failing. The queued HLS shutdown correction has its own behavioral RED/GREEN receipt. Compiler-only failures are not counted as behavioral evidence.
- [x] Implement private checked admission counter and owned reservation. Unlimited still counts. Add Source/Content/Range decorators forwarding every trait method. Hold shared VOD resource owner across returned ranges, including errors/cancellation; destroy inner resources before releasing reservations. Do not release merely on manager map removal.
- [x] Integrate direct and HLS provider opens and VOD resolution after per-key/cache rechecks, before provider I/O. Pending starts count. Keep unrelated opens concurrent, no queueing or global lock across provider awaits. Handle stop/reap/shutdown and cached VOD expiry without premature release or leaked admission. Document asynchronous teardown measurement limits.
- [x] Add validated config/env parsing and pass it through daemon manager construction; preserve existing unlimited constructors. Add distinct capacity ProviderError and exhaustively preserve native/VOD/HLS/compat response mappings. 503 has Retry-After: 1; compat JSON keeps its error shape. Metadata-only negotiation may precede playback refusal.
- [x] Expose occupied and configured max without redefining active_sessions; log fixed-text refusal with numeric counts and no content selector. Document unlimited default, opt-in example, lifetime semantics and separate byte-budget scope.
- [x] Run focused tests, then cargo test --workspace --offline --locked; cargo clippy --workspace --all-targets --offline --locked -- -D warnings; cargo fmt --all -- --check; python3 tools/hygiene/check_identifiers.py; git diff --check. No dependencies fetched or silent skips.
- [x] Source self-review against design; retain exact diff, source hashes, RED/GREEN receipts and limits in ignored mode-0600 re/sdd/issue286-implementation.md. Release write ownership to root; no commits/push/deploy.

## Root follow-up

- [x] Fresh independent whole-branch spec/quality review; fix concrete findings and rerun affected checks.
- [ ] Normal-hook commit, integrate current main and rerun gates, publish PR and monitor CI.
- [ ] Milestone remains open unless all remaining acceptance items are complete; no premature v0.9.0 release.
