# Empty Discovery Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Recover a completed-empty initial discovery in the same open call with bounded resource ownership.
**Architecture:** One provider-local absolute discovery budget and at most one retry; reuse DiscoveryRun and existing follower.
**Tech Stack:** Rust, Tokio, synthetic loopback protocol tests.
**Spec:** docs/superpowers/specs/2026-10-25-empty-discovery-recovery-design.md

## Global Constraints

At most two initial generations; one-second gap after completed-empty first; absolute ten-second budget includes warm initialization and never resets. Bootstrap direct path and successful warm/ongoing discovery remain intact. No new dependencies, public APIs/config, policy changes outside initial discovery, live probes or identifiers.

## Review Focus

Cancellation in retry gap and pending generation; deadline versus late-candidate race; fast-empty spin/third generation; warm/bootstrap paths; source ownership transfer and unchanged initial connection admission.

### Task 1: Bounded same-open discovery recovery

Files: ace-engine/src/ace_provider/live/mod.rs production; ace_provider/discovery_tests.rs tests; this spec/plan. Paths are under crates/ except docs.
Interfaces: existing open_resolved and public DiscoveryRun; private helper/constants only if needed. No new public seam.

- [x] Reproduce completed-empty failure and healthy-first media control (Luna report /tmp/outpace-253/empty-recovery-red.md).
- [x] Luna adds deterministic policy negatives and runs focused RED without production edits.
- [x] Sol implements the single agreed provider-local policy, preserving existing source creation and follower.
- [x] Luna runs focused GREEN, relevant existing controls, workspace tests, Clippy, formatting and hygiene. Fix fixture-only mistakes; return product failures to Sol.
- [x] Fresh independent scoped review of the whole change and retained evidence; fix findings and validate affected behavior.
- [x] Return reviewed, validated change to user. No push/merge authorized by this plan; commit only after verification if needed for reviewability.

Self-review: all contract requirements covered by task1 and negatives; one coherent unit, no new cross-task interface. Execution already authorized by user; retain established Sol implementation/review and Luna execution roles.

## Validation and review ledger

- Original deterministic RED and healthy-media positive control: `/tmp/outpace-253/empty-recovery-red.md`.
- Policy RED controls and implementation handoff: `/tmp/outpace-253/empty-recovery-policy-red.md` and `/tmp/outpace-253/empty-recovery-implementation.md`.
- Focused GREEN, provider discovery module, offline workspace tests, Clippy, formatting, hygiene, exact-deadline race, and warm-initialization timing limit: `/tmp/outpace-253/empty-recovery-green.md`.
- P3 cleanup correction and focused controls: `/tmp/outpace-253/empty-recovery-nit-fix.md` and `/tmp/outpace-253/empty-recovery-nit.diff`.
- Fresh scoped review verdict PASS / nit addressed: `/tmp/outpace-253/empty-recovery-nit-rereview.md`.
- Remaining validation limit: no existing seam delays warm-cache initialization independently; its deadline placement is reviewed statically, not controlled-time tested.
