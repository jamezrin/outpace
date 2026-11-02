# Shared Serving Chunk Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remove intermediate memory payload copies from internal serving while preserving public APIs, verification and bounded read ownership.

**Architecture:** Independent immutable Bytes per stored memory chunk; crate-private shared serving reads; disk Vec ownership conversion with reciprocal admission retained. Replies and public compatibility results remain owned as before.

**Tech Stack:** Existing Rust/Bytes/Tokio; no new dependency.

**Spec:** `docs/superpowers/specs/2026-11-02-shared-serving-288-design.md`

## Constraints and review focus

- Keep public Cow/Vec signatures and externally observable bytes/header/accounting semantics.
- One allocation per stored chunk; do not retain whole-piece backing through small slices.
- Preserve verified publication and disk permit/drop/cancellation/replacement behavior.
- No new queue/task/concurrency/default/budget policy.
- Measure cold clone promotion and warm serving; no blanket zero-allocation/RSS claim.

## Task 1: Baselines and contracts

**Files:** test sections/focused modules of `crates/ace-swarm/src/store.rs` and `seed/protocol.rs`.

- [x] Run existing store/serve/signature/publication guards on the base.
- [x] Add pointer-sharing, per-chunk uniqueness after eviction, old-generation immutability, exact accounting and public owned-API compatibility contracts. Confirm expected missing-interface/type or behavioral RED before implementation.
- [x] Add exact inbound/reciprocal reply-header/byte controls using both backends; retain disk admission/cancellation/replacement guards.
- [x] Record bounded release baseline through actual prepare_reply: both profiles, cold-first and warmed repeated serves, three fixed trials. Input construction must be outside the timed warm loop and explicitly accounted for in cold cases.

## Task 2: Internal sharing

**Files:** `crates/ace-swarm/src/store.rs`, `store/disk.rs`, `seed/protocol.rs`.

- [x] Store independent Bytes chunks and preserve borrowed public access/accounting/publication behavior.
- [x] Add shared_serving_chunk with atomically captured data/header; keep public shared_chunk compatibility path efficient.
- [x] Convert ReciprocalChunk payload to Bytes and disk owned results without altering permit ownership/drop order; expose a borrowed data slice outside the store, not a cloneable shared disk handle. Migrate the cancellation content snapshot to an owned copy while preserving every permit assertion.
- [x] Route live inbound/reciprocal preparation and shared_block chunk acquisition through internal sharing; keep encoding and public owned results unchanged.
- [x] Run unchanged contracts and compare the same benchmark inputs, investigating regressions.

## Task 3: Review and integrate

- [x] Run affected store/protocol/authentication/actor suites and appropriate repository gates.
- [x] Obtain independent correctness/performance/test-fidelity review and resolve concrete findings.
- [x] Refresh merged base while preserving unrelated work; commit with normal hooks.
- [ ] Push/open/link PR, handle CI/review and merge only when ready.
- [x] Update remaining #288 work and milestone ledger without making readiness or end-to-end zero-copy claims.
