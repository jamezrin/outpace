# Borrowed Live Chunk Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Eliminate the pool's temporary owned live payload copy without breaking owned parsing APIs.

**Architecture:** Add a message-borrowed view, keep the owned parser as a compatibility adapter, and use the view at the existing synchronous reassembly call.

**Tech Stack:** Existing Rust/ace-wire/ace-swarm; no dependency or default change.

**Spec:** `docs/superpowers/specs/2026-11-02-borrowed-live-chunks-288-design.md`

## Constraints and review focus

- Preserve exact ten-byte header framing, wrong-variant/short/empty-data behavior and owned public return types.
- Prove borrowed payload aliasing and owned independence; no trust promotion from parsing.
- Keep message ownership, actor holds, verification/publication and bounded reassembly unchanged.
- No unsafe code, new tasks, queues, configuration or live observations.

## Task 1: Baseline and contracts

- [x] Pin existing wire tests and bounded release inspect-and-borrow baseline using the current owned parser.
- [x] Add expected missing-interface/aliasing contracts for the view and owned compatibility; distinguish compile RED from behavioral defect claims.

## Task 2: Implementation

- [x] Add LiveChunkRef and borrow_from_message, preserving one shared framing parser.
- [x] Keep from_message returning an independent Vec for ownership callers.
- [x] Replace pool on_piece's temporary owned parse with a view and pass its slice directly to reassembly.
- [x] Run wire/pool/authentication guards and compare the same synthetic hot-path inputs; inspect semantic/performance regressions.

## Task 3: Review and integration

- [x] Run appropriate lint/format/hygiene/test gates and obtain independent correctness/performance/test-fidelity review.
- [ ] Refresh base as needed, preserve unrelated work, commit with hooks and open/link PR.
- [ ] Resolve CI/review and merge only when ready; keep remaining #288 and readiness criteria open.

Validation: full workspace tests pass on the merged shared-serving base. Paired warmed release measurements compare owned and borrowed inspection in one binary; historical baseline harness provenance is limited. No deployment or live performance claim.
