# Reciprocal Read Responsiveness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep PoolActor control/cancellation responsive during reciprocal store reads with bounded, generation-safe serving.

**Architecture:** One actor-owned read future, per-current-peer FIFO of 64 waiting coordinate pairs, round-robin selection. A separate disk-store-owned reciprocal read permit travels through queued command and buffered result, remaining held through reply construction across caller cancellation and pool replacement.

**Tech Stack:** Existing Rust 1.99.1, Tokio, std disk actor, ace-wire codecs; no new dependency.

**Spec:** docs/superpowers/specs/2026-10-29-pool-reciprocal-read-287-design.md

## Global Constraints

- At most 64 WAITING typed requests per current peer plus one actor-wide in-flight request; ignore new overflow for retry, no eviction or invented wire error.
- One reciprocal-read semaphore per DISK STORE, independent of publication; command/result owns admission until read data is consumed/dropped, including cancellation.
- Preserve reciprocal stream zero, exact stored header/chunk/signature bytes, verified-index reads and admission-success upload accounting. Ordinary/inbound reads keep existing admission and profile behavior.
- No detached task/result queue, socket-write await, new public setting or dependency. No activation/output/publication behavior redesign.
- Poll completion before the closed-peer event prefix and in select; at most one completion/start per actor turn without parking ready work until an unrelated timer.
- Preserve original read_progress.rs byte-for-byte. Bounds apply to actual payload B: one B-byte result and B+10 formatted block, not a universal 16 KiB cap or process RSS budget.
- Identifier hygiene is absolute; only synthetic fixtures. Evidence stays ignored under re/sdd (0700 directories,0600 files). No live requests/deployment. Root owns commits/publication.

## Review Focus

- Completed-but-unconsumed read result must retain the store permit across generation replacement; Task1 tests this with actual command/result lifecycle.
- Lost peer completion must not count as uploaded: send_peer_command None also means absent; Task1 explicitly tests current-ID gate and same-address replacement.
- Flood/replacement queues must stay finite and fair without stale tombstones; Task1 tests 64 overflow/FIFO/round-robin and repeated pruning.
- Immediate memory hits must not consume multiple completion budgets or leave ready work parked; Task1 tests independent poll/select progression and closed-prefix competition.
- Read mutex waits and independent publication must not reintroduce lock/permit cycles; Task1 tests actual actor progress and store admission cancellation.

## Task 1: Integrated admitted reads and actor serving

**Files:**
- Create: crates/ace-swarm/src/live_session/pool/actor/reciprocal.rs (bounded queues/job and focused tests).
- Modify: crates/ace-swarm/src/live_session/pool/actor.rs, pool.rs and mod.rs (initialization/wakes/nonblocking request admission/current-generation completion).
- Modify: crates/ace-swarm/src/store.rs and store/disk.rs (private ReciprocalChunk and admitted disk read path).
- Modify: crates/ace-swarm/src/seed/protocol.rs (Reciprocal profile alone uses admitted path).
- Test: existing new live_session/tests/read_progress.rs remains byte-identical; add separate tests/read_schedules.rs if real-actor schedules need their own fixture module; store/disk test sections and seed/protocol_tests.rs as appropriate.
- Update: this plan and companion approved spec status only after evidence exists.

**Interfaces:**
- Consume existing prepare_reply(request: &PeerMessage, store: Option<&Arc<Mutex<PieceStore>>>, profile: ServeProfile) -> Option<PreparedReply>, preserving callers and profile semantics.
- Produce crate-private ReciprocalChunk owning data/header/optional disk permit, plus PieceStore::shared_reciprocal_chunk(store, piece, chunk) -> Option<ReciprocalChunk>. Permit cannot be destructured/dropped before formatting; data drops before permit.
- Produce actor-private ReciprocalRequest { piece:u32,chunk:u16 }, bounded queues/cursor and persistent owned future yielding originating peer ID plus optional prepared reply. Names may be adjusted idiomatically, contracts may not.

- [x] Step1: Preserve copied reviewed RED tests/hooks and prior RED receipt (2 intended failures), with baseline2publication and4seeding passes. Record unchanged test hash before implementation.
- [x] Step2: Add deterministic store contracts (supplemental GREEN-only provenance) for before-admission cancellation, executing read cancellation, buffered-unconsumed result, second-generation admission wait, permit/data restoration and independent publication. Preserve the real Get pause hook for admitted Get.
- [x] Step3: Add queue and actor contracts: per-peer FIFO/64 waiting overflow/retry, round-robin under flood, no raw request Vec retention, trailing/short/disabled requests, lost/replaced same-address generation, no stale accounting and bounded pruning after repeated replacement. Gate actual mutex wait and disk read; ensure actor control/consumer-close progress and exact response bytes. Include memory-ready continuation and closed-prefix progress.
- [x] Step4: Retain original two behavioral RED cases and missing-interface queue RED. Supplemental lifecycle/schedule tests were added after implementation and have GREEN-only evidence; no additional behavioral RED is claimed.
- [x] Step5: Implement the minimal store admission/result ownership, profile routing and actor job/queue integration. Do not change original read regressions or unrelated timer/cursor/publication policy.
- [x] Step6: Run unchanged read_progress first, then focused new/store/protocol/serving/publication/recovery controls. Stop and diagnose genuine failures; record fixture-only corrections separately.
- [x] Step7: Run cargo test --workspace --offline --locked; cargo clippy --workspace --all-targets --offline --locked -- -D warnings; cargo fmt --all --check; python3 tools/hygiene/check_identifiers.py; git diff --check. Record source/test-binary pins, exact exits/counts and gaps under re/sdd/issue287-reciprocal-read-implementation.md. Do not claim unrun matrices.
- [x] Step8: Independent spec/quality review approved the exact source and receipts with no P1/P2 findings. Root owns the normal-hook commit and PR/CI; no child commits or publication.

## Integration status

Implementation, acceptance witnesses and independent spec/quality review are complete. Original read-progress regressions retain behavioral RED2 -> GREEN2 evidence and their original source hash. Supplemental queue, cancellation and schedule tests have GREEN-only provenance. The legacy seeding precondition was migrated to require retirement while its store lock is held, retaining EOF, PeerLost and zero accounting assertions.

Workspace: 1547 passed, 0 failed, 7 ignored. A subsequent test-only TurnTrace type alias resolved a lint finding; final focused actor/generation tests and workspace all-target Clippy, fmt, hygiene and whitespace checks passed on final source. No workspace rerun is claimed after that alias-only change. Independent review verified the source pins and alias-only difference.

Two actual pool generations share one store-owned disk read permit, with old consumer cancellation, successor admission Pending, healthy control, exact signed reply and zero old accounting. The endpoints use fresh addresses; same-address stale-generation handling is separately covered at the actor terminal seam. The bounded actor trace observes initial/select completion paths and per-turn start/completion budgets. No actual disk-thread join, copied-byte destructor instrumentation or aggregate memory bound is claimed.

Root commit/publication and CI remain pending. #287 remains open for remaining activation/output acceptance; v0.9 remains open. No runtime comparison or automatic soak is part of this change.
