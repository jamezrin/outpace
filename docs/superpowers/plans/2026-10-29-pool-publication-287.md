# Bounded Publication Responsiveness Implementation Plan

> **For agentic workers:** Use subagent-driven-development or executing-plans; follow the approved protocol and root scope refinement, not rejected alternatives.

**Status:** Implementation, integration coverage and independent review are complete. Local gates passed on the reviewed source; publication/CI/merge and runtime validation remain pending. #287 stays open for its remaining acceptance.

**Goal:** Pending accepted-piece publication does not block unrelated actor control or consumer cancellation.

**Architecture:** Explicit owned admission/result/accounting tickets share the existing store semaphore. A bounded Continuity publication ledger holds original accepted bytes in the fixed reassembly window. Actor polls publication phases alongside normal event handling; media/request/cursor hold is intentional.

**Tech Stack:** Existing Rust/Tokio futures, mutexes, semaphore and disk actor; no dependencies.

**Spec:** docs/superpowers/specs/2026-10-29-pool-publication-287-design.md, explicitly approved including root refinement (no general rotating wake scheduler, no ready-prefix API required).

## Constraints

Base b159646. Shared per-store semaphore acquired BEFORE owned wire copy; one pending whole-wire allocation per STORE across generations. Accepted-only atomic immutable publication and signature-tail retention remain. No detached publisher/result callback/unbounded queue, new default/config, network/live/release/deploy. Existing two reviewed store_progress RED tests remain unchanged. Additional tests must be test-first where practical and meaningfully prove the protocol, not mirror code.

## Task 1: Whole store/continuity/actor protocol

Ownership: ace-swarm store.rs and store/disk.rs publication protocol; live_session/mod.rs Continuity state, continuity.rs guards, pool.rs initialization, pool/actor.rs selected phases and output sequencing, pool_support.rs indirect request/skip guards; ace-wire/reassembly.rs minimal bounded completion lookup only if needed. Existing tests plus focused new protocol tests; protocol note updates may clarify intentional hold. Supplied design/plan may receive honest status edits. Preserve all unrelated work.

- [ ] Inspect source and verify reviewed test-only seed. Run both exact behavioral REDs in this worktree before production changes; save private durable logs/source pins. Run relevant prior store/authentication controls as baseline.
- [ ] Add failing store admission/result contracts: waiting mutex/permit owns no wire; synchronous submit copies only after existing permit; memory/disabled/undersized/error results retain old semantics; cancellation at each boundary, admitted command lifetime and accounting refresh remain independently selectable.
- [ ] Implement one shared store protocol and adapt existing shared_put_complete_piece to it. Keep immutable commit/ack/wire-drop/permit order and trusted writers. No duplicated path or early permit release.
- [ ] Add Continuity bounded Needed/Terminal metadata, completed-entry lookup and global publication hold. Audit every cursor movement (including resume, skip_far, skip_evicted and indirect scheduling/retransmission calls). Block take_ready while any Needed/operation exists. Preserve current field limits and validate every pool entry; no unbounded generic-reassembler proof or silent new cap/default.
- [ ] Add at most one actor-owned Acquiring/Awaiting/Refreshing operation with no actor/reassembly borrow or wire ownership before admission. Poll it before existing finite closed-event prefix and in select; no general wake scheduling rewrite. Continue FIFO reliable control and bounded in-window reassembly; no deferred PieceEvent buffer. Suppress new selection/retransmission/cursor moves while holding. Attribute actual completion/receipt once, avoiding duplicate-only metadata growth.
- [ ] Terminal success/AlreadyPresent/NotRetained/error release the attempt barrier; errors keep current log-and-play policy, not fake successful retention. Drain finite batch, emit existing ordered accepted playback BEFORE deferred head recovery; then restore normal scheduling. Carry Needed across pool reconnect; re-admission after lost ack is immutable duplicate-safe. Cancellation drops local ticket without dropping disk command ownership.
- [ ] Both unchanged reviewed REDs turn green. Add deterministic ordering/out-of-order/multiple completions and immutable duplicate tests, raw tail/cache vs stripped media; paused wire/permit/retained-index/ledger bounds under in-window/out-of-window/head churn; memory mutex wait, refresh wait, shared-store/replacement generations and cancellation phases; failures/cache-disabled/undersized and valid retry. Retain all #284 FIFO/credit/gossip and #285 authentication/atomicity controls.
- [ ] Run focused protocol and relevant engine integration tests, then full offline locked workspace and all-target Clippy -D warnings; fmt/hygiene/whitespace. Record all actual commands/exits/counts and coverage limits. Do not weaken tests or silently change publication/error policy to make gates green.
- [ ] Write private mode-0600 re/sdd/issue287-publication-implementation.md and private logs0700 with source/binary pins, exact diff, RED/GREEN receipts, cursor audit, memory/drop/state proofs, rulings and limitations. Release source/test/doc writes. No commits/push/PR/live/deploy.

## Root delivery

- [x] Fresh whole-branch independent spec/quality review, fixes and relevant gate reruns.
- [ ] Normal-hook commit, integrate current main and final gates, PR/CI/merge. #287 remains open until broader remaining awaits/runtime acceptance assessed.
- [ ] Reviewed merged artifact and bounded approved runtime observation only after local proof/CI; no automatic milestone/release claim.

## Implementation handoff status

The owned admission/result/refresh protocol and finite-batch publication hold have been implemented in the existing issue branch. The two reviewed responsiveness tests remain byte-for-byte unchanged and have passed after reproducing RED here. Private implementation receipts record verification and coverage limits. Independent whole-branch review and root publication remain pending; #287 and v0.9 remain open. This status does not establish live playback readiness.
