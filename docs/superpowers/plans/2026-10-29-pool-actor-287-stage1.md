# PoolActor Extraction Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development or superpowers:executing-plans; implement the single cohesive task below without beginning stage 2.

**Status:** Stage 1 implementation, independent review and root gates are complete. PR #295 is published; required CI passed on the production revision. Documentation corrections await updated CI and merge. Stage 2, issue closure and deployment remain outside this change.

**Goal:** Extract explicit live pool state and handlers with no intentional behavior change.

**Architecture:** Group session inputs/context, own per-pool state in PoolActor, centralize equivalent refill work, and split wake/message methods. Keep all await sites and resource destruction ordering equivalent.

**Tech Stack:** Existing Rust/Tokio types and modules; no dependencies or new runtime primitives.

**Spec:** docs/superpowers/specs/2026-10-29-pool-actor-287-design.md (stage 1 approved).

## Global constraints

Branch chore/pool-actor-287 from main; baseline c8d0cbe. Preserve #284 queue credits, closed-generation event ordering and cleanup, and #285 accepted whole-piece publication/memory/cancellation behavior. Preserve log literals verbatim. No stage-2 gossip tasks, queue/deadline/default changes, release, deployment or live requests. No real identifiers or registry reads. Existing tests keep their assertions; mechanical input construction changes only.

## Task 1: Extract and validate the production actor

Ownership: live_session/mod.rs input grouping, pool.rs adapter/setup/run orchestration, actor implementation/handler sibling modules under live_session/pool/ if useful, minimal pool_support/support visibility/signature changes. Colocated tests and existing external live/discovery tests may change construction only if grouping requires it. Root owns commit, review, publication. Keep production modules roughly below 1500 LOC excluding tests; avoid unrelated decomposition.

- [x] Inspect actual current paths and all pool entry callers. Save source/hash and operational-log literal inventories privately under re/sdd. Run existing focused pool/scheduler/reconnect/authentication tests as characterization BEFORE structural edits; no new regression assertions are required for this behavior-preserving refactor. Record a missing baseline prerequisite rather than masking it.
- [x] Define grouped LiveSessionInputs/PoolContext/seed-observer inputs as appropriate after inspecting existing types. Remove positional-argument lint suppressions in the production live pool path rather than relocating them. Preserve externally useful interfaces through thin grouped adapters where needed; mechanical test constructor updates are allowed.
- [x] Move state to PoolActor with run(self); keep session-level continuity/candidates/discovery ownership borrowed across pool replacement, and own only existing pool-level peer/channel/connection state. Preserve LiveStatsGuard and task/peer Drop ordering on every exit.
- [x] Extract per-wake/per-peer-message methods. Represent outer-loop continue/exit and progress outcomes explicitly so extraction does not run a shared postamble on a path that skipped it before. Preserve exact select priority, loop timing, startup and stale activity logic, inline gossip/store awaits, peer generation checks, and candidate cooldown/accounting order.
- [x] Centralize identical capacity/refill condition plus spawn_candidate_connects in maybe_refill. Audit call-site differences: preserve explicit startup/retry exceptions instead of treating superficially similar branches as identical. No fake callback layer or behavior redesign.
- [x] Compare old/new log literals and handler/await/state mutation order, with a branch mapping inventory. Run existing tests unmodified apart from mechanical signatures: ace-swarm scheduler/live_session/reconnect/authentication, ace-engine discovery/resync, then offline locked workspace and all-target Clippy with -D warnings; fmt, identifier hygiene, whitespace. No behavioral test assertions relaxed or skipped; no dependency downloads.
- [x] Write private mode-0600 re/sdd/issue287-stage1-implementation.md with precise scope/diff/source pins, baseline/final commands/results, mechanical test changes and log equivalence, unresolved limits and self-review. Logs private durable directories mode-0700. Mark completed plan items honestly. Release write ownership; no commits/push/PR/deploy.

## Review and delivery

- [x] Root obtains fresh whole-branch spec/quality review emphasizing refactor equivalence, early exits, ownership/drop ordering, logs and test fidelity; fix concrete findings and rerun affected checks.
- [x] Normal-hook commit, integration gates and stage-1 PR publication; required CI passed on the production revision. PR references #287 without closing it.
- [ ] Updated documentation CI and stage-1 merge.
- [ ] Only after stage 1 merges, design exact bounded gossip work ownership/result/cancellation for stage 2; inventory remaining store/reply/output await constraints without silently weakening verified publication.

Milestone v0.9.0 remains open; extracting the actor is not production readiness or a release gate by itself.
