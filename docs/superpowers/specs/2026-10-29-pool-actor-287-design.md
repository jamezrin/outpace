# Issue 287: staged PoolActor refactor

Status: user approved stage 1 implementation. Main baseline c8d0cbe. Stage 2 remains a later, separately reviewed change. Stage 1 extraction, independent review and local validation are complete; PR #295 is published, with updated documentation CI and merge pending. Issue #287 remains open.

## Stage 1: behavior-preserving extraction

Separate chore/pool-actor-287 branch from main in an isolated worktree. Group live-session inputs and pool context rather than forwarding long positional argument lists. Extract PoolActor with run(self) and explicit per-wake/per-message methods; isolate peer-message handlers into a sibling module if necessary to keep production files reviewable. Actor state owns peers, channel endpoints, capped connection JoinSet, pending sets, retry/progress clocks and closed-generation reconciliation. Borrow session-owned continuity, candidate history and discovery run for the same lifetime as today; preserve session-level ownership across pool reconnects.

One maybe_refill method centralizes the repeated capacity predicate and identical spawn_candidate_connects call. Preserve exceptions at call sites explicitly rather than combining predicates that look similar but have different startup or retry semantics. Remove too_many_arguments suppressions in the live pool path through input grouping, not lint relocation. Test wrappers may make mechanical construction updates only.

Preserve wake priority, retry deadlines, task ownership, default capacities, candidate cooldown and generation handling, selected-peer behavior, logs verbatim, progress/deadline updates and every early exit. In particular keep #284's zero-credit closed senders, finite FIFO terminal reconciliation, producer-aware loss handler and timed credit replenishment; keep #285's accepted whole-wire publication before playback drain and one-pending-piece disk ownership. No task offloading, new queue, new timeout, changed option, or production policy change in this stage.

All existing scheduler/live-session/authentication/recovery/discovery/HTTP composition tests pass without altered assertions; mechanical signature changes only. Compare static log literal inventories and handler/await order against baseline. Full workspace tests, Clippy, fmt and identifier hygiene. Independently review before merging the first PR. Stage 1 alone does not close #287.

## Stage 2: owned bounded gossip work

Start from merged stage 1. Replace inline stale-gossip harvesting in the active pool with owned bounded asynchronous work, consumed as a pool wake. Preserve the existing harvest time budget, candidate validation and provenance, exploration/cooldown semantics and startup evidence rules. Inspect the separate pre-pool stale-gossip path and either share the owned facility or document why it cannot stall an active actor.

Worker tasks may borrow no mutable actor state: return bounded validated observations, then merge decisions on the actor. No detached tasks, unbounded result queue, unbounded upstream accumulation or concurrent writes to candidate history. Own cancellation and joins across consumer departure, stale teardown, reconnect, errors and drop; late results must not revive a retired generation. Exact job concurrency, result payload cap and cancellation ownership must be designed from stage-1 code before implementation, preserving current configuration bounds rather than inventing an unlimited side pool.

Add deterministic tests: a stalled gossip harvest does not prevent healthy-peer event/request/output progress; deadline remains unchanged; capped work and results; cancellation destroys/reaps owned workers; late results cannot cross generations; ordinary gossip still learns candidates. Maintain all prior tests and a matched bounded runtime smoke through the approved private VPN-routed test environment after merge. Runtime source must pass the unencrypted gate; never use excluded environments.

## Await and acceptance limits

Do not equate an async signature with nonblocking actor progress. Inventory store publication, reciprocal reply preparation, activation and downstream output awaits after extraction. The new #285 publication path still awaits store admission/commit; moving it blindly can violate accepted-only visibility, replay order, one-piece pending memory or cancellation. This design does not authorize weakening those guarantees or claim that moving gossip alone eliminates every handler stall. If other long awaits prevent #287 acceptance, record the precise boundary and propose a separate focused change rather than silently introducing behavior changes into stage 1 or closing #287 prematurely.

## Delivery and release

Each stage has its own review and PR/CI boundary. Preserve unrelated root edits and keep all real identifiers/private evidence out of tracked files. No release/default/deployment settings change as part of the refactor. v0.9.0 remains open until all outstanding issue acceptance is met, including recovery/sign-off evidence; no release merely for extracting the actor.
