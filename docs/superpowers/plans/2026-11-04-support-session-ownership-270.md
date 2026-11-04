# Support session ownership (#270 stage 1) implementation plan

> Execute inline with `superpowers:executing-plans`; one fresh whole-branch review
> before integration. No routine user approval checkpoint is required for this goal.

**Goal:** Make live-session stop own cancellation and provider cleanup even when
media handles are retained, as the prerequisite for autonomous support pins.

**Architecture:** Keep a single pump and broadcast backlog. Only the pump owns a
strong broadcast sender; media handles keep a weak sender and completion control.
Manager-tracked cleanup owns pump joins. Source shutdown propagates through wrappers
to the actual follower and its worker scope before the admission reservation drops.

**Tech stack:** Existing Rust/Tokio watch, broadcast, JoinSet, and manager cleanup.
No dependency, networking default, payload queue or viewer-count changes.

**Spec:** ../specs/2026-11-04-support-service-270-design.md

## Global constraints

Public defaults and unlimited session admission remain unchanged. Preserve #286's
reservation through actual source destruction, including paused cleanup; retained
VOD content/range behavior is unchanged. No source identifiers or private captures
in tracked files. Tests use synthetic providers/loopback only. This stage does not
add headless pins/configuration or close #270. Root checkout remains untouched.
Rebased onto merged #327 (d878b5b) before final validation. Integration remains pending.

## Review focus

- Retained handles and waiting/late subscribers must reach terminal output.
- A canceled stop caller must not lose source/follower/retired-worker joins.
- Capacity stays occupied while actual cleanup is paused and releases once afterward.
- Cancellation wins against a ready chunk before new publication; already published
  broadcast backlog may drain before EOF, not be mistaken for post-stop publication.
- Reaper/stop/start/shutdown races must not join under lifecycle locks or mutate a
  replacement generation. Independent VOD/HLS ownership remains intact.

## Task 1: characterize explicit stop with retained live handles

Files: `crates/ace-engine/src/session_admission_tests.rs` and `session.rs` tests.

- [x] Add `stop_retained_live_handle_terminates_source_before_completion`: start the
  existing pending synthetic source, retain the returned Arc, stop through manager,
  capture occupancy and waiting receiver terminal state before releasing the Arc.
  Always release old handles and call manager shutdown before asserting observations.
  Expected new contract: stop returned true, source destroyed/occupied zero, receiver
  closed. Observe actual behavioral RED against existing map-removal-only stop.
- [x] Add a late receiver/ordinary source EOF control. A completed pump must not leave
  receivers open merely because its StreamSession Arc still exists.
- [x] Run `cargo +1.99.1 test -p ace-engine --lib --offline --locked stop_retained_live_handle`
  and the EOF control; retain exact failing assertions. Never count zero matched tests.

## Task 2: owned source shutdown contract and wrapper forwarding

Files: `provider.rs`, `admission.rs`, `startup_buffer.rs`, `session.rs`, `manager.rs`.

Interfaces:
- `TsSource::shutdown(&mut self)` async default no-op for genuinely stateless sources.
  Confirmed success follows owned background completion. Unknown cleanup is pending
  or explicitly failed with its admitted owner retained; no timeout silently frees it.
- `StreamSession::start_with_cleanup(source, buffer, &CleanupTracker) -> Arc<Self>`.
- `StreamSession::cancel(&self)` signals shutdown without awaiting.
- `StreamSession::stop(&self)` asynchronously waits idempotently and reports whether
  owned cleanup was confirmed.
- Existing `start(source,buffer)` remains a standalone owner with a private cleanup
  tracker; explicit stop drains it, and final Drop transfers cancellation/joins.

- [x] Add a source with paused shutdown and a drop notification. Through admission
  and startup-buffer decorators, prove cancellation reaches its shutdown once,
  reservation remains occupied while paused, and source drop precedes release.
  Add interrupted stop waiter: another stop/manager shutdown still observes completion.
- [x] Implement wrapper forwarding and one owned pump. Reuse WorkerControl/cleanup
  completion signals rather than spawning a monitor per session. Expose only the
  cancellation/completion methods needed on existing crate-private controls.
- [x] Store a `broadcast::WeakSender<StreamEvent>` in StreamSession; pump retains the
  strong sender. Subscribe/raw_receiver upgrade it or return a newly closed receiver
  if terminal. No per-chunk mutex or extra payload queue is needed. Preserve count
  increment/drop behavior and concrete broadcast receiver APIs.
- [x] Pump selects cancellation before next output, closes publication, awaits source
  shutdown, destroys source, then completes. It must run shutdown on ordinary EOF too.
  A standalone tracker is private to that session; never drain/close a shared manager
  tracker on individual stop. Final Drop sends cancellation and transfers join before
  dropping private tracker ownership. Explicit completion does not rely on Arc count.
- [x] Run paused shutdown/EOF/late-subscription/cancel-ready-output tests and admission
  decorator tests. Keep pending controls under bounded test watchdogs and cleanup first.

## Task 3: retain and finish ACE follower descendants

Files: `ace_provider/live/mod.rs`, `live_stats.rs`, `live_session/mod.rs`, `peer.rs`,
`pool.rs`, `pool_support.rs`, `pool/actor.rs`, `live_session/discovery.rs` and tests.

Interfaces:
- AceSource retains the spawned follower/announce owner. Its shutdown closes rx,
  joins owner and metrics sampler, and cannot be canceled into abandoned ownership.
- Follower owns a bounded scope for peer worker joins, including retired workers.
  PeerRuntime cancellation handles do not discard join ownership. Existing owned
  discovery/connect/gossip/activation futures stay within that scope/session lifetime.

- [x] Write owned worker tests: retire a worker before source close, pause its exit,
  prove follower completion waits; active and retired workers close before capacity
  release. Include failed activation and a canceled opening future.
- [x] Replace dropped peer JoinHandles with scope-retained joins. Poll completed joins
  during normal turns and bound outstanding retirement by configured transport limits;
  block new admission rather than accumulate a queue when cleanup has not completed.
  Do not spawn a new reaper task per peer. Drain aborted tasks on every follower exit.
- [x] Retain the follower owner in AceSource and add sampler close/join. Audit any
  ownership gap between spawn and returned StartupBufferedSource; there must be no
  cancelable await after spawning work without an already-established owner.
- [x] Prove actual signed owned source closes and producer lease is removed on manager
  stop while a media handle remains. Independently admitted inbound snapshots retain
  their own fixture owner and are joined separately. Do not infer descendant joins
  from leader completion or producer removal alone.

## Task 4: manager transitions and reservation compatibility

Files: `manager.rs`, `session_admission_tests.rs`, native/compat/HLS lifecycle tests.

- [x] Route direct and HLS-created sessions through manager cleanup registration.
  Stop/reaper/shutdown cancel sessions under existing publication fence, transfer
  removed owners out of locks and await stop outside locks. A canceled HTTP stop
  cannot discard joins; manager shutdown drains registered cleanup.
- [x] Migrate #286's retained-pump test only after paused actual-cleanup witness is
  green. The new assertion concerns destroyed source, never map-entry disappearance.
  Keep default-unlimited, same-key sharing at cap, retained VOD/ranges and queued
  HLS shutdown regressions intact.
- [x] Test stop versus blocked start, two concurrent stops, reaper versus replacement,
  shutdown with pending cleanup, and a ready output chunk racing cancellation.
  Failed/canceled cleanup remains visible and reserved; do not report successful
  source closure just because a JoinHandle was aborted.
- [x] Run affected engine/swarm suites, then workspace once after production settles.

## Task 5: integration, review and stage completion

- [x] Add stage-1 documentation explaining explicit stop versus retained handles,
  occupied capacity during cleanup, and independent inbound/VOD ownership limits.
- [x] Measure one shared session/source with viewer and zero-viewer consumers: provider
  calls/slots stay one, no new payload queue, bounded cancellation/resource observations.
  No throughput improvement claim is required; do not substitute virtual time for CPU.
- [x] Run pinned offline workspace tests, all-target Clippy with warnings denied, fmt,
  hygiene and diff check. Use a worktree-specific target; shared target reuse across
  different worktrees previously produced a stale zero-test artifact.
- [ ] Fresh independent architecture/correctness/performance/test-fidelity review;
  address concrete findings. Commit via hooks, PR with stage scope/no closure keyword,
  CI/review and automatic exact-head merge when ready. #270 stays open for service
  pins/configuration/supervision/topology/documentation stages.

## Self-review / resumption

Every stage-1 lifetime requirement maps above; tasks 3–4 carry paused descendants and
capacity/replacement races. Terminal receivers retain public API shape via weak sender.
No timer frees uncertain work. The broader support spec's structured retry classification
is deferred to supervisor stage, not encoded as a string heuristic in this prerequisite.
Builder report `re/sdd/270-service/service-architect-questions.private.md` was actually
written before its terminal usage-limit failure; inspected source-backed hazards are
incorporated here. It is a design buildability check, not implementation approval.

## Owned initialization refinement

Direct and HLS initialization now execute as transient manager-registered workers.
The owner retains the same-key lock, transition and reservation across caller
cancellation; a queued canceled waiter exits without opening. Registration occurs
under the existing finalization fence, and the join transfers immediately to the
cleanup coordinator so completion does not require the caller to poll again.
An already-started provider open finishes under its own startup bound. Canceled,
stopped and shutdown results await source shutdown outside finalization locks.
Manager shutdown consequently waits for pending live initializers. VOD resolve
and retained range semantics remain separate. A successfully published session
remains the shared map owner if its requester subsequently disappears.

Behavioral controls cover cancellation with a second same-key request at cap two
(no capacity masking), both direct/HLS paths, paused returned-source cleanup, and
a manager shutdown that waits without depending on requester polls. The actual
ACE discovery deadline now retains its run outside the selected acquisition
future, closes and joins before returning errors, and transfers successful runs
to the follower. Candidate selection still ends at the original deadline; joined
cleanup may finish afterward. Whole-stage acceptance remains pending.

Generation registration itself is synchronous under the finalization fence, before
the initializer task can first run. A stop after the public request becomes pending
therefore fences that generation even when provider work has not started. A direct-
and-HLS regression establishes this boundary without using spare-capacity failure
as a proxy. Cleanup completion transfers immediately, independently of result polls.

## Current acceptance evidence

Implementation and local acceptance controls are complete; final scoped review
passed and PR #328 is open. CI and merge remain pending. This is stage 1 only and does not close
#270 or provide an autonomous service.

- Corrected full workspace: 1,709 passed, zero failed, 15 ignored. Latest admitted-
  reader extension to an existing test passed separately; production was unchanged.
- Signed manager/provider/admission/startup/source integration retains the media
  handle, stops the producer, releases its slot and closes the source socket.
  A separately admitted inbound reader fetches the exact signed piece after
  producer removal, then its fixture closes and joins it independently.
- Same synthetic one-source workload on merged base and current records zero-,
  four- and three-viewer phases. Both keep one provider call/slot; FD/thread counts
  remain stable. Current confirmed stop releases the source despite a retained
  handle; base continues acquisition until that handle drops. The current manager
  adds one shared coordinator task until shutdown. CPU/RSS/task/stop-time samples
  and limits are retained privately; one debug sample establishes no statistical
  throughput or memory improvement. No new payload queue was introduced.
- Genuine RED/GREEN controls cover pending initializer cancellation, late EOF,
  paused cleanup, source pull failure, source/follower cleanup failure, coordinator
  false success and daemon failure propagation. Additional generation-isolation
  and reader/resource witnesses are supplemental, not separately claimed REDs.

The private execution ledger maps exact commands, source/binary pins, failure
receipts and scoped limitations. Failed cleanup is not a successful join; retained
failed owners remain admitted until their cleanup owner is destroyed. General
process abort or arbitrary destructor failure is not a clean-shutdown guarantee.
