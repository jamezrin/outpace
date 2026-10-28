# Bounded peer-command admission (#284) Implementation Plan

> **Status (2026-10-28):** Implementation, revised contracts, scan replenishment and F1 lifecycle correction are complete and independently reviewed. Fresh integrated workspace validation passed 1,493 tests (0 failed, 7 ignored), workspace Clippy, formatting and hygiene. PR #292 passed all nine checks on the reviewed production revision. Documentation corrections are under PR validation; merge and deployment remain pending.

**Goal:** Break both command/event cycles without evicting healthy peers for a normal 256-piece request burst.

**Architecture:** Pure per-peer queue admission credits constrain request ownership before enqueue; try_send never waits. Credited scans account for visited existing reservations within a fixed empty-batch policy horizon; public unconstrained scan behavior stays unchanged. Existing timed pool turns replenish opportunities without incoming messages. Command failures retain their generation until terminal handling; closed finished-worker reconciliation drains its bounded event prefix before synthesizing Lost. Normal and reconciled Lost share the producer-loss handler and once-only accounting.

**Tech Stack:** Rust, Tokio bounded mpsc, existing scheduler and pool loop; no dependencies or new channels.

**Spec:** docs/superpowers/specs/2026-10-28-bounded-peer-channels-284-design.md

## Global constraints and ownership

- Root accepted and supplied the revised RED. Luna owns tests, including current live_session/tests/pool.rs and scheduler.rs test sections; they remain untouched. Preserve #246 and all unrelated work.
- Production implementation: crates/ace-swarm/src/scheduler.rs, crates/ace-swarm/src/live_session/pool_support.rs, crates/ace-swarm/src/live_session/pool.rs. Report-first integration exception: the now-test-only scheduling wrapper/import in live_session/{support.rs,mod.rs} was gated with cfg(test), preserving its test body/interface and avoiding unused production items. No worker/options/public API change or #287 refactor.
- Keep command 64/event policy default 128, max_piece_advance 256 default, request-check 1000 ms default, all existing deadlines and reliable media/events.
- A routine full request queue defers without ownership/timer/loss. CLOSED means zero new request credit, not immediate runtime removal. Enqueue failures keep their generation for queued reliable events; failed unsent selections are released peer-locally. Removal/accounting occurs once at terminal handling.
- No new wake channel/task, capacity await, larger/unbounded queue, timer reset/purge, identifier exposure or dependency change.

## Review focus

- Shrinking scan range by queue credits can starve B behind full A.
- Worker drain without an incoming event must replenish request opportunities.
- Admission must not reserve/time 192 deferred pieces of a normal 256 burst.
- Closed/exceptional failure must retain queued messages, release only unsent selections, and reach producer-aware terminal cleanup once without hurting B.
- Full-loop TCP healthy recovery proves integration, not exact internal saturation.

### Task 1: Root assigns Luna's revised RED contracts

**Tests:** existing live_session/tests/pool.rs plus scheduler.rs test section. Existing live_session/reconnect_tests.rs remains the TCP recovery/cancellation control. Test-only ownership is assigned separately from production, especially scheduler.rs shared-file sections.

- [x] Preserve reported two component REDs as evidence. Update RequestPiece's positive contract to defer A66 unowned/untimed, retain A's 65 admitted pieces and send B67. Retain full 64/128 real-worker setup receipts and no-stall/cleanup assertions; do not force eviction or unchanged old reservation counts.
- [x] Add normal 256-burst component RED: single-poll the first advance before allowing worker polls and require Ready, no loss, 64 newly admitted and remaining 192 unowned/untimed when capacity was 64, bounded queue unchanged. Then real worker/socket drain and subsequent timed-turn opportunities deliver all 256 unique request commands, in order, no incoming messages, no eviction and policy cap never exceeded.
- [x] Pure scheduler tests: zero/mixed slots, window/choke filters, policy cap vs queue cap, missing credit=0, most-policy-spare/tie-break compatibility, and A-only early pieces/B-only later piece within ORIGINAL policy scan range. Existing unconstrained assign results must remain identical.
- [x] Test idle-tick admission despite unchanged retransmit state. First record zero capacity/deferral; drain via worker without any inbound frames; drive the existing wait expiry and assert fresh assignment before another event/retransmit. Do not model std::Instant aging by advancing Tokio time.
- [x] Test closed worker before snapshot and concurrent Closed after selection. If defensive Full cannot occur with the sole-producer production invariant, pin the cleanup via a bounded same-module test of the admission-result branch or a competing test-only sender; label it invariant-failure coverage, not normal workload.
- [x] Preserve B slots/timers, old admitted timer ages, partial/chunk/provenance/authenticated ready data; test peer release and actual record_pool_losses once, then stale Lost filtering.
- [x] Preserve Send Full/Closed cycle RED and once-only peer removal. Give the draining Send control a real free-slot barrier before its final enqueue; retain all 66 FIFO/no-loss/B-request assertions. A merely scheduled reader with a still-full Send queue remains overload. Include healthy mixed request/control traffic and explicitly cover a Send encountering a request-filled queue; no control-slot reservation is implied by credits.
- [x] Retain existing learned_source_recovers_after_peer_lost actual TCP test: first output, learned-source recovery/output, real consumer-close/producer-lease/gauge/task cleanup. Do not require it to certify internal 64/128 state.
- [x] Root received the queue-credit RED handoff, accepted the revision and assigned implementation. No impossible full-pool TCP saturation RED is a gate. Task 1 follow-ups are complete; production and test ownership remained separate during implementation.

**Actor-poll test seam choice:** Prefer a focused same-module test of the actual loop wiring using existing fixtures. If exact tick/queue receipts are unavailable, root may assign one cfg(test)-only per-Continuity watch receipt (one coalesced slot) at pre-wait/post-admission, carrying fixed stage, queue counts and assigned counts. It must only observe, never await/gate/inject scheduling or replace workers. No public API/global callback; production builds contain no receipt code. A small explicit seam contract may be accepted for that assertion; not an open-ended instrumentation prerequisite.

### Task 2: Pure queue-limited selection and nonblocking admission

**Modify:** scheduler.rs production sections; live_session/pool_support.rs. Tests remain Luna-owned.

**New crate-private interfaces:**
- Scheduler::assign_with_queue_slots(&mut self, next_needed: u64, head: u64, peers: &[PeerView], queue_slots: &BTreeMap<u64, usize>) -> Vec<(u64, u64)>.
- ActivePeers::assign_with_queue_slots(&mut self, scheduler: &mut Scheduler, next_needed: u64, head: u64, queue_slots: &BTreeMap<u64, usize>) -> Vec<PeerAssignment>.
- Keep public assign methods unchanged, sharing the same selection core with unrestricted queue credits. No fake PeerView.in_flight inflation or duplicate algorithm.
- Keep current async send_peer_command/advance_pool_requests signatures and loss-address returns.

- [x] Add credits BEFORE requested ownership; choose only peers with positive credit and policy spare and decrement both. Start the credited scan from remaining policy spare, extending once per visited reserved piece up to the fixed empty-batch policy horizon. Keep the public unconstrained scan unchanged; preserve ranking/tie behavior. The replenishment correction passed the unchanged 256-piece test and subsequent full gates after the initial 129-piece failure.
- [x] ActivePeers records only the constrained selected assignments. Unselected work must not enter active/global in-flight sets or create timers.
- [x] advance_pool_requests retains startup/skip guards, gives closed senders zero credit without removal, snapshots actual open-sender capacity and calls the constrained ActivePeers method directly. The support.rs wrapper body/test interface and unrelated call sites stay unchanged; its definition/import now compile only for tests under the reported integration exception.
- [x] Remove upfront batch timer insertion; try_send each selected RequestPiece with no intervening await. Only successful enqueue creates/retains requested_at.entry(piece).or_insert(now).
- [x] Unexpected Full/Closed aborts the failed worker as before but retains its generation. Cancel failed/skipped unsent selections only; preserve admitted work/timers and queued reliable events until terminal handling. Continue B; no early loss accounting or global cleanup.
- [x] Send stays try_send; Full/Closed returns a failed-admission marker and retains the generation after abort, without immediate accounting. The pool callers count loss only through terminal handling. Retain worker reliable Message/Lost sends and existing Drop teardown.

### Task 3: Existing timed credit poll and GREEN

**Modify:** live_session/pool.rs, existing timed admission plus terminal event reconciliation. No new tasks, interval, select wake variant, event type or media buffer.

- [x] After existing retransmit_stalled_requests, ALWAYS call advance_pool_requests and record_pool_losses before empty-pool/wait logic. It must not depend on retransmit.changed, incoming activity or a new pending flag.
- [x] Keep timeout(wait) <= request_check_interval and timeout -> continue. Thus idle capacity released just after a scan is examined next turn; repeated events also reach the loop. Keep existing stale/startup deadlines and output cancellation; admission/poll is not progress and cannot refresh them.
- [x] Source-review sole-producer/no-await proof and both cycle breaks, queue-limit ownership/timer transitions, fixed finite loop work, loss idempotence, fairness and missing-peer credit 0.
- [x] Luna runs all revised component/scheduler/tick cases, actual TCP recovery/cancellation, ace-swarm and workspace tests, formatting, Clippy and hygiene. No skipped positive contract, weakened bounds, changed default queue or fabricated full-pool saturation claim.
- [x] Fresh independent review of complete diff. Root coordinates external contributor test-only interest/overlap before publishing; no message is authorized here. Normal hooks and PR/CI checks passed for the production revision. Keep #284 open until integration completes and avoid historical #246 claims.
- [ ] Merge PR #292 after validation of the documentation correction; deployment is a separate follow-up.

### Scan replenishment correction after first GREEN failure

- Evidence: scheduler 18 tests passed on the prior source; the actual 256 component stopped at 129 with capacity 64/64. Test owner stopped before remaining controls and did not weaken the contract.
- Let n=next_needed, S=sum remaining policy spare, U=unchoked peer count, M=per-peer policy cap. The old scan ended at min(head,n+S). For one 256-cap peer at n=1 with 128 reserved, S=128 gives ceiling 129, so only 129 is newly visible.
- The credited scan starts at that old ceiling but each visited existing reservation extends it by 1, capped at H=min(head,n+U*M), with saturating arithmetic. It visits at most U*M+1 positions; it does not use queue credits, head magnitude or the highest reservation as a scan budget.
- For head 256, n=1, M=256 and fresh 64-slot worker opportunities, the source-derived cumulative admissions are 64/128/192/256, with the cursor, existing ownership and old timers unchanged. The unchanged 256-piece regression subsequently verified complete FIFO delivery across drains.
- Public assign uses H equal to its original ceiling, so the extension cannot change its existing scan behavior. Credited selection alone repairs the multi-turn equivalence to the old empty-batch opportunity.
- [x] Prepare scheduler production-only correction; preserve the entire test module. Gate the unused PeerAssignment import with cfg(test), and restore the production import wrapping reported by fmt.
- [x] Luna first reruns the unchanged queue_credit_admits_a_default_256_piece_burst_across_worker_turns. Stop/report any failure before more tests or fixture changes.
- [x] After that passes, rerun scheduler 18 and saturated_ controls, then add/pin bounded reserved-hole/public-parity cases and remaining idle-poll/loss/partial/mixed-pressure controls in separately owned test sections. Resume full gates and fresh review; no new TCP saturation oracle.

### F1 lifecycle correction after product-review RED

This correction replaced the earlier remove-before-snapshot proposal; the companion spec now reflects the implemented event-ordered lifecycle.

- [x] CLOSED gets zero credit. Keep the peer generation current while queued Message/gossip/Lost events are processed. Failed command admission does not directly remove/count the peer.
- [x] Add crate-private ActivePeers::cancel_assignment(id,piece) for failed/skipped unsent selections only; keep old timers, B slots, partial and verified data.
- [x] Add poll_closed_peer_event: only after worker finished AND command receiver closed, retain an identity-scoped counter capped by existing event capacity. Poll one FIFO event per turn, decrement on actual dequeues, and synthesize Lost only after that prefix or fully free queue capacity (including no reserved permits). No event purge/buffer/task/channel or capacity wait.
- [x] Route both normal and synthetic Lost through the extracted private handle_pool_peer_loss, preserving producer-specific obsolete-Source refresh, ordinary rescheduling/counting and pool-empty result. Prioritize the finite queued prefix while preserving consumer-close cancellation.
- [x] Address actual Send Full/Closed and defensive request failures too: the existing abort action remains, generation removal/accounting is deferred; failed unsent reservations are canceled peer-locally. send_peer_command's Some(address) means not-admitted, NOT accounted loss. Pool call sites were migrated.
- [x] FIRST Luna reruns the unchanged queued_worker_message_remains_current_before_terminal_loss_is_drained RED.
- [x] Luna migrates the old closed-before-credit/immediate-Send-removal test expectations to event-ordered terminal cleanup. Preserve all nonblocking/B-progress/full 64/128 receipts and bounded teardown; no test changes by the production implementer.
- [x] Add normal producer vs finished-without-Lost controls using the shared terminal handler: queued message/gossip first, last-producer obsolete-Source refresh, usable peers retained, loss/cooldown/gauges once. Prove fallback after at most existing-capacity dequeues under continued healthy traffic or immediate true queue quiescence; stale-generation barriers cannot survive replacement.
- [x] Verify cancellation/idle timed credit/256 burst, unsent cancellation and old timer/partial/verified retention, Send overload/reliable-event ordering, then full GREEN and fresh review. No full TCP saturation claim and no new observer seam.

The fallback bound is in existing FIFO events/processing turns after worker completion, with existing handler, stale and consumer-cancellation bounds unchanged. It is not an independent hard wall-clock promise through arbitrary executor or I/O stalls. Active failure abort retains its prior cancellation semantics for in-progress work; the new paths preserve every already-enqueued reliable event instead of invalidating its generation.

Validation commands used across the lifecycle and integrated validation (detailed receipts are retained privately):
```sh
timeout --signal=TERM --kill-after=5s 180s cargo test --offline --locked -p ace-swarm saturated_ -- --nocapture
cargo test --offline --locked -p ace-swarm learned_source_recovers_after_peer_lost -- --nocapture
cargo test --offline --locked -p ace-swarm scheduler
cargo test --offline --locked -p ace-swarm
cargo test --offline --locked --workspace
cargo fmt --all --check
cargo clippy --offline --workspace --all-targets --locked -- -D warnings
python3 tools/hygiene/check_identifiers.py
```

Missing offline cache is reported, not fetched or worked around with dependency changes. Root/Luna run whitespace/Git checks only at their authorized stage. Existing required Rust lint/test and hygiene CI checks remain merge gates.

Lifecycle self-review: closed credit exclusion is separate from generation retirement; native FIFO/capacity bounds protect queued messages; normal and synthesized terminals share the same producer-aware handler. Enqueue failures no longer bypass that path, and only unsent selections are canceled early. Credits/scan/fairness/timers, queue sizes and peer worker remain intact. No public API or worker event changes; runtime lifecycle state and tests are covered by the final review. The ignored SDD points to the new private exact lifecycle diff, hashes, ordering proof, migration contract and GREEN requests. Final lifecycle and integrated GREEN receipts supersede the earlier pending status. No full-pool saturation claim is made.

