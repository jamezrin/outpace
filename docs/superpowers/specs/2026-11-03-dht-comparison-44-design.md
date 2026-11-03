# Bounded DHT comparison design (#44)

Status: implementing offline qualification; no adoption or live clearance. Only milestone44; existing custom production discovery stays unchanged.

## Purpose and alternatives

Compare current custom walker with fixed mainline8.0.1 and n0-mainline0.6.0 under identical bounded inputs. Do not choose based on Tokio branding, peer count alone or dependency age. Directly replacing production DHT would combine architecture and measurement, so rejected. Selected approach: separate external comparison executable with isolated per-candidate processes, plus deterministic loopback qualification; adoption is a later evidence-based decision. A library actor continuing after dropped lookup is contained by the owned process, not claimed cancelled.

Primary sources inspected2026-11-03:
- https://docs.rs/mainline/8.0.1/mainline/struct.DhtBuilder.html
- https://docs.rs/mainline/8.0.1/mainline/async_dht/struct.AsyncDht.html
- https://docs.rs/n0-mainline/0.6.0/n0_mainline/struct.Dht.html
- https://docs.rs/n0-mainline/0.6.0/n0_mainline/struct.DhtBuilder.html
No claim these documents establish cleanup, query cancellation, security, memory bounds or superiority. Versions are pinned candidates observed in this research, not a perpetual latest-version statement. Actual source/license/dependency/runtime review remains before adoption.

## Existing contract

ace-swarm discover.rs runs tracker and DHT concurrently; target defaults8, budget15s, incremental output capped1024. A weak tracker response cannot crowd out DHT. Observer false cancels custom harvesting. DHT announcements are separate explicit calls with token/peer-port semantics; discovery must not silently become announcement or server mode. Current cached-router candidate was rejected under42startup regression acceptance; retain stale/cold failure controls rather than reusing a warm-cache-only win.

## Measurement boundaries

One runtime-derived input hash supplied only via environment or protected stdin; no registry reading and no identifiers/endpoints in tracked fixtures, summaries or command arguments. Same numeric bootstrap list for all arms, resolved once under the approved network gate; retain DNS cost separately, do not claim timed walks include it. Disable implicit bootstrap defaults and server/announce behavior unless independently proven necessary and declared. IPv4 comparable endpoint policy, deduplicate/cap1024, peer target8. Candidates must demonstrate the configured bootstrap list is actually honored before live admission.

Cold cases use fresh processes and sockets with no routing-cache reuse. Measure monotonic time from before construction through lookup, first eligible unique peer, and terminal cleanup. Report failure/timeout/no-peer distinctly; no first-peer value when none found. Process startup overhead recorded separately if worker-origin clock starts after exec; constructor/bootstrap delay cannot disappear from that clock. Do not mix cold/warm results. No repeated live matrix by default; proposed first observation is one three-arm matched block with15s per lookup, max30s owned worker and120s whole observation including infrastructure checks/cleanup. This is a proposed bound, not live authorization; fresh reviewed source pins, tailgw-0/Mullvad provenance and synthetic qualification precede admission. No playback/source fetch in this harness. Announcements remain OFF in first discovery comparison.

Counts: unique advertised IPv4 endpoints are not reachable peers or playback readiness. Query/timeout/error diagnostics unavailable from an adapter are null with a reason, never zero. Collect numeric elapsed/first-peer/count/budget/terminal/cleanup records only. Retain private diagnostics bounded64KiB/worker, drain beyond cap and flag truncation; no inherited stdout ambiguity. Establish clock origin/caps with deterministic tests. Actual CPU/RSS observations require a separate justified sampling plan; source reasoning is not a measurement.

## Ownership and integration

Each worker owns exactly one candidate instance and one bounded lookup. Stop on target, observer cancellation or absolute deadline; drop resources and prove local process/socket terminal before next arm. Constructor must not block the parent runtime. Parent enforces a whole-operation deadline and exact owned kill/reap. Library background actor/thread ownership is explicitly inspected; process exit is the hard boundary for this research and is NOT evidence that a future in-process production adapter is cancellation-safe. Preserve real errors and original exits.

Deterministic qualification: silent/late bootstrap, multiple/coalesced peer batches, duplicate filtering/cap, target stop, drop during construction and lookup, no automatic announce, exact bootstrap destinations, malformed replies and terminal cleanup. Use local UDP synthetic nodes only. A candidate unable to expose/verify these controls is recorded as a limitation, not shimmed into apparently equivalent results. Do not use a fake wrapper alone to claim its real library passed.

Future adoption must preserve public discover_peers_with_options API/source fairness and independent budgets, incremental admission cap/observer cancellation, transport policy, explicit announce ports/token semantics and default network exposure. No dependency manifests or production paths change during methodology research.

## Implementation sequence

1. Inspect pinned upstream source for construction, bootstrap defaults, get_peers stream, cancellation/drop/actor ownership, announce behavior and license/dependencies. Record exact unsupported controls.
2. Create compliant chore/dht-comparison-44 branch from then-currentmain in an isolated worktree; verify ancestry; standalone tools package to avoid root production lock churn.
3. Test-first numeric record/clock/deadline/owned-process coordinator with loopback/fault fixtures, then real per-library adapters qualified with synthetic UDP nodes. Retain RED/GREEN and source/binary pins. No live content identifiers in tests.
4. Independent architecture/performance/test-fidelity review and gates; publish PR only with concrete reviewable runnable offline harness. CI/no-network default; live opt-in denies without reviewed runtime prerequisites.
5. Only after qualification, concrete live plan and fresh gates: one bounded discovery block; preserve all outcomes, no automatic retry. Write comparison/recommendation separating discovery count from startup readiness. No library adoption without proven benefit and owned in-process lifecycle.

## Current evidence and remaining scope

Pinned upstream sources and dependency graphs have been inspected and built in the
standalone tool workspace. All three actual lookup implementations have passed
loopback target, silent-node, and malformed/uncorrelated/duplicate-response tests
inside separate offline namespaces. These are qualification results, not a live
network comparison or an adoption recommendation.

The offline qualification runner pins the two test binaries, runs nine fixed cases
with a 180-second whole-suite deadline, and stops on failure or uncertain cleanup.
This is a separate fault-test suite, not an extension of the proposed 120-second
live comparison block. Test output must prove that the exact selected test ran;
a stale binary matching zero tests cannot qualify.

Late replies, cancellation during real construction/lookup, observed destination
scope across all threads, and server/announce behavior still need qualification.
The common live worker entry, externally enforced destination policy, fresh runtime
plan, written live comparison, and recommendation remain outstanding. There is no
live entry in the current tool. Issue #44 remains open; the independent #271 scope
question and #269 native-consumer evidence gaps remain recorded in the private ledger.

## Builder-check resolutions (delegated engineering decisions)

- Custom arm: use a cfg(test)-only worker in the existing dht module with the existing numeric-seed/observer seams and actual production walker, built as its unit-test binary. Do not publish a research API or rewrite/copy the algorithm. Pin the real source and the test-only diff. External candidate workers remain separate executables. If a needed terminal/error fact is unavailable, report unknown instead of changing production to manufacture telemetry.
- Target: preserve atomic emitted-batch behavior and cap1024; stop after an emission batch crosses8unique endpoints. Record batch sizes and overshoot. Do not claim counts around8rank quality. All arms use IPv4/nonzero-port eligibility; no peer dialing. Record library-level exclusions as differences. Live routing destinations must be externally contained to approved global addresses before admission, because current compact parsing accepts private addresses and library policy is not assumed. Synthetic loopback exceptions exist only inside an isolated no-external-egress topology with observed attempted destinations across all worker threads.
- Common seeds: at most four numeric bootstrap nodes. Returned routing nodes remain discoverable according to the configured destination policy; exact bootstrap does not prohibit legitimate iterative lookup. Disable automatic server/announce/signed-peer extras or classify a candidate as non-equivalent. Source and confined real-UDP tests must prove this through construction, lookup and teardown.
- Clocks: P0parent pre-spawn; W0worker immediately before candidate construction; W1first eligible unique endpoint; W2lookup terminal; P1parent reap. W0starts the absolute15second budget, including constructor/socket/binding/bootstrap. Compare W1-W0 andW2-W0 only within that worker clock; separately report P1-P0. Distinct terminal codes for target/frontier-exhausted/deadline/adapter-error/protocol-error/parent-kill/cleanup-unknown. No cross-process Instant subtraction.
- Ownership: retain original exit, graceful/forced mode and independent parent reap. Drain capped pipes throughout. Linux-owned PID namespace containment (or separately proved equivalent) bounds descendants; reaping a process-group leader alone is insufficient. If cleanup is uncertain, stop the block and do not start another arm. The30second worker ceiling includes15seconds lookup plus15seconds cleanup, never an extra lookup budget.
- Total bound:120seconds = at most90seconds workers plus30seconds prerequisite/DNS/infrastructure/outer-cleanup reserve. Parent uses one absolute deadline; gates consuming reserve abort before an arm starts, without extensions/retries. No promised live admission until real source and fault-test qualification prove feasibility.
- Order for a first descriptive block: custom,mainline,n0-mainline, fixed in advance; temporal/node-ID/remote-cache bias explicitly recorded. One block cannot support statistical superiority or an adoption decision alone. A later order-balanced block requires a separate concrete need and bounded plan, not an automatic matrix.
- Repository/source check found compact_peer only rejects wrong length/zero port; frontier accepts advertised nodes after source/transaction correlation. This is source evidence of accepted address scope, not a newly reproduced exploit or an authorization to change production defaults. Hold live comparison until common egress confinement is actually qualified.

Builder report retained alongside design. These choices settle implementation interfaces without user approval; upstream source inspection is still required before executable code or live bounds are claimed validated.
