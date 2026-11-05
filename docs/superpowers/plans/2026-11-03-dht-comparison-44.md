# DHT comparison harness implementation plan (#44)

> Use superpowers:executing-plans with test-driven development; final independent review before publication.

**Goal:** Compare actual candidate lookups with bounded, owned, explicit behavior; no production adoption.

**Spec:** docs/superpowers/specs/2026-11-03-dht-comparison-44-design.md

## Task 1: Shared worker input contract

- [x] Add tools/dhtcompare as its own Cargo workspace; root production manifest/lock unchanged.
- [x] RED tests for exactly20byte hex hash,1..4explicit numeric IPv4 seed endpoints, zero-port rejection, duplicate seeds, absolute15s budget and target8/cap1024. Worker inputs are environment-only; no registry access, raw hash output or command argument.
- [x] Implement strict parsing with sanitized errors and redacted Debug.
- [ ] Common worker entry must deny default/unrecognized operations before constructing sockets.
- [x] GREEN parser tests and tool lint; network entry/default-deny gate remains unimplemented.

## Task 2: Real workers and qualification

- [ ] Add pinned mainline8.0.1 andn0-mainline0.6.0 adapters plus actualcustom cfgtestworker using existing numeric-seed seam, with per-batch threshold semantics and same numeric records.
- [ ] Test actual UDP paths in confined synthetic topology; prove bootstrap, noimplicitannounce/server, silent/late/duplicate/malformed handling, constructor/deadline and cleanup behavior. Record source behavior differences rather than weakening assertions.
- [ ] Parent supervisor owns PIDnamespace/children, bounded64KiB retained pipes/draining, originalexit, clock origins,15slookup/30sworker/120sblock bounds and failclosed uncertainty.

## Task 3: Review and integration

- [ ] Appropriate repository and tool gates; dependency/license/lock/source pins, independent architecture/performance/test-fidelity review.
- [ ] Commit/hooks, PR/link, CI and exact-head merge after success; no release/readiness claim.
- [ ] Separate fresh reviewed runtime plan before one live discovery block. No automatic matrix/retry or adoption without comparison evidence.

## Offline qualification progress

- [x] Actual custom, mainline and n0-mainline target-path fixtures.
- [x] Actual silent and malformed/uncorrelated/duplicate fixtures for all three.
- [x] Fixed offline nine-case runner with binary pins, original exits, independent
  namespace cleanup, exact-test execution checks and an absolute 180-second suite limit.
- [ ] Late replies, construction/lookup cancellation and destination/server observation.
- [ ] Common live worker/supervisor, fresh runtime gates, live results and recommendation.

The offline suite does not complete Task 2 or #44. No live entry is exposed.

## Review and verification of the offline increment

Independent review found two fixture-fidelity defects: an invalid-width forged
transaction ID and safety-negative coordinator fixtures rejected before their
intended safety checks. Both were reproduced and corrected. The schema guard and
the strengthened safety tests pass; removing the safety predicates is rejected.
The corrected upstream UDP cases also pass in owned offline namespaces.

Workspace tests: 1,628 passed, zero failed, 14 ignored. Tool tests: seven passed,
six UDP cases ignored by default and separately exercised. Python tests: 16 passed.
Workspace/tool Clippy, formatting, identifier hygiene and whitespace gates pass.
CI includes the standalone tool's contracts/lints and pure coordinator tests;
real namespace qualification remains an explicit operator-run suite.

These gates qualify the offline increment only. Commit/PR/CI integration remains
pending; no live measurement or issue-closure claim follows.

## Client-mode and cancellation follow-up

- [x] PR #320 merged the initial offline increment after CI and review fixes.
- [x] Two actual upstream cold-client probes: read-only query flags, unfinished
  lookup across a 250 ms unsolicited-ping observation, then normal target completion.
- [x] Behavioral RED/GREEN for a late stream-end reported as exhausted after deadline.
- [x] Consumed-construction-budget and stream-drop ownership contracts; these do not
  claim interruption inside a real constructor or internal actor joining.
- [x] Witness-triggered external cancellation of all three actual lookups with original
  SIGKILL statuses and namespace cleanup, through a fixed 40-second offline profile.
- [x] Independent review found no scoped P1/P2; integration gates recorded privately.
- [ ] Late UDP replies, destination policy and the live comparison remain outstanding.

## Bounded emission-record follow-up

- [x] Retain the first 64 adapter-emission sizes, total observations and explicit
  history truncation; preserve unique-peer cap and target-crossing batches.
- [x] Expose unique target overshoot in upstream records and custom JSON.
- [x] Missing-interface REDs and real custom JSON missing-field behavioral RED retained.
- [x] All three actual isolated UDP target cases assert batch size, total and overshoot.
- [x] Independent review found no scoped P1/P2; workspace/tool tests, Clippy,
  formatting, hygiene and whitespace checks passed. PR/CI integration remains pending.

Custom response-parser and upstream stream boundaries differ; these counts are
not raw-wire equivalence or a live performance comparison. The custom observer now rejects post-deadline batches before accounting and retains
earlier peers. Its deterministic controls cover that boundary; actual late UDP
qualification is recorded in the follow-up below, without a live comparison claim.

## Beyond-budget UDP opportunity follow-up

- [x] Actual custom/mainline/n0 cases observe a get_peers query and send a correlated
  ten-peer response after16seconds, with zero admitted peers and bounded lookup.
- [x] Separate fixed80second/three22second-case profile, exact execution and both
  socket witnesses, known namespace cleanup and failure/truncation controls.
- [x] Allthree isolated cases pass in48.4seconds; target-positive controls pass.
- [x] Numeric report query marker selection corrected with a behavioral regression;
  original socket report/logs retained privately.
- [ ] Independent review and PR/CI integration of this follow-up.

Early exhaustion can precede the late send. Sending to a no-longer-polled socket
does not prove delivery or internal rejection. This adds offline record/containment
evidence only; public destination confinement, live workers/results and an adoption
recommendation remain outstanding.
