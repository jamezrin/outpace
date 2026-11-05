# DHT late-reply qualification implementation plan

> Use superpowers:executing-plans; independent review before publication.

**Goal:** Qualify late UDP result exclusion in actual research lookups without changing production discovery.
**Spec:** docs/superpowers/specs/2026-11-05-dht-late-replies-44-design.md
**Architecture:** Existing actual-library fixtures and custom cfg(test) walker, isolated by the existing external namespace owner. One new fixed profile; no general live input surface.
**Tech:** Rust/Tokio, standalone pinned candidates, existing Python namespace runner.

## Constraints and review focus

-15second lookup origin includes construction; do not quietly shorten it in tests.
-Actual query and late-send witnessed; early exhaustion is recorded honestly.
-Tasks joined even when lookup times out; fixture cannot fail before cleanup.
-Late stream batch cannot become target, and positive target still passes.
-Exact one test execution, bounded pipe retention and known external cleanup required.

## Task1: Actual late UDP fixtures

Files: tools/dhtcompare/tests/real_udp.rs and crates/ace-swarm/src/dht/comparison.rs.
- [x] Add two upstream cases mainline_real_udp_late_reply/n0_real_udp_late_reply and custom_real_udp_late_reply using actual run/run_comparison. Seed handles bootstrap normally, withholds get_peers peers until16seconds after witnessed query; assert valid late transmission, no count/first-peer/target, lookup bounded and all joins.
- [x] Retain early-exhaustion vs deadline distinctions; print sanitized numeric witnesses only. Do not claim late receive/internal rejection from send alone.
- [x] Build exact ignored binaries without running sockets; run only through offline namespace helper. Run existing target positives as controls. If RED identifies actual behavior conflict, stop and record evidence before changing production.

## Task2: Fixed profile and coordinator negative controls

Files: tools/dhtcompare/qualification.py, test_qualification.py, README.md, existing44plan status.
- [x] First add failing mock orchestration test for late-replies schedule:3exact cases,22second timeouts,80second whole-bound, rejects missing send/query witness or zero-test success and cleanup uncertainty.
- [x] Implement only this fixed profile, preserve9case/default and40second cancellation profiles. Update CLI/docs with exact binaries and source/test-count limits; repair stale custom-clock status without claiming live acceptance.
- [x] Focused Python gates and actual3case qualification, tool+affected Rust Clippy/fmt/hygiene/diff. No redundant whole workspace suite for test-only changes unless failures justify it.
- [ ] Independent review of test fidelity/cleanup/measurement wording, then normal hooks commit/PR/CI and exact-head merge. Keep44open; live destination policy/results/recommendation remain outstanding.

Task boundaries are one cohesive test increment. No routine human approval checkpoint under standing delegated goal; uncertain protocol/externally visible requirements remain escalated.
