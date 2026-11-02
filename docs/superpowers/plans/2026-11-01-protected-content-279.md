# Protected content refusal implementation plan

Status: implementation, offline validation and independent review complete; PR integration and CI pending.

Design: ../specs/2026-11-01-protected-content-279-design.md.

1. Wire contract: add synthetic integration regressions for strict classification and authoritative placement in transport/metainfo. Observe RED; implement one protection classifier and minimal decoded-model plumbing. Preserve original info bytes and existing geometry validation. Run wire tests.
2. Resolver contract: add tests for typed terminal protected/invalid-protection errors across descriptor URL/content-id, fallback and warm-cache paths. Implement inspection state in ace-swarm resolution and engine content/selection caches. Keep metadata-only analysis separate from playback refusal. Assert no discovery or provider work on refusal.
3. Admission/reporting contract: test native/CLI/HLS/VOD and compatibility routes; getstream response null/error without token or URLs; protected/public analysis flag and explicit descriptor-unavailable analysis error for bare selectors. Implement shared admission rather than facade-only rejection. Update protocol compatibility matrix.
4. Verification: focused wire/swarm/engine tests, workspace offline tests, all-target Clippy, formatting, identifier hygiene and diff checks. Independent correctness/architecture/performance/test-fidelity review; fix concrete findings. Preserve private provenance separately from tracked docs.
5. Integration: normal commit hooks, focused PR with scope/validation, address CI/review feedback, merge exact reviewed head after gates. Close #279 only after every route criterion is evidenced. No deployment required to prove refusal-before-network behavior.

Performance: classification is a single decoded-map lookup, small copied state, no repeated decoding/refetch or descriptor retention solely for a flag. Public-session scheduling, queues, latency and operator configuration remain unchanged.

## Verification checkpoint (2026-11-02)

Wire classification, inspected-state propagation, terminal resolver errors, shared admission,
inspection-only analysis and native/compatibility refusal reporting are implemented. Focused
controls cover raw and disk VOD parsing, warm live/HLS/VOD reuse, URL metadata authority,
analysis isolation and native route error bodies. Independent review found no remaining
concrete P1/P2 blocker after the refusal-text correction.

Offline workspace validation passed: 1,606 tests, zero failures and seven existing ignored
network tests across 32 test binaries. Workspace all-target Clippy, formatting, identifier
hygiene and whitespace checks passed. Classification adds a decoded-map lookup and copied
state; performance impact is assessed from source, with no throughput benchmark claim.

Public raw/disk VOD compatibility getstream selection remains unsupported; this change
refuses protected descriptors there without expanding API parity. Some supplemental witnesses
were GREEN controls rather than independently demonstrated RED tests; private receipts retain
that provenance. Synthetic offline evidence establishes refusal boundaries, not live readiness.

Normal commit hooks, PR CI and reviewed integration remain pending. No live validation or
network/configuration change is required for this refusal-before-discovery contract.
