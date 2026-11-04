# Private peer policy implementation plan (#271)

Spec: ../specs/2026-11-04-private-peer-policy-271-design.md

- [x] Add bounded immutable endpoint policy, exact membership and deny-before-dial tests.
- [x] Thread policy through live candidates, initial/refill/reconnect sockets and VOD workers.
- [x] Suppress private public discovery/catalog work and persistent hints; guard metadata dials.
- [x] Admit operator files once; preserve protection and provenance across live/VOD aliases.
- [x] Normalize/validate startup configuration and carry policy into runtime, CLI and HTTP state.
- [x] Guard inbound/mapping/announcer construction, eager compatibility fetches and CLI widening.
- [x] Add redacted native/compat status, stable refusals and captured logging controls.
- [x] Document topology, controls, unsupported cases and operator workflow.
- [x] Complete real local-alias runtime playback and valid private-runtime negative admission witnesses.
- [x] Audit every dial, advertisement and externally exposed error/status path against the spec.
- [x] Run final workspace gates and obtain independent security/performance/test-fidelity review.
- [ ] Address findings, commit through normal hooks, open PR, handle CI/review, merge and close #271 only with complete evidence.

Validation provenance and exact offline receipts are maintained privately. Initial
characterizations are distinguished from reproduced defects. No readiness, anonymity,
private origin distribution or closed-engine parity claim follows from this feature.
