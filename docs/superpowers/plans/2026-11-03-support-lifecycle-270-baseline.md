# Support lifecycle baseline implementation plan

> Execute inline; obtain independent test-fidelity review before integration.

**Goal:** Prove the existing signed acquisition/publication/reseeding/cancellation composition for #270.
**Architecture:** Real follower between two owned loopback seeding connections; no product changes.
**Tech stack:** Rust, Tokio, existing RSA live authentication and bounded stores.
**Spec:** docs/superpowers/specs/2026-11-03-support-lifecycle-270-baseline-design.md

- [x] Add `live_session/tests/support_lifecycle.rs` and its module declaration. Create source and support admission futures using max_inbound/max_per_stream/max_unchoked = 1, retaining every JoinHandle.
- [x] Publish signed synthetic source pieces, start the real follower with a capacity-one receiver, await complete support publication under a deadline, and retrieve through an independent real peer handshake/request exchange.
- [x] Assert exact signed bytes/header and verification, no local playback/drain, then close receiver, join follower, assert producer removal and zero peer gauge. Drop leecher and join admitted serving tasks; abort/reap every remaining task on timeout before assertions.
- [x] Run the focused baseline, relevant existing authentication/registry controls, crate tests and normal lint/format/hygiene gates. Record failures without treating an initial passing characterization as a product RED.
- [ ] Obtain independent review, fix concrete findings, commit/open PR when gates pass. Keep #270 open and record remaining service requirements.

Review focus: genuine acquisition rather than injected support state; signed whole-wire identity; bounded cancellation; snapshot versus registry lifetime; no detached fixture tasks; claims limited to this finite topology.

Validation: 509 swarm tests passed, 8 ignored; focused lifecycle pass after review correction. Swarm all-target Clippy, formatting, hygiene and whitespace gates passed. Independent review identified premature-return ambiguity; pre-close running/registry assertions address it. PR/CI integration remains pending. This is a characterization baseline, not a reproduced product defect or #270 completion.
