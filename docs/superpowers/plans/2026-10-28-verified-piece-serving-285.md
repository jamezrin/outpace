# Verified piece serving Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans after root accepts this design. No delegates or implementation are authorized by this design-only handoff.

**Goal:** Make signed live seed bytes visible only after successful whole-piece authentication.

**Architecture:** Reuse reassembler buffers; expose full accepted wire completions before playback drains them. Publish one whole piece atomically in memory or the disk actor; leave trusted writer APIs compatible.

**Tech Stack:** Rust, Tokio, existing RSA signing helpers and disk actor; no new dependencies intended.

**Spec:** `docs/superpowers/specs/2026-10-28-verified-piece-serving-285-design.md`

## Global constraints

Root acceptance is required before implementation. Preserve root's authentication RED and all unrelated dirty work. No dependence on unpublished #284. No queue redesign, default changes, public API widening for tests, real identifiers, registry reads or live testing. Preserve trusted broadcast/VOD semantics and local playback bytes. New interfaces and exact policy are defined in the spec.

## Review focus

- Keyless nonzero-trailer playback must not accidentally grant verified publication (Task 1/3).
- Header conflicts and malformed completed duplicates must not destroy valid ahead bytes (Task 1/3).
- Cancellation after disk admission must retain the permit and settle accounting (Task 2).
- Concurrent duplicate puts and eviction must not expose mixed generations (Task 2).
- Actual id=4/id=5 frames must be observed, rather than inferred from store lists (Task 3).

### Task 1: Expose accepted full-wire completion

**Ownership:** `crates/ace-wire/src/reassembly.rs`, including colocated tests. Interfaces: `LiveBlockOutcome`, `LiveAcceptance`, `CompletedLivePiece`, `add_live_chunk`, `completed_live_piece` exactly as specified. Existing generic APIs stay compatible.

- [ ] Add synthetic failing tests for complete-ahead observation, preserved signature versus stripped playback, contribution/no-op behavior, strict geometry and consistent headers.
- [ ] Run `cargo test --offline --locked -p ace-wire reassembly`; require the new assertions to fail for the intended missing behavior before implementation.
- [ ] Implement the new live entry point and borrowed completion view using metadata in existing entries; distinguish unsigned and unauthenticated modes. Add controls for nonzero trailer without verifier, conflicts followed by valid retry and malformed completed duplicates.
- [ ] Run the same focused command; require all tests pass, including existing provenance and playback tests.

### Task 2: Atomic whole-piece store transaction

**Ownership:** `crates/ace-swarm/src/store.rs`, `crates/ace-swarm/src/store/disk.rs`, colocated tests. Consumes full wire/header from Task 1; produces `shared_put_complete_piece -> io::Result<CompletePut>` as specified. No cryptographic verifier in store.

- [ ] Add backend-parameterized failing whole-publication tests: atomic visibility, exact chunks/header, immutable complete duplicates, disabled/undersized budgets, exact accounting and eviction, invalid geometry. Add disk barriers/failure injection solely under tests.
- [ ] Run `cargo test --offline --locked -p ace-swarm store`; require intentional new behavioral failures.
- [ ] Implement memory atomic insertion and actor whole-file publication, single per-store pending permit acquired before copy, command-owned permit, atomic index/budget update and temporary cleanup. Retain legacy trusted writers and existing disk FD bound.
- [ ] Add cancellation before/after admission, concurrent duplicate, I/O failure, eviction/read race and actor-drain controls. Assert pending bytes return to zero and retained bytes stay within budget; assert no partial reply/index at write barrier.
- [ ] Repeat focused store tests; require pass including `cancelled_disk_put_still_commits_index_and_budget_atomically` and existing budget/FD controls.

### Task 3: Switch real pool and prove wire behavior

**Ownership:** `crates/ace-swarm/src/live_session/pool.rs`; only minimal metadata cleanup in `live_session/continuity.rs`/`live_session/mod.rs` if required by the chosen interfaces; coordinate root-owned `live_session/tests/authentication.rs`. Tests additionally in `live_session/tests/seeding.rs`, `seed.rs` or its private test modules, and `seed/protocol_tests.rs`. Do not change seed/protocol production visibility. Add `docs/protocol/notes/55-verified-live-store-publication.md` only if the filename is still free, otherwise select the next unused note number.

- [ ] Re-run root's exact regression: `timeout 300s cargo test --offline --locked -p ace-swarm rejected_signed_piece_retries_through_full_peer_pool -- --nocapture`. Preserve actual RSA/full-pool entry and post-cleanup assertions. Require the documented all-true rejected state before switching publication.
- [ ] Add phase-barrier controls listed in spec: first partial, rejection, last-chunk-first retry, valid complete-ahead, corrupt duplicate, cancellation. Exercise memory and disk stores; generated keys and loopback only. Decode actual inbound bootstrap id=5 and advancing id=4 with excluded bad/incomplete index and included valid positive marker. Pin exact reply bytes and profile rules plus inbound BT read behavior.
- [ ] Replace pool per-chunk writes with Task 1 completion followed by Task 2 transaction before playback drain. Gate publication on `SourceVerified` or `UnsignedAssemblyAccepted`; preserve recovery, provenance and playback accounting. Cache I/O failure must not masquerade as signature rejection or suppress valid playback.
- [ ] Run focused authentication, store, seed and reassembly tests; require all new negative and positive controls pass. Run existing engine broadcast/VOD tests to preserve writer fences and final-piece compatibility.
- [ ] Write the protocol policy note; describe unsigned acceptance, signed wire retention, trusted writer scope, cancellation and cache-failure limits. Supply #208 gate evidence in local handoff without public checklist writes.
- [ ] Run `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all --check`, and `python3 tools/hygiene/check_identifiers.py`. Format only touched Rust files if needed; do not sweep unrelated formatting.
- [ ] Root reviews diff and verification evidence before any separately authorized commit/publication. GitHub writes remain blocked; do not change credentials or claim assignment. Reconcile #284 separately after it publishes.

Self-review: each spec invariant maps to Tasks 1–3; all five review-focus conditions have explicit controls. Design-only phase ran no tests/builds/checks and performed no Git mutation. This plan does not grant implementation or publication approval.
