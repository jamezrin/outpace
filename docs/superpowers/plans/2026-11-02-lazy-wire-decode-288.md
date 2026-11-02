# Lazy Wire Decode Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans task-by-task.

**Goal:** Remove discarded fallback allocations from typed decoding.

**Architecture:** Lazy Unknown construction; existing owned APIs unchanged.

**Tech Stack:** Rust, System allocator in an isolated test binary; no dependencies.

**Spec:** `docs/superpowers/specs/2026-11-02-lazy-wire-decode-288-design.md`

## Constraints and review focus

Preserve wrong-length known IDs as Unknown, empty payloads, incomplete and oversized frames, exact consumption with coalesced tails, and owned data. Counting must exclude fixture/assertion allocation and other test threads. Production remains safe Rust.

## Task 1: Decoder allocation contract

- [x] Add `crates/ace-wire/tests/decode_allocations.rs` with a thread-local counting System wrapper and synchronous scoped measurement.
- [x] Exercise fixed-size typed messages, nonempty variable payloads, wrong-length IDs, unknown IDs, empty payloads, incomplete/oversized/coalesced frames; assert exact output plus allocation budgets.
- [x] Run `cargo +1.99.1 test -p ace-wire --test decode_allocations --offline --locked` and retain genuine allocation RED.
- [x] Change only fallback evaluation in `crates/ace-wire/src/message.rs` to lazy construction.
- [x] Run allocation contracts and all wire suites; retain GREEN.

## Task 2: Review and integration

- [x] Run workspace all-target Clippy, formatting, identifier hygiene and diff checks.
- [x] Obtain independent review of semantics, allocator safety and test fidelity; correct concrete findings.
- [ ] Commit with normal hooks; push/open/link PR; merge after CI succeeds.
