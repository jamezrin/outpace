# Peer Receive Buffer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Remove temporary receive copies and per-message prefix movement while preserving peer read semantics.

**Architecture:** Keep one session-owned `BytesMut`, decode existing owned messages, advance consumed bytes, and read directly into bounded spare capacity. Message/store ownership stays unchanged.

**Tech Stack:** Rust, Tokio, existing workspace `bytes` major version 1.

**Spec:** `docs/superpowers/specs/2026-11-02-peer-read-buffer-288-design.md`

## Global Constraints

- Initial capacity 32 KiB; maximum underlying read 16 KiB; declared frame-body cap 2 MiB (plus the four-byte prefix).
- Preserve per-I/O timeout and cancellation ownership; no unsafe initialization shortcuts.
- No message/store API changes, public configuration changes or live network tests.
- No builds or benchmarks while the active sustained validation runs.

## Review Focus

- Partial input consumed before cancellation must survive resumption exactly once.
- Coalesced messages must return in order without losing the buffered tail.
- Spare capacity must not silently increase the per-read ceiling.
- Maximum and oversized frames must preserve current decoder behavior.
- EOF and timeouts must remain distinguishable, with buffered data retained.

### Task 1: Pin receive behavior and baseline

**Files:** `crates/ace-peer/src/session.rs` test sections, optionally a focused sibling receive-test module; bounded synthetic benchmark fixture under `crates/ace-peer`.

**Interfaces:** Existing `PeerSession::new`, `with_timeout`, `read_message` and `PeerMessage::encode/decode`; no production interface changes.

- [x] After sustained validation is terminal, add guards for each small-frame split, partial prefix/body cancellation and resumption, coalesced tails, command/read selection, timeout/resume, EOF, maximum and oversized declarations.
- [x] Use deterministic stream barriers and one explicit poll-to-Pending witness, not sleeps that can accidentally cancel before any input was consumed.
- [x] Run guards on the unchanged implementation. These are refactoring baselines, not claims of a functional defect.
- [x] Record fixed three-run warmed baseline timing, read-call counts, largest requested read and peak visible buffer capacity sampled before/after reserve/read/advance for coalesced, fragmented and piece inputs; distinguish it from allocator bytes/RSS. Keep fixture sizes bounded and commands/source hashes private if they contain incidental local state.

### Task 2: Replace the internal buffer

**Files:** `crates/ace-peer/Cargo.toml`, intended `Cargo.lock` dependency edge, `crates/ace-peer/src/session.rs`.

- [x] Add direct `bytes = "1"` dependency using the already-resolved compatible version; reject unrelated lockfile churn.
- [x] Change the persistent buffer to `BytesMut`; decode unchanged owned messages and use `Buf::advance` only after decode success.
- [x] Replace temporary array plus extend with cancel-safe `read_buf`, restricting each read to 16 KiB via a bounded buffer/reader adapter. Check actual spare with `capacity() - len()`; when below 16 KiB request `reserve(16 * 1024)` (additional beyond len), not just the spare deficit. Do not use growable `remaining_mut()` as spare capacity. Bound the per-read BufMut view to 16 KiB and document reserve compaction behavior.
- [x] Run Task 1 guards and existing peer/wire suites. Inspect exact cancellation/timeout states when a guard fails.
- [x] Run the same three benchmark trials and report measured results separately from source-level copy-removal reasoning. Investigate material regressions.

### Task 3: Review and integrate

- [x] Run affected all-target Clippy with warnings denied, formatting, identifier hygiene and diff checks. Broaden tests only for a justified dependency/interface effect.
- [x] Obtain independent correctness/performance/test-fidelity review; resolve concrete findings.
- [x] Update design/plan status to reflect actual evidence and remaining #288 items.
- [x] Commit with normal hooks, push the compliant branch, open/link the PR, handle review/CI, and merge when all required checks succeed.
- [x] Keep #288 open for store/message copying, startup scan and dependency follow-ups unless separately reconciled. No deployment or readiness claim from this change.
