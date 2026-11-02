# Startup Reservoir Packet Scan Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Observe complete startup packets by borrowed slice while preserving reset offsets and release semantics.

**Architecture:** Complete the bounded partial packet, scan complete 188-byte slices, then retain only the tail. Share timing/reset handling without changing the queue or public source wrapper.

**Tech Stack:** Rust, existing ace-media timing parser and Bytes queues.

**Spec:** `docs/superpowers/specs/2026-11-02-startup-reservoir-288-design.md`

## Constraints and review focus

- No unsafe code, parser replacement, sync search, public API/default or dependency change.
- Preserve packet order, selected-clock semantics and last-reset boundary across chunks.
- At call boundaries retain fewer than 188 partial bytes.
- Keep queue/cap/deadline/bitrate/release paths unchanged.
- Differential guards must observe intermediate state for each split, including empty chunks and cross-boundary reset packets.

## Task 1: Baseline and partition guards

**Files:** test sections or focused test module under `crates/ace-engine/src/startup_buffer.rs`.

- [x] Retain the old byte-loop as a test-only differential oracle; no production legacy switch.
- [x] Add bounded synthetic sequence and every two-way split guards for offset, partial bytes, PCR and reset state.
- [x] Run guards and existing startup tests before modifying the production algorithm.
- [x] Add an explicit ignored local release benchmark: warmup plus three trials on fixed aligned and split chunks, with exact source/log provenance.

## Task 2: Bulk observation

**File:** `crates/ace-engine/src/startup_buffer.rs` production observation method only.

- [x] Fill existing partial bytes in one copy; observe its timing before clearing it.
- [x] Iterate over exact 188-byte slices without copying complete packets; retain the trailing slice.
- [x] Share timing/reset updates and compute combined reset starts with the original offset convention.
- [x] Run the unchanged guards and wrapper tests, compare the same benchmark inputs, and investigate any semantic or material performance regression.

## Task 3: Review and integration

- [x] Run affected tests, repository lint/format/hygiene/diff gates; avoid redundant unchanged suites.
- [x] Obtain independent correctness/performance/test-fidelity review and fix concrete findings.
- [x] Refresh base after the receive-buffer PR merges if necessary; preserve unrelated work and normal hooks.
- [ ] Commit, push/open/link PR, resolve CI/review and merge only when ready.
- [x] Keep remaining #288 items and unmet readiness criteria open; no automatic deployment or live run.
