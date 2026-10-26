# PES access boundaries implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox syntax for tracking.

**Goal:** Reject no-RAI false keyframes found in PES headers or trailing bytes.
**Architecture:** Share the existing packet-local PES validation as an elementary-slice boundary; retain codec-specific access scans.
**Tech Stack:** Rust, ace-media, existing test vectors; no dependencies.
**Spec:** docs/superpowers/specs/2026-10-25-pes-access-boundaries-design.md

## Global Constraints

Preserve RAI, selected PID/PUSI/codec policy, MPEG-2 opt-in/first-picture behavior, gate/reset ordering, buffer and network defaults. Packet-local only; malformed/split headers are conservative negatives. Silent deterministic library parsing. Synthetic tracked data only. No real identifiers or private captures. Root main and unrelated worktrees untouched.

## Review Focus

- Valid optional private data may contain arbitrary start-code patterns: never inspect it as elementary video.
- A declared short PES end must exclude TS payload trailing patterns.
- Incomplete optional syntax must not expose header bytes as video.
- Existing real IDR/IRAP and MPEG-2 I-picture plus RAI paths must remain usable.
- Corrections of legacy invalid test fixtures must preserve their behavioral assertions.

### Task 1: Bound no-RAI access scans to elementary bytes

**Files:** crates/ace-media/src/mpegts.rs; this plan and its spec.
**Interface:** private borrowed-slice PES helper; no public interface change.

- [x] Luna adds pes_access_ regressions for valid private-data fake IDR/IRAP, declared end, both public gate/state paths and valid controls; runs focused filter and establishes assertion RED.
- [x] Sol reads RED and implements minimal shared elementary-slice extraction from existing MPEG-2 envelope logic; codec scans consume slice. Does not edit test assertions.
- [x] Luna runs new and full media tests; migrates only malformed synthetic fixtures to valid PES, adds malformed/split/RAI controls, and establishes GREEN.
- [x] Luna runs cargo test --workspace, cargo clippy --workspace --all-targets -- -D warnings, cargo fmt --all --check, identifier hygiene including untracked docs and git diff --check; reports exact exits and limits.
- [x] Fresh independent reviewer reads spec, complete diff and retained test evidence; fix substantive findings with RED/GREEN and scoped rereview.
- [ ] Luna commits reviewed files via normal hook. Root opens/registers PR, monitors required CI, merges only reviewed exact head after required checks pass.

Runtime playback/soak deployment is a separate validation stage; this change alone cannot close #246.
