# Handshake Reply Diagnostics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Distinguish zero, partial and invalid ACE basic replies using bounded safe diagnostics.

**Architecture:** An additive peer observer counts bytes through the existing reader; caller-owned progress survives outer timeout cancellation. Swarm maps existing errors to finite causes and appends counters to existing attempt summaries.

**Tech Stack:** Rust, Tokio, existing ace-peer/ace-swarm diagnostics and alog.

**Spec:** `docs/superpowers/specs/2026-10-25-handshake-reply-diagnostics-design.md`

## Global Constraints

- Sol owns production and review; Luna owns all test edits/execution, formatting, lint, hygiene and hooks.
- Keep existing APIs, FnOnce compatibility, wire behavior, standard BT/VOD, deadlines and fairness unchanged.
- ACE reply count 0..=66; absent before Sent; no payloads/identifiers/endpoints/history or new dependencies.

## Review Focus

- A consumed prefix must survive cancellation even when read_exact returns Pending in the same poll.
- Blocked local write must not look like a zero-byte remote reply.
- Full malformed/hash-mismatched replies must not become valid merely from byte count.
- A later window timeout must retain validated basic-reply cause.
- Existing FnOnce closures and the separate standard-BT path must retain behavior and framing.

### Task 1: Add bounded progress observation and classify completed attempts

**Files:** Modify `crates/ace-peer/src/session.rs`, export through `crates/ace-peer/src/lib.rs` if needed; modify `crates/ace-swarm/src/live_session/{connect_diagnostics.rs,peer.rs}`. Tests belong beside these modules or in focused test companions; migrate explicit timing literals only as necessary.

**Interfaces:** Public observer/event and fixed log names are specified above. Internal progress/cause type names may follow local conventions. Existing result/error types and attempt stage categories stay unchanged.

- [x] Luna writes synthetic reader/observer and stats tests for the exact spec cases, then runs focused filters to record RED (missing-interface compile RED must be identified as such). Preserve existing controls and give Sol explicit test seams without inventing parallel production logic.
- [x] Sol implements the additive observer, private adapter, legacy wrapper, caller-owned byte snapshot, typed result classification and fixed summary counters. No tests/checks run by Sol; report exact interfaces and caller migrations.
- [x] Luna completes any real production integration assertions, runs focused cases including real outer-timeout cancellation, and then workspace tests, Clippy with warnings denied, fmt, identifier hygiene and whitespace checks. Fix test defects only; return product defects to Sol.
- [x] Fresh Sol reviews the whole focused diff for spec and quality; no findings. Luna commits the approved green tree through normal hooks.
- [ ] Root opens a PR, monitors CI and merges under the explicit user authorization. Reference #253 without closing it.
