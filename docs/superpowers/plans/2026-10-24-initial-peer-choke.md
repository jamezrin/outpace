# Initial Peer Choke State Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve request eligibility across the initial live-window handoff.

**Architecture:** Retain latest connection-local choke state in the bounded window reader and connection result. After successful activation writes, initialize the scheduler incoming state while keeping reciprocal outgoing state independent, then use existing control-message handling.

**Tech Stack:** Rust, Tokio, existing peer-wire and follower modules.

**Spec:** `docs/superpowers/specs/2026-10-24-initial-peer-choke-design.md`

## Global Constraints

- GPT-6.1 Sol owns production edits/review; GPT-6 Luna owns all tests, checks, builds and hooks.
- Default choked; last pre-window Choke/Unchoke wins; no control replay queue.
- Preserve bounds, deadlines, fairness, verification and activation/cancellation policy.
- No dependencies, configuration/public API changes or real source identifiers.

## Review Focus

- No control message must not grant request permission.
- An intervening Choke must override an earlier Unchoke.
- A later Unchoke must override an earlier Choke.
- Incoming eligibility must not imply that we already sent a reciprocal Unchoke.
- Subsequent worker events and separate connections must retain independent state.

### Task 1: Preserve connection-local state through activation

**Files:** Modify `crates/ace-swarm/src/live_session/{support.rs,mod.rs,peer.rs,pool.rs}`. Test in `crates/ace-engine/src/ace_provider/discovery_tests.rs` and existing focused swarm test modules as needed. Migrate existing test-only connection/window fixtures if their internal shape changes.

**Interfaces:** `read_peer_window` retains its existing window/address result with an additional Boolean, initially false; `ConnectedUpstream` carries `unchoked: bool`. `connect_upstream_inner` transfers it. `activate_upstream_peer` uses existing `Continuity::set_peer_unchoked` after registration ; `PeerRuntime::unchoked_peer` tracks our independent outgoing permission and must remain false until our existing Interested handler sends Unchoke. Internal naming may follow local conventions without broadening the representation.

- [x] Write and run paired production follower controls: window-first requests a piece, early Unchoke should request equivalently. Existing RED: positive passes, early ordering fails its finite request bound.
- [ ] Luna extends the ordering fixture to assert request eligibility: `[] -> false`, `[Unchoke] -> true`, `[Unchoke, Choke] -> false`, `[Choke, Unchoke] -> true`; cover later normal control, reciprocal Interested/Unchoke, and independent-connection defaults using existing production paths. Run focused tests before product edits and preserve RED evidence.
- [ ] Sol implements only the state plumbing above; preserve all parsing/failure paths and activation ordering. Report self-review, exact files and caller migrations; run no tests.
- [ ] Luna runs the focused regressions and existing window/phase/activation controls, then workspace tests, Clippy with warnings denied, formatting and identifier hygiene. Fix only test harness issues; report production failures to Sol.
- [ ] Fresh Sol reviews the complete diff for specification and quality. Fix findings with scoped tests and fresh re-review; Luna commits the approved green tree through normal hooks. Root opens a PR, monitors CI and merges under existing authorization. Reference #253 without claiming its public-runtime root cause solved.
