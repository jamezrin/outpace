# Supported Ace Compatibility Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce supported, default-off `OUTPACE_ACE_COMPAT` configuration with a backward-compatible legacy fallback and an explicit disable switch.

**Architecture:** Resolve the canonical environment variable before the deprecated alias in the existing runtime parser. Keep the current Config/HTTP field and route gate; migrate deployment templates and active documentation without applying a deployment.

**Tech Stack:** Existing Rust runtime/configuration, Compose YAML, Markdown; no new dependencies.

**Spec:** [Approved migration design](../specs/2026-10-23-ace-compat-config-design.md).

## Global Constraints

- Compatibility stays off by default; canonical false overrides legacy true.
- Selected booleans accept exactly `1`, `true`, `0`, `false`; invalid selected values fail startup.
- Preserve the existing internal/config-file `experimental_ace_compat` field.
- Sol owns implementation/review; Luna writes and executes all tests and validation.
- No route/authentication/SSRF/bind/dependency changes, deployment application, live source, registry read, or WARP change.

## Review Focus

- Canonical false must remain effective even when the legacy alias is true.
- Invalid canonical values must fail rather than fall back to legacy true.
- A valid canonical value must ignore an invalid unused legacy value.
- Compose must preserve legacy-only configuration, explicit false, and invalid empty canonical values.
- Existing opted-in LXC configuration must stay opted in; default-off runtime routes must remain unavailable.

---

### Task 1: Migrate the compatibility configuration and operator examples

**Files:** Modify `crates/ace-engine/src/runtime.rs` and Config comments in `crates/ace-engine/src/config.rs`; update `compose.yml`, `tools/deploy/lxc/compose.yml`, `.env.example`, `README.md`, `docs/protocol/compat-matrix.md`, `docs/protocol/ace-client-inventory.md`, `docs/native-api.md`, and applicable deployment docs. Tests stay with existing runtime/router tests; use a small private Compose validation harness if needed.

**Interfaces:** Consumes existing `config_from_env() -> Result<Config, Box<dyn std::error::Error>>` and `bool_from_env(&str) -> Result<Option<bool>, Box<dyn std::error::Error>>`. Produces the same Config field and router behavior, selected by the new canonical variable or legacy fallback.

- [x] **Step 1 — Luna adds failing runtime regressions.** Under the existing environment lock/cleanup, assert default false; all four canonical boolean values; legacy-only true/false; canonical `0` plus legacy `1` yields false; canonical `1` plus legacy `0` yields true; canonical empty/invalid/non-Unicode fails where supported; invalid legacy fails only when selected; valid canonical plus invalid legacy succeeds. Add both variable names to shared cleanup lists.
- [x] **Step 2 — Luna runs focused runtime tests and records expected RED.** Run `cargo test -p ace-engine --lib runtime::tests`; failures must identify missing canonical precedence rather than unrelated environment leakage.
- [x] **Step 3 — Sol updates selection and descriptions.** Replace the single legacy environment lookup with canonical-first, lazy legacy fallback using the existing strict parser. Keep Config/HTTP field names and defaults unchanged. Document the supported subset and deprecated alias without a removal date.
- [x] **Step 4 — Sol migrates templates and active docs.** Generic Compose uses `OUTPACE_ACE_COMPAT: ${OUTPACE_ACE_COMPAT-${OUTPACE_EXPERIMENTAL_ACE_COMPAT-0}}`; LXC uses canonical `1`. Environment examples use canonical `0`. Explain precedence, restart/application requirements, migration, and the explicit disable switch. Keep historical plans/notes unchanged; fix nearby stale compatibility claims as specified.
- [x] **Step 5 — Luna verifies migration and routes.** Validate Compose output for unset/unset => `0`, legacy-only `1` => `1`, canonical `0` with legacy `1` => `0`, canonical empty => empty, and LXC canonical opt-in. Use `docker compose config` only if available; it must not start containers. Verify the selected Config reaches the existing router and false keeps compatibility routes at 404 while native health remains available. Reuse existing fixture/security tests rather than duplicating lifecycle suites.
- [x] **Step 6 — Luna runs focused and required gates.** Run runtime and compatibility tests, `cargo fmt --all --check`, `cargo test --workspace --locked`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, identifier hygiene and whitespace checks. Retain exit codes; validate changed documentation links/examples without live network.
- [ ] **Step 7 — Commit and independent review.** Luna executes the conventional commit with the normal hygiene hook; Sol supplies the implementation/evidence handoff. Root dispatches a fresh Sol reviewer, resolves findings, then opens the PR and merges only after CI. No administrator bypass or force push.
