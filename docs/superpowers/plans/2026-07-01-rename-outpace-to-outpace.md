# Rename Outpace To Outpace Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Hard-rename the daemon/product identity from `outpace` to `outpace`.

**Architecture:** Keep Rust crate names and protocol module names unchanged. Rename only product-facing runtime surfaces, docs, commands, env vars, logs, config defaults, and metadata labels. Do not add compatibility aliases for the old `OUTPACE_*` env vars.

**Tech Stack:** Rust workspace, Cargo, axum daemon in `crates/ace-engine`.

---

## Current Repo Facts

- The daemon package is `ace-engine`; its binary is currently declared as `outpace` in `crates/ace-engine/Cargo.toml`.
- The daemon entry point reads `OUTPACE_*` env vars in `crates/ace-engine/src/main.rs`.
- The default persistent data directory currently ends in `outpace` in `crates/ace-engine/src/config.rs`.
- Broadcast metadata currently writes the key `outpace_name` in `crates/ace-engine/src/broadcast.rs`.
- Product-name references exist in code comments and docs, including files under `docs/protocol/notes`, `docs/superpowers/plans`, `docs/superpowers/specs`, and `README.md`.
- At the time this plan was written, the worktree already had unrelated uncommitted work. Future implementation must preserve it.

## Task 1: Rename Runtime Surfaces

**Files:**
- Modify: `crates/ace-engine/Cargo.toml`
- Modify: `crates/ace-engine/src/main.rs`
- Modify: `crates/ace-engine/src/config.rs`

- [ ] **Step 1: Add/update config test coverage**

  In `crates/ace-engine/src/config.rs`, update or add a test that asserts the default data dir uses `outpace`:

  ```rust
  #[test]
  fn default_config_uses_outpace_data_dir() {
      let c = Config::default();
      assert!(c.data_dir.ends_with("outpace"));
  }
  ```

- [ ] **Step 2: Run the targeted test and verify it fails**

  Run:

  ```bash
  cargo test -p ace-engine default_config_uses_outpace_data_dir
  ```

  Expected before implementation: the test fails because `Config::default().data_dir` still ends in `outpace`.

- [ ] **Step 3: Rename the binary**

  In `crates/ace-engine/Cargo.toml`, change:

  ```toml
  [[bin]]
  name = "outpace"
  path = "src/main.rs"
  ```

  to:

  ```toml
  [[bin]]
  name = "outpace"
  path = "src/main.rs"
  ```

- [ ] **Step 4: Rename env vars and logs**

  In `crates/ace-engine/src/main.rs`, replace the daemon module comment and product-facing comments/logs with `outpace`.

  Replace env var reads exactly:

  ```rust
  "OUTPACE_BIND" -> "OUTPACE_BIND"
  "OUTPACE_DATA_DIR" -> "OUTPACE_DATA_DIR"
  "OUTPACE_PEER_LISTEN" -> "OUTPACE_PEER_LISTEN"
  "OUTPACE_SEED_STORE_BYTES" -> "OUTPACE_SEED_STORE_BYTES"
  "OUTPACE_MAX_UNCHOKED" -> "OUTPACE_MAX_UNCHOKED"
  "OUTPACE_MAX_INBOUND" -> "OUTPACE_MAX_INBOUND"
  "OUTPACE_ENABLE_SEEDING" -> "OUTPACE_ENABLE_SEEDING"
  "OUTPACE_ENABLE_INBOUND" -> "OUTPACE_ENABLE_INBOUND"
  "OUTPACE_ACE_PEERS" -> "OUTPACE_ACE_PEERS"
  ```

  Replace log prefixes exactly:

  ```rust
  "outpace: node_id={} data_dir={}" -> "outpace: node_id={} data_dir={}"
  "outpace: inbound seeding ENABLED on {} (max {} peers) ..." -> "outpace: inbound seeding ENABLED on {} (max {} peers) ..."
  "outpace: listening on http://{} networks={:?}" -> "outpace: listening on http://{} networks={:?}"
  ```

  Do not keep fallback reads from `OUTPACE_*`; this is a hard rename.

- [ ] **Step 5: Rename default config storage**

  In `crates/ace-engine/src/config.rs`, change:

  ```rust
  .join("outpace")
  ```

  to:

  ```rust
  .join("outpace")
  ```

  Change the temp test directory prefix:

  ```rust
  format!("outpace-test-{}", std::process::id())
  ```

  to:

  ```rust
  format!("outpace-test-{}", std::process::id())
  ```

- [ ] **Step 6: Verify targeted tests pass**

  Run:

  ```bash
  cargo test -p ace-engine default_config_uses_outpace_data_dir
  cargo test -p ace-engine config
  ```

  Expected: both commands pass.

## Task 2: Rename Internal Product Metadata And Comments

**Files:**
- Modify: `crates/ace-engine/src/broadcast.rs`
- Modify: product-facing comments in `crates/ace-engine`, `crates/ace-swarm`, and `crates/ace-wire`

- [ ] **Step 1: Rename broadcast metadata key**

  In `crates/ace-engine/src/broadcast.rs`, change:

  ```rust
  d.insert(b"outpace_name".to_vec(), Bencode::Bytes(name.as_bytes().to_vec()));
  ```

  to:

  ```rust
  d.insert(b"outpace_name".to_vec(), Bencode::Bytes(name.as_bytes().to_vec()));
  ```

  Update the nearby comment to refer to `outpace`.

- [ ] **Step 2: Rename product-facing code comments**

  Run:

  ```bash
  rg -n "outpace|Outpace|OUTPACE" crates
  ```

  For matches in Rust comments or docs, replace the product name with `outpace`, `Outpace`, or `OUTPACE` as appropriate. Do not rename crate names such as `ace-engine`, `ace-wire`, `ace-swarm`, or protocol identifiers.

- [ ] **Step 3: Verify crate references**

  Run:

  ```bash
  rg -n "outpace|Outpace|OUTPACE" crates
  ```

  Expected: no remaining matches in `crates/`.

## Task 3: Rename Documentation Broadly

**Files:**
- Move: `docs/superpowers/plans/2026-06-29-outpace-daemon.md` to `docs/superpowers/plans/2026-06-29-outpace-daemon.md`
- Move: `docs/superpowers/specs/2026-06-29-outpace-daemon-design.md` to `docs/superpowers/specs/2026-06-29-outpace-daemon-design.md`
- Modify: docs containing product-name references

- [ ] **Step 1: Rename docs files**

  Run:

  ```bash
  git mv docs/superpowers/plans/2026-06-29-outpace-daemon.md docs/superpowers/plans/2026-06-29-outpace-daemon.md
  git mv docs/superpowers/specs/2026-06-29-outpace-daemon-design.md docs/superpowers/specs/2026-06-29-outpace-daemon-design.md
  ```

- [ ] **Step 2: Replace docs references**

  In docs, broadly replace:

  ```text
  outpace -> outpace
  Outpace -> Outpace
  OUTPACE -> OUTPACE
  --bin outpace -> --bin outpace
  outpace_name -> outpace_name
  2026-06-29-outpace-daemon.md -> 2026-06-29-outpace-daemon.md
  2026-06-29-outpace-daemon-design.md -> 2026-06-29-outpace-daemon-design.md
  ```

  Include historical notes, plans, specs, and `README.md`; the requested docs scope is "everything".

- [ ] **Step 3: Verify docs references**

  Run:

  ```bash
  rg -n "outpace|Outpace|OUTPACE" docs
  ```

  Expected: no remaining matches in `docs/`.

## Task 4: Final Verification

**Files:**
- All files changed by Tasks 1-3

- [ ] **Step 1: Verify no repo-wide old product references remain**

  Run:

  ```bash
  rg -n "outpace|Outpace|OUTPACE" .
  ```

  Expected: no remaining matches. If a match is found in an external binary fixture or intentionally immutable artifact, document the reason in the final report and do not edit that artifact.

- [ ] **Step 2: Verify the focused crate**

  Run:

  ```bash
  cargo test -p ace-engine
  ```

  Expected: all `ace-engine` tests pass.

- [ ] **Step 3: Verify the workspace**

  Run:

  ```bash
  cargo test --workspace
  ```

  Expected: all workspace tests pass.

- [ ] **Step 4: Verify the new binary starts**

  Run:

  ```bash
  OUTPACE_BIND=127.0.0.1:6900 cargo run -p ace-engine --bin outpace
  ```

  Expected: startup logs use the `outpace:` prefix and print the HTTP bind address. Stop the process after confirming startup.

- [ ] **Step 5: Verify the old binary name is gone**

  Run:

  ```bash
  cargo run -p ace-engine --bin outpace
  ```

  Expected: Cargo fails because there is no `outpace` binary target.

## Assumptions

- Keep Rust crate/package names such as `ace-engine`, `ace-wire`, `ace-swarm`, and `ace-media`; these are protocol/library names, not the product-facing daemon name.
- Do not rename the local checkout directory `/home/jamezrin/dev/outpace`; it is outside repo-tracked product behavior.
- This is a hard rename: old `OUTPACE_*` env vars should not be supported after implementation.
- Preserve unrelated uncommitted work. Inspect `git status --short` before editing and keep the final diff limited to rename-plan scope.
