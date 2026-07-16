# Fast Portability CI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep native and cross-compilation CI coverage automatic while moving slow QEMU runtime smokes behind explicit manual inputs.

**Architecture:** Separate native/cross-compiled build gates from foreign-architecture runtime gates. Ordinary pull requests run native Compose and ARMv7 cross-build checks; workflow dispatch inputs selectively enable QEMU runtime jobs.

**Tech Stack:** GitHub Actions, Docker Buildx, QEMU, Rust/Cargo, ARM musl cross toolchain.

## Global Constraints

- Preserve support for `linux/amd64`, `linux/arm64`, and `linux/arm/v7` release artifacts.
- Use QEMU only for explicitly requested foreign-architecture runtime checks.
- Keep the root checkout on `main`; implement on `fix/ci-emulation-smokes`.
- Do not change runtime defaults, release packaging, or application code.

---

### Task 1: Split native and emulated Compose coverage

**Files:**
- Modify: `.github/workflows/compose.yml`

**Interfaces:**
- Consumes: GitHub `workflow_dispatch` boolean input `run_arm64`.
- Produces: automatic native `linux/amd64` smoke and opt-in `linux/arm64` smoke.

- [x] **Step 1: Add the manual ARM64 input**

Define `workflow_dispatch.inputs.run_arm64` as a boolean with a `false` default.

- [x] **Step 2: Make the existing smoke native-only**

Make its platform matrix resolve to only `linux/amd64` for automatic events, and retain all Compose
model, health, permissions, architecture, SIGTERM, logging, and teardown assertions.

- [x] **Step 3: Add the opt-in ARM64 smoke**

Expand the platform matrix to include `linux/arm64` only when
`github.event_name == 'workflow_dispatch' && inputs.run_arm64`, and conditionally set up QEMU for
the non-native matrix entry. Reuse the same runtime assertions for both platforms.

- [x] **Step 4: Validate workflow syntax**

Run a YAML parser and `actionlint` when installed. Expected: both commands exit zero.

### Task 2: Replace automatic ARMv7 emulation with cross-compilation

**Files:**
- Modify: `.github/workflows/armv7-portability.yml`
- Modify: `docs/linux-portability.md`

**Interfaces:**
- Consumes: GitHub `workflow_dispatch` boolean input `run_container_smoke` and target
  `armv7-unknown-linux-musleabihf`.
- Produces: automatic cross-compiled release binary and opt-in QEMU container health smoke.

- [x] **Step 1: Add the manual ARMv7 runtime input**

Define `workflow_dispatch.inputs.run_container_smoke` as a boolean with a `false` default.

- [x] **Step 2: Convert the automatic test job to a cross-build job**

Keep the pinned Rust target and ARM musl toolchain installation, remove the QEMU runner and test
execution, and run:

```sh
cargo build --locked --release -p ace-engine --bin outpace --target "$TARGET"
```

- [x] **Step 3: Guard the ARMv7 container smoke**

Set its job condition to
`github.event_name == 'workflow_dispatch' && inputs.run_container_smoke` while preserving its image
build and health check.

- [x] **Step 4: Correct portability documentation**

Describe cross-compilation as the pull-request compatibility gate and the QEMU container startup
check as manually invoked runtime evidence.

- [x] **Step 5: Validate workflow syntax and repository checks**

Run YAML parsing, `actionlint` when available, `cargo fmt --all --check`, and
`cargo test --workspace`. Expected: all available checks exit zero.

### Task 3: Publish the change

**Files:**
- Include only the two workflows, portability documentation, design, and plan.

**Interfaces:**
- Produces: a draft pull request from `fix/ci-emulation-smokes` to `main`.

- [x] **Step 1: Review the final diff and branch ancestry**

Run `git diff --check`, inspect `git diff`, and verify
`git merge-base --is-ancestor main fix/ci-emulation-smokes`.

- [ ] **Step 2: Commit intentionally**

Commit the scoped files with `fix(ci): avoid routine qemu smoke builds`.

- [ ] **Step 3: Push and open a draft PR**

Push `fix/ci-emulation-smokes` and create a draft PR targeting `main`. The description must explain
the QEMU root cause, automatic versus manual coverage, commands run, and lack of runtime/default
impact.
