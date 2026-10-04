# CI Test Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a CI workflow that runs `cargo fmt`, `cargo clippy` and `cargo test` on every pull request and every push to `main`, so that Renovate automerges and contributor PRs are gated on the test suite.

**Architecture:** A new `.github/workflows/ci.yml` has two jobs that run in parallel. `Rust lint` runs rustfmt and then Clippy. `Rust test` runs the offline workspace tests. Both jobs install the toolchain pinned in `rust-toolchain.toml` through `actions-rust-lang/setup-rust-toolchain@v2`, which is already used by `release.yml` and `armv7-portability.yml`. Both jobs use that action's bundled `Swatinem/rust-cache`. Renovate needs no config change, because its automerge already waits for every check run on the PR. Making the checks *required* is a GitHub setting, which is listed as a human follow-up at the end.

**Tech Stack:** GitHub Actions, Cargo, rustup, `act` 0.2.89 with `catthehacker/ubuntu:act-latest` for local runs, `rhysd/actionlint` via Docker, and PyYAML.

**Spec:** GitHub issue #163, "CI: gate PRs on cargo test, clippy and fmt before Renovate automerge" (`gh issue view 163`).

## Global Constraints

- **Workspace.**
  - Work only in the existing worktree `/home/jamezrin/dev/outpace/.worktrees/chore/ci-test-gate-163`, on branch `chore/ci-test-gate-163`, based on `main` @ `6f65e76`. Every command below runs there.
  - Do not create another worktree or branch, and do not touch `/home/jamezrin/dev/outpace`.
  - Do not push, open PRs or change GitHub settings.
- **New file.** The only new file is `.github/workflows/ci.yml`, with workflow `name: CI`.
- **Triggers.** Exactly `pull_request:` (no `paths`, `paths-ignore` or `branches` filter) and `push:` with `branches: [main]` (no path filter). There is no `workflow_dispatch` and no `merge_group`.
- **Permissions.** `permissions: contents: read` at workflow level.
- **Concurrency.**
  - `group: ${{ github.workflow }}-${{ github.event.pull_request.number || github.ref }}`
  - `cancel-in-progress: ${{ github.event_name == 'pull_request' }}`
- **Jobs.** Job ids are `lint` and `test`. Job `name`s are exactly `Rust lint` and `Rust test`. These names are the branch-protection check contexts; never rename them.
  - Both jobs use `runs-on: ubuntu-24.04` and `timeout-minutes: 20`.
  - They have no `needs`, `if`, `strategy` or `continue-on-error`.
- **Actions.** Actions are pinned by major tag, as in the existing workflows. Do not add a separate `Swatinem/rust-cache` step.
  - `actions/checkout@v7` with `persist-credentials: false`.
  - `actions-rust-lang/setup-rust-toolchain@v2` with exactly these two inputs: `build-warnings: ""` and `cache-save-if: ${{ github.ref == 'refs/heads/main' }}`.
    - Do not pass a `toolchain` input; the action then reads `rust-toolchain.toml`.
    - Leave `cache` at its default (`true`). That enables the bundled Swatinem/rust-cache v2.9.2, which the action itself pins.
- **Commands.** Use these exact strings:
  - `cargo fmt --all --check`
  - `cargo clippy --workspace --all-targets --locked -- -D warnings`
  - `cargo test --workspace --locked`
  - The 7 `#[ignore]` live-network tests stay ignored; do not pass `--ignored` or `--include-ignored`.
- **Out of bounds.** Do not modify:
  - `renovate.json`, `hygiene.yml`, `compose.yml`, `armv7-portability.yml` or `release.yml`;
  - `rust-toolchain.toml`, `Cargo.toml` or `Cargo.lock`;
  - `AGENTS.md`, `CLAUDE.md`, or any Rust source.
- **Local hygiene.**
  - Never run `cargo fmt --all` without `--check`; it rewrites unrelated files.
  - If any local cargo command changes `Cargo.lock`, restore it with `git checkout -- Cargo.lock` and report it.
  - Revert every deliberate probe edit with `git checkout -- <file>`.
- **AceStream identifier hygiene.** No content id, infohash, stream name or other 40-hex value may appear in any file or commit message. Run `python3 tools/hygiene/check_identifiers.py <files>` before each commit; it must exit 0.
- **act.** Invoke it with `-P ubuntu-24.04=catthehacker/ubuntu:act-latest`. The local `~/.config/act/actrc` does not map `ubuntu-24.04`.
- **Commit subjects:**
  - `ci: gate pull requests on fmt, clippy and tests (#163)`
  - `docs: describe the CI test gate (#163)`

## Design Notes (why, for the PR description)

- **No `renovate.json` change.**
  - Renovate's own automerge "only automerge[s] if every status check has succeeded" ([`ignoreTests`](https://docs.renovatebot.com/configuration-options/#ignoretests)).
  - With the default `automergeType: "pr"`, it merges "on a subsequent run once it detects the PR's status checks are 'green'" ([`automergeType`](https://docs.renovatebot.com/configuration-options/#automergetype)). Setting `"automergeType": "pr"` explicitly would be a no-op.
  - `platformAutomerge` "falls back to Renovate-based automerge if the platform-native automerge is not available" ([`platformAutomerge`](https://docs.renovatebot.com/configuration-options/#platformautomerge)). GitHub-native auto-merge "requires the 'Allow auto-merge' checkbox" ([automerge key concepts](https://docs.renovatebot.com/key-concepts/automerge/)), and this repo has `allow_auto_merge: false`.
  - The repo's history matches this. PR #157 was merged by `app/renovate` with `autoMergeRequest: null`, after its `acestream identifier hygiene` check runs succeeded. That is the Renovate-side fallback waiting for check runs.
  - So once `ci.yml` reports `Rust lint` and `Rust test` on Renovate PRs, Renovate's automerge waits for them with no config change.
- **No `prCreation: "not-pending"`.** It only delays PR *creation*, not merging. The Renovate docs also warn that it "can stall PR creation" when CI runs only on `pull_request` events ([`prCreation`](https://docs.renovatebot.com/configuration-options/#prcreation)), which is exactly this workflow's trigger. YAGNI.
- **No path filters.** GitHub's "Troubleshooting required status checks" doc says the checks of a workflow skipped by path or branch filtering "stay in a 'Pending' state and block merging", and advises "Avoid requiring workflows that can be skipped". `ci.yml` must therefore run on every PR, docs-only PRs included.
- **`hygiene.yml` already covers PRs.** It triggers on `push` (all branches) and `pull_request` with no filters. It needs no change and can be a required check as-is.
- **Cache.**
  - The action's built-in rust-cache keys on job id, `Cargo.lock` and rustc version, so `lint` (check artifacts) and `test` (debug build) keep separate caches.
  - Saving only from `main` keeps PR runs from filling the 10 GB repo cache. `release.yml` already pushes `type=gha` Docker layers into that same quota.
  - PRs restore main's cache.
- **Measured locally.** In `act`, on a 16-core host with a cold container and no cache, `Rust lint` took about 75 s and `Rust test` about 65 s (809 passed, 0 failed, 7 ignored). For comparison, the existing hosted `ARMv7 portability` cross-build takes about 2.5 minutes. The 10-minute target in #163 has wide margin; `timeout-minutes: 20` only guards against a hang.
- **Docs.** `README.md` already documents the local gate, so it gains the fmt command and one sentence naming `ci.yml`. The line in `docs/testing/interop-swarm.md` that says swarmtest "is never executed" in CI becomes inaccurate, because CI now runs swarmtest's offline unit tests through `cargo test --workspace`. That line is clarified.

## Review Focus

1. **The RTMP loopback test only runs where ffmpeg exists.**
   - `rtmp::tests::rtmp_publish_reaches_broadcast_piece_store` in `crates/ace-engine/src/rtmp.rs` self-skips when `ffmpeg` is missing. The `act` image has no ffmpeg, so `act` never exercises it.
   - It runs for real on hosts with ffmpeg, and possibly on the hosted runner, which makes it the most likely divergence between local and hosted CI.
   - Expected: it passes wherever ffmpeg is installed. Pinned in Task 1, Step 6.
2. **The toolchain comes from `rust-toolchain.toml`, not `stable`.**
   - The action's log step is titled "rustup toolchain install stable" even when it installs from the file.
   - Expected: the job prints `rustc <channel from rust-toolchain.toml>`, and `rustfmt` and `clippy` are present. Pinned in Task 1, Step 7.
3. **Skippable required checks.**
   - A `paths`, `paths-ignore` or `branches` filter on `pull_request`, or a job-level `if`, `needs` or `continue-on-error`, would leave a required context Pending or hollow.
   - Expected: both jobs report on every PR and fail when their command fails. Pinned by the Task 1 structural check (Steps 1 and 3) and the act failure probes (Step 8).
4. **Check-context name drift.**
   - Branch protection matches on the job `name`. A rename silently unrequires the gate.
   - Expected: the names stay exactly `Rust lint` and `Rust test`. Pinned by the Task 1 structural check and named in the README in Task 2.
5. **`Cargo.lock` drift under `--locked`.**
   - A lock file that cargo wants to rewrite makes every CI run fail.
   - Expected: `--locked` resolves cleanly on `main`, and local runs leave `Cargo.lock` untouched. Pinned in Task 1, Step 5.

---

### Task 1: Add the CI workflow

Working directory: `/home/jamezrin/dev/outpace/.worktrees/chore/ci-test-gate-163`.

**Files:**
- Create: `.github/workflows/ci.yml`
- Test: an inline Python structural check (Steps 1 and 3), actionlint, host cargo runs and `act` runs. No test file is committed.

**Interfaces:**
- Consumes:
  - `rust-toolchain.toml`, with `channel = "<x.y.z>"` and `components = ["clippy", "rustfmt"]`. Do not modify it.
  - `Cargo.lock`, which must satisfy `--locked`.
- Produces: a workflow named `CI` whose check runs are reported as exactly `Rust lint` (job id `lint`) and `Rust test` (job id `test`), on `pull_request` and on `push` to `main`. Task 2's docs and the Operator follow-up's ruleset name these two contexts.

- [ ] **Step 1: Run the structural check and confirm it fails**

Save this as `/tmp/check_ci.py` (outside the repo, so it is never committed), then run it from the worktree:

```python
import pathlib, sys, yaml

path = pathlib.Path(".github/workflows/ci.yml")
if not path.exists():
    sys.exit("missing .github/workflows/ci.yml")
doc = yaml.safe_load(path.read_text())
on = doc.get("on", doc.get(True))  # PyYAML reads a bare `on:` key as True
if doc.get("name") != "CI":
    sys.exit(f"workflow name is {doc.get('name')!r}")
if not isinstance(on, dict) or set(on) != {"pull_request", "push"}:
    sys.exit(f"triggers are {on!r}")
if on["pull_request"] not in (None, {}):
    sys.exit(f"pull_request must have no filters, got {on['pull_request']!r}")
if on["push"] != {"branches": ["main"]}:
    sys.exit(f"push trigger is {on['push']!r}")
if doc.get("permissions") != {"contents": "read"}:
    sys.exit(f"permissions are {doc.get('permissions')!r}")
concurrency = doc.get("concurrency") or {}
group = "${{ github.workflow }}-${{ github.event.pull_request.number || github.ref }}"
if concurrency.get("group") != group:
    sys.exit(f"concurrency group is {concurrency.get('group')!r}")
if concurrency.get("cancel-in-progress") != "${{ github.event_name == 'pull_request' }}":
    sys.exit(f"cancel-in-progress is {concurrency.get('cancel-in-progress')!r}")
expected = {
    "lint": ("Rust lint", [
        "cargo fmt --all --check",
        "cargo clippy --workspace --all-targets --locked -- -D warnings",
    ]),
    "test": ("Rust test", ["cargo test --workspace --locked"]),
}
jobs = doc.get("jobs") or {}
if set(jobs) != set(expected):
    sys.exit(f"jobs are {sorted(jobs)}")
rust_with = {
    "build-warnings": "",
    "cache-save-if": "${{ github.ref == 'refs/heads/main' }}",
}
for job_id, (name, commands) in expected.items():
    job = jobs[job_id]
    if job.get("name") != name:
        sys.exit(f"{job_id}: name is {job.get('name')!r}")
    if job.get("runs-on") != "ubuntu-24.04":
        sys.exit(f"{job_id}: runs-on is {job.get('runs-on')!r}")
    if job.get("timeout-minutes") != 20:
        sys.exit(f"{job_id}: timeout-minutes is {job.get('timeout-minutes')!r}")
    for key in ("needs", "if", "continue-on-error", "strategy"):
        if key in job:
            sys.exit(f"{job_id}: unexpected job key {key!r}")
    steps = job.get("steps") or []
    uses = [step["uses"] for step in steps if "uses" in step]
    if uses != ["actions/checkout@v7", "actions-rust-lang/setup-rust-toolchain@v2"]:
        sys.exit(f"{job_id}: actions are {uses}")
    if steps[0].get("with") != {"persist-credentials": False}:
        sys.exit(f"{job_id}: checkout inputs are {steps[0].get('with')!r}")
    if steps[1].get("with") != rust_with:
        sys.exit(f"{job_id}: Install Rust inputs are {steps[1].get('with')!r}")
    runs = [step["run"].strip() for step in steps if "run" in step]
    if runs != commands:
        sys.exit(f"{job_id}: commands are {runs}")
    for step in steps:
        if "if" in step or "continue-on-error" in step:
            sys.exit(f"{job_id}: step {step.get('name')!r} can be skipped or ignored")
print("ok")
```

Run: `python3 /tmp/check_ci.py`

Expected: exit 1, and stderr is `missing .github/workflows/ci.yml`.

- [ ] **Step 2: Create the workflow**

Write `.github/workflows/ci.yml` with exactly this content:

```yaml
name: CI

# Merge gate for main (#163): rustfmt, Clippy and the offline test suite.
# No path filters, on purpose: when a path filter skips a workflow, its required
# checks stay "Pending" forever and block the pull request.
on:
  pull_request:
  push:
    branches: [main]

permissions:
  contents: read

# A newer push to the same pull request cancels its stale run. Runs on main are
# never cancelled mid-flight.
concurrency:
  group: ${{ github.workflow }}-${{ github.event.pull_request.number || github.ref }}
  cancel-in-progress: ${{ github.event_name == 'pull_request' }}

env:
  CARGO_TERM_COLOR: always

jobs:
  # The job names are the check contexts that the main ruleset requires.
  # Renaming a job silently drops it from the gate, so rename the ruleset entry too.
  lint:
    name: Rust lint
    runs-on: ubuntu-24.04
    timeout-minutes: 20
    steps:
      - name: Checkout
        uses: actions/checkout@v7
        with:
          persist-credentials: false

      # Installs the toolchain pinned in rust-toolchain.toml, with clippy and rustfmt.
      # The bundled Swatinem/rust-cache restores on every run but saves only from
      # main, so pull requests reuse main's cache without filling the cache quota.
      - name: Install Rust
        uses: actions-rust-lang/setup-rust-toolchain@v2
        with:
          build-warnings: ""
          cache-save-if: ${{ github.ref == 'refs/heads/main' }}

      - name: Check formatting
        run: cargo fmt --all --check

      - name: Clippy
        run: cargo clippy --workspace --all-targets --locked -- -D warnings

  test:
    name: Rust test
    runs-on: ubuntu-24.04
    timeout-minutes: 20
    steps:
      - name: Checkout
        uses: actions/checkout@v7
        with:
          persist-credentials: false

      # Same toolchain and cache policy as the lint job.
      - name: Install Rust
        uses: actions-rust-lang/setup-rust-toolchain@v2
        with:
          build-warnings: ""
          cache-save-if: ${{ github.ref == 'refs/heads/main' }}

      # Live-network tests are #[ignore]d, so the default run stays offline.
      - name: Test
        run: cargo test --workspace --locked
```

- [ ] **Step 3: Run the structural check and actionlint, and confirm both pass**

```bash
python3 /tmp/check_ci.py
docker run --rm -v "$PWD":/repo -w /repo rhysd/actionlint:latest -color=false
act -l -W .github/workflows/ci.yml
```

Expected:
- The structural check prints `ok` and exits 0.
- actionlint prints nothing and exits 0. It lints every workflow in the repo, and the existing four are already clean.
- `act -l` lists two stage-0 jobs, `lint` / `Rust lint` and `test` / `Rust test`, each with events `pull_request,push`.

- [ ] **Step 4: Confirm the identifier gate accepts the file**

Run: `python3 tools/hygiene/check_identifiers.py .github/workflows/ci.yml`

Expected: exit 0.

- [ ] **Step 5: Run the exact CI commands on the host (Review Focus 5)**

```bash
cargo fmt --all --check; echo "fmt exit=$?"
cargo clippy --workspace --all-targets --locked -- -D warnings; echo "clippy exit=$?"
cargo test --workspace --locked > /tmp/ci-host-test.log 2>&1; echo "test exit=$?"
tail -5 /tmp/ci-host-test.log
grep -c 'FAILED' /tmp/ci-host-test.log || true
git diff --exit-code -- Cargo.lock; echo "lock diff exit=$?"
```

Expected:
- `fmt exit=0`, `clippy exit=0` and `test exit=0`.
- The log tail shows `test result: ok. … 0 failed` lines, and the `FAILED` count prints `0`. Do not assert a total test count; it drifts with every PR.
- `lock diff exit=0`. If not, run `git checkout -- Cargo.lock` and stop: CI's `--locked` would fail on `main`, and that must be reported, not papered over.

- [ ] **Step 6: Pin the ffmpeg-dependent RTMP test (Review Focus 1)**

Run this only if `command -v ffmpeg` succeeds on the host. If it does not, record "RTMP loopback test not exercised locally (no ffmpeg)" in the task report and continue.

```bash
cargo test -p ace-engine --locked --lib rtmp_publish_reaches_broadcast_piece_store -- --nocapture > /tmp/ci-rtmp.log 2>&1; echo "rtmp exit=$?"
tail -3 /tmp/ci-rtmp.log
grep -c 'skipping RTMP loopback smoke' /tmp/ci-rtmp.log || true
```

Expected:
- `rtmp exit=0`.
- The tail shows `test rtmp::tests::rtmp_publish_reaches_broadcast_piece_store ... ok` and `test result: ok. 1 passed`.
- The skip count prints `0`, which proves the test really ran rather than self-skipping.

- [ ] **Step 7: Run both jobs locally with act (Review Focus 2)**

Each run copies the working tree, minus gitignored paths, into a fresh container and installs rustup and the toolchain over the network. "No cache found" from the cache step is expected, and nothing is saved, because `act`'s ref is not `main`.

```bash
channel="$(sed -n 's/^channel = "\(.*\)"$/\1/p' rust-toolchain.toml)"
act pull_request -j lint -W .github/workflows/ci.yml -P ubuntu-24.04=catthehacker/ubuntu:act-latest > /tmp/act-lint.log 2>&1; echo "lint exit=$?"
act pull_request -j test -W .github/workflows/ci.yml -P ubuntu-24.04=catthehacker/ubuntu:act-latest > /tmp/act-test.log 2>&1; echo "test exit=$?"
grep -c "rustc ${channel} " /tmp/act-lint.log /tmp/act-test.log || true
grep -E 'Success - Main (Check formatting|Clippy|Test)|Job succeeded' /tmp/act-lint.log /tmp/act-test.log
grep -c 'FAILED' /tmp/act-test.log || true
```

Expected:
- `lint exit=0` and `test exit=0`.
- The `rustc ${channel} ` count is at least 1 for each log, so the toolchain came from `rust-toolchain.toml`. The action's step title still says "rustup toolchain install stable"; ignore the title.
- `Success - Main Check formatting`, `Success - Main Clippy` and `Success - Main Test` each appear, plus `Job succeeded` in both logs.
- The `FAILED` count is `0`.
- On a 16-core host, each run takes roughly 1 to 1.5 minutes.

If the `test` job fails in act for a container-only reason (for example IPv6 or socket permissions) while Step 5 passed on the host, the host result is authoritative. Record the act failure and its log lines in the task report, and do not change the workflow to work around act.

- [ ] **Step 8: Prove each gate fails CI (issue #163 acceptance, Review Focus 3)**

Each probe appends to a tracked file, runs one act job, then reverts. Run them one at a time, and always run the revert line, even if act errors.

**Probe A.** Misformatted code must fail `Rust lint` at the formatting step.

```bash
printf '\nfn   ci_fmt_probe( ) {}\n' >> crates/ace-log/src/lib.rs
act pull_request -j lint -W .github/workflows/ci.yml -P ubuntu-24.04=catthehacker/ubuntu:act-latest > /tmp/act-probe-fmt.log 2>&1; echo "fmt probe exit=$?"
git checkout -- crates/ace-log/src/lib.rs
grep -E 'Failure - Main Check formatting|Job failed' /tmp/act-probe-fmt.log
```

Expected: `fmt probe exit=1`, and the log contains `Failure - Main Check formatting` and `Job failed`.

**Probe B.** A Clippy warning in rustfmt-clean code must fail `Rust lint` at the Clippy step.

```bash
printf '\npub fn ci_clippy_probe() -> bool {\n    let v = vec![1];\n    v.len() == 0\n}\n' >> crates/ace-log/src/lib.rs
cargo fmt --all --check; echo "probe fmt exit=$?"
act pull_request -j lint -W .github/workflows/ci.yml -P ubuntu-24.04=catthehacker/ubuntu:act-latest > /tmp/act-probe-clippy.log 2>&1; echo "clippy probe exit=$?"
git checkout -- crates/ace-log/src/lib.rs
grep -E 'Success - Main Check formatting|Failure - Main Clippy|Job failed' /tmp/act-probe-clippy.log
```

Expected: `probe fmt exit=0`, so the probe is rustfmt-clean. Then `clippy probe exit=1`, and the log contains `Success - Main Check formatting`, `Failure - Main Clippy` and `Job failed`.

**Probe C.** A failing unit test must fail `Rust test`.

```bash
printf '\n#[cfg(test)]\nmod ci_test_probe {\n    #[test]\n    fn fails() {\n        panic!("ci test probe");\n    }\n}\n' >> crates/ace-log/src/lib.rs
act pull_request -j test -W .github/workflows/ci.yml -P ubuntu-24.04=catthehacker/ubuntu:act-latest > /tmp/act-probe-test.log 2>&1; echo "test probe exit=$?"
git checkout -- crates/ace-log/src/lib.rs
grep -E 'ci_test_probe::fails \.\.\. FAILED|Failure - Main Test|Job failed' /tmp/act-probe-test.log
```

Expected: `test probe exit=1`, and the log contains `test ci_test_probe::fails ... FAILED`, `Failure - Main Test` and `Job failed`.

**After all three probes:**

```bash
git status --short
```

Expected: only `?? .github/workflows/ci.yml`. `crates/ace-log/src/lib.rs` must not appear.

- [ ] **Step 9: Commit**

```bash
git status --short
python3 tools/hygiene/check_identifiers.py .github/workflows/ci.yml && git add .github/workflows/ci.yml && git commit -m "ci: gate pull requests on fmt, clippy and tests (#163)"; echo "commit chain exit=$?"
git show --stat --oneline HEAD
```

Expected:
- `git status --short` prints only `?? .github/workflows/ci.yml`.
- `commit chain exit=0`. The commit only happens if the hygiene gate passes.
- The commit contains exactly one file, `.github/workflows/ci.yml`.

### Task 2: Document the CI gate

Working directory: `/home/jamezrin/dev/outpace/.worktrees/chore/ci-test-gate-163`.

**Files:**
- Modify: `README.md`, the "Run the normal local gate with:" block (currently around lines 26–31, just before `## Quick Start`).
- Modify: `docs/testing/interop-swarm.md`, the `> **NOT in CI.**` callout (currently lines 8–10).
- Test: an inline Python check (Steps 1 and 4).

**Interfaces:**
- Consumes: `.github/workflows/ci.yml` (Task 1), whose check runs are named `Rust lint` and `Rust test`. It runs `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings` and `cargo test --workspace --locked` on every pull request and every push to `main`.
- Produces: documentation only. Nothing later depends on it.

- [ ] **Step 1: Run the docs check and confirm it fails**

```bash
python3 - <<'PY'
import pathlib, sys
readme = pathlib.Path("README.md").read_text()
block = (
    "```bash\n"
    "cargo fmt --all --check\n"
    "cargo clippy --workspace --all-targets --locked -- -D warnings\n"
    "cargo test --workspace --locked\n"
    "```"
)
if block not in readme:
    sys.exit("README local gate block does not match CI")
for needle in ("`.github/workflows/ci.yml`", "`Rust lint`", "`Rust test`"):
    if needle not in readme:
        sys.exit(f"README does not mention {needle}")
interop = pathlib.Path("docs/testing/interop-swarm.md").read_text()
if "so `swarmtest` is never executed" in interop:
    sys.exit("interop-swarm.md still says swarmtest never runs in CI")
if "offline unit tests" not in interop:
    sys.exit("interop-swarm.md does not mention the offline unit tests")
print("ok")
PY
```

Expected: exit 1, and stderr is `README local gate block does not match CI`.

- [ ] **Step 2: Update the README local gate**

In `README.md`, replace exactly this text:

````markdown
Run the normal local gate with:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```
````

with:

````markdown
Run the normal local gate with:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

CI runs these same three commands on every pull request and on every push to `main`
(`.github/workflows/ci.yml`, reported as the `Rust lint` and `Rust test` checks). The
ignored live-network tests never run in CI.
````

Leave the rest of the README unchanged, including the swarmtest bullet under "Project Docs".

- [ ] **Step 3: Clarify the swarmtest CI callout**

In `docs/testing/interop-swarm.md`, replace exactly this text:

```markdown
> **NOT in CI.** This harness needs the proprietary AceStream engine binary and a rootful
> Docker daemon. Both are unavailable to CI runners, so `swarmtest` is never executed
> there. Run it by hand on a Linux box when you want engine<->outpace interop evidence.
```

with:

```markdown
> **NOT in CI.** This harness needs the proprietary AceStream engine binary and a rootful
> Docker daemon. Both are unavailable to CI runners, so the harness never runs there;
> CI only runs its offline unit tests as part of `cargo test --workspace`. Run it by
> hand on a Linux box when you want engine<->outpace interop evidence.
```

- [ ] **Step 4: Run the docs check and confirm it passes**

Run the Python command from Step 1 again.

Expected: prints `ok` and exits 0.

- [ ] **Step 5: Commit**

```bash
git status --short
python3 tools/hygiene/check_identifiers.py README.md docs/testing/interop-swarm.md && git add README.md docs/testing/interop-swarm.md && git commit -m "docs: describe the CI test gate (#163)"; echo "commit chain exit=$?"
git show --stat --oneline HEAD
```

Expected:
- `git status --short` lists only ` M README.md` and ` M docs/testing/interop-swarm.md` before staging.
- `commit chain exit=0`. The commit only happens if the hygiene gate passes.
- The commit contains exactly those two files.

---

## Operator follow-up (human applies)

Not a task. The implementer does not run any of this. A repository admin applies it after the CI PR is merged.

1. **Wait for the first `CI` run on `main`.** Required contexts must match the names GitHub actually reports.

   ```bash
   gh run list --repo jamezrin/outpace --workflow ci.yml --branch main --limit 1
   gh api repos/jamezrin/outpace/commits/main/check-runs \
     --jq '.check_runs[] | [.name, .app.slug, .app.id] | @tsv'
   ```

   Expected: the latest run succeeded. The check runs include these lines, where 15368 is the GitHub Actions app id observed on this repo's existing check runs:

   ```text
   Rust lint	github-actions	15368
   Rust test	github-actions	15368
   acestream identifier hygiene	github-actions	15368
   ```

2. **Create the `main` ruleset.** It requires the three checks, blocks force-pushes and blocks deletion. It has no bypass actors.

   ```bash
   gh api --method POST repos/jamezrin/outpace/rulesets --input - <<'JSON'
   {
     "name": "main merge gate",
     "target": "branch",
     "enforcement": "active",
     "bypass_actors": [],
     "conditions": {
       "ref_name": { "include": ["~DEFAULT_BRANCH"], "exclude": [] }
     },
     "rules": [
       { "type": "deletion" },
       { "type": "non_fast_forward" },
       {
         "type": "required_status_checks",
         "parameters": {
           "strict_required_status_checks_policy": false,
           "required_status_checks": [
             { "context": "Rust lint", "integration_id": 15368 },
             { "context": "Rust test", "integration_id": 15368 },
             { "context": "acestream identifier hygiene", "integration_id": 15368 }
           ]
         }
       }
     ]
   }
   JSON
   gh api repos/jamezrin/outpace/rules/branches/main --jq '.[].type'
   ```

   Expected: the last command prints `deletion`, `non_fast_forward` and `required_status_checks`.
   - `integration_id` pins each context to GitHub Actions, so no other app can satisfy it with a same-named status.
   - `strict_required_status_checks_policy: false` avoids forcing extra rebases. Renovate already merges only up-to-date branches.

3. **Deliberately not required: the path-filtered smoke checks.** These are `Smoke (linux/amd64)` (`compose.yml`) and `Cross-build ARMv7 release binary` (`armv7-portability.yml`). This deviates from the issue's fix direction, which asks for "the existing smoke checks".
   - Both workflows use `paths:` filters. GitHub's "Troubleshooting required status checks" doc says checks of workflows skipped by path filtering "stay in a 'Pending' state and block merging", and advises "Avoid requiring workflows that can be skipped". A docs-only or Actions-only PR would be blocked forever.
   - They still gate Renovate when they run, because Renovate's own automerge waits for *every* check on the PR.
   - Requiring them later would mean dropping their path filters first, which runs a Docker build on every PR.

4. **Leave "Allow auto-merge" off. It is currently `false`.**
   - Renovate's `platformAutomerge` doc warns that without enforced required checks the platform "might merge Renovate PRs even if the repository's tests haven't started".
   - Even with the ruleset active, GitHub-native auto-merge waits only for the three *required* checks. The current Renovate-side fallback waits for *all* checks, smoke checks included.
   - If it is ever enabled for speed, do it only after step 2.

5. **Open PRs created before `ci.yml` existed** (for example the Renovate PRs #158–#162) have no `Rust lint`/`Rust test` runs. Once the ruleset is active, they stay blocked until their branch gets a new commit.
   - Renovate rebases automerge-enabled PRs when they fall behind `main`.
   - For the others, tick the PR's rebase checkbox, or rebase manually.

6. **Operator decisions:**
   - **Bypass list.** It is empty, so nobody can push directly to `main`. Every recent change landed through a PR, so none is proposed. If the owner wants a direct-push escape hatch, add "Repository admin" under Settings → Rules → Rulesets → *main merge gate* → Bypass list.
   - **Merge queue.** If a merge queue is enabled later, add a `merge_group:` trigger to `ci.yml`. The Renovate automerge docs require CI to run on the `gh-readonly-queue/*` branches.
