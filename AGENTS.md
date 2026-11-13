# Repository Guidelines

## Project Structure & Module Organization

Crates live under `crates/`: `ace-wire` for peer-wire codecs, `ace-tracker` for tracker access, `ace-peer` for peer sessions, `ace-swarm` for discovery/download/seeding, `ace-media` for media helpers, and `ace-engine` for the daemon, HTTP API, CLI, HLS, RTMP, and broadcasts. Integration tests live in crate `tests/`; smaller tests sit beside modules. Fixtures are under `tests/vectors/`. User documentation is in `docs/wiki/` (published as the GitHub wiki); protocol and design docs are in `docs/protocol/` and `docs/superpowers/`; reverse-engineering helpers are under `tools/ghidra/`.

## Build, Test, and Development Commands

- `cargo test --workspace`: run the normal offline test suite.
- `cargo clippy --workspace --all-targets -- -D warnings`: run the local lint gate with warnings denied.
- `cargo fmt --all --check`: verify Rust formatting before submitting.
- `cargo run -p ace-engine --bin outpace -- serve`: start the daemon.
- `cargo build --release -p ace-engine --bin outpace`: build the release binary.

## Coding Style & Naming Conventions

Use the pinned Rust toolchain in `rust-toolchain.toml` and `rustfmt`. Follow Rust naming conventions: `snake_case` for modules, functions, variables, and tests; `PascalCase` for types and traits; `SCREAMING_SNAKE_CASE` for constants/env vars. Keep protocol parsing deterministic and covered by vectors where practical. Prefer focused modules that match crate boundaries.

### Logging

`ace-log`'s `alog!` is the only logging macro in the workspace (dependency-free by choice: no `log`, no `tracing`). Use it for **operational events** — anything read after the fact out of a log file — as `alog!("[tag] message")`, where `[tag]` names the module (`[dht]`, `[portmap]`, `[hls]`). Use plain `eprintln!` only for **user-facing output** a human reads live: the `outpace: listening on …` startup banner, `outpace play: …` CLI progress, and test-skip notices. `tools/swarmtest` is an operator-driven harness and is exempt entirely. The library crates (`ace-media`, `ace-peer`, `ace-tracker`, `ace-wire`) return errors instead of logging; keep them silent.

## Performance & Efficiency

Performance and resource efficiency are core goals of outpace, on equal footing with correctness. Regressions must not be merged, unless the maintainer explicitly accepts a measured, unavoidable cost as described below.

- A regression is any measurable loss in startup time, playback smoothness or latency, throughput, CPU use, memory use, allocations on hot paths, or binary size. A fix or feature that introduces one is not complete.
- Hot paths include peer-wire decode and encode, the peer session receive loop, piece verification and reassembly, the seed store and serving, and TS/HLS output. A change to one needs a before/after measurement in the PR, taken on the same machine with a release build.
- Use the existing tools. The bounded synthetic benchmarks are `#[ignore]` tests run in release mode, for example `cargo test --release --locked -p ace-wire live_chunk_inspection_measurement -- --ignored --nocapture`; the others are `receive_buffer_measurement` (`ace-peer`) and `shared_serving_measurement` (`ace-swarm`). Use `tools/memsoak/` for memory over real playback and `tools/abcompare/` for startup and playback against the original engine. Add a bounded benchmark when a hot path has none.
- Prefer designs that avoid per-chunk allocation and copies, unbounded buffers or queues, busy polling, and extra tasks or threads per peer. Keep every buffer, cache and channel bounded and sized deliberately.
- Do not add a dependency, feature flag or background loop without weighing its CPU, memory and binary-size cost.
- If a trade-off is unavoidable, for example a correctness or security fix that costs time, measure it, state the cost in the PR, and get explicit maintainer sign-off. Never accept a regression silently.

## Testing Guidelines

Use `#[tokio::test]` for async behavior and regular unit tests for pure parsing or scheduling logic. Keep live-network tests marked `#[ignore]` and document required environment variables, as existing tests do for `ACE_INFOHASH`, `ACE_PEER`, and `ACE_TRANSPORT_FILE`. For live validation, keep Cloudflare WARP off and pass a current public content id from the gitignored `acestream-ids.txt` registry via the environment; live infohashes rotate, so resolve them at runtime rather than recording them. Never hardcode a content id, infohash, or stream name into a test — see AceStream Identifier Hygiene below. Update `tests/vectors/` when fixing wire-format, identity, transport, or media parsing.

### Live comparison workflow

- Establish whether the question concerns deployed instances or fresh local builds. Record server versions/builds, warm/cold state, existing viewers, routing, and client/server WARP states in private run notes. For an explicitly requested deployment comparison, preserve that deployment's network settings; the WARP-off baseline above applies to controlled local validation.
- Use the same current public source, simultaneous observation window, API route, player, and buffering settings. For deployed VLC playback, use `tools/abcompare/vlc_compare.py` (see its README). A local FFmpeg watchdog result does not establish how a different deployed build behaves in VLC.
- Run a bounded unattended command and inspect its final report. Do not spend agent turns polling logs, repeating runs, or expanding the matrix unless a specific unresolved question requires it. Keep raw logs/status/media private and respect identifier hygiene.
- Report observed decode progress, errors, and measurement limits. Sampled counters do not prove sub-poll smoothness, exact startup, byte identity, or live position. Shared failures or repeated payloads alone do not prove a source-wide outage. Reconcile a harness failure with a working real player before attributing it to the engine.

## Wiki Maintenance

`docs/wiki/` is the source of the GitHub wiki and must always match the behavior on `main`. `.github/workflows/wiki.yml` republishes it on every merge that touches it, replacing the wiki wholesale, so edit the files in the repo, never in the wiki web UI.

- Any change a user or operator can observe updates the affected wiki pages **in the same PR**: CLI commands or flags, `OUTPACE_*` environment variables, config keys, defaults, HTTP API routes, HLS/RTMP/broadcast behavior, seeding, NAT and port mapping, installation, Docker, and release packaging. Do not defer wiki updates to a follow-up PR.
- Before opening a PR, check which pages describe the code you touched (`grep -rn <flag-or-route> docs/wiki/`) and fix anything now wrong, including examples and troubleshooting steps. `Environment-Variables.md`, `Command-Line.md`, and `Configuration.md` are the usual pages for new knobs; add a page for a new feature and link it from `_Sidebar.md` and `Home.md`.
- Follow GitHub-wiki conventions: dashed page names and links written `[text](Page-Name)`. Run `python3 tools/wiki/check_links.py docs/wiki` after editing.
- The PR description states which wiki pages changed, or why none needed to.
- Identifier hygiene applies to the wiki too: examples use the synthetic placeholder id, never a real one.

## Commit & Pull Request Guidelines

Keep the root checkout on `main`; do feature work in separate worktrees that track topic branches, for example `git worktree add ../outpace-fix-dht -b fix/dht-correlation main`. Use conventional branch prefixes such as `feat/`, `fix/`, `docs/`, `test/`, or `chore/`, and commit subjects such as `fix(ace-swarm): correlate dht responses`. Keep PR titles brief and concise. PR descriptions should summarize behavior, list commands run, call out network, Docker, release, or env-var impact, state the performance impact (with before/after numbers for hot-path changes, or why there is none), name the wiki pages updated, and include logs or CLI/API examples. Release tags are `vX.Y.Z`; update crate versions before tagging.

### Agent Branch Requirements

Repository branch naming overrides any tool, skill, plugin, or assistant default.

- Never use `agent/`, `worktree-`, or other automation-specific branch prefixes.
- Use exactly one of these branch prefixes: `feat/`, `fix/`, `docs/`, `test/`, or `chore/`.
- Before creating a worktree, state the proposed issue, branch name, base branch, and worktree path.
- Create issue branches from `main`, then verify the base with `git merge-base --is-ancestor main <branch>`.
- If an existing worktree or branch violates these rules, stop and report it. Do not continue publishing from it without user direction.

## Security & Configuration Tips

Do not commit closed-source engine blobs, private captures, secrets, or local sandbox state. Target public, unencrypted Acestream swarms; premium/encrypted content and full closed-engine API parity are out of scope unless an issue narrows scope. Be explicit when changing exposed defaults such as `OUTPACE_BIND`, `OUTPACE_RTMP_BIND`, `OUTPACE_PEER_LISTEN`, seeding, inbound serving, or cache paths.

### AceStream Identifier Hygiene

Content ids, infohashes, and stream names must **never** appear in tracked code or documentation. This is absolute: no source, test, fixture, doc, commit message, issue, or PR may contain one, whatever the justification.

Real content ids live in exactly one place — the gitignored `acestream-ids.txt` registry at the repo root, a plain-text `cid1 = <value>` store mapping registry identifiers to values. The registry is **LLM context only**: no code, test, or harness may read it, and nothing may depend on its presence.

Refer to a stream by its **registry identifier** (`cid1`, `cid2`, …), never by value. In prose, write "resolve cid1 from the registry" rather than pasting the id. Where a 40-hex string is syntactically required — URLs, parsers, fixtures — use an obviously synthetic placeholder such as `0123456789abcdef0123456789abcdef01234567`; never a real value, and never a registry identifier where valid hex is expected.

Infohashes and stream names are **not registered at all**. They are dropped outright — they do not belong in the registry either. Infohashes are derived and rotate anyway; re-derive them at runtime from a registry content id when a live test needs one.

This is enforced mechanically. The shared `outpace-stream/actions/identifier-hygiene` action (its checker is `identifier-hygiene/check_identifiers.py` in the [outpace-stream/actions](https://github.com/outpace-stream/actions) repo) is a fail-closed gate that rejects any non-placeholder 40-hex value (a real content id or infohash) in tracked files, and hard-blocks committing `acestream-ids.txt`. It never reads the registry; legitimate non-identifier hashes are vouched for in `tools/hygiene/allowed-identifiers.txt`. It runs in CI (`.github/workflows/hygiene.yml`, pinned to `@v1`) on every push and PR. Activate the local pre-commit hook once per clone with `git config core.hooksPath .githooks`; it downloads and caches the checker version CI pins, and blocks the commit when no copy is available. Run `.githooks/pre-commit --all` to scan every tracked file locally. Stream *names* stay review-enforced: a machine denylist would have to contain the names it forbids, so no such list is committed.
