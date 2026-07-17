# Repository Guidelines

## Project Structure & Module Organization

Crates live under `crates/`: `ace-wire` for peer-wire codecs, `ace-tracker` for tracker access, `ace-peer` for peer sessions, `ace-swarm` for discovery/download/seeding, `ace-media` for media helpers, and `ace-engine` for the daemon, HTTP API, CLI, HLS, RTMP, and broadcasts. Integration tests live in crate `tests/`; smaller tests sit beside modules. Fixtures are under `tests/vectors/`. Protocol and design docs are in `docs/protocol/` and `docs/superpowers/`; reverse-engineering helpers are under `tools/ghidra/`.

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

## Testing Guidelines

Use `#[tokio::test]` for async behavior and regular unit tests for pure parsing or scheduling logic. Keep live-network tests marked `#[ignore]` and document required environment variables, as existing tests do for `ACE_INFOHASH`, `ACE_PEER`, and `ACE_TRANSPORT_FILE`. For live validation, keep Cloudflare WARP off and pass a current public content id from the gitignored `acestream-ids.txt` registry via the environment; live infohashes rotate, so resolve them at runtime rather than recording them. Never hardcode a content id, infohash, or stream name into a test — see AceStream Identifier Hygiene below. Update `tests/vectors/` when fixing wire-format, identity, transport, or media parsing.

## Commit & Pull Request Guidelines

Keep the root checkout on `main`; do feature work in separate worktrees that track topic branches, for example `git worktree add ../outpace-fix-dht -b fix/dht-correlation main`. Use conventional branch prefixes such as `feat/`, `fix/`, `docs/`, `test/`, or `chore/`, and commit subjects such as `fix(ace-swarm): correlate dht responses`. Keep PR titles brief and concise. PR descriptions should summarize behavior, list commands run, call out network, Docker, release, or env-var impact, and include logs or CLI/API examples. Release tags are `vX.Y.Z`; update crate versions before tagging.

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

This is enforced mechanically. `tools/hygiene/check_identifiers.py` is a fail-closed gate that rejects any non-placeholder 40-hex value (a real content id or infohash) in tracked files, and hard-blocks committing `acestream-ids.txt`. It never reads the registry; legitimate non-identifier hashes are vouched for in `tools/hygiene/allowed-identifiers.txt`. It runs in CI (`.github/workflows/hygiene.yml`) on every push and PR. Activate the local pre-commit hook once per clone with `git config core.hooksPath .githooks`. Stream *names* stay review-enforced: a machine denylist would have to contain the names it forbids, so no such list is committed.
