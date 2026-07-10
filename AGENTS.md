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

## Testing Guidelines

Use `#[tokio::test]` for async behavior and regular unit tests for pure parsing or scheduling logic. Keep live-network tests marked `#[ignore]` and document required environment variables, as existing tests do for `ACE_INFOHASH`, `ACE_PEER`, and `ACE_TRANSPORT_FILE`. For live validation, keep Cloudflare WARP off and use a current public content id; live infohashes rotate. Update `tests/vectors/` when fixing wire-format, identity, transport, or media parsing.

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
