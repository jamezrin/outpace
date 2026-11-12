# Contributing to Outpace

Thanks for helping. Outpace is a community project, and bug reports, testing, docs, code and
protocol research all count. This guide covers how to contribute so your work lands smoothly.
The [README](README.md#contributing) has the short version.

Be kind and assume good faith. Disagreement about designs is welcome when it comes with
reasons. Newcomers' questions are welcome too.

## Scope

Outpace is an independent, open-source P2P streaming engine. It targets **public, unencrypted**
Ace Stream swarms.

**Not in scope**, unless an issue explicitly narrows it:
- premium or encrypted content;
- accounts, a web UI, search or EPG;
- full parity with the closed engine's API.

Contributions in those areas won't be accepted.

## Reporting a bug

Open an [issue](https://github.com/jamezrin/outpace-stream/issues) with:

- `outpace --version`, and whether you run a release, the `edge` image (with its `sha-` tag) or
  your own build;
- the platform (OS, architecture, container or bare metal) and any non-default `OUTPACE_*`
  settings;
- what you did, what you expected and what happened: the HTTP status, the player's error, and
  the stream's `/status`;
- a log excerpt around the problem.

**Never include stream identifiers**, in issues, logs, screenshots or PRs:
- Replace content ids and infohashes (40-character hex strings) with the placeholder
  `0123456789abcdef0123456789abcdef01234567`.
- Remove stream and channel names, source URLs and tokens, and IP addresses.

The wiki's [bug report checklist](https://github.com/jamezrin/outpace-stream/wiki/Troubleshooting#reporting-a-bug)
includes a one-line command to mask ids in a log.

For feature ideas, describe the problem you're trying to solve before the solution.

## Security issues

Please don't post exploit details publicly. Open a minimal issue that says you have a security
report, without details, and the maintainer will arrange a private channel.

## Setting up

You need the Rust toolchain pinned in `rust-toolchain.toml` (rustup installs it automatically)
and a C toolchain.

```bash
git clone https://github.com/jamezrin/outpace-stream.git outpace
cd outpace
git config core.hooksPath .githooks   # enable the identifier-hygiene pre-commit hook
cargo build --locked -p ace-engine --bin outpace
```

[Development notes](docs/development.md) cover the project docs, live-network testing and how
releases are cut. [AGENTS.md](AGENTS.md) holds the full repository rules. Read it if you use an
AI coding agent, since it's written for both people and agents.

## Making a change

1. **Open or find an issue first** for anything larger than a small fix, so the approach is
   agreed before you invest time.
2. **Branch** from `main` using one of these prefixes: `feat/`, `fix/`, `docs/`, `test/` or
   `chore/`, for example `fix/hls-endlist-on-eof`.
3. **Commit** with [Conventional Commits](https://www.conventionalcommits.org/) subjects scoped to
   the crate or area, for example `fix(ace-swarm): correlate dht responses` or
   `docs(wiki): explain port mapping`.
4. **Keep the PR focused** on one change, and keep the branch up to date with `main` by merging
   it in.

### Run the local checks

CI runs these on every PR, along with a few extra jobs. The checks required to merge are lint
(formatting and Clippy), the tests, the A/B harness test and identifier hygiene.

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
python3 tools/hygiene/check_identifiers.py
```

If you changed `docs/wiki/`, also run `python3 tools/wiki/check_links.py docs/wiki`. A local
build can rewrite `Cargo.lock`. Revert that before committing unless you changed dependencies.

### Code conventions

- Use `rustfmt`, and the usual Rust naming: `snake_case`, `PascalCase` for types,
  `SCREAMING_SNAKE_CASE` for constants and env vars. Prefer focused modules that match crate
  boundaries.
- **Logging:**
  - Use `alog!("[tag] message")` for operational events, with a tag naming the module, such as
    `[dht]` or `[hls]`.
  - Use plain `eprintln!` only for output a person reads live, such as the startup banner and
    `outpace play` progress.
  - The library crates (`ace-wire`, `ace-tracker`, `ace-peer`, `ace-media`) return errors
    instead of logging.
- **Exposed defaults:** be explicit in the PR when you change bind addresses, the peer listener,
  seeding, inbound serving, port mapping or cache paths.

### Tests

- Use `#[tokio::test]` for async behaviour, and plain unit tests for parsing and scheduling
  logic.
- Keep protocol parsing deterministic. When you fix wire-format, identity, transport or media
  parsing, add or update vectors under `tests/vectors/`.
- **Live-network tests** must be `#[ignore]` and must take their inputs from environment
  variables, as the existing tests do. Never hardcode a content id, infohash or stream name in a
  test, a fixture or anywhere else.
- Fuzz targets and their seed corpora are described in [fuzzing](docs/testing/fuzzing.md).

### Performance

Performance and efficiency are core goals of Outpace, and regressions must not be merged unless
the maintainer explicitly accepts a measured, unavoidable cost (see the last point below). A
change that makes startup slower, playback less smooth, or CPU, memory or binary size worse is
not finished, even if it's otherwise correct.

- If you touch a hot path (peer-wire parsing, the peer session, piece verification and
  reassembly, seeding and serving, or TS/HLS output), include before/after numbers in the PR,
  measured on the same machine with a release build.
- The bounded synthetic benchmarks are `#[ignore]`d tests that run in release mode, for example:

  ```bash
  cargo test --release --locked -p ace-wire live_chunk_inspection_measurement -- --ignored --nocapture
  ```

  The others are `receive_buffer_measurement` in `ace-peer` and `shared_serving_measurement` in
  `ace-swarm`. For memory over real playback use [`tools/memsoak/`](tools/memsoak/), and for
  comparisons with the original engine use [`tools/abcompare/`](tools/abcompare/).
- Keep buffers, caches and channels bounded, avoid per-chunk allocations and copies, and weigh
  the cost of any new dependency or background task.
- If a cost is unavoidable, for example for a security fix, measure it and say so in the PR. The
  maintainer decides whether it's worth it.

### Identifier hygiene

Real content ids, infohashes and stream names must never appear in tracked files, commit
messages, issues or PRs. Where a 40-hex value is syntactically required, use an obviously
synthetic placeholder such as `0123456789abcdef0123456789abcdef01234567`.

`tools/hygiene/check_identifiers.py` enforces this in CI and in the pre-commit hook. If you have
a genuine non-identifier hash, such as a test-fixture digest, add it to
`tools/hygiene/allowed-identifiers.txt` with a comment explaining it.

## Documentation

- **User docs** live in [`docs/wiki/`](docs/wiki) and are published to the
  [GitHub wiki](https://github.com/jamezrin/outpace-stream/wiki) automatically on merge. Edit them
  there, not in the wiki web UI, which is overwritten.
  - Pages use GitHub-wiki conventions: dashed page names, and links written `[text](Page-Name)`.
  - Mark features that aren't in a release yet as **Unreleased**.
- **Technical references** live in `docs/` (for example the [native API](docs/native-api.md)
  and the [configuration contract](docs/configuration.md)). Update them in the same PR as the
  behaviour they describe.
- **Style:** write in plain, direct English, describe what a setting does rather than how it's
  implemented, and don't use em dashes. Prefer a colon, a comma or a new sentence.

## Opening a pull request

Keep the title short. In the description:

- summarise the behaviour change, and link the issue (`Closes #123`);
- list the commands you ran;
- call out any impact on the network, Docker, releases or environment variables;
- state the performance impact, with before/after numbers if you changed a hot path;
- include logs or CLI and API examples where they help, with identifiers removed.

CodeRabbit reviews every PR automatically. Treat its comments like any reviewer's: fix what's
right, explain what isn't, and resolve the threads. The maintainer reviews and merges.

## Using AI

AI-assisted contributions are welcome and supported, as described in the
[README](README.md#ai). The agent rules in [AGENTS.md](AGENTS.md) apply to your agent as much as
the maintainer's. They need effort and a human who understands them:

- run the checks;
- read the diff;
- be ready to discuss the change in review;
- back protocol claims with evidence (a synthetic test vector or a reproducible run), not a
  model's summary.

Unreviewed, fully automated submissions will be closed.

## License

Outpace is licensed under [AGPL-3.0-or-later](LICENSE). By contributing, you agree that your
contribution is licensed under the same terms. Don't submit closed-source engine code or
binaries, private captures, credentials or local sandbox state.
