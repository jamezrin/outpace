# Development

Contributor notes: the local gate, live-network testing, the Rust API surface and how releases are cut. Read [AGENTS.md](../AGENTS.md) first; it holds the repository rules, including identifier hygiene.

## Project docs

- [`superpowers/specs/2026-06-28-acestream-engine-reimplementation-design.md`](superpowers/specs/2026-06-28-acestream-engine-reimplementation-design.md): original architecture and scope.
- [`protocol/wire-protocol.md`](protocol/wire-protocol.md): consolidated peer-wire protocol notes.
- [`protocol/transport-file.md`](protocol/transport-file.md): transport container and infohash math.
- [`protocol/notes/`](protocol/notes/): chronological reverse-engineering and live validation notes.
- [`testing/interop-swarm.md`](testing/interop-swarm.md): the `swarmtest` engine/Outpace interop harness. It runs on demand and locally only, and needs Docker plus the proprietary engine, so it isn't part of CI.
- [`testing/fuzzing.md`](testing/fuzzing.md): parser fuzz targets. Committed seeds replay in `cargo test`; mutation campaigns run locally with `cargo-fuzz`.
- [`superpowers/plans/`](superpowers/plans/) and [`superpowers/specs/`](superpowers/specs/): implementation plans and design specs, kept as historical records.
- [`wiki/`](wiki/): the user wiki source, published to the GitHub wiki.

GitHub [milestones](https://github.com/jamezrin/outpace-stream/milestones) track release scope; [#212](https://github.com/jamezrin/outpace-stream/issues/212) records the permanent non-goals and compatibility scope.

## Status and the local gate

The live byte path is built and proven against real public swarms:

- protocol recovery and interop vectors are in place;
- the `ace-wire`, `ace-tracker`, `ace-peer`, `ace-swarm`, `ace-media`, and
  `ace-engine` crates are in the workspace;
- the daemon can discover peers, connect, download live media, and serve it over
  the native API;
- reciprocal upload, inbound seeding plumbing, signed broadcast origination,
  HTTP raw ingest, managed HTTP(S) MPEG-TS sources, RTMP ingest, content-id metadata serving, and the three-command
  CLI are implemented;
- live-network tests remain ignored by default because they need the public swarm
  and a suitable network.

The single production Rust live entrypoint is
`ace_swarm::live_session::follow_live(LiveSessionInputs)`. It emits authenticated
media as `LiveOutput { bytes, discontinuity }` and reconnects until the consumer
drops. Pre-1.0 Rust compatibility break (#193): `driver::download_from_peer`,
`driver::DownloadParams`, `live::LiveSession`, `live::LiveConfig`, and
`LiveSession::run` have been removed; Rust callers must migrate to `follow_live`.

Run the normal local gate with:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

CI runs these same three commands on every pull request and on every push to `main`
(`.github/workflows/ci.yml`, reported as the `Rust lint` and `Rust test` checks). The
ignored live-network tests never run in CI.

## Live-network notes

- Cloudflare WARP breaks P2P swarm testing by interfering with UDP/inbound paths.
  Keep it off for live swarm validation. The Spain network itself has worked once
  WARP is disabled.
- A known-good live content id for smoke tests is `cid1` in the gitignored
  `acestream-ids.txt` registry (see AGENTS.md); resolve it to the current
  infohash when needed because live infohashes rotate.
- `status=prebuf, peers=0` forever usually means a dead live channel. The old promo
  transport id `685e...6067` is dead and should not be used as proof.
- The official closed engine sandbox, when present, is started with:

```bash
docker compose -f re/sandbox/docker-compose.yml up -d acestream
```

It exposes the engine HTTP API on `127.0.0.1:6878` and is intended for controlled
capture/interop work, not for runtime use by Outpace.

## Cutting a release

The release version is the Cargo package version: the workspace crates inherit a
single `version` from `[workspace.package]` in the root `Cargo.toml`, and that is
the version `outpace --version` reports. The git tag is only a validated trigger.

First bump the shared version and commit the change, including the updated lockfile.
Merge that release-preparation commit into `main` before creating the matching tag
with a leading `v`:

```bash
# On a release-preparation branch, edit [workspace.package] in Cargo.toml,
# e.g. to 0.10.0, then refresh Cargo.lock and commit both files:
cargo check --workspace
git add Cargo.toml Cargo.lock
git commit -m "chore: prepare release 0.10.0"
# After the release-preparation PR is merged:
git switch main
git pull --ff-only origin main
git tag v0.10.0
git push origin v0.10.0
```

Pushing the tag runs `.github/workflows/release.yml`. It reads the version from
`cargo metadata` and fails the release before building anything if the tag does
not match `v<cargo_version>`, so artifact names and Docker tags always agree with
the Cargo version. The workflow:

- builds Linux binaries for amd64, arm64, and ARMv7, plus macOS and Windows binaries for amd64
  and arm64;
- uploads `tar.gz` archives for Unix targets and `zip` archives for Windows;
- generates one `SHA256SUMS` file for all archives;
- creates a GitHub Release with generated release notes;
- publishes `ghcr.io/jamezrin/outpace:<version>` and `ghcr.io/jamezrin/outpace:latest`.

Pushes to `main` run `.github/workflows/edge.yml`, which publishes
`ghcr.io/jamezrin/outpace:edge` and `ghcr.io/jamezrin/outpace:sha-<commit>` for
`linux/amd64` only. `edge` moves with `main`. Keep production pinned to a
release tag.
