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

GitHub [milestones](https://github.com/outpace-stream/outpace-engine/milestones) track release scope; [#212](https://github.com/outpace-stream/outpace-engine/issues/212) records the permanent non-goals and compatibility scope.

## Status and the local gate

The live byte path is built and proven against real public swarms:

- protocol recovery and interop vectors are in place;
- the `ace-wire`, `ace-tracker`, `ace-peer`, `ace-swarm`, `ace-media`, and
  `ace-engine` crates are in the workspace;
- the daemon can discover peers, connect, download live media, and serve it over
  the native API;
- reciprocal upload, inbound seeding plumbing, signed broadcast origination,
  HTTP raw ingest, managed HTTP(S) MPEG-TS sources, RTMP ingest, optional SRT, content-id
  metadata serving, and the CLI are implemented;
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
- `status=prebuf, peers=0` alone does not establish a dead source. Check current source
  availability, discovery progress, routing and buffering before attributing a failure.
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

Prepare a topic-branch PR from current `main`: bump the shared version and matching workspace
lockfile entries, add `docs/releases/<version>.md`, and update affected wiki pages/examples.
The release workflow includes that file as the curated release body alongside generated notes.
Run the local gate, wiki link checker and identifier hygiene, then review candidate CI and any
required acceptance evidence before merging. The [1.0.0 preparation checklist](superpowers/plans/2026-11-13-release-1.0.0-398.md#tomorrows-cut-checklist-maintainer-action)
records publication and post-publication steps for that release.

After the preparation PR is merged and publication is authorized, tag the exact reviewed commit
with an annotated tag (the following is an example for the planned 1.0.0 cut):

```bash
git switch main
git pull --ff-only origin main
# Verify HEAD is the accepted candidate and CI is green before tagging.
git tag -a v1.0.0 -m "Outpace 1.0.0"
git push origin v1.0.0
```

Pushing the tag runs `.github/workflows/release.yml`. Its metadata job rejects a tag that
does not match `v<cargo_version>`, so artifact names and image tags agree with Cargo. It:

- builds ten required archives using the `dist` profile: Linux musl x86_64, ARM64, ARMv7,
  ARMv6 hard-float and RISC-V 64; FreeBSD x86_64; macOS Intel/Apple silicon; Windows x86_64/ARM64;
- packages Unix `.tar.gz` and Windows `.zip` archives, plus six `.deb`/`.rpm` packages for
  Linux x86_64, ARM64 and ARMv7;
- creates `SHA256SUMS` for those required archives/packages and attests their build provenance;
- creates the GitHub Release with curated and generated notes;
- publishes standard and minimal images for linux/amd64, linux/arm64 and linux/arm/v7,
  with versioned and moving `latest`/`minimal` tags and provenance;
- attempts Android ARM64 afterward as best-effort, with a separate `.sha256` file. Its failure
  does not block the standard release.

Inspect the final workflow status and verify exact assets/checksums, image manifests and version
output. Packaging success does not prove playback acceptance. After publication, refresh
Homebrew/Scoop definitions in `outpace-stream/homebrew-outpace` and `outpace-stream/scoop-outpace`
using `tools/release/package_manifests.py` with the actual published `SHA256SUMS` and explicit
`--homebrew-root` / `--scoop-root` checkout directories (see the wiki release guide); switch the active `.env.example` pin and remove preparation caveats from the
README/wiki. Keep the previous release available for rollback; never move a published tag.

Pushes to `main` run `.github/workflows/edge.yml`, which publishes
`ghcr.io/outpace-stream/outpace-engine:edge` and `ghcr.io/outpace-stream/outpace-engine:sha-<commit>` for
`linux/amd64` only. `edge` moves with `main`. Keep production pinned to a
release tag.
