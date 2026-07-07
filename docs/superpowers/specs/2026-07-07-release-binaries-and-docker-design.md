# Release Binaries and Docker Image Design

## Context

Issue #23 asks for public release infrastructure now that `outpace` is public:
a top-level license, pinned Rust toolchain, tag-driven binary releases for six
desktop/server targets, a multi-arch GHCR image, and README installation docs.
The workspace builds one release binary, `outpace`, from the `ace-engine` crate.

## Decisions

- License the repository as `AGPL-3.0-or-later`, matching the goal that modified
  network-service deployments must keep their corresponding source available.
- Pin Rust to `1.96.0`, the current local toolchain used for development and
  verification in this workspace.
- Use `vX.Y.Z` git tags as the release trigger. The archive and Docker version
  strips the leading `v`, so tag `v0.1.0` publishes `outpace-0.1.0-...` archives
  and `ghcr.io/jamezrin/outpace:0.1.0`.
- Keep Cargo crate versions aligned with the release version before cutting a
  tag.

## Release Architecture

The release workflow has three jobs. The binary matrix builds Linux musl,
macOS, and Windows targets and uploads packaged artifacts to the workflow run.
A publish job downloads all packages, generates one aggregate `SHA256SUMS`, and
creates or updates the GitHub Release with generated notes. A Docker job builds
and pushes a `linux/amd64,linux/arm64` image to GHCR with version and `latest`
tags.

The workflow packages Unix targets as `tar.gz` and Windows targets as `zip`.
Each package includes the binary, `LICENSE`, and `README.md`.

## Docker Runtime

The Dockerfile uses a Rust builder stage and a Debian slim runtime. The runtime
binds HTTP and RTMP to `0.0.0.0`, persists state under `/var/lib/outpace`, exposes
HTTP, RTMP, and peer ports, and defaults to `outpace serve`.

## Verification

Local verification covers manifest parsing, Dockerfile/workflow syntax where
available, and a normal Cargo build/test gate. The real six-platform release
matrix and GHCR push are verified by pushing a `vX.Y.Z` tag.
