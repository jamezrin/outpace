# Renovate Design

## Problem

Dependency updates are manual. `renovate.json` only extends `config:recommended`, which is enough
for the Renovate GitHub App to open pull requests, and it does not describe how this repo wants
those pull requests grouped or merged.

The Rust compiler version is also copied in three places that already disagree.
`rust-toolchain.toml` pins `1.96.1`. The Dockerfile `RUST_VERSION` argument and both
`dtolnay/rust-toolchain` workflow pins use `1.96.0`. The workflow pin is the compiler that receives
cross-compile targets. `dtolnay/rust-toolchain` treats its `@` ref as that compiler, so a second
copy of the version is required for those targets to land on the compiler Cargo will use.

## Decisions

- `rust-toolchain.toml` is the only Rust version pin. CI and Docker read it.
- Renovate updates that channel from `rust-lang/rust` GitHub releases. It updates Cargo,
  GitHub Actions, and the Debian base image through the recommended preset.
- Non-major updates automerge after CI. Major updates stay open for review.
- Non-major Cargo updates share one pull request, and that pull request refreshes `Cargo.lock`.
- This change does not install the Renovate GitHub App and does not add a workflow that runs
  Renovate.

## Rust toolchain

`rust-toolchain.toml` stays at the repository root. Its `channel` remains an exact `x.y.z`
release. Its `clippy` and `rustfmt` components stay in that file.

Release and ARMv7 workflows replace `dtolnay/rust-toolchain@1.96.0` with
`actions-rust-lang/setup-rust-toolchain@v2`. They pass no `toolchain` input, so the action installs
the root toolchain file, including its components. Cross jobs pass the existing target on the
`target` input (`matrix.target` in the release workflow, `env.TARGET` in the ARMv7 workflow).
`cache` is `false` and `build-warnings` is an empty string, so the action does not add a Rust
cache or turn warning-free release builds into a new gate. Checkout stays before this step.

The action installs stable when the root toolchain file is absent. The file stays at the
repository root so CI cannot fall through to that default.

## Docker

The builder stage stops using `rust:${RUST_VERSION}-slim-bookworm` and the `RUST_VERSION`
argument. It uses `debian:bookworm-slim`, the same base as the runtime stage.

The builder installs `build-essential`, `ca-certificates`, `curl`, and `pkg-config`, then installs
rustup from `https://sh.rustup.rs` with `--default-toolchain none`, `--profile minimal`, and
`--no-modify-path`. `RUSTUP_HOME` is `/usr/local/rustup`, `CARGO_HOME` is `/usr/local/cargo`, and
`/usr/local/cargo/bin` is on `PATH`. It copies `rust-toolchain.toml` and runs `rustup show`, which
installs the channel and components from that file. The existing copy of the workspace manifests and the locked
`cargo build --release -p ace-engine --bin outpace` stay after that step. The runtime stage is
unchanged, and the Rust toolchain is not copied into it.

`docker build` takes no Rust version argument. An invalid or missing channel fails `rustup show`
and fails the image build.

## Renovate

`renovate.json` keeps `$schema` and `config:recommended`. It adds one regex custom manager and
package rules.

The custom manager matches `/(^|/)rust-toolchain\.toml$/`. Its `matchStrings` entry is
`channel\s*=\s*"(?<currentValue>\d+\.\d+\.\d+)"`. In `renovate.json` those backslashes are escaped
again. The dependency name is `rust`, the package name is `rust-lang/rust`, the datasource is
`github-releases`, and the versioning is `semver`. `extractVersion` is
`^(?<version>\d+\.\d+\.\d+)$`, so beta and nightly tags are ignored. A new
Rust 1.y or 1.y.z release replaces the channel string. A 2.0.0 release is a major update.

Package rules:

- `minor`, `patch`, `pin`, and `digest` updates set `automerge` to true.
- Cargo `minor`, `patch`, and `pin` updates use group name `cargo dependencies` and slug `cargo`.
- `major` updates set `automerge` to false.

`platformAutomerge` is true, so GitHub merges an automerge pull request after required checks pass.
Rust 1.97 is minor and automerges once CI is green. Rust 2.0 stays a separate pull request. Cargo
requirements stay range-style (`"1"`, `"0.12"`); Renovate updates `Cargo.lock` inside the grouped
pull request. There is no separate lock-file maintenance schedule and no digest pinning. The
recommended preset still applies its pull-request rate limits and dependency dashboard.

`debian:bookworm-slim` and GitHub Actions, including `actions-rust-lang/setup-rust-toolchain`,
follow that preset. A move from `@v2` to `@v3`, or from `actions/checkout@v4` to a newer major, is
a major update and stays open for review.

## Operator setup

The repo needs the Renovate GitHub App installed and GitHub "Allow auto-merge" enabled. With
required reviews, a non-major pull request still waits for approval, then GitHub completes the
merge. Neither setting lives in the repo.

## Validation

- Validate `renovate.json` with Renovate's config validator.
- Parse `.github/workflows/release.yml` and `.github/workflows/armv7-portability.yml` as YAML.
- Confirm neither workflow contains `dtolnay/rust-toolchain`, and the Dockerfile contains neither
  `RUST_VERSION` nor `FROM rust:`.
- Confirm `rust-toolchain.toml` still sets `channel` to `1.96.1`.

Historical design docs that mention the old `1.96.0` pin stay as written. `docs/linux-portability.md`
already refers to the pinned toolchain without copying the version.

## Out of scope

- Installing or configuring the Renovate GitHub App.
- A GitHub Actions workflow that runs Renovate itself.
- Rewriting historical specs or plans.
- Changing release artifacts, supported targets, or runtime image defaults.
