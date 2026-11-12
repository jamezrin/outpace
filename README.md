<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/hero-dark.svg">
    <img alt="outpace: public AceStream swarms in, MPEG-TS and HLS out" src="docs/assets/hero-light.svg" width="100%">
  </picture>
</p>

<p align="center">
  <a href="https://github.com/jamezrin/outpace-stream/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/jamezrin/outpace-stream/actions/workflows/ci.yml/badge.svg?branch=main"></a>
  <a href="https://github.com/jamezrin/outpace-stream/releases/latest"><img alt="Latest release" src="https://img.shields.io/github/v/release/jamezrin/outpace-stream?sort=semver"></a>
  <a href="https://github.com/jamezrin/outpace-stream/releases"><img alt="Downloads" src="https://img.shields.io/github/downloads/jamezrin/outpace-stream/total"></a>
  <a href="https://github.com/jamezrin/outpace-stream/stargazers"><img alt="Stars" src="https://img.shields.io/github/stars/jamezrin/outpace-stream?style=flat"></a>
  <a href="LICENSE"><img alt="License: AGPL-3.0-or-later" src="https://img.shields.io/badge/license-AGPL--3.0--or--later-blue"></a>
</p>

**An open-source, AceStream-compatible engine written in Rust.** outpace joins public,
unencrypted AceStream swarms and serves the media to your player as MPEG-TS or HLS. It runs on
Linux, macOS and Windows, and in Docker. It doesn't need the closed-source engine.

[Wiki](https://github.com/jamezrin/outpace-stream/wiki)
· [Getting started](https://github.com/jamezrin/outpace-stream/wiki/Getting-Started)
· [Configuration](https://github.com/jamezrin/outpace-stream/wiki/Configuration)
· [Command line](https://github.com/jamezrin/outpace-stream/wiki/Command-Line)
· [HTTP API](docs/native-api.md)
· [Releases](https://github.com/jamezrin/outpace-stream/releases)
· [Troubleshooting](https://github.com/jamezrin/outpace-stream/wiki/Troubleshooting)

## Start here

| You want to... | Read |
| --- | --- |
| Play a stream in five minutes | [Getting started](https://github.com/jamezrin/outpace-stream/wiki/Getting-Started) |
| Install a binary (Linux x64/ARM64/ARMv7, macOS, Windows) or the container | [Installation](https://github.com/jamezrin/outpace-stream/wiki/Installation) |
| Run it with Compose, systemd or behind a proxy | [Deployment](https://github.com/jamezrin/outpace-stream/wiki/Deployment) |
| Point VLC, mpv, Jellyfin, Dispatcharr or an AceStream app at it | [Playing streams](https://github.com/jamezrin/outpace-stream/wiki/Playing-Streams) |
| Look up a setting or command | [Configuration](https://github.com/jamezrin/outpace-stream/wiki/Configuration) · [Environment variables](https://github.com/jamezrin/outpace-stream/wiki/Environment-Variables) · [Command line](https://github.com/jamezrin/outpace-stream/wiki/Command-Line) |
| Replace an existing AceStream engine | [Migrating](https://github.com/jamezrin/outpace-stream/wiki/Migrating-from-AceStream-Engine) |
| Publish your own stream | [Broadcasting](https://github.com/jamezrin/outpace-stream/wiki/Broadcasting) |
| Integrate over HTTP | [Native API](docs/native-api.md) · [AceStream-compatible subset](docs/protocol/compat-matrix.md) |
| Contribute | [Development](docs/development.md) · [AGENTS.md](AGENTS.md) |

## What it does

| Capability | Behaviour |
| --- | --- |
| Live playback | Resolves content ids, finds peers, verifies signed pieces and serves continuous MPEG-TS. Viewers of one stream share a single download. |
| HLS | Built-in live HLS packaging. Optional audio conversion to AAC for picky players. |
| AceStream-compatible API | Opt-in subset (`OUTPACE_ACE_COMPAT=1`): `/ace/getstream`, `/ace/manifest.m3u8`, `/server/api`. See the [contract](docs/protocol/compat-matrix.md). |
| VOD | Single-file VOD with per-piece verification and byte-range seeking. |
| Peer participation | Uploads and accepts inbound peers by default, like the original engine. Download-only is one setting away. |
| Broadcasting | Publish your own signed swarm from HTTP, RTMP, an HTTP pull source or encrypted SRT. |
| Timeshift and HLS origins | Per-viewer rewind of live HLS, and a guarded relay for descriptors that point at HLS servers. Newer than v0.9.0 ([details](https://github.com/jamezrin/outpace-stream/wiki/Releases-and-Versions#not-yet-released)). |

Premium or encrypted content, accounts, search, EPG and a web UI are out of scope.

## First run

```bash
docker run --rm -p 127.0.0.1:6878:6878 -p 8621:8621/tcp \
  -v outpace-data:/var/lib/outpace ghcr.io/jamezrin/outpace:0.9.0
# or, with a release binary on PATH:
outpace serve
```

```bash
curl http://127.0.0.1:6878/healthz
vlc "http://127.0.0.1:6878/streams/ace/0123456789abcdef0123456789abcdef01234567"
```

Replace the obviously fake id with a current public content id. Add `.m3u8` to the URL for HLS.
A cold start takes 20–60 seconds while outpace finds peers and buffers.

By default:

- the HTTP API listens on `127.0.0.1:6878`, and RTMP ingest on `127.0.0.1:1935`;
- the peer listener is TCP `0.0.0.0:8621`;
- seeding and inbound peers are on, and router port mapping is off.

The API has no authentication, so keep it local or put it behind a proxy. No UDP port needs
forwarding. See [NAT and port mapping](https://github.com/jamezrin/outpace-stream/wiki/NAT-and-Port-Mapping).

## Documentation

- **[User wiki](https://github.com/jamezrin/outpace-stream/wiki):** installation, configuration,
  playback, HLS, timeshift, broadcasting, peers, deployment and troubleshooting. It's generated
  from [`docs/wiki/`](docs/wiki); edit it there.
- **Reference:**
  - [native API](docs/native-api.md)
  - [Ace HTTP subset](docs/protocol/compat-matrix.md)
  - [configuration contract](docs/configuration.md)
  - [live audio](docs/live-audio.md)
  - [timeshift](docs/timeshift.md)
  - [SRT ingest](docs/srt-ingest.md)
  - [private peer mode](docs/private-peer-mode.md)
  - [support sources](docs/support-operation.md)
- **Operations:**
  - [Compose deployment](docs/deployment.md)
  - [LXC behind WARP](docs/deployment-lxc-warp.md)
  - [Linux portability](docs/linux-portability.md)
  - [engine migration](docs/engine-migration.md)
- **Protocol:** [wire protocol](docs/protocol/wire-protocol.md),
  [transport file](docs/protocol/transport-file.md) and the
  [reverse-engineering notes](docs/protocol/notes/).
- **Releases:** [GitHub Releases](https://github.com/jamezrin/outpace-stream/releases),
  [release notes](docs/releases/) and [milestones](https://github.com/jamezrin/outpace-stream/milestones).

## Build and contribute

```bash
cargo build --release --locked -p ace-engine --bin outpace
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

The pinned toolchain is in `rust-toolchain.toml`. Tests are offline; live-swarm tests are
`#[ignore]`d. Read [development](docs/development.md) and [AGENTS.md](AGENTS.md) before you
open a PR. Never commit real content ids, infohashes or stream names; the identifier-hygiene
check enforces this.

## Project activity

<p>
  <a href="https://github.com/jamezrin/outpace-stream/stargazers"><img alt="Stars" src="https://img.shields.io/github/stars/jamezrin/outpace-stream?style=flat"></a>
  <a href="https://github.com/jamezrin/outpace-stream/forks"><img alt="Forks" src="https://img.shields.io/github/forks/jamezrin/outpace-stream?style=flat"></a>
  <a href="https://github.com/jamezrin/outpace-stream/watchers"><img alt="Watchers" src="https://img.shields.io/github/watchers/jamezrin/outpace-stream?style=flat"></a>
  <a href="https://github.com/jamezrin/outpace-stream/graphs/contributors"><img alt="Contributors" src="https://img.shields.io/github/contributors/jamezrin/outpace-stream"></a>
  <a href="https://github.com/jamezrin/outpace-stream/issues"><img alt="Open issues" src="https://img.shields.io/github/issues/jamezrin/outpace-stream"></a>
  <a href="https://github.com/jamezrin/outpace-stream/pulls"><img alt="Open pull requests" src="https://img.shields.io/github/issues-pr/jamezrin/outpace-stream"></a>
  <a href="https://github.com/jamezrin/outpace-stream/commits/main"><img alt="Last commit" src="https://img.shields.io/github/last-commit/jamezrin/outpace-stream/main"></a>
  <a href="https://github.com/jamezrin/outpace-stream/pulse"><img alt="Commit activity" src="https://img.shields.io/github/commit-activity/m/jamezrin/outpace-stream"></a>
  <a href="https://github.com/jamezrin/outpace-stream/releases"><img alt="Release downloads" src="https://img.shields.io/github/downloads/jamezrin/outpace-stream/total"></a>
  <a href="https://github.com/jamezrin/outpace-stream"><img alt="Top language" src="https://img.shields.io/github/languages/top/jamezrin/outpace-stream"></a>
</p>

<a href="https://star-history.com/#jamezrin/outpace-stream&Date">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=jamezrin/outpace-stream&type=Date&theme=dark">
    <img alt="Star history" src="https://api.star-history.com/svg?repos=jamezrin/outpace-stream&type=Date" width="600">
  </picture>
</a>

## License

outpace is licensed under [AGPL-3.0-or-later](LICENSE). It contains no closed-source engine
code or binaries.
