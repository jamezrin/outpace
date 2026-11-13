<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/hero-dark.svg">
    <img alt="Outpace: open-source peer-to-peer streaming engine, Ace Stream compatible" src="docs/assets/hero-light.svg" width="100%">
  </picture>
</p>

<h1 align="center">Outpace: the open-source P2P streaming engine</h1>

<p align="center">
  <a href="LICENSE"><img alt="License: AGPL-3.0-or-later" src="https://img.shields.io/badge/license-AGPL--3.0--or--later-blue"></a>
  <a href="https://github.com/jamezrin/outpace-stream/releases/latest"><img alt="Latest release" src="https://img.shields.io/github/v/release/jamezrin/outpace-stream?sort=semver"></a>
  <a href="https://github.com/jamezrin/outpace-stream/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/jamezrin/outpace-stream/actions/workflows/ci.yml/badge.svg?branch=main"></a>
  <a href="https://www.rust-lang.org"><img alt="Written in Rust" src="https://img.shields.io/badge/written%20in-Rust-orange?logo=rust"></a>
</p>

<p align="center">
  <b>A modern, open-source alternative to the Ace Stream engine, written from scratch in Rust.</b><br>
  Watch, broadcast and seed live streams on Ace Stream peer-to-peer networks with one headless
  binary, without any proprietary engine binaries.
</p>

<p align="center">
  <a href="https://github.com/jamezrin/outpace-stream/wiki"><b>Documentation</b></a> ·
  <a href="#quick-start">Quick start</a> ·
  <a href="https://github.com/jamezrin/outpace-stream/wiki/Installation">Installation</a> ·
  <a href="https://github.com/jamezrin/outpace-stream/wiki/FAQ">FAQ</a> ·
  <a href="docs/native-api.md">HTTP API</a> ·
  <a href="https://github.com/jamezrin/outpace-stream/releases">Releases</a>
</p>

## What is Outpace?

Outpace is an open-source streaming engine for peer-to-peer live video. It joins public,
unencrypted Ace Stream (AceStream) swarms, verifies the signed media, and serves it over plain
HTTP as MPEG-TS or HLS. Anything that can open a URL can then play it: VLC, mpv, Jellyfin,
Dispatcharr, or an app built for the official AceStream engine.

It runs as a self-hosted server (`outpace serve`) or as a one-shot command (`outpace play`), on
Linux, macOS, Windows or in Docker. Ace Stream is the peer-to-peer network it supports today;
the engine is built around pluggable provider networks, so it can grow beyond that protocol.

Outpace is an independent project. It isn't affiliated with or endorsed by Ace Stream, and it
uses Ace Stream names only to describe protocol compatibility.

## Why Outpace?

- **Fully open source.** AGPL-3.0-or-later, written from scratch in Rust, with the protocol work
  [documented in the repository](docs/protocol/wire-protocol.md).
- **No proprietary engine at runtime.** Outpace never downloads or runs the official Ace Stream
  engine or its libraries.
- **Fast and efficient by design.** Performance and resource efficiency are core goals, not
  afterthoughts: quick startup, smooth playback, and low CPU and memory use, so one small box
  can serve many viewers. Regressions are treated as bugs and must not be merged.
- **Headless by design.** A daemon and a CLI, configured with environment variables, with no
  desktop UI. That suits servers, containers, NAS boxes and single-board computers.
- **A real command line.** `outpace play` pipes a stream to standard output, and
  `outpace broadcast` publishes one.
- **Native HTTP API and HLS.** Stream URLs for TS and HLS, so Ace Stream content plays on
  HLS-only devices, plus per-stream status and optional AAC audio conversion.
- **Works with existing Ace Stream content.** Content ids, `acestream://` links and transport
  files play directly. Apps that use the engine's HTTP API can connect through a
  [documented compatible subset](docs/protocol/compat-matrix.md).
- **Broadcast and share.** Publish your own signed live stream from HTTP, RTMP or an HTTP pull
  source. By default Outpace also seeds what it plays back to other peers.
- **Cross-platform builds.** Prebuilt binaries for Linux x86_64, ARM64 and ARMv7, macOS Intel and
  Apple silicon, and Windows x64 and ARM64, plus a container image for amd64, arm64 and arm/v7.

What it isn't:

- It doesn't play premium or encrypted content.
- It has no accounts, EPG or web UI.
- Stream-name search is a separate, default-off external services adapter. No
  operator API key is needed. It is available from v1.0.0; see
  [search setup and limits](https://github.com/jamezrin/outpace-stream/wiki/Stream-Search).
- The compatible API is a subset, not full parity with the official engine.

See [differences from the official engine](https://github.com/jamezrin/outpace-stream/wiki/Migrating-from-AceStream-Engine#whats-different).

## Quick start

### 1. Install

Download the archive for your platform from the
[latest release](https://github.com/jamezrin/outpace-stream/releases/latest). For example, on
Linux x86_64:

```bash
curl -LO https://github.com/jamezrin/outpace-stream/releases/download/v1.0.0/outpace-1.0.0-x86_64-unknown-linux-musl.tar.gz
tar -xzf outpace-1.0.0-x86_64-unknown-linux-musl.tar.gz
sudo install -m 0755 outpace-1.0.0-x86_64-unknown-linux-musl/outpace /usr/local/bin/outpace
```

On Windows, unzip the archive and use `outpace.exe`. The
[installation guide](https://github.com/jamezrin/outpace-stream/wiki/Installation) covers
checksum verification, every platform and building from source.

### 2. Start the engine

```bash
outpace serve
# outpace: listening on http://127.0.0.1:6878 networks=["ace"]
```

### 3. Play a stream

Open the stream URL in VLC or any HLS-capable player. Replace the obviously fake id with the
content id of a public stream you're allowed to watch:

```bash
vlc "http://127.0.0.1:6878/streams/ace/0123456789abcdef0123456789abcdef01234567"       # MPEG-TS
mpv "http://127.0.0.1:6878/streams/ace/0123456789abcdef0123456789abcdef01234567.m3u8"  # HLS
```

Or play an `acestream://` link straight from the command line, with no server running:

```bash
outpace play acestream://0123456789abcdef0123456789abcdef01234567 > live.ts
outpace play acestream://0123456789abcdef0123456789abcdef01234567 | vlc -
```

A cold start takes 20–60 seconds while Outpace finds peers and buffers.

### Run it in Docker

```bash
docker run -d --name outpace -p 127.0.0.1:6878:6878 -p 8621:8621/tcp \
  -v outpace-data:/var/lib/outpace ghcr.io/jamezrin/outpace:1.0.0
```

For a production setup, use the repository's [`compose.yml`](compose.yml) and
[`.env.example`](.env.example); see [Deployment](https://github.com/jamezrin/outpace-stream/wiki/Deployment).

### Connect your apps

- **Jellyfin, Dispatcharr and other IPTV tools:** put the stream URL above in your M3U playlist.
  No handshake is needed.
- **Apps built for the Ace Stream engine:** start Outpace with `OUTPACE_ACE_COMPAT=1`. They can
  then use the supported `/ace/getstream?id=…` and `/server/api` routes.

The HTTP API has no authentication, so keep it on `127.0.0.1` or behind a proxy. Only TCP 8621
(the peer port) is meant to be public. See
[Playing streams](https://github.com/jamezrin/outpace-stream/wiki/Playing-Streams) and
[NAT and port mapping](https://github.com/jamezrin/outpace-stream/wiki/NAT-and-Port-Mapping).

## Features

| Capability | Behaviour |
| --- | --- |
| Live playback | Resolves content ids, finds peers through trackers and the DHT, verifies signed pieces and serves continuous MPEG-TS. Viewers of one stream share a single download. |
| HLS | Built-in live HLS packaging, so no FFmpeg is needed. Optional audio conversion to AAC for players that need it. |
| Ace Stream-compatible API | Opt-in subset (`OUTPACE_ACE_COMPAT=1`): `/ace/getstream`, `/ace/manifest.m3u8`, `/server/api`. See the [contract](docs/protocol/compat-matrix.md). |
| VOD | Single-file VOD with per-piece verification and byte-range seeking. |
| Peer participation | Uploads and accepts inbound peers by default, like the official engine. Download-only mode is one setting away. |
| Broadcasting | Publish your own signed swarm from HTTP, RTMP or an HTTP pull source, or from encrypted SRT in builds that include it. |
| Timeshift and HLS origins | Per-viewer rewind of live HLS, and a guarded relay for descriptors that point at HLS servers. Available from v1.0.0 ([details](https://github.com/jamezrin/outpace-stream/wiki/Releases-and-Versions#v100-highlights)). |

## Documentation

| You want to... | Read |
| --- | --- |
| Get answers to common questions | [FAQ](https://github.com/jamezrin/outpace-stream/wiki/FAQ) |
| Play a stream in five minutes | [Getting started](https://github.com/jamezrin/outpace-stream/wiki/Getting-Started) |
| Install on Linux, macOS, Windows or Docker | [Installation](https://github.com/jamezrin/outpace-stream/wiki/Installation) |
| Run it with Compose, systemd or behind a proxy | [Deployment](https://github.com/jamezrin/outpace-stream/wiki/Deployment) |
| Point VLC, mpv, Jellyfin, Dispatcharr or an Ace Stream app at it | [Playing streams](https://github.com/jamezrin/outpace-stream/wiki/Playing-Streams) |
| Look up a setting or command | [Configuration](https://github.com/jamezrin/outpace-stream/wiki/Configuration) · [Environment variables](https://github.com/jamezrin/outpace-stream/wiki/Environment-Variables) · [Command line](https://github.com/jamezrin/outpace-stream/wiki/Command-Line) |
| Replace an existing AceStream engine | [Migrating](https://github.com/jamezrin/outpace-stream/wiki/Migrating-from-AceStream-Engine) |
| Publish your own stream | [Broadcasting](https://github.com/jamezrin/outpace-stream/wiki/Broadcasting) |
| Fix a problem | [Troubleshooting](https://github.com/jamezrin/outpace-stream/wiki/Troubleshooting) |

The [user wiki](https://github.com/jamezrin/outpace-stream/wiki) is generated from
[`docs/wiki/`](docs/wiki); edit it there.

Technical reference:

- **Interfaces:**
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

## Contributing

Outpace is a community project, and you're welcome to take part, whether you're fixing a typo,
reporting a stream that won't play, testing it with your player or app, or digging into the
protocol. Every kind of contribution counts:

- **Bug reports and feature ideas.** Open an [issue](https://github.com/jamezrin/outpace-stream/issues).
  Include what you ran, what you expected and what happened.
- **Questions and discussion.** Ask in an issue for now; GitHub Discussions is planned
  ([#399](https://github.com/jamezrin/outpace-stream/issues/399)). Disagreement is healthy, so
  challenge a design or a decision when you think it's wrong, and explain why.
- **Docs and guides.** The wiki lives in [`docs/wiki/`](docs/wiki). If something confused you,
  improving the page helps the next person.
- **Code.** Look for [good first issues](https://github.com/jamezrin/outpace-stream/labels/good%20first%20issue),
  or open an issue first for larger changes, so we can agree on the approach before you invest
  the time.

The [contributing guide](CONTRIBUTING.md) covers bug reports, the local checks, conventions
and pull requests in detail. Be kind and assume good faith. Newcomers' questions are welcome, and nobody needs to be an
expert in Rust or peer-to-peer streaming to help.

To build and check your changes:

```bash
cargo build --release --locked -p ace-engine --bin outpace
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

The pinned toolchain is in `rust-toolchain.toml`. Tests are offline; live-swarm tests are
`#[ignore]`d. Read [development](docs/development.md) and [AGENTS.md](AGENTS.md) before you
open a PR, and keep PRs focused on one change. Never commit real content ids, infohashes or
stream names, including in issues and logs; the identifier-hygiene check enforces this for code.

## AI

Outpace has one maintainer and is built largely with AI coding agents, which follow the
repository rules in [AGENTS.md](AGENTS.md). Agents write most of the Rust and much of the
documentation. The maintainer sets the scope (public, unencrypted content only), owns the
design, and decides what ships.

A model's idea of how Ace Stream works is never treated as evidence. Protocol behaviour comes
from observing real traffic:

- it's written up in the [protocol notes](docs/protocol/notes/);
- wire formats are pinned by test vectors under `tests/vectors/`;
- parsers are covered by [fuzz targets](docs/testing/fuzzing.md);
- playback behaviour is checked against the original engine with the
  [A/B comparison tools](tools/abcompare/) and the [interop harness](docs/testing/interop-swarm.md).

Every change passes rustfmt, Clippy with warnings denied, the offline test suite, and an
identifier-hygiene check that keeps real content ids and infohashes out of the repository.

AI-assisted contributions are welcome and supported here: the same agent rules the maintainer
uses apply to yours. Two things are required:

- **Effort.** Generated code still has to be shaped, tested and trimmed. Run the tests, read the
  diff, and back protocol claims with evidence, such as a synthetic test vector or a
  reproducible run, not a model's summary.
- **A human who understands it.** Whoever opens the issue or pull request must understand what
  the change does and why, and be able to discuss it in review.

Unreviewed, fully automated submissions will be closed.

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
</p>

<a href="https://star-history.com/#jamezrin/outpace-stream&Date">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=jamezrin/outpace-stream&type=Date&theme=dark">
    <img alt="Star history" src="https://api.star-history.com/svg?repos=jamezrin/outpace-stream&type=Date" width="600">
  </picture>
</a>

## License

Outpace is licensed under [AGPL-3.0-or-later](LICENSE). It contains no closed-source engine code
or binaries. Ace Stream is a trademark of its respective owner; Outpace is an independent,
unaffiliated implementation.
