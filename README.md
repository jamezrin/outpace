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
- It has no accounts, search, EPG or web UI.
- The compatible API is a subset, not full parity with the official engine.

See [differences from the official engine](https://github.com/jamezrin/outpace-stream/wiki/Migrating-from-AceStream-Engine#whats-different).

## Quick start

### 1. Install

Download the archive for your platform from the
[latest release](https://github.com/jamezrin/outpace-stream/releases/latest). For example, on
Linux x86_64:

```bash
curl -LO https://github.com/jamezrin/outpace-stream/releases/download/v0.9.0/outpace-0.9.0-x86_64-unknown-linux-musl.tar.gz
tar -xzf outpace-0.9.0-x86_64-unknown-linux-musl.tar.gz
sudo install -m 0755 outpace-0.9.0-x86_64-unknown-linux-musl/outpace /usr/local/bin/outpace
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
  -v outpace-data:/var/lib/outpace ghcr.io/jamezrin/outpace:0.9.0
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
| Timeshift and HLS origins | Per-viewer rewind of live HLS, and a guarded relay for descriptors that point at HLS servers. Newer than v0.9.0 ([details](https://github.com/jamezrin/outpace-stream/wiki/Releases-and-Versions#not-yet-released)). |

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

## AI

Outpace is maintained by one person, using the best frontier models and workflows written for
this repository. The models write most of the code. The maintainer owns the architecture, the
design, and the product decisions. Every change still goes through tests, CI, and review before
it merges. An issue or a pull request has to come from a person who has read it, checked it,
and can discuss it. Low-effort and fully automated submissions are not welcome.

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
