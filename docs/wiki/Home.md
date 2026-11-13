<!--
  This wiki is generated from docs/wiki/ in the main repository
  (https://github.com/jamezrin/outpace-stream/tree/main/docs/wiki).
  Edit the files there through a pull request. Every merge to main that
  touches docs/wiki/ republishes the wiki (.github/workflows/wiki.yml), so
  edits made in the GitHub wiki web UI are overwritten.
-->

# Outpace: the open-source P2P streaming engine

Outpace is a modern, open-source alternative to the Ace Stream engine, written from scratch in
Rust. It joins public, unencrypted Ace Stream (AceStream) peer-to-peer swarms, verifies the
live media, and serves it to your players over HTTP:

- as a continuous MPEG-TS stream;
- as HLS;
- through an optional Ace Stream-compatible HTTP API that existing apps and add-ons already
  understand.

It's a single self-contained, headless binary (or container image) for Linux, macOS and
Windows. Performance and efficiency are core goals: Outpace aims to start quickly, play
smoothly and stay light on CPU and memory, and the project treats performance regressions as
bugs. It never downloads or runs the proprietary Ace Stream engine. Ace Stream is the network
it supports today. Outpace is an independent project, not affiliated with or endorsed by Ace
Stream.

New here? Start with the [FAQ](FAQ) or [Getting Started](Getting-Started).

## What Outpace does

- **Plays public AceStream content.** Give it a content id, a transport-file URL, an infohash or
  a magnet link and it streams the result to VLC, mpv, ffmpeg, Jellyfin, Dispatcharr and similar
  tools.
- **Serves HLS.** It packages live streams into HLS itself, with optional audio normalisation
  for players that only handle AAC.
- **Takes part in the swarm.** By default it uploads to other peers and accepts inbound peer
  connections, as the original engine does. You can turn this off for a download-only node.
- **Broadcasts.** You can publish your own MPEG-TS feed into a new signed swarm over HTTP, RTMP,
  an HTTP pull source or (in builds that include it) encrypted SRT.
- **Replaces the engine for most clients.** An optional compatibility layer serves the common
  `/ace/getstream`, `/ace/manifest.m3u8` and `/server/api` routes.

## What Outpace is not

- It does **not** play premium, encrypted or protected content. That is a permanent non-goal.
- It is **not** a full clone of the closed engine. There are no accounts, subscriptions, web UI,
  EPG or general channel catalogues. The compatibility API is a documented subset.
- [Stream-name search](Stream-Search) is a separate, default-off external services adapter,
  available from v1.0.0. It needs no operator API key and uses external HTTP services; it does
  not bundle a UI or catalog.
- It is not a recorder or DVR. The optional [Timeshift](Timeshift) buffer lives in memory and
  disappears on restart.

## Where to start

| If you want to... | Read |
| --- | --- |
| Get quick answers | [FAQ](FAQ) |
| Play a stream in five minutes | [Getting Started](Getting-Started) |
| Pick a binary, container or source build | [Installation](Installation) |
| Look up a setting | [Configuration](Configuration) (by topic) · [Environment Variables](Environment-Variables) (A–Z) |
| Use `outpace play`, `broadcast` or other commands | [Command Line](Command-Line) |
| Point a player or app at Outpace | [Playing Streams](Playing-Streams) |
| Tune HLS or fix audio for a player | [HLS Output](HLS-Output) |
| Rewind a live stream | [Timeshift](Timeshift) |
| Play descriptors that point at an HLS origin | [Managed HLS Origins](Managed-HLS-Origins) |
| Publish your own stream | [Broadcasting](Broadcasting) |
| Understand upload and inbound peers | [Seeding and Peers](Seeding-and-Peers) |
| Open your peer port through a router | [NAT and Port Mapping](NAT-and-Port-Mapping) |
| Restrict peers to an allowlist | [Private Peer Mode](Private-Peer-Mode) |
| Run it as a service, in Docker or behind a proxy | [Deployment](Deployment) |
| Run it on a router, NAS or Raspberry Pi | [Routers and Small Devices](Routers-and-Small-Devices) |
| Replace an existing AceStream engine | [Migrating from AceStream Engine](Migrating-from-AceStream-Engine) |
| Keep sources warm and monitor the daemon | [Support Sources and Operations](Support-Sources-and-Operations) |
| Fix a problem or read the logs | [Troubleshooting](Troubleshooting) |
| Check what version has which feature | [Releases and Versions](Releases-and-Versions) |

## Release availability

The latest tagged release is **v1.0.1**. Features marked **From v1.0.0** are available in
release binaries and multi-architecture images; optional SRT still requires a feature build.
The [Configuration](Configuration) reference records feature version floors.
See [Releases and Versions](Releases-and-Versions) for the release contents and remaining limits.

## Placeholder ids

Examples in this wiki use the obviously fake content id
`0123456789abcdef0123456789abcdef01234567`. It does not play anything. Replace it with the id of
a current public stream you are allowed to watch. When you report a problem, please don't paste
real content ids, infohashes or stream names; see
[Troubleshooting](Troubleshooting#reporting-a-bug).

## License

Outpace is licensed under AGPL-3.0-or-later.
