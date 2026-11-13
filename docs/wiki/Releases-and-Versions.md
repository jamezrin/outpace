# Releases and Versions

## How versions work

- Outpace uses `MAJOR.MINOR.PATCH` version numbers. Read the release notes before upgrading;
  the supported compatibility subset and optional features are documented explicitly.
- A release is a git tag `vX.Y.Z`. Publishing one builds the binaries, the `SHA256SUMS` file and
  the container images, and creates a GitHub Release.
- The version a binary reports is the version in the source it was built from. The tag must
  match it.

**v1.0.0 is being prepared for publication.** Until its release workflow completes,
**v0.9.0** remains the latest published release. Examples in this wiki target 1.0.0 once
published; use 0.9.0 for existing downloads or `main`/`edge` for the new functionality.
See the [1.0.0 notes](https://github.com/jamezrin/outpace-stream/blob/main/docs/releases/1.0.0.md)
for scope, upgrades and validation limits.

## Where to find release notes

- **GitHub Releases:** <https://github.com/jamezrin/outpace-stream/releases>. Each release lists
  its downloads and generated change notes.
- **Curated notes:** for releases that have them, in
  [`docs/releases/`](https://github.com/jamezrin/outpace-stream/tree/main/docs/releases)
  (including the upcoming `1.0.0.md`).
- **Milestones:** <https://github.com/jamezrin/outpace-stream/milestones> track what each
  version covers.

## Which version am I running?

```bash
outpace --version
docker run --rm ghcr.io/jamezrin/outpace:1.0.0 --version
curl "http://127.0.0.1:6878/server/api?method=get_version"   # needs OUTPACE_ACE_COMPAT=1
```

`get_version` returns `version`, a numeric `code` (`MAJOR*10000 + MINOR*100 + PATCH`) and the
`platform`. It identifies Outpace, not an AceStream engine build.

**`edge` builds report the workspace version at their source commit.** During release
preparation this can be the upcoming version before that release is published. To find out exactly which commit an `edge` image contains:

```bash
docker image inspect ghcr.io/jamezrin/outpace:edge \
  --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}'
```

Every `edge` build is also tagged `sha-<full commit hash>`. Pin that tag if you need a specific
build.

## Image tags

| Tag | Moves? | Architectures | Use for |
| --- | --- | --- | --- |
| `X.Y.Z` (e.g. `1.0.0`) | No | amd64, arm64, arm/v7 | Production. Pin this. |
| `latest` | With each release | amd64, arm64, arm/v7 | Trying the newest release. |
| `X.Y.Z-minimal`, `minimal` | Like `X.Y.Z` and `latest` | amd64, arm64, arm/v7 | Smaller `scratch` image (from v1.0.0). |
| `edge` | With every merge to `main` | amd64 | Testing unreleased features. |
| `sha-<commit>` | No | amd64 | Pinning one specific `main` build. |

## v1.0.0 highlights

These changes are merged into `main` and planned for 1.0.0; they are absent from v0.9.0:

**New features**

- [Timeshift](Timeshift): per-viewer rewind of live HLS (`OUTPACE_HISTORY_*`).
- [Managed HLS origins](Managed-HLS-Origins): HLS-origin descriptors, the opt-in relay
  (`OUTPACE_MANAGED_HLS`), gzip playlists, connection reuse, `X-Outpace-Upstream-Status`, and
  origin replay.
- [SRT ingest](Broadcasting#srt-ingest), in builds with the `srt` feature
  (`OUTPACE_SRT_*`).
- [Shared live audio normalisation](HLS-Output#audio-normalisation) for TS and HLS
  (`OUTPACE_LIVE_AUDIO_NORMALIZE`).
- [Private peer mode](Private-Peer-Mode) and local descriptors (`OUTPACE_PRIVATE_PEERS`,
  `OUTPACE_PRIVATE_DESCRIPTORS`, `/network-policy`).
- [Support sources](Support-Sources-and-Operations#support-sources) (`OUTPACE_SUPPORT_SOURCES`,
  `/support`).
- A Windows service (`outpace service install`) and a macOS LaunchAgent; see
  [Deployment](Deployment#windows-service).
- A best-effort Android (`aarch64-linux-android`) binary for Termux; see
  [Installation](Installation#android-with-termux-best-effort).
- An OpenWrt procd service and installer; see [Routers and Small Devices](Routers-and-Small-Devices#openwrt).
- `.deb` and `.rpm` packages with a systemd unit; see
  [Installation](Installation#debian-ubuntu-and-fedora-packages).
- A minimal `scratch` image (`X.Y.Z-minimal`) and the
  [`outpace healthcheck`](Command-Line#outpace-healthcheck) command.
- A FreeBSD x86_64 release binary; see
  [Installation](Installation#freebsd-and-other-bsds).
- Signed build provenance for release archives and images, checked with
  `gh attestation verify` (see [Installation](Installation#verify-where-a-release-came-from)).
- Release binaries for ARMv6 hard-float (`arm-unknown-linux-musleabihf`) and RISC-V 64
  (`riscv64gc-unknown-linux-musl`); see [Installation](Installation#release-binaries).
- [Stream-name search](Stream-Search), behind the separate default-off
  `OUTPACE_COMPAT_ACESTREAM_SERVICES` setting; client integration limits still apply.
- Opt-in [Prometheus metrics](Support-Sources-and-Operations#prometheus-metrics),
  enabled with `OUTPACE_METRICS=1`.
- Homebrew and Scoop installers; their manifests are refreshed after publication using
  the new release's checksums.

**Fixes you'll notice**

- An HLS stream whose source ends now returns its segments with `#EXT-X-ENDLIST` instead of
  `503` for ever.
- `get_media_files` reports `mime: video/mp2t` and a display filename for live streams, which the
  Android SDK needs.
- `OUTPACE_PREBUFFER_TIMEOUT_MS` is counted from the first media byte, and source gaps no longer
  restart it.
- Stopping the daemon no longer waits for connected viewers to disconnect.
- One stuck stream cleanup no longer stops later idle streams from being cleaned up.
- The compatibility API rejects duplicated query parameters and file or stream selections it
  can't honour, while still accepting the Android SDK's duplicated descriptor parameters.

## v0.9.0 highlights

From the [0.9.0 notes](https://github.com/jamezrin/outpace-stream/blob/main/docs/releases/0.9.0.md):

- better live protocol compatibility, and serving only complete, verified pieces;
- an optional daemon-wide cap on concurrent sources (`OUTPACE_MAX_SESSIONS`);
- more responsive peer handling under load.

The 0.9.0 notes describe the evidence available at that release. Since then, the reviewed
7,500-second matched comparison completed with more than 7,200 sampled active seconds per
player, and the maintainer recorded [production-replacement acceptance in #210](https://github.com/jamezrin/outpace-stream/issues/210#issuecomment-6827431678).
That acceptance is not a new passing soak of every later commit. Latency to live against the
reference remains unmeasured; the historical reconnect stall was closed as inconclusive,
not as causally fixed. See the 1.0.0 notes for the remaining limits.

## Deferred scope

For 1.0.0, multi-file/selected-file VOD (#79), optional broadcast discovery and metadata fields
(#269), and original-engine HLS transport origination/support parity (#272) are explicitly
deferred. VOD remains single-file; native VOD HLS has approximate byte-based segmentation,
and PCR/keyframe seeking (#128) remains conditional on a demonstrated player failure.
Managed HLS relay support does not establish original-engine HLS origination parity.
These limitations do not disable supported live playback, native HLS or the documented
broadcast subset. See the linked release notes for issue references and evidence requirements.

## Updating the Homebrew formula and Scoop manifest

`Formula/outpace.rb` and `bucket/outpace.json` always point at the newest published release,
because their checksums only exist once it's out. After publishing a release, a maintainer
refreshes them and opens a PR:

```bash
gh release download vX.Y.Z --pattern SHA256SUMS --dir /tmp/sums
python3 tools/release/package_manifests.py vX.Y.Z /tmp/sums/SHA256SUMS
```

## Upgrading

See [Deployment](Deployment#upgrades-and-rollback). In short:

1. Back up the data directory.
2. Pin the new tag.
3. Recreate the container (or restart the service).
4. Check `/healthz` and the logs.

Take extra care with active broadcasts when you roll back.
