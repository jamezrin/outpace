# Releases and Versions

## How versions work

- Outpace uses `MAJOR.MINOR.PATCH` version numbers. Read the release notes before upgrading;
  the supported compatibility subset and optional features are documented explicitly.
- A release is a git tag `vX.Y.Z`. Publishing one builds the binaries, the `SHA256SUMS` file and
  the container images, and creates a GitHub Release.
- The version a binary reports is the version in the source it was built from. The tag must
  match it.

The latest published release is **v1.0.1**. See the
[GitHub Release](https://github.com/outpace-stream/outpace-engine/releases/tag/v1.0.1) for downloads and
[1.0.1 notes](https://github.com/outpace-stream/outpace-engine/blob/main/docs/releases/1.0.1.md)
for scope, upgrades and validation limits.

## Where to find release notes

- **GitHub Releases:** <https://github.com/outpace-stream/outpace-engine/releases>. Each release lists
  its downloads and generated change notes.
- **Curated notes:** for releases that have them, in
  [`docs/releases/`](https://github.com/outpace-stream/outpace-engine/tree/main/docs/releases)
  (including `1.0.0.md`).
- **Milestones:** <https://github.com/outpace-stream/outpace-engine/milestones> track what each
  version covers.

## Which version am I running?

```bash
outpace --version
docker run --rm ghcr.io/outpace-stream/outpace-engine:1.0.1 --version
curl "http://127.0.0.1:6878/server/api?method=get_version"   # needs OUTPACE_ACE_COMPAT=1
```

`get_version` returns `version`, a numeric `code` (`MAJOR*10000 + MINOR*100 + PATCH`) and the
`platform`. It identifies Outpace, not an AceStream engine build.

**`edge` builds report the workspace version at their source commit.** During release
preparation this can be the upcoming version before that release is published. To find out exactly which commit an `edge` image contains:

```bash
docker image inspect ghcr.io/outpace-stream/outpace-engine:edge \
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

These changes are included in 1.0.0; they are absent from v0.9.0:

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

From the [0.9.0 notes](https://github.com/outpace-stream/outpace-engine/blob/main/docs/releases/0.9.0.md):

- better live protocol compatibility, and serving only complete, verified pieces;
- an optional daemon-wide cap on concurrent sources (`OUTPACE_MAX_SESSIONS`);
- more responsive peer handling under load.

The 0.9.0 notes describe the evidence available at that release. Since then, the reviewed
7,500-second matched comparison completed with more than 7,200 sampled active seconds per
player, and the maintainer recorded [production-replacement acceptance in #210](https://github.com/outpace-stream/outpace-engine/issues/210#issuecomment-6827431678).
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

The [Homebrew tap](https://github.com/outpace-stream/homebrew-outpace) and
[Scoop bucket](https://github.com/outpace-stream/scoop-outpace) contain the catalogs, pinned to
published release checksums. `packaging/` stays in the engine repository because it builds
release packages and provides platform service/install files.

After publishing a release, clone both catalog repositories next to the engine checkout,
create a topic branch in each, generate the manifests, and open a PR in each catalog:

```bash
gh repo clone outpace-stream/homebrew-outpace ../homebrew-outpace
gh repo clone outpace-stream/scoop-outpace ../scoop-outpace
git -C ../homebrew-outpace switch -c chore/update-outpace
git -C ../scoop-outpace switch -c chore/update-outpace
gh release download vX.Y.Z --repo outpace-stream/outpace-engine --pattern SHA256SUMS --dir /tmp/sums
python3 tools/release/package_manifests.py vX.Y.Z /tmp/sums/SHA256SUMS \
  --homebrew-root ../homebrew-outpace --scoop-root ../scoop-outpace
```

The generator requires explicit destination directories; it does not create `Formula/` or
`bucket/` inside the engine. Each catalog runs its own real-platform install checks.

## Organization migration

The engine repository moved from `jamezrin/outpace-stream` to
`outpace-stream/outpace-engine`. GitHub redirects old repository links and Git remotes;
update local remotes with `git remote set-url origin git@github.com:outpace-stream/outpace-engine.git`.
The Android companion app is developed independently in
[outpace-stream/outpace-android](https://github.com/outpace-stream/outpace-android).
Its APK releases belong there; engine binaries remain in engine releases.

Container publication now targets `ghcr.io/outpace-stream/outpace-engine`. Existing personal-
namespace images at `ghcr.io/jamezrin/outpace` remain available for rollback. The initial
organization image is v1.0.1; older image tags remain in the previous namespace. The binary
name, configuration, data paths and release archive names have not changed. Before switching
a deployment, verify the desired tag exists in the new namespace. See
[Installation](Installation#verify-where-a-release-came-from) for historical attestation identity.

## Upgrading

See [Deployment](Deployment#upgrades-and-rollback). In short:

1. Back up the data directory.
2. Pin the new tag.
3. Recreate the container (or restart the service).
4. Check `/healthz` and the logs.

Take extra care with active broadcasts when you roll back.
