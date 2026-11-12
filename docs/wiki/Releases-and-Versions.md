# Releases and Versions

## How versions work

- Outpace uses `MAJOR.MINOR.PATCH` version numbers. It is still before 1.0, so minor releases
  can change behaviour; read the notes before you upgrade.
- A release is a git tag `vX.Y.Z`. Publishing one builds the binaries, the `SHA256SUMS` file and
  the container images, and creates a GitHub Release.
- The version a binary reports is the version in the source it was built from. The tag must
  match it.

The latest release is **v0.9.0**. Work toward 1.0, the production-replacement milestone, is
ongoing.

## Where to find release notes

- **GitHub Releases:** <https://github.com/jamezrin/outpace-stream/releases>. Each release lists
  its downloads and generated change notes.
- **Curated notes:** for releases that have them, in
  [`docs/releases/`](https://github.com/jamezrin/outpace-stream/tree/main/docs/releases)
  (for example `0.9.0.md`).
- **Milestones:** <https://github.com/jamezrin/outpace-stream/milestones> track what each
  version covers.

## Which version am I running?

```bash
outpace --version
docker run --rm ghcr.io/jamezrin/outpace:0.9.0 --version
curl "http://127.0.0.1:6878/server/api?method=get_version"   # needs OUTPACE_ACE_COMPAT=1
```

`get_version` returns `version`, a numeric `code` (`MAJOR*10000 + MINOR*100 + PATCH`) and the
`platform`. It identifies Outpace, not an AceStream engine build.

**`edge` builds report the last release's version number**, because the version only changes
when a release is cut. To find out exactly which commit an `edge` image contains:

```bash
docker image inspect ghcr.io/jamezrin/outpace:edge \
  --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}'
```

Every `edge` build is also tagged `sha-<full commit hash>`. Pin that tag if you need a specific
build.

## Image tags

| Tag | Moves? | Architectures | Use for |
| --- | --- | --- | --- |
| `X.Y.Z` (e.g. `0.9.0`) | No | amd64, arm64, arm/v7 | Production. Pin this. |
| `latest` | With each release | amd64, arm64, arm/v7 | Trying the newest release. |
| `edge` | With every merge to `main` | amd64 | Testing unreleased features. |
| `sha-<commit>` | No | amd64 | Pinning one specific `main` build. |

## Not yet released

These are merged into `main` (and so in `edge`), but aren't in v0.9.0:

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
- A FreeBSD x86_64 release binary; see
  [Installation](Installation#freebsd-and-other-bsds).
- Signed build provenance for release archives and images, checked with
  `gh attestation verify` (see [Installation](Installation#verify-where-a-release-came-from)).
- Release binaries for ARMv6 hard-float (`arm-unknown-linux-musleabihf`) and RISC-V 64
  (`riscv64gc-unknown-linux-musl`); see [Installation](Installation#release-binaries).

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

The notes also say plainly that 0.9.0 isn't a production sign-off. Long-running playback
stability is still being investigated.

## Upgrading

See [Deployment](Deployment#upgrades-and-rollback). In short:

1. Back up the data directory.
2. Pin the new tag.
3. Recreate the container (or restart the service).
4. Check `/healthz` and the logs.

Take extra care with active broadcasts when you roll back.
