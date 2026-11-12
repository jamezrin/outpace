# Installation

Outpace ships as prebuilt binaries, as a multi-architecture container image, and as source you
can build yourself. All three run the same program: `outpace serve` starts the daemon.

## Choose a version

- **Releases** (`vX.Y.Z` tags) are the stable choice. The latest is **v0.9.0**.
- **`edge`** follows `main` and includes features newer than the latest release. It is
  `linux/amd64` only and changes with every merge. Don't use it where you need predictable
  upgrades.

See [Releases and Versions](Releases-and-Versions) for which features are in which build.

## Release binaries

Every release publishes archives named `outpace-<version>-<target>.<ext>` and one `SHA256SUMS`
file on the [releases page](https://github.com/jamezrin/outpace-stream/releases).

| Platform | Target | Archive |
| --- | --- | --- |
| Linux x86_64 | `x86_64-unknown-linux-musl` | `.tar.gz` |
| Linux ARM64 (aarch64) | `aarch64-unknown-linux-musl` | `.tar.gz` |
| Linux ARMv7 hard-float (Raspberry Pi 2/3 class) | `armv7-unknown-linux-musleabihf` | `.tar.gz` |
| macOS Intel | `x86_64-apple-darwin` | `.tar.gz` |
| macOS Apple silicon | `aarch64-apple-darwin` | `.tar.gz` |
| Windows x86_64 | `x86_64-pc-windows-msvc` | `.zip` |
| Windows ARM64 | `aarch64-pc-windows-msvc` | `.zip` |

The Linux binaries are statically linked (musl) and run on any reasonably recent distribution.

Download, verify and install on Linux or macOS. Set `target` to the row for your platform from
the table above:

```bash
version=0.9.0
target=x86_64-unknown-linux-musl   # e.g. aarch64-apple-darwin on Apple silicon
artifact="outpace-${version}-${target}.tar.gz"
base="https://github.com/jamezrin/outpace-stream/releases/download/v${version}"
curl -LO "${base}/${artifact}"
curl -LO "${base}/SHA256SUMS"
awk -v a="$artifact" '$2 == a { print }' SHA256SUMS | grep -q . \
  && awk -v a="$artifact" '$2 == a { print }' SHA256SUMS | shasum -a 256 --check \
  && tar -xzf "$artifact" \
  && sudo install -m 0755 "outpace-${version}-${target}/outpace" /usr/local/bin/outpace \
  && outpace --version
```

On Windows, unzip the archive and run `outpace.exe serve` from a terminal.

### ARMv7 and small devices

ARMv7 builds cover hard-float ARMv7 Linux (Raspberry Pi 2/3, older NAS boxes). ARMv6 and
soft-float ARM are not supported. Expect lower throughput and fewer simultaneous streams than on
64-bit hardware. On 32-bit targets each in-memory pool, such as the piece cache or the timeshift
budget, is capped at about 512 MiB. A conservative start for a small device:

```bash
OUTPACE_CACHE_TYPE=disk \
OUTPACE_SEED_STORE_BYTES=67108864 \
OUTPACE_SESSION_BUFFER=64 \
outpace serve
```

### ARM64 with 16 KiB memory pages

Outpace doesn't bundle the precompiled Android libraries that break some other engines on
16 KiB-page kernels, but 16 KiB-page support has not been verified on real hardware. If you run
one (for example some Asahi Linux or Raspberry Pi 5 kernels), please report whether it works.

## Container image

The image is `ghcr.io/jamezrin/outpace`.

| Tag | What it is | Architectures |
| --- | --- | --- |
| `0.9.0` (any `X.Y.Z`) | A release. Pin this in production. | `linux/amd64`, `linux/arm64`, `linux/arm/v7` |
| `latest` | The most recent release. | same as releases |
| `edge` | The current `main` branch. | `linux/amd64` only |
| `sha-<commit>` | One specific `main` commit. | `linux/amd64` only |

What the image sets up:

- It runs as an unprivileged `outpace` user.
- Data lives in the `/var/lib/outpace` volume.
- The default command is `outpace serve`.
- It sets `OUTPACE_BIND=0.0.0.0:6878`, `OUTPACE_RTMP_BIND=0.0.0.0:1935` and
  `OUTPACE_DATA_DIR=/var/lib/outpace`, so the listeners are reachable through published ports.
- It exposes `6878/tcp` (HTTP), `1935/tcp` (RTMP ingest) and `8621/tcp` (peers).

```bash
docker run -d --name outpace --restart unless-stopped \
  -p 127.0.0.1:6878:6878 \
  -p 127.0.0.1:1935:1935 \
  -p 8621:8621/tcp \
  -v outpace-data:/var/lib/outpace \
  ghcr.io/jamezrin/outpace:0.9.0
```

Publish HTTP and RTMP on `127.0.0.1` unless a firewall or authenticating proxy protects them.
Only the peer port is meant to face the internet. Outpace has no UDP listener, so there is
nothing to publish for UDP.

The image doesn't include FFmpeg; see [FFmpeg](#ffmpeg) below.

### Docker Compose

The repository's
[`compose.yml`](https://github.com/jamezrin/outpace-stream/blob/main/compose.yml) and
[`.env.example`](https://github.com/jamezrin/outpace-stream/blob/main/.env.example) give you a
production layout with a health check, log rotation and safe port bindings:

```bash
curl -LO https://raw.githubusercontent.com/jamezrin/outpace-stream/main/compose.yml
curl -L -o .env https://raw.githubusercontent.com/jamezrin/outpace-stream/main/.env.example
# edit OUTPACE_IMAGE in .env to the release you want, e.g. ghcr.io/jamezrin/outpace:0.9.0
docker compose up -d --wait
```

Compose forwards only some settings into the container. [Deployment](Deployment#docker-compose)
explains which ones, and how to pass the rest.

## Build from source

You need git, a C toolchain (`build-essential` on Debian/Ubuntu, Xcode command-line tools on
macOS) and [rustup](https://rustup.rs). The repository pins its Rust version in
`rust-toolchain.toml`, and rustup installs it automatically.

```bash
git clone https://github.com/jamezrin/outpace-stream.git outpace
cd outpace
git checkout v0.9.0          # or stay on main for the newest features
cargo build --release --locked -p ace-engine --bin outpace
./target/release/outpace --version
```

The binary is `target/release/outpace`. To build the container image yourself:

```bash
docker build -t outpace:local .
```

### Building with SRT ingest

[SRT ingest](Broadcasting#srt-ingest) is an optional compile-time feature. It isn't in the
release binaries or any published image. It needs native Linux (cross-compiling isn't supported),
`pkg-config`, and an encryption-enabled system **libsrt 1.5.7 or newer** with development
headers:

```bash
cargo build --release --locked -p ace-engine --bin outpace --features srt
```

SRT is newer than v0.9.0, so build from `main`.

## FFmpeg

Outpace doesn't need FFmpeg to play, package HLS or broadcast. It only runs FFmpeg when you turn
on audio normalisation with `OUTPACE_HLS_AUDIO_NORMALIZE` (all versions) or
`OUTPACE_LIVE_AUDIO_NORMALIZE` (unreleased: `edge` and builds from `main` only); see
[HLS Output](HLS-Output#audio-normalisation). With both off, it never looks for FFmpeg.

If you turn normalisation on:

- Install an FFmpeg with MPEG-TS mux/demux, the native AAC encoder, MP2/MP3/AC3 decoders and pipe
  support. Distribution packages normally include all of these.
- Outpace checks the FFmpeg at startup and runs a short test conversion before it opens any
  listener. Startup fails if the check fails.
- Point `OUTPACE_FFMPEG_PATH` at the executable if it isn't on `PATH`. It must be FFmpeg itself,
  not a wrapper script that starts other processes.
- The published image doesn't contain FFmpeg. Build a derived image, for example:

```dockerfile
FROM ghcr.io/jamezrin/outpace:0.9.0
USER root
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*
USER outpace
```

## Where Outpace keeps its data

`OUTPACE_DATA_DIR` holds:

| Path | Contents | Back it up? |
| --- | --- | --- |
| `identity.seed` | This node's stable peer identity. | Yes |
| `broadcasts/` | Signing keys and piece reservations of your [broadcasts](Broadcasting). These files are private keys. | Yes, and protect them |
| `cache/` | The disk piece cache, when enabled. Wiped on start. | No |

The default is the platform data directory plus `outpace`: `~/.local/share/outpace` on Linux,
`~/Library/Application Support/outpace` on macOS, and `%APPDATA%\outpace` on Windows. In the
container it is `/var/lib/outpace`. Run only one daemon per data directory.

## Next steps

- [Getting Started](Getting-Started): play something.
- [Deployment](Deployment): systemd, Compose and reverse proxies.
- [Configuration](Configuration): every setting.
