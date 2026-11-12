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
| Linux ARMv6 hard-float (Raspberry Pi Zero/1 class), from the next release | `arm-unknown-linux-musleabihf` | `.tar.gz` |
| Linux RISC-V 64, from the next release | `riscv64gc-unknown-linux-musl` | `.tar.gz` |
| FreeBSD 14 and 15 x86_64, from the next release | `x86_64-unknown-freebsd` | `.tar.gz` |
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

### Debian, Ubuntu and Fedora packages

From the next release, each release also has `.deb` and `.rpm` packages for x86_64, ARM64 and
ARMv7. They contain the same static binary, plus a systemd unit and a configuration file, and
they are listed in `SHA256SUMS`.

| Architecture | `.deb` | `.rpm` |
| --- | --- | --- |
| x86_64 | `outpace_<version>_amd64.deb` | `outpace_<version>_x86_64.rpm` |
| ARM64 | `outpace_<version>_arm64.deb` | `outpace_<version>_aarch64.rpm` |
| ARMv7 hard-float | `outpace_<version>_armhf.deb` | `outpace_<version>_armv7hl.rpm` |

```bash
sudo apt install ./outpace_<version>_amd64.deb      # Debian, Ubuntu
sudo dnf install ./outpace_<version>_x86_64.rpm     # Fedora, RHEL and derivatives
sudoedit /etc/outpace/outpace.env                   # optional: change settings
sudo systemctl enable --now outpace
```

What the package does:

- installs `/usr/bin/outpace`, `/usr/lib/systemd/system/outpace.service` and
  `/etc/outpace/outpace.env`;
- creates a system user and group `outpace`;
- does **not** enable or start the service. On upgrade, a running service is restarted on the
  new binary;
- on removal, stops and disables the service. It keeps the `outpace` user, your edited
  `outpace.env` and `/var/lib/outpace` (your node identity and broadcast keys). Delete them by hand
  to purge.

The service runs with the hardening described in [Deployment](Deployment#systemd). Data lives in
`/var/lib/outpace`.

### FreeBSD and other BSDs

From the next release there is a FreeBSD x86_64 binary. It uses the same archive layout, so
the commands above work with `target=x86_64-unknown-freebsd` and `version` set to a release
that includes it (v0.9.0 does not). Use `sha256 -c` or
`shasum -a 256 --check` to verify. The binary is built against FreeBSD 14 and is tested on 14
and 15. That also covers pfSense and OPNsense, but install it with care on a firewall: keep the
API on `127.0.0.1` and open only the peer port. To run it as a service, see
[Deployment](Deployment#freebsd-rcd).

There are no NetBSD or OpenBSD builds. On those systems,
[build from source](#build-from-source) on the machine itself.

### Verify where a release came from

`SHA256SUMS` proves an archive wasn't corrupted, but it comes from the same release page as the
archive. Releases newer than v0.9.0 also carry signed
[build provenance](https://docs.github.com/actions/security-for-github-actions/using-artifact-attestations),
which proves that this repository's release workflow built the archive from the tagged commit.
With the [GitHub CLI](https://cli.github.com):

```bash
gh attestation verify "$artifact" --repo jamezrin/outpace-stream
```

The same works for release images, including the `-minimal` ones, by tag or digest:

```bash
gh attestation verify oci://ghcr.io/jamezrin/outpace:<version> --repo jamezrin/outpace-stream
```

`edge` images and v0.9.0 and older releases have no attestations.

### ARM, RISC-V and small devices

ARMv7 builds cover hard-float ARMv7 Linux (Raspberry Pi 2/3, older NAS boxes). From the next
release, ARMv6 builds cover hard-float ARMv6 (Raspberry Pi Zero and 1, older ARM routers and NAS
boxes), and RISC-V 64 builds cover 64-bit RISC-V boards. Soft-float ARM is not supported. The
container image doesn't cover ARMv6 or RISC-V 64 yet: use the binaries. Expect lower throughput
and fewer simultaneous streams on 32-bit ARM than on 64-bit hardware. On 32-bit targets each in-memory pool, such as the piece cache or the timeshift
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
| `X.Y.Z-minimal`, `minimal` | The same release in a minimal `scratch` image, from the next release. | `linux/amd64`, `linux/arm64`, `linux/arm/v7` |
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

### Minimal image

From the next release, every release image also comes as `X.Y.Z-minimal` (and `minimal` for the
newest). It is built `FROM scratch`: the same static binary, its licence, and a data directory,
with no shell, package manager or `curl`. It is about a fifth of the size of the Debian image.

- It runs as user and group `10001`, so a bind-mounted data directory must be writable by that
  ID.
- Its built-in health check runs `outpace healthcheck`, because there is no `curl`. In Compose,
  override the health check (see [Deployment](Deployment#docker-compose)).
- You can't extend it with `apt`. To add FFmpeg, start from the Debian image.
- Everything else matches the Debian image: environment, ports, volume and default command.

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

The binary is `target/release/outpace`.

Release binaries use a slower, smaller `dist` profile (link-time optimisation, one codegen unit,
symbols stripped). It roughly halves the binary, which helps on routers and boards with little
flash. To build the same way, from source newer than v0.9.0:

```bash
cargo build --profile dist --locked -p ace-engine --bin outpace
./target/dist/outpace --version
```

To build the container image yourself:

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

The default is the platform data directory plus `outpace`: `~/.local/share/outpace` on Linux and the BSDs,
`~/Library/Application Support/outpace` on macOS, and `%APPDATA%\outpace` on Windows. In the
container it is `/var/lib/outpace`. Run only one daemon per data directory.

## Next steps

- [Getting Started](Getting-Started): play something.
- [Deployment](Deployment): systemd, Compose and reverse proxies.
- [Configuration](Configuration): every setting.
