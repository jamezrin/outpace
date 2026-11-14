# Installation

Outpace ships as prebuilt binaries, as a multi-architecture container image, and as source you
can build yourself. All three run the same program: `outpace serve` starts the daemon.

## Choose a version

- **Releases** (`vX.Y.Z` tags) are the stable choice. The latest is **v1.0.3**.
- **`edge`** follows `main` and includes features newer than the latest release. It is
  `linux/amd64` only and changes with every merge. Don't use it where you need predictable
  upgrades.

See [Releases and Versions](Releases-and-Versions) for which features are in which build.

## Release binaries

Every release publishes archives named `outpace-<version>-<target>.<ext>` and one `SHA256SUMS`
file on the [releases page](https://github.com/outpace-stream/outpace-engine/releases).

| Platform | Target | Archive |
| --- | --- | --- |
| Linux x86_64 | `x86_64-unknown-linux-musl` | `.tar.gz` |
| Linux ARM64 (aarch64) | `aarch64-unknown-linux-musl` | `.tar.gz` |
| Linux ARMv7 hard-float (Raspberry Pi 2/3 class) | `armv7-unknown-linux-musleabihf` | `.tar.gz` |
| Linux ARMv6 hard-float (Raspberry Pi Zero/1 class), from v1.0.0 | `arm-unknown-linux-musleabihf` | `.tar.gz` |
| Linux RISC-V 64, from v1.0.0 | `riscv64gc-unknown-linux-musl` | `.tar.gz` |
| FreeBSD 14 and 15 x86_64, from v1.0.0 | `x86_64-unknown-freebsd` | `.tar.gz` |
| macOS Intel | `x86_64-apple-darwin` | `.tar.gz` |
| macOS Apple silicon | `aarch64-apple-darwin` | `.tar.gz` |
| Windows x86_64 | `x86_64-pc-windows-msvc` | `.zip` |
| Windows ARM64 | `aarch64-pc-windows-msvc` | `.zip` |

The Linux binaries are statically linked (musl) and run on any reasonably recent distribution.

Download, verify and install on Linux or macOS. Set `target` to the row for your platform from
the table above:

```bash
version=1.0.3
target=x86_64-unknown-linux-musl   # e.g. aarch64-apple-darwin on Apple silicon
artifact="outpace-${version}-${target}.tar.gz"
base="https://github.com/outpace-stream/outpace-engine/releases/download/v${version}"
curl -LO "${base}/${artifact}"
curl -LO "${base}/SHA256SUMS"
awk -v a="$artifact" '$2 == a { print }' SHA256SUMS | grep -q . \
  && awk -v a="$artifact" '$2 == a { print }' SHA256SUMS | shasum -a 256 --check \
  && tar -xzf "$artifact" \
  && sudo install -m 0755 "outpace-${version}-${target}/outpace" /usr/local/bin/outpace \
  && outpace --version
```

On Windows, unzip the archive and run `outpace.exe serve` from a terminal. To run it in the background, see [Deployment](Deployment#windows-service).

### Android with Termux (best-effort)

From v1.0.0, there is a best-effort `aarch64-linux-android` build for 64-bit ARM
phones and Android TV boxes, to run under [Termux](https://termux.dev). From v1.0.2, there is
also an `armv7-linux-androideabi` build for 32-bit ARM devices, packaged the
same way. Many Android TV boxes run 32-bit Android on a 64-bit chip; they need the 32-bit build.

- **Best-effort** means it is uploaded after the rest of the release. If a build fails, that
  release has no archive for that target.
- These archives aren't covered by `SHA256SUMS`. Each has its own `<archive>.sha256` file.
- These builds run on Android 5 and newer, without root.

In Termux, `dpkg --print-architecture` prints `aarch64` for the 64-bit build or `arm` for the
32-bit one. For `arm`, set `target=armv7-linux-androideabi` and use a release that has it:

```bash
pkg install curl
version=1.0.3
target=aarch64-linux-android
artifact="outpace-${version}-${target}.tar.gz"
base="https://github.com/outpace-stream/outpace-engine/releases/download/v${version}"
curl -LO "${base}/${artifact}" && curl -LO "${base}/${artifact}.sha256"
sha256sum -c "${artifact}.sha256" && tar -xzf "$artifact"
install -m 0755 "outpace-${version}-${target}/outpace" "$PREFIX/bin/outpace"
termux-wake-lock    # keep Android from suspending it in the background
outpace serve
```

- Data lives in `~/.local/share/outpace` inside Termux.
- The API listens on `127.0.0.1:6878`, which players on the same device can reach. To reach it
  from the LAN, set `OUTPACE_BIND=0.0.0.0:6878` on a trusted network only.
- Android may still stop background apps. Disable battery optimisation for Termux if streams
  stop when the screen is off.

The [Android companion app](Android-App) is developed separately. Signed APKs start at v0.1.0
in that repository. [Android TV](Android-TV) is the sideload path for a television. The Termux
instructions above remain the way to run the engine binary yourself.

### Homebrew and Scoop

Package catalogs live in separate repositories: [Homebrew](https://github.com/outpace-stream/homebrew-outpace)
(macOS and Linux on x86_64 or ARM64) and [Scoop](https://github.com/outpace-stream/scoop-outpace)
(Windows x86_64 and ARM64). They install the release selected by the catalog, verified against
its published `SHA256SUMS`:

```bash
brew tap outpace-stream/outpace
brew install outpace-stream/outpace/outpace
brew services start outpace      # optional: run in the background (launchd or systemd)
```

```powershell
scoop bucket add outpace https://github.com/outpace-stream/scoop-outpace
scoop install outpace/outpace
```

- `brew services` runs `outpace serve` with HTTP and RTMP on `127.0.0.1` and logs to
  `$(brew --prefix)/var/log/outpace.log`.
- Upgrade with `brew upgrade outpace` or `scoop update outpace`. The formula and manifest are
  updated shortly after each release, so a brand-new release can take a little while to appear.

If you installed through the previous engine-repository tap or bucket, switch catalogs once:

```bash
brew tap outpace-stream/outpace
brew uninstall jamezrin/outpace/outpace
brew untap jamezrin/outpace
brew install outpace-stream/outpace/outpace
# If you used brew services, stop the old service first and start the new one afterward.
```

```powershell
scoop bucket rm outpace
scoop bucket add outpace https://github.com/outpace-stream/scoop-outpace
scoop update
```

### Arch Linux (AUR)

Two packages are available in the Arch User Repository:

- [outpace-bin](https://aur.archlinux.org/packages/outpace-bin) installs the official
  release binary, verified against its published SHA-256 checksum.
- [outpace-git](https://aur.archlinux.org/packages/outpace-git) builds from Git. Each build
  selects the highest stable `vX.Y.Z` tag fetched from upstream, excluding prereleases and
  untagged commits. Its AUR version records the last published package metadata; rebuild
  it to pick up a newer release tag. It uses the upstream lockfile and the optimised
  `dist` profile, and runs the offline workspace tests. Arch's Rust package supplies the
  compiler; when using rustup instead, the repository's pinned toolchain applies.

Choose one; they provide the same `outpace` command and conflict with each other. Both also
conflict with `acestream-engine`, including packages that provide that name, so pacman
requires removing the original engine before installing Outpace. Outpace does not declare
that it provides `acestream-engine`; applications requiring the original package may need
separate configuration. With an AUR helper already installed:

```bash
yay -S outpace-bin          # official binary
# or: yay -S outpace-git    # compile the latest stable Git tag
```

To install manually, first install Arch's `base-devel` group and `git`, then clone and
review the recipe before running `makepkg` as a regular user:

```bash
git clone https://aur.archlinux.org/outpace-bin.git
cd outpace-bin
less PKGBUILD
makepkg -si
```

Use `outpace-git.git` instead to build from source. Both recipes support x86_64, aarch64,
ARMv7 hard-float (`armv7h`), ARMv6 hard-float (`armv6h`) and RISC-V 64 (`riscv64`), including
compatible Arch derivatives. FFmpeg is optional for audio normalisation and media processing.

Both packages install `/usr/bin/outpace`, a systemd unit, a system-user definition and
`/etc/outpace/outpace.env`. They do not enable or start the service automatically:

```bash
sudoedit /etc/outpace/outpace.env
sudo systemctl enable --now outpace
```

The service runs as `outpace`, with data in `/var/lib/outpace`. HTTP and RTMP bind to
loopback by default. Pacman preserves edited configuration on upgrade and saves it as
`outpace.env.pacsave` on removal; the system user and service data remain. Service lifecycle
handling uses Arch's standard systemd package hooks. See [Deployment](Deployment#systemd)
for service hardening and configuration.

### Debian, Ubuntu and Fedora packages

From v1.0.0, each release also has `.deb` and `.rpm` packages for x86_64, ARM64 and
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

From v1.0.0 there is a FreeBSD x86_64 binary. It uses the same archive layout, so
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
archive. Releases from v1.0.0 onward also carry signed
[build provenance](https://docs.github.com/actions/security-for-github-actions/using-artifact-attestations),
which proves that this repository's release workflow built the archive from the tagged commit.
With the [GitHub CLI](https://cli.github.com):

```bash
gh attestation verify "$artifact" --repo outpace-stream/outpace-engine
```

The `.deb` and `.rpm` packages are attested the same way. This works directly from v1.0.2, the
first release built in the organization. The best-effort Android archives have no attestation;
check them against their `.sha256` file.

Releases v1.0.0 and v1.0.1 were built before the organization migration. GitHub's repository
attestation lookup currently returns 404 for these historical bundles after the transfer;
changing `--repo` to the old URL does not fix the lookup. Fetch the bundle from the original
owner and verify against the original signed repository identity instead:

```bash
digest=$(sha256sum "$artifact" | cut -d ' ' -f1)
gh api "users/jamezrin/attestations/sha256:$digest" \
  --jq '.attestations[].bundle | tojson' > attestation.jsonl
gh attestation verify "$artifact" --bundle attestation.jsonl --repo jamezrin/outpace-stream
```

This verifies the historical signature and artifact digest, using the repository name recorded
at build time. It does not assert current ownership of that old name. The procedure was
verified against the v1.0.1 Linux x86_64 archive during migration.

The same works for release images, including the `-minimal` ones, by tag or digest:

```bash
gh attestation verify oci://ghcr.io/outpace-stream/outpace-engine:<version> --repo outpace-stream/outpace-engine
```

`edge` images and v0.9.0 and older releases have no attestations. Existing images at
`ghcr.io/jamezrin/outpace` remain available; their provenance refers to the old repository.
New images use `ghcr.io/outpace-stream/outpace-engine`.

### ARM, RISC-V and small devices

ARMv7 builds cover hard-float ARMv7 Linux (Raspberry Pi 2/3, older NAS boxes). From v1.0.0,
ARMv6 builds cover hard-float ARMv6 (Raspberry Pi Zero and 1, older ARM routers and NAS
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

[Routers and Small Devices](Routers-and-Small-Devices) covers memory sizing, OpenWrt and
NAS boxes in more detail.

### ARM64 with 16 KiB memory pages

Outpace doesn't bundle the precompiled Android libraries that break some other engines on
16 KiB-page kernels, but 16 KiB-page support has not been verified on real hardware. If you run
one (for example some Asahi Linux or Raspberry Pi 5 kernels), please report whether it works.

## Container image

The image is `ghcr.io/outpace-stream/outpace-engine`.

| Tag | What it is | Architectures |
| --- | --- | --- |
| `1.0.0` (any `X.Y.Z`) | A release. Pin this in production. | `linux/amd64`, `linux/arm64`, `linux/arm/v7` |
| `latest` | The most recent release. | same as releases |
| `X.Y.Z-minimal`, `minimal` | The same release in a minimal `scratch` image, from v1.0.0. | `linux/amd64`, `linux/arm64`, `linux/arm/v7` |
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
  ghcr.io/outpace-stream/outpace-engine:1.0.3
```

Publish HTTP and RTMP on `127.0.0.1` unless a firewall or authenticating proxy protects them.
Only the peer port is meant to face the internet. Outpace has no UDP listener, so there is
nothing to publish for UDP.

The image doesn't include FFmpeg; see [FFmpeg](#ffmpeg) below.

### Minimal image

From v1.0.0, every release image also comes as `X.Y.Z-minimal` (and `minimal` for the
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
[`compose.yml`](https://github.com/outpace-stream/outpace-engine/blob/main/compose.yml) and
[`.env.example`](https://github.com/outpace-stream/outpace-engine/blob/main/.env.example) give you a
production layout with a health check, log rotation and safe port bindings:

```bash
curl -LO https://raw.githubusercontent.com/outpace-stream/outpace-engine/main/compose.yml
curl -L -o .env https://raw.githubusercontent.com/outpace-stream/outpace-engine/main/.env.example
# edit OUTPACE_IMAGE in .env to the release you want, e.g. ghcr.io/outpace-stream/outpace-engine:1.0.3
docker compose up -d --wait
```

Compose forwards only some settings into the container. [Deployment](Deployment#docker-compose)
explains which ones, and how to pass the rest.

## Build from source

You need git, a C toolchain (`build-essential` on Debian/Ubuntu, Xcode command-line tools on
macOS) and [rustup](https://rustup.rs). The repository pins its Rust version in
`rust-toolchain.toml`, and rustup installs it automatically.

```bash
git clone https://github.com/outpace-stream/outpace-engine.git outpace
cd outpace
git checkout v1.0.3          # or stay on main for the newest features
cargo build --release --locked -p ace-engine --bin outpace
./target/release/outpace --version
```

The binary is `target/release/outpace`.

Release binaries use a slower, smaller `dist` profile (link-time optimisation, one codegen unit,
symbols stripped). It roughly halves the binary, which helps on routers and boards with little
flash. To build the same way from v1.0.0 source or later:

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

SRT is available from v1.0.0; build from that tag, or from `main` for development.

## FFmpeg

Outpace doesn't need FFmpeg to play, package HLS or broadcast. It only runs FFmpeg when you turn
on audio normalisation with `OUTPACE_HLS_AUDIO_NORMALIZE` (all versions) or
`OUTPACE_LIVE_AUDIO_NORMALIZE` (from v1.0.0); see
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
FROM ghcr.io/outpace-stream/outpace-engine:1.0.3
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
