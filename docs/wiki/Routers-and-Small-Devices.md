# Routers and Small Devices

Outpace runs on routers, NAS boxes and single-board computers, but they have far less RAM
and flash than a server. This page collects what changes on them.

## Which binary

| Device | Use |
| --- | --- |
| OpenWrt router | The [OpenWrt installer](#openwrt) below. It picks the right build. |
| Raspberry Pi 2/3/4/5, most ARM NAS boxes | `armv7-unknown-linux-musleabihf` (32-bit) or `aarch64-unknown-linux-musl` (64-bit OS) from [Installation](Installation#release-binaries). |
| x86 mini PC or appliance | `x86_64-unknown-linux-musl`. |

All Linux release binaries are static (musl), so they run on any distribution, including
ones without glibc. Check [Installation](Installation#release-binaries) for which other
architectures a release has.

## Memory settings

The defaults suit a server. The large buffers are per stream (and the session buffer per
viewer), so on a small device cap them and allow one stream at a time:

| Variable | Default | Small device |
| --- | --- | --- |
| `OUTPACE_MAX_SESSIONS` | `0` (unlimited) | `1` |
| `OUTPACE_SEED_STORE_BYTES` | 128 MiB | `33554432` (32 MiB) |
| `OUTPACE_PREBUFFER_BYTES` | 128 MiB | `16777216` (16 MiB) |
| `OUTPACE_SESSION_BUFFER` | `256` | `64` |
| `OUTPACE_CACHE_TYPE` | `memory` | `disk`, on USB or other external storage only |

- On 32-bit systems each in-memory pool is capped at about 512 MiB whatever you set.
- Don't put `OUTPACE_CACHE_DIR` on the router's internal flash: the cache is rewritten
  constantly and wears flash out. Use USB storage, or keep the memory cache with a small seed
  store.
- See [Configuration](Configuration) for every setting.

## OpenWrt

The repository's
[`packaging/openwrt/`](https://github.com/jamezrin/outpace-stream/tree/main/packaging/openwrt)
directory has a procd service, a UCI configuration with the small-device settings above, and an
installer. It is tested under procd on OpenWrt 24.10.

Download the repository to the router, then run the installer as root with the release you
want. The installer and its files come from the repository; the binary comes from the release:

```sh
cd /tmp
wget -O outpace-src.tar.gz https://github.com/jamezrin/outpace-stream/archive/refs/heads/main.tar.gz
tar -xzf outpace-src.tar.gz
sh outpace-stream-main/packaging/openwrt/install.sh 0.9.0
```

The installer:

- reads the router's architecture from `/etc/openwrt_release` and downloads the matching release
  archive;
- checks the archive against `SHA256SUMS`, or against the archive's own `.sha256` file for
  best-effort MIPS builds;
- installs `/usr/bin/outpace`, `/etc/init.d/outpace` and `/etc/config/outpace`, and creates an
  unprivileged `outpace` user;
- leaves the service disabled.

To install a binary you already have, run `install.sh --binary /path/to/outpace` instead.
Running the installer again upgrades the binary and keeps your configuration.

| OpenWrt architecture | Release build |
| --- | --- |
| `x86_64` | `x86_64-unknown-linux-musl` |
| `aarch64_*` | `aarch64-unknown-linux-musl` |
| `arm_*` with VFP/NEON (e.g. `arm_cortex-a7_neon-vfpv4`) | `armv7-unknown-linux-musleabihf` |
| `arm_arm1176jzf-s_vfp` | `arm-unknown-linux-musleabihf` |
| `mipsel_*` (e.g. MT7621) | `mipsel-unknown-linux-musl`, best-effort |
| `mips_*` (e.g. ath79) | `mips-unknown-linux-musl`, best-effort |
| `riscv64_*` | `riscv64gc-unknown-linux-musl` |

Only releases that ship an architecture's build can be installed on it. ARMv6, RISC-V 64 and
MIPS builds are newer than v0.9.0, so on those routers install a later release.

Enable and start it:

```sh
uci set outpace.main.enabled=1
uci commit outpace
/etc/init.d/outpace enable
/etc/init.d/outpace start
logread -e outpace
```

- Edit `/etc/config/outpace` to change settings, then run `/etc/init.d/outpace restart`. The
  named options cover the common settings. Anything else goes in `list env 'NAME=value'`.
- The API listens on `127.0.0.1:6878`. To reach it from the LAN, set `option bind` to the
  router's LAN address, for example `192.168.1.1:6878`. Never bind it on the WAN.
- `/etc/outpace` holds the node identity and broadcast keys. It and the binary are kept across
  `sysupgrade`.
- To take part as a full peer, open TCP 8621 on the WAN in the firewall, or turn on
  [port mapping](NAT-and-Port-Mapping) on a router that runs UPnP.
- Best-effort MIPS builds need `libgcc`, which most images include. If the installer says the
  binary doesn't run, install it with `apk add libgcc` or `opkg install libgcc`.

## pfSense and OPNsense

These run FreeBSD. From the next release, the FreeBSD x86_64 binary covers them. Treat a
firewall carefully: keep the API on `127.0.0.1` and open only the peer port.

## Related pages

- [Installation](Installation): every binary and image.
- [Deployment](Deployment): systemd, Docker Compose and reverse proxies.
- [NAT and Port Mapping](NAT-and-Port-Mapping): making the peer port reachable.
