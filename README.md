# outpace

outpace is a from-scratch Rust implementation of the public Acestream P2P live
streaming path. It joins Acestream swarms, pulls MPEG-TS media, and exposes the
result through a native daemon API, HLS, and CLI commands that work with players
such as VLC and downstream tools such as Jellyfin or dispatcharr.

The project does not use closed-source engine blobs at runtime. Reverse-engineering
fixtures, protocol captures, and reference binaries are kept under git-ignored
`re/` and `references/` directories when present on a development machine.

## Status

The live byte path is built and proven against real public swarms:

- protocol recovery and interop vectors are in place;
- the `ace-wire`, `ace-tracker`, `ace-peer`, `ace-swarm`, `ace-media`, and
  `ace-engine` crates are in the workspace;
- the daemon can discover peers, connect, download live media, and serve it over
  the native API;
- reciprocal upload, inbound seeding plumbing, signed broadcast origination,
  HTTP raw ingest, RTMP ingest, content-id metadata serving, and the three-command
  CLI are implemented;
- live-network tests remain ignored by default because they need the public swarm
  and a suitable network.

Run the normal local gate with:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

## Quick Start

Run the daemon:

```bash
cargo run -p ace-engine --bin outpace -- serve
```

With no subcommand, `outpace` defaults to `serve`.

Play a known Acestream link to stdout:

```bash
cargo run -p ace-engine --bin outpace -- play acestream://<content-id> > live.ts
```

`play` accepts four mutually exclusive selectors, applied in precedence order
`content_id` > `infohash` > `url` > `magnet`:

- `acestream://<content_id>` or `acestream:?content_id=<40-hex>`;
- `acestream:?infohash=<40-hex>`;
- a transport-file URL - either passed bare (`play https://host/x.acelive?a=1&b=2`,
  which handles a URL carrying its own `&`-joined query) or as
  `acestream:?url=<https://…>` (in the `acestream:?` form the URL's own query must
  be percent-encoded, e.g. `%26` for `&`). The descriptor is fetched over http/https
  with SSRF protection (private, loopback, and link-local hosts are blocked), a 1 MiB
  size cap, disabled redirects, and a request timeout; unsafe, oversized, or
  non-transport responses fail closed;
- `magnet:?xt=urn:btih:<40-hex-or-32-base32>` - a BitTorrent v1 magnet, reduced to
  its infohash (v2 `urn:btmh:` magnets are rejected).

The `/ace/getstream` compatibility route accepts the same selectors plus legacy
`id=<40-hex>`, which is an alias for `content_id` (not `infohash`). Its precedence is
`content_id` > `infohash` > `id` > `url` > `magnet`. A `url=` selector uses a self-contained
playback id, so playback works after a daemon restart without any server-side alias table.

### VOD (single-file)

For a **VOD** transport (a descriptor carrying a `pieces` SHA1 list), add `--vod` to
download and verify the file to stdout instead of following a live stream:

```bash
cargo run -p ace-engine --bin outpace -- play --vod acestream://<content-id> > movie.mp4
```

Each piece is SHA1-checked against the transport's `pieces` before any bytes are written.
The daemon serves the same content over `GET /vod/<network>/<id>` with a `Content-Length`,
advertises `Accept-Ranges: bytes`, and honors single `Range` requests (`206 Partial Content`)
so players can seek — each served range is still SHA1-verified against the covering pieces.
Multi-file VOD is intentionally unsupported and fails with a clear error; VOD HLS is a tracked
follow-up.

Start a named broadcast:

```bash
cargo run -p ace-engine --bin outpace -- broadcast my-channel
```

The broadcast command prints:

- a raw MPEG-TS HTTP ingest URL;
- an RTMP ingest URL at `rtmp://<host>:<rtmp-port>/live/<name>`;
- an `acestream://<content_id>` link backed by BEP-9 `ut_metadata` serving.

## Installation

Release artifacts are created from `vX.Y.Z` git tags and published on GitHub
Releases. Binary archives are named `outpace-<version>-<target>.<ext>`, where
`<version>` is the tag without its leading `v`.

Available targets:

- `x86_64-unknown-linux-musl`
- `aarch64-unknown-linux-musl`
- `armv7-unknown-linux-musleabihf`
- `x86_64-apple-darwin`
- `aarch64-apple-darwin`
- `x86_64-pc-windows-msvc`
- `aarch64-pc-windows-msvc`

Install a prebuilt Unix binary:

```bash
version=0.1.0
target=x86_64-unknown-linux-musl
artifact="outpace-${version}-${target}.tar.gz"
curl -LO "https://github.com/jamezrin/outpace/releases/download/v${version}/${artifact}"
curl -LO "https://github.com/jamezrin/outpace/releases/download/v${version}/SHA256SUMS"
awk -v artifact="$artifact" '$2 == artifact { print }' SHA256SUMS | shasum -a 256 --check
tar -xzf "$artifact"
sudo install -m 0755 "outpace-${version}-${target}/outpace" /usr/local/bin/outpace
```

Windows releases use `.zip` archives with the same naming scheme.

Run the published container image:

```bash
docker run --rm \
  -p 6878:6878 \
  -p 1935:1935 \
  -p 8621:8621/tcp \
  -v outpace-data:/var/lib/outpace \
  ghcr.io/jamezrin/outpace:0.1.0
```

The container defaults to `outpace serve`. It sets `OUTPACE_BIND=0.0.0.0:6878`,
`OUTPACE_RTMP_BIND=0.0.0.0:1935`, and `OUTPACE_DATA_DIR=/var/lib/outpace`.
Inbound peer serving currently listens on TCP 8621; DHT and tracker UDP traffic use outbound
sockets and does not require publishing UDP 8621.
The image manifest includes `linux/amd64`, `linux/arm64`, and `linux/arm/v7`. See
[`docs/linux-portability.md`](docs/linux-portability.md) for ARMv7 resource guidance and the
manual aarch64 16 KiB-page qualification procedure.

For a checked-in production Compose deployment, safe exposure defaults, leecher/disk/broadcast
examples, DNS overrides, and upgrade guidance, see
[`docs/deployment.md`](docs/deployment.md).

## Cutting a Release

The release version is the Cargo package version: the workspace crates inherit a
single `version` from `[workspace.package]` in the root `Cargo.toml`, and that is
the version `outpace --version` reports. The git tag is only a validated trigger.

First bump the shared version, then tag it with a leading `v`:

```bash
# edit [workspace.package] version in Cargo.toml, e.g. to 0.2.0, then:
git tag v0.2.0
git push origin v0.2.0
```

Pushing the tag runs `.github/workflows/release.yml`. It reads the version from
`cargo metadata` and fails the release before building anything if the tag does
not match `v<cargo_version>`, so artifact names and Docker tags always agree with
the Cargo version. The workflow:

- builds Linux binaries for amd64, arm64, and ARMv7, plus macOS and Windows binaries for amd64
  and arm64;
- uploads `tar.gz` archives for Unix targets and `zip` archives for Windows;
- generates one `SHA256SUMS` file for all archives;
- creates a GitHub Release with generated release notes;
- publishes `ghcr.io/jamezrin/outpace:<version>` and `ghcr.io/jamezrin/outpace:latest`.

## Runtime Surfaces

The supported integration contract is the native CLI and HTTP API. See the
route table, response shapes, and VLC/Jellyfin/dispatcharr examples in
[`docs/native-api.md`](docs/native-api.md).

Legacy Acestream-compatible routes such as `/ace/*` and `/server/api` are
experimental and disabled by default. Enable them only when needed:

```bash
OUTPACE_EXPERIMENTAL_ACE_COMPAT=1 cargo run -p ace-engine --bin outpace -- serve
```

On that compatibility surface, `/ace/stat` fields live under `.response`. A directly playable
legacy URL streams MPEG-TS without a JSON handshake:

```text
http://127.0.0.1:6878/ace/getstream?id=<content-id>
```

Call `/ace/getstream?format=json&content_id=<content-id>` when the client needs the tokenized
playback/stat/command URL envelope instead. `id=` and `content_id=` enter content-ID catalog
resolution; only `infohash=` bypasses it as an explicit swarm key. All hash selectors must be
40 hexadecimal characters. The route also accepts `url=` (transport-file URL) and `magnet=`;
see the compatibility matrix for exact precedence and error behavior.

`/server/api` serves a targeted subset of the engine's JSON control API, dispatched
on `?method=` and wrapped in a `{ "result", "error" }` envelope (note the `result`
key, distinct from `/ace/*`'s `response`): `get_version`, `get_status`,
`get_network_connection_status`, `analyze_content`, `get_content_id`, and
`get_media_files`. The supported routes/methods, intentionally rejected surface, and
deferred work are documented in
[`docs/protocol/compat-matrix.md`](docs/protocol/compat-matrix.md).

## Configuration

Environment variables parsed by the daemon include:

- `OUTPACE_BIND` - HTTP bind address, default `127.0.0.1:6878`.
- `OUTPACE_RTMP_BIND` - RTMP ingest bind address, default `127.0.0.1:1935`.
- `OUTPACE_DATA_DIR` - persistent identity/cache root, default platform data dir
  plus `outpace`.
- `OUTPACE_PEER_LISTEN` - inbound peer listener bind, default `0.0.0.0:8621`.
- `OUTPACE_SEED_STORE_BYTES` - byte budget for retained piece data (sizes both cache backends),
  default `134217728` (128 MiB). A hard safety cap; on a live stream
  `OUTPACE_SEED_RETENTION_SECS` is the primary limiter.
- `OUTPACE_SEED_RETENTION_SECS` - age bound (seconds) for a live reseed store, default `45`:
  retain roughly this much recent downloaded data for reseeding instead of filling
  `OUTPACE_SEED_STORE_BYTES`, so RAM tracks bitrate rather than always growing to the byte cap.
  `0` disables the age bound (byte-only, the pre-0.2 behavior). VOD stores are always byte-only.
- `OUTPACE_CACHE_TYPE` - where the seed store keeps piece data: `memory` (default) or `disk`.
  `disk` trades RAM for capacity, mirroring Acestream's disk-cache option.
- `OUTPACE_CACHE_DIR` - root dir for disk-mode piece files (one subdir per served stream; see
  below), default `<data_dir>/cache`. Only used when `OUTPACE_CACHE_TYPE=disk`.
- `OUTPACE_PREFETCH_PIECES` - pieces behind the live edge to start at, default `8`.
- `OUTPACE_SESSION_BUFFER` - per-client fan-out channel depth, default `256`;
  must be at least `1`.
- `OUTPACE_REQUEST_TIMEOUT_MS` - per-piece request timeout before re-requesting or skipping an
  evicted gap, default `1500`; must be lower than `OUTPACE_STALE_UPSTREAM_TIMEOUT_MS`. A live
  player drains in realtime, so raising this leaves a stuck piece to be healed by the much slower
  `OUTPACE_STALE_UPSTREAM_TIMEOUT_MS` pool teardown instead — a visible playback gap.
- `OUTPACE_STALE_UPSTREAM_TIMEOUT_MS` - whole-upstream no-progress timeout before reconnecting,
  default `12000`.
- `OUTPACE_REQUEST_CHECK_INTERVAL_MS` - request timeout sweep interval, default `1000`; must be
  lower than or equal to `OUTPACE_REQUEST_TIMEOUT_MS`.
- `OUTPACE_MAX_ACTIVE_UPSTREAMS` - active upstream peer cap for one live follower, default `4`;
  max `256`.
- `OUTPACE_MAX_PARALLEL_CONNECT` - peer connect race batch size, default `12`; max `1024`.
- `OUTPACE_MAX_PIECE_ADVANCE` - max pieces scheduled in one forward step, default `256`;
  max `16384`.
- `OUTPACE_MAX_REASM_PIECES_AHEAD` - max pieces accepted ahead of the emit cursor, default `512`;
  must be at least `OUTPACE_MAX_PIECE_ADVANCE`, max `65536`.
- `OUTPACE_HLS_SEGMENT_PACKETS` - hard MPEG-TS packet ceiling per HLS segment and packet-count
  fallback for streams without usable PCR, default `65536` (about 12.3 MB). This is a
  memory-safety bound, not the primary cut mechanism: segments normally cut on a keyframe once
  the target duration elapses, so the ceiling must comfortably hold one target-duration segment
  of a peaky high-bitrate stream (a 2160p HEVC GOP can burst well past its average).
- `OUTPACE_HLS_WINDOW_SEGMENTS` - retained HLS live window size, default `6`.
- `OUTPACE_HLS_SEGMENT_DURATION_MS` - requested PCR-timed HLS segment duration, default `1000`.
- `OUTPACE_MAX_UNCHOKED` - max simultaneously-unchoked peers per served stream (default 8). Wired
  into the inbound serve path via the per-infohash serve coordinator: each stream unchokes up to
  this many interested peers plus one rotating optimistic slot (rotated on a ~10s rechoke tick).
- `OUTPACE_SEED_TTL_SECS` - idle-TTL (seconds, default 300; 0 disables) after which an *ownerless*
  seed-registry entry (one with no live producer lease) is force-evicted by the reaper. A backstop
  only — normal teardown rides the lease drop, and entries held by a live producer or a broadcast
  are never reaped.
- `OUTPACE_MAX_INBOUND` - inbound peer connection limit, default `64`.
- `OUTPACE_ENABLE_SEEDING` - reciprocal upload gate over outbound leech connections
  (answering peers' chunk requests). Self-announce is gated on `OUTPACE_ENABLE_INBOUND`.
- `OUTPACE_ENABLE_INBOUND` (default: on) - inbound peer serving (S2) gate. On by default,
  intentionally matching the Acestream engine's out-of-the-box behavior: a full P2P participant
  that binds its peer port (`OUTPACE_PEER_LISTEN`), accepts inbound peers, seeds, and
  self-announces to trackers + DHT. Only the HTTP API (`OUTPACE_BIND`) stays on localhost by
  default; the exposed surface is the peer port, as with Acestream. Set
  `OUTPACE_ENABLE_INBOUND=0` for a pure-leecher deployment (no inbound listener, no seeder
  self-announce).
- `OUTPACE_ENABLE_PORT_MAPPING` - best-effort UPnP/NAT-PMP mapping for the inbound TCP peer
  listener, default off. Use only when the process owns the host/LAN-facing network address;
  normal bridge-mode containers should use a manual router-to-Docker-host TCP forward.
- `OUTPACE_PORT_MAP_BACKEND` - `auto`, `upnp`, `natpmp`, or `none`; default `auto`.
- `OUTPACE_PORT_MAP_EXTERNAL_PORT` - optional external TCP port requested from the gateway.
- `OUTPACE_EXPERIMENTAL_ACE_COMPAT` - enables legacy compatibility routes.
- `OUTPACE_ACE_PEERS` - comma-separated bootstrap peer list for the live path.
- `OUTPACE_TRACKERS` - comma-separated `scheme://…` (typically `udp://host:port/announce`)
  tracker URLs minted into `outpace broadcast` descriptors and used for broadcast self-announce.
  When set (non-empty after trimming) it fully replaces the built-in public default
  (`udp://t1.torrentstream.org:2710/announce`); unset leaves that default in place. Entries with
  no `scheme://` prefix (or longer than 256 bytes) are dropped with a warning, and the list is
  clamped to 64 entries. Note outpace only **self-announces** to lowercase `udp://` trackers;
  entries with any other scheme are kept in the minted descriptor for other clients but warned
  about, since outpace itself never announces there. Use this to point a broadcast at a
  private/local tracker (announcing to a private-address tracker also requires the non-global
  tracker allowance, tracked separately).
- `OUTPACE_TRACKER_ALLOW_NON_GLOBAL` - set to `1` to allow announcing to trackers that resolve
  to non-globally-routable addresses (private/LAN/loopback/link-local/multicast). Default deny;
  the SSRF guard on tracker destinations stays closed for normal use. Opt in only for controlled
  self-hosted or offline/test swarms on a trusted network. Scoped to tracker traffic.
- `OUTPACE_ALLOW_NON_GLOBAL_TRANSPORT` - set to `1` to allow fetching a transport descriptor
  file from a private/LAN/loopback host. Default deny; this relaxes the SSRF guard on the
  transport-file fetch **specifically** (not peer or tracker traffic), and even when enabled it
  still blocks link-local (e.g. the `169.254.169.254` cloud-metadata endpoint), unspecified,
  multicast, broadcast, and documentation ranges. Opt in only for controlled self-hosted / LAN
  swarms. If that transport file also points at a private *tracker*, announcing to it
  additionally needs `OUTPACE_TRACKER_ALLOW_NON_GLOBAL`.

These two `*_ALLOW_NON_GLOBAL*` knobs enable on the exact string `1` only (unlike the general
boolean gates below, `true` does **not** enable them).

Boolean gates accept exactly `1`, `true`, `0`, or `false`; other values are configuration errors
rather than silently disabling a feature.

The disk cache is **ephemeral**: its directory is cleared when a store is created and
never reloaded across restarts (live piece data goes stale), which also avoids serving
evicted-stale pieces. Disk I/O is currently synchronous.

Disk mode never silently converts `OUTPACE_SEED_STORE_BYTES` into an equal per-stream RAM
allocation. An invalid/unwritable cache root fails daemon startup. If a new per-stream directory
cannot be created later (for example after a permission change), outpace logs an escalating
`disk store creation failure #N` error and keeps playback running with **zero piece retention** for
that stream: no memory cache fallback is allocated, and reciprocal/inbound seeding has no retained
pieces to serve. This favors the operator's RAM bound over transient seeding capacity.

In disk mode each served stream keeps its pieces under
`<OUTPACE_CACHE_DIR>/<infohash_hex>-<generation>` (a process-unique suffix per store instance).
The directory is removed automatically when the stream is torn down (leech consumer disconnects,
broadcast `DELETE`, or process exit), and the whole cache root is wiped on startup, so no
per-stream directories accumulate.

## Project Docs

Start with these durable docs instead of a session resume file:

- `docs/superpowers/specs/2026-06-28-acestream-engine-reimplementation-design.md`
  - original architecture and scope.
- `docs/protocol/wire-protocol.md` - consolidated peer-wire protocol notes.
- `docs/protocol/transport-file.md` - transport container and infohash math.
- `docs/protocol/notes/` - chronological reverse-engineering and live validation
  notes.
- [`docs/testing/interop-swarm.md`](docs/testing/interop-swarm.md) - the `swarmtest`
  engine<->outpace interop harness (on-demand, local-only; needs docker + the
  proprietary engine, so it is NOT run in CI).
- `docs/superpowers/plans/` and `docs/superpowers/specs/` - implementation plans
  and design specs retained as historical project records.

The issue tracker is the canonical backlog:

- #1 live robustness validation;
- #2 PEX advertised-window peer ranking;
- #3 broadcast persistence and ingest-resume continuity;
- #4 inbound seeding lifecycle and policy;
- #5 disk-backed piece cache;
- #6 native CLI/API polish;
- #7 SRT ingest;
- #8 removal of the old resume workflow.

## Operational Notes

- Cloudflare WARP breaks P2P swarm testing by interfering with UDP/inbound paths.
  Keep it off for live swarm validation. The Spain network itself has worked once
  WARP is disabled.
- A known-good live content id for smoke tests is `cid1` in the gitignored
  `acestream-ids.txt` registry (see AGENTS.md); resolve it to the current
  infohash when needed because live infohashes rotate.
- `status=prebuf, peers=0` forever usually means a dead live channel. The old promo
  transport id `685e...6067` is dead and should not be used as proof.
- The official closed engine sandbox, when present, is started with:

```bash
docker compose -f re/sandbox/docker-compose.yml up -d acestream
```

It exposes the engine HTTP API on `127.0.0.1:6878` and is intended for controlled
capture/interop work, not for runtime use by outpace.

## Scope Boundaries

outpace targets public, unencrypted Acestream swarms. Premium/encrypted content,
Android/player apps, and a full clone of the closed engine HTTP API are out of
scope unless a specific issue narrows that work.

## License

outpace is licensed under `AGPL-3.0-or-later`. See `LICENSE`.
