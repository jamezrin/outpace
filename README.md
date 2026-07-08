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

`play` (and the `/ace/getstream` compatibility route) accept four mutually
exclusive selectors, applied in precedence order
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

On the `/ace/getstream` compatibility route a `url=` selector returns a
self-contained playback id, so playback works after a daemon restart without any
server-side session table.

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
  -p 8621:8621/udp \
  -v outpace-data:/var/lib/outpace \
  ghcr.io/jamezrin/outpace:0.1.0
```

The container defaults to `outpace serve`. It sets `OUTPACE_BIND=0.0.0.0:6878`,
`OUTPACE_RTMP_BIND=0.0.0.0:1935`, and `OUTPACE_DATA_DIR=/var/lib/outpace`.

## Cutting a Release

The release version is driven by a git tag named `vX.Y.Z`. Before tagging, update
the Cargo crate versions to match the release.

```bash
git tag v0.1.0
git push origin v0.1.0
```

Pushing the tag runs `.github/workflows/release.yml`, which:

- builds Linux, macOS, and Windows binaries for amd64 and arm64;
- uploads `tar.gz` archives for Unix targets and `zip` archives for Windows;
- generates one `SHA256SUMS` file for all archives;
- creates a GitHub Release with generated release notes;
- publishes `ghcr.io/jamezrin/outpace:<version>` and `ghcr.io/jamezrin/outpace:latest`.

## Runtime Surfaces

Native API:

- `GET /healthz`
- `GET /streams`
- `GET /streams/ace/<id>.ts`
- HLS and playlist routes backed by the native stream session layer
- `PUT /broadcast/<name>`
- `GET /broadcast/<name>`

Legacy Acestream-compatible routes such as `/ace/*` and `/server/api` are
experimental and disabled by default. Enable them only when needed:

```bash
OUTPACE_EXPERIMENTAL_ACE_COMPAT=1 cargo run -p ace-engine --bin outpace -- serve
```

On that compatibility surface, `/ace/stat` fields live under `.response`, and
`acestream://` ids should be passed as `content_id=`, not `infohash=`.
`/ace/getstream` also accepts `url=` (transport-file URL) and `magnet=` selectors,
with the same `content_id` > `infohash` > `url` > `magnet` precedence as the CLI.

## Configuration

Environment variables parsed by the daemon include:

- `OUTPACE_BIND` - HTTP bind address, default `127.0.0.1:6878`.
- `OUTPACE_RTMP_BIND` - RTMP ingest bind address, default `127.0.0.1:1935`.
- `OUTPACE_DATA_DIR` - persistent identity/cache root, default platform data dir
  plus `outpace`.
- `OUTPACE_PEER_LISTEN` - inbound peer listener bind, default `0.0.0.0:8621`.
- `OUTPACE_SEED_STORE_BYTES` - byte budget for retained piece data (sizes both cache backends).
- `OUTPACE_CACHE_TYPE` - where the seed store keeps piece data: `memory` (default) or `disk`.
  `disk` trades RAM for capacity, mirroring Acestream's disk-cache option.
- `OUTPACE_CACHE_DIR` - root dir for disk-mode piece files (one subdir per served stream; see
  below), default `<data_dir>/cache`. Only used when `OUTPACE_CACHE_TYPE=disk`.
- `OUTPACE_PREFETCH_PIECES` - pieces behind the live edge to start at, default `8`.
- `OUTPACE_SESSION_BUFFER` - per-client fan-out channel depth, default `256`;
  must be at least `1`.
- `OUTPACE_REQUEST_TIMEOUT_MS` - per-piece request timeout before re-requesting or skipping an
  evicted gap, default `4000`; must be lower than `OUTPACE_STALE_UPSTREAM_TIMEOUT_MS`.
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
- `OUTPACE_HLS_SEGMENT_PACKETS` - MPEG-TS packets per HLS segment, default `256`.
- `OUTPACE_HLS_WINDOW_SEGMENTS` - retained HLS live window size, default `6`.
- `OUTPACE_HLS_SEGMENT_DURATION_MS` - advertised HLS segment duration, default `1000`.
- `OUTPACE_MAX_UNCHOKED` - max simultaneously-unchoked peers per served stream (default 8). Wired
  into the inbound serve path via the per-infohash serve coordinator: each stream unchokes up to
  this many interested peers plus one rotating optimistic slot (rotated on a ~10s rechoke tick).
- `OUTPACE_SEED_TTL_SECS` - idle-TTL (seconds, default 300; 0 disables) after which an *ownerless*
  seed-registry entry (one with no live producer lease) is force-evicted by the reaper. A backstop
  only — normal teardown rides the lease drop, and entries held by a live producer or a broadcast
  are never reaped.
- `OUTPACE_MAX_INBOUND` - inbound peer connection limit.
- `OUTPACE_ENABLE_SEEDING` - reciprocal upload gate over outbound leech connections
  (answering peers' chunk requests). Self-announce is gated on `OUTPACE_ENABLE_INBOUND`.
- `OUTPACE_ENABLE_INBOUND` (default: on) - inbound peer serving (S2) gate. On by default,
  intentionally matching the Acestream engine's out-of-the-box behavior: a full P2P participant
  that binds its peer port (`OUTPACE_PEER_LISTEN`), accepts inbound peers, seeds, and
  self-announces to trackers + DHT. Only the HTTP API (`OUTPACE_BIND`) stays on localhost by
  default; the exposed surface is the peer port, as with Acestream. Set
  `OUTPACE_ENABLE_INBOUND=0` for a pure-leecher deployment (no inbound listener, no seeder
  self-announce).
- `OUTPACE_EXPERIMENTAL_ACE_COMPAT` - enables legacy compatibility routes.
- `OUTPACE_ACE_PEERS` - comma-separated bootstrap peer list for the live path.

The disk cache is **ephemeral**: its directory is cleared when a store is created and
never reloaded across restarts (live piece data goes stale), which also avoids serving
evicted-stale pieces. Disk I/O is currently synchronous; a write failure logs and falls
back to memory rather than crashing.

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
