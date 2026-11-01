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
  HTTP raw ingest, managed HTTP(S) MPEG-TS sources, RTMP ingest, content-id metadata serving, and the three-command
  CLI are implemented;
- live-network tests remain ignored by default because they need the public swarm
  and a suitable network.

The single production Rust live entrypoint is
`ace_swarm::live_session::follow_live(LiveSessionInputs)`. It emits authenticated
media as `LiveOutput { bytes, discontinuity }` and reconnects until the consumer
drops. Pre-1.0 Rust compatibility break (#193): `driver::download_from_peer`,
`driver::DownloadParams`, `live::LiveSession`, `live::LiveConfig`, and
`LiveSession::run` have been removed; Rust callers must migrate to `follow_live`.

Run the normal local gate with:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

CI runs these same three commands on every pull request and on every push to `main`
(`.github/workflows/ci.yml`, reported as the `Rust lint` and `Rust test` checks). The
ignored live-network tests never run in CI.

## Quick Start

Run the daemon:

```bash
cargo run -p ace-engine --bin outpace -- serve
```

With no subcommand, `outpace` defaults to `serve`.

For native live playback, use `/streams/ace/cid:<content-id>` (or a bare content
id), adding `.m3u8` for HLS. Both forms share status and DELETE session keys.
Infohash playback uses `/streams/ace/ih:<infohash>` and resolves a verified descriptor
through the catalog when needed. **Migration:** bare hex ids on native live routes now mean content ids,
matching compat `id=`; update older infohash URLs to include `ih:`. See the
[native API](docs/native-api.md) for selector and session details.

Play a known Acestream link to stdout:

```bash
cargo run -p ace-engine --bin outpace -- play acestream://<content-id> > live.ts
```

`play` accepts four mutually exclusive selectors, applied in precedence order
`content_id` > `infohash` > `url` > `magnet`:

- `acestream://<content_id>` or `acestream:?content_id=<40-hex>`;
- `acestream:?infohash=<40-hex>` - resolves the live descriptor through the signed catalog;
- a transport-file URL - either passed bare (`play https://host/x.acelive?a=1&b=2`,
  which handles a URL carrying its own `&`-joined query) or as
  `acestream:?url=<https://…>` (in the `acestream:?` form the URL's own query must
  be percent-encoded, e.g. `%26` for `&`). The descriptor is fetched over http/https
  with SSRF protection (private, loopback, and link-local hosts are blocked), a 1 MiB
  size cap, disabled redirects, and a request timeout; unsafe, oversized, or
  non-transport responses fail closed;
- `magnet:?xt=urn:btih:<40-hex-or-32-base32>` - a BitTorrent v1 magnet, reduced to
  its infohash (v2 `urn:btmh:` magnets are rejected), then resolved through the signed catalog.

The `/ace/getstream` compatibility route accepts the same selectors plus legacy
`id=<40-hex>`, which is an alias for `content_id` (not `infohash`). Its precedence is
`content_id` > `infohash` > `id` > `url` > `magnet`. A `url=` selector uses a self-contained
playback id, so playback works after a daemon restart without any server-side alias table.

### Content-id resolution

Live and VOD content-id inputs use the catalog by default. A catalog failure returns an
error rather than automatically querying BEP-9 metadata peers. The daemon can also resolve
its own broadcasts through its trusted local broadcast registry without a catalog lookup.

`OUTPACE_BEP9_CONTENT_ID_FALLBACK=1` explicitly enables the raw transport-hash convention
used by outpace broadcasts, after catalog failure. This applies to both `serve` and standalone
`play` (including `--vod`). Peer metadata is accepted only when the SHA-1 of the complete
wrapped transport bytes equals the requested identifier. This check does not establish the
binding of a public catalog content id; that derivation remains unresolved. See
[the content-id policy](docs/protocol/notes/63-bep9-content-id-policy.md).

### Infohash inputs

Catalog replies require the engine-compatible response signature, matching request nonce and
transport checksum. The response key is embedded in clients, so this does not provide
server-only authentication over plain HTTP; see [the protocol evidence and trust limits](docs/protocol/notes/62-catalog-response-authentication.md).

A bare infohash identifies a swarm but carries no piece geometry or source key. For
native `ih:`, compat `infohash=`/`magnet=`, and CLI infohash/magnet inputs, outpace
first checks its own broadcasts and verified descriptor index, then queries the
signed catalog directly by infohash. Catalog response authentication, request
nonce and checksum checks precede decoding, and the descriptor's computed swarm
infohash must match the requested value before discovery or indexing.

The verified descriptor supplies geometry, pubkey and trackers. Successful lookups
populate the bounded in-process index; failures are suppressed for five seconds
in a separate cache capped at 128 entries. A verified local descriptor takes
precedence over a cached failure. Unavailable or mismatched descriptors fail
closed without guessing geometry or falling back to BEP-9. Native playback returns
`422` with a reason; compat JSON URL creation permits cold inputs, with resolution
errors reported when playback is opened. CLI playback exits with an error.

A BEP-9 content-id result or caller-supplied transport URL does not populate the
catalog-provenance index. Transport-URL streams reopen by their `turl-` id.

### Live HLS audio

Live HLS uses its existing native selection by default: the first ADTS AAC
track wins; without ADTS AAC, audio passes through unchanged. Direct/raw TS and
VOD retain their existing behavior.

Set `OUTPACE_HLS_AUDIO_NORMALIZE=true` (default `false`; accepts `true`/`false`
or `1`/`0`) to convert selected AC3 or MPEG Layer II/III audio into AAC-LC stereo,
48 kHz, 128 kbit/s. `OUTPACE_FFMPEG_PATH` selects a direct FFmpeg executable
(default `ffmpeg`, resolved through PATH); it must be nonempty when enabled.
These are the only normalization options. Disabled mode never looks up or probes
FFmpeg. Native ADTS AAC and video-only streams start no conversion process.

Enabled mode requires unencrypted 188-byte TS, one program, one MPEG-2/H.264/HEVC
video stream, valid PCR and current CRC-valid single-packet PAT/PMT sections.
Audio-only, multiple video/programs, fragmented PSI, unidentified private streams
and unsupported selected codecs fail closed. MPEG PMT types do not identify a
layer: bounded elementary-stream frame checks admit Layers II/III and reject
Layer I, malformed, free-format or unaligned MPEG framing. LATM, EAC3 and DTS
cannot be selected. The first ADTS AAC wins, otherwise the first recognized
MPEG audio or AC3 wins; this does not search for a different track after rejection.
Enabled strictness can reject inputs that disabled mode historically passes through.

Conversion copies video without encoding it; player compatibility (including
Safari) is not guaranteed. Stereo conversion can discard surround channels and
lossy re-encoding reduces quality. At most four conversion children run globally;
this is a process bound, not an RSS/CPU capacity promise. Recovery retains completed
segments but playlists wait for fresh-generation readiness. A process, codec,
resource or output failure is terminal for that session: HTTP 503 with
`Retry-After: 1` does not restart a failed worker. Stop/idle teardown and a fresh
session are required. Authorization and expiry still take precedence.

Operators supply and maintain FFmpeg with MPEG-TS mux/demux, native AAC encoding,
MP2/MP3/AC3 decoding and pipe support. Startup checks capabilities and performs a
bounded AC3 conversion before opening listeners. Enabled containers need an
operator-built image containing this binary; the base Docker/compose setup does
not install it. Wrappers launching descendants are unsupported. Embedders must
await manager shutdown before destroying the runtime so children and pumps are
reaped; Drop only signals cancellation.

Synthetic codec acceptance is optional locally: run
`OUTPACE_TEST_HLS_AUDIO_FFMPEG=1 cargo test -p ace-engine --test hls_audio_ffmpeg -- --nocapture`.
Without opt-in it prints an explicit skip. Opt-in requires FFmpeg/ffprobe,
synthetic video/sine inputs and AC3/MP2/MP3/AAC encoders; a missing prerequisite
fails the suite. The designated CI job always opts in. No public media is used.

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
- an `acestream://<content_id>` link using the raw wrapped transport-file hash.

The originating daemon resolves that link through its trusted local broadcast registry.
A different receiving daemon or standalone `outpace play` needs
`OUTPACE_BEP9_CONTENT_ID_FALLBACK=1` to resolve it through BEP-9 `ut_metadata`; the default
catalog policy does not automatically resolve these raw-hash links. Alternatively, pass the
broadcast transport URL (`http://<host>:<http-port>/broadcast/<name>`) to `outpace play`
or use the `url=` selector. The receiver's transport-URL access policy still applies.

Set one immutable profile for every producer path (raw PUT, RTMP, managed pull and CLI):

```bash
OUTPACE_BROADCAST_OPTIONS='{"synthetic-origin":{"title":"Synthetic broadcast","bitrate_bytes_per_sec":500000,"piece_length":131072,"quality":"HD","categories":[]}}' \
  cargo run -p ace-engine --bin outpace -- broadcast synthetic-origin
```

An unused profile creates nothing. Fresh omissions retain the legacy defaults; resumed
identities retain raw transport/key bytes and match only explicit fields. Changing an
explicit option can fail with `broadcast_options_conflict`; use a new name or explicitly
DELETE/recreate, losing identity/cursor continuity. Each default/profile/stored piece must fit
one full wire piece in the store budget, including disk and unused profiles. See the
[complete profile schema and startup/resume rules](docs/native-api.md#broadcast-startup-profiles).

Attach an engine-owned HTTP(S) MPEG-TS source with `PUT /broadcast/<name>/source`, or
set startup declarations in `OUTPACE_BROADCAST_SOURCES`:

```bash
OUTPACE_BROADCAST_SOURCES='{"demo-source":{"url":"https://source.invalid/live.ts"}}' \
  cargo run -p ace-engine --bin outpace -- serve
```

The URL above is synthetic; replace it with an authorized public, unencrypted MPEG-TS origin.
Acceptance reserves the signing identity and schedules a worker; check
`GET /broadcast/demo-source/source` for upstream health. A worker reconnects on finite EOF
and discards incomplete tails. It keeps exclusive production through backoff and failure;
DELETE the `/source` attachment before replacing it. Detach retains identity and cached data;
DELETE the broadcast itself to remove identity. API attachments last for this process only.
Current environment declarations reattach on restart, including declarations detached or
deleted during the previous process. Remove a declaration to keep it absent after restart.

Source URLs/query tokens stay out of status, logs and persisted broadcast records. Protect
the deployment environment and avoid dumping configuration. Default source policy denies
private/loopback addresses; only exact `OUTPACE_ALLOW_NON_GLOBAL_BROADCAST_SOURCE=1` adds
the documented LAN subset. This switch is independent of transport/tracker opt-ins and
never admits link-local metadata endpoints. Sources ignore proxies and reject redirects.
See [source API/configuration](docs/native-api.md#managed-http-mpeg-ts-sources) and
[storage, restart and deployment guidance](docs/deployment.md#managed-source-declarations).

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

Pushes to `main` run `.github/workflows/edge.yml`, which publishes
`ghcr.io/jamezrin/outpace:edge` and `ghcr.io/jamezrin/outpace:sha-<commit>` for
`linux/amd64` only. `edge` moves with `main`. Keep production pinned to a
release tag.

## Runtime Surfaces

The primary integration contract is the native CLI and HTTP API. See the
route table, response shapes, and VLC/Jellyfin/dispatcharr examples in
[`docs/native-api.md`](docs/native-api.md).

Legacy Acestream-compatible routes (`/ace/*`, `/server/api`, and
`/webui/api/service`) provide a supported subset, disabled by default. Enable them
when needed with `OUTPACE_ACE_COMPAT=1` on the serving daemon, or explicitly disable
them with `OUTPACE_ACE_COMPAT=0`. Restart the daemon to apply a change; for Compose,
recreate the service with `docker compose up -d --wait`.
The canonical [Acestream HTTP support contract](docs/protocol/compat-matrix.md)
covers direct VLC and JSON-session examples, supported routes/selectors, errors,
lease ownership, live HLS and intentional gaps. The
[pinned client inventory](docs/protocol/ace-client-inventory.md) supplies supporting
client evidence; it does not claim full engine or application parity.

## Configuration

Environment variables parsed by the daemon include:

- `OUTPACE_BEP9_CONTENT_ID_FALLBACK` - raw transport-hash BEP-9 fallback for content-id
  resolution after catalog failure, default off. Set to `1` on the receiving daemon or
  standalone `play` process for outpace broadcast Ace links from another daemon. Applies to
  live and VOD; peer results do not populate the catalog-provenance infohash index.
- `OUTPACE_BIND` - HTTP bind address, default `127.0.0.1:6878`.
- `OUTPACE_RTMP_BIND` - RTMP ingest bind address, default `127.0.0.1:1935`.
- `OUTPACE_BROADCAST_OPTIONS` - strict named startup profile JSON, default `{}`; at most
  64 names and 262144 UTF-8 bytes. Optional title, bitrate in bytes/second, piece length,
  quality and ordered categories; invalid values fail with `invalid_broadcast_options`.
  Defaults: path name, `8375`, `65536`, `auto`, `["other"]`. No hot reload or profile-only mint.
  See [bounds and immutable resume](docs/native-api.md#broadcast-startup-profiles).
- `OUTPACE_BROADCAST_SOURCES` - startup source JSON map, default `{}`; at most 64 names,
  256 KiB UTF-8, URLs up to 8,192 bytes. Duplicate/unknown fields fail startup. Source timing
  defaults: connect 5s, read 15s, reconnect initial 1s, maximum 30s; see the native API limits.
- `OUTPACE_ALLOW_NON_GLOBAL_BROADCAST_SOURCE` - independent operator-only source address
  opt-in; only exact `1` enables it, default off. It cannot be set in an API request.
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
  below), default `<data_dir>/cache`. Only used when `OUTPACE_CACHE_TYPE=disk`. Startup wipes
  this directory only when it is empty or already contains a regular `.outpace-cache` sentinel
  file (written automatically on first successful disk-cache start). A symlink is not accepted
  as a sentinel. An unmarked non-empty path is refused rather than deleted.
- `OUTPACE_PREFETCH_PIECES` - optional exact number of pieces behind the live edge to start at.
  When unset, outpace derives the depth from the startup target and advertised bitrate, falling
  back to `32` pieces when the bitrate is unavailable.
- `OUTPACE_SESSION_BUFFER` - per-client fan-out channel depth, default `256`;
  must be at least `1`.
- `OUTPACE_PREBUFFER_MS` - target server-resident live startup queue duration, default `30000`.
  The server intentionally waits before sending the first media so clients begin with more
  tolerance for swarm jitter. `0` disables startup prebuffering and preserves the previous
  immediate-release behavior.
- `OUTPACE_PREBUFFER_BYTES` - hard byte ceiling for the startup queue, default `134217728`
  (128 MiB). Reaching it releases the available media early instead of exceeding the reservoir
  budget. Queue metadata and the rest of the daemon add memory overhead beyond this payload cap.
- `OUTPACE_PREBUFFER_TIMEOUT_MS` - deadline after the first clean media byte for reaching the
  target duration, default `15000`. On expiry, outpace degrades gracefully by releasing the
  available queue; it does not turn a short advertised live window into a playback failure.
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
- `OUTPACE_HLS_WINDOW_SEGMENTS` - retained HLS live window size, default `8`.
- `OUTPACE_HLS_SEGMENT_DURATION_MS` - requested PCR-timed HLS segment duration, default `5000`.
- `OUTPACE_HLS_STARTUP_SEGMENTS` - completed segments retained before a new HLS playlist request
  becomes ready, default `6`. `0` is accepted as a compatibility setting and behaves as `1`.
- `OUTPACE_HLS_STARTUP_TIMEOUT_MS` - maximum HLS playlist startup wait, default `45000`; must be
  at least `OUTPACE_HLS_SEGMENT_DURATION_MS`. If the startup segment count is not reached, the
  request returns a retryable HTTP `503` at the deadline.
- `OUTPACE_MAX_UNCHOKED` - stable desired unchokes per exact served key, default `8` (U).
  For I interested peers, the coordinator selects `min(I, U + 1)` desired states in
  interest-entry order, with one rotating tail peer on nominal 10-second ticks. `0` still
  selects one optimistic peer. Watch updates and pending writes make this a desired selection,
  not an instantaneous wire-visible bound. Metadata replies bypass Interested/Unchoke.
- `OUTPACE_SEED_TTL_SECS` - idle-TTL (seconds, default 300; 0 disables) after which an *ownerless*
  seed-registry entry (one with no live producer lease) is force-evicted by the reaper. A backstop
  only — normal teardown rides the lease drop, and entries held by a live producer or a broadcast
  are never reaped. This TTL limits registry lifetime, not connection lifetime.
- `OUTPACE_MAX_INBOUND` - retained inbound connection limit, default `64` (G); `0` acts as `1`.
  Unidentified/incomplete handshakes consume this capacity too. Excess accepted sockets close
  immediately without queuing behind retained connections.
- `OUTPACE_MAX_INBOUND_PER_STREAM` - admitted connections per canonical stream, default `16`
  (N), serialized in Config as `max_inbound_per_stream`. Broadcast media and metadata aliases
  share this allowance, including idle, uninterested, choked and metadata-only connections.
  At defaults the 17th combined admission closes before a handshake response. `0` restores
  global-only admission and preserves U; positive N greater than G is valid and redundant for
  occupancy. Empty, negative, malformed, overflow and non-Unicode environment values fail
  startup; absence retains 16. Changes require restart/recreation. For rollback set
  `OUTPACE_MAX_INBOUND_PER_STREAM=0`; Compose requires an explicit service environment override,
  shown in [deployment.md](docs/deployment.md), rather than `.env` interpolation alone.
- `OUTPACE_ENABLE_SEEDING` - reciprocal upload gate over outbound leech connections
  (answering peers' chunk requests). Self-announce is gated on `OUTPACE_ENABLE_INBOUND`.
  Outbound reciprocal serving is outside the inbound admission/coordinator budgets.
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
- `OUTPACE_ACE_COMPAT` - enables the supported Ace HTTP subset, default off. Accepts
  exactly `1`, `true`, `0`, or `false`. A present value takes precedence over the legacy
  alias, including `0`/`false`; an empty, malformed, or non-Unicode selected value fails
  startup. Set `0` to explicitly disable compatibility routes.
- `OUTPACE_EXPERIMENTAL_ACE_COMPAT` - deprecated fallback, parsed only when
  `OUTPACE_ACE_COMPAT` is unset, with the same strict boolean rules. Rename it to
  `OUTPACE_ACE_COMPAT` when migrating; an unused invalid alias is ignored.
- `OUTPACE_ACE_PEERS` - comma-separated bootstrap peer list for the live path.
- `OUTPACE_TRACKERS` - comma-separated `scheme://…` (typically `udp://host:port/announce`)
  tracker URLs minted into fresh broadcast descriptors for every producer path. Self-announce
  uses that stored list for both keys on resume; process changes affect future identities only,
  and empty stored lists remain empty.
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

Inbound admission grants no bandwidth/equal-share or class reservation guarantee; metadata-only
clients can fill a stream's allowance. DELETE removes registry/producer ownership, while existing
connections retain data and coordinator Arcs; their old coordinators stop receiving registry
rotations. Aborting only the listener leaves detached handlers running. The serve loop's 250 ms
polling can cancel reads and their inner timeouts, so those per-operation timeouts do not establish
a whole-session idle deadline. See [shared seeding](docs/protocol/shared-seeding.md) for the
admission, coordination and lifetime boundaries.

The disk cache is **ephemeral**: a dedicated cache directory is cleared when a store is created and
never reloaded across restarts (live piece data goes stale), which also avoids serving
evicted-stale pieces. Disk I/O is currently synchronous. The first disk-cache start writes a
`.outpace-cache` sentinel so later wipes can distinguish a dedicated cache from an accidental
operator path. An existing non-empty cache without that regular sentinel file fails startup
instead of being deleted; use an empty directory or create the file only after verifying that
all existing contents may be deleted.

Disk mode never silently converts `OUTPACE_SEED_STORE_BYTES` into an equal per-stream RAM
allocation. An invalid/unwritable cache root fails daemon startup. If a new per-stream directory
cannot be created later (for example after a permission change), outpace logs an escalating
`disk store creation failure #N` error and keeps playback running with **zero piece retention** for
that stream: no memory cache fallback is allocated, and reciprocal/inbound seeding has no retained
pieces to serve. This favors the operator's RAM bound over transient seeding capacity.

In disk mode each served stream keeps its pieces under
`<OUTPACE_CACHE_DIR>/<infohash_hex>-<generation>` (a process-unique suffix per store instance).
The directory is removed automatically when the stream is torn down (leech consumer disconnects,
broadcast `DELETE`, or process exit), and a dedicated cache root is wiped on startup only if it
is empty or contains `.outpace-cache`, so no per-stream directories accumulate.

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
- [`docs/deployment-lxc-warp.md`](docs/deployment-lxc-warp.md) - LXC guest behind Cloudflare
  WARP, `edge` auto-deploy, and the inbound-peer workaround (scripts in `tools/deploy/lxc/`).
- `docs/superpowers/plans/` and `docs/superpowers/specs/` - implementation plans
  and design specs retained as historical project records.

### Roadmap

GitHub milestones track release scope and remaining work; coordination epics link
related issues. Closed milestone history:

- [v0.3.0](https://github.com/jamezrin/outpace/milestone/1).
- [v0.4.0](https://github.com/jamezrin/outpace/milestone/2) — architecture realignment
  ([#189](https://github.com/jamezrin/outpace/issues/189)).
- [v0.6.0](https://github.com/jamezrin/outpace/milestone/4) — media and VOD
  ([#199](https://github.com/jamezrin/outpace/issues/199)).
- [v0.7.0](https://github.com/jamezrin/outpace/milestone/5) — supported Ace HTTP compatibility
  ([#201](https://github.com/jamezrin/outpace/issues/201)).
- [v0.8.0](https://github.com/jamezrin/outpace/milestone/6) — broadcast and seeding
  ([#205](https://github.com/jamezrin/outpace/issues/205)).

Active work and next steps:

- [v0.9.0](https://github.com/jamezrin/outpace/milestone/3) — live gateway hardening
  ([#195](https://github.com/jamezrin/outpace/issues/195)). The
  [release is published](https://github.com/jamezrin/outpace/releases/tag/v0.9.0),
  but the milestone remains open for follow-up work.
- [v1.0.0](https://github.com/jamezrin/outpace/milestone/7) — production replacement
  gate ([#208](https://github.com/jamezrin/outpace/issues/208)); the milestone and
  gate remain open.
- [Post-1.0](https://github.com/jamezrin/outpace/milestone/8) — deferred work beyond
  the v1.0 scope.

See [Scope Boundaries](#scope-boundaries) for the supported target and
[#212](https://github.com/jamezrin/outpace/issues/212) for the permanent
non-goals and compatibility scope contract.

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

Provider admission is unlimited by default. Set `OUTPACE_MAX_SESSIONS=8` for
an opt-in daemon-wide cap; unset or `0` keeps it unlimited. Negative, malformed
or overflowing values fail startup. Pending live opens and VOD resolutions count
alongside retained resources. Direct TS and HLS viewers of one live source share
one slot; cached VOD content and all of its ranges share one slot. Existing
same-key viewers can join at capacity. Metadata-only negotiation can succeed
before later playback admission fails.

New resources at capacity receive HTTP 503 with `Retry-After: 1`; compatibility
JSON start errors retain the response/error envelope. Native stream status and
service `get_status` expose numeric `occupied_sessions` and `max_sessions`
(`null` for unlimited), without changing `active_sessions`. Occupancy includes
pending work and retained owners after stop or cache expiry. Pump abort is
asynchronous: reuse is possible only after actual source destruction, and VOD
ranges retain their content owner until the last range is dropped. This limits
provider ownership, not RSS or exact socket counts during asynchronous cleanup.
There is no separate aggregate seed-store byte budget. CLI `outpace play` is
unaffected.
