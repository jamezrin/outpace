# Configuration

Outpace is configured entirely through `OUTPACE_*` environment variables. There is no config
file and no command-line flag for daemon settings. This page lists every setting the daemon
reads.

## How settings are applied

- **Precedence:** your environment wins over the container image's defaults, and those win over
  the built-in defaults.
- **Read once at startup.** Nothing reloads while the daemon runs. Restart it after any change.
  With Docker Compose, recreate the container (`docker compose up -d --wait`); a plain
  `docker compose restart` keeps the old environment.
- **Strict validation.** A malformed value or an impossible combination stops the daemon before
  it opens any listener, with a message naming the setting. A typo never silently turns a
  feature off.
- **Booleans** accept exactly `1`, `true`, `0` or `false`. Anything else, including an empty
  value, is an error.
- **"Exact `1`" switches.** Three security-sensitive switches
  (`OUTPACE_ALLOW_NON_GLOBAL_BROADCAST_SOURCE`, `OUTPACE_ALLOW_NON_GLOBAL_TRANSPORT`,
  `OUTPACE_TRACKER_ALLOW_NON_GLOBAL`) turn on only for the string `1`. Every other value,
  including `true`, leaves them off.
- **Units** are in the setting name: `_MS` is milliseconds, `_SECS`/`_SECONDS` is seconds and
  `_BYTES` is bytes. Byte values are plain integers: write `134217728`, not `128MiB`.
- **Addresses** are numeric socket addresses: `127.0.0.1:6878`, `0.0.0.0:8621`,
  `[::1]:6878`. Hostnames aren't accepted.

### Exposed defaults

These settings decide what the daemon exposes to the network. Change them deliberately.

| Setting | Default | Exposure |
| --- | --- | --- |
| `OUTPACE_BIND` | `127.0.0.1:6878` (container: `0.0.0.0:6878`) | HTTP API and playback. No authentication. |
| `OUTPACE_RTMP_BIND` | `127.0.0.1:1935` (container: `0.0.0.0:1935`) | RTMP broadcast ingest. Always listening. |
| `OUTPACE_SRT_BIND` | unset (off) | Encrypted SRT ingest on UDP, in SRT-enabled builds only. |
| `OUTPACE_PEER_LISTEN` | `0.0.0.0:8621` | Inbound peer port on **all interfaces**. Used when inbound serving is on. |
| `OUTPACE_ENABLE_INBOUND` | `true` | Accept inbound peers and announce yourself to trackers and the DHT. |
| `OUTPACE_ENABLE_SEEDING` | `true` | Upload to peers you connect to. |
| `OUTPACE_ENABLE_PORT_MAPPING` | `false` | Ask your router (UPnP/NAT-PMP) to open the peer port. |
| `OUTPACE_DATA_DIR` | platform data dir + `outpace` | Node identity and broadcast signing keys. |
| `OUTPACE_CACHE_DIR` | `<data dir>/cache` | Disk cache. **Wiped at startup** when disk caching is on. |

The image binds HTTP and RTMP to `0.0.0.0` *inside the container* so that published ports work.
What reaches your network is decided by the `-p`/`ports:` mapping. See
[Deployment](Deployment#exposing-the-api-safely).

### Availability

The **Avail.** column shows where each setting is available:

- **all**: v0.9.0 and later.
- **1.0.0**: v1.0.0 and later, also in `edge` and builds from `main`. A v0.9.0
  binary ignores these variables. See [Releases and Versions](Releases-and-Versions) for publication status.
- **1.1.0**: v1.1.0 and later, also in `edge` and builds from `main`. Earlier releases ignore
  these variables.
- **srt build**: v1.0.0 and later, only in binaries built with the `srt` feature.
- **edge**: current `main` and the `edge` image. Not in v1.1.0.

## Network and listeners

| Setting | Default | Accepted values | Avail. | Notes |
| --- | --- | --- | --- | --- |
| `OUTPACE_BIND` | `127.0.0.1:6878` | socket address | all | HTTP API, playback, HLS and ingest. Set `0.0.0.0:6878` to reach it from other machines, and protect it with a firewall or proxy first. |
| `OUTPACE_RTMP_BIND` | `127.0.0.1:1935` | socket address | all | RTMP ingest for [broadcasting](Broadcasting). Use port `0` for a random local port if you never use RTMP. |
| `OUTPACE_PEER_LISTEN` | `0.0.0.0:8621` | socket address | all | TCP peer listener. Its port is also the port Outpace announces, so the router's external port must match. See [NAT and Port Mapping](NAT-and-Port-Mapping). |

SRT listener settings are under [Broadcasting and ingest](#broadcasting-and-ingest).

## Playback and startup buffering

These control how long Outpace waits before sending the first media, and how it recovers when
peers stall. The defaults favour smooth playback over a fast start.

| Setting | Default | Accepted values | Avail. | Notes |
| --- | --- | --- | --- | --- |
| `OUTPACE_PREBUFFER_MS` | `30000` | ms, `0` disables | all | Target amount of media to queue on the server before a live stream is released to viewers (direct TS and HLS alike). Measured on the media clock when possible, otherwise from the advertised bitrate. Lower it (for example `5000`) for faster starts; `0` sends media immediately. When the peers hold less live history than the target, playback starts as soon as everything up to the live edge has arrived, with that shorter lead. |
| `OUTPACE_PREBUFFER_BYTES` | `134217728` (128 MiB) | bytes, at least `188` | all | Hard cap on the startup queue. Reaching it releases playback early. |
| `OUTPACE_PREBUFFER_TIMEOUT_MS` | `15000` | ms, at least `1` when prebuffering is on | all | Longest wait, counted from the **first media byte**, before releasing whatever is queued. A short live window degrades to a shorter buffer, not a failure. Since v1.0.0 it is absolute: source gaps no longer restart it. |
| `OUTPACE_PREFETCH_PIECES` | unset (automatic) | pieces | all | How far behind the live edge to start. When unset it is derived from the prebuffer target and the advertised bitrate, or 32 pieces if the bitrate is unknown. Leave unset unless you are debugging. |
| `OUTPACE_SESSION_BUFFER` | `256` | messages, at least `1` | all | Per-viewer fan-out queue depth. Lower it (for example `64`) on small devices. A viewer that falls this far behind skips ahead and sees a discontinuity. |

### Live recovery (advanced)

Leave these at their defaults unless you are diagnosing stalls with a maintainer. The defaults
have been tuned against real swarms.

| Setting | Default | Accepted values | Avail. | Notes |
| --- | --- | --- | --- | --- |
| `OUTPACE_REQUEST_TIMEOUT_MS` | `1500` | ms, at least 1, below the stale timeout | all | How long to wait for one piece before asking another peer. Raising it turns a short hiccup into a visible gap. |
| `OUTPACE_STALE_UPSTREAM_TIMEOUT_MS` | `12000` | ms, at least 1 | all | No progress from any peer for this long rebuilds the peer pool. Don't lower it much: discovery needs this margin. |
| `OUTPACE_REQUEST_CHECK_INTERVAL_MS` | `1000` | ms, at least 1, no more than the request timeout | all | How often outstanding requests are checked. |
| `OUTPACE_MAX_ACTIVE_UPSTREAMS` | `4` | 1–256 | all | Peers one stream downloads from at the same time. |
| `OUTPACE_MAX_PARALLEL_CONNECT` | `12` | 1–1024 | all | Peers dialled at once while searching for upstreams. |
| `OUTPACE_MAX_PIECE_ADVANCE` | `256` | 1–16384 | all | Pieces scheduled in one forward step. |
| `OUTPACE_MAX_REASM_PIECES_AHEAD` | `512` | at least `OUTPACE_MAX_PIECE_ADVANCE`, at most 65536 | all | Pieces accepted ahead of the playback cursor. |

## HLS

See [HLS Output](HLS-Output) for how these interact.

| Setting | Default | Accepted values | Avail. | Notes |
| --- | --- | --- | --- | --- |
| `OUTPACE_HLS_SEGMENT_DURATION_MS` | `5000` | ms, at least 1 | all | Target segment length. Segments cut on the next keyframe after this time. |
| `OUTPACE_HLS_WINDOW_SEGMENTS` | `8` | at least 1 | all | Segments kept in the live playlist. |
| `OUTPACE_HLS_STARTUP_SEGMENTS` | `6` | 0 to the window size (`0` acts as `1`) | all | Completed segments needed before the first playlist is returned. Lower it for faster HLS starts. |
| `OUTPACE_HLS_STARTUP_TIMEOUT_MS` | `45000` | ms, at least the segment duration | all | How long a playlist request waits for startup before returning `503`. Also the startup deadline for audio normalisation. |
| `OUTPACE_HLS_SEGMENT_PACKETS` | `65536` (about 12.3 MB) | at least 3 | all | Memory safety ceiling per segment, in 188-byte packets. Also the cut size for streams without usable timing. Raise it only for very high bitrates. |

## Audio normalisation

Off by default. Turning either switch on requires FFmpeg; see
[HLS Output](HLS-Output#audio-normalisation).

| Setting | Default | Accepted values | Avail. | Notes |
| --- | --- | --- | --- | --- |
| `OUTPACE_HLS_AUDIO_NORMALIZE` | `false` | boolean | all | Convert AC3 or MPEG audio to AAC for live HLS only. |
| `OUTPACE_LIVE_AUDIO_NORMALIZE` | `false` | boolean | 1.0.0 | Convert once per source, before TS, HLS and support fan-out. Takes over from the HLS-only switch when both are on. |
| `OUTPACE_FFMPEG_PATH` | `ffmpeg` | path or name on `PATH` | all | The FFmpeg executable. Resolved once at startup and only used when a switch above is on. |

## Timeshift

Off by default. See [Timeshift](Timeshift).

| Setting | Default | Accepted values | Avail. | Notes |
| --- | --- | --- | --- | --- |
| `OUTPACE_HISTORY_SECONDS` | `0` (off) | 0–3600 seconds | 1.0.0 | How far back viewers can rewind. Any nonzero value turns timeshift on. |
| `OUTPACE_HISTORY_BYTES` | `134217728` (128 MiB) | bytes, 188 up to the platform pool limit | 1.0.0 | Memory budget per source. At 8 Mbit/s, 128 MiB is about two minutes. |
| `OUTPACE_HISTORY_TOTAL_BYTES` | `268435456` (256 MiB) | bytes, at least `OUTPACE_HISTORY_BYTES` | 1.0.0 | Memory budget shared by all sources and managed-origin viewers. |

## Managed HLS origins

| Setting | Default | Accepted values | Avail. | Notes |
| --- | --- | --- | --- | --- |
| `OUTPACE_MANAGED_HLS` | `false` | boolean | 1.0.0 | Serve descriptors that point at an HTTP(S) HLS origin through a local relay instead of redirecting the player to the origin. The daemon then fetches public origin URLs itself. See [Managed HLS Origins](Managed-HLS-Origins). |

## Broadcasting and ingest

See [Broadcasting](Broadcasting).

| Setting | Default | Accepted values | Avail. | Notes |
| --- | --- | --- | --- | --- |
| `OUTPACE_BROADCAST_OPTIONS` | `{}` | JSON object, at most 64 names and 262144 bytes | all | Per-broadcast title, bitrate, piece size, quality and categories, applied when a broadcast identity is created. |
| `OUTPACE_BROADCAST_SOURCES` | `{}` | JSON object, at most 64 names and 256 KiB | all | HTTP(S) MPEG-TS sources to pull from and broadcast at startup. URLs can contain tokens, so treat this as a secret. An explicitly empty value is an error. |
| `OUTPACE_ALLOW_NON_GLOBAL_BROADCAST_SOURCE` | off | exact `1` | all | Let broadcast pull sources use private, loopback, CGNAT and IPv6 ULA addresses. Link-local and cloud-metadata addresses stay blocked. |
| `OUTPACE_TRACKERS` | built-in public tracker | comma-separated URLs, at most 64 kept | all | Trackers written into **new** broadcast descriptors and used for their announcements. When set, it fully replaces the default. Outpace itself only announces to lowercase `udp://` entries. Existing broadcasts keep the trackers they were created with. |
| `OUTPACE_SRT_BIND` | unset (off) | numeric socket address | srt build | Turns on the SRT listener (UDP), for example `127.0.0.1:9000`. Starting a non-SRT build with this set fails. |
| `OUTPACE_SRT_PASSPHRASE_FILE` | unset | absolute file path | srt build | Required with `OUTPACE_SRT_BIND`. The file holds a 10–79 byte passphrase, is owned by the daemon user, and has no group or other permissions. |
| `OUTPACE_SRT_LATENCY_MS` | `120` | 20–2000 | srt build | SRT receiver latency. |
| `OUTPACE_SRT_MAX_CONNECTIONS` | `4` | 1–16 | srt build | Simultaneous SRT connections, including ones still connecting or closing. |

## Seeding and inbound peers

See [Seeding and Peers](Seeding-and-Peers).

| Setting | Default | Accepted values | Avail. | Notes |
| --- | --- | --- | --- | --- |
| `OUTPACE_ENABLE_SEEDING` | `true` | boolean | all | Answer piece requests from peers you downloaded from. |
| `OUTPACE_ENABLE_INBOUND` | `true` | boolean | all | Run the peer listener on `OUTPACE_PEER_LISTEN`, serve inbound peers and announce to trackers and the DHT. Set both this and seeding to `0` for download-only. |
| `OUTPACE_MAX_INBOUND` | `64` | integer (`0` acts as `1`) | all | Inbound connections kept open in total, including ones still handshaking. Extra connections close at once. |
| `OUTPACE_MAX_INBOUND_PER_STREAM` | `16` | integer, `0` = no per-stream cap | all | Inbound connections admitted per stream. |
| `OUTPACE_MAX_UNCHOKED` | `8` | integer | all | Peers per stream allowed to download from you at once, plus one rotating "optimistic" slot. `0` still allows the optimistic slot. |
| `OUTPACE_SEED_TTL_SECS` | `300` | seconds, `0` disables | all | Safety net that removes a stream's seeding data when nothing owns it any more. Normal teardown doesn't wait for this. |

## Storage and cache

| Setting | Default | Accepted values | Avail. | Notes |
| --- | --- | --- | --- | --- |
| `OUTPACE_DATA_DIR` | platform data dir + `outpace` | directory | all | Identity and broadcast state. Container: `/var/lib/outpace`. Keep it persistent and private. |
| `OUTPACE_CACHE_TYPE` | `memory` | `memory` or `disk` | all | Where retained pieces for seeding are kept. `disk` saves RAM on small machines. |
| `OUTPACE_CACHE_DIR` | `<data dir>/cache` | directory | all | Disk cache root, used only with `disk`. Wiped at startup. It may be inside the data directory, but it must not be the data directory or contain it. |
| `OUTPACE_SEED_STORE_BYTES` | `134217728` (128 MiB) | bytes | all | Ceiling on retained pieces **per stream** (not a total). In memory mode it is capped per platform (about 512 MiB on 32-bit, 8 GiB on 64-bit). It must hold at least one full piece of every broadcast you run. |
| `OUTPACE_SEED_RETENTION_SECS` | `45` | seconds, `0` = byte cap only | all | For live streams, keep only about this many seconds of recent pieces, so memory follows the bitrate instead of filling the byte cap. VOD always uses the byte cap. |

### How the disk cache wipe works

Disk-cache contents are throwaway live data and are never reused after a restart. To avoid
deleting the wrong directory, Outpace only wipes a cache directory that is empty or contains a
regular file named `.outpace-cache`. It writes that file itself the first time it uses an empty
directory. If you point `OUTPACE_CACHE_DIR` at a non-empty directory without that file, startup
fails instead of deleting anything.

If a stream's cache directory can't be created later (for example after a permission change),
playback continues but that stream keeps no pieces for seeding. The log shows
`[cache] ERROR: disk store creation failure`.

## NAT and port mapping

See [NAT and Port Mapping](NAT-and-Port-Mapping).

| Setting | Default | Accepted values | Avail. | Notes |
| --- | --- | --- | --- | --- |
| `OUTPACE_ENABLE_PORT_MAPPING` | `false` | boolean | all | Ask the gateway to forward the peer port. Only works when Outpace owns the LAN address (bare metal or host networking), and only when inbound serving is on. Failure is logged and isn't fatal. |
| `OUTPACE_PORT_MAP_BACKEND` | `auto` | `auto`, `upnp`, `natpmp`, `none` | all | `auto` tries UPnP-IGD, then NAT-PMP. |
| `OUTPACE_PORT_MAP_EXTERNAL_PORT` | same as the peer port | 1–65535 | all | External port to request. Leave unset so the announced port matches. |

## Private peers and descriptors

See [Private Peer Mode](Private-Peer-Mode).

| Setting | Default | Accepted values | Avail. | Notes |
| --- | --- | --- | --- | --- |
| `OUTPACE_PRIVATE_PEERS` | unset (public mode) | comma-separated `IPv4:port`, at most 256 entries and 8 KiB | 1.0.0 | Any value switches on outbound-only private mode: only these peers, no trackers, no DHT, no inbound. An empty value is an error. |
| `OUTPACE_PRIVATE_DESCRIPTORS` | unset | JSON object `{"alias":"/abs/path"}`, at most 64 entries and 64 KiB | 1.0.0 | Local descriptor files playable as `local:<alias>`. Setting it without `OUTPACE_PRIVATE_PEERS` fails startup. |

## Discovery, trackers and resolution

| Setting | Default | Accepted values | Avail. | Notes |
| --- | --- | --- | --- | --- |
| `OUTPACE_ACE_PEERS` | unset | comma-separated `IPv4:port` | all | Extra bootstrap peers tried on every live stream. In public mode malformed entries are skipped silently; in private mode they must be in the allowlist. |
| `OUTPACE_BEP9_CONTENT_ID_FALLBACK` | `false` | boolean | all | If the catalog can't resolve a content id, ask peers for its metadata. Needed to play an Outpace broadcast's `acestream://` link from a **different** daemon. |
| `OUTPACE_ALLOW_NON_GLOBAL_TRANSPORT` | off | exact `1` | all | Allow transport-file URLs on private, loopback or CGNAT hosts. Link-local, multicast and documentation ranges stay blocked. For self-hosted LAN swarms. |
| `OUTPACE_TRACKER_ALLOW_NON_GLOBAL` | off | exact `1` | all | Allow announcing to trackers on private or local addresses. For self-hosted or offline swarms. |

## Limits and admission

| Setting | Default | Accepted values | Avail. | Notes |
| --- | --- | --- | --- | --- |
| `OUTPACE_MAX_SESSIONS` | `0` (unlimited) | non-negative integer | all | Cap on distinct sources (live streams, VOD files, support sources) the daemon runs at once. All viewers of one stream share one slot. New sources at the cap get `503` with `Retry-After: 1`; existing ones stay joinable. It doesn't cap viewers, memory or sockets. |

## Support sources

| Setting | Default | Accepted values | Avail. | Notes |
| --- | --- | --- | --- | --- |
| `OUTPACE_SUPPORT_SOURCES` | unset (off) | JSON object `{"name":"selector"}`, at most 8 entries and 64 KiB | 1.0.0 | Live streams to keep running and seeding with no viewer. Requires inbound serving and seeding. See [Support Sources and Operations](Support-Sources-and-Operations). |

## AceStream compatibility API

| Setting | Default | Accepted values | Avail. | Notes |
| --- | --- | --- | --- | --- |
| `OUTPACE_ACE_COMPAT` | `false` | boolean | all | Turns on `/ace/*`, `/server/api` and `/webui/api/service`. When off, those routes return `404`. Required for [Timeshift](Timeshift) seeking. |
| `OUTPACE_EXPERIMENTAL_ACE_COMPAT` | unset | boolean | all | Deprecated old name. Read only when `OUTPACE_ACE_COMPAT` is unset; a set `OUTPACE_ACE_COMPAT=0` wins over it. Rename it. |

### AceStream search services

| Setting | Default | Accepted values | Avail. | Notes |
| --- | --- | --- | --- | --- |
| `OUTPACE_COMPAT_ACESTREAM_SERVICES` | `false` | boolean | 1.0.0 | Enables `/search` and external index requests. Independent of `OUTPACE_ACE_COMPAT`; rejected in private peer mode. |

No operator API key is required. Search uses external plain HTTP services, exposing queries
and the server IP to the service and network observers. See [Stream Search](Stream-Search)
for limits and client integration. Existing
signed-catalog content-ID resolution and its defaults remain unchanged.

## AceStream identity

See [Engine Camouflage](Engine-Camouflage). Off by default: Outpace keeps its own labels.
Turn it on only when every value a peer, tracker, search host or catalog can observe should
match one official Linux engine profile. A bad value stops startup and names the variable.
Overrides are ignored while the switch is off.

| Setting | Default | Accepted values | Avail. | Notes |
| --- | --- | --- | --- | --- |
| `OUTPACE_ACESTREAM_IDENTITY` | `false` | boolean | 1.1.0 | `1` or `true` installs the profile for the process. |
| `OUTPACE_ACESTREAM_IDENTITY_VERSION` | `3.2.11` | `X.Y.Z` | 1.1.0 | Search, catalog, handshake and the User-Agent use this version. The numeric code is `X*1000000 + Y*10000 + Z*100`. |
| `OUTPACE_ACESTREAM_IDENTITY_PLATFORM` | `linux` | `linux` | 1.1.0 | Any other value is rejected. |
| `OUTPACE_ACESTREAM_IDENTITY_DEVICE_ID` | random, persisted | 40 hex digits | 1.1.0 | Stable id for search and the UPnP label. Stored in the data directory when unset. |
| `OUTPACE_ACESTREAM_IDENTITY_HARDWARE_KEY` | derived from this host | 40 hex digits | 1.1.0 | Linux default comes from the machine's memory size. Hosts without `/proc/meminfo` must set it. |
| `OUTPACE_ACESTREAM_IDENTITY_ASN` | looked up | integer, optional `AS` prefix | 1.1.0 | Set both this and the country to skip the lookup. Setting only one is an error. |
| `OUTPACE_ACESTREAM_IDENTITY_COUNTRY` | looked up | two letters | 1.1.0 | ISO 3166 alpha-2. Paired with the ASN. |
| `OUTPACE_ACESTREAM_IDENTITY_USER_AGENT` | engine UA for the version | printable ASCII, at most 256 characters | 1.1.0 | Sent to search and AceStream web hosts. Catalog requests keep a per-request Chrome User-Agent. |

## Diagnostics

| Setting | Default | Notes |
| --- | --- | --- |
| `OUTPACE_METRICS` | `false` | `1` or `true` serves Prometheus text metrics at `GET /metrics` on the HTTP bind. See [Support sources and operations](Support-Sources-and-Operations#prometheus-metrics). |
| `OUTPACE_SEED_DEBUG` | unset | Any value logs every inbound seeding message (`[seed-session]` lines). Very verbose. Turn it on only while debugging. |

Variables named `OUTPACE_TEST_*`, `OUTPACE_DHT_COMPARE_*` or `OUTPACE_RUN_*` belong to the test
suite and do nothing in normal operation.

## Docker Compose variables

The repository's `compose.yml` also reads these. They configure Compose, not the daemon:

| Variable | Default | Meaning |
| --- | --- | --- |
| `OUTPACE_IMAGE` | `ghcr.io/outpace-stream/outpace-engine:latest` | Image to run. Pin a release tag. |
| `OUTPACE_HTTP_HOST` | `127.0.0.1` | Host address the HTTP port is published on. |
| `OUTPACE_HTTP_PORT` | `6878` | Host port for HTTP. |
| `OUTPACE_RTMP_HOST` | `127.0.0.1` | Host address the RTMP port is published on. |
| `OUTPACE_RTMP_PORT` | `1935` | Host port for RTMP. |

Compose only passes a fixed list of daemon settings from `.env` into the container:
`OUTPACE_BROADCAST_SOURCES`, `OUTPACE_ALLOW_NON_GLOBAL_BROADCAST_SOURCE`,
`OUTPACE_ENABLE_INBOUND`, `OUTPACE_ENABLE_SEEDING`, `OUTPACE_ENABLE_PORT_MAPPING`,
`OUTPACE_CACHE_TYPE`, `OUTPACE_CACHE_DIR`, `OUTPACE_SEED_STORE_BYTES` and `OUTPACE_ACE_COMPAT`.
The bind addresses and data directory are fixed in the file. **Any other setting you put in
`.env` is ignored.** Add those to a `compose.override.yml`; see
[Deployment](Deployment#passing-other-settings).
