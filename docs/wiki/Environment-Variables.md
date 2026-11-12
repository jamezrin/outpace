# Environment Variables

Every `OUTPACE_*` variable the daemon reads, A to Z. Each name links to its topic on the
[Configuration](Configuration) page, which explains accepted values, validation and when to
change it. See [Command Line](Command-Line) for commands and flags.

Restart the daemon after any change. Booleans accept exactly `1`, `true`, `0` or `false`.
Variables marked "exact `1`" turn on only for the string `1`. **Avail.** says where a variable
is available: **all** means v0.9.0 and later; **edge** means only the `edge` image and builds
from `main`; **srt build** means binaries built with the `srt` feature.

| Variable | Default | Avail. | Purpose |
| --- | --- | --- | --- |
| [`OUTPACE_ACE_COMPAT`](Configuration#acestream-compatibility-api) | `false` | all | Turn on the AceStream-compatible `/ace/*` and `/server/api` routes. |
| [`OUTPACE_ACE_PEERS`](Configuration#discovery-trackers-and-resolution) | unset | all | Extra bootstrap peers, as `IPv4:port` separated by commas. |
| [`OUTPACE_ALLOW_NON_GLOBAL_BROADCAST_SOURCE`](Configuration#broadcasting-and-ingest) | off | all | Exact `1`: allow private and loopback broadcast pull sources. |
| [`OUTPACE_ALLOW_NON_GLOBAL_TRANSPORT`](Configuration#discovery-trackers-and-resolution) | off | all | Exact `1`: allow transport-file URLs on private hosts. |
| [`OUTPACE_BEP9_CONTENT_ID_FALLBACK`](Configuration#discovery-trackers-and-resolution) | `false` | all | Resolve content ids from peer metadata when the catalog fails. |
| [`OUTPACE_BIND`](Configuration#network-and-listeners) | `127.0.0.1:6878` | all | HTTP API address. The container sets `0.0.0.0:6878`. |
| [`OUTPACE_BROADCAST_OPTIONS`](Configuration#broadcasting-and-ingest) | `{}` | all | Per-broadcast metadata profiles (JSON). |
| [`OUTPACE_BROADCAST_SOURCES`](Configuration#broadcasting-and-ingest) | `{}` | all | HTTP(S) MPEG-TS pull sources started at boot (JSON). Treat as a secret. |
| [`OUTPACE_CACHE_DIR`](Configuration#storage-and-cache) | `<data dir>/cache` | all | Disk-cache root. Wiped at startup. |
| [`OUTPACE_CACHE_TYPE`](Configuration#storage-and-cache) | `memory` | all | Where seeding pieces are kept: `memory` or `disk`. |
| [`OUTPACE_DATA_DIR`](Configuration#storage-and-cache) | platform data dir + `outpace` | all | Identity and broadcast state. The container uses `/var/lib/outpace`. |
| [`OUTPACE_ENABLE_INBOUND`](Configuration#seeding-and-inbound-peers) | `true` | all | Accept inbound peers and announce to trackers and the DHT. |
| [`OUTPACE_ENABLE_PORT_MAPPING`](Configuration#nat-and-port-mapping) | `false` | all | Ask the router to open the peer port with UPnP or NAT-PMP. |
| [`OUTPACE_ENABLE_SEEDING`](Configuration#seeding-and-inbound-peers) | `true` | all | Upload to the peers you download from. |
| [`OUTPACE_EXPERIMENTAL_ACE_COMPAT`](Configuration#acestream-compatibility-api) | unset | all | Deprecated name for `OUTPACE_ACE_COMPAT`. |
| [`OUTPACE_FFMPEG_PATH`](Configuration#audio-normalisation) | `ffmpeg` | all | FFmpeg executable, used only for audio normalisation. |
| [`OUTPACE_HISTORY_BYTES`](Configuration#timeshift) | 128 MiB | edge | Timeshift memory per source. |
| [`OUTPACE_HISTORY_SECONDS`](Configuration#timeshift) | `0` (off) | edge | How far back viewers can rewind, up to 3600 seconds. |
| [`OUTPACE_HISTORY_TOTAL_BYTES`](Configuration#timeshift) | 256 MiB | edge | Timeshift memory shared by all sources. |
| [`OUTPACE_HLS_AUDIO_NORMALIZE`](Configuration#audio-normalisation) | `false` | all | Convert AC3 and MPEG audio to AAC for live HLS. |
| [`OUTPACE_HLS_SEGMENT_DURATION_MS`](Configuration#hls) | `5000` | all | Target HLS segment length. |
| [`OUTPACE_HLS_SEGMENT_PACKETS`](Configuration#hls) | `65536` | all | Size ceiling per segment, in 188-byte packets. |
| [`OUTPACE_HLS_STARTUP_SEGMENTS`](Configuration#hls) | `6` | all | Segments ready before the first playlist is returned. |
| [`OUTPACE_HLS_STARTUP_TIMEOUT_MS`](Configuration#hls) | `45000` | all | HLS startup wait before answering `503`. |
| [`OUTPACE_HLS_WINDOW_SEGMENTS`](Configuration#hls) | `8` | all | Segments kept in the live playlist. |
| [`OUTPACE_LIVE_AUDIO_NORMALIZE`](Configuration#audio-normalisation) | `false` | edge | Convert audio once per source, for TS and HLS. |
| [`OUTPACE_MANAGED_HLS`](Configuration#managed-hls-origins) | `false` | edge | Relay HLS-origin descriptors locally instead of redirecting players. |
| [`OUTPACE_MAX_ACTIVE_UPSTREAMS`](Configuration#live-recovery-advanced) | `4` | all | Peers one stream downloads from at once. |
| [`OUTPACE_MAX_INBOUND`](Configuration#seeding-and-inbound-peers) | `64` | all | Total inbound peer connections. |
| [`OUTPACE_MAX_INBOUND_PER_STREAM`](Configuration#seeding-and-inbound-peers) | `16` | all | Inbound connections per stream. `0` means no per-stream cap. |
| [`OUTPACE_MAX_PARALLEL_CONNECT`](Configuration#live-recovery-advanced) | `12` | all | Peers dialled at once while searching. |
| [`OUTPACE_MAX_PIECE_ADVANCE`](Configuration#live-recovery-advanced) | `256` | all | Pieces scheduled in one forward step. |
| [`OUTPACE_MAX_REASM_PIECES_AHEAD`](Configuration#live-recovery-advanced) | `512` | all | Pieces accepted ahead of the playback cursor. |
| [`OUTPACE_MAX_SESSIONS`](Configuration#limits-and-admission) | `0` (unlimited) | all | Cap on concurrent sources. |
| [`OUTPACE_MAX_UNCHOKED`](Configuration#seeding-and-inbound-peers) | `8` | all | Peers per stream that may download from you at once. |
| [`OUTPACE_PEER_LISTEN`](Configuration#network-and-listeners) | `0.0.0.0:8621` | all | TCP peer listener. Its port is also the announced port. |
| [`OUTPACE_PORT_MAP_BACKEND`](Configuration#nat-and-port-mapping) | `auto` | all | `auto`, `upnp`, `natpmp` or `none`. |
| [`OUTPACE_PORT_MAP_EXTERNAL_PORT`](Configuration#nat-and-port-mapping) | the peer port | all | External port to request from the router. |
| [`OUTPACE_PREBUFFER_BYTES`](Configuration#playback-and-startup-buffering) | 128 MiB | all | Cap on the startup buffer. |
| [`OUTPACE_PREBUFFER_MS`](Configuration#playback-and-startup-buffering) | `30000` | all | Media to collect before playback starts. `0` disables it. |
| [`OUTPACE_PREBUFFER_TIMEOUT_MS`](Configuration#playback-and-startup-buffering) | `15000` | all | Longest startup wait after the first media byte. |
| [`OUTPACE_PREFETCH_PIECES`](Configuration#playback-and-startup-buffering) | automatic | all | Pieces behind the live edge to start at. |
| [`OUTPACE_PRIVATE_DESCRIPTORS`](Configuration#private-peers-and-descriptors) | unset | edge | Local descriptor files, as an alias-to-path JSON object. |
| [`OUTPACE_PRIVATE_PEERS`](Configuration#private-peers-and-descriptors) | unset | edge | Allowlist that switches on private peer mode. |
| [`OUTPACE_REQUEST_CHECK_INTERVAL_MS`](Configuration#live-recovery-advanced) | `1000` | all | How often outstanding piece requests are checked. |
| [`OUTPACE_REQUEST_TIMEOUT_MS`](Configuration#live-recovery-advanced) | `1500` | all | Wait for a piece before asking another peer. |
| [`OUTPACE_RTMP_BIND`](Configuration#network-and-listeners) | `127.0.0.1:1935` | all | RTMP ingest address. The container sets `0.0.0.0:1935`. |
| [`OUTPACE_SEED_DEBUG`](Configuration#diagnostics) | unset | all | Log every inbound seeding message. Very verbose. |
| [`OUTPACE_SEED_RETENTION_SECS`](Configuration#storage-and-cache) | `45` | all | Seconds of recent pieces kept per live stream. |
| [`OUTPACE_SEED_STORE_BYTES`](Configuration#storage-and-cache) | 128 MiB | all | Ceiling on retained pieces per stream. |
| [`OUTPACE_SEED_TTL_SECS`](Configuration#seeding-and-inbound-peers) | `300` | all | Safety net that frees seeding data nothing owns. |
| [`OUTPACE_SESSION_BUFFER`](Configuration#playback-and-startup-buffering) | `256` | all | Per-viewer fan-out queue depth. |
| [`OUTPACE_SRT_BIND`](Configuration#broadcasting-and-ingest) | unset (off) | srt build | SRT ingest address (UDP). |
| [`OUTPACE_SRT_LATENCY_MS`](Configuration#broadcasting-and-ingest) | `120` | srt build | SRT receiver latency, 20–2000 ms. |
| [`OUTPACE_SRT_MAX_CONNECTIONS`](Configuration#broadcasting-and-ingest) | `4` | srt build | Simultaneous SRT connections, 1–16. |
| [`OUTPACE_SRT_PASSPHRASE_FILE`](Configuration#broadcasting-and-ingest) | unset | srt build | SRT passphrase file. Required with `OUTPACE_SRT_BIND`. |
| [`OUTPACE_STALE_UPSTREAM_TIMEOUT_MS`](Configuration#live-recovery-advanced) | `12000` | all | Rebuild the peer pool after this long without progress. |
| [`OUTPACE_SUPPORT_SOURCES`](Configuration#support-sources) | unset (off) | edge | Streams kept running and seeding with no viewer (JSON). |
| [`OUTPACE_TRACKER_ALLOW_NON_GLOBAL`](Configuration#discovery-trackers-and-resolution) | off | all | Exact `1`: allow announcing to trackers on private addresses. |
| [`OUTPACE_TRACKERS`](Configuration#broadcasting-and-ingest) | built-in public tracker | all | Trackers written into new broadcasts. |

Variables named `OUTPACE_TEST_*`, `OUTPACE_DHT_COMPARE_*` or `OUTPACE_RUN_*` belong to the test
suite. The repository's Compose file also reads `OUTPACE_IMAGE`, `OUTPACE_HTTP_HOST`,
`OUTPACE_HTTP_PORT`, `OUTPACE_RTMP_HOST` and `OUTPACE_RTMP_PORT`; see
[Configuration](Configuration#docker-compose-variables).
