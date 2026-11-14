# Playing Streams

Outpace has two HTTP interfaces for playback:

- the **native API** (`/streams/...`, `/vod/...`). It is always on, and it's the one to use for
  players, media servers and playlist tools;
- the **AceStream-compatible API** (`/ace/...`, `/server/api`). It is off by default. Turn it on
  for apps and add-ons written for the original engine.

Both serve the same streams. A stream opened through one interface is shared with viewers on the
other.

Examples use `http://127.0.0.1:6878` and the placeholder id
`0123456789abcdef0123456789abcdef01234567`.

## What you can play

| Input | Native form | Notes |
| --- | --- | --- |
| Content id (40 hex characters) | `/streams/ace/<id>` or `/streams/ace/cid:<id>` | The usual AceStream id. Resolved through the AceStream catalog. |
| Infohash | `/streams/ace/ih:<infohash>` | A swarm hash. Plays only if Outpace can find a verified descriptor for it (its own broadcasts, ones seen before, or the catalog). Otherwise `422`. |
| Transport file (`.acelive` / `.torrent` URL) | via the compat `url=` selector or `outpace play <url>` | Fetched over HTTP(S) with a 1 MiB limit, no redirects, and private addresses refused. |
| Magnet (`magnet:?xt=urn:btih:...`) | via compat `magnet=` or `outpace play` | Reduced to its infohash, then treated like an infohash. BitTorrent v2 magnets aren't supported. |
| Local descriptor | `/streams/ace/local%3A<alias>` | [Private Peer Mode](Private-Peer-Mode) only. |

> **Upgrading from an early build?** A bare 40-hex id on `/streams/ace/` means a **content id**. Older
> playlists that used bare infohashes there must switch to `ih:<infohash>`. There is no silent
> fallback from one to the other.

Content ids are case-insensitive. Outpace lowercases them, and the bare and `cid:` forms share
one session.

## Native live playback

| URL | Returns |
| --- | --- |
| `GET /streams/ace/<id>` | Continuous MPEG-TS (`video/mp2t`). Includes `Icy-Name` when the stream has a title. |
| `GET /streams/ace/<id>.ts` | The same, written explicitly. |
| `GET /streams/ace/<id>.m3u8` | A live HLS playlist. Segments are `/streams/ace/<id>/seg/<n>.ts`. See [HLS Output](HLS-Output). |

Opening any of these starts the stream, or joins it if it's already running. Every viewer of one
source shares a single download.

```bash
vlc "http://127.0.0.1:6878/streams/ace/0123456789abcdef0123456789abcdef01234567"
mpv "http://127.0.0.1:6878/streams/ace/0123456789abcdef0123456789abcdef01234567.m3u8"
ffmpeg -i "http://127.0.0.1:6878/streams/ace/0123456789abcdef0123456789abcdef01234567" \
  -c copy -t 60 clip.ts
```

### Watching a stream's status

```bash
curl http://127.0.0.1:6878/streams
curl http://127.0.0.1:6878/streams/ace/0123456789abcdef0123456789abcdef01234567/status
```

A status response looks like this:

```json
{
  "network": "ace",
  "id": "cid:0123456789abcdef0123456789abcdef01234567",
  "phase": "playing",
  "error": null,
  "clients": 1,
  "peers": 4,
  "bitrate": 2450000,
  "buffer_ms": 6200,
  "uploaded": 1048576,
  "peers_served": 2,
  "recovery": {"upstream_reconnects": 0, "stale_teardowns": 0,
               "skipped_evicted_gaps": 0, "signature_rejects": 0, "active_upstreams": 4},
  "metadata": {"title": "My Channel", "bitrate": 2450000, "categories": ["other"]}
}
```

| Field | Meaning |
| --- | --- |
| `phase` | Where the start is: `resolving`, `connecting`, `buffering`, `playing` or `failed`. See below. |
| `error` | `{"code": ..., "message": ...}` when `phase` is `failed`, otherwise `null`. |
| `clients` | Direct MPEG-TS viewers. HLS viewers don't add to it. |
| `peers` | Connected upstream peers. |
| `bitrate` | Measured incoming media rate in bits per second, over about five seconds. |
| `buffer_ms` | Media queued on the **server**. It isn't the player's buffer and can correctly be near zero. |
| `uploaded`, `peers_served` | What you have uploaded to other peers. |
| `recovery` | Counters for reconnects, pool rebuilds, skipped gaps and rejected pieces. Rising numbers mean a struggling swarm. |
| `metadata` | Title, advertised bitrate (bits/s) and categories from the descriptor, or `null`, `null` and `[]`. |
| `normalization` | Audio-conversion state when [live audio normalisation](HLS-Output#audio-normalisation) is on, otherwise `null`. |

#### Start phase

`phase` tells a player or app why a stream hasn't started yet:

| `phase` | Meaning |
| --- | --- |
| `resolving` | Resolving the id to its descriptor. |
| `connecting` | Descriptor known; looking for and connecting to peers. |
| `buffering` | Connected to peers; waiting for the first media. |
| `playing` | Media is flowing out of the stream. |
| `failed` | The start failed; `error` says why. A running stream also reports `failed` when [audio normalisation](HLS-Output#audio-normalisation) failed before any media went out. |

The status route answers from the moment a playback URL is opened, before the stream exists.
While it is still `resolving` or `connecting`, and for about 60 seconds after a start
`failed`, the response carries only `network`, `id`, `phase`, `error`, `clients` (`0`) and
`peers` (`0`):

```json
{
  "network": "ace",
  "id": "cid:0123456789abcdef0123456789abcdef01234567",
  "phase": "failed",
  "error": { "code": "no_peers", "message": "no peer is sharing this stream" },
  "clients": 0,
  "peers": 0
}
```

Outpace remembers at most 64 such start attempts. A new start of the same stream replaces the
old record. A stopped or cancelled start leaves none.

The status route never starts a stream. It returns `404` when the stream isn't running and
has no recent start attempt. Use it (and `/streams`) for monitoring. **Don't poll a playback
URL as a health check**, because that starts or keeps alive a real stream.

### Stopping a stream

Closing the player is enough. When no viewer, HLS client or lease is left, the stream is torn
down after a 30-second grace period, which lets a player reconnect without starting over.

To stop a stream immediately for **everyone** watching it:

```bash
curl -X DELETE http://127.0.0.1:6878/streams/ace/0123456789abcdef0123456789abcdef01234567
```

This returns `204`, or `404` if the stream wasn't running.

### Native error responses

| Status | Meaning |
| --- | --- |
| `400` | A malformed `cid:` or `ih:` selector. |
| `404` | Unknown network, invalid id, or a start that failed (see the `code`). For a segment: not produced yet, or already outside the window. |
| `422` | The input couldn't be turned into a playable descriptor, for example an infohash with no verified descriptor, or protected content. |
| `503` + `Retry-After: 1` | HLS not ready within the startup timeout, the [session cap](Configuration#limits-and-admission) reached, or audio conversion temporarily unavailable. |
| `507` | A VOD that doesn't fit on the cache disk (code `no_space`). |

When a stream can't be **started**, the playback routes (`/streams/<network>/<id>`, `.ts`,
`.m3u8`) and the VOD routes (`/vod/...`) answer with `Content-Type: application/json` and a
stable error code:

```json
{ "error": { "code": "unresolvable", "message": "..." } }
```

The same `error` object appears on the status route while `phase` is `failed`, but only for
live starts on a known network. A VOD failure or an unknown network leaves no start record, so
the status route answers `404`: read the `error` from the start response itself. Key off
`code`; `message` is human-readable text and can change.

| `code` | Status | Meaning |
| --- | --- | --- |
| `not_found` | `404` | Unknown network, or the start was refused. |
| `no_peers` | `404` (`502` for VOD) | Nobody is sharing the stream right now: no metadata peer answered, or initial discovery found no peer. |
| `network` | `404` (`502` for VOD); `422` for an `ih:` infohash | The catalog couldn't be reached. Check the network, VPN and firewall. |
| `unresolvable` | `422` (`502` for public VOD) | The id has no verified descriptor. `message` explains why and what to try instead. |
| `protected` | `422` | Encrypted or invalid-protection content, which Outpace doesn't play. |
| `capacity` | `503` + `Retry-After: 1` (`404` when the catalog resolver is saturated) | The [session cap](Configuration#limits-and-admission), the catalog resolver or audio conversion is at capacity. Retry later. |
| `no_space` | `507` | VOD only, with the disk cache: the file doesn't fit in the free space on the cache disk, or the disk filled during the download. `message` gives the required and available sizes. See [VOD free-space check](Configuration#vod-free-space-check). |
| `internal` | `404` (`502` for VOD) | Any other failure. The daemon log shows how far the start got. |

The [compatibility API](#acestream-compatible-api) keeps its own error format.

## Live and VOD

Most AceStream content is **live**: a continuous signed stream with no fixed length. Outpace
follows the live edge and keeps only a short window.

**VOD** content is a single file with a fixed length and per-piece hashes. Outpace verifies
every piece before sending it:

| URL | Returns |
| --- | --- |
| `GET /vod/ace/cid:<id>` | The whole file, with `Content-Length` and `Accept-Ranges: bytes`. Single byte ranges (`206`) work, so players can seek. An unsatisfiable range gives `416`. |
| `GET /vod/ace/cid:<id>/manifest.m3u8` | A static HLS playlist for the file. |
| `GET /vod/ace/cid:<id>/seg/<n>.ts` | One verified HLS segment. |

```bash
mpv "http://127.0.0.1:6878/vod/ace/cid:0123456789abcdef0123456789abcdef01234567"
outpace play --vod acestream://0123456789abcdef0123456789abcdef01234567 > movie.mp4
```

With `OUTPACE_CACHE_TYPE=disk`, a VOD start checks the free space on the cache disk first and
answers `507` with the `no_space` code if the file won't fit (see
[VOD free-space check](Configuration#vod-free-space-check)):

```json
{ "error": { "code": "no_space", "message": "not enough free disk space: 356 MiB required, 300 MiB available" } }
```

If the disk fills during a download anyway, the response ends early and the next request for
that VOD gets the same `507` `no_space` error. The memory cache writes nothing to disk and skips
the check.

Multi-file VOD isn't supported and fails with a clear error; selected-file support (#79) is
explicitly deferred for 1.0.0. VOD isn't available through the compatibility API.

VOD HLS segments use byte-based cuts and approximate durations. They are not keyframe-aligned,
and PCR-based media-time seeking (#128) remains conditional on a demonstrated player failure.
Byte-range seeking in the direct `/vod` file response is a separate supported feature; it does
not establish precise HLS seeking. See [Releases and Versions](Releases-and-Versions#deferred-scope).

## Command-line playback

`outpace play` streams to standard output without a server. Progress and errors go to standard
error, so piping is safe:

```bash
outpace play acestream://0123456789abcdef0123456789abcdef01234567 | vlc -
outpace play "acestream:?content_id=0123456789abcdef0123456789abcdef01234567" > live.ts
outpace play https://example.com/channel.acelive | mpv -
outpace play --peer 192.0.2.10:8621 acestream://0123456789abcdef0123456789abcdef01234567 > live.ts
```

- `--vod` downloads and verifies a VOD file.
- `--peer IP:PORT` adds a bootstrap peer. You can repeat it.
- Infohash and magnet inputs need a verified descriptor from the catalog. A one-shot process
  has no local history, so they fail if the catalog can't help.

## AceStream-compatible API

Turn it on with `OUTPACE_ACE_COMPAT=1` and restart. With it off, every route below returns
`404`.

### Direct playback

```text
http://127.0.0.1:6878/ace/getstream?id=0123456789abcdef0123456789abcdef01234567
http://127.0.0.1:6878/ace/manifest.m3u8?id=0123456789abcdef0123456789abcdef01234567
```

- `getstream` without `format` (or with `format=`) returns MPEG-TS directly.
- `manifest.m3u8` redirects (`302`) to a stable, tokenised HLS playlist.

Selectors are tried in this order: `content_id`, `infohash`, `id` (an alias for `content_id`),
`url`, `magnet`. URL-encode `url=` and `magnet=` values.

### JSON sessions

Many apps ask for `format=json`, then use the URLs they get back:

```bash
curl "http://127.0.0.1:6878/ace/getstream?content_id=0123456789abcdef0123456789abcdef01234567&format=json"
```

```json
{"response": {
   "playback_url": "http://127.0.0.1:6878/ace/r/.../...",
   "stat_url": "http://127.0.0.1:6878/ace/stat/.../...",
   "command_url": "http://127.0.0.1:6878/ace/cmd/.../...",
   "infohash": "...", "playback_session_id": "...",
   "is_live": 1, "is_encrypted": 0, "metadata": {...}},
 "error": null}
```

- Open `playback_url` to play. Read `stat_url` for statistics: `speed_down` and `speed_up` are
  in KiB/s.
- Stop with `command_url?method=stop`.
- Always check **both** the HTTP status and the JSON `error` field. Most errors come back as HTTP
  `200` with `response: null`.
- Treat the returned URLs as opaque. Don't build URLs from `infohash` or `playback_session_id`.
- A TS `format=json` request doesn't start the stream; opening `playback_url` does. An HLS
  `format=json` request starts it straight away.

### Control API

`/server/api?method=...` and `/webui/api/service?method=...` answer
`{"result": ..., "error": ...}`:

| Method | Returns |
| --- | --- |
| `get_version` | Outpace's version (not an AceStream engine version). |
| `get_status` | `dl` when any stream is active, otherwise `idle`, plus session counts. |
| `get_network_connection_status` | Registered networks. Not a real connectivity test. |
| `analyze_content` | Whether an id is live and whether it's encrypted, without joining the swarm. |
| `get_content_id` | Echoes the content id you passed. It can't look one up from an infohash. |
| `get_media_files` | One synthetic file entry. Live entries report `mime: video/mp2t`. |

### Leases

Each `format=json` or HLS request creates a **lease**:

- A lease expires **six hours** after it is created. Using it doesn't extend it.
- At most 4096 leases are kept; the oldest go first.
- `method=stop` ends that lease only. Other viewers keep playing.
- Leases live in memory, so a restart invalidates them. The app has to request new URLs.

Once no lease or viewer holds a stream, it is torn down after the usual 30-second grace period.

### What the compatibility API doesn't do

There is no premium or encrypted content, no accounts, no web UI or remote control, no general
channel lists or EPG, no file selection beyond the single file (`_idx=0`), no seeking in the
original engine's sense, and no VOD. Outpace adds its own [timeshift](Timeshift) commands
instead.

[Stream-name search](Stream-Search) is a separate, default-off AceStream services adapter in
builds available from v1.0.0. Enabling `OUTPACE_ACE_COMPAT` alone does not add search, and
`get_content_id` still does not convert an infohash to a content ID. Select results using
explicit `/streams/ace/ih:<infohash>` or compatibility `infohash=` playback selectors.

### Behind a proxy

The compatibility API builds returned URLs from `http://` plus the request's `Host` header. It
ignores `X-Forwarded-*` and `Forwarded`, and it never returns HTTPS. If clients reach Outpace
through TLS termination or a sub-path, the returned URLs won't work. See
[Deployment](Deployment#behind-a-reverse-proxy).

## Known clients

| Client | How it connects | Status |
| --- | --- | --- |
| VLC | Native URL or `/ace/getstream?id=` | Used in the documented examples. |
| mpv, ffmpeg, ffplay | Native TS or HLS URL | Plain HTTP TS/HLS consumers. Expected to work, but not separately tested. |
| Jellyfin and other IPTV-style media servers | Native URL in an M3U playlist | Supported integration target. |
| Dispatcharr | Native URL as the channel's media URL | Covered by local compatibility tests. |
| acexy | Compat JSON handshake, `playback_url`, stop command | Covered by local compatibility tests. |
| Acestream-Manager | Compat health check, stats, TS/HLS proxy | Covered by local compatibility tests (search excluded). |
| Apps built on the public AceStream Android SDK | Compat `/ace/*` and `/server/api` | SDK request quirks are handled: duplicated selectors, `stream_id=-1`, `api_version=2`. |
| Kodi and other add-ons using `/ace/getstream` | Compat API | Likely to work if they stick to the supported routes. Not tested. |

"Covered by local tests" means the client's requests were reproduced offline. It doesn't
guarantee playback quality with every stream. For a playlist entry, prefer the plain native
URL; it needs no handshake.

## Related pages

- [HLS Output](HLS-Output): segment and startup settings, audio, end of stream.
- [Timeshift](Timeshift): rewinding compatibility HLS viewers.
- [Managed HLS Origins](Managed-HLS-Origins): descriptors that point at an HLS server.
- [Troubleshooting](Troubleshooting): slow starts, stalls and errors.
