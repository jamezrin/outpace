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
| `clients` | Direct MPEG-TS viewers. HLS viewers don't add to it. |
| `peers` | Connected upstream peers. |
| `bitrate` | Measured incoming media rate in bits per second, over about five seconds. |
| `buffer_ms` | Media queued on the **server**. It isn't the player's buffer and can correctly be near zero. |
| `uploaded`, `peers_served` | What you have uploaded to other peers. |
| `recovery` | Counters for reconnects, pool rebuilds, skipped gaps and rejected pieces. Rising numbers mean a struggling swarm. |
| `metadata` | Title, advertised bitrate (bits/s) and categories from the descriptor, or `null`, `null` and `[]`. |
| `normalization` | Audio-conversion state when [live audio normalisation](HLS-Output#audio-normalisation) is on, otherwise `null`. |

The status route never starts a stream; it returns `404` when the stream isn't running. Use it
(and `/streams`) for monitoring. **Don't poll a playback URL as a health check**, because that
starts or keeps alive a real stream.

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
| `404` | Unknown network or invalid id. For a segment: not produced yet, or already outside the window. |
| `422` | The input couldn't be turned into a playable descriptor, for example an infohash with no verified descriptor. The body gives the reason. |
| `503` + `Retry-After: 1` | HLS not ready within the startup timeout, the [session cap](Configuration#limits-and-admission) reached, or audio conversion temporarily unavailable. |

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

Multi-file VOD isn't supported and fails with a clear error. VOD isn't available through the
compatibility API.

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
builds newer than v0.9.0. Enabling `OUTPACE_ACE_COMPAT` alone does not add search, and
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
