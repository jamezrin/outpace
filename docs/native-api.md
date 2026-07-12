# Native CLI and HTTP API

The native CLI and HTTP routes are outpace's supported integration surface. They are suitable for
players such as VLC, media servers such as Jellyfin, and playlist/proxy tools such as dispatcharr.
Playback requests start and share sessions, while status routes observe sessions already active.

Examples use `http://127.0.0.1:6878` and the built-in `ace` network. An `<id>` may be a
40-character infohash or another provider identifier accepted by the configured network.

## CLI

| Command | Behavior |
| --- | --- |
| `outpace serve` | Runs the native HTTP API, RTMP ingest, and enabled peer services. With no command, outpace defaults to `serve`. |
| `outpace play <input>` | Writes live MPEG-TS bytes to stdout. Diagnostics go to stderr, so redirection and piping are safe. Accepts `acestream://`, `acestream:?`, `magnet:`, and HTTP(S) transport-file inputs. |
| `outpace play --vod <input>` | Downloads a single-file VOD, verifies every piece, and writes the file to stdout. |
| `outpace broadcast <name>` | Creates or resumes a broadcast, prints HTTP/RTMP ingest and playback metadata to stderr, and runs the server. |

Use `outpace <command> --help` for arguments and accepted inputs. For example:

```bash
outpace play acestream://<content-id> | vlc -
outpace play --vod acestream://<content-id> > movie.mp4
outpace broadcast sports --public-host stream.example
```

## HTTP routes

| Route | Success response | Operational notes |
| --- | --- | --- |
| `GET /healthz` | `200 text/plain` (`ok`) | Process health probe; it does not prove swarm connectivity. |
| `GET /networks` | `200 application/json`, `{"networks":["ace"]}` | Provider networks configured in this daemon. |
| `GET /streams` | `200 application/json`, `{"streams":[...]}` | Active shared sessions. Entries have `network`, `id`, and direct-client count `clients`. |
| `GET /streams/<network>/<id>.ts` | `200 video/mp2t` streaming body | Starts or joins a shared live session. Unknown networks or invalid ids return `404`. |
| `GET /streams/<network>/<id>.m3u8` | `200 application/vnd.apple.mpegurl` | Starts or joins live HLS packaging and returns a sliding playlist. |
| `GET /streams/<network>/<id>/seg/<n>.ts` | `200 video/mp2t` | Retained live HLS segment. Missing, expired, or not-yet-produced segments return `404`; a segment request alone never starts a stream. |
| `GET /streams/<network>/<id>/status` | `200 application/json` | Active-session status; `404` before playback starts or after teardown. |
| `DELETE /streams/<network>/<id>` | `204` | Force-stops an active session; `404` if inactive. A `.ts` or `.m3u8` suffix is also accepted. |
| `GET /vod/<network>/<id>` | `200` or `206` streaming body | Verified single-file VOD. Supports one byte range and advertises `Accept-Ranges: bytes`; an unsatisfiable range returns `416`. |
| `GET /vod/<network>/<id>/manifest.m3u8` | `200 application/vnd.apple.mpegurl` | Static VOD HLS playlist. |
| `GET /vod/<network>/<id>/seg/<n>.ts` | `200 video/mp2t` | Verified VOD HLS segment. |
| `PUT /broadcast/<name>` | `200 application/json` | Starts/resumes raw MPEG-TS ingest. Returns `name`, `content_id`, and `infohash` while consuming the request body. |
| `GET /broadcast/<name>` | `200 application/octet-stream` | Returns the broadcast transport descriptor; `404` until minted. |
| `DELETE /broadcast/<name>` | `204` | Stops and removes a broadcast; idempotent. Names must be 1-64 ASCII letters, digits, `.`, `_`, or `-`. |

An active stream status response has stable field names and numeric counters:

```json
{
  "network": "ace",
  "id": "<id>",
  "clients": 1,
  "peers": 4,
  "bitrate": 2450000,
  "buffer_ms": 6200,
  "uploaded": 1048576,
  "peers_served": 2
}
```

`clients` counts direct consumers of the shared byte stream; internal HLS packaging does not
inflate it. `bitrate` is bits per second, `buffer_ms` is milliseconds, and `uploaded` is bytes.

## Player and middleware integration

Point VLC or a media-server channel directly at either native playback URL:

```text
http://127.0.0.1:6878/streams/ace/<id>.ts
http://127.0.0.1:6878/streams/ace/<id>.m3u8
```

For dispatcharr-style playlist integration, generate entries using one of those URLs. No
`/ace/getstream` handshake is required. Use `GET /streams` and per-stream `/status` for
monitoring; do not poll a playback URL as a health check because that creates or joins a session.

## Experimental legacy compatibility

Legacy `/ace/*` and `/server/api` routes are disabled by default. Enable the thin adapter only for
middleware that cannot consume native URLs:

```bash
OUTPACE_EXPERIMENTAL_ACE_COMPAT=1 outpace serve
```

The flag does not change native routes. Compatibility is deliberately not a full engine clone:
account/premium, UI remote control, channel catalog, playlist/EPG, and encrypted-content features
are unsupported. The live `/ace/manifest.m3u8`, tokenized `/ace/m/...m3u8`, and
`/ace/c/<session>/<seq>.ts` adapter reuses the native live HLS packager; compatibility VOD remains
deferred. The exact supported methods and error envelopes are pinned in
[`protocol/compat-matrix.md`](protocol/compat-matrix.md).
