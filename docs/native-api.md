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
| `GET /streams` | `200 application/json`, `{"streams":[...]}` | Active shared sessions. Entries have `network`, `id`, direct-client count `clients`, and descriptor `metadata`. |
| `GET /streams/<network>/<id>` | `200 video/mp2t` streaming body | Default live playback for dot-free provider ids; starts or joins the same shared session as the explicit `.ts` form. When the descriptor supplies a title, the response includes `Icy-Name`. Dotted ids, unknown networks, and invalid ids return `404`. |
| `GET /streams/<network>/<id>.ts` | `200 video/mp2t` streaming body | Explicit continuous MPEG-TS form; equivalent for dot-free ids, preserves dots within the provider id, and includes `Icy-Name` when the descriptor supplies a title. |
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
  "peers_served": 2,
  "metadata": {
    "title": "Example Sports",
    "bitrate": 2450000,
    "categories": ["sports"]
  }
}
```

`clients` counts direct consumers of the shared byte stream; internal HLS packaging does not
inflate it. The top-level `bitrate` is the measured session rate; `metadata.bitrate` is the
descriptor's advertised rate. Rates are bits per second, `buffer_ms` is the estimated duration
currently queued on the server, and `uploaded` is bytes. In particular, `buffer_ms` is not a
decoder or player lead measurement: a client can drain the server queue faster than real time,
or maintain its own independent buffer. Metadata always has the stable `title`, `bitrate`, and
`categories` fields;
bare infohashes and descriptors without metadata return `null`, `null`, and `[]` respectively.
The descriptor title is authoritative for both `metadata.title` and `Icy-Name`; outpace does not
invent a title for a bare infohash.
Live HLS media playlists are unchanged because they have no portable stream-title field.

## Live startup buffering

Direct MPEG-TS playback now defaults to a server-resident startup reservoir with a 30-second
target (`OUTPACE_PREBUFFER_MS=30000`), a 128 MiB payload ceiling
(`OUTPACE_PREBUFFER_BYTES=134217728`), and a 15-second collection deadline
(`OUTPACE_PREBUFFER_TIMEOUT_MS=15000`). The target is media-clock duration when usable timing is
present, with the advertised bitrate as a fallback. This intentionally adds startup latency in
exchange for resilience to transient swarm jitter. The server releases early when the byte limit
or deadline is reached, so a short advertised live window degrades to the media collected so far
rather than waiting indefinitely. The byte ceiling covers queued payload; allocation metadata,
the seed store, HLS segments, and other daemon state are additional memory overhead.

Set `OUTPACE_PREBUFFER_MS=0` to disable the reservoir and retain the earlier immediate-release
behavior. The byte budget must still hold at least one 188-byte MPEG-TS packet. A nonzero target
requires a nonzero timeout.

HLS defaults to 5-second target segments (`OUTPACE_HLS_SEGMENT_DURATION_MS=5000`), an eight-segment
retained window (`OUTPACE_HLS_WINDOW_SEGMENTS=8`), six completed startup segments
(`OUTPACE_HLS_STARTUP_SEGMENTS=6`), and a 45-second playlist startup timeout
(`OUTPACE_HLS_STARTUP_TIMEOUT_MS=45000`). A new playlist request waits for the startup count or
the timeout. `OUTPACE_HLS_STARTUP_SEGMENTS=0` retains compatibility by behaving as one segment.
The generated `EXT-X-START` points near the beginning of the retained startup window, but it is
advisory: clients may choose a different starting position and maintain their own buffer policy.

## Player and middleware integration

Point VLC or a media-server channel at the extensionless native URL for direct MPEG-TS playback:

```text
http://127.0.0.1:6878/streams/ace/<id>
```

The explicit MPEG-TS and HLS forms remain available:

```text
http://127.0.0.1:6878/streams/ace/<id>.ts
http://127.0.0.1:6878/streams/ace/<id>.m3u8
```

For dispatcharr-style playlist integration, generate entries using one of these URLs. No
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

Compatibility `getstream`, `manifest` JSON, and `stat` responses expose the same nested metadata
object. Direct and tokenized MPEG-TS playback also emits `Icy-Name` when a title is known.
