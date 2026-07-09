# Acestream HTTP compatibility matrix

outpace keeps a **thin, opt-in** Acestream-compatible HTTP surface. The native `/streams` and
`/broadcast` APIs are the supported primary surface; the legacy `/ace/*` and `/server/api` routes
exist only to interoperate with existing Acestream middleware. This document is the contract: what
is supported, what is intentionally rejected, and what is deferred.

The whole compatibility surface is gated behind `OUTPACE_EXPERIMENTAL_ACE_COMPAT=1`
(`experimental_ace_compat`) and is disabled by default. When the gate is off, every route below
returns `404`.

Two envelope shapes are in play (a real engine quirk, not an outpace choice):

- `/ace/*` playback routes wrap their payload under a **`response`** key.
- `/server/api` wraps its payload under a **`result`** key, always with HTTP `200` and any error
  reported in-band under `error` (see `docs/protocol/notes/10-interop.md`, which proves
  `analyze_content` → `.result.infohash`).

## `/ace/*` playback routes

| Route | Status | Notes |
| --- | --- | --- |
| `GET /ace/getstream` | Supported | Selectors `content_id` > `infohash` > `url` > `magnet`. Returns playback/stat/command URLs. |
| `GET /ace/r/<id>/<token>` | Supported | Playback byte stream for a started session. |
| `GET /ace/stat/<id>/<token>` | Supported | Session stats under `.response`. |
| `GET /ace/cmd/<id>/<token>?method=stop` | Supported | Only `stop` is honored; other methods return an error envelope. |
| `GET /ace/manifest.m3u8` | Deferred | Parsed by `routes.rs` but not served. Native `/vod/<net>/<id>/manifest.m3u8` HLS is the supported manifest surface. |
| `GET /ace/c/<session>/<seq>.ts` | Deferred | HLS segment counterpart to `manifest.m3u8`; deferred with it. |

## `/server/api` control methods

Dispatched on `?method=`; the response envelope is `{ "result": <value>, "error": <null|message> }`.

| Method | Status | Result fields | Notes |
| --- | --- | --- | --- |
| `get_version` | Supported | `version`, `code` | `version` is the crate version; `code` packs `MAJOR*10000 + MINOR*100 + PATCH`. |
| `get_status` | Supported | `status`, `active_sessions` | `status` is `dl` when any session is active, else `idle`. |
| `get_network_connection_status` | Supported | `status`, `connected`, `networks` | `connected` reflects whether any provider network is registered. |
| `analyze_content` | Supported | `infohash`, `content_id`, `is_live`, `is_encrypted`, `status` | Resolves the content selector to its infohash. `infohash`/`magnet` selectors resolve offline; `content_id`/`url` need the live catalog/transport and are gated by the same switch as `/ace/getstream` content-id resolution. |
| `get_content_id` | Supported (echo only) | `content_id` | Echoes a caller-supplied `content_id`/`query`. It cannot derive a content id from a bare infohash/url and returns an error envelope in that case. |
| `get_media_files` | Supported (best-effort) | `infohash`, `files[]` | outpace transports are single-file, so one media file is reported, keyed by infohash. `dump_transport_file` is not supported. |

### Selector parameters (`analyze_content`, `get_content_id`, `get_media_files`)

Same precedence as `/ace/getstream`: `content_id` (or its alias `query`) > `infohash` > `url` >
`magnet`. A malformed or missing selector returns an error envelope, never an HTTP error status.

## Intentionally unsupported

These are **non-goals** for outpace (see the epic #46 non-goals) and are not planned:

- Premium / account / auth / subscription / DAO / encrypted-premium methods.
- Player / remote-control UI methods and the bundled web UI.
- Playlist / EPG / channel-catalog methods (e.g. `get_available_channels`, playlist `.m3u`/XML
  endpoints). A caller hitting an unknown `/server/api` method gets
  `{ "result": null, "error": "unknown method: <name>" }`.

## Deferred (possible future work)

- `/ace/manifest.m3u8` + `/ace/c/<session>/<seq>.ts` HLS session routes (native `/vod` HLS exists).
- `get_media_files&dump_transport_file=1` raw transport-file dumping.
- Reverse `get_content_id` (deriving a content id from an infohash/transport).
