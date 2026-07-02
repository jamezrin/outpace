# 34 — Acestream-compatible getstream by content_id is wired

Follow-up to note 33. The wire path was proven, but the daemon still exposed only the clean
`/streams/...` API. An Acestream-style client that expects the engine's HTTP surface would
call `/ace/getstream?format=json&content_id=...` and receive 404 from outpace, even
though the same bytes were playable through `/streams/ace/<id>.ts`.

## Implemented surface

`ace-engine` now mounts the minimal compatibility routes:

- `GET /ace/getstream?format=json&content_id=<id>`
- `GET /ace/r/<id>/<token>`
- `GET /ace/stat/<id>/<token>`
- `GET /ace/cmd/<id>/<token>?method=stop`

Selector priority matches the existing pure route parser: `content_id` wins, then
`infohash`, then `id`.

The important playback choice is that `content_id` is passed directly to the provider as
the session/swarm key. It is not rewritten to `cid:<id>`. Note 20 proved the known-good live
content id works directly as the live swarm key, while the `cid:`/`ut_metadata` path can be
blocked by peers that advertise `ut_metadata` but omit `metadata_size`.

`/ace/getstream` returns an Acestream-shaped JSON envelope with:

- `.error = null`
- `.response.infohash = <selected id>`
- `.response.playback_url = http://host/ace/r/<id>/outpace`
- `.response.stat_url = http://host/ace/stat/<id>/outpace`
- `.response.command_url = http://host/ace/cmd/<id>/outpace`
- `.response.playback_session_id = "outpace"`
- `.response.is_live = 1`
- `.response.is_encrypted = 0`

Playback starts lazily when the returned `/ace/r/...` URL is opened. `/ace/stat/...` reports
`status="idle"` before playback and `status="dl"` for a running session. `/ace/cmd/...stop`
removes the same `StreamManager` session, so the background download is torn down.

## Regression coverage

Added HTTP tests covering:

- `/ace/getstream?content_id=...` returns a playback URL whose `/ace/r/...` body streams
  MPEG-TS from the fixture provider;
- `/ace/stat/...` tracks idle vs running state for the direct content-id session;
- `/ace/cmd/...?...method=stop` stops that session.

Targeted verification:

```bash
cargo test -p ace-engine 'ace_' -- --nocapture
```

## Current limitations

This is not full byte-for-byte HTTP API parity with the official engine:

- the compatibility token is static (`outpace`);
- `downloaded` in `/ace/stat` is still a placeholder;
- `/ace/manifest.m3u8`, `/ace/c/...` HLS compatibility paths, `/server/api`, and
  `getstream?url=<transport-url>` are not wired into axum yet;
- for `content_id=...`, `.response.infohash` is the selected direct playback key, not
  necessarily a descriptor-derived transport infohash.

Those do not block Acestream-style content-id playback through the returned `/ace/r/...`
URL.
