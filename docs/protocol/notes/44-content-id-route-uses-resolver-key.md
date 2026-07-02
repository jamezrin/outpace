# 44 - `/ace/getstream?content_id=` now enters the `cid:` resolver path

Follow-up to notes 34 and 43.

## Correction

Note 34 wired the Acestream-compatible HTTP surface, but it made one wrong routing choice:
it passed `content_id=<40hex>` through as the direct playback/session key. That only works
when the caller has already supplied a BT swarm infohash. It is not correct for real
`acestream://` ids: the official engine maps the known public content id
`cid1` to swarm infohash
`50e93529d3eb46a50506b14464185a15292d6e47` (confirmed again in note 43).

`ace-engine` now keeps those two cases distinct:

- `content_id=<id>` returns `/ace/.../cid:<id>/outpace` URLs, so playback calls
  `AceProvider::open("cid:<id>")` and uses `resolve_content_id` / BEP-9 `ut_metadata`.
- `infohash=<id>` and `id=<id>` still return raw `/ace/.../<id>/outpace` URLs and are
  treated as direct swarm infohashes.
- Public JSON fields still report the caller-facing id rather than leaking the internal
  `cid:` prefix. In particular, `/ace/stat/cid:<id>/outpace` reports
  `.response.infohash = <id>` for now. The exact official behavior would be to expose the
  resolved infohash after metadata resolution, but the current manager/session API does not
  surface that value back to the HTTP handler yet.

Regression coverage:

- `ace_getstream_content_id_returns_a_playback_url_that_streams` now asserts the returned
  playback URL uses `/ace/r/cid:<content_id>/outpace` and that the manager has only the
  `cid:<content_id>` session, not a bare content-id session.
- `ace_stat_and_stop_track_content_id_session` covers idle/running/stop for the same
  `cid:<content_id>` key while keeping the public `infohash` field prefix-free.

Verified:

```bash
cargo test -p ace-engine ace_getstream_content_id -- --nocapture
cargo test -p ace-engine ace_stat_and_stop_track_content_id_session -- --nocapture
cargo test -p ace-engine
```

## Live smoke

Daemon:

- `OUTPACE_BIND=127.0.0.1:6900`
- `OUTPACE_DATA_DIR=/tmp/outpace-cid-smoke`

`GET /ace/getstream?format=json&content_id=cid1`
returned the expected URL shape:

```text
playback_url=http://127.0.0.1:6900/ace/r/cid:cid1/outpace
stat_url=http://127.0.0.1:6900/ace/stat/cid:cid1/outpace
command_url=http://127.0.0.1:6900/ace/cmd/cid:cid1/outpace
```

Opening the playback URL with a 45 s curl timeout returned `HTTP 404` after
`37.598872` s and zero media bytes. The daemon log proves the request reached the metadata
resolver:

```text
[dht] seeded 4 bootstrap node(s)
[ace] resolve cid:cid1: 14 metadata peer(s)
[ace] resolve 85.87.156.75:8621: peer sent no metadata_size
```

Interpretation: the HTTP compatibility bug is fixed, but this public target still does not
prove live `content_id` startup through `ut_metadata`. The next blocker is metadata
resolution quality/coverage for real public content ids: either find peers that provide
`metadata_size`, add another official-equivalent metadata source, or expose a pre-resolved
content-id→infohash cache path. For direct regression smoke on this known target, using
`infohash=50e93529d3eb46a50506b14464185a15292d6e47` still exercises the already-proven
swarm playback path.
