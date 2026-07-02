# 45 - `content_id` now resolves through the signed catalog

Date: 2026-07-02.

Note 44 fixed the `/ace/getstream?content_id=` route shape but still depended on
BEP-9 `ut_metadata` peers for the content-id -> transport-file step. Live smoke failed
there: DHT found metadata peers, but the reachable peer did not advertise `metadata_size`.

The official engine does not rely on that path for this public id. A fresh official
3.2.11 sandbox resolves:

- content_id: `cid1`
- infohash: `50e93529d3eb46a50506b14464185a15292d6e47`
- raw transport SHA1/cache key: `34df422b80a4bd94ac1e51be9ede60364ec7a7dd`

The official `get_media_files&dump_transport_file=1` transport bytes match
`tests/vectors/transport-01.bin` exactly.

## Catalog request

Packet capture and Python-level `hashlib.sha1` wrapping showed the official engine calls:

```text
GET /gettorrent?_n=3.2.11&_p=linux&_r=<random>&_v=3021100&pid=<content_id>&_s=<sig>
Host: 5.252.161.191:8081
```

The request signature is:

```text
SHA1("_n=3.2.11#_p=linux#_r=<random>#_v=3021100#pid=<content_id>" + secret).hexdigest()
```

Important transcription detail: `secret` contains literal backslash escape text such as
`\\x0b`, `\\n`, and `\\t`, not control bytes. The regression vectors now lock that down:

- `_r=4245094384320117676` -> `_s=ee6c8422795ced4cd5c1fb80f10276801ab636c6`
- `_r=2142790664659308268` -> `_s=08c4ba548457e883f8711d3f3cb846c20058d912`
- `_r=3472462845767567311` -> `_s=c7c3cd3c7268be1a48144653cdbd7912991cb337`

The endpoint returns XML containing base64 `torrent` bytes and a hex `checksum`.
Outpace now decodes the base64, verifies `SHA1(transport_bytes) == checksum`, and then
uses the existing transport decoder/infohash path.

## Implementation

- `ace_swarm::resolve::resolve_via_catalog(content_id)` signs and fetches the official
  catalog response, handles raw HTTP including chunked bodies, verifies the checksum, and
  returns `StreamInfo`.
- `AceProvider::resolve_content_id` tries the catalog first, then falls back to the old
  peer `ut_metadata` resolver.
- Production `/ace/getstream?content_id=` now resolves the id before responding and returns
  official-shaped JSON/URLs keyed by the resolved infohash. Internally it records an alias
  from that public infohash URL id back to `cid:<content_id>`, so playback still opens via
  the catalog-derived `StreamInfo` instead of losing transport trackers/geometry by using
  the bare-infohash fallback.

## Verification

Automated:

```text
cargo test -p ace-swarm --lib resolve::tests::
cargo test -p ace-engine --lib http::tests::
cargo test -p ace-engine
```

Live smoke against the Synthetic Live Channel public id on `127.0.0.1:6900`:

```text
GET /ace/getstream?format=json&content_id=cid1
```

returned:

```text
infohash=50e93529d3eb46a50506b14464185a15292d6e47
playback_url=http://127.0.0.1:6900/ace/r/50e93529d3eb46a50506b14464185a15292d6e47/outpace
```

Following that playback URL:

```text
http_code=200
time_starttransfer=0.369436
size_download=8308472
```

Daemon log proved the public infohash URL used the internal content-id resolver path:

```text
[ace] resolved cid:cid1 via catalog -> infohash 50e93529d3eb46a50506b14464185a15292d6e47
[ace] open cid:cid1: discovered 14 peer(s)
```

The saved sample was `8,308,472` bytes, 188-byte aligned from offset 0, and `ffprobe`
identified H.264 video plus AAC audio.

This resolves the content-id startup blocker from note 44. It does not change the separate
Synthetic Live Channel continuity limitation documented in notes 41-43: the currently reachable upstream
still serves only the initial window and then advertises a stale window behind the next
needed piece.
