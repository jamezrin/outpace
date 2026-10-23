# Native CLI and HTTP API

The native CLI and HTTP routes are outpace's supported integration surface. They are suitable for
players such as VLC, media servers such as Jellyfin, and playlist/proxy tools such as dispatcharr.
Playback requests start and share sessions, while status routes observe sessions already active.

Examples use `http://127.0.0.1:6878` and the built-in `ace` network. On live
`/streams/ace/` routes, a bare 40-character hex `<id>` is a **content id**, matching
compat `id=`. `cid:<content-id>` is its explicit alias; `ih:<infohash>` explicitly
selects a swarm with a verified descriptor (see [Infohash inputs](#infohash-inputs)).
Transport-url ids (`turl-…`) remain supported. VOD requires `cid:` or a transport-url id.

Hex values are normalized to lowercase. Bare content ids and their `cid:` aliases
share one session across TS, HLS, segment, status and DELETE routes. Native session
lists, status responses and HLS URLs return canonical `cid:` or `ih:` identifiers.
Malformed explicit hex selectors return `400`; status and segment requests never
start sessions. Other provider networks keep their own identifier semantics.

**Migration:** native live bare hex ids previously meant infohashes. Existing
infohash playlists must now use `ih:<infohash>`; there is no implicit infohash
fallback after a content-id resolution failure. Compat `infohash=`/`magnet=` and
CLI inputs retain their existing meanings.

## CLI

| Command | Behavior |
| --- | --- |
| `outpace serve` | Runs the native HTTP API, RTMP ingest, and enabled peer services. With no command, outpace defaults to `serve`. |
| `outpace play <input>` | Writes live MPEG-TS bytes to stdout. Diagnostics go to stderr, so redirection and piping are safe. Accepts `acestream://`, `acestream:?`, `magnet:`, and HTTP(S) transport-file inputs; infohash and magnet inputs fail closed (no verified descriptor in a one-shot process). |
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
| `GET /streams/<network>/<id>` | `200 video/mp2t` streaming body | Default live playback for dot-free provider ids; starts or joins the same shared session as the explicit `.ts` form. When the descriptor supplies a title, the response includes `Icy-Name`. Dotted ids, unknown networks, and invalid ids return `404`. An `ih:` selector without a verified descriptor returns `422` with a plain-text reason. Malformed `cid:`/`ih:` selectors return `400`. |
| `GET /streams/<network>/<id>.ts` | `200 video/mp2t` streaming body | Explicit continuous MPEG-TS form; equivalent for dot-free ids, preserves dots within the provider id, and includes `Icy-Name` when the descriptor supplies a title. |
| `GET /streams/<network>/<id>.m3u8` | `200 application/vnd.apple.mpegurl` | Starts or joins live HLS packaging and returns a sliding playlist. The same `422` applies. |
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
  "recovery": {
    "upstream_reconnects": 0,
    "stale_teardowns": 0,
    "skipped_evicted_gaps": 0,
    "signature_rejects": 0,
    "active_upstreams": 4
  },
  "metadata": {
    "title": "Example Sports",
    "bitrate": 2450000,
    "categories": ["sports"]
  }
}
```

`clients` counts direct consumers of the shared byte stream; internal HLS packaging does not
inflate it. The top-level `bitrate` is the measured session rate; `metadata.bitrate` is the
descriptor's advertised rate converted from its raw bytes-per-second unit. Both JSON rates
are bits per second (conversion saturates at the unsigned 64-bit maximum). `buffer_ms` is the
estimated duration currently queued on the server, and `uploaded` is bytes. In particular, `buffer_ms` is not a
decoder or player lead measurement: a client can drain the server queue faster than real time,
or maintain its own independent buffer. Metadata always has the stable `title`, `bitrate`, and
`categories` fields;
descriptors without metadata return `null`, `null`, and `[]` respectively.
The descriptor title is authoritative for both `metadata.title` and `Icy-Name`; outpace does not
invent a title.
Live HLS media playlists are unchanged because they have no portable stream-title field.

## Content-id resolution

Live and VOD content-id inputs resolve through the catalog by default. Catalog failure
returns an error; automatic BEP-9 peer fallback is disabled. The same daemon can resolve
its own broadcast content ids using its trusted local broadcast registry.

Set `OUTPACE_BEP9_CONTENT_ID_FALLBACK=1` on the receiving daemon or standalone `play`
process to enable BEP-9 fallback after catalog failure. This supports the raw wrapped
transport-file hash convention used by outpace broadcasts. The peer result must satisfy
`SHA1(raw wrapped transport bytes) == requested identifier`; this is not an established
public catalog content-id derivation. Peer results stay outside the catalog-provenance
infohash index and do not authorize later bare-infohash playback.

The broadcast Ace link needs this opt-in when used across daemons or by standalone `play`.
Alternatively, use `http://<host>:<http-port>/broadcast/<name>` as a transport URL, subject
to the receiver's transport-URL access policy. Same-daemon broadcast playback needs no
opt-in.

Transport-file URL fetching explicitly ignores environment and system proxies and never
follows redirects. Its independent `OUTPACE_ALLOW_NON_GLOBAL_TRANSPORT=1` opt-in stays
narrow: private/loopback/CGNAT targets are permitted, while link-local, unspecified,
multicast, broadcast and documentation addresses remain blocked. Fetches retain a 1 MiB
body ceiling, a 5-second connect timeout and a 15-second finite request deadline.

See [protocol note 63](protocol/notes/63-bep9-content-id-policy.md) for evidence,
trust boundaries and migration details.

## Infohash inputs

Catalog resolution verifies the response signature, request nonce and transport checksum.
The engine-compatible signature uses an embedded shared key rather than a server-only trust
anchor; [protocol note 62](protocol/notes/62-catalog-response-authentication.md) describes the
remaining forgery risk over plain HTTP.

An `ih:<infohash>` selector first uses an owned broadcast or an indexed verified
descriptor, then queries the signed catalog on a cold lookup. Authentication,
nonce and checksum validation precede decoding; the descriptor's computed swarm
infohash must equal the requested hash before its geometry, pubkey and trackers
are used. The descriptor is indexed only after that check. Failed cold lookups
are cached for five seconds in a separate bounded 128-entry cache; verified local
metadata takes precedence. Unavailable or mismatched descriptors return
`422 Unprocessable Content` with a plain-text reason. BEP-9 and transport-URL
results do not populate this index. Native bare hex values still mean content ids;
use `ih:` explicitly for swarm infohashes.

## Live statistics

Top-level `bitrate` measures contiguous reassembled media payload in bits/second,
not the descriptor's advertised bitrate. A nominal five-second window is sampled
once per second independently of status polling. Reads do not alter the rate;
inactivity expires it even while source output is pending. Sampling limits
boundary precision to about one second. Framing, signatures, duplicate blocks and
injected tables are excluded. Existing `downloaded` totals, where exposed, still
count delivered MPEG-TS, so their delta need not equal this incoming-payload rate.

`buffer_ms` observes the startup reservoir while collecting/draining, using PCR
or the existing descriptor-rate estimate. After release it observes contiguous
complete media at the reassembler cursor; partial pieces and pieces beyond a
hole do not count. It excludes the provider handoff channel, socket buffers,
session broadcast backlog, HLS retention and all client buffers. It can correctly
approach zero after draining and must not be interpreted as player lead.

`livepos` is null until a position is known. Otherwise `pos` is the next piece
needed by reassembly, `last` the last observed provider head, `live_first` and
`live_last` the advertised active-window bounds (null when unavailable), and
`buffer_pieces` the complete contiguous pieces ready at that cursor. `is_live` is
1; `first_ts` and `last_ts` are null because no verified mapping to engine
wall-clock timestamps is available. These observations are not a client's
playhead or an authenticated source clock. Other providers without independent
readers retain their existing post-output cached statistics behavior.

`recovery` is null for providers without recovery observations. Observed live
sources expose all five numeric fields from session creation, initially zero.
Counters last for one source session, survive upstream pool rebuilds, and reset
for a new source. Teardown clears active coverage and `active_upstreams` while
retained snapshots preserve historical counters.

- `upstream_reconnects`: successful runtime admissions after the first admitted
  pool has ended, once per admitted transport, including reuse of an address.
  Initial admissions, ordinary expansion of the initial pool, failed attempts,
  gossip-only transports and provisional startup replacement do not count.
- `stale_teardowns`: actual stale-budget pool shutdowns returning `PoolStale`,
  once per shutdown; individual peer losses do not count.
- `skipped_evicted_gaps`: actual forward cursor jumps in reconnect resume or
  evicted-gap recovery, once per gap regardless of piece count. Initial prefetch
  and live-head catchup pruning do not count.
- `signature_rejects`: assembled pieces whose source signature verification
  returns false. A retried piece can fail again. Malformed blocks, trailer split
  failures, invalid keys, stale/future pieces and duplicates do not count.
- `active_upstreams`: currently admitted peer runtimes, including choked and
  provisional runtimes; connecting, queued and gossip-only transports are excluded.
  Every pool exit, cancellation and source teardown clears this gauge.

Compatibility `/ace/stat` keeps its established fields and mappings unchanged;
these recovery observations are exposed only by native status.

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

## Supported Ace HTTP subset

Legacy `/ace/*`, `/server/api`, and `/webui/api/service` routes are disabled by default.
Enable the supported adapter subset for middleware that cannot consume native URLs:

```bash
OUTPACE_ACE_COMPAT=1 outpace serve
```

Set `OUTPACE_ACE_COMPAT=0` to disable it explicitly. The canonical variable wins whenever
present, including false; `OUTPACE_EXPERIMENTAL_ACE_COMPAT` is a deprecated fallback only
when it is unset. The selected value must be exactly `1`, `true`, `0`, or `false`;
empty, malformed, and non-Unicode selected values fail startup. Unused legacy values
are ignored. Rename the old variable when migrating and restart the daemon to apply changes
(recreate the service for Compose).

The flag does not change native routes. Compatibility is deliberately not a full engine clone:
account/premium, UI remote control, channel catalog, playlist/EPG, and encrypted-content features
are unsupported. The live `/ace/manifest.m3u8`, tokenized `/ace/m/...m3u8`, and
`/ace/c/<session>/<seq>.ts` adapter reuses the native live HLS packager; compatibility VOD remains
deferred. The exact supported methods and error envelopes are pinned in
[`protocol/compat-matrix.md`](protocol/compat-matrix.md).

Compatibility `getstream`, `manifest` JSON, and `stat` responses expose the same nested metadata
object. Compatibility direct and tokenized MPEG-TS playback do not emit `Icy-Name`;
the native TS routes described above include it when the descriptor supplies a title.
Compatibility live HLS follows the existing global `OUTPACE_HLS_AUDIO_NORMALIZE` policy;
per-request transcoding hints do not configure conversion.
