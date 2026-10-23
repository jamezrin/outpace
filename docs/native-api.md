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
| `PUT /broadcast/<name>` | `200 application/json` | Exclusively admits raw MPEG-TS ingest. Returns `name`, `content_id`, and `infohash` immediately while an owned task consumes the request body. |
| `PUT /broadcast/<name>/source` | `202 application/json` | Attaches one managed HTTP(S) MPEG-TS source; returns `name`, `content_id`, `infohash`, `source`. Acceptance is independent of upstream health. |
| `GET /broadcast/<name>/source` | `200 application/json` | Returns exactly `name`, `source`; never creates a broadcast. |
| `DELETE /broadcast/<name>/source` | `204` | Cancels/joins the source only, retaining identity/store/announcements. Detached or unknown is idempotent. |
| `GET /broadcast/<name>` | `200 application/octet-stream` | Returns the broadcast transport descriptor; `404` until minted. |
| `DELETE /broadcast/<name>` | `204` | Stops ingress/announcements and deletes identity after persistence settles; unknown names are idempotent. Names must be 1–64 ASCII letters, digits, `.`, `_`, or `-`, excluding `.` and `..`. |

A broadcast accepts one producer at a time across raw PUT, RTMP and managed HTTP sources. A competing publisher
receives `409` / `producer_conflict`; separate names can ingest concurrently. Sequential
producers retain the signing identity and continue piece numbering. RTMP keys use the exact
same name rules, without whitespace trimming. Ordinary PUT EOF/body termination and RTMP
unpublish sign the buffered finite tail once. Deletion, shutdown and fatal ingest failures
discard that tail. A short signed final piece retains the existing fixed-geometry consumer
limitation; it is not guaranteed playable by consumers that require full pieces.

New mutation failures are JSON with exactly `{"error":{"code":"..."}}`:

| Status | Code |
| --- | --- |
| `400` | `invalid_name` |
| `404` | `broadcasts_disabled` |
| `409` | `producer_conflict`, `broadcast_stopping` |
| `500` | `broadcast_persistence_failed`, `broadcast_state_invalid`, `piece_index_exhausted` |
| `503` | `broadcast_service_stopping`, `broadcast_cleanup_timeout` |

DELETE returns `204` only after broadcast work has stopped, both seed registrations have been
evicted and the identity record has been deleted under the platform guarantees below. While
stopping, descriptor GET is `404` and new producers receive `409`. Cleanup has a shared
5-second grace. A blocked filesystem operation retains the stopping name and its I/O fence;
`503` does not authorize reuse. The owned deletion may complete later; repeat DELETE to observe
it. A settled disk failure returns `500` and requires an explicit DELETE retry. A successfully
deleted name mints a fresh identity on its next admission. A raw admission already answered
`200` cannot report a later ingest failure through that response; such failures stop its
session and log a fixed error code.

Before publication, the daemon saves exclusive reservations in blocks of 128 piece indexes.
Restart skips the entire last reservation, even after a very short ingest; repeated restarts
therefore create intentional forward gaps rather than reusing signed indexes. Unfinished
pieces are also skipped. Invalid/unreadable identity records fail closed instead of silently
minting over the same name. Shutdown preserves signing identities and reservations.

Linux saves use a private exclusive same-directory temporary file, complete write, file sync,
replacement and directory/new-link sync. macOS uses file `sync_all` (full-sync) and separate
directory `fsync`. These depend on the filesystem/storage honoring the requested operations;
they are not universal power-loss certification. Windows preserves process-crash/ordinary
daemon restart safety while OS/filesystem state survives, using checked write, file sync,
replacement and deletion without requiring directory sync. Windows system crash/power loss
can roll back reservations, lose identities or resurrect deleted records; DELETE has the same
limitation. Other filesystems without established namespace/flush semantics have the same
strong-guarantee limitation. See [deployment storage/rollback guidance](deployment.md#upgrade-rollback-and-removal).

RTMP cleanup fences broadcast operations and waits for the per-publisher mux/ingest/finish
barrier. Runtime cleanup also stops/joins its owned RTMP listener. The dependency owns
established connection tasks: they may remain connected, but canceled-session callbacks cannot
publish, finish tails or mutate a replacement publisher. New publish requests are rejected
after service admission closes. A publishing connection canceled by full deletion must reconnect
to publish that name again; ordinary unpublish may republish on the same connection.

## Managed HTTP MPEG-TS sources

The source routes are native routes and remain available when Ace compatibility is off. They
use the existing native listener/exposure policy; protect broadcast mutation access with the
same firewall/authenticating proxy as raw ingest. They add no new authentication framework.
Names are 1–64 ASCII letters/digits/`.`/`_`/`-`, excluding `.` and `..`.

A PUT body requires `application/json` (JSON structured suffixes are also accepted), is at
most 16 KiB, and contains exactly this schema. `url` is required; timings default as shown:

```json
{
  "url": "https://source.invalid/live.ts?token=SYNTHETIC_TOKEN",
  "connect_timeout_secs": 5,
  "read_timeout_secs": 15,
  "reconnect_initial_secs": 1,
  "reconnect_max_secs": 30
}
```

The URL is synthetic. Durations must be integer seconds: connect 1–60, read 1–300, and both
reconnect values 1–300 with initial <= maximum. Reject null/float/negative/zero durations,
duplicate keys, unknown fields, userinfo (even empty), fragments, controls, non-HTTP(S)
schemes, missing hosts, port zero and URLs longer than 8,192 bytes. Literal address policy
is checked synchronously without DNS, before identity/producer admission. Hostname resolution
happens in the worker. Timings and normalized parsed URL determine idempotency: host case
and default-port normalization apply; path/query ordering and token values remain significant.

`OUTPACE_BROADCAST_SOURCES` declares a JSON object mapping names to those objects (default
`{}`). It allows at most 64 entries and 256 KiB UTF-8. Empty/malformed text and duplicate names
fail startup. Programmatic configuration passes the same validation. All declarations validate
before filesystem/identity/runtime effects, then attach in name order after the HTTP listener
and owned RTMP service start, before API traffic. A down/blocked origin is accepted; a local
identity/reservation failure unwinds startup, including earlier workers and HLS cleanup.

API attachment specifications are process-local and are never written to disk. Current
environment declarations reattach on each startup. Detach/full delete suppress a declaration
only until restart; remove it from the environment to keep it absent. API-only sources stay
detached after restart although persisted descriptors/signing identities survive. There is no
live environment reload, durable job specification or automatic source replacement.

With a controlled MPEG-TS origin already listening on loopback port 8080, start the daemon
with the independent operator opt-in, then issue these executable requests:

```bash
OUTPACE_ALLOW_NON_GLOBAL_BROADCAST_SOURCE=1 outpace serve
# In a second terminal:
curl --fail-with-body -X PUT http://127.0.0.1:6878/broadcast/demo-source/source \
  -H 'Content-Type: application/json' \
  --data '{"url":"http://127.0.0.1:8080/live.ts"}'
curl --fail-with-body http://127.0.0.1:6878/broadcast/demo-source/source
curl --fail-with-body -X DELETE http://127.0.0.1:6878/broadcast/demo-source/source
```

PUT returns `202` with exactly `{name, content_id, infohash, source}` after durable admission
and owned worker registration; this does not establish health. Identical normalized/default-filled
configuration returns the same attachment/identity/current attempts, even in backoff or failed
state. Changed URL/token/timing or a competing producer conflicts; detach before replacement.
GET returns `200` with exactly `{name, source}`, or `404` / `source_not_found`. Source detach
waits for cancellation and worker/publication cleanup; concurrent deletes share the stop. It
keeps descriptor, signing identity, cached seed lease/store and one announcement pair, and
does not stop raw PUT/RTMP. Full broadcast DELETE stops all producers/announcements, removes
the seed lease and durably deletes identity. During full deletion descriptor GET is `404`,
source GET may still show `stopping`, and new admissions get `broadcast_stopping`.

`source` always has exactly four fields, with no URL or upstream diagnostics:

```json
{"state":"backoff","attempts":2,"last_error":"upstream_eof","retry_in_ms":950}
```

| State | Meaning |
| --- | --- |
| `connecting` | Opening an attempt, including DNS; headers alone do not establish streaming. |
| `streaming` | Authorized ingest emitted aligned TS; this is alignment, not codec validation. |
| `backoff` | Waiting to retry a transient upstream failure or finite EOF. |
| `blocked` | DNS resolved only disallowed addresses; waiting to re-resolve/revalidate. |
| `failed` | Terminal local ingest/persistence/worker failure; keeps the exclusive attachment. |
| `stopping` | Canceling/joining work; producer authority remains fenced until cleanup settles. |

`attempts` saturates at `u64::MAX` and increments when each attempt (including DNS) begins.
`last_error` is nullable and clears on the first aligned TS of a new attempt. `retry_in_ms`
is nullable and reports monotonic remaining delay only in `backoff`/`blocked`.

Retryable status codes are `dns_timeout`, `dns_failed`, `address_blocked`, `connect_timeout`,
`connect_failed`, `tls_failed`, `headers_timeout`, `redirect_rejected`, `http_status`,
`unsupported_response`, `body_failed`, `body_frame_too_large`, `read_timeout`,
`progress_timeout`, `upstream_eof`. Terminal codes are `persistence_failed`,
`piece_index_exhausted`, `ingest_failed`, `worker_failed`. A terminal failure stays attached
without retry. Detach/reattach requests fresh admission, but a poisoned persistence generation
remains closed until safe reload on restart or successful full deletion.

Source errors and newly introduced mutation failures use `application/json` and exactly
`{"error":{"code":"..."}}`. Source names validate first, followed by disabled support,
service shutdown and per-name stopping, before any mutation:

| HTTP status | Fixed codes |
| --- | --- |
| `400` | `invalid_name`, `invalid_source_config`, `invalid_source_url`, `source_address_blocked` |
| `404` | `broadcasts_disabled`, `source_not_found` |
| `409` | `producer_conflict`, `source_conflict`, `broadcast_stopping` |
| `413` | `source_request_too_large` |
| `415` | `source_content_type_required` |
| `500` | `broadcast_persistence_failed`, `broadcast_state_invalid`, `piece_index_exhausted` |
| `503` | `broadcast_service_stopping`, `broadcast_cleanup_timeout` |

Only exact `OUTPACE_ALLOW_NON_GLOBAL_BROADCAST_SOURCE=1` expands the source address policy;
all other strings deny. This operator switch is separate from transport/tracker opt-ins and
cannot be supplied in source JSON. The established denylist rejects IPv4 private/loopback,
CGNAT, link-local, unspecified, broadcast, documentation and multicast, and IPv6 loopback,
unspecified, multicast, ULA and link-local; mapped IPv4 follows IPv4. The opt-in adds only
private/loopback/CGNAT IPv4 and ULA/loopback IPv6 (including mapped equivalents). It never
unblocks link-local metadata services, unspecified, multicast, IPv4 broadcast/documentation.
Other special ranges retain the existing classifier; this is not a complete IANA classifier.

Each attempt resolves afresh with a fixed 5s DNS limit, chooses/pins one permitted candidate,
and preserves original Host/TLS SNI and certificate verification. There is no previous-address
fallback when DNS becomes blocked. Sources use fresh HTTP/1 clients, no proxies (including
environment/system proxies), redirects, hidden retries, compression, Range resume, forwarded
headers, cookies or custom authorization headers. HTTPS retains bundled roots and normal
hostname checks. Open/header completion is capped at 15s after DNS; configured connection
limits cannot extend it. Accept body-bearing 2xx except 204/205/206; reject partial semantics,
upgrades and nonidentity encoding. Generic/missing MIME is allowed, with TS progress required.
The header acceptance limits are 64 headers and 32 KiB parsed name/value bytes; the locked
HTTP/1 parser also has a finite 417,792-byte read-buffer bound (not a total heap guarantee).

Read idle measures nonempty body data while waiting upstream; empty frames do not reset it.
Aligned TS must progress within 30s, so junk trickle cannot keep a source alive. A local ingest
operation has a 15s terminal limit and never releases a still-active disk fence. Retries use
`min(maximum, initial * 2^k)` from `k=0`, with uniform integer milliseconds in
`[ceil(0.9 * base_ms), base_ms]`. Only 30s of streaming with at least one committed complete
piece resets backoff. Policy denials and clean finite Content-Length EOF also wait and retry.

Every attempt creates fresh resync/signing state at the shared cursor. Pull EOF/error/cancel
never calls finite `finish()`; incomplete TS/signing tails are discarded and exposed piece
indexes are skipped permanently. Completed signed pieces survive, but timestamp/PAT/PMT
continuity across origin restarts is not promised. Reads/ingest are sequential with backpressure,
frames <=256 KiB and feeds <=64 KiB; retained application buffers are one frame, resync tail,
signing payload and one slice's output vector. There is no lifetime body-size limit or claim
about total HTTP/TLS heap. No source URL/query token, address, upstream body/error chain,
storage path or key appears in status/logs/Debug or persisted broadcast records. Operational
source logs contain only validated name/state/fixed codes, throttled to 30s plus one terminal
event. See [deployment environment handling](deployment.md#managed-source-declarations) and
the reservation/storage/platform/RTMP guarantees above before backing up or rolling back.

## Stream status

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
