# Acestream HTTP support contract

This is the canonical operator contract for Outpace's opt-in Acestream HTTP adapter.
Prefer the [native CLI and HTTP API](../native-api.md) for new integrations. The adapter
supports public, unencrypted **live** playback and a small JSON control subset; it does
not claim full engine or application parity. Native VOD is a separate `/vod` surface.

## Enable and advertise the adapter

The gate is **off by default**. Enable it on the serving daemon:

```bash
OUTPACE_EXPERIMENTAL_ACE_COMPAT=1 cargo run -p ace-engine --bin outpace -- serve
```

With the gate off, all routes in this document return HTTP 404. Enabling it does not
change the bind address or add engine access-token authentication. Caller `pid`, input
`token`, `use_api_events`, and unknown query hints are inert; authorization of minted
playback/control URLs uses Outpace's own path token.

Returned playback, stat, command and HLS segment URLs use `http://` plus the current
request's `Host` header, falling back to `127.0.0.1:6878` if absent or unusable.
`Forwarded` and `X-Forwarded-*` are not consulted. There is no advertised HTTPS scheme
or path-prefix setting here. Behind a proxy, arrange a reachable, matching engine
origin or rewrite URLs; a container-only hostname, TLS termination or subpath mount
is not repaired automatically. Treat returned URLs as opaque and private. Do not build
a playback path from the response's `infohash` or `playback_session_id`.

## Quickstart: direct VLC playback

Supply your own current public, unencrypted live source. The following 40-hex value
is an **obviously synthetic syntax placeholder**, not playable content; replace it
before running either example. No closed-source engine binary is required.

```bash
CONTENT_ID='0123456789abcdef0123456789abcdef01234567' # replace with your source
vlc "http://127.0.0.1:6878/ace/getstream?id=${CONTENT_ID}&format="
```

Omitting `format` or setting it empty returns MPEG-TS directly (`video/mp2t`). Keep
URLs shell-quoted, including their ampersands. For live HLS, open
`/ace/manifest.m3u8` with the same selector; its default response redirects to a
stable leased playlist. Media availability and player decoding are separate from
accepting the request.

## Quickstart: a JSON session client

This Python standard-library example checks HTTP status and the in-band `error`,
opens the **returned** playback URL, samples the returned stat URL, then stops its
own lease through the returned command URL. It reads a small amount of media rather
than saving a capture. Replace the placeholder before use.

```bash
CONTENT_ID='0123456789abcdef0123456789abcdef01234567' \
OUTPACE_URL='http://127.0.0.1:6878' python3 - <<'PY'
import json
import os
from urllib.parse import urlencode
from urllib.request import urlopen


def ace_json(url):
    with urlopen(url, timeout=60) as response:  # HTTP failures raise HTTPError
        if response.status != 200:
            raise RuntimeError('unexpected HTTP status')
        envelope = json.load(response)
    if envelope.get('error') or envelope.get('response') is None:
        raise RuntimeError('compatibility request failed')
    return envelope['response']


base = os.environ['OUTPACE_URL'].rstrip('/')
query = urlencode({'content_id': os.environ['CONTENT_ID'], 'format': 'json'})
session = None
try:
    session = ace_json(base + '/ace/getstream?' + query)
    before = ace_json(session['stat_url'])
    print('before playback:', before['status'])
    with urlopen(session['playback_url'], timeout=60) as media:
        if media.status != 200 or not media.read(188):
            raise RuntimeError('playback did not return media')
        stats = ace_json(session['stat_url'])
        print('status:', stats['status'], 'KiB/s:', stats['speed_down'])
finally:
    if session is not None:
        command = session['command_url']
        separator = '&' if '?' in command else '?'
        stopped = ace_json(command + separator + urlencode({'method': 'stop'}))
        if stopped != 'ok':
            raise RuntimeError('unexpected stop response')
PY
```

TS JSON creation mints a lease without opening the media source. Content-id catalog
resolution may already occur during that request, but successful JSON does **not**
prove playback will start. A stat-only client stays idle unless another consumer
holds the same source. In contrast, HLS manifest creation, including `format=json`,
starts the source/packager **before** minting URLs. HLS clients must consume the
returned playlist and its segment URLs, rather than treating the playlist as TS.

## Routes, envelopes and errors

The supported request method is **GET**; no POST/body-form API is provided. Framework
HEAD handling is not a separate playback/control contract. `/ace/*` JSON uses
`{"response": <value>, "error": null}`; errors use `response: null` and a string
`error`. `/server/api` and `/webui/api/service` instead use **`result`**. Always check
both HTTP status and the JSON error when JSON is expected. Media/error responses
need not be JSON.

| GET route | Supported behavior | Failure context |
| --- | --- | --- |
| `/ace/getstream` | Missing/empty `format`: direct TS. `format=json`: new playback/stat/command URLs, session fields and metadata. | Selector/format/no-network errors: HTTP 200, `response: null`. Direct source-start failure: HTTP 404. |
| `/ace/r/<id>/<token>` | Leased TS; opening it starts or joins the source. | Invalid, wrong-content, stopped, expired or evicted lease: 404. Start failures: 404/422/502 as below. |
| `/ace/manifest.m3u8` | Missing/empty `format` or `format=redirect`: HTTP 302 to a stable playlist. `format=json`: HLS playback/stat/command URLs and session metadata. | Selector/format/no-network errors: HTTP 200, `response: null`. Source/packager-start failure: 404, including JSON mode. |
| `/ace/m/<id>/<token>.m3u8` | Token-authenticated live media playlist; absolute segment URLs; `no-store`. | Invalid lease or removed packager: 404. Not ready, stalled/recovering or failed packager: 503 with `Retry-After: 1`; authorization/expiry takes precedence. |
| `/ace/c/<token>/<seq>.ts` | Token-authenticated retained native HLS bytes, `video/mp2t`, `no-store`. Does not start a source. | Unknown/revoked token, missing packager, malformed/future/evicted segment: 404. |
| `/ace/stat/<id>/<token>` | Stats and metadata under `response`; does not start a source. | Invalid lease: HTTP 200, `response: null`, `error: "invalid or expired playback session"`. No network also gives an in-band error. |
| `/ace/cmd/<id>/<token>?method=stop` | Revokes this lease; success is `response: "ok"`. | Invalid lease: same in-band error as stat. Any method other than `stop`, including absent method: HTTP 200 with `error: "missing method"`. |
| `/server/api`, `/webui/api/service` | Identical GET control-method dispatch on `?method=`; table below. | Handler responses are HTTP 200 with `result`/`error`, including unknown/missing methods and resolution failures. |

For **leased TS start failures**, `NotFound` maps to 404 (an unregistered provider,
not proof that a content id is unpublished). `Backend` maps to plain-text 502
`stream start failed`; `Unresolvable` maps to plain-text 422
`stream metadata unavailable`. The lease remains available for retry until revoked
or expired. This path logs only the error category and returns generic text, not
provider details; do not assume every other resolution error is similarly redacted.

Content-id catalog failures, no-peer discovery and transport-URL fetch/SSRF rejection
can produce `Backend`. Bare-infohash/magnet catalog failures, including transient
failures and the short failure cache, currently produce `Unresolvable`. Thus 422 does
not prove permanent failure, and 502 does not prove retrying will help. There is no
message-based timeout-to-504 mapping. Direct `getstream` and HLS creation retain
their 404 start-failure behavior. These are **Outpace-pinned** statuses, not an
established official-engine error matrix.

## Selectors and provenance

Use URL encoding for selector values (especially nested URLs and magnets). Empty
values are skipped; a nonempty higher-priority invalid value fails rather than
falling through.

| Surface | First nonempty selector wins |
| --- | --- |
| `/ace/getstream`, `/ace/manifest.m3u8` | `content_id` > `infohash` > `id` > `url` > `magnet` |
| `analyze_content`, `get_media_files`, `get_content_id` | `content_id` > `query` > `infohash` > `url` > `magnet`; `id` is not an alias here |

- `content_id` and legacy playback `id` mean a **content id**, not a bare swarm
  hash. Playback requires exactly 40 hexadecimal characters and normalizes to
  lowercase. Control analysis validates content ids through the catalog resolver;
  it does not share playback's normalization step. `query` is a control-only
  content-id alias. The echo-only `get_content_id` exception is described below.
- `infohash` is a 40-hex swarm selector, normalized to lowercase. `magnet` extracts
  only a v1 `xt=urn:btih:` hash (40-hex or 32-character base32); v2 `btmh` is
  unsupported. Magnet names, trackers and other source options are not a promise
  of additional playback features.
- `url` means an HTTP(S) **transport descriptor** URL, not an arbitrary media URL.
  Playback performs a scheme check before minting; fetching uses the SSRF guard,
  a 1 MiB cap, timeouts and disabled redirects. The
  [configuration reference](../../README.md#configuration) documents explicit
  controlled-network exceptions.
  Returned ids may be encoded transport URLs rather than infohashes.

Content ids resolve through the signed catalog or the serving daemon's own broadcast
registry. Playback alone can opt into raw transport-hash BEP-9 fallback with
`OUTPACE_BEP9_CONTENT_ID_FALLBACK=1` (default off); this is useful for cross-daemon
Outpace broadcast Ace links, not a general catalog substitute. Control analysis
remains catalog-only. See [content-id policy](notes/63-bep9-content-id-policy.md).

Cold `infohash`/`magnet` playback uses an owned broadcast or verified descriptor
index, then the authenticated catalog. Response authentication and requested-hash
verification must succeed before discovery; Outpace does not guess live geometry.
Failures are briefly cached. Only signed-catalog descriptors populate the shared
provenance index: neither BEP-9 nor a caller-supplied transport URL qualifies.
Successful content-id analysis may populate that index; offline infohash/magnet
analysis only parses the hash and does **not** establish playability. See
[native infohash inputs](../native-api.md#infohash-inputs) and the
[catalog trust boundary](notes/62-catalog-response-authentication.md).

## JSON control methods

Both aliases use `{"result": <value>, "error": null}` on success and
`{"result": null, "error": "..."}` on failure. Unknown methods return
`error: "unknown method: <name>"` with HTTP 200; a missing method uses an empty name.

| Method | Supported result and limits |
| --- | --- |
| `get_version` | `version` is Outpace's crate version, `code` is `MAJOR*10000 + MINOR*100 + PATCH`, `platform` is the target OS. Successful JSON on both aliases includes the literal `"error": null` for the sampled healthcheck. It does not identify an official engine build. |
| `get_status` | `status: "dl"` when the manager has sessions, otherwise `"idle"`; `active_sessions` is that count, not a viewer count. |
| `get_network_connection_status` | `status`, boolean `connected`, and `networks` reflect registered provider networks, not measured Internet or peer reachability. |
| `analyze_content` | `infohash`, nullable `content_id`, integer `is_live`/`is_encrypted`, and `status: "complete"`. Bare infohash/magnet parsing is offline; content-id/URL selectors resolve a live descriptor. Flags do not qualify arbitrary encrypted or VOD inputs. |
| `get_content_id` | **Echo only:** returns a nonempty caller `content_id`/`query` unchanged, even malformed strings, without 40-hex validation or catalog lookup. Cannot derive a content id from an infohash/URL/magnet. |
| `get_media_files` | Best-effort `infohash` and one synthetic `files[]` entry with `infohash`, `index: 0`, `is_live`. Same resolution as analysis; no multi-file or selection contract. `dump_transport_file` does not produce a raw dump. |

## Lease ownership and live HLS

Each request creating URLs mints a new unpredictable lease, even for the same input
or `pid`. Tokens expire **six hours after minting**, with **4096 leases** retained
at most; capacity evicts the oldest deadline. Playback/stat/playlist/segment reads
do not renew the deadline. Stop, expiry or eviction revokes authorization and ends
that lease's active TS responses or HLS pin. A restart loses in-memory leases.

A TS body holds a source subscription. Closing a direct response also removes its
hidden lease; closing a leased TS body leaves its token usable until stop/expiry.
HLS holds its subscription in the lease, since playlist requests are short-lived.
Stopping one lease releases only its ownership, not other compatibility clients or
native consumers of the same source. Sharing is by resolved manager source key;
clients must not assume all different selector spellings coalesce. Ordinary idle
cleanup applies when no owner remains.

HLS serves the shared native packager's retained live window, not an archive.
Segments leaving that window return 404; tokens do not preserve old segments.
Playlist/segment reads do not count as native activity or start replacement work.

Live HLS follows the daemon's **global** audio policy. By default it selects the
first ADTS AAC track, otherwise passes audio through. With
`OUTPACE_HLS_AUDIO_NORMALIZE=true`, selected AC3 or MPEG Layer II/III can be converted
to AAC-LC stereo (48 kHz, 128 kbit/s), copying video, under strict input constraints.
`OUTPACE_FFMPEG_PATH` selects the operator-provided executable. Read
[Live HLS audio](../../README.md#live-hls-audio) for admission rules, startup checks,
process limits and recovery behavior. A terminal normalization failure may keep
returning retryable 503 without restarting the worker; teardown and a fresh source
session are required. This does not qualify any particular browser or live client.

Query hints such as `audio_normalize`, codec/playlist/transcode settings, and caller
source options do not configure FFmpeg or enable arbitrary remuxing, video encoding,
seek or file selection. Direct TS and native VOD do not use this HLS conversion.

## Live statistics units and limits

`speed_down` and `speed_up` are **KiB/s** (1024 bytes), possibly fractional.
Download speed measures contiguous reassembled media payload before output gating;
`downloaded` counts delivered MPEG-TS bytes and need not match its integral. Upload
speed follows existing `uploaded` byte accounting. The nominal five-second rate
window is sampled once per second independently of requests and expires on inactivity.

Valid idle leases report zero speeds and null `livepos`; reading stats neither
starts nor retains a source. `livepos` is null until learned; otherwise `pos` is the
next reassembly piece, `last` the observed head, `live_first`/`live_last` the advertised
window bounds (nullable), `buffer_pieces` the complete contiguous ready pieces, and
`is_live` is 1. `first_ts`/`last_ts` remain null. This is provider observation, not a
client playhead, exact engine timestamp, wall-clock position or player buffer.
Progress percentages, disk-cache and selection-index statistics are outside this
subset. See [native statistics](../native-api.md#live-statistics) for sampling limits.

Playback/stat JSON adds `metadata`: `title` (string or null), advertised `bitrate`
(bits/s or null), and `categories` (array, empty if unknown). This bitrate is distinct
from measured speed. Metadata may be unknown at lazy mint time. Session flags are
integer `is_live: 1`, `is_encrypted: 0`, with `client_session_id: -1`; caller `pid`
does not change them. They are not an independent media-capability probe.

## Unsupported and deferred work

| Feature | Status | Rationale / limit |
| --- | --- | --- |
| Premium/encrypted content | Unsupported; intentional non-goal | The adapter supports public, unencrypted live inputs. |
| Accounts, subscriptions, DAO/auth methods and engine access-token authentication | Unsupported; intentional non-goal | Outpace does not clone these engine services. |
| Player/remote UI and bundled web UI | Unsupported; intentional non-goal | Outpace does not clone these application services. |
| Search, channel catalogs, EPG and playlist enumeration | Unsupported; intentional non-goal | Outpace serves known public inputs. Unknown control methods use the in-band error above; absent routes such as `/search` return 404. |

| Feature | Status | Rationale / limit |
| --- | --- | --- |
| Legacy VOD playback/HLS | Deferred; not supported | Native `/vod` is a separate surface, not this legacy contract. |
| Seek/file selection | Deferred; not supported | No legacy seeking or file-selection contract is implemented. |
| Reverse content-id lookup | Deferred; not supported | `get_content_id` only echoes a supplied content id. |
| Raw transport dumping | Deferred; not supported | `get_media_files` does not produce a raw transport dump. |
| Arbitrary client-selected transcoding | Deferred; not supported | The bounded global live-HLS audio option does not implement this legacy contract. |

No future availability is promised for deferred features.

## Evidence boundary

This document pins current Outpace behavior. The
[client inventory](ace-client-inventory.md) supplies pinned primary client call sites,
local regression coverage and application-specific limits; it supplements this
contract. Protocol notes record narrower engine observations:
[control envelopes](notes/10-interop.md),
[direct playback](notes/55-legacy-getstream-direct-mode.md#evidence-boundary), and
[live HLS](notes/56-legacy-live-hls-adapter.md#observed-contract).
Historical notes are not a promise that every observed engine feature is implemented.
Static client sources and offline fixtures do not establish deployed routing, startup
time, source availability, decoding continuity, playback smoothness or full official
error parity.
