# Acestream HTTP support contract

This is the canonical operator contract for Outpace's opt-in Acestream HTTP adapter.
Prefer the [native CLI and HTTP API](../native-api.md) for new integrations. The adapter
provides a supported subset of public, unencrypted **live** playback and JSON control; it does
not claim full engine or application parity. Native VOD is a separate `/vod` surface.

## Enable and advertise the adapter

The gate is **off by default**. Enable it on the serving daemon:

```bash
OUTPACE_ACE_COMPAT=1 cargo run -p ace-engine --bin outpace -- serve
```

Set `OUTPACE_ACE_COMPAT=0` to explicitly disable it. The selected value accepts exactly
`1`, `true`, `0`, or `false`; empty, malformed, or non-Unicode values fail startup.
`OUTPACE_EXPERIMENTAL_ACE_COMPAT` is a deprecated fallback used only when
`OUTPACE_ACE_COMPAT` is unset. A present canonical value always wins, including false;
invalid canonical values fail rather than falling back, and unused legacy values are
ignored. Rename the old variable to migrate. Restart the daemon to apply changes;
Compose users must recreate the service as described in the
[deployment guide](../deployment.md#configuration-contract).

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
| `/ace/m/<id>/<token>.m3u8` | Token-authenticated live media playlist; absolute segment URLs; `no-store`. | Invalid lease or removed packager: 404. Not ready, stalled/recovering or failed packager: 503 with `Retry-After: 1`; authorization/expiry takes precedence. After a committed timeshift transition: 410. |
| `/ace/c/<token>/<seq>.ts` | Token-authenticated retained native HLS bytes, `video/mp2t`, `no-store`. Does not start a source. | Unknown/revoked token, missing packager, malformed/future/evicted segment: 404. After a committed timeshift transition: 410. |
| `/ace/stat/<id>/<token>` | Stats and metadata under `response`, plus the Outpace `timeshift` status object; does not start a source. | Invalid lease: HTTP 200, `response: null`, `error: "invalid or expired playback session"`. No network also gives an in-band error. |
| `/ace/cmd/<id>/<token>?method=stop` | Revokes this lease; success is `response: "ok"`. Extra query parameters, such as the `api_version=2` the Android SDK appends, are ignored. Responses are `no-store`. | Invalid lease: same in-band error as stat. Unknown or absent method: HTTP 200 with `error: "unknown or missing method"`. A duplicated query parameter: `error: "duplicate command parameter"`, and nothing runs. |
| `/ace/cmd/<id>/<token>?method=timeshift_status` | **Outpace extension.** Viewer generation/mode plus retained-history status (`retained_media_duration` coordinates; `persistent`, `peer_archive`, `utc_seek` all `false`). | Non-HLS lease: `supported: false`. |
| `/ace/cmd/<id>/<token>?method=timeshift_seek&back=<s>` or `&position_ms=<ms>` | **Outpace extension.** Prepares a finite replay from an independently decodable retained boundary and commits it to this viewer only; returns a generation-specific `playback_url` and `reload_required: true`. Optional `generation=<n>` compare-and-swap. | History disabled, not ready or unavailable, a bad or ambiguous target, or a stale generation: in-band `error`; the current view is unchanged. |
| `/ace/cmd/<id>/<token>?method=timeshift_live` | **Outpace extension.** Commits a new live view for this viewer and returns its URL. | Live not ready or stale generation: in-band `error`, view unchanged. |
| `/ace/t/<id>/<token>/<generation>.m3u8`, `/ace/tc/<token>/<generation>/<seq>.ts` | **Outpace extension.** The current view's playlist and segments. A replay is a VOD playlist ending in `EXT-X-ENDLIST`. | Older generation, a pruned replay segment or playlist, or a replay whose source was replaced: 410, never live media. Invalid lease or removed packager: 404. |
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

### Origin HLS descriptors

A descriptor that names an HTTP(S) HLS manifest takes precedence over the requested
output route: `/ace/getstream` does not relabel it as TS and `/ace/manifest.m3u8` does
not repackage it. No swarm session or stat/command lease is created.

- **Default (direct handoff).** Default and `redirect` playback answer `302` to the
  origin URL with `no-store`. Early-resolved `format=json` returns `playback_url` (the
  origin URL), `is_direct: true`, `transport: "hls"`, `content_id` and descriptor
  `metadata`. A TS JSON lease whose source resolves late redirects its `playback_url`
  to the origin and revokes itself.
- **`OUTPACE_MANAGED_HLS=1` (managed relay, default off).** The same starts return
  a local `/origin-hls/<token>/0.m3u8` URL instead. JSON keeps `is_direct: true` and
  adds `managed_origin: true`, `command_url` and `stop_url`; `DELETE stop_url` returns
  `204` once, then `404`. `command_url` (`/origin-hls/<token>/cmd`) reports and
  controls the restricted [origin TS replay](../timeshift.md#managed-origin-ts-replay);
  its status truthfully reports a disabled or unsupported state. Relay statuses,
  limits and the outbound fetch policy are described in
  [Origin HLS](../native-api.md#origin-hls).

## Selectors and provenance

Use URL encoding for selector values (especially nested URLs and magnets). Empty
values are skipped; a nonempty higher-priority invalid value fails rather than
falling through.

| Surface | First nonempty selector wins |
| --- | --- |
| `/ace/getstream`, `/ace/manifest.m3u8` | `content_id` > `infohash` > `id` > `url` > `magnet` |
| `analyze_content`, `get_media_files`, `get_content_id` | `content_id` > `query` > `infohash` > `url` > `magnet`; `id` is not an alias here |

A repeated query key on `/ace/getstream`, `/ace/manifest.m3u8`, `/server/api`,
`/webui/api/service` or `/ace/cmd/…` is rejected in-band rather than resolved by
position. Playback is single-file, so file and stream selection is checked before
anything resolves or starts:

- `_idx=0` and the Android client's no-selection sentinel `stream_id=-1` are accepted.
- Any other `_idx`, any other `stream_id`, and the URI-only `index` are rejected.
- `direct_url`, `transport_file_data`, `playlist_selected_indexes`,
  `playlist_start_from` and `next_file_indexes` are rejected.
- SDK expansion flags such as `expand_wrapper` and `mode` remain accepted.
- The public Android SDK's GET builder appends its descriptor query twice. Equal
  decoded repeats of `content_id`, `infohash`, `url`, `magnet` and the inert
  `transport_file_cache_key` count as one value. Conflicting values and every other
  repeated parameter (file selection, options, commands) are rejected.

`outpace play` applies the URI contract: `index=0` is accepted, while `_idx`, any
`stream_id` and duplicate keys are rejected.

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
  [configuration reference](../configuration.md) documents explicit
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
Content-id analysis inspects descriptor metadata but does **not** populate that
index. Opening a content id for playback (`/ace/getstream`, `/streams`) does.
Offline infohash/magnet analysis returns `descriptor metadata is unavailable`
and does **not** establish playability; later playback may still resolve through
the authenticated catalog. See
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
| `analyze_content` | `infohash`, nullable `content_id`, integer `is_live`/`is_encrypted`, and `status: "complete"`. Bare infohash/magnet analysis returns the error envelope `descriptor metadata is unavailable`. Content-id/URL analysis inspects supported wrapped live/historical VOD or single-file raw/disk metainfo without joining a swarm. Inspected public reports `is_encrypted: 0`; protected reports `1`. Malformed protection is an error. |
| `get_content_id` | **Echo only:** returns a nonempty caller `content_id`/`query` unchanged, even malformed strings, without 40-hex validation or catalog lookup. Cannot derive a content id from an infohash/URL/magnet. |
| `get_media_files` | Best-effort `infohash` and one synthetic `files[]` entry with `infohash`, `index: 0`, `is_live`. A classified descriptor adds `type: live` or `type: vod`; verified single-file VOD adds `size` in bytes. Live entries also carry `mime: video/mp2t` (the served output) and a display `filename: stream.ts`; VOD entries carry neither. An origin-HLS descriptor returns an in-band error telling the client to use an HTTP playback route; no file table is fabricated. Same resolution as analysis; no multi-file contract, and selection is limited as described under [Selectors and provenance](#selectors-and-provenance). `dump_transport_file` does not produce a raw dump. |

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
[Live HLS audio](../live-audio.md) for admission rules, startup checks,
process limits and recovery behavior. A terminal normalization failure may keep
returning retryable 503 without restarting the worker; teardown and a fresh source
session are required. This does not qualify any particular browser or live client.

Query hints such as `audio_normalize`, codec/playlist/transcode settings, and caller
source options do not configure FFmpeg or enable arbitrary remuxing, video encoding,
seek or file selection. `OUTPACE_LIVE_AUDIO_NORMALIZE=true` separately enables
shared-source conversion for native and compatibility TS as well as HLS. With both
switches enabled there is one conversion child. TS startup failures return explicit
422/502/503/504 responses; failures after headers terminate the body with an I/O
error. Native stream status retains the bounded failure code. VOD lengths, ranges
and bytes are unchanged. This operator policy does not add per-request conversion
options or new compatibility response keys.

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
| General channel catalogs, EPG and playlist enumeration | Unsupported; outside the bounded search scope | No catalog product or full engine application-service parity is planned. |

| Feature | Status | Rationale / limit |
| --- | --- | --- |
| Service-backed stream-name search | Separate opt-in adapter; from v1.0.0 | `GET /search` requires `OUTPACE_COMPAT_ACESTREAM_SERVICES=1`. The native HTTP backend needs no operator API key; legacy HTTP compatibility alone does not enable it. See [the search contract, limits and current-client boundary](../wiki/Stream-Search.md). |
| Legacy VOD playback/HLS | Deferred; not supported | Native `/vod` is a separate surface, not this legacy contract. |
| Seek/file selection | Deferred; not supported | No legacy seeking or file-selection contract is implemented. |
| Reverse content-id lookup | Deferred; not supported | `get_content_id` only echoes a supplied content id. Search selections use explicit infohash playback selectors and existing descriptor verification; no reverse mapping is fabricated. Older clients requiring it need an integration change. |
| Raw transport dumping | Deferred; not supported | `get_media_files` does not produce a raw transport dump. |
| Arbitrary client-selected transcoding | Deferred; not supported | The bounded global live-HLS audio option does not implement this legacy contract. |

No future availability is promised for deferred features.

## Native origination scope and follow-ups

Native [broadcast startup profiles](../native-api.md#broadcast-startup-profiles) cover explicit
title, bytes/second bitrate, piece size, quality and categories across all MPEG-TS producers.
They preserve default descriptor encoding, stored identity/trackers on resume and consumer-valid
legacy geometry; they add no compatibility endpoint or automatic media/bitrate detection.
API bitrate metadata remains bits/second. See [deployment configuration](../deployment.md#broadcast-profile-configuration)
for container forwarding, one-full-piece budgets and immutable restart behavior.

The following distinguishes the implemented profile subset from other delivered capabilities
and explicitly deferred protocol work. A tracker override alone does not establish privacy.

| Area | Current scope / evidence still required |
| --- | --- |
| Metatracker and startup-node fields (#269) | Explicitly deferred for 1.0.0; partial static evidence does not establish producer-to-consumer or endpoint/advertisement semantics. |
| Tags, logo and description (#269) | Explicitly deferred for 1.0.0 pending wire/runtime evidence; no arbitrary metadata bag. |
| Private peer/advertisement policy (#271) | Implemented as separate outbound-only private mode; see [private peer policy](../private-peer-mode.md). It is not enabled by a tracker override. |
| HLS transport origination/support parity (#272) | Explicitly deferred for 1.0.0. Native TS-to-HLS packaging and managed upstream relay do not establish original-engine HLS transport parity. |
| Autonomous support-source lifecycle (#270) | Implemented for the documented MPEG-TS source subset; see [support operation](../support-operation.md). This does not implement HLS transport support parity. |
| SRT (#7) | Implemented as optional native-Linux ingest with the `srt` feature and encryption-enabled libsrt; absent from standard release binaries/images. See [SRT ingest](../srt-ingest.md). |
| Live interoperability/deployment (#205) | Recorded evidence is scoped to its tested topology and build; offline profile tests alone do not establish deployed playback or full interoperability. |

See the [1.0.0 notes](../releases/1.0.0.md#deferred-scope) for the maintainer's release-scope
decision and [validation limits](../releases/1.0.0.md#validation-and-limits).

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

## Descriptor protection (#279, delivered in #314)

Recognized wrapped descriptors use root `premium`; single-file raw/disk metainfo uses
`info/premium`. Missing/integer zero is public, integer one is protected, and other
values are invalid metadata. Transport encryption and live signatures remain separate.
Playback refusal text is `encrypted content is not supported`; malformed protection
uses `invalid content protection metadata`. Catalog protection/format errors are terminal,
including when raw-hash fallback is enabled. Inspection does not grant authorization.

The implementation gates Ace provider live discovery and VOD store creation, manager cached
session admission, and compatibility catalog/URL preparation before lease minting.
Uninspected playback retains existing policy and legacy zero flags. Synthetic regression
coverage and acceptance were delivered in [#314](https://github.com/outpace-stream/outpace-engine/pull/314).
That acceptance establishes the documented refusal policy; it does not implement protected
playback or turn an uninspected zero flag into proof that content is public.
