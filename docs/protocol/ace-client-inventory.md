# Ace client compatibility inventory

This inventory pins three representative clients inspected on 2026-10-23. Their source shows
what they request, not complete official-engine behavior or successful playback in a deployed
application. The [compatibility matrix](compat-matrix.md) remains the route and error contract.
The supported subset remains opt-in through `OUTPACE_ACE_COMPAT=1`, disabled by
default; `OUTPACE_ACE_COMPAT=0` explicitly disables it. The deprecated
`OUTPACE_EXPERIMENTAL_ACE_COMPAT` alias is a fallback only when the canonical variable
is unset; see the [configuration contract](compat-matrix.md#enable-and-advertise-the-adapter)
for precedence, strict validation, and restart requirements. Native APIs remain the primary
interface. No new control methods are required by these samples.

Source snapshots use abbreviated Git commit URLs to distinguish provenance from content
identifiers. Examples below use only the synthetic selector
`0123456789abcdef0123456789abcdef01234567`.

| Project | Pinned revision | Sampled integration |
| --- | --- | --- |
| Dispatcharr | [cc68a83f5a02](https://github.com/Dispatcharr/Dispatcharr/tree/cc68a83f5a02) (2026-10-19) | Configured media URL consumed as HTTP bytes |
| acexy | [162972e4362e](https://github.com/Javinator9889/acexy/tree/162972e4362e) (2026-05-15) | JSON handshake, returned playback URL, stop URL |
| Acestream-Manager | [e63917a092c7](https://github.com/gabo-it/Acestream-Manager/tree/e63917a092c7) (2026-09-14) | Compose healthcheck, stats polling, TS/HLS proxy, optional search |

## Client requests and adapter behavior

### Dispatcharr: configured media URL

The [HTTP reader](https://github.com/Dispatcharr/Dispatcharr/blob/cc68a83f5a02/apps/proxy/live_proxy/input/http_streamer.py#L48-L84)
GETs its configured URL, optionally supplies User-Agent, requires final HTTP 200 and consumes
streamed chunks. The [input manager](https://github.com/Dispatcharr/Dispatcharr/blob/cc68a83f5a02/apps/proxy/live_proxy/input/manager.py#L1347-L1359)
passes the URL through; [stream-profile substitution](https://github.com/Dispatcharr/Dispatcharr/blob/cc68a83f5a02/core/models.py#L151-L168)
also inserts a configured URL into external player commands. These are generic URL consumers.
No hardcoded Ace route or JSON/control handshake was found in this snapshot; custom plugins
and user commands are outside that finding.

A configured `/ace/getstream?id=0123456789abcdef0123456789abcdef01234567` uses Outpace's
direct `video/mp2t` response, without `format=json`. This supports the sampled public live TS
request shape. It does not establish source availability, startup latency or player smoothness.

### acexy: JSON handshake and playback lifecycle

The [selector type](https://github.com/Javinator9889/acexy/blob/162972e4362e/acexy/lib/acexy/aceid.go#L19-L35)
selects `id` or `infohash`. The [proxy entry](https://github.com/Javinator9889/acexy/blob/162972e4362e/acexy/proxy.go#L63-L88)
accepts GET, rejects a caller-supplied `pid`, and forwards other query arguments.
The [backend DTO](https://github.com/Javinator9889/acexy/blob/162972e4362e/acexy/lib/acexy/acexy.go#L29-L47)
and [request builder](https://github.com/Javinator9889/acexy/blob/162972e4362e/acexy/lib/acexy/acexy.go#L398-L450)
use the selected endpoint with the selector, `format=json`, and a generated UUID `pid`.

Outpace returns `.response` with string `playback_url`, `stat_url`, `command_url`, `infohash`
and `playback_session_id`, integer `is_live`, `is_encrypted`, `client_session_id`, and
`error: null`. Errors use `response: null` and a nonempty string `error`. The legacy `id`
means a content id; it is not reinterpreted as a swarm hash. `pid` and other compatibility
hints do not control authorization: Outpace mints an independent unpredictable lease token.

The [proxy lifecycle](https://github.com/Javinator9889/acexy/blob/162972e4362e/acexy/proxy.go#L95-L137)
opens the returned playback URL, and releases its last client through the returned command URL
with `method=stop`, decoding a string response/error. Outpace returns `response: "ok"` on
stop and revokes only that caller's lease. Playback enforces the token and retains typed
404/422/502 failures described in the matrix. acexy's media copier does not check the backend
playback status, so an error body can reach its media output; upstream retry behavior is not
promised.

Its HLS mode copies playlist bodies without rewriting segments; its
[README](https://github.com/Javinator9889/acexy/blob/162972e4362e/README.md#L95-L98)
labels HLS untested. Absolute compatibility segment URLs avoid resolving segments against
acexy's origin, but HLS in acexy itself remains unverified. TS is its default.

### Acestream-Manager: application and Compose playbook

The [Compose healthcheck](https://github.com/gabo-it/Acestream-Manager/blob/e63917a092c7/docker-compose.yml#L26-L30)
GETs `/webui/api/service?method=get_version` and greps for the literal `"error": null`.
Its [interactive engine check](https://github.com/gabo-it/Acestream-Manager/blob/e63917a092c7/webui/server.js#L1149-L1167)
instead parses HTTP success, falsy `error` and `.result.version`. Successful Outpace
`get_version` responses on both `/server/api` and `/webui/api/service` contain that exact
spaced literal, retain `application/json`, and retain the parsed `result`/`error` envelope.
Version/code identify Outpace rather than spoofing the official engine. This closes the
compact-JSON whitespace mismatch shown by the pinned playbook; the playbook was not installed
or executed for this change.

The [stats proxy](https://github.com/gabo-it/Acestream-Manager/blob/e63917a092c7/webui/statsProxy.js#L30-L66)
GETs `/ace/getstream?id=...&format=json&pid=webui-stats-...`, retains only the returned stat
and command URLs, and polls stats. Truthy errors fail the request; poll failures invalidate
its cached session so a later request remints. Stop uses `method=stop` and ignores the body.
The [stats UI](https://github.com/gabo-it/Acestream-Manager/blob/e63917a092c7/webui/views/stats.ejs#L78-L104)
consumes status, peers, speeds and byte counters, which Outpace supplies (speed in KiB/s).

**Stats-only lifecycle limit:** TS JSON minting is lazy. A stat read neither starts a source
nor retains one, so these sessions remain idle unless another consumer holds the same source.
This is not an eager warmup contract. Leases expire after six hours, with at most 4096 retained
leases; stop, expiry and capacity eviction revoke access. Stats do not renew leases, and no
automatic renewal was found in acexy. Long sessions can therefore reach the deadline. HLS
manifest creation, including JSON mode, starts and pins its shared HLS packager separately.

The [TS/HLS proxy](https://github.com/gabo-it/Acestream-Manager/blob/e63917a092c7/webui/streamProxy.js#L64-L151)
GETs direct TS with `id`, or `/ace/manifest.m3u8?id=...` with an optional engine-access
`token` query hint. HLS follows the redirect and replaces absolute segment URLs beginning
with its configured engine base with its own proxy base. The
[catch-all route](https://github.com/gabo-it/Acestream-Manager/blob/e63917a092c7/webui/server.js#L579-L581)
forwards the remaining path to the engine. Root-relative segments bypassed that rewrite and
resolved against the Manager origin. Compatibility playlists now emit absolute `/ace/c/...`
segment URLs using the same request-base policy as playback/control URLs: `http://` plus
request `Host`, or `127.0.0.1:6878` when absent. Forwarded headers are not consulted. Native
HLS retains its relative segment URLs. The configured engine base and advertised origin must
match and be reachable; this change cannot repair container-only hostnames or an HTTPS/base
path mismatch. The optional input query hint is distinct from Outpace's minted path token;
it does not implement engine access-token authentication.

The [optional search UI](https://github.com/gabo-it/Acestream-Manager/blob/e63917a092c7/webui/search.js#L29-L60)
GETs `/search` with `query`, `page`, `page_size` and optional `category`, expecting
`.result.results[].items`, icons and totals. Outpace has no `/search` route. Search, channel
catalog and EPG enumeration are non-goals here; known public sources can use playback.
Consequently this inventory does not claim full application replacement.

## Local coverage and remaining boundaries

Tests are offline router/fixture tests, not executions of the third-party applications.
The HTTP tests live in `crates/ace-engine/src/http/tests/`.

| Required operation | Local coverage | Boundary |
| --- | --- | --- |
| Direct `getstream` with legacy `id` | `compat.rs`: direct TS response, body/drop ownership, failures | Public live TS only; official error matrix is not established |
| JSON `id` + `format=json` + `pid`; playback/stat/stop | `compat.rs`: typed client DTO flow and malformed-input error; existing token, expiry and multi-consumer stop tests | JSON minting alone does not prove playable content; stat does not start work |
| HLS redirect and JSON, playlist and segment | `compat_hls.rs`: engine Host, absolute segment, sampled Manager rewrite/catch-all mapping, retained TS bytes; existing forged-token, expiry and eviction cases | Tests URI routing locally; no proxy deployment or player timing claim |
| `get_version` on both aliases | `server_api.rs`: literal spaced error, parsed result fields and JSON content type | Successful version formatting only; other methods keep their envelopes |
| Gate off | Playback/control route table plus existing native route coverage | Supported opt-in configuration; default remains off |
| Stats counters | `compat.rs`: measured units/positions, idle/invalid-token cases | Provider observations, not an exact client playhead |

The other existing methods have no discovered caller in these three snapshots. Their current
subset remains available, without expanding the method set:

| Method | Existing coverage | Semantics and gaps |
| --- | --- | --- |
| `get_status` | Pure `status_result_reflects_active_session_count` builder test | Active-session count and idle/dl; no dedicated HTTP method test or full official status parity claim |
| `get_network_connection_status` | HTTP and pure builder tests | Registered provider networks, not measured Internet reachability |
| `analyze_content` | HTTP offline selector and error tests | Content-id/URL resolution remains gated; no new arbitrary content types |
| `get_content_id` | HTTP echo and unavailable-selector tests | Echoes nonempty supplied `content_id`/`query`, including malformed strings; reverse mapping is deferred. Unlike validating analysis/playback selectors, this echo does not enforce 40-hex syntax |
| `get_media_files` | HTTP and pure builder tests | Best-effort synthetic single file; no raw transport dump or multi-file VOD contract |

No sampled caller requires POST, PUT, body forms, new aliases, account methods or a new token
API. Existing selector precedence, source leases, token isolation, typed errors, transport URL
SSRF checks and authenticated catalog resolution remain in force. Compatibility gate policy
is documented in the [operator contract](compat-matrix.md#enable-and-advertise-the-adapter).

Premium/account/subscription/encrypted inputs, remote/player UI, search/channel catalogs and
EPG are excluded. Legacy VOD playback/HLS, seek/file selection, arbitrary client-selected
transcoding, raw transport dumps and reverse content-id lookup remain deferred.
Native `/vod` is a separate surface. Inert
transcoding hints do not mean conversion occurred. Live HLS follows the existing daemon-wide
`OUTPACE_HLS_AUDIO_NORMALIZE` policy described in the
[contract](compat-matrix.md#lease-ownership-and-live-hls); request hints do not enable it.
Neither these static sources nor fixture tests establish official failure parity,
deployed routing, startup time, source availability,
continuous decoding or playback smoothness.
