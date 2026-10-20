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
| `GET /ace/getstream` | Supported | Selectors `content_id` > `infohash` > legacy `id` > `url` > `magnet`. With no `format`, directly streams `video/mp2t` and emits `Icy-Name` when the descriptor has a title; `format=json` returns playback/stat/command URLs and nested descriptor metadata with a new unpredictable, client-specific token. `id` is a content-ID alias; `infohash` is the only explicit bare-swarm selector, and it (like `magnet`) permits cold inputs. JSON URL creation mints a lease; authenticated catalog lookup and descriptor/hash verification occur when playback is opened. Unresolvable playback fails closed. Tokens expire after six hours and state is capped at 4096 live leases. Unsupported formats return an `/ace/*` error envelope. |
| `GET /ace/r/<id>/<token>` | Supported | Playback byte stream for a valid lease. Missing, invalid, expired, stopped, or wrong-content tokens and `ProviderError::NotFound` (unknown network/content) return HTTP 404. Start errors return plain-text HTTP 502 (`Backend`) or 422 (`Unresolvable`); the lease remains usable for retry until expiry or revocation. |
| `GET /ace/manifest.m3u8` | Supported (live) | Uses the same selectors and catalog resolution as `getstream`. The default and `format=redirect` return `302` to a stable `/ace/m/...` playlist; `format=json` returns documented playback/stat/command fields plus nested descriptor metadata. Each request owns one bounded HLS lease over the shared native packager. |
| `GET /ace/m/<id>/<token>.m3u8` | Supported (live) | Token-authenticated media playlist over the native live HLS window. Missing, forged, expired, stopped, or evicted leases return `404`; responses are `no-store`. |
| `GET /ace/c/<session>/<seq>.ts` | Supported (live) | `session` is the unpredictable HLS lease token emitted in the playlist. Retained native bytes are returned as `video/mp2t`; forged tokens and evicted, future, or malformed sequence numbers return `404`. Segment probes never start a stream. |
| `GET /ace/stat/<id>/<token>` | Supported | Session stats and nested descriptor metadata under `.response`. Invalid/expired tokens return HTTP 200 with `response: null` and `error: "invalid or expired playback session"`. |
| `GET /ace/cmd/<id>/<token>?method=stop` | Supported | `stop` revokes only this compatibility client's lease; other clients and native consumers keep the shared source alive. Invalid/expired tokens use the same error envelope as `stat`; other methods return an error envelope. |

`content_id`, `infohash`, and `id` must be exactly 40 hexadecimal characters and are normalized
to lowercase. A malformed higher-priority selector is rejected rather than silently falling
through to another selector. Compatibility hints such as `use_api_events` and other unknown query
parameters are safely ignored. Missing/malformed selectors and unsupported formats currently use
HTTP 200 with `{ "response": null, "error": "..." }`; this is outpace's pinned compatibility
behavior pending a broader official-engine error capture.

Playback start failures are logged by error category without provider text, which can contain
identifiers or tokens. `Backend` includes discovery/upstream failures and timeouts; there is no
typed timeout variant, so all use 502 rather than guessing 504 from message text. `Unresolvable`
means verified metadata could not be obtained or did not match; 422 follows the native stream
route. Bodies are short generic reasons, without provider details. This is an Outpace contract:
[note 30](notes/30-official-consumer-proof-after-infohash-fix.md#playback-still-fails-at-zero-peers)
observed official-engine playback stay in prebuffer with zero peers and no HTTP response before
the client's 45-second timeout, not a server error status. [Note 55](notes/55-legacy-getstream-direct-mode.md#evidence-boundary)
also records the missing official error-response matrix. Direct `/ace/getstream` and HLS manifest
start-error mappings remain unchanged.

Cold `infohash=` and `magnet=` selectors may mint playback URLs. On playback open,
the provider uses an owned broadcast or verified index entry, then the signed
catalog. The authenticated descriptor must match the requested swarm hash before
discovery. Failures are briefly cached; unavailable or mismatched metadata fails
playback closed. JSON URL creation alone does not establish that playback will
succeed. A BEP-9 result or transport URL does not seed the catalog-provenance index.
HLS manifest creation starts resolution before minting its control URLs, including
`format=json`; an unavailable descriptor returns HTTP 404 through the existing HLS
start-failure path.

Compatibility HLS is currently limited to public, unencrypted live inputs. Playlist/transcode
parameters are accepted as inert compatibility hints: the adapter always exposes the existing raw
MPEG-TS HLS packager and never claims that remuxing or transcoding occurred. VOD compatibility is
not claimed; native VOD remains available under `/vod`. See note 56 for the observed-versus-pinned
contract and lifecycle details.

The additive `metadata` object has stable `title`, `bitrate`, and `categories` fields. Unknown
values are represented as `null`, `null`, and `[]`. HLS media playlists are unchanged; stream
titles are exposed in the control JSON rather than encoded into a non-portable playlist tag.

## `/server/api` control methods

Dispatched on `?method=`; the response envelope is `{ "result": <value>, "error": <null|message> }`.

| Method | Status | Result fields | Notes |
| --- | --- | --- | --- |
| `get_version` | Supported | `version`, `code` | `version` is the crate version; `code` packs `MAJOR*10000 + MINOR*100 + PATCH`. |
| `get_status` | Supported | `status`, `active_sessions` | `status` is `dl` when any session is active, else `idle`. |
| `get_network_connection_status` | Supported | `status`, `connected`, `networks` | `connected` reflects whether any provider network is registered. |
| `analyze_content` | Supported | `infohash`, `content_id`, `is_live`, `is_encrypted`, `status` | Resolves the content selector to its infohash. `infohash`/`magnet` selectors resolve offline; `content_id`/`url` need the live catalog/transport and are gated by the same switch as `/ace/getstream` content-id resolution. Resolving a `content_id` through the signed catalog here also lets a later `infohash=` playback of the result succeed (a BEP-9 result or `url` does not); an offline `infohash`/`magnet` analysis does not make that infohash playable. |
| `get_content_id` | Supported (echo only) | `content_id` | Echoes a caller-supplied `content_id`/`query`. It cannot derive a content id from a bare infohash/url and returns an error envelope in that case. |
| `get_media_files` | Supported (best-effort) | `infohash`, `files[]` | outpace transports are single-file, so one media file is reported, keyed by infohash. Resolving a `content_id` through the signed catalog here also lets a later `infohash=` playback of the result succeed (a BEP-9 result or `url` does not). `dump_transport_file` is not supported. |

Content-id resolution for live and VOD is catalog-only by default, with a trusted local
registry for the daemon's own broadcasts. `OUTPACE_BEP9_CONTENT_ID_FALLBACK=1` opts the
receiving daemon into raw transport-hash BEP-9 fallback after catalog failure. It checks
SHA-1 of the wrapped transport bytes against the requested identifier and does not populate
the catalog-provenance infohash index. Cross-daemon outpace broadcast Ace links need this
opt-in; a transport URL is an alternative. See [note 63](notes/63-bep9-content-id-policy.md).
The `/server/api` content-analysis methods remain catalog-only; this opt-in applies to
playback resolution, not content analysis.

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

- Legacy HLS VOD behavior (native `/vod` HLS exists).
- `get_media_files&dump_transport_file=1` raw transport-file dumping.
- Reverse `get_content_id` (deriving a content id from an infohash/transport). The official engine maps an infohash to its content id (observed during #164's spike; the request it uses was not captured), which is presumably how it plays a cold infohash; adopting it would let outpace play cold infohash inputs.


## Live statistics units and limits

`/ace/stat/<id>/<token>` includes `speed_down`, `speed_up` and `livepos` for active
sessions. Speed units are KiB/s (1,024 bytes), including fractional values. Download
speed measures contiguous reassembled media payload before output gating;
`downloaded` retains its delivered-MPEG-TS total meaning. Upload speed follows the
source's existing `uploaded` accounting. The nominal five-second measurement
window is sampled once per second independently of HTTP requests and expires on
inactivity. A status read neither starts a provider nor refreshes its lease.
Idle valid leases report zero speeds and null `livepos`; invalid tokens retain
the existing error envelope.

The `livepos` object describes provider observations: `pos` is the next reassembly
piece, `last` the last observed head, `live_first`/`live_last` the active advertised
window bounds or null, `buffer_pieces` the complete contiguous ready pieces, and
`is_live` is 1. `first_ts`/`last_ts` are null. Before any position is learned the
whole object is null. These are not exact engine playhead/timestamp semantics;
no client position or wall-clock timestamp is invented. Native status exposes the
same optional object. See [native statistics](../native-api.md#live-statistics)
for rate/buffer boundaries and sampling limits. Progress percentages, disk-cache
statistics and selection-index fields remain outside this implementation.
