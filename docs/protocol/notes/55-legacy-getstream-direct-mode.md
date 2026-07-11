# 55 — Legacy `getstream` selector and direct-playback contract

Issue #107 corrects two independent mistakes in outpace's opt-in HTTP adapter: a request without
`format=json` returned a JSON document to media players, and legacy `id=` was treated as a bare
swarm infohash instead of an AceStream content id.

## Evidence boundary

Note 30 captures an official `format=json` playback response, including its `infohash` and
`playback_url` (whose path embeds a token). Note 45 captures outpace's catalog-resolved adapter, not
an official response. Outpace's JSON compatibility contract additionally returns `stat_url` and
`command_url`; those fields are covered by deterministic local tests but are not claimed as
official-engine observations by these notes. The pinned `jopsis/docker-acestream-aceserve` README
documents the directly playable form `/ace/getstream?id=CONTENTHASH` and explicitly calls that
value the content ID. The original phase-0 plan also used `/ace/getstream?infohash=...` without a
format as an MPEG-TS capture.

There is no checked-in response matrix from a currently runnable official engine for malformed
selectors, conflicting selectors, or unsupported `format` values. Consequently this change does
not claim byte-for-byte official error parity or infer a redirect. It pins a conservative outpace
contract for those cases and leaves a future live capture free to refine it:

| Request shape | Outpace behavior |
| --- | --- |
| no `format` / empty `format` | HTTP 200 streaming `video/mp2t` |
| `format=json` | existing tokenized JSON response envelope |
| another non-empty `format` | HTTP 200 `/ace/*` error envelope: `unsupported format` |
| missing selector | HTTP 200 `/ace/*` error envelope |
| malformed selected 40-hex value | HTTP 200 selector-specific error envelope |

## Selector contract

Precedence stays deterministic and minimally changes the earlier adapter:

1. `content_id=<40-hex>` — internal `cid:<id>` resolver key;
2. `infohash=<40-hex>` — bare swarm key, with no catalog resolution;
3. `id=<40-hex>` — legacy content-ID alias, internal `cid:<id>` resolver key;
4. guarded `url=` transport;
5. validated `magnet=` infohash.

Hash selectors are normalized to lowercase. If the highest-priority non-empty selector is
malformed, the request fails instead of falling through. Unknown compatibility hints, including
`use_api_events`, do not affect selection or the native `/streams` routes.

## Lifecycle

Both response modes use `StreamManager` and `AceSessionStore`. Direct mode mints a bounded,
expiring lease and immediately opens it rather than returning its URL. The response body owns one
ordinary stream subscription. Disconnecting that body drops only its subscription; JSON playback
clients and native clients attached to the same manager session continue receiving data.

Deterministic HTTP tests cover direct `id=` MPEG-TS, explicit-infohash separation, JSON envelope
preservation, shared direct/JSON lifecycle, malformed/conflicting selectors, unsupported format,
ignored `use_api_events`, and the existing compatibility-disabled 404 gate.

## Capture-dependent follow-up

When an official/public engine is available, capture status, headers, redirect behavior, and body
shape across `id`/`content_id`/`infohash`, direct/JSON/unsupported formats, malformed values, and
conflicts. Any resulting parity adjustment should update this note and the deterministic matrix;
it should not weaken 40-hex validation or the SSRF-safe `url=` path without separate evidence and
security review.
