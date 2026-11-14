# Stream Search

Optional service-backed stream-name search is available in builds containing
[#408](https://github.com/outpace-stream/outpace-engine/pull/408), available from v1.0.0. It is disabled
by default and independent of the legacy HTTP compatibility API. Outpace remains headless:
client apps supply the UI. General catalog management, EPG, playlist management and a
production crawler are outside this adapter's scope.

## Enable search

Enable the services setting and start the daemon:

```bash
OUTPACE_COMPAT_ACESTREAM_SERVICES=1 outpace serve
```

No operator API key or account is required. The repository's Compose file forwards the
services setting; recreate the container after changing it. Private peer mode rejects this
opt-in.

Search uses the fixed AceStream hosts `s5.torrentstream.net`, `s5.torrentstream.org` and
`s5.torrentstream.info` over **plain HTTP**. Queries and the server's IP reach the external
service and are visible to network observers. Requests use public-address validation and
DNS pinning; redirects and environment proxies are disabled. Outpace identifies itself as
Outpace and sends no device or account identity. With
[engine camouflage](Engine-Camouflage) on, search uses the official engine's parameter set
and User-Agent instead. There is no automatic fallback to the
public API or a user-supplied endpoint.

Responses must pass the native protocol's signature and request-nonce checks. These shared
protocol checks do not establish trusted playback provenance. Service availability and
quotas remain controlled by the provider. Outpace does not log queries, result bodies or
upstream diagnostics. Existing signed-catalog content-ID resolution and defaults remain
unchanged, and known-input playback does not depend on search availability.

## Request and response

`GET /search` requires only the services setting, not `OUTPACE_ACE_COMPAT`. With services off,
the route returns `404`. Parameters are decoded as form fields:

| Parameter | Default | Limit |
| --- | --- | --- |
| `query` | empty (browse) | 256 UTF-8 bytes; no control characters |
| `page` | `0` | Zero-based integer, 0–10,000 |
| `page_size` | `10` | Integer, 1–200 |
| `category` | omitted | 64 UTF-8 bytes; no control characters; empty means omitted |

Unknown and duplicate fields are rejected before outbound work. Clients cannot supply an
upstream URL or API key through this route. For example, using an invented query:

```bash
curl --get --data-urlencode 'query=synthetic-demo' \
  --data-urlencode 'page=0' --data-urlencode 'page_size=10' \
  http://127.0.0.1:6878/search
```

Success returns `{"result":{"total":0,"results":[]},"error":null}` for a valid empty
result. Nonempty `results` are grouped as `name` plus `items`; each item has `infohash`,
`name` and categories, with available bitrate, status, availability, availability timestamp,
country/language metadata and a boolean `disabled` flag (`true` or `false`). Native compact
results are normalized to this envelope. URL-only items are unsupported; icons, EPG, content-ID claims and unknown
fields are stripped.
Metadata is discovery data, not proof of a public, verified or playable descriptor.

## Bounds and failures

- Each search has a ten-second total deadline, with up to three fixed-host attempts and
  a three-second budget per host. Wire bytes across all attempts share a 1 MiB limit;
  decoded/expanded results are also limited to 1 MiB. Invalid responses may try the next host;
  an exceeded byte limit stops the search.
- At most two searches run concurrently, with at least one second between starts. Overload
  fails promptly rather than accumulating queued work.
- Validated successes, including empty pages, are cached for 30 seconds. Cache capacity is
  32 entries and 8 MiB of serialized responses; expired/old entries are evicted.
- A page may contain no more than the requested number of groups and 200 total items. Reduce
  `page_size` if grouping exceeds the item cap. Names are limited to 256 bytes, metadata
  lists to 32 strings, categories to 64 bytes and country/language codes to 16 bytes each.
- Failures are not cached as successes and impose a 30-second outbound cooldown. Existing
  cached successes remain usable. HTTP responses use `Cache-Control: no-store`.

| HTTP status | Meaning |
| --- | --- |
| `400` | Invalid query |
| `429` | Start-rate limit or failure cooldown; `Retry-After: 30` is conservative |
| `502` | Upstream unavailable/rejected request, malformed result, or body/item limit exceeded |
| `503` | Two searches already active (`Retry-After: 1`), or service unavailable |
| `504` | Request deadline exceeded |

Errors return `result: null` and a fixed `search_*` error code, without reflecting upstream
messages, keys, queries or identifiers. An upstream failure is never presented as an empty page.

## Select and play a result

Use the returned infohash explicitly: native `/streams/ace/ih:<infohash>` (or its `.m3u8`
form), or `/ace/getstream?infohash=<infohash>` with `OUTPACE_ACE_COMPAT=1`. Existing catalog
resolution and descriptor/media verification still apply. Do not pass the infohash as a bare
content ID or invent a content ID by hashing metadata.

The original request in [#253](https://github.com/outpace-stream/outpace-engine/issues/253#issuecomment-6334792867)
was for an older scraper workflow that called `/search`, then
`/server/api?method=get_content_id&infohash=...`. Reverse lookup remains unsupported. Those
clients need an explicit integration change to use infohash playback selectors.

The current [acestream-scraper](https://github.com/visesa84/acestream-scraper) client uses
explicit `ih:` selectors and searches the external service directly. Its search bypasses
Outpace; using this adapter requires a client endpoint/parser integration change.
[#406](https://github.com/outpace-stream/outpace-engine/issues/406) tracks remaining client
integration qualification. Local adapter tests do not establish a working deployed scraper
installation.

DHT discovery for a known hash remains supported. DHT keyword search, a crawler and an
aggregate playlist are outside this adapter's scope.

## Related pages

- [Playing Streams](Playing-Streams#acestream-compatible-api)
- [Configuration](Configuration#acestream-search-services)
- [Migrating from AceStream Engine](Migrating-from-AceStream-Engine#whats-different)
