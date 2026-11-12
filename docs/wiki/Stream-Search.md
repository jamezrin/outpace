# Stream Search

Optional service-backed stream-name search is available in builds containing
[#408](https://github.com/jamezrin/outpace-stream/pull/408), newer than v0.9.0. It is disabled
by default and independent of the legacy HTTP compatibility API. Outpace remains headless:
client apps supply the UI. General catalog management, EPG, playlist management and a
production crawler are outside this adapter's scope.

## Enable search

Supply an accepted key for the external search service in your protected service environment,
then start the daemon:

```bash
OUTPACE_COMPAT_ACESTREAM_SERVICES=1 \
OUTPACE_ACESTREAM_SEARCH_API_KEY="${OUTPACE_ACESTREAM_SEARCH_API_KEY:?Supply an accepted upstream search key}" \
  outpace serve
```

The repository's Compose file forwards both variables. Keep credentials in a protected
operator environment or `.env` file and recreate the container after changes. The key is
required only when search is enabled, must contain 1–512 visible ASCII bytes without spaces,
and is omitted from configuration serialization and Debug output. No test/beta key is
embedded or selected automatically. Private peer mode rejects this services opt-in.

Requests go to the fixed HTTPS endpoint `api.acestream.me`. The legacy
`search.acestream.net` redirected there during the November 12 investigation. Requests use
public-address validation and DNS pinning; redirects and environment proxies are disabled,
so credentials are not forwarded to another host. Current key issuance, entitlement, upstream
quotas and availability must be checked with the provider. Historical shared-test-key success
is not a stable production contract; this adapter does not create accounts or obtain keys.
Queries and the server's IP reach the external service. Outpace does not log query/result
bodies or upstream diagnostics. Existing signed-catalog content-ID resolution and defaults
remain unchanged, and known-input playback does not depend on search availability.

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
country and language metadata. Current flat-root and historical enveloped upstream responses
are normalized to this envelope. Icons, EPG, content-ID claims and unknown fields are stripped.
Metadata is discovery data, not proof of a public, verified or playable descriptor.

## Bounds and failures

- Each external attempt has a ten-second total deadline and a 1 MiB body limit; no retries.
- At most two attempts run concurrently, with at least one second between starts. Overload
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
| `503` | Two attempts already active (`Retry-After: 1`), or service unavailable |
| `504` | Total attempt deadline exceeded |

Errors return `result: null` and a fixed `search_*` error code, without reflecting upstream
messages, keys, queries or identifiers. An upstream failure is never presented as an empty page.

## Select and play a result

Use the returned infohash explicitly: native `/streams/ace/ih:<infohash>` (or its `.m3u8`
form), or `/ace/getstream?infohash=<infohash>` with `OUTPACE_ACE_COMPAT=1`. Existing catalog
resolution and descriptor/media verification still apply. Do not pass the infohash as a bare
content ID or invent a content ID by hashing metadata.

The original request in [#253](https://github.com/jamezrin/outpace-stream/issues/253#issuecomment-6334792867)
was for an older scraper workflow that called `/search`, then
`/server/api?method=get_content_id&infohash=...`. Reverse lookup remains unsupported. Those
clients need an explicit integration change to use infohash playback selectors.

The named repository now resolves to `visesa84/acestream-scraper`; its
[November 11 migration](https://github.com/visesa84/acestream-scraper/commit/5668ec41d089)
queries external search directly and saves `ih:` selectors, removing reverse lookup. That
current client's search bypasses Outpace; using this adapter requires a client endpoint/parser
integration change. Neither source inspection nor local tests establish a working deployed
scraper installation. [#406](https://github.com/jamezrin/outpace-stream/issues/406) tracks the
remaining client/access qualification requirements.

## Validation and separate DHT research

Offline tests cover query/response adaptation, flags, limits, cache behavior, failures and
cancellation. A synthetic local search selection is exercised through verified playback.
An optional ignored probe uses `OUTPACE_TEST_SEARCH_API_KEY` with an invented query and emits
no source metadata; it checks discovery access, not public playback or production entitlement.
No real source identifiers, stream names or private responses belong in fixtures or reports.

DHT peer discovery for a known hash remains supported. Keyword search and an aggregate
playlist would require a crawler/index and remain independent feasibility research in
[#213](https://github.com/jamezrin/outpace-stream/issues/213), not a dependency of this adapter.

## Related pages

- [Playing Streams](Playing-Streams#acestream-compatible-api)
- [Configuration](Configuration#acestream-search-services)
- [Migrating from AceStream Engine](Migrating-from-AceStream-Engine#whats-different)
