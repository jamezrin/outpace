# Stream Search

**Planned, not available yet.** Optional AceStream service-backed stream-name search is in
the v1.0.0 scope, tracked in [#406](https://github.com/jamezrin/outpace-stream/issues/406)
under [#213](https://github.com/jamezrin/outpace-stream/issues/213). There is no supported
search setting or `/search` implementation today; the route returns `404`.

## What is planned

Outpace will host a bounded adapter to AceStream index/search services in this repository.
Existing client apps supply the search UI; Outpace remains a headless engine. A separate app
is not required for this adapter. A standalone catalog product with its own UI, indexer,
EPG or playlist management is outside this scope.

The original target is the search → select/add → playback workflow in
[visesa84/acestream-scraper-PyAcexy](https://github.com/visesa84/acestream-scraper-PyAcexy),
requested in [#253](https://github.com/jamezrin/outpace-stream/issues/253#issuecomment-6334792867).
That compatibility contract needs name queries, zero-based pagination, category filtering, empty-query
browsing, and results grouped by channel name with stream items and totals.

Search results alone are insufficient. The older client workflow also calls
`/server/api?method=get_content_id&infohash=...` before adding a selection. Outpace currently
only echoes an already supplied content ID through `get_content_id`; reverse lookup is not
implemented. A supported result-to-content-ID/verified-descriptor path, or an explicitly
coordinated client change, is required before claiming this workflow works.

Investigation on November 12 found that the client repository now resolves to
`visesa84/acestream-scraper`. Its
[November 11 migration](https://github.com/visesa84/acestream-scraper/commit/5668ec41d089)
queries the external search service directly and uses explicit `ih:` selectors instead of
reverse content-ID lookup. Outpace already supports infohash selectors through verified
catalog resolution. That change provides an integration path, but does not make older clients
compatible or prove playback of search results. The current client's search also bypasses
Outpace; using the planned adapter will require a client integration change.

## Opt-in and playback boundaries

- The new services setting will default off. `OUTPACE_COMPAT_ACESTREAM_SERVICES` is a
  **proposed name**, not an available environment variable.
- It will be independent of `OUTPACE_ACE_COMPAT`, whose deprecated alias is
  `OUTPACE_EXPERIMENTAL_ACE_COMPAT`. Enabling the HTTP compatibility API alone must not
  enable the new external search requests. Exact route gating is still to be defined.
- Existing signed-catalog content-ID resolution and its defaults remain unchanged. Search
  being disabled or unavailable must not prevent playback of known inputs.
- Search metadata is discovery data, not proof of a verified descriptor or playable source.
  Existing descriptor verification and public, unencrypted content limits still apply.
- No accounts, premium integration, bundled UI or closed-engine runtime are included.

## Prerequisites and acceptance

Before implementation, establish current upstream access/key requirements and usage limits,
and the supported result-to-playback path. Historical beta/test keys are not a production
contract. Access requirements and feasibility are not verified by this scope decision.

The adapter must bound request timeouts, response/page sizes, concurrency and cache storage
and lifetime, handle rate limits, and distinguish upstream errors from valid empty results.
Deterministic offline tests must cover adaptation, flag combinations, limits and failures;
optional live validation follows the repository's identifier hygiene rules. Real search queries,
result metadata, identifiers and credentials must not enter published evidence or fixtures;
offline fixtures use synthetic values.

## Original engine protocol research

[Reverse-engineering note 74](https://github.com/jamezrin/outpace-stream/blob/main/docs/protocol/notes/74-ace-search-native-services.md)
traces a different native-engine path: compact HTTP requests to `s5.torrentstream.*`, signed
with embedded request/response shared keys. The inspected native path does not require an
operator-supplied search API key. Original-module offline fixtures verified request signing,
response nonce/signature checks and synthetic result normalization; current remote service
availability was not tested.

The public HTTP API adapter proposed in [#408](https://github.com/jamezrin/outpace-stream/pull/408)
is a separate backend with its own key configuration. That requirement must not be presented
as original-engine parity. A native-compatible backend needs current endpoint and transport
security validation before adoption; all search metadata still passes through existing
playback verification.

## DHT search is separate research

Known-hash peer discovery already uses the DHT. Keyword search would need a crawler to
retrieve descriptors and maintain an index; hash sampling alone does not provide names or
useful catalog coverage. That remains a bounded feasibility spike in #213, not a promised
production crawler or aggregate playlist, and is not a dependency of service-backed search.

## Related pages

- [Playing Streams](Playing-Streams#acestream-compatible-api)
- [Configuration](Configuration#planned-services-compatibility)
- [Migrating from AceStream Engine](Migrating-from-AceStream-Engine#whats-different)
