# 74 — Native Ace Search request protocol

Date: 2026-11-12. Related: #406, #408, #212 and #213.

## Finding

The inspected engine's ordinary name-search path does **not** use the public
`search.acestream.net` API with `api_key=test_api_key`. `CoreApp.SearchManager`
constructs compact HTTP requests to `s5.torrentstream.net`,
`s5.torrentstream.org` and `s5.torrentstream.info`, using separate embedded
request/response shared keys. There is no operator-supplied search API key in
this path. A no-account synthetic context successfully exercised the original
request builder and signed-response parser with network access disabled.

This is client implementation evidence, not evidence that these remote services
are available today. It also does not establish a decentralized keyword-search
wire protocol: the traced client path uses HTTP services. The implementation
behind those services was not inspected.

## Artifacts and confidence

Two local ELF x86-64 `CoreApp.so` artifacts were examined:

| Artifact | SHA-256 | Evidence obtained |
| --- | --- | --- |
| Android-derived 3.2.17, Python 3.8, from local `jopsis/acestream:v3.2.17-fix` reference image | `5b1677d8ae7dca4842d3b581c5300fb1e5f8f221b826e43a13276502409581f9` | Static decompilation and original-module offline execution |
| Linux 3.2.11, previously retained private reference artifact | `f166dea2356740772faca641d50547b29071c44a6841da87707b86e171616450` | Static constructor, method-table and string-table comparison |

The Android-derived image is a third-party package, not a pristine official Linux
installation. The binary hashes identify what was examined; its wrapper's
network behavior was not used as engine evidence. Both constructors initialize
the same 56-byte request key, 53-byte response key, three hosts and method/field
maps. Linux request/response execution was not repeated, so dynamic results below
apply to the pinned 3.2.17 artifact.

Ghidra 12.2, ELF relocation parsing and native disassembly were used. Cython
string entries were recovered from their relocation-backed table; encoded byte
length is twice the decoded byte count. For these entries, decoding each little-endian
16-bit unit yields `low8(word[i] XOR (17 - i))`. The Linux decoder uses a different
full-width increment whose low-byte arithmetic agrees. The single zero-unit empty
string is special. Relocated pointers must be resolved, and Ghidra's imported
image base must be accounted for when using ELF relative virtual addresses.

Raw binaries, keys, decompiled code and probe logs remain in ignored private
research artifacts. No real content IDs, swarm infohashes, source names or account
state were used. The source container was stopped and left unchanged; probes ran
in newly created, automatically removed containers with `--network none`, CPU,
memory, process and wall-clock limits.

## Entry points and routing

`SearchManager` exposes `search`, `search_internal`, asynchronous wrappers,
infohash-search wrappers, `suggest`, `get_epg` and disk-cache maintenance.
`WebSearchManager` is separate; preferences disable its optional web-search path
by default. Its existence is not evidence that normal name search uses the
operator-facing public search API.

The native method map is:

| Method | HTTP path |
| --- | --- |
| `search` | `/a` |
| `suggest` | `/b` |
| `get_epg` | `/c` |

The host list is shuffled for failover. In the offline interception fixture,
one valid response completed the request. A bad response signature or wrong
nonce caused attempts against all three hosts, followed by
`SearchRequestNetworkError`. The HTTP helper received a timeout argument of
30 seconds. This does not prove a 30-second whole-operation deadline or a
response-size bound. Outpace should retain its own stricter resource policies.

Only `/a` was dynamically exercised. `/b` and `/c` are established by the
constructors' method map, not by live requests or EPG/suggestion fixtures.

## Compact query fields

Both constructors initialize this map:

| Input | Wire key | Input | Wire key |
| --- | --- | --- | --- |
| `query` | `a` | `name` | `b` |
| `category` | `c` | `epg_name` | `d` |
| `epg_desc` | `e` | `bitrate_gte` | `f` |
| `bitrate_lte` | `g` | `page` | `h` |
| `page_size` | `i` | `lang` | `j` |
| `api_version` | `m` | `sort` | `n` |
| `flags` | `o` | `infohash` | `p` |
| `country` | `q` | `language` | `r` |
| `preferred_languages` | `s` | `preferred_countries` | `t` |
| `verbose` | `u` | `_data` | `_d` |

The normal 3.2.17 search call selected API version 4, even though the public
server API example uses version 1. An infohash filter used `/a` with key `p`;
it did not use a separate content-ID reverse-lookup endpoint.

Generated metadata includes `_n` (engine version string), `_v` (numeric version),
`_p` (platform), `_t` (integer timestamp), `_r` (random request nonce) and `_s`
(request signature). The observed context advertised its actual 3.2.17/Android
identity. Optional context fields and a device-ID-derived `_c` exist in the
builder. The fixture used an empty synthetic device-ID map and no account state;
no account or premium behavior was exercised.

A caller `_r` supplied to the public `search` method did not become the request
nonce: the observed request used its generated nonce. Do not infer that the
public wrapper rejects the field before networking; it did not in this fixture.

## Request signing: verified GET case

Let `K_request` denote the embedded 56-byte request shared key. For the observed
GET request, after mapping fields and adding generated metadata:

```text
pairs = [key + "=" + value for key in lexicographically_sorted_keys]
preimage = UTF8("#".join(pairs)) || K_request
_s = lowercase_hex(SHA1(preimage))
```

`pairs` excludes `_s` itself. Values in this preimage are the decoded string
values, not their percent-encoded representations. The URL query form-encodes
each value separately and adds `_s`. The path and hostname are not part of the
verified preimage. An independent implementation reproduced every intercepted
request signature, including an invented query containing `&`, `+` and non-ASCII
text. The exercised requests had no POST body. Optional payload/POST variants
were identified in the native routine but are not qualified by these fixtures.

## Response verification and decoding

The wire response is an outer JSON object containing `data` and `sign`.
`data` is a Base64 string containing an inner JSON result. Let `K_response`
denote the distinct embedded 53-byte response shared key:

```text
expected_sign = lowercase_hex(SHA1(
    ASCII(data) || UTF8(request_nonce) || K_response
))
```

The normal request passes its nonce into the response parser. It compares the
signature before Base64-decoding `data` and parsing the inner JSON. The routine
also has a no-nonce variant; that variant was not used to qualify the normal
search path. A correctly signed synthetic result was accepted; an all-zero
signature and a signature computed for a different nonce were rejected.

These are **shared-key checks**, not server-only digital signatures. The client
contains the keys, and the native host URLs are plain HTTP. A compatible
implementation must not promote search metadata into trusted playback
provenance on this basis. No encryption was needed for the qualified GET/result
path; Base64 is an encoding.

## Result normalization and cache behavior

The original `search` wrapper expects `total`, `time` and `results`, and adds
`request_time`. Grouped results have `name` and `items`; compact stream fields
are expanded, including:

| Wire item field | Output |
| --- | --- |
| `n` | `name` |
| `i` | `infohash` |
| `u` | `url` when the infohash form is absent |
| `b` | `bitrate` |
| `c` | `categories` |
| `a` | `availability` |
| `au` | `availability_updated_at` |
| `ci` | `channel_id` |
| `s` | `status` |
| `d` | `disabled` (default false) |

The parser also handles countries, languages and optional `_data`; group icons
and optional metadata can be retained. Playlist/favorite enrichment exists but
was not part of the no-account fixture. No meaning or descriptor trust was
established for `_data`, and no content-ID derivation was recovered from it.

A signed nonempty synthetic group was expanded into the expected `name/items`
shape, with the synthetic infohash preserved. Repeating an identical successful
query made no new HTTP-helper call. Preferences in this package default memory
caching on with a 120-second TTL; disk caching defaults off (240-second TTL,
3600-second update interval and 1000-item page setting). Only immediate memory
reuse was dynamically checked, not TTL expiry or disk-cache operation.

## Reproduction and limits

The private original-module fixture initializes preferences normally, creates
unstarted engine/search objects and supplies an empty synthetic device-ID map.
Only the HTTP helper is replaced with an in-memory response producer; the
original request builder, signature checker and result normalizer execute.
The container has no network, so this cannot prove remote service availability.
The final executable checks covered:

- Independent request-signature agreement, including form-encoding edge cases.
- Valid empty result acceptance and nonempty compact-item normalization.
- Bad-signature and wrong-nonce rejection, with three-host fallback.
- Generated nonce taking precedence over the caller's `_r`.
- Immediate successful-query memory-cache reuse.
- Infohash-filter request construction via the ordinary search method.

The convenience bulk/asynchronous APIs, suggestions, EPG, optional POST payloads,
web-search backend, account enrichment and real remote services remain
unqualified. No live sources, playback measurements or source-wide availability
claims are part of this research.

## Implications for Outpace

#408 implements the **public HTTP search API backend**, requiring a configured
API key. That is distinct from the native signed service protocol recovered
here. Its operator-key requirement is an implementation choice for that backend,
not a requirement established for all original-engine search.

A native-compatible backend is now a concrete implementation candidate that
could avoid operator API-key provisioning. Before adoption, validate current
endpoint reachability, responses and usable transport security; the original
code's HTTP URLs do not establish HTTPS support. Preserve the separate,
default-off services gate, private-mode egress restrictions, bounded requests,
identifier hygiene and existing descriptor/media verification. Do not copy an
old engine's device or account identity.

This does not solve the older scraper's reverse-content-ID workflow, prove a
production credential/availability contract, or authorize a DHT crawler. #406
remains the client/access qualification tracker.
