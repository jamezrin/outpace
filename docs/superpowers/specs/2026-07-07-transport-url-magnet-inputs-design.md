# Transport-file URL and magnet playback inputs (issue #50)

## Goal

Outpace today accepts only `acestream://<content_id>`, `acestream:?content_id=…`,
`acestream:?infohash=…`, and bare 40-hex infohash provider IDs. The original Acestream
playback API also accepts a transport-file **`url=`** and a **`magnet`**. This work adds those
two input shapes so operators can start playback from a descriptor they already hold, without
going through catalog/content-id resolution.

Closes #50. Part of the #46 engine-parity epic.

## Scope

In scope:

- Parse `magnet:?xt=urn:btih:<hash>` inputs (CLI, `acestream:?magnet=`, `/ace/getstream?magnet=`).
- Parse transport-file `url=` inputs (CLI `acestream:?url=`, `/ace/getstream?url=`).
- Fetch a remote transport file over HTTP/HTTPS with **fail-closed** safety limits
  (SSRF guard, size cap, timeouts, no redirects).
- Decode the fetched transport with the existing `decode_transport` /
  `stream_info_from_transport` path.
- Enforce selector precedence `content_id > infohash > url > magnet`.

Out of scope (unchanged behavior / non-goals):

- Live/content-id/infohash playback behavior is untouched.
- No BitTorrent v2 (`btmh`) magnets, no private/encrypted transports — rejected with a clear
  error.
- No new native `/streams` request shape beyond what a provider ID already supports.

## Background: how inputs resolve today

Two provider-ID shapes reach `AceProvider::open(id)`
(`crates/ace-engine/src/ace_provider.rs`):

- `cid:<40hex>` — resolved over the network (signed catalog, then BEP-9 `ut_metadata`).
- bare `<40hex>` — an infohash, resolved directly via `stream_info_from_infohash` with default
  live geometry.

Input surfaces that mint those IDs:

- **CLI** `PlaybackTarget::parse` (`crates/ace-engine/src/cli.rs`): `acestream://<id>` →
  `cid:<id>`; `acestream:?content_id=` → `cid:<id>`; `acestream:?infohash=` → bare infohash.
- **HTTP** `ace_selected_stream` (`crates/ace-engine/src/http.rs`): `content_id` →
  `cid:<id>`; else `infohash`/`id` → bare infohash.

The transport-file decoder already exists and is pure:
`ace_swarm::resolve::stream_info_from_transport(bytes) -> Result<StreamInfo, ResolveError>`
(validates geometry, computes the real infohash, extracts trackers + pubkey). The signed
catalog path already fetches a transport over a hand-rolled raw-TCP HTTP request and enforces a
1 MiB response cap (`CATALOG_RESPONSE_LIMIT`). There is no HTTP-client dependency in the tree.

## Design

Three layers, mirroring the existing structure.

### 1. Input parsing (per surface, enforces precedence)

Precedence when multiple selectors are present: `content_id > infohash > url > magnet`.

**Magnet** maps onto the **existing bare-infohash path** — it needs no new provider scheme.
A `magnet:?xt=urn:btih:<hash>` yields a 20-byte infohash, rendered as a bare 40-hex provider
ID, so magnet playback dedups with direct-infohash playback and the session key is identical.

**Transport URL** gets a **new provider-ID scheme `turl:<url>`**, dispatched in the provider.

- **CLI** (`cli.rs`):
  - Extend `acestream:?` handling: after `content_id` and `infohash`, check `url` then `magnet`.
  - Accept a bare `magnet:?…` input (scheme `magnet:`).
  - New target constructors: `url_target(url) -> Ok(PlaybackTarget{ provider_id: "turl:<url>" })`
    after validating the scheme is http/https and the URL is non-empty; `magnet_target(m)`
    parses the btih and reuses `infohash_target`.
- **HTTP** (`http.rs` `ace_selected_stream`): after `content_id`/`infohash`/`id`, add `url`
  (→ `public_id`/`playback_id`/`session_key` = `turl:<url>`), then `magnet` (→ bare infohash
  from btih). Malformed url/magnet params make selection fail with the existing
  "missing/invalid" JSON error rather than silently falling through.

### 2. Provider dispatch (`ace_provider.rs::open`)

Add one branch before the bare-infohash check:

```
if let Some(url) = id.strip_prefix("turl:") {
    stream_info_from_transport_url(url).await
        .map_err(|e| ProviderError::Backend(format!("transport url: {e:?}").into()))?
}
```

Magnet needs no branch here (already a bare infohash by this point).

### 3. Guarded fetch (`ace-swarm/src/resolve.rs`)

New `pub async fn stream_info_from_transport_url(url: &str) -> Result<StreamInfo, ResolveError>`
and a new error variant `ResolveError::Url(&'static str)`.

Steps, all **fail-closed**:

1. **Parse + scheme check.** Parse the URL; require scheme `http` or `https`. Anything else
   (`file`, `ftp`, `gopher`, …) → `Url("unsupported scheme")`.
2. **SSRF guard.** Extract host + port. Resolve the host with `tokio::net::lookup_host`. Reject
   if the resolved `IpAddr` is loopback, private (RFC1918), link-local, unspecified, broadcast,
   documentation, shared/CGNAT (100.64.0.0/10), or multicast for IPv4; and loopback,
   unspecified, link-local, unique-local (fc00::/7), or multicast for IPv6. Keep the **first
   safe** resolved address.
3. **Pin the connection.** Build a `reqwest::Client` with `.resolve(host, safe_addr)` so the
   request connects to exactly the validated address (defeats DNS-rebinding TOCTOU), with
   `.redirect(Policy::none())` (a redirect is an SSRF bypass), a connect timeout, and a total
   request timeout.
4. **Size-capped body.** Stream the response with `Response::chunk()`, accumulating into a
   `Vec<u8>`; abort with `Url("transport too large")` once the accumulation would exceed
   `MAX_TRANSPORT_FILE` (reuse the existing 1 MiB transport ceiling — same bound the catalog
   path and `MAX_METADATA_SIZE` already impose on this descriptor). Never trust
   `Content-Length`. A non-200 status → `Url("http status")`.
5. **Decode.** `stream_info_from_transport(&body)`. A body that isn't a valid transport fails
   closed through the existing decoder error, surfaced as `Transport(_)`.

**Test seam.** The SSRF guard must stay strict in production, but deterministic fetch/size-cap
/decode tests need a loopback server. Split the function so the guard is a standalone,
directly-unit-testable `fn resolve_safe_addr(host, port) -> Result<SocketAddr, ResolveError>`,
and the fetch body takes an already-validated `(url, safe_addr)`. Deterministic tests drive the
fetch/size-cap/decode logic against a `tokio` loopback server through that inner seam; the
guard is tested separately with IP literals; a public-URL end-to-end test is `#[ignore]`.

### Magnet parsing (input layer, no new dependency)

`fn parse_magnet_infohash(magnet: &str) -> Result<String, String>`:

- Require the `magnet:` scheme (bare) or a `magnet=`-supplied value.
- Read `xt` params; find `urn:btih:<hash>`.
- Accept `<hash>` as 40 hex chars (lowercased) **or** 32-char RFC-4648 base32 (decoded to 20
  bytes → 40 hex). Base32 decode is hand-rolled (~20 lines), avoiding a new dependency.
- Reject: no `xt`, only `urn:btmh:` (v2), or an unparseable hash → clear error string.

### Dependency

Add to `crates/ace-swarm/Cargo.toml` only:

```
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls", "stream"] }
```

`default-features = false` avoids pulling native-tls/openssl; `rustls-tls` gives HTTPS with no
system OpenSSL; `stream` enables the chunked, size-capped body read.

## Error handling

| Condition                              | Result                                            |
|----------------------------------------|---------------------------------------------------|
| Non-http/https scheme                  | `ResolveError::Url("unsupported scheme")`         |
| Host resolves only to unsafe addresses | `ResolveError::Url("blocked address")` (SSRF)     |
| DNS failure / connect / timeout        | `ResolveError::Url("fetch failed")`               |
| Redirect returned                      | Not followed → non-2xx → `Url("http status")`     |
| Body exceeds cap                       | `ResolveError::Url("transport too large")`        |
| Body not a valid transport             | `ResolveError::Transport(_)` (existing decoder)   |
| Magnet without supported btih          | Clear error string (CLI) / selection error (HTTP) |

All map to `ProviderError::Backend` at the provider boundary, so playback surfaces a clean
error instead of hanging.

## Testing (deterministic unless noted)

- **Precedence (CLI + HTTP):** `content_id` beats `infohash` beats `url` beats `magnet` when
  several are present.
- **Magnet:** 40-hex btih → correct infohash provider ID; 32-char base32 btih → same 20 bytes;
  reject `btmh`-only and malformed magnets.
- **SSRF guard (`resolve_safe_addr`):** reject `127.0.0.1`, `10.x`, `192.168.x`, `169.254.x`,
  `100.64.x`, `::1`, `fc00::…`, `0.0.0.0`; accept a public literal.
- **Fetch path (loopback server via the inner seam):** happy path decodes a real transport
  fixture to the expected infohash; oversized body → `Url("transport too large")`;
  non-transport body → `Transport(_)`; non-200 → `Url("http status")`.
- **Scheme rejection:** `file://…`, `ftp://…` rejected.
- **Regression:** existing content-id/infohash CLI + HTTP tests still pass.
- **`#[ignore]`:** a live public transport-file URL end-to-end fetch.

## Acceptance criteria (from #50) → coverage

- *Play from a transport-file URL* → provider `turl:` branch + guarded fetch + decode.
- *Supported magnet plays or clear provider error* → magnet→infohash path + rejection tests.
- *Unsafe URLs / oversized / unsupported schemes / invalid bodies fail closed* → SSRF guard,
  size cap, scheme check, decoder error — all covered by tests.
- *Existing content-id/infohash behavior unchanged* → precedence keeps them first; regression
  tests.

## Files touched

- `crates/ace-swarm/Cargo.toml` — add `reqwest`.
- `crates/ace-swarm/src/resolve.rs` — `stream_info_from_transport_url`, `resolve_safe_addr`,
  fetch seam, `ResolveError::Url`, magnet-independent (magnet lives in the input layer).
- `crates/ace-engine/src/ace_provider.rs` — `turl:` dispatch branch.
- `crates/ace-engine/src/cli.rs` — `url`/`magnet`/bare-`magnet:` parsing + precedence,
  `parse_magnet_infohash`, base32 decode.
- `crates/ace-engine/src/http.rs` — `url`/`magnet` params in `ace_selected_stream` + precedence.
- Docs: note the new supported inputs where the CLI/HTTP inputs are documented (README).
