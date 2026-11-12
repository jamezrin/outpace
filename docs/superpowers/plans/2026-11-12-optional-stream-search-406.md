# Optional Stream Search Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the separately opted-in, bounded stream-name search adapter approved in #406 and documentation PR #407.

**Architecture:** Keep discovery metadata and external search work in `ace-engine::acestream_search`; do not modify descriptor trust or catalog defaults. Normalize only the named search contract, expose `/search` independently of legacy HTTP compatibility, and use existing explicit infohash playback selectors. No fabricated content IDs.

**Tech Stack:** Rust, Tokio, axum, serde_json, existing guarded HTTP client in ace-swarm.

**Spec:** Approved `docs/wiki/Stream-Search.md` in PR #407 and issue #406. This plan records implementation choices within that scope.

## Global constraints

- Public, unencrypted content only; default-off services integration, independent of `OUTPACE_ACE_COMPAT`.
- Fixed HTTPS upstream, no operator-supplied URL and no redirect forwarding of an API key.
- An explicit operator-supplied key is required; no embedded beta/test credential or account feature.
- Existing signed-catalog resolution, verification and private-peer egress restrictions remain intact.
- Synthetic identifiers/names only in tests and tracked artifacts; credentials and raw upstream bodies never appear in diagnostics.

## Investigation: 2026-11-12

Primary sources:
- Historical server API: https://wiki.acestream.media/Search_API
- Engine endpoint: https://docs.acestream.net/developers/search/
- Current scraper search service: https://github.com/visesa84/acestream-scraper/blob/5668ec41d089/app/services/acestream_search_service.py
- Current client migration: https://github.com/visesa84/acestream-scraper/commit/5668ec41d089

The client repository now resolves to `visesa84/acestream-scraper`. Its November 11 migration calls the external search service directly, parses a flat response and saves explicit `ih:` selectors; it removed reverse content-ID lookup. The older workflow recorded in #406 must therefore be treated as a historical client contract. Current clients already have a supported selector path through Outpace's existing infohash catalog resolution; this does not prove live playback for any search result, or make old reverse-lookup clients compatible.

Twelve bounded HTTPS probes were made in total (12-second per-request timeout, 128 KiB read cap, page size one, empty or invented queries). Raw bodies, source names and identifiers were not persisted. The legacy host redirected to `api.acestream.me`. Requests with the historically documented test key returned JSON with root `total`, `results`, `time` and `groupped`; grouped rows contained `name` and `items`, whose keys included `infohash`, `name`, categories, bitrate and availability metadata. One browse response contained one group and one item. Missing-key responses did not establish the same contract: one returned category-shaped rows instead. Some different-header attempts were not JSON. This is bounded feasibility evidence, not an upstream SLA, a production key entitlement, or authentication/limit qualification. Operators must supply an accepted key; historical shared-key success is not a production guarantee.

## Review focus

- Current upstream root JSON versus historical `result` envelope: same normalized client result; malformed shapes fail distinctly from empty results.
- Missing/invalid keys and upstream errors: sanitized failures, never empty success or credential-bearing logs.
- Duplicate/malformed query fields and oversized pages: reject before any outbound work.
- Requests under overload, timeout or cancellation: bounded active attempts, cache memory and lifetime; fail promptly and release capacity.
- Disabled integration/private peer mode and known-input playback: no new search egress and no alteration of existing playback/catalog defaults.

## Task 1: Pure query and response contract

**Files:** `crates/ace-engine/src/acestream_search.rs`, `crates/ace-engine/src/acestream_search/tests.rs`, module declaration in `lib.rs`.

**Interfaces:** `SearchQuery::parse(Vec<(String, String)>) -> Result<SearchQuery, SearchError>`; `normalize_response(&[u8], &SearchQuery) -> Result<Value, SearchError>` produces only a `result.total/results` plus `error: null` envelope. Query fields are `query`, `page`, `page_size`, optional `category`.

- [x] Add failing tests for zero-based defaults, limits, duplicates, UTF-8 query preservation and unknown fields; query <=256 bytes, category <=64 bytes, page <=10,000, page size 1..=200.
- [x] Add failing tests for flat/enveloped grouped results, empty success, upstream error, malformed JSON, invalid identifiers, ungrouped rows and result-size bounds. Group name/item name <=256 bytes; <=200 total items per page and <=requested page size groups; body <=1 MiB.
- [x] Implement deterministic parsing and bounded normalization with a metadata allowlist; no upstream error text or unvalidated identifiers in diagnostics.
- [x] Run the focused tests, formatting, then commit the independently usable parsing slice.

## Task 2: Guarded service, cache and request admission

**Files:** `acestream_search/service.rs` and its tests; reuse `ace_swarm::http_fetch`.

**Interfaces:** `SearchService::search(SearchQuery) -> Result<Bytes, SearchError>`; `SearchSettings` owns the redacted API key. Fixed `https://api.acestream.me/`, no redirects. URL-encode each query parameter independently.

- [x] Write failing tests for request encoding, key redaction, per-page caching, expiry and cache byte eviction, upstream failures, deadline/size caps, and cancellation releasing admission.
- [x] Implement a 10-second total deadline, 1 MiB response limit, at most two active attempts, a bounded 8 MiB/32-entry cache with 30-second TTL, minimum one second between outbound starts, and 30-second failure cooldown. Cache only validated successes; distinguish overload/rate failures from valid empty results.
- [x] Test against an injected offline transport at the service boundary; do not weaken production destination policy for tests.
- [x] Run focused tests and lint, then commit.

## Task 3: Configuration, route and playback boundary

**Files:** `config.rs`, `runtime.rs`, `http.rs`, `http/acestream_search.rs`, HTTP/runtime tests, `.env.example`, `compose.yml`.

- [x] Write failing tests for independent flag combinations and existing native playback with services disabled/failed. `/search` requires only services opt-in; legacy `/server/api` still requires HTTP compatibility.
- [x] Add strict `OUTPACE_COMPAT_ACESTREAM_SERVICES` boolean (default false) and `OUTPACE_ACESTREAM_SEARCH_API_KEY` (required only when enabled, redacted in Debug/serialization). Reject services opt-in in private peer mode before listeners open.
- [x] Construct the service only after validation; expose bounded `/search` route and typed sanitized errors. No new resolution or reverse lookup behavior.
- [x] Add deterministic search → selection of an explicit infohash selector → existing verified-descriptor playback-path coverage, and state the historical-client limitation explicitly.
- [x] Run offline tests and commit.

## Task 4: Operator documentation and full validation

**Files:** README, configuration/variables/search/playback/migration wiki pages, compatibility matrix/inventory, `.env.example`, issue #406.

- [x] Document actual flags, required key, fixed upstream, bounds, error behavior, privacy and current/older client integration requirements; preserve the fact that result availability does not prove playback.
- [x] Run `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, identifier hygiene and wiki link checks.
- [x] Request branch review, fix material findings, push and open a PR. Do not close #406 until the named end-to-end acceptance path and external access requirements are actually supported and qualified.

## Completion boundary

The adapter and its offline/synthetic playback contract are implemented for PR #408. Full workspace tests, workspace all-target Clippy, formatting, identifier hygiene, wiki links and Compose configuration pass. The fresh branch review found no blockers. The production-transport discovery probe passed with the historical public test key, without source metadata output. This does not establish production key entitlement or deployed scraper behavior. #406 remains open for the named client's endpoint/parser integration and production access qualification.
