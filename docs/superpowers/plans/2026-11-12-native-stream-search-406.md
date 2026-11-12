# Native Stream Search Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Adapt #408 to the recovered native search protocol, without operator API-key provisioning.

**Architecture:** Replace the public API transport with fixed native `/a` service requests, signed using recovered protocol constants. Verify nonce-bound response signatures before decoding compact metadata. Retain the bounded search service, separate opt-in, synthetic playback contract and existing descriptor verifier.

**Tech Stack:** Rust, Tokio, serde_json, SHA-1, Base64, existing guarded HTTP fetcher.

**Spec:** `docs/protocol/notes/74-ace-search-native-services.md` in #409; user authorized adaptation on 2026-11-12. This plan supersedes the public-backend/key decisions in `2026-11-12-optional-stream-search-406.md`.

## Global Constraints

- Services remain default-off and independent of the HTTP compatibility flag; private peer mode rejects opt-in.
- Fixed `http://s5.torrentstream.net`, `.org`, `.info` hosts and `/a`; no arbitrary URLs, redirects, environment proxies, TLS bypass or public API fallback.
- Native hosts currently fail HTTPS hostname verification. The opt-in uses plain HTTP explicitly; document query/IP exposure. Shared-key checks do not establish server-only authenticity or descriptor trust.
- Protocol constants are embedded public-client compatibility material, like existing transport keys, not personal/account credentials. Do not copy device/account identity; advertise Outpace and its own package version.
- Request nonce is a fresh positive decimal integer. A synthetic live probe with an Outpace identity and decimal nonce produced a valid signed empty result. Hexadecimal nonces did not; never accept a missing/invalid nonce-bound signature.
- At most three fixed-host attempts in a ten-second total operation; three-second per-host budget. Each wire/decoded result is <=1 MiB, and cumulative wire bytes across failover are <=1 MiB.
- Preserve two concurrent operations, one-second start interval, 30-second failure cooldown, 32-entry/8-MiB/30-second success cache, query and metadata limits.
- No real source names, identifiers, private captures, or personal secrets in tracked material or diagnostics. Tests use synthetic data only.

## Review Focus

- Special characters in query/category must be signed before percent encoding; reserved caller fields remain rejected.
- Invalid signature, stale/different nonce, invalid Base64/JSON and URL-only items must never become successful cached pages.
- Failover must validate each response, preserve destination guards, and obey cumulative bytes/time limits, including cancellation.
- Compact fields must retain valid metadata but strip content-ID claims, URLs and unknown fields; descriptor verification remains authoritative.
- No key variable or account/device identity is required; flag combinations and private mode remain enforced.

## Task 1: Native request/response codec

**Files:** `acestream_search/native.rs`, `acestream_search/native/tests.rs`, existing normalization tests, `Cargo.toml`/lock.
**Interfaces:** `NativeRequest::new(&SearchQuery, timestamp, nonce)`; `url(host)`; `decode_response(&[u8], nonce) -> Result<Vec<u8>, SearchError>` returning validated expanded metadata for the existing normalizer.

- [x] Write failing independent known-vector tests for signing/encoding, nonce binding, malformed envelopes and compact result expansion.
- [x] Run focused tests and observe the expected missing-protocol failures.
- [x] Implement compact v4 request fields, honest Outpace identity, embedded protocol constants, signature verification before bounded Base64/JSON decoding, and explicit allowed field expansion.
- [x] Run focused tests; cross-check the codec against the original module privately.

## Task 2: Bounded native transport and configuration

**Files:** `acestream_search/service.rs`, its tests, `acestream_search.rs`, `runtime.rs`, HTTP tests.
**Interfaces:** Preserve `SearchService::search(SearchQuery) -> Result<Bytes, SearchError>`. `SearchSettings` contains only `enabled`. Transport consumes a signed native request and returns verified expanded bytes.

- [x] Write failing tests for key-free opt-in, independent flags/private mode, verified failover, invalid signature/nonce exhaustion, size and total/per-host deadlines.
- [x] Replace public endpoint/key handling; randomize fixed host order, generate fresh decimal nonce/timestamp, bound each host and all attempts, sanitize every error.
- [x] Preserve existing admission/cache/cancellation and verified-selection tests; run focused suite.
- [x] Run a bounded ignored production-transport probe with an invented query; emit no source metadata.

## Task 3: Documentation and complete validation

**Files:** `.env.example`, `compose.yml`, user wiki configuration/search/variables pages, previous plan supersession note.

- [x] Remove API-key provisioning and document opt-in, HTTP privacy, fixed native hosts, failover/bounds and usable response contract. Research stays in developer notes.
- [ ] Run fmt, full workspace tests, all-target workspace Clippy, identifier hygiene, wiki links and Compose validation.
- [ ] Obtain one fresh whole-branch review, resolve material findings, commit/push and update #408 around the native backend. Keep #406 open for deployed client integration qualification.
