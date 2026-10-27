# Optional live-BT hash fields (#253) implementation plan

> **Status tracking:** Implementation, tests, documentation, review and genuine offline synthetic validation are complete. Checkboxes record completed work and remaining CI/merge steps.

**Goal:** Hash present optional live-BT selector fields in the recovered order while preserving six-field and VOD compatibility.

**Architecture:** One fixed fifteen-key raw Bencode selector, with the first six still required. One decoded-descriptor hash entrypoint reuses the decoder's existing live/VOD classification and keeps legacy VOD on the six-field rule; existing consumers retain validation and authentication ordering.

**Tech Stack:** Existing Rust toolchain, `Bencode`, SHA-1 and transport encoder; no dependency/configuration change.

**Spec:** [Design and acceptance contract](../specs/2026-10-27-live-descriptor-optional-hash-fields-253-design.md).

**Status:** Implementation, tests, documentation, independent review and genuine offline synthetic validation are complete. CI and merge remain pending; deployment is outside this plan and has not been authorized.

## Global constraints

- Luna owns RED/tests. Wait for root's actual validated RED handoff before changing product code; preserve concurrent edits in shared files.
- Required six: `name, authmethod, pubkey, piece_length, chunk_length, bitrate`.
- Optional suffix, in order: `premium, commerce, provider_key, provider_sid, access, sharing, private, tns, license`.
- Membership only: raw values, including zero/empty, survive; absent keys supply no default. Ignore all unlisted keys.
- Preserve existing missing-base errors, decoder classification, authentication, geometry, wrappers, VOD and default configuration.
- No tracked real identifiers/names/hashes/keys, original artifacts or private captures; no registry-reading harness or live validation dependency.
- The implementation and local validation are complete. Root owns current CI and merge; no deployment is included.

## Review focus

1. Missing required base field with optionals present must still fail — Task 1 required-field table.
2. Present zero/empty differs from absence — Task 1 membership table.
3. Dictionary order or text normalization must not change the raw ordered encoding — Task 1 independent order/type expectations.
4. Unlisted metadata must not enter the hash — Task 1 tracker/quality/category controls.
5. Shared consumers must not silently change VOD identity/classification or bypass key/geometry checks — Task 2 transport/StreamInfo/VOD controls.

## Task 1 — Luna: freeze the synthetic selector RED

**Files:** Preserve/extend Luna's in-progress tests in `crates/ace-wire/src/infohash.rs`; existing synthetic vector at `tests/vectors/transport/synthetic-live-512k.bin`. No product implementation in this task.

**Interfaces:** Consume `infohash_of_descriptor(&Bencode) -> Result<[u8; 20]>`, `decode_transport` and `encode_transport`. Produce root's RED handoff and an independent test-side expected ordered-pair encoding; no new production API.

- [x] Preserve the `original_bt_hash_includes_present_provider_selectors_in_order` test. The genuine offline synthetic control accepted the provider-pair fixture and confirmed eight selected original pairs, equality with the postfix Rust candidate, and agreement on the shared six fields.
- [x] Pin absence compatibility and ignored tracker metadata using the accepted synthetic positive. Expected digests are independently derived.
- [x] Add focused pure tests for selector order, required fields, raw zero/empty membership, and ignored metadata. These tests make no broader original-schema acceptance claim.
- [x] Run the focused wire tests and retain the genuine eight-versus-six RED plus postfix eight-versus-eight GREEN evidence.

## Task 2 — Luna: pin consumer and VOD boundaries

**Files:** Tests beside `crates/ace-swarm/src/resolve.rs` and, where useful, `crates/ace-swarm/tests/metainfo.rs` / `disk_envelope.rs`. Reuse wire tests rather than editing original captured vectors.

**Interfaces:** Consume `try_infohash_of_transport(&[u8])`, `stream_info_from_transport(&[u8])`, `vod_info_from_transport(&[u8])` and existing `InfohashIndex`. Produce synthetic propagation/compatibility assertions for Task 3.

- [x] Extend a synthetic live descriptor with the provider pair; wire and StreamInfo hashes match the independent eight-pair expectation while geometry, source key, signature length and index behavior remain covered.
- [x] Add legacy wrapped VOD with those optionals; wire, StreamInfo and VOD paths retain the independent six-field identity and existing classification/geometry behavior.
- [x] Retain malformed wrapper, raw/disk-metainfo VOD, invalid geometry, catalog/authentication and source-key controls; add only focused seam coverage.
- [x] Run focused `ace-swarm` resolve, metainfo and disk-envelope tests and retain the completed RED/GREEN handoff.

## Task 3 — Implement only after root's RED handoff

**Files:** Product changes limited to `crates/ace-wire/src/infohash.rs` and hash imports/assignments in `crates/ace-swarm/src/resolve.rs`. Do not overwrite Luna's co-located tests. No `transport.rs`, broadcast, private oracle/probe or dependency change is expected.

**Interfaces:** Preserve `infohash_of_descriptor(&Bencode) -> Result<[u8; 20]>` as the live raw entrypoint and existing transport APIs. Add `infohash_of_decoded_transport(&TransportDescriptor) -> Result<[u8; 20]>`. Internally use one private fixed-policy routine, `hash_descriptor(descriptor: &Bencode, include_optional: bool) -> Result<[u8; 20]>`; no public arbitrary selector.

- [x] Implement the fixed ordered fifteen-key table and shared raw pair-building routine, preserving required-base errors and raw membership semantics.
- [x] Keep the raw live entrypoint on the live selector and use the decoded descriptor's existing `is_live` classification to retain the six-field VOD selector.
- [x] Route transport and swarm hash assignments through the decoded helper after existing validation; leave raw/disk-metainfo VOD and broadcast behavior unchanged.
- [x] Run focused GREEN tests, wire vectors and resolve metadata tests without weakening independent expectations.

## Task 4 — Verify, document and review

**Files:** Later update the formula explanation only in `docs/protocol/transport-file.md` and qualify the historical six-field observation in `docs/protocol/notes/29-infohash-formula-cracked.md`. Preserve unrelated edits/old evidence.

**Interfaces:** Task 3's unchanged existing APIs plus the decoded-descriptor helper; recorded synthetic RED/GREEN evidence.

- [x] Document the ordered selector, exact presence filtering, required-base behavior and VOD boundary; state that the synthetic provider example does not identify discarded production fields or a historical playback cause.
- [x] Complete formatting, lint, workspace/focused test and identifier-hygiene validation; the workspace suite reported 1,468 passing tests and the later focused additions passed.
- [x] Complete independent product review and genuine offline synthetic validation. The postfix oracle confirmed a hash match with equal eight-pair preimages and passing controls.
- [x] Commit only the reviewed six-file allowlist locally with the conventional `fix(ace-wire): include optional live hash fields` subject; no push, PR creation, merge or deployment is part of this local commit.
- [ ] Complete current CI and merge review before merging. Deployment remains out of scope and unperformed.

## Planning self-review

All spec requirements map to Tasks 1–4. The five review-focus inputs have test coverage; the decoded helper centralizes existing classification without decoder changes. Implementation, tests, documentation, independent review and genuine synthetic validation are complete. Current CI and merge remain pending. No deployment is included.
