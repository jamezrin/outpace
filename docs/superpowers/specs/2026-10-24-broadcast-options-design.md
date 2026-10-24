# Broadcast startup profiles

Date: 2026-10-24. Status: complete design approved by the user after the fresh builder
check (`/tmp/outpace-051/builder-check.md`: “Nothing important left.”).
Implementation plan requires separate human confirmation before execution.

## Intent and scope

Finish the supported MPEG-TS origination options in #51 with one named startup profile
shared by raw PUT, RTMP, HTTP pull, and CLI creation/resume. Operators can supply a
title, explicit bitrate and piece size, quality, and categories without a new management API.
Keep existing producer admission, reconnect/cancellation, persistence, and seeding limits.

The selected alternative is a per-name process-start dictionary. Global-only knobs are
smaller but cannot describe independent origins; a precreation API plus CLI flags adds
runtime mutation/admission contracts unnecessary here. Neither alternative is selected.
This document's bounds and legacy-resume policy are design proposals for approval.

No automatic bitrate detection, transcoding, hot reload, per-profile trackers, arbitrary
metadata bag, new source protocol, or compatibility endpoint is included. #206 supplies
HTTP pull/retry behavior; #207 supplies canonical admission. Preserve those completed paths.

## Current code and public configuration

`broadcast.rs::build_descriptor` already emits all five proposed fields. Production
`http.rs::BroadcastState` callers currently pass the path name as the title. Geometry is
constant in descriptor construction, restored-state validation, store construction, both
`BroadcastIngest` chunker constructors, payload capacity, and full-piece progress detection.
Changing only the descriptor is therefore insufficient.

Add proposed `Config.broadcast_options: BTreeMap<String, BroadcastOptions>` and
`OUTPACE_BROADCAST_OPTIONS`, using a focused `broadcast_options.rs` module exported by
`lib.rs`. This is separate from `broadcast_sources`; a profile needs no source declaration.
Unset environment means `{}`; explicit `{}` is valid; blank/whitespace-only, non-UTF-8,
`null`, non-object, and trailing JSON are invalid. A profile alone creates no broadcast,
key, record, store, source job, or announcement. Explicit CLI creation retains its current
mint-before-producer behavior; existing persisted records still reload at startup.

```json
{"synthetic-origin":{"title":"Synthetic broadcast","bitrate_bytes_per_sec":500000,"piece_length":131072,"quality":"HD","categories":["other"]}}
```

Names use `valid_broadcast_name`: 1–64 ASCII bytes, alphanumeric or `._-`, excluding
`.` and `..`. The JSON input is at most 262144 bytes and 64 profiles. Reject duplicate
decoded names (including escaped equivalents), duplicate fields, and unknown fields.
Do not parse through an intermediate JSON value/map that discards duplicates.

| Optional field | Accepted explicit value | Fresh identity when absent |
| --- | --- | --- |
| `title` | UTF-8 string, 1–256 bytes after the text rules below | path name |
| `bitrate_bytes_per_sec` | JSON integer, 1–2305843009213693951 inclusive | 8375 |
| `piece_length` | JSON integer, power of two, 65536–1048576 bytes inclusive | 65536 |
| `quality` | exact string `auto`, `SD`, or `HD` | `auto` |
| `categories` | array of 0–32 distinct strings, each 1–128 UTF-8 bytes | `["other"]` |

All supplied fields reject `null` and wrong types. Reject fractional/exponent numeric
forms rather than rounding. For title/category text reject leading/trailing whitespace,
empty strings, and Unicode control characters; preserve accepted bytes exactly. Reject
oversize values instead of trimming/truncating. Category strings are bounded labels,
not a newly claimed taxonomy; order is preserved and explicit `[]` means an empty list.
Quality values are this interface's supported choices, not a claim about every engine.

Wire `bitrate` and `StreamMetadata.bitrate` are **bytes/second**, not kbit/s. The upper
bound is `u64::MAX / 8`, fits signed bencode integers, and permits exact bits/second
conversion with checked multiplication by eight. It is an arithmetic bound, not a
throughput guarantee. Reject zero; detection is not represented by a sentinel value.
Existing API bitrate projection remains bits/second; no bitrate unit changes elsewhere.
`chunk_length` is fixed at 16384 and is not an accepted profile key.

Retain missing-versus-present fields in `BroadcastOptions` until matching/resolution;
a plain default-filled struct loses required information. `Option` fields need strict
present-value deserialization so explicit null cannot become absence. Validate direct
Rust `Config` callers too, including map/name/count/text/numeric bounds and a bounded
compact JSON size counting every present field, even explicit defaults. Absence remains
omitted on serialization; do not omit present values merely because they equal defaults.

## Frozen identity, matching, and startup ordering

A fresh descriptor applies defaults only to missing fields. Keep the legacy exact field
set, bencode types/order, encoding, RSA generation, `outpace_name`, and
`allow_public_trackers=1`. No options means the same descriptor/transport bytes as the
old builder for identical name/title/key/tracker/encoder inputs; random fresh keys still
produce distinct identities. No new profile/version key is emitted in the transport.

`infohash_of_descriptor` hashes title (`name`), authmethod, pubkey, piece/chunk length,
and bitrate. Thus title/bitrate/geometry can change the swarm identity; quality/categories
are outside that subset but still change serialized descriptor/transport-file identity.
Never edit an existing descriptor in place, generate a replacement key on load failure,
or silently remint a name. Use a new name for changed options, or explicitly delete and
recreate with documented loss of identity/cursor continuity.

On resume, stored transport bytes and private key are authoritative. Compare **only
present profile fields** against their exact raw descriptor fields: integer equality,
exact text bytes, and ordered category list equality. Do not compare lossy/truncated
metadata projections. An absent stored field is not an implicit default. Omitted profile,
`{}`, and an omitted individual field assert nothing. Explicit `auto`, `8375`, `65536`,
or `["other"]` assert those values and conflict with different or absent stored fields.
No trimming, default filling, field insertion, or transport re-encoding occurs on resume.

New-origination geometry policy is intentionally narrower than consumption. Accept a
persisted live transport with positive pieces that are multiples of 16384 up to 8388608
bytes, including non-powers of two or sizes outside 64 KiB–1 MiB, if current consumer
validation and the signing/store checks below pass. Such a record resumes unchanged
when `piece_length` is omitted. An explicit out-of-origination-range profile value is
invalid even if it would match that record; an explicit in-range mismatch conflicts.
Likewise retain otherwise-valid legacy positive bitrate and metadata outside the new
profile bounds when omitted; existing consumer normalization/saturating API projection
for legacy metadata remains. Explicit fields must pass the new schema and match exactly.

Startup sequence, shared by env and direct-Config callers:

1. Validate the complete source and options dictionaries and existing runtime/cache
   configuration before normalizer preflight, identity creation, cache cleanup, or tasks.
2. Read persisted records without mutation; preflight valid records' geometry/store
   capacity and all explicit profile comparisons. A configured profile or configured
   source targeting invalid stored state fails startup. A profile conflict fails startup
   even if that name has no configured source. Preserve existing tombstones for unrelated
   malformed records; directory-enumeration failure remains fatal.
3. Install one immutable options dictionary in the registry before `reload_persisted`.
   Only after preflight succeeds perform normal runtime setup and actual reload/reservation.
   Revalidate at the real per-entry initialization boundary under its lifecycle/I/O fences,
   so a changed disk record cannot bypass checks. Read-only preflight makes no reservations.
4. Reload preserves raw transport/key and current reservation advance/atomic-save rules.
   Propagate conflicts, insufficient capacity, and configured-name invalid state instead
   of the present reload loop's log-and-continue; unrelated invalid records stay tombstoned.
   Failure during real reload uses existing runtime cleanup; no new rollback guarantee.
   Listeners, announcers, and configured source workers start only after successful reload.

## Geometry and cache invariants

Derive immutable effective geometry and tracker list from the validated transport once
per `Broadcast`. Before saving/reserving or leasing a store, retain existing live/RSA,
pubkey/key equality, required identity fields, `outpace_name`, and cursor checks. Require
`0 < signature_len < piece_length`, using the actual loaded key's modulus length; fresh
keys currently have 96-byte signatures. Neither signing chunker imposes a 1 MiB ceiling;
that is this interface's allocation policy, not an inferred protocol/signing limit.
Check piece/chunk divisibility, chunk count fitting `u16`, bencode `i64` representation,
and checked target `usize`/object-size conversions before constructing buffers/stores.

Every fresh effective profile, the unprofiled default, and every valid reloaded broadcast
must fit at least one **full wire piece** in `seed_store_bytes`, on both memory and disk.
Validate even unused declared profiles at startup; this does not mint them. A valid legacy
piece larger than the budget is a startup `broadcast_store_too_small` failure, not a
silent fallback or geometry change. Keep the existing memory-pool upper bound and disk
path protections; do not resize budgets, add bitrate-derived retention, or promise that
a single-piece budget keeps history. Existing disk-I/O fallback behavior is unchanged.

`BroadcastIngest::authorized`, rebasing in `push_bytes`, and `store_outputs` must use
effective piece/chunk lengths for payload capacity, both `SigningChunker::new` calls,
and last-full-chunk detection. `initialize` must pass those lengths to `build_piece_store`.
Reserve signature room in the piece; do not require `(piece_length - signature_len)` to
divide by 188. Keep TS resynchronization, per-piece signing/header semantics, checked
global piece advancement, partial EOF tail handling, and cancellation/publication fences.
`committed_pieces` counts full signed pieces only, including pull retry/healthy-progress
consumers; fixed 64 KiB input slicing is merely a feed bound and may remain unchanged.

## Trackers, errors, and exposure

Keep `OUTPACE_TRACKERS` as the sole global fresh-mint tracker input with its current
unset/blank defaults, filtering, 64-entry/256-byte caps, and non-UDP preservation. It is
not an assertion against persisted transports. Changing it affects future identities only.
`ensure_announcing` must take the effective list from `Broadcast`, not the current process
list, for both existing announcement keys. Empty stored lists remain empty; do not restore
public defaults. Existing UDP resolver/address policy, inbound-port gating, DHT behavior,
task ownership, and deduplication remain. Other schemes stay available in the descriptor
for other consumers; this adds no announcer protocol and makes no privacy guarantee.

Malformed options produce only `invalid_broadcast_options`; capacity preflight uses
`broadcast_store_too_small`; add proposed `BroadcastError::OptionsConflict` with code
`broadcast_options_conflict`. Invalid persisted data keeps `broadcast_state_invalid`.
Startup/CLI failures are nonzero and sanitized; runtime conflict maps to HTTP 409 using
the existing error response shape and RTMP rejection uses the same stable code. Direct
registry admission also checks options/capacity before persistence or store exposure;
an inconsistent direct-call store budget returns `StateInvalid`. Existing producer/source,
stopping, persistence, and ingest error meanings are retained.

Collapse serde/parser failures at public boundaries; do not echo raw JSON, unknown key
text, title/category values, descriptor bytes, upstream URLs, keys, or mismatch values.
Use redacted diagnostics for the new profile type and `alog!` for operational events.
Accepted metadata/trackers are intentionally public transport data; add no URL fetches,
status metadata dump, persistent profile copy, new bind default, or management route.

## Implementation seams and bounded acceptance

All interfaces below that do not exist today are **proposed**, not callable builder APIs.
`broadcast_options.rs` owns strict parsing/presence, validation, fresh resolution, and
raw-field matching. `config.rs` and `runtime::{config_from_env,build_runtime}` expose and
preflight it; `lib.rs` exports the module. `BroadcastRegistry::{prepare,mint,
acquire_producer,commit_source,reload_persisted}` and `broadcast.rs::initialize` enforce
the same immutable dictionary. Replace per-call title selection with registry resolution;
constructor plumbing supplies the dictionary, with existing no-options constructors using
an empty dictionary. Callers cannot override a named profile during producer admission.
`http.rs::BroadcastState` remains the common entrypoint; CLI `run_broadcast` via
`runtime::mint_broadcast`, `http/broadcast.rs`, `http/broadcast_source.rs`, and
`rtmp.rs::RtmpIngestHandler` must all reach that policy. `ensure_announcing` and
`Broadcast` carry effective trackers/geometry; adapt the test `AnnouncerFactory` to capture
the actual tracker list. `broadcast_ingest.rs` owns the geometry replacements above.
`broadcast_persist.rs` retains its disk schema and private atomic-save behavior.
Reuse `ace-wire::{transport,infohash,signing_chunker,live_auth}`,
`ace-swarm::{resolve::stream_info_from_transport,store::PieceStore,types::StreamMetadata}`;
do not expand their wire vocabulary or consumer geometry ceiling.

Luna owns **all** test authoring/execution, builds, fmt, checks, hygiene, and hooks;
Sol owns specification/review only. No commands or tests were run for this design.
Bound the implementation evidence to:

- Parser tests beside the new module: absent versus explicit defaults/null, duplicate and
  unknown rejection, numeric/text/map limits, one valid 128 KiB profile, cache boundaries.
- `broadcast.rs` tests: legacy default descriptor bytes with fixed synthetic key inputs,
  explicit fields/identity subset, exact persisted transport/key retention, matching/missing
  and conflicting fields, one consumer-valid geometry outside origination bounds, cursor
  reservation continuity and invalid-record behavior. Reuse persistence failure hooks.
- `broadcast_ingest.rs` tests: default and 128 KiB deterministic synthetic TS, real store
  chunk reassembly/signature verification, two full pieces plus finite tail, rebase/resume,
  and exact `committed_pieces` counts. No live media or new capture fixtures are needed.
- Existing HTTP/RTMP/pull/runtime/CLI test seams: one shared named profile reaches each
  adapter and CLI mint; startup conflict precedes effects; unused profile does not mint.
  Reuse `broadcast/tests_pull206.rs`, `broadcast_ingest/tests_pull206.rs`,
  `runtime/tests_pull206.rs`, and existing source-conflict/cancellation cases as appropriate.
- Fake announcer: fresh and resumed identities use their transport's trackers even after
  process tracker changes, including an empty stored list; no real network announcement.

Luna runs the repository fmt/clippy/workspace tests and identifier gate when implementation
is authorized. Examples/tests use synthetic data only. Update `README.md`,
`docs/native-api.md`, `docs/deployment.md`, and `docs/protocol/compat-matrix.md` for the
schema/units, every producer path, immutable resume and tracker migration, and scope below.

## Issue accounting and deferred work

This slice addresses #51's remaining supported metadata/explicit sizing acceptance while
preserving PUT/RTMP and the existing remote-pull/retry work. It does not complete #51's
broader metatracker/startup-node work: prove descriptor key/type/behavior separately;
consumer-side `OUTPACE_ACE_PEERS` is not a published startup-node option. Tags, logo, and
description also need wire evidence before emission. No arbitrary fields are invented.
Private peer policy/advertisement control, HLS transport/support behavior, and autonomous
support-node lifecycle/deployment remain explicit separate scopes; native HLS output and
empty tracker lists do not establish those capabilities. SRT remains separate (#7).
Root must record those dispositions/follow-ups before claiming #51 fully closed.
#205's actual official/outpace playback interoperability and deployment validation remain
separate; synthetic tests here establish neither full engine parity nor live performance.
