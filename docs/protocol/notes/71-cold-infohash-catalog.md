# 71 — Authenticated catalog lookup by swarm infohash

Date: 2026-10-18 (Europe/Madrid). Issue #176.

## Engine evidence

The original engine 3.2.11 can fetch a transport directly by swarm infohash.
An intermediate content-id lookup is not required for this playback path.
Static inspection of `TSService.get_torrent` found two request selectors: `pid`
for a player/content id and `infohash=tobase64_string(binary_infohash)` for an
infohash. The helper comes from `ACEStream.Core.Utilities.utilities`.

A private, fresh-engine capture confirmed the request through the normal HTTP
playback API. cid20 was supplied manually to a separate authenticated resolver,
and its runtime-derived infohash was then passed to a fresh original-engine
container. The resolver stopped before that container was started. Capture was
active before both the cold `getstream` call and the playback-body read.

The catalog request used TCP port 8081 and this shape:

```text
GET /gettorrent?_n=3.2.11&_p=linux&_r=<nonce>&_v=3021100&infohash=<form-encoded-padded-base64>&_s=<signature>
```

`infohash` encodes the 20 binary bytes, not the hexadecimal text. The engine's
helper uses standard Base64 with the `+/` alphabet and padding; a 20-byte value
has one trailing `=`. Query values are form-encoded, so `+`, `/` and `=` become
`%2B`, `%2F` and `%3D` where present. Earlier probes using hexadecimal parameter
values did not exercise this request format.

The signature is SHA-1 of the following raw, unescaped parameter material followed
by the existing catalog request key:

```text
_n=3.2.11#_p=linux#_r=<nonce>#_v=3021100#infohash=<padded-base64>
```

The captured request verified with the same key as the `pid` path. Its chunked
HTTP 200 XML response used root attributes `r` and `sig`, with direct `torrent`
and `checksum` children. Exact response-byte signature verification, request nonce
matching and decoded transport checksum validation all succeeded using the
existing catalog verifier. The shared-key/plain-HTTP trust limits in note 62
still apply; this does not establish server-only authentication.

The first capture mistakenly filtered port 80; its empty result is invalid
evidence of catalog absence. The corrected capture included only the owned
container's traffic across ports and established the exchange above. Owned
containers and capture processes stopped, and WARP was restored. Raw captures,
identifiers, tokens, responses and controller records remain private under
`/tmp/outpace-lookup-176-probe-1792277075/retry-correct`.

## Resolver behavior

Cold live infohash and magnet playback now follows the existing resolution flow:
owned broadcast, verified descriptor index, then authenticated catalog lookup by
infohash. Native clients use the explicit `ih:` selector; native bare hex ids
continue to denote content ids. CLI and compat selector precedence is unchanged.

The catalog response is authenticated before transport decoding. The descriptor's
computed swarm infohash must equal the requested value before discovery or
indexing. A mismatched descriptor is rejected without indexing either swarm.
The descriptor supplies geometry, source pubkey and trackers; no geometry is
guessed, and cold infohash lookup never uses BEP-9 as a fallback.

Successful descriptors enter the existing bounded index. Failed lookups are
suppressed for five seconds in a separate cache capped at 128 entries, keyed by
binary infohash. Expired failures can be retried; local verified metadata wins
over a prior failure. Cache locks are synchronous and are not held across network
awaits. The caches remain in-process.

The synchronous provider pre-check permits cold inputs. JSON `getstream` may mint
URLs before asynchronous resolution; opening playback still requires verified
metadata. HLS manifest creation starts resolution before minting its URLs, even
with `format=json`, and retains its existing HTTP 404 start-failure response.
Native unresolved playback returns 422; CLI playback reports an error.

The reverse `get_content_id` server API remains echo-only: direct descriptor
lookup does not invent or recover a content id. Cold VOD-by-infohash remains
outside this live playback change.

## Regression coverage

Tests reuse the existing authenticated synthetic catalog response and transport
fixtures. They cover query quoting versus raw-value signing, requested-hash
binding, index reuse, case normalization, unavailable metadata, failure-cache
expiry/capacity, local-metadata precedence, native failure responses, compat cold
URL admission and HLS failure behavior. All test identifiers are synthetic;
live identifiers and captures are not fixtures.

The offline workspace suite passed 999 tests with seven ignored. Clippy with
warnings denied, formatting, the optimized build, identifier hygiene, whitespace
checks and all 34 comparison-harness tests passed. Independent code review found
no actionable issues. Commands used the pinned Rust 1.99.1 toolchain explicitly.

## Cold live validation

The implementation at `36f076f` was validated with cid20 under controlled local
WARP-off routing. A separate fresh daemon authenticated the current content-id
descriptor and derived its swarm infohash, then stopped. A new candidate process
started with empty state; no descriptor or index was copied to it. Original engine
3.2.11 used the content-id selector while the candidate used compat `infohash=`.
Both used defaults, fresh state and one capture client during the same 180-second
window. The candidate logged authenticated infohash catalog resolution before
source output, with zero logged dropped-piece rejections.

| Observation | Content-id reference | Cold-infohash candidate |
| --- | ---: | ---: |
| Captured bytes | 64,475,516 | 78,153,480 |
| First positive body read | 0.93 s | 2.15 s |
| Captured PCR media span | 205.08 s | 248.36 s |
| Frames decoded in a bounded 10-second check | 221 | 248 |
| Decoder exit status | 0 | 0 |

The reference decoder emitted 68 diagnostic lines; the candidate emitted none.
These are diagnostics from the bounded check, not comparable frame-error rates.
PCR spans describe captured media time and can include startup history; they
are not wall-clock observation durations and do not measure live position.

The analyzer found a 203.08-second shared PCR interval with 338,594 considered
media packets in each capture and 100% directional packet membership. This is
evidence of common source media in that interval, not whole-transport byte
identity or packet ordering. Its separate order-mapping diagnostic matched
299,354 of 370,502 considered candidate packets (80.8%), skipped 44,467 ambiguous
candidate packets and identified 43,263 ambiguous reference packets. It reported
186,494 reference holes and two backward/duplicate steps. These exclusions and
the different captured prefixes limit any stronger equivalence claim.

After the pair stopped, two separate fresh CLI processes ran for a predefined
45 seconds each with empty state and the runtime-derived infohash:

| Selector | Captured bytes | Frames in bounded decode | Decoder exit |
| --- | ---: | ---: | ---: |
| `acestream:?infohash=` | 35,644,612 | 204 | 0 |
| `magnet:?xt=urn:btih:` | 36,168,756 | 238 | 0 |

Both logged authenticated cold catalog resolution. Each decode examined ten
seconds; both streaming processes were deliberately terminated at the capture
deadline. Their termination is not a natural end-of-stream result. This validates
the new cold selectors on one public live source, not all swarms, VOD, continuous
180-second decode quality or real-player smoothness.

The first validation controller stopped during setup because its metadata
deriver lacked the experimental compatibility flag; no playback phase ran. The
corrected controller enabled it for the deriver and candidate, then completed
the predefined pair and CLI phases once. It made no playback-policy adjustments.
WARP was restored, all owned daemons/CLI processes/reference containers stopped,
and only the pre-existing BuildKit container remained. Private controller, logs,
media and numeric records are under
`/tmp/outpace-lookup-176-live-final-fpgfgpds`.
