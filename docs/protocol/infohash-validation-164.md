# Infohash safety acceptance audit (#164)

This audit follows PR #179's safety scope: a bare infohash can open only from a
catalog-provenanced descriptor remembered in the same process, or a descriptor
minted by that daemon's own broadcast. Geometry and source key come from that
descriptor. Cold infohash network lookup remains tracked in
[#176](https://github.com/jamezrin/outpace/issues/176).

## Original acceptance and deviations

| Original requirement | Evidence and scope |
| --- | --- |
| Infohash playback uses descriptor geometry/pubkey | Provider tests exercise 524,288-byte pieces, 16,384-byte chunks, a 96-byte RSA tail and the source key through content-id cache resolution into the infohash index. Native and compat share this provider. |
| Non-1 MiB vector under `tests/vectors/transport/` | `synthetic-live-512k.bin` now satisfies the literal fixture requirement; its provenance and invented fields are documented beside it. PR #179 initially generated the vector in tests instead. |
| No descriptor fails clearly | Native raw/TS/HLS routes return 422 with a content-id hint. Compat infohash/magnet starts return a JSON error before minting a lease. Tests inspect the actual session store and find zero active leases. |
| CLI `infohash=` and `magnet:` use metadata | One-shot CLI processes have no remembered descriptor or own broadcast, so these inputs fail closed. Positive cold CLI playback remains a #176 parity gap. |
| Live cid9/infohash equivalence and no sustained continuity corruption | Two staged WARP captures now provide nonempty simultaneous-reference coverage on the user-authorized alternative cid10, with measured 512 KiB geometry. cid9 content-id matched but its subsequent infohash phase received no media. SDT continuity remains #172; no sustained media duplication/corruption was observed in qualifying overlap. See the measured results below; this is narrowed safety acceptance, not all-counter-zero or availability parity. |

`warm_infohash_continuity_authenticates_pieces_before_emitting` resolves a
generated-key 512 KiB descriptor through the catalog cache and infohash index,
then constructs the download path's real `Continuity`. It feeds a corrupted piece
as 32 chunks, checks rejection/no output/no cursor advance, then feeds an authentic
retry and checks exact payload output with its RSA tail removed. Deliberately
disconnecting the constructor's source-key wiring makes this test fail at the
corruption assertion; restored wiring passes.

The namespace test proves that caching a content id does not make that same
40-hex input openable as an infohash. The peer-cache test proves BEP-9 resolutions
cannot populate the shared infohash index. The own-broadcast precedence test
changes unbound trackers without changing the descriptor's computed infohash,
then checks that the daemon's minted trackers win over the conflicting index.
The transport-URL resolver returns directly without indexing its caller-supplied
descriptor. These restrictions preserve descriptor provenance; they do not
authenticate catalog responses. Catalog authentication and BEP-9 binding remain
[#178](https://github.com/jamezrin/outpace/issues/178) and
[#177](https://github.com/jamezrin/outpace/issues/177), respectively.

The original runtime geometry-mismatch detection proposal was deferred in
PR #179 when the guessed-geometry path was removed. This audit adds no guessed
fallback, persistent index, network lookup or new exposed default.

## Reproduction

Build a release candidate, start it and official engine 3.2.11 with fresh state,
and inspect active connections plus `ip route get` before the live capture.
The user's 2026-10-06 session instruction keeps WARP enabled for online work,
overriding the repository's WARP-off guidance. Record the actual WARP route and
settings without changing VPN/services; these captures establish no direct-route
or general network performance guarantee.
Resolve cid9 or a user-supplied alternative from the gitignored registry as
operator context, supply its value
through runtime environment/arguments, and derive its infohash at runtime.
Exercise refusals before warming the candidate. Use `tools/abcompare` for two
phases, each with its own monotonic capture clock and simultaneous reference:

1. Capture official-engine content-id playback alongside candidate native
   content-id playback. This resolves and remembers the candidate descriptor.
2. Close the phase's readers and explicitly delete the candidate `cid:` session.
   Confirm its status returns 404, the session list is empty and its logged
   upstream peer TCP connections have closed. Check that the retained descriptor still permits a
   compat infohash JSON start; revoke that probe lease without opening playback.
   Keep the candidate daemon running so its descriptor index survives.
3. Capture official-engine playback alongside candidate native and compat
   infohash playback. During capture, require exactly one candidate infohash
   session with two clients and no CID session. The two candidate listeners share
   one upstream pool, keeping each phase to two upstream consumers on the host.

The content-id and infohash windows differ. Compare each candidate path against
its own simultaneous reference window; source drift prevents cross-phase startup,
throughput or packet-identity claims. Native and compat infohash outputs can be
compared within their shared phase. Keep raw descriptors, keys, names, identifiers,
media, event logs and capture metadata in private disk-backed directories under
`/home/jamezrin/.cache`, outside the repository. Avoid `/tmp` for captures because
its quota was exhausted during this audit.

The stop removes the manager session and aborts its pull pump; source follower
shutdown is asynchronous. Match sockets belonging to the owned candidate PID
against peer-specific pool/window log entries. Local HTTP clients and unrelated
catalog/tracker connections are separate observations. This confirms manager
removal and closure of observed peer connections, not that every background task
has stopped. If peer evidence or the transition cannot be confirmed, stop before
the infohash phase and report the limitation.

Analyzer success means it produced metrics. No media, no startup or no shared
PCR window yields null/unmeasured quality, and cannot establish equivalence.
Decoder diagnostic counts include startup/discontinuity effects and are not
standardized quality scores. Cold lookup parity is not implied by warm playback.

## Measured live evidence (2026-10-06)

Fresh release outpace and official engine 3.2.11 ran with fresh state and default
playback settings through the explicitly authorized WARP route. Candidate listener
and storage isolation plus experimental compat opt-in were the only overrides.
The descriptor probe measured both cid9 and cid10 at 524,288-byte pieces,
16,384-byte chunks, 32 chunks per piece, a 96-byte RSA tail and a 124-byte source
public key. Catalog resolution placed the exact `StreamInfo` clone in the index;
subsequent infohash logs confirmed that index path and the privately derived hash
matched the catalog result. Discovery, handshake, identity, playback settings and
`Continuity` then follow the same provider path. Live media therefore traversed
the configured source-key verification and tail-removal path; the generated-key
corruption/retry regression supplies independent rejection evidence. This was not
an independent forensic verification of captured raw peer signatures.

| Target/phase | Candidate bytes / first byte | Shared PCR span / media identity |
| --- | --- | --- |
| cid9 content-id, 180 s | 80,097,024 / 133.778 s | 54.48 s; 389,862 packets each, 100% both directions |
| cid9 warmed native/compat infohash, 180 s | 0 / unmeasured | No overlap; not passing playback evidence |
| cid10 content-id, 180 s | 130,251,476 / 32.327 s | 164.48 s; 680,931 packets each, 100% both directions |
| cid10 warmed native/compat infohash, 180 s | 125,453,152 each / 40.049 s | 158.48 s; 655,267 packets each, 100% both directions against the reference |

The user supplied alternative targets and authorized trying any. cid10 supplies
positive non-default live geometry coverage; cid9's no-media infohash window
remains an observed availability failure. It repeatedly failed peer handshakes
before obtaining an upstream while its simultaneous reference delivered media.
The cause was not established. Logs do not identify every failed peer or detailed
handshake error; this does not prove the source-node loss or stale-window causes
in #168/#170. The discovery wait and good-peer caching proposal remain relevant to #167, but
all-handshake exhaustion needs a separate bounded investigation; this audit
changes no connection behavior.

For qualifying cid10 infohash output, all 663,814 nonambiguous whole-candidate
media packets mapped in order to the reference, with zero reference holes or
backward/duplicate mapping steps. Native and compat whole captures were identical.
Measured candidate media duplication, sync losses, malformed/transport-error
packets and PCR jumps were zero. SDT PID 17 still had 79 continuity errors and
240 exact repeated PSI packets (80 each PAT/PMT/SDT), so the literal requirement
for zero continuity errors across all PIDs remains covered by
[#172](https://github.com/jamezrin/outpace/issues/172). The media-integrity result
does not claim that every transport counter is zero.

Both staged transitions returned DELETE 204 and reached stable status 404,
empty manager sessions and no observed logged-peer sockets after approximately
15 seconds. Descriptor-only probe leases were revoked without opening media;
no source session remained. Each infohash phase observed one shared session with
two clients and no CID session. Sampled manager/socket evidence has the lifecycle
limits described above. The reference daemon persisted across phases, and a cold
infohash JSON preflight may have warmed its metadata: these timings are measured
observations, not cold-start or cross-phase performance comparisons.

`ffprobe` identified H.264 1080p video and 48 kHz AAC on all cid10 outputs;
`ffprobe` and full video/audio `ffmpeg` decodes exited 0. Decoder error logs still
contained one line per candidate capture, versus 147 and 51 reference lines in
CID and infohash phases, including reference startup PPS/frame diagnostics.
These unequal media spans and startup boundaries prevent a quality-score
comparison; exit 0 does not mean error-free decoding. Exact overlapping media
identity supplies the stronger corruption check.

Actual descriptorless native raw/TS/HLS requests returned 422; compat infohash
and magnet getstream/manifest requests returned error/null response without a
playback URL; release CLI infohash/magnet commands exited 1 with no stdout. Exact
zero-lease refusal is independently asserted against the offline session store.
The official engine's cold-infohash JSON request returned HTTP 200 and minted a
URL, but that URL was not opened: this establishes URL minting only.

The evidence supports closing the guessed-geometry/unverified-output safety
scope after independent review, with positive cold CLI/network parity in #176,
all-PID SDT continuity in #172 and connection availability still explicit. It
supports neither full original-engine parity nor general network performance.
