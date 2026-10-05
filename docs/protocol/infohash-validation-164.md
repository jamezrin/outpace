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
| Live cid9/infohash equivalence and no sustained continuity corruption | Requires two staged comparisons, each simultaneous with the official engine: candidate content-id first, then warmed native/compat infohash sharing one candidate session after the content-id pool stops. Current runtime identifiers and explicitly verified direct routing are required. Source faults and reserved-table continuity are distinguished from payload corruption. |

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
Resolve cid9 from the gitignored registry as operator context, supply its value
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
