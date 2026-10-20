# Bounded DHT routing cache evaluation (#42)

## Decision

Do not ship the evaluated cache under #42's no-startup-regression acceptance
criterion. Both the initial spike and the subsequent production candidate showed
warm discovery benefit, but the final stale-cache comparison did not demonstrate
startup safety. The candidate was reviewed and tested, then discarded. Production
DHT behavior remains unchanged; private source and reports remain available.

This is a rejection of the measured candidate under the agreed acceptance, not
evidence that routing caches can never help. It does not establish that the cache
caused the observed slowdown or that playback startup regressed.

## Method

On 2026-10-18, Luna ran seven offline controls successfully, then one bounded live
experiment using cid20 resolved from the private registry. Its authenticated
descriptor was derived at runtime in a fresh local daemon. WARP was off during
the experiment and its restoration was verified afterward. No playback ran.

The prototype held at most 32 successful public routing responders, two per IPv4
/24, with a ten-minute expiry. Admission required a correlated response, a valid
20-byte responder ID, and public useful routing or peer signal. Advertised peer
endpoints were never admitted as routing nodes. Attempted unsuccessful cached
nodes were evicted at walk completion. Snapshots were ordered by target distance.

Each walk had the same eight-second absolute budget and eight-query batch limit.
Cached nodes filled spare batch positions. All measured arms preserved and
prioritized public bootstrap fallback identically; this differs from the current
production walker's ordinary frontier pruning. Bootstrap hostnames were resolved
once without sending DHT queries, then the same four numeric addresses were used
in all arms. These measurements therefore exclude DNS latency and address drift.

Each block trained a snapshot with an empty-cache walk. Warm and stale arms
started from independent copies of that snapshot. Stale simulation discarded
responses to cached-query transactions before receive/timeout accounting, while
retaining separately correlated bootstrap replies. Order was cold/warm/stale,
cold/stale/warm, then cold/warm/stale. Offline controls covered admission,
deduplication, bounds, expiry, failure eviction, baseline equivalence, spoofed
responses, silent-round accounting, and overlapping cached/bootstrap endpoints.

## Observations

Times are milliseconds; peers are unique advertised endpoints, not verified
reachable peers or playback readiness. Timeout rounds count entirely silent
receive windows, not unanswered individual queries.

| Block | Arm | First peer | Total peers | Queries | Timeout rounds | Walk duration |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 0 | Cold | 6040 | 8 | 44 | 0 | 7668 |
| 0 | Warm | 33 | 8 | 16 | 0 | 235 |
| 0 | Stale | 6045 | 2 | 56 | 0 | 8001 |
| 1 | Cold | 7543 | 2 | 40 | 0 | 8001 |
| 1 | Stale | 6071 | 6 | 56 | 0 | 8001 |
| 1 | Warm | 60 | 8 | 40 | 0 | 4552 |
| 2 | Cold | 6045 | 8 | 40 | 0 | 7700 |
| 2 | Warm | 31 | 8 | 16 | 0 | 200 |
| 2 | Stale | 6050 | 2 | 56 | 0 | 8000 |

Cold walks retained 23–28 eligible routing responders. Each warm walk queried
12 cached nodes; each stale walk queried 16 and suppressed 16 matching replies.
All nine walks queried all four bootstrap addresses. Zero silent rounds does not
mean every query received a reply.

Warm discovery was consistently earlier in this sample. The two five-millisecond
stale first-peer increases do not establish a regression amid live-network drift.
However, stale runs used more queries, exhausted their budgets, and returned fewer
advertised peers in two blocks. A single source, three sequential blocks, changed
bootstrap policy, fresh UDP sockets, and loss simulation limit causal claims.
These results neither prove playback improvement nor justify a production cache.

## Production candidate and final validation

The approved follow-up retained the same admission, capacity, prefix and expiry
rules, capped speculative queries at eight per walk, and used short cache locks
without network I/O. Generation-based failure eviction protected concurrent newer
successes. Ordinary queries had first claim on each eight-query batch. Cached
transactions did not mark endpoints ordinarily queried or create ordinary receive
windows; independent bootstrap fallback survived frontier pruning. No persistence
or new dependencies were introduced.

Independent review found and corrected two timing bugs before live validation:
failed final DNS left a cache-only deadline window alive, and long cache-only
windows accepted transactions after their three-second expiry. Luna reproduced
both failures, then verified the fixes. All 45 focused DHT tests passed; the
workspace suite passed 1,012 tests with eight ignored. Clippy, formatting and
identifier hygiene passed. The only subsequent code change moved a test helper
to satisfy Clippy; compilation and lint gates passed again.

One final unattended comparison used the integrated candidate's real scheduler,
runtime-derived cid20, fresh UDP sockets and the same eight-second budget. Each
arm used the normal concurrent DNS/bootstrap path, unlike the spike's frozen
bootstrap addresses. Cold trained a cache; warm and stale received independent
copies. Warm/stale order alternated as above. Stale suppression affected cached
transactions only, before receive accounting. WARP was disabled for the run and
restoration verified afterward. No playback or peer-connect benchmark ran.

| Block | Arm | First advertised peer (ms) | Total peers | Queries | Cached queries | Suppressed replies | Walk duration (ms) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | Cold | 3049 | 9 | 36 | 0 | 0 | 6072 |
| 0 | Warm | 28 | 8 | 12 | 8 | 0 | 54 |
| 0 | Stale | 6039 | 8 | 40 | 8 | 8 | 6064 |
| 1 | Cold | 6037 | 8 | 32 | 0 | 0 | 6063 |
| 1 | Stale | 4541 | 8 | 32 | 8 | 8 | 4567 |
| 1 | Warm | 34 | 8 | 12 | 8 | 0 | 61 |
| 2 | Cold | 6040 | 8 | 32 | 0 | 0 | 6069 |
| 2 | Warm | 28 | 8 | 12 | 8 | 0 | 59 |
| 2 | Stale | 7572 | 8 | 48 | 8 | 8 | 7725 |

All nine walks seeded four bootstrap addresses and recorded zero entirely silent
timeout rounds. The cold walks retained 19–23 eligible responders. Every stale
arm actually suppressed eight cached replies, establishing that the stale path
was exercised. The existing callback may admit multiple peers from one response,
so a walk can finish above its target of eight peers.

Warm first-peer discovery was consistently fast. Stale discovery was 2,990 ms and
1,532 ms slower than its paired cold observation in two blocks; the other block
was 1,496 ms faster. These sequential public-network observations, including
per-arm DNS and source drift, do not isolate causation. They do not demonstrate
the required stale-cache startup safety, so the candidate is not shipped. No
performance reruns were made to seek a favorable outcome. Further cache work
would need a new evaluation scope rather than treating these results as proof of
playback improvement or relaxing this acceptance criterion.
