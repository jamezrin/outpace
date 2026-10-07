# Session reconnect candidates

Live sessions retain upstream addresses learned from peer exchange (`id=12`) and
source-node announcements (`id=36`) through `PoolStale` and `PeerLost`. Learning
continues when the active pool is full. Eligible candidates with fewer real pool admissions are tried first; within an
equal-admission cohort, source addresses lead PEX then discovery addresses. Each
connection batch stays within one cohort. Initial activation, learned connections
and discovered refills record admission only after successful signed-handshake
activation. Failed or stale transports and cancelled attempts do not consume an
admission. Thus fast-handshaking but nonproducing sources cannot repeatedly end
all connection rounds before an unadmitted alternative has a real opportunity.
The finite snapshot and cohort rule also apply while learned connections are
pending or ready in the refill queue. A cooldown expiring or repeated gossip does
not let older peers overtake that pending opportunity.

Authenticated contiguous output resets all admission counts, restoring ordinary
source priority for later outages without preempting healthy active media. Other
peers' failure/cooldown histories are unchanged. Counts are session-local,
saturating u64 values: equally saturated counters lose historical distinction
after an impractical number of actual admissions. Candidate churn and slow or
unusable peers prevent a universal wall-clock recovery bound. The default
four-slot/eight-source fault topology with a healthy500ms PEX handshake is covered
by a30s output bound across two nonproductive pool lifetimes.

The collection contains at most 1024 distinct valid IPv4 endpoints. Higher-priority
learned addresses can replace lower-priority entries when full. At most eight
source addresses receive source priority; excess source announcements retain PEX
priority within the same bound. Repeated announcements upgrade provenance but
preserve failures and cooldowns. Invalid zero-port, unspecified, multicast and
broadcast destinations are discarded; loopback/private peers remain usable for
local deployments.

Failures and stalled pools use a 1, 2, 4, 8-second exponential cooldown, capped at
four seconds for priority sources. Contiguous media output resets a productive
peer's failure history. Cooling known candidates are retried without waiting for
rediscovery. Discovery after a failed eligible round can run concurrently with
later retries; a discovery result never clears cooldowns. If that failed round
has no learned source/PEX endpoints, rediscovery uses the existing background
target of64 peers and8-second DHT budget. Initial discovery keeps its8-peer target
and15-second DHT budget; learned recovery retains its existing discovery options.
This prevents an already unusable small DHT set from repeatedly cancelling a
slower tracker result. One owned discovery task runs alongside known-candidate
retries and remains cancellable with the session. These budgets constrain DHT,
not total tracker/DNS completion; no new total cutoff discards useful results
arriving after8seconds. Delayed discovery can still wait on the other source,
and this is not incremental result streaming or a universal recovery latency
bound. Candidate retention lasts only for the running session: this is not a persistent peer cache.

A live connect attempt now has one three-second ceiling for TCP, BT handshake and
initial live-window acquisition combined. Previously only TCP had that ceiling,
while each protocol read could take 20 seconds. Valid peers taking more than three
seconds for the combined exchange are retried after cooldown rather than accepted
on that attempt. Catalog/metadata resolution, VOD and inbound peer timeout behavior
are unchanged. Total deadline expiry has a distinct operational `total_timeout`
count; it is not attributed to window acquisition when BT itself is silent.
This bounds a slow preferred source's delay before alternatives;
it is not a claim of improved startup performance.

A reconnect/refill peer with a stale media window is briefly used for gossip only.
The initial stale batch reads concurrently under one 750-ms total deadline,
including handshake writes, with at most 32 messages per peer. Refill harvesting
uses the same bound. The signed handshake advertises no live position, and sends
neither Interested nor requests. Only PEX/source addresses are harvested: stale
HAVE, window updates and media cannot alter the playback cursor or head. Normal
resume-window checks still gate media activation.

Learned connection attempts are deduplicated while pending and collectively capped
by the existing parallel-connect setting. Running attempts plus successfully
queued learned transports share that one cap; queued transports remain pending
until activation/rejection and close with the pool. Discovered refill attempts are separate
from this learned queue; active endpoints are deduplicated on admission. Pool
teardown or consumer cancellation aborts owned peer workers, learned attempts,
refill tasks, and their discovery work, and clears active-peer statistics. Rebuild
logs include the end reason and retained source/PEX counts.

Loopback protocol regressions cover source/PEX retention across connected silence
and drops, a full pool, single discovered relay, source priority/cooldowns/bounds,
slow source fallback, gossip-only safety, and cancellation. They replace public
DHT/tracker I/O at the discovery boundary while exercising the actual live-session,
pool, handshake, chunk and continuity paths.
