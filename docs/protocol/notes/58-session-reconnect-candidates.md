# Session reconnect candidates

Live sessions retain upstream addresses learned from peer exchange (`id=12`) and
source-node announcements (`id=36`) through `PoolStale` and `PeerLost`. Learning
continues when the active pool is full. Reconnect tries eligible source addresses,
then PEX addresses, then discovery addresses. The candidate snapshot is visited
once per round; a preferred source whose cooldown expires during the round does
not jump ahead of working alternatives again.

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
later retries; a discovery result never clears cooldowns. Candidate retention
lasts only for the running session: this is not a persistent peer cache.

A live connect attempt now has one three-second ceiling for TCP, BT handshake and
initial live-window acquisition combined. Previously only TCP had that ceiling,
while each protocol read could take 20 seconds. Valid peers taking more than three
seconds for the combined exchange are retried after cooldown rather than accepted
on that attempt. Catalog/metadata resolution, VOD and inbound peer timeout behavior
are unchanged. This bounds a slow preferred source's delay before alternatives;
it is not a claim of improved startup performance.

A reconnect/refill peer with a stale media window is briefly used for gossip only.
The initial stale batch reads concurrently under one 750-ms total deadline,
including handshake writes, with at most 32 messages per peer. Refill harvesting
uses the same bound. The signed handshake advertises no live position, and sends
neither Interested nor requests. Only PEX/source addresses are harvested: stale
HAVE, window updates and media cannot alter the playback cursor or head. Normal
resume-window checks still gate media activation.

Learned connection attempts are deduplicated while pending and collectively capped
by the existing parallel-connect setting. Discovered refill attempts are separate
from this learned queue; active endpoints are deduplicated on admission. Pool
teardown or consumer cancellation aborts owned peer workers, learned attempts,
refill tasks, and their discovery work, and clears active-peer statistics. Rebuild
logs include the end reason and retained source/PEX counts.

Loopback protocol regressions cover source/PEX retention across connected silence
and drops, a full pool, single discovered relay, source priority/cooldowns/bounds,
slow source fallback, gossip-only safety, and cancellation. They replace public
DHT/tracker I/O at the discovery boundary while exercising the actual live-session,
pool, handshake, chunk and continuity paths.
