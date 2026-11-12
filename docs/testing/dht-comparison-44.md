# DHT comparison: custom, mainline and n0-mainline (#44)

**Recommendation: keep the custom DHT, and fix its round pacing.** Do not adopt
`mainline` or `n0-mainline`. Neither library can replace the custom walk behind
`discover_peers_with_options` without changing what that function promises: one
bounded, ephemeral, client-only lookup that owns its socket and logs through `alog!`.
The live block did show one real advantage of the libraries, which was faster time
to first peer. The source shows that the custom walk's delay comes from a fixed
per-round wait, which can be fixed in place.

This document covers the method, the one matched live block, the integration costs,
and the limits of what was measured.

## Candidates

| Candidate | Version | How it runs a lookup |
|---|---|---|
| custom | `ace-swarm` `dht.rs` | One tokio task and one UDP socket per lookup, discarded afterwards |
| `mainline` | 8.0.1 | Persistent node on its own OS thread, reached over unbounded `flume` channels; async API is a facade |
| `n0-mainline` | 0.6.0 | Persistent node whose actor is `tokio::spawn`ed on the caller's runtime |

## Method

Every arm ran through the same boundary, described in
[the live-boundary design](../superpowers/specs/2026-11-12-dht-live-boundary-44-design.md)
and implemented in `tools/dhtcompare`:

- Each worker ran in fresh user, network, mount and PID namespaces on the validation
  host, which routes all public IPv4 through a VPN exit.
- Inside the namespace the worker had a default-deny nftables table, no capabilities,
  `no_new_privs`, a read-only root and no reachable host UNIX sockets.
- A supervisor relay forwarded only IPv4 UDP to global unicast destinations. It
  admitted replies only from endpoints the same socket had contacted, so no candidate
  could act as a reachable DHT server.
- Before the live block, the 12-case offline qualification of the boundary passed on
  the validation host itself, using the same staged artifacts.

All three arms shared identical inputs:

- One current public, unencrypted live content id, resolved at run time to its
  infohash through the local daemon. Neither value was written anywhere.
- The production bootstrap hostnames, resolved once to four IPv4 seeds and given to
  every arm.
- A 15-second lookup budget that includes construction, a target of 8 unique peers
  and a cap of 1,024.
- Release builds of every arm. Each arm got a 30-second worker envelope, inside one
  135-second block deadline.

The arms ran once each, in the fixed order custom, `mainline`, `n0-mainline`, with no
retries and no announcements. Gateway exit provenance, host routes, firewall ruleset,
IPv6 state and the daemon's process were checked before and after the block and were
unchanged. The staged directory was removed and no process outlived it.

## Live results

The block completed in 7.5 seconds. Every arm reached the target well inside its
budget. No arm exhausted its frontier and none timed out.

| | custom | mainline | n0-mainline |
|---|---|---|---|
| Time to first peer | 4.56 s | 0.96 s | 0.64 s |
| Lookup elapsed at target | 4.56 s | 1.04 s | 1.02 s |
| Unique peers when the target was reached | 51 | 23 | 21 |
| Datagrams sent | 24 | 552 | 236 |
| Bytes sent | 2,280 | 51,174 | 26,365 |
| Unique endpoints contacted | 24 | 271 | 231 |
| Endpoints that had not replied by teardown | 8 | 97 | 77 |
| Replies admitted | 17 | 349 | 157 |
| Denied or unsolicited datagrams | 0 | 0 | 0 |
| Peak threads, sampled | 2 | 2 | 1 |
| Peak `VmHWM`, sampled | 7.7 MiB | 3.9 MiB | 3.9 MiB |
| CPU time, user + system | 4 ms | 14 ms | 10 ms |

Peer counts overshoot the target because a single response can carry many peers. The
custom walk received all 51 in one response.

Library-internal diagnostics differ by candidate:

- **custom** reported its own counters: 24 nodes queried and 15 valid responses.
- **`mainline` and `n0-mainline`** expose no query, response or timeout counters. Their
  records carry `null` with the reason `library_not_exposed`, not zero.

The boundary counters in the table are the uniform substitute. They come from the
relay, not from the libraries.

### Why the custom walk was slower

The custom walk was not slow because of the network: it got replies from 16 of its 24
contacts. The delay comes from its pacing.

- `dht_walk_owned` sends a batch of up to 8 queries, then waits a fixed 1.5-second
  response window.
- The window ends only on its timer or a bootstrap resolution, not when every inflight
  query has been answered.

The walk queried 24 nodes over at least four rounds, the first being its four seeds.
Three full windows take 4.5 seconds, and the response that carried values arrived
early in the fourth round. That matches the measured 4.56 seconds.

Both libraries refill queries as responses arrive. That is why they reached their
first peer in under a second while sending about 10 to 23 times as many datagrams.

## Integration costs

These were measured with `cargo tree` against the comparison tool's lockfile,
counting normal dependency edges, and compared with the root lockfile (288 packages).

| | mainline 8.0.1 | n0-mainline 0.6.0 |
|---|---|---|
| Transitive crates, including itself | 54 | 51 |
| Crate names new to the root lockfile | 13 | 12 |
| Name and version pairs new to the root lockfile | 23 | 23 |

The duplicates are new major versions of the signing stack:
- `ed25519-dalek` 3 alongside root's 2,
- `curve25519-dalek` 5 alongside 4,
- `ed25519` and `signature` 3 alongside 2,
- a second `syn` 3 patch.

BEP-44 mutable items need these crates; peer discovery does not. Both libraries also
pull in `tracing` and `log`. The workspace logs only through `alog!`, so library
diagnostics would be silently dropped.

The following come from reading the pinned crate sources; none of them were measured.

**Lifetime and background traffic.** Both libraries are built around a long-lived
node with a routing table:
- they ping the table every 5 minutes;
- they refresh it every 15 minutes.

The custom walk keeps no state between lookups.

**Inbound serving.** Both libraries default to *adaptive* mode. After a table refresh,
a node that has seen it is not firewalled switches itself into server mode and answers
inbound DHT queries. Adopting either library would silently change Outpace's
inbound-serving default unless it is pinned off. The live block could not observe this,
because the boundary admits no unsolicited inbound traffic.

**Runtime.**
- `mainline` adds one OS thread per node. Its result channels are unbounded.
- `n0-mainline` requires a tokio runtime context. Its inbox is bounded, but its result
  streams are unbounded.

Ending the process does not prove that either library's actor has joined.

**Announce semantics.**
- The custom `dht_announce_peer` announces to the first 8 nodes that return a token, in
  walk order. It reports sends, not acceptance.
- Both libraries announce to the up-to-20 closest responding nodes from a completed
  query, and support the implied-port flag. This is the more faithful BEP-5 behavior,
  and a useful reference for improving the custom announce.

Announces were not exercised live.

## What `discover_peers_with_options` requires

`discover_peers_with_options` races tracker and DHT sources. Its DHT half must:
- stay inside `DiscoveryOptions::dht_budget`, including construction;
- stop at `peer_target`, and retain at most 1,024 unique IPv4 peers;
- bind its own ephemeral socket;
- serve nobody;
- release everything when dropped.

The incremental variant also publishes each correlated peer as soon as it arrives, and
lets the receiver cancel further harvesting.

A persistent library node conflicts with all of the following: ephemeral ownership,
"serves nobody", and "release on drop". Bridging those gaps would mean:
- a shared process-wide node,
- adaptive serving pinned off,
- bounded adapters over its unbounded channels,
- a logging bridge.

That is more new code than the pacing fix, and it gives up the bounded ownership this
crate relies on.

## Recommended follow-up

Tracked in [#384](https://github.com/jamezrin/outpace-stream/issues/384).

Change the custom walk's pacing so that `discover_peers_with_options` keeps its
semantics but gets faster. Two concrete options:
- end a response window once nothing is inflight;
- keep up to 8 queries inflight, refilling as each reply or `INFLIGHT_TTL` expiry frees
  a slot.

Validate the change through the same offline qualification and one matched live block
against this baseline. The libraries' sub-second first peer shows the network allows it.
The custom walk's 24 datagrams show that the conservative footprint is worth keeping.

## Limits of this evidence

- **One observation.** This is one block: one target, one VPN exit, one moment. Timing
  differences of a few hundred milliseconds between `mainline` and `n0-mainline` are
  within what a single run cannot separate. The custom walk's roughly 4.5-second floor
  for four rounds follows from source, independently of this run.
- **Sends are not deliveries.** Admitted datagrams are successful local sends, not
  proof of delivery or of library handling.
- **Unanswered is not ignored.** An endpoint counted as unanswered may have replied
  after teardown.
- **Late injections.** For `mainline`, the relay injected 4 more replies than nftables
  accepted. Each arm also had one or two ICMP packets dropped on output. Both are
  consistent with replies injected after the worker closed its socket or after the
  final counter read. This is an inference: the counters do not record the reason.
- **Sampled lower bounds.** Thread, socket and memory peaks were sampled every 10 ms,
  so shorter spikes are invisible. The custom arm runs as a libtest case, so its memory
  and thread counts include the test harness and are not directly comparable to the
  standalone library worker.
- **CPU resolution.** CPU times are a few milliseconds each. That shows all three are
  cheap per lookup, not how they rank.
- **Unobserved behavior.** Background routing-table traffic, adaptive server mode and
  announces were not exercised. Statements about them come from source.
