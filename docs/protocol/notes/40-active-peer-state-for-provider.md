# 40 - Active-peer state is in the provider request path

Follow-up to note 39. The provider still runs one TCP peer session at a time, but the
request path now uses the same active-peer state model needed by the future multi-session
coordinator.

## Root cause

Note 39 moved global request ownership into `ace_swarm::Scheduler`, but the provider still
constructed a one-off `PeerView` on every request fill. That preserved one more single-peer
assumption in production: there was no persistent state for a peer's choke state, advertised
window, or pieces assigned specifically to that peer.

A real active-peer coordinator needs that state before it can safely fan out requests:

- only unchoked peers should receive assignments;
- assignments must respect each peer's advertised live window;
- in-flight counts must be per peer, not inferred from the global scheduler;
- when one peer drops, only that peer's assigned pieces should be requeued.

## Implemented behavior

`ace_swarm::scheduler` now provides a reusable `ActivePeers` model alongside `Scheduler`.
It tracks each active peer's:

- advertised window;
- choke state;
- per-peer in-flight piece set.

It can assign missing pieces through the shared scheduler and records which peer owns each
assignment. Dropping a peer returns its in-flight pieces so callers can requeue them with
`Scheduler::on_drop`.

The production `AceProvider::Continuity` now owns `ActivePeers` in addition to `Scheduler`.
The current single-peer loop registers the connected peer under a stable temporary handle,
updates its unchoke/window state from peer messages, and fills chunk requests via
`ActivePeers::assign`. Completed pieces clear both the scheduler entry and the active
peer's in-flight entry.

Regression tests:

- `active_peers_assign_missing_pieces_to_unchoked_peers_with_coverage`
- `active_peer_drop_returns_its_in_flight_pieces_for_requeue`

## Verification

- `cargo test -p ace-swarm`
- `cargo test -p ace-engine`
- `cargo clippy -p ace-engine -p ace-swarm --all-targets -- -D warnings`
- `rustfmt --edition 2021 --check crates/ace-engine/src/ace_provider.rs crates/ace-swarm/src/scheduler.rs`
- `cargo build -p ace-engine --bin outpace`
- `git diff --check`

## Live smoke

Target:

- infohash `50e93529d3eb46a50506b14464185a15292d6e47`
- daemon `127.0.0.1:6900`
- capture length: 75 s curl timeout

Result:

- `HTTP 200`
- first byte: `7.600419` s
- downloaded: `8,308,472` bytes
- MPEG-TS sync at offsets `0`, `188`, `376`; first 1000 packets had zero sync errors.

Relevant daemon log:

```text
[ace] open 50e93529d3eb46a50506b14464185a15292d6e47: discovered 11 peer(s)
[ace] 85.87.156.75:8621: connected + handshaked (window max=14718269)
[ace] 85.87.156.75:8621: window min=14718220 max=14718269 -> start=14718261 head=14718269
[ace] 85.87.156.75:8621: UNCHOKE -> scheduling from piece 14718261 toward head 14718269
[ace] 85.87.156.75:8621: served 6 MiB (head=14718269, next piece needed=14718268)
[ace] 85.87.156.75:8621: stale upstream - no live progress for 12s; reconnecting
[ace] no reachable peer among 11 known; rediscovery found 10, added 0 (known now 11)
[ace] 85.87.156.75:8621: connected + handshaked (window max=14718269)
[ace] 85.87.156.75:8621: stale advertised window min=14718220 max=14718269 cannot cover next needed piece 14718270; trying another peer
```

The live byte result is unchanged for this target, which is expected: this note does not
yet run multiple sessions concurrently. It verifies that the active-peer request path does
not regress the valid initial MPEG-TS window or the stale-window rejection behavior.

## Current blocker

The next missing production piece is no longer request ownership or peer-state bookkeeping.
It is the async multi-peer I/O coordinator:

> Spawn/own several handshaked `ConnectedUpstream` sessions, feed their message events into
> one shared `Continuity`, and send chunk-request commands back to whichever peer
> `ActivePeers::assign` selected.

That coordinator must also requeue only the dropped peer's assignments, not the whole
scheduler, which is now supported by `ActivePeers::remove` plus `Scheduler::on_drop`.
