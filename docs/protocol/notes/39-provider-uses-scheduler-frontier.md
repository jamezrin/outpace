# 39 - Provider request frontier uses the shared scheduler

Follow-up to note 38. This moves the production `AceProvider` request path off its private
single-peer `requested_to` frontier and onto `ace-swarm::Scheduler`, the pure scheduler that
already models assigning pieces across peer views.

## Root cause

The live provider still had a single-connection request model embedded directly in
`ace_provider.rs`: `advance_requests` extended a local `requested_to` piece frontier and
sent every chunk for each piece in that range to the one current peer.

That worked for a single usable upstream, but it was the wrong seam for the remaining
no-stutter work. A future active-peer loop needs request ownership to be separate from the
TCP session so that:

- outstanding requests from a dropped peer can be requeued;
- the scheduler can assign the next-needed piece to whichever active peer covers it;
- the provider can stop treating "requested from peer A" as a global stream position.

## Implemented behavior

`Continuity` now owns:

- `ace_swarm::scheduler::Scheduler`
- per-piece received chunk bookkeeping
- the existing reassembler/resync state

The provider now fills the request pipeline by asking the scheduler for pieces from
`PieceReassembler::next_needed()` toward the known head, constrained by the current peer's
advertised live window. Assigned pieces are still sent to the current single peer as
Acestream chunk requests, so this is intentionally not the full multi-peer I/O rewrite yet.

On complete piece receipt, the provider marks the scheduler request complete. On reconnect,
`Continuity::resume` clears all scheduler in-flight state so pieces requested from a dropped
peer are requeueable against the next peer.

`ace-swarm::Scheduler` gained:

- `clear_in_flight()`, a peer-drop primitive that requeues every outstanding piece.

Regression tests:

- `clear_in_flight_requeues_every_outstanding_piece_after_peer_drop`
- provider tests that previously asserted `requested_to` behavior now assert scheduler
  assignments from the reassembler cursor.

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
- first byte: `6.239245` s
- downloaded: `8,308,472` bytes
- MPEG-TS sync at offsets `0`, `188`, `376`; first 1000 packets had zero sync errors.

Relevant daemon log:

```text
[ace] 85.87.156.75:8621: connected + handshaked (window max=14718269)
[ace] 85.87.156.75:8621: window min=14718220 max=14718269 -> start=14718261 head=14718269
[ace] 85.87.156.75:8621: UNCHOKE -> scheduling from piece 14718261 toward head 14718269
[ace] 85.87.156.75:8621: served 6 MiB (head=14718269, next piece needed=14718268)
[ace] 85.87.156.75:8621: stale upstream - no live progress for 12s; reconnecting
[ace] no reachable peer among 14 known; rediscovery found 14, added 0 (known now 14)
[ace] 85.87.156.75:8621: connected + handshaked (window max=14718269)
[ace] 85.87.156.75:8621: stale advertised window min=14718220 max=14718269 cannot cover next needed piece 14718270; trying another peer
```

The live result is intentionally unchanged: the target still only yielded the same static
usable upstream and capped at the 9-piece initial MPEG-TS window. The value of this change
is structural: production now uses the same request-assignment primitive that can address
multiple peers, rather than a single-peer-only frontier.

## Current blocker

The next step is the real active-peer I/O coordinator:

> Keep several `ConnectedUpstream` sessions alive, feed their window/choke/piece events into
> one shared `Continuity` + `Scheduler`, and let the scheduler assign missing pieces to any
> unchoked peer whose advertised window covers `reasm.next_needed()`.

This note removes one production obstacle to that coordinator, but it does not complete it.
