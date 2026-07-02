# 37 - Upstream selection ranks advertised live windows

Follow-up to note 36. This implements the first small slice of the multi-peer lead without
rewriting the downloader into a full active-peer scheduler: outpace no longer commits to
the first peer that merely completes the TCP + Acestream handshake.

## Root cause

The live path still used a single upstream at a time. `connect_any` raced connection and
handshake, then returned whichever peer won that race. Only after that did `follow_one_peer`
read the peer's extended handshake and discover the advertised live window.

For live playback, the fastest handshake is not necessarily the best upstream. A slightly
slower peer may advertise a newer `mi.max_piece` / `mi.position`, while the fastest peer may
be a static peer that can only serve an old initial window.

## Implemented behavior

`connect_any` now treats a peer as usable only after it has:

- completed TCP connect;
- completed the Acestream peer handshake;
- sent an extended handshake carrying the live `mi` window.

After the first usable candidate arrives, outpace waits a short grace window
(`250 ms`) for any other already-near-complete candidates from the same connect batch. It
then selects the best advertised window by:

1. higher `max_piece`;
2. higher `position`;
3. lower `distance_from_source`.

The selected peer's already-read `LivePosition` is passed into `follow_one_peer`, so the
initial extended handshake is not consumed twice.

Regression tests:

- `upstream_selection_prefers_the_freshest_live_head`
- `upstream_selection_uses_distance_as_a_tiebreaker`

Verification:

- `cargo test -p ace-engine upstream_selection_ -- --nocapture`
- `cargo test -p ace-engine`
- `cargo clippy -p ace-engine --all-targets -- -D warnings`
- `rustfmt --edition 2021 --check crates/ace-engine/src/ace_provider.rs`
- `git diff --check`

## Live smoke

Target:

- infohash `50e93529d3eb46a50506b14464185a15292d6e47`
- daemon `127.0.0.1:6900`
- capture length: 75 s curl timeout

Result:

- `HTTP 200`
- first byte: `7.605448` s
- downloaded: `8,308,472` bytes
- MPEG-TS sync at offsets `0`, `188`, `376`; first 1000 packets had zero sync errors.

Relevant daemon log:

```text
[ace] open 50e93529d3eb46a50506b14464185a15292d6e47: discovered 16 peer(s)
[ace] 85.87.156.75:8621: connected + handshaked (window max=14718269)
[ace] 85.87.156.75:8621: window min=14718220 max=14718269 -> start=14718261 head=14718269
[ace] 85.87.156.75:8621: UNCHOKE -> requesting pieces 14718261..=14718269
[ace] 85.87.156.75:8621: served 6 MiB (head=14718269, next piece needed=14718268)
[ace] 85.87.156.75:8621: unhandled msg id=12 (2932 bytes) 05001b000000110000006c23e33a2b0000000031e1353900
[ace] 85.87.156.75:8621: stale upstream - no live progress for 12s; reconnecting
[ace] no reachable peer among 16 known; rediscovery found 1, added 0 (known now 16)
[ace] 85.87.156.75:8621: connected + handshaked (window max=14718269)
[ace] 85.87.156.75:8621: reconnected; window min=14718220 max=14718269 -> resuming from 14718270 head=14718269
```

So the new selection step is active, but this swarm snapshot still only produced one usable
upstream, and that upstream still advertised the same static `max_piece=14718269` on every
retry. No alternate connected candidate with a fresher live head appeared during the grace
window.

## Current blocker

This note improves the peer choice before committing to a single upstream, but it does not
solve the no-stutter blocker for this target:

> Outpace can rank initially connected upstreams by advertised live window, but the live
> run still found only one usable upstream, and that upstream's window did not advance beyond
> the initial 9-piece MPEG-TS window.

Next highest-leverage lead remains a real small active-peer set: keep several handshaked
peers alive at once, track which windows advance over time, and request missing chunks from
any peer that advertises coverage instead of relying on one selected upstream.
