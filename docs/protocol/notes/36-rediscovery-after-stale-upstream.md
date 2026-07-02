# 36 — Rediscovery after stale upstream exhaustion

Follow-up to note 35. The first next lead from that note is now implemented:
`follow_live` no longer ends the stream source when the initial discovered peer set is
exhausted after a stale upstream.

## Root cause

After note 35's stale-upstream guard, the live loop behaved like this:

1. Connect to the only reachable peer (`85.87.156.75:8621`).
2. Pull the valid initial 9-piece MPEG-TS window.
3. Declare the peer stale after 12 s with no live progress.
4. Exclude that peer and try the rest of the original discovered list.
5. If none completed connect+handshake, end `follow_live`.

That bounded the freeze, but it still made the initial peer list terminal. A live swarm is
not static; newly discovered peers can appear later, and a previously static reachable peer
may later advertise a newer window.

## Implemented behavior

`follow_live` now refreshes discovery when `connect_any` exhausts the current candidates:

- runs `discover_peers` again for the same infohash/trackers;
- merges newly discovered peers into the existing list without reordering known peers;
- keeps previously excluded peers excluded when fresh candidates were added, so new peers
  are tried first;
- if rediscovery found no new peers, clears the exclusion set so previously lost peers can
  be re-evaluated after a discovery cycle.

This keeps an active client in a prebuffer/retry state instead of tearing down the source
just because the first peer set went bad.

Regression tests:

- `rediscovery_merges_only_new_peers_and_keeps_existing_order`
- `rediscovery_reports_zero_when_every_peer_was_already_known`
- `rediscovery_without_new_peers_allows_excluded_peers_to_be_retried`

## Live smoke

Target:

- infohash `50e93529d3eb46a50506b14464185a15292d6e47`
- daemon `127.0.0.1:6900`
- capture length: 75 s curl timeout

Result:

- `HTTP 200`
- first byte: `9.221741` s
- downloaded: `8,308,472` bytes
- MPEG-TS sync at offsets `0`, `188`, `376`; first 1000 packets had zero sync errors.

Relevant daemon log:

```text
[ace] 85.87.156.75:8621: stale upstream — no live progress for 12s; reconnecting
[ace] no reachable peer among 15 known; rediscovery found 16, added 1 (known now 16)
[ace] no reachable peer among 16 known; rediscovery found 16, added 0 (known now 16)
[ace] 85.87.156.75:8621: connected + handshaked
[ace] 85.87.156.75:8621: reconnected; window min=14718220 max=14718269 -> resuming from 14718270 head=14718269
[ace] 85.87.156.75:8621: UNCHOKE -> requesting pieces 14718270..=14718269
[ace] 85.87.156.75:8621: stale upstream — no live progress for 12s; reconnecting
```

So the new behavior works as intended, but this target still does not prove continuous
playback. Rediscovery added one peer, yet no alternate peer completed the live follow path,
and retrying the known reachable peer still showed the same static window.

## Current blocker

The remaining no-stutter blocker is now narrower:

> Outpace can keep the session alive and continue discovery/retry after a stale upstream,
> but for this swarm snapshot it still has only one usable upstream and that upstream's live
> window does not advance.

Next highest-leverage lead is multi-peer following / scheduling: keep a small set of
connected peers active, prefer peers whose advertised windows advance, and request missing
chunks from any peer that has them instead of committing the whole stream to the first peer
that handshakes.
