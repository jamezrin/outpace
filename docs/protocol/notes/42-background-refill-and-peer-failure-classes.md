# 42 - Background upstream refill works; alternate Synthetic Live Channel peers were mostly unreachable

Follow-up to note 41. The multi-upstream coordinator now keeps trying to fill the active
pool after the first usable peer starts serving, and peer acquisition failures are
classified by stage.

## Implemented behavior

`AceProvider` now classifies upstream acquisition outcomes:

- `tcp`: TCP connect failed or timed out;
- `handshake`: TCP connected but the 66-byte Acestream handshake failed;
- `window`: the peer handshook but did not provide a usable extended-handshake/live `mi`
  window;
- `connected`: TCP + Acestream handshake + live-window read succeeded.

The production live path also starts a background refill task whenever the first selected
pool has fewer than `MAX_ACTIVE_UPSTREAMS` peers. The refill task:

- skips peers already active in the pool and peers already excluded by the current session;
- keeps trying the remaining candidates while the first upstream is serving;
- sends successful `ConnectedUpstream`s back to the coordinator;
- lets the coordinator sign the extended handshake, send `Interested`, spawn a worker, and
  schedule through the same shared `Continuity` + `ActivePeers` + `Scheduler`;
- updates the shared head immediately if a refilled peer advertises a fresher live window.

New focused tests:

- `pool_refill_candidates_skip_active_and_excluded_peers`
- `peer_connect_stats_reports_failure_classes`

## Verification

- `cargo test -p ace-engine`
- `cargo test -p ace-swarm`
- `rustfmt --edition 2021 --check crates/ace-engine/src/ace_provider.rs`
- `cargo clippy -p ace-engine -p ace-swarm --all-targets -- -D warnings`
- `cargo build -p ace-engine --bin outpace`
- `git diff --check`

## Live smoke

Target:

- infohash `50e93529d3eb46a50506b14464185a15292d6e47`
- daemon `127.0.0.1:6900`
- capture length: 75 s curl timeout

Result:

- `HTTP 200`
- first byte: `7.714653` s
- downloaded: `8,308,472` bytes
- MPEG-TS sync at offsets `0`, `188`, `376`; first 1000 packets had zero sync errors.

Relevant daemon log:

```text
[ace] open 50e93529d3eb46a50506b14464185a15292d6e47: discovered 13 peer(s)
[ace] initial upstream selection: attempted=3 connected=1 tcp=2
[ace] connected + handshaked upstream pool (1 peer(s)): 85.87.156.75:8621:14718220..14718269
[ace] 85.87.156.75:8621: window min=14718220 max=14718269 -> start=14718261 head=14718269
[ace] background upstream refill: trying 12 candidate(s)
[ace] 85.87.156.75:8621: UNCHOKE -> scheduling from piece 14718261 toward head 14718269
[ace] 85.87.156.75:8621: served 1 MiB (head=14718269, next piece needed=14718263)
[ace] 85.87.156.75:8621: served 6 MiB (head=14718269, next piece needed=14718268)
[ace] background upstream refill finished: attempted=12 tcp=11 handshake=1
[ace] upstream pool stale - no live progress for 12s; reconnecting 1 peer(s)
[ace] no usable upstreams: attempted=12 tcp=11 handshake=1
[ace] no reachable peer among 13 known; rediscovery found 12, added 0 (known now 13)
[ace] initial upstream selection: attempted=2 connected=1 tcp=1
[ace] 85.87.156.75:8621: stale advertised window min=14718220 max=14718269 cannot cover next needed piece 14718270; trying another peer
[ace] no usable upstreams: attempted=12 tcp=11 handshake=1
```

## Interpretation

This closes one ambiguity from note 41: the active pool is not stuck at one peer only
because outpace stops trying after first byte. In this run, outpace kept trying the
other discovered peers while serving the initial window, but none became usable:

- 11 of 12 refill candidates failed at TCP connect;
- 1 of 12 connected but failed the Acestream handshake;
- 0 of 12 reached the live-window stage.

The remaining blocker is now peer acquisition quality for this public target, not the
multi-peer coordinator itself:

> outpace can continue filling a live upstream pool in the background, but the current
> Synthetic Live Channel discovery set still contains only one usable upstream, and that upstream advertises
> a static window behind the next-needed piece.

Next highest-leverage leads:

- compare official-engine peer acquisition for the same infohash on the same network and
  capture how many reachable peers it finds;
- deepen background discovery without delaying first byte, then feed newly discovered peers
  into the same refill path;
- add address-level samples for failure classes if the next comparison needs exact overlap
  between official-engine peers and outpace peers.
