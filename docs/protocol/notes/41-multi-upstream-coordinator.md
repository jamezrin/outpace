# 41 - Multi-upstream coordinator is wired, but the Synthetic Live Channel swarm still yielded one usable upstream

Follow-up to note 40. The missing production coordinator now exists: `AceProvider` can own
a bounded set of handshaked upstream sessions, feed their messages into one shared
`Continuity`, and dispatch chunk requests to whichever peer `ActivePeers::assign` selected.

This is a structural fix, not a playback fix for the public Synthetic Live Channel target yet. The live
smoke still produced the same valid initial MPEG-TS window and then stalled, because the
current swarm snapshot produced only one peer that completed TCP connect, Acestream
handshake, and live-window advertisement.

## Implemented behavior

`connect_any` has been replaced by `connect_pool`:

- races candidate peers as before;
- requires the peer's extended handshake/live `mi` window before accepting it;
- briefly collects other near-complete candidates;
- sorts selected upstreams by freshest advertised window;
- returns up to `MAX_ACTIVE_UPSTREAMS` handshaked `ConnectedUpstream` sessions.

`follow_live` now passes that pool into `follow_peer_pool`. The coordinator:

- initializes or resumes one shared `Continuity`;
- sends the signed extended handshake and `Interested` to each selected upstream;
- spawns one `peer_worker` task per upstream;
- receives `PeerEvent::{Message,Lost}` centrally;
- updates `ActivePeers` on `Unchoke`, `Choke`, `Have`, `id=4`, and bencoded live-window
  updates;
- schedules through `ActivePeers::assign`;
- sends chunk requests back to each selected peer through a command channel;
- on peer loss, removes only that peer and requeues only that peer's in-flight pieces with
  `Scheduler::on_drop`;
- keeps the reciprocal upload path working through worker `Send` commands.

Focused tests added:

- `peer_worker_sends_chunk_requests_from_commands`
- `peer_worker_emits_messages_from_session`

## Verification

- `cargo test -p ace-engine peer_worker -- --nocapture`
- `cargo test -p ace-engine active_peer -- --nocapture`
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
- first byte: `7.605502` s
- downloaded: `8,308,472` bytes
- MPEG-TS sync at offsets `0`, `188`, `376`; first 1000 packets had zero sync errors.

Relevant daemon log:

```text
[ace] open 50e93529d3eb46a50506b14464185a15292d6e47: discovered 13 peer(s)
[ace] connected + handshaked upstream pool (1 peer(s)): 85.87.156.75:8621:14718220..14718269
[ace] 85.87.156.75:8621: window min=14718220 max=14718269 -> start=14718261 head=14718269
[ace] 85.87.156.75:8621: UNCHOKE -> scheduling from piece 14718261 toward head 14718269
[ace] 85.87.156.75:8621: served 1 MiB (head=14718269, next piece needed=14718263)
[ace] 85.87.156.75:8621: served 6 MiB (head=14718269, next piece needed=14718268)
[ace] 85.87.156.75:8621: unhandled msg id=11 (178 bytes) 64313a61693165313a62693065313a636934353230313765
[ace] 85.87.156.75:8621: unhandled msg id=34 (8 bytes) 0000000100009286
[ace] upstream pool stale - no live progress for 12s; reconnecting 1 peer(s)
[ace] no reachable peer among 13 known; rediscovery found 1, added 0 (known now 13)
[ace] 85.87.156.75:8621: stale advertised window min=14718220 max=14718269 cannot cover next needed piece 14718270; trying another peer
[ace] no reachable peer among 13 known; rediscovery found 13, added 1 (known now 14)
```

## Interpretation

The coordinator path is active in production: the log says `upstream pool`, and request
dispatch is now worker/channel based. However, this live run did not exercise true
multi-upstream piece filling because only one usable upstream entered the pool.

The remaining blocker is therefore more precise than note 40:

> outpace can coordinate multiple handshaked upstreams, but the current Synthetic Live Channel live
> run still obtains only one window-advertising upstream, and that upstream's window stays
> static behind the next-needed piece.

Next highest-leverage leads:

- keep filling the active pool in the background while the first peer is already serving,
  instead of only collecting near-complete peers during the initial selection grace window;
- log/connect-failure classes for the other discovered peers so we know whether they fail
  TCP connect, 66-byte handshake, extended-handshake/window read, or signed-handshake
  acceptance;
- if alternate peers are still unavailable, compare official-engine peer acquisition for
  this same target on the same network.
