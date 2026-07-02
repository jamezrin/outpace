# 43 - Concurrent background discovery runs before stale timeout; official engine also stalls on this target

Follow-up to note 42. The prior background refill still ran deeper DHT discovery only
after exhausting the known candidate list. With 13-14 mostly dead peers and a 3 s TCP
timeout, that could leave too little time before the 12 s stale-upstream reconnect.

## Implemented behavior

`ace-swarm::discover` now exposes `DiscoveryOptions` and
`discover_peers_with_options`, and `ace-swarm::dht::dht_get_peers_with_target` is public
so callers can ask the DHT for more than the fast-start default target.

The provider's background refill now starts deeper discovery immediately, in parallel with
retrying the known non-active candidates. Once the known candidates are exhausted, it
consumes the already-running discovery result, dedupes it against the known set, and tries
any newly discovered addresses through the same refill path.

Background refill options:

- DHT budget: 8 s
- peer target: 64
- budget is intentionally below the 12 s stale-upstream timeout, so a discovery round can
  finish before the static peer is dropped.

New tests:

- `discovery_options_default_to_fast_start_target`
- `newly_discovered_refill_candidates_are_deduped_against_known_peers`
- `background_discovery_options_leave_stale_timer_margin`

## Verification

- `cargo test -p ace-engine`
- `cargo test -p ace-swarm`
- `rustfmt --edition 2021 --check crates/ace-engine/src/ace_provider.rs crates/ace-swarm/src/discover.rs crates/ace-swarm/src/dht.rs`
- `cargo clippy -p ace-engine -p ace-swarm --all-targets -- -D warnings`
- `cargo build -p ace-engine --bin outpace`
- `git diff --check`

## Outpace live smoke

Target:

- infohash `50e93529d3eb46a50506b14464185a15292d6e47`
- daemon `127.0.0.1:6900`
- capture length: 75 s curl timeout

Result:

- `HTTP 200`
- first byte: `3.012109` s
- downloaded: `8,308,472` bytes
- MPEG-TS sync at offsets `0`, `188`, `376`; first 1000 packets had zero sync errors.

Relevant daemon log:

```text
[ace] open 50e93529d3eb46a50506b14464185a15292d6e47: discovered 1 peer(s)
[ace] no usable upstreams: attempted=1 tcp=1
[ace] no reachable peer among 1 known; rediscovery found 15, added 14 (known now 15)
[ace] initial upstream selection: attempted=2 connected=1 tcp=1
[ace] connected + handshaked upstream pool (1 peer(s)): 85.87.156.75:8621:14718220..14718269
[ace] background upstream refill: trying 14 candidate(s)
[ace] 85.87.156.75:8621: UNCHOKE -> scheduling from piece 14718261 toward head 14718269
[ace] 85.87.156.75:8621: served 6 MiB (head=14718269, next piece needed=14718268)
[dht] frontier exhausted: queried=8
[ace] background upstream discovery: found 1, added 0
[ace] background upstream refill finished: attempted=14 tcp=13 handshake=1
[ace] upstream pool stale - no live progress for 12s; reconnecting 1 peer(s)
```

The important change relative to note 42 is that the deeper background discovery now
finishes before stale reconnect and logs its result. In this run it still found no new
candidate beyond the known set.

## Official-engine comparison

Official engine sandbox:

- engine `3.2.11`
- HTTP API `127.0.0.1:6878`
- same content id `cid1`
- same resolved infohash `50e93529d3eb46a50506b14464185a15292d6e47`

Official `/ace/stat` after startup:

```text
status=dl peers=1 speed_down=19 downloaded=10485760
status=dl peers=1 speed_down=15 downloaded=10485760
...
status=dl peers=1 speed_down=2 downloaded=10485760
```

Official playback capture with redirects followed:

- `HTTP 200`
- first byte after redirect: `0.001584` s
- downloaded: `10,484,740` bytes
- MPEG-TS sync at offsets `0`, `188`, `376`; first 1000 packets had zero sync errors.
- capture timed out at 75 s with no more bytes.

## Interpretation

For this target and this network snapshot, the reference engine also did not demonstrate
continuous playback. It found one peer, buffered about 10 MB, and then stopped advancing.
Outpace found the same swarm infohash, one usable peer, and a smaller but valid initial
window.

So this specific Synthetic Live Channel content id is currently not a good proof target for the user's
"no pauses/stutter" requirement. It remains useful for startup and protocol-regression
smoke tests, but not for proving continuous playback.

Remaining compatibility work:

- find or generate a live target where the official engine actually advances continuously,
  then compare outpace against that target;
- or use a controlled official source-node/local tracker setup as a continuous live source
  and prove outpace can follow it by `content_id`/transport-derived infohash;
- if a public target is required, capture official-engine peer endpoints early enough to
  compare exact reachable peer sets before its single connection closes.
