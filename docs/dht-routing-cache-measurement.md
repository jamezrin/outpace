# DHT routing-node cache — measurement runbook (#42)

Evaluate-first, **default-OFF** prototype. This document describes how to measure whether a
bounded, in-memory cache of recently-successful DHT routing nodes improves repeated `get_peers`
startup **without** slowing the cold path or displacing the public bootstrap fallback. The
implement-vs-reject decision is made from live-DHT numbers gathered with this procedure. The
2026-07-11 measurement below recommends rejecting the prototype under the issue's strict criteria.

## What the prototype does

- `RoutingNodeCache` (`crates/ace-swarm/src/dht_cache.rs`): daemon-session, in-memory, bounded,
  prefix-diverse cache of nodes that returned a correlated valid `get_peers` response carrying a
  node id from a **public** address. `values`/peer records are never cached as routing nodes.
- When enabled, `ace_swarm::dht::dht_walk` seeds a new walk's frontier with cached nodes
  **alongside** the bootstrap routers (never instead of them), harvests fresh eligible responders
  back into the cache, and penalizes cached seeds that were queried but stayed silent.
- Gate: `OUTPACE_DHT_ROUTING_CACHE=1` (default off). With it off, the walk is byte-for-byte the
  original bootstrap-only path — no cache reads, no cache writes, identical frontier and metrics.

## Metrics to compare (`DhtWalkMetrics`, logged once per walk)

The one-line `[dht] walk done: ...` log carries every field. Compare across runs:

| Field | Meaning | What to watch |
| --- | --- | --- |
| `ttfp_ms` (`time_to_first_peer`) | walk start → first correlated reply carrying a peer | **headline signal**: should DROP warm, never rise |
| `peers` (`peers_discovered`) | total peers harvested | should be >= cold |
| `queried` (`nodes_queried`) | `get_peers` sent | warm may reach useful nodes in fewer queries |
| `timeouts` | silent response windows | must NOT rise warm (stale seeds must not add timeouts) |
| `cache_seeded` | cached nodes added to the frontier | 0 when disabled; >0 warm |
| `seeded` (`bootstrap_seeded`) | bootstrap routers seeded | unchanged by the cache |

## Runs

All three use a real infohash on the live DHT. Use the ignored live benchmark as the harness:

```
ACE_INFOHASH=<40hex> ACE_DHT_MEASURE_RUNS=5 \
  cargo test -p ace-swarm dht_live_routing_cache_measurement -- --ignored --nocapture
```

The harness interleaves cases in one process to reduce time-of-day drift. Every warm sample is the
second walk after a separate cache-populating walk. Every stale sample injects a successful public
node timestamped just beyond the 15-minute freshness window, then performs a real-network walk;
the entry must be excluded (`cache_seeded=0`) without waiting 15 minutes. Population walks are not
included in the reported samples.

- **COLD**: `OUTPACE_DHT_ROUTING_CACHE` unset. Bootstrap-only. Baseline `ttfp_ms`.
- **WARM**: cache enabled and freshly populated by a prior walk. Expect lower `ttfp_ms`,
  `cache_seeded > 0`, `peers >= cold`, `timeouts` no worse.
- **STALE**: cache enabled but entries stale/dead. Stale seeds are not seeded and dead ones are
  penalized without a fixed timeout — expect `ttfp_ms`/`timeouts` ~= COLD, never worse.

## Decision criteria (from the issue)

- **Reject** (close as "not worth implementing") if there is **no benefit** OR **any startup
  regression** — i.e. WARM does not beat COLD on `ttfp_ms`/`peers`, or STALE (or WARM) is worse
  than COLD on `ttfp_ms`/`timeouts`.
- **Keep** only if cached nodes measurably speed repeated startup, do **not** slow startup in the
  cold/stale cases, and **never** displace the public bootstrap fallback (`bootstrap_seeded` is
  unchanged and bootstrap is always seeded). Disk persistence is a separate follow-up, considered
  only if in-memory caching proves useful.

## Recorded live result — 2026-07-11

Environment: `feat/dht-routing-cache-42` at `4363323` plus the measurement harness, Linux
`7.0.14-arch1-1` x86_64, `rustc 1.96.0`, Europe/Madrid, public official-API example infohash
`685edf209ccfdf88977c0d317e1407baca486067`. No WARP/Cloudflare interface was present in
`ip -brief link`. Five interleaved samples per case completed in 113.27 seconds.

| Case/run | TTFP ms | Peer records | Queries | Timeouts | Bootstrap | Cache seeds |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| cold 1 | 7539 | 13 | 40 | 0 | 4 | 0 |
| cold 2 | 4630 | 14 | 32 | 0 | 4 | 0 |
| cold 3 | 9036 | 12 | 48 | 0 | 4 | 0 |
| cold 4 | 6303 | 14 | 40 | 0 | 4 | 0 |
| cold 5 | 4801 | 14 | 32 | 0 | 4 | 0 |
| warm 1 | 3032 | 13 | 24 | 0 | 4 | 14 |
| warm 2 | 4531 | 12 | 32 | 0 | 4 | 16 |
| warm 3 | 3031 | 12 | 24 | 0 | 4 | 14 |
| warm 4 | 4534 | 12 | 32 | 0 | 4 | 13 |
| warm 5 | 4534 | 12 | 32 | 0 | 4 | 8 |
| stale 1 | 6035 | 13 | 32 | 0 | 4 | 0 |
| stale 2 | 6034 | 12 | 32 | 0 | 4 | 0 |
| stale 3 | 6322 | 14 | 40 | 0 | 4 | 0 |
| stale 4 | 4534 | 12 | 24 | 0 | 4 | 0 |
| stale 5 | 6034 | 12 | 32 | 0 | 4 | 0 |
| **cold median** | **6303** | **14** | **40** | **0** | **4** | **0** |
| **warm median** | **4531** | **12** | **32** | **0** | **4** | **14** |
| **stale median** | **6034** | **12** | **32** | **0** | **4** | **0** |

Warm TTFP improved by 1772 ms (28.1%) and median query count fell by 8 (20%). Bootstrap remained
present in every run, and neither warm nor stale produced a timeout. Aged-stale median TTFP was
269 ms faster than cold, so there is no measured stale-startup regression.

**Recommendation: reject / close as not worth implementing under the issue's stated criteria.**
Although latency improved, warm median peer records fell from 14 to 12, failing the explicit
`peers >= cold` keep condition. This is a conservative decision: the walk stops after reaching its
eight-unique-peer target, so `peers_discovered` counts all records in the terminal response and is
not a fixed-work throughput measure; live DHT membership and response timing also vary between
runs. A future evaluation could use randomized targets, more repetitions, and a fixed-duration or
fixed-query walk, but that would be a new experiment rather than evidence satisfying this issue's
current acceptance rule.
