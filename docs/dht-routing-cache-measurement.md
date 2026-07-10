# DHT routing-node cache — measurement runbook (#42)

Evaluate-first, **default-OFF** prototype. This document describes how to measure whether a
bounded, in-memory cache of recently-successful DHT routing nodes improves repeated `get_peers`
startup **without** slowing the cold path or displacing the public bootstrap fallback. The
implement-vs-reject decision is made from live-DHT numbers gathered with this procedure — it is
NOT decided here (the offline test suite only proves the invariants, not the payoff).

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

All three use a real infohash on the live DHT. Use the ignored live test as the harness:

```
# COLD (baseline, cache OFF — the default). Repeat a few times for a median.
ACE_INFOHASH=<40hex> cargo test -p ace-swarm dht_live_finds_peers -- --ignored --nocapture

# WARM (cache ON). Same process/session so the cache is populated by the first walk, then
# measure a SECOND walk toward a nearby/related target that reuses the harvested nodes.
OUTPACE_DHT_ROUTING_CACHE=1 ACE_INFOHASH=<40hex> \
  cargo test -p ace-swarm dht_live_finds_peers -- --ignored --nocapture
```

Because `dht_live_finds_peers` is a single walk, the most faithful warm/stale comparison is at
the daemon level: start the daemon with `OUTPACE_DHT_ROUTING_CACHE=1`, resolve one stream (cold —
cache empty), then resolve a second stream shortly after (warm — cache populated) and diff the two
`[dht] walk done` lines. For the **STALE** case, populate the cache, then let it age past the
15-minute freshness window (or point it at a target whose cached nodes have since gone offline)
and resolve again: seeds are excluded once stale/failed, so the walk must fall back to bootstrap
with `ttfp_ms` and `timeouts` no worse than COLD.

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
```
