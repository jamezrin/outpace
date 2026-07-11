# DHT routing-node cache — measurement runbook (#42)

Evaluate-first, **default-OFF** prototype. This document describes how to measure whether a
bounded, in-memory cache of recently-successful DHT routing nodes improves repeated `get_peers`
startup **without** slowing the cold path or displacing the public bootstrap fallback. The
implement-vs-reject decision is made from live-DHT numbers gathered with this procedure. The
2026-07-11 multi-target measurement below recommends keeping the bounded in-memory prototype.

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
| unique peers | deduplicated peers collected by the walk | must reach the production target (8) |
| `queried` (`nodes_queried`) | `get_peers` sent | warm may reach useful nodes in fewer queries |
| `timeouts` | silent response windows | must NOT rise warm (stale seeds must not add timeouts) |
| `cache_seeded` | cached nodes added to the frontier | 0 when disabled; >0 warm |
| `seeded` (`bootstrap_seeded`) | bootstrap routers seeded | unchanged by the cache |

## Runs

All four cases use real infohashes on the live DHT. Use the ignored live benchmark as the harness:

```
ACE_INFOHASHES=<40hex,40hex,...> ACE_DHT_MEASURE_RUNS=5 \
  cargo test -p ace-swarm dht_live_routing_cache_measurement -- --ignored --nocapture
```

The harness interleaves cases in one process to reduce time-of-day drift. Every warm sample is the
second walk after a separate cache-populating walk. Every stale sample injects a successful public
node timestamped just beyond the 15-minute freshness window, then performs a real-network walk;
the entry must be excluded (`cache_seeded=0`) without waiting 15 minutes. Population walks are not
included in the reported samples.

- **COLD**: `OUTPACE_DHT_ROUTING_CACHE` unset. Bootstrap-only. Baseline `ttfp_ms`.
- **WARM**: cache enabled and freshly populated by a prior walk. Expect lower `ttfp_ms`,
  `cache_seeded > 0`, at least eight unique peers, and no timeout regression.
- **STALE**: cache enabled with an already-aged entry. It must not be seeded and must behave like
  cold bootstrap without a fixed delay.
- **DEAD**: cache enabled with one fresh but nonresponsive eligible hint. It must be queried
  alongside bootstrap without adding a serialized preflight delay.

## Decision criteria (from the issue)

- **Reject** (close as "not worth implementing") if there is **no benefit** or a systematic
  startup regression: failure to reach the production peer target, worse TTFP/timeouts, or a
  fixed stale/dead-hint delay.
- **Keep** only if cached nodes measurably speed repeated startup, do **not** slow startup in the
  cold/stale cases, and **never** displace the public bootstrap fallback (`bootstrap_seeded` is
  unchanged and bootstrap is always seeded). Disk persistence is a separate follow-up, considered
  only if in-memory caching proves useful.

## Recorded live result — 2026-07-11

Environment: `feat/dht-routing-cache-42` plus the measurement harness, Linux
`7.0.14-arch1-1` x86_64, `rustc 1.96.0`, Europe/Madrid. No WARP/Cloudflare interface was present
in `ip -brief link`. Case order was deterministically shuffled per target/run. A successful walk
means it reached the production target of at least eight **unique** peers; raw peer-record totals
are deliberately not an acceptance gate because the terminal response can contain several records.

The retained raw JSONL contains all 60 measured samples for the three targets that reached the
production peer target, including the one failed cold sample for `47eda...`:

- `docs/measurements/2026-07-11-dht-routing-cache.jsonl`
- SHA-256: `457de0fca63aeaa55977b43ea52204d445260a54a0e0f6d126554a6fbca72252`

| Target | Case | Successes | Median TTFP | Median unique peers | Median queries | Median timeouts |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `685edf...6067` | cold | 5/5 | 6037 ms | 9 | 32 | 0 |
| | warm | 5/5 | 3300 ms | 9 | 32 | 0 |
| | stale | 5/5 | 7536 ms | 9 | 40 | 0 |
| | fresh-dead | 5/5 | 6038 ms | 9 | 33 | 0 |
| `50e935...6e47` | cold | 4/5 | 6050 ms | 8 | 48 | 0 |
| | warm | 5/5 | 6047 ms | 8 | 48 | 0 |
| | stale | 5/5 | 6049 ms | 8 | 40 | 0 |
| | fresh-dead | 5/5 | 6045 ms | 8 | 49 | 0 |
| `c12345...4567` | cold | 5/5 | 6155 ms | 15 | 32 | 0 |
| | warm | 5/5 | 6048 ms | 15 | 40 | 0 |
| | stale | 5/5 | 6270 ms | 15 | 32 | 0 |
| | fresh-dead | 5/5 | 6074 ms | 15 | 33 | 0 |

The fresh-dead case inserts `1.1.1.1:1` through the normal public-address eligibility path at the
current time. Every sample reports `cache_seeded=1`, proving it was actually queried alongside all
four bootstrap seeds. It added one query at the median and no median TTFP penalty. One of 15 dead
samples recorded a timeout; because the hint shares the existing response round rather than adding
a preflight, paired TTFP did not regress at the median. Aged entries always reported
`cache_seeded=0`. Bootstrap remained four in every retained sample.

`d12345...4567` was too weak for comparison: cold reached eight peers only 1/5 times, warm 3/5,
and aged/dead 0/5. It is recorded as a current target failure and excluded from performance
conclusions rather than selectively treating its sparse successes as paired evidence.

Across the two fully reliable targets and the still-current `47eda...` target, warm success was
15/15 versus cold 14/15. Warm materially improved `0a4848...` (median -2737 ms), was effectively
neutral on `47eda...` (-3 ms), and modestly improved `9439a4...` (-107 ms). Stale and fresh-dead
medians stayed within ordinary live-DHT variation and did not add a fixed delay; public bootstrap
was never displaced.

**Recommendation: KEEP the bounded daemon-session cache implementation.** It demonstrates a large
repeat-start benefit on one current target, no loss of reaching the peer target, and no systematic
stale/dead startup regression. Keep `OUTPACE_DHT_ROUTING_CACHE` **default off** for now: the sample
is small, one dead run timed out, and query cost rose on one target. This lands the measured,
operator-opt-in implementation without changing existing startup defaults. Enabling it by default
should require broader/longer telemetry and is separate from disk persistence.
