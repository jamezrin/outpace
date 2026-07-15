# Outpace memory hardening — design

Date: 2026-07-15
Branch context: investigation may spin its own branch off `main`.

## Problem

Operators observe outpace RSS climbing ~300 MB within <2 min of use and never
falling again, even with no playback clients connected. Goal: no performance
regression, and steady-state memory usage better than the reference Acestream
engine.

## Three candidate causes (the fix differs per cause)

1. **Real unbounded leak** — heap `allocated` grows without bound (a
   `Vec`/map/registry never pruned, a task/connection never dropped).
2. **Allocator retention** — glibc arenas hold freed pages; heap is fine but RSS
   never drops. Plausible given the default glibc allocator and a multi-threaded,
   bursty byte path.
3. **By-design P2P retention** — the 128 MB in-RAM `PieceStore` seed budget plus
   swarm/DHT/announce state that should be bounded but may be mis-bounded
   (owner-less seed entries reaped only after a 300 s TTL).

Step one is **separating leak from fragmentation from budget**, not guessing.

## Approach: layered measurement

- **Outer soak layer** — a harness sampling RSS/PSS/FDs/threads + `/status`
  counters over 5 min → 1 h → 2 h, driving the real release binary with real
  players decoding real bytes. Used for long runs and the Acestream A/B.
- **Truth layer** — jemalloc (`tikv-jemallocator`) behind a `profiling` feature,
  reading `stats.allocated` vs `stats.resident` vs `stats.retained` via
  `jemalloc_ctl`, exposed on `GET /debug/memstats`. Distinguishes cause #1
  (allocated grows) from #2 (resident−allocated grows). No external tools/sudo.
- **Localization layer** — subsystem bisection via config toggles
  (`OUTPACE_ENABLE_INBOUND=0`, seeding off, HLS vs raw `.ts`) plus jemalloc heap
  profiling (`MALLOC_CONF=prof:true`) if a precise call stack is needed.

Rejected: valgrind/massif as primary (20–50× slowdown starves a live
multi-Mbps stream; only for short synthetic replays); pure in-process counters
as the sole method (misses leaks outside instrumented structs).

## Harness (`tools/memsoak/`, committed)

1. Builds `--release`, launches `outpace serve` with a fresh isolated
   `--data-dir`.
2. Starts real consumers of `http://127.0.0.1:6878/streams/ace/<id>.ts`
   (`ffprobe`/`ffmpeg -f null` decoding; `cvlc --intf dummy` variant) and
   verifies liveness (frame count advancing, bytes flowing).
3. Samples every N s to CSV: `VmRSS`, `VmHWM`, PSS (`smaps_rollup`), FD count,
   thread count, `/debug/memstats`, and `/streams/.../status`. Computes RSS
   slope (bytes/min) and plateau detection.
4. Same driver points at the Acestream docker container
   (`references/docker-acestream-aceserve`, x86_64) for the A/B; container RSS
   from cgroup / `docker stats`.
5. Stream-death guard: an id that stops yielding bytes is replayed against
   Acestream — both fail ⇒ dead stream, skip; only outpace fails ⇒ separate
   outpace bug, logged and excluded from the leak metric.

## Experiment matrix

| Phase | Config | Isolates |
|---|---|---|
| Pure idle | serve, never play | background loops leaking at zero playback |
| Single 5-min play | 1 stream, raw `.ts` | baseline growth rate during playback |
| Post-play idle | stop client, wait > grace + 300 s TTL | reaper release vs retention |
| P2P off | `OUTPACE_ENABLE_INBOUND=0`, seeding off | seed store / peer layer vs media path |
| HLS vs raw | `.m3u8` vs `.ts` | segment-retention path |
| Multi-stream / churn | start+stop the 4 ids repeatedly | per-session teardown leaks |
| Long soak | best single id, 1 h → 2 h | slow leaks invisible at 5 min |

Content ids under test: `cid5`,
`cid6`,
`cid7`,
`cid8`.

## Fix workflow (no regressions)

1. Reproduce + classify each growth source.
2. Per confirmed cause, write a failing memory-bound assertion / soak check
   first, then fix (systematic-debugging + TDD).
3. Likely levers (evidence-driven): switch global allocator to jemalloc/mimalloc
   and/or periodic `malloc_trim` for fragmentation; fix any unbounded
   structure/registry for a true leak; tighten seed-store/reaper bounds if
   budget-shaped.
4. No-regression gates captured before & after: served bitrate/throughput, CPU,
   startup-to-first-byte, decode continuity (ffprobe frame/drop counts), plus
   `cargo test --workspace` and `clippy -D warnings`. "Better than Acestream" =
   lower steady-state RSS at equal decoded bitrate in the A/B.

## Deliverables

- `tools/memsoak/` harness + short runbook.
- Findings report (per-phase CSVs + slopes, leak-vs-fragmentation verdict, A/B).
- Fixes, each behind its own memory-bound regression check, all tests + clippy
  green.

## Caveats

- Streams may die mid-soak; the death-guard handles it but a clean 2 h run needs
  at least one id alive that long.
- `heaptrack`/`gnuplot` not installed — routed around via jemalloc stats +
  awk/python for slopes. `sudo pacman -S heaptrack` optional.
- jemalloc added behind a feature flag for diagnosis; shipping it as the default
  is a fix decision brought back with evidence, not assumed.
