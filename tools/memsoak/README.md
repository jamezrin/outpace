# memsoak — outpace memory soak harness

Tools to reproduce and quantify outpace's memory behavior under real playback, and to A/B it
against the reference Acestream engine. Everything plays **real bytes** (ffmpeg decodes to
`-f null`), never mocked.

## Scripts

- `run.sh` — launch `outpace serve` with a fresh isolated data-dir, drive N real decoders against
  `/streams/ace/<id>.ts` (or `.m3u8`), and sample process memory + `/debug/memstats` +
  `/streams/.../status` into CSVs. Modes: `idle`, `play`, `churn`.
- `acestream_probe.sh` — run `jopsis/aceserve` (Acestream 3.2.11) in docker, play the same ids,
  sample the container's working-set memory. Same CSV schema for apples-to-apples comparison.
- `slope.py` — summarize a results dir: RSS slope/plateau, jemalloc `allocated` (real-leak signal)
  vs `resident-allocated` gap (fragmentation signal), thread/fd counts, per-stream decode liveness.

## Examples

```bash
# 5-minute single-stream soak (default config)
bash tools/memsoak/run.sh --mode play --ids <content-id> --duration 300

# isolate the seed store: shrink it while keeping the download path intact
bash tools/memsoak/run.sh --mode play --ids <id> --duration 180 \
  --env "OUTPACE_SEED_STORE_BYTES=8388608"

# idle daemon (no playback) — background-loop leak check
bash tools/memsoak/run.sh --mode idle --duration 600

# A/B baseline against Acestream
bash tools/memsoak/acestream_probe.sh --ids <id> --duration 240
```

Results land in `tools/memsoak/results/<timestamp>-<label>-<mode>/` (`process.csv`,
`streams.csv`, `summary.txt`, per-id ffmpeg/progress logs). The `results/` dir is git-ignored.

## Reading the output

`/debug/memstats` (jemalloc, on by default on non-Windows builds) is the truth layer:

- **`allocated` flat over time** → no leak; the live heap is bounded.
- **`allocated` climbs without bound** → real leak; localize with subsystem toggles.
- **`resident - allocated` grows while `allocated` is flat** → allocator retention/fragmentation,
  not a leak.

## Note on stream liveness

Public content ids go offline without warning. `streams.csv` records decoded `frames` and `peers`
per id; if a stream shows `peers~0` and flat frames, confirm with `acestream_probe.sh` before
blaming outpace — if both fail, the stream is dead.
