# memsoak — outpace memory soak harness

Tools to reproduce and quantify outpace's memory behavior under real playback, and to A/B it
against the reference Acestream engine. Existing playback harnesses play **real bytes** (ffmpeg
decodes to `-f null`), never mocked.

## Scripts

- `run.sh` — launch `outpace serve` with a fresh isolated data-dir, drive N real decoders against
  `/streams/ace/cid:<id>.ts` (or `.m3u8`), and sample process memory + `/debug/memstats` +
  `/streams/.../status` into CSVs. Modes: `idle`, `play`, `churn`.
- `acestream_probe.sh` — run `jopsis/aceserve` (Acestream 3.2.11) in docker, play the same ids,
  sample the container's working-set memory. Same CSV schema for apples-to-apples comparison.
- `slope.py` — summarize a results dir: RSS slope/plateau, jemalloc counters, thread/fd counts,
  and per-stream decode liveness. A fitted slope or allocator counter is an observation, not a
  standalone leak diagnosis.
- `session_cycles.py` — bounded session lifecycle soak for one explicitly selected local daemon;
  see [Session lifecycle soak](#session-lifecycle-soak).

## Examples

```bash
# 5-minute single-stream soak (resolve cid1 from the private registry)
bash tools/memsoak/run.sh --mode play --ids "$ACE_CONTENT_ID" --duration 300

# isolate the seed store: shrink it while keeping the download path intact
bash tools/memsoak/run.sh --mode play --ids <id> --duration 180 \
  --env "OUTPACE_SEED_STORE_BYTES=8388608"

# idle daemon (no playback) — background-loop soak
bash tools/memsoak/run.sh --mode idle --duration 600

# A/B baseline against Acestream
bash tools/memsoak/acestream_probe.sh --ids "$ACE_CONTENT_ID" --duration 240
```

Results land in `tools/memsoak/results/<timestamp>-<label>-<mode>/` (`process.csv`,
`streams.csv`, `summary.txt`, per-id ffmpeg/progress logs). The `results/` dir is git-ignored.

## Reading the output

`/debug/memstats` can report jemalloc `allocated` and resident counters on supported builds. A
valid `allocated` sample measures the current live allocation count; a persistent increase across
controlled runs may warrant investigation, but does not by itself establish a leak. RSS can remain
high because freed pages are retained by an allocator. The `resident - allocated` gap is useful
context for retention and fragmentation, but is not a leak verdict either.

## Note on stream liveness

`--ids` accepts content ids; the outpace harness adds the native API's `cid:` prefix so the
daemon resolves their current infohash and piece geometry. The playback harnesses decode video and
audio, leaving subtitle and data tracks out of the null output.

Public content ids go offline without warning. `streams.csv` records decoded `frames` and `peers`
per id; if a stream shows `peers~0` and flat frames, confirm with `acestream_probe.sh` before
blaming outpace — if both fail, the source may be unavailable.

## Session lifecycle soak

`session_cycles.py` is a bounded harness for an operator-selected, already-built local daemon. It
owns that child process, uses fresh mode-0700 data/cache directories under `/tmp`, and binds HTTP
and RTMP listeners to loopback. Inbound serving and port mapping are disabled, so no peer
listener is expected or required. There is no attach mode, CI integration, Docker use, or automatic
binary build. The selector is read only from
`ACE_CONTENT_ID` in the invoking environment; the harness never reads the local identifier registry.

Resolve cid1 from the private registry into `ACE_CONTENT_ID` in your shell. The registry is
LLM-context-only; the harness never reads it. Select a budget appropriate to your workload:

```sh
ACE_CONTENT_ID="$ACE_CONTENT_ID" python3 tools/memsoak/session_cycles.py \
  --binary ./target/release/outpace --budget-mib 32 \
  --warmups 1 --cycles 4 --media hls --teardown idle --playback-duration 30 --idle-grace 65
```

The 32 MiB value above is an illustrative operator choice, not an accepted threshold or default
recommendation. Select a budget for the workload and build under investigation.

Choose `--media ts` or `hls`, and `--teardown idle` or `delete`. Playback duration and teardown
grace are separate. Both media modes require a fixed idle grace of at least 65 seconds; it always
runs to completion before the harness checks session absence. Delete teardown requires a 204
response and subsequent validated session absence. Both teardown paths, including warmups, receive
the fixed `--postclose-grace` (seconds; 5 by default), followed by absence validation and measurement.
Warmups and their teardown finish before the metric is frozen and the baseline window begins. The
baseline and each cycle window contain the configured `--baseline-samples` and `--window-samples`
observations; the harness uses their median and does not claim continuous measurement between samples.
Playback duration, `--idle-grace`, `--reap-timeout`, `--postclose-grace`, `--timeout`, and
`--overall-timeout` are all specified in seconds. The
harness removes private temporary state, discards daemon output, and emits a sanitized report
containing units, baseline, budget, cycle medians, deltas, peaks, executable SHA-256, and reported
version. It does not include identifiers, URLs, or raw exception strings.

Exit status is 0 for PASS (every cycle's median delta is within the chosen budget), 1 for FAIL (a
valid measurement exceeds it or a valid zero-client target remains after teardown), and 2 for
INCONCLUSIVE (invalid sample/schema, source/progress, identity, timeout, HTTP ambiguity, clients
still present, or interruption). A failed source
or no playback progress is not evidence about memory behavior. Jemalloc `allocated` is selected
only when the initial allocator, explicitly present null epoch error, and counter are valid; later
invalid samples make the run inconclusive and never switch metrics. If jemalloc is unavailable
initially, Linux RSS is selected only after verifying that the child executable matches the
requested binary and remains the same process. The harness uses a fixed environment allowlist,
loopback listeners, no proxy, and refuses redirects. Reports are bounded observations and do not
prove or disprove a leak; no slope extrapolation is performed.

For operator-run live validation, the operator independently verifies WARP is off and the route is
direct. The harness does not inspect or change network settings. Supply a current public source
through the private `ACE_CONTENT_ID` environment variable; never put identifiers in reports or
tracked files. TS playback requires continuous bytes through playback duration. HLS requires
advancing media sequence numbers and new segment bytes restricted to the native loopback route.
The harness does not decode media and makes no decode-progress claim. Measurements cover only the
session lifetime observed in `/streams`; inbound peer lifetimes are excluded, and a bounded
per-cycle delta does not establish a leak.
