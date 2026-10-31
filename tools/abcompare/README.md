# Simultaneous live-stream comparison

Compare delivered MPEG-TS from outpace and the original AceStream engine at the
same time. This operator tool records raw bytes and per-read arrival times, then
compares startup, continuity, packet identity and live position. It uses Python
3.10+ and the standard library. The optional runner also needs Docker and an
already built outpace binary; analysis requires neither Docker nor live access.

Resolve **cid9 from the gitignored registry yourself**, copy only its 40-hex value
into `ACE_CONTENT_ID` in your shell, and keep that environment private. Neither
the scripts nor any shell helper may read the registry. Ignore trailing whitespace
and comments when copying. No content id, infohash or stream name belongs in code,
documentation, issues, commit messages or PRs.

Build the candidate (`cargo build --release -p ace-engine --bin outpace`) and have
`swarmtest-engine:latest` available; its image recipe is
[`../swarmtest/assets/engine.Dockerfile`](../swarmtest/assets/engine.Dockerfile),
using the official 3.2.11 engine. With cid9 supplied in `ACE_CONTENT_ID`, one command
runs the comparison with fresh state and prints its private `/tmp` artifact path:

```sh
READ_TIMEOUT=60 python3 tools/abcompare/run.py --outpace "$PWD/target/release/outpace" --seconds 180
```

The runner uses the engine image's default command, a uniquely named container
with `--network host`, and a fresh candidate data/cache directory. It checks the
reference ports 6878/8621 and the candidate ports before starting. The candidate
HTTP API binds to loopback on 16879, RTMP to loopback on 19350, and its peer listener
to all interfaces on 18622 (TCP/UDP). Override the latter three with
`--candidate-port`, `--rtmp-port`, `--peer-port`. Existing `OUTPACE_*` playback and
network settings are inherited; storage and listener settings are isolated by the
runner. For a default-behavior baseline, clear any tuning overrides first. It stops
only its own candidate PID and exact container name, including on Ctrl-C/SIGTERM.
It never prunes Docker, deletes artifacts, or kills other daemons. A forced kill of
the runner cannot execute cleanup: use the container name in private `run.json` if
manual cleanup is needed.

`--outdir /tmp/your-new-run` selects a fresh private directory; the runner rejects
existing paths and paths outside the system temporary directory. The original
engine's state is fresh inside its disposable container. Container/image metadata,
`run.json`, daemon logs, events, stats and raw media are **private evidence**: they
can contain identifiers, stream names, tokens or peer addresses. Do not commit or
publish them. `summary.json` omits URLs, ids and stat payloads, but review any report
before sharing it. Engine/candidate names are user-controlled in direct capture.

## Compare deployed instances with VLC

To investigate playback on existing servers, use the same route and decoder as
the reported playback. This separate runner starts two **local VLC clients**;
it does not start/reconfigure servers, Docker containers, WARP, or remote VLC.
It requires Python 3.10+, a POSIX host, and VLC with its HTTP interface and dummy
audio/video outputs (Debian/Ubuntu: `vlc`, including `vlc-plugin-base`). Run as an
ordinary user; VLC refuses to run as root. Both servers must already support
`/ace/getstream?id=…`; Outpace's compat API must already be enabled.

Supply a current public content id privately in `ACE_CONTENT_ID` as described
above. The runner never reads the registry. For example, with servers reachable
through local port forwarding:

```sh
python3 tools/abcompare/vlc_compare.py \
  --engine-url http://127.0.0.1:6878 \
  --outpace-url http://127.0.0.1:16879 \
  --seconds 600 --network-caching 1000 --poll-seconds 5 \
  --client-warp off --server-warp unknown
```

WARP flags **record operator observations**, they do not detect or change VPN
state. For a deployment reproduction, preserve and record the deployment's
settings. For a controlled local baseline, keep WARP off. Record server builds,
warm/cold cache state, and other viewers separately in private notes; the runner
does not inspect remote build/configuration. Concurrent viewers can affect swarms.

One command runs to completion without agent supervision and prints a fresh
private `/tmp/outpace-vlc-*` artifact directory and a numeric summary. Artifacts
include `comparison.txt`, `comparison.json`, `config.json`, VLC logs, playlists,
and raw sampled status. The directory is mode 0700; raw artifacts can contain
identifiers, URLs and stream names. Keep them private. Summaries omit source URLs,
identifiers and raw metadata; review before sharing. Inputs go in private playlist
files rather than VLC command arguments. The content-id environment variable is
removed from child environments. Local VLC controls bind to loopback and use a
random password; control requests bypass HTTP proxies.

Ctrl-C/SIGTERM stops and reaps only the two client processes started by this run,
then writes a partial report. A forced kill cannot clean up those clients.
Exit 0 means the bounded observation finished and both clients had some audio or
video decode progress; it is **not a playback quality pass**. Exit 2 means an
incomplete observation, launch/control setup problem, or no observed decode;
exit 130 means interrupted. Inspect control error counts, state counts, resets,
and per-client sample counts even on exit 0. Initial HTTP-interface startup may
produce control errors before the first successful sample.

Both clients ignore saved VLC configuration, use the same network caching, and
decode into dummy outputs. This exercises VLC demux/decode without a display or
audio device. It cannot measure visual presentation, device drops, or the exact
experience of a GUI player with different settings. Video-only/audio-only media
are accepted; an absent track has null timing rather than an invented failure.

`first_decode_observed_seconds` is the time a poll first sees a positive decode
counter, measured from the start of the comparison. It is an upper bound, not
precise startup latency; launch offsets are included in the JSON. The maximum
sampled progress gap includes the final unsampled tail after the last observed
counter change. Even smooth playback normally reports a gap near one polling
interval. Polling/control failures increase uncertainty; brief stalls between
polls may be missed. Counter resets are reported rather than silently adding
incompatible epochs. Equal counters do not establish content identity or live
position, and a completed run is not a full live acceptance matrix.

## Capture or analyze existing clients

For clients you start yourself on separate ports:

```sh
python3 tools/abcompare/ab_capture.py "$ACE_CONTENT_ID" 180 /tmp/new-ab-run \
  engine=http://127.0.0.1:6878 outpace=nativecid:http://127.0.0.1:16879
python3 tools/abcompare/ab_analyze.py /tmp/new-ab-run 180 engine outpace
```

Engine specifications are `name=BASE` for `/ace/getstream?id=…&format=json`,
`name=nativecid:BASE` for `/streams/ace/cid:…`, or `name=native:BASE` for
`/streams/ace/ih:…` using an **infohash**. Outpace compat routes
require `OUTPACE_ACE_COMPAT=1`; the runner uses native content-id
routing so it needs no compat flag. Names must contain only letters, digits,
underscores and hyphens, and must be unique. Playback follows up to five relative
or absolute redirects, including the original engine's 302 to `/content/…` and
HTTP(S) redirects. Capture retries EOF/errors after one second; these reconnects
remain visible in the events log. Playback-url discovery retries every two seconds.
All workers use one monotonic clock and a fixed global capture deadline.

Direct capture requires POSIX owner/mode permissions. It creates the requested
output directory with mode 0700 and exclusively reserves capture files with mode
0600 before writing. The parent directory must already exist. An existing output
directory is accepted only if it is a non-symlink directory owned by the caller
with mode 0700; otherwise capture rejects it without changing its permissions or
contents. This accepts the runner's existing private directory and preserves its
other files. Existing capture artifacts, including symlinks, are never overwritten.
Endpoint details also appear on stdout; redirect it only into private storage.

Files are `<name>.ts`, `<name>.arrivals.csv` (`t,nbytes,offset`),
`<name>.events.txt`, `<name>.stats.jsonl` (five-second polls) and `capture.json`.
The analyzer prints a concise per-client report and writes schema-versioned
`summary.json`. Missing media, missing PCR/RAI and no overlapping window are
reported as null metrics, not success. When PCR exists but the player never
starts, its stall count, stall time and stall list are null, because playback
quality has not been measured. Successful capture/analysis exit status
means artifacts were produced, **not** that the candidate passed a quality gate.
No automatic pass/fail thresholds are imposed on source-dependent live streams.

## What the metrics mean

- Startup: first completed byte read, first video RAI (random-access indicator)
  discovered via single-packet PAT/PMT, and estimated startup with three seconds
  of PCR media buffered. RAI is a TS flag, not a verified decodable keyframe.
- Arrival gaps: inter-read gaps over one second, including silence at capture end.
  Initial silence appears in first-byte time. Reads use `read1`, not fixed-size
  reads that wait to fill a large buffer; byte-rate buckets would hide pauses.
  Host scheduling, TCP buffering and read coalescing still affect these timings.
- Integrity: sync loss, leading/trailing partial bytes, malformed adaptations,
  transport error flags, continuity errors per PID, exact repeated packets and
  PCR jumps. Payload continuity increments modulo 16; one exact retransmission is
  allowed, a different packet with the same CC is an error, and discontinuity
  flags reset continuity expectations. Adaptation-only packets don't increment CC.
  PCR rollover is unwrapped per PID. Source discontinuities remain visible.
- `stale_prefix_suspect`: bytes before the first forward PCR jump over 60 seconds.
  It is a **suspect**, since a broadcaster restart or clock reset can look alike.
  Those bytes are excluded from the live-position/player timeline, but retained
  in integrity and packet mapping. Correlate the reference before blaming outpace.
- Packet mapping: candidate media packets map to unique exact packet hashes in
  the reference. Contiguous runs use ranks among unique media packets and count actual packets; reference holes and
  backwards/duplicate steps are separate. Ambiguous repeated reference packets
  are skipped and counted on both sides; they do not manufacture mapping holes. Reserved PSI/SI PIDs (including SDT), discovered PMTs,
  and null packets are excluded; cached/repeated tables are not media duplication.
  Whole-capture misses include startup/tail differences. `identity_in_overlap`
  restricts both sides to their shared PCR window with one-second edge margins.
  Match fractions include repeated candidate occurrences, so also check mapping
  breaks. No shared media window yields null identity.
- Frontier/buffer: the running maximum of the dominant PCR PID, sampled at shared
  wall times. Positive `frontier_reference_minus_candidate` means reference ahead.
  PCR wrap epochs must agree; independent PCR clocks or a broadcaster restart can
  make frontier differences meaningless.
- Player: a realtime drain estimate with three-second startup/recovery buffering,
  in 50-ms steps. Stalls include an unfinished stall at capture end. This is an
  **optimistic PCR-frontier model**, not a decoder: it can treat missing media
  inside a PCR jump as playable, ignores keyframe readiness, and freezes frontier
  on backward jumps. Large jumps make stall/buffer numbers unreliable. Always
  interpret them alongside continuity, identity, jumps and decoder checks.

The analyzer reads captures into memory and stores packet/hash indexes. Keep runs
bounded; multi-hour captures may need substantial RAM and disk. It is intended for
investigations, not an unattended soak service.

## Method and pitfalls

Run both clients **simultaneously**; live swarms drift and captures minutes apart
cannot establish byte identity. Hiccups shared by both outputs are source-side
candidates, not candidate regressions. Before starting, inspect `pgrep -a outpace`
and `docker ps` for other clients of the same stream. Small swarms sharing one
public IP can compete for two or three peers. Keep Cloudflare WARP off. The runner
does not change VPN services or stop other clients. Never use
`pkill -f "outpace serve"`: it can kill unrelated daemons and the invoking shell.

A bare 40-hex identifier on outpace's native live route is a **content id** (#165).
Use `nativecid:` or compat for content ids; `native:` adds the explicit `ih:` prefix
for verified infohash playback. Default outpace prebuffer/discovery can
produce headers followed by more than 15 seconds without body data. Set
`READ_TIMEOUT` above prebuffer timeout plus discovery (default here: 60 seconds).
Too short a timeout triggers retries, changing startup behavior. The global capture
deadline still stops a read at the requested end. Use enough duration for startup
and a meaningful common window; 180 seconds is a practical starting point.

For independent stream discovery/decoder evidence, keep outputs private:

```sh
ffprobe -v error -show_programs -show_streams -of json /tmp/your-run/engine.ts > /tmp/your-run/engine.probe.json
ffmpeg -v error -i /tmp/your-run/outpace.ts -map '0:v:0' -map '0:a:0' -f null - 2> /tmp/your-run/outpace.decode.log
```

Repeat both for each output. Quote map arguments for zsh/fish. Decoder stderr line
counts are diagnostic counts, not a standardized quality score: streams often
start mid-GOP and the logs can contain private stream metadata. Existing startup
and recovery issues must be reported, not hidden by tuning the candidate to win.

## Offline regression tests

```sh
python3 -m unittest discover -s tools/abcompare -v
```

Synthetic TS fixtures and a loopback HTTP server test continuity, RAI, PCR wrap,
stale-prefix detection, holes/duplicates, no-start/no-overlap handling, redirects
and silent-reader deadlines. CI runs these without Docker, registry access or
public-network streams. A live original-engine comparison remains an operator step.

## Bounded release gate (#196)

This optional offline gate supports the [v1.0 checklist (#208)](https://github.com/jamezrin/outpace/issues/208).
It does not sign off a production replacement or multi-hour reliability; the
long-soak work is separate (#210). Missing live prerequisites mean the operator
records **skipped**, never pass; the evaluator itself has no skip outcome.

For a release run, build the release binary above, manually supply `ACE_CONTENT_ID`
from the registry, and verify WARP is off and the direct route is in use. Inspect
other viewers first. Use the existing fresh candidate/container runner, simultaneous
180-second capture and owned-process cleanup:

```sh
READ_TIMEOUT=60 python3 tools/abcompare/run.py \
  --outpace "$PWD/target/release/outpace" --seconds 180 \
  --outdir /tmp/outpace-196-release
python3 tools/abcompare/evaluate.py /tmp/outpace-196-release/summary.json \
  --policy /tmp/outpace-196-policy.json \
  --output /tmp/outpace-196-release/gate.json
```

Create the policy privately **before interpreting results**. The explicit numbers
below are illustrative operator choices, not accepted release thresholds. Timing
allowance must be at least the model's 50-ms resolution; it is explicitly added
to startup and excess modeled stall-time limits, with no hidden tolerance.

```json
{
  "schema_version": 1,
  "max_startup_delta_seconds": 1.0,
  "max_excess_modeled_stall_count": 0,
  "max_excess_modeled_stall_seconds": 0.5,
  "min_byte_ratio": 0.95,
  "timing_allowance_seconds": 0.05,
  "attestation": {
    "simultaneous": true,
    "same_current_public_source": true,
    "same_settings": true,
    "same_routes": true,
    "fresh_state": true,
    "viewers_observed": true,
    "warp_off": true,
    "direct_route_verified": true,
    "complete": true
  },
  "provenance": {
    "candidate_build": "replace with verified candidate build identity",
    "reference_version": "replace with observed reference version (baseline 3.2.11)",
    "reference_image_digest": "replace with immutable image digest"
  }
}
```

Attest only after observing these conditions, including successful full capture,
analysis and cleanup without interruption/setup failure. Keep actual build identity,
observed engine version, immutable Docker image digest, run settings/routes, fresh
state, viewers and completion evidence in private provenance. API readiness HTTP
200 and a mutable image tag do not prove any 3.2.x version. The controlled baseline
is 3.2.11; verify it independently rather than trusting the tag. Do not put content
ids, infohashes, stream names or raw URLs in policy fields or shared reports.

The evaluator consumes analyzer `schema_version: 1` without redoing analysis.
Policy schema 1 requires every key shown, rejects unknown keys, non-finite numbers,
booleans as numbers and ratios outside (0, 1]. It emits gate schema 1 with
`outcome`, fixed diagnostic `reasons` and numeric `metrics`; it never copies
provenance strings or stream names. Exit 0 is pass, 1 fail, 2 inconclusive/error.
Pass requires complete comparable evidence and every explicit threshold met.
Candidate zero delivery or modeled no-start against a usable reference fails;
threshold breaches fail. Unusable reference, missing PCR overlap, suspect clocks
or unestablished comparability are inconclusive. Malformed/incomplete artifacts,
missing required metrics, interruption or setup failure are errors. Null modeled
no-start metrics stay null, never zero. Gate JSON is evidence to review, not a
release guarantee.

Startup here is PCR-modeled startup. Modeled stalls are never decoder stalls;
arrival gaps are reported separately and do not alone establish playback stalls.
Byte ratio is delivery volume, not completeness. Integrity/overlap diagnostics
remain in the analyzer summary for review; this gate adds no integrity thresholds.
PCR jumps invalidate the timing model. Shared pauses do not prove a source-wide
outage. Retain private summary/policy/gate links in release provenance and link that
review from #208 after merge; follow #210 for longer observation requirements.


For orchestration that already owns validated metadata leases, the Python callable
`vlc_compare.compare(args, None, playback_urls={"engine": official_url,
"outpace": candidate_url})` accepts tokenized playback URLs. Both are required;
validation requires the configured origin and role-specific `/ace/r/` capability
shape before launching VLC. URLs enter private mode-0600 playlists, not process
arguments or summary/config fields. The caller must verify unencrypted scope and
source identity, retain ownership and stop each exact lease on every exit path.
This option does not negotiate or stop leases. The CLI and default direct-ID
comparison behavior remain unchanged. Sampled VLC counters retain their existing
measurement limits.

For owned orchestration, `compare` also accepts `cancel_requested`, a cooperative
callback, and `artifact_parent`, an existing owner-only mode-0700 directory with
no symlink ancestors. Artifacts are created in a fresh private child beneath that
parent; omitting it preserves the system temporary-directory default. The callback
is checked before setup, around registered client launches, between bounded
control requests and between polls. Poll waits check the latch at most every 250 ms, subject to scheduling. It does
not interrupt an in-progress request, version probe or operating-system call. A cancelled observation exits
130 and uses the existing targeted client cleanup. The CLI latches SIGINT/SIGTERM
through client registration and cleanup using this same path. Metadata scope,
lease lifetime, exact stops and redirect policy still belong to the caller.
Cancellation is reconciled after the observation loop and again after all owned
client cleanup, before the report is finalized. Requests received through that
completion boundary produce interrupted status and exit 130; cleanup errors stay
visible in the report. A later signal does not retroactively change a completed
observation.

Both `config.json` and `comparison.json` expose `origin_monotonic`, the exact
local Python monotonic timestamp used as zero for relative sample and launch
seconds. It is captured before the VLC version probe and client launches, so
setup time is included. It remains present in partial reports. Use it only with
samples from the same machine and monotonic clock domain; it is neither UTC nor
a cross-host clock or an exact first-decode measurement.

### Optional timestamped client diagnostics

`vlc_compare.py --timestamped-diagnostics` (or keyword-only
`compare(..., diagnostics=True)`) captures both clients' stdout and stderr through
one owned selector thread. Disabled mode retains direct regular-file logging.
VLC argv and verbosity are unchanged; the version probe still uses `--vlc` and
has a cancellable ten-second timeout. No media proxy is inserted.

In this mode `engine-vlc.log`, `outpace-vlc.log`, and `probe-vlc.log` are private
0600 JSONL files containing base64 raw byte prefixes. They are persisted only
after owned children are reaped and capture has finished. A crash can lose buffered
evidence. Receipt timestamps share `origin_monotonic` with status observations;
they bracket parent pipe receipt, never VLC emission or server delivery. Order is
per stream only. Cleanup records are explicitly marked and do not extend the
reported observation endpoint.

Playback retention is at most 1 MiB encoded JSONL and 4096 records per role;
the probe has 64 KiB/64 records. Records are admitted whole, at most 16 KiB,
with at most 4096 raw prefix bytes per line and reads of at most 8192 bytes.
After a cap, draining continues without parsing further messages. Summary
counters saturate at signed 64-bit maximum. Missing/capped records cannot prove
message absence; `diagnostics.complete` is false for exhausted retention, forced
pipe closure, capture or persistence failure. This is separate from playback
quality and the existing observation-complete flag.

The fixed executable wrapper's RLIMIT_FSIZE remains installed, but limits on
regular-file writes do not bound parent memory capture from pipes. Parent limits
replace direct regular-file retention in opt-in mode. Encoded storage is bounded
by 2 MiB + 64 KiB, plus bounded prefixes, chunks, and at most 64 KiB of added
numeric/enum metadata. Python/runtime overhead is additional; this is no exact
RSS bound. Drain CPU remains proportional to output and can perturb scheduling.
No CPU/latency performance guarantee is claimed.

Bounded sample intervals report normalized state and selected counter deltas;
invalid/missing counters are unknown and decreases are resets. Flat input/decode
or advancing-input/flat-decode labels describe sampled counters only. They cannot
establish TCP delivery, pause provenance, source failure, or causal ordering with
client messages. Raw version/status/log data remain private. No new live acceptance
or server-wire evidence follows from this diagnostic option.
