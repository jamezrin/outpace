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

## Capture or analyze existing clients

For clients you start yourself on separate ports:

```sh
python3 tools/abcompare/ab_capture.py "$ACE_CONTENT_ID" 180 /tmp/new-ab-run \
  engine=http://127.0.0.1:6878 outpace=nativecid:http://127.0.0.1:16879
python3 tools/abcompare/ab_analyze.py /tmp/new-ab-run 180 engine outpace
```

Engine specifications are `name=BASE` for `/ace/getstream?id=…&format=json`,
`name=nativecid:BASE` for `/streams/ace/cid:…`, or `name=native:BASE` for
`/streams/ace/…` using an **infohash**, not a content id. Outpace compat routes
require `OUTPACE_EXPERIMENTAL_ACE_COMPAT=1`; the runner uses native content-id
routing so it needs no compat flag. Names must contain only letters, digits,
underscores and hyphens, and must be unique. Playback follows up to five relative
or absolute redirects, including the original engine's 302 to `/content/…` and
HTTP(S) redirects. Capture retries EOF/errors after one second; these reconnects
remain visible in the events log. Playback-url discovery retries every two seconds.
All workers use one monotonic clock and a fixed global capture deadline.

Files are `<name>.ts`, `<name>.arrivals.csv` (`t,nbytes,offset`),
`<name>.events.txt`, `<name>.stats.jsonl` (five-second polls) and `capture.json`.
The analyzer prints a concise per-client report and writes schema-versioned
`summary.json`. Missing media, missing PCR/RAI and no overlapping window are
reported as null metrics, not success. Successful capture/analysis exit status
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

A bare 40-hex identifier on outpace's native route is an **infohash** (#165); use
`nativecid:` or compat for content ids. Default outpace prebuffer/discovery can
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
