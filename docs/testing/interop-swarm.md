# Interop swarm test harness (`swarmtest`)

`swarmtest` (`tools/swarmtest`) stands up a real, containerized AceStream swarm and
asserts that the closed-source AceStream engine and outpace interoperate as source and
consumer nodes. It is an **on-demand, local-only** harness: it downloads and runs the
proprietary engine and drives `docker compose`, so it is **NOT part of CI**.

> **NOT in CI.** This harness needs the proprietary AceStream engine binary and a rootful
> Docker daemon. Both are unavailable to CI runners, so `swarmtest` is never executed
> there. Run it by hand on a Linux box when you want engine<->outpace interop evidence.

## What it proves

Each scenario builds a fresh swarm (dedicated bridge network, static IPs), attaches every
consumer to the same swarm descriptor, and samples them through a measurement window. A
consumer passes when, post-warmup:

- **Swarm health** — it reports `status = dl` in >=90% of samples, always sees >=1 peer,
  and its download counter rises.
- **Stream stability** — its playback delivers >=80% of the expected byte total with no
  stall (a second under 188 B/s) lasting more than 5 seconds.
- **TS contiguity** — the head of its playback body is MPEG-TS packet-aligned: some
  packet phase has a run of aligned sync bytes `0x47` (a live stream is joined
  mid-packet, so the check resyncs rather than assuming the capture starts on a boundary).

Seeding is judged **per role, at the swarm level**, not per consumer. A consumer's own
`uploaded` counter is reported (the `up` column, and `report.json`) but does not gate its
verdict: in a swarm whose source satisfies everyone, a consumer is legitimately never
pulled from, so per-peer upload proves nothing. Instead a scenario passes only if **at
least one peer uploaded** (`swarm reciprocation: yes`), proving the mesh does real
peer-to-peer relaying. Outpace's ability to seed specifically is proven by the
`outpace-source` scenario, where real engine consumers download from the outpace
broadcaster.

The verdict math lives in `tools/swarmtest/src/assertions.rs` and is unit-tested; the
docker orchestration in `scenario.rs` only samples and delegates to it.

## The three scenarios

Run serially, each a fresh swarm:

| scenario         | source            | consumers                          |
| ---------------- | ----------------- | ---------------------------------- |
| `baseline`       | real engine       | 3 real-engine                      |
| `mixed`          | real engine       | 2 real-engine + 2 outpace          |
| `outpace-source` | outpace broadcast | 2 real-engine + 2 outpace          |

A single ffmpeg container generates ~1.4 Mb/s H.264 + AAC MPEG-TS. In `baseline`/`mixed`
it serves the stream over HTTP (`-listen 1`) for the engine source node to pull; in
`outpace-source` it pushes the stream into the outpace source via chunked HTTP `PUT`.

## Network layout

A dedicated `172.28.0.0/24` bridge with static IPs:

- gateway `.1` — the **host**, where `swarmtest` runs the in-process BEP-15 UDP tracker
  (default `:7001`) and a static descriptor HTTP server (default `:7002`). Containers
  reach both via the gateway.
- media `.10`, source `.11`, engine consumers `.21+`, outpace consumers `.31+`.

Every source descriptor's trackers are patched to `udp://172.28.0.1:7001/announce`
(this leaves the swarm infohash unchanged — infohash excludes `trackers`) and served at
`http://172.28.0.1:7002/<scenario>.acelive`. Engine consumers attach with
`GET /ace/getstream?url=<that URL>&format=json`; outpace consumers stream
`GET /streams/ace/turl-<base64url(that URL)>.ts`.

## Prerequisites

- **Linux** with a **rootful Docker daemon** (Docker Engine + the Compose v2 plugin).
  Verify with `docker compose version` and `docker info`.
- **ffmpeg image** — pulled automatically (default `jrottenberg/ffmpeg:6.1-ubuntu`; its
  entrypoint is `ffmpeg`).
- **The proprietary engine** — downloaded and extracted on first run into
  `~/.cache/outpace-swarmtest/engine-3.2.11/`. The image is built from
  `tools/swarmtest/assets/engine.Dockerfile` with the extracted engine dir as the build
  context (installs the engine's own `requirements.txt`; `build-essential libffi-dev
  libxml2-dev libxslt1-dev` cover the native wheels). The outpace image is built once
  from the repo-root `Dockerfile`.

## Running

```bash
# All three scenarios (default):
cargo run -p swarmtest -- run

# One scenario:
cargo run -p swarmtest -- run --scenario baseline
cargo run -p swarmtest -- run --scenario mixed
cargo run -p swarmtest -- run --scenario outpace-source

# Tuning windows / keeping the swarm and logs after the run:
cargo run -p swarmtest -- run --scenario mixed --warmup-secs 60 --window-secs 75 --keep

# Point at an already-extracted engine dir instead of downloading:
cargo run -p swarmtest -- run --engine-dir /path/to/extracted/engine
```

### Exit codes

- `0` — every scenario passed.
- `1` — at least one scenario failed.
- `2` — preflight skip: Docker or the engine is unavailable. The harness prints an
  actionable message and does **not** panic, so wrappers can distinguish "not runnable
  here" from a real failure.

## Run-directory layout

Each invocation writes to `target/swarmtest/<UTC-timestamp>/` (gitignored):

```text
target/swarmtest/<ts>/
  report.json                 # machine-readable per-peer verdicts for every scenario
  <scenario>/
    docker-compose.yaml       # the generated compose file for that scenario
    pub/                      # engine source publish dir (bind-mounted; holds test.acelive)
    compose-logs.txt          # `docker compose logs` (on failure or --keep)
    tracker-journal.json      # every BEP-15 announce the in-process tracker recorded
```

A readable per-peer table is also printed to stdout.

## Environment knobs

The generated compose file sets these on the outpace containers:

- `OUTPACE_TRACKER_ALLOW_NON_GLOBAL=1` — the interop tracker lives on a private
  (`172.28.0.0/24`) address; outpace refuses non-global tracker addresses unless this
  opts in. Required for the harness.
- `OUTPACE_TRACKERS=udp://172.28.0.1:7001/announce` — point outpace at the in-process
  tracker on the gateway.
- The outpace source additionally gets `OUTPACE_ENABLE_INBOUND=1`, `OUTPACE_SEED_DEBUG=1`,
  `OUTPACE_PEER_LISTEN=0.0.0.0:8621`, and broadcasts with `outpace broadcast test`.

## Pinning the engine hash

`ENGINE_SHA256` in `tools/swarmtest/src/engine.rs` starts as the `UNPINNED` sentinel: the
download path runs but logs a loud warning and **skips** integrity verification. To pin
it, download the tarball and print its hash, then replace the constant:

```bash
cargo run -p swarmtest -- verify-engine-hash
# -> <sha256>  <engine-url>
```

Once `ENGINE_SHA256` is a real 64-hex string, the download path enforces it.

## Known first-run gaps to check

This harness is committed but has not been executed against the real engine end to end.
On the first live `baseline` run, confirm these before trusting a green/red verdict:

1. **Engine `/ace/stat` field names.** The normalizer expects `status`/`peers`/
   `downloaded`/`uploaded`. If a given engine build spells one differently (e.g.
   `speed_up` for upload), it will show up in `report.json` under that peer's
   `missing_fields` (absent fields are flagged, not silently read as 0) and the raw body
   is in `last_raw_stat`. Reconcile `parse_engine_stat` in `tools/swarmtest/src/peers.rs`
   if needed.
2. **Source rendezvous.** Check `tracker-journal.json` shows consumers announcing the
   swarm infohash, and that the seeded source peer (noted in `report.json` as
   "seeded source peer ...") actually let consumers connect. The seed is a safety net for
   when the source announces only to its own embedded trackers; if consumers still see 0
   peers, verify the source's peer-wire port (engine source node 7764, outpace 8621) and
   static IP match what was seeded.
3. **`--keep` with `--scenario all`.** All three scenarios share the fixed
   `172.28.0.0/24` subnet and are torn down between runs; `--keep` leaves the first
   scenario's network up, so a subsequent scenario in the same `all` run collides on the
   subnet. Use `--keep` only with a single `--scenario <name>`.

## Verifying real-engine stat fields

The engine's `/ace/stat` field names are captured verbatim into `report.json`
(`last_raw_stat`) and normalized in `tools/swarmtest/src/peers.rs`
(`status`, `peers`, `downloaded`, `uploaded`). If a given engine build spells the upload
counter differently (e.g. `speed_up`), the raw capture makes it visible so the
normalization can be adjusted.
