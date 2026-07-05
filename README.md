# outpace

outpace is a from-scratch Rust implementation of the public Acestream P2P live
streaming path. It joins Acestream swarms, pulls MPEG-TS media, and exposes the
result through a native daemon API, HLS, and CLI commands that work with players
such as VLC and downstream tools such as Jellyfin or dispatcharr.

The project does not use closed-source engine blobs at runtime. Reverse-engineering
fixtures, protocol captures, and reference binaries are kept under git-ignored
`re/` and `references/` directories when present on a development machine.

## Status

The live byte path is built and proven against real public swarms:

- protocol recovery and interop vectors are in place;
- the `ace-wire`, `ace-tracker`, `ace-peer`, `ace-swarm`, `ace-media`, and
  `ace-engine` crates are in the workspace;
- the daemon can discover peers, connect, download live media, and serve it over
  the native API;
- reciprocal upload, inbound seeding plumbing, signed broadcast origination,
  HTTP raw ingest, RTMP ingest, content-id metadata serving, and the three-command
  CLI are implemented;
- live-network tests remain ignored by default because they need the public swarm
  and a suitable network.

Run the normal local gate with:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

## Quick Start

Run the daemon:

```bash
cargo run -p ace-engine --bin outpace -- serve
```

With no subcommand, `outpace` defaults to `serve`.

Play a known Acestream link to stdout:

```bash
cargo run -p ace-engine --bin outpace -- play acestream://<content-id> > live.ts
```

Start a named broadcast:

```bash
cargo run -p ace-engine --bin outpace -- broadcast my-channel
```

The broadcast command prints:

- a raw MPEG-TS HTTP ingest URL;
- an RTMP ingest URL at `rtmp://<host>:<rtmp-port>/live/<name>`;
- an `acestream://<content_id>` link backed by BEP-9 `ut_metadata` serving.

## Runtime Surfaces

Native API:

- `GET /healthz`
- `GET /streams`
- `GET /streams/ace/<id>.ts`
- HLS and playlist routes backed by the native stream session layer
- `PUT /broadcast/<name>`
- `GET /broadcast/<name>`

Legacy Acestream-compatible routes such as `/ace/*` and `/server/api` are
experimental and disabled by default. Enable them only when needed:

```bash
OUTPACE_EXPERIMENTAL_ACE_COMPAT=1 cargo run -p ace-engine --bin outpace -- serve
```

On that compatibility surface, `/ace/stat` fields live under `.response`, and
`acestream://` ids should be passed as `content_id=`, not `infohash=`.

## Configuration

Environment variables parsed by the daemon include:

- `OUTPACE_BIND` - HTTP bind address, default `127.0.0.1:6878`.
- `OUTPACE_RTMP_BIND` - RTMP ingest bind address, default `127.0.0.1:1935`.
- `OUTPACE_DATA_DIR` - persistent identity/cache root, default platform data dir
  plus `outpace`.
- `OUTPACE_PEER_LISTEN` - inbound peer listener bind, default `0.0.0.0:8621`.
- `OUTPACE_SEED_STORE_BYTES` - byte budget for retained piece data.
- `OUTPACE_PREFETCH_PIECES` - pieces behind the live edge to start at, default `8`.
- `OUTPACE_SESSION_BUFFER` - per-client fan-out channel depth, default `256`;
  must be at least `1`.
- `OUTPACE_MAX_UNCHOKED` - accepted config knob for future multi-peer S2 policy.
- `OUTPACE_MAX_INBOUND` - inbound peer connection limit.
- `OUTPACE_ENABLE_SEEDING` - reciprocal upload gate over outbound leech connections
  (answering peers' chunk requests). Self-announce is gated on `OUTPACE_ENABLE_INBOUND`.
- `OUTPACE_ENABLE_INBOUND` - inbound peer listener gate; on by default (matching
  the Acestream engine's full P2P participation). Set `0` for a pure leecher.
- `OUTPACE_EXPERIMENTAL_ACE_COMPAT` - enables legacy compatibility routes.
- `OUTPACE_ACE_PEERS` - comma-separated bootstrap peer list for the live path.

Disk-backed cache configuration is tracked separately in issue #5. The documented
`OUTPACE_CACHE_TYPE` and `OUTPACE_CACHE_DIR` knobs are not production code yet.

## Project Docs

Start with these durable docs instead of a session resume file:

- `docs/superpowers/specs/2026-06-28-acestream-engine-reimplementation-design.md`
  - original architecture and scope.
- `docs/protocol/wire-protocol.md` - consolidated peer-wire protocol notes.
- `docs/protocol/transport-file.md` - transport container and infohash math.
- `docs/protocol/notes/` - chronological reverse-engineering and live validation
  notes.
- `docs/superpowers/plans/` and `docs/superpowers/specs/` - implementation plans
  and design specs retained as historical project records.

The issue tracker is the canonical backlog:

- #1 live robustness validation;
- #2 PEX advertised-window peer ranking;
- #3 broadcast persistence and ingest-resume continuity;
- #4 inbound seeding lifecycle and policy;
- #5 disk-backed piece cache;
- #6 native CLI/API polish;
- #7 SRT ingest;
- #8 removal of the old resume workflow.

## Operational Notes

- Cloudflare WARP breaks P2P swarm testing by interfering with UDP/inbound paths.
  Keep it off for live swarm validation. The Spain network itself has worked once
  WARP is disabled.
- A known-good live content id for smoke tests is `cid1` in the gitignored
  `acestream-ids.txt` registry (see AGENTS.md); resolve it to the current
  infohash when needed because live infohashes rotate.
- `status=prebuf, peers=0` forever usually means a dead live channel. The old promo
  transport id `685e...6067` is dead and should not be used as proof.
- The official closed engine sandbox, when present, is started with:

```bash
docker compose -f re/sandbox/docker-compose.yml up -d acestream
```

It exposes the engine HTTP API on `127.0.0.1:6878` and is intended for controlled
capture/interop work, not for runtime use by outpace.

## Scope Boundaries

outpace targets public, unencrypted Acestream swarms. Premium/encrypted content,
Android/player apps, and a full clone of the closed engine HTTP API are out of
scope unless a specific issue narrows that work.
