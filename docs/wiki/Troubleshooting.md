# Troubleshooting

Start with the quick checks, then go to the section that matches what you see.

## Quick checks

```bash
curl http://127.0.0.1:6878/healthz             # is the daemon up?
curl http://127.0.0.1:6878/streams             # what is running?
curl http://127.0.0.1:6878/streams/ace/<id>/status   # peers, bitrate, buffer, recovery
outpace --version                              # what you are running
```

- Try a different stream that you know is live. A dead channel looks exactly like a network
  problem.
- Turn off full-tunnel VPNs, Cloudflare WARP in particular, and test again.
- Look at the logs: `docker compose logs -f`, `docker logs -f outpace` or
  `journalctl -u outpace -f`.

## The daemon won't start

outpace validates all settings before it opens any listener, and names the problem:

| Message contains | Fix |
| --- | --- |
| `invalid OUTPACE_X: "..."; expected one of: 1, true, 0, false` | Use exactly `1`, `true`, `0` or `false`. |
| `... must be >= ...`, `... must be <= ...` | A value is out of range or contradicts another one. See [Configuration](Configuration). |
| `OUTPACE_CACHE_DIR ... is not a dedicated Outpace cache` | The disk-cache directory isn't empty and has no `.outpace-cache` marker. Point it at an empty directory, or create the marker only if everything in it may be deleted. |
| `OUTPACE_CACHE_DIR must not equal or contain OUTPACE_DATA_DIR` | Use a cache directory inside the data directory, or somewhere separate. |
| `exceeds this target's conservative ...-byte memory-cache limit` | Lower `OUTPACE_SEED_STORE_BYTES`, or use `OUTPACE_CACHE_TYPE=disk`. Common on 32-bit. |
| `broadcast_store_too_small` | `OUTPACE_SEED_STORE_BYTES` must hold at least one full broadcast piece. |
| `invalid_broadcast_options`, `invalid_source_config` | Fix the JSON in `OUTPACE_BROADCAST_OPTIONS` or `OUTPACE_BROADCAST_SOURCES`. |
| `private_mode_conflict_*`, `local_descriptors_require_private_mode` | See [Private Peer Mode](Private-Peer-Mode#settings-that-conflict). |
| `srt_feature_unavailable` | `OUTPACE_SRT_BIND` is set, but this binary was built without SRT. |
| `srt_unsafe_passphrase_file` | Fix the key file's owner and permissions (`chmod 600`, owned by the daemon user). |
| Address already in use | Another process (often the old AceStream engine) holds 6878, 1935 or 8621. |

With Compose, remember that most settings must go in `compose.override.yml`. If a change in
`.env` seems to do nothing, that's usually why; see
[Deployment](Deployment#passing-other-settings).

## Slow startup

A cold start has three stages:

1. **Resolving** the content id through the catalog.
2. **Discovering** peers through trackers and the DHT, then connecting.
3. **Prebuffering**: collecting media before the first byte goes out, up to 30 seconds of media
   or 15 seconds after the first byte, whichever comes first. HLS also waits for six segments.

20–60 seconds in total is normal with the defaults. To start faster, at the cost of more
stutter on weak swarms:

```bash
OUTPACE_PREBUFFER_MS=5000          # direct TS and HLS
OUTPACE_HLS_STARTUP_SEGMENTS=2     # HLS
```

The logs show which stage is slow:

```text
[ace] discovery stage=resolved resolve_ms=...
[ace] discovery stage=first-candidates after_resolution_ms=... count=...
[ace] discovery stage=tcp-connected
[ace] discovery stage=first-source-output
[prebuffer] startup buffer released: reason=TargetDuration duration_ms=... queued_bytes=...
```

The `reason` on the `[prebuffer]` line tells you why playback started:

| Reason | Meaning |
| --- | --- |
| `TargetDuration` | The full target was reached. |
| `Deadline` | The 15-second limit expired; playback started with what was there. |
| `ByteLimit` | The 128 MiB cap was hit. |
| `EndOfStream` | The source ended during startup. |
| `Disabled` | Prebuffering is off. |

## HLS returns 503 during startup

The playlist request waited `OUTPACE_HLS_STARTUP_TIMEOUT_MS` (45 seconds) and fewer than
`OUTPACE_HLS_STARTUP_SEGMENTS` segments were ready. The response carries `Retry-After: 1`, and
most players retry on their own.

- If it eventually plays, the stream is just slow to start. Lower the startup segment count or
  raise the timeout.
- If it never plays, check the stream's `/status`. `peers: 0` means a discovery problem;
  `bitrate` near 0 means the peers aren't delivering.
- A `503` that never goes away, after playback had worked, can mean
  [audio normalisation](HLS-Output#audio-normalisation) failed. Check `normalization` in
  `/status`. Stop the stream and start it again.
- On v0.9.0, an HLS stream whose source **ended** also answers `503` for ever. Newer builds
  return a closed playlist instead.

## Stalls and gaps during playback

Check the stream's `/status` while it stalls:

- `peers` dropping, or `recovery.upstream_reconnects` and `recovery.stale_teardowns` climbing:
  the swarm is losing peers. outpace reconnects on its own.
- `recovery.skipped_evicted_gaps` climbing: peers no longer had the pieces outpace needed, so
  it skipped ahead. That shows up as a jump or glitch in the picture.
- `bitrate` far below `metadata.bitrate`: not enough upload capacity in the swarm.

Matching log lines:

```text
[ace] re-requesting 3 piece(s) outstanding > 1.5s (from ...)
[ace] upstream pool stale — no live progress for 12s; reconnecting 4 peer(s)
[ace] rebuilding upstream pool: reason=PoolStale ...
[ace] next needed piece evicted from all upstream windows; skipping ahead to ...
[ace] cursor fell too far behind live edge ...; re-syncing forward to piece ...
```

What helps:

- A larger startup buffer (the default 30 seconds is already generous) gives more slack.
- Being reachable (an open port 8621) lets more peers connect to you.
- Avoid VPNs.
- Leave the [live recovery](Configuration#live-recovery-advanced) settings alone. Raising
  `OUTPACE_REQUEST_TIMEOUT_MS` makes stalls longer, not shorter.

Also check the player: a client that reads slower than real time eventually falls behind the
server's window and gets a discontinuity.

## No peers, or discovery fails

`status` shows `peers: 0` for a long time, or the request fails:

- **Is the stream alive?** Try another one. Dead channels never find peers.
- **Outbound UDP** must work for the DHT and trackers (replies to outbound UDP must come back).
  Some firewalls and VPNs block this.
- **Outbound TCP** to arbitrary ports must work to reach peers.
- `422` with a reason: the id couldn't be resolved. For `ih:` infohashes, outpace needs a
  verified descriptor; try the content id instead.
- Compat `502 stream start failed`: resolution or discovery failed. Compat `422 stream
  metadata unavailable`: no descriptor was found for an infohash or magnet. Neither proves the
  content is gone for good.
- A content id of a broadcast made by another outpace daemon only resolves with
  `OUTPACE_BEP9_CONTENT_ID_FALLBACK=1`. See [Broadcasting](Broadcasting#watching-a-broadcast).

`[dht] walk done: ... peers=N` tells you how many peers the DHT found.

## No inbound peers

The log keeps saying:

```text
[reachability] externally reachable: no (0 inbound peer(s) ...)
[reachability] HINT: after 10 minutes, no inbound peers or yourip observations; check advertised peer TCP port 8621 ...
```

Work through these:

1. Is inbound serving on (`OUTPACE_ENABLE_INBOUND` not `0`)? Did you publish `8621:8621/tcp`?
2. Does the router forward **TCP** 8621 to this machine's port 8621, with the same port on both
   sides?
3. Does the host firewall allow inbound TCP 8621?
4. Test from **outside** your network:
   `timeout 8 bash -c 'echo > /dev/tcp/<public-ip>/8621' && echo open`.
5. A `WARNING ... likely double-NAT / CGNAT` line means your ISP's NAT is in the way.
6. VPN or WARP? See [NAT and Port Mapping](NAT-and-Port-Mapping#vpns-and-cloudflare-warp).

Playback doesn't need inbound peers. You can also run download-only
(`OUTPACE_ENABLE_INBOUND=0`).

## Too many streams

`503` with `Retry-After: 1` when opening a new stream, and
`[manager] session capacity reached` in the log: you hit `OUTPACE_MAX_SESSIONS`. Raise or unset
it, or stop unused streams. A slot frees up only after the stream's cleanup has finished.

## Shutdown takes a long time

On v0.9.0, a connected viewer keeps the daemon running until the stop timeout. Newer builds end
viewer connections first. See [Deployment](Deployment#graceful-shutdown).

## Reading the logs

Each line looks like `HH:MM:SS.mmm [tag] message`, where the time is UTC. The tag says which part
of outpace logged it:

| Tag | Area |
| --- | --- |
| `[ace]` | Resolving content, discovering and managing peers, downloading a stream. Most playback lines. |
| `[ace-compat]` | The AceStream-compatible API. `playback start failed: error=<category> status=<code>`. |
| `[prebuffer]` | Startup buffer release, with reason and duration. |
| `[mpegts]` | Repairing a damaged or misaligned TS byte stream (`transport resync`). Occasional lines are harmless. |
| `[hls]` | HLS packaging, for example a segment ceiling too small. |
| `[manager]` | Stream lifecycle: capacity reached, idle cleanup. |
| `[session]` | Problems cleaning up one source. |
| `[dht]` | DHT peer search results. |
| `[reachability]` | Whether inbound peers can reach you, every two minutes. |
| `[portmap]` | UPnP/NAT-PMP mapping, renewal and removal. |
| `[seed]`, `[seed-listener]` | Inbound seeding: accepted connections, cleanup of idle seeding data. |
| `[seed-session]` | Per-message seeding detail. Only shown with `OUTPACE_SEED_DEBUG` set. |
| `[cache]` | Disk cache errors. |
| `[broadcast]` | Broadcasts: reloads, tracker warnings, cleanup. |
| `[rtmp]`, `[srt]` | Ingest listeners. |
| `[support]` | Support sources. |
| `[runtime]` | Daemon start and shutdown. |

Lines without a tag are messages for the person at the terminal, such as
`outpace: listening on ...` at startup.

`OUTPACE_SEED_DEBUG=1` adds a log line for every seeding message. Turn it on only briefly while
debugging inbound peers.

## Reporting a bug

Open an issue at <https://github.com/jamezrin/outpace-stream/issues> with:

- `outpace --version`, and whether you use a release, `edge` (with its `sha-` tag) or your own
  build;
- the platform (OS, architecture; container or bare metal);
- the non-default `OUTPACE_*` settings you use;
- what you did, what you expected and what happened: the HTTP status, the player's error, and
  a `/status` snapshot;
- whether the same stream plays in another engine at the same time, if you can check;
- a log excerpt around the problem.

**Don't include stream identifiers.** Before you paste anything:

- Replace content ids and infohashes (40-character hex strings) with a placeholder such as
  `0123456789abcdef0123456789abcdef01234567`. They appear in URLs, `/streams` output, `/status`
  and some `[ace]` log lines.
- Remove stream and channel names (the `title` and `Icy-Name` values).
- Remove broadcast source URLs and any tokens, SRT passphrases, your public IP and peer IP
  addresses.
- Don't attach captures or recordings of the media.

For example, to mask hex ids in a log before sharing it:

```bash
sed -E 's/[0-9a-fA-F]{40}/<id>/g' outpace.log > outpace-redacted.log
```

## FAQ

**Can it play premium or encrypted channels?** No, and it never will.

**Does it need the original AceStream engine installed?** No. outpace doesn't use or download
any part of it.

**Why does the stream start later than with the original engine?** outpace buffers more by
default, for smoother playback. See [Slow startup](#slow-startup).

**Does it need FFmpeg?** Only for [audio normalisation](HLS-Output#audio-normalisation).

**Which ports do I open?** Only TCP 8621 inbound, for peers. Keep 6878 and 1935 private.

**Can I rewind live TV?** For a few minutes, with [Timeshift](Timeshift) (newer than v0.9.0).

**Is there a web UI?** No. Use your player, media server or the HTTP API.

**Why did my old playlist of bare infohashes stop working?** Bare ids on `/streams/ace/` are now
content ids. Prefix infohashes with `ih:`.
