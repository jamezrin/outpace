# 46 - Stale upstream detection now keys off emitted output

Date: 2026-07-02.

Note 45 fixed `content_id` startup by reproducing the official signed catalog resolver. The
next live blocker was different: outpace could compute the same public infohash as the
official engine and start playback, but then pause while the upstream connection remained
busy with live protocol traffic.

The healthier comparison target for this pass was:

- content id: `cid2`
- resolved infohash: `c123456789abcdef0123456789abcdef01234567`

The earlier Synthetic Live Channel cid1 id still resolves correctly, but the currently reachable
upstream is stale for both outpace and the official engine, so it is a poor target for
measuring pause/stutter improvements.

## Baseline

The official 3.2.11 engine resolved `cf5327...` to the same infohash
`c123456789abcdef0123456789abcdef01234567`. Following the official playback URL for 60 s
returned:

```text
http_code=200
time_starttransfer=0.003322
size_download=57666380
```

Before this fix, outpace on the same target returned first byte quickly but then stopped
emitting contiguous MPEG-TS:

```text
http_code=200
time_starttransfer=0.363953
size_download=23795724
```

The newly surfaced `/ace/stat` downloaded counter showed the stall precisely:

```text
5s   downloaded=0
10s  downloaded=14675844
15s  downloaded=18869184
20s  downloaded=22014048
25s  downloaded=24110624
30s  downloaded=24110624
35s  downloaded=24110624
40s  downloaded=24110624
```

Daemon logs still did not report `upstream pool stale`. The reason was local: the pool loop
treated any piece chunk, unchoke, or live-window/head activity as "progress", even after
playback had already started. That meant non-contiguous chunks or head chatter could keep
refreshing the stale timer while no bytes reached the HTTP client.

## Fix

`SourceStats` now includes `downloaded`, backed by the provider's emitted-byte counter, so
`/ace/stat/...` reports actual MPEG-TS bytes emitted by the source instead of a constant 0.

The stale-deadline policy is now:

```text
made_output || (emitted == 0 && made_activity)
```

Before first output, handshake/window/chunk activity can keep startup alive. After the first
MPEG-TS bytes have been emitted, only a successful send of contiguous, TS-aligned output
refreshes the stale deadline. This is applied in both the production multi-upstream
`follow_peer_pool` coordinator and the legacy `follow_one_peer` fallback.

## Verification

Automated:

```text
cargo test -p ace-engine --lib ace_provider::tests::stale_deadline_refreshes_only_for_output_after_playback_starts
cargo test -p ace-engine --lib tests::
```

Live outpace run after the fix, same `cf5327...` target:

```text
http_code=200
time_starttransfer=0.369128
time_total=75.002118
size_download=69777140
```

This time the daemon logged the expected recovery at the old failure point:

```text
[ace] upstream pool stale — no live progress for 12s; reconnecting 3 peer(s)
[ace] ... reconnected; window min=5435781 max=5435879 -> resuming from 5435850 head=5435879
```

After reconnect, output resumed and logs advanced through later live pieces (`served 78 MiB`
in the daemon log for that bounded run).

A second run using the returned path-shaped stat URL
`/ace/stat/c123456789abcdef0123456789abcdef01234567/outpace` showed the source counter
advancing throughout a 45 s curl:

```text
0s   status=idle peers=0 downloaded=0
5s   status=dl   peers=3 downloaded=12579456
10s  status=dl   peers=4 downloaded=14676032
15s  status=dl   peers=3 downloaded=20965760
20s  status=dl   peers=3 downloaded=26207200
25s  status=dl   peers=3 downloaded=29352064
30s  status=dl   peers=3 downloaded=31448640
35s  status=dl   peers=3 downloaded=38786656
40s  status=dl   peers=3 downloaded=42979808
45s  status=dl   peers=3 downloaded=46124672
```

The HTTP client received:

```text
http_code=200
time_starttransfer=0.083484
time_total=45.001233
size_download=45282432
```

`ffprobe` identified the saved sample as 1920x1080 H.264 video plus AAC audio. It logged
expected early decode warnings from joining mid-GOP, then identified both streams.

## Remaining

This closes the specifically observed "busy upstream masks visible playback stall" bug. It
does not prove indefinite pause-free playback on every public id. The next useful check is a
longer VLC/ffmpeg soak on the `cf5327...` target with PTS-gap reporting, plus a fresh target
set if public swarm health changes.
