# Timeshift: retained live-HLS replay

Outpace can keep a bounded, in-memory history of the live HLS segments it has
already packaged. Each compatibility HLS viewer can then rewind into that history,
watch it as a finite replay, and return to live, without moving other viewers of
the same source.

This is **local** replay of media this engine has packaged since the source
started. It is not a persistent DVR, not a peer-network archive, and not a
UTC/EPG programme catch-up. History disappears when its packager is removed and
on process restart.

## Enable it

History is off by default. Set these in the **engine's** environment and restart:

```sh
OUTPACE_ACE_COMPAT=1
OUTPACE_HISTORY_SECONDS=600
OUTPACE_HISTORY_BYTES=134217728
OUTPACE_HISTORY_TOTAL_BYTES=268435456
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `OUTPACE_HISTORY_SECONDS` | `0` (disabled) | Longest retained span, max `3600`. |
| `OUTPACE_HISTORY_BYTES` | `134217728` (128 MiB) | Budget per HLS source. Minimum `188`. |
| `OUTPACE_HISTORY_TOTAL_BYTES` | `268435456` (256 MiB) | Budget shared by every HLS source of the engine. Must be at least `OUTPACE_HISTORY_BYTES`. |

Retention obeys elapsed time, accepted media duration, both byte budgets and a
hard 4,096-segment cap, whichever is reached first. Bytes usually bind first: at
8 Mbit/s, 128 MiB holds roughly two minutes, not ten. The real backing allocation
of each segment is charged, so usable payload can be a little smaller. When the
shared budget is full, a source first gives up its own oldest history.

The budgets cover retained history only. The live window, in-flight responses,
peer buffers and runtime metadata add to process memory. History shares each
segment's bytes with the live window rather than copying them, and viewers never
get their own copy: a replay holds coordinates only and cannot prevent pruning.

Active native HLS packagers and eligible managed-origin viewers collect history.
Raw `/ace/getstream` TS sessions, direct origin-HLS redirects and inactive channels
do not record anything.

## Managed-origin TS replay

With `OUTPACE_MANAGED_HLS=1` **and** a nonzero `OUTPACE_HISTORY_SECONDS`, the
[managed origin-HLS relay](native-api.md#origin-hls) also retains completed media
segments that an active viewer fetched through it. Both settings are off by default.
Recording is driven by the viewer's own requests: there is no background polling and
nothing is written to disk. The relay's short HTTP media cache is a separate mechanism.
Each viewer is charged against `OUTPACE_HISTORY_BYTES`, and all viewers and native
packagers share `OUTPACE_HISTORY_TOTAL_BYTES`.

Only a narrow subset is recorded: a media playlist (not a master) that declares
`EXT-X-INDEPENDENT-SEGMENTS`, with unique full-segment URLs and complete `200`
MPEG-TS bodies. Each retained segment needs CRC-checked PAT/PMT tables and evidence of
a video access point on the first selected video PID. That packet-level check is a
conservative heuristic, not a full decoder. Master and alternate-rendition playlists,
initialization maps, fMP4, byte ranges, gaps and audio-only media are not recorded.
Their live relay behaviour is unchanged, and status reports the reason.

Managed JSON starts include `command_url` (`/origin-hls/<token>/cmd`). It accepts
`timeshift_status`, `timeshift_seek`, `timeshift_live` and `stop` with the same
envelope, selectors and generation checks as the native contract below, and the
Android SDK's `api_version=2` hint is accepted. Status reports
`kind: "retained_origin_ts"`, `time_basis: "observed_media_duration"`, `enabled`,
`supported` and a `reason` (for example `history_disabled` or
`requires_single_independent_ts_media_playlist`). Check
`history.seekable_start_ms` and `history.end_ms` before seeking: a supported playlist
does not mean media has arrived yet.

A successful seek returns `/origin-hls/<token>/replay/<generation>/index.m3u8`, a
finite playlist with `EXT-X-ENDLIST` that serves only retained bytes and never
re-fetches the origin. Whole segments and an initial `Range: bytes=0-` probe return
`200`; other ranges and `If-Range` are refused. A failed seek leaves the current view
intact; a seek or live return invalidates older replay generations (`410`).
Unlike native playback, **the original `/origin-hls/<token>/0.m3u8` stays the live
URL**, and `timeshift_live` returns it. Pruned media also returns `410`; a stopped or
expired viewer returns `404`. A media-sequence rollback clears incompatible history.
Stop, expiry and restart discard the recording, and other viewers are unaffected.

## Native playback contract

1. Start HLS with
   `GET /ace/manifest.m3u8?content_id=0123456789abcdef0123456789abcdef01234567&format=json`
   (substitute your own current source). Keep `playback_url`, `command_url` and
   `playback_session_id`. The response's `timeshift` object reports support and
   the viewer's current generation.
2. Play `playback_url`. `command_url?method=timeshift_status` reports the
   retained history and this viewer's `generation` and `mode`.
3. Rewind with `command_url?method=timeshift_seek&back=30&generation=0`. The
   replay starts at least 30 seconds behind the newest completed retained
   segment, aligned **backwards** to a segment that opens at a video access point.
4. Check the JSON `error` field. On success, switch the player to
   `response.playback_url` (`reload_required` is `true`) and remember
   `response.view.generation`. On failure, keep playing the current URL: the
   view did not change.
5. Return to live with `command_url?method=timeshift_live&generation=<current>`
   and reload the returned URL. End the lease with `command_url?method=stop`.

`timeshift_seek` alternatively takes `position_ms=<unsigned integer>`: an
absolute position on the same media-duration timeline that `timeshift_status`
reports (`time_basis: "retained_media_duration"`). Unlike `back`, it stays on the
same media while the live head advances. `end_ms` is exclusive. Read the returned
snapshot's `start_ms` for the actual aligned start. Coordinates belong to one
source/packager lifetime; do not reuse them after a source restart.

Supply exactly one of `back` (positive seconds, at most one year) or
`position_ms`. Any duplicated query parameter is rejected and nothing runs, even
when the name is percent-encoded differently. Signed, fractional, exponent or
overflowing `position_ms`, conflicting selectors, missing or expired ranges and a
stale `generation` all return an in-band `error` and leave the current view
unchanged. `timeshift_live` accepts neither selector. Sending `generation` is
recommended: it rejects reordered client commands, and the server's
compare-and-swap also keeps concurrent commands from replacing a newer view.

These methods and the `/ace/t/...` and `/ace/tc/...` URLs are **Outpace
extensions**, not claims of compatibility with the original engine's control
protocol or its `liveseek`. They use the existing HTTP-200 JSON envelope, so
callers must check `error`. Command responses are `Cache-Control: no-store`.

The status always reports `persistent: false`, `peer_archive: false` and
`utc_seek: false`. `seekable_start_ms` is the earliest decodable entry contiguous
with the current head, or `null` when there is none. Retained bytes alone do not
guarantee a seek point: a segment forced out at the byte ceiling may begin
mid-GOP, and it is never used as a replay start. `intervals` lists retained
sequence runs, split at discontinuities and gaps.

## What a replay is

A replay is a finite, immutable HLS VOD playlist (`EXT-X-PLAYLIST-TYPE:VOD`,
`EXT-X-ENDLIST`) running from the aligned start to the head observed at seek
time. It does not grow into live playback; use `timeshift_live` at its end.

Every committed transition, seek or live, starts a new generation for that viewer
only. All older URLs return **410**, including the original `/ace/m/...` live
playlist and `/ace/c/...` segments. Returning to live gives a new URL; the
original one is never revived. Acquisition stays shared: other viewers keep their
own view and generation.

## Expiry and errors

| Situation | Result |
| --- | --- |
| URL of an older generation | 410 |
| Replay segment that has been pruned | 410 |
| Replay playlist whose start has been pruned | 410, never a silently truncated playlist |
| Replay prepared before the source was restarted | 410 |
| Stopped or expired lease, removed packager | 404 |
| Failed seek or unavailable live return | in-band `error`, view unchanged |

Missing media is never replaced by live media. A player that has already loaded a
replay playlist may keep fetching its unread segments while they are retained,
even after its consumed start has been pruned.

A paused or slow viewer can fall outside retention. Show an explicit message and
offer a new seek or a return to live. Raising the limits cannot recover media
that is already pruned, or media from before the source started.

## Proxies and players

The player has to keep the JSON control session, show the available range,
issue the seek and reload the returned URL. Proxies must pass the tokenized
`/ace/t/<id>/<token>/<generation>.m3u8` playlists and
`/ace/tc/<token>/<generation>/<seq>.ts` segments through with the generation
intact. Clients that never seek keep using `/ace/m/...` and `/ace/c/...` as before.

A classic EPG catch-up menu asks for a programme by UTC start time, which this
cannot satisfy. Do not advertise `catchup-days` or map a short local buffer onto
guide history.

## Qualification

Offline tests cover budget accounting, discontinuities, holes, mid-GOP
continuations, viewer isolation, racing and stale commands, stale URLs, source
replacement, expiry and stop. A decoder-level check is opt-in:

```sh
cargo test -p ace-engine --test media_history -- --ignored --nocapture
```

It needs `ffmpeg` and `ffprobe` (override with `FFMPEG` / `FFPROBE`), generates
its own media and uses loopback HTTP only. It rewinds, decodes the replay, checks
that it precedes live media, and returns to live with one shared acquisition.

The design is ported from the `open-ott-play/outpace-acestream` fork's timeshift
work.
