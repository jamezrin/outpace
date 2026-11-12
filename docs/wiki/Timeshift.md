# Timeshift

> **Unreleased:** timeshift is in the `edge` image and builds from `main`. v0.9.0 doesn't have
> it. See [Releases and Versions](Releases-and-Versions).

Timeshift lets a viewer rewind a live HLS stream by a few minutes, watch from there, and jump
back to live. outpace keeps recently packaged HLS segments in memory, and each viewer can seek
into them independently.

It is a **short local rewind buffer**:

- **Memory only.** It disappears when the stream stops or the daemon restarts. Nothing is
  written to disk.
- **Only since the stream started here.** You can't rewind past the moment this daemon began
  packaging the stream.
- **Not a DVR or archive.** There is no recording, no peer-network archive, and no catch-up by
  UTC time or EPG programme.

## What can be rewound

| Viewer | Can rewind? |
| --- | --- |
| Compatibility HLS (`/ace/manifest.m3u8`) with `OUTPACE_ACE_COMPAT=1` | Yes, through `/ace/cmd` |
| [Managed HLS origin](Managed-HLS-Origins) viewers with `OUTPACE_MANAGED_HLS=1` | Yes, through the viewer's `command_url`, for qualifying streams |
| Native HLS (`/streams/ace/<id>.m3u8`) | No seek control. The source still records history if a compat viewer could use it. |
| Direct MPEG-TS (`/streams/...`, `/ace/getstream`) | No. Raw TS sessions record nothing. |
| Origin HLS in the default direct-redirect mode | No. outpace never sees that media. |

## Turn it on

Set these in the daemon's environment, then restart it:

```bash
export OUTPACE_ACE_COMPAT=1
export OUTPACE_HISTORY_SECONDS=600
export OUTPACE_HISTORY_BYTES=134217728
export OUTPACE_HISTORY_TOTAL_BYTES=268435456
outpace serve
```

| Setting | Default | Meaning |
| --- | --- | --- |
| `OUTPACE_HISTORY_SECONDS` | `0` (off) | The longest span to keep, up to `3600`. Any nonzero value turns timeshift on. |
| `OUTPACE_HISTORY_BYTES` | 128 MiB | Memory budget **per source** (and per managed-origin viewer). Minimum 188 bytes. |
| `OUTPACE_HISTORY_TOTAL_BYTES` | 256 MiB | Budget **shared** by all sources and managed-origin viewers. Must be at least `OUTPACE_HISTORY_BYTES`. |

With Docker Compose, these must go in `compose.override.yml`, not just `.env`; see
[Deployment](Deployment#passing-other-settings).

## Sizing memory

History stops growing at whichever limit is reached first:

- the number of seconds;
- the per-source byte budget;
- the shared byte budget;
- 4096 segments.

**Bytes usually win.** A rough guide:

| Stream bitrate | 128 MiB holds about | For 10 minutes you need about |
| --- | --- | --- |
| 2 Mbit/s | 8.5 minutes | 150 MiB |
| 5 Mbit/s | 3.5 minutes | 375 MiB |
| 8 Mbit/s | 2 minutes | 600 MiB |
| 15 Mbit/s | 1 minute | 1.1 GiB |

Work it out as **bitrate (Mbit/s) × seconds ÷ 8 = MB**, then add a little because the real
allocation of each segment is charged. For example, to give an 8 Mbit/s channel a full 10
minutes:

```bash
export OUTPACE_HISTORY_SECONDS=600
export OUTPACE_HISTORY_BYTES=671088640        # 640 MiB per source
export OUTPACE_HISTORY_TOTAL_BYTES=1342177280 # 1.25 GiB for two such channels
```

Then start (or restart) outpace from the same shell. With `docker run`, pass each setting
as `-e NAME=value`; with Compose, see [Deployment](Deployment#passing-other-settings).

Keep in mind:

- When the shared budget is full, a source gives up its **own** oldest history first.
- The budgets cover history only. The live window, buffers and everything else come on top, so
  leave headroom in container memory limits.
- History shares segment memory with the live window. Viewers never get their own copy, so 50
  viewers rewinding the same channel cost no more than one.
- On 32-bit systems each budget is capped at about 512 MiB.

## Seeking (compatibility HLS)

1. **Start the session in JSON mode** and keep the URLs it returns:

   ```bash
   curl "http://127.0.0.1:6878/ace/manifest.m3u8?content_id=0123456789abcdef0123456789abcdef01234567&format=json"
   ```

   Play `response.playback_url`. Keep `response.command_url`.

2. **Check what's available:**

   ```bash
   curl "<command_url>?method=timeshift_status"
   ```

   This reports the retained range (`history.seekable_start_ms`, `history.end_ms`) and this
   viewer's `generation` and `mode` (`live` or `replay`). `seekable_start_ms` is `null` when
   there is nothing to seek to yet.

3. **Rewind**, either relative to live or to an absolute position:

   ```bash
   curl "<command_url>?method=timeshift_seek&back=30&generation=0"
   curl "<command_url>?method=timeshift_seek&position_ms=120000&generation=0"
   ```

   - `back=<seconds>` starts at least that far behind the newest segment.
   - `position_ms=<ms>` is a point on the timeline that `timeshift_status` reports. It counts
     accumulated media time since the stream started here; it isn't wall-clock time. Unlike
     `back`, it stays on the same piece of media while live moves on.
   - Pass exactly one of the two.
   - The start snaps **backwards** to the nearest segment that begins on a keyframe. Read
     `response.view.snapshot.start_ms` for the actual start.

4. **Check `error`, then reload.** On success the response has a new `playback_url` and
   `reload_required: true`. Load that URL in the player and remember
   `response.view.generation`. On failure the `error` field explains why, and the current
   view is unchanged, so keep playing.

5. **Return to live:**

   ```bash
   curl "<command_url>?method=timeshift_live&generation=1"
   ```

   Load the returned `playback_url`. It's a **new** live URL.

6. **End the session** with `<command_url>?method=stop`.

Passing `generation` is optional but recommended. It makes the server reject a command based on
an outdated view, for example two seeks sent out of order.

### Why old URLs stop working

Each successful seek or return to live starts a new **generation** for that viewer. Every URL
from an older generation then answers **`410 Gone`**, including the original `/ace/m/...`
playlist and `/ace/c/...` segments. Returning to live gives a new URL; the old live URL never
comes back.

This is deliberate. A player that kept polling an old URL could otherwise silently receive
different media. **Your player or proxy must switch to the returned URL each time.** A client
that never seeks keeps using its original URLs as before.

### What a replay is

A replay is a finite HLS VOD playlist (`EXT-X-PLAYLIST-TYPE:VOD`, `EXT-X-ENDLIST`) from the
chosen start to wherever live was at the moment you seeked. It doesn't grow. When it ends, call
`timeshift_live` (or seek again).

### Viewers are independent

Seeking moves only the viewer that asked. Other viewers of the same channel, on live or at their
own replay positions, are unaffected, and all of them still share one download.

### When media expires

History keeps rolling forward while someone watches a replay, so a paused or slow viewer can
fall behind what's retained:

| Situation | Result |
| --- | --- |
| URL from an older generation | `410` |
| Replay segment that has been pruned | `410` |
| Replay playlist whose start has been pruned | `410` (never silently shortened) |
| Replay prepared before the stream restarted | `410` |
| Stopped or expired lease, stream gone | `404` |
| Seek target not retained, or nothing decodable yet | In-band `error`, view unchanged |

Missing media is **never** replaced with live media. When a player gets `410`, show a message
and offer a new seek or a return to live. Raising the limits afterwards can't bring back media
that has already been pruned.

## Managed-origin replay

With **both** `OUTPACE_MANAGED_HLS=1` and a nonzero `OUTPACE_HISTORY_SECONDS`, the
[managed HLS relay](Managed-HLS-Origins) also keeps the segments a viewer fetched through it.

- **Recorded:** a media playlist (not a master playlist) that declares
  `EXT-X-INDEPENDENT-SEGMENTS`, with unique segment URLs and complete MPEG-TS segments that start
  with valid tables and a video keyframe.
- **Not recorded:** master or alternate-rendition playlists, fMP4 or init maps, byte ranges,
  gaps and audio-only streams. These still play live as usual, and status says why they can't
  be replayed (for example `requires_single_independent_ts_media_playlist`).
- Recording is driven only by what the viewer actually fetches. There is no background polling.
- Each viewer counts against `OUTPACE_HISTORY_BYTES`, and all of them share
  `OUTPACE_HISTORY_TOTAL_BYTES` with native sources.

The JSON start response includes `command_url` (`/origin-hls/<token>/cmd`). It takes the same
`timeshift_status`, `timeshift_seek`, `timeshift_live` and `stop` methods. The differences from
compatibility HLS:

- Status reports `kind: "retained_origin_ts"` and `time_basis: "observed_media_duration"`.
- A seek returns `/origin-hls/<token>/replay/<generation>/index.m3u8`, served only from retained
  bytes. It never re-fetches the origin, so it still works after the origin has deleted the
  segment.
- **The original `/origin-hls/<token>/0.m3u8` stays the live URL.** `timeshift_live` returns it
  again. Only replay URLs from older generations return `410`.
- Check `history.seekable_start_ms` before seeking. A supported playlist doesn't mean any media
  has been retained yet.

## Players, proxies and EPG

- Players need to keep the JSON control session, show the available range, send the seek and
  reload the returned URL. Most generic players can't do this alone; it takes an app or
  middleware that knows these commands.
- Proxies must pass through `/ace/t/<id>/<token>/<generation>.m3u8` and
  `/ace/tc/<token>/<generation>/<seq>.ts` with the generation intact.
- Don't advertise `catchup-days` or map this onto EPG catch-up. EPG catch-up asks for a
  programme by UTC time, which this buffer can't provide.
- These `timeshift_*` methods are outpace extensions. They aren't the original engine's
  `liveseek`.

## Related pages

- [HLS Output](HLS-Output)
- [Managed HLS Origins](Managed-HLS-Origins)
- [Configuration](Configuration#timeshift)
