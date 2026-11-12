# Migrating from AceStream Engine

Outpace can replace the AceStream engine for most setups that play public, unencrypted streams:
media servers, IPTV playlists, proxies such as acexy, and apps that use `/ace/getstream`. It is
not a full clone, so check the differences below before you switch.

Outpace hasn't reached 1.0. Test it with your own clients before retiring the old engine.

## What carries over

| You used | In Outpace |
| --- | --- |
| `http://host:6878/ace/getstream?id=<id>` | Same URL, with `OUTPACE_ACE_COMPAT=1`. |
| `/ace/manifest.m3u8?id=<id>` (HLS) | Same, with `OUTPACE_ACE_COMPAT=1`. |
| `format=json` sessions with `playback_url`, `stat_url`, `command_url` | Same, with compat on. |
| `/server/api?method=get_version`, `get_status`, ... | A documented subset, with compat on. See [Playing Streams](Playing-Streams#control-api). |
| `acestream://<id>` links | `outpace play acestream://<id>`, or the native `/streams/ace/<id>`. |
| HTTP port 6878 | Same port. |
| Peer port 8621 | Same port, TCP only. |
| Broadcasting | `outpace broadcast <name>` with HTTP, RTMP, pull or SRT input. |

New integrations should prefer the native URLs (`/streams/ace/<id>`, `.m3u8`). They need no
handshake and work with compat off.

## What's different

- **No premium or encrypted content**, accounts, subscriptions, web UI, remote control, search,
  channel lists or EPG. These are permanent non-goals.
- **No legacy seek or file selection**, and no VOD through the compat API. Native `/vod` exists.
- **No port 62062** and no UDP 8621 listener.
- **No access-token authentication** on the compat API. Protect it with your network.
- **Different timings.** Startup buffering, error codes and discovery differ. Outpace waits for a
  bigger buffer by default; see [HLS Output](HLS-Output) and
  [Configuration](Configuration#playback-and-startup-buffering).
- **No config-file import.** Outpace reads only `OUTPACE_*` environment variables. Its identity is
  new; never copy the old engine's identity or account database across.
- **Returned compat URLs are always `http://<Host>`.** See
  [Deployment](Deployment#behind-a-reverse-proxy) if you proxy.

## Settings map

| Concern | Outpace setting |
| --- | --- |
| HTTP API | `OUTPACE_BIND` (binary default `127.0.0.1:6878`) |
| Compat API | `OUTPACE_ACE_COMPAT=1` (default off) |
| Peer port | `OUTPACE_PEER_LISTEN` (default `0.0.0.0:8621`, TCP) |
| Seeding and inbound | `OUTPACE_ENABLE_SEEDING`, `OUTPACE_ENABLE_INBOUND` (both on by default) |
| UPnP | `OUTPACE_ENABLE_PORT_MAPPING=1` (default off) |
| Disk cache (`--live-cache-type disk`) | `OUTPACE_CACHE_TYPE=disk`, `OUTPACE_CACHE_DIR`, `OUTPACE_SEED_STORE_BYTES` |
| State directory | `OUTPACE_DATA_DIR` |
| RTMP ingest | `OUTPACE_RTMP_BIND` (default `127.0.0.1:1935`) |
| Concurrent-stream cap | `OUTPACE_MAX_SESSIONS` (default unlimited) |

If you used the old variable `OUTPACE_EXPERIMENTAL_ACE_COMPAT`, rename it to
`OUTPACE_ACE_COMPAT`. The old name is read only when the new one is unset.

## Step by step

1. **Write down the current setup:**
   - the engine version;
   - which ports are exposed and to whom;
   - which clients use which URLs;
   - where the engine's data lives (back it up).
2. **Stage Outpace next to it** on other ports, download-only, so they don't fight over 6878 or
   8621:

   ```bash
   OUTPACE_DATA_DIR=/var/lib/outpace \
   OUTPACE_BIND=127.0.0.1:16878 OUTPACE_RTMP_BIND=127.0.0.1:11935 \
   OUTPACE_ENABLE_INBOUND=0 OUTPACE_ENABLE_SEEDING=0 OUTPACE_ACE_COMPAT=1 \
     outpace serve
   ```

3. **Check the basics:**

   ```bash
   curl --fail http://127.0.0.1:16878/healthz
   curl --fail http://127.0.0.1:16878/networks
   curl --max-time 30 -o /dev/null -w '%{size_download} bytes\n' \
     "http://127.0.0.1:16878/streams/ace/${CONTENT_ID:?set a current public content id}"
   ```

   A live stream never ends, so `curl` stops at the 30-second limit (exit code 28). What matters
   is that bytes arrived.
4. **Test in your real clients**: player, media server, proxy and any app using `format=json`.
   Check that stopping works and that `/ace/stat` reports what your tooling expects.
5. **Cut over:**
   - stop the old engine;
   - move Outpace to port 6878 (and 8621, with inbound and seeding on if you want to stay a full
     participant);
   - restart;
   - recheck your clients.
6. **Keep the old engine and its backup** until you're confident. To roll back, stop Outpace and
   start the old engine with its own data. Never give Outpace's data directory to the old engine.

## Compose profiles

If you use Compose, two ready-made standalone profiles cover the common cases: a leecher gateway
(HTTP only, no inbound, no seeding) and a signed broadcast origin. They are in
[`docs/engine-migration.md`](https://github.com/jamezrin/outpace-stream/blob/main/docs/engine-migration.md#two-standalone-compose-profiles).
Use them as separate files, not as overrides of the root `compose.yml`.

## Related pages

- [Playing Streams](Playing-Streams#acestream-compatible-api)
- [Deployment](Deployment)
- [Configuration](Configuration)
