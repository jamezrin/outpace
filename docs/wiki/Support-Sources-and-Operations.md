# Support Sources and Operations

This page is for operators: keeping chosen streams running with nobody watching, and monitoring
a running daemon.

## Support sources

> **Unreleased:** support sources are in the `edge` image and builds from `main`. v0.9.0
> doesn't have them. See [Releases and Versions](Releases-and-Versions).

A **support source** is a live stream that outpace keeps open and seeds to other peers even
when no local viewer is watching. Use it to:

- run a node that helps distribute particular public channels;
- keep a channel warm so the first viewer starts instantly.

The support pin works like an invisible viewer. Real viewers and HLS clients join the same
download and share its [session slot](Configuration#limits-and-admission).

### Configure

```bash
export OUTPACE_SUPPORT_SOURCES='{"relay1":"0123456789abcdef0123456789abcdef01234567"}'
outpace serve
```

The value is a JSON object mapping a name of your choice to a live selector: a content id, an
`ih:` infohash with a verified descriptor, or a transport-file URL.

- At most 8 entries and 64 KiB.
- Names are 1–64 ASCII letters, digits, `_` or `-`. Selectors are at most 4096 bytes.
- Don't put credentials in names.
- **Requires** `OUTPACE_ENABLE_INBOUND` and `OUTPACE_ENABLE_SEEDING` to be on. outpace won't turn
  them on for you; startup fails instead.
- Not compatible with [private peer mode](Private-Peer-Mode).
- VOD, protected content, and transport URLs on private or loopback addresses are rejected.
- Sources start after all listeners are up.

### Control

| Request | Effect |
| --- | --- |
| `GET /support` | Lists each declaration: name, desired and observed state, reason, attempts, bytes emitted, retry delay. It never shows selectors. |
| `POST /support/<name>/stop` | Stops supporting that source. Real viewers keep watching. |
| `POST /support/<name>/start` | Starts it again, or restarts it after a terminal error. |

```bash
curl http://127.0.0.1:6878/support
curl -X POST http://127.0.0.1:6878/support/relay1/stop
curl -X POST http://127.0.0.1:6878/support/relay1/start
```

- POST bodies must be empty.
- A `202` means the command was accepted. Poll `GET /support` to see it take effect.
- Unknown names return `404`. You can't create declarations over HTTP.
- `DELETE /streams/ace/<id>` on a supported stream also disables its support, so it isn't
  immediately restarted. Use `start` to re-enable it.
- API start and stop aren't saved. A restart applies the declarations from the environment
  again.

### Recovery

| Event | What happens |
| --- | --- |
| Network, discovery or admission failure | Retries after 1, 2, 4, 8 and 16 seconds, then every 30 seconds. The backoff resets after 30 seconds of real progress. |
| The source ends normally | A fresh run starts. Direct TS viewers see the stream end and reconnect. HLS viewers get a closed playlist (`ENDLIST`) and must reopen the URL. |
| Invalid, unsafe or protected descriptor | Stops with an error until you `start` it again. |
| `OUTPACE_MAX_SESSIONS` reached | Waits and retries. Existing streams stay joinable. |

## Health and status endpoints

None of these start a stream. Use them for monitoring, never a playback URL.

| Endpoint | Shows | Avail. |
| --- | --- | --- |
| `GET /healthz` | `ok` while the HTTP server runs. Liveness only: it doesn't prove peers or media. | all |
| `GET /networks` | Provider networks, normally `{"networks":["ace"]}`. | all |
| `GET /streams` | Active streams with their viewer count and metadata. | all |
| `GET /streams/ace/<id>/status` | One stream: peers, bitrate, server buffer, upload, recovery counters, session occupancy. `404` when not running. | all |
| `GET /origin-hls/status` | Managed-relay state: `{"enabled":...,"active_sessions":...}`. | edge |
| `GET /support` | Support-source states. | edge |
| `GET /network-policy` | Public or private peer mode and allowlist size. | edge |
| `GET /debug/memstats` | Allocator counters (live heap, resident) on 64-bit Linux and macOS builds. | all |
| `GET /server/api?method=get_status` | `dl` or `idle`, plus `active_sessions`, `occupied_sessions` and `max_sessions`. Needs compat on. | all |
| `GET /server/api?method=get_version` | outpace version. Needs compat on. | all |

The provided Compose file health-checks `/healthz` every 10 seconds.

### Session occupancy

With `OUTPACE_MAX_SESSIONS` set, stream status and `get_status` report `occupied_sessions` and
`max_sessions` (`null` when unlimited). Occupancy includes streams that are still starting, and
ones still cleaning up. A slot can stay busy briefly after a stream stops. When the cap is
reached, the log shows `[manager] session capacity reached: occupied=… max=…`, and new
streams get `503` with `Retry-After: 1`.

### Is the daemon idle?

Before a planned restart, check that `/streams` is empty, `/origin-hls/status` shows
`active_sessions: 0` and no broadcast is being fed. Relay viewers don't appear in `/streams`.

## Logs

outpace logs to standard error, one event per line, prefixed with the UTC time of day and a
`[tag]`. See [Troubleshooting](Troubleshooting#reading-the-logs) for what the tags mean. With
Compose the logs are rotated (3 × 10 MB).

## Related pages

- [Deployment](Deployment)
- [Troubleshooting](Troubleshooting)
- [Seeding and Peers](Seeding-and-Peers)
