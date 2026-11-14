# Support Sources and Operations

This page is for operators: keeping chosen streams running with nobody watching, and monitoring
a running daemon.

## Support sources

> **From v1.0.0:** Support sources. See [Releases and Versions](Releases-and-Versions)
> for publication status.

A **support source** is a live stream that Outpace keeps open and seeds to other peers even
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
- **Requires** `OUTPACE_ENABLE_INBOUND` and `OUTPACE_ENABLE_SEEDING` to be on. Outpace won't turn
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
| `GET /streams/ace/<id>/status` | One stream: start `phase` and `error`, peers, bitrate, server buffer, upload, recovery counters, session occupancy. While starting, or shortly after a failed start, only `phase`, `error`, `clients` and `peers`. `404` when neither running nor recently started. | all |
| `GET /origin-hls/status` | Managed-relay state: `{"enabled":...,"active_sessions":...}`. | 1.0.0 |
| `GET /support` | Support-source states. | 1.0.0 |
| `GET /network-policy` | Public or private peer mode and allowlist size. | 1.0.0 |
| `GET /debug/memstats` | Allocator counters (live heap, resident) on 64-bit Linux and macOS builds. | all |
| `GET /metrics` | Prometheus text metrics. Only when `OUTPACE_METRICS=1`, otherwise `404`. | 1.0.0 |
| `GET /server/api?method=get_status` | `dl` or `idle`, plus `active_sessions`, `occupied_sessions` and `max_sessions`. Needs compat on. | all |
| `GET /server/api?method=get_version` | Outpace version. Needs compat on. | all |

The provided Compose file health-checks `/healthz` every 10 seconds.

### Prometheus metrics

Set `OUTPACE_METRICS=1` to serve `GET /metrics` in the Prometheus text format on the normal HTTP
bind. It is off by default. The endpoint has no authentication, so keep `OUTPACE_BIND` on a
trusted network or put it behind your reverse proxy.

At most two scrapes collect metrics at the same time, and each has two seconds to finish. A scrape
over either limit gets `503 Service Unavailable` with `Retry-After: 1` and an empty body, which
Prometheus records as a failed scrape (`up` is `0`) rather than partial values. A normal
`scrape_interval` from one or two Prometheus servers never hits these limits.

```yaml
scrape_configs:
  - job_name: outpace
    static_configs:
      - targets: ["outpace:6878"]
```

Labels never contain content ids, infohashes, stream or broadcast names. Per-stream values are
summed for each provider network, and HTTP series use the route template (for example
`/streams/{network}/{id}/status`), not the requested path.

| Metric | Type | Meaning |
| --- | --- | --- |
| `outpace_build_info{version}` | gauge | Always `1`, labelled with the running version. |
| `outpace_uptime_seconds` | gauge | Seconds since the HTTP server started. |
| `outpace_allocator_bytes{kind}` | gauge | jemalloc `allocated`, `active`, `resident`, `retained`, `mapped`. Absent on builds without jemalloc. |
| `outpace_admission_occupied` | gauge | Admitted sources, including pending opens. |
| `outpace_admission_limit` | gauge | `OUTPACE_MAX_SESSIONS`. Absent when unlimited. |
| `outpace_sessions{network}` | gauge | Active stream sessions. |
| `outpace_subscribers{network}` | gauge | Clients attached to those sessions. |
| `outpace_peers{network}` | gauge | Connected swarm peers. |
| `outpace_peers_served{network}` | gauge | Distinct peers served at least one chunk. |
| `outpace_active_upstreams{network}` | gauge | Peers currently admitted as download upstreams. |
| `outpace_download_bytes_per_second{network}` | gauge | Download rate. |
| `outpace_upload_bytes_per_second{network}` | gauge | Upload rate. |
| `outpace_session_downloaded_bytes{network}` | gauge | Bytes downloaded by active sessions. |
| `outpace_session_uploaded_bytes{network}` | gauge | Bytes uploaded by active sessions. |
| `outpace_session_upstream_reconnects{network}` | gauge | Upstream reconnects in active sessions. |
| `outpace_session_stale_teardowns{network}` | gauge | Stale upstream teardowns in active sessions. |
| `outpace_session_skipped_evicted_gaps{network}` | gauge | Evicted gaps skipped in active sessions. |
| `outpace_session_signature_rejects{network}` | gauge | Chunks rejected for bad signatures in active sessions. |
| `outpace_http_requests_total{route,method,status}` | counter | HTTP requests that matched a route. Methods outside the standard set are reported as `other`. |
| `outpace_http_response_header_seconds_total{route,method,status}` | counter | Total time until response headers were sent. |

The `outpace_session_*` values only cover sessions that are still running, so they go down when a
stream stops. Treat them as gauges, not as process-lifetime counters. For streamed responses
(MPEG-TS, HLS segments) the HTTP timing stops at the response headers, not at the end of the
transfer. Divide it by `outpace_http_requests_total` for the mean.

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

Outpace logs to standard error, one event per line, prefixed with the UTC time of day and a
`[tag]`. See [Troubleshooting](Troubleshooting#reading-the-logs) for what the tags mean. With
Compose the logs are rotated (3 × 10 MB).

## Related pages

- [Deployment](Deployment)
- [Troubleshooting](Troubleshooting)
- [Seeding and Peers](Seeding-and-Peers)
