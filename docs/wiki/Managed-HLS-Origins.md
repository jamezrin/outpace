# Managed HLS Origins

> **From v1.0.0:** HLS-origin descriptors and the managed relay. See [Releases and Versions](Releases-and-Versions)
> for publication status.

Some AceStream descriptors don't point at a peer-to-peer swarm. Instead they name an ordinary
HTTP(S) HLS stream (an "origin"). Outpace recognises these and never tries to open a swarm for
them. What it does next depends on `OUTPACE_MANAGED_HLS`.

This is HTTP playback handoff or relay, not original-engine HLS transport origination or
peer-to-peer HLS support-node parity. That protocol scope (#272) is explicitly deferred for
1.0.0. Native MPEG-TS-to-HLS packaging is also a separate supported feature; see
[Releases and Versions](Releases-and-Versions#deferred-scope).

## Default: direct handoff

With `OUTPACE_MANAGED_HLS` unset or `0`, every playback route answers **`302`** with the
origin's URL (and `Cache-Control: no-store`). The player then talks to the origin directly. This
applies to `/streams/...`, `/ace/getstream`, `/ace/manifest.m3u8` and a compat lease's
`playback_url`.

- Outpace makes **no** request to the origin.
- Compat `format=json` returns `playback_url` (the origin URL), `is_direct: true` and
  `transport: "hls"`, with no stat or command URL.
- `get_media_files` returns an error telling the client to use an HTTP playback route.

This is the safe default. Its drawbacks:

- the player sees the origin URL, including any signature in it;
- the player needs direct internet access to the origin;
- there is no timeshift.

## Managed relay

Set `OUTPACE_MANAGED_HLS=1` (and restart) to have Outpace fetch the origin itself and serve it
through a local relay:

- Playback routes redirect to a per-viewer relay URL, `/origin-hls/<token>/0.m3u8`.
- Outpace rewrites the playlists, so variants, alternate renditions, I-frame playlists, segments
  and init maps all point back at the relay. **Origin URLs and their signatures never reach the
  player.**
- Media sequence numbers, discontinuities, programme date-time, byte ranges and `ENDLIST` pass
  through unchanged.
- It doesn't transcode or buffer ahead. The player still picks variants and pulls segments, and
  each request is fetched from the origin, or served from a short cache when the origin allows
  caching.
- Compat JSON keeps `is_direct: true` and adds `managed_origin: true` and a `stop_url`
  (`DELETE` it to end that viewer: `204`, then `404`). With [timeshift](Timeshift) on, it also
  adds a `command_url`.

Use the relay when:

- players are on a network that can reach Outpace but not the internet;
- you don't want origin tokens exposed to players;
- you want [managed-origin replay](Timeshift#managed-origin-replay);
- you need a single egress point for origin traffic.

With Compose, `OUTPACE_MANAGED_HLS` must go in `compose.override.yml`; see
[Deployment](Deployment#passing-other-settings).

## Security posture

Turning the relay on makes the daemon fetch URLs that come from descriptors, so every fetch is
locked down. These rules can't be relaxed:

- **Public destinations only.** Every request and every redirect is resolved, and the resulting
  address is checked. Loopback, private, link-local (including cloud metadata endpoints),
  CGNAT, IPv6 unique-local, multicast and unspecified addresses are refused.
  `OUTPACE_ALLOW_NON_GLOBAL_*` switches don't affect this.
- **Pinned connections.** The connection goes to the address that was checked. System proxies,
  cookies and stored credentials aren't used, and URLs with credentials are rejected.
- **Redirects:** at most three, each re-checked. An HTTPS-to-HTTP redirect fails.
- **Deadline:** 30 seconds per request, including time spent queueing.
- **Size limits:**
  - playlists 1 MiB, 16 KiB per line and 512 references;
  - media responses 16 MiB;
  - per viewer, at most 1024 URL entries and 512 KiB of URL text.
- **Connection reuse** is bounded:
  - at most 16 HTTP/1 clients, keyed by origin and checked address, each with one idle
    connection;
  - idle connections close after 15 seconds, and clients retire after 60;
  - a changed DNS answer retires the client for the old address.

## Gzip playlists

Some origins compress their playlists. The relay accepts that, within limits:

- Only playlist requests send `Accept-Encoding: gzip, identity`. Segments, init maps and byte
  ranges ask for `identity`, and any other encoding is refused.
- A gzip playlist may be at most 1 MiB compressed **and** 1 MiB decompressed.
- Every gzip member's checksum and length are verified, and at most 16 concatenated members are
  accepted. Trailing or malformed data fails.
- Decompression is subject to the same 30-second deadline and per-viewer stop.

## Capacity

| Limit | Value |
| --- | --- |
| Concurrent viewers | 64 |
| Client requests being handled | 16 |
| Concurrent fetches to origins | 4 |
| Viewer idle expiry | 60 seconds without requests |
| Viewer maximum lifetime | 6 hours |
| Media cache | Only when the origin allows it, at most 30 seconds per entry, 16 MiB and 1024 entries in total |

When the limits are reached, new viewers get `503 origin_capacity`.

## Errors

Relay errors are short plain-text codes with `Cache-Control: no-store`:

| Status | Codes | Meaning |
| --- | --- | --- |
| `400` | `invalid_hls_origin`, `invalid_origin_range` | Refused origin URL or destination, or a bad `Range` |
| `404` | `origin_not_found`, `origin_cancelled` | Unknown, expired or stopped viewer or resource |
| `415` | `unsupported_origin_playlist` | An unsupported HLS feature, or a query string on a relay URL |
| `502` | `invalid_origin_playlist`, `origin_resource_limit`, fetch codes, `origin_upstream_*` | The origin's response was invalid, too large, failed, or was an HTTP error |
| `503` | `origin_capacity` | Viewer or request limit reached |
| `504` | `origin_timeout` and timeout fetch codes | The 30-second deadline expired |

### X-Outpace-Upstream-Status

When the origin itself returns an HTTP error, the relay still answers `502`, but adds the
origin's status in **`X-Outpace-Upstream-Status`** with a stable body code:

| Origin status | Body |
| --- | --- |
| 401 | `origin_upstream_unauthorized` |
| 403 | `origin_upstream_forbidden` |
| 404 | `origin_upstream_not_found` |
| 410 | `origin_upstream_gone` |
| 429 | `origin_upstream_rate_limited` |
| 503 | `origin_upstream_unavailable` |
| anything else | `origin_upstream_http_status` |

```bash
# TOKEN is the viewer token from the relay URL /origin-hls/<token>/0.m3u8
curl -si "http://127.0.0.1:6878/origin-hls/${TOKEN:?set the viewer token}/0.m3u8" \
  | grep -i '^x-outpace-upstream-status'
```

Nothing else from the origin's error is passed on: no cookies, no `Location` and no error page.
A `403` or `410` usually means the origin's signed URL expired. Restart playback from the
playback route to get a fresh one.

## Not supported

- Encrypted HLS (`EXT-X-KEY`)
- Low-latency HLS (partial segments, preload hints, delta updates)
- Content steering and variable substitution
- `If-Range` and query-string playlist reloads
- Multi-file selection: only the single default file is played
- Swarm statistics, peer controls and audio normalisation, since none of these apply to an HTTP
  origin

## Monitoring

```bash
curl http://127.0.0.1:6878/origin-hls/status
# {"enabled":true,"active_sessions":2}
```

It never shows tokens or origin URLs. Relay viewers aren't stream sessions, so they don't appear
in `/streams`. When you decide whether the daemon is idle (for example before a restart), check
both.

## Related pages

- [Timeshift](Timeshift#managed-origin-replay): replay for managed origins.
- [Playing Streams](Playing-Streams)
- [Configuration](Configuration#managed-hls-origins)
