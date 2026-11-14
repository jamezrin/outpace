# Casting

Outpace can send a live stream to a DLNA/UPnP **media renderer** on your network: most smart
TVs, many AV receivers and set-top boxes, and software renderers. The engine finds the
renderers, then tells one to play a URL that the engine serves. The TV needs no app and no
copied URL. Casting is in `edge` builds from `main`; it is off by default.

Casting covers **live streams**. VOD files aren't supported yet.

## Turn it on

Casting needs two settings:

```bash
OUTPACE_ENABLE_CAST=1 OUTPACE_BIND=0.0.0.0:6878 outpace serve
```

- `OUTPACE_ENABLE_CAST=1` adds the `/cast/*` routes. Without it they answer `404`.
- `OUTPACE_BIND` must be an address the TV can reach. The default `127.0.0.1:6878` only
  accepts connections from this machine, so `play` fails with `409` and the error
  `loopback_bind` instead of sending the TV a URL it can't open. Use `0.0.0.0:6878` or the
  machine's LAN address, and read [Exposing the API safely](Deployment#exposing-the-api-safely)
  first: the HTTP API has no authentication.

With a wildcard bind, Outpace gives each renderer the local address the operating system
would use to reach it. Discovery and renderer addresses are IPv4 only.

**Docker:** SSDP discovery uses multicast, which doesn't cross Docker's default bridge
network, and on a bridge network the engine would hand the TV the container's internal
address. Run the container with `--network host` (Linux) and set `OUTPACE_ENABLE_CAST=1`
yourself; the repository's Compose file doesn't set it.

## Use it

1. List the renderers. This sends one SSDP search, waits about 2 seconds, and reads each
   device description:

   ```bash
   curl -s http://192.168.1.2:6878/cast/targets
   ```

2. Tell one to play a live stream, using a target `id` from the list:

   ```bash
   curl -s -X POST http://192.168.1.2:6878/cast/targets/3f2a9c0d41b7e865/play \
     -H 'Content-Type: application/json' \
     -d '{"network":"ace","id":"0123456789abcdef0123456789abcdef01234567"}'
   ```

3. Check it, and stop it when you're done:

   ```bash
   curl -s http://192.168.1.2:6878/cast/targets/3f2a9c0d41b7e865/status
   curl -s -X POST http://192.168.1.2:6878/cast/targets/3f2a9c0d41b7e865/stop
   ```

Outpace offers the stream as `video/mp2t`. If the renderer refuses that type (UPnP error
`714`), it offers it once more as `video/mpeg`, the type DLNA's MPEG-TS profiles use, and
serves the stream with whichever type the renderer accepted.

`play` starts the stream before it contacts the TV, so it answers once the stream is running
and the TV has accepted it. Outpace sends one renderer one request at a time: a `stop` sent
while a `play` is still talking to the same TV waits for it to finish, then stops it. Many TVs open the stream before they answer, which is why `play`
can take several seconds. The TV fetches the stream like any other player: it shares the
running session with your other viewers and counts as one of the stream's clients.

## HTTP API

All bodies are JSON. Every route answers `404` when `OUTPACE_ENABLE_CAST` is off.

| Route | Does |
| --- | --- |
| `GET /cast/targets` | Lists renderers that have an AVTransport service. Results are cached for 60 seconds; add `?refresh=1` to search again now. |
| `POST /cast/targets/{target}/play` | Body `{"network":"ace","id":"<content id>"}`. Starts the live stream if it isn't running, then points the renderer at it and starts playback. |
| `POST /cast/targets/{target}/stop` | Stops playback. Answers `204` with no body. |
| `GET /cast/targets/{target}/status` | The renderer's transport state, and whether it is still fetching the stream. |
| `GET /cast/s/{token}.ts` | The stream URL handed to the renderer. Each `play` makes a new random token, and `stop` retires it. You don't call this yourself. |

The `id` in `play` takes the same forms as `/streams/ace/<id>`: a bare 40-hex content id,
`cid:<content id>` or `ih:<infohash>`.

`GET /cast/targets`:

```json
{
  "targets": [
    {
      "id": "3f2a9c0d41b7e865",
      "name": "Living Room TV",
      "manufacturer": "Example Corp",
      "model": "ExampleTV 55",
      "address": "192.168.1.40"
    }
  ],
  "age_secs": 0
}
```

A target `id` is derived from the device's UPnP UDN, so it stays the same across searches.
`manufacturer` and `model` are `null` when the device doesn't report them. `age_secs` is how
old the cached result is.

`POST /cast/targets/{target}/play`:

```json
{
  "target": "3f2a9c0d41b7e865",
  "network": "ace",
  "id": "cid:0123456789abcdef0123456789abcdef01234567",
  "url": "http://192.168.1.2:6878/cast/s/00112233445566778899aabbccddeeff.ts"
}
```

`GET /cast/targets/{target}/status`:

```json
{
  "target": "3f2a9c0d41b7e865",
  "transport_state": "PLAYING",
  "transport_status": "OK",
  "speed": "1",
  "cast": {
    "network": "ace",
    "id": "cid:0123456789abcdef0123456789abcdef01234567",
    "url": "http://192.168.1.2:6878/cast/s/00112233445566778899aabbccddeeff.ts",
    "stream_connections": 1
  },
  "fetching": true
}
```

`transport_state` is what the renderer reports, usually `PLAYING`, `TRANSITIONING`,
`PAUSED_PLAYBACK`, `STOPPED` or `NO_MEDIA_PRESENT`. `cast` is `null` when Outpace hasn't sent
this renderer a stream. `fetching` tells "the TV stopped" apart from "the TV stopped
fetching": a TV that still says `PLAYING` while `fetching` is `false` has dropped the stream.

### Errors

Errors carry `{"error":"<code>","message":"<text>"}`.

| Status | `error` | Meaning |
| --- | --- | --- |
| `400` | `invalid_request`, `unknown_network`, `invalid_id` | The `play` body is wrong. |
| `404` | `unknown_target` | No renderer with that id in the last search. Call `GET /cast/targets` first. |
| `404` | `stream_not_found` | The stream couldn't be started. |
| `422` | `stream_unavailable`, `origin_hls_unsupported` | The stream can't be opened (for example protected content), or it points at an HLS origin, which casting doesn't relay. |
| `409` | `loopback_bind` | `OUTPACE_BIND` is loopback; see [Turn it on](#turn-it-on). |
| `409` | `unsupported_bind` | `OUTPACE_BIND` is an IPv6 address an IPv4 renderer can't reach. |
| `502` | `renderer_fault` | The renderer refused the action. `upnp_error` holds its UPnP error code, for example `714` for a media type it can't play. |
| `502` | `renderer_unreachable`, `renderer_http_error`, `renderer_invalid` | The renderer didn't answer, answered with an HTTP error, or sent something Outpace won't accept. |
| `503` | `discovery_failed` | The SSDP search couldn't be sent. |
| `503` | `capacity_exhausted` | `OUTPACE_MAX_SESSIONS` is reached. |
| `504` | `renderer_timeout` | The renderer didn't answer in time: 15 seconds for `play`, 5 seconds for `stop` and `status`. |

## Security and limits

Anything on your network can answer a discovery search, so Outpace treats every reply as
untrusted:

- It only reads a reply from a private (RFC 1918) or link-local IPv4 address, and only
  fetches a description from the address that sent the reply. Host names aren't accepted, so
  DNS can't redirect it.
- The renderer's control URL must be on the same host and port as its description. A
  description that points somewhere else is skipped, so a device can't make Outpace send
  requests to your router, another machine or another service.
- One search reads at most 64 replies and 16 devices. A description may be at most 64 KiB
  and a control response at most 16 KiB. XML document type declarations are refused.
- Fetching a description times out after 2 seconds. A control request times out after
  5 seconds, or 15 seconds for the two `play` requests, because renderers open the stream
  before they answer them.
- Redirects are never followed.

There is no background discovery. The engine searches only when `GET /cast/targets` asks it
to, and remembers at most 16 renderers. A renderer you're casting to stays controllable with
`stop` and `status` even if a later search misses it.

The `/cast/*` routes follow the same exposure rule as the other control routes: they are
reachable wherever `OUTPACE_BIND` is. Anyone who can reach the API can list your renderers and
start or stop playback on them.

## Troubleshooting

- **The list is empty.** The TV must be on and on the same network segment. Many TVs need
  their DLNA or "screen sharing" setting turned on. Multicast doesn't cross VLANs or a
  router, and guest Wi-Fi often blocks it. Firewalls must allow UDP replies to the engine.
- **`play` succeeds but the TV shows an error.** The TV couldn't fetch the URL from `play`.
  Open that URL in VLC on another device; if that fails too, check the firewall and
  `OUTPACE_BIND`. A renderer that refuses both MPEG-TS types answers `renderer_fault` with
  UPnP error `714`; it can't play this stream.
- **Status says `PLAYING` but `fetching` is `false`.** The TV dropped the connection. Send
  `play` again.

The `[dlna]` log lines say which devices were skipped during discovery and why, and record
every play and stop. See [Troubleshooting](Troubleshooting#reading-the-logs).
