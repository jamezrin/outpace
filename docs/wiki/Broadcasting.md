# Broadcasting

Outpace can publish your own MPEG-TS feed as a new signed AceStream live swarm. You feed it
media, and it cuts the feed into pieces, signs them with a per-broadcast key, announces the
swarm and serves it to peers and local players.

Only broadcast content you have the right to distribute. The output is a **public, unencrypted**
swarm.

## Concepts

- A **broadcast** has a **name** (1–64 ASCII letters, digits, `.`, `_` or `-`; not `.` or `..`).
  The name is local. It is how you refer to the broadcast in URLs and settings.
- The first use of a name creates a **signing identity**: a key, a transport descriptor, a
  content id and an infohash. These are saved in `<data dir>/broadcasts/` and reused when you
  restart.
- A broadcast takes media from **one producer at a time**: raw HTTP, RTMP, an HTTP pull source
  or SRT. A second producer gets `409 producer_conflict`. Different names can run side by side.
- Producers can take turns on the same name. The identity stays and piece numbering carries on.

## Start a broadcast

Either run a dedicated broadcast process:

```bash
outpace broadcast my-channel --public-host stream.example.com
```

It creates or resumes the broadcast, prints its ingest URLs and `acestream://` link to standard
error, and then runs the normal server. `--public-host` sets the host name used in those
printed URLs. This command always turns inbound peer serving on, and it refuses to run in
[private peer mode](Private-Peer-Mode).

Or run `outpace serve` and simply start pushing to a name: the first producer creates the
broadcast.

The node must be reachable by peers for others to watch. Publish and forward TCP 8621; see
[NAT and Port Mapping](NAT-and-Port-Mapping).

## Feeding media

The input must be **MPEG-TS** (or RTMP, which Outpace muxes into TS), preferably with H.264 or
HEVC video and AAC audio.

### Raw HTTP PUT

```bash
# from a file
curl -T movie.ts http://127.0.0.1:6878/broadcast/my-channel
# live from FFmpeg
ffmpeg -re -i input.mp4 -c copy -f mpegts - | curl -T - http://127.0.0.1:6878/broadcast/my-channel
```

The `PUT` returns `200` straight away with `name`, `content_id` and `infohash`, while Outpace
keeps reading the body. When the upload ends, the buffered final piece is signed and published.

### RTMP

```text
rtmp://127.0.0.1:1935/live/my-channel
```

```bash
ffmpeg -re -i input.mp4 -c:v libx264 -c:a aac -f flv rtmp://127.0.0.1:1935/live/my-channel
```

In OBS, use `rtmp://<host>:1935/live` as the server and `my-channel` as the stream key. The key
must be exactly the broadcast name. RTMP listens on `127.0.0.1:1935` by default
(`OUTPACE_RTMP_BIND`). It has no authentication, so expose it only on trusted networks.

### HTTP pull sources

Outpace can pull a continuous HTTP(S) MPEG-TS stream and broadcast it, reconnecting on its own
when it drops.

Attach one through the API:

```bash
curl -X PUT http://127.0.0.1:6878/broadcast/my-channel/source \
  -H 'Content-Type: application/json' \
  --data '{"url":"https://source.example.com/live.ts"}'
curl http://127.0.0.1:6878/broadcast/my-channel/source
curl -X DELETE http://127.0.0.1:6878/broadcast/my-channel/source
```

Or declare sources at startup, so they reattach on every restart:

```bash
export OUTPACE_BROADCAST_SOURCES='{"my-channel":{"url":"https://source.example.com/live.ts"}}'
outpace serve
```

The provided Compose file reads `OUTPACE_BROADCAST_SOURCES` from `.env`.

| Field | Default | Range |
| --- | --- | --- |
| `url` | required | `http://` or `https://`, at most 8192 bytes, no credentials or fragment |
| `connect_timeout_secs` | 5 | 1–60 |
| `read_timeout_secs` | 15 | 1–300 |
| `reconnect_initial_secs` | 1 | 1–300 |
| `reconnect_max_secs` | 30 | 1–300, at least the initial value |

Things to know:

- **`202` means accepted, not healthy.** Check `GET /broadcast/<name>/source` for the actual
  state: `connecting`, `streaming`, `backoff`, `blocked`, `failed` or `stopping`. You'll also
  see `attempts`, `last_error` and `retry_in_ms`.
- **Reconnection:** retries back off exponentially from the initial delay to the maximum. The
  backoff resets after 30 seconds of successful streaming. When the source ends, it is treated as
  a live drop and Outpace reconnects.
- **Address policy:** sources on private or loopback addresses are refused unless you set
  `OUTPACE_ALLOW_NON_GLOBAL_BROADCAST_SOURCE=1`. That switch adds private, loopback and CGNAT
  IPv4 and ULA and loopback IPv6. It never allows link-local or cloud-metadata addresses.
  Redirects are rejected and proxies are ignored.
- **Secrets:** source URLs (and tokens in them) never appear in status, logs or saved state.
  The environment variable still contains them, so protect your `.env` and don't share
  `docker compose config` output.
- **Detaching vs deleting:** `DELETE .../source` stops pulling but keeps the identity. Detach
  before changing a source's URL or timings. API attachments are forgotten on restart. Startup
  declarations come back on restart even if you detached them, so remove them from the
  environment to stop them for good.

## SRT ingest

> **From v1.0.0, only in special builds.** SRT isn't in any
> release binary or published image. Build from the `v1.0.2` tag,
> on native Linux with `--features srt` and libsrt 1.5.7 or newer; see [Installation](Installation#building-with-srt-ingest).

SRT gives you an **encrypted** input over UDP, useful for publishing from a remote encoder.

```bash
install -d -m 700 ~/.config/outpace
(umask 077; openssl rand -hex 24 > ~/.config/outpace/srt.key)
OUTPACE_SRT_BIND=127.0.0.1:9000 \
OUTPACE_SRT_PASSPHRASE_FILE="$HOME/.config/outpace/srt.key" \
  outpace serve
```

| Setting | Default | Notes |
| --- | --- | --- |
| `OUTPACE_SRT_BIND` | unset (off) | Numeric UDP address. Setting it in a non-SRT build fails startup. |
| `OUTPACE_SRT_PASSPHRASE_FILE` | none (required) | Passphrase of 10–79 bytes, without NUL or line breaks (one trailing newline is fine). The file must be a regular file (not a symlink), owned by the daemon user, with no group or other access. It is read once, so restart to change the key. |
| `OUTPACE_SRT_LATENCY_MS` | `120` | 20–2000. SRT receive latency, not an end-to-end guarantee. |
| `OUTPACE_SRT_MAX_CONNECTIONS` | `4` | 1–16, counting connections that are still opening or closing. |

Publish in caller mode, with the broadcast name as the `streamid`:

```bash
ffmpeg -re -i input.mp4 -c:v libx264 -c:a aac -f mpegts \
  "srt://127.0.0.1:9000?mode=caller&streamid=my-channel&passphrase=$(cat ~/.config/outpace/srt.key)&pkt_size=1316"
```

Security notes:

- Encryption is mandatory; there is no plaintext fallback. The sender picks AES-128, -192 or -256.
- The passphrase admits a publisher to **any** broadcast name. The `streamid` isn't a credential.
- Encryption protects only the leg from the publisher into Outpace. The broadcast itself is
  still public and unencrypted.
- The passphrase ends up in the publisher's process arguments, so publish from a trusted host.
- Bind to loopback or a firewalled interface. Unauthenticated UDP still costs some work before
  it is rejected, so rate-limit or allowlist publishers at the network edge.
- Outpace never maps or advertises the SRT port.
- A disconnect is a live break, not end of file. The partial last piece is discarded, and
  reconnecting continues the same identity.

## Broadcast profiles

`OUTPACE_BROADCAST_OPTIONS` sets per-name descriptor metadata. It applies to every producer type
and to the `broadcast` command. Supported fields are listed below. Metatracker/startup-node
settings and tags/logo/description (#269) are explicitly deferred for 1.0.0 pending complete
wire and runtime evidence; arbitrary metadata is not accepted. See
[Releases and Versions](Releases-and-Versions#deferred-scope).


```bash
export OUTPACE_BROADCAST_OPTIONS='{"my-channel":{"title":"My Channel","bitrate_bytes_per_sec":500000,"piece_length":131072,"quality":"HD","categories":["other"]}}'
outpace broadcast my-channel
```

With Compose, forward it through `compose.override.yml`; see
[Deployment](Deployment#passing-other-settings).

| Field | Values | Default when omitted |
| --- | --- | --- |
| `title` | 1–256 bytes of text | The broadcast name |
| `bitrate_bytes_per_sec` | positive integer, in **bytes** per second | `8375` |
| `piece_length` | power of two, 65536–1048576 | `65536` |
| `quality` | `auto`, `SD` or `HD` | `auto` |
| `categories` | up to 32 distinct labels of 1–128 bytes | `["other"]` |

Unknown fields, wrong types and out-of-range values stop startup with
`invalid_broadcast_options`.

Profiles apply when an identity is **created**. For an existing broadcast, any field you set
explicitly must match what was stored, or the broadcast fails with
`broadcast_options_conflict`. To change a broadcast's metadata or trackers, use a new name, or
`DELETE` the broadcast and recreate it. That gives it a new identity and content id.

`OUTPACE_SEED_STORE_BYTES` must hold at least one full piece. With the default 128 MiB that is
never a problem; with tiny budgets, startup fails with `broadcast_store_too_small`.

`OUTPACE_TRACKERS` (comma-separated tracker URLs) replaces the default tracker for **new**
broadcasts. Outpace announces itself only to `udp://` trackers. To announce to a tracker on a
private address, also set `OUTPACE_TRACKER_ALLOW_NON_GLOBAL=1`.

## Watching a broadcast

| From | How |
| --- | --- |
| The same daemon | `http://127.0.0.1:6878/streams/ace/<content_id>`. It resolves locally, with no catalog lookup. |
| Another Outpace daemon | Set `OUTPACE_BEP9_CONTENT_ID_FALLBACK=1` on that daemon and play the content id, or play the descriptor URL below. |
| Any client that takes a transport-file URL | `http://<host>:6878/broadcast/<name>` serves the descriptor. |

The `acestream://` link that Outpace prints uses the descriptor's own hash as its content id.
Other Outpace daemons can only resolve it through the peer-metadata fallback above, because
it isn't registered in the public catalog.

## Managing broadcasts

| Request | Effect |
| --- | --- |
| `GET /broadcast/<name>` | The transport descriptor, or `404` before the identity exists. |
| `DELETE /broadcast/<name>` | Stops all producers and announcements, and **permanently deletes the identity**. Returns `204` when finished. |
| `DELETE /broadcast/<name>/source` | Stops only the pull source. The identity stays. |

Errors use `{"error":{"code":"..."}}`:

| Status | Codes |
| --- | --- |
| `400` | `invalid_name`, `invalid_source_config`, `invalid_source_url`, `source_address_blocked` |
| `404` | `broadcasts_disabled`, `source_not_found` |
| `409` | `producer_conflict`, `source_conflict`, `broadcast_options_conflict`, `broadcast_stopping` |
| `413`, `415` | `source_request_too_large`, `source_content_type_required` |
| `500` | `broadcast_persistence_failed`, `broadcast_state_invalid`, `piece_index_exhausted` |
| `503` | `broadcast_service_stopping`, `broadcast_cleanup_timeout` (retry the `DELETE` later) |

**None of these routes require authentication.** Keep the HTTP API on loopback or behind an
authenticating proxy.

## Backups, restarts and rollback

- Back up `<data dir>/broadcasts/` (it contains private signing keys) and keep it private.
- Piece numbers are reserved on disk in blocks of 128. After a restart, the unused part of the
  last block is skipped, so small gaps in numbering are normal.
- **Never restore an old copy of a broadcast's state** while keeping its identity, and never run
  two daemons on one data directory. Either could republish piece numbers that were already
  used.
- To downgrade Outpace while you have an active broadcast, stop it, delete the broadcast, roll
  back, and then create a fresh one.

## Related pages

- [Seeding and Peers](Seeding-and-Peers)
- [NAT and Port Mapping](NAT-and-Port-Mapping)
- [Configuration](Configuration#broadcasting-and-ingest)
- [Deployment](Deployment#upgrades-and-rollback)
