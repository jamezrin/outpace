# Getting Started

This page gets you from nothing to a playing stream as quickly as possible. For other platforms
and options, see [Installation](Installation).

You need:

- a public, unencrypted AceStream **content id** for a stream that is live right now;
- a player that can open an HTTP URL, such as VLC or mpv;
- a network that allows outbound TCP and UDP. Full-tunnel VPNs such as Cloudflare WARP can stop
  peer-to-peer traffic; turn them off while you test.

The examples use the placeholder id `0123456789abcdef0123456789abcdef01234567`. Replace it with
your own.

## Option A: run the container

```bash
docker run --rm --name outpace \
  -p 127.0.0.1:6878:6878 \
  -p 8621:8621/tcp \
  -v outpace-data:/var/lib/outpace \
  ghcr.io/outpace-stream/outpace-engine:1.0.1
```

- `127.0.0.1:6878` publishes the HTTP API **only on this machine**. The API has no
  authentication, so don't publish it on all interfaces unless something else protects it.
- `8621/tcp` is the peer port. Publishing it lets other peers connect to you. You can leave it
  out; playback still works, you just won't accept inbound peers.
- The `outpace-data` volume keeps the node identity between runs.

## Option B: run the binary

Download the archive for your platform from the
[releases page](https://github.com/outpace-stream/outpace-engine/releases), unpack it and run:

```bash
./outpace serve
```

With no command, `outpace` runs `serve` anyway. You should see a line like:

```text
outpace: listening on http://127.0.0.1:6878 networks=["ace"]
```

## Check that it's running

```bash
curl http://127.0.0.1:6878/healthz
# ok
curl http://127.0.0.1:6878/networks
# {"networks":["ace"]}
```

`/healthz` only proves that the process is up. It says nothing about peers or media.

## Play a stream

Open the native playback URL in your player:

```bash
vlc "http://127.0.0.1:6878/streams/ace/0123456789abcdef0123456789abcdef01234567"
# or
mpv "http://127.0.0.1:6878/streams/ace/0123456789abcdef0123456789abcdef01234567"
```

For HLS, add `.m3u8`:

```text
http://127.0.0.1:6878/streams/ace/0123456789abcdef0123456789abcdef01234567.m3u8
```

### Expect a delay before the picture

Outpace deliberately buffers before it sends the first byte, so playback rides out swarm
hiccups:

- it collects up to 30 seconds of media, waiting no more than 15 seconds after the first media
  byte arrives. On v0.9.0 a gap in the source restarts that 15-second clock, so a choppy source
  can wait longer; newer builds count it once;
- HLS also waits until six segments are ready. If that doesn't happen within 45 seconds it
  returns HTTP `503`, and most players retry.

Peer discovery comes first, so a cold start often takes 20–60 seconds. You can shorten the
wait; see [HLS Output](HLS-Output) and
[Configuration](Configuration#playback-and-startup-buffering).

## See what it's doing

While the player is open, in another terminal:

```bash
curl http://127.0.0.1:6878/streams
curl http://127.0.0.1:6878/streams/ace/cid:0123456789abcdef0123456789abcdef01234567/status
```

The status shows connected peers, the measured bitrate and how much media the server has
queued. Closing the player releases the stream about 30 seconds later. To stop it at once:

```bash
curl -X DELETE http://127.0.0.1:6878/streams/ace/0123456789abcdef0123456789abcdef01234567
```

## Play from the command line

`outpace play` writes the stream to standard output without running a server:

```bash
outpace play acestream://0123456789abcdef0123456789abcdef01234567 | mpv -
outpace play acestream://0123456789abcdef0123456789abcdef01234567 > recording.ts
```

## Need an app to work, not just a player?

Apps and add-ons written for the original engine call `/ace/getstream` and `/server/api`. Those
routes are off by default. Turn them on with `OUTPACE_ACE_COMPAT=1`. Stop the daemon you
started above first (`docker stop outpace`, or Ctrl+C), because it holds port 6878:

```bash
docker run -d --rm --name outpace -e OUTPACE_ACE_COMPAT=1 \
  -p 127.0.0.1:6878:6878 -p 8621:8621/tcp \
  -v outpace-data:/var/lib/outpace ghcr.io/outpace-stream/outpace-engine:1.0.1
# for the binary instead: OUTPACE_ACE_COMPAT=1 ./outpace serve  (and open VLC in another terminal)
vlc "http://127.0.0.1:6878/ace/getstream?id=0123456789abcdef0123456789abcdef01234567"
```

See [Playing Streams](Playing-Streams#acestream-compatible-api).

## If it doesn't play

- `status` shows `peers: 0` for a long time: the channel may be offline, or your network blocks
  peer traffic. Try another stream and turn off VPNs.
- HLS returns `503`: the stream didn't produce enough segments within the startup timeout.
- `422` with a reason: the id couldn't be resolved to a stream Outpace can play.

[Troubleshooting](Troubleshooting) covers these and more.

## Next steps

- [Deployment](Deployment): run it permanently with Compose or systemd.
- [Configuration](Configuration): every setting.
- [Seeding and Peers](Seeding-and-Peers): what the peer port does and how to run download-only.
