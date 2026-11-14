# FAQ

Answers to the questions people ask most about Outpace. Each answer links to the page with the
details.

## What is Outpace?

Outpace is an open-source, peer-to-peer streaming engine written from scratch in Rust. It joins
public, unencrypted Ace Stream (AceStream) swarms, verifies the media, and serves it over HTTP
as MPEG-TS or HLS. Your player, media server or app opens a URL; Outpace handles the
peer-to-peer side. It runs headless, as a server (`outpace serve`) or a one-shot command
(`outpace play`). See [Home](Home).

## Is Outpace an alternative to the official Ace Stream engine?

For playing public live streams, yes. It's an independent, open-source implementation that
works with Ace Stream content ids and `acestream://` links. It can also serve the common
engine HTTP routes that existing apps call, through an opt-in compatibility mode.

It isn't a full clone. It doesn't play premium or encrypted content, and it has no accounts, web
UI or EPG. [Optional stream-name search](Stream-Search) requires a separate services opt-in
in builds available from v1.0.0. It needs no operator API key. See
[what's different](Migrating-from-AceStream-Engine#whats-different).

Outpace isn't affiliated with or endorsed by Ace Stream. It uses Ace Stream names only to
describe compatibility.

## Do I need to install Ace Stream to use Outpace?

No. Outpace never downloads or runs the official Ace Stream engine or any of its libraries. You
install only the `outpace` binary or container. If an old engine is still running on the same
machine, stop it first, because both want ports 6878 and 8621.

## How do I play an Ace Stream link on Linux, macOS or Windows?

1. Download the release for your platform from
   [Installation](Installation#release-binaries). There are binaries for Linux x86_64, ARM64 and
   ARMv7, macOS Intel and Apple silicon, and Windows x64 and ARM64.
2. Start it: `outpace serve` (`outpace.exe serve` on Windows).
3. Take the content id from the `acestream://` link, the 40 characters after `acestream://`, and
   open `http://127.0.0.1:6878/streams/ace/<content-id>` in your player.

Or skip the server and play the link directly:

```bash
outpace play acestream://0123456789abcdef0123456789abcdef01234567 | vlc -
```

See [Getting Started](Getting-Started) and [Command Line](Command-Line#outpace-play).

## How do I run Outpace in Docker?

```bash
docker run -d --name outpace -p 127.0.0.1:6878:6878 -p 8621:8621/tcp \
  -v outpace-data:/var/lib/outpace ghcr.io/outpace-stream/outpace-engine:1.0.3
```

The image is multi-arch (amd64, arm64, arm/v7). For a production setup with Docker Compose, a
health check and log rotation, see [Deployment](Deployment#docker-compose).

## How do I use Outpace with VLC?

Open **Media → Open Network Stream** and enter:

```text
http://127.0.0.1:6878/streams/ace/0123456789abcdef0123456789abcdef01234567
```

Use the id of a current public stream. Add `.m3u8` to the URL for HLS. From a terminal,
`vlc "<url>"` or `outpace play acestream://<content-id> | vlc -` both work. See
[Playing Streams](Playing-Streams#native-live-playback).

## How do I use Outpace with Jellyfin, Dispatcharr or other IPTV tools?

Put the native stream URL in your M3U playlist, one entry per channel:

```text
#EXTM3U
#EXTINF:-1,My Channel
http://outpace-host:6878/streams/ace/0123456789abcdef0123456789abcdef01234567
```

No handshake or compatibility mode is needed. If those tools run on another machine, make the
API reachable from them, and protect it. See
[Playing Streams](Playing-Streams#known-clients) and
[Deployment](Deployment#exposing-the-api-safely).

## My app was built for the AceStream engine. Will it work?

Probably, if it uses the common HTTP routes. Start Outpace with `OUTPACE_ACE_COMPAT=1`. Apps
that call these then work through Outpace:

- `/ace/getstream`;
- `/ace/manifest.m3u8`;
- the `format=json` sessions;
- `/server/api`.

Apps built on the public Android SDK are among the tested clients. See
[Playing Streams](Playing-Streams#acestream-compatible-api).

## Can I broadcast my own P2P live stream?

Yes. `outpace broadcast my-channel` creates a signed live stream that other peers can watch.
Feed it MPEG-TS over HTTP, RTMP (from OBS or FFmpeg) or an HTTP pull source, or encrypted SRT in
builds that include it. Peers need to reach your TCP port 8621. See
[Broadcasting](Broadcasting).

## Can it play premium or encrypted channels?

No. Only public, unencrypted content is supported, and that won't change.

## What are the main differences and limitations compared with the official engine?

- **Content:** only public, unencrypted streams.
- **No application features:** no accounts, subscriptions, web UI, remote control,
  channel lists or EPG.
- **Search is separate:** [service-backed name search](Stream-Search) needs a default-off opt-in
  and uses external plain HTTP services. No operator API key is required; HTTP compatibility
  alone does not enable it.
- **A compatibility subset:** the common playback and control routes. There is no legacy
  seeking or file selection, and VOD isn't available through it (native VOD is).
- **Timing:** Outpace buffers more by default, so streams start later but stutter less.
- **Ports:** TCP 8621 for peers, with no UDP listener and no legacy port 62062.

See [Migrating from AceStream Engine](Migrating-from-AceStream-Engine#whats-different).

## Why does a stream take a while to start?

Outpace has to find peers, then deliberately buffers before it sends the first byte, so playback
rides out swarm hiccups. 20–60 seconds is normal with the defaults, and you can shorten it. See
[Troubleshooting](Troubleshooting#slow-startup).

## Does it need FFmpeg?

Only for optional [audio normalisation](HLS-Output#audio-normalisation). Playback, HLS packaging
and broadcasting don't use it.

## Which ports do I need to open?

Only TCP 8621 inbound, if you want other peers to connect to you. Keep the HTTP API (6878) and
RTMP (1935) private. No UDP port needs forwarding. See
[NAT and Port Mapping](NAT-and-Port-Mapping).

## Can I rewind live TV?

For a few minutes, with [Timeshift](Timeshift) (available from v1.0.0). It's an in-memory buffer,
not a recorder, and it doesn't do EPG catch-up.

## Why did my old playlist of bare infohashes stop working?

On `/streams/ace/`, a bare 40-character id now means a content id. Prefix infohashes with `ih:`,
for example `/streams/ace/ih:<infohash>`. See [Playing Streams](Playing-Streams#what-you-can-play).

## Where do I report a bug?

Open an issue at <https://github.com/outpace-stream/outpace-engine/issues>, following
[Troubleshooting](Troubleshooting#reporting-a-bug). Please remove content ids and stream names
first.
