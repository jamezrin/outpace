# Command Line

outpace is one executable with three commands. Daemon behaviour is configured with `OUTPACE_*`
environment variables, not flags; see [Configuration](Configuration) and the
[A–Z list](Environment-Variables).

```text
outpace [serve]                         run the daemon (the default)
outpace play [--vod] [--peer IP:PORT]... <input>
outpace broadcast [--public-host HOST] <name>
outpace --version | --help | <command> --help
```

## `outpace serve`

Runs the daemon:

- the HTTP API on `OUTPACE_BIND`;
- RTMP ingest on `OUTPACE_RTMP_BIND`;
- SRT ingest, if configured and built in;
- the peer services that are enabled.

It takes no flags. Running `outpace` with no command does the same.

At startup it prints, for example:

```text
outpace: RTMP ingest listening on rtmp://127.0.0.1:1935/live/<name>
outpace: listening on http://127.0.0.1:6878 networks=["ace"]
```

If any setting is invalid, it exits with an error before opening a listener. It stops cleanly
on `SIGTERM` or Ctrl+C; see [Deployment](Deployment#graceful-shutdown).

## `outpace play`

Plays one stream to **standard output** without starting the HTTP API. Progress and errors go
to standard error, so piping and redirecting are safe.

```bash
outpace play acestream://0123456789abcdef0123456789abcdef01234567 | mpv -
outpace play acestream://0123456789abcdef0123456789abcdef01234567 > live.ts
outpace play --vod acestream://0123456789abcdef0123456789abcdef01234567 > movie.mp4
```

| Flag | Meaning |
| --- | --- |
| `--vod` | Treat the input as a single-file VOD. Every piece is checked against its hash before it is written, and the command exits when the file is complete. |
| `--peer IP:PORT` | An extra IPv4 bootstrap peer. Repeat it for several. In [private mode](Private-Peer-Mode) it must be in the allowlist. |

### Accepted inputs

| Form | Meaning |
| --- | --- |
| `acestream://<content-id>` | A content id (40 hex characters). The only option accepted after `?` is `index=0`. |
| `acestream:?content_id=<id>` | The same, in query form. |
| `acestream:?infohash=<hash>` | An infohash. It needs a verified descriptor from the catalog. |
| `acestream:?url=<encoded URL>` | A transport-file URL. Percent-encode its own query, e.g. `%26` for `&`. |
| `acestream:?magnet=<encoded magnet>` | A BitTorrent v1 magnet, reduced to its infohash. |
| `https://host/path.acelive?a=1&b=2` | A transport-file URL given directly. Use this form when the URL has its own `&` query. |
| `magnet:?xt=urn:btih:<hash>` | A v1 magnet (40 hex or 32 base32 characters). `urn:btmh:` (v2) isn't supported. |
| `local:<alias>` | A local descriptor in [private peer mode](Private-Peer-Mode). |

Rules for these inputs:

- When an `acestream:?` link carries several selectors, the first present wins:
  `content_id`, then `infohash`, then `url`, then `magnet`.
- Duplicate query keys, and the file-selection options `_idx` and `stream_id`, are rejected.
- Transport-file URLs are fetched over HTTP(S) with a 1 MiB limit, no redirects, and a time
  limit. Private and loopback hosts are refused unless `OUTPACE_ALLOW_NON_GLOBAL_TRANSPORT=1`.
- The `acestream://` link of a broadcast made by **another** outpace daemon needs
  `OUTPACE_BEP9_CONTENT_ID_FALLBACK=1`.

### How `play` behaves

- It reads the same `OUTPACE_*` environment as `serve`: data directory and identity, buffering,
  recovery, seeding and private peers. It never opens a listener, and never announces itself.
- It writes `outpace play: <input>` and the resolved id to standard error. Remove these lines
  before you share logs; see [Troubleshooting](Troubleshooting#reporting-a-bug).
- A live stream runs until you stop it (Ctrl+C) or the reader closes the pipe.
- It exits non-zero if the input can't be resolved or played.

## `outpace broadcast`

Creates the named [broadcast](Broadcasting), or resumes it, then runs the normal daemon so you
can feed it.

```bash
outpace broadcast my-channel
outpace broadcast my-channel --public-host stream.example.com
```

| Argument | Meaning |
| --- | --- |
| `<name>` | 1–64 ASCII letters, digits, `.`, `_` or `-`. Not `.` or `..`. |
| `--public-host HOST` | Host name or address used in the printed URLs. Defaults to the bind address. |

On standard error it prints:

- the raw HTTP ingest URL, `http://<host>:<port>/broadcast/<name>`;
- the RTMP ingest URL, `rtmp://<host>:<port>/live/<name>`;
- the broadcast's `acestream://` link.

The command:

- always turns inbound peer serving on, because a broadcast needs it;
- refuses to run in private peer mode;
- applies the name's profile from `OUTPACE_BROADCAST_OPTIONS`, if there is one.

## `--version` and `--help`

```bash
outpace --version      # e.g. "outpace 0.9.0"
outpace --help
outpace play --help
```

`edge` builds report the last release's version; see
[Releases and Versions](Releases-and-Versions#which-version-am-i-running).

## In a container

The image's entry point is `outpace` and its default command is `serve`, so arguments go after
the image name:

```bash
docker run --rm ghcr.io/jamezrin/outpace:0.9.0 --version
docker run --rm -v outpace-data:/var/lib/outpace ghcr.io/jamezrin/outpace:0.9.0 \
  play acestream://0123456789abcdef0123456789abcdef01234567 > live.ts
docker run -d -p 127.0.0.1:6878:6878 -p 127.0.0.1:1935:1935 -p 8621:8621/tcp \
  -v outpace-data:/var/lib/outpace ghcr.io/jamezrin/outpace:0.9.0 broadcast my-channel
```

## Related pages

- [Getting Started](Getting-Started)
- [Playing Streams](Playing-Streams)
- [Environment Variables](Environment-Variables)
