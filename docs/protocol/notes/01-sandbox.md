# 01 — Official Engine Sandbox Setup

## Purpose

Stand up the official closed-source Acestream engine (3.2.11) inside Docker so
later tasks can capture its network traffic and instrument it.

## Files

- `re/sandbox/Dockerfile` — image definition (git-ignored, lives under `re/`)
- `re/sandbox/docker-compose.yml` — compose service definitions (git-ignored)

## Dockerfile adjustments vs. the spec template

The task description noted that `apsw`, `pynacl`, and `lxml` may require
C-build tooling. Added those to the `apt-get install` line upfront to avoid
a two-pass build:

```
build-essential libffi-dev libxml2-dev libxslt1-dev
```

All seven packages in `requirements.txt` (pycryptodome, lxml, apsw, psutil,
pynacl, iso8601, aiohttp) installed cleanly on `python:3.10-slim-bookworm`
with those extras present. `frida-tools` also installed without issues.

## Build-context resolution

`COPY engine/ /app/` inside the Dockerfile requires the build context to
include `re/engine/`. Using `context: ..` in `docker-compose.yml` (i.e. `re/`
as the context root) satisfies this cleanly. The compose file sets:

```yaml
build:
  context: ..
  dockerfile: sandbox/Dockerfile
```

## Exact commands used

```bash
# Build only (from repo root)
docker compose -f re/sandbox/docker-compose.yml build acestream

# Start the engine (capture sidecar intentionally omitted)
docker compose -f re/sandbox/docker-compose.yml up -d acestream

# Verify
sleep 15
curl -s "http://127.0.0.1:6878/webui/api/service?method=get_version" | python3 -m json.tool
```

## get_version response (verbatim)

```json
{
    "result": {
        "platform": "linux",
        "version": "3.2.11",
        "code": 3021100,
        "websocket_port": 37293
    },
    "error": null
}
```

## Engine startup log highlights

- HTTP server bound on `0.0.0.0:6878` — confirmed by `allow_remote=1`
- WebSocket server bound on `0.0.0.0:37293`
- I2I (Instance-to-Instance) API server listening on port `62062`
- LM (LibraryManager) listening on port `8621`
- `TrafficStatsSender: HTTP Error 451: unused` — engine attempted to phone home;
  451 response is normal (geo-block or license restriction), not a failure
- UPnP port-forward failed — expected in a container, not significant
- Repeated `ls: cannot access '/dev/disk/by-id/'` — engine probing for disk
  hardware ID; harmless in Docker

## Notes

- The `capture` sidecar (`nicolaka/netshoot` running `tcpdump`) is defined in
  the compose file but was NOT started here — it is reserved for later traffic-
  capture tasks.
- `re/` is git-ignored; the Dockerfile and compose file are working artifacts
  only and are not committed.
