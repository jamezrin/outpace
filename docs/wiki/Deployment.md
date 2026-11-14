# Deployment

This page covers running Outpace permanently: with Docker Compose, as a systemd service, behind
a reverse proxy and in an LXC guest. It also covers upgrades, rollback and shutdown.

## Exposing the API safely

The HTTP API has **no authentication**, and some of its routes change state:

- stopping streams;
- creating and deleting broadcasts;
- attaching pull sources;
- starting and stopping support sources.

So:

- Keep `OUTPACE_BIND` on `127.0.0.1` (the default for the binary). With Docker, publish
  `127.0.0.1:6878:6878`.
- To serve players on your LAN, either bind to the LAN address and firewall the port to trusted
  clients, or put an authenticating reverse proxy in front.
- Treat RTMP (`1935`) and SRT the same way. Anyone who can reach RTMP can publish to any
  broadcast name.
- Only the peer port (TCP 8621) is meant to be public.

## Docker Compose

The repository ships a production
[`compose.yml`](https://github.com/outpace-stream/outpace-engine/blob/main/compose.yml) with:

- a non-root user;
- a persistent named volume;
- a `/healthz` health check;
- log rotation (3 × 10 MB);
- a 30-second stop grace period;
- loopback-only HTTP and RTMP.

```bash
cp .env.example .env
# set OUTPACE_IMAGE=ghcr.io/outpace-stream/outpace-engine:1.0.2 (pin a release)
docker compose config        # check the result
docker compose up -d --wait
curl --fail http://127.0.0.1:6878/healthz
docker compose logs -f
```

What the defaults give you:

- HTTP on `127.0.0.1:6878` and RTMP on `127.0.0.1:1935` on the host. Set `OUTPACE_HTTP_HOST` or
  `OUTPACE_RTMP_HOST` to publish them elsewhere.
- The peer port published on `0.0.0.0:8621/tcp`. Keep this mapping fixed at 8621:8621.
- A full participant (inbound and seeding on), port mapping off, memory cache, compatibility
  API off.

After changing `.env`, run `docker compose up -d --wait`. `docker compose restart` doesn't apply
environment changes.

The shipped health check runs `curl`, which the Debian images include. To use a
[minimal image](Installation#minimal-image), which has no `curl`, override it in a
`compose.override.yml`:

```yaml
services:
  outpace:
    image: ghcr.io/outpace-stream/outpace-engine:X.Y.Z-minimal
    healthcheck:
      test: ["CMD", "/usr/local/bin/outpace", "healthcheck"]
```

### Passing other settings

`compose.yml` only forwards a fixed list of variables into the container (listed in
[Configuration](Configuration#docker-compose-variables)). Anything else in `.env` is silently
ignored. Put other settings in a `compose.override.yml` next to it:

```yaml
services:
  outpace:
    environment:
      OUTPACE_ACE_COMPAT: "1"
      OUTPACE_PREBUFFER_MS: "10000"
      OUTPACE_HLS_STARTUP_SEGMENTS: "3"
      OUTPACE_MAX_SESSIONS: "8"
      OUTPACE_BROADCAST_OPTIONS: '${OUTPACE_BROADCAST_OPTIONS-{}}'
```

The last line forwards a value from `.env`; the others set it directly. Then run
`docker compose up -d --force-recreate --wait`.

### Common variations

Pure leecher, in `.env`:

```dotenv
OUTPACE_ENABLE_INBOUND=0
OUTPACE_ENABLE_SEEDING=0
```

Disk-backed piece cache, in `.env`:

```dotenv
OUTPACE_CACHE_TYPE=disk
OUTPACE_CACHE_DIR=/var/lib/outpace/cache
OUTPACE_SEED_STORE_BYTES=536870912
```

The cache may sit inside the data volume as shown, but it must not be the data directory itself.

Custom DNS resolvers, CPU and memory limits, in `compose.override.yml`:

```yaml
services:
  outpace:
    dns: [1.1.1.1, 9.9.9.9]
    deploy:
      resources:
        limits:
          cpus: "2"
          memory: 1g
```

Measure your real workload before you set a memory limit. Each live stream needs memory for its
startup buffer (up to 128 MiB), seeding store, HLS window and timeshift budget, if enabled.

## systemd

The [AUR packages](Installation#arch-linux-aur) and, from v1.0.0, the
[`.deb` and `.rpm` packages](Installation#debian-ubuntu-and-fedora-packages)
set all of this up: edit `/etc/outpace/outpace.env` and run `systemctl enable --now outpace`. The
unit they ship is
[`packaging/linux/outpace.service`](https://github.com/outpace-stream/outpace-engine/blob/main/packaging/linux/outpace.service),
the same as below with the binary at `/usr/bin/outpace`.

To set it up by hand, create a user and an environment file:

```bash
sudo useradd --system --home-dir /var/lib/outpace --shell /usr/sbin/nologin outpace
sudo install -d -m 0750 /etc/outpace
sudo tee /etc/outpace/outpace.env >/dev/null <<'EOF'
OUTPACE_BIND=127.0.0.1:6878
OUTPACE_RTMP_BIND=127.0.0.1:1935
OUTPACE_DATA_DIR=/var/lib/outpace
EOF
sudo chmod 0640 /etc/outpace/outpace.env
```

`/etc/systemd/system/outpace.service`:

```ini
[Unit]
Description=outpace streaming engine
Wants=network-online.target
After=network-online.target

[Service]
User=outpace
Group=outpace
EnvironmentFile=/etc/outpace/outpace.env
ExecStart=/usr/local/bin/outpace serve
Restart=on-failure
RestartSec=5
StateDirectory=outpace
StateDirectoryMode=0700
TimeoutStopSec=30
LimitNOFILE=65536
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now outpace
journalctl -u outpace -f
```

Logs go to standard error. Each line starts with a UTC time of day (`HH:MM:SS.mmm`), and the
journal adds the date. Open TCP 8621 in your firewall if you run as a full participant.

## Windows service

From v1.0.0. On Windows, `outpace.exe` can install itself as
a service. Run these from an elevated (administrator) prompt:

```powershell
New-Item -ItemType Directory -Force "$env:ProgramFiles\outpace"
Copy-Item .\outpace.exe "$env:ProgramFiles\outpace\"
& "$env:ProgramFiles\outpace\outpace.exe" service install
sc.exe start outpace
```

- The service is called `outpace`. It starts automatically at boot, runs as its own
  unprivileged virtual account `NT SERVICE\outpace`, and Windows restarts it if it crashes.
- Its data directory holds the node identity and broadcast signing keys, so it must be private.
  `service install` creates `%ProgramData%\outpace` for SYSTEM, Administrators and the service
  account only, with nothing inherited. The service refuses to start, and `service install`
  refuses to finish, if the directory already exists with any other owner or access, or is a
  link. Remove such a directory and run `service install` again. A custom `OUTPACE_DATA_DIR`
  that doesn't exist yet is created the same way. If it already exists, it must already be
  private.
- Without `OUTPACE_DATA_DIR`, data lives in `%ProgramData%\outpace`. The log is
  `%ProgramData%\outpace\outpace.log`, because a service has no console.
- Settings come from the service's `Environment` registry value: one `NAME=value` per entry,
  separated by `\0`. Restart the service after changing it:

```powershell
reg add HKLM\SYSTEM\CurrentControlSet\Services\outpace /v Environment /t REG_MULTI_SZ `
  /d "OUTPACE_BIND=127.0.0.1:6878\0OUTPACE_CACHE_TYPE=disk" /f
sc.exe stop outpace
# Wait for the stop (graceful drain) to finish before starting again.
$deadline = (Get-Date).AddSeconds(60)
while ((Get-Service outpace).Status -ne 'Stopped') {
  if ((Get-Date) -gt $deadline) { throw 'outpace did not stop within 60 seconds' }
  Start-Sleep -Seconds 1
}
sc.exe start outpace
if ($LASTEXITCODE -ne 0) { throw 'outpace did not start; see %ProgramData%\outpace\outpace.log' }
```

To remove it, run `outpace.exe service uninstall`, which stops the service first. The data
directory is kept. Open TCP 8621 in Windows Defender Firewall if you run as a full participant.

## macOS launchd

From v1.0.0. The repository ships a per-user LaunchAgent. It
starts Outpace at login and restarts it if it crashes:

```bash
git clone https://github.com/outpace-stream/outpace-engine.git outpace && cd outpace
packaging/macos/install-launchagent.sh install /usr/local/bin/outpace
packaging/macos/install-launchagent.sh uninstall     # to remove it
```

- The agent is `~/Library/LaunchAgents/outpace.plist`, created from
  [`packaging/macos/outpace.plist.in`](https://github.com/outpace-stream/outpace-engine/blob/main/packaging/macos/outpace.plist.in).
- To change settings, edit its `EnvironmentVariables`, then run
  `packaging/macos/install-launchagent.sh reload`. Running `install` again, for example after
  updating the binary, keeps your edits and only updates the program path.
- HTTP and RTMP stay on `127.0.0.1`. The log is `~/Library/Logs/outpace.log`, and data stays in
  `~/Library/Application Support/outpace`.
- Unloading sends `SIGTERM`, which drains gracefully. Uninstalling keeps the data and the log.

## FreeBSD rc.d

From v1.0.0, there is a FreeBSD binary (see
[Installation](Installation#freebsd-and-other-bsds)). Run it under `daemon(8)`, which
restarts it if it exits:

```sh
pw useradd outpace -d /var/db/outpace -s /usr/sbin/nologin
install -d -o outpace -g outpace -m 0700 /var/db/outpace
fetch -o /usr/local/etc/rc.d/outpace \
  https://raw.githubusercontent.com/outpace-stream/outpace-engine/main/packaging/freebsd/outpace
chmod 0755 /usr/local/etc/rc.d/outpace
sysrc outpace_enable=YES
service outpace start
```

Run these as root. The script is
[`packaging/freebsd/outpace`](https://github.com/outpace-stream/outpace-engine/blob/main/packaging/freebsd/outpace).
`daemon(8)` stays root so it can write `/var/run/outpace.pid` and `/var/log/outpace.log`, and
runs Outpace itself as `outpace`. To use another account, set `outpace_runas`, not
`outpace_user`: `rc.subr` would run `daemon(8)` as that user too, and the start would fail.

`daemon -P` stops the supervisor and the daemon together, so `service outpace stop` shuts
Outpace down gracefully. To change settings, set `outpace_env` with `sysrc`, for example
`sysrc outpace_env="OUTPACE_BIND=127.0.0.1:6878 OUTPACE_DATA_DIR=/var/db/outpace OUTPACE_CACHE_TYPE=disk"`,
then restart.

## Behind a reverse proxy

A proxy is the usual way to add TLS and authentication. Things to get right:

- **Don't buffer streams.** Turn off response buffering, and allow long reads: a live TS
  response never ends.
- **Native routes** use root-relative URLs (`/streams/...`). Serve Outpace at the root of its own
  host name, not under a sub-path.
- **The compatibility API** builds absolute `http://<Host>/...` URLs from the request's `Host`
  header. It ignores `X-Forwarded-Proto` and `X-Forwarded-Prefix`, and it never returns
  `https://`. Clients that follow returned URLs through a TLS-only proxy will fail unless the
  proxy also serves plain HTTP on that host, or rewrites the response bodies.

An nginx example with basic authentication:

```nginx
server {
    listen 443 ssl;
    server_name outpace.example.com;
    # ssl_certificate ...;

    auth_basic "outpace";
    auth_basic_user_file /etc/nginx/outpace.htpasswd;

    location / {
        proxy_pass http://127.0.0.1:6878;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_buffering off;
        proxy_request_buffering off;   # for broadcast PUT uploads
        proxy_read_timeout 1h;
        client_max_body_size 0;        # broadcast PUT bodies are unbounded
    }
}
```

Most players can't send basic-auth credentials in a URL. For players, a LAN-only firewall rule
is often simpler than proxy authentication.

## LXC guest behind Cloudflare WARP

There is a tested setup for running the image in an unprivileged Proxmox LXC guest that follows
`edge` automatically and sits behind Cloudflare WARP. It is in
[`docs/deployment-lxc-warp.md`](https://github.com/outpace-stream/outpace-engine/blob/main/docs/deployment-lxc-warp.md),
with scripts in
[`tools/deploy/lxc/`](https://github.com/outpace-stream/outpace-engine/tree/main/tools/deploy/lxc).
In short:

- The guest needs `nesting=1,keyctl=1` for Docker and the TUN device for WARP. Give it a static
  LAN address that no other guest uses, including stopped ones.
- The provided Compose file uses host networking, so WARP applies to the daemon directly.
- An `outpace-deploy` systemd timer polls the registry once a minute and redeploys when the
  `edge` image changes.

**The inbound-peer caveat:** with WARP in `Warp` mode, a correctly forwarded peer port never
completes a handshake. WARP's firewall drops inbound connections from public addresses, and its
routing sends replies into the tunnel. The `warp-inbound-fix` helper service opens exactly one
TCP port through WARP by:

- marking the inbound packets;
- turning on `net.ipv4.tcp_fwmark_accept`;
- reusing WARP's own bypass mark.

It depends on undocumented WARP internals, so recheck it after WARP upgrades. Without it, run
the node as download-only, or run it outside WARP.

To compare two engines behind one router, give each its own external peer port and set
`OUTPACE_PEER_LISTEN` to match.

## Linux portability

- Images: `linux/amd64`, `linux/arm64`, `linux/arm/v7` (releases); `edge` is `linux/amd64` only.
- ARMv7 means hard-float ARMv7 such as Raspberry Pi 2/3. From v1.0.0 there are also
  ARMv6 hard-float and RISC-V 64 binaries (no image). Soft-float ARM isn't supported. On 32-bit
  ARM expect fewer simultaneous streams; start with `OUTPACE_CACHE_TYPE=disk`,
  `OUTPACE_SEED_STORE_BYTES=67108864` and `OUTPACE_SESSION_BUFFER=64`.
- On 32-bit systems each in-memory pool is capped at about 512 MiB. Disk caches can be larger.
- ARM64 kernels with 16 KiB pages haven't been verified on real hardware.
- No UDP port needs publishing on any platform.

## Upgrades and rollback

1. **Back up the data directory** (`outpace-data` volume or `/var/lib/outpace`). It holds
   `identity.seed` and `broadcasts/`. Losing it changes your node identity and the content ids of
   your broadcasts. `cache/` doesn't need backing up.
2. Change the pinned version and restart:

   ```bash
   # Compose: edit OUTPACE_IMAGE in .env, then
   docker compose pull
   docker compose up -d --wait
   curl --fail http://127.0.0.1:6878/healthz
   ```

3. Check `/healthz`, `/networks` and the logs before you delete the old image.

Rollback is the same with the older tag. **If you run broadcasts**, older binaries can reuse
piece numbers after a rollback, and so can restoring an older snapshot of the data volume. To
roll back safely: stop the broadcast, delete it (`DELETE /broadcast/<name>`), roll back, then
create a fresh broadcast.

`docker compose down` keeps the volume. `docker compose down --volumes` deletes your identity
and broadcasts for good.

## Graceful shutdown

On `SIGTERM` (`docker stop`, `systemctl stop`) outpace:

1. stops support sources and live streams, which ends open viewer responses;
2. stops broadcasts, allowing cleanup up to 5 seconds;
3. drains HTTP connections;
4. exits.

> **From v1.0.0:** shutdown ends open viewer responses first. On v0.9.0, a
> connected viewer keeps the process alive until it disconnects, or until the stop timeout
> kills it (30 seconds in the provided Compose file).

A stuck filesystem operation can't be cancelled. In that case cleanup gives up after its grace
period and logs it. Already-connected RTMP publisher sockets can stay open until the process
exits.

### Idle streams

A stream with no viewers, HLS requests or leases is torn down after a 30-second grace period.

> **From v1.0.0:** one stream whose cleanup hangs no longer stops later
> idle streams from being cleaned up. The log shows `[manager] idle cleanup still pending` when
> this happens.

## Related pages

- [Installation](Installation)
- [Configuration](Configuration)
- [NAT and Port Mapping](NAT-and-Port-Mapping)
- [Migrating from AceStream Engine](Migrating-from-AceStream-Engine)
- [Support Sources and Operations](Support-Sources-and-Operations)
