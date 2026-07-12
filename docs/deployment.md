# Docker Compose deployment

The root [`compose.yml`](../compose.yml) runs the published image as a non-root user, persists
node and broadcast identity, checks `/healthz`, and rotates container logs. It exposes the HTTP
API and RTMP ingest only on host loopback by default. The inbound peer listener is published on
TCP port 8621 for normal full P2P participation.

## Start and inspect

Pin the image version before production use, then start the service:

```bash
cp .env.example .env
# Edit OUTPACE_IMAGE in .env to the release you intend to run.
docker compose config
docker compose up -d --wait
curl --fail http://127.0.0.1:6878/healthz
curl --fail http://127.0.0.1:6878/networks
```

`OUTPACE_HTTP_HOST` and `OUTPACE_RTMP_HOST` control host-side Docker publishing, while
`OUTPACE_BIND` and `OUTPACE_RTMP_BIND` in the service control listeners inside the container.
The latter must remain on `0.0.0.0` for published ports to reach the process. Changing a host
binding to `0.0.0.0` exposes that service to remote clients; do so only behind an appropriate
firewall or authenticated reverse proxy.

The peer mapping is intentionally fixed at host TCP 8621 to container TCP 8621. Outpace listens
on and self-announces that same port; remapping only the Docker host side would advertise an
unreachable endpoint. Outpace has no fixed UDP peer listener. Its DHT client uses ephemeral
outbound UDP sockets, so allow outbound UDP and established replies rather than forwarding UDP
8621.

Port 62062 is deliberately absent because Outpace does not implement that legacy protocol.
Compatibility routes, gateway port mapping, and disk cache mode are also disabled by default.

## Deployment modes

The checked-in defaults are a full participant: inbound peer serving and reciprocal uploads are
enabled, and TCP 8621 is published. Allow inbound TCP 8621 through the host firewall. When the
host is behind a router/NAT, manually forward external TCP 8621 to TCP 8621 on the Docker host;
Docker port publishing does not configure the router.

Keep `OUTPACE_ENABLE_PORT_MAPPING=0` in the normal bridge-network deployment. The daemon's
UPnP/NAT-PMP implementation is appropriate only when the process owns the host/LAN-facing
network address, such as a bare-metal process or a deliberately host-networked Linux container.
Inside a Compose bridge it sees the container network's gateway and cannot safely establish the
documented router-to-host mapping. Host networking also changes the isolation and port model, so
it is not enabled by the checked-in deployment.

For a pure leecher, put this in `.env` and restart the service:

```dotenv
OUTPACE_ENABLE_INBOUND=0
OUTPACE_ENABLE_SEEDING=0
```

No peer listener or self-announcement is started in that mode. The Compose port entries may stay
present—the container has nothing listening on them—or be removed in a local override.

For a disk-backed piece cache:

```dotenv
OUTPACE_CACHE_TYPE=disk
OUTPACE_CACHE_DIR=/var/lib/outpace/cache
OUTPACE_SEED_STORE_BYTES=536870912
```

The named data volume already covers that path. `OUTPACE_CACHE_DIR` may be nested under
`OUTPACE_DATA_DIR`, as shown, but it may never equal or contain the data directory; unsafe
relationships are rejected before cache cleanup. The byte budget sizes each active stream's cache;
it is not a total volume quota. Disk pieces are ephemeral live-media cache, not durable media:
the cache root is cleared at startup and stream directories are deleted at teardown. Startup
fails if the cache root cannot be prepared. A later per-stream directory failure keeps playback
running with zero retention for that stream rather than silently allocating the same amount of
RAM.

Broadcast ingest is available in the normal service at:

```text
http://127.0.0.1:6878/broadcast/<name>
rtmp://127.0.0.1:1935/live/<name>
```

The RTMP port is host-local by default. See [`native-api.md`](native-api.md) for creation, ingest,
and playback examples.

Docker's ordinary resolver is sufficient. To use custom resolvers, create a local
`compose.override.yml`; Outpace does not need an internal DNS daemon:

```yaml
services:
  outpace:
    dns:
      - 1.1.1.1
      - 9.9.9.9
```

## Configuration contract

For `outpace serve`, supported operator overrides currently come from `OUTPACE_*` environment
variables. Their precedence is:

```text
environment > container-image defaults > built-in defaults
```

The Compose file supplies the container listener/data paths and forwards the common policy and
cache settings from `.env`. Unset values retain the documented safe defaults. Invalid values
(including misspelled boolean gates) and invalid relationships are rejected by the daemon before
network listeners start; existing
environment-only deployments remain supported. A versioned config-file schema and broad CLI
flag surface are intentionally deferred until the configuration model is stable enough to avoid
making internal scheduler details a permanent public contract.

The complete environment-variable reference remains in the root [`README`](../README.md#configuration).
Keep credentials out of `.env`; none of the current daemon settings require secrets.

## Upgrade, rollback, and removal

Back up the `outpace-data` volume before an upgrade. It contains `identity.seed` and persisted
broadcast records under `broadcasts/`; losing it changes the node identity and minted broadcast
identities. Cache contents under `cache/` do not need backing up.

To upgrade or roll back, change the pinned `OUTPACE_IMAGE` tag and run:

```bash
docker compose pull
docker compose up -d --wait
```

Inspect `/healthz`, `/networks`, and the logs before removing an older image. `docker compose
down` preserves the named volume; `docker compose down --volumes` permanently deletes identity
and broadcast state and should only be used intentionally.

Resource limits depend on workload and number of active streams. Add host-appropriate CPU and
memory limits in a local override rather than copying a limit that can trigger avoidable
out-of-memory failures. For example, after measuring a representative workload:

```yaml
services:
  outpace:
    deploy:
      resources:
        limits:
          cpus: "2"
          memory: 1g
```

The checked-in log rotation bounds Docker JSON logs independently of the piece-cache budget.
