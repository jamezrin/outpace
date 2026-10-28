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

Running in an LXC guest behind Cloudflare WARP, or tracking the moving `edge` image, has its own
requirements; see [`deployment-lxc-warp.md`](deployment-lxc-warp.md). In particular, WARP's
firewall blocks forwarded inbound peers unless the guest is told to let the peer port through.

For a pure leecher, put this in `.env` and restart the service:

```dotenv
OUTPACE_ENABLE_INBOUND=0
OUTPACE_ENABLE_SEEDING=0
```

No peer listener or self-announcement is started in that mode. The Compose port entries may stay
present—the container has nothing listening on them—or be removed in a local override.

### Inbound occupancy policy

Defaults are G=64 retained inbound connections, N=16 admitted connections per canonical stream,
and U=8 stable desired unchokes per exact served key. Broadcast media and metadata aliases share
N, including metadata-only and choked occupants. At defaults, a stream's 17th admission closes
before a handshake response. G=0 acts as one; N=0 restores global-only admission; U=0 still
selects one optimistic interested peer. Positive N greater than G is valid. Configuration is
read at startup; it is not hot-reloaded.

To restore global-only admission, put this explicit service override in `compose.override.yml`
(the service name matches the root Compose file):

```yaml
services:
  outpace:
    environment:
      OUTPACE_MAX_INBOUND_PER_STREAM: "0"
```

Recreate the service with `docker compose up -d --force-recreate --wait`. Use `"16"` to restore
the default, or another unsigned integer to select N. Merely placing the new variable in `.env`
does not forward it into the container: the current Compose templates do not interpolate or
forward it. Empty, negative, malformed, overflow or non-Unicode values fail process startup.
This setting changes no listener address, port publishing, enable flag, or cache path.

Admission promises neither bandwidth/equal shares nor class reservations: metadata clients can
fill N and crowd out media, and unidentified handshakes can still fill G. For I interested peers,
each exact-key coordinator selects `min(I, U + 1)` desired unchokes in interest-entry order,
rotating its tail on nominal 10-second ticks. Asynchronous observation/writes prevent treating
this as an instantaneous wire-visible bound; metadata replies bypass Interested/Unchoke.

DELETE removes producer/registry ownership, not existing connections. Admitted sessions retain
their data and coordinator Arcs; old coordinators stop receiving registry-driven rotations.
Aborting only the listener also leaves detached handlers running until they exit. TTL=300
reaps ownerless registry entries, not sessions. The serve loop's 250 ms polling can cancel
pending reads and their inner timeouts, so per-operation timeouts do not establish a whole-session
idle deadline. Outbound reciprocal uploads are outside these inbound budgets. See
[shared seeding](protocol/shared-seeding.md) for lifetime details.

For a disk-backed piece cache:

```dotenv
OUTPACE_CACHE_TYPE=disk
OUTPACE_CACHE_DIR=/var/lib/outpace/cache
OUTPACE_SEED_STORE_BYTES=536870912
```

The named data volume already covers that path. `OUTPACE_CACHE_DIR` may be nested under
`OUTPACE_DATA_DIR`, as shown, but it may never equal or contain the data directory; unsafe
relationships are rejected before cache cleanup. Startup also refuses to wipe a non-empty
cache directory that is missing a regular `.outpace-cache` sentinel file (written automatically
on first successful disk-cache start); symlink sentinels are rejected. For an existing cache,
create this file only after verifying that all contents may be deleted. The byte budget sizes
each active stream's cache; it is not a total volume quota. Disk pieces are ephemeral live-media
cache, not durable media: a dedicated cache root is cleared at startup and stream directories
are deleted at teardown. Startup fails if the cache root cannot be prepared. A later per-stream
directory failure keeps playback running with zero retention for that stream rather than
silently allocating the same amount of RAM.

By default a live stream's in-RAM reseed store is bounded by *age*, not just bytes:
`OUTPACE_SEED_RETENTION_SECS` (default `45`) keeps roughly the last N seconds of downloaded pieces
for reseeding, so RAM tracks the stream's bitrate instead of always growing to
`OUTPACE_SEED_STORE_BYTES` (which remains a hard safety ceiling). Set it to `0` to disable the age
bound and fall back to byte-only retention. VOD caches are always byte-only. outpace also uses
jemalloc with a background purge thread on non-Windows targets, so resident memory falls back toward
the live working set after a stream ends; `GET /debug/memstats` reports the allocator's live-heap
and resident counters.

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

## Broadcast profile configuration

Set `OUTPACE_BROADCAST_OPTIONS` before starting the daemon; the same named profile applies
to raw PUT, RTMP, API/startup HTTP pull and CLI mint/resume. See the
[exact schema/defaults/errors](native-api.md#broadcast-startup-profiles). In a protected env file:

```dotenv
OUTPACE_BROADCAST_OPTIONS='{"synthetic-origin":{"title":"Synthetic broadcast","bitrate_bytes_per_sec":500000,"piece_length":131072,"quality":"HD","categories":[]}}'
```

Compose does not automatically pass arbitrary host variables into containers. Add this to
`compose.override.yml` when using profiles with the supplied Compose files:

```yaml
services:
  outpace:
    environment:
      OUTPACE_BROADCAST_OPTIONS: '${OUTPACE_BROADCAST_OPTIONS-{}}'
```

Use the unset-only default to preserve invalid explicit blank values for strict validation.
Keep JSON quoting intact and recreate the container after changing startup settings. Profiles
are immutable in a running process and are not persisted with the signing record. An unused
profile mints nothing, but its full wire piece must fit `OUTPACE_SEED_STORE_BYTES`. The same
one-piece minimum applies to the default and every valid persisted geometry on memory/disk.
Otherwise startup fails with `broadcast_store_too_small` before normalizer/cache/identity work.
Legacy consumer-valid geometry, including 98304-byte pieces, is preserved when unasserted.

Existing records retain exact transport/key bytes. Explicit fields must match raw stored
values; explicit defaults can conflict with absent fields. Conflicts fail startup without a
source too; configured names with invalid state fail closed, while unrelated invalid names
remain tombstoned. Actual reload rechecks state before reserving/storing, with existing cleanup
on failure and no new rollback guarantee. `OUTPACE_TRACKERS` affects fresh identities only;
resumed announcements retain stored lists, including empty lists. New options/trackers require
a new name or explicit broadcast DELETE/recreate, losing the previous identity and cursor
continuity. These settings change no bind, seeding, source-address or tracker-address policy.

## Managed source declarations

Both Compose examples forward `OUTPACE_BROADCAST_SOURCES` as `{}` when unset and
`OUTPACE_ALLOW_NON_GLOBAL_BROADCAST_SOURCE` as `0`. Listener/cache/seeding defaults are
unchanged. Declarations use one JSON object; for example, in a protected deployment env file:

```dotenv
OUTPACE_BROADCAST_SOURCES='{"demo-source":{"url":"https://source.invalid/live.ts?token=SYNTHETIC_TOKEN"}}'
OUTPACE_ALLOW_NON_GLOBAL_BROADCAST_SOURCE=0
```

Replace the synthetic URL with an authorized public, unencrypted MPEG-TS source. Use your
existing protected environment delivery and restrict env-file/process/container inspection;
URLs may contain private query tokens. Avoid printing the environment, `docker compose config`
output or full serialized config in logs/support bundles. No new secret store is added and
source URLs are never persisted in broadcast state. JSON must survive shell/Compose quoting,
including any literal dollar signs in tokens; single-quote dotenv values as above. An explicitly
empty source-map variable is invalid, so the forwarding uses an unset-only default.

The complete map validates before startup effects: <=64 valid names, <=256 KiB UTF-8,
<=8,192-byte URLs, duplicate/unknown keys rejected. Timing defaults are connect 5s/read 15s/
reconnect initial 1s/maximum 30s; ranges and all API states/errors are documented in
[native API](native-api.md#managed-http-mpeg-ts-sources). HTTP/owned RTMP startup precedes
sorted source admission and API traffic; upstream downtime is accepted, local reservation
failure unwinds earlier broadcast work and HLS. `202` means accepted, so inspect source status
for health rather than treating container `/healthz` as proof of origin progress.

Current environment declarations reattach after every daemon restart. API attachments last
only for that process. Detach or full delete suppresses a declaration only until restart;
remove it from the deployment environment to keep it absent. Detach keeps the signing
identity, cache lease/store and announcement pair; full delete removes identity. Use detach
before changing a source URL or timing; pull claims remain exclusive during backoff/failure.
A finite HTTP resource is treated as a live source: it reconnects after EOF and drops partial
tails, unlike ordinary finite raw/RTMP ingest. Storage reservations create intentional gaps
on reconnect/restart; follow the backup/older-binary/platform caveats below.

Only exact `1` enables the independent non-global source switch. It adds private/loopback/
CGNAT IPv4 and ULA/loopback IPv6, including mapped equivalents; link-local metadata endpoints,
unspecified/multicast and IPv4 broadcast/documentation remain blocked. Transport/tracker
switches do not authorize sources. Every attempt re-resolves and pins a permitted address,
ignores proxies, rejects redirects and preserves normal TLS verification. Protect the existing
native HTTP mutations and RTMP listener using the same exposure controls as other ingest.
Shutdown owns broadcast workers/announcements and the RTMP listener; established RTMP
connections belong to the dependency and can remain connected with all broadcast operations
fenced. The supported cleanup boundary does not promise to terminate those sockets.

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

### Ace HTTP compatibility

The [supported Ace HTTP subset](protocol/compat-matrix.md) is off by default. Set
`OUTPACE_ACE_COMPAT=1` in `.env` to enable it, or `OUTPACE_ACE_COMPAT=0` to explicitly
disable it. Apply the setting by recreating the service with `docker compose up -d --wait`;
`docker compose restart` alone does not apply changed container environment variables.

`OUTPACE_EXPERIMENTAL_ACE_COMPAT` is a deprecated fallback only when the canonical variable
is unset. Rename the old setting to `OUTPACE_ACE_COMPAT` when migrating. A present canonical
value wins, including `0`/`false`; the canonical `0` in a newly copied `.env.example`
therefore overrides a legacy `1`. The selected value accepts exactly `1`, `true`, `0`, or
`false`. Empty, malformed, or non-Unicode selected values fail daemon startup; unused legacy
values are ignored. Native daemon installations must restart the process to apply changes.

Generic Compose forwards the selection as
`OUTPACE_ACE_COMPAT: ${OUTPACE_ACE_COMPAT-${OUTPACE_EXPERIMENTAL_ACE_COMPAT-0}}`.
The unset-only fallback preserves legacy-only opt-in and empty values for runtime validation.
It does not enable any additional compatibility methods or change listener exposure.

## Upgrade, rollback, and removal

Back up the `outpace-data` volume before an upgrade. It contains `identity.seed` and persisted
broadcast records under `broadcasts/`; losing it changes the node identity and minted broadcast
identities. Cache contents under `cache/` do not need backing up.

Run exactly one daemon per data directory. Signing records and piece reservations assume
exclusive ownership and storage that honors checked sync/replacement operations. Protect the
data directory: its broadcast JSON files contain private signing keys (Unix temporary files
are created with mode `0600`; other platforms use the directory's access controls).

Broadcast indexes are durably reserved in 128-piece blocks before publication. Restart skips
unconsumed reservations and abandoned pieces, so gaps are expected. An unreadable/corrupt
record blocks admission for that name; it is never silently replaced. Deliberately delete the
broadcast through the API to remove its identity, or repair state while the daemon is stopped.
Do not edit an active record or restore an old signing/cursor backup under the same identity:
a stale but syntactically valid backup can reuse previously published indexes.

Linux requires file and affected-directory/new-link synchronization; macOS uses file full-sync
and distinct directory `fsync`. The stronger crash/deletion boundary depends on filesystem and
storage support. Windows supports process-crash/ordinary restart while OS/filesystem state
survives, with checked file-sync/replacement/delete ordering and no directory-sync or elevated
execution requirement. It does **not** promise safety after system crash/power loss: reservation
rollback can reuse indexes, identities can disappear and deleted records can reappear. Other
filesystems whose stronger semantics are not established share this limitation. Successful
ordering/fault-injection tests do not certify all filesystems against power loss.

Older binaries ignore the new `reserved_until` field and use the unsafe checkpoint scheme.
For an active persistent broadcast, keep the new binary. Before rolling back, stop origination,
deliberately remove that identity, then originate a fresh identity after rollback. Restoring
an older data-volume snapshot is subject to the same index-reuse hazard; a normal backup alone
does not make rollback safe for a surviving signing identity.

Shutdown closes/cancels broadcast admissions before HTTP drains. Cleanup shares a 5-second
grace and retains stopping fences for unfinished OS I/O; it cannot cancel a stuck syscall or
promise immediate blocking-pool/process exit. Full DELETE can return `500` for persistence
failure or `503` for cleanup timeout instead of acknowledging early. Retry a settled failed
delete explicitly. RTMP shutdown owns the listener and broadcast-operation barriers, while
established dependency-owned connection tasks may remain alive with their producer authority
fenced. Bind, cache, seeding and tracker defaults are unchanged.

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

For bounded playback validation and the limits of soak evidence, use the
[live gateway checklist](testing/live-gateway-checklist.md).

### Provider session admission

`OUTPACE_MAX_SESSIONS` defaults to unlimited (unset or `0`). Opt in with,
for example, `OUTPACE_MAX_SESSIONS=8`; invalid or overflowing values reject
daemon startup. This shared live/VOD resource cap includes pending provider
work. Existing same-key fans and HLS wrappers reuse their source slot. Resolved
VOD handles and active ranges keep their shared slot through cache expiry.

At saturation new native/direct playback starts return 503 and
`Retry-After: 1`. Compatibility JSON preserves its error envelope; metadata-only
negotiation can precede a later playback refusal. Native stream status and
service `get_status` show `occupied_sessions` and `max_sessions` (null when
unlimited). `active_sessions` retains its existing meaning.

Stopping or reaping a map entry does not promise immediate capacity: retained
owners and asynchronous pump destruction still count. The cap bounds ownership,
not exact socket counts, RSS, or aggregate seed-store bytes. No cache, seeding,
bind, container, or CLI play defaults change.
