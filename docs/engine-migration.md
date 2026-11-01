# Migrating an engine installation to Outpace

This is an operator migration guide for the v1.0 milestone, not a production-readiness
signoff. v0.9 is published; #195 and #246 hardening remains unfinished. Verify the exact
binary/image you select: current source documentation can describe changes newer than a
published image. No runtime or official-engine interoperability qualification is implied.

Outpace runs independently of the closed engine. Use the native CLI and
[native API](native-api.md) first; the [Ace HTTP subset](protocol/compat-matrix.md) is an
optional adapter, not complete engine parity. Public, unencrypted content is the scope.
Premium/encrypted content, account/subscription import, player/web UI, and a closed-engine
runtime remain permanent non-goals (#212). Service-backed name search is a separate
post-v1.0 possibility, default off and not implemented; it is not a migration prerequisite.
Protected-descriptor refusal is still tracked in [#279](https://github.com/jamezrin/outpace/issues/279);
do not treat a reported zero encryption flag as independent proof that content is public.

## Inventory and configuration mapping

Record the old binary/image version, listener exposure, data backup, consumers and expected
playback routes before changing anything. Do not translate an engine configuration file or
copy its private identity/account database into Outpace. There is no account importer.
Configure `outpace serve` through environment variables; see the full
[configuration reference](../README.md#configuration).

| Operator concern | Outpace setting and behavior |
| --- | --- |
| HTTP API/playback | `OUTPACE_BIND`, native default `127.0.0.1:6878` |
| RTMP ingest | `OUTPACE_RTMP_BIND`, native default `127.0.0.1:1935` |
| Inbound peers | `OUTPACE_PEER_LISTEN`, default `0.0.0.0:8621`; TCP |
| Participation | `OUTPACE_ENABLE_INBOUND=1` and `OUTPACE_ENABLE_SEEDING=1` by default; choose both explicitly |
| Router automation | `OUTPACE_ENABLE_PORT_MAPPING=0` by default; keep off for bridge containers |
| Persistent identity | `OUTPACE_DATA_DIR`, default platform data directory plus `outpace`; container `/var/lib/outpace` |
| Piece cache | `OUTPACE_CACHE_TYPE=memory` by default; disk cache is ephemeral, not a media archive |
| Provider session admission | `OUTPACE_MAX_SESSIONS` unset or `0` is unlimited; a positive value such as `8` is explicit opt-in, not a viewer limit |
| Legacy HTTP clients | `OUTPACE_ACE_COMPAT=1` opts in; default off |
| Origination | `outpace broadcast <name>` creates/resumes a signed identity and serves ingest; no fictitious broadcast-enable variable is needed |

`OUTPACE_ACE_COMPAT` is canonical. The deprecated `OUTPACE_EXPERIMENTAL_ACE_COMPAT`
is consulted only if the canonical variable is absent. Present canonical `0`/`false` wins
over legacy `1`; accepted values are exactly `1`, `true`, `0`, `false`. Empty or invalid
selected values fail startup; unused legacy values are ignored. Restart native processes,
or recreate containers, after environment changes. Root Compose forwards the unset-only
fallback; a new `.env.example` explicitly sets canonical `0`, overriding legacy opt-in.
Arbitrary variables in `.env` are not automatically passed to containers.

Outpace creates a new stable node identity in `identity.seed`. Keep the same writable data
path and service user across restarts. Signed broadcasts persist private keys and cursor
reservations in `broadcasts/`. Run only one daemon per data directory. Protect that directory
and backups; never transplant closed-engine private identity. See the
[storage and rollback caveats](deployment.md#upgrade-rollback-and-removal).

## Network and compatibility expectations

HTTP 6878 and RTMP 1935 stay loopback-only in the examples. The native API has mutating
operations: remote exposure requires an operator-managed firewall/authenticated proxy.
Enabling Ace compatibility adds no engine access-token authentication. Check returned
playback URLs behind proxies: the adapter uses HTTP and the request Host, not forwarded
scheme/prefix headers. Unknown legacy methods, UI, search, seek and file selection are
not made available by enabling the adapter.

For full P2P participation, publish and manually forward external **TCP 8621 to TCP 8621**
on the host. The daemon self-announces that port; changing only a Docker host mapping
breaks reachability. Docker does not configure the router. There is no UDP 8621 listener:
DHT uses ephemeral outbound UDP sockets; allow outbound UDP and established replies.
UDP tracker traffic is outbound too. There is no legacy port 62062 parity.

Discovery through trackers/DHT and successful catalog resolution do not guarantee peers,
media availability or decode progress. Content ids resolve via the authenticated catalog;
raw infohash/magnet syntax alone does not prove a verified descriptor is available. Do not
assume identical legacy timing, buffering, error codes or discovery behavior. For broadcast
origination, `OUTPACE_TRACKERS` replaces defaults for fresh identities when nonblank;
Outpace self-announces only to lowercase `udp://` entries. Resumed identities retain their
stored trackers. Changing that environment cannot redirect a persisted identity. Tracker
selection is not private-distribution policy. See [known differences](protocol/compat-matrix.md).

## Two standalone Compose profiles

Save **one** block as `migration.compose.yml` in a private deployment directory. These are
alternative standalone files, not overrides of root `compose.yml`: do not combine them with
`-f compose.yml`. Compose normally merges port lists; an override could retain an unwanted
peer/RTMP publication. Each file below explicitly sets all exposed ports and policy gates.
They use the image's non-root `outpace` user and a named volume initialized from its owned
data path. For a bind mount instead, provision ownership for the selected image's actual
UID/GID first; do not solve permissions by running as root or using world-writable storage.

Choose an available immutable image digest or explicit release tag after checking its feature
contract, then export it (no `latest` or `edge` default is supplied):

```bash
export OUTPACE_IMAGE="${OUTPACE_IMAGE:?Supply the reviewed release tag or digest}"
docker compose -p outpace-migration -f migration.compose.yml config --quiet
```

Selecting a tag is not evidence that its behavior matches this source.
Use a digest for an immutable deployment. `config` checks Compose structure/interpolation,
not image existence, runtime configuration validity, permissions or stream readiness.

### Leecher gateway

This choice disables inbound peer serving **and** reciprocal uploads. Only host-local HTTP
is published; RTMP remains inside the container. It deliberately differs from the root
Compose full-participant defaults.

```yaml
services:
  outpace:
    image: ${OUTPACE_IMAGE:?Set an explicit image tag or digest}
    command: [serve]
    environment:
      OUTPACE_BIND: 0.0.0.0:6878
      OUTPACE_RTMP_BIND: 127.0.0.1:1935
      OUTPACE_DATA_DIR: /var/lib/outpace
      OUTPACE_ENABLE_INBOUND: "0"
      OUTPACE_ENABLE_SEEDING: "0"
      OUTPACE_ENABLE_PORT_MAPPING: "0"
      OUTPACE_ACE_COMPAT: "0"
      OUTPACE_MAX_SESSIONS: "0"
    ports:
      - "127.0.0.1:6878:6878/tcp"
    volumes:
      - migration-data:/var/lib/outpace
volumes:
  migration-data:
```

### Signed broadcast origin

`synthetic-origin` below is an invented local broadcast label. The actual CLI command mints
or resumes its signed descriptor at startup. This CLI subcommand forces inbound serving on;
use `serve` for the leecher profile. Inbound serving and seeding are explicitly on;
RTMP ingest and HTTP remain host-local. Retain the named volume for stable origination.

```yaml
services:
  outpace:
    image: ${OUTPACE_IMAGE:?Set an explicit image tag or digest}
    command: [broadcast, synthetic-origin]
    environment:
      OUTPACE_BIND: 0.0.0.0:6878
      OUTPACE_RTMP_BIND: 0.0.0.0:1935
      OUTPACE_PEER_LISTEN: 0.0.0.0:8621
      OUTPACE_DATA_DIR: /var/lib/outpace
      OUTPACE_ENABLE_INBOUND: "1"
      OUTPACE_ENABLE_SEEDING: "1"
      OUTPACE_ENABLE_PORT_MAPPING: "0"
      OUTPACE_ACE_COMPAT: "0"
      OUTPACE_MAX_SESSIONS: "0"
    ports:
      - "127.0.0.1:6878:6878/tcp"
      - "127.0.0.1:1935:1935/tcp"
      - "0.0.0.0:8621:8621/tcp"
    volumes:
      - migration-data:/var/lib/outpace
volumes:
  migration-data:
```

Once deliberately started, feed an authorized unencrypted MPEG-TS file using native ingest:

```bash
# Operator supplies MEDIA_FILE; the response contains private identifiers: keep it local.
curl --fail-with-body --max-time 60 -X PUT \
  --data-binary "@${MEDIA_FILE:?Set a local MPEG-TS file path}" \
  http://127.0.0.1:6878/broadcast/synthetic-origin
```

RTMP publishers can instead target `rtmp://127.0.0.1:1935/live/synthetic-origin`.
For managed HTTP pull, use the [source API](native-api.md#managed-http-mpeg-ts-sources)
with a protected JSON request file and `--data-binary @file`, keeping URL credentials out of
argv. API source attachments are process-local; restart declarations require explicitly
forwarding `OUTPACE_BROADCAST_SOURCES`. Neither minting nor HTTP `202` proves ingest progress.
Profile geometry/metadata settings use the verified
[broadcast options schema](native-api.md#broadcast-startup-profiles), not account imports.

## Bounded validation and cutover

For a native binary, keep the old executable and its separate data backup. Select a reviewed
Outpace executable and a new private directory writable by its service user, then run in a
terminal under that user (set `OUTPACE_DATA_DIR` to that directory first):

```bash
OUTPACE_DATA_DIR="${OUTPACE_DATA_DIR:?Set a new persistent directory}" \
OUTPACE_BIND=127.0.0.1:6878 OUTPACE_RTMP_BIND=127.0.0.1:1935 \
OUTPACE_ENABLE_INBOUND=0 OUTPACE_ENABLE_SEEDING=0 OUTPACE_ACE_COMPAT=0 \
  outpace serve
```

For containers, after reviewing `config --quiet`, start only when the operator chooses:

```bash
docker compose -p outpace-migration -f migration.compose.yml up -d
curl --fail --max-time 5 http://127.0.0.1:6878/healthz
curl --fail --max-time 5 http://127.0.0.1:6878/networks
curl --fail --max-time 5 http://127.0.0.1:6878/streams
```

`/healthz` proves HTTP liveness, `/networks` registered providers, and `/streams` active
sessions; none proves upstream reachability, media readiness, decoder continuity or capacity.
Use an operator-provided current public source, with a bounded observation window:

```bash
# Supply CONTENT_ID through the operator's environment; do not paste real ids into docs/logs.
curl --fail --max-time 30 --output /dev/null \
  "http://127.0.0.1:6878/streams/ace/cid:${CONTENT_ID:?Supply a public content id}"
```

Continuous live media commonly ends this bounded curl with timeout exit 28; distinguish
received bytes from no progress. Then validate in the intended player, inspect the per-stream
`/status` privately while active, and test stop/reconnect and required compatibility routes.
Do not publish identifiers, response bodies, stream names or transport captures. The ignored
`acestream-ids.txt` registry is LLM context only; no command, harness or code reads it.

Keep the old installation available until validation passes. If both use 6878/1935, stage
Outpace on alternate loopback HTTP/RTMP ports (and adjust validation URLs), or on another host.
Do not blindly run both public peer listeners on 8621. Coordinate an explicit final port
handoff; stop the old service only after staged validation, then recheck the final routes.
The operator bounds the window and decides cutover; no automatic Docker actions are required.

Back up Outpace state while stopped and retain old-engine backups separately. Roll back by
stopping Outpace and restoring the old executable/container and its own configuration/data;
never feed Outpace identity to the old engine. Preserve the Outpace volume for a later retry;
`down --volumes` destroys it. For Outpace-to-Outpace downgrades with active signed broadcasts,
ordinary snapshots are insufficient: stale cursors can reuse signed indexes. Stop origination,
remove the identity deliberately and use a fresh identity after downgrade, following the
[platform and rollback constraints](deployment.md#upgrade-rollback-and-removal). Cache files
are disposable; signing keys and reservations are not. Revalidate playback after rollback.
