# Outbound-only private peer mode

Private peer mode permits peer sockets only to an operator's exact IPv4 endpoint
allowlist. It disables inbound peer listening, NAT port mapping, public tracker/DHT
discovery, public announcements, and persistent warm-peer hints. Public defaults
are unchanged when `OUTPACE_PRIVATE_PEERS` is unset.

An allowlisted source must accept inbound peer connections. Two outbound-only
private nodes cannot establish a connection to each other. Verified reciprocal
uploads can use an existing allowed outbound connection; this mode does not add
private origin push/distribution or cryptographic peer membership.

## Configure a source

Use numeric IPv4 addresses with explicit nonzero ports. Hostnames, IPv6, wildcard,
multicast and broadcast endpoints are rejected. At most 256 comma-separated entries
are admitted, with an 8 KiB input limit; duplicates are collapsed. An explicitly
empty setting fails startup instead of selecting public mode.

```sh
export OUTPACE_PRIVATE_PEERS='192.0.2.10:8621,192.0.2.20:8621'
export OUTPACE_PRIVATE_DESCRIPTORS='{"fixture":"/srv/outpace/fixture.acelive"}'
outpace serve
```

These addresses and aliases are synthetic examples. Supply your own reachable
source endpoints and operator-owned descriptor files. `OUTPACE_PRIVATE_DESCRIPTORS`
is a JSON object, limited to 64 entries and 64 KiB. Aliases contain 1–64 ASCII
letters, digits, underscores or hyphens. Paths must be absolute and use no symlink
components. Each file must be regular, owned by the daemon's effective UID, not
group/world writable, and at most 1 MiB. Admission uses descriptor-relative
no-follow traversal; requests never open a path. Files are read and validated once
before network startup. Replacing a file later does not change the running snapshot.
Descriptor admission currently requires Unix file semantics.

Supported metadata is the existing wrapped live/legacy VOD transport or single-file
VOD metainfo/disk envelope. Geometry and content-protection checks still apply;
protected or invalid protection metadata is refused at startup. Operator admission
does not make a descriptor catalog-authenticated or make its bare swarm identity
openable. Existing source-signature and verified-store rules are unchanged.

## Select an admitted alias

```sh
outpace play local:fixture
# For a VOD descriptor:
outpace play --vod local:fixture

curl 'http://127.0.0.1:6878/streams/ace/local%3Afixture.ts'
curl 'http://127.0.0.1:6878/vod/ace/local%3Afixture'
curl 'http://127.0.0.1:6878/network-policy'
```

Live/VOD selection must match the admitted descriptor type. With the compatibility
API explicitly enabled, `id=local:fixture` selects the same snapshot. Public catalog
and HTTP descriptor URL resolution are disabled. An explicit
`OUTPACE_BEP9_CONTENT_ID_FALLBACK=1` still permits raw transport-hash resolution
through allowed peers only; it does not establish a public content-ID or bare
infohash trust binding. Descriptor trackers, advertised source endpoints and cached
public discovery results cannot widen the allowlist.

`OUTPACE_ENABLE_INBOUND=1`, `OUTPACE_ENABLE_PORT_MAPPING=1`, and an explicitly set
`OUTPACE_TRACKERS` conflict with private mode and fail startup. Implicit inbound
and mapping defaults are disabled; explicit `0` values are accepted. Additional
`OUTPACE_ACE_PEERS` or CLI `--peer` entries must be valid subsets of the allowlist.
Inert peer-listen and mapping-address settings do not create listeners or mappings.
Serialized/programmatic configuration must explicitly disable `enable_inbound`
and `enable_port_mapping` before runtime construction; it has no environment
presence information from which to distinguish implicit defaults.

The `outpace broadcast` CLI refuses private mode because its peer-distribution
workflow requires inbound peers. Native broadcast control/ingest operations retain
their existing independent behavior, but do not announce or create a private peer
origin distribution service. Private mode does not imply that an ingested origin
is available to remote peer clients.

## Status and scope

`GET /network-policy` and compatibility `get_status` report mode and allowlist count,
with inbound peers/public discovery/public advertising disabled in private mode.
They do not publish endpoints or descriptor paths. Operator configuration
serialization preserves those declarations for round-tripping; protect configuration
files accordingly. Peer endpoint and packet-preview diagnostics are redacted in
private sessions, while public-mode diagnostics retain their existing format.

This is a peer-network policy, not anonymity, encryption or a host-wide firewall.
Allowed peers learn the requested swarm and can redistribute received data. HTTP,
RTMP, control API bindings and explicitly configured HTTP broadcast inputs remain
separate operator choices. Use loopback bindings or an authenticated gateway/firewall
for control access. Changing mode or declarations requires a restart; no HTTP request
can widen the running peer policy. Shared provider-session accounting remains
unlimited by default or capped by the existing `OUTPACE_MAX_SESSIONS` setting.

Private live activation, gossip and metadata handshakes advertise listening port zero.
This also applies to initially admitted peers and later actor-owned reconnects.
