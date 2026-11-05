# Autonomous public live support

Outpace can keep declared public live sources active and offer verified pieces to peers
without a local player. The declaration owns a source pin, not a viewer or a second media
queue. Playback and HLS clients can join the same manager source and share its provider
session slot. This is an operator-controlled public node; private peer mode is incompatible.

Set `OUTPACE_SUPPORT_SOURCES` to a JSON object mapping operator names to existing ACE live
selectors, for example:

```sh
export OUTPACE_SUPPORT_SOURCES='{"relay":"local:example"}'
outpace serve
```

This example requires an existing local descriptor alias named `example`; it does not create
one. The CLI's existing content-id, verified-infohash, local-descriptor and transport-URL
selector grammar applies. Resolution still enforces descriptor provenance and protection
refusal. VOD is rejected. Never put credentials into declaration names.

Unset means off. The JSON input is limited to 64 KiB and eight declarations. Names must be
1–64 ASCII letters, digits, underscores or hyphens. Input selectors are limited to 4096 bytes.
Duplicate keys, malformed selectors and incompatible configuration fail before identity/cache
creation. Declarations require ACE networking, inbound serving and reciprocal seeding enabled;
configuration never silently turns these on. The configured sources start only after required
HTTP, peer and RTMP listener binds and broadcast initialization succeed.

## Control and recovery

The native API uses the daemon's existing HTTP bind and access controls. Keep it on a trusted
management network; these routes add no authentication layer or exposure default.

- `GET /support` lists declaration name, desired epoch, observed state, static reason,
  attempt count, emitted-source byte snapshot, retry delay and whether a transition is pending.
  It omits selectors and provider diagnostic text. An unconfigured daemon returns an empty list.
- `POST /support/relay/stop` disables that declaration's support ownership.
- `POST /support/relay/start` enables it, or restarts a terminal error when safe.

POST bodies must be empty. Accepted commands return HTTP 202 with `running` (desired intent)
and `epoch`. This acknowledges the command; it does **not** claim source cleanup has finished.
Poll status for the observed transition. Unknown declarations return 404 and cannot be created
through HTTP. Invalid/nonempty bodies are rejected without changing intent; body validation
has finite time and frame limits. Repeating an unchanged desired command is idempotent.

A declaration stop releases only its pin. A real viewer, HLS consumer or another declaration
can keep the shared source active. Force-stopping the stream through the existing stream API
also disables matching declarations, so support will not immediately recreate it. An ordinary
playback request does not reenable support; explicitly start the declaration to do that.

Transient network, metadata-discovery and admission failures retry after 1, 2, 4, 8, 16 and
then at most 30 seconds. Opening a connection alone does not reset this backoff: reset requires
30 observed seconds of emitted-byte progress. Invalid, unsafe or protected descriptors stop
with a static error until explicitly restarted. Natural source EOF allows a fresh supervised
generation; existing playback clients of the ended generation observe EOF and reconnect.

Shutdown closes support admission, cancels opens/retries and releases pins, joins the service,
then shuts down the shared manager. Admission capacity remains held through pending source
cleanup. Failed cleanup is an error, blocks automatic or explicit declaration restart, and
retains uncertain source ownership/capacity for the manager lifetime. Investigate operational
logs rather than treating this as a successful stop or readiness result.

## Ports, discovery, persistence and bounds

The upstream source, this support node and downstream peers use the existing ACE peer wire
and signed verified piece store. Support does not originate or transcode the stream. Configure
`OUTPACE_PEER_LISTEN` and the existing tracker/DHT, port-mapping and public-reachability options
for the deployment. A listening port alone does not establish remote reachability. Support
reuses public catalog and discovery paths; it adds no independent tracker or DHT protocol.

Keep the existing node identity and configured cache directories across restarts. Declarations
persist in operator configuration; desired API changes and workers are not persisted. Restart
reapplies configured declarations. A disk cache is not a durable source lease and cannot replace
upstream acquisition after restart. Existing cache memory/disk bounds remain in force.

`OUTPACE_MAX_SESSIONS` remains unlimited when unset/zero. A positive cap is shared with live,
VOD and HLS source resources, including pending opens and retained cleanup. At the cap, existing
sources remain joinable; new support acquisition waits through bounded retry. Existing inbound
global/per-stream limits and unchoke scheduling still apply. Support creates no playback receiver
and does not count as a viewer. It still uses the normal source pump and media processing;
parser avoidance or a measured throughput improvement is not claimed.

This increment establishes configuration, supervision and native lifecycle controls. Full signed
source → daemon-owned support → independent leecher validation and comparative resource evidence
remain issue #270's final integration stage. Earlier signed pool tests do not by themselves prove
that daemon composition. No official-engine interoperability or sustained readiness claim follows.
Already admitted inbound snapshot readers and dependency-owned RTMP connections have independent
lifetimes; joining the support source is not proof that all such consumers have disappeared.
