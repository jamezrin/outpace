# Strict outbound-only private peer policy (#271)

The user selected an exact peer endpoint allowlist, disabled public discovery and
advertising, and disabled inbound peer connections. Public defaults remain unchanged.
This opt-in policy is a peer boundary, not encryption, anonymity, or a process firewall.

## Architecture

One immutable `PeerPolicy` holds at most 256 numeric IPv4 endpoints. Configuration
rejects malformed/empty lists, unsupported DNS/IPv6 and unusable endpoints. An Arc
shares the set; admission uses expected constant-time lookup with no actor-side
locks, queues or additional workers. Candidate ingestion filters hints, and every
live startup/refill, metadata and VOD worker dial independently enforces policy.

Private provider/session construction uses the configured endpoints, suppresses
public catalog/tracker/DHT work before polling, isolates persistent warm state and
prevents builder order from restoring public announcements or reachability observers.
Transport metadata cannot widen policy. BEP9 raw-hash fallback remains opt-in and
confined to allowed peers; it never grants catalog-authenticated identity trust.
Remote descriptor URLs and eager compatibility fetch paths are refused.

Runtime validation precedes identity/cache creation and worker startup. Implicit
public inbound/mapping defaults normalize off; explicit enabling and public tracker
configuration conflict. Final Rust/Serde configurations must explicitly disable
inbound/mapping; they cannot infer environment-setting presence. Additional bootstrap
or CLI peers must be subsets. Invalid settings updates are transactional. Native
HTTP/RTMP/control services retain their existing independent bindings and privileges.

No peer listener, NAT mapper or public announcer is created in private mode.
Signed live activation, gossip and metadata handshakes advertise listening port zero. Broadcast
announcement admission is guarded separately from the advertised port. The broadcast
CLI refuses private mode because its origin distribution requires inbound peers;
this issue does not add an outbound private origin-push service. Verified reciprocal
uploads remain possible on admitted outbound connections.

## Local metadata and UX

Operators may declare up to 64 aliases with an absolute file path. Declaration input
is bounded; files are regular, daemon-owned, not group/world writable, and at most
1 MiB. Root-anchored directory-descriptor traversal uses no-follow checks for every
component. Validated parsed live/VOD metadata is retained as an immutable snapshot.
Requests select `local:<alias>` and never open files. Protection/geometry checks and
existing source authentication remain; local metadata does not populate the catalog
identity index. CLI, native and compatibility selectors share that snapshot.

Native `/network-policy` and compatibility status report only mode and allowlist
count. Private endpoint and packet-preview diagnostic labels allocate no strings and
hide values; public formatting remains. Operator configuration serialization retains
private declarations for round-tripping and is not used for public status. Refusals
carry stable messages on native live/HLS/VOD and compatibility entry points.

## Alternatives

Filtering initial discovery results would miss reconnect, VOD and metadata paths.
A process-global socket hook would obscure ownership and conflate peer restrictions
with independent HTTP/control traffic. Explicit typed policy at peer sinks plus
construction-time suppression preserves reviewable boundaries and public behavior.
An inbound IP-only approximation would weaken the selected endpoint boundary;
private inbound operation is deliberately unsupported.

## Required evidence

- Bounded parser, exact address/port and immutable sharing controls; deny before dial
  factory invocation and allow real loopback sockets.
- Signed private follower progress with zero public factory calls, unchanged supplied
  warm state, verified publication and bounded cancellation.
- VOD allowed verified range and denied peer controls; owned live/VOD metadata serving
  without catalog polling or identity-index insertion.
- Rooted file traversal, protection/type checks, snapshot behavior, strict aliases and
  a deterministic parent-symlink substitution regression.
- Configuration conflicts/round-trip/transactionality, invalid runtime rejection before
  filesystem effects, no private broadcaster construction and CLI override prevention.
- Native/compat status and refusal tests; real captured private logs plus public controls.
- Positive operator local-alias playback through actual runtime/provider entry points,
  no forbidden runtime peer listener/mapping/announcement admission, full repository
  gates, independent security/test-fidelity review and CI before integration.

No live internet or closed-engine execution is required for these local negative and
positive proofs. The issue stays open until every acceptance item has retained evidence.
