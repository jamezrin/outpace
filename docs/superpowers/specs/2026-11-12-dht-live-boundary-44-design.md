# DHT live comparison boundary design (#44)

Status: implementing. Research tooling only; production discovery, root manifests and
daemon defaults stay unchanged.

## Problem

Offline qualification (#320–#332) proved candidate behavior against loopback nodes.
A public comparison additionally needs one destination and server policy that holds
for every thread and socket a candidate creates, including library actor threads the
adapter cannot observe. In-process checks cannot provide that. The validation host
already forces public IPv4 through its VPN interface. Its egress table still accepts
LAN prefixes, its own loopback, and any IPv4 destination through the VPN, including
private ranges that a DHT response may advertise. The host has no TUN device. Its
firewall, routing and DNS must not change.

## Selected design

Each worker runs in fresh nested user, network and PID namespaces created by the
supervisor. A fixed bootstrap is namespace PID 1:

1. Disables IPv6, creates a veth pair (`dhtw0` for the worker, `dhtr0` for the relay),
   assigns `10.200.0.2/30` to `dhtw0`, adds a permanent neighbour entry for gateway
   `10.200.0.1`, and routes the default route through it.
2. Loads a default-deny nftables table. Output accepts the inner loopback, and UDP to
   IPv4 destinations outside the special-purpose prefixes. Input accepts the inner
   loopback, and conntrack-established UDP replies. Every other packet is counted
   and dropped.
3. Verifies the loaded ruleset structure, opens an `AF_PACKET` socket on `dhtr0`, and
   passes it with an attestation to the supervisor over a sequenced-packet socket.
4. After the supervisor accepts the attestation, forks the worker. The worker has
   empty capability sets and `no_new_privs`, so it cannot alter the policy.
5. Samples the worker's thread count, collects `wait4` resource usage, and reports
   nftables counters after the worker exits.

The supervisor relay reads frames from `dhtr0`. It applies the same destination
policy, independently of nftables, and forwards UDP payloads from root-namespace
sockets bound to the egress device. Replies are admitted only from endpoints that the
same inner socket contacted. Unsolicited inbound traffic is counted and dropped, so
no candidate can act as a reachable DHT server. Mappings, contacts, payload sizes and
datagram totals are bounded.

There are three independent layers: the relay policy, the inner nftables policy, and the
existing host egress table and VPN gateway. Nothing on the host changes. Each worker's
namespace, sockets and relay state are destroyed before the next arm starts.

Rejected alternatives: a host firewall table or cgroup BPF changes host policy;
pasta or slirp4netns needs a TUN device; seccomp address inspection is racy and
library-specific; in-process wrappers cannot cover library-owned sockets.

## Worker gate

Live entries (`dht-worker --live-worker mainline|n0` and the custom libtest
`custom_live_worker`) require mode `live-v1` and global unicast IPv4 seeds. Before
reading protected input, they check the following through per-namespace procfs:

- Empty inheritable, permitted, effective, bounding and ambient capability sets.
- `no_new_privs` set.
- Exactly `lo`, `dhtw0` and `dhtr0`, with the fixed main-table routes.
- IPv6 disabled.

PID-namespace ownership, nftables and relay enforcement are attested by the
supervisor, not by the worker.

## Measurements

The worker clock is unchanged: a 15-second absolute budget that includes construction,
a target of eight unique IPv4 endpoints, and a cap of 1,024. Boundary counters are
uniform across candidates:

- Admitted and denied datagrams by reason.
- Unique contacted endpoints, and endpoints that never replied.
- Admitted replies and unsolicited drops.

These are measured at the boundary, not library-internal query diagnostics.
Per-arm `wait4` peak RSS and CPU time, and sampled peak thread counts, are measured
process facts. Unavailable library counters remain null with their reason.

## Live block

The block has one absolute 120-second deadline and uses a fixed order: custom,
mainline, n0. Each worker envelope is 30 seconds. The steps are:

1. Verify artifact pins, the single default route through the declared egress
   device, disabled IPv6, and the host route and stateless ruleset digests.
2. Resolve the target infohash once through the local daemon's loopback
   `analyze_content` method from a content id on protected stdin. Resolve the
   production bootstrap hostnames once into at most four global IPv4 seeds, shared by
   every arm, and report DNS time separately.
3. Run the three arms, stopping on any cleanup uncertainty.
4. Re-check the host digests and write one private numeric report.

The block has no retries and no announcements. Gateway and VPN exit provenance is
verified separately by the operator immediately before and after.

## Qualification

Offline qualification runs the identical supervisor, bootstrap and relay inside an
isolated outer namespace whose dummy egress device carries synthetic, globally
scoped addresses and nothing else. It covers:

- A probe worker exercising the destination matrix across threads and sockets.
- Unsolicited and wrong-port inbound traffic.
- Denial of policy mutation and raw sockets.
- Gate denials outside the boundary.
- Real candidate positives through binary entries.
- Candidate attempts to query advertised private nodes, which must be counted and blocked.
