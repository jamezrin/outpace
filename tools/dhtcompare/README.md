# DHT comparison research harness (#44)

Strict worker input parsing and bounded batch/deadline accounting are implemented.
All three actual implementations have passed target, silent-node, and
malformed/uncorrelated/duplicate-response cases inside the offline namespace helper.
The custom cases exercise ace-swarm's existing numeric-seed seam.
Fixed offline late-response-opportunity and external lookup-cancellation profiles
are available. Internal cancellation/actor joining, observed late datagram receipt,
and whole-harness/live qualification remain incomplete.
A default-deny live boundary and one bounded live-block supervisor are described
under [Live boundary](#live-boundary); this tool makes no library-adoption decision.
A Linux offline namespace helper has synthetic lifecycle tests; it is not live clearance.
The `candidates` feature is research-only, not a safe execution entry point.
Real-UDP integration tests are ignored by default and reject execution outside
a PID-1, loopback-only, unrouted namespace before opening sockets.

This is a standalone Cargo workspace to avoid production dependency/lockfile changes.
Before entering an offline environment, prefetch both workspaces' locked dependencies
while online. These commands preserve the committed lockfiles and do not perform
DHT lookups:

```sh
cargo fetch --manifest-path Cargo.toml --locked
cargo fetch --manifest-path tools/dhtcompare/Cargo.toml --locked
```

Use the repository's pinned Rust toolchain. The qualification scripts require Linux
and **Python 3.11 or newer** (`hashlib.file_digest` is required). The commands below
use `python3.11`; substitute a newer interpreter when appropriate.

Run offline contract tests with:

```sh
cargo test --manifest-path tools/dhtcompare/Cargo.toml --offline --locked
```

The worker contract accepts exactly twenty hex-decoded target bytes and one to four
unique numeric IPv4 seed endpoints with nonzero ports. Debug/errors redact identities
and endpoint values. Parsing permits loopback for confined synthetic tests and does
not authorize any network destination. Input must later come through protected
worker input/environment, never the registry or command arguments.

The design and implementation plan are in docs/superpowers under the DHT comparison
issue. Candidate source, clock/cleanup and real UDP qualification remain prerequisites.
No public/network comparison should be inferred from these parser tests.

## Fixed offline qualification

Build both ignored-test binaries without executing them:

```sh
cargo test -p ace-swarm --lib --offline --locked --no-run
cargo test --manifest-path tools/dhtcompare/Cargo.toml --all-features --offline --locked --test real_udp --no-run
python3.11 -m unittest discover -s tools/dhtcompare -p 'test_*.py'
```

Use the exact absolute executable paths printed by Cargo and their SHA-256 build
pins. The output directory must not exist and its parent must be private. Run:

```sh
python3.11 tools/dhtcompare/qualification.py --offline \
  --custom /absolute/path/to/ace_swarm-test-binary --custom-sha256 CUSTOM_BUILD_SHA256 \
  --upstream /absolute/path/to/real_udp-test-binary --upstream-sha256 UPSTREAM_BUILD_SHA256 \
  --output /private/path/to/new-run
```

This fixed nine-case suite has a 180-second deadline, separate namespaces per case,
bounded retained pipe prefixes, and exact-test execution checks. It stops on the
first failed case, truncation or uncertain cleanup. It requires Linux user, network
and PID namespace support plus `/usr/bin/ip` and `/usr/bin/unshare`; denial is a
failure, not a skip. Operator-supplied binaries are trusted: pins catch stale builds,
not hostile concurrent edits. Results and diagnostics use private file modes.
The runner accepts no live target, seed, registry or public-network mode. Its report
qualifies these tests only and cannot satisfy #44's live-results acceptance criterion.

## Client-mode and external lookup cancellation

Build the additional ignored fixtures without executing them:

```sh
cargo test --manifest-path tools/dhtcompare/Cargo.toml --all-features --offline --locked --test client_mode --no-run
```

The two `cold_client_mode` cases observe read-only flags on outgoing queries and
no reply to one unsolicited ping during a 250 ms window with an unfinished lookup.
They then release the normal target response. They require the same offline
namespace helper; they do not prove permanently disabled serving. The custom
walker does not emit the read-only flag; its wire behavior is recorded separately.

Run the two cold-client observations from the repository root, substituting the
absolute `client_mode` executable path and its SHA-256 build pin:

```sh
python3.11 - /absolute/path/to/client_mode-test-binary CLIENT_MODE_BUILD_SHA256 <<'PYTHON'
from pathlib import Path
import sys
sys.path.insert(0, str(Path("tools/dhtcompare").resolve()))
from isolation import run_offline
from qualification import Artifact

binary = Path(sys.argv[1])
artifact = Artifact(binary, sys.argv[2])
for name in ("mainline_cold_client_mode", "n0_cold_client_mode"):
    artifact.verify()
    result = run_offline(
        [str(binary), "--exact", name, "--ignored", "--nocapture"], timeout=8
    )
    passed = (
        result.exit_code == 0 and result.cleanup_complete
        and not result.timed_out and not result.truncated
        and f"test {name} ... ok\n".encode() in result.stdout
        and b"test result: ok. 1 passed; 0 failed; 0 ignored;" in result.stdout
    )
    print(name, {"passed": passed, "exit": result.exit_code,
                 "cleanup_complete": result.cleanup_complete})
    if not passed:
        raise SystemExit(1)
PYTHON
```

Both cases must execute exactly one test, exit zero and confirm namespace cleanup
without timeout or truncated output. Their assertions require read-only outgoing
queries, no unsolicited-ping reply during the 250 ms unfinished-lookup window,
and subsequent normal target completion. A zero-test match is a failure.

For the fixed external-cancellation block, pass the `client_mode` executable as
`--upstream` (instead of `real_udp`) with its own build pin. Keep the custom
ace-swarm test executable and its current build pin:

```sh
python3.11 tools/dhtcompare/qualification.py --offline --mode lookup-cancellation \
  --custom /absolute/path/to/ace_swarm-test-binary --custom-sha256 CUSTOM_BUILD_SHA256 \
  --upstream /absolute/path/to/client_mode-test-binary --upstream-sha256 CLIENT_MODE_BUILD_SHA256 \
  --output /private/path/to/new-cancellation-run
```

This profile has a 40-second whole-block deadline and three eight-second execution
timeouts, reserving another 2.1 seconds per worker for cleanup. After a real `get_peers`
query reaches the synthetic server, its flushed marker requests termination of the
owned namespace. Qualification requires that marker, a cancellation request,
SIGKILL status, confirmed cleanup and no early-return marker. Panic, normal exit,
ordinary timeout, truncation and cleanup uncertainty fail the profile. These
fixtures intentionally cannot pass as normal `--ignored` test runs; their success
is the external owner's witnessed cancellation result.

This proves process containment after a query witness and before any observed
return marker. Completion can race signal delivery, so it does not atomically prove
that the lookup remained pending at that instruction. It does not prove that
dropping a library stream joins its actor or interrupt a real constructor.
Adapter clock tests separately cover consumed construction budget and a stream
poll ending after its deadline. A blocking poll cannot be preempted by a Tokio
timeout; the external process deadline remains the hard bound. Observed late datagram
receipt, allowed destinations, live workers and live results remain pending.

## Emission accounting

Adapter records include `batch_sizes` (the first 64 emission sizes),
`batches_observed` (a saturating total), `batch_history_truncated`, and
`target_overshoot` (unique retained peers beyond the target). Empty and duplicate
emissions still consume history entries; retaining 64 entries never stops lookup.
The unique-peer cap and atomic target-crossing behavior remain unchanged.

These are adapter-boundary sizes, not raw UDP records. The custom walker counts
peers parsed from correlated responses, after its compact-endpoint parser filters
invalid entries. The upstream adapters count vectors yielded by each library,
after any library-specific filtering. The common collector then excludes zero
ports and duplicate endpoints. Equal batch sizes do not prove equal parser policy,
reachability, packet counts or discovery quality. Target overshoot explains a
crossing batch; it is not a ranking. No identifiers are retained in these fields.

## Beyond-budget UDP reply opportunities

The `late-replies` offline profile uses the same `real_udp` upstream binary as
initial qualification and the custom `ace-swarm` test binary. Build and pin both
as above, then run:

```sh
python3.11 tools/dhtcompare/qualification.py --offline --mode late-replies \
  --custom /absolute/path/to/ace_swarm-test-binary --custom-sha256 CUSTOM_BUILD_SHA256 \
  --upstream /absolute/path/to/real_udp-test-binary --upstream-sha256 UPSTREAM_BUILD_SHA256 \
  --output /private/path/to/new-late-reply-run
```

The fixed block has three cases, 22 seconds per execution, and an 80-second total
budget including namespace cleanup. Every case must witness an actual `get_peers`
query and a successful correlated UDP response send delayed 16 seconds after that
query, beyond the 15-second lookup budget. The result must contain no peers or
first-peer timestamp, must not report target completion, and must finish the
lookup within 15.5 seconds. The fixture joins its server before asserting; normal
socket scheduling tolerance is distinct from the external hard timeout. Missing
witnesses, zero-test matches, truncation or uncertain cleanup fail qualification.

A lookup may exhaust its frontier before its deadline. A successful send to a
closed or no-longer-polled socket does not prove receipt or internal library
rejection. These tests establish result accounting and external containment for a
late response opportunity, not that all implementations waited the full budget
or joined internal actors. Existing target cases supply the before-deadline
positive controls. No public destination policy, live comparison or adoption is
qualified by this profile.


### Guarded offline worker boundary

The optional `workers` feature adds the fixed `dht-worker --offline-worker
mainline|n0` research executable. Its default invocation denies execution. Valid
arguments still require actual namespace PID 1, one UP loopback interface, only
loopback addresses, and loopback-only IPv4/IPv6 routes across all tables before
reading protected input or creating a runtime. The inspector has a shared
two-second budget and 16 KiB per output stream/command. The custom walker entry
is a Linux-only ignored libtest case; neither entry adds a daemon API.

See [worker-contract.md](worker-contract.md) for the fixed environment and bounded
result schema. `workers.run_worker` cleans inherited comparison fields, verifies
the trusted binary pin, uses only fixed arguments and the existing namespace
owner, and reserves 2.1 seconds of a 30-second envelope for cleanup. It accepts
loopback seeds only; it cannot run a public comparison. Captured output remains
private. Artifact pins assume no hostile concurrent replacement of operator-owned
binaries.

Qualification uses same-entry target fixtures under real namespaces plus separate
compiled executable denials. Same-entry positives cover gate, real adapter and
encoder; they do not prove positive OS-environment/stdout integration through
binary `main()`. The offline worker authorizes no live execution; the public
destination policy and live worker policy are under [Live boundary](#live-boundary),
and the matched comparison is in
[the comparison report](../../docs/testing/dht-comparison-44.md).

## Live boundary

`live.py` runs one worker in fresh nested user, network and PID namespaces with
the fixed topology, policy and relay described in
[the live-boundary design](../../docs/superpowers/specs/2026-11-12-dht-live-boundary-44-design.md):

- The worker gets a veth with a default route, IPv6 disabled, and a default-deny
  nftables table.
- It runs with empty capability sets and `no_new_privs`, on a read-only root,
  with empty tmpfs over every directory holding host UNIX sockets. A root
  supervisor launches the namespace as uid 65534, so root-only host files are
  unreadable.
- The namespace dies with the supervisor through a parent-death SIGKILL.
- The supervisor relay forwards only IPv4 UDP to global unicast destinations,
  from sockets bound to the declared egress device.
- Replies are admitted only from endpoints that the same inner socket contacted.

UDP to a denied prefix is counted by nftables and dropped silently by the relay.
A synchronous `EPERM` would penalize a library for the harness rather than model
network loss. The block reports whether the two counts agree. Every non-UDP, IPv6 or unsolicited packet
is dropped inside the namespace. Unprivileged nested namespaces, veth, `AF_PACKET`
and `SO_BINDTODEVICE` are required. No TUN device and no host firewall or routing
change is needed.

The live entries are `dht-worker --live-worker mainline|n0` and the custom libtest
case `dht::comparison::worker::custom_live_worker`. Before reading protected input,
they require mode `live-v1`, global seeds, empty capabilities, `no_new_privs`, the
fixed interfaces and routes, and IPv6 disabled. An unconfined invocation exits 65;
the custom case fails.

Per arm, the boundary reports admitted, denied and unsolicited datagrams, unique
contacted and unanswered endpoints, and inner nftables counters. These are
measured at the boundary. They are not library-internal query or timeout
diagnostics, and unavailable library counters stay null. The bootstrap also
reports `wait4` CPU time and sampled peak threads, sockets and `VmHWM`. A sampled
peak is a lower bound and is null when nothing was sampled; `ru_maxrss` is not used
because it keeps the forked bootstrap's pre-`exec` peak. Admitted datagrams are
successful local sends, not delivery. Kernel receive-queue drops on the relay
sockets are reported separately; outer-socket drops are a lower bound, because the
kernel reports them only with a later datagram. Relay/nftables agreement and drops
are recorded per arm without changing the verdict, so a comparison must treat a
disagreeing arm as invalid.

### Offline live-boundary qualification

Build release artifacts for every arm, so all of them share the optimization level:

```sh
cargo test -p ace-swarm --lib --release --locked --no-run
cargo build --manifest-path tools/dhtcompare/Cargo.toml --release --features workers --bin dht-worker --locked
python3.11 tools/dhtcompare/live_qualification.py \
  --custom /absolute/path/to/ace_swarm-release-test-binary --custom-sha256 CUSTOM_SHA256 \
  --upstream /absolute/path/to/release/dht-worker --upstream-sha256 UPSTREAM_SHA256 \
  --output /private/path/to/new-live-qualification
```

The 16 fixed cases run inside one outer namespace whose only egress is a dummy
device with synthetic addresses, under a 240-second deadline:

- A probe covers two threads and sockets, eight denied destination classes,
  stranger and wrong-port inbound packets, TCP, IPv6, raw sockets, policy and
  link mutation, a supervisor-owned UNIX socket and a write to the read-only root.
- All three candidates are run through their real binary entries against synthetic
  values, and against advertised private nodes. Each candidate must try and fail to
  reach the private nodes, with matching relay and nftables counts.
- Unconfined gate denials are exercised for every entry.
- The complete block supervisor runs. It refuses encrypted content, enabled IPv6
  and a host-owned global address before starting any arm.
- As root, qualification launches as uid 65534 like live workers, and the probe
  must fail to read a root-only canary file.
- A host-owned global address added mid-arm stops all further forwarding.
- With change events withheld, datagrams to an address on another host interface
  leave through the egress device and a host listener on that address receives none.

Listeners on private outer addresses must receive nothing. `test_live.py` runs the
probe locally without binaries. CI runs the pure relay, policy, consistency and
predicate tests.

### One bounded live block

`live_block.py` runs as host root on the validation host. It reads
`{"content_id": "..."}` on stdin; the operator supplies it, and this tool never
reads the registry. It runs these steps:

1. Verify pins, the single default route via `--egress-device`, disabled IPv6, no
   host-owned address the relay would forward to, and the route and
   stateless-ruleset digests.
2. Resolve the target once through the local daemon's loopback `analyze_content`
   method. Encrypted or unknown protection is refused.
3. Resolve the production bootstrap hostnames once, into at most four global seeds,
   and refuse admission if any bound UNIX socket path lies outside the hidden
   directories. Relative and other-namespace socket paths are invisible to this
   check and rely on the hidden directory list.
4. Run custom, mainline and n0 within one 135-second deadline, without retries.
   No arm starts unless all three still fit.
5. Re-check the digests and write a private numeric report with mode-0600 captures.

Two host preconditions cannot be verified from inside the validation host, so the
operator checks them from the initial namespace before each block:

- uid and gid 65534 of the supervisor's namespace map to non-root host ids. A uid
  map read from the initial namespace is relative to it; on a Proxmox host,
  `cat /proc/$(lxc-info -n CTID -p -H)/uid_map` (and `gid_map`) must map 65534
  to a non-zero host id: the second column plus 65534 minus the first, for the
  range covering 65534. Inside the namespace,
  `launcher_identity()` refuses only root launches it cannot drop.
- No address is added to the egress device during the block. Relay sends bound to
  the egress device are never delivered to addresses on other host interfaces,
  but an address added to the egress device between the relay's last change check
  and its send could receive one datagram. Any address change during a block makes
  it inconclusive, so such a delivery is detected, not prevented
  ([#391](https://github.com/jamezrin/outpace-stream/issues/391)).

The report omits the target, content id and endpoints. Gateway and VPN exit
provenance must be checked separately, immediately before and after. Exit 0 means
all arms produced valid records with confirmed cleanup, 1 means an arm failed, and
2 means inconclusive.
