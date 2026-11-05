# DHT comparison research harness (#44)

Work in progress. Strict worker input parsing and bounded batch/deadline accounting are implemented.
All three actual implementations have passed target, silent-node, and
malformed/uncorrelated/duplicate-response cases inside the offline namespace helper.
The custom cases exercise ace-swarm's existing numeric-seed seam.
Fixed offline late-response-opportunity and external lookup-cancellation profiles
are available. Internal cancellation/actor joining, observed late datagram receipt,
and whole-harness/live qualification remain incomplete.
There is no live worker executable, complete live comparison supervisor, public-network mode or library-adoption decision.
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
