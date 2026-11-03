# DHT comparison research harness (#44)

Work in progress. Strict worker input parsing and bounded batch/deadline accounting are implemented.
All three actual implementations have passed target, silent-node, and
malformed/uncorrelated/duplicate-response cases inside the offline namespace helper.
The custom cases exercise ace-swarm's existing numeric-seed seam.
Late-response, cancellation and whole-harness qualification remain incomplete.
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
timeout; the external process deadline remains the hard bound. Late network
replies, allowed destinations, live workers and live results remain pending.
