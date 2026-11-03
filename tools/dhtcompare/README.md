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
