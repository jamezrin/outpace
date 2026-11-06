# Guarded comparison workers for #44

Scope: the worker-entry prerequisite for #44, not a live comparison or library
adoption. The goal is fixed executable inputs and results with default network
denial. #44 remains open for public destination policy and live methodology and
results. The #195 observation and its 34 pinned files are outside this work.

## Decision

Scenario-specific ignored tests alone do not provide a fixed operator boundary.
Public workers are premature until every candidate's destinations and server
behavior are confined. Use a common versioned contract, a standalone upstream
binary, and a custom test-only entry in owned loopback-only PID/network namespaces.
There is no root production API, dependency or lockfile change, and no fake adapter
in place of an actual library.

## Entry and protected input

The standalone `dht-worker` accepts exactly `--offline-worker mainline` or
`--offline-worker n0`. Other arguments deny execution before environment access or
runtime/socket construction. The synchronous ignored custom entry is
`dht::comparison::worker::custom_offline_worker`; it adds no production API.

Require actual PID 1 and inspect the current network namespace before reading
protected input. Exactly one UP loopback interface and only loopback addresses and
routes are allowed. Inspect IPv4 and IPv6 routes across all tables; reject default
routes, gateways and external devices. Ordinary local loopback routes are allowed.
Use fixed `/usr/bin/ip -j` commands, not network sysfs from another namespace.
Unavailable or malformed inspection denies execution. This is offline confinement,
not proof that a library never attempts an external send. No live override exists.

Four fixed prefixed environment fields carry the request:

- `OUTPACE_DHT_COMPARE_MODE=offline-v1`
- `OUTPACE_DHT_COMPARE_ROLE=custom`, `mainline` or `n0`, matching the fixed entry
- `OUTPACE_DHT_COMPARE_TARGET`: exactly 20 hex-decoded bytes
- `OUTPACE_DHT_COMPARE_SEEDS`: 1–4 unique numeric IPv4 endpoints, nonzero ports,
  loopback only, at most 256 bytes in total

Reject missing, duplicate, unknown, non-Unicode, malformed and oversized relevant
fields before adapter construction. Leave unrelated environment fields alone. The
parent removes inherited comparison-prefixed fields before supplying the intended
four. Never mutate process-global environment in multithreaded libtest. Values
must not appear in argv, Debug, errors or records. Never read the registry. The OS
environment remains operator-controlled; it is not a hostile-local secret boundary.

## Execution and bounds

Parsing and isolation precede Tokio runtime creation. The existing lookup target
is 8 peers, retention cap 1,024, and constructor-inclusive lookup budget 15 seconds.
W0 is immediately before actual candidate construction. The parent owns namespaces,
stdout/stderr, cancellation, deadline and descendant teardown. Candidate input
cannot select arbitrary argv, libraries, budgets or namespaces.

Inspectors share one 2-second deadline. Each capture caps stdout and stderr at
16 KiB per command, drains both pipes without blocking, and kills/reaps the direct
child on error. The fixed trusted inspector is not a caller-selected program.
Parent namespace containment is the final bound under OS stalls; no literal
self-preempting constructor, kernel operation or in-process actor-join claim is made.

The parent allows at most 27.8 seconds in `run_offline`, reserving 2.1 seconds of
the 30-second worker envelope for cleanup. It must not pass a 30-second timeout to
the helper, which rejects that value. Artifact pins assume no hostile concurrent
replacement of operator-owned binaries.

## Versioned results

Emit one JSON object of at most 8,192 bytes plus LF. Upstream stdout contains only
that line. Custom libtest output contains one reserved `DHT_COMPARE_RECORD ` line,
with its exact test name and one passed/zero failed/zero ignored summary.

The exact shared schema is in `tools/dhtcompare/worker-contract.md`: version,
candidate, terminal, peer count, first-peer and elapsed times, bounded batch
history and its count/truncation, overshoot, nullable query/response diagnostics,
and a fixed nullable diagnostic-unavailability reason. No identity, endpoint,
payload or foreign error text is emitted. Upstream unavailable diagnostics are
null with a reason, never invented zeros.

The parent rejects missing, duplicate, unknown, malformed or oversized records,
booleans used as integers, nonfinite numbers and inconsistent counts/timing.
Integers are bounded unsigned 64-bit values; peer count is at most 1,024 and
history at most 64 entries. Raw emission sizes may exceed the retention cap.
Overshoot is `max(count - 8, 0)`. First-peer time is null iff count is zero, otherwise
nonnegative and no greater than elapsed. Target completion requires at least 8
peers; exhaustion/deadline cannot claim the target. Elapsed time may exceed
15 seconds after a late constructor return but must fit the parent envelope.
Measurement outcomes do not all imply successful discovery.

Input denial exits 64, namespace denial 65, output failure 74; denial emits no
result. Any diagnostic uses static tokens. Research-only optional libc and pinned
serde_json 1.0.151 support robust kernel JSON and record encoding. The custom entry
uses existing libc and serde_json dev dependencies. No cross-crate source inclusion
or root dependency/lockfile change is permitted.

## Qualification and ownership

Use pure request, topology, record, redaction and ordering tests. Compiled
executables deny absent/unrecognized arguments and unconfined execution. The
selected positive composition runs the SAME synchronous entry under actual PID 1
and loopback isolation, with an owned seed thread, real adapter and encoder, then
validates its record with the common Python contract. Separate compiled executable
denials exercise bad input. This does not prove positive binary-main OS-environment
and stdout integration. Never weaken PID 1 or spawn a nested worker to obtain a
false positive. Independent builder review found no essential user decision.

Cancellation and failure retain original exit, bounded/drained output and known
namespace cleanup. Real UDP cases stay ignored and require the explicit helper.
No public DNS/bootstrap, content requests, deployment, routing/firewall/VPN changes
or new live block are part of this increment.

## Acceptance and remaining work

The fixed default-deny boundary must cover upstream and custom paths. Pure tests,
confined actual targets, compiled denials, schema conformance and owned cleanup
must pass; independent review and CI integration remain required. Preserve the
source/dependency boundaries above. This does not complete #44: public workers
remain denied until separate destination, actor and server policy and a reviewed
bounded runtime plan are qualified.
