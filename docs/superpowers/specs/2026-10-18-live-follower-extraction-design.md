# Production live follower extraction (#190)

## Intent and scope

Move the existing production multi-upstream live follower from `ace-engine`
into `ace-swarm`, so swarm ownership includes connection pooling, discovery,
scheduling, continuity, recovery, and live download storage. Preserve the
post-v0.3.0 behavior. The engine remains responsible for provider policy,
configuration, startup buffering, statistics presentation, and HTTP lifecycle.

The user approved this direction on 2026-10-18. This document defines the
architecture for review before implementation planning.

Do not consolidate reciprocal and inbound serving (#192), remove the older
single-peer swarm APIs (#193), expand VOD behavior, change exposed defaults,
or add protocols. Preserve existing compatibility APIs during this extraction.

## Chosen approach

Extend `ace-swarm` with a distinct `live_session` module for the production
follower. Its public documentation identifies it as the production entrypoint
and distinguishes the existing `live::LiveSession` and `driver` helpers.

A partial extraction of only peer/pool mechanics would leave recovery and
session ownership in the engine. A new crate would add dependency and API
complexity without resolving a dependency cycle. Direct relocation with small
engine adapters provides the intended ownership with the least algorithmic
change.

## Ownership and interface

Move `ace_provider/live/{peer,pool,pool_support,continuity,single_peer}.rs`, the
follower portion of `live/mod.rs`, candidate/reconnect policy, and warm-peer
cache implementation into focused swarm modules. Move their constants and
private protocol types with them. Keep production files approximately below
1,500 lines, excluding tests.

The swarm entrypoint is an async session runner with grouped input structures,
not an engine trait implementation. Its inputs contain resolved `StreamInfo`,
identity, initial peer candidates, continuing discovery, recovery/prefetch
settings, shared counters, seed context, optional reachability monitoring,
and an optional observation sink. Output is the existing bounded channel of
`LiveOutput { bytes: Bytes, discontinuity: bool }`. Preserve the current channel
capacity and backpressure. Internal peer workers, scheduler state, continuity
types, and reconnect candidate classifications remain private.

Keep `AceSource`, `TsSource` implementation, `AceProvider::open_resolved`,
identifier resolution, VOD adapter, startup buffering, and provider-error
mapping in the engine. The adapter translates engine configuration and starts
the swarm runner. The engine retains the current concurrent periodic seeder
announce wrapper and its `tokio::select!` cancellation relationship.

Expose only the warm-cache operations and bootstrap/discovery handle needed
by the adapter. Discovery retains the injectable callback accepting
`DiscoveryOptions` and a candidate sender. The owned discovery handle keeps
its worker alive while transferring bootstrap results to the running session;
dropping it aborts the worker as today. Warm-cache initialization, first-peer
availability, and the admission of candidates discovered during connection
attempts retain their existing order and deadlines.

## Configuration and dependencies

Environment parsing, defaults, and user-facing validation remain in engine
configuration. Introduce swarm recovery settings containing the values used
by the existing follower and map the engine settings explicitly. Preserve
validation at the session boundary and existing invalid-config termination.
`StartupBufferConfig` remains entirely in the engine; pass the already
computed prefetch policy to swarm.

Promote `ace-media` to a normal `ace-swarm` dependency for the existing pure
`TsResync` and `KeyframeGate` helpers. Neither engine nor HTTP types may be
dependencies of the swarm crate. Continue operational logging through
`ace-log::alog!`, preserving event tags and useful diagnostics.

## Statistics and storage adapters

Keep `LiveMetrics`, rate sampling, `StatsReader`, `SourceStats`, and HTTP
statistics schemas in the engine. Add a narrow swarm observation interface
for received bytes, position/buffer updates, and clearing active coverage on
session termination. Its position value contains the fields already produced
by the follower; the engine adapter maps them without changing their meaning.
The shared peer/download/upload/served counters retain their existing types
and update sites. Received bytes remain distinct from authenticated delivered
bytes. Preserve metrics publication timing and teardown behavior.

The seed context takes the existing registry, limits, enablement, counters,
and a store factory configured by engine cache policy. Do not move engine
`CacheType` into swarm or duplicate its disk/memory selection. Store creation
and diagnostics retain existing fallback behavior. The session acquires its
producer lease once, owns it across reconnects, and releases it on teardown.
The shared store remains visible to inbound serving throughout that lifetime.
Move the engine's legacy single-peer follower helper intact where its tests
or fallback code require it; do not consolidate its separate serving path.

## Behavior and failure invariants

Preserve peer selection/fairness, limits, retry budgets, request ordering,
timeouts, buffer bounds, and discovery trust distinctions. Preserve all
authentication gates before media emission and all discontinuity/keyframe
gating. No new bytes are emitted on a path that previously rejected them.

Consumer channel closure still stops the session. Peer loss and stale pools
use the existing recovery paths; exhaustion and invalid configuration retain
their existing termination/logging semantics. Preserve worker abort guards,
peer-count cleanup, warm-cache flushes, seed lease lifetime, and final metrics
cleanup on normal completion and cancellation. The extraction must not create
detached discovery, peer, or refill tasks.

## Tests and validation

Baseline: `cargo test --workspace --offline` passed with 1,002 tests, zero
failures, and seven ignored tests before edits.

Move direct session, pool, continuity, candidate fairness, warm-cache, recovery,
and injected discovery tests with their private implementation. Split mixed
engine test modules: retain provider policy, resolution, startup buffering,
`TsSource` adaptation, and statistics-handle tests in engine. Preserve existing
assertions and fixtures; do not expose internal APIs merely to retain old test
locations. Add focused adapter tests where the new boundary could lose fields
or ownership, especially configuration mapping and final metrics clearing.

Compare pre/post test inventories and account for moved or renamed tests.
Run the workspace offline suite, formatting, clippy with warnings denied, and
identifier hygiene. Review the diff for unintended algorithm changes and
verify the engine no longer owns live peer read/write loops.

Perform one bounded live smoke using an environment-provided current public
content id resolved manually from the private registry. Follow repository
WARP-off controlled-local rules; if changing host routing needs permission,
request it before the run. Verify MPEG-TS output and report the observation
limits. Keep identifiers and raw captures out of tracked files and PR text.

GPT-6 Luna runs tests and validation; GPT-6.1 Sol performs implementation and
independent code review, per the user's model preferences.

## Completion criteria

The production session and its peer-pool state machine live in `ace-swarm`;
the engine calls its documented entrypoint through a small adapter. Existing
behavioral coverage survives the move, all required checks pass, a live smoke
confirms output, and an independently reviewed PR is merged. Follow-up issues
#191–#193 keep their separate remaining scope.

## Task 2 implementation notes

The engine now builds `LiveSeedOptions` directly and supplies its existing
store factory closure. The temporary engine `SeedConfig` is removed. The
swarm runner groups its inputs in `LiveSessionInputs`; its private helpers
retain the prior signatures through local recovery/seed aliases.

`DiscoveryRun` exposes candidate reception and nonblocking draining while
retaining its abort-on-drop worker. `WarmPeerCache` exposes initialization
and candidate addresses; source/PEX classifications and credit policy stay
private. The cache's existing Unix filesystem hardening requires a normal
Unix-only `libc` dependency in swarm. `ace-media` is promoted from a test
dependency for the unchanged resync/keyframe helpers.

Private continuity, pool, reconnect, credit and cache tests move to swarm.
Provider discovery/bootstrap, geometry, startup buffering and source tests
remain in engine. Mixed statistics tests now exercise a recording observer
in swarm; engine observer mapping remains covered by the Task 1 adapter
tests. Startup-buffer tests consume explicit `LiveOutput` boundaries, with
raw transport recovery still covered by the relocated resync tests. Durable
credit tests construct the runner with a private test bootstrap adapter;
descriptor authority remains asserted through engine catalog/index tests.
Reconnect controls additionally assert one store construction, shared store
identity for inbound consumers, and producer lease release on teardown.

Engine production defaults remain authoritative. Relocated fixtures use a
test-only `Default` implementation for recovery options with their original
values; the public swarm boundary requires explicit production settings.
No seed-serving consolidation or legacy entrypoint retirement is included.
