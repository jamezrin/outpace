# Canonical-stream inbound admission (#207)

Status: approved by the user after the fresh builder check in `/tmp/outpace-207/builder-check.md` on 2026-10-23. The canonical-stream occupancy cap defaults to **16**, with **0** restoring global-only admission. Internal names and mechanics below remain design proposals for the builder, not additional user selections. The approval supersedes the pre-approval workflow wording retained below; implementation planning and the standing execution workflow are authorized.

## Intent and scope

Bound how many identified inbound sessions one served stream can retain. Today a single known key can occupy all 64 global slots even when most peers are choked. Broadcast media and its metadata alias must consume one allowance. Preserve existing serving, producer ownership, and per-key desired unchokes while making admission coherent under registry mutation.

Success means an enforced combined occupancy bound, correct release on every connection exit, honest operator documentation, and deterministic in-process policy/lifetime tests. Reuse the existing mutexes, semaphore, handshake predicate, coordinator, and generic stream handler; introduce no framework.

Non-goals: bandwidth limits or equal service, reserved slots, authentication, pre-handshake denial-of-service protection, idle-expiry repair, producer-delete revocation, a new choking scheduler, byte buckets, upload counters, disk-queue redesign, or a wider listener/session refactor. Existing closed #206 work stays unchanged.

## Current behavior and alternatives

`crates/ace-swarm/src/listen.rs::PeerListener::serve` acquires a global semaphore permit nonblocking after TCP accept. Excess sockets close immediately; retained handlers hold permits through handshake and serving. The default is 64 and a configured global zero acts as one. This bounds retained connections, not accept churn, kernel backlog, or the transient just-accepted socket.

`handle_inbound` currently checks `SeedRegistry::serves` during `PeerSession::accept_handshake`, then separately obtains store, metadata, and coordinator. Removal between these lookups can split the state or yield no coordinator. `SeederSession::serve` then permits legacy inline unchoking when its coordinator argument is `None`.

Each exact registry key has its own `ServeCoordinator`. For I interested peers and configured U, desired selections are `min(I, U + 1)`: the first U peers in interest-entry order plus one rotating tail peer. Defaults remain U=8, with nominal 10-second daemon rotations. These are desired watch states; asynchronous observation and pending writes mean they are not an instantaneous wire-visible concurrency bound. Metadata replies bypass Interested/Unchoke entirely.

Considered approaches:

- **Selected: canonical-stream session cap, default 16.** Associate a broadcast's existing media and metadata keys at registration and share one admission allowance. Ordinary keys identify themselves. This bounds idle, choked, interested, and metadata-only occupancy with a small change to existing admission.
- **Per-key cap.** Cheaper accounting, but a broadcast could retain 16 media plus 16 metadata sessions. It documents a per-key limit rather than the intended stream limit.
- **Byte-rate budget.** Could constrain egress but cannot bound idle sockets. It needs encoded-byte/partial-write accounting, burst and cancellation behavior, and a justified rate default; current enqueue/media counters cannot enforce it. It is outside this issue.

Documentation alone cannot prevent one identified stream from filling the listener.

## Configuration and deployment contract

Add `Config::max_inbound_per_stream: usize`, serialized as `max_inbound_per_stream`, and `OUTPACE_MAX_INBOUND_PER_STREAM`. `Config::default()` supplies 16; the existing struct-level `#[serde(default)]` makes an omitted field inherit 16. Explicit serialized zero remains zero. The environment overrides the default in `config_from_env`; absence retains 16. No hot reload.

The proposed parser uses the existing unsigned-integer parsing convention. Empty, negative, malformed, or out-of-range values fail configuration/startup rather than silently selecting a default; a present non-Unicode environment value also fails. Serialized wrong types, negative numbers, and overflow fail deserialization. Do not clamp positive values or require N <= G.

- Let G be `max(1, max_inbound_peers)`, still default 64. Every retained inbound handler, including an unidentified handshake, consumes a global permit.
- N=0 disables only the new per-stream cap. It preserves the global bound and coordinator policy.
- Positive N allows at most N admitted sessions per canonical stream, regardless of interest, choke state, or whether they request media or metadata. N>G is valid and redundant for occupancy.
- U remains `max_unchoked`, default 8, applied per exact served key. U=0 still selects one optimistic interested peer. Admission zero does not change U.

At defaults, the 17th combined media/metadata admission for one stream closes without a handshake reply, even if global capacity remains. Its media coordinator still desires at most nine unchokes. Metadata clients can exhaust the shared 16 and crowd out media clients; there is no class reservation. Operators may select 32 or 64, or set `OUTPACE_MAX_INBOUND_PER_STREAM=0` to restore global-only admission. Restart/recreate is required. Existing listener addresses, exposure, enable flags, TTL=300, reciprocal uploads, and wire-byte behavior remain unchanged.

Deployment documentation must distinguish process environment from Compose interpolation: placing the new variable in `.env` alone does not forward it through the current Compose files. Show an explicit service `environment` override for 0 or another value. No Compose template change is required for the default to take effect.

## Canonical identity and stable accounting

Extend `SeedEntry` with an explicit canonical admission identity. Ordinary store keys and standalone metadata keys are self-canonical. `lease_broadcast` already receives both public keys; under its existing registry mutation lock, associate the media entry with itself and its metadata alias with that media identity. Repeated leases reuse the association and stores. Admission must not parse/hash metadata or trust the numeric stream field in a wire request to discover this relationship.

Never silently redirect an established alias into another allowance. An inconsistent association fails closed for admission, including collisions involving ordinary entries. Preserve any association needed by live admission accounting across registry removal/recreation; recreating an alias cannot move still-live occupants into a fresh allowance. The builder may choose the compact representation and conflict marker without changing the public producer lease signatures or adding transport parsing.

Use one listener-owned accounting domain, held by `Arc` in connection guards, independent of `SeedEntry` generations and producer counts. A short synchronous mutex protects canonical counts and any live alias bindings needed to enforce the preceding invariant. Checking capacity and reserving a slot is one operation. A non-Clone RAII permit releases exactly once; remove idle accounting buckets/bindings when their last retained permit is gone. With N=0, retain association consistency but omit per-stream capacity rejection.

Removing/recreating registry entries must not reset a nonzero count. For example, if old sessions remain at N after their producer disappears, recreating that same canonical stream admits no additional session until one exits. A permit releases against its stable accounting owner, never a replacement registry entry. Its Drop takes only the accounting lock, never the registry lock. Keep a single lock order for admission: registry then accounting; no awaits, store construction, disk access, socket writes, or signing under either lock. Existing store factories remain outside the registry lock.

## Coherent admission and permit lifetime

Proposed internal interfaces in `listen.rs` are `InboundAdmission`, `InboundPermit`, and an `AdmittedSeed` snapshot containing optional store/metadata, a required coordinator, and the stream permit. A helper such as `SeedRegistry::admit_inbound(key, admission, max_unchoked) -> Option<AdmittedSeed>` performs the following synchronously against registry mutation:

1. Resolve a present entry containing a store or metadata and its valid canonical association.
2. Obtain/create that exact key's coordinator, reserve canonical occupancy, touch existing activity bookkeeping, and clone the serving Arcs as one coherent admission decision.
3. Return the complete snapshot, or reject without leaving a reservation.

Call this helper inside the existing `accept_handshake` synchronous `FnOnce` predicate. Capture the successful snapshot in the handler across the handshake await. Thus reservation precedes optional peer-id stages, handshake response, extended-handshake signing, and serving. Unknown, empty, inconsistent, or full entries return false and use the existing close/error path; add no wire rejection message. `ace-peer`'s handshake API needs no change.

After handshake success, serve only that snapshot. Remove the production sequence of separate `serves/get/metadata/coordinator_for` lookups. The production listener always supplies `Some(coordinator)` to `SeederSession::serve`; a missing coordinator cannot activate its compatibility fallback. If removal wins before admission, reject. If admission wins first, the captured resources remain usable despite subsequent removal, preserving current lifetime behavior.

The spawned connection task retains its existing global permit and the new stream permit throughout all handshake/serve awaits. Both release on handshake read/write failure, signing/send error, EOF, protocol/store error, timeout, or task cancellation. A slow/backpressured write continues to consume both permits until completion/error or cancellation. Interest changes, choking, and producer lease drops do not release connection admission. Do not release after obtaining a snapshot or before the serve future finishes. The current sequential reply path adds no independent upload queue.

Add a policy-aware listener entry point, provisionally `PeerListener::serve_with_policy(..., InboundPolicy)`, carrying the global limit, canonical limit, and existing U. Keep the current public `PeerListener::serve` signature as a wrapper selecting N=0 with its supplied global/U values. The wrapper uses the same coherent, coordinator-required handler. Runtime explicitly constructs the new policy with N from Config. Preserve public `SeederSession::serve`, including direct callers' optional-coordinator behavior. Policy/helper names and argument packaging are builder choices; these compatibility and lifetime contracts are required.

## Fairness and lifecycle limits to document

This cap prevents one identified canonical stream from retaining all 64 slots at the defaults. It promises neither equal bandwidth nor capacity reserved for another stream. Silent/incomplete handshakes remain unclassified and can exhaust G; multiple streams, churn, CPU work, and host egress remain unbounded by N. Knowing a served key is eligibility, not authentication. Lowering U cannot constrain metadata traffic or idle/choked occupancy.

The serve loop's 250 ms polling can cancel pending reads and their inner I/O timeouts. Existing per-operation timeouts therefore do not establish a whole-session idle deadline; TTL reaps ownerless registry entries, not connections. Do not repair that behavior here.

Existing sessions retain store, metadata, and coordinator Arcs after producer deletion and may continue serving retained data. DELETE is not connection revocation. Their old coordinators no longer receive registry-driven rotations after removal. Admission accounting must survive this behavior; it does not change it. Aborting the listener alone also does not cancel its detached handlers. Reciprocal serving on outbound connections remains outside these inbound admission/coordinator budgets.

## Affected files and interfaces

- `crates/ace-swarm/src/listen.rs`: canonical association in registration/`lease_broadcast`, stable accounting and RAII permit, coherent admission helper, policy-aware listener plus public compatibility wrapper, generic `handle_inbound`, and focused unit/duplex tests.
- `crates/ace-engine/src/config.rs`: new public field/default, serde contract, and configuration tests; correct nearby policy comments.
- `crates/ace-engine/src/runtime.rs`: environment parse, direct Config-to-policy construction used by the actual listener call, and focused wiring tests. A small pure helper is acceptable for testing that exact production construction without starting the daemon.
- `crates/ace-swarm/src/seed.rs`: clarify desired-unchoke documentation and extend existing coordinator tests only where coverage is missing; preserve scheduling and serving interfaces.
- `README.md`, `docs/deployment.md`, `docs/protocol/shared-seeding.md`: global/per-key/canonical scope, all zero meanings, rollout/Compose override, aliases, metadata exception, lifetime and fairness limits. Correct existing claims of a simultaneous wire-visible unchoke bound.

No changes are needed to `ace-peer` handshake signatures, reply preparation, broadcast callers, producer lease ownership, wire formats, or the existing TCP integration-test API. Historical specs remain historical.

## Bounded validation plan — Luna exclusively

No validation is run by the design agent. After document approval and implementation, Luna owns new tests and all execution, including formatting/lint/hygiene and normal offline regression gates. Use synthetic byte arrays, barriers/oneshots, `tokio::io::duplex`, and explicit rechoke ticks; no external traffic, registry reads, discovery/daemon startup, live tests, or throughput benchmarks.

Keep the primary proofs in focused `inbound_policy_tests` modules and extend existing coordinator coverage rather than duplicating it:

1. **Admission arithmetic/concurrency:** exactly N retained successes from competing barrier-start admissions, N+1 rejected, another canonical stream independent, choked/uninterested occupants counted, unknown keys leave no bucket, last drop cleans accounting, one release admits exactly one replacement. Exercise N=0 and N>G with the real global-permit acquisition used by handlers.
2. **Alias/lifecycle:** mixed media/metadata admissions share N; repeated leases do not multiply it; remove/recreate while old sessions live preserves counts; stale producer/connection drops cannot evict or decrement replacement state. An inconsistent/live alias reassociation rejects rather than granting a fresh allowance.
3. **Actual generic handler over duplex:** excess admission produces no handshake reply; synchronize removal before/after admission to prove reject-or-coherent-snapshot behavior and required coordination. Handshake write failure, serving EOF/error, and explicit task cancellation return both permits. Fill a small duplex with a real prepared reply, observe backpressure, then cancel and prove replacement admission. Use observed progress/barriers and bounded test deadlines, not sleeps or a configuration-only assertion.
4. **Configuration/wiring and unchanged selection:** defaults and omitted serde field yield 16; explicit zero/positive and invalid inputs behave as specified, without process-global environment races. Feed the resulting production policy through admission so a parsed-but-unwired knob fails. Coordinator regressions assert exact desired sets at U=0/U=2, tail rotation, idempotent interest, leave/NotInterested promotion, and independent keys; these do not prove a wire concurrency or bandwidth bound.

Suggested focused commands are `cargo test --offline -p ace-swarm --lib inbound_policy_tests`, `cargo test --offline -p ace-engine --lib inbound_policy_tests`, and the existing `seed::tests` filter. Luna then runs the repository's required offline regression, fmt, Clippy, and identifier-hygiene gates once implementation is ready. Expand only for a specific unresolved failure, not a new matrix.

No material product choice remains open in this draft. Root must obtain the fresh builder check, resolve its findings, and present this document for human approval before planning.
