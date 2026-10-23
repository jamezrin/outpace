# Canonical-stream Inbound Admission Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans for the assigned execution role. Steps use checkbox (`- [ ]`) syntax for tracking. The approved role split below controls implementation and validation ownership.

**Goal:** Enforce a configurable, combined media/metadata inbound session cap without changing the global connection bound, desired unchokes, producer ownership, or wire protocol.

**Architecture:** One listener-owned accounting domain retains canonical counts and live alias bindings across registry generations. A synchronous handshake predicate atomically captures resources, a required exact-key coordinator, and a non-Clone RAII admission permit; the connection retains that snapshot through serving. Runtime supplies the configured policy to the new listener entry point, while the existing public entry point selects global-only admission.

**Tech Stack:** Existing Rust workspace, Tokio semaphore/duplex/watch, synchronous mutexes, Arc, serde; no new dependencies or framework.

**Spec:** `docs/superpowers/specs/2026-10-23-seeding-admission-design.md` (approved; fresh builder check: `/tmp/outpace-207/builder-check.md`, “Nothing important left.”).

## Global Constraints

- One coherent feature across listener, configuration, runtime, and operator docs; one implementation unit and one fresh independent review. No new product decisions remain; confirm this plan before execution.
- `Config::max_inbound_per_stream: usize`, serialized `max_inbound_per_stream`, environment `OUTPACE_MAX_INBOUND_PER_STREAM`; default/omitted field **16**, explicit **0** disables only canonical capacity rejection. Positive N is not clamped; N greater than G is valid.
- G remains `max(1, max_inbound_peers)`, default **64**; nonblocking acquisition before spawning a retained handler. Unidentified handshakes consume G. U remains `max_unchoked`, default **8**, per exact key: desired selections `min(I, U + 1)`; U=0 retains the optimistic slot. Nominal daemon rotation remains 10 seconds; TTL remains 300 seconds.
- Broadcast media and metadata keys share canonical occupancy; ordinary store/standalone metadata keys are self-canonical. Keep public producer lease signatures and current store construction outside the registry lock.
- Registry mutation/admission lock order is registry then accounting. Permit Drop takes only accounting. No awaits, store construction, disk access, signing, or socket writes under either lock.
- Preserve `PeerListener::serve` as an N=0 wrapper using the same coherent handler. Preserve public `SeederSession::serve` and direct callers' optional coordinator behavior; production inbound serving always supplies `Some(coordinator)`.
- Luna authors **all tests** and exclusively executes all tests/builds/fmt/Clippy/hygiene/hooks. Sol owns production changes, documentation, and review; Sol does not run validation commands. Preserve others' edits and leave the dirty root checkout untouched.
- Use the existing `feat/seeding-admission-207` worktree/branch. Root supplied starting commit `a0c3eba` and verified main ancestry; do not create another checkout. Planning makes only this plan and the spec approval-status edit, with no implementation, checks, commits, hooks, agents, or PR actions.
- Offline, deterministic synthetic inputs only. Never read the identifier registry or introduce real content identifiers, derived identifiers, or media names. No network/live tests, daemon startup, probes, installs, benchmarks, or wider validation matrix.
- No authentication, bandwidth fairness, reservations, idle timeout repair, delete revocation, outbound reciprocal policy changes, new scheduler, or upload queue. Operational events use `alog!`; library crates stay silent.

## Review Focus

1. Removal/recreation with live sockets must preserve counts **and alias bindings**, including aliases not themselves occupied while their old canonical stream remains occupied; Task 1's lifecycle/conflict tests pin this.
2. Removal between eligibility and serving must produce rejection or a complete retained snapshot with a coordinator; Task 1's ordered duplex tests pin both outcomes and prohibit compatibility fallback.
3. Handshake failure, serving EOF/error, and cancellation while a real reply is backpressured must release both permits exactly once; Task 1's handler lifetime tests prove replacement admission.
4. A parsed configuration field that never reaches production admission must fail a test; Task 1's runtime-to-admission test consumes the exact production policy construction.
5. N=0, N>G, metadata-only traffic, and U=0 must retain their different meanings; Task 1 tests capacity and exact desired coordinator sets without claiming instantaneous wire concurrency.

## File and Interface Ownership

| File | Sol production/docs responsibility | Luna test responsibility |
| --- | --- | --- |
| `crates/ace-swarm/src/listen.rs` | Registration canonical identity; stable accounting/permit/snapshot; coherent predicate; policy-aware listener and compatibility wrapper; generic handler integration | New `#[cfg(test)] mod inbound_policy_tests`: arithmetic, concurrent admission, lifecycle/alias conflict, real generic handler over duplex, global/stream release |
| `crates/ace-swarm/src/seed.rs` | Correct desired-state/interest-entry-order comments only; retain scheduler and serving behavior | Extend existing `seed::tests` where exact selection/rotation/promotion coverage is missing |
| `crates/ace-engine/src/config.rs` | Public field/default/serde contract; correct adjacent policy comments | Focused `inbound_policy_tests` for defaults and serialized input |
| `crates/ace-engine/src/runtime.rs` | Strict environment handling; actual `serve_http_inner` Config-to-policy construction and listener call | Focused `inbound_policy_tests` using injected raw environment results and the exact production construction, exercised through admission |
| `README.md`, `docs/deployment.md`, `docs/protocol/shared-seeding.md` | Operator/compatibility/lifetime documentation | No separate documentation test harness |

Coordinate file sections before edits: Luna owns test modules; Sol owns non-test code and docs. Do not overwrite a shared file wholesale or revert another worker's changes. No changes to `ace-peer`, wire formats, broadcast callers, Compose templates, existing TCP integration API, or historical specs are required.

**Existing interfaces to retain/use:**

- `SeedRegistry::register(&self, infohash: [u8; 20], store: SharedStore)`, `register_metadata(&self, key: [u8; 20], metadata: Vec<u8>)`, and `get_or_create(&self, infohash: [u8; 20], make: impl FnOnce() -> PieceStore) -> SharedStore`.
- `lease_store(&self, infohash: [u8; 20], make: impl FnOnce() -> PieceStore) -> (SharedStore, SeedLease)` and `lease_broadcast(&self, infohash: [u8; 20], content_id: [u8; 20], metadata: Vec<u8>, make: impl FnOnce() -> PieceStore) -> (SharedStore, SeedLease)`. Existing generation-stamped producer leases remain distinct from connection permits.
- `PeerListener::serve(listener: TcpListener, registry: SeedRegistry, our_peer_id: [u8; 20], piece_header: [u8; 8], max_inbound: usize, identity: Arc<Identity>, max_unchoked: usize, reachability: Option<Arc<ReachabilityMonitor>>)` is async and returns `()`.
- Current private handler is `async fn handle_inbound<S: AsyncRead + AsyncWrite + Unpin>(stream: S, registry: SeedRegistry, our_peer_id: [u8; 20], piece_header: [u8; 8], identity: &Identity, peer_ip: [u8; 4], max_unchoked: usize) -> ace_peer::Result<()>`. Extend its policy/accounting inputs minimally; keep its stream generic so tests exercise the actual production handler.
- `PeerSession::accept_handshake<F>(&mut self, our_peer_id: [u8; 20], serves: F) -> Result<Handshake>` is async with `F: FnOnce(&[u8; 20]) -> bool`. No signature changes: capture the successful snapshot in this predicate before subsequent handshake awaits.
- `SeederSession::serve<S: AsyncRead + AsyncWrite + Unpin>(session: &mut PeerSession<S>, store: Option<Arc<Mutex<PieceStore>>>, metadata: Option<Arc<Vec<u8>>>, piece_header: [u8; 8], identity: &Identity, peer_ip: [u8; 4], coordinator: Option<Arc<ServeCoordinator>>) -> Result<()>` is async and unchanged.
- `ServeCoordinator::new(usize) -> Arc<Self>`, `join(&self) -> (u64, watch::Receiver<bool>)`, `set_interested(&self, u64, bool)`, `leave(&self, u64)`, `rechoke(&self)` and `SeedRegistry::rechoke_all(&self)` stay unchanged.
- `config_from_env() -> Result<Config, Box<dyn std::error::Error>>` remains the startup interface. `serve_http_inner` currently extracts global/U values and calls `PeerListener::serve`; replace that production construction/call, not just configuration parsing.

**New interface boundary:** Use small policy/accounting, snapshot, and RAII types in `listen.rs`; the spec's `InboundPolicy`, `InboundAdmission`, `AdmittedSeed`, `InboundPermit`, and `serve_with_policy` names/packaging remain builder choices. The policy carries G/N/U, the complete snapshot carries optional store/metadata plus a required coordinator and permit, and the accounting domain outlives removed registry entries through permit Arcs. Before test authoring, Sol and Luna settle only the minimal callable seam needed for engine tests to feed the **production-created policy into real admission** without TCP or duplicating policy logic. Expose no broad testing framework; prefer the smallest normal production interface. This is implementation coordination, not a product approval gate.

## Task 1: Deliver Canonical-stream Inbound Admission

This task is the entire independently reviewable implementation unit. Keep the following test-first phases within it; do not split listener/config/docs into separate implementation/review microtasks.

- [ ] **Step 1 — Luna authors the failing regression suite before production edits.** Use synthetic repeated-byte keys, existing in-memory stores, deterministic identities, barriers/oneshots, and bounded deadlines. Agree the small new type/seam shape with Sol; use it consistently across tests and production. Add these behavioral cases (names may follow local conventions):

  | Test case | Required assertions |
  | --- | --- |
  | `canonical_capacity_is_atomic` | Barrier-start more than N competitors, retain every successful permit until all attempts finish: exactly N successes, N+1 rejected, another canonical stream independent; uninterested/choked occupants count. Drop one: exactly one replacement succeeds. Unknown/empty entries leave no reservation; final drop removes idle buckets/bindings. |
  | `aliases_share_capacity_across_generations` | Mixed media/metadata sessions reach one N; repeated broadcast leases share stores/association. Remove both keys while old sessions fill N, recreate them: still full; one old exit permits one replacement. Stale producer drop cannot evict replacement; stale connection release cannot decrement an unrelated replacement allowance. |
  | `alias_reassociation_fails_closed` | Reassociate an alias to a different canonical stream after remove/recreate with old sockets retained: reject, even with spare capacity and at N=0. Include ordinary-entry collisions, conflicting repeated leases, and a live media-only occupant retaining the metadata alias binding. No fresh bucket or partial reservation on rejection; final drop cleans idle state. |
  | `zero_and_large_n_preserve_global_limit` | N=0 admits beyond 16 until G; N>G remains bounded by G. Use the production nonblocking global-permit acquisition used for retained handlers, including global configured zero acting as one; do not substitute standalone counter arithmetic. |
  | `handler_rejects_without_reply` | Drive the actual generic handler over `tokio::io::duplex`: unknown/inconsistent/full admission closes without handshake reply and restores its global permit. Successful media and metadata sessions share the same allowance. |
  | `handler_removal_is_coherent` | Order removal before admission: reject. Order admission before removal: captured store/metadata remain usable and coordinator is mandatory. Drive interest/choke behavior on the actual handler so a missing coordinator/inline fallback would fail, rather than only inspecting a snapshot field. |
  | `handler_releases_on_exit` | After observed admission, inject handshake read/write failure (including failure after the predicate), serving EOF and protocol/store error where reachable, and abort/join. Both global and canonical capacity become available; replacement handler succeeds. Pre-admission failure leaves no canonical reservation. |
  | `handler_backpressure_retains_until_cancel` | Complete the handshake, request a real prepared media or metadata reply into a small duplex, observe a pending blocked write with controlled I/O progress. Both permits remain occupied; cancel and join the actual handler task, then prove replacement admission and no leaked binding. No sleep-based inference or synthetic permit-only task. |
  | `config_defaults_and_serialization` | Default and omitted serde field give 16; explicit 0 and positive values round-trip. Wrong type, negative, and overflow reject. Keep G=64/U=8 unchanged. |
  | `runtime_policy_controls_admission` | Feed absence, 0, 1, 16, and N>G through the exact parsing/construction used in `config_from_env`/`serve_http_inner`, then exercise real admission with the resulting policy. At N=1, second same-stream admission fails while another stream succeeds; at 0 it succeeds within G. Empty/negative/malformed/overflow/non-Unicode inputs fail, absence retains 16. Inject raw environment lookup results, never mutate process-global environment. A parsed-but-unused field must make this test fail. |

  Reuse existing metadata/chunk reply and coordinator test helpers instead of copying reply logic. In `seed::tests`, assert exact watched desired sets for U=0/U=2, tail rotation/wrap, idempotent Interested, NotInterested and leave promotion, and independent exact-key coordinators; existing sufficient tests need no duplicate. Cover the N=0 compatibility wrapper's policy selection and its use of the same coordinator-required handler. These assertions measure desired selections, not instantaneous wire unchokes.

- [ ] **Step 2 — Luna demonstrates RED.** Run the focused commands below, recording the intended missing-field/interface or behavioral failures. If new interfaces initially prevent compilation, record that once, then obtain meaningful behavioral RED as interfaces become available; do not treat unrelated build failures as proof. Root starts with this test phase, with no production implementation during the planning turn.

  ```bash
  cargo test --offline -p ace-swarm --lib inbound_policy_tests
  cargo test --offline -p ace-engine --lib inbound_policy_tests
  cargo test --offline -p ace-swarm --lib seed::tests
  ```

- [ ] **Step 3 — Sol implements the minimal listener/registry change.** Associate broadcast keys under the existing mutation lock and keep ordinary keys self-canonical. Reject inconsistent identities without changing producer APIs. Accounting must retain the canonical stream's established alias relationships while any old occupants need them, even if that alias currently has no socket; removal/recreation cannot reset counts or redirect those aliases into a fresh allowance. Reserve count/check capacity atomically, including association checking with N=0, and release once through non-Clone RAII against the stable accounting owner. Clean idle state after the last permit. Keep registry-generation protection for producer leases.

  In the handshake's synchronous predicate, perform one registry-locked resolution/reservation/coordinator/activity/resource snapshot. Reject absent, empty, conflicting, or full entries without reservation leakage. Hold the snapshot through the handshake await and every serving await; after success use only its resources and `Some(coordinator)`, removing production `serves/get/metadata/coordinator_for` sequencing. Keep global permit lifetime in the spawned task, nonblocking accept behavior, and reachability counting before the gate. Add the policy-aware entry point; make the unchanged public `serve` delegate with N=0. Retain signing/reply/serve behavior and direct SeederSession compatibility.

- [ ] **Step 4 — Sol wires configuration through the real daemon call.** Add the public usize field/default, relying on existing struct-level `#[serde(default)]` for omitted-field behavior. Parse the new environment value strictly, distinguishing absent from present non-Unicode; use a narrow injected-input parser seam for Luna's deterministic tests. Build G/N/U from Config in the exact construction consumed by `serve_http_inner` and call the policy-aware listener. Do not clamp N or validate it against G. Make the runtime-to-admission test consume this construction, not a separately assembled test policy.

- [ ] **Step 5 — Sol updates operator documentation and adjacent comments.** Document defaults G=64/N=16/U=8, each zero meaning, positive N>G, combined aliases including metadata-only occupancy, rejection before handshake response, 17th admission at defaults, and N=0 rollback/restart. Correct simultaneous-wire-bound claims to per-exact-key desired `min(I, U+1)` states in interest-entry order with nominal 10-second rotations; metadata replies bypass Interested/Unchoke. Explain no bandwidth/equal-share/class reservation guarantee, unidentified handshake exhaustion of G, retained data/coordinators after DELETE, detached handlers after listener abort, old coordinators losing registry rotation, TTL versus session lifetime, 250 ms polling/timeout limitation, and outbound reciprocal exclusion. Keep exposure/cache/enable flags unchanged. In deployment docs show an explicit service `environment` Compose override for `OUTPACE_MAX_INBOUND_PER_STREAM: "0"` (or another value), explaining `.env` interpolation alone does not forward this new variable. No Compose template edits.

- [ ] **Step 6 — Luna demonstrates focused GREEN.** Rerun the same three focused commands after implementation; every intended case must execute and pass. Refine failing assertions or production behavior with their respective owner. Use bounded duplex completion/progress signals, not repeated sleeps/log polling. Expand only to resolve a named failure; no live traffic or installs.

- [ ] **Step 7 — Obtain one fresh independent Sol review of the complete unit.** Root supplies the approved spec, this plan, full production/test/docs diff, Luna's RED/GREEN evidence, and known limits to a fresh reviewer who did not implement the unit. Review canonical/alias lifetime, lock order, complete snapshot/required coordination, permit release under cancellation/backpressure, exact production policy wiring, compatibility, identifier hygiene, and documentation truth. Review tests by inspection; reviewer does not run commands. Resolve material findings with Sol production fixes and Luna-owned regression tests/execution. No separate per-file/per-phase review rounds. A final scoped independent review is needed only if unresolved findings or material post-review changes require it.

- [ ] **Step 8 — Luna runs one broad offline gate set on the settled change.** Luna alone formats and executes the following, plus activates/verifies the repository hook as required. These commands are future execution instructions, not planning-turn actions.

  ```bash
  cargo fmt --all
  cargo test --offline --workspace
  cargo clippy --offline --workspace --all-targets -- -D warnings
  cargo fmt --all --check
  python3 tools/hygiene/check_identifiers.py
  git config core.hooksPath .githooks
  ```

  Success means the normal offline suite passes, Clippy has no warnings, formatting is clean, and identifier hygiene accepts the tracked files. The existing pre-commit hook scans staged files with `--staged`; Luna owns its execution at commit time as well. Record final command outcomes privately. Repeat only affected focused checks/gates after a material fix, then any broad gate invalidated by that fix; do not automatically rerun the whole matrix after an unchanged review.

- [ ] **Step 9 — Root completes the already-authorized PR/merge workflow.** Sol checks the scoped diff and Luna's gate evidence without running checks. Coordinate the scoped commit with Luna so only Luna executes hook-bearing actions; suggested subject: `feat(ace-swarm): cap canonical inbound sessions`. Preserve unrelated edits. Root opens/updates the concise PR against `main`, registers it with T3 when available, and includes defaults/zero rollback, restart/Compose/environment impact, commands actually run, and validation limits. Merge after required CI/review readiness under existing authorization; no new product approval question. Keep raw logs private and identifiers out of commit/PR prose. Report PR URL and merge state honestly; if a check or conflict blocks completion, resolve within scope rather than claim completion.

## Execution Handoff

The design is approved and the standing workflow authorizes PR merge. After human confirmation of this saved plan, root proceeds with Luna's Step 1/2 test phase, then Sol's production phase in this **single** unit. This planning turn changes only the spec approval status and this plan; no implementation or validation was performed.
