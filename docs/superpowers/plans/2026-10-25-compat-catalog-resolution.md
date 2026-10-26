# Compat Catalog Resolution Implementation Plan

> **Status:** Implementation complete, validated, and reviewed; CI pending. Execution and the scoped commit/merge integration were authorized after the final rereview.

**Goal:** Eliminate duplicate compat/live catalog resolution while preserving cid session identity and catalog trust.
**Architecture:** One optional provider preparation hook forwards through StreamManager to AceProvider. Its existing content-id policy owns one bounded positive catalog cache and caller-owned shared attempts; local/peer provenance never becomes catalog trust.
**Tech Stack:** Rust 1.99.1, Tokio, async_trait, futures Shared, current loopback ACE/media fixtures; no new dependencies.
**Spec:** ../specs/2026-10-25-compat-catalog-resolution-design.md

## Global Constraints

300-second non-sliding positive TTL; 256 total cache entries, 32 pending keys, 64 waiters per pending key. Preserve cid session keys, successful public URL shapes/aliases, current authentication, default-off BEP-9 isolation and 30-second reservoir. No negative cache, detached task, broad cache framework, extra retry, new configuration, or changes to VOD/server-api/bare-infohash resolution. Error selection now terminates before mint/open as explicitly described in the spec. Only synthetic identifiers/metadata in tracked artifacts.

## Review Focus

- Different casing of one cid must coalesce, but a bare infohash must not share the cid namespace.
- An error must fan out once without a fresh implicit fetch or capacity-triggered BEP-9 fallback.
- A canceled first caller must leave another caller's work alive; the last drop must release work and capacity.
- TTL hits cannot slide expiry; saturated state must admit hits/joins within limits without spawning extra work.
- Generic providers and the disabled resolution gate retain old behavior; peer/local metadata cannot mint a catalog alias or index entry.

### Task 1: Prove and fix shared compat/live catalog resolution

**Files, all under crates/ace-engine/src unless prefixed docs:**
- Modify `provider.rs`: ContentIdPreparation and default asynchronous preparation hook.
- Modify `manager.rs`: crate-visible registry forwarding method only.
- Modify `ace_provider.rs`, `ace_provider/resolution.rs`: forwarding fetch seam, cache ownership, private result provenance and hook implementation.
- Create `ace_provider/catalog_resolution.rs`: bounded state/guards and adjacent unit tests.
- Modify `http/compat.rs`: prepare through provider; preserve selection/session identities; terminal error propagation.
- Create `http/tests/compat_catalog.rs`; register in `http/tests/mod.rs`: production-router regression and URL/compat controls.
- Modify `ace_provider/content_id_policy_tests.rs` and existing provider fixture uses only as required to retain their catalog/peer/local trust controls.
- Keep this spec/plan updated with evidence; no unrelated refactoring or ace-swarm cache modification.

**Interfaces:** Implement exactly ContentIdPreparation, StreamProvider::prepare_live_content_id, StreamManager::prepare_live_content_id(network, content_id), private ResolvedContentId/resolve_content_id_prepared, CatalogLookupError and CatalogResolutionCache::{new,get,resolve_with} from the spec. Existing `resolve_content_id_with_catalog` remains Result<StreamInfo, ProviderError>; its injected future gains Send + 'static to enter Shared. Adapt borrowed test captures with owned clones/async move. The helper and normal resolution call the same private provenance-aware policy. Convert disabled_content_id_fallback to borrow ResolveError when formatting an Arc-held error. The test-only put is unavailable in production.

- [x] **1. Add only the behavior-preserving observation seam.** In ace_provider.rs define catalog_live_info and cfg(test) CatalogFetch/task-local scope helper `with_catalog_fetch_for_test(fetch, future)`. Outside tests it forwards once to resolve_via_catalog. Replace only the two raw callsites identified in the spec. For concurrent tests, wrap each spawned future in its own scope carrying the same Arc script; task locals do not inherit across spawn. Keep cache/index behavior exactly baseline at this step.

- [x] **2. Add `compat_catalog_cold_direct_open_fetches_once` using the real router and AceProvider.** Decode a synthetic descriptor, script every fetch to return it while counting actual polls, configure existing bootstrap and target_ms=0 builders, set max_active_upstreams=max_parallel_connect=1, keep inbound announce port None, and serve pieces carrying existing HTTP FIXTURE tables/access points from a bounded loopback ACE peer. Keep the peer alive and advertise no source/PEX endpoints so no discovery or self-announce can reach the public network. Assert response status/content-type and nonempty correctly aligned media before asserting calls == 1. Verify the manager uses cid as its session key. Server tasks must be owned, aborted and awaited on every exit. A separate `compat_catalog_gate_disabled_never_fetches` positive control installs a panic-on-fetch override and an appropriately primed provider/local fixture.

- [x] **3. Record behavioral RED before any production caching/hook changes.** Use the filter commands below; confirm the named test actually ran. Expected failure: media succeeds and the count assertion reports left 2/right 1. Record the forwarding-only diff and failure privately; compilation errors/timeouts/fake-provider failures do not establish RED. Gate-disabled control must pass. Do not weaken the assertion when implementation begins.

```bash
RUSTUP_TOOLCHAIN=1.99.1 cargo test -p ace-engine compat_catalog_cold_direct_open_fetches_once -- --nocapture
RUSTUP_TOOLCHAIN=1.99.1 cargo test -p ace-engine compat_catalog_gate_disabled_never_fetches
```

- [x] **4. Add bounded-state and cancellation regressions, then implement the provider-local cache.** Keep one short std::sync::Mutex, Ready/Pending generations, Shared future clones and last-waiter drop guards; no lock across await and no tokio::spawn for fetches. Use the exact limits and FIFO completion eviction/clock rules from the spec. Unit tests use barriers/oneshots and paused Tokio time, never real waiting or network. Implement these named assertions:

  - `catalog_resolution_same_key_coalesces_and_other_keys_progress`: two same-key callers use one blocked script; a distinct key completes independently. Repeat with differently cased cid spellings.
  - `catalog_resolution_errors_fan_out_without_retry`: attached waiters receive the same failure after one fetch; a separate later call may fetch once again.
  - `catalog_resolution_cancel_leader_keeps_waiter` and `catalog_resolution_cancel_waiter_keeps_leader`: remaining caller succeeds with one fetch.
  - `catalog_resolution_cancel_last_drops_fetch_and_releases_slot`: drop all callers; producer drop sentinel fires and a new key can use capacity immediately.
  - `catalog_resolution_old_guard_cannot_remove_new_generation`: deterministically interleave completion/cancellation/new attempt, proving generation-checked cleanup preserves the new attempt.
  - `catalog_resolution_ttl_expires_at_300_seconds_without_sliding`: set completion instant, check hit at +299 seconds, exact miss at +300, and prove a hit did not renew it. A delayed initial completion starts TTL at completion.
  - `catalog_resolution_capacity_evicts_ready_not_pending`: 256 total limit, oldest Ready eviction, expired pruning, Pending preserved.
  - `catalog_resolution_pending_and_waiter_limits_do_not_fetch`: key 33 and waiter 65 fail immediately; hits and permissible same-key joins still work; error/success/cancellation restore reservations.

- [x] **5. Implement the provenance-preserving policy and hook in the spec.** Preserve initial native catalog-cache/local/peer-cache precedence; use resolve_with only for actual signed-catalog work. A true catalog error follows the existing opt-in peer policy, whereas Capacity terminates without discovery/fallback. Catalog results alone enter the infohash index. Native opening maps private provenance to its existing StreamInfo; preparation maps it to Catalog or ProviderResolved. Add manager forwarding and default Unsupported hook. Maintain the helper test seam so cache and production entry points exercise the same code.

- [x] **6. Route enabled compat preparation through the hook.** Use unchanged with_resolved_stream for Catalog, keep cid selection for ProviderResolved, and retain the legacy raw preflight only for Unsupported. Propagate supported-provider failures without token minting, source opening or a direct second catalog call. Preserve disabled gate and transport/infohash selectors. Do not route a ProviderResolved descriptor through remember_live_descriptor. Do not install a route cache or change session_key to an infohash.

- [x] **7. Add HTTP behavioral controls and obtain GREEN on the unchanged RED.** Tests must use actual fetch-poll counters, not only hook counters:

  - `compat_catalog_warm_reuse_preserves_urls_and_cid_key`: cold direct fetch=1; warm direct and JSON requests leave count=1, retain cid manager key, resolved public infohash/playback alias and current tokenized URL shape. Assert body media and metadata; no exact unpredictable tokens.
  - `compat_catalog_concurrent_requests_share_resolution_and_source`: barrier-attached direct/JSON callers share one catalog attempt and the same manager session Arc/cid entry; the no-loss loopback peer observes one accepted TCP connection. Use existing manager same-key open controls to corroborate source-open exclusivity. Canceling either request does not invalidate another request's URL/media.
  - `compat_catalog_error_does_not_retry_or_mint_lease`: one scripted failure, error envelope, zero leases/source opens, no second fetch; a later separate request is allowed one retry. Repeat two simultaneous attached failures.
  - `compat_catalog_default_hook_preserves_legacy_preflight`: a provider omitting the hook still compiles, uses the legacy override once and receives its existing remember callback; resolution-disabled bypasses hook/fetch entirely.
  - `compat_catalog_ttl_expiry_refetches_without_rekeying`: paused-time selection test at expiry performs one new lookup, keeps cid key and uses fresh catalog metadata/public alias; do not expect a running session to restart.

- [x] **8. Pin trust and policy controls.** Add `compat_catalog_peer_and_local_results_keep_cid_alias` with local and explicitly opted-in BEP-9 fixtures: no catalog-cache/index promotion, no catalog alias, native cid opening still works, and no additional catalog call after preparation success. Add `catalog_resolution_capacity_never_enables_peer_fallback` and `catalog_resolution_cid_namespace_does_not_seed_arbitrary_infohash`. Retain existing `disabled_live_and_vod_fail_before_peer_connections_or_discovery`, `enabled_live_peer_resolution_stays_out_of_catalog_trust`, `local_broadcast_content_id_resolves_without_catalog_or_opt_in`, `generic_seed_metadata_does_not_gain_local_broadcast_trust`, and cache/index binding controls. Scripted descriptors bypass authentication only inside the observation seam; preserve/run the lower-layer signed-response replay and wrong-swarm tests rather than claiming the script proves authentication.

- [x] **9. Run focused controls, then required gates once; retain evidence.** These implementation and validation commands were run as recorded in `.superpowers/sdd/2026-10-25-compat-catalog-resolution/` task reports. Final workspace gates are rerun on the integrated candidate in this authorized completion pass; CI remains pending. Expected: meaningful RED followed by GREEN; ignored live tests remain ignored. On a failure investigate before changing product behavior or broadening the test matrix.

```bash
RUSTUP_TOOLCHAIN=1.99.1 cargo test -p ace-engine compat_catalog_
RUSTUP_TOOLCHAIN=1.99.1 cargo test -p ace-engine catalog_resolution_
RUSTUP_TOOLCHAIN=1.99.1 cargo test -p ace-engine content_id_policy_tests
RUSTUP_TOOLCHAIN=1.99.1 cargo test -p ace-swarm catalog_response_rejects_replay_for_a_different_request
RUSTUP_TOOLCHAIN=1.99.1 cargo test -p ace-swarm catalog_infohash_rejects_authenticated_descriptor_for_another_swarm
RUSTUP_TOOLCHAIN=1.99.1 cargo test --workspace
RUSTUP_TOOLCHAIN=1.99.1 cargo clippy --workspace --all-targets -- -D warnings
RUSTUP_TOOLCHAIN=1.99.1 cargo fmt --all --check
python3 tools/hygiene/check_identifiers.py
```

- [x] **10. Review the single change and hand back evidence.** Verify cancellation/completion races, no lock held through await, 256/32/64 accounting, strict provenance, default-provider behavior, and deliberate error-envelope/local-precedence consequences. Confirm no change to authentication, exposed defaults, runtime network settings or buffering. Report RED/GREEN commands and limitations. Final rereview approved the scoped implementation and test correction. Commit and local merge to the current `origin/main` are authorized; publishing, deployment, SSH, and live probes remain out of scope.

Self-review ruling: all contract/five review-focus classes map to the named tests above. One coherent deliverable; no separate cache-framework task or mandatory new public builder. No validation or regression result is claimed by this plan.
