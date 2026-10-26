# Shared compat/live content-id catalog resolution

Design only for #253 at `72f8070`; implementation and validation have not run. Routine builder decisions below are the designer's choices under the user's delegation.

## Intent and evidence

A cold compat content-id request resolves the signed catalog in `http/compat.rs::resolve_ace_selection`, remembers only an infohash-index entry, and preserves its `cid:` session key. `AceProvider::open` consequently resolves that cid through a still-empty content-id cache and fetches the catalog again. Warm compat requests also fetch before consulting the shared session. Existing HTTP fixtures disable preflight and cannot establish this defect behaviorally.

One provider-owned signed-catalog cache must serve compat preparation and native live opening. Preserve successful public URL shapes, resolved public/playback aliases, internal cid keys, catalog authentication/decoding, non-sliding **300-second** positive TTL, and default-off BEP-9 trust isolation. This does not establish a public-swarm root cause.

## Approaches and builder ruling

Choose an optional provider preparation hook and a small engine-local catalog state machine. `StreamProvider` is object-safe through async_trait and already owns descriptor trust/indexing; StreamManager can forward without downcasts. An asynchronous hook is necessary to reach the same provider through the existing registry. Existing providers must compile without implementing it.

Reject a route-only cache: it duplicates TTL/trust ownership and misses native/compat concurrency. Reject changing cid session keys to indexed infohashes: it changes alias/session/stop sharing and does not remove warm preflight. Do not make a generic cache framework or change ace-swarm's shared ResolveCache, which still serves the separate BEP-9 cache.

## Exact interfaces

In `provider.rs`:

```rust
pub enum ContentIdPreparation {
    Unsupported,
    Catalog(StreamInfo),
    ProviderResolved,
}
// Added to StreamProvider; default body: Ok(ContentIdPreparation::Unsupported).
async fn prepare_live_content_id(&self, content_id: &str)
    -> Result<ContentIdPreparation, ProviderError>;
```

`Catalog` is only authenticated catalog metadata; `ProviderResolved` means the provider has resolved its own local/opted-in peer path, without granting catalog provenance. It deliberately carries no descriptor for HTTP to index. `Unsupported` alone permits the legacy direct catalog preflight.

StreamManager adds the same method with `network: &str` before content_id, returning NotFound for an absent provider. AceProvider overrides it by calling its existing live content-id resolution policy, now retaining private provenance:

```rust
enum ResolvedContentId { Catalog(StreamInfo), Provider(StreamInfo) }
async fn resolve_content_id_prepared(&self, cid: &str)
    -> Result<ResolvedContentId, ProviderError>;
```

Keep `resolve_content_id` and `resolve_content_id_with_catalog` returning StreamInfo for existing callers; both delegate to the same policy and discard provenance. Catalog success/hit re-populates the existing infohash index. Local broadcast and BEP-9 remain separate, never enter the catalog cache/index, and generic seed metadata remains untrusted. Local broadcasts retain provider-first local precedence; compat can consequently bypass a redundant catalog call for a locally minted cid and keep its cid alias. This local-precedence consequence is intentional.

In resolve_ace_selection, when its existing resolution gate is enabled: Catalog uses unchanged with_resolved_stream; ProviderResolved keeps the existing cid selection; Unsupported runs the old raw preflight and remember hook. Preserve the resolution-disabled branch. Do not add a second asynchronous trait hook or a public injection builder.

A supported provider's terminal error ends selection through the existing JSON error envelope, before token minting/opening: **no swallowed error followed by a second catalog attempt**. This deliberately changes failed compat selection: it returns an error instead of minting an unusable JSON playback URL or continuing direct opening. A separate later client request can retry. Enabled BEP-9 can resolve during preparation; successful fallback retains cid public/playback aliases and caches only in peer_resolve_cache. Its existing network/read policy is unchanged.

## Bounded cache and cancellation

Create `ace_provider/catalog_resolution.rs`; replace only AceProvider.resolve_cache's type. No process-global state, new dependency, env setting, transport deadline, or negative cache.

```rust
#[derive(Clone)]
enum CatalogLookupError { Catalog(Arc<ResolveError>), Capacity }
struct CatalogResolutionCache { /* one short synchronous mutex */ }
fn new(ttl: Duration) -> Self;
fn get(&self, cid: &str) -> Option<StreamInfo>;
async fn resolve_with<F>(&self, cid: &str, fetch: F)
    -> Result<StreamInfo, CatalogLookupError>
where F: Future<Output = Result<StreamInfo, ResolveError>> + Send + 'static;
```

These methods belong to CatalogResolutionCache. Private constants: **256 total Ready/Pending entries, 32 pending keys, 64 waiters per pending key**. Keys are decoded content-id bytes; namespaces remain separate. On a new miss, prune expired Ready entries, evict the oldest completed insertion if necessary, and reject immediately if limits still prevent admission. Never evict Pending to admit another key. Capacity is a fixed Backend error, not a catalog failure permitting BEP-9. No admission queue. Distinct keys execute independently.

Pending owns a generation, waiter count and futures::future::Shared<BoxFuture<'static, Result<StreamInfo, CatalogLookupError>>>. Each caller retains one clone plus a synchronous drop guard; poll outside the lock. No spawned fetch task and no leader-only ownership. Canceling one caller cannot restart/cancel another's attempt. Last-waiter drop removes only its matching pending generation and drops the stored future. Success atomically replaces Pending with Ready, timestamped at completion using tokio::time::Instant; access does not extend TTL, and exact 300-second expiry is a miss. First publisher wins; older guards cannot remove a newer generation. Errors reach all attached callers and remove Pending without storing a negative entry; they never loop internally. A later caller may start a fresh attempt. Existing four sequential catalog-host deadlines remain unchanged.

Provide test-only put for existing trusted-fixture cache seeding, honoring the same capacity/clock and never replacing Pending. Production insertion occurs only after the existing authenticated resolver succeeds. Return catalog errors by Arc/reference; no need to make ProviderError or ResolveError Clone.

## Behavioral RED seam, before the fix

Introduce only a forwarding function `pub(crate) async fn catalog_live_info(cid: &str) -> Result<StreamInfo, ResolveError>` and substitute the existing raw call at exactly two sites: resolve_ace_selection preflight and AceProvider::resolve_content_id. Its non-test body calls the same ace_swarm::resolve::resolve_via_catalog(cid).await exactly once; it has no cache, retry, builder, or policy. Do not substitute server-api/VOD/infohash paths.

Under cfg(test), this function checks a scoped Tokio task-local CatalogFetch override before forwarding. CatalogFetch is Arc<dyn Fn(String) -> BoxFuture<'static, Result<StreamInfo, ResolveError>> + Send + Sync>. A cfg(test) scope helper installs it per future; every spawned caller gets an explicit scope. Counters increment when the scripted future is polled, not constructed. No globals, environment redirection or real catalog traffic. Shared production futures later also poll this wrapper; the hook is not bypassed in GREEN.

The first regression must run the real router with production AceProvider, compat and preflight enabled, a synthetic trusted descriptor from the override, and a real loopback ACE media peer through existing bootstrap/prebuffer builders. Set max_active_upstreams=max_parallel_connect=1, retain the default None inbound announce port, advertise no source/PEX endpoints, and hold that one connection open; this avoids tracker/DHT work. Use the existing HTTP H264 TS fixture (PAT/PMT/access point), not marker-only packets that the HTTP keyframe gate would withhold. GET direct content_id, receive 200/video-mp2t and actual gated TS bytes, then assert catalog_calls == 1. Against forwarding-only baseline it must fail specifically with actual 2. Use a 5-second test watchdog and owned server cleanup. A fixture provider or counting the new preparation hook is insufficient. Scripted StreamInfo represents an already-validated result; existing signed-response tests separately guard authentication.

## Scope and acceptance

One coherent task in the linked plan covers RED, implementation, controls and review. Success requires warm reuse, same-key coalescing, distinct-key progress, shared errors/no hidden retry, leader/waiter/all cancellation, generation races, TTL/capacity, public aliases, disabled gate/default hook, local/BEP-9 separation, and existing replay/hash-binding tests. Capacity eviction may legitimately refetch before TTL under churn; no guarantee of one lookup after eviction. Cache freshness does not restart an existing stream session. VOD, server-api preflight, bare-infohash resolution, peer discovery/budgets, HLS readiness and the 30-second reservoir are outside this fix.
