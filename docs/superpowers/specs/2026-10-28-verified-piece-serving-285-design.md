# Issue 285: publish accepted whole live pieces

Status: root accepts test-first implementation, subject to the playback-preservation boundary below.
Issue: https://github.com/jamezrin/outpace/issues/285 (public unauthenticated read, 2026-10-28: open, unassigned, zero comments).

## Evidence and scope

The actual full-pool RSA rejection regression reports rejected-state `((true, true), true, true)` for store completeness/list membership and inbound/reciprocal reply preparation, where all must be false. Valid retry playback and the one-rejection control completed before failure. This is behavioral RED, not observed wire advertisement evidence. Private evidence remains in ignored `re/sdd/issue285-red-result.md`; the authentication test belongs to root and must be preserved.

Fix untrusted live download publication without changing playback output, defaults, seeding policy, queue admission, or public API visibility for test convenience. No dependence on unpublished #284 (reviewed separately at beca4d8); integrate it deliberately after publication.

## Approaches and decision

Propose the issue's preferred commit-after-verification route. The reassembler already owns bounded partial and completed wire buffers and authenticates whole pieces. Expose newly completed wire bytes independently of ordered playback, then publish a whole accepted piece atomically. This avoids another incoming-chunk map and closes partial-serving, reject, and completed-duplicate overwrite windows together.

Store staging with explicit verified generations could work but broadens every reader and writer contract. Purge on rejection alone leaves pre-verification serving and completed-ahead duplicate overwrite windows; reject that approach. No purge-only or availability-bit-only substitute satisfies this contract.

## Minimal interfaces and ownership

Names below are proposed interfaces for implementation, not existing APIs.

* `ace-wire/src/reassembly.rs`: retain existing `add_block`, `add_block_with_contribution`, `ready_chunks`, and `take_ready` behavior. Add `add_live_chunk(index: u64, chunk: u16, chunk_length: u64, header: [u8; 8], block: &[u8]) -> Result<LiveBlockOutcome>`, with `LiveBlockOutcome { contributed: bool, completed: bool }`. Add `completed_live_piece(index: u64) -> Option<CompletedLivePiece<'_>>`, returning borrowed full `wire: &[u8]`, consistent `header: [u8; 8]`, and `acceptance: LiveAcceptance`. Store metadata alongside existing partial/complete entries, not in a second chunk map. Existing generic callers remain supported.
* `LiveAcceptance`: `SourceVerified`, `UnsignedAssemblyAccepted`, `Unauthenticated`. Successful configured RSA verification yields the first; no trailer and no verifier yields the second. A nonzero trailer without a usable verifier yields the third and is ineligible for seed publication. Never label unsigned assembly cryptographically verified. Playback behavior stays as today, including existing keyless playback compatibility.
* `ace-swarm/src/store.rs`: add `PieceStore::shared_put_complete_piece(store: &Arc<tokio::sync::Mutex<Self>>, piece: u64, header: [u8; 8], wire: &[u8]) -> std::io::Result<CompletePut>`, with `CompletePut::{Published, AlreadyPresent, NotRetained}`. The name conveys atomic completeness, not cryptographic proof: the caller owns trust. Validate geometry and exact wire length before mutation. Disabled/undersized cache returns `NotRetained`; duplicate retained complete index returns `AlreadyPresent` without changing bytes/header/age/budget. Return errors for invalid geometry or I/O failure; playback continues under existing cache-failure policy.
* `ace-swarm/src/store/disk.rs`: one whole-piece actor command, one per-store publication semaphore permit held by the command until index/budget commit or failure. Acquire permit before copying borrowed wire into an owned command buffer. Async store helper extracts handle/geometry under the Tokio mutex, releases it, then awaits permit/I/O. No synchronous disk I/O under a Tokio lock. Preserve legacy trusted chunk commands.
* `ace-swarm/src/live_session/pool.rs`: remove the untrusted per-chunk store write. Invoke `add_live_chunk`; only `completed == true` may borrow a completion and await whole publication, before `take_ready_with_stats` can drain it. Publish ahead-complete pieces immediately. Preserve contribution attribution, receipt accounting, recovery events and retry scheduling. No public seed/protocol changes should be necessary.

## Geometry and header contract

For live reseeding support only: positive piece/chunk lengths, exact divisibility, quotient in `1..=u16::MAX`, checked offset arithmetic and representable allocation lengths. Every chunk must be exactly `chunk_length` bytes, with index below quotient. Reject zero, short, long, overlapping/misaligned and out-of-range blocks before assembly mutation; do not count arbitrary distinct offsets as coverage. Do not silently invent a short-final-live-chunk geometry. Existing VOD short-final-piece handling is outside this new interface and remains unchanged.

First contributing chunk fixes the eight-byte publication header (including zero); later contributing chunks must match for reseeding eligibility. Header consistency is not source authentication: the source signature authenticates its existing payload domain, not necessarily this wire header. Check completed/stale submissions first: corrupt or malformed late duplicates are no-ops and cannot invalidate or overwrite published content. Rejected assembly metadata must not contaminate retry.

### Mandatory playback-preservation boundary

Additional geometry, header-consistency and authentication-status checks govern publication eligibility only. They must not introduce new playback rejection, clear otherwise accepted assembly, alter contribution/retry bookkeeping, or change ordered playback bytes beyond existing verifier behavior. In particular, a header conflict may make an assembly ineligible for reseeding without invalidating its signature-accepted playback. Existing generic reassembly and keyless playback compatibility remain unchanged. Invalid publication geometry must fail closed at the store/publication boundary, rather than silently tightening the downloader protocol. The implementation must represent publication eligibility separately from completion/contribution and test this distinction.

## Atomicity, concurrency and resource proof

Memory publication constructs the exact chunk representation before inserting under the store mutex. Bytes, header, age and budget/eviction update in one critical section, so readers observe absence or a complete accepted piece. Never expose per-chunk intermediate entries. Retained complete pieces are immutable through the new interface; retain legacy trusted overwrite semantics separately.

Disk actor writes the full wire into an unpublished temporary file in its cache directory. On failure, remove the temporary file and leave index/budget unchanged. On success, install the file under actor serialization, then insert the entire header/chunk index and update retained-byte accounting/eviction in one metadata critical section. Index entries remain absent throughout writing. Filesystem calls and eviction unlink happen outside metadata locks; remove index/bytes before unlink. This is in-process publication atomicity, not a power-loss durability guarantee. Existing initialization clears cache; no new recovery format is required. Trusted legacy partial entry at the same index may be replaced only as one atomic complete publication; an already complete entry wins unchanged.

One new publication command may be pending or executing per store. Its semaphore permit is owned by that command, so caller cancellation after admission cannot free capacity early. Waiting tasks do not allocate an owned wire copy. The live pool serially awaits publication, retaining its existing bounded reassembler buffer. Extra owned pending wire bytes are at most one configured piece per store; retained bytes remain at most `max_bytes`, with at most one piece of temporary disk storage beyond retained disk budget while writing. Account the pending buffer separately from retained bytes; do not claim total memory equals the cache limit. Existing reassembly bound is `max_reasm_pieces_ahead * piece_length` plus fixed per-piece metadata; add no unbounded completion queue or chunk map. This does not repair the preexisting general disk command channel (#284 is separate).

Before admission, cancellation drops borrowed work with no publication. After admission, a valid accepted write may finish despite caller cancellation; actor still commits or fails bytes/index/budget together and releases permit. Session cancellation cannot expose partial/rejected staging because it never entered the store. Already published valid pieces survive until normal eviction. Actor shutdown drains admitted commands and cleans temporary files; no detached per-publication worker tasks.

Availability and chunk/block reads share the same published representation. A read racing eviction may return None; a previously prepared reply may still carry accepted bytes after eviction. Neither permits rejected bytes. Store lease/geometry isolates unrelated content; no new trust flag on globally readable partial entries is needed.

## Writer audit and compatibility

Current production writer inventory: pool network chunks; `ace-engine/src/broadcast_ingest.rs` local signed chunker with publication/cancellation fence and blocking store guard; `ace-engine/src/ace_provider/vod.rs` whole SHA-1-verified downloader output split into cache chunks. Other direct writes found are fixtures/tests. Only the pool changes to whole-piece publication. Broadcast's trusted partial publication and VOD's final-piece behavior keep their current semantics; do not migrate them incidentally or weaken their fences. Existing chunk API is explicitly trusted-input compatibility, never used for incoming live peer data. No arbitrary API weakening, default changes, logging dependencies, identifiers or live validation.

## Acceptance and offline tests

1. Preserve and pass root's actual RSA/full-pool RED, retaining rejection count and valid retry/playback controls. Add first-chunk barrier: no availability, either reply profile, or inbound BT block reply before completion.
2. Valid signed wire: exact bytes including signature tail and header served under both profiles (including existing stream/fallback-header differences), ordered playback emits only media. Unsigned `sig_len == 0` uses explicitly assembly-accepted publication. Nonzero trailer without usable key is never represented as verified or published by pool.
3. Completed ahead of missing cursor: available/servable before playback readiness; corrupt duplicate (data and header) cannot overwrite it or emit again. Conflicting partial headers reject cleanly; a fresh valid retry with last chunk first cannot combine with old rejected data.
4. Actual inbound `SeederSession` frames from the producer-populated shared store: decode live id=5 bootstrap and id=4 advancing HAVE. Incomplete/rejected index excluded; valid retry/ahead completion included. Use deterministic barriers and a valid positive marker, not sleeps or inference from `have_pieces` alone. Keep private test access in seed tests rather than widening APIs.
5. Parameterize whole-store publication and full-pool rejection/retry over memory and disk. At a test-only disk write barrier, assert no index/availability/reply until complete commit. Cancel before admission and after admission; join/drain actor and assert exact retained/pending accounting and normal eviction. Inject I/O failure to prove no partial index/temp leak; conflicting concurrent complete puts preserve the first committed bytes/header.
6. Geometry controls: short/long/empty chunks, bad index, zero lengths, nondivisible lengths, quotient overflow and arithmetic overflow fail without publication/allocation growth. Legacy VOD/broadcast and existing seeding/profile regressions stay green.

Implementation must add a short protocol note describing this policy and evidence for the #208 gate; do not tick a public checklist without verification/publication authority. Run focused tests, workspace tests, clippy with warnings denied, identifier hygiene and formatting verification when root authorizes implementation. No tests or builds were run by the design agent.
