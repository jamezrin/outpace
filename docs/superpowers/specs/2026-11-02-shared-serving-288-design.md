# Shared internal serving chunks (#288)

Status: implemented and independently reviewed; local gates pass, CI and integration pending. This is an internal serving-stage increment, not end-to-end zero-copy or completion of #288.

## Decision and scope

Use immutable `Bytes` for independently allocated memory-backend chunks and for internal serving read results. Clone payload handles for inbound and reciprocal reply preparation and bounded BitTorrent block assembly. Keep public `chunk`, `shared_chunk` and `shared_block` signatures and owned return semantics unchanged. Wire messages, reply encoding, reassembly and ingress/publication ownership are not redesigned here.

The current memory backend stores one Vec per chunk and copies it again when serving an owned read; reply building and encoding then make further copies. The new internal read path removes that intermediate payload copy. A borrowed view held across an await would extend store locks; whole-piece shared allocations could pin an entire piece through one small reply. Neither alternative is accepted. Use a separate allocation for each stored chunk, including trusted complete-piece publication.

## Interfaces and ownership

- MemoryBackend stores `Bytes`; legacy puts and complete publication copy into independent chunk allocations. Existing accounting, header upgrade, immutable complete duplicates and eviction remain unchanged.
- Add crate-private `PieceStore::shared_serving_chunk` returning `Option<(Bytes, [u8; 8])>`. Memory reads clone one handle and capture the header under the same lock; disk reads convert the existing owned Vec into Bytes without a payload copy.
- Existing crate-private ReciprocalChunk carries Bytes instead of Vec. Preserve the disk store-owned permit, result/channel cancellation and declaration-order payload-before-permit drop. Memory admission behavior stays unchanged. Keep the reciprocal payload private to the store boundary and expose a borrowed slice to reply preparation, so disk shared handles cannot escape the permit-bearing owner through that API. Existing cancellation tests must snapshot content with an owned copy rather than clone a shared handle before dropping the owner.
- Both live profiles in prepare_reply use internal shared results. Keep reciprocal stream-zero/exact stored-header behavior, inbound requested-stream/header fallback, media-byte accounting and success boundaries unchanged.
- shared_block uses the internal shared read but keeps its current bounded request validation, assembly order, missing-data behavior and owned Vec result. Further range/encoding copies remain visible follow-ups.
- Public shared_chunk remains the compatibility owned-read path; avoid introducing an extra disk copy merely to reuse the internal helper.

## Resource and security invariants

No new queue, task, read concurrency or store admission policy. Verification precedes publication as before; failed signed pieces stay unadvertised and unservable. A held read survives eviction with immutable old bytes, just as the former owned copy did. Eviction stops store accounting but actual shared payload can live until that bounded caller releases its handle; this is not a new aggregate RAM guarantee. Preserve existing inbound/actor bounds and disk cancellation/replacement guards.

A small held chunk must not pin other chunks from an evicted piece. Bytes has larger handles and may allocate shared metadata when first cloned; do not claim every read is allocation-free. Measure cold first serve and warmed repeated serve separately. No cryptographic, protocol, public default, cache budget or operator configuration change.

## Acceptance

Before implementation, retain ordinary/public compatibility and exact prepared-wire/header tests. Add contracts for shared payload pointer identity, immutable replacement/eviction, independent chunk backing (two held chunks become individually unique after store eviction), unchanged byte accounting, memory/disk serving and disk permit lifetime. Reuse existing failed-signature/full-pool, publication and generation/cancellation tests; do not weaken assertions.

Benchmark fixed synthetic chunks through actual prepare_reply in both profiles, with cold and warmed cases, before/after. Record copy-removal reasoning separately from timings, allocator totals, visible payload budgets and RSS. Use a bounded warmed three-trial sequence and source/log provenance. No live performance or readiness claim.

Run affected store/protocol/authentication/actor tests and appropriate repository gates, independent correctness/performance/test-fidelity review and CI before merge. Leave #288 open for ingress/reassembly/wire encoding copies and the dependency follow-up unless separately reconciled. No deployment or live observation is required.

## Local evidence

Store and seed baselines passed (37 and 30 tests). New contracts cover shared payload pointers, independently owned chunks after eviction, legacy replacement/header snapshots, public owned APIs and permit-bound reciprocal views. Exact prepared live-wire profiles and BitTorrent replies are checked on both backends. The full workspace passed 1,617 tests before two final test-only controls; the final focused shared filter passed 9 tests, including those controls. Workspace all-target Clippy, formatting, hygiene and whitespace gates pass. Independent review found no P1/P2 issues; the two nonblocking coverage areas were addressed afterward with focused tests.

The fixed synthetic prepare_reply benchmark shows a warm repeated-serving benefit. Cold trial ranges overlap, so no clear cold improvement is established. Store construction is excluded from timing; cold cases use fresh stores and warm cases use an untimed reply before measurement. No end-to-end network, allocator/RSS or statistical significance claim follows.
