# Bounded reciprocal read responsiveness (#287)

Status: user-approved on 2026-10-29; implementation, offline validation and independent review complete. CI and integration pending. Fresh builder check found no material unanswered product choice; its P2 memory-bound correction is applied below, and its minor lifecycle recommendations are incorporated. Implementation is authorized within this contract. Root inspected merged base 8fb8659 plus the independently approved test-only read discriminator. User selected one actor-owned future with bounded per-peer queues and round-robin service; excess requests are ignored for peer retry. Constants and internal interfaces below are root proposals.

## Intent and evidence

Keep live pool control, healthy peer events and consumer cancellation pollable while reciprocal piece serving waits on a store mutex or disk read. Preserve verified-only storage, exact reciprocal wire formatting and existing admission-based upload accounting.

The two test-only regressions in live_session/tests/read_progress.rs pause the real disk Command::Get before I/O after a synthetic accepted whole piece is published. Both miss the finite 250 ms desired-progress assertion; release restores progress, correct reply bytes and teardown. Existing publication controls (2) and reciprocal serving controls (4) pass. The independent fidelity review found no P1/P2 blocker. This is a bounded actor responsiveness observation, not a historical playback diagnosis or universal latency promise.

Current on_message awaits prepare_reply -> shared_chunk -> DiskHandle::get inline. Disk Get uses an unbounded std channel; cancelling a response future cannot retract an already queued command. Existing reply command admission is nonblocking, but preparation prevents actor progress before that point.

## Approaches

Chosen by user: one pool-owned future, bounded per-peer queues, round-robin selection. Reuses the actor select and existing disk actor; no detached task or result channel. Request overload is explicitly ignored for retry.

Alternative: a separately spawned bounded serving worker. It isolates reads but adds a task, request/result queues, join protocol and generation/cancellation ownership. User did not select it.

Inline timeout alone would keep the actor blocked until timeout, and repeated timed-out reads could remain queued in the disk actor. It does not meet the demonstrated responsiveness and bounded abandoned-read requirements.

## Scope and non-goals

Only reciprocal live id-6 serving from PoolActor is moved off inline awaits. Keep inbound seeder serving, ordinary shared_chunk/shared_block APIs, authentication, verified store publication, HAVE/bitfield behavior, playback/cursor holds, session defaults, timers and transport activation unchanged. No new public configuration, dependency, generic executor or actor-wide scheduler. No read timeout claimed to cancel OS I/O. Downstream output backpressure and activation writes still require separate #287 acceptance. No milestone or readiness closure.

## Queue contract

Introduce actor-private ReciprocalRequest { piece: u32, chunk: u16 }; source stream is ignored as today. Parse only the first 10 request bytes after the existing len >= 10 check; preserve acceptance of trailing bytes but retain none of them. Preserve serving before Interested when seeding is enabled. Short/invalid requests, disabled seeding and stale peer events do not create work.

Use one FIFO per CURRENT peer generation, at most 64 WAITING typed requests per peer, plus at most one actor-wide in-flight request. Capacity 64 is an internal root choice matching existing peer command slots; it is not a claim that all arbitrary bursts were previously admitted. Duplicate requests remain FIFO entries, bounded by the same cap; do not invent deduplication semantics. Full queues drop the new request without eviction, loss accounting, timers, upload increments or a fabricated wire error. Retrying after capacity is free can succeed.

Round-robin service advances a peer-ID cursor after each selected request, wrapping over nonempty current queues. FIFO within a peer is preserved; an active read counts as that peer's current turn. A newly queued peer cannot be starved by another peer refilling its queue. Selection skips/removes stale generations. Peer IDs are actor-local monotonic IDs; queued work and completions never key only by socket address. Keep queue maps restricted to current peers; prune against the full current peer map on pool turns, loss and replacement, including before admitting another request. A loss handler may remove additional stale sources, so pruning only the explicit event ID is insufficient. Never retain dead-peer tombstones or a duplicate ready-peer list.

With N <= configured validated max_active_upstreams <= 256, waiting metadata <= 64*N <= 16384 requests, plus one active request. Do not retain original request payload Vecs. Existing event and command queue bounds remain separate and unchanged.

## Store-owned reciprocal read admission

One actor future is insufficient across cancelled/replaced pool generations: old Get commands can survive their callers. Add ONE reciprocal-read semaphore per DISK STORE, independent of the existing publication semaphore. Reuse it across all handles and generations; never allocate one per pool or per request.

Add a crate-private shared_reciprocal_chunk path used only by reciprocal prepare_reply. Memory uses the existing shared mutex/chunk lookup semantics. Inbound and ordinary reads remain on their existing path. Disk admission waits asynchronously without holding the store mutex or owning data; clone a stable handle, release the mutex, acquire the store semaphore, then enqueue one reciprocal Get. This path may share internal read helpers with ordinary Get without changing ordinary API admission.

The queued/executing reciprocal command OWNS its permit. A successful result transfers the permit with its owned chunk/header into the oneshot response. Keep the permit wrapper intact through response consumption/reply construction, then drop the read data before releasing admission; it must not be released merely because the disk thread sent bytes to an unpolled receiver. A missing/error read drops its permit; failed sends drop data and permit. Caller cancellation before admission enqueues nothing. Cancellation after enqueue drops only the receiver; the command completes safely and then releases bytes and permit. A buffered result whose caller is cancelled drops both. There is consequently at most one reciprocal command/result holding store-owned read admission, even across pool replacement and repeated cancellation. Other existing disk APIs are outside this narrow bound.

Private proposed types/interfaces: ReciprocalChunk owns Vec data, header and optional disk permit; shared_reciprocal_chunk(&Arc<Mutex<PieceStore>>, piece, chunk) -> Option<ReciprocalChunk>. prepare_reply keeps its existing signature/profile semantics and chooses the admitted path only for ServeProfile::Reciprocal. Internal names may change without changing this contract. Do not expose permits or unverified insertion to callers.

The admitted disk read must reuse the original real-Get test gate/read helper so the reviewed read-progress tests stay unchanged and retain their witness. Keep the store mutex unheld across admission, disk I/O and result wait. Do not use the publication permit for reads: independent reads must not add a publication dependency cycle. No new disk thread, task, blocking-pool job or timeout retry loop.

## Actor operation and completion

Add actor-private reciprocal state (queue map/cursor and Option<ReciprocalRead>). The owned boxed future captures only Arc store, normalized request and originating peer ID, not self, mutable peers, Continuity or transport. It calls the unchanged reciprocal formatting helper through the admitted path. At most one future is present.

Poll it at every actor turn BEFORE the finite closed-peer event prefix, as well as in the existing select. This prevents already-ready completion from being starved by that prefix. Bound each turn to one completion and one new start, accounting for BOTH initial poll and select; do not spin through a whole ready queue or add a general rotating scheduler. If that completion consumes the turn budget, arrange the next actor turn without parking pending FIFO work until an unrelated event/timer. Preserve pending lock/semaphore futures across select calls; do not cancel and recreate them on each wake. Consumer closure is checked before starting/completing work and remains a direct select branch. While a read waits, do not pause events, publication, gossip, discovery, request/retransmit timers or playback beyond their existing independent holds.

When a future completes, clear its slot, explicitly check that its exact peer generation is still current (send_peer_command returning None alone also covers an absent peer), and only then pass the prepared message to existing nonblocking send_peer_command. On successful command admission increment uploaded by stored payload bytes and update peers_served exactly at the existing success boundary. Cache miss, stale completion, ignored overload and command rejection never increment upload counters. Existing Send Full/Closed loss handling remains unchanged; reciprocal serving adds no alternative producer-loss path. No socket write is awaited by the actor.

When a peer disappears, immediately discard its waiting queue. If it owns the active read, keep the single future until completion while the pool is otherwise alive; mark/discover its result as stale and discard it without send or accounting. This avoids repeatedly abandoning and restarting work on every peer-loss event. Consumer cancellation or pool teardown drops all queues and the future immediately. Already admitted store commands follow the owned-permit rule and may drain afterward, without callback or transport access. Replacement pools start with empty reciprocal queues, and share only store admission, never old actor completions. Reconnecting the same address cannot receive old-generation replies.

## Memory and ordering

Let B be the actual retained chunk payload length returned by the store. New request retention is bounded metadata; the single active reciprocal operation retains at most one read payload and one formatted reply during construction, approximately 2*B + 10 bytes plus allocation/type overhead. For accepted live whole-piece publication, existing validated geometry bounds B by the configured chunk size (production descriptors require 16 KiB). Legacy trusted chunk insertion does not universally enforce that size; preserve its existing semantics and do not claim a new universal 16 KiB bound or add a size cap in this change. After command admission the reply belongs to the existing bounded worker command channel; do not clone it into a second queue. Those existing per-peer 64-command buffers and peer codec frame limits are not new allocations hidden in this claim. No aggregate RSS or whole-daemon byte limit is promised.

The disk semaphore bounds reciprocal commands and buffered read results per shared store, not all ordinary disk reads or all pending Tokio futures in all daemon callers. Pool count/session admission remains separately configured and defaults unlimited. Never assert a process-wide memory cap from this design.

Publication remains atomic/immutable and reads see only the retained index. No fallback to reassembly/unverified chunks. Eviction before lookup yields no reply; once a valid stored chunk has been copied, completing its reply after later eviction preserves current read semantics. Reciprocal profile still sends stream zero, exact stored header (including zeros), requested piece/chunk and raw stored wire chunk including signature tail. Inbound stream echo/header fallback and BT requests remain unchanged.

## Required verification

1. Preserve both reviewed read-progress tests unchanged as the original RED -> GREEN gate. Existing publication and reciprocal profile controls remain green.
2. Gate reciprocal store-mutex acquisition as well as disk read; unrelated control and consumer-close must progress. Tests use actual actor and deterministic witnesses, release/drain/reap before assertions.
3. FIFO requests per peer, round-robin across two peers under flood, 64 queued plus one global active bound, ignored 65th waiting entry, retry after capacity, malformed/trailing/disabled behavior. Check actual request/reply coordinates and bytes rather than only counters.
4. Same-address generation replacement and peer loss during paused reads: no old reply or accounting; healthy current peers proceed after release. Queue pruning remains bounded after many replacement generations.
5. Cancel before admission, during queued/executing read and with a completed but unconsumed result. Use two pool generations sharing one store; prove one admitted reciprocal command/result and correct permit restoration, no copied data before admission, and no new command until the old permit is free. Verify ordinary/inbound reads and independent publication behavior remain correct.
6. Exact profile/header/signature bytes; missing/evicted/disabled cache; Send Full/Closed and successful admission accounting. Preserve verified-only serving tests and existing producer-loss lifecycle coverage.
7. Focused actor/store/protocol suites, workspace tests, all-target Clippy -D warnings, fmt, identifier hygiene and diff checks. Fresh independent product review and CI before merge. Runtime comparison only under a later explicit bounded scope; no automatic soak.

## Delivery

After user design approval, create the implementation plan, move reviewed test diff into an isolated fix branch from main, execute test-first stages and retain private pins/logs. Root owns commits/publication. Production implementation proceeds only within the approved contract.
