# Issue 287 stage 2: actor-owned bounded gossip future

Status: user approved stage 2 implementation after stage 1 merge ea80782. Implementation present in the root-owned stage-2 worktree; final offline gates passed; independent review approved; final root gates and publication underway. No issue closure or runtime claim.

## Mechanism

Replace the active PoolActor inline stale-refill gossip await with at most one actor-owned future that owns the stale ConnectedUpstream and its already-encoded signed extended handshake. Poll this future in the main wake select, emitting a Gossip completion wake. No spawned/detached task or additional channel is necessary: Tokio socket operations already yield; selecting this future alongside other wakes allows peer work to proceed. This realizes the issue's nonblocking-gossip goal with structured ownership rather than its suggested spawned-task implementation.

Use a private concrete gossip job wrapper or boxed Send future with bounded result. The current collection loop becomes a reusable single-transport collector returning observations without borrowing actor/candidate state. Preserve existing 32-message limit, outgoing extended handshake fields, no Interested/media requests and existing source/PEX validation/classification. Accumulate in existing SessionCandidates, whose 1024-entry cap bounds returned observations; no second unbounded collection. Reuse current STALE_GOSSIP_BUDGET (750 ms), clamped by remaining startup corroboration deadline. Compute the absolute deadline when admitting the job, including handshake writing and all reads; no timeout renewal. No freshness/output counters updated by gossip work itself.

## Admission and backpressure

Only one gossip job per pool, matching the old handler's one-at-a-time stale harvest. While occupied, disable refill_rx receives only. Keep the existing refill queue capacity, transport reservation set and capped connection set; do not drain into a new queue or discard stale refills to make room. Peer events, discovery, retry clocks, connect completions and output continue through existing actor paths. On Gossip wake, clear/drop the completed job, merge bounded observations into session candidates on the actor, then perform the prior exceptional maybe_refill call. Do not issue that exceptional refill early at job submission. Ordinary other wakes may refill using their existing policy.

The held stale transport replaces the same transport previously owned inside the inline helper; do not add a second gossip transport or grow connection reservations. Existing connection tasks may complete into the same bounded refill queue as before. Preserve reservation-release and explored/failed marking order at stale-refill admission. Audit any predicate needing gossip occupancy to avoid changing configured connection ceilings.

## Lifetime and fairness

Job owns all transport state. Pool return, consumer cancellation, stale teardown and future cancellation drop the job/socket directly; no background worker survives, no join coordinator or detached cleanup required. A job's result can only be consumed by the owning pool, so no cross-generation result channel exists. Drop before replacing any job; never transfer a job into the next pool generation. Preserve peer/connection task drop ordering from stage 1 and document placement of the new field.

Maintain existing wake priority/guards as far as possible; add only Gossip completion and refill-busy suppression. Ensure the finite closed-worker prefix service cannot indefinitely prevent polling an already-ready gossip result; characterize its existing bound. The job must be polled alongside the consumer-closed wake, and dropping the actor must immediately release its socket even if the remote is silent. Existing unrelated blocking awaits remain their separate boundaries.

## Pre-pool path and verified store

Keep the separate pre-pool harvest semantics: it runs before an active PoolActor exists, has the same bounded batch budget and consumer cancellation, and may reuse the collector with its existing session-owned merge. Do not add an extra outer background task or allow it to cross pool generations. It may still delay startup by its existing bounded budget; report this separately from active-loop responsiveness.

Accepted whole-piece store publication, reciprocal reply preparation, activation writes and downstream output await order remain unchanged. In particular #285's borrowed accepted bytes, one pending owned disk piece, and publication-before-playback guarantees are untouched. No claim that gossip isolation alone satisfies all of #287's no-long-inline-await acceptance. If remaining store/reply/activation waits block full acceptance, #287 remains open pending a separately designed change.

## Test-first acceptance

- Behavioral RED through production actor path: keep a stale gossip peer silent behind deterministic handshake/read barriers; a healthy peer must receive requests and deliver authenticated output before gossip is released or its budget expires. Existing inline code must fail this progress contract.
- Positive source/PEX learning: valid bounded gossip result becomes eligible through actor merge; stale peer receives no media requests or admission credit.
- Bounds: a second refill stays in existing bounded channel while gossip is busy; only one gossip transport, <=32 reads and <=1024 retained observations, fixed deadline including writes, startup clamp, no deadline renewal from chatter.
- Cancellation/teardown: consumer closes while job is silent, actor exit/drop closes socket and leaves no owned job; no result applied to replacement pool. Normal completion re-enables refills. Test both completion and cancellation paths with finite task/socket cleanup.
- Preserve #284 terminal FIFO and queue-credit controls; #285 authentication and store publication controls; existing startup/reconnect/discovery/recovery tests. Do not relax their assertions to accommodate this change.
- Full offline locked workspace tests, all-target Clippy -D warnings, formatting, hygiene and independent review. No live request until reviewed merged build; subsequent bounded matched smoke uses approved private VPN-routed environment and unencrypted scope gate.

## Delivery

New chore branch from current main, separate from stage 1; root owns review/publication. No dependency, exposed default, network, release or deployment change. Stage 2 may close only the proven gossip responsiveness portion; issue/milestone closure requires full remaining acceptance assessment. v0.9.0 release remains premature.
