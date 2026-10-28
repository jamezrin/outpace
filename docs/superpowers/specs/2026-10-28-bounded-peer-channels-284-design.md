# Bounded peer-command admission and actor progress (#284)

Revised design proposal after root review. DESIGN ONLY: root will accept this revision and assign the next RED/test contract before implementation.

## Evidence and scope

Real workers with production 64-command/default128-event channels reproduce actor stalls in both RequestPiece and Send enqueue. The component proof and source show a cyclic wait: the actor waits for command capacity while a worker can wait to report an event. This establishes a defect class, not historical #246 playback causality.

The full-pool investigation establishes an observability boundary, not another saturation RED. Its TCP fixture cannot receipt exact private queue occupancy or actor progress without a seam; kernel buffering and absent output are insufficient. Existing learned-source TCP recovery/cancellation passes and remains required regression coverage. Exact duplex component receipts are the saturation evidence; a new exact-state TCP oracle is not an implementation prerequisite.

## Request admission: actual slots before ownership

Root rejected unconditional RequestPiece Full eviction as a normal burst policy. Default max_piece_advance256 can exceed64 before a healthy worker is polled. Ordinary full request queues must defer work without losing a healthy peer.

At each advance_pool_requests call:
1. Remove observed closed command runtimes through drop_peer_runtime and retain their loss addresses once.
2. Snapshot each remaining sender.capacity() as that peer's RequestPiece admission credit. Capacity counts ALL queued command kinds; one RequestPiece consumes one slot regardless of chunks_per_piece. Missing peers have zero credits.
3. Use those credits while the pure scheduler selects assignments, before inserting global requested or ActivePeers in-flight ownership. For each peer admit at most min(policy spare, queue slots). Zero-credit work remains unscheduled and receives no fresh requested_at timer.
4. try_send the bounded selected batch without any intervening await. On success retain/create its timer with entry.or_insert(now). On unexpected Full or concurrent Closed remove that peer immediately through the existing loss path, releasing ALL its reserved batch entries (including later unsent entries). Continue other peers; return each successfully removed address once.

The actor is the sole production command producer and holds exclusive mutable runtime state throughout snapshot/selection/enqueue. Workers can consume slots, increasing availability; they cannot reduce it. Thus normal snapshot-admitted RequestPiece sends cannot be Full. Full remains a defensive invariant/race fallback, not normal queue policy. Do not add another sender producer or channel permit reservation without revisiting this proof.

Preserve the scheduler's existing bounded scan ceiling derived from TOTAL POLICY SPARE, not the smaller sum of queue credits. Otherwise full A's earlier-only pieces can hide an eligible later B piece. Maintain original lowest-piece iteration, windows, choke filter and most-policy-spare ranking/tie-break among peers with credits; decrement policy spare and credit independently. No enqueue-then-rollback scheme for routine zero-credit work, no synthetic inflated in_flight, and no alternate scheduler.

Add crate-private assign_with_queue_slots methods to Scheduler and ActivePeers, taking the existing arguments plus &BTreeMap<u64, usize>. Existing public assign signatures/semantics remain unchanged through the same internal selection core. pool_support.rs calls the constrained ActivePeers path directly; the existing schedule_piece_assignments wrapper and unrelated callers remain unchanged.

## Reliable opportunity after worker drain

Choose the EXISTING pool-loop timed credit poll: after retransmit_stalled_requests, unconditionally call advance_pool_requests and record returned losses before the normal empty-pool/wait handling. This must run even when no retransmit or incoming event changed anything. The existing event wait expires no later than request_check_interval (default1000ms); a timeout continues to the loop top and performs a fresh capacity scan. A received event also returns through the loop. No pending-credit Boolean, acknowledgement channel, Notify, new interval, detached task or worker change is needed.

A drain before the scan is seen immediately. A drain after a zero-credit scan is seen on the next timed turn even if no peer sends any incoming message. Under an idle wait the added admission latency is at most the existing interval; existing handler work may delay a turn. Do not claim an independent hard wall-clock guarantee through unrelated awaits. Keep stale/startup/request deadlines, downstream cancellation and progress accounting unchanged; polling, enqueue and drain are not authenticated output/activity and must not refresh freshness deadlines. No spin/retry loop or waiting for capacity.

## Send overload and lifecycle

send_peer_command uses try_send. Full/Closed drops the peer through drop_peer_runtime and existing record_pool_losses; absent peer remains the existing no-op contract. This retains the deliberate Send-overload tradeoff: even one Send can encounter a shared queue filled by requests or earlier Sends and lose that peer, with normal failure/cooldown accounting. No reserved control slot is introduced. It must not block the actor or silently discard a message while retaining the peer as healthy. Healthy mixed request/control regression coverage must expose this remaining tradeoff; routine RequestPiece credit exhaustion itself is not loss.

No worker Message or terminal Lost delivery is dropped. Reliable bounded events remain awaited. The actor never waits for command space: admitted requests/Send return, zero-credit requests defer, and Send overload/unexpected failure removes the runtime. Therefore the command-capacity edge closing the reproduced cycle is absent. The actor can drain events; a parked Lost remains abortable by ordinary loss/shutdown. Other actor I/O contracts remain outside this proof and #287 remains separate.

## Ownership and preservation

Keep helper async signatures and callers; production scope is scheduler.rs plus live_session/{pool_support.rs,pool.rs}. peer.rs, queue sizes, options/defaults, public APIs, dependencies, activation, authentication, geometry and caches remain unchanged.

drop_peer_runtime owns peer removal/abort and releases that peer's active/global scheduler assignments. Caller record_pool_losses accounts once; stale events fail PeerEvent::is_current. No blanket timer purge, complete_everywhere, partial discard, session reset, cursor skip or recursive scheduling. Existing admitted-work timers retain loss/retransmit semantics; fresh deferred/rejected work gets no new timer. Preserve B ownership/timers, duplicate-active bookkeeping, partial chunk/provenance evidence and verified ready data.

## Acceptance tests and limits

Luna owns tests; this turn changes documents only. Root next assigns revised RED contracts:
- Real-worker default256 request burst: first turn with no worker opportunity admits at most64, retains the peer, and leaves remaining pieces unowned/untimed. Real drain receipts plus later poll turns admit all256 exactly once/FIFO without incoming peer messages, eviction or queue increase.
- Full A with later eligible B: defer A's new piece, keep old A slots, advance B despite the earlier gap; draining A resumes on a timed turn, not a retransmit-state change. Test actual production poll wiring as specified in the plan.
- Closed RequestPiece cleanup, defensive Full cleanup at the private admission boundary if needed, Send Full/Closed and stale-loss once-only accounting; partial/healthy/fairness/startup/choke/retransmit controls.
- Keep Send draining/no-loss control with a real free-slot barrier before its final enqueue, plus still-Full Send eviction. The old RequestPiece RED's A66-owned/evicted expectation must become A66-deferred, A retained and B progress; its positive no-stall assertion and 64/128 receipts remain.
- Existing actual TCP healthy recovery and outer consumer-close cleanup, without claiming they observe 64/128 saturation. Preserve unconditional finite worker/I/O cleanup.

A small cfg(test)-only per-session receipt seam may be proposed if an actor assertion needs internal state, but is optional and requires a specific test contract first. No blind TCP buffering experiment or new instrumentation framework is part of the chosen production fix. No real identifiers/media/private evidence in tracked files, no new configuration, and no #246 historical diagnosis.

