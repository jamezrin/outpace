use super::*;

fn receive_pool_refill(
    refill: PoolRefill,
    learned_pending: &mut HashSet<SocketAddrV4>,
    _candidates: &mut SessionCandidates,
) -> Option<ConnectedUpstream> {
    match refill {
        PoolRefill::Learned(upstream) | PoolRefill::ReservedDiscovered(upstream) => {
            learned_pending.remove(&upstream.addr).then_some(upstream)
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::ace_provider) async fn activate_upstream_peer(
    peer_id: u64,
    mut upstream: ConnectedUpstream,
    start: u64,
    identity: &Identity,
    continuity: &mut Continuity,
    event_tx: &mpsc::Sender<PeerEvent>,
    reachability: Option<&Arc<ReachabilityMonitor>>,
    replacing: Option<(&mut BTreeMap<u64, PeerRuntime>, u64)>,
) -> Result<(PeerRuntime, Option<SocketAddrV4>), SocketAddrV4> {
    // Harvest the public IP this peer echoed to us in `yourip` (issue #22). This is the single
    // funnel every activated outbound upstream passes through, so recording here captures the
    // observation once per live peer. Inert when no monitor is supplied (inbound serving off).
    if let (Some(monitor), Some(ip)) = (reachability, upstream.yourip) {
        monitor.observe_yourip(ip);
    }
    let peer_min = upstream.window.min_piece.max(0) as u64;
    let peer_max = upstream.window.max_piece.max(0) as u64;
    let hs = OutgoingExtendedHandshake {
        ace_metadata_version: 1,
        ut_metadata_id: 2,
        mi: Some(LivePosition {
            min_piece: start as i64,
            max_piece: continuity.head as i64,
            position: -1,
            distance_from_source: 1,
        }),
        node: NodeFields {
            ts: 5000 + peer_id as i64,
            ..NodeFields::default()
        },
        peer_ip: Some(upstream.addr.ip().octets()),
        metadata_size: None,
    };
    if upstream
        .session
        .send_signed_extended_handshake(&hs, identity)
        .await
        .is_err()
        || upstream
            .session
            .send(&PeerMessage::Interested)
            .await
            .is_err()
    {
        return Err(upstream.addr);
    }
    // Only successful writes consume the old provisional transport. A failed candidate
    // leaves its runtime, gossip reader, and request slots intact.
    let replaced = replacing.and_then(|(peers, id)| drop_peer_runtime(id, peers, continuity));
    continuity.register_active_peer(peer_id, upstream.addr, upstream.window);
    let (command_tx, command_rx) = mpsc::channel(64);
    let worker = tokio::spawn(peer_worker(
        peer_id,
        upstream.addr,
        upstream.session,
        command_rx,
        event_tx.clone(),
        continuity.request_floor.clone(),
    ));
    Ok((
        PeerRuntime {
            addr: upstream.addr,
            min_piece: peer_min,
            max_piece: peer_max,
            unchoked_peer: false,
            produced_output: false,
            seen_ids: HashSet::new(),
            commands: command_tx,
            worker,
        },
        replaced,
    ))
}

pub(in crate::ace_provider) fn recovery_channel_capacities(
    live_recovery: LiveRecoveryConfig,
) -> Result<(usize, usize), String> {
    live_recovery.validate()?;
    let event_capacity = live_recovery
        .max_active_upstreams
        .checked_mul(32)
        .ok_or_else(|| {
            "OUTPACE_MAX_ACTIVE_UPSTREAMS event channel capacity overflowed".to_string()
        })?;
    Ok((event_capacity, live_recovery.max_active_upstreams))
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(in crate::ace_provider) async fn follow_peer_pool(
    upstreams: Vec<ConnectedUpstream>,
    info: &StreamInfo,
    identity: &Identity,
    chunks_per_piece: u16,
    tx: &mpsc::Sender<LiveOutput>,
    downloaded: &Arc<AtomicU64>,
    uploaded: &Arc<AtomicU64>,
    peers_served: &Arc<AtomicU32>,
    seed: &SeedConfig,
    store: &Arc<tokio::sync::Mutex<PieceStore>>,
    continuity: &mut Option<Continuity>,
    refill_candidates: Vec<SocketAddrV4>,
    known_refill_peers: Vec<SocketAddrV4>,
    discovery: PeerDiscovery,
    candidates: &mut SessionCandidates,
    peer_count: &Arc<AtomicU32>,
    reachability: Option<&Arc<ReachabilityMonitor>>,
) -> FollowEnd {
    follow_peer_pool_with_discovery(
        upstreams,
        info,
        identity,
        chunks_per_piece,
        tx,
        downloaded,
        uploaded,
        peers_served,
        seed,
        store,
        continuity,
        refill_candidates,
        known_refill_peers,
        discovery,
        candidates,
        peer_count,
        reachability,
        &mut None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(in crate::ace_provider) async fn follow_peer_pool_with_discovery(
    upstreams: Vec<ConnectedUpstream>,
    info: &StreamInfo,
    identity: &Identity,
    chunks_per_piece: u16,
    tx: &mpsc::Sender<LiveOutput>,
    downloaded: &Arc<AtomicU64>,
    uploaded: &Arc<AtomicU64>,
    peers_served: &Arc<AtomicU32>,
    seed: &SeedConfig,
    store: &Arc<tokio::sync::Mutex<PieceStore>>,
    continuity: &mut Option<Continuity>,
    refill_candidates: Vec<SocketAddrV4>,
    known_refill_peers: Vec<SocketAddrV4>,
    discovery: PeerDiscovery,
    candidates: &mut SessionCandidates,
    peer_count: &Arc<AtomicU32>,
    reachability: Option<&Arc<ReachabilityMonitor>>,
    discovery_run: &mut Option<DiscoveryRun>,
) -> FollowEnd {
    let _stats_guard = LiveStatsGuard(seed.metrics.clone());
    debug_assert!(!upstreams.is_empty());
    let lone_fresh = continuity.is_none() && upstreams.len() == 1;
    let primary = upstreams[0].window;
    let primary_addr = upstreams[0].addr;
    let min_piece = primary.min_piece.max(0) as u64;
    let max_piece = primary.max_piece.max(0) as u64;
    let start = match continuity {
        None => {
            let (c, start) = Continuity::fresh(
                info,
                min_piece,
                max_piece,
                seed.prefetch_pieces,
                seed.live_recovery,
            );
            *continuity = Some(c);
            crate::alog!(
                "[ace] {primary_addr}: window min={min_piece} max={max_piece} -> start={start} head={max_piece}"
            );
            start
        }
        Some(c) => {
            let start = c.resume(primary_addr, min_piece, max_piece);
            crate::alog!(
                "[ace] {primary_addr}: reconnected; window min={min_piece} max={max_piece} -> resuming from {start} head={}",
                c.head
            );
            start
        }
    };
    let continuity = continuity.as_mut().expect("initialized just above");
    let live_recovery = continuity.live_recovery;
    if lone_fresh {
        continuity.startup_deadline = Some(Instant::now() + LIVE_START_CORROBORATION);
        crate::alog!(
            "[ace] live start: withholding media for independent window evidence (up to 1000 ms)"
        );
    }
    let (event_capacity, refill_capacity) = match recovery_channel_capacities(live_recovery) {
        Ok(capacities) => capacities,
        Err(err) => {
            crate::alog!("[ace] invalid live recovery configuration: {err}");
            return FollowEnd::ConsumerGone;
        }
    };
    let (event_tx, mut event_rx) = mpsc::channel(event_capacity);
    let (refill_tx, mut refill_rx) = mpsc::channel(refill_capacity);
    // A live-held clone of the refill sender so peers learned from `id=12` peer-exchange
    // gossip can be connected and fed into the same pool-add path (keeps `refill_rx` open
    // even after the background refill task finishes).
    let pex_tx = refill_tx.clone();
    let mut learned_connects: tokio::task::JoinSet<(
        SocketAddrV4,
        Option<PeerConnectFailure>,
        bool,
    )> = tokio::task::JoinSet::new();
    let mut learned_pending = HashSet::new();
    if discovery_run.is_none() && !refill_candidates.is_empty() {
        *discovery_run = Some(DiscoveryRun::start(
            &discovery,
            background_discovery_options(),
        ));
    }
    for addr in refill_candidates {
        candidates.learn(addr, CandidateKind::Discovered);
    }
    let _ = known_refill_peers; // Knowledge already lives in the session candidate store.
                                // No detached discovery/refill producer: the session owns its stream and this pool owns
                                // one capped transport set, including queued reservations of every provenance.
    drop(refill_tx);
    let mut peers: BTreeMap<u64, PeerRuntime> = BTreeMap::new();
    let mut loss_count = 0usize;
    let mut next_peer_id = 1u64;

    for upstream in upstreams {
        let peer_id = next_peer_id;
        next_peer_id += 1;
        match activate_upstream_peer(
            peer_id,
            upstream,
            start,
            identity,
            continuity,
            &event_tx,
            reachability,
            None,
        )
        .await
        {
            Ok((runtime, _)) => {
                candidates.admitted(runtime.addr);
                peers.insert(peer_id, runtime);
            }
            Err(addr) => {
                candidates.explored(addr);
                record_pool_losses(candidates, &mut loss_count, [addr]);
            }
        }
    }
    if peers.is_empty() {
        return FollowEnd::PeerLost(loss_count);
    }
    peer_count.store(peers.len() as u32, Ordering::Relaxed);

    if peers.len() < live_recovery.max_active_upstreams || continuity.startup_deadline.is_some() {
        spawn_candidate_connects(
            candidates,
            &peers,
            info.infohash,
            live_recovery.max_parallel_connect,
            &pex_tx,
            &mut learned_connects,
            &mut learned_pending,
            true,
        );
    }
    let mut last_progress = Instant::now();
    let mut refill_closed = false;
    let mut observed_capacity = (peers.len(), learned_pending.len());
    let mut observed_retry = Instant::now();
    loop {
        let mut released_failed_slot = false;
        while let Some(Ok(result)) = learned_connects.try_join_next() {
            released_failed_slot |=
                finish_candidate_connect(result, &mut learned_pending, candidates);
        }
        let capacity_released =
            peers.len() < observed_capacity.0 || learned_pending.len() < observed_capacity.1;
        if (released_failed_slot || capacity_released)
            && (peers.len() < live_recovery.max_active_upstreams
                || continuity.startup_deadline.is_some())
        {
            spawn_candidate_connects(
                candidates,
                &peers,
                info.infohash,
                live_recovery.max_parallel_connect,
                &pex_tx,
                &mut learned_connects,
                &mut learned_pending,
                true,
            );
        }
        observed_capacity = (peers.len(), learned_pending.len());
        let now = Instant::now();
        publish_live_stats(continuity, seed.metrics.as_ref(), info);
        if continuity
            .startup_deadline
            .is_some_and(|deadline| now >= deadline)
        {
            continuity.startup_deadline = None;
            last_progress = now;
            crate::alog!("[ace] live start: corroboration deadline reached; using best known window without freshness proof");
            let lost = advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
            record_pool_losses(candidates, &mut loss_count, lost);
        }
        let Some(stale_budget) =
            stale_upstream_budget(last_progress, now, live_recovery.stale_upstream_timeout())
                .or_else(|| {
                    continuity
                        .startup_deadline
                        .map(|deadline| deadline.saturating_duration_since(now))
                })
        else {
            let stalled = peers.values().map(|p| p.addr).collect::<Vec<_>>();
            crate::alog!(
                "[ace] upstream pool stale — no live progress for {:?}; reconnecting {} peer(s)",
                live_recovery.stale_upstream_timeout(),
                stalled.len()
            );
            shutdown_peer_runtimes(&mut peers);
            return FollowEnd::PoolStale {
                stalled,
                lost: loss_count,
            };
        };
        // Self-heal a single stuck piece well before the whole-pool stale timeout: re-request
        // pieces outstanding past the request timeout (to a faster peer where possible) and
        // skip a piece evicted from every upstream window.
        let newly_lost =
            retransmit_stalled_requests(&mut peers, continuity, chunks_per_piece, now).await;
        record_pool_losses(candidates, &mut loss_count, newly_lost);
        if peers.is_empty() {
            return FollowEnd::PeerLost(loss_count);
        }
        peer_count.store(peers.len() as u32, Ordering::Relaxed);

        // Wake at least every configured request-check interval so the sweep above runs even
        // while no peer sends anything.
        let wait = stale_budget
            .min(live_recovery.request_check_interval())
            .min(
                continuity
                    .startup_deadline
                    .map(|deadline| deadline.saturating_duration_since(now))
                    .unwrap_or(stale_budget),
            );
        let active = peers.values().map(|peer| peer.addr).collect();
        let retry_deadline = if (peers.len() < live_recovery.max_active_upstreams
            || continuity.startup_deadline.is_some())
            && learned_pending.len() < live_recovery.max_parallel_connect
            && learned_connects.len() < live_recovery.max_parallel_connect
        {
            candidates.retry_deadline_after(observed_retry, &active, &learned_pending)
        } else {
            None
        };
        let event = match tokio::time::timeout(wait, async {
            tokio::select! {
                _ = tx.closed() => PoolWake::ConsumerGone,
                event = event_rx.recv() => PoolWake::Peer(event),
                result=learned_connects.join_next(), if !learned_connects.is_empty()=>PoolWake::ConnectCompletion(result),
                _ = async {
                    tokio::time::sleep_until(retry_deadline.expect("enabled retry deadline").into()).await;
                }, if retry_deadline.is_some() => PoolWake::CandidateRetry,
                peer=next_discovery_peer(discovery_run), if discovery_run.is_some()=>PoolWake::Discovery(peer),
                upstream = refill_rx.recv(), if !refill_closed && (peers.len() < live_recovery.max_active_upstreams || continuity.startup_deadline.is_some()) => {
                    PoolWake::Refill(upstream)
                }
            }
        })
        .await
        {
            Ok(event) => event,
            Err(_) => continue, // sweep tick: re-check stale budget and retransmit
        };

        let event = match event {
            PoolWake::ConsumerGone => return FollowEnd::ConsumerGone,
            PoolWake::CandidateRetry => {
                observed_retry = Instant::now();
                spawn_candidate_connects(
                    candidates,
                    &peers,
                    info.infohash,
                    live_recovery.max_parallel_connect,
                    &pex_tx,
                    &mut learned_connects,
                    &mut learned_pending,
                    true,
                );
                continue;
            }
            PoolWake::ConnectCompletion(result) => {
                if let Some(Ok(result)) = result {
                    if finish_candidate_connect(result, &mut learned_pending, candidates)
                        && (peers.len() < live_recovery.max_active_upstreams
                            || continuity.startup_deadline.is_some())
                    {
                        spawn_candidate_connects(
                            candidates,
                            &peers,
                            info.infohash,
                            live_recovery.max_parallel_connect,
                            &pex_tx,
                            &mut learned_connects,
                            &mut learned_pending,
                            true,
                        );
                    }
                }
                continue;
            }
            PoolWake::Discovery(peer) => {
                if let Some(peer) = peer {
                    candidates.learn(peer, CandidateKind::Discovered);
                }
                if peers.len() < live_recovery.max_active_upstreams
                    || continuity.startup_deadline.is_some()
                {
                    spawn_candidate_connects(
                        candidates,
                        &peers,
                        info.infohash,
                        live_recovery.max_parallel_connect,
                        &pex_tx,
                        &mut learned_connects,
                        &mut learned_pending,
                        true,
                    );
                }
                continue;
            }
            PoolWake::Peer(Some(event)) if event.is_current(&peers) => event,
            PoolWake::Peer(Some(_)) => continue,
            PoolWake::Peer(None) => {
                shutdown_peer_runtimes(&mut peers);
                return FollowEnd::PeerLost(loss_count);
            }
            PoolWake::Refill(Some(upstream)) => {
                let Some(upstream) =
                    receive_pool_refill(upstream, &mut learned_pending, candidates)
                else {
                    spawn_candidate_connects(
                        candidates,
                        &peers,
                        info.infohash,
                        live_recovery.max_parallel_connect,
                        &pex_tx,
                        &mut learned_connects,
                        &mut learned_pending,
                        true,
                    );
                    continue;
                };
                candidates.learn(upstream.addr, CandidateKind::Discovered);
                if peers.values().any(|p| p.addr == upstream.addr) {
                    continue;
                }
                if !continuity.window_can_resume(&upstream.window) {
                    crate::alog!(
                        "[ace] {}: background refill stale window min={} max={} cannot cover next needed piece {}; dropping",
                        upstream.addr,
                        upstream.window.min_piece,
                        upstream.window.max_piece,
                        continuity.reasm.next_needed()
                    );
                    candidates.explored(upstream.addr);
                    candidates.failed(upstream.addr, Instant::now());
                    let gossip_budget = continuity
                        .startup_deadline
                        .map(|deadline| deadline.saturating_duration_since(Instant::now()))
                        .unwrap_or(STALE_GOSSIP_BUDGET)
                        .min(STALE_GOSSIP_BUDGET);
                    tokio::select! {
                        _ = tx.closed() => return FollowEnd::ConsumerGone,
                        _ = harvest_stale_gossip_with_budget(vec![upstream],identity,candidates,gossip_budget) => {},
                    }
                    spawn_candidate_connects(
                        candidates,
                        &peers,
                        info.infohash,
                        live_recovery.max_parallel_connect,
                        &pex_tx,
                        &mut learned_connects,
                        &mut learned_pending,
                        true,
                    );
                    continue;
                }
                let active = peers.values().map(|peer| peer.addr).collect();
                if candidates
                    .exploration_cohort(Instant::now(), &active, &learned_pending)
                    .is_some_and(|cohort| candidates.explorations(upstream.addr) > cohort)
                {
                    // Preserve the opportunity of a less-explored peer still connecting
                    // or queued. Dropping this older transport consumes no new opportunity.
                    continue;
                }
                let corroborates = continuity.startup_deadline.is_some()
                    && upstream.window.max_piece.max(0) as u64 >= continuity.head;
                if corroborates
                    && peers.len() >= live_recovery.max_active_upstreams
                    && upstream.window.max_piece.max(0) as u64 == continuity.head
                {
                    // Independent matching evidence is enough to start. Keep the sole
                    // healthy provisional transport rather than churn it for capacity.
                    continuity.startup_deadline = None;
                    last_progress = Instant::now();
                    crate::alog!("[ace] live start: matching independent window confirmed; retaining current upstream");
                    let lost =
                        advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                    record_pool_losses(candidates, &mut loss_count, lost);
                    continue;
                }
                let replacement = if peers.len() >= live_recovery.max_active_upstreams {
                    if !corroborates {
                        continue;
                    }
                    peers
                        .iter()
                        .min_by_key(|(_, peer)| peer.max_piece)
                        .map(|(&id, _)| id)
                } else {
                    None
                };
                candidates.learn(upstream.addr, CandidateKind::Discovered);
                let peer_id = next_peer_id;
                next_peer_id += 1;
                let addr = upstream.addr;
                continuity.head = continuity.head.max(upstream.window.max_piece.max(0) as u64);
                continuity.skip_far_behind_live(Instant::now());
                match activate_upstream_peer(
                    peer_id,
                    upstream,
                    continuity.reasm.next_needed(),
                    identity,
                    continuity,
                    &event_tx,
                    reachability,
                    replacement.map(|id| (&mut peers, id)),
                )
                .await
                {
                    Ok((runtime, replaced)) => {
                        if let Some(replaced) = replaced {
                            // Already admitted once: no extra exploration or physical-loss
                            // event for retiring a provisional, nonproducing transport.
                            candidates.failed(replaced, Instant::now());
                        }
                        if corroborates {
                            continuity.startup_deadline = None;
                            last_progress = Instant::now();
                            crate::alog!("[ace] live start: independent upstream window confirmed; starting at best known head");
                        }
                        candidates.admitted(runtime.addr);
                        crate::alog!(
                            "[ace] {addr}: added to active upstream pool ({} peer(s))",
                            peers.len() + 1
                        );
                        peers.insert(peer_id, runtime);
                        peer_count.store(peers.len() as u32, Ordering::Relaxed);
                        let newly_lost =
                            advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                        record_pool_losses(candidates, &mut loss_count, newly_lost);
                    }
                    Err(addr) => {
                        candidates.explored(addr);
                        record_pool_losses(candidates, &mut loss_count, [addr]);
                    }
                }
                continue;
            }
            PoolWake::Refill(None) => {
                refill_closed = true;
                continue;
            }
        };

        let (peer_id, addr, msg) = match event {
            PeerEvent::Lost { peer_id, addr } => {
                let lost_producer = peers.get(&peer_id).is_some_and(|peer| peer.produced_output);
                if let Some(lost) = drop_peer_runtime(peer_id, &mut peers, continuity) {
                    crate::alog!("[ace] {addr}: upstream peer lost");
                    record_pool_losses(candidates, &mut loss_count, [lost]);
                    let newly_lost =
                        advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                    record_pool_losses(candidates, &mut loss_count, newly_lost);
                    peer_count.store(peers.len() as u32, Ordering::Relaxed);
                }
                if lost_producer && !peers.values().any(|peer| peer.produced_output) {
                    let cursor = continuity.reasm.next_needed();
                    let stale_sources: Vec<_> = peers
                        .iter()
                        .filter_map(|(&id, peer)| {
                            (peer.max_piece < cursor
                                && candidates.kind(peer.addr) == CandidateKind::Source)
                                .then_some(id)
                        })
                        .collect();
                    for id in stale_sources {
                        if let Some(source) = drop_peer_runtime(id, &mut peers, continuity) {
                            // This transport still works but its old window cannot cover
                            // the cursor. Refresh current source priority after the last
                            // producer is lost; retain all usable fallback transports.
                            candidates.refresh_stale_transport(source, Instant::now());
                            crate::alog!(
                                "[ace] refreshing stale source window after producer loss"
                            );
                        }
                    }
                    peer_count.store(peers.len() as u32, Ordering::Relaxed);
                }
                if peers.is_empty() {
                    return FollowEnd::PeerLost(loss_count);
                }
                continue;
            }
            PeerEvent::Message { peer_id, addr, msg } => (peer_id, addr, msg),
        };

        let mut made_activity = false;
        let mut made_output = false;
        if let PeerMessage::Extended { payload, .. } | PeerMessage::Unknown { payload, .. } = &msg {
            observe_live_window(continuity, peer_id, payload);
        }
        match msg {
            PeerMessage::Unchoke => {
                continuity.set_peer_unchoked(peer_id, true);
                crate::alog!(
                    "[ace] {addr}: UNCHOKE -> scheduling from piece {} toward head {}",
                    continuity.reasm.next_needed(),
                    continuity.head
                );
                let newly_lost =
                    advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                record_pool_losses(candidates, &mut loss_count, newly_lost);
                made_activity = true;
            }
            PeerMessage::Choke => {
                continuity.set_peer_unchoked(peer_id, false);
            }
            PeerMessage::Have(p) => {
                let old_head = continuity.head;
                continuity.head = continuity.head.max(p as u64);
                update_runtime_window(&mut peers, continuity, peer_id, p as u64);
                made_activity |= continuity.head > old_head;
                let newly_lost =
                    advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                record_pool_losses(candidates, &mut loss_count, newly_lost);
            }
            PeerMessage::Extended { ref payload, .. } => {
                if let Some(new_head) = advance_head_from_window(payload, continuity.head) {
                    continuity.head = new_head;
                    update_runtime_window(&mut peers, continuity, peer_id, new_head);
                    made_activity = true;
                    let newly_lost =
                        advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                    record_pool_losses(candidates, &mut loss_count, newly_lost);
                }
            }
            m @ PeerMessage::Piece { .. } => {
                if let Some(lc) = LiveChunk::from_message(&m) {
                    continuity.skip_far_behind_live(Instant::now());
                    let piece = lc.piece as u64;
                    if continuity.startup_deadline.is_some()
                        || piece < continuity.reasm.next_needed()
                    {
                        continue;
                    }
                    PieceStore::shared_put_chunk_with_header(
                        store,
                        piece,
                        lc.chunk,
                        lc.piece_header,
                        &lc.data,
                    )
                    .await;
                    let begin = lc.chunk as u64 * info.chunk_length;
                    let contribution = match continuity
                        .reasm
                        .add_block_with_contribution(piece, begin, &lc.data)
                    {
                        Ok(contribution) => contribution,
                        Err(e) => {
                            // A malformed block, or a completed piece whose live-source signature
                            // didn't verify (#10): drop it rather than emit unauthenticated bytes.
                            // The piece stays incomplete, so it's re-requested from the pool.
                            crate::alog!("[ace] {addr}: dropped piece {piece} block: {e:?}");
                            continuity.release_rejected_piece(piece);
                            let newly_lost =
                                advance_pool_requests(&mut peers, continuity, chunks_per_piece)
                                    .await;
                            record_pool_losses(candidates, &mut loss_count, newly_lost);
                            continue;
                        }
                    };
                    if contribution {
                        continuity
                            .piece_producers
                            .entry(piece)
                            .and_modify(|producer| {
                                if *producer != Some(addr) {
                                    *producer = None;
                                }
                            })
                            .or_insert(Some(addr));
                    }
                    // Request receipt/activity accounting is independent of durable byte
                    // provenance; retain its existing behavior for completed-piece no-ops.
                    continuity.note_chunk(piece, lc.chunk, chunks_per_piece);
                    made_activity = true;
                    #[cfg(test)]
                    if let Some(observer) = &continuity.contribution_observer {
                        let _ = observer.send((piece, addr));
                    }
                    let before = continuity.reasm.next_needed();
                    let ready = take_ready_with_stats(continuity, seed.metrics.as_ref(), info);
                    let after = continuity.reasm.next_needed();
                    let productive: Vec<_> = continuity
                        .piece_producers
                        .range(before..after)
                        .filter_map(|(_, producer)| *producer)
                        .collect();
                    continuity
                        .piece_producers
                        .retain(|&piece, _| piece >= after);
                    if !ready.is_empty() {
                        if !continuity.authenticated_logged
                            && info.sig_len > 0
                            && ace_wire::live_auth::signature_len_from_pubkey_der(
                                &info.source_pubkey,
                            ) == Some(info.sig_len)
                        {
                            continuity.authenticated_logged = true;
                            crate::alog!("[ace] discovery stage=authenticated-contiguous");
                        }
                        for output in continuity.resync_output(&ready) {
                            let aligned = output.bytes;
                            if continuity.emitted == 0 {
                                crate::alog!("[ace] discovery stage=first-source-output");
                            }
                            continuity.emitted += aligned.len() as u64;
                            if continuity.emitted >= continuity.next_log {
                                crate::alog!(
                                    "[ace] {addr}: served {} MiB (head={}, next piece needed={})",
                                    continuity.emitted >> 20,
                                    continuity.head,
                                    continuity.reasm.next_needed()
                                );
                                continuity.next_log = continuity.emitted + (4 << 20);
                            }
                            let len = aligned.len() as u64;
                            if tx
                                .send(LiveOutput {
                                    bytes: aligned,
                                    discontinuity: output.discontinuity,
                                })
                                .await
                                .is_err()
                            {
                                shutdown_peer_runtimes(&mut peers);
                                return FollowEnd::ConsumerGone;
                            }
                            downloaded.fetch_add(len, Ordering::Relaxed);
                            made_output = true;
                        }
                    }
                    if made_output
                        && info.sig_len > 0
                        && ace_wire::live_auth::signature_len_from_pubkey_der(&info.source_pubkey)
                            == Some(info.sig_len)
                    {
                        for producer in productive {
                            seed.warm_peers.record_productive(
                                info.infohash,
                                producer,
                                candidates.kind(producer),
                            );
                        }
                    }
                    let newly_lost =
                        advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                    record_pool_losses(candidates, &mut loss_count, newly_lost);
                }
            }
            PeerMessage::Interested => {
                let should_unchoke = if seed.enabled {
                    peers
                        .get_mut(&peer_id)
                        .map(|peer| {
                            let first = !peer.unchoked_peer;
                            peer.unchoked_peer = true;
                            first
                        })
                        .unwrap_or(false)
                } else {
                    false
                };
                if should_unchoke {
                    if let Some(lost) =
                        send_peer_command(peer_id, PeerMessage::Unchoke, &mut peers, continuity)
                            .await
                    {
                        record_pool_losses(candidates, &mut loss_count, [lost]);
                    }
                }
            }
            PeerMessage::Unknown { id: 6, ref payload } if seed.enabled && payload.len() >= 10 => {
                // payload: [stream u32 @0..4][piece u32 @4..8][chunk u16 @8..10]
                let p = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
                let c = u16::from_be_bytes([payload[8], payload[9]]);
                if let Some((data, header)) = PieceStore::shared_chunk(store, p as u64, c).await {
                    let len = data.len();
                    if let Some(lost) = send_peer_command(
                        peer_id,
                        build_piece(0, p, c, header, &data),
                        &mut peers,
                        continuity,
                    )
                    .await
                    {
                        record_pool_losses(candidates, &mut loss_count, [lost]);
                    } else {
                        uploaded.fetch_add(len as u64, Ordering::Relaxed);
                        peers_served.store(peers.len() as u32, Ordering::Relaxed);
                    }
                }
            }
            PeerMessage::Unknown { id: 4, ref payload } if payload.len() == 8 => {
                let piece =
                    u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]) as u64;
                let window_advanced = peers
                    .get(&peer_id)
                    .is_some_and(|peer| piece > peer.max_piece);
                if piece > continuity.head || window_advanced {
                    // Another upstream can advertise the shared head first. Its gossip
                    // must not suppress this producer's independently advancing window.
                    continuity.head = continuity.head.max(piece);
                    update_runtime_window(&mut peers, continuity, peer_id, piece);
                    made_activity = true;
                    let newly_lost =
                        advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                    record_pool_losses(candidates, &mut loss_count, newly_lost);
                }
            }
            PeerMessage::Unknown { id: 10, .. } => {}
            PeerMessage::Unknown {
                id: 12,
                ref payload,
            } => {
                let advertised = ace_wire::peer_exchange::parse_peer_exchange_detailed(payload);
                let ranked = ace_wire::peer_exchange::rank_by_window_coverage(
                    &advertised,
                    continuity.reasm.next_needed(),
                );
                for addr in ranked {
                    candidates.learn(addr, CandidateKind::Pex);
                }
                if peers.len() < live_recovery.max_active_upstreams
                    || continuity.startup_deadline.is_some()
                {
                    spawn_candidate_connects(
                        candidates,
                        &peers,
                        info.infohash,
                        live_recovery.max_parallel_connect,
                        &pex_tx,
                        &mut learned_connects,
                        &mut learned_pending,
                        true,
                    );
                }
            }
            PeerMessage::Unknown {
                id: 36,
                ref payload,
            } => {
                if let Some(source) = ace_wire::peer_exchange::parse_peer_announce(payload) {
                    candidates.learn(source, CandidateKind::Source);
                    crate::alog!("[ace] source-node announce from {addr}: retained {source}");
                    if peers.len() < live_recovery.max_active_upstreams
                        || continuity.startup_deadline.is_some()
                    {
                        spawn_candidate_connects(
                            candidates,
                            &peers,
                            info.infohash,
                            live_recovery.max_parallel_connect,
                            &pex_tx,
                            &mut learned_connects,
                            &mut learned_pending,
                            true,
                        );
                    }
                }
            }
            // Peer telemetry we don't act on: id=11 bencode stats, id=13 keepalive, id=34
            // counter. Explicit no-ops so they don't spam the "unhandled msg" log.
            PeerMessage::Unknown {
                id: 11 | 13 | 34, ..
            } => {}
            PeerMessage::Unknown { id, ref payload } => {
                if let Some(new_head) = advance_head_from_window(payload, continuity.head) {
                    crate::alog!(
                        "[ace] {addr}: live window update (msg id={id}) head {} -> {new_head}",
                        continuity.head
                    );
                    continuity.head = new_head;
                    update_runtime_window(&mut peers, continuity, peer_id, new_head);
                    made_activity = true;
                    let newly_lost =
                        advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
                    record_pool_losses(candidates, &mut loss_count, newly_lost);
                } else {
                    let should_log = peers
                        .get_mut(&peer_id)
                        .map(|peer| peer.seen_ids.insert(id))
                        .unwrap_or(false);
                    if should_log {
                        crate::alog!(
                            "[ace] {addr}: unhandled msg id={id} ({} bytes) {}",
                            payload.len(),
                            hex_preview(payload)
                        );
                    }
                }
            }
            _ => {}
        }
        if peers.is_empty() {
            return FollowEnd::PeerLost(loss_count);
        }
        if should_refresh_stale_deadline(continuity.emitted, made_output, made_activity) {
            last_progress = Instant::now();
        }
        if made_output {
            if let Some(peer) = peers.get_mut(&peer_id) {
                peer.produced_output = true;
            }
            candidates.productive(addr);
            // Contiguous output means the playback cursor advanced: reset the per-piece
            // eviction-skip timer so it only fires when the cursor is genuinely stuck.
            continuity.next_needed_since = Instant::now();
        }
    }
}
