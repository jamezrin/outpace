mod activation;
mod actor;
use super::*;
use crate::seed::protocol::{prepare_reply, ServeProfile};
use actor::PoolActor;

// Task-local scope keeps parallel actors and activation attempts isolated. These
// witnesses only observe polling; they never suspend or replace a transport.
#[cfg(test)]
pub(super) struct ActivationProbe {
    pub candidate: std::cell::RefCell<Option<ConnectedUpstream>>,
    pub addr: SocketAddrV4,
    pub pending: Arc<tokio::sync::Notify>,
}
#[cfg(test)]
tokio::task_local! {
    pub(super) static ACTIVATION_PROBE: ActivationProbe;
}

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

pub(super) struct ActivationInputs<'a> {
    pub(super) peer_id: u64,
    pub(super) upstream: ConnectedUpstream,
    pub(super) start: u64,
    pub(super) identity: &'a Identity,
    pub(super) continuity: &'a mut Continuity,
    pub(super) event_tx: &'a mpsc::Sender<PeerEvent>,
    pub(super) reachability: Option<&'a Arc<ReachabilityMonitor>>,
    pub(super) replacing: Option<(&'a mut BTreeMap<u64, PeerRuntime>, u64)>,
}

pub(super) async fn activate_upstream_peer(
    inputs: ActivationInputs<'_>,
) -> Result<(PeerRuntime, Option<SocketAddrV4>), SocketAddrV4> {
    let ActivationInputs {
        peer_id,
        upstream,
        start,
        identity,
        continuity,
        event_tx,
        reachability,
        replacing,
    } = inputs;
    let upstream = activation::prepare(
        peer_id,
        upstream,
        start,
        continuity.head,
        identity,
        reachability,
    )
    .await?;
    Ok(commit_upstream_peer(ActivationInputs {
        peer_id,
        upstream,
        start,
        identity,
        continuity,
        event_tx,
        reachability,
        replacing,
    }))
}

fn commit_upstream_peer(inputs: ActivationInputs<'_>) -> (PeerRuntime, Option<SocketAddrV4>) {
    let ActivationInputs {
        peer_id,
        upstream,
        continuity,
        event_tx,
        replacing,
        ..
    } = inputs;
    let peer_min = upstream.window.min_piece.max(0) as u64;
    let peer_max = upstream.window.max_piece.max(0) as u64;
    // Only successful writes consume the old provisional transport. A failed candidate
    // leaves its runtime, gossip reader, and request slots intact.
    let replaced = replacing.and_then(|(peers, id)| drop_peer_runtime(id, peers, continuity));
    continuity.register_active_peer(peer_id, upstream.addr, upstream.window);
    continuity.set_peer_unchoked(peer_id, upstream.unchoked);
    if continuity.reconnect_pool {
        continuity.recovery_event(RecoveryEvent::UpstreamReconnect);
    }
    continuity.has_admitted_pool = true;
    continuity.admitted_upstreams += 1;
    continuity.publish_upstreams();
    let (command_tx, command_rx) = mpsc::channel(64);
    let worker = tokio::spawn(peer_worker(
        peer_id,
        upstream.addr,
        upstream.session,
        command_rx,
        event_tx.clone(),
        continuity.request_floor.clone(),
    ));
    (
        PeerRuntime {
            addr: upstream.addr,
            min_piece: peer_min,
            max_piece: peer_max,
            // Tracks our outgoing reciprocal Unchoke, independently of the peer's incoming state.
            unchoked_peer: false,
            produced_output: false,
            seen_ids: HashSet::new(),
            commands: command_tx,
            worker,
        },
        replaced,
    )
}

pub(super) fn recovery_channel_capacities(
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
pub(super) async fn follow_peer_pool(
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
        PoolContext {
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
            discovery_run: &mut None,
        },
    )
    .await
}

pub(super) struct PoolContext<'a> {
    pub(super) info: &'a StreamInfo,
    pub(super) identity: &'a Identity,
    pub(super) chunks_per_piece: u16,
    pub(super) tx: &'a mpsc::Sender<LiveOutput>,
    pub(super) downloaded: &'a Arc<AtomicU64>,
    pub(super) uploaded: &'a Arc<AtomicU64>,
    pub(super) peers_served: &'a Arc<AtomicU32>,
    pub(super) seed: &'a SeedConfig,
    pub(super) store: &'a Arc<tokio::sync::Mutex<PieceStore>>,
    pub(super) continuity: &'a mut Option<Continuity>,
    pub(super) refill_candidates: Vec<SocketAddrV4>,
    pub(super) known_refill_peers: Vec<SocketAddrV4>,
    pub(super) discovery: PeerDiscovery,
    pub(super) candidates: &'a mut SessionCandidates,
    pub(super) peer_count: &'a Arc<AtomicU32>,
    pub(super) reachability: Option<&'a Arc<ReachabilityMonitor>>,
    pub(super) discovery_run: &'a mut Option<DiscoveryRun>,
}

pub(super) async fn follow_peer_pool_with_discovery(
    upstreams: Vec<ConnectedUpstream>,
    context: PoolContext<'_>,
) -> FollowEnd {
    let PoolContext {
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
        discovery_run,
    } = context;
    let _stats_guard = LiveStatsGuard(seed.metrics.clone());
    // Custom/internal entries must obey the same existing live geometry limits.
    if info.piece_length == 0
        || info.piece_length > crate::resolve::MAX_PIECE_LENGTH
        || info.chunk_length == 0
        || !info.piece_length.is_multiple_of(info.chunk_length)
        || info.piece_length / info.chunk_length > u16::MAX as u64
        || usize::try_from(info.piece_length).is_err()
        || u32::try_from(info.chunk_length).is_err()
        || u64::from(chunks_per_piece) != info.piece_length / info.chunk_length
    {
        crate::alog!("[ace] invalid live pool piece geometry");
        return FollowEnd::ConsumerGone;
    }
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
            let mut c = c;
            c.observer = seed.metrics.clone();
            *continuity = Some(c);
            crate::alog!(
                "[ace] {primary_addr}: window min={min_piece} max={max_piece} -> start={start} head={max_piece}"
            );
            start
        }
        Some(c) => {
            c.observer = seed.metrics.clone();
            c.reconnect_pool = c.has_admitted_pool;
            c.admitted_upstreams = 0;
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
    let (event_tx, event_rx) = mpsc::channel(event_capacity);
    let (refill_tx, refill_rx) = mpsc::channel(refill_capacity);
    // A live-held clone of the refill sender so peers learned from `id=12` peer-exchange
    // gossip can be connected and fed into the same pool-add path (keeps `refill_rx` open
    // even after the background refill task finishes).
    let pex_tx = refill_tx.clone();
    let learned_connects: tokio::task::JoinSet<(SocketAddrV4, Option<PeerConnectFailure>, bool)> =
        tokio::task::JoinSet::new();
    let learned_pending = HashSet::new();
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
        match activate_upstream_peer(ActivationInputs {
            peer_id,
            upstream,
            start,
            identity,
            continuity,
            event_tx: &event_tx,
            reachability,
            replacing: None,
        })
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
    if !peers.is_empty() {
        // Retained early Unchokes need the same scheduling trigger as active peer events.
        let newly_lost = advance_pool_requests(&mut peers, continuity, chunks_per_piece).await;
        record_pool_losses(candidates, &mut loss_count, newly_lost);
    }
    if peers.is_empty() {
        return FollowEnd::PeerLost(loss_count);
    }
    peer_count.store(peers.len() as u32, Ordering::Relaxed);

    // One opportunity per pool after activation/scheduling and corroboration release.
    let first_output_deadline = None;
    let last_progress = Instant::now();
    let refill_closed = false;
    let observed_capacity = (peers.len(), learned_pending.len());
    let observed_retry = Instant::now();
    let closed_peer_events = BTreeMap::new();
    PoolActor {
        activation: None,
        reciprocal: Default::default(),
        publication: None,
        gossip: None,
        closed_peer_events,
        observed_retry,
        observed_capacity,
        refill_closed,
        last_progress,
        first_output_deadline,
        next_peer_id,
        loss_count,
        peers,
        learned_pending,
        learned_connects,
        pex_tx,
        refill_rx,
        event_rx,
        event_tx,
        _stats_guard,
        live_recovery,
        info,
        identity,
        chunks_per_piece,
        tx,
        downloaded,
        uploaded,
        peers_served,
        seed,
        store,
        candidates,
        peer_count,
        reachability,
        discovery_run,
        continuity,
    }
    .run()
    .await
}
