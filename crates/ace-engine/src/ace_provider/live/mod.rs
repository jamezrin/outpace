use super::*;

mod peer;
pub(super) use peer::{
    background_discovery_options, connect_pool, connect_upstream, peer_worker,
    pool_refill_candidates, prefer_window,
};
mod continuity;
pub(super) use continuity::prefetch_start;
mod pool;
#[cfg(test)]
pub(super) use pool::follow_peer_pool;
pub(super) use pool::{
    activate_upstream_peer, follow_peer_pool_with_discovery, recovery_channel_capacities,
};
mod pool_support;
#[cfg(test)]
pub(super) use pool_support::spawn_learned_connects;
use pool_support::*;
pub(super) use pool_support::{
    advance_pool_requests, drop_peer_runtime, finish_candidate_connect, harvest_stale_gossip,
    observe_live_window, publish_live_stats, ready_buffer_snapshot, retransmit_stalled_requests,
    shutdown_peer_runtimes, spawn_candidate_connects, take_ready_with_stats,
};
mod single_peer;
pub(super) use single_peer::{
    advance_head_from_window, hex_preview, read_peer_window, schedule_piece_assignments,
    schedule_piece_requests, should_refresh_stale_deadline, stale_upstream_budget,
};

pub(super) struct LiveOutput {
    pub(super) bytes: Bytes,
    pub(super) discontinuity: bool,
}

pub(super) struct AceSource {
    pub(super) metrics: Option<Arc<crate::live_stats::LiveMetrics>>,
    pub(super) _sampler: Option<crate::live_stats::MetricsSampler>,
    pub(super) rx: mpsc::Receiver<LiveOutput>,
    pub(super) discontinuity: bool,
    pub(super) peers: Arc<AtomicU32>,
    pub(super) downloaded: Arc<AtomicU64>,
    pub(super) uploaded: Arc<AtomicU64>,
    pub(super) peers_served: Arc<AtomicU32>,
    pub(super) metadata: StreamMetadata,
}

#[async_trait]
impl TsSource for AceSource {
    async fn next(&mut self) -> Option<Bytes> {
        let output = self.rx.recv().await?;
        self.discontinuity = output.discontinuity;
        Some(output.bytes)
    }
    fn take_discontinuity(&mut self) -> bool {
        std::mem::take(&mut self.discontinuity)
    }
    fn stats(&self) -> SourceStats {
        if let Some(metrics) = &self.metrics {
            return crate::provider::StatsReader::snapshot(metrics.as_ref());
        }
        SourceStats {
            peers: self.peers.load(Ordering::Relaxed),
            bitrate: 0,
            buffer_ms: 0,
            downloaded: self.downloaded.load(Ordering::Relaxed),
            uploaded: self.uploaded.load(Ordering::Relaxed),
            peers_served: self.peers_served.load(Ordering::Relaxed),
            ..SourceStats::default()
        }
    }
    fn stats_handle(&self) -> Option<crate::provider::StatsHandle> {
        self.metrics
            .as_ref()
            .map(|metrics| metrics.clone() as crate::provider::StatsHandle)
    }
    fn metadata(&self) -> StreamMetadata {
        self.metadata.clone()
    }
}
impl AceProvider {
    // Shared resolved-live entry point; injected discovery keeps protocol regressions offline.
    pub(super) async fn open_resolved(
        &self,
        id: &str,
        info: StreamInfo,
        discovery: PeerDiscovery,
    ) -> Result<Box<dyn TsSource>, ProviderError> {
        let resolved_at = Instant::now();
        // Bootstrap peers are the proven/direct path and must be tried without waiting for
        // tracker/DHT discovery. Background refill can still discover more peers after start.
        let mut initial_run = None;
        let peers = if self.bootstrap_peers.is_empty() {
            let mut run = DiscoveryRun::start(&discovery, DiscoveryOptions::default());
            self.warm_peers.initialized().await;
            let hints = self.warm_peers.hints(&info.infohash);
            let mut peers: Vec<_> = hints.into_iter().map(|(addr, _)| addr).collect();
            if peers.is_empty() {
                peers.extend(run.receiver.recv().await);
            }
            while let Ok(peer) = run.receiver.try_recv() {
                peers.push(peer);
            }
            if !peers.is_empty() {
                initial_run = Some(run);
            }
            peers
        } else {
            self.bootstrap_peers.clone()
        };
        crate::alog!(
            "[ace] discovery stage=first-candidates after_resolution_ms={} count={}",
            resolved_at.elapsed().as_millis(),
            peers.len()
        );
        crate::alog!("[ace] open {id}: discovered {} peer(s)", peers.len());
        if peers.is_empty() {
            return Err(ProviderError::Backend(
                "no peers (no trackers/bootstrap)".into(),
            ));
        }

        let (tx, rx) = mpsc::channel::<LiveOutput>(256);
        let peer_count = Arc::new(AtomicU32::new(0));
        let downloaded = Arc::new(AtomicU64::new(0));
        let uploaded = Arc::new(AtomicU64::new(0));
        let peers_served = Arc::new(AtomicU32::new(0));
        let identity = self.identity.clone();
        let stats_peers = peer_count.clone();
        let stats_downloaded = downloaded.clone();
        let stats_uploaded = uploaded.clone();
        let stats_peers_served = peers_served.clone();
        let metrics = crate::live_stats::LiveMetrics::new(
            stats_downloaded.clone(),
            Arc::new(AtomicU64::new(0)),
            stats_uploaded.clone(),
            stats_peers.clone(),
            stats_peers_served.clone(),
        );
        let sampler = crate::live_stats::spawn_sampler(metrics.clone());
        let prefetch_pieces = self.prefetch_policy_for(&info);
        let startup_buffer = self.startup_buffer;
        let bitrate = info.metadata.bitrate;
        let seed = SeedConfig {
            metrics: Some(metrics.clone()),
            registry: self.seed_registry.clone(),
            store_bytes: self.seed_store_bytes,
            store_retention: self.seed_store_retention,
            enabled: self.enable_seeding,
            prefetch_pieces,
            live_recovery: self.live_recovery,
            cache_type: self.cache_type,
            cache_dir: self.cache_dir.clone(),
            warm_peers: self.warm_peers.clone(),
        };
        let announce_info = info.clone();
        let metadata = info.metadata.clone();
        let announce_port = self.announce_peer_port.clone();
        let reachability = self.reachability.clone();
        tokio::spawn(async move {
            // Run the download loop and the periodic seeder self-announce concurrently;
            // whichever ends first (normally `follow_live`, when the consumer drops) tears
            // down the other — no separate lifecycle to manage.
            tokio::select! {
                _ = follow_live(info, peers, identity, tx, stats_peers, downloaded, uploaded, peers_served, seed, discovery, reachability, initial_run) => {},
                _ = announce_seeder_periodically(announce_info, announce_port) => {},
            }
        });
        let source = AceSource {
            metrics: Some(metrics),
            _sampler: Some(sampler),
            rx,
            discontinuity: false,
            peers: peer_count,
            downloaded: stats_downloaded,
            uploaded: stats_uploaded,
            peers_served: stats_peers_served,
            metadata,
        };
        Ok(StartupBufferedSource::new(
            Box::new(source),
            startup_buffer,
            bitrate,
        ))
    }
}

pub(super) struct ConnectedUpstream {
    pub(super) session: PeerSession<TcpStream>,
    pub(super) addr: SocketAddrV4,
    pub(super) window: LivePosition,
    /// The public IP this peer echoed to us in `yourip`, if any — harvested for reachability
    /// observability (issue #22) when this upstream is activated.
    pub(super) yourip: Option<IpAddr>,
}

pub(super) enum PeerConnectAttempt {
    Connected(ConnectedUpstream),
    Failed(PeerConnectFailure),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PeerConnectFailure {
    pub(super) addr: SocketAddrV4,
    pub(super) stage: PeerConnectStage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PeerConnectStage {
    Tcp,
    Handshake,
    Window,
    TotalTimeout,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct PeerConnectStats {
    pub(super) connected: usize,
    pub(super) tcp: usize,
    pub(super) handshake: usize,
    pub(super) window: usize,
    pub(super) total_timeout: usize,
    pub(super) task: usize,
}

impl PeerConnectStats {
    fn record_connected(&mut self) {
        self.connected += 1;
    }

    pub(super) fn record_failure(&mut self, failure: PeerConnectFailure) {
        let _addr = failure.addr;
        match failure.stage {
            PeerConnectStage::Tcp => self.tcp += 1,
            PeerConnectStage::Handshake => self.handshake += 1,
            PeerConnectStage::Window => self.window += 1,
            PeerConnectStage::TotalTimeout => self.total_timeout += 1,
        }
    }

    fn record_task_failure(&mut self) {
        self.task += 1;
    }

    fn has_observations(&self) -> bool {
        self.connected + self.tcp + self.handshake + self.window + self.total_timeout + self.task
            > 0
    }

    pub(super) fn summary(&self) -> String {
        let attempted = self.connected
            + self.tcp
            + self.handshake
            + self.window
            + self.total_timeout
            + self.task;
        let mut parts = vec![format!("attempted={attempted}")];
        if self.connected > 0 {
            parts.push(format!("connected={}", self.connected));
        }
        if self.tcp > 0 {
            parts.push(format!("tcp={}", self.tcp));
        }
        if self.handshake > 0 {
            parts.push(format!("handshake={}", self.handshake));
        }
        if self.window > 0 {
            parts.push(format!("window={}", self.window));
        }
        if self.total_timeout > 0 {
            parts.push(format!("total_timeout={}", self.total_timeout));
        }
        if self.task > 0 {
            parts.push(format!("task={}", self.task));
        }
        parts.join(" ")
    }
}

#[derive(Debug)]
pub(super) enum PeerCommand {
    RequestPiece { piece: u64, chunks_per_piece: u16 },
    Send(PeerMessage),
    Stop,
}

#[derive(Debug)]
pub(super) enum PeerEvent {
    Message {
        peer_id: u64,
        addr: SocketAddrV4,
        msg: PeerMessage,
    },
    Lost {
        peer_id: u64,
        addr: SocketAddrV4,
    },
}

impl PeerEvent {
    pub(super) fn is_current(&self, peers: &BTreeMap<u64, PeerRuntime>) -> bool {
        let (peer_id, addr) = match self {
            Self::Message { peer_id, addr, .. } | Self::Lost { peer_id, addr } => (peer_id, addr),
        };
        peers.get(peer_id).is_some_and(|peer| peer.addr == *addr)
    }
}

pub(super) struct PeerRuntime {
    pub(super) addr: SocketAddrV4,
    pub(super) min_piece: u64,
    pub(super) max_piece: u64,
    pub(super) unchoked_peer: bool,
    pub(super) produced_output: bool,
    pub(super) seen_ids: HashSet<u8>,
    pub(super) commands: mpsc::Sender<PeerCommand>,
    pub(super) worker: tokio::task::JoinHandle<()>,
}

struct SessionPeerCount(Arc<AtomicU32>);
impl Drop for SessionPeerCount {
    fn drop(&mut self) {
        self.0.store(0, Ordering::Relaxed);
    }
}

impl Drop for PeerRuntime {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

/// Follow the live edge from a peer, pushing contiguous TS. Races a fresh connection on
/// peer loss and refreshes discovery when the current peer set is exhausted; ends when the
/// consumer drops.
#[allow(clippy::too_many_arguments)]
async fn follow_live(
    info: StreamInfo,
    peers: Vec<SocketAddrV4>,
    identity: Arc<Identity>,
    tx: mpsc::Sender<LiveOutput>,
    peer_count: Arc<AtomicU32>,
    downloaded: Arc<AtomicU64>,
    uploaded: Arc<AtomicU64>,
    peers_served: Arc<AtomicU32>,
    seed: SeedConfig,
    discovery: PeerDiscovery,
    reachability: Option<Arc<ReachabilityMonitor>>,
    initial_run: Option<DiscoveryRun>,
) {
    follow_live_session_with_run(
        info,
        peers,
        identity,
        tx,
        peer_count,
        downloaded,
        uploaded,
        peers_served,
        seed,
        discovery,
        reachability,
        initial_run,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(super) async fn follow_live_session(
    info: StreamInfo,
    peers: Vec<SocketAddrV4>,
    identity: Arc<Identity>,
    tx: mpsc::Sender<LiveOutput>,
    peer_count: Arc<AtomicU32>,
    downloaded: Arc<AtomicU64>,
    uploaded: Arc<AtomicU64>,
    peers_served: Arc<AtomicU32>,
    seed: SeedConfig,
    discovery: PeerDiscovery,
    reachability: Option<Arc<ReachabilityMonitor>>,
) {
    follow_live_session_with_run(
        info,
        peers,
        identity,
        tx,
        peer_count,
        downloaded,
        uploaded,
        peers_served,
        seed,
        discovery,
        reachability,
        None,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn follow_live_session_with_run(
    info: StreamInfo,
    peers: Vec<SocketAddrV4>,
    identity: Arc<Identity>,
    tx: mpsc::Sender<LiveOutput>,
    peer_count: Arc<AtomicU32>,
    downloaded: Arc<AtomicU64>,
    uploaded: Arc<AtomicU64>,
    peers_served: Arc<AtomicU32>,
    seed: SeedConfig,
    discovery: PeerDiscovery,
    reachability: Option<Arc<ReachabilityMonitor>>,
    mut rediscovery: Option<DiscoveryRun>,
) {
    let _peer_count_guard = SessionPeerCount(peer_count.clone());
    let _warm_flush = WarmFlush(seed.warm_peers.clone());
    let chunks_per_piece = info.chunks_per_piece();
    let mut candidates = SessionCandidates::default();
    for addr in peers {
        candidates.learn(addr, CandidateKind::Discovered);
    }
    for (addr, kind) in seed.warm_peers.hints(&info.infohash) {
        candidates.learn(addr, kind);
    }
    let mut continuity: Option<Continuity> = None;
    // One owned discovery attempt can finish while learned candidates are retried.
    // Acquire the leech producer lease once for the whole session. `store` is created here and
    // reused across every reconnect (preserving buffered pieces, exactly like the old idempotent
    // `get_or_create`); `_seed_lease` lives for the entire `follow_live` body and drops on return
    // (including any early return), evicting the registry entry once no producer holds it.
    let (store, _seed_lease) = seed.registry.lease_store(info.infohash, || {
        build_piece_store(
            info.piece_length,
            info.chunk_length,
            seed.store_bytes,
            seed.store_retention,
            seed.cache_type,
            &seed.cache_dir,
            &info.infohash,
        )
    });
    loop {
        if tx.is_closed() {
            return;
        }
        drain_discovery(&mut rediscovery, &mut candidates);
        let ready = candidates.eligible(Instant::now());
        let (sources, pex) = candidates.learned_counts();
        crate::alog!(
            "[ace] reconnect candidates: eligible={} learned_source={sources} learned_pex={pex}",
            ready.len()
        );
        let mut upstreams = tokio::select! {
            _ = tx.closed() => return,
            connected = connect_pool(info.infohash,&mut candidates,seed.live_recovery,&mut rediscovery) => connected,
        };
        if let Some(c) = &continuity {
            let mut usable = Vec::new();
            let mut stale = Vec::new();
            for upstream in upstreams {
                if c.window_can_resume(&upstream.window) {
                    usable.push(upstream);
                } else {
                    stale.push(upstream);
                }
            }
            if !stale.is_empty() {
                for upstream in &stale {
                    crate::alog!("[ace] {}: stale advertised window min={} max={} cannot cover next needed piece {}; harvesting gossip only",upstream.addr,upstream.window.min_piece,upstream.window.max_piece,c.reasm.next_needed());
                    candidates.explored(upstream.addr);
                    candidates.failed(upstream.addr, Instant::now());
                }
                tokio::select! {
                    _ = tx.closed() => return,
                    _ = harvest_stale_gossip(stale,&identity,&mut candidates) => {},
                }
            }
            upstreams = usable;
        }
        if upstreams.is_empty() {
            peer_count.store(0, Ordering::Relaxed);
            // Gossip may just have supplied an immediately eligible source. Try it before
            // rediscovery or waiting, rather than dropping it with the stale relay.
            let newly_ready = candidates.eligible(Instant::now());
            if newly_ready.iter().any(|a| !ready.contains(a)) {
                continue;
            }
            let delay = candidates
                .retry_delay(Instant::now())
                .max(Duration::from_millis(50));
            if ready.is_empty() && !candidates.all().is_empty() {
                // Keep cooling histories; a genuinely new batch can wake the retry earlier.
                tokio::select! {
                    _=tx.closed()=>return,
                    _=tokio::time::sleep(delay)=>{},
                    peer=next_discovery_peer(&mut rediscovery), if rediscovery.is_some()=> {
                        if let Some(peer)=peer {candidates.learn(peer,CandidateKind::Discovered);}
                    }
                }
                continue;
            }
            if rediscovery.is_none() {
                let options = if candidates.learned_counts() == (0, 0) {
                    background_discovery_options()
                } else {
                    DiscoveryOptions::default()
                };
                crate::alog!(
                    "[ace] failed-round rediscovery: target={} dht_budget={:?}",
                    options.peer_target,
                    options.dht_budget
                );
                rediscovery = Some(DiscoveryRun::start(&discovery, options));
            }
            tokio::select! {
                _=tx.closed()=>return,
                _=tokio::time::sleep(delay)=>{},
                peer=next_discovery_peer(&mut rediscovery)=> {
                    if let Some(peer)=peer {candidates.learn(peer,CandidateKind::Discovered);}
                }
            }
            continue;
        }
        let windows = upstreams
            .iter()
            .map(|u| format!("{}:{}..{}", u.addr, u.window.min_piece, u.window.max_piece))
            .collect::<Vec<_>>()
            .join(", ");
        let active_addrs: HashSet<SocketAddrV4> = upstreams.iter().map(|u| u.addr).collect();
        let refill_cohort =
            candidates.exploration_cohort(Instant::now(), &active_addrs, &HashSet::new());
        let refill_candidates = pool_refill_candidates(
            &candidates
                .eligible_discovered(Instant::now())
                .into_iter()
                .filter(|addr| Some(candidates.explorations(*addr)) == refill_cohort)
                .collect::<Vec<_>>(),
            &active_addrs,
        );
        let known_refill_peers = candidates.all();
        crate::alog!(
            "[ace] connected + handshaked upstream pool ({} peer(s)): {windows}",
            upstreams.len()
        );
        peer_count.store(upstreams.len() as u32, Ordering::Relaxed);
        let end = tokio::select! {
            _ = tx.closed() => return,
            end = follow_peer_pool_with_discovery(
            upstreams,
            &info,
            &identity,
            chunks_per_piece,
            &tx,
            &downloaded,
            &uploaded,
            &peers_served,
            &seed,
            &store,
            &mut continuity,
            refill_candidates,
            known_refill_peers,
            discovery.clone(),
            &mut candidates,
            &peer_count,
            reachability.as_ref(),
            &mut rediscovery,
        )
        => end,
        };
        match end {
            FollowEnd::ConsumerGone => return,
            FollowEnd::PeerLost(lost) => {
                peer_count.store(0, Ordering::Relaxed);
                crate::alog!("[ace] rebuilding upstream pool: reason=PeerLost lost={lost} learned_source={} learned_pex={}",candidates.learned_counts().0,candidates.learned_counts().1);
            }
            FollowEnd::PoolStale { stalled, lost } => {
                peer_count.store(0, Ordering::Relaxed);
                for addr in stalled {
                    candidates.failed(addr, Instant::now());
                }
                crate::alog!("[ace] rebuilding upstream pool: reason=PoolStale lost={lost} learned_source={} learned_pex={}",candidates.learned_counts().0,candidates.learned_counts().1);
            }
        }
    }
}

pub(super) enum FollowEnd {
    ConsumerGone,
    PeerLost(usize),
    /// Connected peers stopped producing contiguous output past the stale timeout.
    /// Both these peers and genuine drops retain session candidates with bounded cooldowns;
    /// reconnect still uses Continuity::resume/skip_to to recover an evicted gap.
    PoolStale {
        stalled: Vec<SocketAddrV4>,
        lost: usize,
    },
}

/// Piece-continuity state that must survive a peer reconnect within one `follow_live`
/// session. Recreating it fresh per connection (the pre-fix behavior) either re-emitted
/// pieces already served into the live broadcast — a duplicate splice — or left the
/// reassembler waiting forever for a piece the new peer's window had already evicted, since
/// `PieceReassembler` only ever emits strictly contiguously from its cursor. Real swarm
/// connections drop and reconnect routinely, so this was a guaranteed visible stutter on
/// every hop, not an edge case. See `docs/protocol/notes/23-reconnect-continuity.md`.
struct WarmFlush(WarmPeerCache);
impl Drop for WarmFlush {
    fn drop(&mut self) {
        self.0.request_flush();
    }
}

pub(super) struct Continuity {
    pub(super) live_recovery: LiveRecoveryConfig,
    pub(super) reasm: PieceReassembler,
    pub(super) resync: ace_media::mpegts::TsResync,
    output_gate: ace_media::mpegts::KeyframeGate,
    pub(super) transport_recovery: Option<TransportRecovery>,
    discontinuity_pending: bool,
    pub(super) scheduler: Scheduler,
    pub(super) active_peers: ActivePeers,
    pub(super) received_chunks: BTreeMap<u64, HashSet<u16>>,
    /// None marks mixed producers; entries share the bounded reassembly accept window.
    pub(super) piece_producers: BTreeMap<u64, Option<SocketAddrV4>>,
    #[cfg(test)]
    pub(super) contribution_observer: Option<mpsc::UnboundedSender<(u64, SocketAddrV4)>>,
    /// When each still-outstanding piece was (re-)requested — drives per-piece retransmission
    /// independent of the whole-pool stale timer.
    pub(super) requested_at: HashMap<u64, Instant>,
    /// When the playback cursor (`reasm.next_needed()`) last advanced. If it stays put past the
    /// configured request timeout and no upstream window still covers it, the piece was evicted
    /// and we skip forward rather than freeze.
    pub(super) next_needed_since: Instant,
    pub(super) head: u64,
    /// Only a fresh lone-window pool delays media; reconnect continuity stays authoritative.
    startup_deadline: Option<Instant>,
    /// Workers discard queued stale chunk writes after new live-head evidence.
    pub(super) request_floor: Arc<AtomicU64>,
    /// Pieces behind the live edge to leave as a cushion when re-syncing forward to live.
    pub(super) prefetch: u64,
    pub(super) emitted: u64,
    next_log: u64,
    authenticated_logged: bool,
}

pub(super) struct TransportRecovery {
    started: Instant,
    pub(super) discarded_bytes: usize,
    pub(super) withheld_bytes: usize,
    loss_events: usize,
}

// Each typed receipt owns its endpoint reservation, including queued discovered transports.
pub(super) enum PoolRefill {
    ReservedDiscovered(ConnectedUpstream),
    Learned(ConnectedUpstream),
}

type CandidateConnectCompletion = (SocketAddrV4, Option<PeerConnectFailure>, bool);

enum PoolWake {
    ConsumerGone,
    Peer(Option<PeerEvent>),
    Refill(Option<PoolRefill>),
    Discovery(Option<SocketAddrV4>),
    ConnectCompletion(Option<Result<CandidateConnectCompletion, tokio::task::JoinError>>),
    CandidateRetry,
}

pub(super) struct LiveStatsGuard(pub(super) Option<Arc<crate::live_stats::LiveMetrics>>);
impl Drop for LiveStatsGuard {
    fn drop(&mut self) {
        if let Some(metrics) = &self.0 {
            let mut position = crate::provider::StatsReader::snapshot(metrics.as_ref()).livepos;
            if let Some(position) = &mut position {
                position.live_first = None;
                position.live_last = None;
                position.buffer_pieces = 0;
            }
            metrics.publish_position(position, 0);
        }
    }
}
