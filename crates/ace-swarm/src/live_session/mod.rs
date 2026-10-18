//! Production multi-upstream live follower;
//! legacy single-peer entrypoints remain in [`crate::live`] and [`crate::driver`].
mod observation;
mod options;

pub use observation::{LiveCounters, LiveObserver, LivePosition};
pub use options::{
    LiveRecoveryOptions, MAX_LIVE_RECOVERY_ACTIVE_UPSTREAMS, MAX_LIVE_RECOVERY_PARALLEL_CONNECT,
    MAX_LIVE_RECOVERY_PIECE_ADVANCE, MAX_LIVE_RECOVERY_REASM_PIECES_AHEAD,
};

/// Contiguous authenticated media with an optional gap immediately before it.
pub struct LiveOutput {
    pub bytes: bytes::Bytes,
    pub discontinuity: bool,
}

/// Engine-configured store construction, including cache backend and fallback policy.
pub type StoreFactory = std::sync::Arc<dyn Fn([u8; 20]) -> crate::store::PieceStore + Send + Sync>;

use crate::discover::{DiscoveryOptions, MAX_DISCOVERY_PEERS};
use crate::listen::SeedRegistry;
use crate::reachability::ReachabilityMonitor;
use crate::scheduler::{ActivePeers, PeerAssignment, Scheduler};
use crate::store::PieceStore;
use crate::types::StreamInfo;
#[cfg(test)]
use crate::types::StreamMetadata;
use ace_peer::session::{connect, PeerSession};
use ace_wire::extended::{
    ExtendedHandshake, LivePosition as WireLivePosition, NodeFields, OutgoingExtendedHandshake,
};
use ace_wire::handshake::random_peer_id;
use ace_wire::identity::Identity;
use ace_wire::live::LiveWindow;
use ace_wire::live_codec::{build_piece, chunk_request, LiveChunk};
use ace_wire::message::PeerMessage;
use ace_wire::reassembly::PieceReassembler;
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::net::{IpAddr, SocketAddrV4};

use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// Legacy single-peer helper handle. The production path now assigns real peer handles
/// starting at 1, but the old helper is kept as a short-term bisect fallback.
const SINGLE_PEER_ID: u64 = 0;
/// One total deadline for gossip-only harvesting of a stale connected batch.
const STALE_GOSSIP_BUDGET: Duration = Duration::from_millis(750);

struct OwnedTask(tokio::task::JoinHandle<()>);
impl Drop for OwnedTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

mod discovery;
pub use discovery::{DiscoveryRun, PeerDiscovery};

#[cfg(test)]
fn completed_discovery<F, Fut>(factory: F) -> PeerDiscovery
where
    F: Fn(DiscoveryOptions) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Vec<SocketAddrV4>> + Send + 'static,
{
    Arc::new(move |options, sender| {
        let future = factory(options);
        Box::pin(async move {
            for peer in future.await {
                if sender.send(peer).await.is_err() {
                    return;
                }
            }
        })
    })
}

fn drain_discovery(run: &mut Option<DiscoveryRun>, candidates: &mut SessionCandidates) {
    let Some(discovery) = run.as_mut() else {
        return;
    };
    loop {
        match discovery.receiver.try_recv() {
            Ok(peer) => candidates.learn(peer, CandidateKind::Discovered),
            Err(mpsc::error::TryRecvError::Empty) => return,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                *run = None;
                return;
            }
        }
    }
}
async fn next_discovery_peer(run: &mut Option<DiscoveryRun>) -> Option<SocketAddrV4> {
    let Some(discovery) = run.as_mut() else {
        return std::future::pending().await;
    };
    let peer = discovery.receiver.recv().await;
    if peer.is_none() {
        *run = None;
    }
    peer
}

/// Briefly collect other near-complete candidates after the first live handshake.
const UPSTREAM_SELECTION_GRACE: Duration = Duration::from_millis(250);
/// Process gossip while withholding media from an uncorroborated first window.
const LIVE_START_CORROBORATION: Duration = Duration::from_secs(1);

const BACKGROUND_DISCOVERY_BUDGET: Duration = Duration::from_secs(8);
const BACKGROUND_DISCOVERY_PEER_TARGET: usize = 64;

/// Storage, recovery and optional observations for one production session.
#[derive(Clone)]
pub struct LiveSeedOptions {
    pub metrics: Option<Arc<dyn LiveObserver>>,
    pub registry: SeedRegistry,
    pub store_factory: StoreFactory,
    pub enabled: bool,
    pub prefetch_pieces: u64,
    pub live_recovery: LiveRecoveryOptions,
    pub warm_peers: WarmPeerCache,
}
type SeedConfig = LiveSeedOptions;
type LiveRecoveryConfig = LiveRecoveryOptions;

mod peer;
#[cfg(test)]
use peer::prefer_window;
use peer::{
    background_discovery_options, connect_pool, connect_upstream, peer_worker,
    pool_refill_candidates,
};
mod continuity;
#[cfg(test)]
use continuity::prefetch_start;
mod pool;
#[cfg(test)]
use pool::follow_peer_pool;
use pool::follow_peer_pool_with_discovery;
#[cfg(test)]
use pool::{activate_upstream_peer, recovery_channel_capacities};
mod pool_support;
#[cfg(test)]
use pool_support::ready_buffer_snapshot;
#[cfg(test)]
use pool_support::spawn_learned_connects;
use pool_support::*;
use pool_support::{
    advance_pool_requests, drop_peer_runtime, finish_candidate_connect, harvest_stale_gossip,
    observe_live_window, publish_live_stats, retransmit_stalled_requests, shutdown_peer_runtimes,
    spawn_candidate_connects, take_ready_with_stats,
};
mod single_peer;
use single_peer::{
    advance_head_from_window, hex_preview, read_peer_window, schedule_piece_assignments,
    should_refresh_stale_deadline, stale_upstream_budget,
};

#[cfg(test)]
use single_peer::schedule_piece_requests;

mod reconnect;
use reconnect::{CandidateKind, SessionCandidates};
mod warm_peers;
pub use warm_peers::WarmPeerCache;
pub(super) struct ConnectedUpstream {
    pub(super) session: PeerSession<TcpStream>,
    pub(super) addr: SocketAddrV4,
    pub(super) window: WireLivePosition,
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
/// Inputs owned by one production follower session.
pub struct LiveSessionInputs {
    pub info: StreamInfo,
    pub peers: Vec<SocketAddrV4>,
    pub identity: Arc<Identity>,
    pub output: mpsc::Sender<LiveOutput>,
    pub counters: LiveCounters,
    pub seed: LiveSeedOptions,
    pub discovery: PeerDiscovery,
    pub reachability: Option<Arc<ReachabilityMonitor>>,
    pub initial_run: Option<DiscoveryRun>,
}

/// Follow authenticated live media, owning discovery, upstream workers and the producer lease.
pub async fn follow_live(session: LiveSessionInputs) {
    let LiveSessionInputs {
        info,
        peers,
        identity,
        output,
        counters,
        seed,
        discovery,
        reachability,
        initial_run,
    } = session;
    follow_live_session_with_run(
        info,
        peers,
        identity,
        output,
        counters.peers,
        counters.downloaded,
        counters.uploaded,
        counters.peers_served,
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
    let store_factory = &seed.store_factory;
    let (store, _seed_lease) = seed
        .registry
        .lease_store(info.infohash, || store_factory(info.infohash));
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

pub(super) struct LiveStatsGuard(pub(super) Option<Arc<dyn LiveObserver>>);
impl Drop for LiveStatsGuard {
    fn drop(&mut self) {
        if let Some(metrics) = &self.0 {
            metrics.clear_active_coverage();
        }
    }
}

#[cfg(test)]
const PREFETCH_PIECES: u64 = 8;
#[cfg(test)]
mod live_start_tests;
#[cfg(test)]
mod reconnect_tests;
#[cfg(test)]
mod resync_tests;
#[cfg(test)]
mod stats_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
fn test_store_factory(bytes: u64, retention: Option<Duration>) -> StoreFactory {
    Arc::new(move |_| {
        let store = PieceStore::new(752, 752, bytes);
        match retention {
            Some(window) => store.with_retention(window),
            None => store,
        }
    })
}
#[cfg(test)]
struct TestObserver {
    received: Arc<AtomicU64>,
    downloaded: Arc<AtomicU64>,
    position: std::sync::Mutex<(Option<LivePosition>, u64)>,
}
#[cfg(test)]
struct TestSnapshot {
    downloaded: u64,
    livepos: Option<LivePosition>,
    buffer_ms: u64,
}
#[cfg(test)]
impl TestObserver {
    fn new(
        downloaded: Arc<AtomicU64>,
        received: Arc<AtomicU64>,
        _: Arc<AtomicU64>,
        _: Arc<AtomicU32>,
        _: Arc<AtomicU32>,
    ) -> Arc<Self> {
        Arc::new(Self {
            received,
            downloaded,
            position: std::sync::Mutex::new((None, 0)),
        })
    }
    fn snapshot(&self) -> TestSnapshot {
        let position = self.position.lock().unwrap();
        TestSnapshot {
            downloaded: self.downloaded.load(Ordering::Relaxed),
            livepos: position.0.clone(),
            buffer_ms: position.1,
        }
    }
}
#[cfg(test)]
impl LiveObserver for TestObserver {
    fn received_bytes(&self, count: u64) {
        self.received.fetch_add(count, Ordering::Relaxed);
    }
    fn publish_position(&self, position: Option<LivePosition>, buffer_ms: u64) {
        *self.position.lock().unwrap() = (position, buffer_ms);
    }
    fn clear_active_coverage(&self) {
        let mut position = self.position.lock().unwrap();
        if let Some(p) = &mut position.0 {
            p.live_first = None;
            p.live_last = None;
            p.buffer_pieces = 0;
        }
        position.1 = 0;
    }
}
