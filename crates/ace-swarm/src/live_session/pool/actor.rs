use super::*;
mod publication;
use publication::PublicationOperation;

type ConnectCompletion =
    Option<Result<(SocketAddrV4, Option<PeerConnectFailure>, bool), tokio::task::JoinError>>;

enum PieceOutcome {
    Skip,
    End(FollowEnd),
    Progress { activity: bool, output: bool },
}

enum WakeOutcome {
    Peer(PeerEvent),
    Continue,
    End(FollowEnd),
}

// Fields owning resources follow the original locals' reverse declaration order.
// In particular peers retire before connect tasks, channel endpoints, then stats.
pub(super) struct PoolActor<'a> {
    pub(super) publication: Option<PublicationOperation>,
    pub(super) closed_peer_events: BTreeMap<u64, (SocketAddrV4, usize)>,
    pub(super) observed_retry: Instant,
    pub(super) observed_capacity: (usize, usize),
    pub(super) refill_closed: bool,
    pub(super) last_progress: Instant,
    pub(super) first_output_deadline: Option<Instant>,
    pub(super) next_peer_id: u64,
    pub(super) loss_count: usize,
    pub(super) peers: BTreeMap<u64, PeerRuntime>,
    // Drops after peer workers and before connection tasks and channel endpoints.
    pub(super) gossip: Option<pool_support::GossipJob>,
    pub(super) learned_pending: HashSet<SocketAddrV4>,
    pub(super) learned_connects:
        tokio::task::JoinSet<(SocketAddrV4, Option<PeerConnectFailure>, bool)>,
    pub(super) pex_tx: mpsc::Sender<PoolRefill>,
    pub(super) refill_rx: mpsc::Receiver<PoolRefill>,
    pub(super) event_rx: mpsc::Receiver<PeerEvent>,
    pub(super) event_tx: mpsc::Sender<PeerEvent>,
    pub(super) _stats_guard: LiveStatsGuard,
    pub(super) live_recovery: LiveRecoveryConfig,
    pub(super) info: &'a StreamInfo,
    pub(super) identity: &'a Identity,
    pub(super) chunks_per_piece: u16,
    pub(super) tx: &'a mpsc::Sender<LiveOutput>,
    pub(super) downloaded: &'a Arc<AtomicU64>,
    pub(super) uploaded: &'a Arc<AtomicU64>,
    pub(super) peers_served: &'a Arc<AtomicU32>,
    pub(super) seed: &'a SeedConfig,
    pub(super) store: &'a Arc<tokio::sync::Mutex<PieceStore>>,
    pub(super) candidates: &'a mut SessionCandidates,
    pub(super) peer_count: &'a Arc<AtomicU32>,
    pub(super) reachability: Option<&'a Arc<ReachabilityMonitor>>,
    pub(super) discovery_run: &'a mut Option<DiscoveryRun>,
    pub(super) continuity: &'a mut Continuity,
}

impl PoolActor<'_> {
    pub(super) async fn run(mut self) -> FollowEnd {
        self.maybe_refill(false);
        // Start progress/retry clocks after initial refill, as in the adapter before extraction.
        self.last_progress = Instant::now();
        self.observed_capacity = (self.peers.len(), self.learned_pending.len());
        self.observed_retry = Instant::now();
        loop {
            if self.tx.is_closed() {
                return FollowEnd::ConsumerGone;
            }
            self.start_publication();
            let ready =
                std::future::poll_fn(|cx| std::task::Poll::Ready(self.poll_publication(cx))).await;
            if ready {
                self.start_publication();
                if !self.continuity.publication_hold() {
                    if let PieceOutcome::End(end) = self.drain_output().await {
                        return end;
                    }
                }
            }
            let mut released_failed_slot = false;
            while let Some(Ok(result)) = self.learned_connects.try_join_next() {
                released_failed_slot |=
                    finish_candidate_connect(result, &mut self.learned_pending, self.candidates);
            }
            let capacity_released = self.peers.len() < self.observed_capacity.0
                || self.learned_pending.len() < self.observed_capacity.1;
            if released_failed_slot || capacity_released {
                self.maybe_refill(false);
            }
            self.observed_capacity = (self.peers.len(), self.learned_pending.len());
            let now = Instant::now();
            publish_live_stats(self.continuity, self.seed.metrics.as_ref(), self.info);
            if self
                .continuity
                .startup_deadline
                .is_some_and(|deadline| now >= deadline)
            {
                self.continuity.startup_deadline = None;
                self.last_progress = now;
                crate::alog!("[ace] live start: corroboration deadline reached; using best known window without freshness proof");
                let lost =
                    advance_pool_requests(&mut self.peers, self.continuity, self.chunks_per_piece)
                        .await;
                record_pool_losses(self.candidates, &mut self.loss_count, lost);
            }
            // Re-evaluate the budget after startup scheduling.
            let now = Instant::now();
            let Some(stale_budget) = pool_progress_budget(
                &mut self.first_output_deadline,
                self.continuity.emitted,
                self.continuity.startup_deadline,
                self.last_progress,
                now,
                self.live_recovery.stale_upstream_timeout(),
            ) else {
                let stalled = self.peers.values().map(|p| p.addr).collect::<Vec<_>>();
                crate::alog!(
                "[ace] upstream pool stale — no live progress for {:?}; reconnecting {} peer(s)",
                self.live_recovery.stale_upstream_timeout(),
                stalled.len()
            );
                shutdown_peer_runtimes(&mut self.peers);
                self.continuity.recovery_event(RecoveryEvent::StaleTeardown);
                return FollowEnd::PoolStale {
                    stalled,
                    lost: self.loss_count,
                };
            };
            // Self-heal a single stuck piece well before the whole-pool stale timeout: re-request
            // pieces outstanding past the request timeout (to a faster peer where possible) and
            // skip a piece evicted from every upstream window.
            let newly_lost = retransmit_stalled_requests(
                &mut self.peers,
                self.continuity,
                self.chunks_per_piece,
                now,
            )
            .await;
            record_pool_losses(self.candidates, &mut self.loss_count, newly_lost);
            // Workers can drain command slots without an incoming event or a
            // retransmit change. Reconsider deferred requests on every timed turn.
            let newly_lost =
                advance_pool_requests(&mut self.peers, self.continuity, self.chunks_per_piece)
                    .await;
            record_pool_losses(self.candidates, &mut self.loss_count, newly_lost);
            if self.peers.is_empty() {
                return FollowEnd::PeerLost(self.loss_count);
            }
            self.peer_count
                .store(self.peers.len() as u32, Ordering::Relaxed);

            // Wake at least every configured request-check interval for credit
            // admission and retransmit checks, even while no peer sends anything.
            let wait = stale_budget
                .min(self.live_recovery.request_check_interval())
                .min(
                    self.continuity
                        .startup_deadline
                        .map(|deadline| deadline.saturating_duration_since(now))
                        .unwrap_or(stale_budget),
                );
            let active = self.peers.values().map(|peer| peer.addr).collect();
            let retry_deadline = if (self.peers.len() < self.live_recovery.max_active_upstreams
                || self.continuity.startup_deadline.is_some())
                && self.learned_pending.len() + usize::from(self.gossip.is_some())
                    < self.live_recovery.max_parallel_connect
                && self.learned_connects.len() < self.live_recovery.max_parallel_connect
            {
                self.candidates.retry_deadline_after(
                    self.observed_retry,
                    &active,
                    &self.learned_pending,
                )
            } else {
                None
            };
            if self.tx.is_closed() {
                return FollowEnd::ConsumerGone;
            }
            let pending_event = poll_closed_peer_event(
                &self.peers,
                &mut self.event_rx,
                &mut self.closed_peer_events,
            );
            let event_was_polled = pending_event.is_some();
            let event = match pending_event {
            Some(event) => PoolWake::Peer(Some(event)),
            None if ready => PoolWake::Publication,
            None => match tokio::time::timeout(wait, async {
                tokio::select! {
                    _ = self.tx.closed() => PoolWake::ConsumerGone,
                    _ = std::future::poll_fn(|cx| publication::poll_operation(&mut self.publication, self.continuity, self.store, cx).then_some(()).map_or(std::task::Poll::Pending, std::task::Poll::Ready)), if self.publication.is_some() => PoolWake::Publication,
                    learned = async { self.gossip.as_mut().expect("enabled gossip").await }, if self.gossip.is_some() => PoolWake::Gossip(learned),
                    event = self.event_rx.recv() => PoolWake::Peer(event),
                    result=self.learned_connects.join_next(), if !self.learned_connects.is_empty()=>PoolWake::ConnectCompletion(result),
                    _ = async {
                        tokio::time::sleep_until(retry_deadline.expect("enabled retry deadline").into()).await;
                    }, if retry_deadline.is_some() => PoolWake::CandidateRetry,
                    peer=next_discovery_peer(self.discovery_run), if self.discovery_run.is_some()=>PoolWake::Discovery(peer),
                    upstream = self.refill_rx.recv(), if self.gossip.is_none() && !self.refill_closed && (self.peers.len() < self.live_recovery.max_active_upstreams || self.continuity.startup_deadline.is_some()) => {
                        PoolWake::Refill(upstream)
                    }
                }
            })
            .await
            {
                Ok(event) => event,
                Err(_) => continue, // sweep tick: re-check stale budget and retransmit
            },
        };
            if !event_was_polled && matches!(&event, PoolWake::Peer(Some(_))) {
                for (_, remaining) in self.closed_peer_events.values_mut() {
                    *remaining = remaining.saturating_sub(1);
                }
            }

            let event = match self.on_wake(event).await {
                WakeOutcome::Peer(event) => event,
                WakeOutcome::Continue => continue,
                WakeOutcome::End(end) => return end,
            };

            if let Some(end) = self.on_peer_event(event).await {
                return end;
            }
        }
    }

    // Retry wakes and rejected/stale refill receipts deliberately bypass active capacity.
    fn maybe_refill(&mut self, exceptional: bool) {
        if exceptional
            || self.peers.len() < self.live_recovery.max_active_upstreams
            || self.continuity.startup_deadline.is_some()
        {
            spawn_candidate_connects(CandidateConnectInputs {
                candidates: self.candidates,
                peers: &self.peers,
                infohash: self.info.infohash,
                // The held gossip socket still consumes a transport slot.
                max_parallel: self
                    .live_recovery
                    .max_parallel_connect
                    .saturating_sub(usize::from(self.gossip.is_some())),
                tx: &self.pex_tx,
                tasks: &mut self.learned_connects,
                pending: &mut self.learned_pending,
                include_discovered: true,
            });
        }
    }

    async fn on_wake(&mut self, event: PoolWake) -> WakeOutcome {
        match event {
            PoolWake::Gossip(learned) => {
                self.gossip = None;
                for (addr, kind) in learned.into_learned() {
                    self.candidates.learn(addr, kind);
                }
                self.maybe_refill(true);
                WakeOutcome::Continue
            }
            PoolWake::Publication => {
                self.start_publication();
                if !self.continuity.publication_hold() {
                    if let PieceOutcome::End(end) = self.drain_output().await {
                        return WakeOutcome::End(end);
                    }
                }
                WakeOutcome::Continue
            }
            PoolWake::ConsumerGone => WakeOutcome::End(FollowEnd::ConsumerGone),
            PoolWake::CandidateRetry => self.on_candidate_retry(),
            PoolWake::ConnectCompletion(result) => self.on_connect_completion(result),
            PoolWake::Discovery(peer) => self.on_discovery(peer),
            PoolWake::Peer(Some(event)) if event.is_current(&self.peers) => {
                WakeOutcome::Peer(event)
            }
            PoolWake::Peer(Some(_)) => WakeOutcome::Continue,
            PoolWake::Peer(None) => {
                shutdown_peer_runtimes(&mut self.peers);
                WakeOutcome::End(FollowEnd::PeerLost(self.loss_count))
            }
            PoolWake::Refill(Some(upstream)) => self.on_refill(upstream).await,
            PoolWake::Refill(None) => {
                self.refill_closed = true;
                WakeOutcome::Continue
            }
        }
    }

    fn on_candidate_retry(&mut self) -> WakeOutcome {
        self.observed_retry = Instant::now();
        self.maybe_refill(true);
        WakeOutcome::Continue
    }

    fn on_connect_completion(&mut self, result: ConnectCompletion) -> WakeOutcome {
        if let Some(Ok(result)) = result {
            if finish_candidate_connect(result, &mut self.learned_pending, self.candidates) {
                self.maybe_refill(false);
            }
        }
        WakeOutcome::Continue
    }

    fn on_discovery(&mut self, peer: Option<SocketAddrV4>) -> WakeOutcome {
        if let Some(peer) = peer {
            self.candidates.learn(peer, CandidateKind::Discovered);
        }
        self.maybe_refill(false);
        WakeOutcome::Continue
    }

    async fn on_refill(&mut self, upstream: PoolRefill) -> WakeOutcome {
        let Some(upstream) =
            receive_pool_refill(upstream, &mut self.learned_pending, self.candidates)
        else {
            self.maybe_refill(true);
            return WakeOutcome::Continue;
        };
        self.candidates
            .learn(upstream.addr, CandidateKind::Discovered);
        if self.peers.values().any(|p| p.addr == upstream.addr) {
            return WakeOutcome::Continue;
        }
        if !self.continuity.window_can_resume(&upstream.window) {
            crate::alog!(
                        "[ace] {}: background refill stale window min={} max={} cannot cover next needed piece {}; dropping",
                        upstream.addr,
                        upstream.window.min_piece,
                        upstream.window.max_piece,
                        self.continuity.reasm.next_needed()
                    );
            self.candidates.explored(upstream.addr);
            self.candidates.failed(upstream.addr, Instant::now());
            let gossip_budget = self
                .continuity
                .startup_deadline
                .map(|deadline| deadline.saturating_duration_since(Instant::now()))
                .unwrap_or(STALE_GOSSIP_BUDGET)
                .min(STALE_GOSSIP_BUDGET);
            debug_assert!(self.gossip.is_none());
            self.gossip = Some(pool_support::stale_gossip_job(
                upstream,
                self.identity,
                tokio::time::Instant::now() + gossip_budget,
            ));
            return WakeOutcome::Continue;
        }
        let active = self.peers.values().map(|peer| peer.addr).collect();
        if self
            .candidates
            .exploration_cohort(Instant::now(), &active, &self.learned_pending)
            .is_some_and(|cohort| self.candidates.explorations(upstream.addr) > cohort)
        {
            // Preserve the opportunity of a less-explored peer still connecting
            // or queued. Dropping this older transport consumes no new opportunity.
            return WakeOutcome::Continue;
        }
        let corroborates = self.continuity.startup_deadline.is_some()
            && upstream.window.max_piece.max(0) as u64 >= self.continuity.head;
        if corroborates
            && self.peers.len() >= self.live_recovery.max_active_upstreams
            && upstream.window.max_piece.max(0) as u64 == self.continuity.head
        {
            // Independent matching evidence is enough to start. Keep the sole
            // healthy provisional transport rather than churn it for capacity.
            self.continuity.startup_deadline = None;
            self.last_progress = Instant::now();
            crate::alog!("[ace] live start: matching independent window confirmed; retaining current upstream");
            let lost =
                advance_pool_requests(&mut self.peers, self.continuity, self.chunks_per_piece)
                    .await;
            record_pool_losses(self.candidates, &mut self.loss_count, lost);
            return WakeOutcome::Continue;
        }
        let replacement = if self.peers.len() >= self.live_recovery.max_active_upstreams {
            if !corroborates {
                return WakeOutcome::Continue;
            }
            self.peers
                .iter()
                .min_by_key(|(_, peer)| peer.max_piece)
                .map(|(&id, _)| id)
        } else {
            None
        };
        self.candidates
            .learn(upstream.addr, CandidateKind::Discovered);
        let peer_id = self.next_peer_id;
        self.next_peer_id += 1;
        let addr = upstream.addr;
        self.continuity.head = self
            .continuity
            .head
            .max(upstream.window.max_piece.max(0) as u64);
        self.continuity.skip_far_behind_live(Instant::now());
        match activate_upstream_peer(ActivationInputs {
            peer_id,
            upstream,
            start: self.continuity.reasm.next_needed(),
            identity: self.identity,
            continuity: self.continuity,
            event_tx: &self.event_tx,
            reachability: self.reachability,
            replacing: replacement.map(|id| (&mut self.peers, id)),
        })
        .await
        {
            Ok((runtime, replaced)) => {
                if let Some(replaced) = replaced {
                    // Already admitted once: no extra exploration or physical-loss
                    // event for retiring a provisional, nonproducing transport.
                    self.candidates.failed(replaced, Instant::now());
                }
                if corroborates {
                    self.continuity.startup_deadline = None;
                    self.last_progress = Instant::now();
                    crate::alog!("[ace] live start: independent upstream window confirmed; starting at best known head");
                }
                self.candidates.admitted(runtime.addr);
                crate::alog!(
                    "[ace] {addr}: added to active upstream pool ({} peer(s))",
                    self.peers.len() + 1
                );
                self.peers.insert(peer_id, runtime);
                self.peer_count
                    .store(self.peers.len() as u32, Ordering::Relaxed);
                let newly_lost =
                    advance_pool_requests(&mut self.peers, self.continuity, self.chunks_per_piece)
                        .await;
                record_pool_losses(self.candidates, &mut self.loss_count, newly_lost);
            }
            Err(addr) => {
                self.candidates.explored(addr);
                record_pool_losses(self.candidates, &mut self.loss_count, [addr]);
            }
        }
        WakeOutcome::Continue
    }

    async fn on_peer_event(&mut self, event: PeerEvent) -> Option<FollowEnd> {
        let (peer_id, addr, msg) = match event {
            PeerEvent::Lost { peer_id, .. } => {
                if handle_pool_peer_loss(
                    peer_id,
                    &mut self.peers,
                    self.continuity,
                    self.chunks_per_piece,
                    self.candidates,
                    &mut self.loss_count,
                    self.peer_count,
                )
                .await
                {
                    return Some(FollowEnd::PeerLost(self.loss_count));
                }
                return None;
            }
            PeerEvent::Message { peer_id, addr, msg } => (peer_id, addr, msg),
        };

        self.on_message(peer_id, addr, msg).await
    }

    async fn on_message(
        &mut self,
        peer_id: u64,
        addr: SocketAddrV4,
        msg: PeerMessage,
    ) -> Option<FollowEnd> {
        let mut made_activity = false;
        let mut made_output = false;
        if let PeerMessage::Extended { payload, .. } | PeerMessage::Unknown { payload, .. } = &msg {
            observe_live_window(self.continuity, peer_id, payload);
        }
        match msg {
            PeerMessage::Unchoke => {
                self.continuity.set_peer_unchoked(peer_id, true);
                crate::alog!(
                    "[ace] {addr}: UNCHOKE -> scheduling from piece {} toward head {}",
                    self.continuity.reasm.next_needed(),
                    self.continuity.head
                );
                let newly_lost =
                    advance_pool_requests(&mut self.peers, self.continuity, self.chunks_per_piece)
                        .await;
                record_pool_losses(self.candidates, &mut self.loss_count, newly_lost);
                made_activity = true;
            }
            PeerMessage::Choke => {
                self.continuity.set_peer_unchoked(peer_id, false);
            }
            PeerMessage::Have(p) => {
                let old_head = self.continuity.head;
                self.continuity.head = self.continuity.head.max(p as u64);
                update_runtime_window(&mut self.peers, self.continuity, peer_id, p as u64);
                made_activity |= self.continuity.head > old_head;
                let newly_lost =
                    advance_pool_requests(&mut self.peers, self.continuity, self.chunks_per_piece)
                        .await;
                record_pool_losses(self.candidates, &mut self.loss_count, newly_lost);
            }
            PeerMessage::Extended { ref payload, .. } => {
                if let Some(max_piece) = read_window_max(payload) {
                    let window_advanced = self
                        .peers
                        .get(&peer_id)
                        .is_some_and(|peer| max_piece > peer.max_piece);
                    if max_piece > self.continuity.head || window_advanced {
                        self.continuity.head = self.continuity.head.max(max_piece);
                        update_runtime_window(&mut self.peers, self.continuity, peer_id, max_piece);
                        made_activity = true;
                        let newly_lost = advance_pool_requests(
                            &mut self.peers,
                            self.continuity,
                            self.chunks_per_piece,
                        )
                        .await;
                        record_pool_losses(self.candidates, &mut self.loss_count, newly_lost);
                    }
                }
            }
            m @ PeerMessage::Piece { .. } => match self.on_piece(addr, m).await {
                PieceOutcome::Skip => return None,
                PieceOutcome::End(end) => return Some(end),
                PieceOutcome::Progress { activity, output } => {
                    made_activity = activity;
                    made_output = output;
                }
            },
            PeerMessage::Interested => {
                let should_unchoke = if self.seed.enabled {
                    self.peers
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
                    let _ = send_peer_command(
                        peer_id,
                        PeerMessage::Unchoke,
                        &mut self.peers,
                        self.continuity,
                    )
                    .await;
                }
            }
            request @ PeerMessage::Unknown { id: 6, .. } if self.seed.enabled => {
                if let Some(reply) =
                    prepare_reply(&request, Some(self.store), ServeProfile::Reciprocal).await
                {
                    if send_peer_command(peer_id, reply.message, &mut self.peers, self.continuity)
                        .await
                        .is_none()
                    {
                        self.uploaded
                            .fetch_add(reply.media_bytes as u64, Ordering::Relaxed);
                        self.peers_served
                            .store(self.peers.len() as u32, Ordering::Relaxed);
                    }
                }
            }
            PeerMessage::Unknown { id: 4, ref payload } if payload.len() == 8 => {
                let piece =
                    u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]) as u64;
                let window_advanced = self
                    .peers
                    .get(&peer_id)
                    .is_some_and(|peer| piece > peer.max_piece);
                if piece > self.continuity.head || window_advanced {
                    // Another upstream can advertise the shared head first. Its gossip
                    // must not suppress this producer's independently advancing window.
                    self.continuity.head = self.continuity.head.max(piece);
                    update_runtime_window(&mut self.peers, self.continuity, peer_id, piece);
                    made_activity = true;
                    let newly_lost = advance_pool_requests(
                        &mut self.peers,
                        self.continuity,
                        self.chunks_per_piece,
                    )
                    .await;
                    record_pool_losses(self.candidates, &mut self.loss_count, newly_lost);
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
                    self.continuity.reasm.next_needed(),
                );
                for addr in ranked {
                    self.candidates.learn(addr, CandidateKind::Pex);
                }
                self.maybe_refill(false);
            }
            PeerMessage::Unknown {
                id: 36,
                ref payload,
            } => {
                if let Some(source) = ace_wire::peer_exchange::parse_peer_announce(payload) {
                    self.candidates.learn(source, CandidateKind::Source);
                    crate::alog!("[ace] source-node announce from {addr}: retained {source}");
                    self.maybe_refill(false);
                }
            }
            // Peer telemetry we don't act on: id=11 bencode stats, id=13 keepalive, id=34
            // counter. Explicit no-ops so they don't spam the "unhandled msg" log.
            PeerMessage::Unknown {
                id: 11 | 13 | 34, ..
            } => {}
            PeerMessage::Unknown { id, ref payload } => {
                let max_piece = read_window_max(payload);
                let window_advanced = max_piece.is_some_and(|max_piece| {
                    max_piece > self.continuity.head
                        || self
                            .peers
                            .get(&peer_id)
                            .is_some_and(|peer| max_piece > peer.max_piece)
                });
                if let Some(max_piece) = max_piece.filter(|_| window_advanced) {
                    if max_piece > self.continuity.head {
                        crate::alog!(
                            "[ace] {addr}: live window update (msg id={id}) head {} -> {max_piece}",
                            self.continuity.head
                        );
                    }
                    self.continuity.head = self.continuity.head.max(max_piece);
                    update_runtime_window(&mut self.peers, self.continuity, peer_id, max_piece);
                    made_activity = true;
                    let newly_lost = advance_pool_requests(
                        &mut self.peers,
                        self.continuity,
                        self.chunks_per_piece,
                    )
                    .await;
                    record_pool_losses(self.candidates, &mut self.loss_count, newly_lost);
                } else {
                    let should_log = self
                        .peers
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
        if self.peers.is_empty() {
            return Some(FollowEnd::PeerLost(self.loss_count));
        }
        if should_refresh_stale_deadline(self.continuity.emitted, made_output, made_activity) {
            self.last_progress = Instant::now();
        }
        if made_output {
            if let Some(peer) = self.peers.get_mut(&peer_id) {
                peer.produced_output = true;
            }
            self.candidates.productive(addr);
            // Contiguous output means the playback cursor advanced: reset the per-piece
            // eviction-skip timer so it only fires when the cursor is genuinely stuck.
            self.continuity.next_needed_since = Instant::now();
        }
        None
    }
    async fn on_piece(&mut self, addr: SocketAddrV4, m: PeerMessage) -> PieceOutcome {
        let mut made_activity = false;
        let mut made_output = false;

        if let Some(lc) = LiveChunk::from_message(&m) {
            self.continuity.skip_far_behind_live(Instant::now());
            let piece = lc.piece as u64;
            if self.continuity.startup_deadline.is_some()
                || piece < self.continuity.reasm.next_needed()
            {
                return PieceOutcome::Skip;
            }
            let outcome = match self.continuity.reasm.add_live_chunk(
                piece,
                lc.chunk,
                self.info.chunk_length,
                lc.piece_header,
                &lc.data,
            ) {
                Ok(outcome) => outcome,
                Err(e) => {
                    if e == ace_wire::WireError::SignatureRejected {
                        self.continuity
                            .recovery_event(RecoveryEvent::SignatureReject);
                    }
                    // A malformed block, or a completed piece whose live-source signature
                    // didn't verify (#10): drop it rather than emit unauthenticated bytes.
                    // The piece stays incomplete, so it's re-requested from the pool.
                    crate::alog!("[ace] {addr}: dropped piece {piece} block: {e:?}");
                    self.continuity.release_rejected_piece(piece);
                    let newly_lost = advance_pool_requests(
                        &mut self.peers,
                        self.continuity,
                        self.chunks_per_piece,
                    )
                    .await;
                    record_pool_losses(self.candidates, &mut self.loss_count, newly_lost);
                    return PieceOutcome::Skip;
                }
            };
            if outcome.completed {
                if let Some(done) = self.continuity.reasm.completed_live_piece(piece) {
                    if matches!(
                        done.acceptance,
                        ace_wire::reassembly::LiveAcceptance::SourceVerified
                            | ace_wire::reassembly::LiveAcceptance::UnsignedAssemblyAccepted
                    ) {
                        self.continuity
                            .publication
                            .entry(piece)
                            .or_insert(PublicationStatus::Needed);
                    }
                }
            }
            if outcome.contributed {
                self.continuity
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
            if outcome.contributed && !outcome.completed {
                self.continuity
                    .note_chunk(piece, lc.chunk, self.chunks_per_piece);
            } else if self.continuity.reasm.completed_live_piece(piece).is_some() {
                self.continuity.received_chunks.remove(&piece);
                self.continuity.scheduler.on_complete(piece);
                self.continuity.active_peers.complete_everywhere(piece);
                self.continuity.requested_at.remove(&piece);
            }
            made_activity = true;
            #[cfg(test)]
            if let Some(observer) = &self.continuity.contribution_observer {
                let _ = observer.send((piece, addr));
            }
            self.start_publication();
            if !self.continuity.publication_hold() {
                match self.drain_output().await {
                    PieceOutcome::End(end) => return PieceOutcome::End(end),
                    PieceOutcome::Progress { output, .. } => made_output = output,
                    PieceOutcome::Skip => {}
                }
            }
            let newly_lost =
                advance_pool_requests(&mut self.peers, self.continuity, self.chunks_per_piece)
                    .await;
            record_pool_losses(self.candidates, &mut self.loss_count, newly_lost);
        }
        PieceOutcome::Progress {
            activity: made_activity,
            output: made_output,
        }
    }
    async fn drain_output(&mut self) -> PieceOutcome {
        let mut made_output = false;
        let addr = self.peers.values().next().expect("active pool").addr;
        let before = self.continuity.reasm.next_needed();
        let ready = take_ready_with_stats(self.continuity, self.seed.metrics.as_ref(), self.info);
        let after = self.continuity.reasm.next_needed();
        let productive: Vec<_> = self
            .continuity
            .piece_producers
            .range(before..after)
            .filter_map(|(_, producer)| *producer)
            .collect();
        self.continuity
            .piece_producers
            .retain(|&piece, _| piece >= after);
        if !ready.is_empty() {
            if !self.continuity.authenticated_logged
                && self.info.sig_len > 0
                && ace_wire::live_auth::signature_len_from_pubkey_der(&self.info.source_pubkey)
                    == Some(self.info.sig_len)
            {
                self.continuity.authenticated_logged = true;
                crate::alog!("[ace] discovery stage=authenticated-contiguous");
            }
            for output in self.continuity.resync_output(&ready) {
                let aligned = output.bytes;
                if self.continuity.emitted == 0 {
                    crate::alog!("[ace] discovery stage=first-source-output");
                }
                self.continuity.emitted += aligned.len() as u64;
                if self.continuity.emitted >= self.continuity.next_log {
                    crate::alog!(
                        "[ace] {addr}: served {} MiB (head={}, next piece needed={})",
                        self.continuity.emitted >> 20,
                        self.continuity.head,
                        self.continuity.reasm.next_needed()
                    );
                    self.continuity.next_log = self.continuity.emitted + (4 << 20);
                }
                let len = aligned.len() as u64;
                if self
                    .tx
                    .send(LiveOutput {
                        bytes: aligned,
                        discontinuity: output.discontinuity,
                    })
                    .await
                    .is_err()
                {
                    shutdown_peer_runtimes(&mut self.peers);
                    return PieceOutcome::End(FollowEnd::ConsumerGone);
                }
                self.downloaded.fetch_add(len, Ordering::Relaxed);
                made_output = true;
            }
        }
        if made_output {
            for producer in &productive {
                self.candidates.productive(*producer);
                for peer in self
                    .peers
                    .values_mut()
                    .filter(|peer| peer.addr == *producer)
                {
                    peer.produced_output = true;
                }
            }
        }
        if made_output
            && self.info.sig_len > 0
            && ace_wire::live_auth::signature_len_from_pubkey_der(&self.info.source_pubkey)
                == Some(self.info.sig_len)
        {
            for producer in productive {
                self.seed.warm_peers.record_productive(
                    self.info.infohash,
                    producer,
                    self.candidates.kind(producer),
                );
            }
        }

        if made_output {
            self.last_progress = Instant::now();
            self.continuity.next_needed_since = Instant::now();
        }
        PieceOutcome::Progress {
            activity: false,
            output: made_output,
        }
    }
}
