use super::*;
#[cfg(test)]
mod activation_tests;
#[cfg(test)]
mod drain_tests;
#[cfg(test)]
mod output_schedules;
mod publication;
mod reciprocal;
use publication::PublicationOperation;

type OutputReservation = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<mpsc::OwnedPermit<LiveOutput>, mpsc::error::SendError<()>>,
            > + Send,
    >,
>;

type ConnectCompletion =
    Option<Result<(SocketAddrV4, Option<PeerConnectFailure>, bool), tokio::task::JoinError>>;

enum PieceOutcome {
    Skip,
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
    pub(super) output_reservation: Option<OutputReservation>,
    pub(super) activation: Option<activation::Activation>,
    pub(super) reciprocal: reciprocal::Reciprocal,
    pub(super) publication: Option<PublicationOperation>,
    pub(super) closed_peer_events: BTreeMap<u64, (SocketAddrV4, usize)>,
    pub(super) observed_retry: Instant,
    pub(super) observed_capacity: (usize, usize),
    pub(super) refill_closed: bool,
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
        #[cfg(test)]
        let _ = ACTIVATION_PROBE.try_with(|probe| {
            if let Some(candidate) = probe.candidate.borrow_mut().take() {
                self.learned_pending.insert(candidate.addr);
                self.pex_tx
                    .try_send(PoolRefill::ReservedDiscovered(candidate))
                    .unwrap();
            }
        });
        self.maybe_refill(false);
        // Start progress/retry clocks after initial refill, as in the adapter before extraction.
        if !self.continuity.has_delivery_work() {
            self.continuity.last_progress = self.continuity.output_clock.now(Instant::now());
            self.continuity.first_output = if self.continuity.emitted > 0 {
                FirstOutput::Done
            } else {
                FirstOutput::AwaitCorroboration
            };
        }
        self.continuity.output_budget(Instant::now());
        self.observed_capacity = (self.peers.len(), self.learned_pending.len());
        self.observed_retry = Instant::now();
        loop {
            #[cfg(test)]
            output_schedules::turn();
            if self.tx.is_closed() {
                return FollowEnd::ConsumerGone;
            }
            #[cfg(test)]
            {
                self.reciprocal.turn += 1;
            }
            let completed =
                std::future::poll_fn(|cx| std::task::Poll::Ready(self.poll_activation(cx))).await;
            if let Some(result) = completed {
                self.finish_activation(result).await;
            }
            self.start_reciprocal();
            let reciprocal_ready = std::future::poll_fn(|cx| {
                std::task::Poll::Ready(match reciprocal::poll_job(&mut self.reciprocal, cx) {
                    std::task::Poll::Ready(result) => Some(result),
                    std::task::Poll::Pending => None,
                })
            })
            .await;
            let reciprocal_completed = reciprocal_ready.is_some();
            if let Some((id, reply)) = reciprocal_ready {
                #[cfg(test)]
                self.reciprocal.observe(1);
                self.finish_reciprocal(id, reply).await;
            }
            self.start_publication();
            let ready =
                std::future::poll_fn(|cx| std::task::Poll::Ready(self.poll_publication(cx))).await;
            if ready {
                self.start_publication();
            }
            self.start_output();
            let output_ready = std::future::poll_fn(|cx| {
                std::task::Poll::Ready(poll_output(&mut self.output_reservation, cx))
            })
            .await;
            let output_completed = output_ready.is_some();
            if let Some(result) = output_ready {
                if let Some(end) = self.finish_output(result) {
                    return end;
                }
                tokio::task::yield_now().await;
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
            let _stats_work = self
                .continuity
                .advance_stats(self.seed.metrics.is_some(), self.info.metadata.bitrate);
            #[cfg(test)]
            output_schedules::stats(_stats_work);
            publish_live_stats(self.continuity, self.seed.metrics.as_ref(), self.info);
            if self
                .continuity
                .startup_deadline
                .is_some_and(|deadline| now >= deadline)
            {
                self.continuity.startup_deadline = None;
                self.continuity.output_budget(Instant::now());
                self.continuity.last_progress = self.continuity.output_clock.now(now);
                crate::alog!(
                    "[ace] live start: corroboration deadline reached; using best known window without freshness proof"
                );
                let lost =
                    advance_pool_requests(&mut self.peers, self.continuity, self.chunks_per_piece)
                        .await;
                record_pool_losses(self.candidates, &mut self.loss_count, lost);
            }
            // Re-evaluate the budget after startup scheduling.
            let now = Instant::now();
            let budget = self.continuity.output_budget(now);
            if budget == OutputBudget::Expired {
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
            }
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
            if self.peers.is_empty() && !self.continuity.has_delivery_work() {
                return FollowEnd::PeerLost(self.loss_count);
            }
            self.peer_count
                .store(self.peers.len() as u32, Ordering::Relaxed);

            // Wake at least every configured request-check interval for credit
            // admission and retransmit checks, even while no peer sends anything.
            let mut wait = self.live_recovery.request_check_interval();
            if let OutputBudget::Remaining(remaining) = budget {
                wait = wait.min(remaining);
            }
            if let Some(deadline) = self.continuity.startup_deadline {
                wait = wait.min(deadline.saturating_duration_since(now));
            }
            let active = self
                .peers
                .values()
                .map(|peer| peer.addr)
                .chain(self.activation.as_ref().map(|job| job.addr))
                .collect();
            let retry_deadline = if (self.peers.len() < self.live_recovery.max_active_upstreams
                || self.continuity.startup_deadline.is_some())
                && self.learned_pending.len()
                    + usize::from(self.gossip.is_some())
                    + usize::from(self.activation.is_some())
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
            None if reciprocal_completed => PoolWake::Reciprocal(None),
            None if ready => PoolWake::Publication,
            None if output_completed => PoolWake::Stats,
            None => match tokio::time::timeout(wait, async {
                tokio::select! {
                    result = std::future::poll_fn(|cx| poll_output(&mut self.output_reservation, cx).map_or(std::task::Poll::Pending, std::task::Poll::Ready)), if !output_completed && self.output_reservation.is_some() => PoolWake::Output(result),
                    _ = tokio::task::yield_now(), if self.continuity.stats_pending(self.seed.metrics.is_some()) => PoolWake::Stats,
                    result = std::future::poll_fn(|cx| {
                        let result = reciprocal::poll_job(&mut self.reciprocal, cx);
                        #[cfg(test)]
                        if result.is_pending() && self.reciprocal.job.is_some() { self.reciprocal.observe(3); }
                        result
                    }), if !reciprocal_completed => PoolWake::Reciprocal(Some(result)),
                    result = std::future::poll_fn(|cx| self.activation.as_mut().expect("enabled activation").write.as_mut().poll(cx)), if self.activation.is_some() => PoolWake::Activation(result),
                    _ = self.tx.closed() => PoolWake::ConsumerGone,
                    _ = std::future::poll_fn(|cx| publication::poll_operation(&mut self.publication, self.continuity, self.store, cx).then_some(()).map_or(std::task::Poll::Pending, std::task::Poll::Ready)), if self.publication.is_some() => PoolWake::Publication,
                    learned = async { self.gossip.as_mut().expect("enabled gossip").await }, if self.gossip.is_some() => PoolWake::Gossip(learned),
                    event = self.event_rx.recv(), if !self.peers.is_empty() => PoolWake::Peer(event),
                    result=self.learned_connects.join_next(), if !self.learned_connects.is_empty()=>PoolWake::ConnectCompletion(result),
                    _ = async {
                        tokio::time::sleep_until(retry_deadline.expect("enabled retry deadline").into()).await;
                    }, if retry_deadline.is_some() => PoolWake::CandidateRetry,
                    peer=next_discovery_peer(self.discovery_run), if self.discovery_run.is_some()=>PoolWake::Discovery(peer),
                    upstream = self.refill_rx.recv(), if self.activation.is_none() && self.gossip.is_none() && !self.refill_closed && (self.peers.len() < self.live_recovery.max_active_upstreams || self.continuity.startup_deadline.is_some()) => {
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
                held: self.activation.as_ref().map(|job| job.addr),
                candidates: self.candidates,
                peers: &self.peers,
                infohash: self.info.infohash,
                // The held gossip socket still consumes a transport slot.
                max_parallel: self.live_recovery.max_parallel_connect.saturating_sub(
                    usize::from(self.gossip.is_some()) + usize::from(self.activation.is_some()),
                ),
                tx: &self.pex_tx,
                tasks: &mut self.learned_connects,
                pending: &mut self.learned_pending,
                include_discovered: true,
            });
        }
    }

    async fn on_wake(&mut self, event: PoolWake) -> WakeOutcome {
        match event {
            PoolWake::Output(result) => {
                if let Some(end) = self.finish_output(result) {
                    return WakeOutcome::End(end);
                }
                tokio::task::yield_now().await;
                WakeOutcome::Continue
            }
            PoolWake::Activation(result) => {
                self.finish_activation(result).await;
                WakeOutcome::Continue
            }
            PoolWake::Reciprocal(result) => {
                if let Some((id, reply)) = result {
                    #[cfg(test)]
                    self.reciprocal.observe(2);
                    self.finish_reciprocal(id, reply).await;
                }
                WakeOutcome::Continue
            }
            PoolWake::Gossip(learned) => {
                self.gossip = None;
                for (addr, kind) in learned.into_learned() {
                    self.candidates.learn(addr, kind);
                }
                self.maybe_refill(true);
                WakeOutcome::Continue
            }
            PoolWake::Stats => WakeOutcome::Continue,
            PoolWake::Publication => {
                self.start_publication();
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
                if self.continuity.has_delivery_work() {
                    WakeOutcome::Continue
                } else {
                    WakeOutcome::End(FollowEnd::PeerLost(self.loss_count))
                }
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
            let now = Instant::now();
            let output_remaining = match self.continuity.output_budget(now) {
                OutputBudget::Remaining(remaining) => Some(remaining),
                OutputBudget::Expired => Some(Duration::ZERO),
                OutputBudget::Suspended => None,
            };
            let gossip_budget = self
                .continuity
                .startup_deadline
                .map(|deadline| deadline.saturating_duration_since(now))
                .or(output_remaining)
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
            self.continuity.output_budget(Instant::now());
            self.continuity.last_progress = self.continuity.output_clock.now(Instant::now());
            crate::alog!(
                "[ace] live start: matching independent window confirmed; retaining current upstream"
            );
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
        self.continuity.head = self
            .continuity
            .head
            .max(upstream.window.max_piece.max(0) as u64);
        self.continuity
            .skip_far_behind_live(self.continuity.output_clock.now(Instant::now()));
        self.activation = Some(activation::Activation {
            addr: upstream.addr,
            peer_id,
            replacement,
            write: activation::prepare(
                peer_id,
                upstream,
                self.continuity.reasm.next_needed(),
                self.continuity.head,
                self.identity,
                self.reachability,
            ),
        });
        WakeOutcome::Continue
    }

    fn poll_activation(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Option<activation::WriteResult> {
        match self.activation.as_mut()?.write.as_mut().poll(cx) {
            std::task::Poll::Ready(result) => Some(result),
            std::task::Poll::Pending => None,
        }
    }

    async fn finish_activation(&mut self, result: activation::WriteResult) {
        let job = self.activation.take().expect("completed activation");
        if self.tx.is_closed() {
            return;
        }
        let peer_id = job.peer_id;
        let replacement = job.replacement;
        let result = result.map(|upstream| {
            let active = self.peers.values().map(|peer| peer.addr).collect();
            let corroborates = self.continuity.startup_deadline.is_some()
                && upstream.window.max_piece.max(0) as u64 >= self.continuity.head;
            let eligible = self.continuity.window_can_resume(&upstream.window)
                && !self.peers.values().any(|peer| peer.addr == upstream.addr)
                && !self
                    .candidates
                    .exploration_cohort(Instant::now(), &active, &self.learned_pending)
                    .is_some_and(|cohort| self.candidates.explorations(upstream.addr) > cohort);
            if !eligible {
                return None;
            }
            let candidate_max = upstream.window.max_piece.max(0) as u64;
            let full = self.peers.len() == self.live_recovery.max_active_upstreams;
            if full
                && corroborates
                && candidate_max == self.continuity.head
                && replacement
                    .and_then(|id| self.peers.get(&id))
                    .is_some_and(|peer| peer.max_piece == candidate_max)
            {
                // The provisional caught up while writes were Pending. Preserve
                // the existing matching-window corroboration/retention policy.
                self.continuity.startup_deadline = None;
                self.continuity.output_budget(Instant::now());
                self.continuity.last_progress = self.continuity.output_clock.now(Instant::now());
                return None;
            }
            let valid = match replacement {
                None => self.peers.len() < self.live_recovery.max_active_upstreams,
                Some(id) => {
                    full && corroborates
                        && self
                            .peers
                            .get(&id)
                            .is_some_and(|peer| peer.max_piece < candidate_max)
                }
            };
            if !valid {
                return None;
            }
            let runtime = commit_upstream_peer(ActivationInputs {
                peer_id,
                upstream,
                start: 0,
                identity: self.identity,
                continuity: self.continuity,
                event_tx: &self.event_tx,
                reachability: self.reachability,
                replacing: replacement.map(|id| (&mut self.peers, id)),
            });
            Some((runtime, corroborates))
        });
        match result {
            Ok(Some(((runtime, replaced), corroborates))) => {
                let addr = runtime.addr;

                if let Some(replaced) = replaced {
                    // Already admitted once: no extra exploration or physical-loss
                    // event for retiring a provisional, nonproducing transport.
                    self.candidates.failed(replaced, Instant::now());
                }
                if corroborates {
                    self.continuity.startup_deadline = None;
                    self.continuity.output_budget(Instant::now());
                    self.continuity.last_progress =
                        self.continuity.output_clock.now(Instant::now());
                    crate::alog!(
                        "[ace] live start: independent upstream window confirmed; starting at best known head"
                    );
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
            Ok(None) => (),
            Err(addr) => {
                self.candidates.explored(addr);
                record_pool_losses(self.candidates, &mut self.loss_count, [addr]);
            }
        }
        // Reservation consumption may have been observed on an earlier busy
        // turn. Completion itself releases the held transport slot.
        self.maybe_refill(false);
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
                    && !self.continuity.has_delivery_work()
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
            PeerMessage::Unknown { id: 6, payload } if self.seed.enabled => {
                self.queue_reciprocal(peer_id, &payload);
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
        if self.peers.is_empty() && !self.continuity.has_delivery_work() {
            return Some(FollowEnd::PeerLost(self.loss_count));
        }
        if should_refresh_stale_deadline(self.continuity.emitted, made_output, made_activity) {
            self.continuity.last_progress = self.continuity.output_clock.now(Instant::now());
        }
        if made_output {
            if let Some(peer) = self.peers.get_mut(&peer_id) {
                peer.produced_output = true;
            }
            self.candidates.productive(addr);
            // Contiguous output means the playback cursor advanced: reset the per-piece
            // eviction-skip timer so it only fires when the cursor is genuinely stuck.
            self.continuity.next_needed_since = self.continuity.output_clock.now(Instant::now());
        }
        None
    }
    async fn on_piece(&mut self, addr: SocketAddrV4, m: PeerMessage) -> PieceOutcome {
        let mut made_activity = false;
        let made_output = false;

        if let Some(lc) = LiveChunk::from_message(&m) {
            self.continuity
                .skip_far_behind_live(self.continuity.output_clock.now(Instant::now()));
            let piece = lc.piece as u64;
            if self.continuity.startup_deadline.is_some()
                || self
                    .continuity
                    .output_ceiling
                    .is_some_and(|ceiling| piece >= ceiling)
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
                if !self.continuity.stats_scan.pending() {
                    self.continuity.invalidate_stats();
                }
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
    fn start_output(&mut self) {
        if self.output_reservation.is_none() && self.continuity.output_demand() {
            self.continuity.output_ceiling.get_or_insert_with(|| {
                self.continuity
                    .reasm
                    .next_needed()
                    .saturating_add(self.live_recovery.max_reasm_pieces_ahead)
            });
            self.continuity.output_clock.begin(Instant::now());
            self.output_reservation = Some(Box::pin(self.tx.clone().reserve_owned()));
        }
    }

    fn finish_output(
        &mut self,
        result: Result<mpsc::OwnedPermit<LiveOutput>, mpsc::error::SendError<()>>,
    ) -> Option<FollowEnd> {
        finish_output_step(
            OutputContext {
                continuity: self.continuity,
                candidates: self.candidates,
                peers: &mut self.peers,
                info: self.info,
                seed: self.seed,
                downloaded: self.downloaded,
                tx: self.tx,
            },
            result,
        )
    }
}

fn poll_output(
    reservation: &mut Option<OutputReservation>,
    cx: &mut std::task::Context<'_>,
) -> Option<Result<mpsc::OwnedPermit<LiveOutput>, mpsc::error::SendError<()>>> {
    let future = reservation.as_mut()?;
    match future.as_mut().poll(cx) {
        std::task::Poll::Pending => {
            #[cfg(test)]
            output_schedules::pending();
            #[cfg(test)]
            let _ = crate::live_session::tests::output_progress::OUTPUT_SEND_PENDING
                .try_with(|seen| seen.notify_one());
            None
        }
        std::task::Poll::Ready(result) => {
            *reservation = None;
            Some(result)
        }
    }
}

struct OutputContext<'a> {
    continuity: &'a mut Continuity,
    candidates: &'a mut SessionCandidates,
    peers: &'a mut BTreeMap<u64, PeerRuntime>,
    info: &'a StreamInfo,
    seed: &'a SeedConfig,
    downloaded: &'a Arc<AtomicU64>,
    tx: &'a mpsc::Sender<LiveOutput>,
}

fn finish_output_step(
    context: OutputContext<'_>,
    result: Result<mpsc::OwnedPermit<LiveOutput>, mpsc::error::SendError<()>>,
) -> Option<FollowEnd> {
    let OutputContext {
        continuity,
        candidates,
        peers,
        info,
        seed,
        downloaded,
        tx,
    } = context;
    let Ok(permit) = result else {
        return Some(FollowEnd::ConsumerGone);
    };
    if tx.is_closed() {
        return Some(FollowEnd::ConsumerGone);
    }
    if continuity.pending_output.is_empty() {
        if !continuity.output_demand() {
            continuity.output_ceiling = None;
            continuity.output_clock.end(Instant::now());
            return None;
        }
        let piece = continuity.reasm.take_ready_piece()?;
        #[cfg(test)]
        output_schedules::prepare();
        continuity.publication.remove(&piece.index);
        let credit = continuity
            .piece_producers
            .remove(&piece.index)
            .flatten()
            .map(|producer| {
                Arc::new(OutputCredit {
                    producer,
                    kind: candidates.kind(producer),
                    credited: std::sync::atomic::AtomicBool::new(false),
                })
            });
        continuity.invalidate_stats();
        if let Some(metrics) = &seed.metrics {
            metrics.received_bytes(piece.bytes.len() as u64);
        }
        #[cfg(test)]
        let preparation_start = Instant::now();
        let segments = continuity.resync_output_credited(&piece.bytes, credit);
        #[cfg(test)]
        output_schedules::prepared_in(preparation_start.elapsed());
        continuity.pending_output.extend(segments);
        #[cfg(test)]
        output_schedules::retained(continuity);
        if continuity.pending_output.is_empty() {
            continuity.output_ceiling = None;
            continuity.output_clock.end(Instant::now());
            return None;
        }
    }
    if let Some(output) = continuity.pending_output.pop_front() {
        let len = output.bytes.len() as u64;
        drop(permit.send(output));
        #[cfg(test)]
        output_schedules::enqueue();
        for credit in continuity.pending_credit.pop_front().unwrap_or_default() {
            if !credit.credited.swap(true, Ordering::Relaxed) {
                candidates.productive(credit.producer);
                for peer in peers
                    .values_mut()
                    .filter(|peer| peer.addr == credit.producer)
                {
                    peer.produced_output = true;
                }
                if info.sig_len > 0
                    && ace_wire::live_auth::signature_len_from_pubkey_der(&info.source_pubkey)
                        == Some(info.sig_len)
                {
                    seed.warm_peers
                        .record_productive(info.infohash, credit.producer, credit.kind);
                }
            }
        }
        if !continuity.authenticated_logged
            && info.sig_len > 0
            && ace_wire::live_auth::signature_len_from_pubkey_der(&info.source_pubkey)
                == Some(info.sig_len)
        {
            continuity.authenticated_logged = true;
            crate::alog!("[ace] discovery stage=authenticated-contiguous");
        }
        if continuity.emitted == 0 {
            crate::alog!("[ace] discovery stage=first-source-output");
        }
        continuity.first_output = FirstOutput::Done;
        continuity.emitted += len;
        if continuity.emitted >= continuity.next_log {
            crate::alog!(
                "[ace] served {} MiB (head={}, next piece needed={})",
                continuity.emitted >> 20,
                continuity.head,
                continuity.reasm.next_needed()
            );
            continuity.next_log = continuity.emitted + (4 << 20);
        }
        downloaded.fetch_add(len, Ordering::Relaxed);
        continuity.last_progress = continuity.output_clock.now(Instant::now());
        continuity.next_needed_since = continuity.output_clock.now(Instant::now());
    }
    if continuity.pending_output.is_empty() && continuity.reasm.ready_chunks().next().is_none() {
        continuity.output_ceiling = None;
        continuity.output_clock.end(Instant::now());
    }
    None
}

/// Drain only already accepted work, before discovery or any resume mutation.
pub(in crate::live_session) async fn drain_retained(
    c: &mut Continuity,
    candidates: &mut SessionCandidates,
    info: &StreamInfo,
    seed: &SeedConfig,
    store: &Arc<tokio::sync::Mutex<PieceStore>>,
    downloaded: &Arc<AtomicU64>,
    tx: &mpsc::Sender<LiveOutput>,
) -> bool {
    let mut reservation = None;
    let mut publication = None;
    let mut peers = BTreeMap::new();
    while c.has_delivery_work() {
        if tx.is_closed() {
            return false;
        }
        if c.startup_deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            c.startup_deadline = None;
        }
        c.output_budget(Instant::now());
        c.advance_stats(seed.metrics.is_some(), info.metadata.bitrate);
        publish_live_stats(c, seed.metrics.as_ref(), info);
        publication::start_operation(&mut publication, c, store);
        if reservation.is_none() && c.output_demand() {
            c.output_ceiling.get_or_insert(
                c.reasm
                    .next_needed()
                    .saturating_add(c.live_recovery.max_reasm_pieces_ahead),
            );
            c.output_clock.begin(Instant::now());
            reservation = Some(Box::pin(tx.clone().reserve_owned()) as OutputReservation);
        }
        let corroboration = c.startup_deadline;
        let stats_pending = c.stats_pending(seed.metrics.is_some());
        tokio::select! {
            _ = tx.closed() => return false,
            _ = tokio::task::yield_now(), if stats_pending => {},
            _ = async {
                if let Some(deadline) = corroboration {
                    tokio::time::sleep(deadline.saturating_duration_since(Instant::now())).await;
                } else { std::future::pending::<()>().await; }
            } => {},
            result = std::future::poll_fn(|cx| {
                if publication::poll_operation(&mut publication, c, store, cx) {
                    return std::task::Poll::Ready(None);
                }
                match poll_output(&mut reservation, cx) {
                    Some(result) => std::task::Poll::Ready(Some(result)),
                    None => std::task::Poll::Pending,
                }
            }) => {
                if let Some(result) = result {
                    if finish_output_step(OutputContext { continuity: c, candidates, peers: &mut peers, info, seed, downloaded, tx }, result).is_some() { return false; }
                }
            }
        }
        tokio::task::yield_now().await;
    }
    true
}
