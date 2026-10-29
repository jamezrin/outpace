use super::*;
use std::collections::VecDeque;

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;

    // Uses the real actor admission/start/finish seams and real peer command
    // channels. Transport activation and TCP ordering are outside this fixture.
    #[tokio::test(start_paused = true)]
    async fn actor_fifo_overflow_retry_round_robin_stale_generation_and_ready_drain() {
        let info = StreamInfo {
            infohash: [0; 20],
            piece_length: 4,
            chunk_length: 2,
            trackers: vec![],
            metadata: Default::default(),
            sig_len: 0,
            source_pubkey: vec![],
        };
        let identity = Identity::generate();
        let policy = LiveRecoveryConfig::default();
        let seed = SeedConfig {
            metrics: None,
            registry: SeedRegistry::new(),
            store_factory: test_store_factory(4096, None),
            enabled: true,
            prefetch_pieces: 0,
            live_recovery: policy,
            warm_peers: WarmPeerCache::memory(),
        };
        let store = Arc::new(tokio::sync::Mutex::new(PieceStore::new(4, 2, 4096)));
        for piece in 0..100 {
            store
                .lock()
                .await
                .put_chunk_with_header(piece, 0, [0; 8], &[piece as u8, 0xaa]);
        }
        let (tx, mut output) = mpsc::channel(1);
        let downloaded = Arc::new(AtomicU64::new(0));
        let uploaded = Arc::new(AtomicU64::new(0));
        let served = Arc::new(AtomicU32::new(0));
        let count = Arc::new(AtomicU32::new(0));
        let (mut continuity, _) = Continuity::fresh(&info, 0, 0, 0, policy);
        let mut candidates = SessionCandidates::default();
        let mut discovery = None;
        let (event_tx, event_rx) = mpsc::channel(128);
        let (pex_tx, refill_rx) = mpsc::channel(2);
        let (peer1, mut commands1, worker1) = runtime(1);
        let (peer2, mut commands2, worker2) = runtime(2);
        for (id, peer) in [(1, &peer1), (2, &peer2)] {
            continuity.register_active_peer(
                id,
                peer.addr,
                WireLivePosition {
                    min_piece: 0,
                    max_piece: 0,
                    position: 0,
                    distance_from_source: 1,
                },
            );
        }
        let mut actor = PoolActor {
            reciprocal: Default::default(),
            publication: None,
            gossip: None,
            closed_peer_events: BTreeMap::new(),
            observed_retry: Instant::now(),
            observed_capacity: (2, 0),
            refill_closed: true,
            last_progress: Instant::now(),
            first_output_deadline: None,
            next_peer_id: 3,
            loss_count: 0,
            peers: BTreeMap::from([(1, peer1), (2, peer2)]),
            learned_pending: HashSet::new(),
            learned_connects: tokio::task::JoinSet::new(),
            pex_tx,
            refill_rx,
            event_rx,
            event_tx,
            _stats_guard: LiveStatsGuard(None),
            live_recovery: policy,
            info: &info,
            identity: &identity,
            chunks_per_piece: 2,
            tx: &tx,
            downloaded: &downloaded,
            uploaded: &uploaded,
            peers_served: &served,
            seed: &seed,
            store: &store,
            candidates: &mut candidates,
            peer_count: &count,
            reachability: None,
            discovery_run: &mut discovery,
            continuity: &mut continuity,
        };
        let payload = |piece: u32| {
            let mut bytes = 999u32.to_be_bytes().to_vec();
            bytes.extend(piece.to_be_bytes());
            bytes.extend(0u16.to_be_bytes());
            bytes.extend([0xbb; 8]);
            bytes
        };
        actor.queue_reciprocal(1, &payload(0));
        actor.start_reciprocal();
        let held = store.lock().await;
        let pending = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(poll_job(&mut actor.reciprocal, cx).is_pending())
        })
        .await;
        for piece in 1..=65 {
            actor.queue_reciprocal(1, &payload(piece));
        }
        actor.queue_reciprocal(2, &payload(90));
        let bounded =
            actor.reciprocal.queues.waiting[&1].len() == 64 && actor.reciprocal.job.is_some();
        actor.queue_reciprocal(1, &[0; 9]);
        actor.queue_reciprocal(999, &payload(99));
        drop(held);
        let mut observed = Vec::new();
        for turn in 0..67 {
            actor.start_reciprocal();
            let (id, reply) = tokio::time::timeout(
                Duration::from_secs(5),
                std::future::poll_fn(|cx| poll_job(&mut actor.reciprocal, cx)),
            )
            .await
            .unwrap();
            actor.finish_reciprocal(id, reply).await;
            let command = tokio::time::timeout(Duration::from_secs(5), async {
                if id == 1 {
                    commands1.recv().await
                } else {
                    commands2.recv().await
                }
            })
            .await
            .unwrap();
            observed.push((id, command));
            if turn == 2 {
                actor.queue_reciprocal(1, &payload(65));
            }
        }
        let empty = actor.reciprocal.queues.waiting.is_empty();
        // Same socket address, new ID: an already prepared old completion cannot
        // use the replacement channel or change upload counters.
        actor.queue_reciprocal(1, &payload(0));
        actor.start_reciprocal();
        let (old, reply) = std::future::poll_fn(|cx| poll_job(&mut actor.reciprocal, cx)).await;
        let before = uploaded.load(Ordering::Relaxed);
        actor.queue_reciprocal(1, &payload(1));
        let old_peer = actor.peers.remove(&1).unwrap();
        let (mut replacement, mut replacement_rx, worker3) = runtime(3);
        replacement.addr = old_peer.addr;
        drop(old_peer);
        actor.peers.insert(3, replacement);
        for id in 4..1000 {
            actor
                .reciprocal
                .queues
                .push(id, Request { piece: 0, chunk: 0 });
        }
        actor.prune_reciprocal();
        let pruned = actor.reciprocal.queues.waiting.is_empty();
        actor.finish_reciprocal(old, reply).await;
        let stale_silent =
            replacement_rx.try_recv().is_err() && uploaded.load(Ordering::Relaxed) == before;
        // Exercise run(), rather than manually completing, for a backlog of
        // immediately ready memory reads with no new incoming events.
        for piece in 70..75 {
            actor.queue_reciprocal(3, &payload(piece));
        }
        // Exercise the finite closed-peer prefix using the production terminal
        // seam: mark peer2 closed and populate its queued event prefix.
        actor
            .closed_peer_events
            .insert(2, (actor.peers[&2].addr, 128));
        for _ in 0..100 {
            actor
                .event_tx
                .try_send(PeerEvent::Message {
                    peer_id: 3,
                    addr: actor.peers[&3].addr,
                    msg: PeerMessage::KeepAlive,
                })
                .unwrap();
        }
        let trace = Arc::new(std::sync::Mutex::new(Vec::new()));
        actor.reciprocal.trace = Some(trace.clone());
        let events = actor.event_tx.clone();
        let addr = actor.peers[&3].addr;
        let started = tokio::time::Instant::now();
        let mut run = Box::pin(actor.run());
        let mut ready = Vec::new();
        let drained = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    _ = &mut run => break false,
                    command = replacement_rx.recv() => {
                        if let Some(PeerCommand::Send(PeerMessage::Piece { begin, .. })) = command {
                            ready.push(begin);
                            if ready.len() == 5 { break true; }
                        }
                    }
                }
            }
        })
        .await
        .unwrap_or(false);
        let no_timer = tokio::time::Instant::now() == started;
        // Hold the real store mutex until run has polled its persistent job
        // Pending in select, then release it without advancing paused time.
        let held = store.lock().await;
        let (registration, waiting) = crate::store::reciprocal_lock_witness::register(&store);
        events
            .send(PeerEvent::Message {
                peer_id: 3,
                addr,
                msg: PeerMessage::Unknown {
                    id: 6,
                    payload: payload(75),
                },
            })
            .await
            .unwrap();
        let pending_select = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = &mut run => false,
                _ = waiting.notified() => true,
            }
        })
        .await
        .unwrap_or(false);
        // Poll once more with the lock held: the notification above can arise
        // from the initial poll; this second poll reaches the actual select.
        std::future::poll_fn(|cx| {
            let _ = run.as_mut().poll(cx);
            std::task::Poll::Ready(())
        })
        .await;
        drop(held);
        drop(registration);
        let select_reply = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = &mut run => None,
                command = replacement_rx.recv() => command,
            }
        })
        .await
        .ok()
        .flatten();
        events
            .send(PeerEvent::Message {
                peer_id: 3,
                addr,
                msg: PeerMessage::Interested,
            })
            .await
            .unwrap();
        let healthy_control = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = &mut run => false,
                command = replacement_rx.recv() => matches!(command, Some(PeerCommand::Send(PeerMessage::Unchoke))),
            }
        }).await.unwrap_or(false);
        output.close();
        let exited = tokio::time::timeout(Duration::from_secs(5), &mut run)
            .await
            .is_ok();
        // run owns peers until its future is dropped; abort handles witness reap.
        drop(run);

        let reaped = tokio::time::timeout(Duration::from_secs(5), async {
            let _ = worker1.await;
            let _ = worker2.await;
            let _ = worker3.await;
        })
        .await
        .is_ok();
        assert!(
            pending
                && bounded
                && empty
                && pruned
                && stale_silent
                && drained
                && exited
                && reaped
                && no_timer
        );
        assert!(pending_select && healthy_control);
        assert!(
            trace.lock().unwrap().iter().any(|(_, event)| *event == 3),
            "actual select Pending poll"
        );
        assert!(matches!(
            select_reply,
            Some(PeerCommand::Send(PeerMessage::Piece { begin: 75, .. }))
        ));
        let trace = trace.lock().unwrap();
        assert!(trace.len() < 4096, "trace must not truncate");
        assert!(
            trace.iter().any(|(_, event)| *event == 1),
            "initial completion"
        );
        assert!(
            trace.iter().any(|(_, event)| *event == 2),
            "select completion"
        );
        let mut turns = BTreeMap::<u64, (usize, usize)>::new();
        for &(turn, event) in trace.iter() {
            let counts = turns.entry(turn).or_default();
            if event == 0 {
                counts.0 += 1;
            } else if event <= 2 {
                counts.1 += 1;
            }
        }
        assert!(turns
            .values()
            .all(|&(starts, completions)| starts <= 1 && completions <= 1));
        assert_eq!(ready, vec![70, 71, 72, 73, 74]);
        assert_eq!(uploaded.load(Ordering::Relaxed), 67 * 2 + 6 * 2);
        let expected = std::iter::once((1, 0))
            .chain(std::iter::once((2, 90)))
            .chain((1..=64).map(|p| (1, p)))
            .collect::<Vec<_>>();
        // Retry was appended after one FIFO slot became free.
        let mut expected = expected;
        expected.push((1, 65));

        assert_eq!(observed.len(), 67);
        for ((id, command), (expected_id, piece)) in observed.into_iter().zip(expected) {
            assert_eq!(id, expected_id);
            let Some(PeerCommand::Send(PeerMessage::Piece {
                index,
                begin,
                block,
            })) = command
            else {
                panic!("piece command")
            };
            assert_eq!(
                (index, begin, block),
                (0, piece, [vec![0; 10], vec![piece as u8, 0xaa]].concat())
            );
        }
    }

    fn runtime(
        id: u16,
    ) -> (
        PeerRuntime,
        mpsc::Receiver<PeerCommand>,
        tokio::task::JoinHandle<()>,
    ) {
        let (commands, rx) = mpsc::channel(64);
        let (done, finished) = tokio::sync::oneshot::channel();
        struct Finished(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for Finished {
            fn drop(&mut self) {
                if let Some(done) = self.0.take() {
                    let _ = done.send(());
                }
            }
        }
        let guard = Finished(Some(done));
        let runtime_worker = tokio::spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        let witness = tokio::spawn(async move {
            let _ = finished.await;
        });
        (
            PeerRuntime {
                addr: SocketAddrV4::new([127, 0, 0, 1].into(), id),
                min_piece: 0,
                max_piece: 0,
                unchoked_peer: false,
                produced_output: false,
                seen_ids: HashSet::new(),
                commands,
                worker: runtime_worker,
            },
            rx,
            witness,
        )
    }

    #[test]
    fn bounded_fifo_round_robin_and_retry() {
        let mut queues = Queues::default();
        for piece in 0..65 {
            queues.push(1, Request { piece, chunk: 0 });
        }
        queues.push(
            2,
            Request {
                piece: 99,
                chunk: 3,
            },
        );
        assert_eq!(queues.pop(), Some((1, Request { piece: 0, chunk: 0 })));
        assert_eq!(
            queues.pop(),
            Some((
                2,
                Request {
                    piece: 99,
                    chunk: 3
                }
            ))
        );
        queues.push(
            1,
            Request {
                piece: 100,
                chunk: 4,
            },
        );
        for piece in 1..64 {
            assert_eq!(queues.pop(), Some((1, Request { piece, chunk: 0 })));
        }
        assert_eq!(
            queues.pop(),
            Some((
                1,
                Request {
                    piece: 100,
                    chunk: 4
                }
            ))
        );
        assert_eq!(queues.pop(), None);
    }
    #[test]
    fn replacement_prunes_all_generations_without_tombstones() {
        let mut queues = Queues::default();
        for id in 1..1000 {
            queues.push(id, Request { piece: 1, chunk: 0 });
            queues.retain_current(|current| current == id);
            assert_eq!(queues.waiting.len(), 1);
        }
        assert_eq!(queues.pop().map(|(id, _)| id), Some(999));
        assert!(queues.waiting.is_empty());
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Request {
    piece: u32,
    chunk: u16,
}

#[derive(Default)]
struct Queues {
    waiting: BTreeMap<u64, VecDeque<Request>>,
    cursor: u64,
}
impl Queues {
    fn push(&mut self, peer: u64, request: Request) {
        let queue = self.waiting.entry(peer).or_default();
        if queue.len() < 64 {
            queue.push_back(request);
        }
    }
    fn retain_current(&mut self, mut current: impl FnMut(u64) -> bool) {
        self.waiting.retain(|id, _| current(*id));
    }
    fn pop(&mut self) -> Option<(u64, Request)> {
        let id = self
            .waiting
            .range((
                std::ops::Bound::Excluded(self.cursor),
                std::ops::Bound::Unbounded,
            ))
            .next()
            .or_else(|| self.waiting.iter().next())
            .map(|(&id, _)| id)?;
        let queue = self.waiting.get_mut(&id)?;
        let request = queue.pop_front()?;
        if queue.is_empty() {
            self.waiting.remove(&id);
        }
        self.cursor = id;
        Some((id, request))
    }
}
type Job = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = (u64, Option<crate::seed::protocol::PreparedReply>)>
            + Send,
    >,
>;
#[cfg(test)]
type TurnTrace = std::sync::Arc<std::sync::Mutex<Vec<(u64, u8)>>>;
#[derive(Default)]
pub(in crate::live_session::pool) struct Reciprocal {
    queues: Queues,
    pub(super) job: Option<Job>,
    #[cfg(test)]
    trace: Option<TurnTrace>,
    #[cfg(test)]
    pub(super) turn: u64,
}
#[cfg(test)]
impl Reciprocal {
    pub(super) fn observe(&self, event: u8) {
        if let Some(trace) = &self.trace {
            let mut trace = trace.lock().unwrap();
            if trace.len() < 4096 {
                trace.push((self.turn, event));
            }
        }
    }
}
impl PoolActor<'_> {
    pub(super) fn prune_reciprocal(&mut self) {
        self.reciprocal
            .queues
            .retain_current(|id| self.peers.contains_key(&id));
    }
    pub(super) fn start_reciprocal(&mut self) {
        self.prune_reciprocal();
        if self.reciprocal.job.is_some() {
            return;
        }
        let Some((id, request)) = self.reciprocal.queues.pop() else {
            return;
        };
        #[cfg(test)]
        self.reciprocal.observe(0);
        let store = Arc::clone(self.store);
        self.reciprocal.job = Some(Box::pin(async move {
            let mut payload = Vec::with_capacity(10);
            payload.extend_from_slice(&0u32.to_be_bytes());
            payload.extend_from_slice(&request.piece.to_be_bytes());
            payload.extend_from_slice(&request.chunk.to_be_bytes());
            let reply = prepare_reply(
                &PeerMessage::Unknown { id: 6, payload },
                Some(&store),
                ServeProfile::Reciprocal,
            )
            .await;
            (id, reply)
        }));
    }
    pub(super) fn queue_reciprocal(&mut self, id: u64, payload: &[u8]) {
        self.prune_reciprocal();
        if payload.len() >= 10 && self.peers.contains_key(&id) {
            self.reciprocal.queues.push(
                id,
                Request {
                    piece: u32::from_be_bytes(payload[4..8].try_into().unwrap()),
                    chunk: u16::from_be_bytes(payload[8..10].try_into().unwrap()),
                },
            );
        }
    }
    pub(super) async fn finish_reciprocal(
        &mut self,
        id: u64,
        reply: Option<crate::seed::protocol::PreparedReply>,
    ) {
        if self.tx.is_closed() || !self.peers.contains_key(&id) {
            return;
        }
        if let Some(reply) = reply {
            if send_peer_command(id, reply.message, &mut self.peers, self.continuity)
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
}
pub(super) fn poll_job(
    state: &mut Reciprocal,
    cx: &mut std::task::Context<'_>,
) -> std::task::Poll<(u64, Option<crate::seed::protocol::PreparedReply>)> {
    let Some(job) = state.job.as_mut() else {
        return std::task::Poll::Pending;
    };
    match job.as_mut().poll(cx) {
        std::task::Poll::Ready(result) => {
            state.job = None;
            std::task::Poll::Ready(result)
        }
        std::task::Poll::Pending => std::task::Poll::Pending,
    }
}
