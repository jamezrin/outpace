use super::*;
use std::sync::atomic::AtomicBool;
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

#[tokio::test]
async fn activation_obsolete_completion_preserves_current_peers_and_counters() {
    for case in 0..12 {
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
        let mut policy = LiveRecoveryConfig::default();
        if matches!(case, 3 | 4 | 11) {
            policy.max_active_upstreams = 2;
        }
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
        let (peer1, _commands1, _worker1) = runtime(1);
        let (peer2, _commands2, _worker2) = runtime(2);
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
            output_reservation: None,
            activation: None,
            reciprocal: Default::default(),
            publication: None,
            gossip: None,
            closed_peer_events: BTreeMap::new(),
            observed_retry: Instant::now(),
            observed_capacity: (2, 0),
            refill_closed: true,
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

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = match listener.local_addr().unwrap() {
            std::net::SocketAddr::V4(addr) => addr,
            _ => unreachable!(),
        };
        let socket = TcpStream::connect(addr).await.unwrap();
        let (mut remote, _) = listener.accept().await.unwrap();
        let window = WireLivePosition {
            min_piece: 0,
            max_piece: 10,
            position: 0,
            distance_from_source: 1,
        };
        actor.continuity.head = 10;
        actor.continuity.startup_deadline = Some(Instant::now() + Duration::from_secs(1));
        // Fill the real candidate's TCP send path, then poll the same persistent
        // actor-owned write future before changing any admission state.
        let padding = [0x5a; 65536];
        let mut written = 0;
        let deadline = Instant::now() + Duration::from_secs(2);
        while written < 32 * 1024 * 1024 && Instant::now() < deadline {
            match socket.try_write(&padding) {
                Ok(n) => written += n,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(error) => panic!("prefill: {error}"),
            }
        }
        let target = if matches!(case, 3 | 4 | 6 | 9 | 10 | 11) {
            Some(1)
        } else {
            None
        };
        actor.activation = Some(activation::Activation {
            addr,
            peer_id: 3,
            replacement: target,
            write: activation::prepare(
                3,
                ConnectedUpstream {
                    session: PeerSession::new(socket),
                    addr,
                    window,
                    unchoked: false,
                    yourip: None,
                },
                0,
                10,
                actor.identity,
                None,
            ),
        });
        let pending =
            std::future::poll_fn(|cx| std::task::Poll::Ready(actor.poll_activation(cx).is_none()))
                .await;
        assert!(
            pending,
            "real activation write must be Pending before state changes"
        );
        let cohort_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cohort_addr = match cohort_listener.local_addr().unwrap() {
            std::net::SocketAddr::V4(addr) => addr,
            _ => unreachable!(),
        };
        let replacement = match case {
            0 => {
                actor.continuity.head = 20;
                actor.continuity.reasm.skip_to(12);
                None
            } // cursor no longer covered
            1 => {
                actor.peers.get_mut(&1).unwrap().addr = addr;
                None
            }
            2 => {
                actor.live_recovery.max_active_upstreams = 2;
                None
            }
            3 => {
                let old = actor.peers.remove(&1).unwrap();
                actor.peers.insert(9, old);
                Some(1)
            } // removed exact generation
            4 => {
                actor.continuity.startup_deadline = None;
                Some(1)
            }
            5 => {
                actor.candidates.learn(addr, CandidateKind::Discovered);
                actor.candidates.explored(addr);
                actor
                    .candidates
                    .learn(cohort_addr, CandidateKind::Discovered);
                None
            }
            6 => {
                actor.live_recovery.max_active_upstreams = 1;
                Some(1)
            }
            7 => {
                output.close();
                None
            }
            9 => {
                actor.live_recovery.max_active_upstreams = 2;
                actor.peers.get_mut(&1).unwrap().max_piece = 10;
                Some(1)
            }
            10 => {
                actor.live_recovery.max_active_upstreams = 3;
                Some(1)
            }
            11 => Some(1), // unchanged-state successful replacement control
            8 => None,
            _ => unreachable!(),
        };
        if matches!(case, 3 | 4 | 11) {
            assert_eq!(actor.peers.len(), actor.live_recovery.max_active_upstreams);
            assert!(actor.continuity.window_can_resume(&window));
            assert!(actor.peers.values().all(|peer| peer.addr != addr));
            assert!(actor.peers[&2].max_piece < 10);
            if case != 3 {
                assert!(actor.peers[&1].max_piece < 10);
            } else {
                assert_eq!(actor.peers[&9].addr.port(), 1);
            }
            assert_eq!(actor.continuity.startup_deadline.is_some(), case != 4);
        }
        let ids: Vec<_> = actor.peers.keys().copied().collect();
        if matches!(case, 3 | 4 | 11) {
            actor.continuity.admitted_upstreams = 2;
            count.store(2, Ordering::Relaxed);
        }
        let admissions = actor.continuity.admitted_upstreams;
        assert_eq!(actor.activation.as_ref().unwrap().replacement, replacement);
        if case == 0 {
            actor.candidates.learn(addr, CandidateKind::Discovered);
            actor.live_recovery.max_parallel_connect = 1;
            actor.maybe_refill(true);
            assert!(actor.learned_pending.is_empty());
            assert!(actor.learned_connects.is_empty());
            actor.live_recovery.max_parallel_connect = 2;
            actor.maybe_refill(true);
            assert!(
                actor.learned_pending.is_empty(),
                "held address must not reconnect"
            );
            assert!(actor.learned_connects.is_empty());
        }
        use tokio::io::AsyncReadExt;
        let mut remaining = written;
        let mut scratch = [0; 65536];
        tokio::time::timeout(Duration::from_secs(5), async {
            while remaining > 0 {
                let n = remaining.min(scratch.len());
                remote.read_exact(&mut scratch[..n]).await.unwrap();
                remaining -= n;
            }
        })
        .await
        .unwrap();
        if case == 8 {
            struct CompletionWitness {
                before_prefix: AtomicBool,
                count: Arc<AtomicU32>,
            }
            impl LiveObserver for CompletionWitness {
                fn active_upstreams(&self, admitted: u32) {
                    if admitted == 1 && self.count.load(Ordering::Relaxed) == 2 {
                        self.before_prefix.store(true, Ordering::Relaxed);
                    }
                }
                fn received_bytes(&self, _: u64) {}
                fn publish_position(&self, _: Option<LivePosition>, _: u64) {}
                fn clear_active_coverage(&self) {}
            }
            count.store(2, Ordering::Relaxed);
            let witness = Arc::new(CompletionWitness {
                before_prefix: AtomicBool::new(false),
                count: count.clone(),
            });
            actor.continuity.observer = Some(witness.clone());
            // Seed the bounded final-event prefix of an already closed worker.
            // A ready activation must be serviced before this prefix can retire
            // the provisional peer and change the observed pool capacity.
            actor.peers[&1].worker.abort();
            tokio::task::yield_now().await;
            let old_addr = actor.peers[&1].addr;
            for _ in 0..128 {
                actor
                    .event_tx
                    .try_send(PeerEvent::Message {
                        peer_id: 1,
                        addr: old_addr,
                        msg: PeerMessage::Choke,
                    })
                    .unwrap();
            }
            actor.closed_peer_events.insert(1, (old_addr, 128));
            let (end, progress) = tokio::join!(actor.run(), async {
                let progressed = tokio::time::timeout(Duration::from_secs(2), async {
                    while !witness.before_prefix.load(Ordering::Relaxed) {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .is_ok();
                output.close();
                let mut bytes = Vec::new();
                let eof =
                    tokio::time::timeout(Duration::from_secs(2), remote.read_to_end(&mut bytes))
                        .await
                        .is_ok_and(|result| result.is_ok());
                (progressed, eof)
            });
            _worker1.await.unwrap();
            _worker2.await.unwrap();
            assert!(matches!(end, FollowEnd::ConsumerGone));
            assert!(
                progress.0 && progress.1,
                "activation serviced ahead of closed prefix; natural cancellation EOF"
            );
            continue;
        }
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            std::future::poll_fn(|cx| actor.activation.as_mut().unwrap().write.as_mut().poll(cx)),
        )
        .await
        .unwrap();
        actor.finish_activation(result).await;
        if case == 11 {
            assert_eq!(actor.peers.keys().copied().collect::<Vec<_>>(), [2, 3]);
            assert_eq!(actor.continuity.admitted_upstreams, admissions);
            assert_eq!(actor.loss_count, 0);
            assert_eq!(count.load(Ordering::Relaxed), 2);
            assert!(actor.continuity.startup_deadline.is_none());
            assert!(actor.activation.is_none());
            drop(actor);
            let mut activation_bytes = Vec::new();
            tokio::time::timeout(
                Duration::from_secs(1),
                remote.read_to_end(&mut activation_bytes),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(!activation_bytes.is_empty());
            _worker1.await.unwrap();
            _worker2.await.unwrap();
            continue;
        }
        if case == 9 {
            assert!(
                actor.continuity.startup_deadline.is_none(),
                "matching evidence releases hold while retaining the provisional"
            );
        }
        if case == 0 {
            assert!(
                actor.learned_pending.contains(&addr),
                "completion releases its transport slot to eligible refill work"
            );
        }
        assert!(actor.activation.is_none());
        assert_eq!(
            actor.peers.keys().copied().collect::<Vec<_>>(),
            ids,
            "case {case}"
        );
        assert_eq!(actor.continuity.admitted_upstreams, admissions);
        assert_eq!(actor.loss_count, 0);
        let mut activation_bytes = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(1),
            remote.read_to_end(&mut activation_bytes),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            !activation_bytes.is_empty(),
            "completed write before rejected commit"
        );
        drop(actor);
        _worker1.await.unwrap();
        _worker2.await.unwrap();
    }
}
