use super::*;

struct PendingWriteIo<S> {
    inner: S,
    pending_writes: Arc<AtomicU64>,
    written_bytes: Arc<AtomicU64>,
}

impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for PendingWriteIo<S> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<S: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for PendingWriteIo<S> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let result = std::pin::Pin::new(&mut this.inner).poll_write(cx, buf);
        if result.is_pending() {
            this.pending_writes.fetch_add(1, Ordering::Relaxed);
        } else if let std::task::Poll::Ready(Ok(n)) = result {
            this.written_bytes.fetch_add(n as u64, Ordering::Relaxed);
        }
        result
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

fn test_peer_runtime(
    addr: SocketAddrV4,
    min_piece: u64,
    max_piece: u64,
    unchoked_peer: bool,
    produced_output: bool,
    commands: mpsc::Sender<PeerCommand>,
    worker: tokio::task::JoinHandle<()>,
) -> PeerRuntime {
    PeerRuntime {
        addr,
        min_piece,
        max_piece,
        unchoked_peer,
        produced_output,
        seen_ids: HashSet::new(),
        commands,
        worker,
    }
}

#[tokio::test(start_paused = true)]
async fn saturated_peer_enqueue_does_not_stall_the_live_pool_actor() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let policy = default_live_recovery();
    let (event_capacity, refill_capacity) = recovery_channel_capacities(policy).unwrap();
    assert_eq!((event_capacity, refill_capacity), (128, 4));

    let (events, event_rx) = mpsc::channel(event_capacity);
    let (a_client, mut a_socket) = tokio::io::duplex(8);
    let pending_writes = Arc::new(AtomicU64::new(0));
    let a_written_bytes = Arc::new(AtomicU64::new(0));
    let a_session = PeerSession::new(PendingWriteIo {
        inner: a_client,
        pending_writes: Arc::clone(&pending_writes),
        written_bytes: a_written_bytes,
    });
    let a_addr = "127.0.0.1:8621".parse().unwrap();
    let (a_commands, a_receiver) = mpsc::channel(64);
    let a_worker = tokio::spawn(peer_worker(
        1,
        a_addr,
        a_session,
        a_receiver,
        events.clone(),
        Arc::new(AtomicU64::new(1)),
    ));
    let (b_client, mut b_socket) = tokio::io::duplex(4096);
    let b_written_bytes = Arc::new(AtomicU64::new(0));
    let b_addr = "127.0.0.1:8622".parse().unwrap();
    let (b_commands, b_receiver) = mpsc::channel(64);
    let b_worker = tokio::spawn(peer_worker(
        2,
        b_addr,
        PeerSession::new(PendingWriteIo {
            inner: b_client,
            pending_writes: Arc::new(AtomicU64::new(0)),
            written_bytes: Arc::clone(&b_written_bytes),
        }),
        b_receiver,
        events,
        Arc::new(AtomicU64::new(1)),
    ));

    let mut peers = BTreeMap::from([
        (
            1,
            PeerRuntime {
                addr: a_addr,
                min_piece: 1,
                max_piece: 65,
                unchoked_peer: false,
                produced_output: false,
                seen_ids: HashSet::new(),
                commands: a_commands.clone(),
                worker: a_worker,
            },
        ),
        (
            2,
            PeerRuntime {
                addr: b_addr,
                min_piece: 67,
                max_piece: 67,
                unchoked_peer: false,
                produced_output: false,
                seen_ids: HashSet::new(),
                commands: b_commands.clone(),
                worker: b_worker,
            },
        ),
    ]);

    let (mut continuity, _) = Continuity::fresh(&info(), 1, 1, 256, policy);
    continuity.head = 1;
    continuity.register_active_peer(1, a_addr, live_pos(1, 65));
    continuity.set_peer_unchoked(1, true);
    let initial_lost = advance_pool_requests(&mut peers, &mut continuity, 1).await;
    let mut write_pending_seen = false;
    for _ in 0..128 {
        if pending_writes.load(Ordering::Relaxed) > 0 {
            write_pending_seen = true;
            break;
        }
        tokio::task::yield_now().await;
    }

    continuity.head = 65;
    continuity.update_peer_window(1, 1, 65);
    let backlog_lost = advance_pool_requests(&mut peers, &mut continuity, 1).await;
    let backlog_capacity = a_commands.capacity();
    let backlog_owned = continuity.active_peers.in_flight_count(1);
    let backlog_timers = (
        continuity.requested_at.get(&1).copied(),
        continuity.requested_at.get(&65).copied(),
    );

    let keep_alive = PeerMessage::KeepAlive.encode();
    let frames = keep_alive.repeat(event_capacity);
    b_socket.write_all(&frames).await.unwrap();
    for _ in 0..512 {
        if event_rx.len() == event_capacity {
            break;
        }
        tokio::task::yield_now().await;
    }
    let events_full = event_rx.len() == event_capacity;

    continuity.register_active_peer(2, b_addr, live_pos(67, 67));
    continuity.set_peer_unchoked(2, true);
    continuity.head = 67;
    continuity.update_peer_window(1, 1, 66);
    let mut actor = Box::pin(advance_pool_requests(&mut peers, &mut continuity, 1));
    let mut actor_completed = false;
    let mut actor_lost = Vec::new();
    for _ in 0..128 {
        tokio::select! {
            biased;
            result = &mut actor => {
                actor_completed = true;
                actor_lost = result;
                break;
            }
            _ = tokio::task::yield_now() => {}
        }
    }
    for _ in 0..128 {
        if b_written_bytes.load(Ordering::Relaxed) > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    let b_progress_before_cleanup = b_written_bytes.load(Ordering::Relaxed) > 0;
    if !actor_completed {
        tokio::time::advance(Duration::from_secs(21)).await;
        for _ in 0..128 {
            tokio::select! {
                biased;
                result = &mut actor => {
                    actor_completed = true;
                    actor_lost = result;
                    break;
                }
                _ = tokio::task::yield_now() => {}
            }
        }
    }
    let event_queue_still_full = event_rx.len() == event_capacity;
    let final_b_progress = b_written_bytes.load(Ordering::Relaxed) > 0;

    drop(actor);
    let request_ownership = (
        continuity.active_peers.in_flight_count(1),
        continuity.active_peers.in_flight_count(2),
        continuity.requested_at.contains_key(&66),
        continuity.requested_at.contains_key(&67),
    );
    let retained_backlog_timers = (
        continuity.requested_at.get(&1).copied(),
        continuity.requested_at.get(&65).copied(),
    );
    let worker_still_owns_receiver = !a_commands.is_closed();
    let worker_still_running = peers.get(&1).is_some_and(|peer| !peer.worker.is_finished());
    let a_worker_abort = peers.get(&1).unwrap().worker.abort_handle();
    let b_worker_abort = peers.get(&2).unwrap().worker.abort_handle();
    shutdown_peer_runtimes(&mut peers);
    let workers_reaped = tokio::time::timeout(Duration::from_secs(1), async {
        while !a_worker_abort.is_finished() || !b_worker_abort.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let a_receiver_closed = tokio::time::timeout(Duration::from_secs(1), a_commands.closed())
        .await
        .is_ok();
    let b_receiver_closed = tokio::time::timeout(Duration::from_secs(1), b_commands.closed())
        .await
        .is_ok();
    let mut a_remaining = Vec::new();
    let a_io_closed = tokio::time::timeout(
        Duration::from_secs(1),
        a_socket.read_to_end(&mut a_remaining),
    )
    .await
    .is_ok_and(|result| result.is_ok());
    let mut b_remaining = Vec::new();
    let b_io_closed = tokio::time::timeout(
        Duration::from_secs(1),
        b_socket.read_to_end(&mut b_remaining),
    )
    .await
    .is_ok_and(|result| result.is_ok());

    assert!(initial_lost.is_empty() && backlog_lost.is_empty());
    assert!(
        write_pending_seen,
        "A's real request write never reached Pending"
    );
    assert_eq!(backlog_capacity, 0, "A's 64-command channel was not full");
    assert_eq!(backlog_owned, 65, "A did not own pieces 1 through 65");
    assert!(events_full, "B did not fill the real 128-event channel");
    assert!(
        worker_still_running || actor_completed,
        "A worker exited before actor admission returned"
    );
    assert!(
        worker_still_owns_receiver || actor_completed,
        "A worker dropped its command receiver before actor admission returned"
    );
    assert!(
        event_queue_still_full,
        "the actor did not retain its full event receiver while enqueueing"
    );
    assert!(workers_reaped, "peer workers did not terminate after abort");
    assert!(
        a_receiver_closed && b_receiver_closed,
        "peer command receivers stayed open"
    );
    assert!(
        a_io_closed && b_io_closed,
        "peer I/O did not terminate after runtime shutdown"
    );
    assert!(
        actor_completed
            && actor_lost.is_empty()
            && request_ownership == (65, 1, false, true)
            && retained_backlog_timers == backlog_timers
            && final_b_progress,
        "credit contract failed: actor_completed={actor_completed}, lost={actor_lost:?}, \
         ownership={request_ownership:?}, timers_preserved={}, B_progress={}, \
         A write-pending polls={}, command capacity={}, events={}/{}, \
         A worker running={}, receiver open={}",
        retained_backlog_timers == backlog_timers,
        final_b_progress || b_progress_before_cleanup,
        pending_writes.load(Ordering::Relaxed),
        backlog_capacity,
        event_rx.len(),
        event_capacity,
        worker_still_running,
        worker_still_owns_receiver
    );
}

#[tokio::test(start_paused = true)]
async fn queue_credit_admits_a_default_256_piece_burst_across_worker_turns() {
    use std::future::Future;
    use tokio::io::AsyncReadExt;

    let policy = default_live_recovery();
    let (event_capacity, refill_capacity) = recovery_channel_capacities(policy).unwrap();
    let (events, _event_rx) = mpsc::channel(event_capacity);
    let mut stream = info();
    stream.piece_length = 4;
    stream.chunk_length = 4;
    let chunks_per_piece = stream.chunks_per_piece();
    let (mut continuity, _) = Continuity::fresh(&stream, 1, 256, 256, policy);
    let (client, mut server) = tokio::io::duplex(4096);
    let addr = "127.0.0.1:8651".parse().unwrap();
    let (commands, command_rx) = mpsc::channel(64);
    let worker = tokio::spawn(peer_worker(
        1,
        addr,
        PeerSession::new(client),
        command_rx,
        events,
        Arc::clone(&continuity.request_floor),
    ));
    let mut peers = BTreeMap::from([(
        1,
        PeerRuntime {
            addr,
            min_piece: 1,
            max_piece: 256,
            unchoked_peer: false,
            produced_output: false,
            seen_ids: HashSet::new(),
            commands: commands.clone(),
            worker,
        },
    )]);
    continuity.head = 256;
    continuity.register_active_peer(1, addr, live_pos(1, 256));
    continuity.set_peer_unchoked(1, true);

    let mut advance = Box::pin(advance_pool_requests(
        &mut peers,
        &mut continuity,
        chunks_per_piece,
    ));
    let mut first_losses = None;
    let first_turn_ready = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(match advance.as_mut().poll(cx) {
            std::task::Poll::Ready(losses) => {
                first_losses = Some(losses);
                true
            }
            std::task::Poll::Pending => false,
        })
    })
    .await;
    drop(advance);

    let first_capacity = commands.capacity();
    let max_capacity = commands.max_capacity();
    let first_peer_owned = continuity.active_peers.in_flight_count(1);
    let first_scheduler_owned = continuity.scheduler.in_flight_count();
    let first_timers = continuity.requested_at.len();
    let deferred_owned = (65..=256)
        .filter(|piece| continuity.requested_at.contains_key(piece))
        .count();
    let retained_timers: Vec<_> = (1..=64)
        .map(|piece| continuity.requested_at.get(&piece).copied())
        .collect();
    let mut all_losses = first_losses.clone().unwrap_or_default();
    let mut observed_pieces = Vec::new();
    let mut request_frames_valid = true;
    let mut timers_stable = true;
    let mut admission_trace = Vec::new();

    if first_turn_ready && all_losses.is_empty() {
        let mut first = 1u64;
        while first <= 256 {
            let last = (first + 63).min(256);
            for expected_piece in first..=last {
                let mut frame = [0u8; 15];
                match tokio::time::timeout(Duration::from_secs(1), server.read_exact(&mut frame))
                    .await
                {
                    Ok(Ok(_)) => match PeerMessage::decode(&frame) {
                        Ok(Some((PeerMessage::Unknown { id: 6, payload }, used)))
                            if used == frame.len() && payload.len() == 10 =>
                        {
                            let observed_piece =
                                u32::from_be_bytes(payload[4..8].try_into().unwrap()) as u64;
                            request_frames_valid &= observed_piece == expected_piece;
                            observed_pieces.push(observed_piece);
                        }
                        _ => {
                            request_frames_valid = false;
                            break;
                        }
                    },
                    _ => {
                        request_frames_valid = false;
                        break;
                    }
                }
            }
            if !request_frames_valid || last == 256 {
                break;
            }

            let mut worker_fully_drained = commands.capacity() == max_capacity;
            for _ in 0..1024 {
                if worker_fully_drained {
                    break;
                }
                tokio::task::yield_now().await;
                worker_fully_drained = commands.capacity() == max_capacity;
            }
            request_frames_valid &= worker_fully_drained;
            if !worker_fully_drained {
                break;
            }

            let prior_timers: Vec<_> = (1..=last)
                .map(|piece| continuity.requested_at.get(&piece).copied())
                .collect();
            let before_owned = continuity.scheduler.in_flight_count();
            let slots_before_admission = commands.capacity();
            let losses = advance_pool_requests(&mut peers, &mut continuity, chunks_per_piece).await;
            all_losses.extend(losses);
            admission_trace.push((
                before_owned,
                continuity.scheduler.in_flight_count(),
                slots_before_admission,
            ));
            timers_stable &= (1..=last)
                .map(|piece| continuity.requested_at.get(&piece).copied())
                .collect::<Vec<_>>()
                == prior_timers;
            first = last + 1;
        }
    }

    let fifo_unique = observed_pieces == (1..=256).collect::<Vec<_>>();
    let final_peer_owned = continuity.active_peers.in_flight_count(1);
    let final_scheduler_owned = continuity.scheduler.in_flight_count();
    let final_timers = continuity.requested_at.len();
    let initial_timers_preserved = retained_timers
        == (1..=64)
            .map(|piece| continuity.requested_at.get(&piece).copied())
            .collect::<Vec<_>>();
    let queue_max_capacity = commands.max_capacity();

    let worker_abort = peers.get(&1).map(|peer| peer.worker.abort_handle());
    shutdown_peer_runtimes(&mut peers);
    let worker_reaped = if let Some(worker_abort) = worker_abort {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !worker_abort.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok()
    } else {
        true
    };
    let command_receiver_closed = tokio::time::timeout(Duration::from_secs(1), commands.closed())
        .await
        .is_ok();
    let mut remaining = Vec::new();
    let io_closed =
        tokio::time::timeout(Duration::from_secs(1), server.read_to_end(&mut remaining))
            .await
            .is_ok_and(|result| result.is_ok());

    assert_eq!((event_capacity, refill_capacity), (128, 4));
    assert!(worker_reaped && command_receiver_closed && io_closed);
    assert!(
        first_turn_ready
            && first_losses.as_ref().is_some_and(Vec::is_empty)
            && first_capacity == 0
            && max_capacity == 64
            && first_peer_owned == 64
            && first_scheduler_owned == 64
            && first_timers == 64
            && deferred_owned == 0,
        "first 256-burst turn must admit exactly64 and defer192 before worker polling: \
         ready={first_turn_ready}, capacity={first_capacity}/{max_capacity}, \
         peer_owned={first_peer_owned}, scheduler_owned={first_scheduler_owned}, \
         timers={first_timers}, deferred_timers={deferred_owned}"
    );
    assert!(
        all_losses.is_empty(),
        "queue credits must retain the healthy peer"
    );
    assert!(
        request_frames_valid && timers_stable && fifo_unique,
        "worker requests must drain FIFO and once each: frame_valid={request_frames_valid}, \
         timers_stable={timers_stable}, observed={}, first={:?}, last={:?}, unique={}, \
         final_queue_capacity={}/{}, final_peer_owned={final_peer_owned}, \
         final_scheduler_owned={final_scheduler_owned}, final_timers={final_timers}, \
         admission_trace={admission_trace:?}",
        observed_pieces.len(),
        observed_pieces.first(),
        observed_pieces.last(),
        fifo_unique,
        commands.capacity(),
        max_capacity
    );
    assert_eq!(observed_pieces.len(), 256);
    assert_eq!(final_peer_owned, 256);
    assert_eq!(final_scheduler_owned, 256);
    assert_eq!(final_timers, 256);
    assert_eq!(
        queue_max_capacity, 64,
        "the production queue must stay bounded"
    );
    assert!(
        initial_timers_preserved,
        "admitted timer deadlines must not reset"
    );
}

#[tokio::test]
async fn closed_peer_waits_for_synthetic_terminal_without_clearing_other_state() {
    let policy = default_live_recovery();
    let mut stream = info();
    stream.piece_length = 4;
    stream.chunk_length = 2;
    let (mut continuity, _) = Continuity::fresh(&stream, 1, 3, 3, policy);
    let a_addr = "127.0.0.1:8652".parse().unwrap();
    let b_addr = "127.0.0.1:8653".parse().unwrap();
    let (a_commands, a_receiver) = mpsc::channel(64);
    let (b_commands, b_receiver) = mpsc::channel(64);
    let (finish_a_tx, finish_a_rx) = tokio::sync::oneshot::channel();
    let a_worker = tokio::spawn(async move {
        let _ = finish_a_rx.await;
        drop(a_receiver);
    });
    let b_worker = tokio::spawn(std::future::pending::<()>());
    let a_worker_abort = a_worker.abort_handle();
    let b_worker_abort = b_worker.abort_handle();
    let mut peers = BTreeMap::from([
        (
            1,
            PeerRuntime {
                addr: a_addr,
                min_piece: 1,
                max_piece: 1,
                unchoked_peer: false,
                produced_output: false,
                seen_ids: HashSet::new(),
                commands: a_commands.clone(),
                worker: a_worker,
            },
        ),
        (
            2,
            PeerRuntime {
                addr: b_addr,
                min_piece: 2,
                max_piece: 3,
                unchoked_peer: false,
                produced_output: false,
                seen_ids: HashSet::new(),
                commands: b_commands.clone(),
                worker: b_worker,
            },
        ),
    ]);
    continuity.head = 3;
    continuity.register_active_peer(1, a_addr, live_pos(1, 1));
    continuity.register_active_peer(2, b_addr, live_pos(2, 3));
    continuity.set_peer_unchoked(1, true);
    continuity.set_peer_unchoked(2, true);
    continuity.admitted_upstreams = 2;
    let observer = TestObserver::new(
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        Arc::new(AtomicU32::new(0)),
    );
    continuity.observer = Some(observer.clone());
    continuity.publish_upstreams();

    let initial_lost = advance_pool_requests(&mut peers, &mut continuity, 2).await;
    let old_timers: Vec<_> = (1..=3)
        .map(|piece| continuity.requested_at.get(&piece).copied())
        .collect();
    let initial_ownership = (
        continuity.active_peers.in_flight_count(1),
        continuity.active_peers.in_flight_count(2),
    );

    let partial_result = continuity.reasm.add_block(1, 0, &[1, 2]);
    continuity.received_chunks.insert(1, HashSet::from([0]));
    continuity.piece_producers.insert(1, Some(a_addr));
    let ready_first = continuity.reasm.add_block(2, 0, &[5, 6]);
    let ready_second = continuity.reasm.add_block(2, 2, &[7, 8]);
    finish_a_tx.send(()).unwrap();
    let worker_closed = tokio::time::timeout(Duration::from_secs(1), async {
        while !a_worker_abort.is_finished() || !a_commands.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    continuity.head = 4;
    update_runtime_window(&mut peers, &mut continuity, 1, 4);
    let admission_losses = advance_pool_requests(&mut peers, &mut continuity, 2).await;
    let retained_at_zero_credit = peers.contains_key(&1)
        && a_commands.is_closed()
        && continuity.active_peers.in_flight_count(1) == 1
        && !continuity.requested_at.contains_key(&4);
    let (_events_tx, mut events) = mpsc::channel::<PeerEvent>(128);
    let mut closed_prefix = BTreeMap::new();
    let terminal_event = poll_closed_peer_event(&peers, &mut events, &mut closed_prefix);
    let terminal_current = terminal_event
        .as_ref()
        .is_some_and(|event| event.is_current(&peers))
        && matches!(
            terminal_event.as_ref(),
            Some(PeerEvent::Lost {
                peer_id: 1,
                addr,
            }) if *addr == a_addr
        );
    let mut candidates = SessionCandidates::default();
    candidates.learn(a_addr, CandidateKind::Source);
    let mut loss_count = 0;
    let peer_count = AtomicU32::new(2);
    let pool_empty = if terminal_current {
        handle_pool_peer_loss(
            1,
            &mut peers,
            &mut continuity,
            2,
            &mut candidates,
            &mut loss_count,
            &peer_count,
        )
        .await
    } else {
        false
    };
    let stale_duplicate = PeerEvent::Lost {
        peer_id: 1,
        addr: a_addr,
    };
    let duplicate_current = stale_duplicate.is_current(&peers);
    let pending_after_terminal = poll_closed_peer_event(&peers, &mut events, &mut closed_prefix);

    let timers_preserved = old_timers
        == (1..=3)
            .map(|piece| continuity.requested_at.get(&piece).copied())
            .collect::<Vec<_>>();
    let loss_addresses_once = admission_losses.is_empty()
        && terminal_current
        && !duplicate_current
        && loss_count == 1
        && !candidates.eligible(Instant::now()).contains(&a_addr)
        && peer_count.load(Ordering::Relaxed) == 1
        && continuity.admitted_upstreams == 1
        && observer.snapshot().active_upstreams == 1;
    let a_partial_preserved = continuity.received_chunks.get(&1) == Some(&HashSet::from([0]))
        && continuity.piece_producers.get(&1) == Some(&Some(a_addr));
    let cursor_before_finish = continuity.reasm.next_needed();
    let complete_partial = continuity.reasm.add_block(1, 2, &[3, 4]);
    let preserved_ready = continuity.reasm.take_ready();
    let b_state = (
        continuity.active_peers.in_flight_count(2),
        (2..=3)
            .map(|piece| continuity.requested_at.get(&piece).copied())
            .collect::<Vec<_>>(),
        continuity.scheduler.in_flight_count(),
    );

    shutdown_peer_runtimes(&mut peers);
    drop(b_receiver);
    drop(events);
    let workers_reaped = tokio::time::timeout(Duration::from_secs(1), async {
        while !a_worker_abort.is_finished() || !b_worker_abort.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let receivers_closed = tokio::time::timeout(Duration::from_secs(1), async {
        a_commands.closed().await;
        b_commands.closed().await;
    })
    .await
    .is_ok();

    assert!(worker_closed);
    assert!(initial_lost.is_empty());
    assert_eq!(initial_ownership, (1, 2));
    assert!(partial_result.is_ok() && ready_first.is_ok() && ready_second.is_ok());
    assert!(
        admission_losses.is_empty() && retained_at_zero_credit,
        "closed peers keep the generation and old ownership while receiving zero new credit"
    );
    assert!(terminal_current && !pool_empty);
    assert!(pending_after_terminal.is_none());
    assert!(
        loss_addresses_once,
        "shared terminal handling accounts one loss and updates gauges"
    );
    assert!(
        a_partial_preserved,
        "peer removal must retain partial provenance"
    );
    assert_eq!(cursor_before_finish, 1);
    assert!(complete_partial.is_ok());
    assert_eq!(preserved_ready, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    assert!(
        timers_preserved,
        "old A and B deadlines must not be reset or purged"
    );
    assert_eq!(
        b_state.0, 2,
        "healthy B's active assignments must remain owned"
    );
    assert_eq!(b_state.1, old_timers[1..].to_vec());
    assert_eq!(b_state.2, 2, "only A's scheduler slot should be released");
    assert!(workers_reaped && receivers_closed);
}

#[tokio::test]
async fn missing_lost_fallback_drains_bounded_prefix_while_healthy_events_continue() {
    use tokio::io::AsyncWriteExt;

    let event_capacity = 128;
    let (events, mut event_rx) = mpsc::channel(event_capacity);
    let a_addr = "127.0.0.1:8658".parse().unwrap();
    let (a_client, mut a_server) = tokio::io::duplex(4096);
    let (a_commands, a_receiver) = mpsc::channel(64);
    let a_worker = tokio::spawn(peer_worker(
        1,
        a_addr,
        PeerSession::new(a_client),
        a_receiver,
        events.clone(),
        Arc::new(AtomicU64::new(1)),
    ));
    let b_addr = "127.0.0.1:8659".parse().unwrap();
    let (b_client, mut b_server) = tokio::io::duplex(4096);
    let (b_commands, b_receiver) = mpsc::channel(64);
    let b_worker = tokio::spawn(peer_worker(
        2,
        b_addr,
        PeerSession::new(b_client),
        b_receiver,
        events.clone(),
        Arc::new(AtomicU64::new(1)),
    ));
    let a_worker_abort = a_worker.abort_handle();
    let b_worker_abort = b_worker.abort_handle();
    let mut peers = BTreeMap::from([
        (
            1,
            PeerRuntime {
                addr: a_addr,
                min_piece: 1,
                max_piece: 2,
                unchoked_peer: false,
                produced_output: false,
                seen_ids: HashSet::new(),
                commands: a_commands.clone(),
                worker: a_worker,
            },
        ),
        (
            2,
            PeerRuntime {
                addr: b_addr,
                min_piece: 1,
                max_piece: 2,
                unchoked_peer: false,
                produced_output: false,
                seen_ids: HashSet::new(),
                commands: b_commands.clone(),
                worker: b_worker,
            },
        ),
    ]);
    let (mut continuity, _) = Continuity::fresh(&info(), 1, 2, 2, default_live_recovery());
    continuity.admitted_upstreams = 2;

    a_server
        .write_all(&PeerMessage::Have(2).encode())
        .await
        .unwrap();
    let a_message_queued = tokio::time::timeout(Duration::from_secs(1), async {
        while event_rx.len() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    a_commands.send(PeerCommand::Stop).await.unwrap();
    let a_finished_without_lost = tokio::time::timeout(Duration::from_secs(1), async {
        while !a_worker_abort.is_finished() || !a_commands.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    b_server
        .write_all(&PeerMessage::KeepAlive.encode().repeat(event_capacity * 2))
        .await
        .unwrap();
    let initially_full = tokio::time::timeout(Duration::from_secs(1), async {
        while event_rx.len() != event_capacity {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();

    let mut closed_prefix = BTreeMap::new();
    let first = poll_closed_peer_event(&peers, &mut event_rx, &mut closed_prefix);
    let first_is_a_message = first.as_ref().is_some_and(|event| event.is_current(&peers))
        && matches!(
            first.as_ref(),
            Some(PeerEvent::Message {
                peer_id: 1,
                addr,
                msg: PeerMessage::Have(2),
            }) if *addr == a_addr
        );
    let mut healthy_prefix_count = 0usize;
    let mut kept_full_between_turns = true;
    for _ in 1..event_capacity {
        let Some(event) = poll_closed_peer_event(&peers, &mut event_rx, &mut closed_prefix) else {
            kept_full_between_turns = false;
            break;
        };
        if event.is_current(&peers)
            && matches!(
                event,
                PeerEvent::Message {
                    peer_id: 2,
                    msg: PeerMessage::KeepAlive,
                    ..
                }
            )
        {
            healthy_prefix_count += 1;
        } else {
            kept_full_between_turns = false;
            break;
        }
        let refilled = tokio::time::timeout(Duration::from_secs(1), async {
            while event_rx.len() != event_capacity {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
        kept_full_between_turns &= refilled;
    }
    let queue_still_full = event_rx.len() == event_capacity;
    let terminal = poll_closed_peer_event(&peers, &mut event_rx, &mut closed_prefix);
    let terminal_is_a_lost = terminal
        .as_ref()
        .is_some_and(|event| event.is_current(&peers))
        && matches!(
            terminal.as_ref(),
            Some(PeerEvent::Lost {
                peer_id: 1,
                addr,
            }) if *addr == a_addr
        );
    let mut candidates = SessionCandidates::default();
    candidates.learn(a_addr, CandidateKind::Source);
    let mut loss_count = 0;
    let peer_count = AtomicU32::new(2);
    let pool_empty = if terminal_is_a_lost {
        handle_pool_peer_loss(
            1,
            &mut peers,
            &mut continuity,
            1,
            &mut candidates,
            &mut loss_count,
            &peer_count,
        )
        .await
    } else {
        false
    };
    let remaining_healthy = peers.contains_key(&2);
    let pending_pruned =
        poll_closed_peer_event(&peers, &mut event_rx, &mut closed_prefix).is_none();

    shutdown_peer_runtimes(&mut peers);
    drop(event_rx);
    drop(events);
    drop(a_server);
    drop(b_server);
    let workers_reaped = tokio::time::timeout(Duration::from_secs(1), async {
        while !a_worker_abort.is_finished() || !b_worker_abort.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();

    assert!(a_message_queued && a_finished_without_lost && initially_full);
    assert!(first_is_a_message);
    assert_eq!(healthy_prefix_count, event_capacity - 1);
    assert!(kept_full_between_turns && queue_still_full);
    assert!(
        terminal_is_a_lost,
        "fallback runs at the capacity prefix while B still has queued events"
    );
    assert!(!pool_empty && remaining_healthy);
    assert_eq!(loss_count, 1);
    assert!(pending_pruned && workers_reaped);
}

#[tokio::test]
async fn reserved_event_permit_blocks_fallback_and_replacement_prunes_old_generation() {
    let old_addr = "127.0.0.1:8664".parse().unwrap();
    let (old_commands, old_receiver) = mpsc::channel(64);
    let old_worker = tokio::spawn(async move { drop(old_receiver) });
    let old_worker_abort = old_worker.abort_handle();
    let mut peers = BTreeMap::from([(
        1,
        test_peer_runtime(
            old_addr,
            1,
            1,
            false,
            false,
            old_commands.clone(),
            old_worker,
        ),
    )]);
    let worker_closed = tokio::time::timeout(Duration::from_secs(1), async {
        while !old_worker_abort.is_finished() || !old_commands.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let (event_tx, mut events) = mpsc::channel::<PeerEvent>(128);
    let permit = event_tx.reserve().await.unwrap();
    let capacity_with_reserved_permit = events.capacity();
    let mut closed_prefix = BTreeMap::new();
    let no_fallback_while_reserved =
        poll_closed_peer_event(&peers, &mut events, &mut closed_prefix).is_none();
    let original_bound = closed_prefix.get(&1).copied();

    permit.send(PeerEvent::Message {
        peer_id: 1,
        addr: old_addr,
        msg: PeerMessage::Have(9),
    });
    let committed = poll_closed_peer_event(&peers, &mut events, &mut closed_prefix);
    let committed_is_current = committed
        .as_ref()
        .is_some_and(|event| event.is_current(&peers))
        && matches!(
            committed.as_ref(),
            Some(PeerEvent::Message {
                peer_id: 1,
                addr,
                msg: PeerMessage::Have(9),
            }) if *addr == old_addr
        );
    let bound_after_committed_event = closed_prefix.get(&1).copied();

    let new_addr = "127.0.0.1:8665".parse().unwrap();
    let (new_commands, new_receiver) = mpsc::channel(64);
    let new_worker = tokio::spawn(std::future::pending::<()>());
    let new_worker_abort = new_worker.abort_handle();
    peers.insert(
        1,
        test_peer_runtime(
            new_addr,
            1,
            1,
            false,
            false,
            new_commands.clone(),
            new_worker,
        ),
    );
    let no_old_generation_terminal =
        poll_closed_peer_event(&peers, &mut events, &mut closed_prefix).is_none();
    let pending_pruned = closed_prefix.is_empty();
    let stale_old_event = PeerEvent::Lost {
        peer_id: 1,
        addr: old_addr,
    };
    event_tx.send(stale_old_event).await.unwrap();
    let stale_received = events.recv().await.unwrap();
    let stale_ignored = !stale_received.is_current(&peers);

    shutdown_peer_runtimes(&mut peers);
    drop(new_receiver);
    drop(events);
    drop(event_tx);
    let new_worker_reaped = tokio::time::timeout(Duration::from_secs(1), async {
        while !new_worker_abort.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let new_receiver_closed = tokio::time::timeout(Duration::from_secs(1), new_commands.closed())
        .await
        .is_ok();

    assert!(worker_closed);
    assert_eq!(capacity_with_reserved_permit, 127);
    assert!(no_fallback_while_reserved);
    assert_eq!(original_bound, Some((old_addr, 128)));
    assert!(committed_is_current);
    assert_eq!(bound_after_committed_event, Some((old_addr, 127)));
    assert!(no_old_generation_terminal && pending_pruned);
    assert!(stale_ignored);
    assert!(new_worker_reaped && new_receiver_closed);
}

#[tokio::test]
async fn queued_worker_message_remains_current_before_terminal_loss_is_drained() {
    use tokio::io::AsyncWriteExt;

    let auth = ace_wire::live_auth::LiveSourceAuth::generate();
    let payload: Vec<u8> = (0..4)
        .flat_map(|cc| {
            let mut packet = vec![0x55; 188];
            packet[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10 | cc]);
            packet
        })
        .collect();
    let mut signed_piece = payload.clone();
    signed_piece.extend(auth.sign(&payload));
    let stream = StreamInfo {
        infohash: [0; 20],
        piece_length: signed_piece.len() as u64,
        chunk_length: signed_piece.len() as u64,
        trackers: vec![],
        metadata: StreamMetadata::default(),
        sig_len: auth.signature_len(),
        source_pubkey: auth.pubkey_der(),
    };
    let policy = default_live_recovery();
    let mut auth_control = Continuity::fresh(&stream, 1, 3, 3, policy).0;
    assert!(auth_control
        .reasm
        .add_block_with_contribution(1, 0, &signed_piece)
        .is_ok());

    let (mut continuity, _) = Continuity::fresh(&stream, 1, 3, 3, policy);
    continuity.head = 3;
    let observer = TestObserver::new(
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        Arc::new(AtomicU32::new(0)),
    );
    continuity.observer = Some(observer.clone());
    continuity.admitted_upstreams = 3;
    continuity.publish_upstreams();
    let addr = "127.0.0.1:8661".parse().unwrap();
    let stale_source = "127.0.0.1:8662".parse().unwrap();
    let healthy_fallback = "127.0.0.1:8663".parse().unwrap();
    continuity.register_active_peer(1, addr, live_pos(1, 1));
    continuity.register_active_peer(2, stale_source, live_pos(1, 1));
    continuity.register_active_peer(3, healthy_fallback, live_pos(2, 3));
    continuity.set_peer_unchoked(1, true);
    continuity.set_peer_unchoked(3, true);

    let (events, mut event_rx) = mpsc::channel(128);
    let (client, mut server) = tokio::io::duplex(4096);
    let (commands, command_rx) = mpsc::channel(64);
    let worker = tokio::spawn(peer_worker(
        1,
        addr,
        PeerSession::new(client),
        command_rx,
        events.clone(),
        Arc::new(AtomicU64::new(1)),
    ));
    let worker_abort = worker.abort_handle();
    let (stale_commands, stale_receiver) = mpsc::channel(64);
    let (fallback_commands, fallback_receiver) = mpsc::channel(64);
    let stale_worker = tokio::spawn(std::future::pending::<()>());
    let fallback_worker = tokio::spawn(std::future::pending::<()>());
    let stale_worker_abort = stale_worker.abort_handle();
    let fallback_worker_abort = fallback_worker.abort_handle();
    let mut peers = BTreeMap::from([
        (
            1,
            PeerRuntime {
                addr,
                min_piece: 1,
                max_piece: 1,
                unchoked_peer: true,
                produced_output: false,
                seen_ids: HashSet::new(),
                commands: commands.clone(),
                worker,
            },
        ),
        (
            2,
            test_peer_runtime(
                stale_source,
                1,
                1,
                false,
                false,
                stale_commands.clone(),
                stale_worker,
            ),
        ),
        (
            3,
            test_peer_runtime(
                healthy_fallback,
                2,
                3,
                true,
                false,
                fallback_commands.clone(),
                fallback_worker,
            ),
        ),
    ]);

    let initial_losses = advance_pool_requests(&mut peers, &mut continuity, 1).await;
    let request_is_outstanding =
        continuity.active_peers.in_flight_count(1) == 1 && continuity.requested_at.contains_key(&1);
    server
        .write_all(
            &PeerMessage::Piece {
                index: 0,
                begin: 1,
                block: {
                    let mut block = [0u8; 8].to_vec();
                    block.extend_from_slice(&0u16.to_be_bytes());
                    block.extend_from_slice(&signed_piece);
                    block
                },
            }
            .encode(),
        )
        .await
        .unwrap();
    server.shutdown().await.unwrap();

    let worker_finished = tokio::time::timeout(Duration::from_secs(1), async {
        while !peers[&1].worker.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let receiver_closed = tokio::time::timeout(Duration::from_secs(1), commands.closed())
        .await
        .is_ok();
    let mut candidates = SessionCandidates::default();
    candidates.learn(addr, CandidateKind::Source);
    candidates.learn(stale_source, CandidateKind::Source);
    candidates.learn(healthy_fallback, CandidateKind::Pex);
    candidates.attempting(stale_source, Instant::now() + Duration::from_secs(30));
    let mut loss_count = 0;
    let closed_send_marker =
        send_peer_command(1, PeerMessage::Have(1), &mut peers, &mut continuity).await;
    let runtime_retained_after_send = peers.contains_key(&1);
    let event_queue_after_send = event_rx.len();
    let no_send_loss_accounted =
        loss_count == 0 && candidates.eligible(Instant::now()).contains(&addr);
    let peer_count = AtomicU32::new(3);
    let mut closed_prefix = BTreeMap::new();

    let admission_losses = advance_pool_requests(&mut peers, &mut continuity, 1).await;
    let generation_retained = peers.contains_key(&1);
    let first_event = poll_closed_peer_event(&peers, &mut event_rx, &mut closed_prefix);
    let first_is_current_piece = first_event
        .as_ref()
        .is_some_and(|event| event.is_current(&peers))
        && matches!(
            first_event.as_ref(),
            Some(PeerEvent::Message {
                peer_id: 1,
                addr: event_addr,
                msg: PeerMessage::Piece { begin: 1, .. },
            }) if *event_addr == addr
        );
    let message_verified_and_consumed = match first_event {
        Some(PeerEvent::Message { msg, .. }) if first_is_current_piece => {
            let chunk = LiveChunk::from_message(&msg).unwrap();
            let verified = continuity
                .reasm
                .add_block_with_contribution(chunk.piece as u64, 0, &chunk.data)
                .is_ok();
            if verified {
                continuity.piece_producers.insert(1, Some(addr));
                let ready = continuity.reasm.take_ready();
                let cursor_after_output = continuity.reasm.next_needed();
                continuity
                    .piece_producers
                    .retain(|piece, _| *piece >= cursor_after_output);
                peers.get_mut(&1).unwrap().produced_output = !ready.is_empty();
            }
            verified
        }
        _ => false,
    };
    let lost_event = poll_closed_peer_event(&peers, &mut event_rx, &mut closed_prefix);
    let terminal_is_current_lost = lost_event
        .as_ref()
        .is_some_and(|event| event.is_current(&peers))
        && matches!(
            lost_event.as_ref(),
            Some(PeerEvent::Lost {
                peer_id: 1,
                addr: event_addr,
            }) if *event_addr == addr
        );
    let pool_empty = if terminal_is_current_lost {
        handle_pool_peer_loss(
            1,
            &mut peers,
            &mut continuity,
            1,
            &mut candidates,
            &mut loss_count,
            &peer_count,
        )
        .await
    } else {
        false
    };
    let pending_after_terminal = poll_closed_peer_event(&peers, &mut event_rx, &mut closed_prefix);
    let event_queue_empty = event_rx.is_empty();
    let stale_duplicate = PeerEvent::Lost { peer_id: 1, addr };
    let duplicate_is_stale = !stale_duplicate.is_current(&peers);
    let loss_accounted_once = loss_count == 1
        && !candidates.eligible(Instant::now()).contains(&addr)
        && candidates.eligible(Instant::now()).contains(&stale_source)
        && peer_count.load(Ordering::Relaxed) == 1
        && continuity.admitted_upstreams == 1
        && observer.snapshot().active_upstreams == 1
        && peers.contains_key(&3)
        && !peers.contains_key(&2);
    let fallback_retained_after_terminal = peers.contains_key(&3);
    let stale_source_removed_after_terminal = !peers.contains_key(&2);

    shutdown_peer_runtimes(&mut peers);
    worker_abort.abort();
    drop(stale_receiver);
    drop(fallback_receiver);
    let worker_reaped = tokio::time::timeout(Duration::from_secs(1), async {
        while !worker_abort.is_finished()
            || !stale_worker_abort.is_finished()
            || !fallback_worker_abort.is_finished()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let idle_receivers_closed = tokio::time::timeout(Duration::from_secs(1), async {
        stale_commands.closed().await;
        fallback_commands.closed().await;
    })
    .await
    .is_ok();
    drop(event_rx);
    drop(server);

    assert!(initial_losses.is_empty());
    assert_eq!(closed_send_marker, Some(addr));
    assert!(runtime_retained_after_send && event_queue_after_send == 2);
    assert!(
        no_send_loss_accounted,
        "failed Send is only an admission marker"
    );
    assert!(
        admission_losses.is_empty(),
        "admission is not a loss receipt"
    );
    assert!(request_is_outstanding);
    assert!(
        worker_finished && receiver_closed,
        "worker must finish after EOF"
    );
    assert!(
        generation_retained,
        "closed command sender receives zero credit but keeps its generation"
    );
    assert!(
        event_queue_empty,
        "bounded FIFO prefix must be fully consumed"
    );
    assert!(
        first_is_current_piece,
        "queued real-worker Piece must remain current"
    );
    assert!(
        message_verified_and_consumed,
        "verified media must be consumed before terminal loss"
    );
    assert!(
        terminal_is_current_lost,
        "real worker Lost must remain current after its Piece"
    );
    assert!(!pool_empty);
    assert_eq!(continuity.reasm.next_needed(), 2);
    assert!(fallback_retained_after_terminal && stale_source_removed_after_terminal);
    assert!(
        pending_after_terminal.is_none(),
        "terminal handling prunes reconciliation state"
    );
    assert!(
        duplicate_is_stale,
        "a repeated terminal event cannot target the removed generation"
    );
    assert!(
        loss_accounted_once,
        "closed peer loss must enter cooldown once"
    );
    assert!(worker_reaped, "worker cleanup must complete");
    assert!(idle_receivers_closed);
}

#[tokio::test]
async fn another_verified_producer_suppresses_stale_source_refresh() {
    let auth = ace_wire::live_auth::LiveSourceAuth::generate();
    let make_signed_piece = |marker: u8| {
        let payload: Vec<u8> = (0..4)
            .flat_map(|cc| {
                let mut packet = vec![marker; 188];
                packet[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10 | cc]);
                packet
            })
            .collect();
        let mut signed = payload.clone();
        signed.extend(auth.sign(&payload));
        (payload, signed)
    };
    let (_first_payload, first_signed) = make_signed_piece(0x55);
    let (_second_payload, second_signed) = make_signed_piece(0x66);
    let stream = StreamInfo {
        infohash: [0; 20],
        piece_length: first_signed.len() as u64,
        chunk_length: first_signed.len() as u64,
        trackers: vec![],
        metadata: StreamMetadata::default(),
        sig_len: auth.signature_len(),
        source_pubkey: auth.pubkey_der(),
    };
    let policy = default_live_recovery();
    let (mut continuity, _) = Continuity::fresh(&stream, 1, 3, 3, policy);
    continuity.head = 3;
    let observer = TestObserver::new(
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        Arc::new(AtomicU32::new(0)),
    );
    continuity.observer = Some(observer.clone());
    continuity.admitted_upstreams = 3;
    continuity.publish_upstreams();

    let producer = "127.0.0.1:8666".parse().unwrap();
    let stale_source = "127.0.0.1:8667".parse().unwrap();
    let continuing_producer = "127.0.0.1:8668".parse().unwrap();
    continuity.register_active_peer(1, producer, live_pos(1, 3));
    continuity.register_active_peer(2, stale_source, live_pos(1, 2));
    continuity.register_active_peer(3, continuing_producer, live_pos(2, 3));

    let (producer_commands, producer_receiver) = mpsc::channel(64);
    let (stale_commands, stale_receiver) = mpsc::channel(64);
    let (continuing_commands, continuing_receiver) = mpsc::channel(64);
    let producer_worker = tokio::spawn(std::future::pending::<()>());
    let stale_worker = tokio::spawn(std::future::pending::<()>());
    let continuing_worker = tokio::spawn(std::future::pending::<()>());
    let producer_abort = producer_worker.abort_handle();
    let stale_abort = stale_worker.abort_handle();
    let continuing_abort = continuing_worker.abort_handle();
    let mut peers = BTreeMap::from([
        (
            1,
            test_peer_runtime(
                producer,
                1,
                3,
                false,
                false,
                producer_commands,
                producer_worker,
            ),
        ),
        (
            2,
            test_peer_runtime(
                stale_source,
                1,
                2,
                false,
                false,
                stale_commands,
                stale_worker,
            ),
        ),
        (
            3,
            test_peer_runtime(
                continuing_producer,
                2,
                3,
                false,
                false,
                continuing_commands,
                continuing_worker,
            ),
        ),
    ]);

    let first_verified = continuity
        .reasm
        .add_block_with_contribution(1, 0, &first_signed)
        .is_ok();
    let first_ready = continuity.reasm.take_ready();
    let first_output = continuity.resync_output(&first_ready);
    let first_produced = first_verified && !first_output.is_empty();
    if first_produced {
        peers.get_mut(&1).unwrap().produced_output = true;
    }
    let second_verified = continuity
        .reasm
        .add_block_with_contribution(2, 0, &second_signed)
        .is_ok();
    let second_ready = continuity.reasm.take_ready();
    let second_output = continuity.resync_output(&second_ready);
    let second_produced = second_verified && !second_output.is_empty();
    if second_produced {
        peers.get_mut(&3).unwrap().produced_output = true;
    }
    let mut candidates = SessionCandidates::default();
    candidates.learn(producer, CandidateKind::Source);
    candidates.learn(stale_source, CandidateKind::Source);
    candidates.learn(continuing_producer, CandidateKind::Pex);
    candidates.attempting(stale_source, Instant::now() + Duration::from_secs(30));
    let terminal = PeerEvent::Lost {
        peer_id: 1,
        addr: producer,
    };
    let terminal_current = terminal.is_current(&peers);
    let mut loss_count = 0;
    let peer_count = AtomicU32::new(3);
    let pool_empty = if terminal_current {
        handle_pool_peer_loss(
            1,
            &mut peers,
            &mut continuity,
            1,
            &mut candidates,
            &mut loss_count,
            &peer_count,
        )
        .await
    } else {
        false
    };
    let stale_source_retained = peers.contains_key(&2);
    let other_producer_retained = peers.get(&3).is_some_and(|peer| peer.produced_output);
    let stale_source_still_cooling = !candidates.eligible(Instant::now()).contains(&stale_source);
    let loss_once = loss_count == 1
        && !candidates.eligible(Instant::now()).contains(&producer)
        && peer_count.load(Ordering::Relaxed) == 2
        && continuity.admitted_upstreams == 2
        && observer.snapshot().active_upstreams == 2;
    let stale_duplicate = PeerEvent::Lost {
        peer_id: 1,
        addr: producer,
    };
    let duplicate_is_stale = !stale_duplicate.is_current(&peers);

    shutdown_peer_runtimes(&mut peers);
    drop(producer_receiver);
    drop(stale_receiver);
    drop(continuing_receiver);
    let workers_reaped = tokio::time::timeout(Duration::from_secs(1), async {
        while !producer_abort.is_finished()
            || !stale_abort.is_finished()
            || !continuing_abort.is_finished()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();

    assert!(first_produced && second_produced);
    assert_eq!(continuity.reasm.next_needed(), 3);
    assert!(terminal_current && !pool_empty);
    assert!(stale_source_retained && stale_source_still_cooling);
    assert!(
        other_producer_retained,
        "a remaining verified producer suppresses stale-source refresh"
    );
    assert!(loss_once && duplicate_is_stale);
    assert!(workers_reaped);
}

#[tokio::test]
async fn a_send_full_marker_defers_loss_until_terminal_event() {
    let policy = default_live_recovery();
    let a_addr = "127.0.0.1:8654".parse().unwrap();
    let b_addr = "127.0.0.1:8655".parse().unwrap();
    let (a_commands, mut a_receiver) = mpsc::channel(64);
    let (b_commands, mut b_receiver) = mpsc::channel(64);
    let a_worker = tokio::spawn(std::future::pending::<()>());
    let b_worker = tokio::spawn(std::future::pending::<()>());
    let a_worker_abort = a_worker.abort_handle();
    let b_worker_abort = b_worker.abort_handle();
    let mut peers = BTreeMap::from([
        (
            1,
            PeerRuntime {
                addr: a_addr,
                min_piece: 1,
                max_piece: 64,
                unchoked_peer: false,
                produced_output: false,
                seen_ids: HashSet::new(),
                commands: a_commands.clone(),
                worker: a_worker,
            },
        ),
        (
            2,
            PeerRuntime {
                addr: b_addr,
                min_piece: 65,
                max_piece: 65,
                unchoked_peer: false,
                produced_output: false,
                seen_ids: HashSet::new(),
                commands: b_commands.clone(),
                worker: b_worker,
            },
        ),
    ]);
    let (mut continuity, _) = Continuity::fresh(&info(), 1, 65, 65, policy);
    continuity.head = 65;
    continuity.register_active_peer(1, a_addr, live_pos(1, 64));
    continuity.register_active_peer(2, b_addr, live_pos(65, 65));
    continuity.set_peer_unchoked(1, true);
    continuity.set_peer_unchoked(2, true);
    let request_losses = advance_pool_requests(&mut peers, &mut continuity, 1).await;
    let a_capacity_after_requests = a_commands.capacity();
    let a_owned_after_requests = continuity.active_peers.in_flight_count(1);
    let b_owned_after_requests = continuity.active_peers.in_flight_count(2);
    let a_timers_before_send: Vec<_> = (1..=64)
        .map(|piece| continuity.requested_at.get(&piece).copied())
        .collect();
    let b_timer_before_send = continuity.requested_at.get(&65).copied();

    let mut candidates = SessionCandidates::default();
    candidates.learn(a_addr, CandidateKind::Source);
    let mut loss_count = 0;
    let a_failure = send_peer_command(1, PeerMessage::Have(64), &mut peers, &mut continuity).await;
    let repeated_failure =
        send_peer_command(1, PeerMessage::Have(63), &mut peers, &mut continuity).await;
    let b_send = send_peer_command(2, PeerMessage::Have(65), &mut peers, &mut continuity).await;
    let a_retained_after_failure = peers.contains_key(&1);
    let b_retained_after_failure = peers.contains_key(&2);
    let loss_not_accounted_at_admission = loss_count == 0
        && candidates.eligible(Instant::now()).contains(&a_addr)
        && continuity.active_peers.in_flight_count(1) == 64;
    let b_owned_after_send = continuity.active_peers.in_flight_count(2);
    let b_timer_after_send = continuity.requested_at.get(&65).copied();
    let b_commands_seen: Vec<_> = std::iter::from_fn(|| b_receiver.try_recv().ok()).collect();
    let a_queue_preserved = a_receiver.len() == 64;
    let mut a_queued_requests = Vec::new();
    while let Ok(command) = a_receiver.try_recv() {
        a_queued_requests.push(command);
    }
    let a_queue_only_requests = a_queued_requests.len() == 64
        && a_queued_requests
            .iter()
            .all(|command| matches!(command, PeerCommand::RequestPiece { .. }));

    let worker_finished = tokio::time::timeout(Duration::from_secs(1), async {
        while !a_worker_abort.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    drop(a_receiver);
    let receiver_closed = tokio::time::timeout(Duration::from_secs(1), a_commands.closed())
        .await
        .is_ok();
    let (_events_tx, mut events) = mpsc::channel::<PeerEvent>(128);
    let mut closed_prefix = BTreeMap::new();
    let terminal_event = poll_closed_peer_event(&peers, &mut events, &mut closed_prefix);
    let terminal_current = terminal_event
        .as_ref()
        .is_some_and(|event| event.is_current(&peers))
        && matches!(
            terminal_event.as_ref(),
            Some(PeerEvent::Lost {
                peer_id: 1,
                addr,
            }) if *addr == a_addr
        );
    let peer_count = AtomicU32::new(2);
    let pool_empty = if terminal_current {
        handle_pool_peer_loss(
            1,
            &mut peers,
            &mut continuity,
            1,
            &mut candidates,
            &mut loss_count,
            &peer_count,
        )
        .await
    } else {
        false
    };
    let a_removed_after_terminal = !peers.contains_key(&1);
    let a_ownership_after_terminal = continuity.active_peers.in_flight_count(1);
    let a_timers_preserved = a_timers_before_send
        == (1..=64)
            .map(|piece| continuity.requested_at.get(&piece).copied())
            .collect::<Vec<_>>();
    let loss_accounted_once = loss_count == 1
        && !candidates.eligible(Instant::now()).contains(&a_addr)
        && peer_count.load(Ordering::Relaxed) == 1;
    let pending_after_terminal = poll_closed_peer_event(&peers, &mut events, &mut closed_prefix);

    shutdown_peer_runtimes(&mut peers);
    drop(b_receiver);
    drop(events);
    let workers_reaped = tokio::time::timeout(Duration::from_secs(1), async {
        while !a_worker_abort.is_finished() || !b_worker_abort.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let b_receiver_closed = tokio::time::timeout(Duration::from_secs(1), b_commands.closed())
        .await
        .is_ok();

    assert!(request_losses.is_empty());
    assert_eq!(
        a_capacity_after_requests, 0,
        "RequestPiece fills A's 64 slots"
    );
    assert_eq!(a_owned_after_requests, 64);
    assert_eq!(b_owned_after_requests, 1);
    assert!(a_timers_before_send.iter().all(Option::is_some));
    assert_eq!(b_timer_before_send, b_timer_after_send);
    assert_eq!(a_failure, Some(a_addr));
    assert_eq!(
        repeated_failure,
        Some(a_addr),
        "failure markers do not account loss"
    );
    assert_eq!(b_send, None);
    assert!(a_retained_after_failure && b_retained_after_failure);
    assert!(
        loss_not_accounted_at_admission,
        "loss waits for terminal handling"
    );
    assert!(
        a_queue_preserved && a_queue_only_requests,
        "failed Send does not purge the request queue"
    );
    assert_eq!(b_owned_after_send, 1);
    assert!(matches!(
        b_commands_seen.as_slice(),
        [
            PeerCommand::RequestPiece { piece: 65, .. },
            PeerCommand::Send(PeerMessage::Have(65))
        ]
    ));
    assert!(worker_finished && receiver_closed && terminal_current);
    assert!(!pool_empty && a_removed_after_terminal);
    assert_eq!(a_ownership_after_terminal, 0);
    assert!(a_timers_preserved);
    assert!(loss_accounted_once);
    assert!(pending_after_terminal.is_none());
    assert!(workers_reaped && b_receiver_closed);
}

#[tokio::test]
async fn a_send_closed_marker_defers_loss_until_terminal_event() {
    let a_addr = "127.0.0.1:8656".parse().unwrap();
    let b_addr = "127.0.0.1:8657".parse().unwrap();
    let (a_commands, a_receiver) = mpsc::channel(64);
    let (b_commands, mut b_receiver) = mpsc::channel(64);
    let a_worker = tokio::spawn(async move { drop(a_receiver) });
    let b_worker = tokio::spawn(std::future::pending::<()>());
    let a_worker_abort = a_worker.abort_handle();
    let b_worker_abort = b_worker.abort_handle();
    let mut peers = BTreeMap::from([
        (
            1,
            PeerRuntime {
                addr: a_addr,
                min_piece: 1,
                max_piece: 1,
                unchoked_peer: false,
                produced_output: false,
                seen_ids: HashSet::new(),
                commands: a_commands.clone(),
                worker: a_worker,
            },
        ),
        (
            2,
            PeerRuntime {
                addr: b_addr,
                min_piece: 2,
                max_piece: 2,
                unchoked_peer: false,
                produced_output: false,
                seen_ids: HashSet::new(),
                commands: b_commands.clone(),
                worker: b_worker,
            },
        ),
    ]);
    let (mut continuity, _) = Continuity::fresh(&info(), 1, 2, 2, default_live_recovery());
    let worker_closed = tokio::time::timeout(Duration::from_secs(1), async {
        while !a_worker_abort.is_finished() || !a_commands.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();

    let mut candidates = SessionCandidates::default();
    candidates.learn(a_addr, CandidateKind::Source);
    let mut loss_count = 0;
    let first_failure =
        send_peer_command(1, PeerMessage::Have(1), &mut peers, &mut continuity).await;
    let repeated_failure =
        send_peer_command(1, PeerMessage::Have(1), &mut peers, &mut continuity).await;
    let healthy_send =
        send_peer_command(2, PeerMessage::Have(2), &mut peers, &mut continuity).await;
    let retained_before_terminal = peers.contains_key(&1) && peers.contains_key(&2);
    let no_loss_at_admission =
        loss_count == 0 && candidates.eligible(Instant::now()).contains(&a_addr);
    let b_commands_seen: Vec<_> = std::iter::from_fn(|| b_receiver.try_recv().ok()).collect();

    let (_events_tx, mut events) = mpsc::channel::<PeerEvent>(128);
    let mut closed_prefix = BTreeMap::new();
    let terminal_event = poll_closed_peer_event(&peers, &mut events, &mut closed_prefix);
    let terminal_current = terminal_event
        .as_ref()
        .is_some_and(|event| event.is_current(&peers))
        && matches!(
            terminal_event.as_ref(),
            Some(PeerEvent::Lost {
                peer_id: 1,
                addr,
            }) if *addr == a_addr
        );
    let peer_count = AtomicU32::new(2);
    let pool_empty = if terminal_current {
        handle_pool_peer_loss(
            1,
            &mut peers,
            &mut continuity,
            1,
            &mut candidates,
            &mut loss_count,
            &peer_count,
        )
        .await
    } else {
        false
    };
    let absent_peer_noop =
        send_peer_command(1, PeerMessage::Have(1), &mut peers, &mut continuity).await;
    let duplicate_current = PeerEvent::Lost {
        peer_id: 1,
        addr: a_addr,
    }
    .is_current(&peers);
    let pending_after_terminal = poll_closed_peer_event(&peers, &mut events, &mut closed_prefix);
    let accounted_once = loss_count == 1
        && !candidates.eligible(Instant::now()).contains(&a_addr)
        && peer_count.load(Ordering::Relaxed) == 1;

    shutdown_peer_runtimes(&mut peers);
    drop(b_receiver);
    drop(events);
    let workers_reaped = tokio::time::timeout(Duration::from_secs(1), async {
        while !a_worker_abort.is_finished() || !b_worker_abort.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let b_receiver_closed = tokio::time::timeout(Duration::from_secs(1), b_commands.closed())
        .await
        .is_ok();

    assert!(worker_closed);
    assert_eq!(first_failure, Some(a_addr));
    assert_eq!(
        repeated_failure,
        Some(a_addr),
        "repeated failures are markers, not loss receipts"
    );
    assert_eq!(healthy_send, None);
    assert!(retained_before_terminal && no_loss_at_admission);
    assert!(matches!(
        b_commands_seen.as_slice(),
        [PeerCommand::Send(PeerMessage::Have(2))]
    ));
    assert!(terminal_current && !pool_empty && !peers.contains_key(&1));
    assert_eq!(absent_peer_noop, None);
    assert!(!duplicate_current && pending_after_terminal.is_none());
    assert!(accounted_once);
    assert!(workers_reaped && b_receiver_closed);
}

#[tokio::test(start_paused = true)]
async fn saturated_send_enqueue_does_not_stall_other_peer_commands() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (event_capacity, refill_capacity) =
        recovery_channel_capacities(default_live_recovery()).unwrap();
    assert_eq!((event_capacity, refill_capacity), (128, 4));
    let (events, mut event_rx) = mpsc::channel(event_capacity);

    let (a_client, mut a_socket) = tokio::io::duplex(8);
    let a_pending_writes = Arc::new(AtomicU64::new(0));
    let a_written_bytes = Arc::new(AtomicU64::new(0));
    let a_addr = "127.0.0.1:8631".parse().unwrap();
    let (a_commands, a_receiver) = mpsc::channel(64);
    let a_worker = tokio::spawn(peer_worker(
        1,
        a_addr,
        PeerSession::new(PendingWriteIo {
            inner: a_client,
            pending_writes: Arc::clone(&a_pending_writes),
            written_bytes: Arc::clone(&a_written_bytes),
        }),
        a_receiver,
        events.clone(),
        Arc::new(AtomicU64::new(1)),
    ));

    let (b_client, mut b_socket) = tokio::io::duplex(4096);
    let b_written_bytes = Arc::new(AtomicU64::new(0));
    let b_addr = "127.0.0.1:8632".parse().unwrap();
    let (b_commands, b_receiver) = mpsc::channel(64);
    let b_worker = tokio::spawn(peer_worker(
        2,
        b_addr,
        PeerSession::new(PendingWriteIo {
            inner: b_client,
            pending_writes: Arc::new(AtomicU64::new(0)),
            written_bytes: Arc::clone(&b_written_bytes),
        }),
        b_receiver,
        events.clone(),
        Arc::new(AtomicU64::new(1)),
    ));
    let mut peers = BTreeMap::from([
        (
            1,
            PeerRuntime {
                addr: a_addr,
                min_piece: 1,
                max_piece: 100,
                unchoked_peer: false,
                produced_output: false,
                seen_ids: HashSet::new(),
                commands: a_commands.clone(),
                worker: a_worker,
            },
        ),
        (
            2,
            PeerRuntime {
                addr: b_addr,
                min_piece: 1,
                max_piece: 100,
                unchoked_peer: false,
                produced_output: false,
                seen_ids: HashSet::new(),
                commands: b_commands.clone(),
                worker: b_worker,
            },
        ),
    ]);
    let (mut continuity, _) = Continuity::fresh(&info(), 1, 1, 256, default_live_recovery());

    let first_send = send_peer_command(1, PeerMessage::Have(1), &mut peers, &mut continuity).await;
    let mut write_pending_seen = false;
    for _ in 0..128 {
        if a_pending_writes.load(Ordering::Relaxed) > 0 {
            write_pending_seen = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    let mut filled_without_loss = true;
    for piece in 2..=65 {
        if send_peer_command(1, PeerMessage::Have(piece), &mut peers, &mut continuity)
            .await
            .is_some()
        {
            filled_without_loss = false;
            break;
        }
    }
    let queue_capacity = a_commands.capacity();

    let keep_alive = PeerMessage::KeepAlive.encode();
    b_socket
        .write_all(&keep_alive.repeat(event_capacity))
        .await
        .unwrap();
    for _ in 0..512 {
        if event_rx.len() == event_capacity {
            break;
        }
        tokio::task::yield_now().await;
    }
    let events_full = event_rx.len() == event_capacity;

    // Retain handles before constructing the actor future, which mutably borrows runtimes.
    let a_worker_abort = peers.get(&1).unwrap().worker.abort_handle();
    let b_worker_abort = peers.get(&2).unwrap().worker.abort_handle();
    let mut actor = Box::pin(async {
        let overloaded =
            send_peer_command(1, PeerMessage::Have(66), &mut peers, &mut continuity).await;
        let healthy =
            send_peer_command(2, PeerMessage::Have(67), &mut peers, &mut continuity).await;
        (overloaded, healthy)
    });
    let mut actor_completed = false;
    let mut actor_result = None;
    for _ in 0..128 {
        tokio::select! {
            biased;
            result = &mut actor => {
                actor_completed = true;
                actor_result = Some(result);
                break;
            }
            _ = tokio::task::yield_now() => {}
        }
    }
    if !actor_completed {
        tokio::time::advance(Duration::from_secs(21)).await;
        for _ in 0..128 {
            tokio::select! {
                biased;
                result = &mut actor => {
                    actor_completed = true;
                    actor_result = Some(result);
                    break;
                }
                _ = tokio::task::yield_now() => {}
            }
        }
    }
    for _ in 0..128 {
        if b_written_bytes.load(Ordering::Relaxed) > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    drop(actor);
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    let still_full_events = event_rx.len() == event_capacity;

    let a_finished = tokio::time::timeout(Duration::from_secs(1), async {
        while !a_worker_abort.is_finished() || !a_commands.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let mut closed_prefix = BTreeMap::new();
    let mut prefix_current_b_messages = 0usize;
    let mut prefix_was_bounded = true;
    for _ in 0..event_capacity {
        match poll_closed_peer_event(&peers, &mut event_rx, &mut closed_prefix) {
            Some(event)
                if event.is_current(&peers)
                    && matches!(event, PeerEvent::Message { peer_id: 2, .. }) =>
            {
                prefix_current_b_messages += 1;
            }
            _ => {
                prefix_was_bounded = false;
                break;
            }
        }
    }
    let synthetic_terminal = poll_closed_peer_event(&peers, &mut event_rx, &mut closed_prefix);
    let terminal_is_current = synthetic_terminal
        .as_ref()
        .is_some_and(|event| event.is_current(&peers))
        && matches!(
            synthetic_terminal.as_ref(),
            Some(PeerEvent::Lost {
                peer_id: 1,
                addr,
            }) if *addr == a_addr
        );
    let mut candidates = SessionCandidates::default();
    candidates.learn(a_addr, CandidateKind::Source);
    let mut loss_count = 0;
    let peer_count = AtomicU32::new(2);
    let pool_empty = if terminal_is_current {
        handle_pool_peer_loss(
            1,
            &mut peers,
            &mut continuity,
            1,
            &mut candidates,
            &mut loss_count,
            &peer_count,
        )
        .await
    } else {
        false
    };
    let a_runtime_removed = !peers.contains_key(&1);
    let b_runtime_retained = peers.contains_key(&2);
    let pending_pruned =
        poll_closed_peer_event(&peers, &mut event_rx, &mut closed_prefix).is_none();
    shutdown_peer_runtimes(&mut peers);
    let workers_reaped = tokio::time::timeout(Duration::from_secs(1), async {
        while !a_worker_abort.is_finished() || !b_worker_abort.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let a_receiver_closed = tokio::time::timeout(Duration::from_secs(1), a_commands.closed())
        .await
        .is_ok();
    let b_receiver_closed = tokio::time::timeout(Duration::from_secs(1), b_commands.closed())
        .await
        .is_ok();
    let mut a_remaining = Vec::new();
    let a_io_closed = tokio::time::timeout(
        Duration::from_secs(1),
        a_socket.read_to_end(&mut a_remaining),
    )
    .await
    .is_ok_and(|result| result.is_ok());
    let mut b_remaining = Vec::new();
    let b_io_closed = tokio::time::timeout(
        Duration::from_secs(1),
        b_socket.read_to_end(&mut b_remaining),
    )
    .await
    .is_ok_and(|result| result.is_ok());

    assert_eq!(first_send, None);
    assert!(write_pending_seen);
    assert!(filled_without_loss);
    assert_eq!(queue_capacity, 0, "A's real 64-command queue must be full");
    assert!(events_full, "B's real 128-message event queue must be full");
    assert!(actor_completed);
    assert!(a_finished && still_full_events);
    assert!(prefix_was_bounded && prefix_current_b_messages == event_capacity);
    assert!(terminal_is_current && !pool_empty && a_runtime_removed);
    assert!(b_runtime_retained, "healthy B must remain active");
    assert_eq!(loss_count, 1, "terminal event accounts A's loss once");
    assert!(!candidates.eligible(Instant::now()).contains(&a_addr));
    assert_eq!(peer_count.load(Ordering::Relaxed), 1);
    assert!(pending_pruned);
    assert!(workers_reaped, "peer workers did not terminate after abort");
    assert!(
        a_receiver_closed && b_receiver_closed,
        "peer command receivers stayed open"
    );
    assert!(
        a_io_closed && b_io_closed,
        "peer I/O did not terminate after runtime shutdown"
    );
    assert!(
        actor_completed,
        "the 65th Send enqueue stayed pending after 21s virtual time; \
         A queue capacity={queue_capacity}, events={}/{event_capacity}, \
         A runtime removed={a_runtime_removed}",
        event_rx.len()
    );
    let (overloaded_peer, healthy_peer) = actor_result.unwrap();
    assert_eq!(overloaded_peer, Some(a_addr));
    assert_eq!(healthy_peer, None);
    assert!(b_written_bytes.load(Ordering::Relaxed) > 0);
}

#[tokio::test(start_paused = true)]
async fn saturated_send_enqueue_completes_when_the_peer_resumes_reading() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (event_capacity, refill_capacity) =
        recovery_channel_capacities(default_live_recovery()).unwrap();
    assert_eq!((event_capacity, refill_capacity), (128, 4));
    let (events, event_rx) = mpsc::channel(event_capacity);

    let (a_client, a_socket) = tokio::io::duplex(8);
    let a_pending_writes = Arc::new(AtomicU64::new(0));
    let a_written_bytes = Arc::new(AtomicU64::new(0));
    let a_addr = "127.0.0.1:8641".parse().unwrap();
    let (a_commands, a_receiver) = mpsc::channel(64);
    let a_worker = tokio::spawn(peer_worker(
        1,
        a_addr,
        PeerSession::new(PendingWriteIo {
            inner: a_client,
            pending_writes: Arc::clone(&a_pending_writes),
            written_bytes: Arc::clone(&a_written_bytes),
        }),
        a_receiver,
        events.clone(),
        Arc::new(AtomicU64::new(1)),
    ));
    let (b_client, mut b_socket) = tokio::io::duplex(4096);
    let b_written_bytes = Arc::new(AtomicU64::new(0));
    let b_addr = "127.0.0.1:8642".parse().unwrap();
    let (b_commands, b_receiver) = mpsc::channel(64);
    let b_worker = tokio::spawn(peer_worker(
        2,
        b_addr,
        PeerSession::new(PendingWriteIo {
            inner: b_client,
            pending_writes: Arc::new(AtomicU64::new(0)),
            written_bytes: Arc::clone(&b_written_bytes),
        }),
        b_receiver,
        events.clone(),
        Arc::new(AtomicU64::new(1)),
    ));
    let mut peers = BTreeMap::from([
        (
            1,
            PeerRuntime {
                addr: a_addr,
                min_piece: 1,
                max_piece: 100,
                unchoked_peer: false,
                produced_output: false,
                seen_ids: HashSet::new(),
                commands: a_commands.clone(),
                worker: a_worker,
            },
        ),
        (
            2,
            PeerRuntime {
                addr: b_addr,
                min_piece: 1,
                max_piece: 100,
                unchoked_peer: false,
                produced_output: false,
                seen_ids: HashSet::new(),
                commands: b_commands.clone(),
                worker: b_worker,
            },
        ),
    ]);
    let (mut continuity, _) = Continuity::fresh(&info(), 1, 1, 256, default_live_recovery());

    let first_send = send_peer_command(1, PeerMessage::Have(1), &mut peers, &mut continuity).await;
    let mut write_pending_seen = false;
    for _ in 0..128 {
        if a_pending_writes.load(Ordering::Relaxed) > 0 {
            write_pending_seen = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    let mut filled_without_loss = true;
    for piece in 2..=65 {
        if send_peer_command(1, PeerMessage::Have(piece), &mut peers, &mut continuity)
            .await
            .is_some()
        {
            filled_without_loss = false;
            break;
        }
    }
    let queue_capacity = a_commands.capacity();
    let keep_alive = PeerMessage::KeepAlive.encode();
    b_socket
        .write_all(&keep_alive.repeat(event_capacity))
        .await
        .unwrap();
    for _ in 0..512 {
        if event_rx.len() == event_capacity {
            break;
        }
        tokio::task::yield_now().await;
    }
    let events_full = event_rx.len() == event_capacity;

    let a_reader = tokio::spawn(async move {
        let mut socket = a_socket;
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).await.unwrap();
        bytes
    });
    for _ in 0..128 {
        if a_commands.capacity() > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    let send_slot_freed_before_final_enqueue = a_commands.capacity() > 0;
    let mut actor = Box::pin(async {
        let overloaded =
            send_peer_command(1, PeerMessage::Have(66), &mut peers, &mut continuity).await;
        let healthy =
            send_peer_command(2, PeerMessage::Have(67), &mut peers, &mut continuity).await;
        (overloaded, healthy)
    });
    let result = tokio::time::timeout(Duration::from_secs(1), &mut actor).await;
    for _ in 0..128 {
        if b_written_bytes.load(Ordering::Relaxed) > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    let b_wrote = b_written_bytes.load(Ordering::Relaxed) > 0;
    for _ in 0..1024 {
        if a_written_bytes.load(Ordering::Relaxed) >= 66 * 9 {
            break;
        }
        tokio::task::yield_now().await;
    }
    let a_drained_all_sends = a_written_bytes.load(Ordering::Relaxed) >= 66 * 9;
    drop(actor);

    continuity.head = 2;
    continuity.register_active_peer(1, a_addr, live_pos(1, 1));
    continuity.register_active_peer(2, b_addr, live_pos(2, 2));
    continuity.set_peer_unchoked(1, true);
    continuity.set_peer_unchoked(2, true);
    let request_lost = advance_pool_requests(&mut peers, &mut continuity, 1).await;
    for _ in 0..1024 {
        if a_written_bytes.load(Ordering::Relaxed) >= 66 * 9 + 15
            && b_written_bytes.load(Ordering::Relaxed) >= 9 + 15
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    let both_request_commands_written = a_written_bytes.load(Ordering::Relaxed) >= 66 * 9 + 15
        && b_written_bytes.load(Ordering::Relaxed) >= 9 + 15;

    let a_worker_abort = peers.get(&1).unwrap().worker.abort_handle();
    let b_worker_abort = peers.get(&2).unwrap().worker.abort_handle();
    shutdown_peer_runtimes(&mut peers);
    let workers_reaped = tokio::time::timeout(Duration::from_secs(1), async {
        while !a_worker_abort.is_finished() || !b_worker_abort.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let a_read = tokio::time::timeout(Duration::from_secs(1), a_reader).await;
    let b_receiver_closed = tokio::time::timeout(Duration::from_secs(1), b_commands.closed())
        .await
        .is_ok();
    let mut b_remaining = Vec::new();
    let b_io_closed = tokio::time::timeout(
        Duration::from_secs(1),
        b_socket.read_to_end(&mut b_remaining),
    )
    .await
    .is_ok_and(|result| result.is_ok());

    assert_eq!(first_send, None);
    assert!(write_pending_seen);
    assert!(filled_without_loss);
    assert_eq!(queue_capacity, 0);
    assert!(events_full);
    assert!(send_slot_freed_before_final_enqueue);
    assert!(workers_reaped && b_receiver_closed && b_io_closed);
    let (overloaded_peer, healthy_peer) =
        result.expect("draining peer should release its queued writes before the one-second bound");
    assert_eq!(
        overloaded_peer, None,
        "healthy peer A must not be marked lost"
    );
    assert_eq!(healthy_peer, None, "healthy peer B must not be marked lost");
    assert!(
        b_wrote,
        "peer B should send its command after A's queue drains"
    );
    assert!(
        a_drained_all_sends,
        "A did not write all 66 queued Have frames"
    );
    assert!(
        request_lost.is_empty(),
        "the recovered pool marked a peer lost"
    );
    assert!(
        both_request_commands_written,
        "advance_pool_requests did not write its eligible request to both healthy peers"
    );
    let a_bytes = a_read
        .expect("A reader did not finish after runtime shutdown")
        .expect("A reader failed");
    assert_eq!(
        a_bytes.len(),
        66 * 9 + 15,
        "A did not drain all queued Send and RequestPiece frames"
    );
    assert_eq!(
        b_remaining.len(),
        9 + 15,
        "B did not write Send and RequestPiece"
    );
}

#[tokio::test]
async fn peer_worker_sends_chunk_requests_from_commands() {
    use tokio::io::AsyncReadExt;

    let (client, mut server) = tokio::io::duplex(4096);
    let session = PeerSession::new(client).with_timeout(Duration::from_millis(250));
    let addr: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();
    let (command_tx, command_rx) = mpsc::channel(4);
    let (event_tx, _event_rx) = mpsc::channel(4);
    let worker = tokio::spawn(peer_worker(
        1,
        addr,
        session,
        command_rx,
        event_tx,
        Arc::new(AtomicU64::new(0)),
    ));

    command_tx
        .send(PeerCommand::RequestPiece {
            piece: 42,
            chunks_per_piece: 2,
        })
        .await
        .unwrap();

    let expected = [chunk_request(42, 0).encode(), chunk_request(42, 1).encode()].concat();
    let mut got = vec![0u8; expected.len()];
    server.read_exact(&mut got).await.unwrap();
    assert_eq!(got, expected);

    drop(command_tx);
    worker.await.unwrap();
}

#[tokio::test]
async fn peer_worker_emits_messages_from_session() {
    use tokio::io::AsyncWriteExt;

    let (client, mut server) = tokio::io::duplex(4096);
    let session = PeerSession::new(client).with_timeout(Duration::from_millis(250));
    let addr: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();
    let (command_tx, command_rx) = mpsc::channel(4);
    let (event_tx, mut event_rx) = mpsc::channel(4);
    let worker = tokio::spawn(peer_worker(
        7,
        addr,
        session,
        command_rx,
        event_tx,
        Arc::new(AtomicU64::new(0)),
    ));

    server
        .write_all(&PeerMessage::Unchoke.encode())
        .await
        .unwrap();
    match event_rx.recv().await.unwrap() {
        PeerEvent::Message {
            peer_id,
            addr: event_addr,
            msg,
        } => {
            assert_eq!(peer_id, 7);
            assert_eq!(event_addr, addr);
            assert_eq!(msg, PeerMessage::Unchoke);
        }
        other => panic!("unexpected event: {other:?}"),
    }

    command_tx.send(PeerCommand::Stop).await.unwrap();
    worker.await.unwrap();
}

fn test_pool_peer(addr: SocketAddrV4) -> (PeerRuntime, mpsc::Receiver<PeerCommand>) {
    let (commands, receiver) = mpsc::channel(8);
    (
        PeerRuntime {
            addr,
            min_piece: 7,
            max_piece: 8,
            unchoked_peer: false,
            produced_output: false,
            seen_ids: HashSet::new(),
            commands,
            worker: tokio::spawn(std::future::pending()),
        },
        receiver,
    )
}

#[tokio::test]
async fn timed_out_piece_retries_when_all_peer_slots_are_full() {
    let policy = LiveRecoveryConfig {
        max_piece_advance: 1,
        ..default_live_recovery()
    };
    let (mut c, _) = Continuity::fresh(&info(), 7, 7, 0, policy);
    let addr = "127.0.0.1:1".parse().unwrap();
    let (peer, mut commands) = test_pool_peer(addr);
    let mut peers = BTreeMap::from([(1, peer)]);
    c.register_active_peer(1, addr, live_pos(7, 7));
    c.set_peer_unchoked(1, true);
    advance_pool_requests(&mut peers, &mut c, 32).await;
    assert!(matches!(
        commands.try_recv(),
        Ok(PeerCommand::RequestPiece { piece: 7, .. })
    ));
    let now = Instant::now() + policy.request_timeout();
    retransmit_stalled_requests(&mut peers, &mut c, 32, now).await;
    let retry = commands.try_recv();
    shutdown_peer_runtimes(&mut peers);
    assert!(
        matches!(retry, Ok(PeerCommand::RequestPiece { piece: 7, .. })),
        "timeout must queue a retry even when every peer was at capacity"
    );
}

#[tokio::test]
async fn timed_out_piece_still_prefers_a_peer_with_spare_capacity() {
    let policy = LiveRecoveryConfig {
        max_piece_advance: 1,
        ..default_live_recovery()
    };
    let (mut c, _) = Continuity::fresh(&info(), 7, 7, 0, policy);
    let addr = "127.0.0.1:1".parse().unwrap();
    let (peer, mut original) = test_pool_peer(addr);
    let mut peers = BTreeMap::from([(1, peer)]);
    c.register_active_peer(1, addr, live_pos(7, 7));
    c.set_peer_unchoked(1, true);
    advance_pool_requests(&mut peers, &mut c, 32).await;
    original.try_recv().unwrap();
    let (peer, mut faster) = test_pool_peer(addr);
    peers.insert(2, peer);
    c.register_active_peer(2, addr, live_pos(7, 7));
    c.set_peer_unchoked(2, true);
    retransmit_stalled_requests(
        &mut peers,
        &mut c,
        32,
        Instant::now() + policy.request_timeout(),
    )
    .await;
    let retry = faster.try_recv();
    let unexpected = original.try_recv();
    shutdown_peer_runtimes(&mut peers);
    assert!(matches!(
        retry,
        Ok(PeerCommand::RequestPiece { piece: 7, .. })
    ));
    assert!(
        unexpected.is_err(),
        "normal retry should still prefer spare peer capacity"
    );
    assert_eq!(
        c.active_peers.in_flight_count(1),
        1,
        "original late response stays tracked"
    );
}

#[tokio::test]
async fn rejected_piece_releases_duplicate_assignments_without_dropping_other_requests() {
    for rejecting_peer_choked in [true, false] {
        let policy = LiveRecoveryConfig {
            max_piece_advance: 2,
            ..default_live_recovery()
        };
        let (mut c, _) = Continuity::fresh(&info(), 7, 8, 1, policy);
        let addr = "127.0.0.1:1".parse().unwrap();
        let (peer, mut original) = test_pool_peer(addr);
        let mut peers = BTreeMap::from([(1, peer)]);
        c.register_active_peer(1, addr, live_pos(7, 8));
        c.set_peer_unchoked(1, true);
        advance_pool_requests(&mut peers, &mut c, 32).await;
        original.try_recv().unwrap(); // piece 7
        original.try_recv().unwrap(); // unrelated piece 8
        let (peer, mut rejecting) = test_pool_peer(addr);
        peers.insert(2, peer);
        c.register_active_peer(2, addr, live_pos(7, 7));
        c.set_peer_unchoked(2, true);
        let now = Instant::now() + policy.request_timeout();
        c.requested_at.insert(8, c.output_clock.now(now));
        retransmit_stalled_requests(&mut peers, &mut c, 32, now).await;
        assert!(matches!(
            rejecting.try_recv(),
            Ok(PeerCommand::RequestPiece { piece: 7, .. })
        ));
        assert_eq!(c.active_peers.in_flight_count(1), 2);
        if rejecting_peer_choked {
            c.set_peer_unchoked(2, false);
        } else {
            c.update_peer_window(2, 8, 9);
        }
        c.received_chunks.insert(7, HashSet::from([0]));
        c.received_chunks.insert(8, HashSet::from([0]));
        c.release_rejected_piece(7);
        assert!(!c.received_chunks.contains_key(&7));
        assert!(!c.requested_at.contains_key(&7));
        assert_eq!(c.received_chunks.get(&8).unwrap().len(), 1);
        assert_eq!(c.requested_at.get(&8), Some(&c.output_clock.now(now)));
        advance_pool_requests(&mut peers, &mut c, 32).await;
        let retry = original.try_recv();
        let unexpected = rejecting.try_recv();
        shutdown_peer_runtimes(&mut peers);
        assert!(matches!(
            retry,
            Ok(PeerCommand::RequestPiece { piece: 7, .. })
        ));
        assert!(unexpected.is_err());
        assert_eq!(
            c.active_peers.in_flight_count(1),
            2,
            "piece 8 remains outstanding"
        );
        assert_eq!(
            c.active_peers.in_flight_count(2),
            0,
            "all rejected copies must release"
        );
    }
}

#[test]
fn rejected_malformed_block_resets_partial_bytes_with_chunk_counts() {
    let mut stream = info();
    stream.piece_length = 8;
    stream.chunk_length = 4;
    stream.sig_len = 0;
    let (mut c, _) = Continuity::fresh(&stream, 7, 7, 0, default_live_recovery());
    c.reasm.add_block(7, 0, &[1, 2, 3, 4]).unwrap();
    assert!(!c.note_chunk(7, 0, 2));
    assert!(c.reasm.add_block(7, 8, &[9]).is_err());
    c.release_rejected_piece(7);
    assert_eq!(c.reasm.next_needed(), 7);
    c.reasm.add_block(7, 4, &[5, 6, 7, 8]).unwrap();
    assert!(!c.note_chunk(7, 1, 2));
    assert!(
        c.reasm.take_ready().is_empty(),
        "retry must not use discarded partial bytes"
    );
    c.reasm.add_block(7, 0, &[1, 2, 3, 4]).unwrap();
    assert!(c.note_chunk(7, 0, 2));
    assert_eq!(c.reasm.take_ready(), vec![1, 2, 3, 4, 5, 6, 7, 8]);
}

#[tokio::test]
async fn timed_out_piece_without_eligible_peer_retries_on_unchoke_and_keeps_partial_data() {
    let mut stream = info();
    stream.piece_length = 8;
    stream.chunk_length = 4;
    stream.sig_len = 0;
    let policy = LiveRecoveryConfig {
        max_piece_advance: 1,
        ..default_live_recovery()
    };
    let (mut c, _) = Continuity::fresh(&stream, 7, 7, 0, policy);
    let addr = "127.0.0.1:1".parse().unwrap();
    let (peer, mut commands) = test_pool_peer(addr);
    let mut peers = BTreeMap::from([(1, peer)]);
    c.register_active_peer(1, addr, live_pos(7, 7));
    c.set_peer_unchoked(1, true);
    advance_pool_requests(&mut peers, &mut c, 2).await;
    commands.try_recv().unwrap();
    c.reasm.add_block(7, 0, &[1, 2, 3, 4]).unwrap();
    assert!(!c.note_chunk(7, 0, 2));
    c.set_peer_unchoked(1, false);
    retransmit_stalled_requests(
        &mut peers,
        &mut c,
        2,
        Instant::now() + policy.request_timeout(),
    )
    .await;
    assert!(
        commands.try_recv().is_err(),
        "choked peer cannot receive retry requests"
    );
    assert_eq!(c.active_peers.in_flight_count(1), 0);
    assert!(!c.requested_at.contains_key(&7));
    assert_eq!(c.received_chunks.get(&7).unwrap().len(), 1);
    c.set_peer_unchoked(1, true);
    advance_pool_requests(&mut peers, &mut c, 2).await;
    let retry = commands.try_recv();
    shutdown_peer_runtimes(&mut peers);
    assert!(matches!(
        retry,
        Ok(PeerCommand::RequestPiece {
            piece: 7,
            chunks_per_piece: 2
        })
    ));
    c.reasm.add_block(7, 4, &[5, 6, 7, 8]).unwrap();
    assert!(c.note_chunk(7, 1, 2));
    assert_eq!(c.reasm.take_ready(), vec![1, 2, 3, 4, 5, 6, 7, 8]);
}

#[test]
fn active_peers_assign_missing_pieces_to_unchoked_peers_with_coverage() {
    let mut active = ActivePeers::new();
    let addr1: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();
    let addr2: SocketAddrV4 = "1.2.3.5:8621".parse().unwrap();
    active.insert(1, addr1, live_pos(100, 102));
    active.insert(2, addr2, live_pos(103, 105));
    active.set_unchoked(1, true);
    active.set_unchoked(2, true);
    let mut scheduler = Scheduler::new(2);

    let assigned = active.assign(&mut scheduler, 101, 105);

    assert_eq!(
        assigned,
        vec![
            PeerAssignment {
                peer_id: 1,
                piece: 101
            },
            PeerAssignment {
                peer_id: 1,
                piece: 102
            },
            PeerAssignment {
                peer_id: 2,
                piece: 103
            },
            PeerAssignment {
                peer_id: 2,
                piece: 104
            },
        ]
    );
    assert_eq!(active.in_flight_count(1), 2);
    assert_eq!(active.in_flight_count(2), 2);
}

#[test]
fn active_peer_drop_returns_its_in_flight_pieces_for_requeue() {
    let mut active = ActivePeers::new();
    let addr1: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();
    let addr2: SocketAddrV4 = "1.2.3.5:8621".parse().unwrap();
    active.insert(1, addr1, live_pos(100, 105));
    active.insert(2, addr2, live_pos(100, 105));
    active.set_unchoked(1, true);
    active.set_unchoked(2, true);
    let mut scheduler = Scheduler::new(4);
    let assigned = active.assign(&mut scheduler, 100, 105);
    assert!(assigned.iter().any(|a| a.peer_id == 1));

    let dropped = active.remove(1);
    for piece in &dropped {
        scheduler.on_drop(*piece);
    }

    assert!(!dropped.is_empty());
    assert_eq!(active.in_flight_count(1), 0);
    let reassigned = active.assign(&mut scheduler, 100, 105);
    assert!(
        reassigned.iter().any(|a| dropped.contains(&a.piece)),
        "dropped peer pieces should be eligible for reassignment"
    );
    assert!(reassigned.iter().all(|a| a.peer_id == 2));
}

#[test]
fn pool_refill_candidates_skip_active_and_cooling_peers() {
    let peers = addrs(&[1, 2, 3, 4]);
    let active: HashSet<SocketAddrV4> = addrs(&[2]).into_iter().collect();
    let mut candidates = SessionCandidates::default();
    for addr in peers {
        candidates.learn(addr, CandidateKind::Discovered);
    }
    candidates.failed(addrs(&[3])[0], Instant::now());
    assert_eq!(
        pool_refill_candidates(&candidates.eligible_discovered(Instant::now()), &active),
        addrs(&[1, 4])
    );
}

#[test]
fn peer_connect_stats_reports_failure_classes() {
    let mut stats = PeerConnectStats::default();
    stats.record_failure(PeerConnectFailure {
        addr: addrs(&[1])[0],
        stage: PeerConnectStage::Tcp,
        timing: PeerConnectTiming::default(),
    });
    stats.record_failure(PeerConnectFailure {
        addr: addrs(&[2])[0],
        stage: PeerConnectStage::Handshake,
        timing: PeerConnectTiming::default(),
    });
    stats.record_failure(PeerConnectFailure {
        addr: addrs(&[3])[0],
        stage: PeerConnectStage::Window,
        timing: PeerConnectTiming::default(),
    });
    stats.record_failure(PeerConnectFailure {
        addr: addrs(&[4])[0],
        stage: PeerConnectStage::Window,
        timing: PeerConnectTiming::default(),
    });

    assert_eq!(stats.summary(), "attempted=4 tcp=1 handshake=1 window=2");
}

#[test]
fn newly_discovered_refill_candidates_are_deduped_against_known_peers() {
    let mut candidates = SessionCandidates::default();
    for addr in addrs(&[1, 2]) {
        candidates.learn(addr, CandidateKind::Discovered);
    }
    let before = candidates.all();
    for addr in addrs(&[2, 3, 1, 4, 3]) {
        candidates.learn(addr, CandidateKind::Discovered);
    }
    let after = candidates.all();
    assert_eq!(
        after
            .iter()
            .copied()
            .filter(|addr| !before.contains(addr))
            .collect::<Vec<_>>(),
        addrs(&[3, 4])
    );
    assert_eq!(after.len(), 4);
}

#[test]
fn background_discovery_options_leave_stale_timer_margin() {
    let opts = background_discovery_options();
    assert!(opts.peer_target > 8);
    assert!(opts.dht_budget < default_live_recovery().stale_upstream_timeout());
}

#[test]
fn upstream_selection_prefers_the_freshest_live_head() {
    let stale = WireLivePosition {
        min_piece: 100,
        max_piece: 120,
        position: 120,
        distance_from_source: 1,
    };
    let fresh = WireLivePosition {
        min_piece: 105,
        max_piece: 124,
        position: 124,
        distance_from_source: 3,
    };

    assert!(
        prefer_window(&fresh, &stale),
        "a newer advertised head should beat a lower distance value"
    );
    assert!(!prefer_window(&stale, &fresh));
}

#[test]
fn upstream_selection_uses_distance_as_a_tiebreaker() {
    let near = WireLivePosition {
        min_piece: 100,
        max_piece: 120,
        position: 120,
        distance_from_source: 1,
    };
    let far = WireLivePosition {
        min_piece: 90,
        max_piece: 120,
        position: 120,
        distance_from_source: 4,
    };

    assert!(prefer_window(&near, &far));
    assert!(!prefer_window(&far, &near));
}

#[test]
fn recovery_channel_capacities_are_checked() {
    let policy = LiveRecoveryConfig {
        max_active_upstreams: 2,
        ..LiveRecoveryConfig::default()
    };
    assert_eq!(recovery_channel_capacities(policy).unwrap(), (64, 2));

    let invalid = LiveRecoveryConfig {
        max_active_upstreams: usize::MAX,
        ..LiveRecoveryConfig::default()
    };
    assert!(recovery_channel_capacities(invalid).is_err());
}
