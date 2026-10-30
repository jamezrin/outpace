const TS_PACKET_LEN: usize = 188;
const TS_SYNC: u8 = 0x47;
use super::*;
use std::cell::RefCell;

fn ts(pid: u16, pusi: bool, rai: bool, payload: &[u8]) -> Vec<u8> {
    let mut p = vec![0xFFu8; TS_PACKET_LEN];
    p[0] = TS_SYNC;
    p[1] = ((pid >> 8) as u8 & 0x1F) | if pusi { 0x40 } else { 0 };
    p[2] = (pid & 0xFF) as u8;
    let payload_start = if rai {
        // adaptation field (1 byte length + 1 flags byte) + payload, AFC = 11
        p[3] = 0x30;
        p[4] = 1; // adaptation_field_length = just the flags byte
        p[5] = 0x40; // random_access_indicator
        6
    } else {
        p[3] = 0x10; // AFC = 01, payload only
        4
    };
    let n = payload.len().min(TS_PACKET_LEN - payload_start);
    p[payload_start..payload_start + n].copy_from_slice(&payload[..n]);
    p
}
fn psi(pid: u16, section: &[u8]) -> Vec<u8> {
    let mut payload = vec![0u8]; // pointer_field = 0
    payload.extend_from_slice(section);
    ts(pid, true, false, &payload)
}
fn pat(pmt_pid: u16) -> Vec<u8> {
    // section after table_id: tsid(2) + flags(1) + sec#(1) + last#(1) + 1 program(4) + crc(4)
    let section_length = 5 + 4 + 4;
    let mut s = vec![0x00u8]; // table_id = PAT
    s.push(0xB0 | ((section_length >> 8) as u8 & 0x0F));
    s.push((section_length & 0xFF) as u8);
    s.extend_from_slice(&[0x00, 0x01]); // transport_stream_id
    s.push(0xC1); // version 0, current
    s.push(0x00); // section_number
    s.push(0x00); // last_section_number
    s.extend_from_slice(&[0x00, 0x01]); // program_number = 1
    s.extend_from_slice(&[0xE0 | ((pmt_pid >> 8) as u8 & 0x1F), (pmt_pid & 0xFF) as u8]);
    s.extend_from_slice(&[0, 0, 0, 0]); // CRC32 placeholder
    psi(0, &s)
}
fn pmt(pmt_pid: u16, video_pid: u16) -> Vec<u8> {
    pmt_codec(pmt_pid, video_pid, 0x1B) // H.264
}
fn pmt_codec(pmt_pid: u16, video_pid: u16, stream_type: u8) -> Vec<u8> {
    // after table_id: prog#(2)+flags(1)+sec#(1)+last#(1)+pcrpid(2)+pinfolen(2)+stream(5)+crc(4)
    let section_length = 5 + 4 + 5 + 4;
    let mut s = vec![0x02u8]; // table_id = PMT
    s.push(0xB0 | ((section_length >> 8) as u8 & 0x0F));
    s.push((section_length & 0xFF) as u8);
    s.extend_from_slice(&[0x00, 0x01]); // program_number
    s.push(0xC1); // version 0, current
    s.push(0x00); // section_number
    s.push(0x00); // last_section_number
    s.extend_from_slice(&[
        0xE0 | ((video_pid >> 8) as u8 & 0x1F),
        (video_pid & 0xFF) as u8,
    ]); // PCR_PID = video
    s.extend_from_slice(&[0xF0, 0x00]); // program_info_length = 0
    s.push(stream_type);
    s.extend_from_slice(&[
        0xE0 | ((video_pid >> 8) as u8 & 0x1F),
        (video_pid & 0xFF) as u8,
    ]);
    s.extend_from_slice(&[0xF0, 0x00]); // ES_info_length = 0
    s.extend_from_slice(&[0, 0, 0, 0]); // CRC32 placeholder
    psi(pmt_pid, &s)
}
fn pes_nal(nal: &[u8]) -> Vec<u8> {
    let mut pes = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
    pes.extend_from_slice(&[0x00, 0x00, 0x01]);
    pes.extend_from_slice(nal);
    pes
}

#[derive(Default, Debug)]
struct Turn {
    prepares: usize,
    enqueues: usize,
    packets: usize,
    transitions: usize,
    pending: usize,
    prepare_us: u128,
    retained_bytes: usize,
    segments: usize,
    causal_refs: usize,
}

tokio::task_local! { static COUNTS: RefCell<Vec<Turn>>; }

pub(super) fn turn() {
    let _ = COUNTS.try_with(|v| v.borrow_mut().push(Turn::default()));
}
fn count(f: impl FnOnce(&mut Turn)) {
    let _ = COUNTS.try_with(|v| f(v.borrow_mut().last_mut().expect("actor turn")));
}
pub(super) fn pending() {
    count(|v| v.pending += 1);
}
pub(super) fn prepared_in(d: Duration) {
    count(|v| v.prepare_us += d.as_micros());
}
pub(super) fn retained(c: &Continuity) {
    count(|v| {
        v.retained_bytes = c.pending_output.iter().map(|o| o.bytes.len()).sum();
        v.segments = c.pending_output.len();
        v.causal_refs = c.pending_credit.iter().map(Vec::len).sum();
    });
}
pub(super) fn prepare() {
    count(|v| v.prepares += 1);
}
pub(super) fn enqueue() {
    count(|v| v.enqueues += 1);
}
pub(super) fn stats((packets, transitions): (usize, usize)) {
    count(|v| {
        v.packets += packets;
        v.transitions += transitions;
    });
}

#[derive(Default)]
struct Observer {
    complete: std::sync::atomic::AtomicBool,
    notifications: tokio::sync::Notify,
}
impl LiveObserver for Observer {
    fn received_bytes(&self, _: u64) {
        assert!(
            !self.complete.load(Ordering::Relaxed),
            "extraction must synchronously invalidate native completeness before metrics receipt"
        );
    }
    fn publish_position(&self, position: Option<LivePosition>, _: u64) {
        let complete = position.is_some_and(|p| p.buffer_complete);
        self.complete.store(complete, Ordering::Relaxed);
        if complete {
            self.complete.store(true, Ordering::Relaxed);
            self.notifications.notify_one();
        }
    }
    fn clear_active_coverage(&self) {}
}

// No sockets or sleeps: consumer capacity and observer notification are barriers.
async fn schedule(piece_length: usize, pieces: usize, full: bool, close: bool) {
    let info = StreamInfo {
        infohash: [0; 20],
        piece_length: piece_length as u64,
        chunk_length: piece_length as u64,
        trackers: vec![],
        metadata: Default::default(),
        sig_len: 0,
        source_pubkey: vec![],
    };
    let observer = Arc::new(Observer::default());
    let seed = SeedConfig {
        metrics: Some(observer.clone()),
        registry: SeedRegistry::new(),
        store_factory: test_store_factory(4096, None),
        enabled: true,
        prefetch_pieces: 0,
        live_recovery: LiveRecoveryConfig::default(),
        warm_peers: WarmPeerCache::memory(),
    };
    let identity = Identity::generate();
    let store = Arc::new(tokio::sync::Mutex::new(PieceStore::new(
        piece_length as u64,
        piece_length as u64,
        4096,
    )));
    let (mut c, _) = Continuity::fresh(&info, 0, 0, 0, seed.live_recovery);
    c.observer = Some(observer.clone());
    c.startup_deadline = None;
    let mut packet = vec![0xff; piece_length];
    for p in packet.as_chunks_mut::<188>().0 {
        p[0..4].copy_from_slice(&[0x47, 0x1f, 0xff, 0x10]);
    }
    if piece_length == 8 * 1_048_576 {
        let pattern = [
            pat(0x100),
            pmt(0x100, 0x101),
            ts(0x101, true, false, &pes_nal(&[0x65, 2])),
            ts(0x101, false, false, &[8; 30]),
        ]
        .concat();
        for (i, byte) in packet.iter_mut().enumerate() {
            *byte = pattern[i % pattern.len()];
        }
        c.output_gate = ace_media::mpegts::KeyframeGate::new();
    }
    for i in 0..pieces {
        c.piece_producers.insert(
            i as u64,
            Some(SocketAddrV4::new([127, 0, 0, 1].into(), 1234)),
        );
        c.reasm
            .add_live_chunk(i as u64, 0, 1, [0; 8], &packet)
            .unwrap();
    }
    let (tx, mut rx) = mpsc::channel(1);
    if full {
        tx.try_send(LiveOutput {
            bytes: Bytes::from_static(b"barrier"),
            discontinuity: false,
        })
        .unwrap();
    }
    let downloaded = Arc::new(AtomicU64::new(0));
    let uploaded = Arc::new(AtomicU64::new(0));
    let served = Arc::new(AtomicU32::new(0));
    let count = Arc::new(AtomicU32::new(0));
    let mut candidates = SessionCandidates::default();
    let mut discovery = None;
    let (event_tx, event_rx) = mpsc::channel(16);
    let control_events = event_tx.clone();
    let addr = SocketAddrV4::new([127, 0, 0, 1].into(), 1234);
    let (commands, mut commands_rx) = mpsc::channel(16);
    let worker = tokio::spawn(std::future::pending::<()>());
    let worker_abort = worker.abort_handle();
    let mut peers = BTreeMap::from([(
        1,
        PeerRuntime {
            addr,
            min_piece: 0,
            max_piece: 0,
            unchoked_peer: false,
            produced_output: false,
            seen_ids: HashSet::new(),
            commands,
            worker,
        },
    )]);
    // A finished worker with a finite queued prefix competes with output/stats.
    let (closed_commands, closed_rx) = mpsc::channel(1);
    drop(closed_rx);
    let closed_worker = tokio::spawn(async {});
    while !closed_worker.is_finished() {
        tokio::task::yield_now().await;
    }
    peers.insert(
        2,
        PeerRuntime {
            addr,
            min_piece: 0,
            max_piece: 0,
            unchoked_peer: false,
            produced_output: false,
            seen_ids: HashSet::new(),
            commands: closed_commands,
            worker: closed_worker,
        },
    );
    for _ in 0..8 {
        event_tx
            .try_send(PeerEvent::Message {
                peer_id: 2,
                addr,
                msg: PeerMessage::KeepAlive,
            })
            .unwrap();
    }
    let (pex_tx, refill_rx) = mpsc::channel(1);
    let actor = PoolActor {
        output_reservation: None,
        activation: None,
        reciprocal: Default::default(),
        publication: None,
        gossip: None,
        closed_peer_events: BTreeMap::new(),
        observed_retry: Instant::now(),
        observed_capacity: (0, 0),
        refill_closed: true,
        next_peer_id: 1,
        loss_count: 0,
        peers,
        learned_pending: HashSet::new(),
        learned_connects: tokio::task::JoinSet::new(),
        pex_tx,
        refill_rx,
        event_rx,
        event_tx,
        _stats_guard: LiveStatsGuard(None),
        live_recovery: seed.live_recovery,
        info: &info,
        identity: &identity,
        chunks_per_piece: 1,
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
        continuity: &mut c,
    };
    let start = Instant::now();
    COUNTS.scope(RefCell::new(vec![]), async {
        let run = actor.run();
        tokio::pin!(run);
        if full {
            tokio::select! {
                _end = &mut run => panic!("actor ended before full-channel scan"),
                _ = observer.notifications.notified() => {}
            }
            assert!(observer.complete.load(Ordering::Relaxed));
            COUNTS.with(|v| assert!(v.borrow().iter().any(|t| t.pending > 0), "real reserve Pending"));
            assert_eq!(downloaded.load(Ordering::Relaxed), 0);
            // Completed statistics must not leave an idle continuation wake.
            std::future::poll_fn(|cx| {
                assert!(run.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            }).await;
            let turns = COUNTS.with(|v| v.borrow().len());
            std::future::poll_fn(|cx| {
                assert!(run.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            }).await;
            assert_eq!(COUNTS.with(|v| v.borrow().len()), turns);
            control_events.send(PeerEvent::Message { peer_id: 1, addr, msg: PeerMessage::Interested }).await.unwrap();
            tokio::time::timeout(Duration::from_millis(250), async {
                loop {
                    tokio::select! {
                        _ = &mut run => panic!("actor ended before control"),
                        command = commands_rx.recv() => if matches!(command, Some(PeerCommand::Send(PeerMessage::Unchoke))) { break; }
                    }
                }
            }).await.expect("healthy Unchoke command within original 250ms while output Pending");
            if close { rx.close(); }
            else { assert_eq!(rx.recv().await.unwrap().bytes.as_ref(), b"barrier"); }
        }
        let mut retired = false;
        let mut output_bytes = 0;
        loop {
            tokio::select! {
                end = &mut run => { assert!(if close { matches!(end, FollowEnd::ConsumerGone) } else { matches!(end, FollowEnd::PeerLost(_)) }); break; }
                output = rx.recv(), if !close => {
                    let output = output.unwrap();
                    assert!(!output.bytes.is_empty());
                    output_bytes += output.bytes.len();
                    if !retired {
                        control_events.send(PeerEvent::Lost { peer_id: 1, addr }).await.unwrap();
                        retired = true;
                    }
                }
            }
        }
        while let Ok(output) = rx.try_recv() {
            if !close { output_bytes += output.bytes.len(); }
        }
        COUNTS.with(|v| {
            let v = v.borrow();
            assert!(!v.is_empty());
            for (i,t) in v.iter().enumerate() {
                assert!(t.prepares <= 1 && t.enqueues <= 1 && t.packets <= 2048 && t.transitions <= 64, "turn {i}: {t:?}");
            }
            assert_eq!(v.iter().map(|t|t.prepares).sum::<usize>(), if close { 0 } else { pieces });
            assert!(v.iter().map(|t|t.packets).sum::<usize>() >= pieces * (piece_length / 188));
            if piece_length == 8 * 1_048_576 {
                assert!(output_bytes > pieces * (piece_length / 188 * 188), "actual table insertion");
                assert!(v.iter().any(|t|t.causal_refs > 0));
            }
            eprintln!("retained_bytes_max={} segments_max={} causal_refs_max={} output_bytes={output_bytes}",v.iter().map(|t|t.retained_bytes).max().unwrap(),v.iter().map(|t|t.segments).max().unwrap(),v.iter().map(|t|t.causal_refs).max().unwrap());
            eprintln!("max_prepare_us={} reserve_pending={}",v.iter().map(|t|t.prepare_us).max().unwrap(),v.iter().map(|t|t.pending).sum::<usize>());
            eprintln!("actor schedule piece_bytes={piece_length} pieces={pieces} full={full} turns={} prepares={} enqueues={} packets={} transitions={} wall_us={}", v.len(), v.iter().map(|t|t.prepares).sum::<usize>(), v.iter().map(|t|t.enqueues).sum::<usize>(), v.iter().map(|t|t.packets).sum::<usize>(),v.iter().map(|t|t.transitions).sum::<usize>(),start.elapsed().as_micros());
        });
    }).await;
    worker_abort.abort();
}

#[tokio::test]
async fn actor_full_channel_metrics_eventually_complete() {
    tokio::time::timeout(
        Duration::from_secs(10),
        schedule(188 * 4096, 8, true, false),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn actor_maximum_piece_metrics_ready_backlog() {
    tokio::time::timeout(
        Duration::from_secs(10),
        schedule(8 * 1_048_576, 8, true, false),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn actor_full_channel_close_preserves_unextracted_output() {
    tokio::time::timeout(Duration::from_secs(10), schedule(188 * 4096, 8, true, true))
        .await
        .unwrap();
}

#[tokio::test]
async fn actor_capture_fence_freeze_and_retained_operation_recreation() {
    let piece_length = 188 * 8 + 48;
    let info = StreamInfo {
        infohash: [0; 20],
        piece_length: piece_length as u64,
        chunk_length: piece_length as u64,
        trackers: vec![],
        metadata: Default::default(),
        sig_len: 0,
        source_pubkey: vec![],
    };
    let observer = Arc::new(Observer::default());
    let seed = SeedConfig {
        metrics: Some(observer.clone()),
        registry: SeedRegistry::new(),
        store_factory: test_store_factory(4096, None),
        enabled: true,
        prefetch_pieces: 0,
        live_recovery: LiveRecoveryConfig::default(),
        warm_peers: WarmPeerCache::memory(),
    };
    let identity = Identity::generate();
    let store = Arc::new(tokio::sync::Mutex::new(PieceStore::new(
        piece_length as u64,
        piece_length as u64,
        4096,
    )));
    let (mut c, _) = Continuity::fresh(&info, 0, 0, 0, seed.live_recovery);
    c.observer = Some(observer.clone());
    c.startup_deadline = None;
    let mut packet = vec![0xff; piece_length];
    for p in packet.as_chunks_mut::<188>().0 {
        p[0..4].copy_from_slice(&[0x47, 0x1f, 0xff, 0x10]);
    }
    if true {
        let pattern = [
            pat(0x100),
            pmt(0x100, 0x101),
            ts(0x101, true, false, &pes_nal(&[0x65, 2])),
            ts(0x101, false, false, &[8; 30]),
        ]
        .concat();
        for (i, byte) in packet.iter_mut().enumerate() {
            *byte = pattern[i % pattern.len()];
        }
        c.output_gate = ace_media::mpegts::KeyframeGate::new();
    }
    let (tx, mut rx) = mpsc::channel(1);
    if true {
        tx.try_send(LiveOutput {
            bytes: Bytes::from_static(b"barrier"),
            discontinuity: false,
        })
        .unwrap();
    }
    let downloaded = Arc::new(AtomicU64::new(0));
    let uploaded = Arc::new(AtomicU64::new(0));
    let served = Arc::new(AtomicU32::new(0));
    let count = Arc::new(AtomicU32::new(0));
    let mut candidates = SessionCandidates::default();
    let mut discovery = None;
    let (event_tx, event_rx) = mpsc::channel(16);

    let addr = SocketAddrV4::new([127, 0, 0, 1].into(), 1234);
    let (commands, _commands_rx) = mpsc::channel(16);
    let worker = tokio::spawn(std::future::pending::<()>());
    let worker_abort = worker.abort_handle();
    let mut peers = BTreeMap::from([(
        1,
        PeerRuntime {
            addr,
            min_piece: 0,
            max_piece: 0,
            unchoked_peer: false,
            produced_output: false,
            seen_ids: HashSet::new(),
            commands,
            worker,
        },
    )]);
    // A finished worker with a finite queued prefix competes with output/stats.
    let (closed_commands, closed_rx) = mpsc::channel(1);
    drop(closed_rx);
    let closed_worker = tokio::spawn(async {});
    while !closed_worker.is_finished() {
        tokio::task::yield_now().await;
    }
    peers.insert(
        2,
        PeerRuntime {
            addr,
            min_piece: 0,
            max_piece: 0,
            unchoked_peer: false,
            produced_output: false,
            seen_ids: HashSet::new(),
            commands: closed_commands,
            worker: closed_worker,
        },
    );
    for _ in 0..8 {
        event_tx
            .try_send(PeerEvent::Message {
                peer_id: 2,
                addr,
                msg: PeerMessage::KeepAlive,
            })
            .unwrap();
    }
    let (pex_tx, refill_rx) = mpsc::channel(1);
    let mut actor = PoolActor {
        output_reservation: None,
        activation: None,
        reciprocal: Default::default(),
        publication: None,
        gossip: None,
        closed_peer_events: BTreeMap::new(),
        observed_retry: Instant::now(),
        observed_capacity: (0, 0),
        refill_closed: true,
        next_peer_id: 1,
        loss_count: 0,
        peers,
        learned_pending: HashSet::new(),
        learned_connects: tokio::task::JoinSet::new(),
        pex_tx,
        refill_rx,
        event_rx,
        event_tx,
        _stats_guard: LiveStatsGuard(None),
        live_recovery: seed.live_recovery,
        info: &info,
        identity: &identity,
        chunks_per_piece: 1,
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
        continuity: &mut c,
    };

    let message = |piece: u32| {
        let mut block = vec![0; 10];
        block.extend_from_slice(&packet);
        PeerMessage::Piece {
            index: 0,
            begin: piece,
            block,
        }
    };
    // Production receipt owns producer capture; no direct attribution tokens.
    for piece in 0..3 {
        actor.on_message(1, addr, message(piece)).await;
    }
    for _ in 0..3 {
        actor.start_publication();
        std::future::poll_fn(|cx| {
            if actor.poll_publication(cx) {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        })
        .await;
    }
    actor.start_output();
    std::future::poll_fn(|cx| {
        assert!(poll_output(&mut actor.output_reservation, cx).is_none());
        std::task::Poll::Ready(())
    })
    .await;
    let ceiling = actor.continuity.output_ceiling.unwrap();
    let floor = actor.continuity.request_floor.load(Ordering::Relaxed);
    let cursor = actor.continuity.reasm.next_needed();
    let requested = actor.continuity.requested_at.clone();
    let frozen = actor.continuity.output_clock.now(Instant::now());
    for piece in [ceiling - 1, ceiling, ceiling + 1] {
        actor
            .on_message(1, addr, ace_wire::live_codec::live_have(piece as u32))
            .await;
        actor.on_message(1, addr, message(piece as u32)).await;
    }
    assert!(actor
        .continuity
        .reasm
        .completed_live_piece(ceiling - 1)
        .is_some());
    assert!(actor
        .continuity
        .reasm
        .completed_live_piece(ceiling)
        .is_none());
    assert!(actor
        .continuity
        .reasm
        .completed_live_piece(ceiling + 1)
        .is_none());
    assert!(!actor.continuity.piece_producers.contains_key(&ceiling));
    assert!(retransmit_stalled_requests(
        &mut actor.peers,
        actor.continuity,
        1,
        Instant::now() + Duration::from_secs(600)
    )
    .await
    .is_empty());
    assert!(advance_pool_requests(&mut actor.peers, actor.continuity, 1)
        .await
        .is_empty());
    assert_eq!(
        actor.continuity.request_floor.load(Ordering::Relaxed),
        floor
    );
    assert_eq!(actor.continuity.reasm.next_needed(), cursor);
    assert_eq!(actor.continuity.requested_at, requested);
    assert_eq!(
        actor
            .continuity
            .output_clock
            .now(Instant::now() + Duration::from_secs(600)),
        frozen
    );
    drop(actor); // destroy the real reservation before extraction
    assert_eq!(c.resume(addr, ceiling + 10, ceiling + 20), cursor);
    assert_eq!(c.output_ceiling, Some(ceiling));
    assert_eq!(rx.recv().await.unwrap().bytes.as_ref(), b"barrier");
    let mut bytes = Vec::new();
    let mut markers = Vec::new();
    // Each cancellation happens at actual capacity Pending, preserving exact segments.
    loop {
        let before = c.emitted;
        let done = {
            let operation = drain_retained(
                &mut c,
                &mut candidates,
                &info,
                &seed,
                &store,
                &downloaded,
                &tx,
            );
            tokio::pin!(operation);
            std::future::poll_fn(|cx| match operation.as_mut().poll(cx) {
                std::task::Poll::Ready(done) => std::task::Poll::Ready(Some(done)),
                std::task::Poll::Pending if tx.capacity() == 0 => std::task::Poll::Ready(None),
                std::task::Poll::Pending => std::task::Poll::Pending,
            })
            .await
        };
        while let Ok(output) = rx.try_recv() {
            if output.discontinuity {
                markers.push(bytes.len());
            }
            bytes.extend_from_slice(&output.bytes);
        }
        assert_eq!(downloaded.load(Ordering::Relaxed), bytes.len() as u64);
        if done.is_some() {
            assert_eq!(done, Some(true));
            break;
        }
        assert!(
            c.emitted > before,
            "a reserved segment committed before cancellation"
        );
    }
    // Independent persistent adapter computes byte/marker oracle over the accepted prefix.
    let (mut expected, _) = Continuity::fresh(&info, 0, 0, 0, seed.live_recovery);
    expected.output_gate = ace_media::mpegts::KeyframeGate::new();
    let mut expected_bytes = Vec::new();
    let mut expected_markers = Vec::new();
    for _ in 0..3 {
        for output in expected.resync_output(&packet) {
            if output.discontinuity {
                expected_markers.push(expected_bytes.len());
            }
            expected_bytes.extend_from_slice(&output.bytes);
        }
    }
    assert_eq!(bytes, expected_bytes);
    assert_eq!(markers, expected_markers);
    assert!(!markers.is_empty());
    assert!(c.output_ceiling.is_none());
    worker_abort.abort();
}

// Supplemental review witness: real signed receipt and publication, actor enqueue effects.
#[tokio::test]
async fn review_authenticated_receipt_to_enqueue_credit() {
    review_credit(true, false, false, false).await;
}
#[tokio::test]
async fn review_unsigned_receipt_never_warms() {
    review_credit(false, false, false, false).await;
}
async fn review_credit(signed: bool, completions: bool, close: bool, publication: bool) {
    let auth = ace_wire::live_auth::LiveSourceAuth::generate();
    let packet = [
        pat(0x100),
        pmt(0x100, 0x101),
        ts(0x101, true, false, &pes_nal(&[0x65, 2])),
        ts(0x101, false, false, &[8; 30]),
    ]
    .concat();
    let mut wire = packet.clone();
    if signed {
        wire.extend(auth.sign(&packet));
    }
    let piece_length = wire.len();
    let info = StreamInfo {
        infohash: [0; 20],
        piece_length: piece_length as u64,
        chunk_length: piece_length as u64,
        trackers: vec![],
        metadata: Default::default(),
        sig_len: if signed { auth.signature_len() } else { 0 },
        source_pubkey: if signed { auth.pubkey_der() } else { vec![] },
    };
    let withheld = [
        pat(0x100),
        pmt(0x100, 0x101),
        ts(0x101, true, false, &pes_nal(&[0x67, 1])),
        ts(0x101, true, false, &pes_nal(&[0x67, 1])),
    ]
    .concat();
    let mut oracle = Continuity::fresh(&info, 0, 0, 0, LiveRecoveryConfig::default()).0;
    oracle.output_gate = ace_media::mpegts::KeyframeGate::new();
    assert!(oracle.resync_output(&withheld).is_empty());
    let expected: Vec<u8> = oracle
        .resync_output(&packet)
        .into_iter()
        .flat_map(|o| o.bytes.to_vec())
        .collect();
    let observer = Arc::new(Observer::default());
    let seed = SeedConfig {
        metrics: Some(observer.clone()),
        registry: SeedRegistry::new(),
        store_factory: test_store_factory(4096, None),
        enabled: true,
        prefetch_pieces: 0,
        live_recovery: LiveRecoveryConfig::default(),
        warm_peers: WarmPeerCache::new(std::path::Path::new(""), true),
    };
    let identity = Identity::generate();
    let store = Arc::new(tokio::sync::Mutex::new(PieceStore::new(
        piece_length as u64,
        piece_length as u64,
        4096,
    )));
    let (mut c, _) = Continuity::fresh(&info, 0, 0, 0, seed.live_recovery);
    c.observer = Some(observer.clone());
    c.startup_deadline = None;
    let (tx, mut rx) = mpsc::channel(1);
    if true {
        tx.try_send(LiveOutput {
            bytes: Bytes::from_static(b"barrier"),
            discontinuity: false,
        })
        .unwrap();
    }
    let downloaded = Arc::new(AtomicU64::new(0));
    let uploaded = Arc::new(AtomicU64::new(0));
    let served = Arc::new(AtomicU32::new(0));
    let count = Arc::new(AtomicU32::new(0));
    let mut candidates = SessionCandidates::default();
    let mut discovery = None;
    let (event_tx, event_rx) = mpsc::channel(16);

    let addr = SocketAddrV4::new([127, 0, 0, 1].into(), 1234);
    let (commands, mut commands_rx) = mpsc::channel(16);
    let worker = tokio::spawn(std::future::pending::<()>());
    let worker_abort = worker.abort_handle();
    let peers = BTreeMap::from([(
        1,
        PeerRuntime {
            addr,
            min_piece: 0,
            max_piece: 0,
            unchoked_peer: false,
            produced_output: false,
            seen_ids: HashSet::new(),
            commands,
            worker,
        },
    )]);
    let (pex_tx, refill_rx) = mpsc::channel(1);
    let mut actor = PoolActor {
        output_reservation: None,
        activation: None,
        reciprocal: Default::default(),
        publication: None,
        gossip: None,
        closed_peer_events: BTreeMap::new(),
        observed_retry: Instant::now(),
        observed_capacity: (0, 0),
        refill_closed: true,
        next_peer_id: 1,
        loss_count: 0,
        peers,
        learned_pending: HashSet::new(),
        learned_connects: tokio::task::JoinSet::new(),
        pex_tx,
        refill_rx,
        event_rx,
        event_tx,
        _stats_guard: LiveStatsGuard(None),
        live_recovery: seed.live_recovery,
        info: &info,
        identity: &identity,
        chunks_per_piece: 1,
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
        continuity: &mut c,
    };

    let other = SocketAddrV4::new([127, 0, 0, 2].into(), 1234);
    let (other_commands, _other_rx) = mpsc::channel(16);
    actor.peers.insert(
        3,
        PeerRuntime {
            addr: other,
            min_piece: 0,
            max_piece: 0,
            unchoked_peer: false,
            produced_output: false,
            seen_ids: HashSet::new(),
            commands: other_commands,
            worker: tokio::spawn(std::future::pending()),
        },
    );
    for a in [addr, other] {
        actor.candidates.learn(a, CandidateKind::Discovered);
        actor.candidates.explored(a);
    }
    actor.continuity.output_gate = ace_media::mpegts::KeyframeGate::new();
    let mut withheld_wire = withheld.clone();
    if signed {
        withheld_wire.extend(auth.sign(&withheld));
    }
    let mut block = vec![0; 10];
    block.extend(withheld_wire);
    actor
        .on_message(
            3,
            other,
            PeerMessage::Piece {
                index: 0,
                begin: 0,
                block,
            },
        )
        .await;
    while actor.publication.is_some() {
        std::future::poll_fn(|cx| {
            if actor.poll_publication(cx) {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        })
        .await;
    }
    assert_eq!(actor.continuity.piece_producers.get(&0), Some(&Some(other)));
    assert_eq!(rx.recv().await.unwrap().bytes.as_ref(), b"barrier");
    actor.start_output();
    let result = std::future::poll_fn(|cx| match poll_output(&mut actor.output_reservation, cx) {
        Some(v) => std::task::Poll::Ready(v),
        None => std::task::Poll::Pending,
    })
    .await;
    assert!(finish_output_step(
        actor.continuity,
        actor.candidates,
        &mut actor.peers,
        &info,
        &seed,
        &downloaded,
        &tx,
        result
    )
    .is_none());
    assert!(actor.continuity.pending_output.is_empty());
    assert_eq!(downloaded.load(Ordering::Relaxed), 0);
    assert!(seed.warm_peers.hints(&info.infohash).is_empty());
    assert!(actor.peers.values().all(|p| !p.produced_output));
    tx.try_send(LiveOutput {
        bytes: Bytes::from_static(b"barrier"),
        discontinuity: false,
    })
    .unwrap();
    let mut block = vec![0; 10];
    block.extend_from_slice(&wire);
    actor
        .on_message(
            1,
            addr,
            PeerMessage::Piece {
                index: 0,
                begin: 1,
                block,
            },
        )
        .await;
    assert_eq!(actor.continuity.piece_producers.get(&1), Some(&Some(addr)));
    while actor.publication.is_some() {
        std::future::poll_fn(|cx| {
            if actor.poll_publication(cx) {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        })
        .await;
    }
    assert!(store.lock().await.has_piece(1));
    if publication {
        // Actual parser prepares the older segment; actor owns the newer
        // publication completion while this segment has no consumer capacity.
        let older = actor.continuity.resync_output(&packet);
        assert!(!older.is_empty());
        let older_bytes: Vec<_> = older.iter().flat_map(|o| o.bytes.to_vec()).collect();
        actor.continuity.pending_output.extend(older);
        let mut block = vec![0; 10];
        block.extend_from_slice(&wire);
        actor
            .on_message(
                1,
                addr,
                PeerMessage::Piece {
                    index: 0,
                    begin: 2,
                    block,
                },
            )
            .await;
        let guard = store.lock().await;
        actor.start_output();
        std::future::poll_fn(|cx| {
            assert!(poll_output(&mut actor.output_reservation, cx).is_none());
            std::task::Poll::Ready(())
        })
        .await;
        assert!(actor.publication.is_some());
        std::future::poll_fn(|cx| {
            assert!(!actor.poll_publication(cx));
            std::task::Poll::Ready(())
        })
        .await;
        {
            let run = actor.run();
            tokio::pin!(run);
            std::future::poll_fn(|cx| {
                assert!(run.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            drop(guard);
            tokio::time::timeout(Duration::from_millis(250),async {loop {tokio::select! {_=&mut run=>panic!("early end"),guard=store.lock()=>{if guard.has_piece(2) {break;}}} tokio::task::yield_now().await;}}).await.unwrap();
            rx.close();
            assert!(matches!(
                tokio::time::timeout(Duration::from_millis(250), &mut run)
                    .await
                    .unwrap(),
                FollowEnd::ConsumerGone
            ));
        }
        assert!(store.lock().await.has_piece(2));
        assert_eq!(c.reasm.next_needed(), 1);
        assert_eq!(
            c.pending_output
                .iter()
                .flat_map(|o| o.bytes.to_vec())
                .collect::<Vec<_>>(),
            older_bytes
        );
        assert_eq!(downloaded.load(Ordering::Relaxed), 0);
        worker_abort.abort();
        return;
    }
    actor.start_output();
    std::future::poll_fn(|cx| {
        assert!(poll_output(&mut actor.output_reservation, cx).is_none());
        std::task::Poll::Ready(())
    })
    .await;
    for a in [addr, other] {
        assert_eq!(actor.candidates.explorations(a), 1);
    }
    assert!(actor.peers.values().all(|p| !p.produced_output));
    assert!(seed.warm_peers.hints(&info.infohash).is_empty());
    assert_eq!(actor.continuity.reasm.next_needed(), 1);

    if completions {
        // Completion seam only: real TCP write Pending is independently covered by
        // activation_obsolete_completion_preserves_current_peers_and_counters.
        // This owned future is genuinely Pending on a oneshot before run starts.
        let (release, wait) = tokio::sync::oneshot::channel();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let candidate = match listener.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!(),
        };
        let socket = TcpStream::connect(candidate).await.unwrap();
        let (_remote, _) = listener.accept().await.unwrap();
        actor.live_recovery.max_active_upstreams = 4;
        actor.continuity.startup_deadline = Some(Instant::now() + Duration::from_secs(60));
        actor.activation = Some(activation::Activation {
            addr: candidate,
            peer_id: 4,
            replacement: None,
            write: Box::pin(async move {
                wait.await.unwrap();
                Ok(ConnectedUpstream {
                    session: PeerSession::new(socket),
                    addr: candidate,
                    window: WireLivePosition {
                        min_piece: 0,
                        max_piece: 10,
                        position: 0,
                        distance_from_source: 1,
                    },
                    unchoked: false,
                    yourip: None,
                })
            }),
        });
        std::future::poll_fn(|cx| {
            assert!(actor.poll_activation(cx).is_none());
            std::task::Poll::Ready(())
        })
        .await;
        let guard = store.lock().await;
        actor.queue_reciprocal(1, &[0; 10]);
        actor.start_reciprocal();
        std::future::poll_fn(|cx| {
            assert!(reciprocal::poll_job(&mut actor.reciprocal, cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        actor
            .continuity
            .requested_at
            .insert(99, actor.continuity.output_clock.now(Instant::now()));
        let requested = actor.continuity.requested_at.clone();
        let cursor = actor.continuity.reasm.next_needed();
        let floor = actor.continuity.request_floor.load(Ordering::Relaxed);
        let frozen = actor.continuity.output_clock.now(Instant::now());
        // Closed transport with a queued final-event prefix.
        let (closed, closed_rx) = mpsc::channel(1);
        drop(closed_rx);
        let worker = tokio::spawn(async {});
        while !worker.is_finished() {
            tokio::task::yield_now().await;
        }
        actor.peers.insert(
            8,
            PeerRuntime {
                addr: other,
                min_piece: 0,
                max_piece: 0,
                unchoked_peer: false,
                produced_output: false,
                seen_ids: HashSet::new(),
                commands: closed,
                worker,
            },
        );
        for _ in 0..8 {
            actor
                .event_tx
                .try_send(PeerEvent::Message {
                    peer_id: 8,
                    addr: other,
                    msg: PeerMessage::KeepAlive,
                })
                .unwrap();
        }
        let events = actor.event_tx.clone();
        {
            let run = actor.run();
            tokio::pin!(run);
            std::future::poll_fn(|cx| {
                assert!(run.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            release.send(()).unwrap();
            drop(guard);
            tokio::time::timeout(Duration::from_millis(250),async {
                loop {tokio::select! { _=&mut run=>panic!("early end"), command=commands_rx.recv()=> if matches!(command,Some(PeerCommand::Send(PeerMessage::Piece{..}))) {break;} }}
                events.send(PeerEvent::Message{peer_id:1,addr,msg:PeerMessage::Interested}).await.unwrap();
                loop {tokio::select! { _=&mut run=>panic!("early end"), command=commands_rx.recv()=> if matches!(command,Some(PeerCommand::Send(PeerMessage::Unchoke))) {break;} }}
            }).await.unwrap();
            assert_eq!(tx.capacity(), 0);
            assert_eq!(downloaded.load(Ordering::Relaxed), 0);
            if close {
                rx.close();
                assert!(matches!(
                    tokio::time::timeout(Duration::from_millis(250), &mut run)
                        .await
                        .unwrap(),
                    FollowEnd::ConsumerGone
                ));
            }
            // Cancellation releases owned operations and worker guards.
        }
        assert!(
            c.startup_deadline.is_none(),
            "activation confirmed while held"
        );
        assert!(c.admitted_upstreams > 0, "actual admission");
        assert_eq!(c.reasm.next_needed(), cursor);
        assert_eq!(c.request_floor.load(Ordering::Relaxed), floor);
        assert_eq!(c.requested_at, requested);
        assert!(!requested.is_empty());
        assert_eq!(
            c.output_clock
                .now(Instant::now() + Duration::from_secs(600)),
            frozen
        );
        if close {
            worker_abort.abort();
            return;
        }
        assert_eq!(rx.recv().await.unwrap().bytes.as_ref(), b"barrier");
        assert!(
            drain_retained(
                &mut c,
                &mut candidates,
                &info,
                &seed,
                &store,
                &downloaded,
                &tx
            )
            .await
        );
        assert_eq!(rx.recv().await.unwrap().bytes.as_ref(), expected);
        worker_abort.abort();
        return;
    }
    assert_eq!(rx.recv().await.unwrap().bytes.as_ref(), b"barrier");
    let result = std::future::poll_fn(|cx| match poll_output(&mut actor.output_reservation, cx) {
        Some(v) => std::task::Poll::Ready(v),
        None => std::task::Poll::Pending,
    })
    .await;
    assert!(finish_output_step(
        actor.continuity,
        actor.candidates,
        &mut actor.peers,
        &info,
        &seed,
        &downloaded,
        &tx,
        result
    )
    .is_none());
    assert!(actor.peers[&1].produced_output);
    assert!(!actor.peers[&3].produced_output);
    assert_eq!(actor.candidates.explorations(addr), 0);
    assert_eq!(actor.candidates.explorations(other), 0);
    assert!(
        seed.warm_peers.hints(&info.infohash)
            == if signed {
                vec![(addr, CandidateKind::Discovered)]
            } else {
                vec![]
            }
    );
    assert_eq!(rx.recv().await.unwrap().bytes.as_ref(), expected);
    // Retained parser provenance comes from the accepted piece, not a forged token.
    let credit = actor
        .continuity
        .resync_credit
        .first()
        .expect("retained final packet provenance")
        .1
        .clone();
    actor.candidates.explored(addr);
    actor.candidates.explored(other);
    let segments = actor
        .continuity
        .resync_output_credited(&packet, Some(credit));
    actor.continuity.pending_output.extend(segments);
    assert!(!actor.continuity.pending_output.is_empty());
    tx.try_send(LiveOutput {
        bytes: Bytes::from_static(b"cancel"),
        discontinuity: false,
    })
    .unwrap();
    actor.start_output();
    std::future::poll_fn(|cx| {
        assert!(poll_output(&mut actor.output_reservation, cx).is_none());
        std::task::Poll::Ready(())
    })
    .await;
    actor.output_reservation = None; // cancel the actually Pending capacity operation
    rx.recv().await.unwrap();
    actor.start_output();
    let result = std::future::poll_fn(|cx| match poll_output(&mut actor.output_reservation, cx) {
        Some(v) => std::task::Poll::Ready(v),
        None => std::task::Poll::Pending,
    })
    .await;
    assert!(finish_output_step(
        actor.continuity,
        actor.candidates,
        &mut actor.peers,
        &info,
        &seed,
        &downloaded,
        &tx,
        result
    )
    .is_none());
    assert_eq!(
        actor.candidates.explorations(addr),
        1,
        "duplicate provenance must preserve later exploration"
    );
    assert_eq!(actor.candidates.explorations(other), 1);
    drop(actor);
    worker_abort.abort();
    // Keep the command receiver alive through receipt, avoiding incidental loss.
    let _ = commands_rx.try_recv();
}

#[tokio::test]
async fn review_owned_completions_under_real_output_pending() {
    review_credit(true, true, false, false).await;
}

#[tokio::test]
async fn review_owned_completions_close_under_pressure() {
    review_credit(true, true, true, false).await;
}

#[tokio::test]
async fn review_new_publication_completes_behind_prepared_segment() {
    review_credit(true, false, false, true).await;
}
