use super::connect_diagnostics::{BasicReplyCause, BasicReplyProgress};
use super::*;
use ace_peer::PeerError;
use ace_wire::handshake::HANDSHAKE_LEN;

fn reply(received: usize, cause: BasicReplyCause) -> Option<BasicReplyProgress> {
    Some(BasicReplyProgress {
        received,
        cause: Some(cause),
    })
}

#[test]
fn upstream_phase_summary_counts_timeouts_and_only_samples_reached_phases() {
    let addr = SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 1);
    let ms = |value| Some(Duration::from_millis(value));
    let mut stats = PeerConnectStats::default();
    for (phase, durations) in [
        (ConnectPhase::Connect, [ms(3000), None, None, None]),
        (ConnectPhase::HandshakeSend, [ms(100), ms(2900), None, None]),
        (
            ConnectPhase::HandshakeReply,
            [ms(200), ms(10), ms(2790), None],
        ),
        (ConnectPhase::Window, [ms(300), ms(20), ms(30), ms(2650)]),
    ] {
        stats.record_failure(PeerConnectFailure {
            addr,
            stage: PeerConnectStage::TotalTimeout,
            timing: PeerConnectTiming {
                phase,
                durations,
                reply: None,
            },
        });
    }
    stats.record_connected(PeerConnectTiming {
        phase: ConnectPhase::Window,
        durations: [ms(0), ms(0), ms(0), ms(0)],
        reply: None,
    });
    stats.record_task_failure();
    let summary = stats.summary();
    assert!(summary.starts_with("attempted=6 connected=1 total_timeout=4 task=1 "));
    for field in [
        "timeout_connect=1",
        "timeout_handshake_send=1",
        "timeout_handshake_reply=1",
        "timeout_window=1",
        "connect_ms_p50=200 connect_ms_max=3000",
        "handshake_send_ms_p50=15 handshake_send_ms_max=2900",
        "handshake_reply_ms_p50=30 handshake_reply_ms_max=2790",
        "window_ms_p50=1325 window_ms_max=2650",
    ] {
        assert!(summary.contains(field), "missing {field}: {summary}");
    }
}

#[test]
fn upstream_phase_errors_are_timed_without_becoming_total_timeouts() {
    let mut stats = PeerConnectStats::default();
    stats.record_failure(PeerConnectFailure {
        addr: SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 1),
        stage: PeerConnectStage::Handshake,
        timing: PeerConnectTiming {
            phase: ConnectPhase::HandshakeReply,
            durations: [
                Some(Duration::ZERO),
                Some(Duration::ZERO),
                Some(Duration::from_millis(50)),
                None,
            ],
            reply: None,
        },
    });
    let summary = stats.summary();
    assert!(summary.starts_with("attempted=1 handshake=1 "));
    assert!(summary.contains("handshake_reply_ms_p50=50 handshake_reply_ms_max=50"));
    assert!(!summary.contains("timeout_"));
    assert!(!summary.contains("window_ms"));
}

#[tokio::test(start_paused = true)]
async fn upstream_phase_progress_retains_partial_phase_on_cancellation() {
    let mut progress = PeerConnectProgress::new();
    tokio::time::sleep(Duration::from_millis(100)).await;
    progress.advance(ConnectPhase::HandshakeSend);
    tokio::time::sleep(Duration::from_millis(20)).await;
    progress.advance(ConnectPhase::HandshakeReply);
    // Simulate the same cancellation that drops the in-flight I/O at the total deadline.
    assert!(
        tokio::time::timeout(Duration::from_millis(80), std::future::pending::<()>())
            .await
            .is_err()
    );
    let timing = progress.finish();
    assert_eq!(timing.phase, ConnectPhase::HandshakeReply);
    assert_eq!(
        timing.durations,
        [
            Some(Duration::from_millis(100)),
            Some(Duration::from_millis(20)),
            Some(Duration::from_millis(80)),
            None
        ]
    );
    assert_eq!(timing.summary(), "phase=handshake_reply connect_ms=100 handshake_send_ms=20 handshake_reply_ms=80 total_ms=200");
}

#[tokio::test]
async fn upstream_phase_window_timeout_is_distinct_from_basic_handshake_timeout() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let connect = tokio::spawn(connect_upstream(addr, [0; 20]));
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut request = [0; 66];
    tokio::io::AsyncReadExt::read_exact(&mut stream, &mut request)
        .await
        .unwrap();
    tokio::io::AsyncWriteExt::write_all(
        &mut stream,
        &ace_wire::handshake::Handshake::new([0; 20], [0; 20]).encode(),
    )
    .await
    .unwrap();
    let PeerConnectAttempt::Failed(failure) = connect.await.unwrap() else {
        panic!("missing extended handshake must time out");
    };
    assert_eq!(failure.stage, PeerConnectStage::TotalTimeout);
    assert_eq!(failure.timing.phase, ConnectPhase::Window);
    assert_eq!(
        failure.timing.reply,
        reply(HANDSHAKE_LEN, BasicReplyCause::Valid)
    );
    assert!(failure.timing.durations.iter().all(Option::is_some));
    let mut stats = PeerConnectStats::default();
    stats.record_failure(failure);
    assert!(stats.summary().contains("timeout_window=1"));
    assert!(!stats.summary().contains("timeout_handshake_reply="));
    assert!(stats.summary().contains("reply_valid_complete=1"));
}

#[tokio::test]
async fn upstream_phase_success_records_all_phases() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let connect = tokio::spawn(connect_upstream(addr, [0; 20]));
    let (stream, _) = listener.accept().await.unwrap();
    let mut server = PeerSession::new(stream);
    server.accept_handshake([0; 20], |_| true).await.unwrap();
    server
        .send_extended_handshake(&OutgoingExtendedHandshake {
            mi: Some(WireLivePosition {
                min_piece: 1,
                max_piece: 2,
                position: 2,
                distance_from_source: 1,
            }),
            ace_metadata_version: 1,
            ut_metadata_id: 2,
            node: NodeFields::default(),
            peer_ip: None,
            metadata_size: None,
        })
        .await
        .unwrap();
    let PeerConnectAttempt::Connected(_, timing) = connect.await.unwrap() else {
        panic!("valid window must connect");
    };
    assert_eq!(timing.phase, ConnectPhase::Window);
    assert_eq!(timing.reply, reply(HANDSHAKE_LEN, BasicReplyCause::Valid));
    assert!(timing.durations.iter().all(Option::is_some));
    let mut stats = PeerConnectStats::default();
    stats.record_connected(timing);
    assert!(stats.summary().starts_with("attempted=1 connected=1 "));
    assert!(stats.summary().contains("window_ms_p50="));
    assert!(!stats.summary().contains("timeout_"));
    assert!(stats.summary().contains("reply_valid_complete=1"));
}

#[tokio::test]
async fn upstream_partial_reply_outer_deadline_retains_bytes_and_counts_once() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let connect = tokio::spawn(connect_upstream(addr, [0; 20]));
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut request = [0; HANDSHAKE_LEN];
    tokio::io::AsyncReadExt::read_exact(&mut stream, &mut request)
        .await
        .unwrap();
    tokio::io::AsyncWriteExt::write_all(&mut stream, &[0xE1; 7])
        .await
        .unwrap();
    let PeerConnectAttempt::Failed(failure) = connect.await.unwrap() else {
        panic!("partial basic reply must hit the outer connection deadline");
    };

    assert_eq!(failure.stage, PeerConnectStage::TotalTimeout);
    assert_eq!(failure.timing.phase, ConnectPhase::HandshakeReply);
    assert_eq!(failure.timing.reply, reply(7, BasicReplyCause::Timeout));
    let mut stats = PeerConnectStats::default();
    stats.record_failure(failure);
    assert!(stats.summary().starts_with("attempted=1 total_timeout=1 "));
    assert!(stats.summary().contains("timeout_handshake_reply=1"));
    assert!(stats.summary().contains("reply_timeout_partial=1"));
}

#[test]
fn reply_stats_are_fixed_one_count_per_terminal_cause_and_leave_stage_totals_unchanged() {
    let addr = SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 1);
    let mut stats = PeerConnectStats::default();
    let cases = [
        (0, BasicReplyCause::Timeout, "reply_timeout_zero=1"),
        (7, BasicReplyCause::Eof, "reply_eof_partial=1"),
        (7, BasicReplyCause::Reset, "reply_reset_partial=1"),
        (0, BasicReplyCause::IoOther, "reply_io_other_zero=1"),
        (
            HANDSHAKE_LEN,
            BasicReplyCause::Timeout,
            "reply_timeout_complete=1",
        ),
        (
            HANDSHAKE_LEN,
            BasicReplyCause::Decode,
            "reply_decode_complete=1",
        ),
        (
            HANDSHAKE_LEN,
            BasicReplyCause::HashMismatch,
            "reply_hash_mismatch_complete=1",
        ),
        (
            HANDSHAKE_LEN,
            BasicReplyCause::Valid,
            "reply_valid_complete=1",
        ),
    ];
    for (received, cause, _) in cases {
        stats.record_failure(PeerConnectFailure {
            addr,
            stage: PeerConnectStage::Handshake,
            timing: PeerConnectTiming {
                phase: ConnectPhase::HandshakeReply,
                durations: [None; 4],
                reply: reply(received, cause),
            },
        });
    }
    let summary = stats.summary();
    assert!(summary.starts_with("attempted=8 handshake=8 "));
    for (_, _, field) in cases {
        assert!(summary.contains(field), "missing {field}: {summary}");
    }
    assert_eq!(summary.matches("reply_").count(), cases.len());
}

#[test]
fn reply_stats_omit_attempts_before_sent_and_classify_typed_errors() {
    let mut stats = PeerConnectStats::default();
    stats.record_failure(PeerConnectFailure {
        addr: SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 1),
        stage: PeerConnectStage::Tcp,
        timing: PeerConnectTiming {
            phase: ConnectPhase::Connect,
            durations: [None; 4],
            reply: None,
        },
    });
    let summary = stats.summary();
    assert_eq!(summary, "attempted=1 tcp=1");
    assert!(!summary.contains("reply_"));

    let cases = [
        (PeerError::Timeout, BasicReplyCause::Timeout),
        (
            PeerError::Io(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)),
            BasicReplyCause::Eof,
        ),
        (
            PeerError::Io(std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
            BasicReplyCause::Reset,
        ),
        (
            PeerError::Io(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
            BasicReplyCause::IoOther,
        ),
        (
            PeerError::Io(std::io::Error::from(std::io::ErrorKind::TimedOut)),
            BasicReplyCause::IoOther,
        ),
        (PeerError::Protocol("synthetic"), BasicReplyCause::Decode),
        (PeerError::InfohashMismatch, BasicReplyCause::HashMismatch),
        (PeerError::Closed, BasicReplyCause::Eof),
    ];
    for (error, expected) in cases {
        assert_eq!(BasicReplyCause::from_error(&error), expected);
    }
}

#[test]
fn received_payload_is_counted_once_before_output_gating() {
    let info = StreamInfo {
        infohash: [0; 20],
        piece_length: 4,
        chunk_length: 4,
        sig_len: 0,
        source_pubkey: vec![],
        trackers: vec![],
        metadata: StreamMetadata::default(),
    };
    let (mut continuity, _) = Continuity::fresh(&info, 0, 0, 0, LiveRecoveryConfig::default());
    let received = Arc::new(AtomicU64::new(0));
    let metrics = TestObserver::new(
        Arc::new(AtomicU64::new(0)),
        received.clone(),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        Arc::new(AtomicU32::new(0)),
    );
    continuity.reasm.add_block(0, 0, b"ab").unwrap();
    continuity.reasm.add_block(2, 0, b"ijkl").unwrap();
    assert_eq!(ready_buffer_snapshot(&continuity, Some(2)), (0, 0));
    continuity.reasm.add_block(0, 2, b"cd").unwrap();
    assert_eq!(ready_buffer_snapshot(&continuity, Some(2)), (2000, 1));
    assert_eq!(
        take_ready_with_stats(
            &mut continuity,
            Some(&(metrics.clone() as Arc<dyn LiveObserver>)),
            &info
        ),
        b"abcd"
    );
    // Duplicate old blocks cannot become new received media.
    continuity.reasm.add_block(0, 0, b"abcd").unwrap();
    assert!(take_ready_with_stats(
        &mut continuity,
        Some(&(metrics.clone() as Arc<dyn LiveObserver>)),
        &info
    )
    .is_empty());
    assert_eq!(received.load(Ordering::Relaxed), 4);
    assert_eq!(metrics.snapshot().downloaded, 0);
}

#[test]
fn positions_clear_active_bounds_on_reconnect_and_reject_negative_windows() {
    let info = StreamInfo {
        infohash: [0; 20],
        piece_length: 4,
        chunk_length: 4,
        sig_len: 0,
        source_pubkey: vec![],
        trackers: vec![],
        metadata: StreamMetadata::default(),
    };
    let (mut c, _) = Continuity::fresh(&info, 0, 0, 0, LiveRecoveryConfig::default());
    let metrics = TestObserver::new(
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(1)),
        Arc::new(AtomicU32::new(0)),
    );
    c.register_active_peer(
        1,
        SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 1),
        WireLivePosition {
            min_piece: -1,
            max_piece: -1,
            position: -1,
            distance_from_source: 1,
        },
    );
    c.set_peer_unchoked(1, true);
    publish_live_stats(&c, Some(&(metrics.clone() as Arc<dyn LiveObserver>)), &info);
    assert_eq!(metrics.snapshot().livepos.unwrap().live_first, None);
    assert_eq!(metrics.snapshot().buffer_ms, 0);
    // Publication alone must not perform a media scan.
    c.reasm.add_block(0, 0, b"abcd").unwrap();
    publish_live_stats(&c, Some(&(metrics.clone() as Arc<dyn LiveObserver>)), &info);
    assert_eq!(metrics.snapshot().livepos.unwrap().buffer_pieces, 0);
    c.active_peers.observe_window(1, Some((2, 7)));
    publish_live_stats(&c, Some(&(metrics.clone() as Arc<dyn LiveObserver>)), &info);
    assert_eq!(metrics.snapshot().livepos.unwrap().live_last, Some(7));
    metrics.active_upstreams(2);
    metrics.recovery_event(RecoveryEvent::UpstreamReconnect);
    drop(LiveStatsGuard(Some(metrics.clone())));
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.livepos.unwrap().live_last, None);
    assert_eq!(snapshot.buffer_ms, 0);
    assert_eq!(snapshot.active_upstreams, 0);
    assert_eq!(
        snapshot.recovery_events,
        vec![RecoveryEvent::UpstreamReconnect]
    );
}

#[tokio::test]
async fn aborting_live_session_retains_recovery_history_and_clears_active_gauge() {
    let metrics = TestObserver::new(
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        Arc::new(AtomicU32::new(0)),
    );
    let observer: Arc<dyn LiveObserver> = metrics.clone();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _guard = LiveStatsGuard(Some(observer.clone()));
        observer.recovery_event(RecoveryEvent::UpstreamReconnect);
        observer.active_upstreams(2);
        let _ = ready_tx.send(());
        std::future::pending::<()>().await;
    });
    ready_rx.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot.recovery_events,
        vec![RecoveryEvent::UpstreamReconnect]
    );
    assert_eq!(snapshot.active_upstreams, 0);
}

#[test]
fn sliding_window_observations_do_not_change_scheduler_coverage() {
    let info = StreamInfo {
        infohash: [0; 20],
        piece_length: 4,
        chunk_length: 4,
        sig_len: 0,
        source_pubkey: vec![],
        trackers: vec![],
        metadata: StreamMetadata::default(),
    };
    let (mut c, _) = Continuity::fresh(&info, 100, 200, 0, LiveRecoveryConfig::default());
    c.register_active_peer(
        1,
        SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 1),
        WireLivePosition {
            min_piece: 100,
            max_piece: 200,
            position: -1,
            distance_from_source: 1,
        },
    );
    c.set_peer_unchoked(1, true);
    observe_live_window(&mut c, 1, b"d9:max_piecei250e9:min_piecei150ee");
    assert_eq!(c.active_peers.coverage_bounds(), Some((150, 250)));
    assert!(c.active_peers.any_unchoked_covers(120));
    // Scheduler/head-only updates must not restore the old observational minimum.
    c.update_peer_window(1, 100, 300);
    assert_eq!(c.active_peers.coverage_bounds(), Some((150, 250)));
    observe_live_window(&mut c, 1, b"d9:max_piecei250e9:min_piecei180ee");
    assert_eq!(c.active_peers.coverage_bounds(), Some((180, 250)));
    observe_live_window(&mut c, 1, b"d9:max_piecei250e9:min_piecei-1ee");
    assert_eq!(c.active_peers.coverage_bounds(), None);
}

#[test]
fn bounded_stats_pcr_differential_and_invalidation() {
    fn packet(pid: u16, pcr: u64, discontinuity: bool) -> Vec<u8> {
        let mut p = vec![0; 188];
        p[0] = 0x47;
        p[1] = (pid >> 8) as u8;
        p[2] = pid as u8;
        p[3] = 0x20;
        p[4] = 7;
        p[5] = 0x10 | if discontinuity { 0x80 } else { 0 };
        p[6] = (pcr >> 25) as u8;
        p[7] = (pcr >> 17) as u8;
        p[8] = (pcr >> 9) as u8;
        p[9] = (pcr >> 1) as u8;
        p[10] = (pcr << 7) as u8;
        p
    }
    for events in [
        vec![(1, 0, false), (2, 900000, false), (1, 180000, false)],
        vec![(1, (1 << 33) - 90000, false), (1, 90000, false)],
        vec![
            (1, 180000, false),
            (1, 90000, false),
            (2, 0, false),
            (2, 270000, false),
        ],
        vec![
            (1, 0, false),
            (2, 1, true),
            (2, 90000, false),
            (2, 180000, false),
        ],
        vec![(1, 0, false), (1, 0, false)],
    ] {
        for bitrate in [None, Some(0), Some(1000)] {
            let media: Vec<u8> = events
                .iter()
                .flat_map(|&(pid, pcr, d)| packet(pid, pcr, d))
                .collect();
            let mut bytes = vec![0; 188 * 2050];
            bytes.extend(media);
            bytes.extend([1, 2, 3]);
            let info = StreamInfo {
                infohash: [0; 20],
                piece_length: bytes.len() as u64,
                chunk_length: bytes.len() as u64,
                sig_len: 0,
                source_pubkey: vec![],
                trackers: vec![],
                metadata: StreamMetadata::default(),
            };
            let (mut c, _) = Continuity::fresh(&info, 0, 0, 0, LiveRecoveryConfig::default());
            c.reasm.add_block(0, 0, &bytes).unwrap();
            c.stats_scan.advance(&c.reasm, bitrate);
            assert!(c.stats_scan.pending());
            // An immutable ahead completion must not reset the active clock/offset.
            c.reasm.add_block(1, 0, &bytes).unwrap();
            while c.stats_scan.pending() {
                c.stats_scan.advance(&c.reasm, bitrate);
            }
            assert_eq!(
                c.stats_scan.result(0),
                Some(ready_buffer_snapshot(&c, bitrate))
            );
            c.invalidate_stats();
            assert!(c.stats_scan.pending());
            c.stats_scan.advance(&c.reasm, bitrate);
            c.reasm.take_ready_piece();
            c.stats_scan.advance(&c.reasm, bitrate);
            while c.stats_scan.pending() {
                c.stats_scan.advance(&c.reasm, bitrate);
            }
            assert_eq!(
                c.stats_scan.result(1),
                Some(ready_buffer_snapshot(&c, bitrate))
            );
        }
    }
}

#[test]
fn bounded_stats_gap_cache_and_synchronous_unknown() {
    let info = StreamInfo {
        infohash: [0; 20],
        piece_length: 4,
        chunk_length: 2,
        sig_len: 0,
        source_pubkey: vec![],
        trackers: vec![],
        metadata: StreamMetadata::default(),
    };
    let (mut c, _) = Continuity::fresh(&info, 0, 0, 0, LiveRecoveryConfig::default());
    let observer = TestObserver::new(
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        Arc::new(AtomicU32::new(0)),
    );
    c.observer = Some(observer.clone());
    assert_eq!(c.advance_stats(false, Some(2)), (0, 0));
    assert_eq!(c.stats_scan.result(0), None);
    assert!(!c.stats_pending(false));
    assert!(c.stats_pending(true));
    c.stats_scan.advance(&c.reasm, None);
    publish_live_stats(&c, c.observer.as_ref(), &info);
    assert!(observer.snapshot().livepos.unwrap().buffer_complete);
    assert_eq!(c.stats_scan.result(0), Some((0, 0)));
    assert!(!c.stats_pending(true));
    let partial = c.reasm.add_live_chunk(0, 0, 2, [0; 8], b"ab").unwrap();
    assert!(!partial.completed);
    assert!(!c.stats_scan.pending());
    let done = c.reasm.add_live_chunk(0, 1, 2, [0; 8], b"cd").unwrap();
    assert!(done.completed);
    c.invalidate_stats();
    assert!(!observer.snapshot().livepos.unwrap().buffer_complete);
    assert_eq!(observer.snapshot().buffer_ms, 0);
    c.stats_scan.advance(&c.reasm, Some(2));
    assert_eq!(c.stats_scan.result(0), Some((2000, 1)));
    let duplicate = c.reasm.add_live_chunk(0, 1, 2, [0; 8], b"cd").unwrap();
    assert!(!duplicate.completed);
    assert!(!c.stats_scan.pending());
    c.reasm.add_block(2, 0, b"abcd").unwrap();
    c.invalidate_stats();
    c.stats_scan.advance(&c.reasm, Some(2));
    assert_eq!(c.stats_scan.result(0), Some((2000, 1)));
    c.reasm.add_block(1, 0, b"abcd").unwrap();
    c.invalidate_stats();
    c.stats_scan.advance(&c.reasm, Some(2));
    assert_eq!(c.stats_scan.result(0), Some((6000, 3)));
}
