use super::*;

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
    let metrics = crate::live_stats::LiveMetrics::new(
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
        take_ready_with_stats(&mut continuity, Some(&metrics), &info),
        b"abcd"
    );
    // Duplicate old blocks cannot become new received media.
    continuity.reasm.add_block(0, 0, b"abcd").unwrap();
    assert!(take_ready_with_stats(&mut continuity, Some(&metrics), &info).is_empty());
    assert_eq!(received.load(Ordering::Relaxed), 4);
    assert_eq!(
        crate::provider::StatsReader::snapshot(metrics.as_ref()).downloaded,
        0
    );
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
    let metrics = crate::live_stats::LiveMetrics::new(
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(1)),
        Arc::new(AtomicU32::new(0)),
    );
    c.register_active_peer(
        1,
        SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 1),
        LivePosition {
            min_piece: -1,
            max_piece: -1,
            position: -1,
            distance_from_source: 1,
        },
    );
    c.set_peer_unchoked(1, true);
    publish_live_stats(&c, Some(&metrics), &info);
    assert_eq!(
        crate::provider::StatsReader::snapshot(metrics.as_ref())
            .livepos
            .unwrap()
            .live_first,
        None
    );
    c.active_peers.observe_window(1, Some((2, 7)));
    publish_live_stats(&c, Some(&metrics), &info);
    assert_eq!(
        crate::provider::StatsReader::snapshot(metrics.as_ref())
            .livepos
            .unwrap()
            .live_last,
        Some(7)
    );
    drop(LiveStatsGuard(Some(metrics.clone())));
    let snapshot = crate::provider::StatsReader::snapshot(metrics.as_ref());
    assert_eq!(snapshot.livepos.unwrap().live_last, None);
    assert_eq!(snapshot.buffer_ms, 0);
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
        LivePosition {
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
