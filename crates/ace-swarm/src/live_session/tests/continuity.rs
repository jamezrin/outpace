use super::*;

// Note: the "no peers -> Backend error" path is intentionally not unit-tested, since
// discovery now always consults the live DHT (network). It's exercised by the live
// capture path instead.

#[test]
fn first_request_after_unchoke_covers_the_prefetch_window() {
    let mut scheduler = Scheduler::new(default_live_recovery().max_piece_advance as usize);
    let mut active = single_active_peer(100, 109);
    let pieces = schedule_piece_requests(&mut scheduler, &mut active, 100, 109);
    assert_eq!(pieces, (100..=109).collect::<Vec<_>>());
}

#[test]
fn caught_up_requests_nothing() {
    // The reassembler cursor is already past the head -> nothing new yet (the loop
    // keeps waiting for window updates to advance it).
    let mut scheduler = Scheduler::new(default_live_recovery().max_piece_advance as usize);
    let mut active = single_active_peer(100, 109);
    assert!(schedule_piece_requests(&mut scheduler, &mut active, 110, 109).is_empty());
}

#[test]
fn window_update_drives_a_forward_request() {
    // The head advanced by one piece -> request exactly that new piece. This is the
    // step the pre-fix loop never took (it only advanced on a `Have` that never came).
    let mut one = Scheduler::new(default_live_recovery().max_piece_advance as usize);
    let mut one_active = single_active_peer(100, 110);
    assert_eq!(
        schedule_piece_requests(&mut one, &mut one_active, 110, 110),
        vec![110]
    );

    let mut many = Scheduler::new(default_live_recovery().max_piece_advance as usize);
    let mut many_active = single_active_peer(100, 113);
    assert_eq!(
        schedule_piece_requests(&mut many, &mut many_active, 110, 113),
        vec![110, 111, 112, 113]
    );
}

#[test]
fn forward_request_is_bounded_against_a_bogus_head() {
    // A garbled window update claiming a wildly-advanced head can't burst-request the
    // whole range; it's clamped, and subsequent updates catch up.
    let mut scheduler = Scheduler::new(default_live_recovery().max_piece_advance as usize);
    let mut active = single_active_peer(0, 10_000_000);
    let pieces = schedule_piece_requests(&mut scheduler, &mut active, 101, 10_000_000);
    assert_eq!(pieces.first(), Some(&101));
    assert_eq!(
        pieces.len() as u64,
        default_live_recovery().max_piece_advance
    );
}

#[test]
fn continuity_uses_configured_live_recovery_bounds() {
    let info = StreamInfo {
        infohash: [0x01; 20],
        piece_length: 1_048_576,
        chunk_length: 16_384,
        trackers: vec![],
        metadata: StreamMetadata::default(),
        sig_len: 96,
        source_pubkey: vec![],
    };
    let policy = LiveRecoveryConfig {
        max_piece_advance: 7,
        max_reasm_pieces_ahead: 9,
        ..LiveRecoveryConfig::default()
    };
    let (mut c, start) = Continuity::fresh(&info, 10, 20, 2, policy);

    assert_eq!(start, 18);
    c.register_active_peer(
        1,
        "127.0.0.1:1".parse().unwrap(),
        WireLivePosition {
            min_piece: 10,
            max_piece: 100,
            position: -1,
            distance_from_source: 1,
        },
    );
    c.head = 100;
    c.set_peer_unchoked(1, true);

    let pieces = schedule_piece_assignments(
        &mut c.scheduler,
        &mut c.active_peers,
        c.reasm.next_needed(),
        c.head,
    );

    assert_eq!(pieces.len(), 7);
}

#[test]
fn stale_upstream_budget_expires_after_no_forward_progress() {
    let now = std::time::Instant::now();
    let timeout = default_live_recovery().stale_upstream_timeout();
    assert_eq!(
        stale_upstream_budget(now, now + Duration::from_secs(3), timeout),
        Some(timeout - Duration::from_secs(3))
    );
    assert_eq!(stale_upstream_budget(now, now + timeout, timeout), None);
    assert_eq!(
        stale_upstream_budget(now, now + timeout + Duration::from_millis(1), timeout),
        None
    );
}

#[test]
fn timed_out_requests_returns_only_aged_pieces_and_prunes_passed_ones() {
    let (mut c, _) = Continuity::fresh(
        &info(),
        5,
        15,
        PREFETCH_PIECES,
        LiveRecoveryConfig::default(),
    ); // next_needed = 7
    let base = Instant::now();
    c.requested_at.insert(4, base); // below cursor -> pruned, not returned
    c.requested_at.insert(8, base); // aged
    let request_timeout = default_live_recovery().request_timeout();
    c.requested_at.insert(9, base + request_timeout); // fresh
    let now = base + request_timeout;
    let mut out = c.timed_out_requests(now);
    out.sort_unstable();
    assert_eq!(out, vec![8]);
    assert!(!c.requested_at.contains_key(&4), "passed piece pruned");
}

#[test]
fn skip_evicted_gap_jumps_to_lowest_covered_when_cursor_is_stranded() {
    let (mut c, _) = Continuity::fresh(
        &info(),
        100,
        110,
        PREFETCH_PIECES,
        LiveRecoveryConfig::default(),
    ); // next_needed = 102
    c.register_active_peer(1, test_addr(), win(105, 120)); // evicted 102..104
    c.set_peer_unchoked(1, true);
    // Not stuck long enough yet.
    assert_eq!(c.skip_evicted_gap(c.next_needed_since), None);
    // Stuck past the timeout with no peer covering 102 -> skip to 105.
    let now = c.next_needed_since + default_live_recovery().request_timeout();
    assert_eq!(c.skip_evicted_gap(now), Some(105));
    assert_eq!(c.reasm.next_needed(), 105);
}

#[test]
fn skip_far_behind_live_resyncs_forward_even_when_peers_cover_the_cursor() {
    // Reproduces the production wedge: the cursor is stuck on a piece a peer's window still
    // covers (so `skip_evicted_gap` refuses to skip), while the live edge has raced far ahead.
    let lr = LiveRecoveryConfig {
        max_reasm_pieces_ahead: 16,
        ..LiveRecoveryConfig::default()
    };
    let (mut c, _) = Continuity::fresh(&info(), 100, 110, PREFETCH_PIECES, lr); // next=102, head=110
    c.register_active_peer(1, test_addr(), win(100, 10_000)); // still "covers" 102
    c.set_peer_unchoked(1, true);
    // Live edge is now well beyond next + max_reasm_pieces_ahead (102 + 16).
    c.head = 5_000;
    // `skip_evicted_gap` would never fire here (a peer covers the cursor).
    assert_eq!(
        c.skip_evicted_gap(c.next_needed_since + Duration::from_secs(60)),
        None
    );
    // The live-edge re-sync jumps forward to head - prefetch regardless of coverage.
    let target = c.skip_far_behind_live(c.next_needed_since);
    assert_eq!(target, Some(5_000 - PREFETCH_PIECES));
    assert_eq!(c.reasm.next_needed(), 5_000 - PREFETCH_PIECES);
}

#[test]
fn skip_far_behind_live_stays_put_within_the_lookahead_window() {
    let (mut c, _) = Continuity::fresh(
        &info(),
        100,
        110,
        PREFETCH_PIECES,
        LiveRecoveryConfig::default(),
    ); // next=102, head=110, lag=8 << max_reasm_pieces_ahead
    assert_eq!(c.skip_far_behind_live(c.next_needed_since), None);
    assert_eq!(c.reasm.next_needed(), 102);
}

#[test]
fn skip_evicted_gap_does_not_skip_while_a_peer_still_covers_the_cursor() {
    let (mut c, _) = Continuity::fresh(
        &info(),
        100,
        110,
        PREFETCH_PIECES,
        LiveRecoveryConfig::default(),
    ); // next_needed = 102
    c.register_active_peer(1, test_addr(), win(100, 120)); // still covers 102
    c.set_peer_unchoked(1, true);
    let now = c.next_needed_since + default_live_recovery().request_timeout() * 3;
    assert_eq!(c.skip_evicted_gap(now), None);
    assert_eq!(c.reasm.next_needed(), 102);
}

#[test]
fn stale_deadline_refreshes_only_for_output_after_playback_starts() {
    assert!(
        should_refresh_stale_deadline(0, false, true),
        "startup can stay alive on handshake/window/chunk activity before first output"
    );
    assert!(
        should_refresh_stale_deadline(1024, true, false),
        "contiguous MPEG-TS output is real playback progress"
    );
    assert!(
        !should_refresh_stale_deadline(1024, false, true),
        "after playback starts, non-output activity must not mask a visible stall"
    );
}

#[test]
fn acestream_have_payload_decodes_to_the_head_piece() {
    // The captured live HAVE (note 22): `[u32 stream=0][u32 piece]`. We read the piece
    // at bytes [4..8]; this exact payload was `head -> 5360483` in the operator's log.
    let payload = [0x00, 0x00, 0x00, 0x00, 0x00, 0x51, 0xcb, 0x63];
    let piece = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]) as u64;
    assert_eq!(piece, 5_360_483);
}

#[test]
fn advance_head_only_when_window_moves_forward() {
    // A myinfo update past the head advances it; one at/behind the head is ignored.
    let ahead = b"d9:max_piecei210ee";
    let behind = b"d9:max_piecei150ee";
    assert_eq!(advance_head_from_window(ahead, 200), Some(210));
    assert_eq!(advance_head_from_window(behind, 200), None);
    assert_eq!(advance_head_from_window(b"not-a-window", 200), None);
}

#[test]
fn prefetch_start_honors_configured_depth() {
    // window 100..=200, prefetch 32 -> start 168 (not 200 - 8)
    assert_eq!(prefetch_start(100, 200, 32), 168);
    // clamps to min_piece when the window is shorter than prefetch
    assert_eq!(prefetch_start(195, 200, 32), 195);
}

#[test]
fn fresh_starts_prefetch_pieces_behind_head_clamped_to_min() {
    let (c, start) = Continuity::fresh(
        &info(),
        100,
        200,
        PREFETCH_PIECES,
        LiveRecoveryConfig::default(),
    );
    assert_eq!(start, 200 - PREFETCH_PIECES);
    assert_eq!(c.head, 200);
    assert_eq!(c.scheduler.in_flight_count(), 0);
    assert_eq!(c.reasm.next_needed(), start);
}

#[test]
fn fresh_clamps_start_to_min_piece_on_a_narrow_window() {
    // min_piece is closer to head than PREFETCH_PIECES allows -> clamp, don't request
    // an evicted piece.
    let (_c, start) = Continuity::fresh(
        &info(),
        198,
        200,
        PREFETCH_PIECES,
        LiveRecoveryConfig::default(),
    );
    assert_eq!(start, 198);
}

#[test]
fn resume_continues_seamlessly_when_the_new_window_still_covers_our_position() {
    // We'd already emitted through piece 150; the new peer's window still covers the
    // next needed piece, so resume exactly there.
    let (mut c, start) = Continuity::fresh(
        &info(),
        100,
        149,
        PREFETCH_PIECES,
        LiveRecoveryConfig::default(),
    );
    for piece in start..=150 {
        c.reasm.add_block(piece, 0, &[1, 1]).unwrap();
        c.reasm.add_block(piece, 2, &[2, 2]).unwrap();
    }
    assert_eq!(c.reasm.take_ready().len(), ((150 - start + 1) * 4) as usize);
    assert_eq!(c.reasm.next_needed(), 151);
    let addr: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();
    let resume = c.resume(addr, 100, 160);
    assert_eq!(
        resume, 151,
        "continues right after what we've actually emitted"
    );
    assert_eq!(c.head, 160);
    assert_eq!(c.reasm.next_needed(), 151);
}

#[test]
fn resume_retries_from_the_reassembler_cursor_not_the_old_request_frontier() {
    // In-flight scheduler entries only mean "asked the previous peer", not "received".
    // If that peer dies before delivery, the next peer must be asked for the first
    // still-missing piece rather than skipping past the old request frontier.
    let (mut c, start) = Continuity::fresh(
        &info(),
        100,
        149,
        PREFETCH_PIECES,
        LiveRecoveryConfig::default(),
    );
    c.active_peers = single_active_peer(100, 150);
    let assigned = c.active_peers.assign(&mut c.scheduler, start, 150);
    assert!(!assigned.is_empty());
    assert!(c.scheduler.in_flight_count() > 0);
    let addr: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();

    let resume = c.resume(addr, 100, 160);

    assert_eq!(resume, start);
    assert_eq!(c.reasm.next_needed(), start);
    assert_eq!(
        c.scheduler.in_flight_count(),
        0,
        "pieces requested from the dropped peer must be requeueable"
    );
}

#[test]
fn peer_window_must_cover_the_next_needed_piece_on_reconnect() {
    let (c, start) = Continuity::fresh(
        &info(),
        100,
        149,
        PREFETCH_PIECES,
        LiveRecoveryConfig::default(),
    );
    let stale = WireLivePosition {
        min_piece: 100,
        max_piece: (start - 1) as i64,
        position: (start - 1) as i64,
        distance_from_source: 1,
    };
    let covering = WireLivePosition {
        min_piece: 100,
        max_piece: start as i64,
        position: start as i64,
        distance_from_source: 1,
    };

    assert!(!c.window_can_resume(&stale));
    assert!(c.window_can_resume(&covering));
}

#[test]
fn live_edge_peer_stays_usable_while_the_next_needed_piece_does_not_exist_yet() {
    let (mut c, _start) = Continuity::fresh(
        &info(),
        100,
        149,
        PREFETCH_PIECES,
        LiveRecoveryConfig::default(),
    );
    // Caught up to the live edge: the piece we want next is one past the newest piece the
    // source has produced, so no peer in the swarm can advertise it yet.
    c.reasm.skip_to(150);
    assert_eq!(c.reasm.next_needed(), 150);
    assert_eq!(c.head, 149);

    let at_live_edge = WireLivePosition {
        min_piece: 100,
        max_piece: 149,
        position: 149,
        distance_from_source: 1,
    };
    let lagging = WireLivePosition {
        min_piece: 100,
        max_piece: 120,
        position: 120,
        distance_from_source: 1,
    };

    assert!(
        c.window_can_resume(&at_live_edge),
        "a peer current with the live edge must stay usable while we wait on the next piece"
    );
    assert!(
        !c.window_can_resume(&lagging),
        "a peer far behind the live edge still cannot serve the piece we need"
    );
}

#[test]
fn resume_skips_forward_over_an_unrecoverable_eviction_gap() {
    // We were disconnected long enough that the new peer's window no longer has the
    // piece we needed next (min_piece has advanced past it) — must skip, not stall.
    let (mut c, _start) = Continuity::fresh(
        &info(),
        100,
        149,
        PREFETCH_PIECES,
        LiveRecoveryConfig::default(),
    );
    let addr: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();
    let resume = c.resume(addr, 500, 600); // min_piece way ahead of 151
    assert_eq!(resume, 500);
    assert_eq!(
        c.reasm.next_needed(),
        500,
        "reassembler cursor jumped past the gap"
    );
    assert_eq!(c.scheduler.in_flight_count(), 0);
}

#[test]
fn resume_gap_arms_output_keyframe_gate() {
    let (mut c, _) = Continuity::fresh(
        &info(),
        100,
        149,
        PREFETCH_PIECES,
        LiveRecoveryConfig::default(),
    );
    assert!(!c.output_gate_armed());

    c.resume("127.0.0.1:1".parse().unwrap(), 170, 200);

    assert!(c.output_gate_armed());
}

#[test]
fn arming_output_gate_discards_buffered_pre_gap_ts_bytes() {
    const TS_PACKET: usize = 188;

    fn packet(fill: u8) -> Vec<u8> {
        let mut bytes = vec![fill; TS_PACKET];
        bytes[0] = 0x47;
        bytes
    }

    let (mut c, _) = Continuity::fresh(
        &info(),
        100,
        149,
        PREFETCH_PIECES,
        LiveRecoveryConfig::default(),
    );
    assert!(
        c.resync.push(&packet(0x11)).is_empty(),
        "one packet is buffered until lookahead confirms sync"
    );

    c.arm_output_gate();
    let post_gap = [packet(0x22), packet(0x33)].concat();
    let aligned = c.resync.push(&post_gap);

    assert_eq!(aligned.len(), TS_PACKET);
    assert_eq!(aligned[1], 0x22);
}

#[test]
fn resync_boundary_loss_marks_one_discontinuity_and_rearms_output_gate() {
    const TS_PACKET: usize = 188;
    fn packet(fill: u8) -> Vec<u8> {
        let mut bytes = vec![fill; TS_PACKET];
        bytes[0] = 0x47;
        bytes
    }

    let (mut c, _) = Continuity::fresh(
        &info(),
        100,
        149,
        PREFETCH_PIECES,
        LiveRecoveryConfig::default(),
    );
    let initial = [packet(1), packet(2), packet(3)].concat();
    assert!(!c.resync_output(&initial).is_empty());
    assert!(!c.take_discontinuity());

    let boundary = [vec![0; 96], packet(4), packet(5), packet(6)].concat();
    let prefix = c.resync_output(&boundary);
    assert_eq!(prefix.len(), 1);
    assert!(!prefix[0].discontinuity);
    assert_eq!(prefix[0].bytes[1], 3);
    assert!(c.output_gate_armed());
    assert!(c.take_discontinuity());
    assert!(!c.take_discontinuity());
}

#[test]
fn skip_evicted_gap_arms_output_keyframe_gate() {
    let (mut c, _) = Continuity::fresh(
        &info(),
        100,
        110,
        PREFETCH_PIECES,
        LiveRecoveryConfig::default(),
    );
    c.register_active_peer(1, test_addr(), win(105, 120));
    c.set_peer_unchoked(1, true);
    assert!(!c.output_gate_armed());

    let now = c.next_needed_since + default_live_recovery().request_timeout();
    assert_eq!(c.skip_evicted_gap(now), Some(105));

    assert!(c.output_gate_armed());
}

#[test]
fn resume_head_never_regresses() {
    let (mut c, _start) = Continuity::fresh(
        &info(),
        100,
        200,
        PREFETCH_PIECES,
        LiveRecoveryConfig::default(),
    );
    let addr: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();
    c.resume(addr, 100, 150); // a new peer with a "smaller" (staler) window
    assert_eq!(c.head, 200, "head must not go backward");
}

#[test]
fn prefetch_start_bounds_huge_depth_and_peer_window() {
    assert_eq!(prefetch_start(180, 200, 64), 180);
    assert_eq!(prefetch_start(0, 20, u64::MAX), 0);
}
