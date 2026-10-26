//! Real provider/discovery/BT cold-start regressions, synthetic loopback addresses only.
use super::*;
use ace_swarm::discover::discover_peers_from_sources_incremental;
use ace_swarm::discover::MAX_DISCOVERY_PEERS;
use std::sync::atomic::AtomicBool;

fn info() -> StreamInfo {
    StreamInfo {
        infohash: [0; 20],
        piece_length: 752,
        chunk_length: 752,
        trackers: Vec::new(),
        metadata: StreamMetadata::default(),
        sig_len: 0,
        source_pubkey: Vec::new(),
    }
}

async fn assert_first_attempt(slow_second: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let (attempt_tx, attempt_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut session = PeerSession::new(stream);
        session.accept_handshake([0; 20], |_| true).await.unwrap();
        attempt_tx.send(Instant::now()).unwrap();
    });
    let available = Arc::new(std::sync::Mutex::new(None));
    let observed = available.clone();
    let discovery: PeerDiscovery = Arc::new(move |_options, sender| {
        let observed = observed.clone();
        Box::pin(async move {
            let early = async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                *observed.lock().unwrap() = Some(Instant::now());
                vec![addr]
            };
            let other = async move {
                if slow_second {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                Vec::new()
            };
            discover_peers_from_sources_incremental(early, other, sender).await
        })
    });
    let start = Instant::now();
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0);
    let open =
        tokio::spawn(async move { provider.open_resolved("synthetic", info(), discovery).await });
    let attempt = tokio::time::timeout(Duration::from_secs(2), attempt_rx).await;
    // Always clean owned operations before asserting, including the original failing path.
    open.abort();
    if let Ok(Ok(source)) = open.await {
        drop(source);
    }
    server.abort();
    let _ = server.await;
    let candidate_at = available.lock().unwrap().expect("early source ran");
    assert!(candidate_at.duration_since(start) < Duration::from_millis(500));
    crate::alog!(
        "[test] candidate available after {} ms; first BT attempt within 2s={}",
        candidate_at.duration_since(start).as_millis(),
        attempt.is_ok()
    );
    assert!(
        attempt.is_ok(),
        "production resolved-open waited for the slow aggregate despite a candidate at {:?}",
        candidate_at.duration_since(start),
    );
    let attempt_at = attempt.unwrap().unwrap();
    assert!(attempt_at.duration_since(start) < Duration::from_secs(2));
}

#[tokio::test]
async fn provider_first_bt_attempt_does_not_wait_for_slow_second_source() {
    assert_first_attempt(true).await;
}

#[tokio::test]
async fn provider_immediate_complete_discovery_positive_control() {
    assert_first_attempt(false).await;
}

fn media_piece(marker: u8) -> Vec<u8> {
    (0..4)
        .flat_map(|cc| {
            let mut packet = vec![marker; 188];
            packet[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10 | cc]);
            packet
        })
        .collect()
}

fn signed_media_pieces(auth: &ace_wire::live_auth::LiveSourceAuth, marker: u8) -> Vec<Vec<u8>> {
    let mut ts = vec![marker; 188 * 64];
    for (packet_index, packet) in ts.chunks_mut(188).enumerate() {
        if packet.len() >= 4 {
            packet[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10 | (packet_index as u8 & 0x0f)]);
        }
    }
    signed_media_pieces_from_ts(auth, &ts)
}

fn signed_media_pieces_from_ts(
    auth: &ace_wire::live_auth::LiveSourceAuth,
    stream: &[u8],
) -> Vec<Vec<u8>> {
    const PAYLOAD_PER_PIECE: usize = 752 - 96;
    let total = PAYLOAD_PER_PIECE * 16;
    let mut ts = Vec::with_capacity(total);
    while ts.len() < total {
        ts.extend_from_slice(stream);
    }
    ts.truncate(total);
    ts.as_chunks::<PAYLOAD_PER_PIECE>()
        .0
        .iter()
        .map(|payload| {
            let mut piece = payload.to_vec();
            piece.extend_from_slice(&auth.sign(payload));
            piece
        })
        .collect()
}

fn test_ts_packet(pid: u16, start: bool, counter: u8, payload: &[u8]) -> Vec<u8> {
    assert!(payload.len() <= 184);
    let mut packet = vec![0xff; 188];
    packet[..4].copy_from_slice(&[
        0x47,
        ((start as u8) << 6) | ((pid >> 8) as u8 & 0x1f),
        pid as u8,
        0x10 | (counter & 0x0f),
    ]);
    packet[4..4 + payload.len()].copy_from_slice(payload);
    packet
}

fn with_mpeg_crc(mut section: Vec<u8>) -> Vec<u8> {
    let crc = ace_media::mpegts::mpeg_crc32(&section);
    section.extend_from_slice(&crc.to_be_bytes());
    section
}

fn audio_only_test_stream() -> Vec<u8> {
    let pat = with_mpeg_crc(vec![
        0x00, 0xb0, 0x0d, 0x00, 0x01, 0xc1, 0x00, 0x00, 0x00, 0x01, 0xe1, 0x00,
    ]);
    let pmt = with_mpeg_crc(vec![
        0x02, 0xb0, 0x12, 0x00, 0x01, 0xc1, 0x00, 0x00, 0xe1, 0x01, 0xf0, 0x00, 0x0f, 0xe1, 0x01,
        0xf0, 0x00,
    ]);
    let mut stream = Vec::new();
    let mut pat_payload = vec![0];
    pat_payload.extend_from_slice(&pat);
    stream.extend_from_slice(&test_ts_packet(0, true, 0, &pat_payload));
    let mut pmt_payload = vec![0];
    pmt_payload.extend_from_slice(&pmt);
    stream.extend_from_slice(&test_ts_packet(0x100, true, 0, &pmt_payload));
    for counter in 0..16u8 {
        // AAC-LC ADTS frame carried by an audio PES on PID 0x101.
        let mut pes = vec![0x00, 0x00, 0x01, 0xc0, 0x00, 0x0e, 0x80, 0x00, 0x00];
        pes.extend_from_slice(&[
            0xff, 0xf1, 0x50, 0x80, 0x01, 0x7f, 0xfc, 0x21, 0x10, 0x56, 0xe5,
        ]);
        stream.extend_from_slice(&test_ts_packet(0x101, true, counter, &pes));
    }
    stream
}

fn audio_only_packet_reached(bytes: &[u8]) -> bool {
    bytes.as_chunks::<188>().0.iter().any(|packet| {
        packet[0] == 0x47
            && (((packet[1] as u16 & 0x1f) << 8) | packet[2] as u16) == 0x101
            && packet.windows(2).any(|pair| pair == [0xff, 0xf1])
    })
}

#[derive(Clone, Copy)]
enum StartupPeerMode {
    Healthy,
    Silent,
    AdvancingHeads,
    NearBoundHealthy,
    NearBoundVideo,
    NearBoundAudio,
    ShortStaleHealthy,
    ChatterLateAdmission,
}

fn expected_source_marker(mode: StartupPeerMode, bytes: &[u8]) -> Option<u8> {
    match mode {
        StartupPeerMode::NearBoundVideo if bytes.windows(4).any(|n| n == [0, 0, 1, 0x65]) => {
            Some(0x77)
        }
        StartupPeerMode::NearBoundAudio if audio_only_packet_reached(bytes) => Some(0x78),
        StartupPeerMode::NearBoundVideo | StartupPeerMode::NearBoundAudio => None,
        _ if bytes.contains(&0x55) => Some(0x55),
        _ if bytes.contains(&0x66) => Some(0x66),
        _ => None,
    }
}

async fn read_until_source_marker(
    source: &mut Box<dyn TsSource>,
    mode: StartupPeerMode,
) -> Option<u8> {
    let mut output = Vec::new();
    while let Some(bytes) = source.next().await {
        output.extend_from_slice(&bytes);
        if let Some(marker) = expected_source_marker(mode, &output) {
            return Some(marker);
        }
    }
    None
}

fn stale_teardown_count(source: &dyn TsSource) -> u64 {
    source
        .stats()
        .recovery
        .map_or(0, |recovery| recovery.stale_teardowns)
}

#[derive(Debug)]
struct StartupProgressObservation {
    delivered: bool,
    from_a: bool,
    requested: bool,
    published_b: bool,
    tried_b: bool,
    first_request_after_ms: Option<u128>,
    survived_post_output_window: bool,
    late_admission_peers: u32,
    late_admission_published_after_request_ms: Option<u128>,
    late_admission_activated_after_request_ms: Option<u128>,
    late_admission_first_closed_after_request_ms: Option<u128>,
    late_admission_closed_by_original_bound: bool,
    stale_teardowns_after_admission: u64,
    stale_teardowns_before_original_expiry: u64,
    stale_teardowns_at_first_b_close: u64,
    stale_teardown_observed_in_original_window: bool,
    stale_teardown_after_request_ms: Option<u128>,
    late_admission_head_updates: u32,
    late_admission_requests: u32,
}

async fn startup_progress_recovery(
    mode: StartupPeerMode,
    active_cap: usize,
) -> StartupProgressObservation {
    let stale_ms = if matches!(mode, StartupPeerMode::ShortStaleHealthy) {
        200
    } else {
        2_000
    };
    let request_timeout_ms = if stale_ms == 200 { 100 } else { 900 };
    let request_check_interval_ms = if stale_ms == 200 { 20 } else { 100 };
    let auth = ace_wire::live_auth::LiveSourceAuth::generate();
    let mut signed_info = info();
    signed_info.sig_len = auth.signature_len();
    signed_info.source_pubkey = auth.pubkey_der();
    let a_signed_pieces = Arc::new(match mode {
        StartupPeerMode::NearBoundVideo => {
            let vector = include_bytes!("../../../../tests/vectors/media/h264-keyframes.ts");
            assert!(vector.windows(4).any(|window| window == [0, 0, 1, 0x65]));
            signed_media_pieces_from_ts(&auth, vector)
        }
        StartupPeerMode::NearBoundAudio => {
            signed_media_pieces_from_ts(&auth, &audio_only_test_stream())
        }
        _ => signed_media_pieces(&auth, 0x55),
    });
    let b_signed_pieces = Arc::new(signed_media_pieces(&auth, 0x66));
    let mut servers = tokio::task::JoinSet::new();
    let a_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(a_addr) = a_listener.local_addr().unwrap() else {
        unreachable!()
    };
    let b_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(b_addr) = b_listener.local_addr().unwrap() else {
        unreachable!()
    };
    let a_active = Arc::new(tokio::sync::Notify::new());
    let a_active_server = a_active.clone();
    let a_requests = Arc::new(AtomicU32::new(0));
    let a_requests_server = a_requests.clone();
    let first_request_at = Arc::new(std::sync::Mutex::new(None));
    let first_request_at_server = first_request_at.clone();
    let a_pieces = a_signed_pieces;
    servers.spawn(async move {
        let Ok((stream, _)) = a_listener.accept().await else {
            return;
        };
        let mut session = PeerSession::new(stream);
        let requests = a_requests_server;
        if session.accept_handshake([0; 20], |_| true).await.is_err() {
            return;
        }
        if session
            .send_extended_handshake(&OutgoingExtendedHandshake {
                ace_metadata_version: 1,
                ut_metadata_id: 2,
                mi: Some(LivePosition {
                    min_piece: if matches!(mode, StartupPeerMode::NearBoundVideo | StartupPeerMode::NearBoundAudio) { 0 } else { 7 },
                    max_piece: match mode {
                        StartupPeerMode::NearBoundVideo => 3,
                        StartupPeerMode::NearBoundAudio => 1,
                        _ => 8,
                    },
                    position: -1,
                    distance_from_source: 1,
                }),
                node: NodeFields::default(),
                peer_ip: None,
                metadata_size: None,
            })
            .await
            .is_err()
        {
            return;
        }
        if session.send(&PeerMessage::Unchoke).await.is_err() {
            return;
        }
        let mut head = 10u32;
        let mut delayed_first_output = false;
        let mut ticker = tokio::time::interval(Duration::from_millis(400));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick(), if matches!(mode, StartupPeerMode::AdvancingHeads | StartupPeerMode::ChatterLateAdmission) => {
                    head = head.saturating_add(1);
                    if session.send(&PeerMessage::Have(head)).await.is_err() {
                        return;
                    }
                    if matches!(mode, StartupPeerMode::ChatterLateAdmission)
                        && session.send(&PeerMessage::Unchoke).await.is_err()
                    {
                        return;
                    }
                }
                message = session.read_message() => match message {
                    Ok(PeerMessage::Unknown { id: 6, payload }) => {
                        if requests.fetch_add(1, Ordering::Relaxed) == 0 {
                            *first_request_at_server.lock().unwrap() = Some(Instant::now());
                        }
                        a_active_server.notify_one();
                        if matches!(
                            mode,
                            StartupPeerMode::NearBoundHealthy
                                | StartupPeerMode::NearBoundVideo
                                | StartupPeerMode::NearBoundAudio
                        )
                            && !delayed_first_output
                        {
                            delayed_first_output = true;
                            tokio::time::sleep(Duration::from_millis(1800)).await;
                        }
                        if matches!(
                            mode,
                            StartupPeerMode::Healthy
                                | StartupPeerMode::NearBoundHealthy
                                | StartupPeerMode::NearBoundVideo
                                | StartupPeerMode::NearBoundAudio
                                | StartupPeerMode::ShortStaleHealthy
                        ) {
                            let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                            let wire_piece = &a_pieces[piece as usize % a_pieces.len()];
                            if session.send(&build_piece(0, piece, 0, [0; 8], wire_piece)).await.is_err() {
                                return;
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(_) => return,
                }
            }
        }
    });

    let b_attempts = Arc::new(AtomicU32::new(0));
    let b_attempts_server = b_attempts.clone();
    let b_active = Arc::new(tokio::sync::Notify::new());
    let b_active_server = b_active.clone();
    let b_activated_at = Arc::new(std::sync::Mutex::new(None));
    let b_activated_at_server = b_activated_at.clone();
    let b_first_closed = Arc::new(tokio::sync::Notify::new());
    let b_first_closed_server = b_first_closed.clone();
    let b_first_closed_at = Arc::new(std::sync::Mutex::new(None));
    let b_first_closed_at_server = b_first_closed_at.clone();
    let b_first_requests = Arc::new(AtomicU32::new(0));
    let b_first_requests_server = b_first_requests.clone();
    let b_head_updates = Arc::new(AtomicU32::new(0));
    let b_head_updates_server = b_head_updates.clone();
    let b_pieces = b_signed_pieces;
    servers.spawn(async move {
        let mut attempt = 0u32;
        loop {
            let Ok((stream, _)) = b_listener.accept().await else {
                return;
            };
            attempt += 1;
            b_attempts_server.fetch_add(1, Ordering::Relaxed);
            let mut session = PeerSession::new(stream);
            if session.accept_handshake([0; 20], |_| true).await.is_err() {
                return;
            }
            if session
                .send_extended_handshake(&OutgoingExtendedHandshake {
                    ace_metadata_version: 1,
                    ut_metadata_id: 2,
                    mi: Some(LivePosition {
                        min_piece: 7,
                        max_piece: if matches!(mode, StartupPeerMode::ChatterLateAdmission)
                            && attempt > 1
                        {
                            10_000
                        } else {
                            1000
                        },
                        position: -1,
                        distance_from_source: 1,
                    }),
                    node: NodeFields::default(),
                    peer_ip: None,
                    metadata_size: None,
                })
                .await
                .is_err()
                || session.send(&PeerMessage::Unchoke).await.is_err()
            {
                return;
            }

            if matches!(mode, StartupPeerMode::ChatterLateAdmission) && attempt == 1 {
                let mut saw_signed_extension = false;
                let mut saw_interested = false;
                let mut active_recorded = false;
                let mut head = 1000u32;
                let mut ticks = 0u8;
                let mut ticker = tokio::time::interval(Duration::from_millis(150));
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            head = head.saturating_add(1);
                            ticks = ticks.wrapping_add(1);
                            b_head_updates_server.fetch_add(1, Ordering::Relaxed);
                            if session.send(&PeerMessage::Have(head)).await.is_err()
                                || (ticks.is_multiple_of(3) && session.send(&PeerMessage::Unchoke).await.is_err())
                            {
                                break;
                            }
                        }
                        message = session.read_message() => match message {
                            Ok(PeerMessage::Extended { ext_id: 0, .. }) => saw_signed_extension = true,
                            Ok(PeerMessage::Interested) => saw_interested = true,
                            Ok(PeerMessage::Unknown { id: 6, .. }) => {
                                b_first_requests_server.fetch_add(1, Ordering::Relaxed);
                            }
                            Ok(_) => {}
                            Err(_) => break,
                        }
                    }
                    if !active_recorded && saw_signed_extension && saw_interested {
                        active_recorded = true;
                        *b_activated_at_server.lock().unwrap() = Some(Instant::now());
                        b_active_server.notify_one();
                    }
                }
                *b_first_closed_at_server.lock().unwrap() = Some(Instant::now());
                b_first_closed_server.notify_one();
                continue;
            }

            while let Ok(message) = session.read_message().await {
                if let PeerMessage::Unknown { id: 6, payload } = message {
                    let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                    let wire_piece = &b_pieces[piece as usize % b_pieces.len()];
                    if session
                        .send(&build_piece(0, piece, 0, [0; 8], wire_piece))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            if !matches!(mode, StartupPeerMode::ChatterLateAdmission) || attempt > 1 {
                return;
            }
        }
    });

    let published_b = Arc::new(AtomicBool::new(false));
    let published_b_discovery = published_b.clone();
    let b_published_at = Arc::new(std::sync::Mutex::new(None));
    let b_published_at_discovery = b_published_at.clone();
    let published_notify = Arc::new(tokio::sync::Notify::new());
    let published_notify_discovery = published_notify.clone();
    let discovery_active = a_active.clone();
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        let published = published_b_discovery.clone();
        let published_at = b_published_at_discovery.clone();
        let published_notify = published_notify_discovery.clone();
        let active = discovery_active.clone();
        Box::pin(async move {
            sender.send(a_addr).await.unwrap();
            if matches!(mode, StartupPeerMode::Healthy) {
                sender.closed().await;
                return;
            }
            // The first request occurs only after the lone-peer corroboration window.
            // Publish B 1.2 seconds later, while A still occupies the active slot(s).
            active.notified().await;
            let admission_delay = if matches!(mode, StartupPeerMode::ChatterLateAdmission) {
                600
            } else {
                1200
            };
            tokio::time::sleep(Duration::from_millis(admission_delay)).await;
            *published_at.lock().unwrap() = Some(Instant::now());
            published.store(true, Ordering::Release);
            published_notify.notify_one();
            sender.send(b_addr).await.unwrap();
            sender.closed().await;
        })
    });

    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_prefetch_pieces(Some(1))
        .with_live_recovery(LiveRecoveryConfig {
            max_active_upstreams: active_cap,
            request_timeout_ms,
            stale_upstream_timeout_ms: stale_ms,
            request_check_interval_ms,
            ..LiveRecoveryConfig::default()
        })
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let opened_at = Instant::now();
    let mut source = match provider
        .open_resolved("synthetic", signed_info, discovery)
        .await
    {
        Ok(source) => source,
        Err(error) => {
            servers.abort_all();
            while servers.join_next().await.is_some() {}
            panic!("startup progress loopback open failed before observation: {error}");
        }
    };
    let mut late_admission_peers = 0;
    let mut late_admission_closed_by_original_bound = false;
    let mut stale_teardowns_after_admission = 0;
    let mut stale_teardowns_before_original_expiry = 0;
    let mut stale_teardowns_at_first_b_close = 0;
    let mut stale_teardown_observed_in_original_window = false;
    let mut stale_teardown_at = None;
    let (published_seen, output) = match mode {
        StartupPeerMode::Healthy => (
            true,
            tokio::time::timeout(
                Duration::from_secs(2),
                read_until_source_marker(&mut source, mode),
            )
            .await,
        ),
        StartupPeerMode::ShortStaleHealthy => {
            let output = tokio::time::timeout(
                Duration::from_secs(2),
                read_until_source_marker(&mut source, mode),
            )
            .await;
            let published =
                tokio::time::timeout(Duration::from_secs(4), published_notify.notified()).await;
            (published.is_ok(), output)
        }
        StartupPeerMode::ChatterLateAdmission => {
            let published =
                tokio::time::timeout(Duration::from_secs(4), published_notify.notified()).await;
            let activated = tokio::time::timeout(Duration::from_secs(2), b_active.notified()).await;
            late_admission_peers = source.stats().peers;
            stale_teardowns_after_admission = stale_teardown_count(source.as_ref());
            let first_request = *first_request_at.lock().unwrap();
            if activated.is_ok() {
                if let Some(first_request) = first_request {
                    // The admission is scheduled 600 ms after the original request. Allow
                    // 250 ms past T for cooperative loop scheduling, still 350 ms before a
                    // mistaken admission-time renewal could expire.
                    let before_expiry = first_request + Duration::from_millis(stale_ms - 250);
                    tokio::time::sleep_until(tokio::time::Instant::from_std(before_expiry)).await;
                    stale_teardowns_before_original_expiry = stale_teardown_count(source.as_ref());
                    let remained_zero =
                        stale_teardowns_before_original_expiry == stale_teardowns_after_admission;
                    let original_bound = first_request + Duration::from_millis(stale_ms + 250);
                    let observed_stale = tokio::time::timeout_at(
                        tokio::time::Instant::from_std(original_bound),
                        async {
                            loop {
                                if stale_teardown_count(source.as_ref())
                                    > stale_teardowns_after_admission
                                {
                                    return Some(Instant::now());
                                }
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                        },
                    )
                    .await
                    .ok()
                    .flatten();
                    stale_teardown_at = observed_stale;
                    stale_teardown_observed_in_original_window = remained_zero
                        && observed_stale.is_some_and(|observed| observed <= original_bound);
                    let _ = tokio::time::timeout_at(
                        tokio::time::Instant::from_std(original_bound),
                        b_first_closed.notified(),
                    )
                    .await;
                    late_admission_closed_by_original_bound = b_first_closed_at
                        .lock()
                        .unwrap()
                        .is_some_and(|closed_at| closed_at <= original_bound);
                    stale_teardowns_at_first_b_close = stale_teardown_count(source.as_ref());
                }
            }
            let output = tokio::time::timeout(
                Duration::from_secs(4),
                read_until_source_marker(&mut source, mode),
            )
            .await;
            (published.is_ok(), output)
        }
        StartupPeerMode::Silent
        | StartupPeerMode::AdvancingHeads
        | StartupPeerMode::NearBoundHealthy
        | StartupPeerMode::NearBoundVideo
        | StartupPeerMode::NearBoundAudio => {
            let published =
                tokio::time::timeout(Duration::from_secs(4), published_notify.notified()).await;
            let output = tokio::time::timeout(
                Duration::from_millis(stale_ms + 800),
                read_until_source_marker(&mut source, mode),
            )
            .await;
            (published.is_ok(), output)
        }
    };
    let marker = output.ok().flatten();
    let delivered = marker.is_some();
    let came_from_a = marker.is_some_and(|value| value != 0x66);
    let post_output_window_ms = match mode {
        StartupPeerMode::NearBoundHealthy
        | StartupPeerMode::NearBoundVideo
        | StartupPeerMode::NearBoundAudio => 500,
        StartupPeerMode::ShortStaleHealthy => 300,
        _ => 0,
    };
    let survived_post_output_window = if delivered && post_output_window_ms > 0 {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(post_output_window_ms);
        tokio::time::timeout_at(deadline, async {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => return true,
                    item = source.next() => if item.is_none() { return false; },
                }
            }
        })
        .await
        .unwrap_or(false)
    } else {
        true
    };
    let b_was_published = published_b.load(Ordering::Acquire);
    let first_request_after_ms = first_request_at
        .lock()
        .unwrap()
        .map(|first| first.duration_since(opened_at).as_millis());
    let first_request = *first_request_at.lock().unwrap();
    let late_admission_published_after_request_ms = first_request
        .zip(*b_published_at.lock().unwrap())
        .map(|(first, published)| published.duration_since(first).as_millis());
    let late_admission_activated_after_request_ms = first_request
        .zip(*b_activated_at.lock().unwrap())
        .map(|(first, activated)| activated.duration_since(first).as_millis());
    let late_admission_first_closed_after_request_ms = first_request
        .zip(*b_first_closed_at.lock().unwrap())
        .map(|(first, closed)| closed.duration_since(first).as_millis());
    let stale_teardown_after_request_ms = first_request
        .zip(stale_teardown_at)
        .map(|(first, teardown)| teardown.duration_since(first).as_millis());
    drop(source);
    servers.abort_all();
    while servers.join_next().await.is_some() {}
    StartupProgressObservation {
        delivered,
        from_a: came_from_a,
        requested: a_requests.load(Ordering::Relaxed) > 0,
        published_b: published_seen && b_was_published,
        tried_b: b_was_published && b_attempts.load(Ordering::Relaxed) > 0,
        first_request_after_ms,
        survived_post_output_window,
        late_admission_peers,
        late_admission_published_after_request_ms,
        late_admission_activated_after_request_ms,
        late_admission_first_closed_after_request_ms,
        late_admission_closed_by_original_bound,
        stale_teardowns_after_admission,
        stale_teardowns_before_original_expiry,
        stale_teardowns_at_first_b_close,
        stale_teardown_observed_in_original_window,
        stale_teardown_after_request_ms,
        late_admission_head_updates: b_head_updates.load(Ordering::Relaxed),
        late_admission_requests: b_first_requests.load(Ordering::Relaxed),
    }
}

#[tokio::test]
async fn startup_progress_healthy_peer_positive_control() {
    let observed = startup_progress_recovery(StartupPeerMode::Healthy, 1).await;
    assert!(
        observed.delivered,
        "healthy A must deliver signed contiguous TS"
    );
    assert!(observed.from_a, "positive-control media must come from A");
    assert!(observed.requested, "A must consume a production request");
    assert!(
        !observed.published_b,
        "healthy control should not publish a late B candidate"
    );
    assert!(
        !observed.tried_b,
        "healthy A must not require the late candidate"
    );
}

#[tokio::test]
async fn startup_progress_silent_peer_existing_recovery_control() {
    let observed = startup_progress_recovery(StartupPeerMode::Silent, 1).await;
    assert!(
        observed.delivered,
        "silent A must recover through healthy B"
    );
    assert!(!observed.from_a, "silent A cannot supply output");
    assert!(
        observed.requested,
        "silent A must consume production requests"
    );
    assert!(
        observed.published_b,
        "B must be published after A's corroboration window"
    );
    assert!(
        observed.tried_b,
        "published B must be tried after stale recovery"
    );
}

#[tokio::test]
async fn startup_progress_advancing_heads_do_not_starve_healthy_late_peer() {
    let observed = startup_progress_recovery(StartupPeerMode::AdvancingHeads, 1).await;
    assert!(
        observed.requested,
        "head-only A must consume production requests"
    );
    assert!(
        observed.published_b,
        "B must be published after A's corroboration window"
    );
    assert!(!observed.from_a, "head-only A cannot supply output");
    assert!(
        observed.delivered && observed.tried_b,
        "after B was published and one stale interval elapsed, authenticated contiguous TS was absent; B attempted={}",
        observed.tried_b
    );
}

#[tokio::test]
async fn startup_progress_advancing_heads_refills_spare_four_peer_capacity() {
    let observed = startup_progress_recovery(StartupPeerMode::AdvancingHeads, 4).await;
    assert!(
        observed.requested,
        "head-only A must consume production requests"
    );
    assert!(
        observed.published_b,
        "B must be published after A's corroboration window"
    );
    assert!(!observed.from_a, "head-only A cannot supply output");
    assert!(
        observed.tried_b,
        "a spare active slot must allow healthy B to connect"
    );
    assert!(
        observed.delivered,
        "healthy B must deliver actual TS through the spare slot"
    );
}

#[tokio::test]
async fn startup_progress_chatter_and_late_admission_do_not_reset_bound() {
    const STALE_MS: u128 = 2_000;
    let observed = startup_progress_recovery(StartupPeerMode::ChatterLateAdmission, 2).await;
    assert!(observed.requested, "A must consume a production request");
    assert!(
        observed
            .first_request_after_ms
            .is_some_and(|elapsed| elapsed >= 900),
        "the original request barrier must follow the lone-peer corroboration gate; {observed:?}"
    );
    assert!(observed.published_b, "real discovery must publish late B");
    assert!(
        observed
            .late_admission_published_after_request_ms
            .is_some_and(|elapsed| (500..STALE_MS).contains(&elapsed)),
        "B must be published after the original request barrier but before its T; {observed:?}"
    );
    assert!(
        observed
            .late_admission_activated_after_request_ms
            .is_some_and(|elapsed| elapsed < STALE_MS),
        "the new B must complete activation before the original first-output expiry; {observed:?}"
    );
    assert_eq!(
        observed.late_admission_peers, 2,
        "production must observe A and late B active at the admission barrier"
    );
    assert_eq!(
        observed.stale_teardowns_after_admission, 0,
        "no stale pool teardown may precede late B's active admission"
    );
    assert_eq!(
        observed.stale_teardowns_before_original_expiry, 0,
        "the admitted pool must remain free of stale teardown before the original T"
    );
    assert!(
        observed.late_admission_head_updates > 0,
        "the admitted unproductive B must continue advertising advancing heads"
    );
    assert!(
        observed.late_admission_requests > 0,
        "late B must participate in the active request pool without sending media"
    );
    assert!(
        observed
            .late_admission_first_closed_after_request_ms
            .is_some_and(|elapsed| (STALE_MS - 250..=STALE_MS + 250).contains(&elapsed)),
        "the unproductive B connection should close around the original T, not admission + T; {observed:?}"
    );
    assert!(
        observed.late_admission_closed_by_original_bound,
        "the original first-output deadline must tear down the admitted pool within T + 250 ms; {observed:?}"
    );
    assert_eq!(
        observed.stale_teardowns_at_first_b_close, 1,
        "the first B socket EOF must coincide with exactly one observed PoolStale teardown; {observed:?}"
    );
    assert!(
        observed.stale_teardown_observed_in_original_window
            && observed
                .stale_teardown_after_request_ms
                .is_some_and(|elapsed| (STALE_MS - 250..=STALE_MS + 250).contains(&elapsed)),
        "source recovery stats must report StaleTeardown inside the original request+T window; {observed:?}"
    );
    assert!(
        observed.delivered && !observed.from_a && observed.tried_b,
        "after ordinary stale recovery, healthy B must deliver signed contiguous TS; {observed:?}"
    );
}

#[tokio::test]
async fn startup_progress_healthy_peer_just_before_bound_survives_past_it() {
    let observed = startup_progress_recovery(StartupPeerMode::NearBoundHealthy, 1).await;
    assert!(
        observed.requested,
        "healthy A must receive a production request"
    );
    assert!(
        observed.published_b,
        "healthy B is published while A delays output"
    );
    assert!(
        observed
            .first_request_after_ms
            .is_some_and(|elapsed| elapsed >= 900),
        "first output budget opportunity begins after the lone-peer corroboration gate; observed={observed:?}"
    );
    assert!(
        observed.delivered && observed.from_a,
        "A must deliver signed TS before its bound"
    );
    assert!(
        observed.survived_post_output_window,
        "A must remain usable beyond the former startup bound"
    );
    assert!(
        !observed.tried_b,
        "late B must stay unused after A's first output"
    );
}

#[tokio::test]
async fn startup_progress_healthy_video_just_before_bound_is_not_rebuilt() {
    let observed = startup_progress_recovery(StartupPeerMode::NearBoundVideo, 1).await;
    assert!(observed.requested, "healthy video A must receive requests");
    assert!(
        observed
            .first_request_after_ms
            .is_some_and(|elapsed| elapsed >= 900),
        "the video request opportunity begins after the lone-peer corroboration gate; observed={observed:?}"
    );
    assert!(
        observed.published_b,
        "healthy video B is queued while A delays output"
    );
    assert!(
        observed.delivered && observed.from_a,
        "A must produce the signed H.264 IDR TS"
    );
    assert!(
        observed.survived_post_output_window,
        "video A must remain active after crossing its former no-output deadline"
    );
    assert!(
        !observed.tried_b,
        "the late candidate must not replace productive video A"
    );
}

#[tokio::test]
async fn startup_progress_healthy_audio_just_before_bound_is_not_rebuilt() {
    let observed = startup_progress_recovery(StartupPeerMode::NearBoundAudio, 1).await;
    assert!(observed.requested, "healthy audio A must receive requests");
    assert!(
        observed
            .first_request_after_ms
            .is_some_and(|elapsed| elapsed >= 900),
        "the audio request opportunity begins after the lone-peer corroboration gate; observed={observed:?}"
    );
    assert!(
        observed.published_b,
        "healthy audio B is queued while A delays output"
    );
    assert!(
        observed.delivered && observed.from_a,
        "A must output signed audio-only TS"
    );
    assert!(
        observed.survived_post_output_window,
        "audio-only output must count as usable source progress"
    );
    assert!(
        !observed.tried_b,
        "the late candidate must not replace productive audio A"
    );
}

#[tokio::test]
async fn startup_progress_short_stale_interval_starts_after_corroboration() {
    let observed = startup_progress_recovery(StartupPeerMode::ShortStaleHealthy, 1).await;
    assert!(
        observed.requested,
        "A must receive a request after corroboration"
    );
    assert!(
        observed
            .first_request_after_ms
            .is_some_and(|elapsed| elapsed >= 900),
        "200 ms stale interval must not expire during the existing corroboration gate; observed={observed:?}"
    );
    assert!(
        observed.delivered && observed.from_a,
        "A must deliver signed TS"
    );
    assert!(
        observed.published_b,
        "B publication should occur after the request gate"
    );
}

#[derive(Debug)]
struct FullPoolObservation {
    activated: u32,
    requests: u32,
    peers_at_publish: u32,
    published: bool,
    b_attempts_before_expiry: u32,
    b_attempts_after_wait: u32,
    b_output: bool,
    active_connections_closed: u32,
    discovery_stopped: bool,
}

async fn startup_progress_full_four_peer_pool(cancel_after_publish: bool) -> FullPoolObservation {
    const ACTIVE_CAP: usize = 4;
    const STALE_MS: u64 = 2_000;
    let auth = ace_wire::live_auth::LiveSourceAuth::generate();
    let mut signed_info = info();
    signed_info.sig_len = auth.signature_len();
    signed_info.source_pubkey = auth.pubkey_der();
    let b_pieces = Arc::new(signed_media_pieces(&auth, 0x66));
    let mut servers = tokio::task::JoinSet::new();
    let activated = Arc::new(AtomicU32::new(0));
    let activation_notify = Arc::new(tokio::sync::Notify::new());
    let requests = Arc::new(AtomicU32::new(0));
    let request_notify = Arc::new(tokio::sync::Notify::new());
    let active_connections_closed = Arc::new(AtomicU32::new(0));
    let mut a_addrs = Vec::with_capacity(ACTIVE_CAP);

    for _ in 0..ACTIVE_CAP {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        a_addrs.push(addr);
        let activated_server = activated.clone();
        let activation_notify_server = activation_notify.clone();
        let requests_server = requests.clone();
        let request_notify_server = request_notify.clone();
        let closed_server = active_connections_closed.clone();
        servers.spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let mut session = PeerSession::new(stream);
            if session.accept_handshake([0; 20], |_| true).await.is_err() {
                return;
            }
            if session
                .send_extended_handshake(&OutgoingExtendedHandshake {
                    ace_metadata_version: 1,
                    ut_metadata_id: 2,
                    mi: Some(LivePosition {
                        min_piece: 7,
                        max_piece: 10,
                        position: -1,
                        distance_from_source: 1,
                    }),
                    node: NodeFields::default(),
                    peer_ip: None,
                    metadata_size: None,
                })
                .await
                .is_err()
                || session.send(&PeerMessage::Unchoke).await.is_err()
            {
                return;
            }
            let mut saw_signed_extension = false;
            let mut saw_interested = false;
            let mut counted_active = false;
            let mut head = 10u32;
            let mut ticker = tokio::time::interval(Duration::from_millis(400));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        head = head.saturating_add(1);
                        if session.send(&PeerMessage::Have(head)).await.is_err() {
                            closed_server.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    }
                    message = session.read_message() => match message {
                        Ok(PeerMessage::Extended { ext_id: 0, .. }) => saw_signed_extension = true,
                        Ok(PeerMessage::Interested) => saw_interested = true,
                        Ok(PeerMessage::Unknown { id: 6, payload }) => {
                            if requests_server.fetch_add(1, Ordering::Relaxed) == 0 {
                                request_notify_server.notify_one();
                            }
                            let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                            let _ = piece;
                        }
                        Ok(_) => {}
                        Err(_) => {
                            closed_server.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    }
                }
                if !counted_active && saw_signed_extension && saw_interested {
                    counted_active = true;
                    if activated_server.fetch_add(1, Ordering::Relaxed) + 1 == ACTIVE_CAP as u32 {
                        activation_notify_server.notify_one();
                    }
                }
            }
        });
    }

    let b_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(b_addr) = b_listener.local_addr().unwrap() else {
        unreachable!()
    };
    let b_attempts = Arc::new(AtomicU32::new(0));
    let b_attempts_server = b_attempts.clone();
    servers.spawn(async move {
        let Ok((stream, _)) = b_listener.accept().await else {
            return;
        };
        b_attempts_server.fetch_add(1, Ordering::Relaxed);
        let mut session = PeerSession::new(stream);
        if session.accept_handshake([0; 20], |_| true).await.is_err() {
            return;
        }
        if session
            .send_extended_handshake(&OutgoingExtendedHandshake {
                ace_metadata_version: 1,
                ut_metadata_id: 2,
                mi: Some(LivePosition {
                    min_piece: 7,
                    max_piece: 1000,
                    position: -1,
                    distance_from_source: 1,
                }),
                node: NodeFields::default(),
                peer_ip: None,
                metadata_size: None,
            })
            .await
            .is_err()
            || session.send(&PeerMessage::Unchoke).await.is_err()
        {
            return;
        }
        while let Ok(message) = session.read_message().await {
            if let PeerMessage::Unknown { id: 6, payload } = message {
                let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                let wire_piece = &b_pieces[piece as usize % b_pieces.len()];
                if session
                    .send(&build_piece(0, piece, 0, [0; 8], wire_piece))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    });

    struct DiscoveryStopped(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for DiscoveryStopped {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    let (discovery_stopped_tx, discovery_stopped_rx) = tokio::sync::oneshot::channel();
    let (published_tx, published_rx) = tokio::sync::oneshot::channel();
    let discovery_state = Arc::new(std::sync::Mutex::new(Some((
        discovery_stopped_tx,
        published_tx,
    ))));
    let discovery_activated = activated.clone();
    let discovery_activation_notify = activation_notify.clone();
    let discovery_requests = requests.clone();
    let discovery_request_notify = request_notify.clone();
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        let addresses = a_addrs.clone();
        let state = discovery_state.clone();
        let active = discovery_activated.clone();
        let active_notify = discovery_activation_notify.clone();
        let request_count = discovery_requests.clone();
        let first_request = discovery_request_notify.clone();
        Box::pin(async move {
            let (stopped, published) = state.lock().unwrap().take().unwrap();
            let _stopped = DiscoveryStopped(Some(stopped));
            for address in addresses {
                sender.try_send(address).unwrap();
            }
            tokio::time::timeout(Duration::from_secs(3), async {
                while active.load(Ordering::Acquire) < ACTIVE_CAP as u32 {
                    active_notify.notified().await;
                }
            })
            .await
            .unwrap();
            tokio::time::timeout(Duration::from_secs(1), async {
                while request_count.load(Ordering::Acquire) == 0 {
                    first_request.notified().await;
                }
            })
            .await
            .unwrap();
            // The fresh four-peer pool has no lone-peer gate. Hold B back beyond the
            // first scheduling pass so it is known only after all four are active.
            tokio::time::sleep(Duration::from_millis(1200)).await;
            sender.send(b_addr).await.unwrap();
            let _ = published.send(());
            sender.closed().await;
        })
    });

    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_prefetch_pieces(Some(1))
        .with_live_recovery(LiveRecoveryConfig {
            max_active_upstreams: ACTIVE_CAP,
            request_timeout_ms: 900,
            stale_upstream_timeout_ms: STALE_MS,
            request_check_interval_ms: 100,
            ..LiveRecoveryConfig::default()
        })
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let mut source = match provider
        .open_resolved("synthetic", signed_info, discovery)
        .await
    {
        Ok(source) => source,
        Err(error) => {
            servers.abort_all();
            while servers.join_next().await.is_some() {}
            panic!("four-peer loopback open failed before observation: {error}");
        }
    };
    let published = tokio::time::timeout(Duration::from_secs(5), published_rx).await;
    let published = matches!(published, Ok(Ok(())));
    let peers_at_publish = source.stats().peers;
    let b_attempts_before_expiry = if published {
        tokio::time::sleep(Duration::from_millis(100)).await;
        b_attempts.load(Ordering::Relaxed)
    } else {
        b_attempts.load(Ordering::Relaxed)
    };
    let b_output = if cancel_after_publish || !published {
        false
    } else {
        matches!(
            tokio::time::timeout(Duration::from_millis(STALE_MS + 800), async {
                loop {
                    match source.next().await {
                        Some(bytes) if bytes.contains(&0x66) => return true,
                        Some(_) => {}
                        None => return false,
                    }
                }
            })
            .await,
            Ok(true)
        )
    };
    if cancel_after_publish {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    drop(source);
    let active_closed = tokio::time::timeout(Duration::from_secs(1), async {
        while active_connections_closed.load(Ordering::Acquire) < ACTIVE_CAP as u32 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let discovery_stopped = tokio::time::timeout(Duration::from_secs(1), discovery_stopped_rx)
        .await
        .is_ok_and(|stopped| stopped.is_ok());
    let active_closed_count = active_connections_closed.load(Ordering::Relaxed);
    let b_attempts_after_wait = b_attempts.load(Ordering::Relaxed);
    servers.abort_all();
    while servers.join_next().await.is_some() {}

    FullPoolObservation {
        activated: activated.load(Ordering::Relaxed),
        requests: requests.load(Ordering::Relaxed),
        peers_at_publish,
        published,
        b_attempts_before_expiry,
        b_attempts_after_wait,
        b_output,
        active_connections_closed: active_closed_count,
        discovery_stopped: active_closed && discovery_stopped,
    }
}

#[tokio::test]
async fn startup_progress_four_advancing_peers_full_pool_recovers_late_healthy_peer() {
    let observed = startup_progress_full_four_peer_pool(false).await;
    assert_eq!(observed.activated, 4, "all four initial As must activate");
    assert_eq!(
        observed.peers_at_publish, 4,
        "production peer count must be full"
    );
    assert!(
        observed.requests > 0,
        "the active A pool must consume requests"
    );
    assert!(
        observed.published,
        "late B must be published through discovery"
    );
    assert_eq!(
        observed.b_attempts_before_expiry, 0,
        "a still-live full pool must retain B until stale recovery"
    );
    assert!(
        observed.b_output && observed.b_attempts_after_wait > 0,
        "after one stale interval, B should be attempted and deliver authenticated contiguous TS; observed={observed:?}"
    );
    assert_eq!(
        observed.active_connections_closed, 4,
        "dropping the source must close all four active A connections"
    );
    assert!(
        observed.discovery_stopped,
        "source drop must stop discovery ownership"
    );
}

#[tokio::test]
async fn startup_progress_consumer_drop_before_first_output_cleans_full_pool() {
    let observed = startup_progress_full_four_peer_pool(true).await;
    assert_eq!(observed.activated, 4, "all four initial As must activate");
    assert_eq!(
        observed.peers_at_publish, 4,
        "production peer count must be full"
    );
    assert!(
        observed.requests > 0,
        "the active A pool must consume requests"
    );
    assert!(
        observed.published,
        "late B must be published through discovery"
    );
    assert_eq!(observed.b_attempts_before_expiry, 0);
    assert_eq!(
        observed.b_attempts_after_wait, 0,
        "consumer drop must not admit B"
    );
    assert_eq!(
        observed.active_connections_closed, 4,
        "consumer drop must close every active A socket"
    );
    assert!(
        observed.discovery_stopped,
        "consumer drop must stop discovery"
    );
}

async fn late_candidate_media_control(late: bool, silent_slot: bool, fail_first: bool) {
    let mut servers = tokio::task::JoinSet::new();
    let mut addrs = Vec::new();
    let healthy_attempts = Arc::new(AtomicU32::new(0));
    for healthy in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        addrs.push(addr);
        let attempts = healthy_attempts.clone();
        servers.spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let attempts = attempts.clone();
                connections.spawn(async move {
                    if healthy {
                        let attempt = attempts.fetch_add(1, Ordering::Relaxed);
                        if fail_first && attempt == 0 {
                            return;
                        }
                    }
                    let mut session = PeerSession::new(stream);
                    if session.accept_handshake([0; 20], |_| true).await.is_err() {
                        return;
                    }
                    if session
                        .send_extended_handshake(&OutgoingExtendedHandshake {
                            ace_metadata_version: 1,
                            ut_metadata_id: 2,
                            mi: Some(LivePosition {
                                min_piece: 7,
                                max_piece: if healthy { 10 } else { 8 },
                                position: -1,
                                distance_from_source: 1,
                            }),
                            node: NodeFields::default(),
                            peer_ip: None,
                            metadata_size: None,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                    let _ = session.send(&PeerMessage::Unchoke).await;
                    while let Ok(msg) = session.read_message().await {
                        if healthy {
                            if let PeerMessage::Unknown { id: 6, payload } = msg {
                                let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                                if session
                                    .send(&build_piece(0, piece, 0, [0; 8], &media_piece(0x66)))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                    }
                });
            }
        });
    }
    let relay = addrs[0];
    let healthy = addrs[1];
    let silent = if silent_slot {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        servers.spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut byte = [0];
            while tokio::io::AsyncReadExt::read(&mut stream, &mut byte)
                .await
                .unwrap()
                > 0
            {}
        });
        Some(addr)
    } else {
        None
    };
    let calls = Arc::new(AtomicU32::new(0));
    let called = calls.clone();
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        called.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move {
            sender
                .send(if late { relay } else { healthy })
                .await
                .unwrap();
            if late {
                if let Some(silent) = silent {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    sender.send(silent).await.unwrap();
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                sender.send(healthy).await.unwrap();
            }
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_prefetch_pieces(Some(1))
        .with_live_recovery(LiveRecoveryConfig {
            max_parallel_connect: if silent_slot { 1 } else { 12 },
            ..LiveRecoveryConfig::default()
        })
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let mut source = provider
        .open_resolved("synthetic", info(), discovery)
        .await
        .unwrap();
    let output = tokio::time::timeout(
        Duration::from_secs(if silent_slot { 5 } else { 3 }),
        source.next(),
    )
    .await;
    drop(source);
    drop(servers);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "cold discovery must remain the sole generation"
    );
    assert!(
        healthy_attempts.load(Ordering::Relaxed) > 0,
        "late candidate was never connected by the active pool"
    );
    assert!(
        matches!(output,Ok(Some(ref bytes)) if bytes.contains(&0x66)),
        "late source must supply actual current media"
    );
}

#[tokio::test]
async fn late_discovery_candidate_enters_existing_active_pool() {
    late_candidate_media_control(true, false, false).await;
}

#[tokio::test]
async fn late_discovery_same_healthy_peer_positive_control() {
    late_candidate_media_control(false, false, false).await;
}

#[tokio::test]
async fn late_discovery_refills_after_silent_reserved_attempt_completes() {
    late_candidate_media_control(true, true, false).await;
}

#[tokio::test]
async fn quiet_candidate_cooldown_expiry_retries_without_new_discovery_or_gossip() {
    late_candidate_media_control(true, false, true).await;
}

#[derive(Clone, Copy)]
enum InitialGeneration {
    EmptyThenHealthy,
    Healthy,
}

async fn completed_empty_generation_control(mode: InitialGeneration) {
    const HARNESS_DEADLINE: Duration = Duration::from_secs(8);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(healthy_addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let healthy_handshakes = Arc::new(AtomicU32::new(0));
    let observed_handshakes = healthy_handshakes.clone();
    let mut server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut session = PeerSession::new(stream);
        session.accept_handshake([0; 20], |_| true).await.unwrap();
        observed_handshakes.fetch_add(1, Ordering::Relaxed);
        session
            .send_extended_handshake(&OutgoingExtendedHandshake {
                ace_metadata_version: 1,
                ut_metadata_id: 2,
                mi: Some(LivePosition {
                    min_piece: 7,
                    max_piece: 10,
                    position: -1,
                    distance_from_source: 1,
                }),
                node: NodeFields::default(),
                peer_ip: None,
                metadata_size: None,
            })
            .await
            .unwrap();
        session.send(&PeerMessage::Unchoke).await.unwrap();
        while let Ok(message) = session.read_message().await {
            if let PeerMessage::Unknown { id: 6, payload } = message {
                let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                if session
                    .send(&build_piece(0, piece, 0, [0; 8], &media_piece(0x66)))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    });

    let generations = Arc::new(AtomicU32::new(0));
    let observed_generations = generations.clone();
    let (first_completed_tx, first_completed_rx) = tokio::sync::oneshot::channel();
    let first_completed_tx = Arc::new(std::sync::Mutex::new(Some(first_completed_tx)));
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        let generation = observed_generations.fetch_add(1, Ordering::Relaxed) + 1;
        let first_completed = first_completed_tx.clone();
        Box::pin(async move {
            match (mode, generation) {
                (InitialGeneration::EmptyThenHealthy, 1) => {
                    discover_peers_from_sources_incremental(
                        async { Vec::new() },
                        async { Vec::new() },
                        sender,
                    )
                    .await;
                    if let Some(completed) = first_completed.lock().unwrap().take() {
                        let _ = completed.send(());
                    }
                }
                (InitialGeneration::EmptyThenHealthy, 2) | (InitialGeneration::Healthy, 1) => {
                    sender.send(healthy_addr).await.unwrap();
                    sender.closed().await;
                }
                _ => {}
            }
        })
    });

    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_prefetch_pieces(Some(1))
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let playback = tokio::time::timeout(HARNESS_DEADLINE, async move {
        let mut source = provider
            .open_resolved("synthetic", info(), discovery)
            .await
            .map_err(|error| error.to_string())?;
        let first_media = source.next().await;
        drop(source);
        Ok::<_, String>(first_media)
    })
    .await;

    // Allow the real source/discovery ownership path to observe consumer closure before
    // assertions. The listener task is always bounded and aborted/awaited if it stays open.
    let server_finished = tokio::time::timeout(Duration::from_secs(1), &mut server).await;
    match server_finished {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("healthy server task failed: {error}"),
        Err(_) => {
            server.abort();
            let aborted = server.await;
            assert!(
                aborted.is_err_and(|error| error.is_cancelled()),
                "healthy server did not stop after bounded abort"
            );
        }
    }
    let first_completed = if matches!(mode, InitialGeneration::EmptyThenHealthy) {
        tokio::time::timeout(Duration::from_secs(1), first_completed_rx).await
    } else {
        Ok(Ok(()))
    };

    assert!(
        first_completed.is_ok() && first_completed.unwrap().is_ok(),
        "the completed-empty first generation did not finish"
    );
    let expected_generations = match mode {
        InitialGeneration::EmptyThenHealthy => 2,
        InitialGeneration::Healthy => 1,
    };
    let healthy_media = matches!(playback, Ok(Ok(Some(ref bytes))) if bytes.contains(&0x66));
    assert_eq!(
        generations.load(Ordering::Relaxed),
        expected_generations,
        "discovery generation count; playback={playback:?}, healthy handshakes={}",
        healthy_handshakes.load(Ordering::Relaxed)
    );
    assert!(
        healthy_media,
        "one resolved-open call under the {HARNESS_DEADLINE:?} harness bound must reach a real healthy handshake and deliver synthetic media; playback={playback:?}, handshakes={}, generations={}",
        healthy_handshakes.load(Ordering::Relaxed),
        generations.load(Ordering::Relaxed),
    );
    assert_eq!(
        healthy_handshakes.load(Ordering::Relaxed),
        1,
        "the loopback peer must complete exactly one real ACE handshake"
    );
}

#[tokio::test]
async fn completed_empty_initial_discovery_recovers_in_the_same_open_call() {
    completed_empty_generation_control(InitialGeneration::EmptyThenHealthy).await;
}

#[tokio::test]
async fn direct_healthy_initial_discovery_delivers_media_positive_control() {
    completed_empty_generation_control(InitialGeneration::Healthy).await;
}

struct GenerationDropNotice {
    generation: u32,
    notices: tokio::sync::mpsc::UnboundedSender<u32>,
}
impl Drop for GenerationDropNotice {
    fn drop(&mut self) {
        let _ = self.notices.send(self.generation);
    }
}

async fn yield_discovery_tasks() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

fn spawn_resolved_open(
    provider: AceProvider,
    discovery: PeerDiscovery,
) -> tokio::task::JoinHandle<Result<(), String>> {
    tokio::spawn(async move {
        provider
            .open_resolved("synthetic", info(), discovery)
            .await
            .map(drop)
            .map_err(|error| error.to_string())
    })
}

#[tokio::test(start_paused = true)]
async fn initial_discovery_policy_caps_empty_generations_and_waits_one_second() {
    let calls = Arc::new(AtomicU32::new(0));
    let observed_calls = calls.clone();
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let (dropped_tx, mut dropped_rx) = tokio::sync::mpsc::unbounded_channel();
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        let generation = observed_calls.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = started_tx.send(generation);
        let dropped = dropped_tx.clone();
        Box::pin(async move {
            let _notice = GenerationDropNotice {
                generation,
                notices: dropped,
            };
            drop(sender);
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0);
    let mut open = spawn_resolved_open(provider, discovery);

    let first_started = tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(1);
    let first_completed = tokio::time::timeout(Duration::from_secs(1), dropped_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(1);
    yield_discovery_tasks().await;
    let waiting_after_first = !open.is_finished();
    tokio::time::advance(Duration::from_millis(999)).await;
    yield_discovery_tasks().await;
    let waiting_before_retry_gap_ends = !open.is_finished() && calls.load(Ordering::Relaxed) == 1;
    tokio::time::advance(Duration::from_millis(1)).await;
    let second_started = tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(2);
    let second_completed = tokio::time::timeout(Duration::from_secs(1), dropped_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(2);
    let open_result = if open.is_finished() {
        Some(open.await)
    } else {
        match tokio::time::timeout(Duration::from_secs(1), &mut open).await {
            Ok(result) => Some(result),
            Err(_) => {
                open.abort();
                let _ = open.await;
                None
            }
        }
    };

    assert!(
        first_started && first_completed,
        "first empty generation must complete"
    );
    assert!(
        waiting_after_first && waiting_before_retry_gap_ends,
        "opening must remain pending until the full one-second retry gap elapses"
    );
    assert!(
        second_started && second_completed,
        "exactly a second generation must run"
    );
    assert_eq!(
        calls.load(Ordering::Relaxed),
        2,
        "a third generation must not start"
    );
    assert!(
        matches!(open_result, Some(Ok(Err(_)))),
        "two completed-empty generations must terminate with a backend error: {open_result:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn initial_discovery_policy_bounds_a_hanging_first_generation() {
    let calls = Arc::new(AtomicU32::new(0));
    let observed_calls = calls.clone();
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let (dropped_tx, mut dropped_rx) = tokio::sync::mpsc::unbounded_channel();
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        let generation = observed_calls.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = started_tx.send(generation);
        let dropped = dropped_tx.clone();
        Box::pin(async move {
            let _notice = GenerationDropNotice {
                generation,
                notices: dropped,
            };
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0);
    let open = spawn_resolved_open(provider, discovery);
    let first_started = tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(1);

    tokio::time::advance(Duration::from_secs(10)).await;
    yield_discovery_tasks().await;
    let expired_at_original_budget = open.is_finished();
    let open_result = if expired_at_original_budget {
        Some(open.await)
    } else {
        open.abort();
        let _ = open.await;
        None
    };
    let first_dropped = tokio::time::timeout(Duration::from_secs(1), dropped_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(1);

    assert!(first_started, "the first generation must start");
    assert!(
        expired_at_original_budget,
        "a hanging first generation must hit the 10s budget"
    );
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "a pending generation must not be restarted"
    );
    assert!(
        first_dropped,
        "budget expiry must cancel the discovery owner"
    );
    assert!(
        matches!(open_result, Some(Ok(Err(_)))),
        "budget expiry must return a backend error: {open_result:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn initial_discovery_policy_does_not_reset_budget_and_rejects_late_candidate() {
    let calls = Arc::new(AtomicU32::new(0));
    let observed_calls = calls.clone();
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let (second_sender_tx, second_sender_rx) = tokio::sync::oneshot::channel();
    let second_sender_tx = Arc::new(std::sync::Mutex::new(Some(second_sender_tx)));
    let (dropped_tx, mut dropped_rx) = tokio::sync::mpsc::unbounded_channel();
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        let generation = observed_calls.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = started_tx.send(generation);
        let second_sender = second_sender_tx.clone();
        let dropped = dropped_tx.clone();
        Box::pin(async move {
            let _notice = GenerationDropNotice {
                generation,
                notices: dropped,
            };
            if generation == 1 {
                drop(sender);
            } else {
                if let Some(deliver) = second_sender.lock().unwrap().take() {
                    let _ = deliver.send(sender.clone());
                }
                sender.closed().await;
            }
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0);
    let open = spawn_resolved_open(provider, discovery);
    let first_started = tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(1);
    let first_dropped = tokio::time::timeout(Duration::from_secs(1), dropped_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(1);
    tokio::time::advance(Duration::from_secs(1)).await;
    let second_started = tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(2);
    let second_sender = tokio::time::timeout(Duration::from_secs(1), second_sender_rx).await;

    // Generation two began at t=1s. The original t=10s deadline must still expire here.
    tokio::time::advance(Duration::from_secs(9)).await;
    yield_discovery_tasks().await;
    let expired_at_original_budget = open.is_finished();
    let open_result = if expired_at_original_budget {
        Some(open.await)
    } else {
        open.abort();
        let _ = open.await;
        None
    };
    let late_candidate_rejected = match second_sender {
        Ok(Ok(sender)) => sender
            .send(SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 1))
            .await
            .is_err(),
        _ => false,
    };
    let second_dropped = tokio::time::timeout(Duration::from_secs(1), dropped_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(2);

    assert!(
        first_started && first_dropped,
        "the first generation must complete empty"
    );
    assert!(
        second_started,
        "the second generation must start after the retry gap"
    );
    assert!(
        expired_at_original_budget,
        "the 10s opening budget must not restart when generation two starts"
    );
    assert!(
        late_candidate_rejected,
        "a candidate arriving after expiry must be rejected"
    );
    assert!(second_dropped, "budget expiry must cancel generation two");
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    assert!(
        matches!(open_result, Some(Ok(Err(_)))),
        "the original budget must terminate opening with a backend error: {open_result:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn exact_initial_discovery_deadline_rejects_candidate_ready_before_repoll() {
    let calls = Arc::new(AtomicU32::new(0));
    let observed_calls = calls.clone();
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let (second_sender_tx, second_sender_rx) = tokio::sync::oneshot::channel();
    let second_sender_tx = Arc::new(std::sync::Mutex::new(Some(second_sender_tx)));
    let (dropped_tx, mut dropped_rx) = tokio::sync::mpsc::unbounded_channel();
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        let generation = observed_calls.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = started_tx.send(generation);
        let second_sender = second_sender_tx.clone();
        let dropped = dropped_tx.clone();
        Box::pin(async move {
            let _notice = GenerationDropNotice {
                generation,
                notices: dropped,
            };
            if generation == 1 {
                drop(sender);
            } else {
                if let Some(deliver) = second_sender.lock().unwrap().take() {
                    let _ = deliver.send(sender.clone());
                }
                sender.closed().await;
            }
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0);
    let mut open = Box::pin(provider.open_resolved("synthetic", info(), discovery));

    let initially_pending = futures::poll!(open.as_mut()).is_pending();
    let first_started = tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(1);
    let first_dropped = tokio::time::timeout(Duration::from_secs(1), dropped_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(1);
    let waiting_in_gap = futures::poll!(open.as_mut()).is_pending();
    tokio::time::advance(Duration::from_secs(1)).await;
    let after_gap = futures::poll!(open.as_mut());
    let second_started = tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(2);
    let second_sender = match tokio::time::timeout(Duration::from_secs(1), second_sender_rx).await {
        Ok(Ok(sender)) => Some(sender),
        _ => None,
    };
    let waiting_for_candidate = futures::poll!(open.as_mut()).is_pending();

    // Advance to the absolute t=10s boundary without polling the opening future, then enqueue
    // a candidate synchronously before the next poll can choose between candidate and timeout.
    tokio::time::advance(Duration::from_secs(9)).await;
    let candidate_ready_at_expiry = match &second_sender {
        Some(sender) => sender
            .try_send(SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 1))
            .is_ok(),
        None => false,
    };
    let result = futures::poll!(open.as_mut());
    let exact_expiry_rejected = matches!(result, std::task::Poll::Ready(Err(_)));
    let channel_closed = second_sender
        .as_ref()
        .is_some_and(|sender| sender.is_closed());
    drop(open);
    let second_dropped = tokio::time::timeout(Duration::from_secs(1), dropped_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(2);

    assert!(initially_pending && first_started && first_dropped);
    assert!(waiting_in_gap && after_gap.is_pending() && second_started);
    assert!(
        waiting_for_candidate,
        "second generation must remain pending before expiry"
    );
    assert!(
        candidate_ready_at_expiry,
        "candidate must be queued at the exact deadline"
    );
    assert!(exact_expiry_rejected && channel_closed);
    assert!(second_dropped, "expiry must cancel the second generation");
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

#[tokio::test(start_paused = true)]
async fn canceling_during_initial_discovery_retry_gap_prevents_second_generation() {
    let calls = Arc::new(AtomicU32::new(0));
    let observed_calls = calls.clone();
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let (dropped_tx, mut dropped_rx) = tokio::sync::mpsc::unbounded_channel();
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        let generation = observed_calls.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = started_tx.send(generation);
        let dropped = dropped_tx.clone();
        Box::pin(async move {
            let _notice = GenerationDropNotice {
                generation,
                notices: dropped,
            };
            drop(sender);
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0);
    let open = spawn_resolved_open(provider, discovery);
    let first_started = tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(1);
    let first_dropped = tokio::time::timeout(Duration::from_secs(1), dropped_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(1);
    yield_discovery_tasks().await;
    let in_retry_gap = !open.is_finished();
    open.abort();
    let canceled = open.await.is_err_and(|error| error.is_cancelled());
    tokio::time::advance(Duration::from_secs(2)).await;
    yield_discovery_tasks().await;

    assert!(
        first_started && first_dropped,
        "the first generation must finish empty"
    );
    assert!(
        in_retry_gap,
        "opening must still be pending in the retry gap"
    );
    assert!(canceled, "dropping opening during the gap must cancel it");
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "the retry must not detach after cancellation"
    );
}

#[tokio::test(start_paused = true)]
async fn canceling_during_second_initial_generation_drops_its_owner() {
    let calls = Arc::new(AtomicU32::new(0));
    let observed_calls = calls.clone();
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let (dropped_tx, mut dropped_rx) = tokio::sync::mpsc::unbounded_channel();
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        let generation = observed_calls.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = started_tx.send(generation);
        let dropped = dropped_tx.clone();
        Box::pin(async move {
            let _notice = GenerationDropNotice {
                generation,
                notices: dropped,
            };
            if generation == 1 {
                drop(sender);
            } else {
                sender.closed().await;
            }
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0);
    let open = spawn_resolved_open(provider, discovery);
    let first_started = tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(1);
    let first_dropped = tokio::time::timeout(Duration::from_secs(1), dropped_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(1);
    tokio::time::advance(Duration::from_secs(1)).await;
    let second_started = tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(2);
    open.abort();
    let canceled = open.await.is_err_and(|error| error.is_cancelled());
    let second_dropped = tokio::time::timeout(Duration::from_secs(1), dropped_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(2);

    assert!(
        first_started && first_dropped,
        "the first generation must finish empty"
    );
    assert!(
        second_started,
        "the second generation must be owned by the opening call"
    );
    assert!(canceled, "dropping opening must cancel generation two");
    assert!(
        second_dropped,
        "generation two must be dropped after caller cancellation"
    );
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

async fn finish_open_with_bounded_cleanup(
    open: &mut tokio::task::JoinHandle<Result<(), String>>,
    bound: Duration,
) -> Result<(), String> {
    match tokio::time::timeout(bound, &mut *open).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(format!("open task failed: {error}")),
        Err(_) => {
            open.abort();
            match open.await {
                Ok(Ok(())) => Err("open completed after the test bound".into()),
                Ok(Err(error)) => Err(format!("open failed after the test bound: {error}")),
                Err(error) if error.is_cancelled() => {
                    Err("open exceeded the test bound and was canceled".into())
                }
                Err(error) => Err(format!("open exceeded the test bound and failed: {error}")),
            }
        }
    }
}

#[tokio::test]
async fn completed_open_error_is_returned_without_repolling_join_handle() {
    let mut completed_open =
        tokio::spawn(async { Err::<(), _>("synthetic open error".to_owned()) });
    let completed = tokio::time::timeout(Duration::from_secs(1), async {
        while !completed_open.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    if !completed {
        completed_open.abort();
        let _ = completed_open.await;
        panic!("the helper regression requires an already-completed handle");
    }
    let result =
        finish_open_with_bounded_cleanup(&mut completed_open, Duration::from_secs(1)).await;

    assert_eq!(result, Err("synthetic open error".to_owned()));
}

#[tokio::test(start_paused = true)]
async fn elapsed_open_bound_aborts_and_awaits_open_task() {
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (dropped_tx, mut dropped_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut pending_open = tokio::spawn(async move {
        let _notice = GenerationDropNotice {
            generation: 99,
            notices: dropped_tx,
        };
        let _ = started_tx.send(());
        std::future::pending::<Result<(), String>>().await
    });
    started_rx.await.unwrap();

    let result = finish_open_with_bounded_cleanup(&mut pending_open, Duration::from_secs(1)).await;
    let dropped = dropped_rx.recv().await;

    assert_eq!(
        result,
        Err("open exceeded the test bound and was canceled".into())
    );
    assert_eq!(
        dropped,
        Some(99),
        "the timed-out open task must be aborted and joined"
    );
}

#[tokio::test(start_paused = true)]
async fn ongoing_initial_discovery_keeps_late_candidate_without_restarting() {
    let calls = Arc::new(AtomicU32::new(0));
    let observed_calls = calls.clone();
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let release_rx = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));
    let (dropped_tx, mut dropped_rx) = tokio::sync::mpsc::unbounded_channel();
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        let generation = observed_calls.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = started_tx.send(generation);
        let dropped = dropped_tx.clone();
        let release = release_rx.clone();
        Box::pin(async move {
            let _notice = GenerationDropNotice {
                generation,
                notices: dropped,
            };
            let released = match release.lock().await.take() {
                Some(receiver) => receiver.await.is_ok(),
                None => false,
            };
            if generation == 1 && released {
                let _ = sender
                    .send(SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 1))
                    .await;
                sender.closed().await;
            }
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0);
    let mut open = spawn_resolved_open(provider, discovery);
    let first_started = tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(1);
    tokio::time::advance(Duration::from_secs(5)).await;
    yield_discovery_tasks().await;
    let one_generation_while_pending = calls.load(Ordering::Relaxed) == 1 && !open.is_finished();
    let _ = release_tx.send(());
    let open_result = finish_open_with_bounded_cleanup(&mut open, Duration::from_secs(1)).await;
    let first_dropped = tokio::time::timeout(Duration::from_secs(1), dropped_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(1);

    assert!(first_started, "the initial generation must start");
    assert!(
        one_generation_while_pending,
        "an ongoing generation must not be restarted just because it yielded no candidate yet"
    );
    assert!(
        open_result.is_ok(),
        "the original generation's late candidate must open the source: {open_result:?}"
    );
    assert!(
        first_dropped,
        "consumer closure must release the sole generation owner"
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn bootstrap_peer_path_still_bypasses_initial_discovery() {
    let calls = Arc::new(AtomicU32::new(0));
    let observed_calls = calls.clone();
    let discovery: PeerDiscovery = Arc::new(move |_, _| {
        observed_calls.fetch_add(1, Ordering::Relaxed);
        Box::pin(async {})
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_bootstrap_peers(vec![SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 1)]);
    let open_result = tokio::time::timeout(
        Duration::from_secs(1),
        provider.open_resolved("synthetic", info(), discovery),
    )
    .await;
    match open_result {
        Ok(Ok(source)) => drop(source),
        Ok(Err(error)) => panic!("bootstrap open failed: {error}"),
        Err(_) => panic!("bootstrap open exceeded its test bound"),
    }
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn warm_peer_hint_still_opens_without_waiting_for_discovery_output() {
    let directory = std::env::temp_dir().join(format!(
        "outpace-empty-discovery-warm-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&directory)
            .unwrap();
    }
    #[cfg(not(unix))]
    std::fs::create_dir(&directory).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let cache_file = directory.join("recent-peers-v1");
    std::fs::write(
        &cache_file,
        format!(
            "OUTPACE-WARM-1\n{} {now} S 127.0.0.1 1\n",
            hex::encode([0u8; 20])
        ),
    )
    .unwrap();
    use std::os::unix::fs::MetadataExt;
    let initial_inode = std::fs::metadata(&cache_file).unwrap().ino();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&cache_file, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    let calls = Arc::new(AtomicU32::new(0));
    let observed_calls = calls.clone();
    let (dropped_tx, mut dropped_rx) = tokio::sync::mpsc::unbounded_channel();
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        observed_calls.fetch_add(1, Ordering::Relaxed);
        let dropped = dropped_tx.clone();
        Box::pin(async move {
            let _notice = GenerationDropNotice {
                generation: 1,
                notices: dropped,
            };
            sender.closed().await;
        })
    });
    let mut provider = AceProvider::new(Arc::new(Identity::generate()), 0);
    provider.warm_peers = WarmPeerCache::new(&directory, true);
    let opened = tokio::time::timeout(
        Duration::from_secs(2),
        provider.open_resolved("synthetic", info(), discovery),
    )
    .await;
    drop(provider);
    let warm_hint_opened = match opened {
        Ok(Ok(source)) => {
            drop(source);
            true
        }
        Ok(Err(_)) | Err(_) => false,
    };
    let owner_released = tokio::time::timeout(Duration::from_secs(1), dropped_rx.recv())
        .await
        .ok()
        .flatten()
        == Some(1);
    yield_discovery_tasks().await;
    let discovery_calls = calls.load(Ordering::Relaxed);
    let cache_worker_flushed = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if std::fs::metadata(&cache_file).is_ok_and(|metadata| metadata.ino() != initial_inode)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok();
    if cache_worker_flushed {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let cleanup = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match std::fs::remove_dir_all(&directory) {
                Ok(()) => break true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break true,
                Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => {
                    tokio::task::yield_now().await
                }
                Err(_) => break false,
            }
        }
    })
    .await
    .unwrap_or(false);

    assert!(
        owner_released,
        "source drop must release the discovery owner"
    );
    assert!(
        cache_worker_flushed,
        "warm cache owner must finish its final bounded write"
    );
    assert!(cleanup, "warm cache test directory must be removed");
    assert!(
        warm_hint_opened,
        "warm peer hint must open without waiting for discovery output"
    );
    assert_eq!(discovery_calls, 1);
}

// Reconstruct the entire provider: process-local candidate memory cannot satisfy this.
struct DiscoveryDropProbe(Arc<AtomicU32>);
impl Drop for DiscoveryDropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}
#[derive(Clone, Copy)]
enum CancellationStage {
    EmptyFirst,
    SilentConnect,
    FullChannel,
}
async fn discovery_cancellation_control(stage: CancellationStage) {
    let dropped = Arc::new(AtomicU32::new(0));
    let calls = Arc::new(AtomicU32::new(0));
    let full = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let (connected_tx, connected_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::io::AsyncReadExt::read(&mut stream, &mut byte)
                .await
                .unwrap(),
            1
        );
        let _ = connected_tx.send(());
        while tokio::io::AsyncReadExt::read(&mut stream, &mut byte)
            .await
            .unwrap()
            > 0
        {}
    });
    let probe = dropped.clone();
    let called = calls.clone();
    let observed_full = full.clone();
    let factory: PeerDiscovery = Arc::new(move |_, sender| {
        let probe = probe.clone();
        let observed_full = observed_full.clone();
        let called = called.clone();
        Box::pin(async move {
            let _owned = DiscoveryDropProbe(probe);
            called.fetch_add(1, Ordering::Relaxed);
            match stage {
                CancellationStage::EmptyFirst => {}
                CancellationStage::SilentConnect => {
                    sender.send(addr).await.unwrap();
                }
                CancellationStage::FullChannel => {
                    for port in 0..MAX_DISCOVERY_PEERS as u16 {
                        sender
                            .try_send(SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, port))
                            .unwrap();
                    }
                    let extra = SocketAddrV4::new(
                        std::net::Ipv4Addr::LOCALHOST,
                        MAX_DISCOVERY_PEERS as u16,
                    );
                    assert!(matches!(
                        sender.try_send(extra),
                        Err(mpsc::error::TrySendError::Full(_))
                    ));
                    observed_full.store(true, Ordering::Relaxed);
                    if sender.send(extra).await.is_err() {
                        return;
                    }
                }
            }
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0);
    let open =
        tokio::spawn(async move { provider.open_resolved("synthetic", info(), factory).await });
    match stage {
        CancellationStage::SilentConnect => {
            tokio::time::timeout(Duration::from_secs(1), connected_rx)
                .await
                .unwrap()
                .unwrap();
        }
        CancellationStage::FullChannel => {
            tokio::time::timeout(Duration::from_secs(1), async {
                while !full.load(Ordering::Relaxed) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        CancellationStage::EmptyFirst => {
            tokio::time::timeout(Duration::from_secs(1), async {
                while calls.load(Ordering::Relaxed) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
    }
    open.abort();
    if let Ok(Ok(source)) = open.await {
        drop(source);
    }
    let canceled = tokio::time::timeout(Duration::from_secs(1), async {
        while dropped.load(Ordering::Relaxed) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    if matches!(stage, CancellationStage::SilentConnect) {
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    } else {
        server.abort();
        let _ = server.await;
    }
    assert!(
        canceled.is_ok(),
        "owned discovery future remained alive after consumer cancellation"
    );
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "observation timeout restarted discovery"
    );
}
#[tokio::test]
async fn cancellation_during_empty_first_batch_collects_owned_discovery() {
    discovery_cancellation_control(CancellationStage::EmptyFirst).await;
}
#[tokio::test]
async fn cancellation_during_silent_first_connect_closes_socket_and_discovery() {
    discovery_cancellation_control(CancellationStage::SilentConnect).await;
}
#[tokio::test]
async fn cancellation_with_actual_full_discovery_channel_collects_owner() {
    discovery_cancellation_control(CancellationStage::FullChannel).await;
}

#[tokio::test]
async fn late_candidate_at_full_active_cap_survives_for_pool_rebuild() {
    let mut servers = tokio::task::JoinSet::new();
    let mut addrs = Vec::new();
    let later_attempts = Arc::new(AtomicU32::new(0));
    let close_first = Arc::new(tokio::sync::Notify::new());
    for index in 0..2 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        addrs.push(addr);
        let later_attempts = later_attempts.clone();
        let close = close_first.clone();
        servers.spawn(async move {
            let mut clients=tokio::task::JoinSet::new();
            loop {
                let (stream,_)=listener.accept().await.unwrap();if index==1 {later_attempts.fetch_add(1,Ordering::Relaxed);}
                let close=close.clone();clients.spawn(async move {
                    let mut session=PeerSession::new(stream);if session.accept_handshake([0;20],|_|true).await.is_err(){return;}
                    if session.send_extended_handshake(&OutgoingExtendedHandshake {
                        mi:Some(LivePosition {min_piece:7,max_piece:if index==0 {10}else {12},position:-1,distance_from_source:1}),
                        ace_metadata_version:1,ut_metadata_id:2,node:NodeFields::default(),peer_ip:None,metadata_size:None,
                    }).await.is_err(){return;}
                    let _=session.send(&PeerMessage::Unchoke).await;
                    loop {
                        let message=tokio::select! {_=close.notified(),if index==0=>return,message=session.read_message()=>message};
                        let Ok(message)=message else {return;};
                        if let PeerMessage::Unknown {id:6,payload}=message {
                            let piece=u32::from_be_bytes(payload[4..8].try_into().unwrap());
                            if session.send(&build_piece(0,piece,0,[0;8],&media_piece(if index==0 {0x55}else {0x66}))).await.is_err(){return;}
                        }
                    }
                });
            }
        });
    }
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let release = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));
    let calls = Arc::new(AtomicU32::new(0));
    let called = calls.clone();
    let generation_addrs = addrs.clone();
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        called.fetch_add(1, Ordering::Relaxed);
        let release = release.clone();
        let addrs = generation_addrs.clone();
        Box::pin(async move {
            sender.send(addrs[0]).await.unwrap();
            release.lock().await.take().unwrap().await.unwrap();
            sender.send(addrs[1]).await.unwrap();
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_prefetch_pieces(Some(1))
        .with_live_recovery(LiveRecoveryConfig {
            max_active_upstreams: 1,
            max_parallel_connect: 1,
            ..LiveRecoveryConfig::default()
        })
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let mut source = provider
        .open_resolved("synthetic", info(), discovery)
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), source.next())
        .await
        .unwrap()
        .unwrap();
    assert!(first.contains(&0x55));
    release_tx.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        later_attempts.load(Ordering::Relaxed),
        0,
        "full current pool must retain rather than dial an unnecessary candidate"
    );
    close_first.notify_waiters();
    let replacement = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(bytes) = source.next().await {
                if bytes.contains(&0x66) {
                    return;
                }
            } else {
                panic!("replacement source ended");
            }
        }
    })
    .await;
    drop(source);
    servers.abort_all();
    while servers.join_next().await.is_some() {}
    assert!(
        replacement.is_ok(),
        "retained late candidate was lost during pool rebuild"
    );
    assert!(later_attempts.load(Ordering::Relaxed) > 0);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "rebuild discarded the existing discovery generation"
    );
}

#[derive(Clone, Copy)]
enum InitialControlOrder {
    NoControl,
    Unchoke,
    UnchokeThenChoke,
    ChokeThenUnchoke,
    WindowThenUnchoke,
    LaterUnchoke,
}

#[derive(Debug, PartialEq, Eq)]
enum RequestObservation {
    Request(Vec<u8>),
    NoRequestWithinBound,
    PeerExited,
}

async fn observe_request(
    session: &mut PeerSession<TcpStream>,
    bound: Duration,
) -> RequestObservation {
    match tokio::time::timeout(bound, async {
        loop {
            match session.read_message().await {
                Ok(PeerMessage::Unknown { id: 6, payload }) => {
                    return RequestObservation::Request(payload);
                }
                Ok(_) => {}
                Err(_) => return RequestObservation::PeerExited,
            }
        }
    })
    .await
    {
        Ok(observation) => observation,
        Err(_) => RequestObservation::NoRequestWithinBound,
    }
}

fn assert_live_piece_request(observation: RequestObservation, context: &str) {
    assert_live_piece_request_in_range(observation, 9..=9, context);
}

fn assert_live_piece_request_in_range(
    observation: RequestObservation,
    expected_pieces: std::ops::RangeInclusive<u32>,
    context: &str,
) {
    let RequestObservation::Request(payload) = observation else {
        panic!("{context}: expected an id-6 request, observed {observation:?}");
    };
    assert_eq!(payload.len(), 10, "{context}: request payload length");
    assert_eq!(&payload[..4], &[0; 4], "{context}: stream identifier");
    let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
    assert!(
        expected_pieces.contains(&piece),
        "{context}: requested piece {piece} outside {expected_pieces:?}"
    );
    assert_eq!(
        u16::from_be_bytes(payload[8..10].try_into().unwrap()),
        0,
        "{context}: requested chunk"
    );
}

async fn unchoke_order_control(order: InitialControlOrder) -> RequestObservation {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let (activated_tx, activated_rx) = tokio::sync::oneshot::channel();
    let (request_tx, request_rx) = tokio::sync::oneshot::channel();
    let mut server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut session = PeerSession::new(stream);
        session.accept_handshake([0; 20], |_| true).await.unwrap();

        match order {
            InitialControlOrder::Unchoke => {
                session.send(&PeerMessage::Unchoke).await.unwrap();
            }
            InitialControlOrder::UnchokeThenChoke => {
                session.send(&PeerMessage::Unchoke).await.unwrap();
                session.send(&PeerMessage::Choke).await.unwrap();
            }
            InitialControlOrder::ChokeThenUnchoke => {
                session.send(&PeerMessage::Choke).await.unwrap();
                session.send(&PeerMessage::Unchoke).await.unwrap();
            }
            InitialControlOrder::NoControl
            | InitialControlOrder::WindowThenUnchoke
            | InitialControlOrder::LaterUnchoke => {}
        }
        session
            .send_extended_handshake(&OutgoingExtendedHandshake {
                ace_metadata_version: 1,
                ut_metadata_id: 2,
                mi: Some(LivePosition {
                    min_piece: 7,
                    max_piece: 10,
                    position: -1,
                    distance_from_source: 1,
                }),
                node: NodeFields::default(),
                peer_ip: None,
                metadata_size: None,
            })
            .await
            .unwrap();
        if matches!(order, InitialControlOrder::WindowThenUnchoke) {
            session.send(&PeerMessage::Unchoke).await.unwrap();
        }

        assert!(matches!(
            session.read_message().await.unwrap(),
            PeerMessage::Extended { ext_id: 0, .. }
        ));
        assert_eq!(
            session.read_message().await.unwrap(),
            PeerMessage::Interested
        );
        activated_tx.send(()).unwrap();
        if matches!(order, InitialControlOrder::LaterUnchoke) {
            session.send(&PeerMessage::Unchoke).await.unwrap();
        }

        let observation = observe_request(&mut session, Duration::from_secs(4)).await;
        let _ = request_tx.send(observation);
    });

    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        Box::pin(async move {
            sender.send(addr).await.unwrap();
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_prefetch_pieces(Some(1))
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let source = match provider.open_resolved("synthetic", info(), discovery).await {
        Ok(source) => source,
        Err(error) => {
            server.abort();
            let _ = server.await;
            panic!("synthetic resolved open failed: {error}");
        }
    };

    let activated = tokio::time::timeout(Duration::from_secs(2), activated_rx).await;
    let request = if matches!(&activated, Ok(Ok(()))) {
        match tokio::time::timeout(Duration::from_secs(5), request_rx).await {
            Ok(Ok(observation)) => observation,
            Ok(Err(_)) => RequestObservation::PeerExited,
            Err(_) => RequestObservation::PeerExited,
        }
    } else {
        RequestObservation::PeerExited
    };

    drop(source);
    let server_done = tokio::time::timeout(Duration::from_secs(1), &mut server).await;
    if server_done.is_err() {
        server.abort();
        let _ = server.await;
    }
    assert!(
        matches!(&activated, Ok(Ok(()))),
        "peer activation must reach the signed-extension and Interested barrier"
    );
    server_done
        .expect("fake peer must stop after source cleanup")
        .expect("fake peer task must not panic");
    request
}

#[tokio::test]
async fn unchoke_ordering_window_then_unchoke_requests_a_live_piece() {
    assert_live_piece_request(
        unchoke_order_control(InitialControlOrder::WindowThenUnchoke).await,
        "window-first Unchoke must reach the real chunk-request path",
    );
}

#[tokio::test]
async fn unchoke_ordering_early_unchoke_is_retained_for_live_piece_requests() {
    assert_live_piece_request(
        unchoke_order_control(InitialControlOrder::Unchoke).await,
        "window-reader must preserve an earlier Unchoke through peer activation",
    );
}

#[tokio::test]
async fn unchoke_ordering_default_peer_does_not_request_without_control() {
    assert_eq!(
        unchoke_order_control(InitialControlOrder::NoControl).await,
        RequestObservation::NoRequestWithinBound,
        "no pre-window control must leave the peer choked and observable"
    );
}

#[tokio::test]
async fn unchoke_ordering_pre_window_unchoke_then_choke_remains_choked() {
    assert_eq!(
        unchoke_order_control(InitialControlOrder::UnchokeThenChoke).await,
        RequestObservation::NoRequestWithinBound,
        "the last pre-window Choke must override Unchoke while observable"
    );
}

#[tokio::test]
async fn unchoke_ordering_pre_window_choke_then_unchoke_requests_a_live_piece() {
    assert_live_piece_request(
        unchoke_order_control(InitialControlOrder::ChokeThenUnchoke).await,
        "the last pre-window Unchoke must enable the real chunk-request path",
    );
}

#[tokio::test]
async fn unchoke_ordering_later_control_uses_the_active_worker_path() {
    assert_live_piece_request(
        unchoke_order_control(InitialControlOrder::LaterUnchoke).await,
        "a post-activation Unchoke must reach the existing worker/event path",
    );
}

#[tokio::test]
async fn unchoke_ordering_initial_two_early_peers_schedule_without_later_control() {
    let mut servers = tokio::task::JoinSet::new();
    let (ready_tx, ready_rx) = tokio::sync::mpsc::channel(2);
    let mut ready_rx = ready_rx;
    let (activated_tx, activated_rx) = tokio::sync::mpsc::channel(2);
    let mut activated_rx = activated_rx;
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let mut addrs = Vec::new();
    let mut request_receivers = Vec::new();

    for _ in 0..2 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        addrs.push(addr);
        let ready_tx = ready_tx.clone();
        let activated_tx = activated_tx.clone();
        let gate = gate.clone();
        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        request_receivers.push(request_rx);
        servers.spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut session = PeerSession::new(stream);
            session.accept_handshake([0; 20], |_| true).await.unwrap();
            ready_tx.send(()).await.unwrap();
            gate.wait().await;

            // Both connected candidates receive exactly one early Unchoke and the same
            // window together; neither sends any later control or advancing window.
            session.send(&PeerMessage::Unchoke).await.unwrap();
            session
                .send_extended_handshake(&OutgoingExtendedHandshake {
                    ace_metadata_version: 1,
                    ut_metadata_id: 2,
                    mi: Some(LivePosition {
                        min_piece: 7,
                        max_piece: 10,
                        position: -1,
                        distance_from_source: 1,
                    }),
                    node: NodeFields::default(),
                    peer_ip: None,
                    metadata_size: None,
                })
                .await
                .unwrap();
            assert!(matches!(
                session.read_message().await.unwrap(),
                PeerMessage::Extended { ext_id: 0, .. }
            ));
            assert_eq!(
                session.read_message().await.unwrap(),
                PeerMessage::Interested
            );
            activated_tx.send(()).await.unwrap();

            let observation = observe_request(&mut session, Duration::from_secs(4)).await;
            let _ = request_tx.send(observation);
        });
    }
    drop(ready_tx);
    drop(activated_tx);

    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        let addrs = addrs.clone();
        Box::pin(async move {
            for addr in addrs {
                sender.send(addr).await.unwrap();
            }
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_prefetch_pieces(Some(1))
        .with_live_recovery(LiveRecoveryConfig {
            max_active_upstreams: 2,
            max_parallel_connect: 2,
            ..LiveRecoveryConfig::default()
        })
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let source = match provider.open_resolved("synthetic", info(), discovery).await {
        Ok(source) => source,
        Err(error) => {
            servers.abort_all();
            while servers.join_next().await.is_some() {}
            panic!("synthetic resolved open failed: {error}");
        }
    };

    let both_connected = tokio::time::timeout(Duration::from_secs(2), async {
        ready_rx.recv().await?;
        ready_rx.recv().await
    })
    .await;
    // Each post-activation barrier is emitted only after the real initial upstream
    // selection has admitted that connection.
    let both_activated = tokio::time::timeout(Duration::from_secs(2), async {
        activated_rx.recv().await?;
        activated_rx.recv().await
    })
    .await;
    let mut request_receivers = request_receivers.into_iter();
    let mut first_request_rx = request_receivers.next().unwrap();
    let mut second_request_rx = request_receivers.next().unwrap();
    let (first_observation, second_observation) = if matches!(&both_activated, Ok(Some(()))) {
        let first = async {
            match tokio::time::timeout(Duration::from_secs(5), &mut first_request_rx).await {
                Ok(Ok(observation)) => observation,
                Ok(Err(_)) | Err(_) => RequestObservation::PeerExited,
            }
        };
        let second = async {
            match tokio::time::timeout(Duration::from_secs(5), &mut second_request_rx).await {
                Ok(Ok(observation)) => observation,
                Ok(Err(_)) | Err(_) => RequestObservation::PeerExited,
            }
        };
        tokio::join!(first, second)
    } else {
        (
            RequestObservation::PeerExited,
            RequestObservation::PeerExited,
        )
    };

    drop(source);
    let servers_done = tokio::time::timeout(Duration::from_secs(1), async {
        let mut all_succeeded = true;
        while let Some(result) = servers.join_next().await {
            all_succeeded &= result.is_ok();
        }
        all_succeeded
    })
    .await;
    if servers_done.is_err() {
        servers.abort_all();
        while servers.join_next().await.is_some() {}
    }

    assert!(
        matches!(both_connected, Ok(Some(()))),
        "both loopback peers must connect before either sends its early control"
    );
    assert!(
        matches!(both_activated, Ok(Some(()))),
        "both peers must be admitted by the initial selected pool"
    );
    assert!(
        matches!(servers_done, Ok(true)),
        "fake peers must stop cleanly after their bounded observation and source cleanup"
    );
    let mut request_count = 0;
    for observation in [first_observation, second_observation] {
        match observation {
            RequestObservation::Request(payload) => {
                assert_live_piece_request_in_range(
                    RequestObservation::Request(payload),
                    9..=10,
                    "initial multi-peer retained Unchoke must request an in-window live piece",
                );
                request_count += 1;
            }
            RequestObservation::NoRequestWithinBound => {}
            RequestObservation::PeerExited => {
                panic!("peer exited before completing its bounded request observation")
            }
        }
    }
    assert!(
        request_count > 0,
        "two initially admitted early-Unchoke peers must schedule a request without a later control"
    );
}

#[tokio::test]
async fn unchoke_ordering_active_peer_does_not_change_another_peers_default() {
    let mut servers = tokio::task::JoinSet::new();
    let (activated_tx, mut activated_rx) = tokio::sync::mpsc::channel(2);
    let mut addrs = Vec::new();
    let mut request_receivers = Vec::new();

    for unchoke_after_activation in [true, false] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        addrs.push(addr);
        let activated_tx = activated_tx.clone();
        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        request_receivers.push(request_rx);
        servers.spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut session = PeerSession::new(stream);
            session.accept_handshake([0; 20], |_| true).await.unwrap();
            session
                .send_extended_handshake(&OutgoingExtendedHandshake {
                    ace_metadata_version: 1,
                    ut_metadata_id: 2,
                    mi: Some(LivePosition {
                        min_piece: 7,
                        max_piece: 10,
                        position: -1,
                        distance_from_source: 1,
                    }),
                    node: NodeFields::default(),
                    peer_ip: None,
                    metadata_size: None,
                })
                .await
                .unwrap();
            assert!(matches!(
                session.read_message().await.unwrap(),
                PeerMessage::Extended { ext_id: 0, .. }
            ));
            assert_eq!(
                session.read_message().await.unwrap(),
                PeerMessage::Interested
            );
            activated_tx.send(unchoke_after_activation).await.unwrap();
            if unchoke_after_activation {
                session.send(&PeerMessage::Unchoke).await.unwrap();
            }
            let observation = observe_request(&mut session, Duration::from_secs(4)).await;
            let _ = request_tx.send(observation);
        });
    }
    drop(activated_tx);

    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        let addrs = addrs.clone();
        Box::pin(async move {
            for addr in addrs {
                sender.send(addr).await.unwrap();
            }
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_prefetch_pieces(Some(1))
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let source = match provider.open_resolved("synthetic", info(), discovery).await {
        Ok(source) => source,
        Err(error) => {
            servers.abort_all();
            while servers.join_next().await.is_some() {}
            panic!("synthetic resolved open failed: {error}");
        }
    };
    let activated = tokio::time::timeout(Duration::from_secs(2), async {
        let mut roles = Vec::new();
        for _ in 0..2 {
            roles.push(activated_rx.recv().await?);
        }
        Some(roles)
    })
    .await;
    let mut request_receivers = request_receivers.into_iter();
    let mut unchoked_request_rx = request_receivers.next().unwrap();
    let mut choked_request_rx = request_receivers.next().unwrap();
    let (unchoked_request, choked_request) = if matches!(&activated, Ok(Some(_))) {
        let unchoked = async {
            match tokio::time::timeout(Duration::from_secs(5), &mut unchoked_request_rx).await {
                Ok(Ok(observation)) => observation,
                Ok(Err(_)) | Err(_) => RequestObservation::PeerExited,
            }
        };
        let choked = async {
            match tokio::time::timeout(Duration::from_secs(5), &mut choked_request_rx).await {
                Ok(Ok(observation)) => observation,
                Ok(Err(_)) | Err(_) => RequestObservation::PeerExited,
            }
        };
        tokio::join!(unchoked, choked)
    } else {
        (
            RequestObservation::PeerExited,
            RequestObservation::PeerExited,
        )
    };

    drop(source);
    let servers_done = tokio::time::timeout(Duration::from_secs(1), async {
        let mut all_succeeded = true;
        while let Some(result) = servers.join_next().await {
            all_succeeded &= result.is_ok();
        }
        all_succeeded
    })
    .await;
    if servers_done.is_err() {
        servers.abort_all();
        while servers.join_next().await.is_some() {}
    }
    assert!(
        matches!(activated, Ok(Some(_))),
        "both peer activations must reach their barriers"
    );
    assert!(
        matches!(servers_done, Ok(true)),
        "fake peers must stop cleanly after source cleanup"
    );
    assert_live_piece_request(
        unchoked_request,
        "the unchoked peer must receive a valid request",
    );
    assert!(
        choked_request == RequestObservation::NoRequestWithinBound,
        "the default-choked peer must remain observably request-free; got {choked_request:?}"
    );
}

#[tokio::test]
async fn unchoke_ordering_early_incoming_does_not_suppress_reciprocal_seed_unchoke() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let (activated_tx, activated_rx) = tokio::sync::oneshot::channel();
    let (reciprocal_tx, reciprocal_rx) = tokio::sync::oneshot::channel();
    let mut server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut session = PeerSession::new(stream).with_timeout(Duration::from_secs(3));
        session.accept_handshake([0; 20], |_| true).await.unwrap();
        session.send(&PeerMessage::Unchoke).await.unwrap();
        session
            .send_extended_handshake(&OutgoingExtendedHandshake {
                ace_metadata_version: 1,
                ut_metadata_id: 2,
                mi: Some(LivePosition {
                    min_piece: 7,
                    max_piece: 10,
                    position: -1,
                    distance_from_source: 1,
                }),
                node: NodeFields::default(),
                peer_ip: None,
                metadata_size: None,
            })
            .await
            .unwrap();
        assert!(matches!(
            session.read_message().await.unwrap(),
            PeerMessage::Extended { ext_id: 0, .. }
        ));
        assert_eq!(
            session.read_message().await.unwrap(),
            PeerMessage::Interested
        );
        session.send(&PeerMessage::Interested).await.unwrap();
        activated_tx.send(()).unwrap();
        let reciprocal = tokio::time::timeout(Duration::from_secs(2), session.read_message())
            .await
            .ok()
            .and_then(Result::ok);
        let _ = reciprocal_tx.send(matches!(reciprocal, Some(PeerMessage::Unchoke)));
    });

    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        Box::pin(async move {
            sender.send(addr).await.unwrap();
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_seeding_enabled(true)
        .with_prefetch_pieces(Some(1))
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let source = match provider.open_resolved("synthetic", info(), discovery).await {
        Ok(source) => source,
        Err(error) => {
            server.abort();
            let _ = server.await;
            panic!("synthetic resolved open failed: {error}");
        }
    };

    let activated = tokio::time::timeout(Duration::from_secs(2), activated_rx).await;
    let reciprocal = if matches!(&activated, Ok(Ok(()))) {
        tokio::time::timeout(Duration::from_secs(3), reciprocal_rx)
            .await
            .ok()
            .and_then(Result::ok)
    } else {
        None
    };

    drop(source);
    let server_done = tokio::time::timeout(Duration::from_secs(1), &mut server).await;
    if server_done.is_err() {
        server.abort();
        let _ = server.await;
    }
    assert!(
        matches!(&activated, Ok(Ok(()))),
        "fake peer must observe our signed extension and Interested"
    );
    server_done
        .expect("fake peer must stop after source cleanup")
        .expect("fake peer task must not panic");
    assert_eq!(
        reciprocal,
        Some(true),
        "retaining incoming Unchoke must not imply we already sent reciprocal Unchoke"
    );
}

async fn surviving_peer_capacity_control(active_cap: usize) {
    let mut servers = tokio::task::JoinSet::new();
    let mut addrs = Vec::new();
    let later_attempts = Arc::new(AtomicU32::new(0));
    let close_first = Arc::new(tokio::sync::Notify::new());
    for index in 0..3 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        addrs.push(addr);
        let later_attempts = later_attempts.clone();
        let close = close_first.clone();
        servers.spawn(async move {
            let mut clients = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                if index == 2 {
                    later_attempts.fetch_add(1, Ordering::Relaxed);
                }
                let close = close.clone();
                clients.spawn(async move {
                    let mut session = PeerSession::new(stream);
                    if session.accept_handshake([0; 20], |_| true).await.is_err() {
                        return;
                    }
                    if session
                        .send_extended_handshake(&OutgoingExtendedHandshake {
                            mi: Some(LivePosition {
                                min_piece: 7,
                                max_piece: if index == 2 { 12 } else { 10 },
                                position: -1,
                                distance_from_source: 1,
                            }),
                            ace_metadata_version: 1,
                            ut_metadata_id: 2,
                            node: NodeFields::default(),
                            peer_ip: None,
                            metadata_size: None,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                    let _ = session.send(&PeerMessage::Unchoke).await;
                    loop {
                        let message = tokio::select! {
                            _ = close.notified(), if index == 0 => return,
                            message = session.read_message() => message,
                        };
                        let Ok(message) = message else {
                            return;
                        };
                        if let PeerMessage::Unknown { id: 6, payload } = message {
                            let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                            if session
                                .send(&build_piece(
                                    0,
                                    piece,
                                    0,
                                    [0; 8],
                                    &media_piece(if index == 2 { 0x66 } else { 0x55 }),
                                ))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                });
            }
        });
    }
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let release = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));
    let calls = Arc::new(AtomicU32::new(0));
    let called = calls.clone();
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        called.fetch_add(1, Ordering::Relaxed);
        let release = release.clone();
        let addrs = addrs.clone();
        Box::pin(async move {
            // Both initial peers enter the same actual connect_pool selection batch.
            sender.try_send(addrs[0]).unwrap();
            sender.try_send(addrs[1]).unwrap();
            release.lock().await.take().unwrap().await.unwrap();
            sender.send(addrs[2]).await.unwrap();
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_prefetch_pieces(Some(1))
        .with_live_recovery(LiveRecoveryConfig {
            max_active_upstreams: active_cap,
            max_parallel_connect: 2,
            ..LiveRecoveryConfig::default()
        })
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let mut source = provider
        .open_resolved("synthetic", info(), discovery)
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), source.next())
        .await
        .unwrap()
        .unwrap();
    assert!(first.contains(&0x55));
    // Publish both pieces through the initial head before raising it. Otherwise the
    // strict one-piece floor correctly skips the outstanding old piece and arms the
    // decoder recovery gate, which this scheduling-only fixture does not model.
    let mut initial_bytes = first.len();
    tokio::time::timeout(Duration::from_secs(2), async {
        // TsResync retains the final packet until another sync byte confirms it.
        while initial_bytes < 7 * 188 {
            let bytes = source.next().await.expect("initial producer ended");
            assert!(bytes.contains(&0x55));
            initial_bytes += bytes.len();
        }
    })
    .await
    .unwrap();
    release_tx.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    if active_cap == 2 {
        assert_eq!(
            later_attempts.load(Ordering::Relaxed),
            0,
            "full active pool dialed a late candidate"
        );
        close_first.notify_waiters();
    }
    let replacement = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(bytes) = source.next().await {
                if bytes.contains(&0x66) {
                    return;
                }
            } else {
                panic!("surviving pool ended");
            }
        }
    })
    .await;
    drop(source);
    servers.abort_all();
    while servers.join_next().await.is_some() {}
    assert!(
        replacement.is_ok(),
        "freed active slot stranded a retained peer while another upstream survived"
    );
    assert!(later_attempts.load(Ordering::Relaxed) > 0);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn retained_candidate_enters_after_peer_loss_with_surviving_upstream() {
    surviving_peer_capacity_control(2).await;
}

#[tokio::test]
async fn surviving_peer_late_candidate_positive_control() {
    surviving_peer_capacity_control(3).await;
}

async fn prepared_peer_after_output_control(newer_window: bool) {
    let mut servers = tokio::task::JoinSet::new();
    let mut addrs = Vec::new();
    for delayed in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        addrs.push(addr);
        servers.spawn(async move {
            let mut clients = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                clients.spawn(async move {
                    // Complete after the real one-second lone-window gate, so the
                    // initial producer publishes before this pending transport is ready.
                    if delayed {
                        tokio::time::sleep(Duration::from_millis(1500)).await;
                    }
                    let mut session = PeerSession::new(stream);
                    if session.accept_handshake([0; 20], |_| true).await.is_err() {
                        return;
                    }
                    if session
                        .send_extended_handshake(&OutgoingExtendedHandshake {
                            mi: Some(LivePosition {
                                min_piece: 7,
                                max_piece: if delayed && newer_window { 12 } else { 10 },
                                position: -1,
                                distance_from_source: 1,
                            }),
                            ace_metadata_version: 1,
                            ut_metadata_id: 2,
                            node: NodeFields::default(),
                            peer_ip: None,
                            metadata_size: None,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                    let _ = session.send(&PeerMessage::Unchoke).await;
                    while let Ok(msg) = session.read_message().await {
                        if let PeerMessage::Unknown { id: 6, payload } = msg {
                            let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                            if !delayed && piece != 9 {
                                continue;
                            }
                            let mut media = include_bytes!(
                                "../../../../tests/vectors/media/transport-resync.ts"
                            )[..752]
                                .to_vec();
                            for packet in media
                                .chunks_mut(188)
                                .filter(|p| ace_media::mpegts::ts_pid(p) == 0x101)
                            {
                                packet[6..].fill(if delayed { 0x66 } else { 0x55 });
                            }
                            if session
                                .send(&build_piece(0, piece, 0, [0; 8], &media))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                });
            }
        });
    }
    let calls = Arc::new(AtomicU32::new(0));
    let called = calls.clone();
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        called.fetch_add(1, Ordering::Relaxed);
        let addrs = addrs.clone();
        Box::pin(async move {
            for addr in addrs {
                sender.try_send(addr).unwrap();
            }
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_prefetch_pieces(Some(1))
        .with_live_recovery(LiveRecoveryConfig {
            max_active_upstreams: 2,
            max_parallel_connect: 1,
            ..LiveRecoveryConfig::default()
        })
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let mut source = provider
        .open_resolved("synthetic", info(), discovery)
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), source.next())
        .await
        .unwrap()
        .unwrap();
    assert!(
        first.contains(&0x55),
        "initial peer must publish before the prepared transport completes"
    );
    let later = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(bytes) = source.next().await {
                if bytes.contains(&0x66) {
                    return;
                }
            } else {
                panic!("prepared producer lost");
            }
        }
    })
    .await;
    drop(source);
    servers.abort_all();
    while servers.join_next().await.is_some() {}
    assert!(
        later.is_ok(),
        "productive history reset discarded a useful prepared producer"
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn prepared_within_floor_peer_survives_productive_history_reset() {
    prepared_peer_after_output_control(false).await;
}

#[tokio::test]
async fn prepared_newer_window_peer_survives_productive_history_reset() {
    prepared_peer_after_output_control(true).await;
}

// Exercise the production resolved-open path before any upstream has a live window.
async fn initial_silent_candidate_control(parallel: usize, cancel: bool) {
    struct DiscoveryDropped(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for DiscoveryDropped {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let healthy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = |listener: &tokio::net::TcpListener| match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(addr) => addr,
        _ => unreachable!(),
    };
    let silent_addr = addr(&silent);
    let healthy_addr = addr(&healthy);
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
    let (healthy_tx, mut healthy_rx) = tokio::sync::oneshot::channel();
    let mut servers = tokio::task::JoinSet::new();
    servers.spawn(async move {
        let (mut stream, _) = silent.accept().await.unwrap();
        let _ = accepted_tx.send(());
        tokio::select! {
            _ = release_rx => {},
            _ = async {
                let mut bytes = [0; 1024];
                while tokio::io::AsyncReadExt::read(&mut stream, &mut bytes).await.unwrap() != 0 {}
                let _ = closed_tx.send(());
            } => {},
        }
    });
    servers.spawn(async move {
        let (stream, _) = healthy.accept().await.unwrap();
        let _ = healthy_tx.send(());
        let mut session = PeerSession::new(stream);
        session.accept_handshake([0; 20], |_| true).await.unwrap();
        session
            .send_extended_handshake(&OutgoingExtendedHandshake {
                mi: Some(LivePosition {
                    min_piece: 7,
                    max_piece: 10,
                    position: -1,
                    distance_from_source: 0,
                }),
                ace_metadata_version: 1,
                ut_metadata_id: 2,
                node: NodeFields::default(),
                peer_ip: None,
                metadata_size: None,
            })
            .await
            .unwrap();
        session.send(&PeerMessage::Unchoke).await.unwrap();
        while let Ok(message) = session.read_message().await {
            if let PeerMessage::Unknown { id: 6, payload } = message {
                let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                if session
                    .send(&build_piece(0, piece, 0, [0; 8], &media_piece(0x66)))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    });
    let (published_tx, published_rx) = tokio::sync::oneshot::channel();
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let state = Arc::new(std::sync::Mutex::new(Some((
        accepted_rx,
        published_tx,
        dropped_tx,
    ))));
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        let (accepted, published, dropped) = state.lock().unwrap().take().unwrap();
        Box::pin(async move {
            let _dropped = DiscoveryDropped(Some(dropped));
            sender.send(silent_addr).await.unwrap();
            accepted.await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            sender.send(healthy_addr).await.unwrap();
            sender.send(healthy_addr).await.unwrap(); // Repeated batches must not duplicate attempts.
            let _ = published.send(Instant::now());
            sender.closed().await;
        })
    });
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_prefetch_pieces(Some(1))
        .with_live_recovery(LiveRecoveryConfig {
            max_parallel_connect: parallel,
            ..LiveRecoveryConfig::default()
        })
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let mut source = provider
        .open_resolved("synthetic", info(), discovery)
        .await
        .unwrap();
    let published = tokio::time::timeout(Duration::from_secs(2), published_rx)
        .await
        .unwrap()
        .unwrap();
    if cancel {
        drop(source);
        tokio::time::timeout(Duration::from_secs(1), closed_rx)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), dropped_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), healthy_rx)
                .await
                .is_err(),
            "consumer cancellation must not admit the queued peer"
        );
        return;
    }
    if parallel == 1 {
        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut healthy_rx)
                .await
                .is_err(),
            "late candidate must not exceed the occupied connection limit"
        );
        release_tx.send(()).unwrap();
    }
    let connected = tokio::time::timeout(Duration::from_secs(1), healthy_rx).await;
    let output = tokio::time::timeout(Duration::from_secs(2), source.next()).await;
    drop(source);
    assert!(
        matches!(connected, Ok(Ok(()))),
        "late candidate did not use available capacity after {:?}",
        published.elapsed()
    );
    assert!(
        matches!(output, Ok(Some(ref bytes)) if bytes.contains(&0x66)),
        "late initial candidate must deliver actual media"
    );
}

#[tokio::test]
async fn late_initial_candidate_uses_spare_slot_during_silent_handshake() {
    initial_silent_candidate_control(2, false).await;
}

#[tokio::test]
async fn late_initial_candidate_waits_for_full_connection_capacity() {
    initial_silent_candidate_control(1, false).await;
}

#[tokio::test]
async fn dropping_initial_connect_cancels_attempts_and_discovery() {
    initial_silent_candidate_control(1, true).await;
}
