use super::*;
use ace_media::mpegts::{ts_pid, TS_PACKET_LEN};
const PMT: u16 = 0x100;
const VIDEO: u16 = 0x101;
fn packet(pid: u16, marker: u8, rai: bool) -> Vec<u8> {
    let mut p = vec![marker; TS_PACKET_LEN];
    p[..4].copy_from_slice(&[0x47, ((pid >> 8) as u8 & 0x1f) | 0x40, pid as u8, 0x10]);
    if rai {
        p[3] = 0x30;
        p[4] = 1;
        p[5] = 0x40;
    }
    p
}

fn psi(pid: u16, section: &[u8]) -> Vec<u8> {
    let mut p = packet(pid, 0xff, false);
    p[4] = 0;
    p[5..5 + section.len()].copy_from_slice(section);
    p
}

fn pat(pmt: u16) -> Vec<u8> {
    psi(
        0,
        &[
            0,
            0xb0,
            13,
            0,
            1,
            0xc1,
            0,
            0,
            0,
            1,
            0xe0 | (pmt >> 8) as u8,
            pmt as u8,
            0,
            0,
            0,
            0,
        ],
    )
}

fn pmt(pmt: u16, video: u16, codec: u8) -> Vec<u8> {
    psi(
        pmt,
        &[
            2,
            0xb0,
            18,
            0,
            1,
            0xc1,
            0,
            0,
            0xe0 | (video >> 8) as u8,
            video as u8,
            0xf0,
            0,
            codec,
            0xe0 | (video >> 8) as u8,
            video as u8,
            0xf0,
            0,
            0,
            0,
            0,
            0,
        ],
    )
}

fn markers(bytes: &[u8]) -> Vec<u8> {
    bytes
        .as_chunks::<TS_PACKET_LEN>()
        .0
        .iter()
        .filter(|p| ts_pid(p.as_slice()) == VIDEO && p[3] & 0x10 != 0)
        .map(|p| p[20])
        .collect()
}

async fn buffered_provider(config: StartupBufferConfig) -> Box<dyn TsSource> {
    let (tx, rx) = mpsc::channel(8);
    // Feed the engine adapter the authenticated output covered by swarm resync tests.
    for chunk in [
        LiveOutput {
            bytes: Bytes::from(
                [pat(PMT), pmt(PMT, VIDEO, 0x1b), packet(VIDEO, b'a', true)].concat(),
            ),
            discontinuity: false,
        },
        LiveOutput {
            bytes: Bytes::from(
                [
                    packet(VIDEO, b'b', false),
                    packet(VIDEO, b'c', false),
                    packet(VIDEO, b'd', false),
                ]
                .concat(),
            ),
            discontinuity: false,
        },
        LiveOutput {
            bytes: Bytes::from(
                [pat(PMT), pmt(PMT, VIDEO, 0x1b), packet(VIDEO, b'r', true)].concat(),
            ),
            discontinuity: true,
        },
    ] {
        tx.send(chunk).await.unwrap();
    }
    drop(tx);
    let source = Box::new(AceSource {
        metrics: None,
        _sampler: None,
        rx,
        discontinuity: false,
        peers: Arc::new(AtomicU32::new(0)),
        downloaded: Arc::new(AtomicU64::new(0)),
        uploaded: Arc::new(AtomicU64::new(0)),
        peers_served: Arc::new(AtomicU32::new(0)),
        metadata: StreamMetadata::default(),
    });
    StartupBufferedSource::new(source, config, Some(188))
}

#[tokio::test]
async fn released_startup_source_emits_provider_prefix_before_gap() {
    let mut source = buffered_provider(StartupBufferConfig {
        target_ms: 1,
        max_bytes: 4096,
        timeout_ms: 10_000,
    })
    .await;
    assert_eq!(markers(&source.next().await.unwrap()), b"a");
    assert!(!source.take_discontinuity());
    assert_eq!(markers(&source.next().await.unwrap()), b"bcd");
    assert!(!source.take_discontinuity());
    assert_eq!(markers(&source.next().await.unwrap()), b"r");
    assert!(source.take_discontinuity());
    assert!(source.next().await.is_none());
}

#[tokio::test]
async fn collecting_startup_source_retains_existing_gap_discard_policy() {
    let mut source = buffered_provider(StartupBufferConfig {
        target_ms: 10_000,
        max_bytes: 4096,
        timeout_ms: 10_000,
    })
    .await;
    assert_eq!(
        markers(&source.next().await.unwrap()),
        b"r",
        "collecting startup still discards its pre-gap reservoir"
    );
    assert!(source.take_discontinuity());
    assert!(source.next().await.is_none());
}

// This fixture deliberately crosses the real peer -> AceSource -> startup buffer ->
// StreamSession pump -> native HTTP body boundary. The only injected condition is the
// deterministic discovery of a second local peer; continuity and discontinuity are produced
// by the real signed-piece follower.
const HTTP_RECOVERY_PAYLOAD: usize = 752 - 96;
const HTTP_RECOVERY_VIDEO_PID: u16 = 0x100;
const HTTP_RECOVERY_FIRST_LAST_PIECE: u32 = 8;
const HTTP_RECOVERY_NEW_LAST_PIECE: u32 = 35;
const HTTP_RECOVERY_MAX_HEAD: u64 = 35;
const HTTP_RECOVERY_PREFETCH: u64 = 35;
const HTTP_RECOVERY_STALE_MS: u64 = 900;
const HTTP_RECOVERY_RESUME_KEYFRAME: usize = 9_400;
// Piece 8 ends at byte 5,904. This is the final complete transport packet before
// that end, so consuming it drains the initial peer's entire complete-packet prefix.
const HTTP_RECOVERY_PREFIX_BARRIER: usize = 5_640;
const HTTP_RECOVERY_FINAL_WITNESS: usize = 6_204;

#[derive(Clone, Copy, Debug)]
enum HttpReconnectWindow {
    EvictedOne,
    Contiguous,
    CancelAfterStale,
}

struct OneShotSourceProvider(std::sync::Mutex<Option<Box<dyn TsSource>>>);

struct DropNotifyingSource {
    inner: Box<dyn TsSource>,
    dropped: Arc<tokio::sync::Notify>,
}

impl Drop for DropNotifyingSource {
    fn drop(&mut self) {
        self.dropped.notify_one();
    }
}

#[async_trait]
impl TsSource for DropNotifyingSource {
    async fn next(&mut self) -> Option<Bytes> {
        self.inner.next().await
    }

    fn take_discontinuity(&mut self) -> bool {
        self.inner.take_discontinuity()
    }

    fn stats(&self) -> SourceStats {
        self.inner.stats()
    }

    fn stats_handle(&self) -> Option<crate::provider::StatsHandle> {
        self.inner.stats_handle()
    }

    fn metadata(&self) -> StreamMetadata {
        self.inner.metadata()
    }
}

#[async_trait]
impl StreamProvider for OneShotSourceProvider {
    fn network(&self) -> &'static str {
        "ace"
    }

    async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.0
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| ProviderError::Backend("test source already opened".into()))
    }
}

fn http_recovery_pieces(auth: &ace_wire::live_auth::LiveSourceAuth) -> Vec<Vec<u8>> {
    let fixture = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/vectors/media/h264-keyframes.ts"
    ));
    let count = (HTTP_RECOVERY_NEW_LAST_PIECE + 1) as usize;
    let mut bytes = Vec::with_capacity(count * HTTP_RECOVERY_PAYLOAD);
    while bytes.len() < count * HTTP_RECOVERY_PAYLOAD {
        bytes.extend_from_slice(fixture);
    }
    bytes.truncate(count * HTTP_RECOVERY_PAYLOAD);
    let (payloads, remainder) = bytes.as_chunks::<HTTP_RECOVERY_PAYLOAD>();
    assert!(remainder.is_empty(), "piece builder uses complete payloads");
    payloads
        .iter()
        .map(|payload| {
            let mut piece = payload.to_vec();
            piece.extend_from_slice(&auth.sign(payload));
            piece
        })
        .collect()
}

fn http_live_position(min_piece: i64, max_piece: i64) -> ace_wire::extended::LivePosition {
    ace_wire::extended::LivePosition {
        min_piece,
        max_piece,
        position: -1,
        distance_from_source: 1,
    }
}

// The owner must close this boundary before dropping body/session consumers.
async fn owned_http_peer(
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    peer: impl std::future::Future<Output = ()>,
) {
    tokio::select! {
        biased;
        result = shutdown.wait_for(|stopping| *stopping) => {
            result.expect("fixture shutdown owner must outlive peer tasks");
        }
        () = peer => {}
    }
}

#[tokio::test]
async fn http_peer_shutdown_cancels_queued_reply_but_preserves_active_failure() {
    // A completion fence holds the same failing reply pending on either side of
    // the ownership boundary. No socket timing or scheduler sleeps are required.
    for shutting_down in [true, false] {
        let (shutdown, receiver) = tokio::sync::watch::channel(false);
        let (queued_tx, queued_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(owned_http_peer(receiver, async move {
            queued_tx.send(()).unwrap();
            let reply: std::io::Result<()> = release_rx.await.unwrap();
            reply.expect("queued fixture reply failed before owned shutdown");
        }));
        tokio::time::timeout(Duration::from_secs(3), queued_rx)
            .await
            .unwrap()
            .unwrap();
        if shutting_down {
            shutdown.send(true).unwrap();
        }
        let delivered = release_tx.send(Err(std::io::Error::from(
            std::io::ErrorKind::ConnectionReset,
        )));
        if !shutting_down {
            assert!(delivered.is_ok(), "active peer still owns its queued reply");
        }
        let result = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .expect("fixture task must join naturally");
        if shutting_down {
            assert!(
                result.is_ok(),
                "owned shutdown must cancel queued reply: {result:?}"
            );
        } else {
            assert!(
                result.unwrap_err().is_panic(),
                "active reply errors remain fatal"
            );
        }
    }
}

async fn http_recovery_peer(
    listener: tokio::net::TcpListener,
    pieces: Arc<Vec<Vec<u8>>>,
    min_piece: i64,
    max_piece: i64,
    serve_through: u32,
    start: Option<Arc<tokio::sync::Notify>>,
    sent: tokio::sync::mpsc::UnboundedSender<u32>,
) {
    use ace_peer::session::PeerSession;
    use ace_wire::extended::{NodeFields, OutgoingExtendedHandshake};
    use ace_wire::message::PeerMessage;

    let (stream, _) = listener.accept().await.unwrap();
    let mut peer = PeerSession::new(stream);
    peer.accept_handshake([0; 20], |_| true).await.unwrap();
    peer.send_extended_handshake(&OutgoingExtendedHandshake {
        ace_metadata_version: 1,
        ut_metadata_id: 2,
        mi: Some(http_live_position(min_piece, max_piece)),
        node: NodeFields::default(),
        peer_ip: None,
        metadata_size: None,
    })
    .await
    .unwrap();
    peer.send(&PeerMessage::Unchoke).await.unwrap();
    if let Some(start) = start {
        start.notified().await;
    }

    loop {
        let message = match peer.read_message().await {
            Ok(message) => message,
            Err(ace_peer::PeerError::Closed) => break,
            Err(error) => panic!("HTTP recovery fixture read failed before shutdown: {error:?}"),
        };
        if let PeerMessage::Unknown { id: 6, payload } = message {
            if payload.len() < 8 {
                continue;
            }
            let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
            if piece > serve_through || piece as usize >= pieces.len() {
                continue;
            }
            peer.send(&ace_wire::live_codec::build_piece(
                0,
                piece,
                0,
                [0; 8],
                &pieces[piece as usize],
            ))
            .await
            .unwrap();
            let _ = sent.send(piece);
        }
    }
}

async fn wait_http_recovery_stats(
    session: &crate::session::StreamSession,
    predicate: impl Fn(&crate::provider::LiveRecoveryStats) -> bool,
) -> crate::provider::LiveRecoveryStats {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let stats = session.stats().await;
            if let Some(recovery) = stats.recovery {
                if predicate(&recovery) {
                    return recovery;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("bounded wait for actual follower recovery statistics")
}

fn http_first_video_packet(bytes: &[u8]) -> Option<&[u8]> {
    bytes
        .as_chunks::<TS_PACKET_LEN>()
        .0
        .iter()
        .map(|packet| packet.as_slice())
        .find(|packet| ts_pid(packet) == HTTP_RECOVERY_VIDEO_PID)
}

fn http_video_discontinuity_count(bytes: &[u8]) -> usize {
    bytes
        .as_chunks::<TS_PACKET_LEN>()
        .0
        .iter()
        .filter(|packet| {
            ts_pid(packet.as_slice()) == HTTP_RECOVERY_VIDEO_PID
                && ace_media::mpegts::ts_timing(packet).discontinuity
        })
        .count()
}

fn http_has_valid_adaptation_flags(packet: &[u8]) -> bool {
    if packet.len() != TS_PACKET_LEN {
        return false;
    }
    let adaptation_control = (packet[3] >> 4) & 0x03;
    let adaptation_length = packet[4] as usize;
    matches!(adaptation_control, 0x02 | 0x03)
        && adaptation_length >= 1
        && 5 + adaptation_length <= TS_PACKET_LEN
}

fn http_source_packet_matches(actual: &[u8], expected: &[u8]) -> bool {
    actual.len() == TS_PACKET_LEN
        && expected.len() == TS_PACKET_LEN
        && actual[0..3] == expected[0..3]
        && actual[3] & 0xf0 == expected[3] & 0xf0
        && actual[4] == expected[4]
        && if http_has_valid_adaptation_flags(actual) && http_has_valid_adaptation_flags(expected) {
            actual[5] & !0x80 == expected[5] & !0x80
        } else {
            actual[5] == expected[5]
        }
        && actual[6..] == expected[6..]
}

fn http_has_source_packet(bytes: &[u8], expected: &[u8]) -> bool {
    bytes
        .as_chunks::<TS_PACKET_LEN>()
        .0
        .iter()
        .any(|packet| http_source_packet_matches(packet.as_slice(), expected))
}

fn http_source_packet_offsets(bytes: &[u8], expected: &[u8]) -> Vec<usize> {
    bytes
        .as_chunks::<TS_PACKET_LEN>()
        .0
        .iter()
        .enumerate()
        .filter_map(|(index, packet)| {
            http_source_packet_matches(packet.as_slice(), expected).then_some(index * TS_PACKET_LEN)
        })
        .collect()
}

fn http_video_packets(bytes: &[u8]) -> Vec<&[u8]> {
    bytes
        .as_chunks::<TS_PACKET_LEN>()
        .0
        .iter()
        .map(|packet| packet.as_slice())
        .filter(|packet| ts_pid(packet) == HTTP_RECOVERY_VIDEO_PID)
        .collect()
}

fn expected_http_video_packets(fixture: &[u8], start: usize, end: usize) -> Vec<&[u8]> {
    (start..end)
        .step_by(TS_PACKET_LEN)
        .filter_map(|absolute| {
            let offset = absolute % fixture.len();
            let packet = &fixture[offset..offset + TS_PACKET_LEN];
            (ts_pid(packet) == HTTP_RECOVERY_VIDEO_PID).then_some(packet)
        })
        .collect()
}

fn http_video_sequence_matches(actual: &[&[u8]], expected: &[&[u8]]) -> bool {
    actual.len() <= expected.len()
        && actual
            .iter()
            .zip(expected)
            .all(|(actual, expected)| http_source_packet_matches(actual, expected))
}

fn http_source_packet_count(bytes: &[u8], expected: &[u8]) -> usize {
    http_source_packet_offsets(bytes, expected).len()
}

#[test]
fn http_video_packet_comparison_normalizes_only_continuity_and_valid_discontinuity() {
    let fixture = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/vectors/media/h264-keyframes.ts"
    ));
    let payload_packet = &fixture[9_776..9_776 + TS_PACKET_LEN];
    assert_eq!(
        &payload_packet[..8],
        &[0x47, 0x01, 0x00, 0x1b, 0xc0, 0x41, 0xdb, 0x7a],
        "fixture packet at offset 9776 has no adaptation field"
    );
    assert!(!http_has_valid_adaptation_flags(payload_packet));
    assert!(http_source_packet_matches(payload_packet, payload_packet));

    let mut altered_payload = payload_packet.to_vec();
    altered_payload[5] = 0x40;
    assert!(!http_source_packet_matches(
        &altered_payload,
        payload_packet
    ));
    assert!(!http_has_source_packet(&altered_payload, payload_packet));
    assert!(http_source_packet_offsets(&altered_payload, payload_packet).is_empty());
    assert!(!http_video_sequence_matches(
        &[altered_payload.as_slice()],
        &[payload_packet]
    ));

    let adaptation_packet = &fixture[9_400..9_400 + TS_PACKET_LEN];
    assert!(http_has_valid_adaptation_flags(adaptation_packet));
    let mut valid_discontinuity = adaptation_packet.to_vec();
    valid_discontinuity[3] ^= 0x01;
    valid_discontinuity[5] ^= 0x80;
    assert!(http_source_packet_matches(
        &valid_discontinuity,
        adaptation_packet
    ));
    assert!(http_video_sequence_matches(
        &[valid_discontinuity.as_slice()],
        &[adaptation_packet]
    ));

    let mut other_adaptation_flag = adaptation_packet.to_vec();
    other_adaptation_flag[5] ^= 0x40;
    assert!(!http_source_packet_matches(
        &other_adaptation_flag,
        adaptation_packet
    ));

    let mut invalid_adaptation = adaptation_packet.to_vec();
    invalid_adaptation[4] = (TS_PACKET_LEN - 4) as u8;
    let mut invalid_adaptation_changed_flags = invalid_adaptation.clone();
    invalid_adaptation_changed_flags[5] ^= 0x80;
    assert!(!http_has_valid_adaptation_flags(&invalid_adaptation));
    assert!(!http_source_packet_matches(
        &invalid_adaptation_changed_flags,
        &invalid_adaptation
    ));
}

async fn extend_raw_until_packet_count(
    raw: &mut tokio::sync::broadcast::Receiver<crate::session::StreamEvent>,
    collected: &mut Vec<u8>,
    expected: &[u8],
    target_count: usize,
    marker_count: &mut usize,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while http_source_packet_count(collected, expected) < target_count {
        let event = tokio::time::timeout_at(deadline, raw.recv())
            .await
            .expect("bounded raw session progress")
            .expect("raw session remains open until explicit stop");
        match event {
            crate::session::StreamEvent::Data(data) => collected.extend_from_slice(&data),
            crate::session::StreamEvent::Discontinuity => *marker_count += 1,
        }
        assert!(collected.len() <= 1 << 20, "bounded raw session capture");
    }
}

async fn wait_seed_registry_state(
    registry: &ace_swarm::listen::SeedRegistry,
    infohash: &[u8; 20],
    expected: bool,
) -> bool {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if registry.serves(infohash) == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok()
}

async fn collect_http_body_until(
    body: &mut (impl futures::Stream<Item = Result<Bytes, axum::Error>> + Unpin),
    done: impl FnMut(&[u8]) -> bool,
) -> Vec<u8> {
    let mut collected = Vec::new();
    extend_http_body_until(body, &mut collected, done).await;
    collected
}

async fn extend_http_body_until(
    body: &mut (impl futures::Stream<Item = Result<Bytes, axum::Error>> + Unpin),
    collected: &mut Vec<u8>,
    mut done: impl FnMut(&[u8]) -> bool,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while !done(collected) {
        let next = tokio::time::timeout_at(deadline, futures::StreamExt::next(body))
            .await
            .expect("bounded HTTP body progress")
            .expect("HTTP body remained open for this live session")
            .expect("HTTP body transport succeeded");
        collected.extend_from_slice(&next);
        assert!(collected.len() <= 1 << 20, "bounded test HTTP body");
    }
}

async fn run_http_reconnect_composition(mode: HttpReconnectWindow, decode: bool) {
    let auth = Arc::new(ace_wire::live_auth::LiveSourceAuth::generate());
    let mut info = ace_swarm::types::StreamInfo {
        infohash: [0; 20],
        piece_length: 752,
        chunk_length: 752,
        trackers: Vec::new(),
        metadata: StreamMetadata::default(),
        sig_len: auth.signature_len(),
        source_pubkey: auth.pubkey_der(),
    };
    // Keep this explicit: the fixture uses the authenticated live format, not injected
    // AceSource::discontinuity state.
    info.metadata.title = None;
    let pieces = Arc::new(http_recovery_pieces(&auth));
    let producer_registry = ace_swarm::listen::SeedRegistry::new();
    let infohash = info.infohash;
    let initial_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(initial_addr) = initial_listener.local_addr().unwrap() else {
        unreachable!()
    };
    let reconnect_listener = if matches!(mode, HttpReconnectWindow::CancelAfterStale) {
        None
    } else {
        Some(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap())
    };
    let reconnect_addr = reconnect_listener.as_ref().map(|listener| {
        let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
            unreachable!()
        };
        addr
    });
    let start_peer = Arc::new(tokio::sync::Notify::new());
    let discovery_release = Arc::new(tokio::sync::Notify::new());
    let discovery_started = Arc::new(tokio::sync::Notify::new());
    let discovery_finished = Arc::new(tokio::sync::Notify::new());
    let release_for_discovery = discovery_release.clone();
    let started_for_discovery = discovery_started.clone();
    let finished_for_discovery = discovery_finished.clone();
    let (sent_tx, sent_rx) = tokio::sync::mpsc::unbounded_channel();
    let (peer_shutdown, _) = tokio::sync::watch::channel(false);
    let mut peer_tasks = tokio::task::JoinSet::new();
    let initial_pieces = pieces.clone();
    let initial_start = Some(start_peer.clone());
    let initial_sent = sent_tx.clone();
    peer_tasks.spawn(owned_http_peer(peer_shutdown.subscribe(), async move {
        http_recovery_peer(
            initial_listener,
            initial_pieces,
            0,
            HTTP_RECOVERY_PREFETCH as i64,
            HTTP_RECOVERY_FIRST_LAST_PIECE,
            initial_start,
            initial_sent,
        )
        .await
    }));
    if let (Some(listener), Some(_addr)) = (reconnect_listener, reconnect_addr) {
        let reconnect_pieces = pieces.clone();
        let reconnect_sent = sent_tx.clone();
        let min_piece = if matches!(mode, HttpReconnectWindow::EvictedOne) {
            10
        } else {
            9
        };
        peer_tasks.spawn(owned_http_peer(peer_shutdown.subscribe(), async move {
            http_recovery_peer(
                listener,
                reconnect_pieces,
                min_piece,
                HTTP_RECOVERY_MAX_HEAD as i64,
                HTTP_RECOVERY_NEW_LAST_PIECE,
                None,
                reconnect_sent,
            )
            .await
        }));
        let release_for_discovery = release_for_discovery.clone();
        let started_for_discovery = started_for_discovery.clone();
        let finished_for_discovery = finished_for_discovery.clone();
        let discovery: PeerDiscovery = Arc::new(move |_, sender| {
            let release = release_for_discovery.clone();
            let started = started_for_discovery.clone();
            let finished = finished_for_discovery.clone();
            Box::pin(async move {
                if sender.send(initial_addr).await.is_err() {
                    finished.notify_one();
                    return;
                }
                started.notify_one();
                if let Some(addr) = reconnect_addr {
                    release.notified().await;
                    let _ = sender.send(addr).await;
                }
                finished.notify_one();
            })
        });
        run_http_reconnect_case(
            mode,
            decode,
            info,
            HttpReconnectPeers {
                tasks: peer_tasks,
                peer_shutdown,
                sent_rx,
                start_peer,
                discovery,
                discovery_release,
                discovery_started,
                discovery_finished,
                producer_registry,
                infohash,
            },
        )
        .await;
        return;
    }

    let started_for_discovery = started_for_discovery.clone();
    let finished_for_discovery = finished_for_discovery.clone();
    let discovery: PeerDiscovery = Arc::new(move |_, sender| {
        let started = started_for_discovery.clone();
        let finished = finished_for_discovery.clone();
        Box::pin(async move {
            if sender.send(initial_addr).await.is_err() {
                finished.notify_one();
                return;
            }
            started.notify_one();
            finished.notify_one();
        })
    });
    run_http_reconnect_case(
        mode,
        decode,
        info,
        HttpReconnectPeers {
            tasks: peer_tasks,
            peer_shutdown,
            sent_rx,
            start_peer,
            discovery,
            discovery_release,
            discovery_started,
            discovery_finished,
            producer_registry,
            infohash,
        },
    )
    .await;
}

struct HttpReconnectPeers {
    tasks: tokio::task::JoinSet<()>,
    peer_shutdown: tokio::sync::watch::Sender<bool>,
    sent_rx: tokio::sync::mpsc::UnboundedReceiver<u32>,
    start_peer: Arc<tokio::sync::Notify>,
    discovery: PeerDiscovery,
    discovery_release: Arc<tokio::sync::Notify>,
    discovery_started: Arc<tokio::sync::Notify>,
    discovery_finished: Arc<tokio::sync::Notify>,
    producer_registry: ace_swarm::listen::SeedRegistry,
    infohash: [u8; 20],
}

async fn run_http_reconnect_case(
    mode: HttpReconnectWindow,
    decode: bool,
    info: ace_swarm::types::StreamInfo,
    peers: HttpReconnectPeers,
) {
    let HttpReconnectPeers {
        tasks: mut peer_tasks,
        peer_shutdown,
        mut sent_rx,
        start_peer,
        discovery,
        discovery_release,
        discovery_started,
        discovery_finished,
        producer_registry,
        infohash,
    } = peers;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures::FutureExt;
    use tower::ServiceExt;

    let live_recovery = LiveRecoveryConfig {
        request_timeout_ms: 150,
        stale_upstream_timeout_ms: HTTP_RECOVERY_STALE_MS,
        request_check_interval_ms: 25,
        max_active_upstreams: 1,
        max_parallel_connect: 1,
        max_piece_advance: 32,
        max_reasm_pieces_ahead: 64,
    };
    let source = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_seed_registry(producer_registry.clone())
        .with_prefetch_pieces(Some(HTTP_RECOVERY_PREFETCH))
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            max_bytes: 1 << 16,
            timeout_ms: 1_000,
        })
        .with_live_recovery(live_recovery)
        .with_seeding_enabled(false)
        .open_resolved("synthetic-http-recovery", info, discovery)
        .await
        .unwrap();
    let mut registry = crate::provider::ProviderRegistry::new();
    let source_dropped = Arc::new(tokio::sync::Notify::new());
    registry.register(Arc::new(OneShotSourceProvider(std::sync::Mutex::new(
        Some(Box::new(DropNotifyingSource {
            inner: source,
            dropped: source_dropped.clone(),
        }) as Box<dyn TsSource>),
    ))));
    let manager = crate::manager::StreamManager::new(registry);
    let app = crate::http::router(crate::http::AppState {
        manager: manager.clone(),
        networks: vec!["ace".into()],
        resolve_content_ids_in_getstream: false,
        ace_sessions: Arc::new(crate::http::AceSessionStore::default()),
        experimental_ace_compat: false,
        broadcasts: None,
    });

    let mut owner: Option<Arc<crate::session::StreamSession>> = None;
    let run = async {
        let response = app
            .clone()
            .oneshot(
                Request::get("/streams/ace/synthetic.ts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let session = manager.get("ace", "synthetic").await.unwrap();
        owner = Some(session.clone());
        let mut raw = session.raw_receiver();
        let mut body = response.into_body().into_data_stream();
        discovery_started.notified().await;
        start_peer.notify_one();

        let mut initial_pieces_sent = vec![false; (HTTP_RECOVERY_FIRST_LAST_PIECE + 1) as usize];
        tokio::time::timeout(Duration::from_secs(5), async {
            while initial_pieces_sent.iter().any(|sent| !sent) {
                let piece = sent_rx
                    .recv()
                    .await
                    .expect("initial peer task remains available for its finite prefix");
                if piece <= HTTP_RECOVERY_FIRST_LAST_PIECE {
                    initial_pieces_sent[piece as usize] = true;
                }
            }
        })
        .await
        .expect("initial peer serves every piece in its bounded pre-gap window");

        let fixture = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/vectors/media/h264-keyframes.ts"
        ));
        let prefix_barrier =
            &fixture[HTTP_RECOVERY_PREFIX_BARRIER..HTTP_RECOVERY_PREFIX_BARRIER + TS_PACKET_LEN];
        let mut raw_prefix = Vec::new();
        let mut raw_markers = 0;
        extend_raw_until_packet_count(
            &mut raw,
            &mut raw_prefix,
            prefix_barrier,
            1,
            &mut raw_markers,
        )
        .await;
        let mut pre_gap = collect_http_body_until(&mut body, |bytes| {
            http_has_source_packet(bytes, prefix_barrier)
        })
        .await;
        let initial_video = http_first_video_packet(&pre_gap).expect("pre-gap HTTP video");
        assert_eq!(ts_pid(initial_video), HTTP_RECOVERY_VIDEO_PID);
        assert_eq!(raw_markers, 0, "no recovery marker before the outage");
        assert!(http_has_source_packet(&raw_prefix, prefix_barrier));
        assert!(http_has_source_packet(&pre_gap, prefix_barrier));
        assert!(
            fixture
                .as_chunks::<TS_PACKET_LEN>()
                .0
                .iter()
                .any(|packet| packet.as_slice() == initial_video),
            "pre-gap HTTP video packet came from the checked-in transport"
        );
        assert!(
            fixture.windows(4).any(|window| window == [0, 0, 1, 0x65]),
            "checked-in fixture includes H.264 IDR data"
        );
        assert!(
            wait_seed_registry_state(&producer_registry, &infohash, true).await,
            "the live follower owns its producer lease before cancellation"
        );

        if matches!(mode, HttpReconnectWindow::CancelAfterStale) {
            let stale =
                wait_http_recovery_stats(&session, |stats| stats.stale_teardowns >= 1).await;
            assert_eq!(stale.stale_teardowns, 1);
            peer_shutdown.send_replace(true);
            drop(body);
            drop(raw);
            return HttpCompositionObservation {
                stale_teardowns: stale.stale_teardowns,
                reconnects: stale.upstream_reconnects,
                skipped_gaps: stale.skipped_evicted_gaps,
                marker_before_data: false,
                body_reached_recovery_progress: false,
            };
        }

        let stale = wait_http_recovery_stats(&session, |stats| stats.stale_teardowns >= 1).await;
        assert_eq!(stale.stale_teardowns, 1, "one actual stale teardown");

        // The finite initial peer has been exhausted and production recorded stale teardown.
        // Drain only the now-finite raw queue, then consume HTTP through its terminal packet
        // before releasing the reconnect peer. This makes the body boundary source-observed.
        loop {
            match raw.try_recv() {
                Ok(crate::session::StreamEvent::Data(data)) => {
                    raw_prefix.extend_from_slice(&data);
                    assert!(raw_prefix.len() <= 1 << 20, "bounded raw prefix");
                }
                Ok(crate::session::StreamEvent::Discontinuity) => raw_markers += 1,
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                    panic!("raw session remains open until the owned manager stop")
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(skipped)) => {
                    panic!("raw prefix observer lagged by {skipped} events")
                }
            }
        }
        assert_eq!(raw_markers, 0, "no marker before reconnect release");
        let raw_terminal_packet = raw_prefix
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .iter()
            .rev()
            .find(|packet| ts_pid(packet.as_slice()) == HTTP_RECOVERY_VIDEO_PID)
            .expect("finite raw prefix contains a video progress packet")
            .as_slice();
        let raw_terminal_count = http_source_packet_count(&raw_prefix, raw_terminal_packet);
        assert!(raw_terminal_count > 0, "finite raw boundary is observable");
        extend_http_body_until(&mut body, &mut pre_gap, |bytes| {
            http_source_packet_count(bytes, raw_terminal_packet) >= raw_terminal_count
        })
        .await;
        assert_eq!(
            http_source_packet_count(&pre_gap, raw_terminal_packet),
            raw_terminal_count,
            "HTTP prefix is consumed through the terminal pre-reconnect raw packet"
        );
        let old_tail = pre_gap
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .iter()
            .filter(|packet| ts_pid(packet.as_slice()) == HTTP_RECOVERY_VIDEO_PID)
            .filter_map(|packet| {
                let packet = packet.as_slice();
                let fixture_offsets = http_source_packet_offsets(fixture, packet);
                (fixture_offsets.len() == 1
                    && http_source_packet_count(&raw_prefix, packet)
                        == http_source_packet_count(&pre_gap, packet))
                .then(|| (packet, fixture_offsets[0]))
            })
            .next_back()
            .expect("a unique video packet is shared by the consumed raw and HTTP prefixes");
        let old_tail_offsets = http_source_packet_offsets(fixture, old_tail.0);
        assert_eq!(old_tail_offsets.len(), 1);
        let old_tail_offset = old_tail.1;
        let old_tail_packet = &fixture[old_tail_offset..old_tail_offset + TS_PACKET_LEN];
        let final_witness_packet =
            &fixture[HTTP_RECOVERY_FINAL_WITNESS..HTTP_RECOVERY_FINAL_WITNESS + TS_PACKET_LEN];
        let final_witness_cycle_offset = fixture.len() + HTTP_RECOVERY_FINAL_WITNESS;
        assert!(
            HTTP_RECOVERY_FINAL_WITNESS > old_tail_offset,
            "the witness fixture offset is beyond the selected old-tail packet"
        );
        assert_eq!(
            http_source_packet_offsets(fixture, final_witness_packet),
            vec![HTTP_RECOVERY_FINAL_WITNESS],
            "the independent final witness packet is unique in the fixture cycle"
        );
        assert_eq!(
            http_source_packet_count(&raw_prefix, final_witness_packet),
            0,
            "the final witness is beyond the consumed first fixture cycle"
        );
        discovery_release.notify_one();
        let mut saw_resumed_data = false;
        let mut markers_before_resumed_data = 0;
        let mut data_after_marker = None;
        tokio::time::timeout(Duration::from_secs(8), async {
            while !saw_resumed_data {
                match raw.recv().await.unwrap() {
                    crate::session::StreamEvent::Discontinuity => {
                        markers_before_resumed_data += 1;
                    }
                    crate::session::StreamEvent::Data(data) => {
                        data_after_marker = Some(data);
                        saw_resumed_data = true;
                    }
                }
            }
        })
        .await
        .expect("actual session observer sees recovery events");
        let raw_after_marker = data_after_marker
            .as_ref()
            .expect("raw Data follows the observed recovery marker");
        let mut raw_progress = raw_prefix;
        raw_progress.extend_from_slice(raw_after_marker);
        assert_eq!(
            http_source_packet_count(&raw_progress, old_tail_packet),
            1,
            "the pre-gap raw prefix contains the selected video sentinel only"
        );
        let raw_witness_target = if matches!(mode, HttpReconnectWindow::Contiguous) {
            2
        } else {
            1
        };
        let raw_witness_before = http_source_packet_count(&raw_progress, final_witness_packet);
        assert!(raw_witness_before < raw_witness_target);
        let mut later_markers = 0;
        extend_raw_until_packet_count(
            &mut raw,
            &mut raw_progress,
            final_witness_packet,
            raw_witness_target,
            &mut later_markers,
        )
        .await;
        let body_witness_target = raw_witness_target;
        let recovered = collect_http_body_until(&mut body, |bytes| {
            http_source_packet_count(bytes, final_witness_packet) >= body_witness_target
        })
        .await;
        let recovery =
            wait_http_recovery_stats(&session, |stats| stats.upstream_reconnects >= 1).await;

        assert_eq!(
            http_source_packet_count(&recovered, final_witness_packet),
            body_witness_target,
            "HTTP recovery is observed through its independent later-cycle witness"
        );
        assert_eq!(
            http_source_packet_count(&recovered, old_tail_packet),
            1,
            "the unique pre-gap video packet has only its expected next-cycle occurrence, not a replayed old tail"
        );
        let first_video = http_first_video_packet(&recovered)
            .expect("HTTP body resumes with a fixture video packet");
        let first_video_packet = first_video
            .as_chunks::<TS_PACKET_LEN>()
            .0
            .first()
            .expect("first HTTP video packet has fixed transport size");
        let mut expected_offsets: Vec<usize> = match mode {
            HttpReconnectWindow::EvictedOne => vec![HTTP_RECOVERY_RESUME_KEYFRAME, 9_588],
            HttpReconnectWindow::Contiguous => {
                vec![6_016, 6_204, 6_768, HTTP_RECOVERY_RESUME_KEYFRAME, 9_588]
            }
            HttpReconnectWindow::CancelAfterStale => unreachable!(),
        };
        expected_offsets.push(old_tail_offset);
        let ordered_body_offsets = expected_offsets
            .iter()
            .map(|offset| {
                let expected = &fixture[*offset..*offset + TS_PACKET_LEN];
                let occurrences = http_source_packet_offsets(&recovered, expected);
                let expected_count = if matches!(mode, HttpReconnectWindow::Contiguous)
                    && (*offset == 6_016 || *offset == HTTP_RECOVERY_FINAL_WITNESS)
                {
                    2
                } else {
                    1
                };
                assert_eq!(
                    occurrences.len(),
                    expected_count,
                    "fixture packet at offset {offset} appears the expected number of times"
                );
                occurrences[0]
            })
            .collect::<Vec<_>>();
        assert!(
            ordered_body_offsets
                .windows(2)
                .all(|pair| pair[0] < pair[1]),
            "recovery interval advances through the ordered fixture packets without replay"
        );
        let expected_start = match mode {
            HttpReconnectWindow::EvictedOne => HTTP_RECOVERY_RESUME_KEYFRAME,
            HttpReconnectWindow::Contiguous => 6_016,
            HttpReconnectWindow::CancelAfterStale => unreachable!(),
        };
        let generated_source_len =
            (HTTP_RECOVERY_NEW_LAST_PIECE as usize + 1) * HTTP_RECOVERY_PAYLOAD;
        let expected_source_end = generated_source_len / TS_PACKET_LEN * TS_PACKET_LEN;
        let expected_video =
            expected_http_video_packets(fixture, expected_start, expected_source_end);
        let expected_video_through_witness = expected_http_video_packets(
            fixture,
            expected_start,
            final_witness_cycle_offset + TS_PACKET_LEN,
        );
        if matches!(mode, HttpReconnectWindow::EvictedOne) {
            let legitimate_tail_index = expected_video_through_witness
                .iter()
                .position(|packet| http_has_source_packet(packet, old_tail_packet))
                .expect("expected source sequence contains the legitimate later old-tail copy");
            let mut premature_replay =
                expected_video_through_witness[..legitimate_tail_index].to_vec();
            premature_replay.insert(2, old_tail_packet);
            assert!(
                premature_replay.len() < expected_video_through_witness.len(),
                "a replay before the independent witness cannot satisfy the full progress bound"
            );
            assert!(
                !http_video_sequence_matches(&premature_replay, &expected_video),
                "the full fixture-sequence oracle rejects a synthetic early old-tail replay"
            );
        }
        let actual_video = http_video_packets(&recovered);
        assert!(
            actual_video.len() >= expected_video_through_witness.len(),
            "the HTTP body contains every video packet through fixture offset {} in the next cycle",
            HTTP_RECOVERY_FINAL_WITNESS
        );
        assert!(
            http_video_sequence_matches(&actual_video, &expected_video),
            "every recovered video packet follows the complete fixture sequence without omitted, replayed, or extra video packets"
        );
        assert!(
            final_witness_cycle_offset > fixture.len() + old_tail_offset,
            "the independent second-cycle witness is strictly beyond the old-tail replay range"
        );
        let witness_positions = http_source_packet_offsets(&recovered, final_witness_packet);
        if body_witness_target == 2 {
            assert_eq!(
                witness_positions[1] - witness_positions[0],
                fixture.len(),
                "contiguous recovery reaches the independent witness in its next fixture cycle"
            );
        } else {
            assert_eq!(witness_positions.len(), 1);
        }
        let matching_raw_video = http_first_video_packet(raw_after_marker)
            .expect("raw Data following the marker contains video");
        assert!(
            http_has_source_packet(&recovered, matching_raw_video),
            "the HTTP body carries video from the raw Data event after the marker"
        );
        let expected_first_video_offset = expected_offsets[0];
        let expected_first_video =
            &fixture[expected_first_video_offset..expected_first_video_offset + TS_PACKET_LEN];
        let expected_first_video_count = if matches!(mode, HttpReconnectWindow::Contiguous) {
            2
        } else {
            1
        };
        assert_eq!(
            http_source_packet_count(&recovered, expected_first_video),
            expected_first_video_count,
            "the recovery access point/first continuation packet is not duplicated"
        );
        let actual_first_video_offsets = http_source_packet_offsets(fixture, first_video);
        assert_eq!(
            &first_video[..3],
            &expected_first_video[..3],
            "first recovered video fixture offsets: {actual_first_video_offsets:?}"
        );
        assert_eq!(first_video[3] & 0xf0, expected_first_video[3] & 0xf0);
        assert_eq!(first_video[4], expected_first_video[4]);
        assert_eq!(&first_video[6..], &expected_first_video[6..]);
        let resumed_at_later_keyframe = actual_video.len() >= expected_video_through_witness.len();
        assert!(
            saw_resumed_data,
            "raw observer receives post-reconnect Data"
        );
        if matches!(mode, HttpReconnectWindow::EvictedOne) {
            assert!(
                ace_media::mpegts::ts_timing(first_video_packet).discontinuity,
                "the first recovered video packet carries the HTTP reset marker"
            );
            assert_eq!(
                first_video[5] & !0x80,
                expected_first_video[5] & !0x80,
                "only the expected adaptation discontinuity bit changes"
            );
            assert_eq!(http_video_discontinuity_count(&recovered), 1);
            assert_eq!(markers_before_resumed_data, 1);
            assert_eq!(
                later_markers, 0,
                "no later raw marker before finite progress"
            );
            assert_eq!(recovery.skipped_evicted_gaps, 1);
        } else {
            assert!(
                !ace_media::mpegts::ts_timing(first_video_packet).discontinuity,
                "contiguous reconnect introduces no HTTP discontinuity flag"
            );
            assert_eq!(http_video_discontinuity_count(&recovered), 0);
            assert_eq!(markers_before_resumed_data, 0);
            assert_eq!(later_markers, 0, "contiguous reconnect has no later marker");
            assert_eq!(recovery.skipped_evicted_gaps, 0);
        }

        if decode {
            // Frame packet offsets, rather than total frames alone, distinguish
            // decoded post-reconnect data from the already consumed HTTP prefix.
            let before = probe_http_video_positions(&pre_gap).await;
            assert!(!before.is_empty(), "pre-gap HTTP body decodes video");
            assert!(before.iter().all(|&pos| pos < pre_gap.len()));
            let boundary = pre_gap.len();
            let mut complete = pre_gap.clone();
            complete.extend_from_slice(&recovered);
            let positions = probe_http_video_positions(&complete).await;
            let post = positions.iter().filter(|&&pos| pos >= boundary).count();
            assert!(post > 0, "actual HTTP body decodes video after reconnect");
        }
        peer_shutdown.send_replace(true);
        drop(body);
        drop(raw);
        HttpCompositionObservation {
            stale_teardowns: recovery.stale_teardowns,
            reconnects: recovery.upstream_reconnects,
            skipped_gaps: recovery.skipped_evicted_gaps,
            marker_before_data: markers_before_resumed_data == 1,
            body_reached_recovery_progress: resumed_at_later_keyframe,
        }
    };
    // Catch assertion panics long enough to explicitly stop the manager and reap the owned
    // loopback tasks. The test future owns only its body/raw receivers and local session.
    let run_result = std::panic::AssertUnwindSafe(run).catch_unwind().await;
    let run_panicked = run_result.is_err();
    peer_shutdown.send_replace(true);
    let owned_session = owner.is_some();
    discovery_release.notify_one();
    let stop_result =
        tokio::time::timeout(Duration::from_secs(5), manager.stop("ace", "synthetic")).await;
    drop(owner.take());
    drop(app);
    drop(manager);
    if run_panicked {
        peer_tasks.abort_all();
    }
    let expected_peers = if matches!(mode, HttpReconnectWindow::CancelAfterStale) {
        1
    } else {
        2
    };
    let owned_peer_tasks = peer_tasks.len();
    let peer_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut peer_join_failed = false;
    let mut peer_join_timed_out = false;
    let mut joined_peers = 0;
    while !peer_tasks.is_empty() {
        match tokio::time::timeout_at(peer_deadline, peer_tasks.join_next()).await {
            Ok(Some(Ok(()))) => joined_peers += 1,
            Ok(Some(Err(error))) => {
                joined_peers += 1;
                peer_join_failed |= !run_panicked || !error.is_cancelled();
                peer_tasks.abort_all();
                break;
            }
            Ok(None) => break,
            Err(_) => {
                peer_join_timed_out = true;
                peer_tasks.abort_all();
                break;
            }
        }
    }
    let peer_abort_drain_completed = if peer_tasks.is_empty() {
        true
    } else {
        tokio::time::timeout(Duration::from_secs(3), peer_tasks.shutdown())
            .await
            .is_ok()
    };
    let peer_tasks_reaped = peer_tasks.is_empty();
    let source_reaped = tokio::time::timeout(Duration::from_secs(5), source_dropped.notified())
        .await
        .is_ok();
    let follower_reaped = wait_seed_registry_state(&producer_registry, &infohash, false).await;
    let discovery_reaped =
        tokio::time::timeout(Duration::from_secs(3), discovery_finished.notified())
            .await
            .is_ok();
    let mut cleanup_errors = Vec::new();
    if owned_session && !matches!(stop_result, Ok(true)) {
        cleanup_errors.push("manager stop did not confirm the owned session");
    }
    if owned_peer_tasks != expected_peers {
        cleanup_errors.push("the fixture did not create its expected peer tasks");
    }
    if !run_panicked && joined_peers != expected_peers {
        cleanup_errors.push("graceful peer join did not account for every owned task");
    }
    if peer_join_failed {
        cleanup_errors.push("a peer task returned a join error");
    }
    if peer_join_timed_out {
        cleanup_errors.push("graceful peer-task join timed out");
    }
    if !peer_abort_drain_completed || !peer_tasks_reaped {
        cleanup_errors.push("abort-and-drain did not reap all peer tasks");
    }
    if !source_reaped {
        cleanup_errors.push("the wrapped AceSource did not drop before its deadline");
    }
    if !follower_reaped {
        cleanup_errors.push("the follower producer lease remained after manager stop");
    }
    if !discovery_reaped {
        cleanup_errors.push("the injected discovery callback did not finish");
    }
    let observation = match run_result {
        Ok(observation) => observation,
        Err(payload) if cleanup_errors.is_empty() => std::panic::resume_unwind(payload),
        Err(payload) => {
            let original = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&'static str>().copied())
                .unwrap_or("non-string scenario panic");
            panic!("scenario failed: {original}; cleanup issues: {cleanup_errors:?}");
        }
    };
    assert!(
        cleanup_errors.is_empty(),
        "owned cleanup issues: {cleanup_errors:?}"
    );
    assert_eq!(observation.stale_teardowns, 1);
    match mode {
        HttpReconnectWindow::EvictedOne => {
            assert_eq!(observation.reconnects, 1);
            assert_eq!(observation.skipped_gaps, 1);
            assert!(observation.marker_before_data);
            assert!(observation.body_reached_recovery_progress);
        }
        HttpReconnectWindow::Contiguous => {
            assert_eq!(observation.reconnects, 1);
            assert_eq!(observation.skipped_gaps, 0);
            assert!(!observation.marker_before_data);
            assert!(observation.body_reached_recovery_progress);
        }
        HttpReconnectWindow::CancelAfterStale => {
            assert_eq!(observation.reconnects, 0);
            assert_eq!(observation.skipped_gaps, 0);
        }
    }
}

#[derive(Debug)]
struct HttpCompositionObservation {
    stale_teardowns: u64,
    reconnects: u64,
    skipped_gaps: u64,
    marker_before_data: bool,
    body_reached_recovery_progress: bool,
}

#[tokio::test]
async fn real_evicted_reconnect_marker_reaches_native_http_body() {
    run_http_reconnect_composition(HttpReconnectWindow::EvictedOne, false).await;
}

#[tokio::test]
async fn real_contiguous_reconnect_keeps_native_http_body_unmarked() {
    run_http_reconnect_composition(HttpReconnectWindow::Contiguous, false).await;
}

#[tokio::test]
async fn real_http_body_cancellation_reaps_peer_and_discovery() {
    run_http_reconnect_composition(HttpReconnectWindow::CancelAfterStale, false).await;
}

// Offline finite-body decoding is deliberately separate from real-time player
// progress: packet positions bind decoded video frames to the recovered suffix.
async fn probe_http_video_positions(bytes: &[u8]) -> Vec<usize> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    assert!(bytes.len() <= 2 << 20, "bounded synthetic HTTP capture");
    let mut child = tokio::process::Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-f",
            "mpegts",
            "-show_frames",
            "-show_entries",
            "frame=media_type,pkt_pos",
            "-of",
            "json",
            "pipe:0",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("available offline ffprobe");
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        let mut input = child.stdin.take().unwrap();
        input.write_all(bytes).await?;
        drop(input);
        let mut output = Vec::new();
        child
            .stdout
            .take()
            .unwrap()
            .take((1 << 20) + 1)
            .read_to_end(&mut output)
            .await?;
        let status = child.wait().await?;
        Ok::<_, std::io::Error>((status, output))
    })
    .await;
    if !matches!(&result, Ok(Ok(_))) {
        let _ = child.kill().await;
        let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
    }
    let (status, output) = result
        .expect("bounded decoder observation")
        .expect("decoder pipes");
    assert!(status.success(), "decoder succeeded");
    assert!(output.len() <= 1 << 20, "bounded frame report");
    let value: serde_json::Value = serde_json::from_slice(&output).unwrap();
    value["frames"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|frame| frame["media_type"] == "video")
        .filter_map(|frame| frame["pkt_pos"].as_str().and_then(|pos| pos.parse().ok()))
        .collect()
}

fn http_decode_available() -> bool {
    let available = std::env::var_os("PATH").is_some_and(|paths| {
        std::env::split_paths(&paths).any(|path| path.join("ffprobe").is_file())
    });
    if !available {
        eprintln!("skipping offline HTTP decode control: ffprobe unavailable");
    }
    available
}

#[tokio::test]
async fn evicted_http_recovery_decodes_post_gap_video() {
    if http_decode_available() {
        run_http_reconnect_composition(HttpReconnectWindow::EvictedOne, true).await;
    }
}

#[tokio::test]
async fn contiguous_http_recovery_decodes_post_gap_video() {
    if http_decode_available() {
        run_http_reconnect_composition(HttpReconnectWindow::Contiguous, true).await;
    }
}
