//! End-to-end synthetic HLS audio normalization acceptance.
#![allow(clippy::duplicate_mod)]

#[allow(dead_code)]
#[path = "support/hls_audio.rs"]
mod support;

use ace_engine::config::HlsConfig;
use ace_engine::http::{router, AceSessionStore, AppState};
use ace_engine::manager::StreamManager;
use ace_engine::provider::{
    ProviderError, ProviderRegistry, SourceStats, StreamProvider, TsSource,
};
use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::Request;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use support::{AudioCase, SyntheticCase, SyntheticTools};
use tower::ServiceExt;

const SYNTHETIC_STREAM_ID: &str = "0123456789abcdef0123456789abcdef01234567";

struct GeneratedProvider {
    data: Bytes,
}
struct GeneratedSource {
    data: Option<Bytes>,
    metadata: ace_swarm::types::StreamMetadata,
}

struct ChanneledProvider(
    std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<(bool, Bytes)>>>,
);
struct ChanneledSource {
    rx: tokio::sync::mpsc::UnboundedReceiver<(bool, Bytes)>,
    gap: bool,
}

struct MemoryVodProvider(Bytes);
struct MemoryVod(Bytes);
struct MemoryVodSource(Option<Bytes>);

#[async_trait]
impl ace_engine::provider::VodByteSource for MemoryVodSource {
    fn content_length(&self) -> u64 {
        self.0.as_ref().map_or(0, |b| b.len()) as u64
    }
    async fn next(&mut self) -> Option<Bytes> {
        self.0.take()
    }
}

#[async_trait]
impl ace_engine::provider::VodContent for MemoryVod {
    fn content_length(&self) -> u64 {
        self.0.len() as u64
    }
    async fn open_range(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Box<dyn ace_engine::provider::VodByteSource>, ProviderError> {
        Ok(Box::new(MemoryVodSource(Some(
            self.0.slice(start as usize..=end as usize),
        ))))
    }
}

#[async_trait]
impl StreamProvider for MemoryVodProvider {
    fn network(&self) -> &'static str {
        "synthetic"
    }
    async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        Err(ProviderError::NotFound)
    }
    async fn resolve_vod(
        &self,
        _id: &str,
    ) -> Result<Box<dyn ace_engine::provider::VodContent>, ProviderError> {
        Ok(Box::new(MemoryVod(self.0.clone())))
    }
}

#[async_trait]
impl TsSource for ChanneledSource {
    async fn next(&mut self) -> Option<Bytes> {
        let (gap, bytes) = self.rx.recv().await?;
        self.gap = gap;
        Some(bytes)
    }
    fn take_discontinuity(&mut self) -> bool {
        std::mem::take(&mut self.gap)
    }
    fn stats(&self) -> SourceStats {
        SourceStats::default()
    }
    fn metadata(&self) -> ace_swarm::types::StreamMetadata {
        ace_swarm::types::StreamMetadata {
            title: Some("Synthetic HLS Acceptance".into()),
            ..Default::default()
        }
    }
}

#[async_trait]
impl StreamProvider for ChanneledProvider {
    fn network(&self) -> &'static str {
        "synthetic"
    }
    async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        Ok(Box::new(ChanneledSource {
            rx: self.0.lock().unwrap().take().expect("single source open"),
            gap: false,
        }))
    }
}

#[async_trait]
impl TsSource for GeneratedSource {
    async fn next(&mut self) -> Option<Bytes> {
        self.data.take()
    }
    fn stats(&self) -> SourceStats {
        SourceStats::default()
    }
    fn metadata(&self) -> ace_swarm::types::StreamMetadata {
        self.metadata.clone()
    }
}

#[async_trait]
impl StreamProvider for GeneratedProvider {
    fn network(&self) -> &'static str {
        "synthetic"
    }
    async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        Ok(Box::new(GeneratedSource {
            data: Some(self.data.clone()),
            metadata: ace_swarm::types::StreamMetadata {
                title: Some("Synthetic HLS Acceptance".into()),
                ..Default::default()
            },
        }))
    }
}

fn opted_in() -> bool {
    match std::env::var("OUTPACE_TEST_HLS_AUDIO_FFMPEG").as_deref() {
        Ok("1") => true,
        _ => {
            eprintln!("SKIP hls_audio_ffmpeg: set OUTPACE_TEST_HLS_AUDIO_FFMPEG=1 to enable synthetic FFmpeg acceptance");
            false
        }
    }
}

fn require_tools() -> Option<SyntheticTools> {
    if !opted_in() {
        return None;
    }
    Some(
        support::require_synthetic_tools()
            .unwrap_or_else(|e| panic!("required synthetic tool unavailable: {e}")),
    )
}

fn run_ffprobe(tools: &SyntheticTools, bytes: &[u8], args: &[&str]) -> std::process::Output {
    use std::io::Write;
    let mut child = std::process::Command::new(&tools.ffprobe)
        .args(["-v", "error", "-f", "mpegts"])
        .args(args)
        .args(["-i", "pipe:0"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("start ffprobe");
    let mut stdin = child.stdin.take().unwrap();
    let input = bytes.to_vec();
    let writer = std::thread::spawn(move || stdin.write_all(&input));
    let output = child.wait_with_output().expect("wait for ffprobe");
    let _ = writer.join().expect("join ffprobe input writer");
    output
}

fn run_ffmpeg(tools: &SyntheticTools, bytes: &[u8], args: &[&str]) -> std::process::Output {
    use std::io::Write;
    let mut child = std::process::Command::new(&tools.ffmpeg)
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "mpegts",
            "-i",
            "pipe:0",
        ])
        .args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("start ffmpeg");
    let mut stdin = child.stdin.take().unwrap();
    let input = bytes.to_vec();
    let writer = std::thread::spawn(move || stdin.write_all(&input));
    let output = child.wait_with_output().expect("wait for ffmpeg");
    let _ = writer.join().expect("join ffmpeg input writer");
    output
}

fn packet_times(tools: &SyntheticTools, data: &[u8], selector: &str) -> Vec<f64> {
    let output = run_ffprobe(
        tools,
        data,
        &[
            "-select_streams",
            selector,
            "-show_packets",
            "-show_entries",
            "packet=pts_time",
            "-of",
            "csv=p=0",
        ],
    );
    assert!(
        output.status.success(),
        "timestamp probe failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split(',').next()?.trim().parse().ok())
        .collect()
}

fn video_keyframe_offsets(tools: &SyntheticTools, data: &[u8]) -> Vec<usize> {
    let output = run_ffprobe(
        tools,
        data,
        &[
            "-select_streams",
            "v:0",
            "-show_packets",
            "-show_entries",
            "packet=pos,flags",
            "-of",
            "json",
        ],
    );
    assert!(
        output.status.success(),
        "video access-point probe failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let packets: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let keyframes: Vec<usize> = packets["packets"]
        .as_array()
        .expect("video packets")
        .iter()
        .filter(|packet| {
            packet["flags"]
                .as_str()
                .is_some_and(|flags| flags.contains('K'))
        })
        .map(|packet| {
            packet["pos"]
                .as_str()
                .expect("packet byte position")
                .parse()
                .expect("numeric packet byte position")
        })
        .collect();
    keyframes
}

fn second_video_keyframe_offset(tools: &SyntheticTools, data: &[u8]) -> usize {
    let keyframes = video_keyframe_offsets(tools, data);
    let offset = *keyframes.get(1).expect("second video access point");
    assert_eq!(offset % 188, 0, "video access point is TS packet aligned");
    offset
}

fn pcr_ticks(data: &[u8], pid: u16) -> Vec<u64> {
    data.as_chunks::<188>()
        .0
        .iter()
        .filter_map(|packet| {
            if packet[0] != 0x47
                || ((u16::from(packet[1] & 0x1f) << 8) | u16::from(packet[2])) != pid
                || packet[3] & 0x20 == 0
                || packet[5] & 0x10 == 0
                || packet[4] < 7
            {
                return None;
            }
            let base = (u64::from(packet[6]) << 25)
                | (u64::from(packet[7]) << 17)
                | (u64::from(packet[8]) << 9)
                | (u64::from(packet[9]) << 1)
                | u64::from(packet[10] >> 7);
            Some(base)
        })
        .collect()
}

fn pat_identity(data: &[u8]) -> (u16, u16) {
    let packet = data
        .as_chunks::<188>()
        .0
        .iter()
        .find(|p| {
            p[0] == 0x47
                && p[1] & 0x40 != 0
                && ((u16::from(p[1] & 0x1f) << 8) | u16::from(p[2])) == 0
        })
        .expect("PAT packet");
    let payload = if packet[3] & 0x20 != 0 {
        5 + usize::from(packet[4])
    } else {
        4
    };
    let section = payload + 1 + usize::from(packet[payload]);
    (
        u16::from_be_bytes([packet[section + 3], packet[section + 4]]),
        u16::from_be_bytes([packet[section + 8], packet[section + 9]]),
    )
}

fn sdt_service_count(packet: &[u8]) -> usize {
    assert_eq!(ace_media::mpegts::ts_pid(packet), 17);
    let payload = if packet[3] & 0x20 != 0 {
        5 + usize::from(packet[4])
    } else {
        4
    };
    let section = payload + 1 + usize::from(packet[payload]);
    let section_end = section
        + 3
        + (((usize::from(packet[section + 1]) & 0x0f) << 8) | usize::from(packet[section + 2]))
        - 4;
    let mut at = section + 11;
    let mut count = 0;
    while at + 5 <= section_end {
        let descriptors = ((usize::from(packet[at + 3] & 0x0f)) << 8) | usize::from(packet[at + 4]);
        at += 5 + descriptors;
        count += 1;
    }
    assert_eq!(at, section_end, "SDT service loop is fully parsed");
    count
}

fn sdt_transport_id(packet: &[u8]) -> u16 {
    assert_eq!(ace_media::mpegts::ts_pid(packet), 17);
    let payload = if packet[3] & 0x20 != 0 {
        5 + usize::from(packet[4])
    } else {
        4
    };
    let section = payload + 1 + usize::from(packet[payload]);
    u16::from_be_bytes([packet[section + 3], packet[section + 4]])
}

async fn run_production_transform(tools: &SyntheticTools, input: &[u8]) -> Vec<u8> {
    let mut selection = ace_media::audio::LiveAudioSelection::default();
    for packet in input.as_chunks::<188>().0 {
        selection.observe(*packet);
    }
    let ace_media::audio::LayoutDecision::Supported(layout) = selection.layout() else {
        panic!("generated transport has no supported audio layout")
    };
    let prepared = ace_engine::hls_audio::PreparedNormalizer {
        executable: tools.ffmpeg.clone(),
    };
    let mut child = ace_engine::hls_audio::process::build_command(&prepared, &layout)
        .spawn()
        .expect("spawn production FFmpeg command");
    let mut stdin = child.stdin.take().unwrap();
    let payload = input.to_vec();
    let writer = std::thread::spawn(move || {
        futures::executor::block_on(async move {
            use tokio::io::AsyncWriteExt;
            stdin.write_all(&payload).await
        })
    });
    let output = child
        .wait_with_output()
        .await
        .expect("wait production FFmpeg");
    writer.join().unwrap().expect("write full generated input");
    assert!(
        output.status.success(),
        "production transform: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn manager_for(case: &SyntheticCase, ffmpeg: &std::path::Path) -> Arc<StreamManager> {
    let mut providers = ProviderRegistry::new();
    providers.register(Arc::new(GeneratedProvider {
        data: Bytes::copy_from_slice(&case.transport),
    }));
    let hls = HlsConfig {
        segment_packets: 65_536,
        window_segments: 16,
        segment_duration_ms: 1000,
        startup_segments: 1,
        startup_timeout_ms: 15_000,
    };
    StreamManager::with_normalizer(
        providers,
        32,
        hls,
        Some(ace_engine::hls_audio::PreparedNormalizer {
            executable: ffmpeg.to_path_buf(),
        }),
    )
}

async fn wait_ready(media: &ace_engine::hls::HlsPackager) {
    let status = tokio::time::timeout(
        Duration::from_secs(15),
        media.wait_available(Duration::from_secs(15)),
    )
    .await
    .expect("generated HLS source readiness deadline");
    assert_eq!(
        status,
        ace_engine::hls::HlsStatus::Ready,
        "startup error status: {status:?}"
    );
}

fn synthetic_app(manager: Arc<StreamManager>) -> axum::Router {
    router(AppState {
        manager,
        networks: vec!["synthetic".into()],
        resolve_content_ids_in_getstream: false,
        ace_sessions: Arc::new(AceSessionStore::default()),
        experimental_ace_compat: true,
        broadcasts: None,
    })
}

async fn route_body(app: &axum::Router, path: &str) -> (axum::http::StatusCode, Vec<u8>) {
    let response = app
        .clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 1 << 24)
        .await
        .unwrap()
        .to_vec();
    (status, body)
}

#[tokio::test]
async fn raw_session_broadcast_is_byte_identical_with_normalization_enabled_or_disabled() {
    let payload = Bytes::from(support::transport_prefix());
    for enabled in [false, true] {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut providers = ProviderRegistry::new();
        providers.register(Arc::new(ChanneledProvider(std::sync::Mutex::new(Some(rx)))));
        let manager = StreamManager::with_normalizer(
            providers,
            4,
            HlsConfig::default(),
            enabled.then(|| ace_engine::hls_audio::PreparedNormalizer {
                executable: "/definitely/missing/ffmpeg".into(),
            }),
        );
        let session = manager
            .get_or_start("synthetic", "raw-control")
            .await
            .unwrap();
        let mut receiver = session.raw_receiver();
        // Subscribe first so the subsequent channel feed is observed by this original session
        // broadcast receiver rather than being lost before a receiver exists.
        tx.send((false, payload.clone())).unwrap();
        let event = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            event,
            ace_engine::session::StreamEvent::Data(payload.clone()),
            "raw broadcast bytes with normalization enabled={enabled}"
        );
        manager.shutdown().await;
    }
}

#[tokio::test]
async fn vod_http_contract_is_identical_with_normalization_enabled_or_disabled() {
    let mut data = Vec::new();
    for _ in 0..32 {
        data.extend_from_slice(&support::transport_prefix());
    }
    let data = Bytes::from(data);
    let mut responses = Vec::new();
    for enabled in [false, true] {
        let mut providers = ProviderRegistry::new();
        providers.register(Arc::new(MemoryVodProvider(data.clone())));
        let manager = StreamManager::with_normalizer(
            providers,
            4,
            HlsConfig::default(),
            enabled.then(|| ace_engine::hls_audio::PreparedNormalizer {
                executable: "/definitely/missing/ffmpeg".into(),
            }),
        );
        let app = router(AppState {
            manager: manager.clone(),
            networks: vec!["synthetic".into()],
            resolve_content_ids_in_getstream: false,
            ace_sessions: Arc::new(AceSessionStore::default()),
            experimental_ace_compat: true,
            broadcasts: None,
        });
        let mut observed = Vec::new();
        for (path, range) in [
            ("/vod/synthetic/vod", None),
            ("/vod/synthetic/vod", Some("bytes=37-311")),
            ("/vod/synthetic/vod/manifest.m3u8", None),
            ("/vod/synthetic/vod/seg/0.ts", None),
        ] {
            let mut request = Request::get(path);
            if let Some(range) = range {
                request = request.header(axum::http::header::RANGE, range);
            }
            let response = app
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            let status = response.status();
            let headers = response.headers().clone();
            let body = to_bytes(response.into_body(), 1 << 20)
                .await
                .unwrap()
                .to_vec();
            observed.push((status, headers, body));
        }
        manager.shutdown().await;
        responses.push(observed);
    }
    assert_eq!(
        responses[0], responses[1],
        "VOD status, headers, ranges, playlist/layout and bytes stay unchanged"
    );
    assert_eq!(responses[1][0].0, axum::http::StatusCode::OK);
    assert_eq!(responses[1][1].0, axum::http::StatusCode::PARTIAL_CONTENT);
    assert_eq!(responses[1][1].2, data.slice(37..312).to_vec());
    assert_eq!(responses[1][2].0, axum::http::StatusCode::OK);
    assert_eq!(responses[1][3].0, axum::http::StatusCode::OK);
}

fn audio_payload_packet(pid: u16, start: bool, cc: u8, payload: &[u8]) -> [u8; 188] {
    assert!(payload.len() <= 182);
    let mut packet = [0xff; 188];
    packet[..4].copy_from_slice(&[
        0x47,
        (if start { 0x40 } else { 0 }) | ((pid >> 8) as u8 & 0x1f),
        pid as u8,
        0x30 | (cc & 0x0f),
    ]);
    let adaptation = 183 - payload.len();
    packet[4] = adaptation as u8;
    if adaptation > 0 {
        packet[5] = 0;
    }
    let offset = 5 + adaptation;
    packet[offset..].copy_from_slice(payload);
    packet
}

fn synthetic_audio_prefix() -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&support::psi_packet(
        0,
        0,
        &support::pat_section(7, 9, 4096),
    ));
    bytes.extend_from_slice(&support::psi_packet(
        4096,
        2,
        &support::pmt_section(9, 256, 256, 0x1b, 257, 0x03),
    ));
    bytes.extend_from_slice(&support::video_access_packet(256));
    bytes
}

#[tokio::test]
async fn worker_waits_for_complete_split_audio_evidence_then_attempts_child() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut providers = ProviderRegistry::new();
    providers.register(Arc::new(ChanneledProvider(std::sync::Mutex::new(Some(rx)))));
    let manager = StreamManager::with_normalizer(
        providers,
        4,
        HlsConfig {
            startup_timeout_ms: 5000,
            ..HlsConfig::default()
        },
        Some(ace_engine::hls_audio::PreparedNormalizer {
            executable: "/definitely/missing/ffmpeg".into(),
        }),
    );
    let media = manager
        .get_or_start_hls("synthetic", "split-mpeg")
        .await
        .unwrap();
    let mut prefix = synthetic_audio_prefix();
    let first = [0, 0, 1, 0xc0, 0, 0, 0x80, 0, 0, 0xff, 0xfd];
    prefix.extend_from_slice(&audio_payload_packet(257, true, 0, &first));
    tx.send((false, Bytes::from(prefix))).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        media.status(),
        ace_engine::hls::HlsStatus::Starting,
        "an incomplete split MPEG header must not launch the child"
    );

    // MPEG-1 Layer II, 128 kb/s, 44.1 kHz: each frame is 417 bytes. Complete the
    // split first header and provide exactly one following frame header.
    let mut rest = vec![0x80, 0x00];
    rest.extend(std::iter::repeat_n(0x55, 413));
    rest.extend_from_slice(&[0xff, 0xfd, 0x80, 0x00]);
    rest.extend(std::iter::repeat_n(0x55, 413));
    let mut packets = Vec::new();
    for (i, chunk) in rest.chunks(182).enumerate() {
        packets.extend_from_slice(&audio_payload_packet(257, false, (i + 1) as u8, chunk));
    }
    packets.extend_from_slice(&support::video_access_packet(256));
    let mut probe = ace_engine::hls_audio::MpegAudioProbe::new(257);
    assert!(!probe
        .observe(&audio_payload_packet(257, true, 0, &first))
        .unwrap());
    let mut probe_confirmed = false;
    for packet in packets.as_chunks::<188>().0 {
        probe_confirmed |= probe.observe(packet).unwrap();
    }
    assert!(
        probe_confirmed,
        "synthetic split frame fixture contains two valid consecutive headers"
    );
    tx.send((false, Bytes::from(packets))).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while media.status() == ace_engine::hls::HlsStatus::Starting {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("complete valid evidence advances to child attempt");
    assert_eq!(
        media.status(),
        ace_engine::hls::HlsStatus::Failed(ace_engine::hls::HlsFailure::Pipe),
        "Pipe is the run_child spawn failure from the deliberately missing executable"
    );
    manager.shutdown().await;
}

#[tokio::test]
async fn worker_incomplete_pes_expires_its_startup_deadline_without_child_attempt() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut providers = ProviderRegistry::new();
    providers.register(Arc::new(ChanneledProvider(std::sync::Mutex::new(Some(rx)))));
    let manager = StreamManager::with_normalizer(
        providers,
        4,
        HlsConfig {
            startup_timeout_ms: 150,
            ..HlsConfig::default()
        },
        Some(ace_engine::hls_audio::PreparedNormalizer {
            executable: "/definitely/missing/ffmpeg".into(),
        }),
    );
    let media = manager
        .get_or_start_hls("synthetic", "incomplete-pes")
        .await
        .unwrap();
    let mut source = synthetic_audio_prefix();
    // A PES start whose fixed header is truncated. The source remains open so only the
    // configured absolute startup deadline can terminate this still-incomplete evidence.
    source.extend_from_slice(&audio_payload_packet(257, true, 0, &[0, 0, 1, 0xc0]));
    tx.send((false, Bytes::from(source))).unwrap();
    let status = tokio::time::timeout(
        Duration::from_secs(2),
        media.wait_available(Duration::from_secs(2)),
    )
    .await
    .expect("incomplete PES startup deadline is bounded");
    assert_eq!(
        status,
        ace_engine::hls::HlsStatus::Failed(ace_engine::hls::HlsFailure::Deadline),
        "incomplete PES evidence expires before the missing child executable can be consulted"
    );
    manager.shutdown().await;
}

#[tokio::test]
async fn worker_rejects_invalid_reserved_and_layer_one_headers_before_child_launch() {
    for (name, header) in [
        ("invalid", [0x00, 0x00, 0x00, 0x00]),
        ("reserved", [0xff, 0xfd, 0xf0, 0x00]),
        ("layer-one", [0xff, 0xff, 0x80, 0x00]),
    ] {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut providers = ProviderRegistry::new();
        providers.register(Arc::new(ChanneledProvider(std::sync::Mutex::new(Some(rx)))));
        let manager = StreamManager::with_normalizer(
            providers,
            4,
            HlsConfig::default(),
            Some(ace_engine::hls_audio::PreparedNormalizer {
                executable: "/definitely/missing/ffmpeg".into(),
            }),
        );
        let media = manager.get_or_start_hls("synthetic", name).await.unwrap();
        let mut source = synthetic_audio_prefix();
        let mut pes = vec![0, 0, 1, 0xc0, 0, 0, 0x80, 0, 0];
        pes.extend_from_slice(&header);
        source.extend_from_slice(&audio_payload_packet(257, true, 0, &pes));
        tx.send((false, Bytes::from(source))).unwrap();
        let status = tokio::time::timeout(
            Duration::from_secs(2),
            media.wait_available(Duration::from_secs(2)),
        )
        .await
        .expect("invalid header is rejected promptly");
        assert_eq!(
            status,
            ace_engine::hls::HlsStatus::Failed(ace_engine::hls::HlsFailure::Unsupported),
            "{name} header is terminal before the nonexistent child path is consulted"
        );
        manager.shutdown().await;
    }
}

fn first_playlist_uri(playlist: &[u8]) -> String {
    std::str::from_utf8(playlist)
        .unwrap()
        .lines()
        .find(|line| !line.starts_with('#') && !line.is_empty())
        .expect("playlist contains segment URI")
        .to_owned()
}

fn playlist_sequence_and_count(playlist: &str) -> (u64, usize) {
    let seq = playlist
        .lines()
        .find_map(|line| line.strip_prefix("#EXT-X-MEDIA-SEQUENCE:"))
        .expect("playlist media sequence")
        .parse()
        .unwrap();
    (seq, playlist.matches("#EXTINF:").count())
}

#[tokio::test]
async fn generated_ac3_and_mpeg_audio_are_normalized_with_copy_video_and_correct_psi() {
    let Some(tools) = require_tools() else { return };
    let mut acceptance_failures = Vec::new();
    for (name, codec) in [
        ("ac3", AudioCase::Ac3),
        ("mp2", AudioCase::MpegLayer2),
        ("mp3", AudioCase::MpegLayer3),
    ] {
        let case = support::generate_case(&tools, codec)
            .unwrap_or_else(|e| panic!("generate {name} case: {e}"));
        // Exercise the exact production command with the complete ten-second input and capture
        // stdout through EOF. The manager/HTTP checks below intentionally measure retained output.
        let complete = run_production_transform(&tools, &case.transport).await;
        let complete_video_times = packet_times(&tools, &complete, "v:0");
        let complete_audio_times = packet_times(&tools, &complete, "a:0");
        let span = |times: &[f64]| times.last().unwrap() - times[0];
        let source_video_times = packet_times(&tools, &case.transport, "v:0");
        let source_audio_times = packet_times(&tools, &case.transport, "a:0");
        let tolerance = 1024.0 / 48_000.0 + 1.0 / 25.0;
        assert!(
            (span(&complete_video_times) - span(&source_video_times)).abs() <= tolerance,
            "{name}: complete copied video span differs from source"
        );
        assert!(
            (span(&complete_audio_times) - span(&source_audio_times)).abs() <= tolerance,
            "{name}: complete normalized audio span differs from source"
        );
        assert!(
            (complete_video_times.last().unwrap() - complete_audio_times.last().unwrap()).abs()
                <= 0.100,
            "{name}: complete transform A/V end drift exceeds 100 ms"
        );
        eprintln!("METRIC {name}: source_video={:.3}s source_audio={:.3}s output_video={:.3}s output_audio={:.3}s end_drift={:.3}s tolerance={:.3}s",
            span(&source_video_times), span(&source_audio_times), span(&complete_video_times),
            span(&complete_audio_times),
            (complete_video_times.last().unwrap() - complete_audio_times.last().unwrap()).abs(), tolerance);
        let full_video = run_ffmpeg(
            &tools,
            &complete,
            &[
                "-map",
                "0:v:0",
                "-c:v",
                "copy",
                "-f",
                "mpeg2video",
                "pipe:1",
            ],
        );
        assert!(full_video.status.success());
        assert_eq!(
            full_video.stdout, case.video_elementary,
            "{name}: complete copied video is byte-identical"
        );
        let manager = manager_for(&case, &tools.ffmpeg);
        let app = synthetic_app(manager.clone());
        let native_path = format!("/streams/synthetic/{SYNTHETIC_STREAM_ID}.m3u8");
        let (native_status, native_playlist) = route_body(&app, &native_path).await;
        assert_eq!(
            native_status,
            axum::http::StatusCode::OK,
            "{name}: native HLS route"
        );
        let native_segment_path = first_playlist_uri(&native_playlist);
        let redirect = app
            .clone()
            .oneshot(
                Request::get(format!("/ace/manifest.m3u8?infohash={SYNTHETIC_STREAM_ID}"))
                    .header(axum::http::header::HOST, "localhost:6900")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            redirect.status(),
            axum::http::StatusCode::FOUND,
            "{name}: compatibility HLS start"
        );
        let compat_playlist_path = redirect.headers()[axum::http::header::LOCATION]
            .to_str()
            .unwrap()
            .strip_prefix("http://localhost:6900")
            .unwrap()
            .to_owned();
        let (compat_status, compat_playlist) = route_body(&app, &compat_playlist_path).await;
        assert_eq!(
            compat_status,
            axum::http::StatusCode::OK,
            "{name}: compatibility playlist route"
        );
        let compat_segment_path = first_playlist_uri(&compat_playlist);
        let (native_segment_status, native_segment) = route_body(&app, &native_segment_path).await;
        let (compat_segment_status, compat_segment) = route_body(&app, &compat_segment_path).await;
        assert_eq!(
            native_segment_status,
            axum::http::StatusCode::OK,
            "{name}: native segment route"
        );
        assert_eq!(
            compat_segment_status,
            axum::http::StatusCode::OK,
            "{name}: compatibility segment route"
        );
        assert_eq!(
            native_segment, compat_segment,
            "{name}: both routes expose identical retained segment bytes"
        );
        let media = manager
            .get_hls("synthetic", SYNTHETIC_STREAM_ID)
            .await
            .unwrap();
        wait_ready(&media).await;
        let first = media.segment(0).expect("normalized startup segment");
        assert_eq!(
            native_segment.as_slice(),
            first.as_ref(),
            "{name}: HTTP retained segment matches manager bytes"
        );
        assert!(
            first.len() >= 3 * 188,
            "{name}: segment carries fresh tables and access point"
        );
        assert_eq!(first[0], 0x47, "{name}: starts on TS boundary");
        assert_eq!(
            ace_media::mpegts::ts_pid(&first[..188]),
            0,
            "{name}: first packet is fresh PAT"
        );
        assert_eq!(
            ace_media::mpegts::ts_pid(&first[188..376]),
            4096,
            "{name}: second packet is fresh PMT"
        );
        let source_identity = pat_identity(&case.transport);
        let output_identity = pat_identity(&first);
        assert_eq!(
            output_identity, source_identity,
            "{name}: output PAT preserves source transport-stream and program identity"
        );

        // The full generated stream passes through the real manager/worker/FFmpeg path. Poll the
        // retained window until all ten one-second segments are present or the bounded deadline expires.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let mut segments = Vec::new();
        while tokio::time::Instant::now() < deadline {
            segments.clear();
            for seq in 0..16 {
                if let Some(bytes) = media.segment(seq) {
                    segments.push(bytes);
                }
            }
            if segments.len() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            segments.len() >= 2,
            "{name}: expected advancing retained output, got {}",
            segments.len()
        );
        let output: Vec<u8> = segments
            .into_iter()
            .flat_map(|segment| segment.to_vec())
            .collect();

        let probe = run_ffprobe(
            &tools,
            &output,
            &[
                "-show_streams",
                "-show_programs",
                "-show_format",
                "-of",
                "json",
            ],
        );
        assert!(
            probe.status.success(),
            "{name}: output ffprobe failed: {}",
            String::from_utf8_lossy(&probe.stderr)
        );
        let json: serde_json::Value = serde_json::from_slice(&probe.stdout).expect("ffprobe JSON");
        let streams = json["streams"].as_array().expect("stream list");
        assert_eq!(
            streams.len(),
            2,
            "{name}: exactly video plus normalized audio"
        );
        let video = streams.iter().find(|s| s["codec_type"] == "video").unwrap();
        let audio = streams.iter().find(|s| s["codec_type"] == "audio").unwrap();
        assert_eq!(
            video["codec_name"], "mpeg2video",
            "{name}: video stays copied"
        );
        assert_eq!(audio["codec_name"], "aac", "{name}: audio is AAC");
        assert_eq!(audio["profile"], "LC");
        assert_eq!(audio["sample_rate"], "48000");
        assert_eq!(audio["channels"], 2);
        assert_eq!(audio["id"], "0x101");
        assert_eq!(video["id"], "0x100");
        let programs = json["programs"].as_array().expect("output program");
        assert_eq!(programs.len(), 1, "{name}: one output program");
        assert_eq!(programs[0]["program_id"], 1);
        assert_eq!(programs[0]["pmt_pid"], 4096);
        assert_eq!(programs[0]["pcr_pid"], 256);
        let video_times = packet_times(&tools, &output, "v:0");
        let audio_times = packet_times(&tools, &output, "a:0");
        assert!(
            video_times.len() > 2,
            "{name}: retained video timestamps advance"
        );
        assert!(
            audio_times.len() > 2,
            "{name}: retained audio timestamps advance"
        );
        assert!(video_times.windows(2).any(|pair| pair[1] > pair[0]));
        assert!(audio_times.windows(2).any(|pair| pair[1] > pair[0]));
        let video_span = video_times.last().unwrap() - video_times[0];
        let audio_span = audio_times.last().unwrap() - audio_times[0];
        let _end_drift = (video_times.last().unwrap() - audio_times.last().unwrap()).abs();
        let pcr = pcr_ticks(&output, 256);
        assert!(pcr.len() > 20, "{name}: output PCR advances on PID 256");
        assert!(pcr.windows(2).any(|pair| pair[1] > pair[0]));
        let sdt_count = output
            .as_chunks::<188>()
            .0
            .iter()
            .filter(|packet| {
                packet[0] == 0x47
                    && ((u16::from(packet[1] & 0x1f) << 8) | u16::from(packet[2])) == 17
            })
            .count();
        for packet in output
            .as_chunks::<188>()
            .0
            .iter()
            .filter(|packet| ace_media::mpegts::ts_pid(*packet) == 17)
        {
            assert_eq!(
                sdt_transport_id(packet),
                source_identity.0,
                "{name}: SDT transport-stream identity matches source PAT"
            );
            assert_eq!(
                sdt_service_count(packet),
                1,
                "{name}: each SDT advertises exactly one service identity"
            );
        }
        let decoded = run_ffmpeg(
            &tools,
            &output,
            &[
                "-map",
                "0:a:0",
                "-f",
                "f32le",
                "-acodec",
                "pcm_f32le",
                "pipe:1",
            ],
        );
        assert!(
            decoded.status.success(),
            "{name}: normalized audio does not decode"
        );
        let samples: Vec<f32> = decoded
            .stdout
            .as_chunks::<4>()
            .0
            .iter()
            .map(|b| f32::from_le_bytes(*b))
            .collect();
        assert!(
            samples.iter().any(|sample| sample.abs() > 0.001),
            "{name}: decoded audio is nonzero"
        );
        let extracted = run_ffmpeg(
            &tools,
            &output,
            &[
                "-map",
                "0:v:0",
                "-c:v",
                "copy",
                "-f",
                "mpeg2video",
                "pipe:1",
            ],
        );
        assert!(
            extracted.status.success(),
            "{name}: cannot demux copied elementary video"
        );
        let source_start = case
            .video_elementary
            .windows(32)
            .position(|window| window == &extracted.stdout[..32.min(extracted.stdout.len())])
            .expect("normalized copied video begins within source elementary payload");
        assert_eq!(
            &extracted.stdout,
            &case.video_elementary[source_start..source_start + extracted.stdout.len()],
            "{name}: copied video elementary payload is byte-identical from its retained opening"
        );
        let mut failures = Vec::new();
        assert!(video_span > 0.0 && audio_span > 0.0);
        if sdt_count == 0 || programs[0]["tags"]["service_name"] != "Synthetic HLS Acceptance" {
            failures.push(format!(
                "metadata-derived service SDT missing (packets={sdt_count}, tag={})",
                programs[0]["tags"]["service_name"]
            ));
        }
        acceptance_failures.extend(
            failures
                .into_iter()
                .map(|failure| format!("{name}: {failure}")),
        );
        manager.shutdown().await;
    }
    assert!(
        acceptance_failures.is_empty(),
        "synthetic codec acceptance failures: {acceptance_failures:?}"
    );
}

#[tokio::test]
async fn native_aac_and_video_only_keep_source_output_without_normalizer_child() {
    let Some(tools) = require_tools() else { return };
    for (name, codec) in [
        ("aac-ac3", AudioCase::AacAndAc3),
        ("video-only", AudioCase::VideoOnly),
    ] {
        let case = support::generate_case(&tools, codec)
            .unwrap_or_else(|e| panic!("generate {name} case: {e}"));
        // Native layouts must not consult the child. A nonexistent path makes any accidental
        // process launch fail the stream instead of silently passing through.
        let manager = manager_for(&case, std::path::Path::new("/definitely/missing/ffmpeg"));
        let media = manager
            .get_or_start_hls("synthetic", name)
            .await
            .expect("start native synthetic stream");
        wait_ready(&media).await;
        let segment = media.segment(0).expect("native segment");
        assert!(segment
            .windows(188)
            .any(|packet| packet.len() == 188 && packet[0] == 0x47));
        let probe = run_ffprobe(&tools, &segment, &["-show_streams", "-of", "json"]);
        assert!(
            probe.status.success(),
            "{name}: ffprobe native output: {}",
            String::from_utf8_lossy(&probe.stderr)
        );
        let streams: serde_json::Value = serde_json::from_slice(&probe.stdout).unwrap();
        let streams = streams["streams"].as_array().unwrap();
        if matches!(codec, AudioCase::AacAndAc3) {
            assert_eq!(
                streams
                    .iter()
                    .filter(|s| s["codec_type"] == "audio")
                    .count(),
                1,
                "AAC+AC3 retains only its selected native AAC stream"
            );
            assert!(streams
                .iter()
                .any(|s| s["codec_type"] == "audio" && s["codec_name"] == "aac"));
        } else {
            assert!(
                !streams.iter().any(|s| s["codec_type"] == "audio"),
                "video-only native stream has no audio output"
            );
        }
        manager.shutdown().await;
    }
}

#[tokio::test]
async fn generated_recovery_switches_ac3_to_mp2_across_native_and_compat_routes() {
    let Some(tools) = require_tools() else { return };
    let ac3 = support::generate_case(&tools, AudioCase::Ac3).expect("generate AC3");
    let mp2 = support::generate_case(&tools, AudioCase::MpegLayer2).expect("generate MP2");
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut providers = ProviderRegistry::new();
    providers.register(Arc::new(ChanneledProvider(std::sync::Mutex::new(Some(rx)))));
    let manager = StreamManager::with_normalizer(
        providers,
        32,
        HlsConfig {
            segment_packets: 65_536,
            window_segments: 16,
            segment_duration_ms: 1000,
            startup_segments: 2,
            startup_timeout_ms: 15_000,
        },
        Some(ace_engine::hls_audio::PreparedNormalizer {
            executable: tools.ffmpeg.clone(),
        }),
    );
    let app = synthetic_app(manager.clone());
    let native = format!("/streams/synthetic/{SYNTHETIC_STREAM_ID}.m3u8");
    let compat_redirect = app
        .clone()
        .oneshot(
            Request::get(format!("/ace/manifest.m3u8?infohash={SYNTHETIC_STREAM_ID}"))
                .header(axum::http::header::HOST, "localhost:6900")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(compat_redirect.status(), axum::http::StatusCode::FOUND);
    let compat = compat_redirect.headers()[axum::http::header::LOCATION]
        .to_str()
        .unwrap()
        .strip_prefix("http://localhost:6900")
        .unwrap()
        .to_owned();
    tx.send((false, Bytes::from(ac3.transport.clone())))
        .unwrap();
    let (native_status, native_initial) = route_body(&app, &native).await;
    assert_eq!(native_status, axum::http::StatusCode::OK);
    let (compat_status, compat_initial) = route_body(&app, &compat).await;
    assert_eq!(compat_status, axum::http::StatusCode::OK);
    let media = manager
        .get_hls("synthetic", SYNTHETIC_STREAM_ID)
        .await
        .unwrap();
    wait_ready(&media).await;
    assert!(String::from_utf8(native_initial.clone())
        .unwrap()
        .contains("#EXTINF"));
    assert!(String::from_utf8(compat_initial)
        .unwrap()
        .contains("#EXTINF"));
    let initial = media.playlist_with_segment_prefix("/segments");
    assert!(initial.contains("#EXTINF"));

    // Keep the source open after a boundary so both route handlers see Recovering.
    let mut marker = [0xff; 188];
    marker[..6].copy_from_slice(&[0x47, 0x40, 0x11, 0x30, 1, 0x80]);
    tx.send((true, Bytes::copy_from_slice(&marker))).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while media.status() != ace_engine::hls::HlsStatus::Recovering {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("source boundary enters recovery");
    // Take the baseline only after the source boundary has been consumed. This fences the old
    // generation so a late segment cannot look like output from the replacement input.
    let boundary_playlist = media.playlist_with_segment_prefix("/segments");
    let (initial_seq, initial_count) = playlist_sequence_and_count(&boundary_playlist);
    let old = media.segment(initial_seq).expect("old segment retained");
    let old_path = first_playlist_uri(&native_initial);
    let (old_status, old_http) = route_body(&app, &old_path).await;
    assert_eq!(old_status, axum::http::StatusCode::OK);
    assert_eq!(old_http, old.as_ref());
    let native_app = app.clone();
    let native_path = native.clone();
    let native_waiter = tokio::spawn(async move { route_body(&native_app, &native_path).await });
    let compat_app = app.clone();
    let compat_path = compat.to_owned();
    let compat_waiter = tokio::spawn(async move { route_body(&compat_app, &compat_path).await });
    tokio::time::timeout(Duration::from_millis(100), async {
        while !native_waiter.is_finished() || !compat_waiter.is_finished() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect_err("both playlist routes remain pending during recovery");

    // Stop the first replacement stage immediately before its second video access point. Without
    // that next GOP boundary the packager cannot close a retained segment; the remainder then
    // allows the configured two-segment readiness threshold to be met.
    let packet_aligned_len = mp2.transport.len() / 188 * 188;
    let first_stage_len = second_video_keyframe_offset(&tools, &mp2.transport);
    assert!(first_stage_len < packet_aligned_len);
    assert_eq!(
        video_keyframe_offsets(&tools, &mp2.transport[..first_stage_len]).len(),
        1,
        "first-stage prefix contains only its opening video access point"
    );
    tx.send((
        false,
        Bytes::copy_from_slice(&mp2.transport[..first_stage_len]),
    ))
    .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(media.status(), ace_engine::hls::HlsStatus::Recovering);
    let before_second_stage =
        playlist_sequence_and_count(&media.playlist_with_segment_prefix("/segments")).1;
    assert_eq!(
        before_second_stage, initial_count,
        "partial replacement input creates no retained segment"
    );
    assert!(
        !native_waiter.is_finished() && !compat_waiter.is_finished(),
        "playlist requests stay pending until replacement readiness"
    );
    tx.send((
        false,
        Bytes::copy_from_slice(&mp2.transport[first_stage_len..packet_aligned_len]),
    ))
    .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let current = media.playlist_with_segment_prefix("/segments");
            if media.status() == ace_engine::hls::HlsStatus::Ready
                && playlist_sequence_and_count(&current).1 >= initial_count + 2
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("new generation reaches configured two-segment readiness");
    for waiter in [native_waiter, compat_waiter] {
        let (status, body) = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("playlist waiter completes on recovery")
            .expect("playlist request task joins");
        assert_eq!(status, axum::http::StatusCode::OK);
        assert!(std::str::from_utf8(&body)
            .unwrap()
            .contains("#EXT-X-DISCONTINUITY"));
    }
    assert!(
        playlist_sequence_and_count(&media.playlist_with_segment_prefix("/segments")).1
            > initial_count,
        "sequence advances"
    );
    assert_eq!(
        media.segment(initial_seq).as_deref(),
        Some(old.as_ref()),
        "retained old bytes remain unchanged"
    );
    let playlist = media.playlist_with_segment_prefix("/segments");
    assert_eq!(
        playlist.matches("#EXT-X-DISCONTINUITY\n").count()
            - initial.matches("#EXT-X-DISCONTINUITY\n").count(),
        1,
        "recovery adds exactly one playlist boundary"
    );
    let first_new = media
        .segment(initial_seq + initial_count as u64)
        .expect("first replacement segment");
    assert_eq!(
        ace_media::mpegts::ts_pid(&first_new[..188]),
        0,
        "fresh PAT opens replacement segment"
    );
    assert_eq!(
        ace_media::mpegts::ts_pid(&first_new[188..376]),
        4096,
        "fresh PMT follows PAT"
    );
    let sdt_packets = first_new
        .as_chunks::<188>()
        .0
        .iter()
        .filter(|packet| ace_media::mpegts::ts_pid(*packet) == 17)
        .count();
    assert_eq!(sdt_packets, 1, "one synthesized service table per segment");
    assert_eq!(
        ace_media::mpegts::read_sdt_service_name(&first_new).as_deref(),
        Some("Synthetic HLS Acceptance"),
        "replacement service table retains the session identity"
    );
    assert!(
        first_new.windows(188).any(|packet| packet[0] == 0x47),
        "replacement segment contains transport packets"
    );
    for path in [&native, compat.as_str()] {
        let (status, body) = route_body(&app, path).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        assert!(std::str::from_utf8(&body)
            .unwrap()
            .contains("#EXT-X-DISCONTINUITY"));
    }
    manager.shutdown().await;
}

#[tokio::test]
async fn selected_layer_one_fails_before_child_launch() {
    let mut source = Vec::new();
    source.extend_from_slice(&support::psi_packet(
        0,
        0,
        &support::pat_section(7, 9, 4096),
    ));
    source.extend_from_slice(&support::psi_packet(
        4096,
        2,
        &support::pmt_section(9, 256, 256, 0x1b, 257, 0x03),
    ));
    let layer_one = (0x7ff_u32 << 21) | (3 << 19) | (3 << 17) | (1 << 16) | (8 << 12);
    let pes = [
        0,
        0,
        1,
        0xc0,
        0,
        0,
        0x80,
        0,
        0,
        layer_one.to_be_bytes()[0],
        layer_one.to_be_bytes()[1],
        layer_one.to_be_bytes()[2],
        layer_one.to_be_bytes()[3],
    ];
    let mut packet = [0xff; 188];
    packet[..4].copy_from_slice(&[0x47, 0x41, 0x01, 0x30]);
    packet[4] = (183 - pes.len()) as u8;
    packet[5] = 0;
    let offset = 5 + usize::from(packet[4]);
    packet[offset..offset + pes.len()].copy_from_slice(&pes);
    source.extend_from_slice(&packet);
    source.extend_from_slice(&support::video_access_packet(256));
    let case = SyntheticCase {
        transport: source,
        video_elementary: Vec::new(),
        duration_seconds: 0.0,
    };
    let manager = manager_for(&case, std::path::Path::new("/definitely/missing/ffmpeg"));
    let media = manager
        .get_or_start_hls("synthetic", "layer-one")
        .await
        .expect("open synthetic source");
    let status = tokio::time::timeout(
        Duration::from_secs(2),
        media.wait_available(Duration::from_secs(2)),
    )
    .await
    .expect("Layer-I rejection is terminal and prompt");
    assert_eq!(
        status,
        ace_engine::hls::HlsStatus::Failed(ace_engine::hls::HlsFailure::Unsupported),
        "Unsupported proves classification rejected Layer I before an attempted child launch"
    );
    manager.shutdown().await;
}
