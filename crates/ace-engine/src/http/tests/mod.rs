use super::*;
use crate::broadcast::{CHUNK_LENGTH, PIECE_LENGTH};
use crate::provider::{
    ProviderError, ProviderRegistry, SourceStats, StreamProvider, TsSource, VodContent,
};
use crate::testprovider::TestProvider;
use ace_swarm::types::StreamMetadata;
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use std::sync::atomic::Ordering;
use tower::ServiceExt;

mod broadcast;
mod compat;
mod compat_hls;
mod native;
mod server_api;
mod vod;

fn state() -> AppState {
    let mut r = ProviderRegistry::new();
    r.register(std::sync::Arc::new(TestProvider { chunks: 10 }));
    AppState {
        manager: StreamManager::new(r),
        networks: vec!["test".into()],
        resolve_content_ids_in_getstream: false,
        ace_sessions: Arc::new(AceSessionStore::default()),
        experimental_ace_compat: false,
        broadcasts: None,
    }
}

/// A real libx264 MPEG-TS (committed fixture). Video PID 0x100; keyframes at byte
/// offsets 564 and 9400.
const FIXTURE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/vectors/media/h264-keyframes.ts"
));

/// Provider that replays `FIXTURE` from byte `skip` (188-aligned), a few TS packets per
/// chunk — used to prove the per-client keyframe gate on a genuine stream.
struct FixtureProvider {
    skip: usize,
}
struct FixtureSource {
    pos: usize,
}
struct PacedFixtureProvider;
struct PacedFixtureSource {
    pos: usize,
}
fn fixture_metadata() -> StreamMetadata {
    StreamMetadata {
        title: Some("Synthetic Demo Channel".to_string()),
        bitrate: Some(100_000),
        categories: vec!["sports".to_string()],
    }
}
#[async_trait]
impl TsSource for FixtureSource {
    async fn next(&mut self) -> Option<Bytes> {
        if self.pos >= FIXTURE.len() {
            std::future::pending().await
        }
        tokio::task::yield_now().await;
        let end = (self.pos + 188 * 3).min(FIXTURE.len());
        let chunk = Bytes::copy_from_slice(&FIXTURE[self.pos..end]);
        self.pos = end;
        Some(chunk)
    }
    fn stats(&self) -> SourceStats {
        SourceStats {
            peers: 1,
            bitrate: 0,
            buffer_ms: 0,
            downloaded: self.pos as u64,
            uploaded: 0,
            peers_served: 0,
            ..SourceStats::default()
        }
    }
    fn metadata(&self) -> StreamMetadata {
        fixture_metadata()
    }
}
#[async_trait]
impl StreamProvider for FixtureProvider {
    fn network(&self) -> &'static str {
        "fix"
    }
    async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        Ok(Box::new(FixtureSource { pos: self.skip }))
    }
}
#[async_trait]
impl TsSource for PacedFixtureSource {
    async fn next(&mut self) -> Option<Bytes> {
        tokio::time::sleep(Duration::from_millis(2)).await;
        if self.pos >= FIXTURE.len() {
            self.pos = 0;
        }
        let end = (self.pos + 188 * 8).min(FIXTURE.len());
        let chunk = Bytes::copy_from_slice(&FIXTURE[self.pos..end]);
        self.pos = end;
        Some(chunk)
    }
    fn stats(&self) -> SourceStats {
        SourceStats::default()
    }
    fn metadata(&self) -> StreamMetadata {
        fixture_metadata()
    }
}
#[async_trait]
impl StreamProvider for PacedFixtureProvider {
    fn network(&self) -> &'static str {
        "fix"
    }
    async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        Ok(Box::new(PacedFixtureSource { pos: 0 }))
    }
}

fn fixture_state(skip: usize) -> AppState {
    let mut r = ProviderRegistry::new();
    r.register(std::sync::Arc::new(FixtureProvider { skip }));
    AppState {
        manager: StreamManager::with_config(
            r,
            256,
            crate::config::HlsConfig {
                window_segments: 6,
                segment_duration_ms: 1000,
                startup_segments: 1,
                ..crate::config::HlsConfig::default()
            },
        ),
        networks: vec!["fix".into()],
        resolve_content_ids_in_getstream: false,
        ace_sessions: Arc::new(AceSessionStore::default()),
        experimental_ace_compat: false,
        broadcasts: None,
    }
}

fn ace_compat_state(skip: usize) -> AppState {
    let mut st = fixture_state(skip);
    st.experimental_ace_compat = true;
    st
}

struct PacedPacketProvider;
struct PacedPacketSource {
    packet_index: u64,
}
struct StalledProvider;
struct StalledSource;

#[async_trait]
impl TsSource for StalledSource {
    async fn next(&mut self) -> Option<Bytes> {
        std::future::pending().await
    }

    fn stats(&self) -> SourceStats {
        SourceStats::default()
    }
}

#[async_trait]
impl StreamProvider for StalledProvider {
    fn network(&self) -> &'static str {
        "stalled"
    }

    async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        Ok(Box::new(StalledSource))
    }
}

fn paced_psi(pid: u16, section: &[u8]) -> Vec<u8> {
    let mut packet = vec![0xff; 188];
    packet[0] = 0x47;
    packet[1] = 0x40 | ((pid >> 8) as u8 & 0x1f);
    packet[2] = pid as u8;
    packet[3] = 0x10;
    packet[4] = 0;
    packet[5..5 + section.len()].copy_from_slice(section);
    packet
}

fn paced_pat(pmt_pid: u16) -> Vec<u8> {
    let mut section = vec![0x00, 0xb0, 13, 0x00, 0x01, 0xc1, 0, 0];
    section.extend_from_slice(&[
        0x00,
        0x01,
        0xe0 | ((pmt_pid >> 8) as u8 & 0x1f),
        pmt_pid as u8,
    ]);
    section.extend_from_slice(&[0; 4]);
    paced_psi(0, &section)
}

fn paced_pmt(pmt_pid: u16, video_pid: u16) -> Vec<u8> {
    let mut section = vec![0x02, 0xb0, 18, 0x00, 0x01, 0xc1, 0, 0];
    section.extend_from_slice(&[
        0xe0 | ((video_pid >> 8) as u8 & 0x1f),
        video_pid as u8,
        0xf0,
        0,
        0x1b,
        0xe0 | ((video_pid >> 8) as u8 & 0x1f),
        video_pid as u8,
        0xf0,
        0,
    ]);
    section.extend_from_slice(&[0; 4]);
    paced_psi(pmt_pid, &section)
}

fn paced_video_access(video_pid: u16, pcr: u64, marker: u8) -> Vec<u8> {
    let mut packet = vec![0xff; 188];
    packet[0] = 0x47;
    packet[1] = 0x40 | ((video_pid >> 8) as u8 & 0x1f);
    packet[2] = video_pid as u8;
    packet[3] = 0x30;
    packet[4] = 7;
    packet[5] = 0x50;
    packet[6] = (pcr >> 25) as u8;
    packet[7] = (pcr >> 17) as u8;
    packet[8] = (pcr >> 9) as u8;
    packet[9] = (pcr >> 1) as u8;
    packet[10] = ((pcr & 1) << 7) as u8 | 0x7e;
    packet[11] = 0;
    packet[12] = marker;
    packet
}

#[async_trait]
impl TsSource for PacedPacketSource {
    async fn next(&mut self) -> Option<Bytes> {
        tokio::time::sleep(Duration::from_millis(2)).await;
        const PMT_PID: u16 = 0x0100;
        const VIDEO_PID: u16 = 0x0101;
        let packet = match self.packet_index {
            0 => paced_pat(PMT_PID),
            1 => paced_pmt(PMT_PID, VIDEO_PID),
            index => paced_video_access(
                VIDEO_PID,
                (index - 2) * 108_000,
                index.wrapping_sub(2) as u8,
            ),
        };
        self.packet_index += 1;
        Some(Bytes::from(packet))
    }
    fn stats(&self) -> SourceStats {
        SourceStats::default()
    }
    fn metadata(&self) -> StreamMetadata {
        fixture_metadata()
    }
}
#[async_trait]
impl StreamProvider for PacedPacketProvider {
    fn network(&self) -> &'static str {
        "hlsfix"
    }
    async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        Ok(Box::new(PacedPacketSource { packet_index: 0 }))
    }
}

fn ace_hls_state(ttl: Duration, capacity: usize) -> AppState {
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(PacedPacketProvider));
    AppState {
        manager: StreamManager::with_config(
            registry,
            32,
            crate::config::HlsConfig {
                // 4, not 3: PacedPacketSource's fixture metadata carries a title (Task 5,
                // #135), so the injected per-segment table prefix is [PAT][PMT][SDT] (3
                // packets) rather than [PAT][PMT] (2) -- one more packet of headroom is
                // needed to still fit a keyframe alongside it.
                segment_packets: 4,
                window_segments: 3,
                segment_duration_ms: 1000,
                startup_segments: 1,
                ..crate::config::HlsConfig::default()
            },
        ),
        networks: vec!["hlsfix".into()],
        resolve_content_ids_in_getstream: false,
        ace_sessions: Arc::new(AceSessionStore::new(ttl, capacity)),
        experimental_ace_compat: true,
        broadcasts: None,
    }
}

fn stalled_hls_state_with(ttl: Duration, capacity: usize) -> AppState {
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(StalledProvider));
    AppState {
        manager: StreamManager::with_config(
            registry,
            32,
            crate::config::HlsConfig {
                segment_packets: 3,
                window_segments: 3,
                segment_duration_ms: 1000,
                startup_segments: 1,
                ..crate::config::HlsConfig::default()
            },
        ),
        networks: vec!["stalled".into()],
        resolve_content_ids_in_getstream: false,
        ace_sessions: Arc::new(AceSessionStore::new(ttl, capacity)),
        experimental_ace_compat: true,
        broadcasts: None,
    }
}

fn stalled_hls_state() -> AppState {
    stalled_hls_state_with(Duration::from_secs(60), 8)
}

async fn mint_stalled_hls(app: &Router, id: &str) -> (String, String, String) {
    let start = app
        .clone()
        .oneshot(
            Request::get(format!("/ace/manifest.m3u8?infohash={id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(start.status(), StatusCode::FOUND);
    let playback = start.headers()[header::LOCATION]
        .to_str()
        .unwrap()
        .strip_prefix("http://127.0.0.1:6878/ace/m/")
        .unwrap();
    let (playback_id, manifest) = playback.split_once('/').unwrap();
    let token = manifest.strip_suffix(".m3u8").unwrap();
    (
        playback_id.to_string(),
        manifest.to_string(),
        format!("/ace/cmd/{playback_id}/{token}?method=stop"),
    )
}

fn spawn_stalled_hls_request(
    state: AppState,
    playback_id: String,
    manifest: String,
) -> tokio::task::JoinHandle<Response> {
    tokio::spawn(async move {
        ace_hls_playback_with_timeout(
            State(state),
            Path((playback_id, manifest)),
            Duration::from_secs(5),
        )
        .await
    })
}

async fn response_bytes(response: Response) -> Bytes {
    axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap()
}

async fn response_text(response: Response) -> String {
    let body = response_bytes(response).await;
    String::from_utf8(body.to_vec()).unwrap()
}

async fn wait_for_hls_playlist(app: &Router, path: &str) -> String {
    for _ in 0..50 {
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let playlist = response_text(response).await;
        if playlist.contains(".ts") {
            return playlist;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("HLS packager did not produce a segment");
}

fn playlist_segment_path(playlist: &str) -> &str {
    playlist
        .lines()
        .find(|line| line.ends_with(".ts"))
        .expect("playlist segment")
}

async fn body_text(resp: Response) -> String {
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}
