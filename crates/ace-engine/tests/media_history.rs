//! Opt-in decoded-media qualification of the local history (timeshift) HTTP path.
//! Adapted from the open-ott-play/outpace-acestream timeshift qualification.
//!
//! Run: cargo test -p ace-engine --test media_history -- --ignored --nocapture
//! Requires ffmpeg and ffprobe on PATH (override with FFMPEG / FFPROBE).
//! All media is generated locally; the only network listener is ephemeral loopback.

use ace_engine::config::{HlsConfig, HlsHistoryConfig};
use ace_engine::http::{router, AceSessionStore, AppState};
use ace_engine::manager::StreamManager;
use ace_engine::provider::{
    ProviderError, ProviderRegistry, SourceStats, StreamProvider, TsSource,
};
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const ID: &str = "0123456789abcdef0123456789abcdef01234567";
const MAX_BODY: usize = 8 * 1024 * 1024;

struct TempMedia(PathBuf);
impl TempMedia {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("outpace-history-{}-{nonce}", std::process::id()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for TempMedia {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct AbortServer(tokio::task::AbortHandle);
impl Drop for AbortServer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct GeneratedProvider {
    bytes: Bytes,
    opens: Arc<AtomicUsize>,
}
struct GeneratedSource {
    bytes: Bytes,
    offset: usize,
}
#[async_trait]
impl TsSource for GeneratedSource {
    async fn next(&mut self) -> Option<Bytes> {
        if self.offset >= self.bytes.len() {
            return None;
        }
        // At the fixture's fixed mux rate this runs at about twice media speed.
        tokio::time::sleep(Duration::from_millis(10)).await;
        let end = (self.offset + 7 * 188).min(self.bytes.len());
        let chunk = self.bytes.slice(self.offset..end);
        self.offset = end;
        Some(chunk)
    }
    fn stats(&self) -> SourceStats {
        SourceStats::default()
    }
}
#[async_trait]
impl StreamProvider for GeneratedProvider {
    fn network(&self) -> &'static str {
        "history-fixture"
    }
    async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(GeneratedSource {
            bytes: self.bytes.clone(),
            offset: 0,
        }))
    }
}

async fn tool(program: &str, args: &[&str]) -> Vec<u8> {
    let mut command = tokio::process::Command::new(program);
    command.args(args).kill_on_drop(true);
    let result = tokio::time::timeout(Duration::from_secs(10), command.output())
        .await
        .expect("media subprocess exceeded ten seconds")
        .expect("media subprocess available");
    assert!(
        result.status.success(),
        "media subprocess failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(result.stdout.len() <= MAX_BODY && result.stderr.len() <= MAX_BODY);
    result.stdout
}

async fn get(address: std::net::SocketAddr, origin: &str, url: &str) -> (u16, Vec<u8>) {
    let path = if url.starts_with('/') {
        url
    } else {
        url.strip_prefix(origin)
            .expect("only loopback fixture URLs")
    };
    assert!(path.starts_with('/') && !path.contains(['\r', '\n']));
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
        socket
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut bytes = Vec::new();
        socket
            .take((MAX_BODY + 8193) as u64)
            .read_to_end(&mut bytes)
            .await
            .unwrap();
        assert!(bytes.len() <= MAX_BODY + 8192, "bounded fixture response");
        let split = bytes
            .windows(4)
            .position(|part| part == b"\r\n\r\n")
            .unwrap();
        let header = std::str::from_utf8(&bytes[..split]).unwrap();
        let status = header
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        // These concrete JSON/String/Bytes responses have exact Content-Length.
        assert!(!header.to_ascii_lowercase().contains("transfer-encoding:"));
        (status, bytes[split + 4..].to_vec())
    })
    .await
    .expect("loopback HTTP request exceeded three seconds")
}

async fn json_get(address: std::net::SocketAddr, origin: &str, url: &str) -> Value {
    let (status, bytes) = get(address, origin, url).await;
    assert_eq!(status, 200);
    serde_json::from_slice(&bytes).unwrap()
}

async fn media(address: std::net::SocketAddr, origin: &str, playlist: &str) -> (String, Vec<u8>) {
    let (status, bytes) = get(address, origin, playlist).await;
    assert_eq!(status, 200);
    let playlist = String::from_utf8(bytes).unwrap();
    let urls: Vec<_> = playlist
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect();
    assert!(!urls.is_empty() && urls.len() <= 64);
    let mut out = Vec::new();
    for url in urls {
        let (status, bytes) = get(address, origin, url).await;
        assert_eq!(
            status, 200,
            "advertised fixture segment must remain available"
        );
        out.extend(bytes);
        assert!(out.len() <= MAX_BODY);
    }
    (playlist, out)
}

async fn frame_pts(ffprobe: &str, path: &Path) -> Vec<f64> {
    let output = tool(
        ffprobe,
        &[
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "frame=best_effort_timestamp_time",
            "-of",
            "json",
            path.to_str().unwrap(),
        ],
    )
    .await;
    let value: Value = serde_json::from_slice(&output).unwrap();
    let pts: Vec<f64> = value["frames"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|frame| frame["best_effort_timestamp_time"].as_str())
        .map(|value| value.parse::<f64>().unwrap())
        .collect();
    assert!(pts.len() >= 5 && pts.iter().all(|value| value.is_finite()));
    assert!(
        pts.windows(2).all(|pair| pair[0] <= pair[1]),
        "decoded frame timestamps must advance"
    );
    pts
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires local ffmpeg and ffprobe; generates and decodes bounded synthetic media"]
async fn generated_media_rewinds_decodes_and_returns_live_without_moving_another_viewer() {
    tokio::time::timeout(Duration::from_secs(55), qualify())
        .await
        .expect("history media qualification exceeded 55 seconds");
}

async fn qualify() {
    let temp = TempMedia::new();
    let input = temp.0.join("generated.ts");
    let ffmpeg = std::env::var("FFMPEG").unwrap_or_else(|_| "ffmpeg".into());
    let ffprobe = std::env::var("FFPROBE").unwrap_or_else(|_| "ffprobe".into());
    tool(
        &ffmpeg,
        &[
            "-nostdin",
            "-v",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=160x96:rate=10",
            "-t",
            "60",
            "-an",
            "-c:v",
            "mpeg2video",
            "-g",
            "10",
            "-bf",
            "0",
            "-b:v",
            "300k",
            "-minrate",
            "300k",
            "-maxrate",
            "300k",
            "-bufsize",
            "600k",
            "-muxrate",
            "500k",
            "-f",
            "mpegts",
            input.to_str().unwrap(),
        ],
    )
    .await;
    let bytes = std::fs::read(&input).unwrap();
    assert!(bytes.len() <= MAX_BODY);
    let opens = Arc::new(AtomicUsize::new(0));
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(GeneratedProvider {
        bytes: Bytes::from(bytes),
        opens: opens.clone(),
    }));
    let manager = StreamManager::with_history_config(
        registry,
        256,
        HlsConfig {
            segment_packets: 1024,
            window_segments: 3,
            segment_duration_ms: 1000,
            startup_segments: 1,
            ..HlsConfig::default()
        },
        HlsHistoryConfig {
            max_seconds: 60,
            max_bytes: 8 * 1024 * 1024,
            global_max_bytes: 16 * 1024 * 1024,
        },
    );
    let app = router(AppState {
        peer_policy: Default::default(),
        manager: manager.clone(),
        networks: vec!["history-fixture".into()],
        resolve_content_ids_in_getstream: false,
        ace_sessions: Arc::new(AceSessionStore::default()),
        experimental_ace_compat: true,
        origin_hls: None,
        broadcasts: None,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let origin = format!("http://{address}");
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let _abort = AbortServer(server.abort_handle());
    let start = format!("/ace/manifest.m3u8?infohash={ID}&format=json");
    let a = json_get(address, &origin, &start).await;
    let b = json_get(address, &origin, &start).await;
    let a_command = a["response"]["command_url"].as_str().unwrap();
    let b_playback = b["response"]["playback_url"].as_str().unwrap();
    let pkg = manager.get_hls("history-fixture", ID).await.unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        while pkg.prepare_history(Duration::from_secs(14)).is_err() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("synthetic media history reaches fourteen seconds");
    let (b_before, b_bytes) = media(address, &origin, b_playback).await;
    let live_path = temp.0.join("live-before.ts");
    std::fs::write(&live_path, b_bytes).unwrap();
    let live_pts = frame_pts(&ffprobe, &live_path).await;
    let seek = json_get(
        address,
        &origin,
        &format!("{a_command}?method=timeshift_seek&back=12&generation=0"),
    )
    .await;
    assert!(seek["error"].is_null(), "{seek}");
    let replay_url = seek["response"]["playback_url"].as_str().unwrap();
    let (replay, replay_bytes) = media(address, &origin, replay_url).await;
    assert!(replay.contains("#EXT-X-ENDLIST"));
    let replay_path = temp.0.join("replay.ts");
    std::fs::write(&replay_path, replay_bytes).unwrap();
    let replay_pts = frame_pts(&ffprobe, &replay_path).await;
    assert!(
        replay_pts[0] + 3.0 < live_pts[0],
        "replay must decode media older than the live playlist"
    );
    assert!(
        replay_pts.last().unwrap() - replay_pts[0] >= 8.0,
        "replay must advance across historical media"
    );
    tool(
        &ffmpeg,
        &[
            "-nostdin",
            "-v",
            "error",
            "-xerror",
            "-i",
            replay_path.to_str().unwrap(),
            "-map",
            "0:v:0",
            "-f",
            "null",
            "-",
        ],
    )
    .await;
    let live = json_get(
        address,
        &origin,
        &format!("{a_command}?method=timeshift_live&generation=1"),
    )
    .await;
    assert!(live["error"].is_null(), "{live}");
    assert_eq!(live["response"]["view"]["generation"], 2);
    assert_eq!(get(address, &origin, replay_url).await.0, 410);
    let (resumed, resumed_bytes) = media(
        address,
        &origin,
        live["response"]["playback_url"].as_str().unwrap(),
    )
    .await;
    assert!(!resumed.contains("#EXT-X-ENDLIST"));
    let resumed_path = temp.0.join("resumed.ts");
    std::fs::write(&resumed_path, resumed_bytes).unwrap();
    let resumed_pts = frame_pts(&ffprobe, &resumed_path).await;
    assert!(resumed_pts[0] >= live_pts[0]);
    let (b_after, _) = media(address, &origin, b_playback).await;
    let sequence = |list: &str| -> u64 {
        list.lines()
            .find_map(|line| line.strip_prefix("#EXT-X-MEDIA-SEQUENCE:"))
            .unwrap()
            .parse()
            .unwrap()
    };
    assert!(sequence(&b_after) >= sequence(&b_before));
    assert!(!b_after.contains("#EXT-X-ENDLIST"));
    assert_eq!(opens.load(Ordering::SeqCst), 1);
    assert_eq!(
        manager
            .get("history-fixture", ID)
            .await
            .unwrap()
            .subscriber_count(),
        2
    );
    let stopped = json_get(address, &origin, &format!("{a_command}?method=stop")).await;
    assert!(stopped["error"].is_null());
    assert_eq!(get(address, &origin, b_playback).await.0, 200);
    assert_eq!(
        manager
            .get("history-fixture", ID)
            .await
            .unwrap()
            .subscriber_count(),
        1
    );
    eprintln!("history qualification: replay first PTS={:.3}, live first PTS={:.3}, resumed first PTS={:.3}; replay frames={}; shared acquisitions=1", replay_pts[0], live_pts[0], resumed_pts[0], replay_pts.len());
    manager.shutdown().await;
    server.abort();
    let _ = server.await;
}
