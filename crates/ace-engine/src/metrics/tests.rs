use super::*;
use crate::provider::{
    LiveRecoveryStats, ProviderError, ProviderRegistry, StreamProvider, TsSource,
};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use std::time::Duration;
use tower::ServiceExt;

/// Synthetic placeholder only; never a real identifier.
const PLACEHOLDER_ID: &str = "0123456789abcdef0123456789abcdef01234567";

fn stats(peers: u32, downloaded: u64, reconnects: u64) -> SourceStats {
    SourceStats {
        peers,
        download_rate: 1000,
        downloaded,
        recovery: Some(LiveRecoveryStats {
            upstream_reconnects: reconnects,
            active_upstreams: 2,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn snapshot(sessions: Vec<(String, u64, SourceStats)>) -> Snapshot {
    Snapshot {
        version: "9.9.9",
        uptime_seconds: 1.5,
        memory: vec![("resident", 4096)],
        networks: vec!["ace".into(), "idle".into()],
        sessions,
        admission: (2, Some(8)),
        http: BTreeMap::new(),
    }
}

/// Every sample line must be `name{labels} value` with a numeric value, and every metric
/// must be declared by `# TYPE` before its first sample.
fn assert_exposition(text: &str) {
    let mut typed = std::collections::HashSet::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let name = rest.split_whitespace().next().unwrap();
            assert!(typed.insert(name.to_owned()), "duplicate TYPE for {name}");
            continue;
        }
        if line.starts_with("# HELP ") {
            continue;
        }
        let (series, value) = line.rsplit_once(' ').expect("sample has a value");
        value
            .parse::<f64>()
            .unwrap_or_else(|_| panic!("bad value: {line}"));
        let name = series.split('{').next().unwrap();
        assert!(
            name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "bad metric name: {line}"
        );
        assert!(typed.contains(name), "sample before TYPE: {line}");
    }
}

#[test]
fn render_sums_sessions_per_network_and_zero_fills_idle_networks() {
    let text = render(&snapshot(vec![
        ("ace".into(), 2, stats(3, 100, 1)),
        ("ace".into(), 1, stats(4, 50, 2)),
    ]));
    assert_exposition(&text);
    for expected in [
        "outpace_build_info{version=\"9.9.9\"} 1",
        "outpace_allocator_bytes{kind=\"resident\"} 4096",
        "outpace_admission_occupied 2",
        "outpace_admission_limit 8",
        "outpace_sessions{network=\"ace\"} 2",
        "outpace_sessions{network=\"idle\"} 0",
        "outpace_subscribers{network=\"ace\"} 3",
        "outpace_peers{network=\"ace\"} 7",
        "outpace_active_upstreams{network=\"ace\"} 4",
        "outpace_download_bytes_per_second{network=\"ace\"} 2000",
        "outpace_session_downloaded_bytes{network=\"ace\"} 150",
        "outpace_session_upstream_reconnects{network=\"ace\"} 3",
    ] {
        assert!(text.contains(expected), "missing {expected:?} in:\n{text}");
    }
}

#[test]
fn render_omits_unlimited_admission_and_unavailable_allocator() {
    let mut s = snapshot(Vec::new());
    s.admission = (0, None);
    s.memory.clear();
    let text = render(&s);
    assert_exposition(&text);
    assert!(!text.contains("outpace_admission_limit"));
    assert!(!text.contains("outpace_allocator_bytes"));
}

#[test]
fn label_values_are_escaped() {
    assert_eq!(escape_label("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
}

struct PendingProvider;
struct PendingSource {
    sent: bool,
}

#[async_trait]
impl TsSource for PendingSource {
    async fn next(&mut self) -> Option<Bytes> {
        if self.sent {
            std::future::pending::<()>().await;
        }
        self.sent = true;
        let mut packet = vec![0u8; 188];
        packet[0] = 0x47;
        Some(Bytes::from(packet))
    }
    fn stats(&self) -> SourceStats {
        stats(5, 188, 0)
    }
}

#[async_trait]
impl StreamProvider for PendingProvider {
    fn network(&self) -> &'static str {
        "test"
    }
    async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        Ok(Box::new(PendingSource { sent: false }))
    }
}

async fn get(app: &Router, uri: &str) -> (StatusCode, String) {
    let response = app
        .clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .map(|v| v.to_str().unwrap().to_owned());
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    if uri == "/metrics" && status == StatusCode::OK {
        assert_eq!(content_type.as_deref(), Some(CONTENT_TYPE));
    }
    (status, String::from_utf8(body.to_vec()).unwrap())
}

fn app(manager: Arc<StreamManager>) -> Router {
    let state = crate::http::AppState {
        peer_policy: Default::default(),
        manager: manager.clone(),
        networks: vec!["test".into()],
        resolve_content_ids_in_getstream: false,
        ace_sessions: Arc::new(crate::http::AceSessionStore::default()),
        experimental_ace_compat: false,
        search: None,
        origin_hls: None,
        broadcasts: None,
    };
    instrument(crate::http::router(state), manager, &["test".into()])
}

#[tokio::test]
async fn metrics_route_is_absent_unless_instrumented() {
    let state = crate::http::AppState {
        peer_policy: Default::default(),
        manager: StreamManager::new(ProviderRegistry::new()),
        networks: Vec::new(),
        resolve_content_ids_in_getstream: false,
        ace_sessions: Arc::new(crate::http::AceSessionStore::default()),
        experimental_ace_compat: false,
        search: None,
        origin_hls: None,
        broadcasts: None,
    };
    let (status, _) = get(&crate::http::router(state), "/metrics").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn metrics_aggregate_live_sessions_without_leaking_stream_ids() {
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(PendingProvider));
    let manager = StreamManager::new(registry);
    let app = app(manager.clone());

    let session = manager.get_or_start("test", PLACEHOLDER_ID).await.unwrap();
    let _subscription = session.subscribe();
    // The pump publishes stats right after forwarding the first chunk.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while session.stats().await.peers != 5 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "stats never published"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let status_uri = format!("/streams/test/{PLACEHOLDER_ID}/status");
    let (status, _) = get(&app, &status_uri).await;
    assert_eq!(status, StatusCode::OK);

    let (status, text) = get(&app, "/metrics").await;
    assert_eq!(status, StatusCode::OK);
    assert_exposition(&text);
    assert!(
        text.contains("outpace_sessions{network=\"test\"} 1"),
        "{text}"
    );
    assert!(text.contains("outpace_peers{network=\"test\"} 5"), "{text}");
    assert!(
        text.contains(
            "outpace_http_requests_total{route=\"/streams/{network}/{id}/status\",method=\"GET\",status=\"200\"} 1"
        ),
        "{text}"
    );
    assert!(
        !text.contains(PLACEHOLDER_ID),
        "stream ids must never appear in metrics:\n{text}"
    );

    // The scrape itself is recorded on the next scrape, under its own template.
    let (_, text) = get(&app, "/metrics").await;
    assert!(text.contains("route=\"/metrics\",method=\"GET\",status=\"200\"} 1"));

    manager.stop("test", PLACEHOLDER_ID).await;
}

#[tokio::test]
async fn extension_methods_collapse_into_one_series() {
    let app = app(StreamManager::new(ProviderRegistry::new()));
    for method in [&b"BOGUS"[..], b"BOGUS2"] {
        let request = Request::builder()
            .method(Method::from_bytes(method).unwrap())
            .uri("/healthz")
            .body(Body::empty())
            .unwrap();
        app.clone().oneshot(request).await.unwrap();
    }
    let (_, text) = get(&app, "/metrics").await;
    assert!(!text.contains("BOGUS"), "{text}");
    assert!(
        text.contains(
            "outpace_http_requests_total{route=\"/healthz\",method=\"other\",status=\"405\"} 2"
        ),
        "{text}"
    );
}
