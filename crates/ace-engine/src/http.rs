//! Clean HTTP API (axum). No `ace`/`acestream` tokens in paths or JSON keys — the only
//! `ace` on the surface is the `{network}` value, selecting a provider.

use crate::manager::StreamManager;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{routing::get, Json, Router};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;

#[derive(Clone)]
pub struct AppState {
    pub manager: Arc<StreamManager>,
    pub networks: Vec<String>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/networks", get(networks))
        .route("/streams", get(list_streams))
        .route("/streams/:network/:file", get(stream_file))
        .route("/streams/:network/:id/status", get(stream_status))
        .route("/streams/:network/:id/seg/:seg", get(stream_segment))
        .with_state(state)
}

async fn networks(State(s): State<AppState>) -> Json<serde_json::Value> {
    Json(json!({ "networks": s.networks }))
}

/// `GET /streams` — active sessions and their client counts.
async fn list_streams(State(s): State<AppState>) -> Json<serde_json::Value> {
    let streams: Vec<_> = s
        .manager
        .list()
        .await
        .into_iter()
        .map(|(network, id, clients)| json!({ "network": network, "id": id, "clients": clients }))
        .collect();
    Json(json!({ "streams": streams }))
}

/// `GET /streams/{network}/{id}/status` — stats for a running session (404 if not active).
async fn stream_status(State(s): State<AppState>, Path((network, id)): Path<(String, String)>) -> Response {
    match s.manager.get(&network, &id).await {
        Some(session) => {
            let stats = session.stats().await;
            Json(json!({
                "network": network,
                "id": id,
                "clients": session.subscriber_count(),
                "peers": stats.peers,
                "bitrate": stats.bitrate,
                "buffer_ms": stats.buffer_ms,
            }))
            .into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// `GET /streams/{network}/{id}/seg/{n}.ts` — a retained HLS segment.
async fn stream_segment(State(s): State<AppState>, Path((network, id, seg)): Path<(String, String, String)>) -> Response {
    let Some(seq) = seg.strip_suffix(".ts").and_then(|n| n.parse::<u64>().ok()) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let pkg = match s.manager.get_or_start_hls(&network, &id).await {
        Ok(p) => p,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    match pkg.segment(seq) {
        Some(bytes) => ([(header::CONTENT_TYPE, "video/mp2t")], bytes).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// `GET /streams/{network}/{id}.ts` (continuous MPEG-TS) or `.m3u8` (live HLS playlist).
async fn stream_file(State(s): State<AppState>, Path((network, file)): Path<(String, String)>) -> Response {
    if let Some(id) = file.strip_suffix(".m3u8") {
        return match s.manager.get_or_start_hls(&network, id).await {
            Ok(pkg) => (
                [(header::CONTENT_TYPE, "application/vnd.apple.mpegurl")],
                pkg.playlist(&network, id),
            )
                .into_response(),
            Err(_) => StatusCode::NOT_FOUND.into_response(),
        };
    }
    let Some(id) = file.strip_suffix(".ts") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let session = match s.manager.get_or_start(&network, id).await {
        Ok(sess) => sess,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    let sub = session.subscribe();
    // Bridge the broadcast receiver to an HTTP body stream; the Subscription rides along so
    // its Drop (decrementing the client count) fires when the client disconnects.
    let stream = futures::stream::unfold(sub, |mut sub| async move {
        loop {
            match sub.rx.recv().await {
                Ok(chunk) => return Some((Ok::<_, std::io::Error>(chunk), sub)),
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    });
    Response::builder()
        .header(header::CONTENT_TYPE, "video/mp2t")
        .body(Body::from_stream(stream))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::ProviderRegistry;
    use crate::testprovider::TestProvider;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn state() -> AppState {
        let mut r = ProviderRegistry::new();
        r.register(std::sync::Arc::new(TestProvider { chunks: 10 }));
        AppState { manager: StreamManager::new(r), networks: vec!["test".into()] }
    }

    #[tokio::test]
    async fn healthz_ok() {
        let resp = router(state())
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn networks_lists_registered() {
        let resp = router(state())
            .oneshot(Request::get("/networks").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("\"test\""));
    }

    #[tokio::test]
    async fn stream_ts_serves_mpegts_first_frame() {
        use futures::StreamExt;
        let resp = router(state())
            .oneshot(Request::get("/streams/test/somechannel.ts").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()[header::CONTENT_TYPE], "video/mp2t");
        // Live body is unbounded; read just the first TS chunk.
        let mut stream = resp.into_body().into_data_stream();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first[0], 0x47);
    }

    #[tokio::test]
    async fn unknown_network_returns_404() {
        let resp = router(state())
            .oneshot(Request::get("/streams/nope/x.ts").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn non_ts_extension_returns_404() {
        let resp = router(state())
            .oneshot(Request::get("/streams/test/x.foo").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn status_404_until_started_then_reports() {
        let st = state();
        let app = router(st.clone());
        // Not started yet.
        let resp = app
            .clone()
            .oneshot(Request::get("/streams/test/chan/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        // Start it, then status reports it with clean keys.
        st.manager.get_or_start("test", "chan").await.unwrap();
        let resp = app
            .oneshot(Request::get("/streams/test/chan/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let txt = String::from_utf8_lossy(&body);
        assert!(txt.contains("\"clients\"") && txt.contains("\"peers\""));
    }

    #[tokio::test]
    async fn m3u8_serves_hls_playlist() {
        let resp = router(state())
            .oneshot(Request::get("/streams/test/chan.m3u8").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()[header::CONTENT_TYPE], "application/vnd.apple.mpegurl");
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        assert!(String::from_utf8_lossy(&body).starts_with("#EXTM3U"));
    }

    #[tokio::test]
    async fn list_streams_reports_active() {
        let st = state();
        st.manager.get_or_start("test", "abc").await.unwrap();
        let resp = router(st)
            .oneshot(Request::get("/streams").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("\"abc\""));
    }
}
