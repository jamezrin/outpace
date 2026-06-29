//! Clean HTTP API (axum). No `ace`/`acestream` tokens in paths or JSON keys — the only
//! `ace` on the surface is the `{network}` value, selecting a provider.

use crate::manager::StreamManager;
use axum::{routing::get, Json, Router};
use serde_json::json;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub manager: Arc<StreamManager>,
    pub networks: Vec<String>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/networks", get(networks))
        .with_state(state)
}

async fn networks(axum::extract::State(s): axum::extract::State<AppState>) -> Json<serde_json::Value> {
    Json(json!({ "networks": s.networks }))
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
}
