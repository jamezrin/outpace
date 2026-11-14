//! Native start `phase` on the status route and typed failure codes on start-route bodies.
use super::*;
use crate::provider::StatsReader;
use crate::start_failure::ProviderStartFailure;
use crate::start_status::StartProgress;
use std::sync::atomic::AtomicU32;
use tokio::sync::Notify;

/// Fails every open with the error its id names.
struct FailingProvider;

#[async_trait]
impl StreamProvider for FailingProvider {
    fn network(&self) -> &'static str {
        "fail"
    }
    async fn open(&self, id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        Err(match id {
            "no-peers" => ProviderError::NoPeers("synthetic discovery diagnostic".into()),
            "network" => ProviderError::Network("synthetic catalog diagnostic".into()),
            "capacity" => ProviderError::CapacityExhausted,
            "protected" => ProviderError::ProtectedContent,
            "invalid-protection" => ProviderError::InvalidProtection,
            "unresolvable" => ProviderError::Unresolvable("synthetic user-facing reason".into()),
            "not-found" => ProviderError::NotFound,
            _ => ProviderError::Backend("synthetic backend diagnostic".into()),
        })
    }
}

fn app_state(provider: Arc<dyn StreamProvider>) -> AppState {
    let mut registry = ProviderRegistry::new();
    let network = provider.network();
    registry.register(provider);
    AppState {
        peer_policy: Default::default(),
        manager: StreamManager::new(registry),
        networks: vec![network.into()],
        resolve_content_ids_in_getstream: false,
        ace_sessions: Arc::new(AceSessionStore::default()),
        experimental_ace_compat: false,
        search: None,
        origin_hls: None,
        broadcasts: None,
    }
}

async fn get(app: &Router, path: &str) -> Response {
    app.clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn json_body(response: Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 16)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn native_start_failures_return_typed_json_and_status_retains_them() {
    let cases = [
        ("no-peers", StatusCode::NOT_FOUND, "no_peers"),
        ("network", StatusCode::NOT_FOUND, "network"),
        ("backend", StatusCode::NOT_FOUND, "internal"),
        ("not-found", StatusCode::NOT_FOUND, "not_found"),
        ("capacity", StatusCode::SERVICE_UNAVAILABLE, "capacity"),
        ("protected", StatusCode::UNPROCESSABLE_ENTITY, "protected"),
        (
            "invalid-protection",
            StatusCode::UNPROCESSABLE_ENTITY,
            "protected",
        ),
        (
            "unresolvable",
            StatusCode::UNPROCESSABLE_ENTITY,
            "unresolvable",
        ),
    ];
    for suffix in [".ts", ".m3u8", ""] {
        let app = router(app_state(Arc::new(FailingProvider)));
        for (id, status, code) in cases {
            let response = get(&app, &format!("/streams/fail/{id}{suffix}")).await;
            assert_eq!(response.status(), status, "{id}{suffix}");
            assert_eq!(
                response.headers()[header::CONTENT_TYPE],
                "application/json",
                "{id}{suffix}"
            );
            if status == StatusCode::SERVICE_UNAVAILABLE {
                assert_eq!(response.headers()[header::RETRY_AFTER], "1");
            }
            let body = json_body(response).await;
            assert_eq!(body["error"]["code"], code, "{id}{suffix}: {body}");
            let message = body["error"]["message"].as_str().unwrap();
            assert!(!message.contains("diagnostic"), "{id}{suffix}: {message}");

            // The failure outlives the (never created) session on the status route.
            let response = get(&app, &format!("/streams/fail/{id}/status")).await;
            assert_eq!(response.status(), StatusCode::OK, "{id}");
            let status_body = json_body(response).await;
            assert_eq!(status_body["phase"], "failed", "{id}: {status_body}");
            assert_eq!(status_body["error"], body["error"], "{id}");
            assert_eq!(status_body["peers"], 0);
            assert_eq!(status_body["network"], "fail");
            assert_eq!(status_body["id"], id);
        }
    }
    let app = router(app_state(Arc::new(FailingProvider)));
    let response = get(&app, "/streams/fail/unresolvable.ts").await;
    assert_eq!(
        json_body(response).await["error"]["message"],
        "synthetic user-facing reason"
    );
}

#[tokio::test]
async fn unknown_network_start_is_typed_not_found_and_leaves_no_status() {
    let app = router(app_state(Arc::new(FailingProvider)));
    for path in ["/streams/nope/x.ts", "/streams/nope/x.m3u8"] {
        let response = get(&app, path).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        assert_eq!(json_body(response).await["error"]["code"], "not_found");
    }
    let response = get(&app, "/streams/nope/x/status").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn vod_failures_return_typed_json_with_unchanged_statuses() {
    let app = router(state());
    let response = get(&app, "/vod/nope/x").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(json_body(response).await["error"]["code"], "not_found");
    // The test network has no VOD path: an internal failure, still a 502.
    let response = get(&app, "/vod/test/x").await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
    assert_eq!(json_body(response).await["error"]["code"], "internal");
}

/// Opens in controlled steps so each phase can be observed.
struct GatedProvider {
    entered: Notify,
    resolve: Notify,
    resolved: Notify,
    open: Notify,
    emit: Arc<Notify>,
    peers: Arc<AtomicU32>,
}

struct GatedSource {
    emit: Arc<Notify>,
    peers: Arc<AtomicU32>,
    sent: bool,
}

#[async_trait]
impl TsSource for GatedSource {
    async fn next(&mut self) -> Option<Bytes> {
        if self.sent {
            return std::future::pending().await;
        }
        self.emit.notified().await;
        self.sent = true;
        Some(Bytes::copy_from_slice(&FIXTURE[..188 * 4]))
    }
    fn stats(&self) -> SourceStats {
        PeerStats(self.peers.clone()).snapshot()
    }
    // Live counters, as the swarm source publishes them before any media arrives.
    fn stats_handle(&self) -> Option<crate::provider::StatsHandle> {
        Some(Arc::new(PeerStats(self.peers.clone())))
    }
}

struct PeerStats(Arc<AtomicU32>);

impl crate::provider::StatsReader for PeerStats {
    fn snapshot(&self) -> SourceStats {
        SourceStats {
            peers: self.0.load(Ordering::SeqCst),
            ..SourceStats::default()
        }
    }
}

#[async_trait]
impl StreamProvider for GatedProvider {
    fn network(&self) -> &'static str {
        "gate"
    }
    async fn open(&self, id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.open_with_progress(id, StartProgress::none())
            .await
            .map_err(ProviderError::from)
    }
    async fn open_with_progress(
        &self,
        _id: &str,
        progress: StartProgress,
    ) -> Result<Box<dyn TsSource>, ProviderStartFailure> {
        self.entered.notify_one();
        self.resolve.notified().await;
        progress.resolved();
        self.resolved.notify_one();
        self.open.notified().await;
        Ok(Box::new(GatedSource {
            emit: self.emit.clone(),
            peers: self.peers.clone(),
            sent: false,
        }))
    }
}

async fn phase(app: &Router) -> (StatusCode, serde_json::Value) {
    let response = get(app, "/streams/gate/chan/status").await;
    let status = response.status();
    if status != StatusCode::OK {
        return (status, serde_json::Value::Null);
    }
    let body = json_body(response).await;
    (status, body["phase"].clone())
}

#[tokio::test]
async fn status_reports_each_phase_from_the_start_request_through_playing() {
    let provider = Arc::new(GatedProvider {
        entered: Notify::new(),
        resolve: Notify::new(),
        resolved: Notify::new(),
        open: Notify::new(),
        emit: Arc::new(Notify::new()),
        peers: Arc::new(AtomicU32::new(0)),
    });
    let state = app_state(provider.clone());
    let app = router(state.clone());
    assert_eq!(
        phase(&app).await.0,
        StatusCode::NOT_FOUND,
        "nothing started"
    );

    let request = tokio::spawn({
        let app = app.clone();
        async move { get(&app, "/streams/gate/chan.ts").await }
    });
    provider.entered.notified().await;
    assert_eq!(phase(&app).await, (StatusCode::OK, json!("resolving")));

    provider.resolve.notify_one();
    provider.resolved.notified().await;
    assert_eq!(phase(&app).await, (StatusCode::OK, json!("connecting")));

    provider.open.notify_one();
    let response = request.await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    // A session without peers is still connecting; the record of the attempt is gone.
    assert_eq!(phase(&app).await, (StatusCode::OK, json!("connecting")));
    assert!(state.manager.start_status("gate", "chan").is_none());

    provider.peers.store(1, Ordering::SeqCst);
    assert_eq!(phase(&app).await, (StatusCode::OK, json!("buffering")));

    provider.emit.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        while phase(&app).await.1 != json!("playing") {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("first media output reaches playing");
    let body = json_body(get(&app, "/streams/gate/chan/status").await).await;
    assert!(body["error"].is_null(), "{body}");
    assert_eq!(body["peers"], 1);
    drop(response);
    state.manager.shutdown().await;
}

#[tokio::test]
async fn cancelled_start_leaves_no_status_record() {
    let provider = Arc::new(GatedProvider {
        entered: Notify::new(),
        resolve: Notify::new(),
        resolved: Notify::new(),
        open: Notify::new(),
        emit: Arc::new(Notify::new()),
        peers: Arc::new(AtomicU32::new(0)),
    });
    let state = app_state(provider.clone());
    let app = router(state.clone());
    let request = tokio::spawn({
        let app = app.clone();
        async move { get(&app, "/streams/gate/chan.ts").await }
    });
    provider.entered.notified().await;
    assert_eq!(phase(&app).await, (StatusCode::OK, json!("resolving")));
    // Stop fences the pending generation; the provider then finishes its (abandoned) open.
    let _ = state.manager.stop("gate", "chan").await;
    provider.resolve.notify_one();
    provider.open.notify_one();
    let _ = request.await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while state.manager.start_status("gate", "chan").is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("a cancelled start drops its record");
    assert_eq!(phase(&app).await.0, StatusCode::NOT_FOUND);
    state.manager.shutdown().await;
}
