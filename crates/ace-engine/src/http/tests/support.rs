use super::*;
use crate::{support::SupportService, support_config::SupportSources};
struct Idle;
#[async_trait]
impl TsSource for Idle {
    async fn next(&mut self) -> Option<Bytes> {
        std::future::pending().await
    }
    fn stats(&self) -> SourceStats {
        Default::default()
    }
}
struct Provider;
#[async_trait]
impl StreamProvider for Provider {
    fn network(&self) -> &'static str {
        "ace"
    }
    async fn open(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        Ok(Box::new(Idle))
    }
}
async fn configured() -> (axum::Router, Arc<SupportService>, Arc<StreamManager>) {
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(Provider));
    let manager = StreamManager::with_session_limit(registry, 1);
    let sources = SupportSources::parse(Some(
        r#"{"synthetic":"https://example.invalid/descriptor?token=private-value"}"#,
    ))
    .unwrap();
    let service = SupportService::spawn(manager.clone(), &sources)
        .await
        .unwrap();
    let mut state = state();
    state.manager = manager.clone();
    state.networks = vec!["ace".into()];
    (
        router_with_support(state, Some(service.clone())),
        service,
        manager,
    )
}
async fn json(response: axum::response::Response) -> serde_json::Value {
    let body = axum::body::to_bytes(response.into_body(), 16384)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}
#[tokio::test]
async fn native_support_defaults_off_and_unknown_declarations_are_not_created() {
    let app = router(state());
    let response = app
        .clone()
        .oneshot(Request::get("/support").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json(response).await, serde_json::json!([]));
    let response = app
        .oneshot(
            Request::post("/support/unknown/start")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
#[tokio::test]
async fn native_support_commands_are_declared_only_idempotent_and_private() {
    let (app, service, manager) = configured().await;
    let response = app
        .clone()
        .oneshot(Request::get("/support").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let listed = json(response).await;
    assert_eq!(listed[0]["name"], "synthetic");
    let encoded = listed.to_string();
    assert!(
        !encoded.contains("example.invalid")
            && !encoded.contains("private-value")
            && !encoded.contains("turl-")
    );
    let mut previous = None;
    for _ in 0..2 {
        let response = app
            .clone()
            .oneshot(
                Request::post("/support/synthetic/stop")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let stopped = json(response).await;
        assert_eq!(stopped["running"], false);
        if let Some(previous) = previous {
            assert_eq!(stopped, previous);
        }
        previous = Some(stopped);
    }
    let response = app
        .clone()
        .oneshot(
            Request::post("/support/synthetic/start")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(json(response).await["running"], true);
    let response = app
        .oneshot(
            Request::post("/support/not-declared/start")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(service.status().len(), 1);
    assert!(service.shutdown().await);
    assert!(manager.shutdown().await);
}
#[tokio::test(start_paused = true)]
async fn native_support_rejects_body_bytes_empty_frame_floods_and_stalled_bodies() {
    let (app, service, manager) = configured().await;
    let initial = service.status()[0].epoch;
    let bodies = [
        (Body::from("x"), StatusCode::BAD_REQUEST),
        (Body::from(vec![0; 8192]), StatusCode::PAYLOAD_TOO_LARGE),
        (
            Body::from_stream(futures::stream::iter(
                (0..16).map(|_| Ok::<_, std::convert::Infallible>(Bytes::new())),
            )),
            StatusCode::BAD_REQUEST,
        ),
        (
            Body::from_stream(futures::stream::pending::<Result<Bytes, std::io::Error>>()),
            StatusCode::REQUEST_TIMEOUT,
        ),
    ];
    for (body, expected) in bodies {
        let response = app
            .clone()
            .oneshot(Request::post("/support/synthetic/stop").body(body).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        assert_eq!(
            service.status()[0].epoch,
            initial,
            "rejected body changed desired state"
        );
    }
    assert!(service.shutdown().await);
    assert!(manager.shutdown().await);
}
