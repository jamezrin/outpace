use super::*;
use crate::provider::ContentIdPreparation;
use ace_swarm::resolve::HlsOrigin;
use axum::http::header;
use std::sync::atomic::AtomicUsize;

const CID: &str = "0123456789abcdef0123456789abcdef01234567";
const URL: &str = "https://cdn.example.test/a%2Fb/master.m3u8?token=a%2Fb%3D&x=1&x=2";

fn origin() -> HlsOrigin {
    HlsOrigin {
        manifest_url: URL.into(),
        metadata: StreamMetadata {
            title: Some("Synthetic HLS fixture".into()),
            bitrate: None,
            categories: vec!["fixture".into()],
        },
    }
}

/// Resolves every selector to an origin: lazily on open, and eagerly when asked to prepare.
struct OriginProvider {
    opens: Arc<AtomicUsize>,
}

#[async_trait]
impl StreamProvider for OriginProvider {
    fn network(&self) -> &'static str {
        "ace"
    }
    async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        Err(ProviderError::DirectHls(Box::new(origin())))
    }
    async fn prepare_live_content_id(
        &self,
        _content_id: &str,
    ) -> Result<ContentIdPreparation, ProviderError> {
        Err(ProviderError::DirectHls(Box::new(origin())))
    }
}

fn origin_state(resolve_early: bool) -> (AppState, Arc<AtomicUsize>) {
    let opens = Arc::new(AtomicUsize::new(0));
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(OriginProvider {
        opens: opens.clone(),
    }));
    let state = AppState {
        peer_policy: Default::default(),
        manager: StreamManager::new(registry),
        networks: vec!["ace".into()],
        resolve_content_ids_in_getstream: resolve_early,
        ace_sessions: Arc::new(AceSessionStore::default()),
        experimental_ace_compat: true,
        broadcasts: None,
    };
    (state, opens)
}

async fn get(state: &AppState, path: &str) -> axum::response::Response {
    router(state.clone())
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn json(response: axum::response::Response) -> serde_json::Value {
    serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap(),
    )
    .unwrap()
}

fn assert_exact_handoff(response: &axum::response::Response, path: &str) {
    assert_eq!(response.status(), StatusCode::FOUND, "{path}");
    assert_eq!(response.headers()[header::LOCATION], URL, "{path}");
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
}

#[tokio::test]
async fn native_routes_hand_off_the_exact_origin_without_a_local_session() {
    let (state, opens) = origin_state(false);
    for suffix in ["", ".ts", ".m3u8"] {
        let path = format!("/streams/ace/cid:{CID}{suffix}");
        let response = get(&state, &path).await;
        assert_exact_handoff(&response, &path);
        assert!(state.manager.list().await.is_empty());
        assert!(state
            .manager
            .get_hls("ace", &format!("cid:{CID}"))
            .await
            .is_none());
    }
    assert_eq!(
        opens.load(Ordering::SeqCst),
        3,
        "signed origin redirects must not become retained sessions"
    );
    for suffix in ["status", "seg/0.ts"] {
        let response = get(&state, &format!("/streams/ace/cid:{CID}/{suffix}")).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
    assert_eq!(opens.load(Ordering::SeqCst), 3, "probes never open origins");
}

#[tokio::test]
async fn late_resolution_redirects_compatibility_starts_and_releases_leases() {
    let (state, _) = origin_state(false);
    for path in [
        format!("/ace/getstream?id={CID}"),
        format!("/ace/manifest.m3u8?id={CID}"),
    ] {
        let response = get(&state, &path).await;
        assert_exact_handoff(&response, &path);
        assert_eq!(state.ace_sessions.active_count(), 0, "{path}");
        assert!(state.manager.list().await.is_empty());
    }

    // JSON mode cannot know the transport yet; following its playback URL hands off.
    let started = json(get(&state, &format!("/ace/getstream?id={CID}&format=json")).await).await;
    let playback = started["response"]["playback_url"]
        .as_str()
        .unwrap()
        .strip_prefix("http://127.0.0.1:6878")
        .unwrap()
        .to_owned();
    assert_eq!(state.ace_sessions.active_count(), 1);
    let response = get(&state, &playback).await;
    assert_exact_handoff(&response, &playback);
    assert_eq!(state.ace_sessions.active_count(), 0);
    assert_eq!(
        get(&state, &playback).await.status(),
        StatusCode::NOT_FOUND,
        "a handed-off lease is revoked"
    );
}

#[tokio::test]
async fn early_resolution_returns_direct_json_without_swarm_controls() {
    let (state, opens) = origin_state(true);
    for path in [
        format!("/ace/getstream?content_id={CID}&format=json"),
        format!("/ace/manifest.m3u8?content_id={CID}&format=json"),
    ] {
        let response = get(&state, &path).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        let body = json(response).await;
        assert!(body["error"].is_null(), "{body}");
        let response = &body["response"];
        assert_eq!(response["playback_url"], URL);
        assert_eq!(response["is_direct"], true);
        assert_eq!(response["transport"], "hls");
        assert_eq!(response["content_id"], CID);
        assert_eq!(response["metadata"]["title"], "Synthetic HLS fixture");
        for field in ["stat_url", "command_url", "infohash", "playback_session_id"] {
            assert!(response.get(field).is_none(), "invented {field}");
        }
    }
    for path in [
        format!("/ace/getstream?content_id={CID}"),
        format!("/ace/manifest.m3u8?content_id={CID}"),
    ] {
        assert_exact_handoff(&get(&state, &path).await, &path);
    }
    assert_eq!(state.ace_sessions.active_count(), 0);
    assert!(state.manager.list().await.is_empty());
    assert_eq!(opens.load(Ordering::SeqCst), 0, "no source was opened");
}
