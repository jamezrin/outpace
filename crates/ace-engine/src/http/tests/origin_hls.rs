use super::*;
use crate::provider::ContentIdPreparation;
use ace_swarm::http_fetch::FetchPolicy;
use ace_swarm::resolve::HlsOrigin;
use axum::http::header;
use axum::routing::get as route_get;
use serde_json::json;
use std::sync::atomic::AtomicUsize;

const CID: &str = "0123456789abcdef0123456789abcdef01234567";
const URL: &str = "https://cdn.example.test/a%2Fb/master.m3u8?token=a%2Fb%3D&x=1&x=2";

/// Resolves every selector to an origin: lazily on open, and eagerly when asked to prepare.
struct OriginProvider {
    manifest_url: String,
    opens: Arc<AtomicUsize>,
}

impl OriginProvider {
    fn origin(&self) -> ProviderError {
        ProviderError::DirectHls(Box::new(HlsOrigin {
            manifest_url: self.manifest_url.clone(),
            metadata: StreamMetadata {
                title: Some("Synthetic HLS fixture".into()),
                bitrate: None,
                categories: vec!["fixture".into()],
            },
        }))
    }
}

#[async_trait]
impl StreamProvider for OriginProvider {
    fn network(&self) -> &'static str {
        "ace"
    }
    async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        Err(self.origin())
    }
    async fn prepare_live_content_id(
        &self,
        _content_id: &str,
    ) -> Result<ContentIdPreparation, ProviderError> {
        Err(self.origin())
    }
}

fn origin_state(resolve_early: bool) -> (AppState, Arc<AtomicUsize>) {
    origin_state_for(URL, resolve_early)
}

fn origin_state_for(manifest_url: &str, resolve_early: bool) -> (AppState, Arc<AtomicUsize>) {
    let opens = Arc::new(AtomicUsize::new(0));
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(OriginProvider {
        manifest_url: manifest_url.into(),
        opens: opens.clone(),
    }));
    let state = AppState {
        peer_policy: Default::default(),
        manager: StreamManager::new(registry),
        networks: vec!["ace".into()],
        resolve_content_ids_in_getstream: resolve_early,
        ace_sessions: Arc::new(AceSessionStore::default()),
        experimental_ace_compat: true,
        search: None,
        origin_hls: None,
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

#[tokio::test]
async fn unsupported_or_ambiguous_selection_is_rejected_before_anything_starts() {
    for resolve_early in [false, true] {
        let (state, opens) = origin_state(resolve_early);
        for base in [
            "/ace/getstream?format=json",
            "/ace/manifest.m3u8?format=json",
            "/server/api?method=get_media_files",
            "/webui/api/service?method=analyze_content",
        ] {
            for query in [
                "_idx=1",
                "_idx=-1",
                "_idx=",
                "_idx=0&%5fidx=0",
                "index=0",
                "stream_id=1",
                "direct_url=https%3A%2F%2Fexample.test%2Fmedia",
                "transport_file_data=AA",
                "playlist_selected_indexes=0",
                "playlist_start_from=0",
                "next_file_indexes=0",
                &format!("content_id={}", CID.replace('0', "f")),
            ] {
                let path = format!("{base}&content_id={CID}&{query}");
                let response = get(&state, &path).await;
                assert_eq!(response.status(), StatusCode::OK, "{path}");
                let body = json(response).await;
                assert!(body["error"].is_string(), "{path}: {body}");
            }
        }
        assert_eq!(state.ace_sessions.active_count(), 0);
        assert!(state.manager.list().await.is_empty());
        assert_eq!(opens.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn duplicate_command_parameters_do_not_execute() {
    let (state, _) = origin_state(false);
    let started = json(get(&state, &format!("/ace/getstream?id={CID}&format=json")).await).await;
    let command = local_path(&started["response"]["command_url"]);
    let body = json(get(&state, &format!("{command}?method=stop&method=stop")).await).await;
    assert_eq!(body["error"], "duplicate command parameter");
    assert_eq!(state.ace_sessions.active_count(), 1, "the lease survives");
    let body = json(get(&state, &format!("{command}?method=stop")).await).await;
    assert_eq!(body["response"], "ok");
    assert_eq!(state.ace_sessions.active_count(), 0);
}

#[tokio::test]
async fn android_default_selection_is_still_accepted() {
    let (state, _) = origin_state(true);
    let path = format!("/ace/getstream?format=json&content_id={CID}&_idx=0&stream_id=-1");
    let body = json(get(&state, &path).await).await;
    assert!(body["error"].is_null(), "{body}");
    assert_eq!(body["response"]["playback_url"], URL);
}

fn managed(state: &mut AppState, policy: FetchPolicy) -> Arc<crate::origin_hls::OriginHls> {
    let relay = crate::origin_hls::OriginHls::new(policy);
    state.origin_hls = Some(relay.clone());
    relay
}

fn local_path(url: &serde_json::Value) -> String {
    url.as_str()
        .unwrap()
        .strip_prefix("http://127.0.0.1:6878")
        .unwrap()
        .to_owned()
}

async fn relay_status(state: &AppState) -> serde_json::Value {
    let response = get(state, "/origin-hls/status").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    json(response).await
}

#[tokio::test]
async fn relay_routes_are_inert_unless_managed_hls_is_enabled() {
    let (state, _) = origin_state(true);
    assert_eq!(
        relay_status(&state).await,
        json!({"enabled": false, "active_sessions": 0})
    );
    let token = "0".repeat(64);
    assert_eq!(
        get(&state, &format!("/origin-hls/{token}/0.m3u8"))
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    let response = router(state.clone())
        .oneshot(
            Request::delete(format!("/origin-hls/{token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn managed_relay_serves_a_rewritten_origin_and_stop_releases_it() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let upstream = axum::Router::new()
        .route(
            "/live/master.m3u8",
            route_get(|| async {
                "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2,\npart.ts?signature=synthetic\n#EXT-X-ENDLIST\n"
            }),
        )
        .route("/live/part.ts", route_get(|| async { "synthetic segment" }));
    let server = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
    let (mut state, _) = origin_state_for(&format!("http://{address}/live/master.m3u8"), true);
    // Loopback fixtures need the test-only opt-in; runtime always uses the default policy.
    let relay = managed(
        &mut state,
        FetchPolicy {
            allow_non_global: true,
        },
    );

    let body = json(
        get(
            &state,
            &format!("/ace/getstream?content_id={CID}&format=json"),
        )
        .await,
    )
    .await;
    let response = &body["response"];
    assert_eq!(response["managed_origin"], true, "{body}");
    assert_eq!(response["is_direct"], true);
    assert_eq!(response["transport"], "hls");
    for field in ["stat_url", "infohash", "playback_session_id"] {
        assert!(response.get(field).is_none(), "invented {field}");
    }
    assert!(local_path(&response["command_url"]).ends_with("/cmd"));
    let playlist_path = local_path(&response["playback_url"]);
    let stop_path = local_path(&response["stop_url"]);
    assert!(playlist_path.starts_with("/origin-hls/"));
    assert_eq!(
        relay_status(&state).await,
        json!({"enabled": true, "active_sessions": 1})
    );

    let playlist = get(&state, &playlist_path).await;
    assert_eq!(playlist.status(), StatusCode::OK);
    assert_eq!(
        playlist.headers()[header::CONTENT_TYPE],
        "application/vnd.apple.mpegurl"
    );
    assert_eq!(playlist.headers()[header::CACHE_CONTROL], "no-store");
    assert_eq!(playlist.headers()["x-content-type-options"], "nosniff");
    let bytes = axum::body::to_bytes(playlist.into_body(), 8192)
        .await
        .unwrap();
    let manifest = std::str::from_utf8(&bytes).unwrap();
    assert!(!manifest.contains("signature"), "{manifest}");
    assert!(!manifest.contains(&address.to_string()), "{manifest}");
    let media = manifest
        .lines()
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .unwrap()
        .to_owned();
    assert!(media.starts_with("/origin-hls/"));
    assert_eq!(
        get(&state, &format!("{media}?reload=1")).await.status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "query semantics are not relayed"
    );
    let segment = get(&state, &media).await;
    assert_eq!(segment.status(), StatusCode::OK);
    assert_eq!(
        axum::body::to_bytes(segment.into_body(), 1024)
            .await
            .unwrap(),
        "synthetic segment"
    );

    let response = router(state.clone())
        .oneshot(Request::delete(&stop_path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    for path in [&playlist_path, &media] {
        assert_eq!(get(&state, path).await.status(), StatusCode::NOT_FOUND);
    }
    assert_eq!(relay.active_count(), 0);
    assert_eq!(state.ace_sessions.active_count(), 0);
    assert!(state.manager.list().await.is_empty());
    server.abort();
}

#[tokio::test]
async fn managed_relay_reports_sanitized_upstream_status_for_playlists_and_media() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let upstream = axum::Router::new()
        .route(
            "/status/{code}",
            route_get(|Path(code): Path<u16>| async move {
                (
                    StatusCode::from_u16(code).unwrap(),
                    [
                        (header::SET_COOKIE, "synthetic-origin-cookie"),
                        (header::LOCATION, "https://example.invalid/synthetic-origin"),
                        (header::CONTENT_ENCODING, "br"),
                    ],
                    "synthetic-origin-body",
                )
            }),
        )
        .route(
            "/manifest/{code}",
            route_get(|Path(code): Path<u16>| async move {
                format!(
                    "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXTINF:1,\n../status/{code}?token=synthetic-origin-query\n#EXT-X-ENDLIST\n"
                )
            }),
        );
    let server = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
    for (status, code) in [
        (401, "origin_upstream_unauthorized"),
        (403, "origin_upstream_forbidden"),
        (404, "origin_upstream_not_found"),
        (410, "origin_upstream_gone"),
        (429, "origin_upstream_rate_limited"),
        (503, "origin_upstream_unavailable"),
        (418, "origin_upstream_http_status"),
    ] {
        for media in [false, true] {
            let origin = if media {
                format!("http://{address}/manifest/{status}")
            } else {
                format!("http://{address}/status/{status}?token=synthetic-origin-query")
            };
            let (mut state, _) = origin_state_for(&origin, true);
            let relay = managed(
                &mut state,
                FetchPolicy {
                    allow_non_global: true,
                },
            );
            let started = get(&state, &format!("/ace/getstream?content_id={CID}")).await;
            let playlist = started.headers()[header::LOCATION]
                .to_str()
                .unwrap()
                .to_owned();
            let resource = if media {
                let response = get(&state, &playlist).await;
                assert_eq!(response.status(), StatusCode::OK);
                assert!(!response.headers().contains_key("x-outpace-upstream-status"));
                let bytes = axum::body::to_bytes(response.into_body(), 8192)
                    .await
                    .unwrap();
                std::str::from_utf8(&bytes)
                    .unwrap()
                    .lines()
                    .find(|line| line.starts_with("/origin-hls/"))
                    .unwrap()
                    .to_owned()
            } else {
                playlist
            };
            let response = get(&state, &resource).await;
            assert_eq!(response.status(), StatusCode::BAD_GATEWAY, "{status}");
            assert_eq!(
                response.headers()["x-outpace-upstream-status"],
                status.to_string()
            );
            assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
            assert!(!format!("{:?}", response.headers()).contains("synthetic-origin"));
            assert!(!response.headers().contains_key(header::SET_COOKIE));
            assert!(!response.headers().contains_key(header::LOCATION));
            let body = axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap();
            assert_eq!(body, code);
            assert_eq!(relay.active_count(), 1);
        }
    }
    server.abort();
}

#[tokio::test]
async fn managed_relay_redirects_late_resolution_to_local_leases() {
    let (mut state, _) = origin_state(false);
    let relay = managed(&mut state, FetchPolicy::default());
    let paths = [
        format!("/ace/getstream?id={CID}"),
        format!("/ace/manifest.m3u8?id={CID}"),
        format!("/streams/ace/cid:{CID}.ts"),
        format!("/streams/ace/cid:{CID}.m3u8"),
    ];
    for path in &paths {
        let response = get(&state, path).await;
        assert_eq!(response.status(), StatusCode::FOUND, "{path}");
        assert!(response.headers()[header::LOCATION]
            .to_str()
            .unwrap()
            .starts_with("/origin-hls/"));
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert_eq!(state.ace_sessions.active_count(), 0, "{path}");
        assert!(state.manager.list().await.is_empty());
    }
    assert_eq!(relay.active_count(), paths.len());
}

#[tokio::test]
async fn managed_relay_refuses_non_global_origins_by_default() {
    for origin in [
        "http://127.0.0.1:9/live.m3u8",
        "http://10.0.0.1/live.m3u8",
        "http://169.254.169.254/latest/meta-data.m3u8",
        "http://[::1]/live.m3u8",
    ] {
        let (mut state, _) = origin_state_for(origin, true);
        let relay = managed(&mut state, FetchPolicy::default());
        for path in [
            format!("/ace/getstream?content_id={CID}"),
            format!("/ace/getstream?content_id={CID}&format=json"),
        ] {
            let response = get(&state, &path).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{origin}");
            assert!(response.headers().get(header::LOCATION).is_none());
            let body = axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap();
            assert_eq!(body, "invalid_hls_origin");
        }
        assert_eq!(relay.active_count(), 0);
    }
}
