use super::*;
use crate::config::HlsHistoryConfig;
use crate::history::HistoryBudget;
use crate::origin_hls::OriginHls;
use ace_swarm::http_fetch::FetchPolicy;
use std::sync::atomic::{AtomicBool, AtomicUsize};

async fn get(app: &Router, path: &str) -> Response {
    app.clone()
        .oneshot(
            Request::get(path)
                .header(header::HOST, "player.example.test:8080")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn bytes(response: Response) -> Bytes {
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap()
}

async fn json_body(response: Response) -> serde_json::Value {
    serde_json::from_slice(&bytes(response).await).unwrap()
}

fn media_urls(playlist: &[u8]) -> Vec<String> {
    std::str::from_utf8(playlist)
        .unwrap()
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_owned)
        .collect()
}

#[tokio::test]
async fn origin_history_controls_reject_ambiguous_queries_without_changing_view() {
    let relay = OriginHls::new(FetchPolicy::default());
    let playback = relay
        .start("https://synthetic.example.test/live.m3u8")
        .await
        .unwrap();
    let mut state = state();
    state.origin_hls = Some(relay);
    let app = router(state);
    let command = &playback.command_url;
    let initial = json_body(get(&app, &format!("{command}?method=timeshift_status")).await).await;
    assert_eq!(initial["response"]["enabled"], false);
    assert_eq!(initial["response"]["reason"], "history_disabled");
    let versioned = json_body(
        get(
            &app,
            &format!("{command}?method=timeshift_status&api_version=2"),
        )
        .await,
    )
    .await;
    assert_eq!(versioned, initial);
    for query in [
        "method=timeshift_seek&back=1&position_ms=0",
        "method=timeshift_seek&back=NaN",
        "method=timeshift_seek&back=0",
        "method=timeshift_seek&position_ms=-1",
        "method=timeshift_seek&position_ms=1.5",
        "method=timeshift_seek&position_ms=18446744073709551616",
        "method=timeshift_seek&position_ms=1&position%5fms=1",
        "method=timeshift_seek&position_ms=0&generation=+0",
        "method=timeshift_seek&position_ms=0&generation=0&generation=0",
        "method=timeshift_live&back=1",
        "method=timeshift_live&unknown=1",
        "method=stop&method=stop",
        "method=stop&generation=0",
        "method=stop&api_version=",
        "method=stop&api_version=1",
        "method=stop&api_version=3",
        "method=stop&api_version=02",
        "method=stop&api_version=+2",
        "method=stop&api_version=%FF",
        "method=stop&api_version=2&api_version=2",
        "method=stop&api_version=2&api%5fversion=2",
        "method=stop&api_version=2&api_version=1",
        "method=stop&api_version=2&unknown=1",
        "method=stop&api_version=2&generation=0",
        "method=timeshift_status&generation=0",
        "method=liveseek&position_ms=0",
        "method=timeshift_seek&position_ms=0",
    ] {
        let result = json_body(get(&app, &format!("{command}?{query}")).await).await;
        assert!(result["response"].is_null(), "{query}: {result}");
        assert!(result["error"].is_string(), "{query}: {result}");
        let current =
            json_body(get(&app, &format!("{command}?method=timeshift_status")).await).await;
        assert_eq!(current["response"]["view"], initial["response"]["view"]);
    }
}

#[tokio::test]
async fn origin_history_http_replays_deleted_media_and_revokes_only_its_view() {
    let deleted = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicUsize::new(0));
    let gone = deleted.clone();
    let reads = calls.clone();
    let origin = Router::new().fallback(axum::routing::get(move |request: Request<Body>| {
        let deleted = gone.clone();
        let calls = reads.clone();
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            match request.uri().path() {
                "/live.m3u8" => ([(header::CONTENT_TYPE, "application/vnd.apple.mpegurl")],
                    "#EXTM3U\n#EXT-X-INDEPENDENT-SEGMENTS\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:10\n#EXTINF:2,\nfirst.ts\n#EXTINF:2,\nsecond.ts\n").into_response(),
                "/first.ts" | "/second.ts" if !deleted.load(Ordering::SeqCst) =>
                    ([(header::CONTENT_TYPE, "video/mp2t"), (header::CACHE_CONTROL, "no-store")], FIXTURE).into_response(),
                _ => StatusCode::NOT_FOUND.into_response(),
            }
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source = format!("http://{}/live.m3u8", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, origin).await.unwrap();
    });
    let history = HlsHistoryConfig {
        max_seconds: 60,
        max_bytes: 1 << 20,
        global_max_bytes: 2 << 20,
    };
    let relay = OriginHls::new_with_history(
        FetchPolicy {
            allow_non_global: true,
        },
        history,
        HistoryBudget::new(history.global_max_bytes),
    );
    let playback = relay.start(&source).await.unwrap();
    let other = relay.start(&source).await.unwrap();
    let mut state = state();
    state.origin_hls = Some(relay);
    let app = router(state);
    let playlist = bytes(get(&app, &playback.playback_url).await).await;
    let media = media_urls(&playlist);
    assert_eq!(media.len(), 2);
    // Request completion order must not become the retained media timeline.
    for url in media.iter().rev() {
        assert_eq!(bytes(get(&app, url).await).await.as_ref(), FIXTURE);
    }
    let command = &playback.command_url;
    let status = json_body(get(&app, &format!("{command}?method=timeshift_status")).await).await;
    assert_eq!(status["response"]["supported"], true);
    assert_eq!(status["response"]["history"]["seekable_start_ms"], 0);
    assert_eq!(status["response"]["history"]["end_ms"], 4000);
    deleted.store(true, Ordering::SeqCst);
    let before = calls.load(Ordering::SeqCst);
    let seek = json_body(
        get(
            &app,
            &format!("{command}?method=timeshift_seek&position_ms=0&generation=0"),
        )
        .await,
    )
    .await;
    assert!(seek["error"].is_null(), "{seek}");
    assert_eq!(seek["response"]["view"]["generation"], 1);
    let replay = seek["response"]["playback_url"].as_str().unwrap();
    assert!(replay.starts_with("http://player.example.test:8080/origin-hls/"));
    assert_eq!(seek["response"]["view"]["playback_url"], replay);
    let snapshot = bytes(get(&app, replay).await).await;
    assert!(std::str::from_utf8(&snapshot)
        .unwrap()
        .contains("#EXT-X-ENDLIST"));
    let retained = media_urls(&snapshot);
    assert_eq!(retained.len(), 2);
    for url in &retained {
        let response = app
            .clone()
            .oneshot(
                Request::get(url)
                    .header(header::RANGE, "bytes=0-")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bytes(response).await.as_ref(), FIXTURE);
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        before,
        "replay must not refetch an expired origin"
    );
    let failed = json_body(
        get(
            &app,
            &format!("{command}?method=timeshift_seek&position_ms=4000&generation=1"),
        )
        .await,
    )
    .await;
    assert!(failed["error"].is_string());
    let stale = json_body(
        get(
            &app,
            &format!("{command}?method=timeshift_live&generation=0"),
        )
        .await,
    )
    .await;
    assert!(stale["error"].is_string());
    assert_eq!(bytes(get(&app, replay).await).await, snapshot);
    let partial = app
        .clone()
        .oneshot(
            Request::get(&retained[0])
                .header(header::RANGE, "bytes=1-2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(partial.status(), StatusCode::BAD_REQUEST);
    let other_status = json_body(
        get(
            &app,
            &format!("{}?method=timeshift_status", other.command_url),
        )
        .await,
    )
    .await;
    assert_eq!(other_status["response"]["view"]["mode"], "live");
    assert_eq!(other_status["response"]["view"]["generation"], 0);
    assert!(!bytes(get(&app, &other.playback_url).await).await.is_empty());
    let live = json_body(
        get(
            &app,
            &format!("{command}?method=timeshift_live&generation=1"),
        )
        .await,
    )
    .await;
    assert_eq!(
        live["response"]["playback_url"],
        format!("http://player.example.test:8080{}", playback.playback_url)
    );
    assert_eq!(
        live["response"]["view"]["playback_url"],
        live["response"]["playback_url"]
    );
    assert_eq!(live["response"]["view"]["generation"], 2);
    assert_eq!(get(&app, replay).await.status(), StatusCode::GONE);
    assert_eq!(get(&app, &retained[0]).await.status(), StatusCode::GONE);
    assert!(!bytes(get(&app, &playback.playback_url).await)
        .await
        .is_empty());
    // EngineApi.stopSession() in the public Android SDK uses this exact query.
    let stop = json_body(get(&app, &format!("{command}?method=stop&api_version=2")).await).await;
    assert_eq!(stop["response"], "ok");
    assert_eq!(get(&app, replay).await.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        get(&app, &retained[0]).await.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(get(&app, &media[0]).await.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        get(&app, &playback.playback_url).await.status(),
        StatusCode::NOT_FOUND
    );
    let stopped_status = json_body(
        get(
            &app,
            &format!("{command}?method=timeshift_status&api_version=2"),
        )
        .await,
    )
    .await;
    assert!(stopped_status["response"].is_null());
    assert!(stopped_status["error"].is_string());
    let repeated_stop =
        json_body(get(&app, &format!("{command}?method=stop&api_version=2")).await).await;
    assert!(repeated_stop["response"].is_null());
    assert!(repeated_stop["error"].is_string());
    assert!(!bytes(get(&app, &other.playback_url).await).await.is_empty());
    let other_after = json_body(
        get(
            &app,
            &format!(
                "{}?method=timeshift_status&api_version=2",
                other.command_url
            ),
        )
        .await,
    )
    .await;
    assert_eq!(
        other_after["response"]["view"],
        other_status["response"]["view"]
    );
    // Omitting the SDK hint remains a supported native control request.
    let plain_stop =
        json_body(get(&app, &format!("{}?method=stop", other.command_url)).await).await;
    assert_eq!(plain_stop["response"], "ok");
    server.abort();
}
