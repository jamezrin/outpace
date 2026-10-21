use super::*;

#[test]
fn stream_metadata_json_has_stable_shape() {
    assert_eq!(
        stream_metadata_json(&StreamMetadata::default()),
        json!({ "title": null, "bitrate": null, "categories": [] })
    );
    assert_eq!(
        stream_metadata_json(&StreamMetadata {
            title: Some("Synthetic Demo Channel".to_string()),
            bitrate: Some(100_000),
            categories: vec!["sports".to_string()],
        }),
        json!({
            "title": "Synthetic Demo Channel",
            "bitrate": 800_000,
            "categories": ["sports"],
        })
    );
}

#[test]
fn stream_metadata_json_saturates_rate_conversion() {
    let metadata = StreamMetadata {
        bitrate: Some(u64::MAX),
        ..Default::default()
    };
    assert_eq!(stream_metadata_json(&metadata)["bitrate"], u64::MAX);
}

#[tokio::test]
async fn stream_ts_starts_on_keyframe_when_joining_mid_gop() {
    use futures::StreamExt;
    const VIDEO_PID: u16 = 0x0100;
    const KEYFRAME2: usize = 9400;
    // Hold the app (and thus the manager/session) alive while we read the streamed body;
    // the session's pump is tied to the session's lifetime.
    let app = router(fixture_state(4136));
    // Join the stream mid-GOP (a non-keyframe video packet between the two keyframes).
    let resp = app
        .clone()
        .oneshot(
            Request::get("/streams/fix/x.ts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Collect a few KB of the served body.
    let mut stream = resp.into_body().into_data_stream();
    let mut body = Vec::new();
    while body.len() < 4096 {
        match stream.next().await {
            Some(Ok(chunk)) => body.extend_from_slice(&chunk),
            _ => break,
        }
    }
    // The first video packet the client sees must be the real keyframe at byte 9400,
    // not the mid-GOP packet we joined on.
    let first_video = body
        .as_chunks::<188>()
        .0
        .iter()
        .map(|packet| packet.as_slice())
        .find(|p| (((p[1] & 0x1F) as u16) << 8 | p[2] as u16) == VIDEO_PID)
        .expect("a video packet was served");
    assert_eq!(first_video, &FIXTURE[KEYFRAME2..KEYFRAME2 + 188]);
}

#[test]
fn stream_ts_resets_keyframe_gate_after_lag() {
    let mut gate = ace_media::mpegts::KeyframeGate::new();
    assert!(
        !gate.push(FIXTURE).is_empty(),
        "fixture should lock the gate on a keyframe"
    );
    let mid_gop_packet = &FIXTURE[4136..4136 + 188];
    assert_eq!(
        gate.push(mid_gop_packet).len(),
        188,
        "locked gate passes through mid-GOP packets"
    );

    reset_stream_keyframe_gate(&mut gate);

    assert!(
        gate.push(mid_gop_packet).is_empty(),
        "after lag reset, mid-GOP packets are held until a keyframe"
    );

    let resumed = gate.push(FIXTURE);
    let first_video = resumed
        .as_chunks::<188>()
        .0
        .iter()
        .map(|packet| packet.as_slice())
        .find(|packet| ace_media::mpegts::ts_pid(packet) == 0x0100)
        .expect("resumed direct stream has a video keyframe");
    let first_video: &[u8; 188] = first_video.try_into().unwrap();
    assert!(
        ace_media::mpegts::ts_timing(first_video).discontinuity,
        "resumed direct TS must expose the discontinuity to the decoder"
    );
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
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&body).contains("\"test\""));
}

#[tokio::test]
async fn stream_ts_serves_mpegts_first_frame() {
    use futures::StreamExt;
    // Stream the real fixture from the start; the gate locks on its leading keyframe.
    // Keep `app` alive so the session/pump survives while we read the body.
    let app = router(fixture_state(0));
    let resp = app
        .clone()
        .oneshot(
            Request::get("/streams/fix/somechannel.ts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()[header::CONTENT_TYPE], "video/mp2t");
    assert!(!resp.headers().contains_key("icy-name"));
    // Live body is unbounded; read just the first TS chunk.
    let mut stream = resp.into_body().into_data_stream();
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first[0], 0x47);
    assert_eq!(
        ace_media::mpegts::read_sdt_service_name(&first).as_deref(),
        Some("Synthetic Demo Channel")
    );
}

#[tokio::test]
async fn stream_metadata_title_header_is_absent_without_metadata() {
    let resp = router(state())
        .oneshot(
            Request::get("/streams/test/somechannel.ts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert!(!resp.headers().contains_key("icy-name"));
}

#[tokio::test]
async fn ace_compat_routes_are_404_by_default() {
    for path in [
        "/ace/getstream?format=json&infohash=0123456789012345678901234567890123456789",
        "/ace/r/0123456789012345678901234567890123456789/outpace",
        "/ace/stat/0123456789012345678901234567890123456789/outpace",
        "/ace/cmd/0123456789012345678901234567890123456789/outpace?method=stop",
        "/server/api?method=get_version",
        "/ace/manifest.m3u8?infohash=0123456789012345678901234567890123456789",
        "/ace/m/0123456789012345678901234567890123456789/token.m3u8",
        "/ace/c/session/0.ts",
    ] {
        let resp = router(fixture_state(0))
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{path}");
    }
}

#[tokio::test]
async fn unknown_network_returns_404() {
    let resp = router(state())
        .oneshot(
            Request::get("/streams/nope/x.ts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn extensionless_stream_is_a_direct_mpegts_alias() {
    let st = state();
    let app = router(st.clone());

    let extensionless = app
        .clone()
        .oneshot(
            Request::get("/streams/test/channel")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(extensionless.status(), StatusCode::OK);
    assert_eq!(extensionless.headers()[header::CONTENT_TYPE], "video/mp2t");
    assert!(!extensionless.headers().contains_key(header::LOCATION));

    let explicit = app
        .oneshot(
            Request::get("/streams/test/channel.ts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(explicit.status(), StatusCode::OK);
    assert_eq!(
        st.manager.list().await,
        vec![(
            "test".to_string(),
            "channel".to_string(),
            2,
            StreamMetadata::default(),
        )]
    );
}

#[tokio::test]
async fn unsupported_dotted_suffixes_return_404_without_starting_sessions() {
    for path in [
        "/streams/test/x.mp4",
        "/streams/test/x.foo",
        "/streams/test/x.",
    ] {
        let st = state();
        let resp = router(st.clone())
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{path}");
        assert!(st.manager.list().await.is_empty(), "{path}");
    }
}

#[tokio::test]
async fn empty_ts_id_returns_404_without_starting_a_session() {
    let st = state();
    let resp = router(st.clone())
        .oneshot(
            Request::get("/streams/test/.ts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert!(st.manager.list().await.is_empty());
}

#[tokio::test]
async fn status_404_until_started_then_reports() {
    let st = fixture_state(0);
    let app = router(st.clone());
    // Not started yet.
    let resp = app
        .clone()
        .oneshot(
            Request::get("/streams/fix/chan/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    // Start it, then status reports it with clean keys.
    st.manager.get_or_start("fix", "chan").await.unwrap();
    let resp = app
        .oneshot(
            Request::get("/streams/fix/chan/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["metadata"]["bitrate"], 800_000);
    assert_eq!(value["metadata"]["title"], "Synthetic Demo Channel");
    assert_eq!(value["bitrate"], 0, "measured bitrate keeps its meaning");
}

#[tokio::test]
async fn m3u8_playlist_omits_icy_headers() {
    // An `icy-*` header on an HLS manifest makes VLC's HTTP access promote the URL to the
    // `icyx://` (Icecast) scheme and treat the body as a raw audio stream: the HLS demux
    // never loads, so VLC busy-loops the playlist and never fetches a segment. The manifest
    // must therefore carry no `icy-*` header even when the stream has a title (the title is
    // still exposed to clients via the JSON metadata endpoints). No `icy-*` header is emitted
    // on any path (direct `.ts` or HLS) any more; the title travels via the MPEG-TS SDT
    // `service_name` instead.
    let resp = router(fixture_state(0))
        .oneshot(
            Request::get("/streams/fix/chan.m3u8")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()[header::CONTENT_TYPE],
        "application/vnd.apple.mpegurl"
    );
    assert!(
        !resp
            .headers()
            .keys()
            .any(|k| k.as_str().starts_with("icy-")),
        "HLS manifest must not carry icy-* headers (breaks VLC HLS playback)"
    );
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let body = String::from_utf8_lossy(&body);
    assert!(body.starts_with("#EXTM3U"));
    assert!(body.lines().any(|line| !line.starts_with('#')));
}

#[tokio::test]
async fn m3u8_stalled_playlist_returns_retryable_503() {
    let response = stream_file_with_hls_timeout(
        State(stalled_hls_state()),
        Path(("stalled".to_string(), "chan.m3u8".to_string())),
        Duration::from_millis(1),
    )
    .await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()[header::RETRY_AFTER], "1");
}

#[tokio::test]
async fn normalization_http_native_terminal_failure_is_retryable_and_private() {
    let st = fixture_state(0);
    let app = router(st.clone());
    // Create the ordinary native entry, then place its media into a terminal enabled-mode
    // state to exercise the HTTP boundary without an FFmpeg dependency.
    let pkg = st
        .manager
        .get_or_start_hls("fix", "terminal")
        .await
        .unwrap();
    pkg.begin_generation(
        crate::hls::GenerationId(1),
        tokio::time::Instant::now() + Duration::from_secs(5),
        false,
    );
    pkg.fail_generation(
        crate::hls::GenerationId(1),
        crate::hls::HlsFailure::ChildExit,
    );

    let response = app
        .oneshot(
            Request::get("/streams/fix/terminal.m3u8")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()[header::RETRY_AFTER], "1");
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    assert!(
        body.is_empty(),
        "terminal details must not reach HTTP clients"
    );
    let retry = st
        .manager
        .get_or_start_hls("fix", "terminal")
        .await
        .unwrap();
    assert!(
        Arc::ptr_eq(&pkg, &retry),
        "HTTP retry must reuse the failed shared entry"
    );
}

#[tokio::test]
async fn normalization_http_starting_recovering_and_stopped_are_retryable() {
    for state in ["starting", "recovering", "stopped"] {
        let st = fixture_state(0);
        let pkg = st.manager.get_or_start_hls("fix", state).await.unwrap();
        match state {
            "starting" => {}
            "recovering" => pkg.begin_generation(
                crate::hls::GenerationId(1),
                tokio::time::Instant::now() + Duration::from_secs(5),
                true,
            ),
            "stopped" => pkg.stop(),
            _ => unreachable!(),
        }
        let response = super::super::await_hls_playlist(&pkg, Duration::ZERO, "/seg", true)
            .await
            .unwrap_err();
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "{state}"
        );
        assert_eq!(response.headers()[header::RETRY_AFTER], "1", "{state}");
        assert!(axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap()
            .is_empty());
    }
}

#[tokio::test]
async fn normalization_http_playlist_wait_returns_retryable_response_at_request_deadline() {
    let st = stalled_hls_state();
    let pkg = st
        .manager
        .get_or_start_hls("stalled", "deadline")
        .await
        .unwrap();
    pkg.begin_generation(
        crate::hls::GenerationId(u64::MAX - 1),
        tokio::time::Instant::now() + Duration::from_secs(30),
        false,
    );

    let response = super::super::await_hls_playlist(
        &pkg,
        Duration::from_millis(10),
        "/streams/stalled/deadline/seg",
        true,
    )
    .await
    .unwrap_err();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()[header::RETRY_AFTER], "1");
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    assert!(body.is_empty());
}

#[tokio::test]
async fn normalization_http_playlist_wait_wakes_promptly_on_terminal_failure() {
    let st = fixture_state(0);
    let pkg = st
        .manager
        .get_or_start_hls("fix", "terminal-wake")
        .await
        .unwrap();
    pkg.begin_generation(
        crate::hls::GenerationId(1),
        tokio::time::Instant::now() + Duration::from_secs(30),
        false,
    );

    let started = tokio::time::Instant::now();
    let waiter = tokio::spawn(async move {
        super::super::await_hls_playlist(
            &pkg,
            Duration::from_secs(10),
            "/streams/fix/terminal-wake/seg",
            true,
        )
        .await
    });
    tokio::task::yield_now().await;
    let pkg = st.manager.get_hls("fix", "terminal-wake").await.unwrap();
    pkg.fail_generation(
        crate::hls::GenerationId(1),
        crate::hls::HlsFailure::ChildExit,
    );

    let response = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("terminal transition must wake the playlist waiter within one second")
        .unwrap()
        .unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()[header::RETRY_AFTER], "1");
}

#[tokio::test]
async fn normalization_http_ready_to_recovery_cannot_serve_old_playlist_or_reset_deadline() {
    let st = fixture_state(0);
    let pkg = st
        .manager
        .get_or_start_hls("fix", "ready-recovery-race")
        .await
        .unwrap();
    let first = crate::hls::GenerationId(1);
    pkg.begin_generation(
        first,
        tokio::time::Instant::now() + Duration::from_secs(5),
        false,
    );
    // Turn the current generation Ready from the checked-in synthetic transport fixture.
    pkg.feed_generation(first, FIXTURE);
    assert_eq!(pkg.status(), crate::hls::HlsStatus::Ready);

    // Start a fresh Starting generation so the request first waits, then make it Ready
    // after a measurable portion of the original request budget has elapsed.
    let second = crate::hls::GenerationId(2);
    pkg.begin_generation(
        second,
        tokio::time::Instant::now() + Duration::from_secs(5),
        true,
    );
    let started = tokio::time::Instant::now();
    let hook_called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hook_was_called = hook_called.clone();
    let response = super::super::HLS_AFTER_READY
        .scope(
            Box::new(move |pkg: &crate::hls::HlsPackager| {
                hook_was_called.store(true, std::sync::atomic::Ordering::SeqCst);
                assert_eq!(pkg.status(), crate::hls::HlsStatus::Ready);
                pkg.begin_generation(
                    crate::hls::GenerationId(3),
                    tokio::time::Instant::now() + Duration::from_secs(5),
                    true,
                );
            }),
            async {
                // The task-local hook is only consulted after wait_available returns Ready.
                let ready = pkg.clone();
                let feed = tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    ready.feed_generation(second, FIXTURE);
                });
                let response = super::super::await_hls_playlist(
                    &pkg,
                    Duration::from_millis(300),
                    "/streams/fix/ready-recovery-race/seg",
                    true,
                )
                .await;
                feed.await.expect("feed task must complete");
                response
            },
        )
        .await
        .unwrap_err();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(hook_called.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(response.headers()[header::RETRY_AFTER], "1");
    assert!(axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap()
        .is_empty());
    assert!(started.elapsed() >= Duration::from_millis(280));
    assert!(started.elapsed() < Duration::from_millis(450));
}

#[tokio::test]
async fn delete_stops_running_session_then_404() {
    let st = state();
    let app = router(st.clone());
    st.manager.get_or_start("test", "z").await.unwrap();
    // First delete stops the running session.
    let resp = app
        .clone()
        .oneshot(
            Request::delete("/streams/test/z")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert!(st.manager.get("test", "z").await.is_none());
    // Second delete finds nothing to stop.
    let resp = app
        .oneshot(
            Request::delete("/streams/test/z")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn list_streams_reports_active() {
    let st = fixture_state(0);
    st.manager.get_or_start("fix", "abc").await.unwrap();
    let resp = router(st)
        .oneshot(Request::get("/streams").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["streams"][0]["id"], "abc");
    assert_eq!(
        value["streams"][0]["metadata"],
        json!({ "title": "Synthetic Demo Channel", "bitrate": 800_000, "categories": ["sports"] })
    );
}

struct NativeAceFixture;

#[async_trait]
impl StreamProvider for NativeAceFixture {
    fn network(&self) -> &'static str {
        "ace"
    }
    async fn open(&self, id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        if id != UNRESOLVED_IH && id != format!("cid:{UNRESOLVED_IH}") {
            return Err(ProviderError::NotFound);
        }
        Ok(Box::new(PacedFixtureSource { pos: 0 }))
    }
}

fn native_ace_fixture_state() -> AppState {
    let mut st = fixture_state(0);
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(NativeAceFixture));
    st.manager = StreamManager::with_config(
        registry,
        256,
        crate::config::HlsConfig {
            startup_segments: 1,
            segment_duration_ms: 1000,
            ..crate::config::HlsConfig::default()
        },
    );
    st.networks = vec!["ace".into()];
    st
}

#[tokio::test]
async fn native_content_id_aliases_share_playback_status_and_delete() {
    let st = native_ace_fixture_state();
    let app = router(st.clone());
    let upper = UNRESOLVED_IH.to_ascii_uppercase();
    let canonical = format!("cid:{UNRESOLVED_IH}");
    let mut responses = Vec::new();
    for id in [upper.clone(), canonical.clone()] {
        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/streams/ace/{id}.ts"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        responses.push(response);
    }
    assert_eq!(st.manager.list().await.len(), 1);
    assert!(st.manager.get("ace", &canonical).await.is_some());
    for id in [upper.clone(), format!("cid:{upper}")] {
        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/streams/ace/{id}/status"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let status: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(status["id"], canonical);
        assert_eq!(status["clients"], 2);
    }
    let response = app
        .oneshot(
            Request::delete(format!("/streams/ace/{upper}.ts"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(st.manager.get("ace", &canonical).await.is_none());
    drop(responses);
}

#[tokio::test]
async fn native_content_id_hls_aliases_share_segments_and_delete() {
    let st = native_ace_fixture_state();
    let app = router(st.clone());
    let upper = UNRESOLVED_IH.to_ascii_uppercase();
    let canonical = format!("cid:{UNRESOLVED_IH}");
    let response = app
        .clone()
        .oneshot(
            Request::get(format!("/streams/ace/{upper}.m3u8"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let playlist = body_text(response).await;
    let segment = playlist_segment_path(&playlist);
    assert!(segment.starts_with(&format!("/streams/ace/{canonical}/seg/")));
    let alias_segment = segment.replace(&canonical, &upper);
    let mut bodies = Vec::new();
    for path in [segment, alias_segment.as_str()] {
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        bodies.push(response_bytes(response).await);
    }
    assert_eq!(bodies[0], bodies[1]);
    let response = app
        .oneshot(
            Request::delete(format!("/streams/ace/cid:{upper}.m3u8"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(st.manager.get("ace", &canonical).await.is_none());
}

#[tokio::test]
async fn native_infohash_hls_emits_roundtrippable_explicit_urls() {
    let st = native_ace_fixture_state();
    let app = router(st);
    let explicit = format!("ih:{UNRESOLVED_IH}");
    let response = app
        .clone()
        .oneshot(
            Request::get(format!("/streams/ace/{explicit}.m3u8"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let playlist = body_text(response).await;
    let path = playlist_segment_path(&playlist);
    assert!(path.starts_with(&format!("/streams/ace/{explicit}/seg/")));
    let response = app
        .clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .clone()
        .oneshot(
            Request::get(format!("/streams/ace/{explicit}/status"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
    assert_eq!(status["id"], explicit);
    let response = app
        .oneshot(Request::get("/streams").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
    assert_eq!(list["streams"][0]["id"], explicit);
}

#[tokio::test]
async fn native_malformed_explicit_identifiers_do_not_start_sessions() {
    let st = native_ace_fixture_state();
    let app = router(st.clone());
    for id in ["cid:bad", "ih:bad"] {
        for path in [
            format!("/streams/ace/{id}"),
            format!("/streams/ace/{id}.m3u8"),
            format!("/streams/ace/{id}/status"),
            format!("/streams/ace/{id}/seg/0.ts"),
        ] {
            let response = app
                .clone()
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
        let response = app
            .clone()
            .oneshot(
                Request::delete(format!("/streams/ace/{id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    assert!(st.manager.list().await.is_empty());
}

#[tokio::test]
async fn native_explicit_infohash_uses_a_separate_session_key() {
    let st = native_ace_fixture_state();
    let app = router(st.clone());
    let upper = UNRESOLVED_IH.to_ascii_uppercase();
    let response = app
        .clone()
        .oneshot(
            Request::get(format!("/streams/ace/ih:{upper}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(st.manager.get("ace", UNRESOLVED_IH).await.is_some());
    assert!(st
        .manager
        .get("ace", &format!("cid:{UNRESOLVED_IH}"))
        .await
        .is_none());
    let status = app
        .clone()
        .oneshot(
            Request::get(format!("/streams/ace/ih:{upper}/status"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);
    let deleted = app
        .oneshot(
            Request::delete(format!("/streams/ace/ih:{upper}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
}

const UNRESOLVED_IH: &str = "0123456789abcdef0123456789abcdef01234567";

/// An empty index and cached synthetic lookup failure keep native error tests offline.
fn ace_provider_state() -> AppState {
    let mut registry = ProviderRegistry::new();
    let provider = crate::ace_provider::AceProvider::new(
        Arc::new(ace_wire::identity::Identity::generate()),
        0,
    );
    provider.cache_infohash_failure_for_test(UNRESOLVED_IH);
    registry.register(Arc::new(provider));
    AppState {
        manager: StreamManager::new(registry),
        networks: vec!["ace".into()],
        resolve_content_ids_in_getstream: false,
        ace_sessions: Arc::new(AceSessionStore::default()),
        experimental_ace_compat: true,
        broadcasts: None,
    }
}

#[tokio::test]
async fn native_routes_refuse_an_explicit_infohash_without_a_descriptor() {
    for path in [
        format!("/streams/ace/ih:{UNRESOLVED_IH}"),
        format!("/streams/ace/ih:{UNRESOLVED_IH}.ts"),
        format!("/streams/ace/ih:{UNRESOLVED_IH}.m3u8"),
    ] {
        let resp = router(ace_provider_state())
            .oneshot(Request::get(path.as_str()).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY, "{path}");
        let content_type = resp.headers()[header::CONTENT_TYPE].to_str().unwrap();
        assert!(
            content_type.starts_with("text/plain"),
            "{path}: {content_type}"
        );
        let body = body_text(resp).await;
        assert!(
            body.contains(&format!("cid:{UNRESOLVED_IH}")),
            "{path}: {body}"
        );
    }
}

#[tokio::test]
async fn compat_routes_admit_cold_infohashes_for_resolution_at_playback_open() {
    let state = ace_provider_state();
    let sessions = state.ace_sessions.clone();
    let app = router(state);
    for query in [
        format!("/ace/getstream?format=json&infohash={UNRESOLVED_IH}"),
        format!("/ace/getstream?format=json&magnet=magnet%3A%3Fxt%3Durn%3Abtih%3A{UNRESOLVED_IH}"),
    ] {
        let resp = app
            .clone()
            .oneshot(Request::get(query.as_str()).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{query}");
        let json: serde_json::Value = serde_json::from_str(&body_text(resp).await).unwrap();
        assert!(json["error"].is_null(), "{query}: {json}");
        assert_eq!(json["response"]["infohash"], UNRESOLVED_IH);
        assert!(json["response"]["playback_url"].is_string());
    }
    assert!(
        sessions.active_count() > 0,
        "cold selectors can mint playback leases"
    );
}

#[tokio::test]
async fn compat_manifest_requires_successful_cold_lookup_before_minting_urls() {
    let state = ace_provider_state();
    let sessions = state.ace_sessions.clone();
    let response = router(state)
        .oneshot(
            Request::get(format!(
                "/ace/manifest.m3u8?format=json&infohash={UNRESOLVED_IH}"
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(sessions.active_count(), 0);
}

#[tokio::test]
async fn compat_getstream_mints_urls_for_an_infohash_with_a_verified_descriptor() {
    let state = ace_provider_state();
    let (transport, _) = crate::ace_provider::test_support::synthetic_live_transport(524_288);
    let info = ace_swarm::resolve::stream_info_from_transport(&transport).unwrap();
    state.manager.remember_live_descriptor("ace", &info);
    let ih = infohash_hex(&info.infohash);
    // JSON mode only mints URLs; it never opens the stream, so this stays offline.
    let resp = router(state)
        .oneshot(
            Request::get(format!("/ace/getstream?format=json&infohash={ih}").as_str())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_str(&body_text(resp).await).unwrap();
    assert!(json["error"].is_null(), "{json}");
    assert_eq!(json["response"]["infohash"], ih);
}
