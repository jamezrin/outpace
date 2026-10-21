use super::*;

#[tokio::test]
async fn ace_manifest_redirects_to_stable_authenticated_native_hls_view() {
    let id = "0123456789abcdef0123456789abcdef01234567";
    let state = ace_hls_state(Duration::from_secs(60), 8);
    let manager = state.manager.clone();
    let app = router(state);

    let start = app
        .clone()
        .oneshot(
            Request::get(format!("/ace/manifest.m3u8?infohash={id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(start.status(), StatusCode::FOUND);
    assert_eq!(start.headers()[header::CACHE_CONTROL], "no-store");
    let playback = start.headers()[header::LOCATION]
        .to_str()
        .unwrap()
        .strip_prefix("http://127.0.0.1:6878")
        .unwrap()
        .to_owned();
    assert!(playback.starts_with(&format!("/ace/m/{id}/")));

    let playlist = wait_for_hls_playlist(&app, &playback).await;
    let segment_path = playlist_segment_path(&playlist).to_owned();
    assert!(segment_path.starts_with("/ace/c/"));
    assert_eq!(segment_path.split('/').nth(3).unwrap().len(), 64);
    assert_eq!(
        manager.get("hlsfix", id).await.unwrap().subscriber_count(),
        1,
        "one HLS lease pins one native session subscriber"
    );

    let compat_segment = app
        .clone()
        .oneshot(Request::get(&segment_path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(compat_segment.status(), StatusCode::OK);
    assert_eq!(compat_segment.headers()[header::CONTENT_TYPE], "video/mp2t");
    assert_eq!(compat_segment.headers()[header::CACHE_CONTROL], "no-store");
}

#[tokio::test]
async fn ace_hls_cold_playlist_200_contains_a_segment() {
    let id = "0123456789abcdef0123456789abcdef01234567";
    let app = router(ace_hls_state(Duration::from_secs(60), 8));
    let start = app
        .clone()
        .oneshot(
            Request::get(format!("/ace/manifest.m3u8?infohash={id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let playback = start.headers()[header::LOCATION]
        .to_str()
        .unwrap()
        .strip_prefix("http://127.0.0.1:6878")
        .unwrap();

    let response = app
        .oneshot(Request::get(playback).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/vnd.apple.mpegurl"
    );
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    let body = response_text(response).await;
    assert!(body.lines().any(|line| !line.starts_with('#')));
}

#[tokio::test]
async fn ace_hls_stalled_playlist_returns_retryable_503() {
    let id = "0123456789abcdef0123456789abcdef01234567";
    let state = stalled_hls_state();
    let app = router(state.clone());
    let (playback_id, manifest, _) = mint_stalled_hls(&app, id).await;

    let response = ace_hls_playback_with_timeout(
        State(state),
        Path((playback_id, manifest)),
        Duration::from_millis(1),
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()[header::RETRY_AFTER], "1");
}

#[tokio::test]
async fn normalization_http_compat_terminal_failure_is_retryable_and_private() {
    let id = "0123456789abcdef0123456789abcdef01234567";
    let state = ace_hls_state(Duration::from_secs(60), 8);
    let manager = state.manager.clone();
    let request_state = state.clone();
    let app = router(state);
    let (playback_id, manifest, _) = mint_stalled_hls(&app, id).await;
    // Fail the same media entry returned to the authenticated compatibility route.
    let pkg = manager.get_hls("hlsfix", id).await.unwrap();
    pkg.begin_generation(
        crate::hls::GenerationId(1),
        tokio::time::Instant::now() + Duration::from_secs(5),
        false,
    );
    pkg.fail_generation(
        crate::hls::GenerationId(1),
        crate::hls::HlsFailure::OutputInvalid,
    );

    let response = ace_hls_playback_with_timeout(
        State(request_state),
        Path((playback_id, manifest)),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()[header::RETRY_AFTER], "1");
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    assert!(body.is_empty(), "failure category must not be exposed");
    let retry = manager.get_or_start_hls("hlsfix", id).await.unwrap();
    assert!(
        Arc::ptr_eq(&pkg, &retry),
        "compatibility retry must reuse the failed shared entry"
    );
}

#[tokio::test]
async fn ace_hls_stop_cancels_a_stalled_playlist_wait_with_404() {
    let id = "0123456789abcdef0123456789abcdef01234567";
    let state = stalled_hls_state();
    let app = router(state.clone());
    let (playback_id, manifest, stop_path) = mint_stalled_hls(&app, id).await;
    let manager = state.manager.clone();
    let request = spawn_stalled_hls_request(state.clone(), playback_id, manifest);
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(
        !request.is_finished(),
        "playlist request must be waiting for readiness"
    );
    let (_, key, _, _) = manager.list().await.into_iter().next().unwrap();
    let pkg = manager.get_hls("stalled", &key).await.unwrap();
    pkg.begin_generation(
        crate::hls::GenerationId(1),
        tokio::time::Instant::now() + Duration::from_secs(5),
        false,
    );
    pkg.fail_generation(
        crate::hls::GenerationId(1),
        crate::hls::HlsFailure::ChildExit,
    );

    let stopped = app
        .oneshot(Request::get(stop_path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(stopped.status(), StatusCode::OK);
    let response = tokio::time::timeout(Duration::from_millis(100), request)
        .await
        .expect("revocation must wake the playlist wait")
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn ace_hls_expiry_cancels_a_stalled_playlist_wait_with_404() {
    let id = "0123456789abcdef0123456789abcdef01234567";
    let state = stalled_hls_state_with(Duration::from_millis(100), 8);
    let app = router(state.clone());
    let (playback_id, manifest, _) = mint_stalled_hls(&app, id).await;
    let request = spawn_stalled_hls_request(state.clone(), playback_id, manifest);
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(
        !request.is_finished(),
        "playlist request must be waiting for readiness"
    );
    let response = tokio::time::timeout(Duration::from_millis(100), request)
        .await
        .expect("expiry must wake the playlist wait")
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "current_thread")]
async fn ace_hls_expiry_wins_when_terminal_failure_is_due_before_waiter_resumes() {
    let id = "2123456789abcdef0123456789abcdef01234567";
    let state = stalled_hls_state_with(Duration::from_millis(30), 8);
    let app = router(state.clone());
    let (playback_id, manifest, _) = mint_stalled_hls(&app, id).await;
    let request = spawn_stalled_hls_request(state.clone(), playback_id, manifest);
    tokio::task::yield_now().await;
    assert!(!request.is_finished(), "playlist request must be pending");

    // Keep the current-thread executor from polling the waiter while both the lease expiry
    // and the failure transition become due. On resumption the request must revalidate its
    // expired authorization before it can expose the packager's 503.
    std::thread::sleep(Duration::from_millis(45));
    let (_, key, _, _) = state.manager.list().await.into_iter().next().unwrap();
    let pkg = state.manager.get_hls("stalled", &key).await.unwrap();
    pkg.begin_generation(
        crate::hls::GenerationId(1),
        tokio::time::Instant::now() + Duration::from_secs(5),
        false,
    );
    pkg.fail_generation(
        crate::hls::GenerationId(1),
        crate::hls::HlsFailure::ChildExit,
    );

    let response = request.await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "current_thread")]
async fn ace_hls_manager_removal_wins_over_terminal_failure_with_valid_lease() {
    let id = "3123456789abcdef0123456789abcdef01234567";
    let state = stalled_hls_state_with(Duration::from_secs(60), 8);
    let app = router(state.clone());
    let (playback_id, manifest, _) = mint_stalled_hls(&app, id).await;
    let request = spawn_stalled_hls_request(state.clone(), playback_id, manifest);
    tokio::task::yield_now().await;
    assert!(!request.is_finished(), "playlist waiter must be pending");

    let (_, key, _, _) = state.manager.list().await.into_iter().next().unwrap();
    let pkg = state.manager.get_hls("stalled", &key).await.unwrap();
    pkg.begin_generation(
        crate::hls::GenerationId(1),
        tokio::time::Instant::now() + Duration::from_secs(5),
        false,
    );
    pkg.fail_generation(
        crate::hls::GenerationId(1),
        crate::hls::HlsFailure::ChildExit,
    );
    assert!(state.manager.stop("stalled", &key).await);

    let response = request.await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn ace_hls_capacity_eviction_cancels_a_stalled_playlist_wait_with_404() {
    let first_id = "0123456789abcdef0123456789abcdef01234567";
    let second_id = "1123456789abcdef0123456789abcdef01234567";
    let state = stalled_hls_state_with(Duration::from_secs(60), 1);
    let app = router(state.clone());
    let (playback_id, manifest, _) = mint_stalled_hls(&app, first_id).await;
    let manager = state.manager.clone();
    let request = spawn_stalled_hls_request(state.clone(), playback_id, manifest);
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(
        !request.is_finished(),
        "playlist request must be waiting for readiness"
    );
    let (_, key, _, _) = manager.list().await.into_iter().next().unwrap();
    let pkg = manager.get_hls("stalled", &key).await.unwrap();
    pkg.begin_generation(
        crate::hls::GenerationId(1),
        tokio::time::Instant::now() + Duration::from_secs(5),
        false,
    );
    pkg.fail_generation(
        crate::hls::GenerationId(1),
        crate::hls::HlsFailure::ChildExit,
    );

    let _newer_lease = mint_stalled_hls(&app, second_id).await;
    let response = tokio::time::timeout(Duration::from_millis(100), request)
        .await
        .expect("capacity eviction must wake the playlist wait")
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn ace_hls_playlist_and_segment_reads_do_not_refresh_native_activity() {
    let id = "0123456789abcdef0123456789abcdef01234567";
    let state = ace_hls_state(Duration::from_secs(60), 8);
    let manager = state.manager.clone();
    let app = router(state);

    let start = app
        .clone()
        .oneshot(
            Request::get(format!("/ace/manifest.m3u8?infohash={id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let playback = start.headers()[header::LOCATION]
        .to_str()
        .unwrap()
        .strip_prefix("http://127.0.0.1:6878")
        .unwrap()
        .to_owned();
    let initial_playlist = wait_for_hls_playlist(&app, &playback).await;
    let segment_path = playlist_segment_path(&initial_playlist).to_owned();
    let pkg = manager.get_hls("hlsfix", id).await.unwrap();
    let stale = Instant::now() - Duration::from_secs(60);

    pkg.set_last_access_for_test(stale);
    let playlist = app
        .clone()
        .oneshot(Request::get(&playback).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(playlist.status(), StatusCode::OK);
    assert_eq!(pkg.last_access_for_test(), Some(stale));

    let segment = app
        .clone()
        .oneshot(Request::get(&segment_path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(segment.status(), StatusCode::OK);
    assert_eq!(pkg.last_access_for_test(), Some(stale));
}

#[tokio::test]
async fn live_hls_segment_carries_sdt_service_name() {
    // The `.ts` path is covered by several HTTP round-trip tests asserting the SDT; this is
    // the HLS-path equivalent, driven end-to-end through the actual served segment bytes
    // rather than at the `HlsPackager` unit level.
    let id = "0123456789abcdef0123456789abcdef01234567";
    let state = ace_hls_state(Duration::from_secs(60), 8);
    let manager = state.manager.clone();
    let app = router(state);

    let start = app
        .clone()
        .oneshot(
            Request::get(format!("/ace/manifest.m3u8?infohash={id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let playback = start.headers()[header::LOCATION]
        .to_str()
        .unwrap()
        .strip_prefix("http://127.0.0.1:6878")
        .unwrap()
        .to_owned();
    let playlist = wait_for_hls_playlist(&app, &playback).await;
    let segment_path = playlist_segment_path(&playlist).to_owned();

    let seg = app
        .clone()
        .oneshot(Request::get(&segment_path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(seg.status(), StatusCode::OK);
    let body = axum::body::to_bytes(seg.into_body(), 1 << 20)
        .await
        .unwrap();
    assert_eq!(
        ace_media::mpegts::read_sdt_service_name(&body).as_deref(),
        Some("Synthetic Demo Channel")
    );
    // Keep `app`/`manager` alive through the reads above, mirroring the other live-HLS tests.
    drop((app, manager));
}

#[tokio::test]
async fn ace_manifest_json_returns_documented_hls_control_urls() {
    let id = "0123456789abcdef0123456789abcdef01234567";
    let app = router(ace_hls_state(Duration::from_secs(60), 8));
    let response = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/ace/manifest.m3u8?format=json&id={id}&use_api_events=1"
            ))
            .header(header::HOST, "localhost:6900")
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let value: serde_json::Value = serde_json::from_str(&response_text(response).await).unwrap();
    assert_eq!(value["error"], serde_json::Value::Null);
    assert_eq!(value["response"]["infohash"], id);
    assert_eq!(value["response"]["is_live"], 1);
    assert_eq!(
        value["response"]["metadata"],
        json!({ "title": "Synthetic Demo Channel", "bitrate": 800_000, "categories": ["sports"] })
    );
    let token = value["response"]["playback_session_id"].as_str().unwrap();
    assert_eq!(token.len(), 64);
    let playback = value["response"]["playback_url"]
        .as_str()
        .unwrap()
        .strip_prefix("http://localhost:6900")
        .unwrap();
    assert!(playback.starts_with(&format!("/ace/m/cid:{id}/")));
    assert!(playback.ends_with(".m3u8"));
    assert_eq!(
        value["response"]["stat_url"],
        format!("http://localhost:6900/ace/stat/cid:{id}/{token}")
    );
    assert_eq!(
        value["response"]["command_url"],
        format!("http://localhost:6900/ace/cmd/cid:{id}/{token}")
    );
    assert!(wait_for_hls_playlist(&app, playback)
        .await
        .contains("/ace/c/"));
}

#[tokio::test]
async fn ace_and_native_hls_return_the_same_retained_segment_bytes() {
    let id = "0123456789abcdef0123456789abcdef01234567";
    let state = ace_hls_state(Duration::from_secs(60), 8);
    let manager = state.manager.clone();
    let app = router(state);

    let start = app
        .clone()
        .oneshot(
            Request::get(format!("/ace/manifest.m3u8?infohash={id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let playback = start.headers()[header::LOCATION]
        .to_str()
        .unwrap()
        .strip_prefix("http://127.0.0.1:6878")
        .unwrap();
    let playlist = wait_for_hls_playlist(&app, playback).await;
    let compat_path = playlist_segment_path(&playlist);
    let compat_packager = manager.get_hls("hlsfix", id).await.unwrap();
    let native_manifest = app
        .clone()
        .oneshot(
            Request::get(format!("/streams/hlsfix/{id}.m3u8"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(native_manifest.status(), StatusCode::OK);
    assert!(Arc::ptr_eq(
        &compat_packager,
        &manager.get_hls("hlsfix", id).await.unwrap()
    ));
    let seq = compat_path
        .rsplit('/')
        .next()
        .unwrap()
        .strip_suffix(".ts")
        .unwrap();
    let compat = app
        .clone()
        .oneshot(Request::get(compat_path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let native = app
        .clone()
        .oneshot(
            Request::get(format!("/streams/hlsfix/{id}/seg/{seq}.ts"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(compat.status(), StatusCode::OK);
    assert_eq!(native.status(), StatusCode::OK);
    let retained = response_bytes(compat).await;
    assert_eq!(retained, response_bytes(native).await);

    // A generation fence suppresses playlist availability while completed segments remain
    // retrievable until normal window eviction or teardown.
    compat_packager.begin_generation(
        crate::hls::GenerationId(1),
        tokio::time::Instant::now() + Duration::from_secs(30),
        true,
    );
    let recovering_segment = app
        .clone()
        .oneshot(Request::get(compat_path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(recovering_segment.status(), StatusCode::OK);
    assert_eq!(response_bytes(recovering_segment).await, retained);
    compat_packager.fail_generation(
        crate::hls::GenerationId(1),
        crate::hls::HlsFailure::ChildExit,
    );
    let failed_segment = app
        .clone()
        .oneshot(Request::get(compat_path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(failed_segment.status(), StatusCode::OK);
    assert_eq!(response_bytes(failed_segment).await, retained);
    assert_eq!(manager.list().await.len(), 1);
    assert!(manager.get_hls("hlsfix", id).await.is_some());
}

#[tokio::test]
async fn ace_manifest_rejects_unknown_format_without_starting_work() {
    let id = "0123456789abcdef0123456789abcdef01234567";
    let state = ace_hls_state(Duration::from_secs(60), 8);
    let manager = state.manager.clone();
    let app = router(state);
    let response = app
        .oneshot(
            Request::get(format!(
                "/ace/manifest.m3u8?format=mp4&infohash={id}&transcode_audio=1"
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let value: serde_json::Value = serde_json::from_str(&response_text(response).await).unwrap();
    assert_eq!(value["response"], serde_json::Value::Null);
    assert_eq!(value["error"], "unsupported format");
    assert!(manager.list().await.is_empty());
    assert!(manager.get_hls("hlsfix", id).await.is_none());
}

#[tokio::test]
async fn ace_hls_stop_revokes_only_one_client_pin() {
    let id = "0123456789abcdef0123456789abcdef01234567";
    let state = ace_hls_state(Duration::from_secs(60), 8);
    let manager = state.manager.clone();
    let app = router(state);

    async fn mint(app: &Router, id: &str) -> serde_json::Value {
        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/ace/manifest.m3u8?format=json&infohash={id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        serde_json::from_str(&response_text(response).await).unwrap()
    }
    let a = mint(&app, id).await;
    let b = mint(&app, id).await;
    assert_eq!(
        manager.get("hlsfix", id).await.unwrap().subscriber_count(),
        2
    );

    let stop_a = a["response"]["command_url"]
        .as_str()
        .unwrap()
        .strip_prefix("http://127.0.0.1:6878")
        .unwrap();
    let stopped = app
        .clone()
        .oneshot(
            Request::get(format!("{stop_a}?method=stop"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stopped.status(), StatusCode::OK);
    assert_eq!(
        manager.get("hlsfix", id).await.unwrap().subscriber_count(),
        1
    );

    let a_playback = a["response"]["playback_url"]
        .as_str()
        .unwrap()
        .strip_prefix("http://127.0.0.1:6878")
        .unwrap();
    assert_eq!(
        app.clone()
            .oneshot(Request::get(a_playback).body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let b_playback = b["response"]["playback_url"]
        .as_str()
        .unwrap()
        .strip_prefix("http://127.0.0.1:6878")
        .unwrap();
    assert!(wait_for_hls_playlist(&app, b_playback)
        .await
        .contains(".ts"));
}

#[tokio::test]
async fn ace_hls_churn_keeps_one_reaper_and_capacity_bounded_pins() {
    let id = "0123456789abcdef0123456789abcdef01234567";
    let state = ace_hls_state(Duration::from_secs(60), 4);
    let manager = state.manager.clone();
    let sessions = state.ace_sessions.clone();
    let workers = sessions.hls_reaper_workers();
    let app = router(state);
    let mut tokens = Vec::new();

    for _ in 0..100 {
        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/ace/manifest.m3u8?infohash={id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FOUND);
        let token = response.headers()[header::LOCATION]
            .to_str()
            .unwrap()
            .rsplit('/')
            .next()
            .unwrap()
            .strip_suffix(".m3u8")
            .unwrap()
            .to_owned();
        tokens.push(token);
    }

    assert_eq!(workers.load(Ordering::SeqCst), 1);
    assert_eq!(sessions.active_count(), 4);
    assert_eq!(
        manager.get("hlsfix", id).await.unwrap().subscriber_count(),
        4
    );
    for token in tokens.iter().rev().take(4) {
        assert!(sessions.revoke(id, token));
    }
    assert_eq!(sessions.active_count(), 0);
    assert_eq!(
        manager.get("hlsfix", id).await.unwrap().subscriber_count(),
        0
    );

    drop(app);
    drop(sessions);
    for _ in 0..20 {
        if workers.load(Ordering::SeqCst) == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        workers.load(Ordering::SeqCst),
        0,
        "the weak-reference store reaper must exit when its store is dropped"
    );
}

#[tokio::test]
async fn ace_hls_rejects_forged_future_evicted_and_expired_segments() {
    let id = "0123456789abcdef0123456789abcdef01234567";
    let state = ace_hls_state(Duration::from_millis(30), 1);
    let manager = state.manager.clone();
    let app = router(state);

    let first = app
        .clone()
        .oneshot(
            Request::get(format!("/ace/manifest.m3u8?infohash={id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let first_playback = first.headers()[header::LOCATION]
        .to_str()
        .unwrap()
        .strip_prefix("http://127.0.0.1:6878")
        .unwrap()
        .to_owned();
    let first_playlist = wait_for_hls_playlist(&app, &first_playback).await;
    let first_segment = playlist_segment_path(&first_playlist).to_owned();
    assert_eq!(
        app.clone()
            .oneshot(
                Request::get(
                    "/ace/c/ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff/0.ts"
                )
                .body(Body::empty())
                .unwrap()
            )
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let token = first_segment.split('/').nth(3).unwrap();
    assert_eq!(
        app.clone()
            .oneshot(
                Request::get(format!("/ace/c/{token}/18446744073709551615.ts"))
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );

    // Capacity one: a second client evicts and drops the first client's pin.
    let second = app
        .clone()
        .oneshot(
            Request::get(format!("/ace/manifest.m3u8?infohash={id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::FOUND);
    assert_eq!(
        manager.get("hlsfix", id).await.unwrap().subscriber_count(),
        1
    );
    assert_eq!(
        app.clone()
            .oneshot(Request::get(&first_segment).body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );

    let second_playback = second.headers()[header::LOCATION]
        .to_str()
        .unwrap()
        .strip_prefix("http://127.0.0.1:6878")
        .unwrap()
        .to_owned();
    let second_playlist = wait_for_hls_playlist(&app, &second_playback).await;
    let second_segment = playlist_segment_path(&second_playlist).to_owned();

    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(
        app.clone()
            .oneshot(Request::get(&second_playback).body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        app.clone()
            .oneshot(Request::get(&second_segment).body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        manager.get("hlsfix", id).await.unwrap().subscriber_count(),
        0
    );
}

#[tokio::test]
async fn ace_hls_returns_404_after_a_segment_leaves_the_native_window() {
    let id = "0123456789abcdef0123456789abcdef01234567";
    let app = router(ace_hls_state(Duration::from_secs(60), 8));
    let start = app
        .clone()
        .oneshot(
            Request::get(format!("/ace/manifest.m3u8?infohash={id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let playback = start.headers()[header::LOCATION]
        .to_str()
        .unwrap()
        .strip_prefix("http://127.0.0.1:6878")
        .unwrap();
    let first = wait_for_hls_playlist(&app, playback).await;
    let old_segment = playlist_segment_path(&first).to_owned();
    let old_seq = old_segment
        .rsplit('/')
        .next()
        .unwrap()
        .strip_suffix(".ts")
        .unwrap()
        .parse::<u64>()
        .unwrap();

    for _ in 0..100 {
        let playlist = wait_for_hls_playlist(&app, playback).await;
        let media_seq = playlist
            .lines()
            .find_map(|line| line.strip_prefix("#EXT-X-MEDIA-SEQUENCE:"))
            .unwrap()
            .parse::<u64>()
            .unwrap();
        if media_seq > old_seq {
            break;
        }
        tokio::time::sleep(Duration::from_millis(3)).await;
    }

    assert_eq!(
        app.oneshot(Request::get(old_segment).body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
}
