//! Per-viewer retained-HLS replay (timeshift) over the compatibility API. Adapted from
//! the open-ott-play/outpace-acestream timeshift suite.
use super::*;

const ID: &str = "0123456789abcdef0123456789abcdef01234567";

fn history_state() -> AppState {
    history_state_with(crate::config::HlsHistoryConfig {
        max_seconds: 600,
        max_bytes: 4 * 1024 * 1024,
        global_max_bytes: 8 * 1024 * 1024,
    })
}

fn history_state_with(history: crate::config::HlsHistoryConfig) -> AppState {
    let mut state = ace_hls_state(Duration::from_secs(60), 8);
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(PacedPacketProvider));
    state.manager = StreamManager::with_history_config(
        registry,
        32,
        crate::config::HlsConfig {
            segment_packets: 4,
            window_segments: 3,
            segment_duration_ms: 1000,
            startup_segments: 1,
            ..crate::config::HlsConfig::default()
        },
        history,
    );
    state
}

async fn request(app: &Router, path: &str) -> Response {
    app.clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn json_request(app: &Router, path: &str) -> serde_json::Value {
    let response = request(app, path).await;
    assert_eq!(response.status(), StatusCode::OK);
    if path.starts_with("/ace/cmd/") {
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    }
    serde_json::from_slice(&response_bytes(response).await).unwrap()
}

fn local_path(url: &str) -> &str {
    url.strip_prefix("http://127.0.0.1:6878").unwrap()
}

async fn start(app: &Router) -> (String, String, String) {
    let json = json_request(
        app,
        &format!("/ace/manifest.m3u8?infohash={ID}&format=json"),
    )
    .await;
    let response = &json["response"];
    (
        local_path(response["playback_url"].as_str().unwrap()).into(),
        local_path(response["command_url"].as_str().unwrap()).into(),
        response["playback_session_id"].as_str().unwrap().into(),
    )
}

async fn wait_history(manager: &StreamManager) {
    let pkg = manager.get_hls("hlsfix", ID).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if pkg.prepare_history(Duration::from_secs(20)).is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("synthetic history becomes available");
}

#[tokio::test]
async fn replay_is_finite_generation_scoped_and_independent_of_other_viewers() {
    let state = history_state();
    let manager = state.manager.clone();
    let app = router(state);
    let (live_a, command_a, token_a) = start(&app).await;
    let (live_b, _, _) = start(&app).await;
    wait_history(&manager).await;
    let old_playlist = wait_for_hls_playlist(&app, &live_a).await;
    let old_segment = playlist_segment_path(&old_playlist).to_owned();
    let seek = json_request(
        &app,
        &format!("{command_a}?method=timeshift_seek&back=12&generation=0"),
    )
    .await;
    assert!(seek["error"].is_null(), "{seek}");
    assert_eq!(seek["response"]["view"]["generation"], 1);
    assert_eq!(seek["response"]["view"]["mode"], "replay");
    assert_eq!(seek["response"]["reload_required"], true);
    let replay = local_path(seek["response"]["playback_url"].as_str().unwrap());
    let playlist = response_text(request(&app, replay).await).await;
    assert!(playlist.contains("#EXT-X-ENDLIST"));
    let first = seek["response"]["view"]["snapshot"]["first_seq"]
        .as_u64()
        .unwrap();
    let last = seek["response"]["view"]["snapshot"]["last_seq"]
        .as_u64()
        .unwrap();
    assert_eq!(
        request(&app, playlist_segment_path(&playlist))
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        request(&app, &format!("/ace/tc/{token_a}/1/{}.ts", last + 1))
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    if first > 0 {
        assert_eq!(
            request(&app, &format!("/ace/tc/{token_a}/1/{}.ts", first - 1))
                .await
                .status(),
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(request(&app, &live_a).await.status(), StatusCode::GONE);
    assert_eq!(request(&app, &old_segment).await.status(), StatusCode::GONE);
    let other = wait_for_hls_playlist(&app, &live_b).await;
    assert!(!other.contains("#EXT-X-ENDLIST"));
    let other_first = other
        .lines()
        .find_map(|line| line.strip_prefix("#EXT-X-MEDIA-SEQUENCE:"))
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert!(
        first < other_first,
        "replay actually precedes the live playlist"
    );
    assert_eq!(
        manager.get("hlsfix", ID).await.unwrap().subscriber_count(),
        2
    );

    for back in ["NaN", "inf", "-1", "0", "31536001", "10000"] {
        let failed = json_request(
            &app,
            &format!("{command_a}?method=timeshift_seek&back={back}"),
        )
        .await;
        assert!(!failed["error"].is_null(), "{failed}");
        assert_eq!(request(&app, replay).await.status(), StatusCode::OK);
    }
    let stale = json_request(
        &app,
        &format!("{command_a}?method=timeshift_live&generation=0"),
    )
    .await;
    assert!(!stale["error"].is_null());
    assert_eq!(request(&app, replay).await.status(), StatusCode::OK);

    let live = json_request(
        &app,
        &format!("{command_a}?method=timeshift_live&generation=1"),
    )
    .await;
    assert!(live["error"].is_null(), "{live}");
    assert_eq!(live["response"]["view"]["generation"], 2);
    assert_eq!(live["response"]["view"]["mode"], "live");
    assert_eq!(request(&app, replay).await.status(), StatusCode::GONE);
    let resumed = local_path(live["response"]["playback_url"].as_str().unwrap());
    assert!(!response_text(request(&app, resumed).await)
        .await
        .contains("#EXT-X-ENDLIST"));
    assert_eq!(request(&app, &live_a).await.status(), StatusCode::GONE);
    json_request(&app, &format!("{command_a}?method=stop")).await;
    assert_eq!(request(&app, resumed).await.status(), StatusCode::NOT_FOUND);
    assert_eq!(request(&app, &live_b).await.status(), StatusCode::OK);
    assert_eq!(
        manager.get("hlsfix", ID).await.unwrap().subscriber_count(),
        1
    );
    manager.shutdown().await;
}

#[tokio::test]
async fn disabled_history_and_raw_ts_never_claim_an_archive() {
    let state = ace_hls_state(Duration::from_secs(60), 8);
    let manager = state.manager.clone();
    let app = router(state);
    let (live, command, _) = start(&app).await;
    wait_for_hls_playlist(&app, &live).await;
    let status = json_request(&app, &format!("{command}?method=timeshift_status")).await;
    assert_eq!(status["response"]["history"]["enabled"], false);
    assert_eq!(status["response"]["peer_archive"], false);
    assert_eq!(status["response"]["persistent"], false);
    assert_eq!(status["response"]["utc_seek"], false);
    let seek = json_request(&app, &format!("{command}?method=timeshift_seek&back=10")).await;
    assert!(!seek["error"].is_null());
    assert_eq!(request(&app, &live).await.status(), StatusCode::OK);

    let raw = json_request(&app, &format!("/ace/getstream?infohash={ID}&format=json")).await;
    let raw_command = local_path(raw["response"]["command_url"].as_str().unwrap());
    let status = json_request(&app, &format!("{raw_command}?method=timeshift_status")).await;
    assert_eq!(status["response"]["supported"], false);
    let seek = json_request(
        &app,
        &format!("{raw_command}?method=timeshift_seek&back=10"),
    )
    .await;
    assert!(!seek["error"].is_null());
    manager.shutdown().await;
}

#[tokio::test]
async fn absolute_seek_stays_on_its_media_position_and_rejects_ambiguous_commands() {
    let state = history_state();
    let manager = state.manager.clone();
    let leases = state.ace_sessions.clone();
    let app = router(state);
    let (_, command, token) = start(&app).await;
    let (other_live, _, other_token) = start(&app).await;
    wait_history(&manager).await;
    let pkg = manager.get_hls("hlsfix", ID).await.unwrap();
    let position = pkg.history_status().end_ms.unwrap() - 12_000;
    let first = json_request(
        &app,
        &format!("{command}?method=timeshift_seek&position_ms={position}&generation=0"),
    )
    .await;
    assert!(first["error"].is_null(), "{first}");
    assert_eq!(first["response"]["view"]["generation"], 1);
    let old_url = local_path(first["response"]["playback_url"].as_str().unwrap());
    let old_view = leases.hls_view(ID, &token).unwrap().0;
    let old_snapshot = old_view.history.as_ref().unwrap();
    assert!(old_snapshot.start_ms <= position && position < old_snapshot.end_ms);

    tokio::time::timeout(Duration::from_secs(5), async {
        while pkg.history_status().end_ms.unwrap() <= old_snapshot.end_ms {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let second = json_request(
        &app,
        &format!("{command}?method=timeshift_seek&position_ms={position}&generation=1"),
    )
    .await;
    assert!(second["error"].is_null(), "{second}");
    let current = leases.hls_view(ID, &token).unwrap().0;
    let snapshot = current.history.as_ref().unwrap();
    assert_eq!(current.generation, 2);
    assert_eq!(snapshot.start_ms, old_snapshot.start_ms);
    assert_eq!(snapshot.first_seq, old_snapshot.first_seq);
    assert!(snapshot.end_ms > old_snapshot.end_ms);
    assert_eq!(request(&app, old_url).await.status(), StatusCode::GONE);
    let replay = local_path(second["response"]["playback_url"].as_str().unwrap());

    for query in [
        "method=timeshift_seek".to_owned(),
        format!("method=timeshift_seek&position_ms={position}&back=1"),
        format!("method=timeshift_seek&position_ms={position}&position_ms={position}"),
        format!("method=timeshift_seek&position_ms={position}&position_%6ds={position}"),
        "method=timeshift_seek&back=1&back=2".to_owned(),
        format!("method=timeshift_seek&position_ms={position}&generation=2&generation=2"),
        format!("method=timeshift_seek&position_ms={position}&method=stop"),
        format!("method=timeshift_seek&position_ms={position}&generation=1"),
        format!("method=timeshift_live&position_ms={position}&generation=2"),
        "method=timeshift_live&back=1&generation=2".to_owned(),
    ] {
        let failed = json_request(&app, &format!("{command}?{query}")).await;
        assert!(!failed["error"].is_null(), "{query}: {failed}");
        let unchanged = leases.hls_view(ID, &token).unwrap().0;
        assert_eq!(unchanged.generation, current.generation);
        assert_eq!(unchanged.history, current.history);
    }
    for value in [
        "",
        "-1",
        "%2B1",
        "1.5",
        "1e3",
        "NaN",
        "%201",
        "18446744073709551616",
        "18446744073709551615",
    ] {
        let failed = json_request(
            &app,
            &format!("{command}?method=timeshift_seek&position_ms={value}&generation=2"),
        )
        .await;
        assert!(!failed["error"].is_null(), "{value}: {failed}");
        let unchanged = leases.hls_view(ID, &token).unwrap().0;
        assert_eq!(unchanged.generation, current.generation);
        assert_eq!(unchanged.history, current.history);
    }
    assert_eq!(request(&app, replay).await.status(), StatusCode::OK);
    assert_eq!(request(&app, &other_live).await.status(), StatusCode::OK);
    let other = leases.hls_view(ID, &other_token).unwrap().0;
    assert_eq!(other.generation, 0);
    assert!(other.history.is_none());
    manager.shutdown().await;
}

#[tokio::test]
async fn concurrent_absolute_seeks_commit_only_one_expected_generation() {
    let state = history_state();
    let manager = state.manager.clone();
    let leases = state.ace_sessions.clone();
    let app = router(state);
    let (_, command, token) = start(&app).await;
    wait_history(&manager).await;
    let pkg = manager.get_hls("hlsfix", ID).await.unwrap();
    let position = pkg.history_status().end_ms.unwrap() - 12_000;
    let path = format!("{command}?method=timeshift_seek&position_ms={position}&generation=0");
    let (first, second) = tokio::join!(json_request(&app, &path), json_request(&app, &path));
    assert_ne!(first["error"].is_null(), second["error"].is_null());
    assert_eq!(leases.hls_view(ID, &token).unwrap().0.generation, 1);
    manager.shutdown().await;
}

#[tokio::test]
async fn view_commit_is_compare_and_swap_and_source_replacement_fenced() {
    let state = history_state();
    let manager = state.manager.clone();
    let leases = state.ace_sessions.clone();
    let app = router(state);
    let (_, command, token) = start(&app).await;
    wait_history(&manager).await;
    let pkg = manager.get_hls("hlsfix", ID).await.unwrap();
    let snapshot = pkg.prepare_history(Duration::from_secs(10)).unwrap();
    let (_, mut change) = leases.hls_view(ID, &token).unwrap();
    let first = leases
        .replace_hls_view(ID, &token, 0, &pkg, Some(snapshot.clone()))
        .unwrap();
    assert_eq!(first.generation, 1);
    change.changed().await.unwrap();
    assert_eq!(*change.borrow(), 1);
    assert!(leases
        .replace_hls_view(ID, &token, 0, &pkg, Some(snapshot))
        .is_none());
    assert_eq!(leases.hls_view(ID, &token).unwrap().0.generation, 1);
    manager.stop("hlsfix", ID).await;
    let (replacement, _, _pin) = manager
        .get_or_start_compatibility_hls("hlsfix", ID)
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&pkg, &replacement));
    // Sequence numbers restart on the replacement packager. The old replay's coordinates
    // must never select its (different) media, even once the same sequence exists there.
    let first_seq = first.history.as_ref().unwrap().first_seq;
    tokio::time::timeout(Duration::from_secs(5), async {
        while replacement.compatibility_segment(first_seq).is_none() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        request(&app, &format!("/ace/t/{ID}/{token}/1.m3u8"))
            .await
            .status(),
        StatusCode::GONE
    );
    assert_eq!(
        request(&app, &format!("/ace/tc/{token}/1/{first_seq}.ts"))
            .await
            .status(),
        StatusCode::GONE
    );
    let status = json_request(&app, &format!("{command}?method=timeshift_status")).await;
    assert_eq!(status["response"]["view"]["generation"], 1);
    assert!(leases.revoke(ID, &token));
    assert!(leases.replace_hls_view(ID, &token, 1, &pkg, None).is_none());
    manager.shutdown().await;
}

#[tokio::test]
async fn pruned_replay_returns_gone_without_switching_to_live() {
    let state = history_state_with(crate::config::HlsHistoryConfig {
        max_seconds: 600,
        max_bytes: 8192,
        global_max_bytes: 16384,
    });
    let manager = state.manager.clone();
    let leases = state.ace_sessions.clone();
    let app = router(state);
    let (_, command, token) = start(&app).await;
    let pkg = manager.get_hls("hlsfix", ID).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while pkg.prepare_history(Duration::from_secs(1)).is_err() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    let seek = json_request(&app, &format!("{command}?method=timeshift_seek&back=1")).await;
    assert!(seek["error"].is_null(), "{seek}");
    let replay = local_path(seek["response"]["playback_url"].as_str().unwrap());
    let view = leases.hls_view(ID, &token).unwrap().0;
    let snapshot = view.history.as_ref().unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while pkg.history_segment(snapshot, snapshot.first_seq).is_ok() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(request(&app, replay).await.status(), StatusCode::GONE);
    assert_eq!(
        request(
            &app,
            &format!("/ace/tc/{token}/1/{}.ts", snapshot.first_seq)
        )
        .await
        .status(),
        StatusCode::GONE
    );
    let expired_seek = json_request(
        &app,
        &format!(
            "{command}?method=timeshift_seek&position_ms={}&generation=1",
            snapshot.start_ms
        ),
    )
    .await;
    assert!(!expired_seek["error"].is_null());
    let still_replay = leases.hls_view(ID, &token).unwrap().0;
    assert_eq!(still_replay.generation, 1);
    assert!(still_replay.history.is_some());
    manager.shutdown().await;
}

#[tokio::test]
async fn changed_view_interrupts_an_in_flight_manifest_wait() {
    let state = stalled_hls_state();
    let app = router(state.clone());
    let (playback_id, manifest, _) = mint_stalled_hls(&app, ID).await;
    let token = manifest.strip_suffix(".m3u8").unwrap().to_string();
    let response = ace_hls_playback_with_timeout(
        State(state.clone()),
        HeaderMap::new(),
        Path((playback_id.clone(), manifest)),
        Duration::from_secs(30),
    );
    tokio::pin!(response);
    // Poll the request before committing a view change, without relying on a timer.
    assert!(futures::poll!(&mut response).is_pending());
    let pkg = state.manager.get_hls("stalled", ID).await.unwrap();
    assert!(state
        .ace_sessions
        .replace_hls_view(&playback_id, &token, 0, &pkg, None)
        .is_some());
    let response = tokio::time::timeout(Duration::from_secs(1), response)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::GONE);
    state.manager.shutdown().await;
}

#[tokio::test]
async fn manifest_and_stat_advertise_timeshift_and_unknown_methods_are_rejected() {
    let state = history_state();
    let manager = state.manager.clone();
    let app = router(state);
    let json = json_request(
        &app,
        &format!("/ace/manifest.m3u8?infohash={ID}&format=json"),
    )
    .await;
    assert_eq!(json["response"]["timeshift"]["supported"], true);
    assert_eq!(json["response"]["timeshift"]["view"]["generation"], 0);
    assert_eq!(json["response"]["timeshift"]["view"]["mode"], "live");
    let command = local_path(json["response"]["command_url"].as_str().unwrap()).to_owned();
    let stat = local_path(json["response"]["stat_url"].as_str().unwrap()).to_owned();
    wait_history(&manager).await;
    let stat = json_request(&app, &stat).await;
    assert_eq!(stat["response"]["timeshift"]["history"]["enabled"], true);
    assert_eq!(
        stat["response"]["timeshift"]["history"]["time_basis"],
        "retained_media_duration"
    );
    for query in ["", "?method=bogus", "?method=stop&method=stop"] {
        let failed = json_request(&app, &format!("{command}{query}")).await;
        assert!(!failed["error"].is_null(), "{query}: {failed}");
    }
    let stop = json_request(&app, &format!("{command}?method=stop")).await;
    assert_eq!(stop["response"], "ok");
    manager.shutdown().await;
}
