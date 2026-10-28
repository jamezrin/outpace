use super::*;

fn params(pairs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn getstream_selects_magnet_as_infohash() {
    let ih = "0123456789abcdef0123456789abcdef01234567";
    let sel =
        ace_selected_stream(&params(&[("magnet", &format!("magnet:?xt=urn:btih:{ih}"))])).unwrap();
    assert_eq!(sel.session_key, ih);
    assert_eq!(sel.playback_id, ih);
}

#[test]
fn getstream_selects_transport_url_with_self_contained_playback_id() {
    let url = "https://example.com/a/b.acelive";
    let sel = ace_selected_stream(&params(&[("url", url)])).unwrap();
    // playback_id == session_key: no server-side alias table, so playback survives a restart
    // or a direct hit, and the id is path-safe (single segment, no '/'/':'/'?').
    assert_eq!(sel.playback_id, sel.session_key);
    assert!(sel.playback_id.starts_with("turl-"));
    assert!(!sel.playback_id.contains('/'));
    assert!(!sel.playback_id.contains(':'));
    assert!(!sel.playback_id.contains('?'));
    // ...and it decodes back to the URL the provider will fetch.
    assert_eq!(
        crate::transport_url::decode_transport_url(&sel.session_key).as_deref(),
        Some(url)
    );
}

#[test]
fn getstream_precedence_content_id_over_infohash_over_url_over_magnet() {
    let ih = "0123456789abcdef0123456789abcdef01234567";
    let cid = "89abcdef0123456789abcdef0123456789abcdef";
    let sel = ace_selected_stream(&params(&[
        ("content_id", cid),
        ("infohash", ih),
        ("id", "fedcba9876543210fedcba9876543210fedcba98"),
        ("url", "https://e/x"),
        ("magnet", &format!("magnet:?xt=urn:btih:{ih}")),
    ]))
    .unwrap();
    assert_eq!(sel.session_key, format!("cid:{cid}"));

    let sel = ace_selected_stream(&params(&[
        ("infohash", ih),
        ("id", "fedcba9876543210fedcba9876543210fedcba98"),
        ("url", "https://e/x"),
        ("magnet", &format!("magnet:?xt=urn:btih:{ih}")),
    ]))
    .unwrap();
    assert_eq!(sel.session_key, ih);

    let sel = ace_selected_stream(&params(&[
        ("url", "https://e/x.acelive"),
        ("magnet", &format!("magnet:?xt=urn:btih:{ih}")),
    ]))
    .unwrap();
    assert_eq!(
        crate::transport_url::decode_transport_url(&sel.session_key).as_deref(),
        Some("https://e/x.acelive")
    );
}

#[test]
fn getstream_legacy_id_uses_content_id_resolver_key() {
    let id = "89ABCDEF0123456789ABCDEF0123456789ABCDEF";
    let sel = ace_selected_stream(&params(&[("id", id)])).unwrap();
    let normalized = id.to_ascii_lowercase();
    assert_eq!(sel.public_id, normalized);
    assert_eq!(sel.playback_id, format!("cid:{normalized}"));
    assert_eq!(sel.session_key, format!("cid:{normalized}"));
    assert_eq!(sel.content_id.as_deref(), Some(normalized.as_str()));
}

#[test]
fn getstream_infohash_remains_a_direct_swarm_key() {
    let infohash = "89ABCDEF0123456789ABCDEF0123456789ABCDEF";
    let sel = ace_selected_stream(&params(&[("infohash", infohash)])).unwrap();
    let normalized = infohash.to_ascii_lowercase();
    assert_eq!(sel.playback_id, normalized);
    assert_eq!(sel.session_key, normalized);
    assert!(sel.content_id.is_none());
}

#[test]
fn getstream_rejects_bad_url_and_magnet() {
    assert!(ace_selected_stream(&params(&[("url", "file:///etc/passwd")])).is_err());
    assert!(ace_selected_stream(&params(&[("magnet", "magnet:?dn=noxt")])).is_err());
}

#[test]
fn getstream_rejects_malformed_hash_selectors_without_falling_through() {
    assert_eq!(
        ace_selected_stream(&params(&[("id", "short")])),
        Err("invalid id")
    );
    assert_eq!(
        ace_selected_stream(&params(&[("infohash", "not-hex")])),
        Err("invalid infohash")
    );
    assert_eq!(
        ace_selected_stream(&params(&[
            ("content_id", "bad"),
            ("infohash", "0123456789abcdef0123456789abcdef01234567",),
        ])),
        Err("invalid content_id")
    );
}

#[test]
fn content_id_selection_uses_resolved_infohash_when_available() {
    let content_id = "2123456789abcdef0123456789abcdef01234567";
    let resolved = "50e93529d3eb46a50506b14464185a15292d6e47";
    let mut params = HashMap::new();
    params.insert("content_id".to_string(), content_id.to_string());

    let selection = ace_selected_stream(&params)
        .unwrap()
        .with_resolved_stream(resolved.to_string(), fixture_metadata());

    assert_eq!(selection.public_id, resolved);
    assert_eq!(selection.playback_id, resolved);
    assert_eq!(selection.session_key, format!("cid:{content_id}"));
    assert_eq!(selection.metadata, fixture_metadata());
}

#[test]
fn ace_session_store_expires_and_bounds_leases_deterministically() {
    let now = Instant::now();
    let store = AceSessionStore::new(Duration::from_secs(10), 2);
    store.insert_at("a".into(), "id-a".into(), "key-a".into(), now);
    store.insert_at(
        "b".into(),
        "id-b".into(),
        "key-b".into(),
        now + Duration::from_secs(1),
    );
    store.insert_at(
        "c".into(),
        "id-c".into(),
        "key-c".into(),
        now + Duration::from_secs(2),
    );

    assert_eq!(
        store.validate_at("id-a", "a", now + Duration::from_secs(2)),
        None
    );
    assert_eq!(
        store.validate_at("id-b", "b", now + Duration::from_secs(2)),
        Some("key-b".into())
    );
    assert_eq!(
        store.validate_at("wrong-id", "b", now + Duration::from_secs(2)),
        None
    );
    assert_eq!(
        store.validate_at("id-b", "b", now + Duration::from_secs(12)),
        None
    );
    assert_eq!(store.leases.lock().unwrap().len(), 0);
}

#[tokio::test]
async fn ace_session_store_revokes_only_matching_client_lease() {
    let now = Instant::now();
    let store = AceSessionStore::new(Duration::from_secs(60), 4);
    store.insert_at("client-a".into(), "same-id".into(), "same-key".into(), now);
    store.insert_at("client-b".into(), "same-id".into(), "same-key".into(), now);

    let (_, _, mut client_a) = store.playback("same-id", "client-a").unwrap();
    let (_, _, client_b) = store.playback("same-id", "client-b").unwrap();
    assert!(store.revoke("same-id", "client-a"));
    client_a.changed().await.unwrap();
    assert!(*client_a.borrow());
    assert!(client_b.has_changed().is_ok_and(|changed| !changed));
    assert_eq!(store.validate("same-id", "client-a"), None);
    assert_eq!(
        store.validate("same-id", "client-b"),
        Some("same-key".into())
    );
}

#[test]
fn direct_lease_guard_removes_its_entry_after_authorization_expiry() {
    let now = Instant::now();
    let sessions = Arc::new(AceSessionStore::new(Duration::from_secs(1), 4));
    sessions.insert_at(
        "expired-token".into(),
        "same-id".into(),
        "same-key".into(),
        now - Duration::from_secs(2),
    );
    let guard = AceDirectLeaseGuard {
        sessions: sessions.clone(),
        playback_id: "same-id".into(),
        token: "expired-token".into(),
    };

    assert!(
        !sessions.revoke("same-id", "expired-token"),
        "normal revoke must retain its unexpired-authorization requirement"
    );
    assert_eq!(sessions.active_count(), 1);
    drop(guard);
    assert_eq!(
        sessions.active_count(),
        0,
        "the owner guard must remove its entry even after authorization expires"
    );
}

#[tokio::test]
async fn ace_getstream_content_id_returns_a_playback_url_that_streams() {
    use futures::StreamExt;
    let content_id = "2123456789abcdef0123456789abcdef01234567";
    let state = ace_compat_state(0);
    let manager = state.manager.clone();
    let app = router(state);

    let resp = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/ace/getstream?format=json&content_id={content_id}"
            ))
            .header(header::HOST, "localhost:6900")
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"], serde_json::Value::Null);
    assert_eq!(json["response"]["is_live"], 1);
    assert_eq!(json["response"]["is_encrypted"], 0);
    assert_eq!(json["response"]["infohash"], content_id);
    assert_eq!(
        json["response"]["metadata"],
        json!({ "title": null, "bitrate": null, "categories": [] })
    );

    let playback_url = json["response"]["playback_url"].as_str().unwrap();
    let playback_path = playback_url
        .strip_prefix("http://localhost:6900")
        .expect("playback URL should point back at this daemon");
    assert!(playback_path.starts_with(&format!("/ace/r/cid:{content_id}/")));
    // Keep `app` alive while reading, as in the clean `/streams` tests: the test
    // manager owns the live session.
    let media = app
        .clone()
        .oneshot(Request::get(playback_path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(media.status(), StatusCode::OK);
    assert_eq!(media.headers()[header::CONTENT_TYPE], "video/mp2t");
    assert!(!media.headers().contains_key("icy-name"));
    let mut stream = media.into_body().into_data_stream();
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first[0], 0x47);
    assert_eq!(
        ace_media::mpegts::read_sdt_service_name(&first).as_deref(),
        Some("Synthetic Demo Channel")
    );
    assert!(manager
        .get("fix", &format!("cid:{content_id}"))
        .await
        .is_some());
    assert!(manager.get("fix", content_id).await.is_none());
}

#[tokio::test]
async fn acexy_id_json_handshake_accepts_typed_response_and_returned_lifecycle_urls() {
    use futures::StreamExt;

    #[derive(serde::Deserialize)]
    struct AcexyResponse {
        playback_url: String,
        stat_url: String,
        command_url: String,
        playback_session_id: String,
        client_session_id: i32,
        infohash: String,
        is_live: u32,
        is_encrypted: u32,
    }

    #[derive(serde::Deserialize)]
    struct AcexyEnvelope {
        response: AcexyResponse,
        error: Option<String>,
    }

    let id = "0123456789abcdef0123456789abcdef01234567";
    let app = router(ace_compat_state(0));
    let response = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/ace/getstream?id={id}&format=json&pid=synthetic-pid"
            ))
            .header(header::HOST, "localhost:6900")
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let envelope: AcexyEnvelope = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(envelope.error.is_none());
    assert_eq!(envelope.response.infohash, id);
    assert_eq!(envelope.response.client_session_id, -1);
    assert_eq!(envelope.response.is_live, 1);
    assert_eq!(envelope.response.is_encrypted, 0);
    assert!(envelope.response.playback_session_id.len() >= 32);

    let playback_path = envelope
        .response
        .playback_url
        .strip_prefix("http://localhost:6900")
        .unwrap();
    let mut playback = app
        .clone()
        .oneshot(Request::get(playback_path).body(Body::empty()).unwrap())
        .await
        .unwrap()
        .into_body()
        .into_data_stream();
    assert_eq!(playback.next().await.unwrap().unwrap()[0], 0x47);

    let stat_path = envelope
        .response
        .stat_url
        .strip_prefix("http://localhost:6900")
        .unwrap();
    let stat = app
        .clone()
        .oneshot(Request::get(stat_path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(stat.status(), StatusCode::OK);
    let stat: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(stat.into_body(), 1 << 20)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(stat["error"], serde_json::Value::Null);
    assert_eq!(stat["response"]["status"], "dl");

    let command_path = envelope
        .response
        .command_url
        .strip_prefix("http://localhost:6900")
        .unwrap();
    let stop = app
        .oneshot(
            Request::get(format!("{command_path}?method=stop"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let stop: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(stop.into_body(), 1 << 20)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(stop["response"], "ok");
    assert_eq!(stop["error"], serde_json::Value::Null);
}

#[tokio::test]
async fn ace_getstream_malformed_json_id_returns_error_envelope() {
    let response = router(ace_compat_state(0))
        .oneshot(
            Request::get("/ace/getstream?id=bad&format=json&pid=synthetic-pid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(json["response"], serde_json::Value::Null);
    assert!(json["error"]
        .as_str()
        .is_some_and(|error| !error.is_empty()));
}

#[tokio::test]
async fn ace_compat_playback_and_control_routes_are_gated_off_by_default() {
    let id = "0123456789abcdef0123456789abcdef01234567";
    for path in [
        format!("/ace/getstream?id={id}&format=json"),
        format!("/ace/r/{id}/synthetic-token"),
        format!("/ace/stat/{id}/synthetic-token"),
        format!("/ace/cmd/{id}/synthetic-token?method=stop"),
        format!("/ace/manifest.m3u8?infohash={id}"),
        format!("/ace/m/{id}/synthetic-token.m3u8"),
        "/ace/c/ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff/0.ts".into(),
        "/server/api?method=get_version".into(),
        "/webui/api/service?method=get_version".into(),
    ] {
        let response = router(fixture_state(0))
            .oneshot(Request::get(&path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }
}

#[tokio::test]
async fn ace_getstream_without_format_directly_streams_legacy_id_as_mpegts() {
    use futures::StreamExt;
    let content_id = "2123456789abcdef0123456789abcdef01234567";
    let state = ace_compat_state(0);
    let manager = state.manager.clone();
    let app = router(state);

    let response = app
        .clone()
        .oneshot(
            Request::get(format!("/ace/getstream?id={content_id}&use_api_events=1"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "video/mp2t");
    assert!(!response.headers().contains_key("icy-name"));
    let mut body = response.into_body().into_data_stream();
    let first = body.next().await.unwrap().unwrap();
    assert_eq!(first[0], 0x47);
    assert_eq!(
        ace_media::mpegts::read_sdt_service_name(&first).as_deref(),
        Some("Synthetic Demo Channel")
    );
    assert!(manager
        .get("fix", &format!("cid:{content_id}"))
        .await
        .is_some());
    assert!(manager.get("fix", content_id).await.is_none());
}

#[tokio::test]
async fn ace_direct_and_json_playback_share_source_and_disconnect_independently() {
    use futures::StreamExt;
    let infohash = "0123456789abcdef0123456789abcdef01234567";
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(PacedFixtureProvider));
    let manager = StreamManager::new(registry);
    let ace_sessions = Arc::new(AceSessionStore::default());
    let app = router(AppState {
        manager: manager.clone(),
        networks: vec!["fix".into()],
        resolve_content_ids_in_getstream: false,
        ace_sessions: ace_sessions.clone(),
        experimental_ace_compat: true,
        broadcasts: None,
    });

    let minted = app
        .clone()
        .oneshot(
            Request::get(format!("/ace/getstream?format=json&infohash={infohash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let minted = axum::body::to_bytes(minted.into_body(), 1 << 20)
        .await
        .unwrap();
    let minted: serde_json::Value = serde_json::from_slice(&minted).unwrap();
    let playback_path = minted["response"]["playback_url"]
        .as_str()
        .unwrap()
        .strip_prefix("http://127.0.0.1:6878")
        .unwrap();
    let mut json_body = app
        .clone()
        .oneshot(Request::get(playback_path).body(Body::empty()).unwrap())
        .await
        .unwrap()
        .into_body()
        .into_data_stream();
    let mut direct_body = app
        .clone()
        .oneshot(
            Request::get(format!("/ace/getstream?infohash={infohash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .into_body()
        .into_data_stream();

    assert!(json_body.next().await.transpose().unwrap().is_some());
    assert!(direct_body.next().await.transpose().unwrap().is_some());
    assert_eq!(
        manager
            .get("fix", infohash)
            .await
            .unwrap()
            .subscriber_count(),
        2
    );
    assert_eq!(ace_sessions.active_count(), 2);
    drop(direct_body);
    assert_eq!(
        manager
            .get("fix", infohash)
            .await
            .unwrap()
            .subscriber_count(),
        1
    );
    assert_eq!(
        ace_sessions.active_count(),
        1,
        "disconnecting direct playback must revoke its hidden lease only"
    );
    assert!(
        json_body.next().await.transpose().unwrap().is_some(),
        "dropping the direct caller must not interrupt the JSON playback caller"
    );
}

#[tokio::test]
async fn ace_getstream_errors_are_deterministic_and_do_not_start_sources() {
    async fn get_json(app: &Router, path: &str) -> serde_json::Value {
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    let state = ace_compat_state(0);
    let manager = state.manager.clone();
    let app = router(state);
    assert_eq!(
        get_json(&app, "/ace/getstream").await["error"],
        "missing content_id/infohash/id/url/magnet"
    );
    assert_eq!(
        get_json(
            &app,
            "/ace/getstream?content_id=bad&infohash=0123456789abcdef0123456789abcdef01234567",
        )
        .await["error"],
        "invalid content_id"
    );
    assert_eq!(
        get_json(
            &app,
            "/ace/getstream?format=xml&infohash=0123456789abcdef0123456789abcdef01234567",
        )
        .await["error"],
        "unsupported format"
    );
    assert!(manager.list().await.is_empty());
}

#[tokio::test]
async fn ace_direct_start_failure_revokes_its_hidden_lease() {
    let sessions = Arc::new(AceSessionStore::default());
    let app = router(AppState {
        manager: StreamManager::new(ProviderRegistry::new()),
        networks: vec!["fix".into()],
        resolve_content_ids_in_getstream: false,
        ace_sessions: sessions.clone(),
        experimental_ace_compat: true,
        broadcasts: None,
    });
    let response = app
        .oneshot(
            Request::get("/ace/getstream?infohash=0123456789abcdef0123456789abcdef01234567")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(sessions.active_count(), 0);
}

#[tokio::test]
async fn ace_playback_start_failures_are_distinct_from_unregistered_network() {
    struct FailingProvider;

    #[async_trait]
    impl StreamProvider for FailingProvider {
        fn network(&self) -> &'static str {
            "fix"
        }

        async fn open(&self, id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
            match id {
                "no-peers" => Err(ProviderError::Backend(
                    "no peers (no trackers/bootstrap)".into(),
                )),
                "catalog-timeout" => Err(ProviderError::Backend(
                    concat!(
                        "content-id catalog resolution failed: Catalog(\"catalog timeout\"); ",
                        "raw-hash BEP-9 fallback is disabled ",
                        "(OUTPACE_BEP9_CONTENT_ID_FALLBACK=1 to opt in)"
                    )
                    .into(),
                )),
                "unresolvable" => Err(ProviderError::Unresolvable("no verified metadata".into())),
                // Models the manager's unregistered-network error. AceProvider itself
                // never returns NotFound, even for unknown or unpublished content ids.
                _ => Err(ProviderError::NotFound),
            }
        }
    }

    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(FailingProvider));
    let mut state = ace_compat_state(0);
    state.manager = StreamManager::new(registry);
    let sessions = state.ace_sessions.clone();
    let app = router(state);

    for (key, status, reason) in [
        ("no-peers", StatusCode::BAD_GATEWAY, "stream start failed"),
        (
            "catalog-timeout",
            StatusCode::BAD_GATEWAY,
            "stream start failed",
        ),
        (
            "unresolvable",
            StatusCode::UNPROCESSABLE_ENTITY,
            "stream metadata unavailable",
        ),
        ("unregistered-network", StatusCode::NOT_FOUND, ""),
    ] {
        let token = sessions.mint(
            "0123456789abcdef0123456789abcdef01234567".into(),
            key.into(),
        );
        let path = format!("/ace/r/0123456789abcdef0123456789abcdef01234567/{token}");
        // A failed start must preserve the lease so the same playback URL can be retried.
        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(Request::get(&path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), status, "{key}");
            if !reason.is_empty() {
                assert_eq!(
                    response.headers()[header::CONTENT_TYPE],
                    "text/plain; charset=utf-8"
                );
            }
            assert_eq!(response_text(response).await, reason, "{key}");
        }
    }
}

#[tokio::test]
async fn ace_playback_unknown_or_expired_tokens_and_networks_still_return_404() {
    let mut state = ace_compat_state(0);
    state.ace_sessions = Arc::new(AceSessionStore::new(Duration::from_secs(1), 8));
    let id = "0123456789abcdef0123456789abcdef01234567";
    state.ace_sessions.insert_at(
        "expired".into(),
        id.into(),
        id.into(),
        Instant::now() - Duration::from_secs(2),
    );
    let token = state.ace_sessions.mint(id.into(), id.into());
    let app = router(state.clone());
    for path in [
        format!("/ace/r/{id}/unknown"),
        format!("/ace/r/{id}/expired"),
        format!("/ace/r/89abcdef0123456789abcdef0123456789abcdef/{token}"),
    ] {
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(response_bytes(response).await.is_empty());
    }
    assert!(state.manager.list().await.is_empty());

    // Cover both network selection failure and an unregistered provider in the manager.
    for networks in [vec![], vec!["fix".into()]] {
        state.networks = networks;
        state.manager = StreamManager::new(ProviderRegistry::new());
        let response = router(state.clone())
            .oneshot(
                Request::get(format!("/ace/r/{id}/{token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(response_bytes(response).await.is_empty());
    }
}

#[tokio::test]
async fn ace_getstream_mints_distinct_enforced_tokens() {
    let content_id = "2123456789abcdef0123456789abcdef01234567";
    let app = router(ace_compat_state(0));
    let mut tokens = Vec::new();
    for _ in 0..2 {
        let response = app
            .clone()
            .oneshot(
                Request::get(format!(
                    "/ace/getstream?format=json&content_id={content_id}"
                ))
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        tokens.push(
            value["response"]["playback_session_id"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }
    assert_ne!(tokens[0], tokens[1]);
    assert_eq!(tokens[0].len(), 64);
    assert!(!tokens[0].contains(content_id));

    let playback = app
        .clone()
        .oneshot(
            Request::get(format!("/ace/r/cid:{content_id}/invalid"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(playback.status(), StatusCode::NOT_FOUND);
    for path in [
        format!("/ace/stat/cid:{content_id}/invalid"),
        format!("/ace/cmd/cid:{content_id}/invalid?method=stop"),
        format!("/ace/cmd/cid:{content_id}/invalid?method=pause"),
        format!("/ace/cmd/cid:{content_id}/invalid"),
    ] {
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["response"], serde_json::Value::Null);
        assert_eq!(value["error"], "invalid or expired playback session");
    }
}

#[tokio::test]
async fn active_ace_playback_ends_at_its_lease_deadline() {
    use futures::StreamExt;
    let content_id = "2123456789abcdef0123456789abcdef01234567";
    let mut state = ace_compat_state(0);
    let manager = state.manager.clone();
    state.ace_sessions = Arc::new(AceSessionStore::new(Duration::from_millis(30), 4));
    let app = router(state);
    let minted = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/ace/getstream?format=json&content_id={content_id}"
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(minted.into_body(), 1 << 20)
        .await
        .unwrap();
    let minted: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let path = minted["response"]["playback_url"]
        .as_str()
        .unwrap()
        .strip_prefix("http://127.0.0.1:6878")
        .unwrap()
        .to_owned();
    let playback = app
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let mut body = playback.into_body().into_data_stream();
    assert_eq!(
        manager
            .get("fix", &format!("cid:{content_id}"))
            .await
            .unwrap()
            .subscriber_count(),
        1
    );
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(
        body.next().await.is_none(),
        "an active body must terminate when its lease expires"
    );
    assert_eq!(
        manager
            .get("fix", &format!("cid:{content_id}"))
            .await
            .unwrap()
            .subscriber_count(),
        0
    );
}

#[tokio::test]
async fn ace_stat_and_stop_track_content_id_session() {
    use futures::StreamExt;
    let content_id = "2123456789abcdef0123456789abcdef01234567";
    let state = ace_compat_state(0);
    let manager = state.manager.clone();
    let app = router(state);
    let minted = app
        .clone()
        .oneshot(
            Request::get(format!(
                "/ace/getstream?format=json&content_id={content_id}"
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(minted.into_body(), 1 << 20)
        .await
        .unwrap();
    let minted: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let token = minted["response"]["playback_session_id"].as_str().unwrap();
    let stat_path = format!("/ace/stat/cid:{content_id}/{token}");
    let idle = app
        .clone()
        .oneshot(Request::get(&stat_path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(idle.status(), StatusCode::OK);
    let body = axum::body::to_bytes(idle.into_body(), 1 << 20)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"], serde_json::Value::Null);
    assert_eq!(json["response"]["status"], "idle");
    assert_eq!(json["response"]["infohash"], content_id);
    assert_eq!(
        json["response"]["metadata"],
        json!({ "title": null, "bitrate": null, "categories": [] })
    );

    let media = app
        .clone()
        .oneshot(
            Request::get(format!("/ace/r/cid:{content_id}/{token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(media.status(), StatusCode::OK);
    let mut stream = media.into_body().into_data_stream();
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first[0], 0x47);

    let running = app
        .clone()
        .oneshot(Request::get(&stat_path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(running.status(), StatusCode::OK);
    let body = axum::body::to_bytes(running.into_body(), 1 << 20)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"], serde_json::Value::Null);
    assert_eq!(json["response"]["status"], "dl");
    assert_eq!(json["response"]["peers"], 1);
    assert_eq!(
        json["response"]["metadata"],
        json!({ "title": "Synthetic Demo Channel", "bitrate": 800_000, "categories": ["sports"] })
    );
    assert!(json["response"]["downloaded"].as_u64().unwrap() > 0);

    let stop = app
        .oneshot(
            Request::get(format!("/ace/cmd/cid:{content_id}/{token}?method=stop"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stop.status(), StatusCode::OK);
    let body = axum::body::to_bytes(stop.into_body(), 1 << 20)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"], serde_json::Value::Null);
    assert_eq!(json["response"], "ok");
    assert!(
        manager
            .get("fix", &format!("cid:{content_id}"))
            .await
            .is_some(),
        "compatibility stop must not force-stop the shared source"
    );
    assert!(manager.get("fix", content_id).await.is_none());
}

#[tokio::test]
async fn ace_stop_isolates_two_compat_clients_and_preserves_native_consumer() {
    use futures::StreamExt;
    let id = "0123456789abcdef0123456789abcdef01234567";
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(PacedFixtureProvider));
    let manager = StreamManager::new(registry);
    let app = router(AppState {
        manager: manager.clone(),
        networks: vec!["fix".into()],
        resolve_content_ids_in_getstream: false,
        ace_sessions: Arc::new(AceSessionStore::default()),
        experimental_ace_compat: true,
        broadcasts: None,
    });

    async fn mint(app: &Router, id: &str) -> serde_json::Value {
        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/ace/getstream?format=json&infohash={id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }
    let a = mint(&app, id).await;
    let b = mint(&app, id).await;
    let a_path = a["response"]["playback_url"]
        .as_str()
        .unwrap()
        .strip_prefix("http://127.0.0.1:6878")
        .unwrap();
    let b_path = b["response"]["playback_url"]
        .as_str()
        .unwrap()
        .strip_prefix("http://127.0.0.1:6878")
        .unwrap();
    let stop_a = a["response"]["command_url"]
        .as_str()
        .unwrap()
        .strip_prefix("http://127.0.0.1:6878")
        .unwrap();

    let mut body_a = app
        .clone()
        .oneshot(Request::get(a_path).body(Body::empty()).unwrap())
        .await
        .unwrap()
        .into_body()
        .into_data_stream();
    let mut body_b = app
        .clone()
        .oneshot(Request::get(b_path).body(Body::empty()).unwrap())
        .await
        .unwrap()
        .into_body()
        .into_data_stream();
    let mut native = app
        .clone()
        .oneshot(
            Request::get(format!("/streams/fix/{id}.ts"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .into_body()
        .into_data_stream();
    assert!(body_a.next().await.transpose().unwrap().is_some());
    assert!(body_b.next().await.transpose().unwrap().is_some());
    assert!(native.next().await.transpose().unwrap().is_some());
    assert_eq!(manager.get("fix", id).await.unwrap().subscriber_count(), 3);

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
    assert!(body_a.next().await.is_none(), "stopped client A must close");
    assert_eq!(manager.get("fix", id).await.unwrap().subscriber_count(), 2);
    assert!(
        body_b.next().await.transpose().unwrap().is_some(),
        "client B must keep receiving"
    );
    assert!(
        native.next().await.transpose().unwrap().is_some(),
        "native consumer must keep receiving"
    );

    drop(body_b);
    drop(native);
    assert_eq!(
        manager.get("fix", id).await.unwrap().subscriber_count(),
        0,
        "the final consumers disappearing must leave the source eligible for idle cleanup"
    );
}

#[derive(Clone)]
struct MeasuredFixture(SourceStats);
impl crate::provider::StatsReader for MeasuredFixture {
    fn snapshot(&self) -> SourceStats {
        self.0.clone()
    }
}
#[async_trait]
impl TsSource for MeasuredFixture {
    async fn next(&mut self) -> Option<Bytes> {
        std::future::pending().await
    }
    fn stats(&self) -> SourceStats {
        self.0.clone()
    }
    fn stats_handle(&self) -> Option<crate::provider::StatsHandle> {
        Some(Arc::new(self.clone()))
    }
}
#[async_trait]
impl StreamProvider for MeasuredFixture {
    fn network(&self) -> &'static str {
        "fix"
    }
    async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        Ok(Box::new(self.clone()))
    }
}
fn measured_state(position: bool) -> AppState {
    let mut st = ace_compat_state(0);
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(MeasuredFixture(SourceStats {
        bitrate: 12288,
        download_rate: 1536,
        upload_rate: 512,
        livepos: position.then_some(crate::provider::LivePositionStats {
            pos: Some(5),
            last: Some(9),
            live_first: Some(2),
            live_last: Some(9),
            buffer_pieces: 1,
        }),
        recovery: Some(crate::provider::LiveRecoveryStats {
            upstream_reconnects: 2,
            stale_teardowns: 3,
            skipped_evicted_gaps: 4,
            signature_rejects: 5,
            active_upstreams: 1,
        }),
        ..SourceStats::default()
    })));
    st.manager = StreamManager::new(registry);
    st
}

#[tokio::test]
async fn measured_stats_schema_reports_units_positions_and_null_timestamps() {
    let st = measured_state(true);
    let _session = st.manager.get_or_start("fix", "stats").await.unwrap();
    let token = st.ace_sessions.mint("stats".into(), "stats".into());
    let Json(json) = ace_stat(State(st.clone()), Path(("stats".into(), token))).await;
    assert_eq!(json["response"]["speed_down"], 1.5);
    assert_eq!(json["response"]["speed_up"], 0.5);
    let pos = &json["response"]["livepos"];
    assert_eq!(pos["pos"], 5);
    assert_eq!(pos["last"], 9);
    assert_eq!(pos["buffer_pieces"], 1);
    assert_eq!(pos["is_live"], 1);
    assert_eq!(pos.get("first_ts"), Some(&serde_json::Value::Null));
    assert_eq!(pos.get("last_ts"), Some(&serde_json::Value::Null));
    let response = stream_status(State(st), Path(("fix".into(), "stats".into()))).await;
    let native: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
    assert_eq!(native["bitrate"], 12288);
    assert_eq!(native["livepos"], *pos);
    assert_eq!(native["recovery"]["upstream_reconnects"], 2);
    assert_eq!(native["recovery"]["stale_teardowns"], 3);
    assert_eq!(native["recovery"]["skipped_evicted_gaps"], 4);
    assert_eq!(native["recovery"]["signature_rejects"], 5);
    assert_eq!(native["recovery"]["active_upstreams"], 1);
    assert!(json["response"].get("recovery").is_none());
}

#[tokio::test]
async fn native_stats_serializes_missing_recovery_as_null() {
    let mut st = ace_compat_state(0);
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(MeasuredFixture(SourceStats::default())));
    st.manager = StreamManager::new(registry);
    let _session = st.manager.get_or_start("fix", "stats").await.unwrap();
    let response = stream_status(State(st), Path(("fix".into(), "stats".into()))).await;
    let native: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
    assert_eq!(native.get("recovery"), Some(&serde_json::Value::Null));
}

#[tokio::test]
async fn measured_stats_schema_idle_unknown_and_invalid_token_do_not_start() {
    let st = measured_state(false);
    let token = st.ace_sessions.mint("stats".into(), "stats".into());
    let Json(idle) = ace_stat(State(st.clone()), Path(("stats".into(), token.clone()))).await;
    assert_eq!(idle["response"]["speed_down"], 0);
    assert_eq!(
        idle["response"].get("livepos"),
        Some(&serde_json::Value::Null)
    );
    let Json(error) = ace_stat(State(st.clone()), Path(("stats".into(), "invalid".into()))).await;
    assert!(error["error"].is_string());
    assert!(st.manager.list().await.is_empty());
    let _session = st.manager.get_or_start("fix", "stats").await.unwrap();
    let Json(active) = ace_stat(State(st), Path(("stats".into(), token))).await;
    assert_eq!(
        active["response"].get("livepos"),
        Some(&serde_json::Value::Null)
    );
}

#[tokio::test]
async fn admission_compat_errors_lazy_negotiation_and_service_status() {
    let id = "0123456789abcdef0123456789abcdef01234567";
    let mut state = ace_hls_state(Duration::from_secs(60), 8);
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(PacedPacketProvider));
    state.manager = StreamManager::with_session_limit(registry, 1);
    let held = state.manager.get_or_start("hlsfix", "held").await.unwrap();
    let app = router(state.clone());
    for path in [
        format!("/ace/getstream?infohash={id}"),
        format!("/ace/manifest.m3u8?infohash={id}"),
    ] {
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["retry-after"], "1");
    }
    let response = app
        .clone()
        .oneshot(
            Request::get(format!("/ace/manifest.m3u8?format=json&infohash={id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), 10000)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        value,
        json!({"response": null, "error": "session capacity reached"})
    );
    let response = app
        .clone()
        .oneshot(
            Request::get(format!("/ace/getstream?format=json&infohash={id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), 10000)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        value["error"].is_null(),
        "metadata-only negotiation remains available"
    );
    let playback = value["response"]["playback_url"]
        .as_str()
        .unwrap()
        .strip_prefix("http://127.0.0.1:6878")
        .unwrap();
    let response = app
        .clone()
        .oneshot(Request::get(playback).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()["retry-after"], "1");
    let response = app
        .oneshot(
            Request::get("/webui/api/service?method=get_status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), 10000)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["result"]["occupied_sessions"], 1);
    assert_eq!(value["result"]["max_sessions"], 1);
    assert_eq!(value["result"]["active_sessions"], 1);
    drop(held);
    state.manager.shutdown().await;
}
