use super::*;
use axum::body::to_bytes;

fn private_state() -> AppState {
    private_state_with_descriptors(Default::default())
}

fn private_state_with_descriptors(
    descriptors: crate::local_descriptors::LocalDescriptors,
) -> AppState {
    let policy =
        ace_swarm::peer_policy::PeerPolicy::from_private_peers(Some("127.0.0.1:1234")).unwrap();
    let mut registry = ProviderRegistry::new();
    registry.register(Arc::new(
        crate::ace_provider::AceProvider::new(
            Arc::new(ace_wire::identity::Identity::generate()),
            0,
        )
        .with_peer_policy(policy.clone())
        .with_local_descriptors(descriptors),
    ));
    AppState {
        peer_policy: policy,
        manager: StreamManager::new(registry),
        networks: vec!["ace".into()],
        resolve_content_ids_in_getstream: true,
        ace_sessions: Arc::new(AceSessionStore::default()),
        experimental_ace_compat: true,
        search: None,
        origin_hls: None,
        broadcasts: None,
    }
}

#[tokio::test]
async fn private_policy_status_reports_boundary_without_endpoint_disclosure() {
    let response = router(private_state())
        .oneshot(
            Request::builder()
                .uri("/network-policy")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let raw = to_bytes(response.into_body(), 4096).await.unwrap();
    let data: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(data["mode"], "private");
    assert_eq!(data["allowed_peer_count"], 1);
    for name in [
        "inbound_peer_connections",
        "public_discovery",
        "public_advertising",
    ] {
        assert_eq!(data[name], false);
    }
    assert!(!std::str::from_utf8(&raw).unwrap().contains("127.0.0.1"));
    let response = router(private_state())
        .oneshot(
            Request::builder()
                .uri("/server/api?method=get_status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let raw = to_bytes(response.into_body(), 4096).await.unwrap();
    let data: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(data["result"]["peer_policy"]["mode"], "private");
    assert!(!std::str::from_utf8(&raw).unwrap().contains("127.0.0.1"));
}

#[tokio::test]
async fn private_catalog_refusals_remain_explicit_on_native_routes() {
    const ID: &str = "0123456789abcdef0123456789abcdef01234567";
    for path in [
        format!("/streams/ace/cid:{ID}.ts"),
        format!("/streams/ace/cid:{ID}.m3u8"),
        format!("/vod/ace/cid:{ID}"),
    ] {
        let state = private_state();
        let manager = state.manager.clone();
        let response = router(state)
            .oneshot(Request::builder().uri(&path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let raw = to_bytes(response.into_body(), 4096).await.unwrap();
        let occupancy = manager.admission_status().0;
        manager.shutdown().await;
        assert_eq!(occupancy, 0);
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "private refusal lost on native route"
        );
        assert_eq!(raw.as_ref(), b"catalog_disabled_in_private_mode");
    }
}

#[tokio::test]
async fn private_vod_alias_refuses_live_hls_and_compat_playback_with_422() {
    for compat in [false, true] {
        for hls in [false, true] {
            if compat && hls {
                continue;
            }
            let bytes = crate::ace_provider::test_support::protection_vod_bytes(0, false);
            let map =
                crate::local_descriptors::LocalDescriptors::from_test_bytes("fixture", &bytes)
                    .unwrap();
            let state = private_state_with_descriptors(map);
            let manager = state.manager.clone();
            let path = if compat {
                let token = state
                    .ace_sessions
                    .mint("local:fixture".into(), "local:fixture".into());
                format!("/ace/r/local:fixture/{token}")
            } else if hls {
                "/streams/ace/local:fixture.m3u8".into()
            } else {
                "/streams/ace/local:fixture.ts".into()
            };
            let response = router(state)
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            let status = response.status();
            let raw = to_bytes(response.into_body(), 4096).await.unwrap();
            manager.shutdown().await;
            assert_eq!(manager.admission_status().0, 0);
            assert_eq!(
                status,
                StatusCode::UNPROCESSABLE_ENTITY,
                "compat={compat}, hls={hls}"
            );
            assert_eq!(
                raw.as_ref(),
                if compat {
                    b"stream metadata unavailable".as_slice()
                } else {
                    b"local_descriptor_is_not_live".as_slice()
                }
            );
        }
    }
}
