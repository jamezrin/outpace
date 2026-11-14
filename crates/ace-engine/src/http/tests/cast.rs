use super::*;
use crate::cast::CastService;

fn cast_app(bind: &str) -> (Router, Arc<CastService>) {
    let cast = Arc::new(CastService::new());
    cast.set_http_bind(bind.parse().unwrap());
    let app = router_with_services(state(), None, Some(cast.clone()));
    (app, cast)
}

async fn send(
    app: &Router,
    method: &str,
    uri: &str,
    body: &str,
) -> (StatusCode, serde_json::Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or_default())
}

#[tokio::test]
async fn cast_routes_are_absent_unless_enabled() {
    let app = router(state());
    for (method, uri) in [
        ("GET", "/cast/targets"),
        ("POST", "/cast/targets/abc/play"),
        ("POST", "/cast/targets/abc/stop"),
        ("GET", "/cast/targets/abc/status"),
        ("GET", "/cast/s/abc.ts"),
    ] {
        assert_eq!(
            send(&app, method, uri, "").await.0,
            StatusCode::NOT_FOUND,
            "{uri}"
        );
    }
}

#[tokio::test]
async fn play_on_a_loopback_bind_is_a_clear_conflict() {
    let (app, _) = cast_app("127.0.0.1:6878");
    let (status, body) = send(
        &app,
        "POST",
        "/cast/targets/abc/play",
        r#"{"network":"test","id":"x"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "loopback_bind");
    assert!(body["message"].as_str().unwrap().contains("OUTPACE_BIND"));
}

#[tokio::test]
async fn play_validates_the_request_before_contacting_a_renderer() {
    let (app, _) = cast_app("192.168.1.2:6878");
    for (body, code) in [
        ("not json", "invalid_request"),
        (r#"{"network":"nope","id":"x"}"#, "unknown_network"),
        (r#"{"network":"test","id":"../x"}"#, "invalid_id"),
        (r#"{"network":"test","id":""}"#, "invalid_id"),
    ] {
        let (status, json) = send(&app, "POST", "/cast/targets/abc/play", body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(json["error"], code, "{body}");
    }
    let (status, json) = send(
        &app,
        "POST",
        "/cast/targets/abc/play",
        r#"{"network":"test","id":"x"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json["error"], "unknown_target");
    for (method, uri) in [
        ("POST", "/cast/targets/abc/stop"),
        ("GET", "/cast/targets/abc/status"),
    ] {
        let (status, json) = send(&app, method, uri, "").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(json["error"], "unknown_target");
    }
}

#[tokio::test]
async fn cast_stream_serves_the_shared_session_and_counts_the_fetch() {
    let (app, cast) = cast_app("192.168.1.2:6878");
    assert_eq!(
        send(&app, "GET", "/cast/s/0000.ts", "").await.0,
        StatusCode::NOT_FOUND
    );
    let renderer = crate::cast::test_support::target("t1", "127.0.0.1:9".parse().unwrap());
    let token = cast.insert_cast_for_test(renderer, "test", "abc");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/cast/s/{token}.ts"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "video/mp2t");
    assert_eq!(response.headers()["transfermode.dlna.org"], "Streaming");
    let (active, guard) = cast.open_stream(&token).unwrap();
    drop(guard);
    assert_eq!(
        active.connections(),
        1,
        "the open response holds one connection"
    );
    drop(response);
    assert_eq!(active.connections(), 0);
}

#[tokio::test]
async fn play_starts_the_stream_then_drives_the_renderer() {
    let (addr, renderer) = crate::cast::tests::fake_renderer(vec![
        (200, crate::cast::tests::OK),
        (200, crate::cast::tests::OK),
    ])
    .await;
    let (app, cast) = cast_app("192.168.1.2:6878");
    cast.insert_target_for_test(crate::cast::test_support::target("t1", addr));
    let (status, json) = send(
        &app,
        "POST",
        "/cast/targets/t1/play",
        r#"{"network":"test","id":"abc"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["target"], "t1");
    assert_eq!(json["network"], "test");
    assert_eq!(json["id"], "abc");
    let url = json["url"].as_str().unwrap();
    assert!(url.starts_with("http://192.168.1.2:6878/cast/s/") && url.ends_with(".ts"));
    let actions: Vec<_> = renderer
        .await
        .unwrap()
        .into_iter()
        .map(|(a, _)| a)
        .collect();
    assert_eq!(actions.len(), 2);
    assert!(actions[0].ends_with("#SetAVTransportURI\""));
    assert!(actions[1].ends_with("#Play\""));
}

#[tokio::test]
async fn cast_stream_answers_head_and_routes_check_the_method() {
    let (app, cast) = cast_app("192.168.1.2:6878");
    let renderer = crate::cast::test_support::target("t1", "127.0.0.1:9".parse().unwrap());
    let token = cast.insert_cast_for_test(renderer, "test", "abc");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/cast/s/{token}.ts"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "video/mp2t");
    drop(response);
    for (method, uri) in [
        ("GET", "/cast/targets/t1/play".to_string()),
        ("POST", "/cast/targets".to_string()),
        ("DELETE", "/cast/targets/t1/stop".to_string()),
        ("GET", format!("/cast/s/{token}")),
        ("GET", "/cast/targets/t1/play/extra".to_string()),
    ] {
        assert_eq!(
            send(&app, method, &uri, "").await.0,
            StatusCode::NOT_FOUND,
            "{method} {uri}"
        );
    }
}
