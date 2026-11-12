use super::*;
use crate::acestream_search::{offline_service, SearchError};

async fn request(app: Router, path: &str) -> Response {
    app.oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}
#[tokio::test]
async fn services_and_legacy_http_flags_are_independent() {
    for compat in [false, true] {
        for services in [false, true] {
            let mut state = state();
            state.experimental_ace_compat = compat;
            if services {
                state.search = Some(offline_service(Ok(
                    br#"{"total":0,"time":0.0,"results":[]}"#.to_vec(),
                )));
            }
            let app = router(state);
            let response = request(app.clone(), "/search").await;
            assert_eq!(
                response.status(),
                if services {
                    StatusCode::OK
                } else {
                    StatusCode::NOT_FOUND
                }
            );
            if services {
                let body: serde_json::Value =
                    serde_json::from_slice(&response_bytes(response).await).unwrap();
                assert_eq!(
                    body,
                    json!({"result":{"total":0,"results":[]},"error":null})
                );
            }
            assert_eq!(
                request(app.clone(), "/server/api?method=get_version")
                    .await
                    .status(),
                if compat {
                    StatusCode::OK
                } else {
                    StatusCode::NOT_FOUND
                }
            );
            assert_eq!(request(app, "/healthz").await.status(), StatusCode::OK);
        }
    }
}
#[tokio::test]
async fn malformed_queries_fail_before_consuming_the_upstream_reply() {
    let mut state = state();
    state.search = Some(offline_service(Ok(
        br#"{"total":0,"time":0.0,"results":[]}"#.to_vec(),
    )));
    let app = router(state);
    for path in [
        "/search?page_size=0",
        "/search?page=-1",
        "/search?query=a&query=b",
        "/search?api_key=client_key",
    ] {
        assert_eq!(
            request(app.clone(), path).await.status(),
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(request(app, "/search").await.status(), StatusCode::OK);
}
#[tokio::test]
async fn search_failure_is_distinct_from_empty_and_does_not_break_known_inputs() {
    let mut state = state();
    state.search = Some(offline_service(Err(SearchError::Upstream)));
    let app = router(state);
    let response = request(app.clone(), "/search").await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body: serde_json::Value = serde_json::from_slice(&response_bytes(response).await).unwrap();
    assert!(body["result"].is_null());
    assert_eq!(body["error"], "search_upstream_unavailable");
    assert_eq!(
        request(app.clone(), "/search").await.status(),
        StatusCode::TOO_MANY_REQUESTS
    );
    let response = request(app, "/streams/test/synthetic-known-source").await;
    assert_eq!(response.status(), StatusCode::OK);
}
#[tokio::test]
async fn private_peer_mode_does_not_expose_external_search_even_for_rust_callers() {
    let mut state = state();
    state.search = Some(offline_service(Err(SearchError::Upstream)));
    state.peer_policy =
        ace_swarm::peer_policy::PeerPolicy::from_private_peers(Some("127.0.0.1:8621")).unwrap();
    assert_eq!(
        request(router(state), "/search").await.status(),
        StatusCode::NOT_FOUND
    );
}
