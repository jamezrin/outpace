use super::*;

use crate::broadcast_persist::{
    tests_pull206::persistence_observer, BroadcastPersist, PersistOperation,
};
use crate::broadcast_pull::{
    validate_source, BroadcastSourceConfig, SourceTestHooks, TestSourceOpen,
};
use ace_swarm::http_fetch::{FetchError, FetchPolicy};
use axum::http::header::CONTENT_TYPE;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn source_state() -> AppState {
    let mut app = state();
    app.broadcasts = Some(BroadcastState {
        registry: crate::broadcast::BroadcastRegistry::new(),
        seed_registry: ace_swarm::listen::SeedRegistry::new(),
        trackers: vec![],
        store_bytes: 4 << 20,
        inbound_peer_port: tokio::sync::watch::channel(None).1,
    });
    app
}

fn request(
    method: axum::http::Method,
    uri: &str,
    body: &str,
    json_content_type: bool,
) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if json_content_type {
        builder = builder.header(CONTENT_TYPE, "application/json");
    }
    builder.body(Body::from(body.to_owned())).unwrap()
}

async fn error_json(response: axum::response::Response) -> (StatusCode, Value) {
    let status = response.status();
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
    let body = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

fn ts_body_for_one_piece() -> Vec<u8> {
    let packets = (65_536usize.div_ceil(188)) + 16;
    let mut body = Vec::with_capacity(packets * 188);
    for index in 0..packets {
        body.push(0x47);
        body.extend(std::iter::repeat_n((index % 251) as u8, 187));
    }
    body
}

async fn loopback_source(
    listener: tokio::net::TcpListener,
    body: Vec<u8>,
    accepted: tokio::sync::oneshot::Sender<()>,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) {
    let (mut socket, _) = tokio::select! {
        biased;
        _ = &mut stop => return,
        result = listener.accept() => result.unwrap(),
    };
    let mut reader = BufReader::new(&mut socket);
    let mut line = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line).await.unwrap() == 0 || line == b"\r\n" {
            break;
        }
    }
    let socket = reader.get_mut();
    socket
        .write_all(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n",
        )
        .await
        .unwrap();
    socket
        .write_all(format!("{:x}\r\n", body.len()).as_bytes())
        .await
        .unwrap();
    socket.write_all(&body).await.unwrap();
    socket.write_all(b"\r\n").await.unwrap();
    let _ = accepted.send(());
    let _ = stop.await;
}

async fn read_http_request(socket: &mut tokio::net::TcpStream) {
    let mut reader = BufReader::new(socket);
    let mut line = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line).await.unwrap() == 0 || line == b"\r\n" {
            break;
        }
    }
}

fn ts_bytes_for_payload(payload_bytes: usize, marker: u8) -> Vec<u8> {
    let packets = payload_bytes.div_ceil(188) + 2;
    let mut bytes = Vec::with_capacity(packets * 188);
    for _ in 0..packets {
        bytes.push(0x47);
        bytes.extend(std::iter::repeat_n(marker, 187));
    }
    bytes
}

async fn reconnect_source(
    listener: tokio::net::TcpListener,
    first_attempt: Vec<u8>,
    resumed_attempt: Vec<u8>,
    accepted: tokio::sync::mpsc::UnboundedSender<usize>,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) {
    let (mut first, _) = listener.accept().await.unwrap();
    read_http_request(&mut first).await;
    first
        .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    first
        .write_all(format!("{:x}\r\n", first_attempt.len()).as_bytes())
        .await
        .unwrap();
    first.write_all(&first_attempt).await.unwrap();
    first.write_all(b"\r\n").await.unwrap();
    first.shutdown().await.unwrap(); // Deliberately omit the terminating chunk.
    drop(first);
    let _ = accepted.send(1);

    let (mut second, _) = tokio::select! {
        biased;
        _ = &mut stop => return,
        connection = listener.accept() => connection.unwrap(),
    };
    read_http_request(&mut second).await;
    second
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    second.shutdown().await.unwrap();
    drop(second);
    let _ = accepted.send(2);

    let (mut third, _) = tokio::select! {
        biased;
        _ = &mut stop => return,
        connection = listener.accept() => connection.unwrap(),
    };
    read_http_request(&mut third).await;
    third
        .write_all(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n",
        )
        .await
        .unwrap();
    third
        .write_all(format!("{:x}\r\n", resumed_attempt.len()).as_bytes())
        .await
        .unwrap();
    third.write_all(&resumed_attempt).await.unwrap();
    third.write_all(b"\r\n").await.unwrap();
    let _ = accepted.send(3);
    let _ = stop.await;
}

async fn source_runtime() -> (AppState, crate::runtime::EngineRuntime) {
    let dir = std::env::temp_dir().join(format!("outpace-source-api-{}", rand::random::<u64>()));
    let runtime = crate::runtime::build_runtime(
        crate::config::Config {
            data_dir: dir,
            networks: vec![],
            enable_inbound: false,
            seed_ttl_secs: 0,
            allow_non_global_broadcast_source: true,
            ..crate::config::Config::default()
        },
        vec![],
    )
    .await
    .unwrap();
    let app = AppState {
        manager: runtime.manager.clone(),
        networks: runtime.networks.clone(),
        resolve_content_ids_in_getstream: false,
        ace_sessions: Arc::new(AceSessionStore::default()),
        experimental_ace_compat: false,
        broadcasts: Some(runtime.broadcasts.clone()),
    };
    (app, runtime)
}

#[tokio::test]
async fn pull206_source_api_contract() {
    let app = router(source_state());

    let response = app
        .clone()
        .oneshot(request(
            axum::http::Method::PUT,
            "/broadcast/source-contract/source",
            r#"{"url":"http://127.0.0.1/live"}"#,
            true,
        ))
        .await
        .unwrap();
    let (status, body) = error_json(response).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body, json!({"error":{"code":"source_address_blocked"}}));

    let response = app
        .clone()
        .oneshot(request(
            axum::http::Method::PUT,
            "/broadcast/source-contract/source",
            "{}",
            false,
        ))
        .await
        .unwrap();
    let (status, body) = error_json(response).await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(
        body,
        json!({"error":{"code":"source_content_type_required"}})
    );

    let response = app
        .clone()
        .oneshot(request(
            axum::http::Method::PUT,
            "/broadcast/source-contract/source",
            "{",
            true,
        ))
        .await
        .unwrap();
    let (status, body) = error_json(response).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body, json!({"error":{"code":"invalid_source_config"}}));

    let response = app
        .clone()
        .oneshot(request(
            axum::http::Method::PUT,
            "/broadcast/source-contract/source",
            r#"{"url":"https://source.invalid/a","url":"https://source.invalid/b"}"#,
            true,
        ))
        .await
        .unwrap();
    let (status, body) = error_json(response).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body, json!({"error":{"code":"invalid_source_config"}}));

    let response = app
        .clone()
        .oneshot(request(
            axum::http::Method::PUT,
            "/broadcast/source-contract/source",
            &format!(
                "{{\"url\":\"https://source.invalid/{}\"}}",
                "x".repeat(16 * 1024)
            ),
            true,
        ))
        .await
        .unwrap();
    let (status, body) = error_json(response).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(body, json!({"error":{"code":"source_request_too_large"}}));
}

#[tokio::test]
async fn pull206_source_api_returns_fixed_persistence_and_stopping_errors() {
    let dir = std::env::temp_dir().join(format!(
        "outpace-source-api-error-{}",
        rand::random::<u64>()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let observer = persistence_observer();
    let registry = crate::broadcast::BroadcastRegistry::with_test_persist(
        BroadcastPersist::with_test_io(&dir, observer.clone()),
    );
    registry.set_source_policy(FetchPolicy {
        allow_non_global: true,
    });
    let mut app_state = state();
    app_state.broadcasts = Some(BroadcastState {
        registry: registry.clone(),
        seed_registry: ace_swarm::listen::SeedRegistry::new(),
        trackers: vec![],
        store_bytes: 4 << 20,
        inbound_peer_port: tokio::sync::watch::channel(None).1,
    });

    // Fail the real initial durable identity save reached through the HTTP PUT handler.
    observer.fail_next(PersistOperation::FileSync);
    let response = router(app_state)
        .oneshot(request(
            axum::http::Method::PUT,
            "/broadcast/persistence-route/source",
            r#"{"url":"https://source.invalid/private"}"#,
            true,
        ))
        .await
        .unwrap();
    let (status, body) = error_json(response).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        body,
        json!({"error":{"code":"broadcast_persistence_failed"}})
    );

    let stopping_state = source_state();
    stopping_state
        .broadcasts
        .as_ref()
        .unwrap()
        .registry
        .close_admissions();
    let response = router(stopping_state)
        .oneshot(request(
            axum::http::Method::PUT,
            "/broadcast/stopping-route/source",
            r#"{"url":"https://source.invalid/private"}"#,
            true,
        ))
        .await
        .unwrap();
    let (status, body) = error_json(response).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body, json!({"error":{"code":"broadcast_service_stopping"}}));

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn pull206_source_api_contract_get_never_mints_and_unknown_detach_is_idempotent() {
    let app_state = source_state();
    let registry = app_state.broadcasts.as_ref().unwrap().registry.clone();
    let app = router(app_state);

    let response = app
        .clone()
        .oneshot(
            Request::get("/broadcast/not-created/source")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, body) = error_json(response).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, json!({"error":{"code":"source_not_found"}}));
    assert!(registry.get("not-created").await.is_none());

    let response = app
        .oneshot(
            Request::delete("/broadcast/not-created/source")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(registry.get("not-created").await.is_none());
}

#[tokio::test]
async fn pull206_source_api_contract_validates_name_before_disabled_state() {
    let app = router(state()); // no BroadcastState configured
    let response = app
        .clone()
        .oneshot(request(
            axum::http::Method::GET,
            "/broadcast/bad%20name/source",
            "",
            false,
        ))
        .await
        .unwrap();
    let (status, body) = error_json(response).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body, json!({"error":{"code":"invalid_name"}}));

    let response = app
        .oneshot(
            Request::get("/broadcast/valid-name/source")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, body) = error_json(response).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, json!({"error":{"code":"broadcasts_disabled"}}));
}

#[tokio::test]
async fn pull206_source_api_contract_preserves_raw_and_descriptor_success_shapes() {
    let app = router(source_state());
    let response = app
        .clone()
        .oneshot(
            Request::put("/broadcast/source-compat")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["name"], "source-compat");
    assert!(value["content_id"].as_str().is_some());
    assert!(value["infohash"].as_str().is_some());
    assert!(value.get("source").is_none());

    let response = app
        .oneshot(
            Request::get("/broadcast/source-compat")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/octet-stream"
    );
    let body = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    assert!(ace_wire::infohash::is_transport_file(&body));
}

#[tokio::test]
async fn pull206_source_api_contract_idempotent_attach_status_detach_and_identity_retention() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source_url = format!(
        "http://{}/live?token=SYNTHETIC_API_MARKER",
        listener.local_addr().unwrap()
    );
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let source_task = tokio::spawn(loopback_source(
        listener,
        ts_body_for_one_piece(),
        accepted_tx,
        stop_rx,
    ));
    let (app_state, runtime) = source_runtime().await;
    let registry = runtime.broadcasts.registry.clone();
    let app = router(app_state);

    let body_with_defaults = json!({"url": source_url.clone()}).to_string();
    let response = app
        .clone()
        .oneshot(request(
            axum::http::Method::PUT,
            "/broadcast/api-source/source",
            &body_with_defaults,
            true,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let first: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(first.as_object().unwrap().len(), 4);
    assert_eq!(first["name"], "api-source");
    assert!(first["content_id"].as_str().is_some());
    assert!(first["infohash"].as_str().is_some());
    assert!(matches!(
        first["source"]["state"].as_str(),
        Some("connecting" | "streaming")
    ));
    assert_eq!(first["source"].as_object().unwrap().len(), 4);
    assert!(!first.to_string().contains("SYNTHETIC_API_MARKER"));
    tokio::time::timeout(Duration::from_secs(3), accepted_rx)
        .await
        .expect("pull worker should reach the loopback source")
        .expect("source script should accept one connection");

    let response = app
        .clone()
        .oneshot(request(
            axum::http::Method::PUT,
            "/broadcast/api-source/source",
            &json!({
                "url": source_url.clone(),
                "connect_timeout_secs": 5,
                "read_timeout_secs": 15,
                "reconnect_initial_secs": 1,
                "reconnect_max_secs": 30,
            })
            .to_string(),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let second: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(first["content_id"], second["content_id"]);
    assert_eq!(first["infohash"], second["infohash"]);
    assert!(
        second["source"]["attempts"].as_u64().unwrap()
            >= first["source"]["attempts"].as_u64().unwrap()
    );

    let status_response = app
        .clone()
        .oneshot(
            Request::get("/broadcast/api-source/source")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(status_response.status(), StatusCode::OK);
    let status_value: Value = serde_json::from_slice(
        &axum::body::to_bytes(status_response.into_body(), 1 << 20)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(status_value.as_object().unwrap().len(), 2);
    assert_eq!(status_value["name"], "api-source");
    assert_eq!(status_value["source"].as_object().unwrap().len(), 4);
    assert!(!status_value.to_string().contains("SYNTHETIC_API_MARKER"));

    let raw_conflict = app
        .clone()
        .oneshot(
            Request::put("/broadcast/api-source")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, body) = error_json(raw_conflict).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body, json!({"error":{"code":"producer_conflict"}}));

    let conflict = app
        .clone()
        .oneshot(request(
            axum::http::Method::PUT,
            "/broadcast/api-source/source",
            &json!({
                "url": source_url.clone(),
                "reconnect_initial_secs": 2,
            })
            .to_string(),
            true,
        ))
        .await
        .unwrap();
    let (status, body) = error_json(conflict).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body, json!({"error":{"code":"source_conflict"}}));

    let other_token_url = source_url.replace("SYNTHETIC_API_MARKER", "SYNTHETIC_OTHER_MARKER");
    let token_conflict = app
        .clone()
        .oneshot(request(
            axum::http::Method::PUT,
            "/broadcast/api-source/source",
            &json!({"url": other_token_url}).to_string(),
            true,
        ))
        .await
        .unwrap();
    let (status, body) = error_json(token_conflict).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body, json!({"error":{"code":"source_conflict"}}));

    let delete_request = || {
        Request::delete("/broadcast/api-source/source")
            .body(Body::empty())
            .unwrap()
    };
    let (first_delete, second_delete) = tokio::join!(
        app.clone().oneshot(delete_request()),
        app.clone().oneshot(delete_request()),
    );
    assert_eq!(first_delete.unwrap().status(), StatusCode::NO_CONTENT);
    assert_eq!(second_delete.unwrap().status(), StatusCode::NO_CONTENT);
    assert!(registry.get("api-source").await.is_some());
    let descriptor = app
        .clone()
        .oneshot(
            Request::get("/broadcast/api-source")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(descriptor.status(), StatusCode::OK);
    let detached = app
        .oneshot(
            Request::get("/broadcast/api-source/source")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, body) = error_json(detached).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, json!({"error":{"code":"source_not_found"}}));

    runtime.broadcasts.shutdown().await.unwrap();
    runtime.manager.shutdown().await;
    let _ = stop_tx.send(());
    let _ = source_task.await;
    let _ = std::fs::remove_dir_all(&runtime.config.data_dir);
}

#[tokio::test]
async fn options51_api_pull_attach_uses_the_named_profile() {
    let mut app_state = source_state();
    let registry =
        crate::broadcast::BroadcastRegistry::new_with_options(std::collections::BTreeMap::from([
            (
                "synthetic-origin".to_owned(),
                crate::broadcast_options::BroadcastOptions {
                    title: Some("Synthetic title".to_owned()),
                    bitrate_bytes_per_sec: Some(500_000),
                    piece_length: Some(131_072),
                    quality: Some("HD".to_owned()),
                    categories: Some(vec![]),
                },
            ),
        ]))
        .unwrap();
    registry.set_source_policy(FetchPolicy {
        allow_non_global: true,
    });
    let (opened_sender, mut opened_rx) = tokio::sync::oneshot::channel::<()>();
    let opened_sender = Arc::new(std::sync::Mutex::new(Some(opened_sender)));
    let open: TestSourceOpen = Arc::new(move |_, _, _, _| {
        let opened_sender = opened_sender.clone();
        Box::pin(async move {
            if let Some(sender) = opened_sender.lock().unwrap().take() {
                let _ = sender.send(());
            }
            std::future::pending::<()>().await;
            Err(FetchError::Cancelled)
        })
    });
    registry.set_source_test_hooks(SourceTestHooks {
        open: Some(open),
        ..SourceTestHooks::default()
    });
    app_state.broadcasts.as_mut().unwrap().registry = registry.clone();

    let response = router(app_state)
        .oneshot(request(
            axum::http::Method::PUT,
            "/broadcast/synthetic-origin/source",
            r#"{"url":"http://127.0.0.1:1/live"}"#,
            true,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    tokio::time::timeout(Duration::from_secs(2), &mut opened_rx)
        .await
        .expect("the real source worker should reach its fake opener")
        .unwrap();
    let broadcast = registry.get("synthetic-origin").await.unwrap();
    let decoded = ace_wire::transport::decode_transport(&broadcast.transport_bytes).unwrap();
    assert_eq!(decoded.name, "Synthetic title");
    assert_eq!(decoded.bitrate, Some(500_000));
    assert_eq!(decoded.piece_length, 131_072);
    assert_eq!(
        decoded.raw.get(b"quality").unwrap().as_bytes(),
        Some(b"HD".as_slice())
    );
    assert!(
        matches!(decoded.raw.get(b"categories"), Some(ace_wire::bencode::Bencode::List(items)) if items.is_empty())
    );
    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn pull206_reconnect_signatures_and_tails() {
    const PIECE_LENGTH: usize = 65_536;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source_url = format!("http://{}/live", listener.local_addr().unwrap());
    let (accepted_tx, mut accepted_rx) = tokio::sync::mpsc::unbounded_channel();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let first_attempt = ts_bytes_for_payload(PIECE_LENGTH * 3 + 400, 0x11);
    let mut first_attempt = first_attempt;
    first_attempt.extend(std::iter::repeat_n(0x22, 37)); // disconnect in a TS packet
    let resumed_attempt = ts_bytes_for_payload(PIECE_LENGTH + 400, 0xa5);
    let source_task = tokio::spawn(reconnect_source(
        listener,
        first_attempt,
        resumed_attempt,
        accepted_tx,
        stop_rx,
    ));

    let (app_state, runtime) = source_runtime().await;
    let broadcasts = runtime.broadcasts.clone();
    let registry = broadcasts.registry.clone();
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();
    registry.set_source_test_hooks(SourceTestHooks {
        on_ingest: Some(Arc::new(move |progress| {
            let _ = progress_tx.send(progress);
        })),
        ..SourceTestHooks::default()
    });
    let app = router(app_state);
    let attach = app
        .oneshot(request(
            axum::http::Method::PUT,
            "/broadcast/reconnect-source/source",
            &json!({
                "url": source_url,
                "reconnect_initial_secs": 1,
                "reconnect_max_secs": 1,
            })
            .to_string(),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(attach.status(), StatusCode::ACCEPTED);
    let accepted: Value = serde_json::from_slice(
        &axum::body::to_bytes(attach.into_body(), 1 << 20)
            .await
            .unwrap(),
    )
    .unwrap();
    let infohash = hex::decode(accepted["infohash"].as_str().unwrap()).unwrap();
    let infohash: [u8; 20] = infohash.try_into().unwrap();

    tokio::time::timeout(Duration::from_secs(8), async {
        let mut committed = 0u64;
        while committed < 4 {
            let progress = progress_rx
                .recv()
                .await
                .expect("worker progress channel closed");
            committed += progress.committed_pieces;
        }
    })
    .await
    .expect("three first-attempt pieces and the resumed piece should commit");
    tokio::time::timeout(Duration::from_secs(2), async {
        while accepted_rx.recv().await != Some(3) {}
    })
    .await
    .expect("clean EOF should reconnect to the third scripted response");

    let broadcast = registry.get("reconnect-source").await.unwrap();
    assert_eq!(broadcast.infohash, infohash);
    assert_eq!(broadcast.cursor.start_piece(), 4);
    let store = broadcasts.seed_registry.get(&infohash).unwrap();
    let store = store.lock().await;
    assert!((0..4).all(|piece| store.has_piece(piece)));
    assert!(
        !store.has_piece(4),
        "abandoned tails must not expose the next index"
    );

    let chunks_per_piece = store.chunks_per_piece();
    let mut reassembler = ace_wire::reassembly::PieceReassembler::new(65_536, 0)
        .with_source_pubkey(broadcast.auth.pubkey_der());
    for piece in 0..4u64 {
        for chunk in 0..chunks_per_piece {
            let bytes = store.chunk(piece, chunk).unwrap();
            reassembler
                .add_block(
                    piece,
                    u64::from(chunk) * crate::broadcast::CHUNK_LENGTH,
                    &bytes,
                )
                .unwrap();
        }
    }
    let media = reassembler.take_ready();
    let signature_len = broadcast.auth.signature_len();
    let payload_bytes = PIECE_LENGTH - signature_len;
    assert_eq!(media.len(), payload_bytes * 4);
    assert_eq!(media[payload_bytes * 3], 0x47);
    assert!(
        media[payload_bytes * 3 + 1..payload_bytes * 3 + 188]
            .iter()
            .all(|byte| *byte == 0xa5),
        "the resumed piece must contain only its new source payload"
    );

    drop(store);
    broadcasts.detach_source("reconnect-source").await.unwrap();
    runtime.broadcasts.shutdown().await.unwrap();
    runtime.manager.shutdown().await;
    let _ = stop_tx.send(());
    let _ = source_task.await;
    let _ = std::fs::remove_dir_all(&runtime.config.data_dir);
}

#[tokio::test]
async fn pull206_canceled_attach_keeps_owned_worker() {
    let app = source_state();
    let broadcasts = app.broadcasts.as_ref().unwrap().clone();
    let registry = broadcasts.registry.clone();
    let policy = FetchPolicy {
        allow_non_global: true,
    };
    registry.set_source_policy(policy);

    let open_count = Arc::new(AtomicUsize::new(0));
    let open_count_for_hook = open_count.clone();
    let (opened_tx, mut opened_rx) = tokio::sync::mpsc::unbounded_channel();
    let open: TestSourceOpen = Arc::new(move |_, _, _, mut cancel| {
        let opened = opened_tx.clone();
        let count = open_count_for_hook.clone();
        Box::pin(async move {
            count.fetch_add(1, Ordering::SeqCst);
            let _ = opened.send(());
            tokio::select! {
                _ = cancel.changed() => Err(FetchError::Cancelled),
                () = std::future::pending() => unreachable!(),
            }
        })
    });
    let (committed_tx, mut committed_rx) = tokio::sync::mpsc::unbounded_channel();
    let after_commit = Arc::new(move || {
        let committed = committed_tx.clone();
        Box::pin(async move {
            let _ = committed.send(());
            std::future::pending().await
        }) as futures::future::BoxFuture<'static, ()>
    });
    registry.set_source_test_hooks(SourceTestHooks {
        open: Some(open),
        after_commit: Some(after_commit),
        ..SourceTestHooks::default()
    });

    let config = BroadcastSourceConfig {
        url: "http://127.0.0.1:1/live".into(),
        connect_timeout_secs: 5,
        read_timeout_secs: 15,
        reconnect_initial_secs: 1,
        reconnect_max_secs: 30,
    };
    assert!(validate_source(config.clone(), policy).is_ok());
    let source_router = router(app);
    let attach = tokio::spawn(async move {
        source_router
            .oneshot(request(
                axum::http::Method::PUT,
                "/broadcast/cancel-attach/source",
                &serde_json::to_string(&config).unwrap(),
                true,
            ))
            .await
    });

    tokio::time::timeout(Duration::from_secs(2), committed_rx.recv())
        .await
        .expect("commit hook should run")
        .expect("commit notification should be sent");
    tokio::time::timeout(Duration::from_secs(2), opened_rx.recv())
        .await
        .expect("committed worker should start")
        .expect("worker should reach its injected open boundary");

    attach.abort();
    let _ = attach.await;
    assert_eq!(open_count.load(Ordering::SeqCst), 1);
    let status = broadcasts
        .source_status("cancel-attach")
        .await
        .unwrap()
        .expect("committed source stays visible after request cancellation");
    assert_eq!(status.attempts, 1);
    match broadcasts
        .acquire_producer("cancel-attach", crate::broadcast::ProducerKind::RawPut)
        .await
    {
        Err(crate::broadcast::BroadcastError::ProducerConflict) => {}
        Err(other) => panic!("expected retained pull claim, got {other:?}"),
        Ok(_) => panic!("raw producer took over the committed pull claim"),
    }

    broadcasts.detach_source("cancel-attach").await.unwrap();
    assert!(broadcasts
        .source_status("cancel-attach")
        .await
        .unwrap()
        .is_none());
}
