use super::*;

fn broadcast_state() -> AppState {
    let mut st = state();
    st.broadcasts = Some(BroadcastState {
        registry: crate::broadcast::BroadcastRegistry::new(),
        seed_registry: ace_swarm::listen::SeedRegistry::new(),
        trackers: vec![],
        // A few pieces' worth of headroom (piece_length is 1 MiB) — big enough that a
        // freshly-completed piece 0 isn't immediately evicted the instant piece 1's
        // first chunk arrives (PieceStore evicts the lowest piece once over budget).
        store_bytes: 4 << 20,
        // No inbound listener in tests -> self-announce is a no-op (see the field doc);
        // keeps these tests offline/instant instead of hitting the network.
        inbound_peer_port: tokio::sync::watch::channel(None).1,
    });
    st
}

#[tokio::test]
async fn broadcast_put_returns_404_when_disabled() {
    let resp = router(state()) // plain `state()`: broadcasts: None
        .oneshot(
            Request::put("/broadcast/x")
                .body(Body::from(vec![0u8; 4]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn broadcast_get_404s_until_minted_then_serves_the_transport_bytes() {
    let st = broadcast_state();
    let app = router(st);

    let resp = app
        .clone()
        .oneshot(Request::get("/broadcast/x").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "not minted yet");

    let put_resp = app
        .clone()
        .oneshot(
            Request::put("/broadcast/x")
                .body(Body::from(vec![0x47u8; 8]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put_resp.status(), StatusCode::OK);

    let get_resp = app
        .oneshot(Request::get("/broadcast/x").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(get_resp.into_body(), 1 << 20)
        .await
        .unwrap();
    assert!(
        ace_wire::infohash::is_transport_file(&body),
        "GET /broadcast/{{name}} must serve real transport-file bytes"
    );
}

#[tokio::test]
async fn options51_raw_put_uses_the_named_profile() {
    let mut app_state = broadcast_state();
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
    app_state.broadcasts.as_mut().unwrap().registry = registry.clone();
    let app = router(app_state);
    let response = app
        .oneshot(
            Request::put("/broadcast/synthetic-origin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
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
async fn options51_raw_put_conflict_uses_the_existing_http_envelope() {
    let dir = std::env::temp_dir().join(format!(
        "outpace-options51-http-conflict-{}",
        rand::random::<u64>()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let original = crate::broadcast::BroadcastRegistry::with_persist(
        &dir,
        crate::config::CacheType::Memory,
        std::path::PathBuf::new(),
    );
    original
        .mint(
            "synthetic-origin",
            &[],
            &ace_swarm::listen::SeedRegistry::new(),
            4 << 20,
        )
        .await
        .unwrap();
    original.shutdown().await.unwrap();

    let registry = crate::broadcast::BroadcastRegistry::with_persist_and_options(
        &dir,
        crate::config::CacheType::Memory,
        std::path::PathBuf::new(),
        std::collections::BTreeMap::from([(
            "synthetic-origin".to_owned(),
            crate::broadcast_options::BroadcastOptions {
                title: Some("Conflicting synthetic title".to_owned()),
                ..Default::default()
            },
        )]),
    )
    .unwrap();
    let mut app_state = broadcast_state();
    app_state.broadcasts.as_mut().unwrap().registry = registry.clone();
    let response = router(app_state)
        .oneshot(
            Request::put("/broadcast/synthetic-origin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({"error":{"code":"broadcast_options_conflict"}})
    );
    registry.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn broadcast_put_mints_and_serves_via_the_shared_seed_registry() {
    let st = broadcast_state();
    let seed_registry = st.broadcasts.as_ref().unwrap().seed_registry.clone();
    // Hold the `BroadcastRegistry` Arc alive for the test's duration too: its `by_name` map
    // now anchors each broadcast's `SeedLease` (Task 5), and `app.oneshot(..)` consumes (and
    // drops) the whole router — including its `AppState` clone — after each request.
    let _registry = st.broadcasts.as_ref().unwrap().registry.clone();
    let app = router(st);

    // A run of 188-byte packets each starting with the TS sync byte (0x47) — enough of
    // them, past TsResync's one-packet lookahead, to yield >= CHUNK_LENGTH bytes of
    // sync-locked output for the chunker.
    const TS_PACKET_LEN: usize = 188;
    let packets_needed = CHUNK_LENGTH.div_ceil(TS_PACKET_LEN as u64) as usize + 1;
    let mut body = Vec::with_capacity(packets_needed * TS_PACKET_LEN);
    for i in 0..packets_needed {
        body.push(0x47);
        body.extend(std::iter::repeat_n((i % 256) as u8, TS_PACKET_LEN - 1));
    }
    let resp = app
        .oneshot(
            Request::put("/broadcast/mychan")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let respbody = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&respbody).unwrap();
    let infohash_hex = json["infohash"].as_str().unwrap().to_string();
    assert_eq!(infohash_hex.len(), 40, "a 20-byte infohash, hex-encoded");
    let content_id_hex = json["content_id"].as_str().unwrap().to_string();
    assert_eq!(
        content_id_hex.len(),
        40,
        "a 20-byte content-id, hex-encoded"
    );
    assert_eq!(json["name"], "mychan");

    // The minted infohash must be immediately servable via the shared registry (S1/S2's
    // existing serve path) — confirming the wiring, not just the HTTP response shape.
    let mut infohash = [0u8; 20];
    for i in 0..20 {
        infohash[i] = u8::from_str_radix(&infohash_hex[i * 2..i * 2 + 2], 16).unwrap();
    }
    let mut content_id = [0u8; 20];
    for i in 0..20 {
        content_id[i] = u8::from_str_radix(&content_id_hex[i * 2..i * 2 + 2], 16).unwrap();
    }
    assert!(seed_registry.serves(&infohash));
    assert!(seed_registry.serves(&content_id));

    // Give the background ingest task a moment to process the body. It's short (well
    // under a full 1 MiB piece), so this reaches the store via `SigningChunker::flush`'s
    // short-final-piece path (see `ace_wire::signing_chunker`), not a full-piece `push`.
    for _ in 0..50 {
        if seed_registry
            .get(&infohash)
            .unwrap()
            .lock()
            .await
            .chunk(0, 0)
            .is_some()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        seed_registry
            .get(&infohash)
            .unwrap()
            .lock()
            .await
            .chunk(0, 0)
            .is_some(),
        "ingested bytes must reach the piece store as chunk (0, 0)"
    );
}

#[tokio::test]
async fn ingested_piece_carries_a_real_verifiable_signature() {
    // A full 1 MiB piece's worth of body (piece_length - signature_len of real TS-shaped
    // content) so the ingest goes through SigningChunker's ordinary `push` path (a
    // complete, signed piece), not the short-final-piece `flush` path the other test
    // covers. Proves the actual B0 signing crack end to end through the real HTTP ingest
    // handler, not just the unit-level SigningChunker tests.
    let st = broadcast_state();
    let seed_registry = st.broadcasts.as_ref().unwrap().seed_registry.clone();
    let registry = st.broadcasts.as_ref().unwrap().registry.clone();
    let app = router(st);

    const TS_PACKET_LEN: usize = 188;
    let payload_capacity = (PIECE_LENGTH - 96) as usize; // 96 = 768-bit RSA signature_len
    let packets_needed = payload_capacity.div_ceil(TS_PACKET_LEN) + 1;
    let mut body = Vec::with_capacity(packets_needed * TS_PACKET_LEN);
    for i in 0..packets_needed {
        body.push(0x47);
        body.extend(std::iter::repeat_n((i % 256) as u8, TS_PACKET_LEN - 1));
    }
    let resp = app
        .oneshot(
            Request::put("/broadcast/bigchan")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let respbody = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&respbody).unwrap();
    let infohash_hex = json["infohash"].as_str().unwrap().to_string();
    let mut infohash = [0u8; 20];
    for i in 0..20 {
        infohash[i] = u8::from_str_radix(&infohash_hex[i * 2..i * 2 + 2], 16).unwrap();
    }

    // Wait for piece 0 to fully complete (all chunks present).
    for _ in 0..500 {
        if seed_registry
            .get(&infohash)
            .unwrap()
            .lock()
            .await
            .has_piece(0)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let store = seed_registry.get(&infohash).unwrap();
    let guard = store.lock().await;
    if !guard.has_piece(0) {
        let window = guard.window();
        panic!("piece 0 never completed; store window = {window:?}");
    }
    let piece_header = guard
        .piece_header(0)
        .expect("ingest records a live piece header");
    assert_ne!(
        piece_header, [0u8; 8],
        "broadcast-originated pieces must not serve the old zero header placeholder"
    );
    assert!(
        f64::from_be_bytes(piece_header) > 1_700_000_000.0,
        "live piece header should decode as a modern Unix timestamp"
    );
    let chunks_per_piece = guard.chunks_per_piece();
    let mut piece_bytes = Vec::with_capacity(PIECE_LENGTH as usize);
    for c in 0..chunks_per_piece {
        piece_bytes.extend_from_slice(&guard.chunk(0, c).unwrap());
    }
    drop(guard);

    let bc = registry.get("bigchan").await.unwrap();
    let sig_len = bc.auth.signature_len();
    let (payload, sig) = ace_wire::live_auth::split_piece(&piece_bytes, sig_len).unwrap();
    assert!(
        ace_wire::live_auth::verify_piece(&bc.auth.pubkey_der(), payload, sig),
        "the ingested piece's embedded signature must verify against the broadcast's own pubkey"
    );
}

#[test]
fn broadcast_name_validation_rejects_traversal_and_junk() {
    assert!(crate::broadcast::valid_broadcast_name("news"));
    assert!(crate::broadcast::valid_broadcast_name("sports-2.hd_1"));
    assert!(!crate::broadcast::valid_broadcast_name(""));
    assert!(!crate::broadcast::valid_broadcast_name("."));
    assert!(!crate::broadcast::valid_broadcast_name(".."));
    assert!(!crate::broadcast::valid_broadcast_name("../etc/passwd"));
    assert!(!crate::broadcast::valid_broadcast_name("a/b"));
    assert!(!crate::broadcast::valid_broadcast_name("has space"));
    assert!(!crate::broadcast::valid_broadcast_name(&"x".repeat(65)));
}

#[tokio::test]
async fn broadcast_put_and_delete_reject_invalid_names_with_400() {
    let app = router(broadcast_state());
    let put = app
        .clone()
        .oneshot(
            Request::put("/broadcast/bad%2Fname")
                .body(Body::from(vec![0x47u8; 8]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::BAD_REQUEST);
    let del = app
        .oneshot(
            Request::delete("/broadcast/bad%2Fname")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(del.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn broadcast_delete_purges_and_is_idempotent() {
    let st = broadcast_state();
    let seed_registry = st.broadcasts.as_ref().unwrap().seed_registry.clone();
    let app = router(st);

    // Mint it.
    let put = app
        .clone()
        .oneshot(
            Request::put("/broadcast/gone")
                .body(Body::from(vec![0x47u8; 8]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let body = axum::body::to_bytes(put.into_body(), 1 << 20)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let ih_hex = json["infohash"].as_str().unwrap();
    let mut infohash = [0u8; 20];
    for i in 0..20 {
        infohash[i] = u8::from_str_radix(&ih_hex[i * 2..i * 2 + 2], 16).unwrap();
    }
    assert!(seed_registry.serves(&infohash));

    // Delete it -> 204, no longer served, GET 404s.
    let del = app
        .clone()
        .oneshot(
            Request::delete("/broadcast/gone")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(del.status(), StatusCode::NO_CONTENT);
    assert!(
        !seed_registry.serves(&infohash),
        "no longer served after delete"
    );
    let get = app
        .clone()
        .oneshot(Request::get("/broadcast/gone").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::NOT_FOUND);

    // Deleting again is idempotent.
    let del2 = app
        .oneshot(
            Request::delete("/broadcast/gone")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(del2.status(), StatusCode::NO_CONTENT);
}
