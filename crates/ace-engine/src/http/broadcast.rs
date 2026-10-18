use super::*;

/// `GET /broadcast/{name}` — the minted broadcast's raw transport-file bytes (the
/// `.acelive`-equivalent), if it's been `PUT` first. Lets any real Acestream node
/// (`--stream-support-node --url http://.../broadcast/{name}`) or our own tooling fetch the
/// descriptor by name instead of needing the infohash out of band. 404 if unminted.
pub(super) async fn broadcast_transport(
    State(s): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    let Some(bs) = &s.broadcasts else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match bs.registry.get(&name).await {
        Some(bc) => (
            [(header::CONTENT_TYPE, "application/octet-stream")],
            (*bc.transport_bytes).clone(),
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Whether `name` is safe to use as both a registry key and a persistence filename. Names
/// come straight from the URL path, so this closes a path-traversal vector: allow only
/// `[A-Za-z0-9._-]`, length 1..=64, and never `.`/`..`.
/// `PUT /broadcast/{name}` (B1) — accepts a chunked MPEG-TS body and originates it as an
/// Acestream-compatible live swarm. Responds immediately with the minted infohash (identical
/// name -> identical, already-minted broadcast; see `BroadcastRegistry::start_or_resume`)
/// while ingest continues in a background task — the request body may be a long-lived,
/// effectively-unbounded stream (a live source), so the handler can't wait for it to finish.
///
/// Piece numbering resumes from the broadcast's persisted cursor, so a second `PUT` to the
/// same name continues the sequence rather than restarting at 0 (issue #3).
pub(super) async fn broadcast_ingest(
    State(s): State<AppState>,
    Path(name): Path<String>,
    body: Body,
) -> Response {
    if !crate::broadcast::valid_broadcast_name(&name) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Some(bs) = &s.broadcasts else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let (bc, freshly_minted) = bs
        .registry
        .start_or_resume(
            &name,
            &name,
            &bs.trackers,
            &bs.seed_registry,
            bs.store_bytes,
        )
        .await;
    let infohash = infohash_hex(&bc.infohash);
    let content_id = infohash_hex(&bc.content_id);
    crate::alog!("[broadcast] {name}: ingesting as infohash {infohash}");

    // Self-announce to tracker + DHT exactly once per freshly-minted name — resumed PUTs and
    // disk reloads must not spawn duplicate loops.
    if freshly_minted {
        bs.spawn_announce(&bc);
    }

    let store = bc.store.clone();
    let auth = bc.auth.clone();
    let cursor = bc.cursor.clone();
    tokio::spawn(async move {
        let mut ingest = crate::broadcast_ingest::BroadcastIngest::new(store, auth, cursor);
        let mut stream = body.into_data_stream();
        while let Some(chunk) = stream.next().await {
            let Ok(chunk) = chunk else { break };
            ingest.push_bytes(&chunk).await;
        }
        ingest.finish().await;
    });

    Json(json!({
        "name": name,
        "content_id": content_id,
        "infohash": infohash,
    }))
    .into_response()
}

/// `DELETE /broadcast/{name}` — purge a broadcast: drop it from the registry (and its persisted
/// record) and stop serving its infohash/content_id, so a subsequent `PUT` mints a fresh
/// identity. Idempotent: `204 No Content` whether or not the name existed.
pub(super) async fn broadcast_delete(
    State(s): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    if !crate::broadcast::valid_broadcast_name(&name) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Some(bs) = &s.broadcasts else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if bs.registry.delete(&name).await.is_some() {
        // Registry-entry eviction (and disk-dir cleanup) now ride the broadcast's SeedLease drop
        // inside `registry.delete` — no explicit seed_registry.remove / remove_cache_dir needed.
        crate::alog!("[broadcast] {name}: deleted");
    }
    StatusCode::NO_CONTENT.into_response()
}
