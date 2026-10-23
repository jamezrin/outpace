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

/// Fixed-code mutation failures; never return a storage path, key or error chain.
pub(super) fn broadcast_error(error: crate::broadcast::BroadcastError) -> Response {
    use crate::broadcast::BroadcastError;
    let status = match error {
        BroadcastError::InvalidName => StatusCode::BAD_REQUEST,
        BroadcastError::ProducerConflict | BroadcastError::Stopping => StatusCode::CONFLICT,
        BroadcastError::PersistenceFailed | BroadcastError::StateInvalid
            | BroadcastError::PieceIndexExhausted => StatusCode::INTERNAL_SERVER_ERROR,
        BroadcastError::ServiceStopping | BroadcastError::CleanupTimeout => StatusCode::SERVICE_UNAVAILABLE,
    };
    (status, Json(json!({ "error": { "code": error.code() } }))).into_response()
}

/// Immediate successful admission; the entry owns the streaming body's task independently
/// of the request/response future. Only ordinary body termination flushes a finite tail.
pub(super) async fn broadcast_ingest(
    State(s): State<AppState>, Path(name): Path<String>, body: Body,
) -> Response {
    use crate::broadcast::{cancelled, BroadcastError, ProducerKind};
    if !crate::broadcast::valid_broadcast_name(&name) {
        return broadcast_error(BroadcastError::InvalidName);
    }
    let Some(bs) = &s.broadcasts else {
        return (StatusCode::NOT_FOUND, Json(json!({"error":{"code":"broadcasts_disabled"}}))).into_response();
    };
    let (bc, session) = match bs.acquire_producer(&name, ProducerKind::RawPut).await {
        Ok(pair) => pair, Err(error) => return broadcast_error(error),
    };
    let infohash = infohash_hex(&bc.infohash);
    let content_id = infohash_hex(&bc.content_id);
    let task_name = name.clone();
    let registered = bs.registry.spawn_ingress(&name, session, move |mut session| async move {
        let mut cancel = session.cancellation();
        let mut ingest = match session.open_ingest() {
            Ok(ingest) => ingest,
            Err(error) => { crate::alog!("[broadcast] {task_name}: {}", error.code()); return; }
        };
        let mut stream = body.into_data_stream();
        loop {
            let chunk = tokio::select! {
                biased;
                () = cancelled(&mut cancel) => return,
                chunk = stream.next() => chunk,
            };
            let Some(Ok(chunk)) = chunk else { break; };
            let result = tokio::select! {
                biased;
                () = cancelled(&mut cancel) => return,
                result = ingest.push_bytes(&chunk) => result,
            };
            if let Err(error) = result {
                crate::alog!("[broadcast] {task_name}: {}", error.code());
                return;
            }
        }
        let result = tokio::select! {
            biased;
            () = cancelled(&mut cancel) => return,
            result = ingest.finish() => result,
        };
        if let Err(error) = result { crate::alog!("[broadcast] {task_name}: {}", error.code()); }
    });
    if let Err(error) = registered { return broadcast_error(error); }
    Json(json!({ "name": name, "content_id": content_id, "infohash": infohash })).into_response()
}

pub(super) async fn broadcast_delete(State(s): State<AppState>, Path(name): Path<String>) -> Response {
    if !crate::broadcast::valid_broadcast_name(&name) {
        return broadcast_error(crate::broadcast::BroadcastError::InvalidName);
    }
    let Some(bs) = &s.broadcasts else {
        return (StatusCode::NOT_FOUND, Json(json!({"error":{"code":"broadcasts_disabled"}}))).into_response();
    };
    match bs.delete(&name).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => broadcast_error(error),
    }
}
