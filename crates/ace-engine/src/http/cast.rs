//! `/cast/*` routes (`OUTPACE_ENABLE_CAST`): renderer discovery and control, plus the
//! per-cast stream URL a renderer fetches.

use super::*;
use crate::cast::{CastError, CastService, Target};
use axum::extract::{Extension, RawQuery};
use axum::http::Method;
use axum::routing::{any, MethodRouter};

/// One route, `/cast/{*path}`, dispatched here. A single handler keeps the binary small: each
/// axum handler is its own monomorphized service, and casting is an opt-in feature.
pub(super) fn route(cast: Arc<CastService>) -> MethodRouter<AppState> {
    any(dispatch).layer(Extension(cast))
}

async fn dispatch(
    State(s): State<AppState>,
    Extension(cast): Extension<Arc<CastService>>,
    method: Method,
    Path(path): Path<String>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    let segments: Vec<&str> = path.split('/').collect();
    let get = method == Method::GET;
    let post = method == Method::POST;
    match segments.as_slice() {
        ["targets"] if get => list_targets(&cast, query.as_deref()).await,
        ["targets", target, "play"] if post => play(&s, &cast, target, &body).await,
        ["targets", target, "stop"] if post => stop(&cast, target).await,
        ["targets", target, "status"] if get => status(&cast, target).await,
        // Renderers often probe the stream with HEAD; hyper sends no body for it.
        ["s", file] if get || method == Method::HEAD => cast_stream(&s, &cast, file).await,
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

fn error_response(error: &CastError) -> Response {
    let status = match error {
        CastError::UnknownTarget => StatusCode::NOT_FOUND,
        CastError::LoopbackBind | CastError::UnsupportedBind => StatusCode::CONFLICT,
        CastError::Discovery => StatusCode::SERVICE_UNAVAILABLE,
        CastError::Timeout => StatusCode::GATEWAY_TIMEOUT,
        _ => StatusCode::BAD_GATEWAY,
    };
    let mut body = json!({ "error": error.code(), "message": error.to_string() });
    if let CastError::Fault(fault) = error {
        body["upnp_error"] = json!(fault.code);
    }
    (status, Json(body)).into_response()
}

fn bad_request(code: &'static str, message: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": code, "message": message })),
    )
        .into_response()
}

/// A stream that cannot be started, reported without starting a redirect handoff.
fn stream_error(error: ProviderError) -> Response {
    let (status, code) = match &error {
        ProviderError::CapacityExhausted => (StatusCode::SERVICE_UNAVAILABLE, "capacity_exhausted"),
        ProviderError::DirectHls(_) => (StatusCode::UNPROCESSABLE_ENTITY, "origin_hls_unsupported"),
        ProviderError::ProtectedContent
        | ProviderError::InvalidProtection
        | ProviderError::Unresolvable(_) => {
            (StatusCode::UNPROCESSABLE_ENTITY, "stream_unavailable")
        }
        ProviderError::NotFound | ProviderError::Backend(_) => {
            (StatusCode::NOT_FOUND, "stream_not_found")
        }
    };
    let message = match &error {
        ProviderError::Backend(_) => "stream could not be started".to_string(),
        other => other.to_string(),
    };
    (status, Json(json!({ "error": code, "message": message }))).into_response()
}

fn target_json(target: &Target) -> serde_json::Value {
    json!({
        "id": target.id,
        "name": target.name,
        "manufacturer": target.manufacturer,
        "model": target.model,
        "address": target.address.to_string(),
    })
}

/// `GET /cast/targets[?refresh=1]` — renderers with AVTransport, cached for 60 s.
async fn list_targets(cast: &CastService, query: Option<&str>) -> Response {
    let refresh = query.is_some_and(|query| {
        query
            .split('&')
            .any(|pair| matches!(pair, "refresh=1" | "refresh=true"))
    });
    match cast.targets(refresh).await {
        Ok(found) => Json(json!({
            "targets": found.targets.iter().map(target_json).collect::<Vec<_>>(),
            "age_secs": found.age.as_secs(),
        }))
        .into_response(),
        Err(error) => error_response(&error),
    }
}

/// `POST /cast/targets/{target}/play` with `{"network":"ace","id":"..."}`.
async fn play(s: &AppState, cast: &CastService, target: &str, body: &[u8]) -> Response {
    // Fail on a loopback bind before anything else, so the error is the actionable one.
    if let Err(error) = cast.check_bind() {
        return error_response(&error);
    }
    // Read through `Value` rather than a derived struct: no extra deserializer to compile.
    let request: serde_json::Value = serde_json::from_slice(body).unwrap_or_default();
    let (Some(network), Some(raw_id)) = (request["network"].as_str(), request["id"].as_str())
    else {
        return bad_request(
            "invalid_request",
            "expected a JSON body {\"network\":...,\"id\":...}",
        );
    };
    if !s.networks.iter().any(|known| known == network) {
        return bad_request("unknown_network", "unknown network");
    }
    let raw_id = raw_id.strip_suffix(".ts").unwrap_or(raw_id);
    if raw_id.is_empty() || raw_id.contains(['/', '.', '?', '#']) || raw_id.len() > 512 {
        return bad_request("invalid_id", "invalid stream id");
    }
    let id = match native_stream_id(network, raw_id) {
        Ok(id) => id.into_owned(),
        Err(reason) => return bad_request("invalid_id", reason),
    };
    if !cast.knows_target(target) {
        return error_response(&CastError::UnknownTarget);
    }
    // Start the shared session and hold a viewer on it while the renderer is told about it:
    // many renderers open the URL inside `SetAVTransportURI`, and should find it running.
    let warm = match s.manager.get_or_subscribe(network, &id).await {
        Ok(warm) => warm,
        Err(error) => return stream_error(error),
    };
    let result = cast.play(target, network, &id, "Outpace live stream").await;
    drop(warm);
    match result {
        Ok(started) => Json(json!({
            "target": started.target,
            "network": started.network,
            "id": native_public_stream_id(&started.network, &started.id),
            "url": started.url,
        }))
        .into_response(),
        Err(error) => error_response(&error),
    }
}

/// `POST /cast/targets/{target}/stop`.
async fn stop(cast: &CastService, target: &str) -> Response {
    match cast.stop(target).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error_response(&error),
    }
}

/// `GET /cast/targets/{target}/status` — transport state plus whether the renderer is still
/// fetching the stream.
async fn status(cast: &CastService, target: &str) -> Response {
    match cast.status(target).await {
        Ok(status) => {
            let connections = status.cast.as_ref().map_or(0, |cast| cast.connections());
            let casting = status.cast.map(|cast| {
                json!({
                    "network": cast.network,
                    "id": native_public_stream_id(&cast.network, &cast.id),
                    "url": cast.url,
                    "stream_connections": connections,
                })
            });
            let fetching = connections > 0;
            Json(json!({
                "target": target,
                "transport_state": status.transport.state,
                "transport_status": status.transport.status,
                "speed": status.transport.speed,
                "cast": casting,
                "fetching": fetching,
            }))
            .into_response()
        }
        Err(error) => error_response(&error),
    }
}

/// `GET /cast/s/{token}.ts` — the stream a renderer was told to play. Serves the ordinary
/// shared live session and counts the open response for `status`.
async fn cast_stream(s: &AppState, cast: &CastService, file: &str) -> Response {
    let Some((active, guard)) = file
        .strip_suffix(".ts")
        .and_then(|token| cast.open_stream(token))
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    // `active.id` is the normalized session key `play` started, so subscribe to it directly
    // rather than re-parsing it as a `/streams` selector (a bare infohash key would be read
    // back as a content id there).
    let session = match s
        .manager
        .get_or_subscribe(&active.network, &active.id)
        .await
    {
        Ok(session) => session,
        Err(error) => return stream_error(error),
    };
    let response = stream_session_response(session, s.manager.hls_startup_timeout()).await;
    if !response.status().is_success() {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    parts.headers.insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static(active.mime),
    );
    parts.headers.insert(
        "transferMode.dlna.org",
        header::HeaderValue::from_static("Streaming"),
    );
    parts.headers.insert(
        "contentFeatures.dlna.org",
        header::HeaderValue::from_static(
            "DLNA.ORG_OP=00;DLNA.ORG_CI=0;DLNA.ORG_FLAGS=01700000000000000000000000000000",
        ),
    );
    let stream = body.into_data_stream().map(move |chunk| {
        let _held = &guard;
        chunk
    });
    Response::from_parts(parts, Body::from_stream(stream))
}
