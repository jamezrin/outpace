//! `/cast/*` routes (`OUTPACE_ENABLE_CAST`): renderer discovery and control, plus the
//! per-cast stream URL a renderer fetches.

use super::*;
use crate::cast::{CastError, CastService, Target};
use axum::extract::Extension;
use axum::routing::post;

pub(super) fn routes(cast: Arc<CastService>) -> Router<AppState> {
    Router::new()
        .route("/cast/targets", get(list_targets))
        .route("/cast/targets/{target}/play", post(play))
        .route("/cast/targets/{target}/stop", post(stop))
        .route("/cast/targets/{target}/status", get(status))
        .route("/cast/s/{file}", get(cast_stream))
        .layer(Extension(cast))
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

#[derive(serde::Deserialize)]
pub(super) struct TargetsQuery {
    refresh: Option<String>,
}

/// `GET /cast/targets[?refresh=1]` — renderers with AVTransport, cached for 60 s.
async fn list_targets(
    Extension(cast): Extension<Arc<CastService>>,
    Query(query): Query<TargetsQuery>,
) -> Response {
    let refresh = matches!(query.refresh.as_deref(), Some("1" | "true"));
    match cast.targets(refresh).await {
        Ok(found) => Json(json!({
            "targets": found.targets.iter().map(target_json).collect::<Vec<_>>(),
            "age_secs": found.age.as_secs(),
        }))
        .into_response(),
        Err(error) => error_response(&error),
    }
}

#[derive(serde::Deserialize)]
pub(super) struct PlayRequest {
    network: String,
    id: String,
}

/// `POST /cast/targets/{target}/play` with `{"network":"ace","id":"..."}`.
async fn play(
    State(s): State<AppState>,
    Extension(cast): Extension<Arc<CastService>>,
    Path(target): Path<String>,
    body: Bytes,
) -> Response {
    // Fail on a loopback bind before anything else, so the error is the actionable one.
    if let Err(error) = cast.check_bind() {
        return error_response(&error);
    }
    let Ok(request) = serde_json::from_slice::<PlayRequest>(&body) else {
        return bad_request(
            "invalid_request",
            "expected a JSON body {\"network\":...,\"id\":...}",
        );
    };
    if !s.networks.contains(&request.network) {
        return bad_request("unknown_network", "unknown network");
    }
    let raw_id = request
        .id
        .strip_suffix(".ts")
        .unwrap_or(request.id.as_str());
    if raw_id.is_empty() || raw_id.contains(['/', '.', '?', '#']) || raw_id.len() > 512 {
        return bad_request("invalid_id", "invalid stream id");
    }
    let id = match native_stream_id(&request.network, raw_id) {
        Ok(id) => id.into_owned(),
        Err(reason) => return bad_request("invalid_id", reason),
    };
    if !cast.knows_target(&target) {
        return error_response(&CastError::UnknownTarget);
    }
    // Start the shared session and hold a viewer on it while the renderer is told about it:
    // many renderers open the URL inside `SetAVTransportURI`, and should find it running.
    let warm = match s.manager.get_or_subscribe(&request.network, &id).await {
        Ok(warm) => warm,
        Err(error) => return stream_error(error),
    };
    let result = cast
        .play(&target, &request.network, &id, "Outpace live stream")
        .await;
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
async fn stop(
    Extension(cast): Extension<Arc<CastService>>,
    Path(target): Path<String>,
) -> Response {
    match cast.stop(&target).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error_response(&error),
    }
}

/// `GET /cast/targets/{target}/status` — transport state plus whether the renderer is still
/// fetching the stream.
async fn status(
    Extension(cast): Extension<Arc<CastService>>,
    Path(target): Path<String>,
) -> Response {
    match cast.status(&target).await {
        Ok(status) => {
            let casting = status.cast.map(|cast| {
                json!({
                    "network": cast.network,
                    "id": native_public_stream_id(&cast.network, &cast.id),
                    "url": cast.url,
                    "stream_connections": cast.connections(),
                })
            });
            let fetching = casting
                .as_ref()
                .is_some_and(|c| c["stream_connections"].as_u64().unwrap_or(0) > 0);
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
async fn cast_stream(
    State(s): State<AppState>,
    Extension(cast): Extension<Arc<CastService>>,
    Path(file): Path<String>,
) -> Response {
    let token = file.strip_suffix(".ts").unwrap_or(&file);
    let Some((active, guard)) = cast.open_stream(token) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let response = stream_file(State(s), Path((active.network.clone(), active.id.clone()))).await;
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
