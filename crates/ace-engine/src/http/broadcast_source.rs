//! Native source routes have a deliberately small, sanitized JSON error surface.
use super::*;
use crate::broadcast::{valid_broadcast_name, BroadcastError};
use crate::broadcast_pull::{validate_source, BroadcastSourceConfig, MAX_SOURCE_REQUEST_BYTES};

fn source_error(status: StatusCode, code: &'static str) -> Response {
    (status, Json(json!({"error":{"code":code}}))).into_response()
}

fn source_state<'a>(
    state: &'a AppState,
    name: &str,
    attaching: bool,
) -> Result<&'a BroadcastState, Box<Response>> {
    if !valid_broadcast_name(name) {
        return Err(Box::new(broadcast_error(BroadcastError::InvalidName)));
    }
    let Some(bs) = &state.broadcasts else {
        return Err(Box::new(source_error(
            StatusCode::NOT_FOUND,
            "broadcasts_disabled",
        )));
    };
    bs.registry
        .source_preflight(name, attaching)
        .map_err(|error| Box::new(broadcast_error(error)))?;
    Ok(bs)
}

pub(super) async fn broadcast_source_put(
    State(state): State<AppState>,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let name = match path {
        Ok(Path(name)) => name,
        Err(_) => return broadcast_error(BroadcastError::InvalidName),
    };
    let bs = match source_state(&state, &name, true) {
        Ok(bs) => bs,
        Err(response) => return *response,
    };
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .map(str::trim);
    let json_type = content_type.is_some_and(|value| {
        let value = value.to_ascii_lowercase();
        value.strip_prefix("application/").is_some_and(|subtype| {
            (subtype == "json" || (subtype.len() > 5 && subtype.ends_with("+json")))
                && subtype
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
        })
    });
    if !json_type {
        return source_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "source_content_type_required",
        );
    }
    // Count actual bytes, not Content-Length. The last frame is rejected without appending
    // it, retaining at most the bounded request plus one adapter-returned frame.
    let mut stream = body.into_data_stream();
    let mut bytes = Vec::new();
    let mut cancel = bs.registry.service_cancellation();
    loop {
        let frame = tokio::select! {
            biased;
            () = crate::broadcast::cancelled(&mut cancel) => return broadcast_error(BroadcastError::ServiceStopping),
            frame = stream.next() => frame,
        };
        let Some(frame) = frame else {
            break;
        };
        let frame = match frame {
            Ok(frame) => frame,
            Err(_) => return source_error(StatusCode::BAD_REQUEST, "invalid_source_config"),
        };
        if frame.len() > MAX_SOURCE_REQUEST_BYTES.saturating_sub(bytes.len()) {
            return source_error(StatusCode::PAYLOAD_TOO_LARGE, "source_request_too_large");
        }
        bytes.extend_from_slice(&frame);
    }
    let config = match serde_json::from_slice::<BroadcastSourceConfig>(&bytes) {
        Ok(config) => config,
        Err(_) => return source_error(StatusCode::BAD_REQUEST, "invalid_source_config"),
    };
    let config = match validate_source(config, bs.registry.source_policy()) {
        Ok(config) => config,
        Err(error) => return source_error(StatusCode::BAD_REQUEST, error.code()),
    };
    match bs.attach_source(&name, config).await {
        Ok((bc, source)) => (
            StatusCode::ACCEPTED,
            Json(json!({
                "name":name, "content_id":infohash_hex(&bc.content_id),
                "infohash":infohash_hex(&bc.infohash), "source":source,
            })),
        )
            .into_response(),
        Err(error) => broadcast_error(error),
    }
}

pub(super) async fn broadcast_source_get(
    State(state): State<AppState>,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
) -> Response {
    let name = match path {
        Ok(Path(name)) => name,
        Err(_) => return broadcast_error(BroadcastError::InvalidName),
    };
    let bs = match source_state(&state, &name, false) {
        Ok(bs) => bs,
        Err(response) => return *response,
    };
    match bs.source_status(&name).await {
        Ok(Some(source)) => Json(json!({"name":name,"source":source})).into_response(),
        Ok(None) => source_error(StatusCode::NOT_FOUND, "source_not_found"),
        Err(error) => broadcast_error(error),
    }
}

pub(super) async fn broadcast_source_delete(
    State(state): State<AppState>,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
) -> Response {
    let name = match path {
        Ok(Path(name)) => name,
        Err(_) => return broadcast_error(BroadcastError::InvalidName),
    };
    let bs = match source_state(&state, &name, false) {
        Ok(bs) => bs,
        Err(response) => return *response,
    };
    match bs.detach_source(&name).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => broadcast_error(error),
    }
}
