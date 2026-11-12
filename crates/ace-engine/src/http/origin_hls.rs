//! Native HTTP adapter for opt-in origin-HLS relay leases.
use super::*;
use crate::origin_hls::OriginError;
use axum::extract::RawQuery;

pub(super) async fn origin_hls_resource(
    State(state): State<AppState>,
    Path((token, id)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let Some(relay) = state.origin_hls else {
        return StatusCode::NOT_FOUND.into_response();
    };
    // This initial backend does not implement LL-HLS reload query semantics.
    if query.is_some_and(|query| !query.is_empty()) {
        return origin_hls_error(OriginError::UnsupportedPlaylist);
    }
    if headers.contains_key(header::IF_RANGE) {
        return origin_hls_error(OriginError::InvalidRange);
    }
    let range = match headers.get(header::RANGE).map(|value| value.to_str()) {
        Some(Ok(value)) => Some(value),
        Some(Err(_)) => return origin_hls_error(OriginError::InvalidRange),
        None => None,
    };
    if headers.get_all(header::RANGE).iter().count() > 1 {
        return origin_hls_error(OriginError::InvalidRange);
    }
    let response = match relay.resource(&token, &id, range).await {
        Ok(response) => response,
        Err(error) => return origin_hls_error(error),
    };
    let mut builder = axum::http::Response::builder()
        .status(response.status)
        .header(header::CONTENT_TYPE, &response.content_type)
        .header(header::CONTENT_LENGTH, response.content_length)
        .header(header::CACHE_CONTROL, "no-store")
        .header("x-content-type-options", "nosniff");
    if let Some(range) = &response.content_range {
        builder = builder.header(header::CONTENT_RANGE, range);
    }
    builder
        .body(Body::from_stream(response.into_stream()))
        .expect("validated origin response headers")
}

pub(super) async fn origin_hls_stop(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> Response {
    let Some(relay) = state.origin_hls else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let status = if relay.stop(&token) {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    };
    (status, [(header::CACHE_CONTROL, "no-store")]).into_response()
}

pub(super) fn origin_hls_error(error: OriginError) -> Response {
    (
        StatusCode::from_u16(error.status()).expect("constant status"),
        [(header::CACHE_CONTROL, "no-store")],
        error.code(),
    )
        .into_response()
}

/// Aggregate operational status only; no tokens, origin URLs or channel identities.
pub(super) async fn origin_hls_status(State(state): State<AppState>) -> Response {
    (
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({
            "enabled": state.origin_hls.is_some(),
            "active_sessions": state.origin_hls.as_ref().map_or(0, |relay| relay.active_count()),
        })),
    )
        .into_response()
}
