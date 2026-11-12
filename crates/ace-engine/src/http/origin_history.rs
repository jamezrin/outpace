//! Outpace retained-origin replay controls; these are not original Ace seek units.
use super::*;
use crate::origin_hls::{OriginError, OriginSeekTarget};
use axum::extract::RawQuery;

const MAX_COMMAND_QUERY: usize = 4096;

fn command_params(query: Option<&str>) -> Result<HashMap<String, String>, &'static str> {
    let query = query.unwrap_or_default();
    if query.len() > MAX_COMMAND_QUERY {
        return Err("command query exceeds limit");
    }
    let mut params = HashMap::new();
    let Query(entries) = Query::<Vec<(String, String)>>::try_from_uri(
        &format!("/?{query}")
            .parse()
            .map_err(|_| "invalid command query")?,
    )
    .map_err(|_| "invalid command query")?;
    for (key, value) in entries {
        if !matches!(
            key.as_str(),
            "method" | "back" | "position_ms" | "generation" | "api_version"
        ) {
            return Err("unsupported command parameter");
        }
        if params.insert(key, value).is_some() {
            return Err("duplicate command parameter");
        }
    }
    // The public Android SDK appends this hint to its stop command. It does
    // not change token ownership or select a different control protocol.
    if params
        .get("api_version")
        .is_some_and(|version| version != "2")
    {
        return Err("unsupported api_version");
    }
    Ok(params)
}

fn unsigned(value: &str) -> Option<u64> {
    (!value.is_empty() && value.len() <= 20 && value.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| value.parse().ok())
        .flatten()
}

fn command_error(error: impl Into<String>) -> Response {
    (
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({ "response": null, "error": error.into() })),
    )
        .into_response()
}

pub(super) async fn origin_history_command(
    State(state): State<AppState>,
    Path(token): Path<String>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let Some(relay) = state.origin_hls else {
        return origin_hls_error(OriginError::NotFound);
    };
    let params = match command_params(query.as_deref()) {
        Ok(params) => params,
        Err(error) => return command_error(error),
    };
    let expected = match params.get("generation") {
        Some(value) => match unsigned(value) {
            Some(value) => Some(value),
            None => return command_error("generation must be an unsigned integer"),
        },
        None => None,
    };
    let method = params.get("method").map(String::as_str).unwrap_or_default();
    if method != "timeshift_seek"
        && (params.contains_key("back") || params.contains_key("position_ms"))
    {
        return command_error("seek targets require timeshift_seek");
    }
    let result = match method {
        "timeshift_status" if expected.is_none() => {
            relay.history_status(&token).map(|mut status| {
                status.view.playback_url =
                    format!("{}{}", request_base(&headers), status.view.playback_url);
                json!({ "response": status, "error": null })
            })
        }
        "timeshift_seek" => {
            let target = match (params.get("back"), params.get("position_ms")) {
                (Some(back), None) => {
                    let Some(back) = back
                        .parse::<f64>()
                        .ok()
                        .filter(|n| n.is_finite() && *n > 0.0 && *n <= 31_536_000.0)
                    else {
                        return command_error("back must be finite positive seconds");
                    };
                    OriginSeekTarget::Back(Duration::from_secs_f64(back))
                }
                (None, Some(position)) => {
                    let Some(position) = unsigned(position) else {
                        return command_error(
                            "position_ms must be unsigned integer media milliseconds",
                        );
                    };
                    OriginSeekTarget::PositionMs(position)
                }
                _ => return command_error("specify exactly one of back or position_ms"),
            };
            relay.seek(&token, target, expected).map(|mut view| {
                view.playback_url = format!("{}{}", request_base(&headers), view.playback_url);
                json!({ "response": { "playback_url": view.playback_url,
                    "reload_required": true, "view": view }, "error": null })
            })
        }
        "timeshift_live" => relay.live(&token, expected).map(|mut view| {
            view.playback_url = format!("{}{}", request_base(&headers), view.playback_url);
            json!({ "response": { "playback_url": view.playback_url,
                "reload_required": true, "view": view }, "error": null })
        }),
        "stop" if expected.is_none() => {
            if relay.stop(&token) {
                Ok(json!({ "response": "ok", "error": null }))
            } else {
                Err(OriginError::NotFound)
            }
        }
        _ => return command_error("unknown method or unsupported generation parameter"),
    };
    match result {
        Ok(body) => ([(header::CACHE_CONTROL, "no-store")], Json(body)).into_response(),
        Err(error) => command_error(error.code()),
    }
}

pub(super) async fn origin_history_resource(
    State(state): State<AppState>,
    Path((token, generation, resource)): Path<(String, String, String)>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let Some(relay) = state.origin_hls else {
        return origin_hls_error(OriginError::NotFound);
    };
    let Some(generation) = unsigned(&generation) else {
        return origin_hls_error(OriginError::NotFound);
    };
    // The first retained TS backend serves whole fragments. Ordinary HLS playback
    // and FFmpeg's initial bytes=0- probe need no partial representation.
    if query.is_some_and(|value| !value.is_empty())
        || headers.contains_key(header::IF_RANGE)
        || headers.get_all(header::RANGE).iter().count() > 1
        || headers
            .get(header::RANGE)
            .is_some_and(|value| value.as_bytes() != b"bytes=0-")
    {
        return origin_hls_error(OriginError::InvalidRange);
    }
    let response = match relay.replay_resource(&token, generation, &resource).await {
        Ok(response) => response,
        Err(error) => return origin_hls_error(error),
    };
    axum::http::Response::builder()
        .status(response.status)
        .header(header::CONTENT_TYPE, &response.content_type)
        .header(header::CONTENT_LENGTH, response.content_length)
        .header(header::CACHE_CONTROL, "no-store")
        .header("x-content-type-options", "nosniff")
        .body(Body::from_stream(response.into_stream()))
        .expect("validated retained-origin response headers")
}
