use super::*;
use crate::acestream_search::{SearchError, SearchQuery};

pub(super) async fn search(
    State(state): State<AppState>,
    Query(params): Query<Vec<(String, String)>>,
) -> Response {
    let Some(service) = state.search else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let result = match SearchQuery::parse(params) {
        Ok(query) => service.search(query).await,
        Err(error) => Err(error),
    };
    match result {
        Ok(bytes) => (
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::CACHE_CONTROL, "no-store"),
            ],
            bytes,
        )
            .into_response(),
        Err(error) => {
            let (status, code) = match error {
                SearchError::InvalidQuery => (StatusCode::BAD_REQUEST, "search_invalid_query"),
                SearchError::InvalidResponse => {
                    (StatusCode::BAD_GATEWAY, "search_invalid_upstream_response")
                }
                SearchError::TooLarge => (StatusCode::BAD_GATEWAY, "search_response_too_large"),
                SearchError::Upstream => (StatusCode::BAD_GATEWAY, "search_upstream_unavailable"),
                SearchError::Timeout => (StatusCode::GATEWAY_TIMEOUT, "search_timeout"),
                SearchError::Busy => (StatusCode::SERVICE_UNAVAILABLE, "search_busy"),
                SearchError::RateLimited => (StatusCode::TOO_MANY_REQUESTS, "search_rate_limited"),
                SearchError::InvalidSettings => {
                    (StatusCode::SERVICE_UNAVAILABLE, "search_unavailable")
                }
            };
            let mut response = (status, Json(json!({"result":null,"error":code}))).into_response();
            response.headers_mut().insert(
                header::CACHE_CONTROL,
                axum::http::HeaderValue::from_static("no-store"),
            );
            if error == SearchError::RateLimited {
                response.headers_mut().insert(
                    header::RETRY_AFTER,
                    axum::http::HeaderValue::from_static("30"),
                );
            }
            if error == SearchError::Busy {
                response.headers_mut().insert(
                    header::RETRY_AFTER,
                    axum::http::HeaderValue::from_static("1"),
                );
            }
            response
        }
    }
}
