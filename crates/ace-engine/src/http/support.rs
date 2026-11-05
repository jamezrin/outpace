//! Declared-source controls; no request may create a declaration or supply a selector.
use super::*;
use crate::support::SupportService;
use axum::body::HttpBody;
type ServiceState = Option<Arc<SupportService>>;

pub(super) fn router(service: ServiceState) -> Router {
    Router::new()
        .route("/support", get(list))
        .route("/support/{name}/start", axum::routing::post(start))
        .route("/support/{name}/stop", axum::routing::post(stop))
        .with_state(service)
}
fn error(status: StatusCode, code: &'static str) -> Response {
    (status, Json(json!({"error": {"code": code}}))).into_response()
}
async fn list(State(service): State<ServiceState>) -> Response {
    Json(service.map_or_else(Vec::new, |service| service.status())).into_response()
}
async fn start(
    State(service): State<ServiceState>,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
    body: Body,
) -> Response {
    command(service, path, body, true).await
}
async fn stop(
    State(service): State<ServiceState>,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
    body: Body,
) -> Response {
    command(service, path, body, false).await
}
async fn empty_body(mut body: Body) -> Result<(), (StatusCode, &'static str)> {
    let validate = async {
        // A finite frame budget also bounds always-ready empty-frame streams.
        for _ in 0..8 {
            let frame =
                futures::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)).await;
            let Some(frame) = frame else {
                return Ok(());
            };
            let frame = frame.map_err(|_| (StatusCode::BAD_REQUEST, "invalid_body"))?;
            let Some(data) = frame.data_ref() else {
                return Err((StatusCode::BAD_REQUEST, "invalid_body"));
            };
            if !data.is_empty() {
                return Err((
                    if data.len() > 1 {
                        StatusCode::PAYLOAD_TOO_LARGE
                    } else {
                        StatusCode::BAD_REQUEST
                    },
                    "body_not_empty",
                ));
            }
        }
        Err((StatusCode::BAD_REQUEST, "invalid_body"))
    };
    tokio::time::timeout(Duration::from_secs(1), validate)
        .await
        .unwrap_or(Err((StatusCode::REQUEST_TIMEOUT, "request_timeout")))
}
async fn command(
    service: ServiceState,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
    body: Body,
    starting: bool,
) -> Response {
    let Ok(Path(name)) = path else {
        return error(StatusCode::NOT_FOUND, "not_found");
    };
    let Some(service) = service
        .filter(|service| crate::local_descriptors::alias_valid(&name) && service.declares(&name))
    else {
        return error(StatusCode::NOT_FOUND, "not_found");
    };
    if let Err((status, code)) = empty_body(body).await {
        return error(status, code);
    }
    let result = if starting {
        service.start(&name).await
    } else {
        service.stop(&name).await
    };
    match result {
        Ok(intent) => (
            StatusCode::ACCEPTED,
            Json(json!({"running": intent.running, "epoch": intent.epoch})),
        )
            .into_response(),
        Err(ProviderError::NotFound) => error(StatusCode::NOT_FOUND, "not_found"),
        Err(_) => error(StatusCode::INTERNAL_SERVER_ERROR, "support_control_failed"),
    }
}
