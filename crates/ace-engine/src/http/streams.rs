use super::*;

pub(super) async fn networks(State(s): State<AppState>) -> Json<serde_json::Value> {
    Json(json!({ "networks": s.networks }))
}

/// `GET /streams` — active sessions and their client counts.
pub(super) async fn list_streams(State(s): State<AppState>) -> Json<serde_json::Value> {
    let streams: Vec<_> = s
        .manager
        .list()
        .await
        .into_iter()
        .map(|(network, id, clients, metadata)| {
            json!({
                "network": network,
                "id": native_public_stream_id(&network, &id),
                "clients": clients,
                "metadata": stream_metadata_json(&metadata),
            })
        })
        .collect();
    Json(json!({ "streams": streams }))
}

/// Normalize native Ace selectors once, before any session lookup. Provider-facing bare
/// hashes remain infohash keys so compat and CLI semantics do not change.
pub(super) fn native_stream_id<'a>(
    network: &str,
    id: &'a str,
) -> Result<Cow<'a, str>, &'static str> {
    if network != "ace" {
        return Ok(Cow::Borrowed(id));
    }
    if let Some(hex) = id.strip_prefix("ih:") {
        return ace_normalized_hex_id(hex)
            .map(Cow::Owned)
            .ok_or("ih: requires a 40-character hexadecimal infohash");
    }
    if let Some(hex) = id.strip_prefix("cid:") {
        return ace_normalized_hex_id(hex)
            .map(|hex| Cow::Owned(format!("cid:{hex}")))
            .ok_or("cid: requires a 40-character hexadecimal content id");
    }
    Ok(match ace_normalized_hex_id(id) {
        Some(hex) => Cow::Owned(format!("cid:{hex}")),
        None => Cow::Borrowed(id),
    })
}

/// Public native identifiers must round-trip through the native selector, including ids
/// originating in compat sessions whose internal infohash key has no prefix.
pub(super) fn native_public_stream_id<'a>(network: &str, id: &'a str) -> Cow<'a, str> {
    if network == "ace" {
        if let Some(hex) = ace_normalized_hex_id(id) {
            return Cow::Owned(format!("ih:{hex}"));
        }
    }
    Cow::Borrowed(id)
}

/// `DELETE /streams/{network}/{id}` — force-stop a running session (admin). 204 if it was
/// running, 404 if absent, or 500 when owned cleanup cannot be confirmed. The `{id}` may carry a `.ts`/`.m3u8` suffix (mirroring the GET
/// URL); it's stripped so either form stops the same session.
pub(super) async fn delete_stream(
    State(s): State<AppState>,
    Path((network, file)): Path<(String, String)>,
) -> Response {
    let id = file
        .strip_suffix(".ts")
        .or_else(|| file.strip_suffix(".m3u8"))
        .unwrap_or(&file);
    let id = match native_stream_id(&network, id) {
        Ok(id) => id,
        Err(reason) => return (StatusCode::BAD_REQUEST, reason).into_response(),
    };
    match s.manager.stop_checked(&network, &id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "stream cleanup failed").into_response(),
    }
}

/// `GET /streams/{network}/{id}/status` — stats for a running session (404 if not active).
pub(super) async fn stream_status(
    State(s): State<AppState>,
    Path((network, id)): Path<(String, String)>,
) -> Response {
    let id = match native_stream_id(&network, &id) {
        Ok(id) => id,
        Err(reason) => return (StatusCode::BAD_REQUEST, reason).into_response(),
    };
    match s.manager.get(&network, &id).await {
        Some(session) => {
            let stats = session.stats().await;
            Json(json!({
                "network": network,
                "id": native_public_stream_id(&network, &id),
                "clients": session.subscriber_count(),
                "occupied_sessions": s.manager.admission_status().0,
                "max_sessions": s.manager.admission_status().1,
                "peers": stats.peers,
                "bitrate": stats.bitrate,
                "livepos": native_live_position_json(stats.livepos.as_ref()),
                "recovery": stats.recovery,
                "normalization": normalization_status_json(session.playback_state()),
                "buffer_ms": stats.buffer_ms,
                "uploaded": stats.uploaded,
                "peers_served": stats.peers_served,
                "metadata": stream_metadata_json(session.metadata()),
            }))
            .into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// `GET /streams/{network}/{id}/seg/{n}.ts` — a retained HLS segment.
pub(super) async fn stream_segment(
    State(s): State<AppState>,
    Path((network, id, seg)): Path<(String, String, String)>,
) -> Response {
    let Some(seq) = seg.strip_suffix(".ts").and_then(|n| n.parse::<u64>().ok()) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let id = match native_stream_id(&network, &id) {
        Ok(id) => id,
        Err(reason) => return (StatusCode::BAD_REQUEST, reason).into_response(),
    };
    // Segment probes must NOT start a stream: only serve from an already-running packager
    // (started when the playlist was fetched). Unknown streams 404 without any provider work.
    let Some(pkg) = s.manager.get_hls(&network, &id).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match pkg.segment(seq) {
        Some(bytes) => ([(header::CONTENT_TYPE, "video/mp2t")], bytes).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Response for a failed stream start on the native routes. `Unresolvable` carries a user-facing
/// reason (a bare infohash without a verified descriptor, #164) and is returned as `422` with
/// that text. Protection policy failures also return 422 with their stable message.
/// Capacity returns 503 with Retry-After; an origin HLS source is a `302` handoff (to the origin,
/// or to a managed relay lease) without a swarm session; other failures retain bodiless 404.
async fn stream_start_error(s: &AppState, e: ProviderError) -> Response {
    match e {
        ProviderError::CapacityExhausted => capacity_error(),
        ProviderError::DirectHls(origin) => {
            super::compat::resolved_origin_redirect(s, &origin.manifest_url).await
        }
        e @ (ProviderError::ProtectedContent | ProviderError::InvalidProtection) => {
            (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response()
        }
        ProviderError::Unresolvable(reason) => {
            (StatusCode::UNPROCESSABLE_ENTITY, reason).into_response()
        }
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

/// `GET /streams/{network}/{id}` or `{id}.ts` (continuous MPEG-TS), or `.m3u8` (live HLS
/// playlist).
pub(super) async fn stream_file(
    State(s): State<AppState>,
    Path((network, file)): Path<(String, String)>,
) -> Response {
    let timeout = s.manager.hls_startup_timeout();
    stream_file_with_hls_timeout(State(s), Path((network, file)), timeout).await
}

pub(super) async fn stream_file_with_hls_timeout(
    State(s): State<AppState>,
    Path((network, file)): Path<(String, String)>,
    timeout: Duration,
) -> Response {
    if let Some(id) = file.strip_suffix(".m3u8") {
        let id = match native_stream_id(&network, id) {
            Ok(id) => id,
            Err(reason) => return (StatusCode::BAD_REQUEST, reason).into_response(),
        };
        return match s.manager.get_or_start_hls(&network, &id).await {
            Ok(pkg) => {
                let prefix = format!(
                    "/streams/{network}/{}/seg",
                    native_public_stream_id(&network, &id)
                );
                let playlist = await_hls_playlist(&pkg, timeout, &prefix, true).await;
                if !s
                    .manager
                    .get_hls(&network, &id)
                    .await
                    .is_some_and(|current| Arc::ptr_eq(&current, &pkg))
                {
                    return StatusCode::NOT_FOUND.into_response();
                }
                let playlist = match playlist {
                    Ok(playlist) => playlist,
                    Err(response) => return *response,
                };
                // Deliberately no `icy-*` header here: on an HLS manifest it makes VLC's HTTP
                // access promote the URL to the `icyx://` (Icecast) scheme and treat the body as
                // a raw audio stream, so the HLS demux never loads and playback busy-loops the
                // playlist without fetching segments. The title reaches clients via the MPEG-TS
                // SDT `service_name` embedded in the segments themselves.
                (
                    [(header::CONTENT_TYPE, "application/vnd.apple.mpegurl")],
                    playlist,
                )
                    .into_response()
            }
            Err(e) => stream_start_error(&s, e).await,
        };
    }
    let id = if let Some(id) = file.strip_suffix(".ts") {
        if id.is_empty() {
            return StatusCode::NOT_FOUND.into_response();
        }
        id
    } else if file.contains('.') {
        return StatusCode::NOT_FOUND.into_response();
    } else {
        &file
    };
    let id = match native_stream_id(&network, id) {
        Ok(id) => id,
        Err(reason) => return (StatusCode::BAD_REQUEST, reason).into_response(),
    };
    let session = match s.manager.get_or_subscribe(&network, &id).await {
        Ok(sess) => sess,
        Err(e) => return stream_start_error(&s, e).await,
    };
    stream_session_response(session, s.manager.hls_startup_timeout()).await
}

fn normalization_status_json(
    state: Option<crate::audio_normalize::PlaybackStateHandle>,
) -> serde_json::Value {
    use crate::audio_normalize::PlaybackPhase;
    let Some(handle) = state else {
        return serde_json::Value::Null;
    };
    let snapshot = handle.snapshot();
    let (state, failure) = match snapshot.phase {
        PlaybackPhase::Classifying => ("classifying", None),
        PlaybackPhase::Starting => ("starting", None),
        PlaybackPhase::Ready => ("ready", None),
        PlaybackPhase::Recovering => ("recovering", None),
        PlaybackPhase::Failed(reason) => ("failed", Some(reason.code())),
        PlaybackPhase::Stopped => ("stopped", None),
    };
    json!({"generation":snapshot.generation,"state":state,"failure":failure})
}

fn normalization_error(error: crate::audio_normalize::PlaybackWaitError) -> Response {
    use crate::audio_normalize::{Failure, PlaybackWaitError};
    match error {
        PlaybackWaitError::Failed(Failure::Admission) => (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::RETRY_AFTER, "1")],
            "audio conversion capacity reached",
        )
            .into_response(),
        PlaybackWaitError::Failed(reason) => (
            if reason == Failure::Unsupported {
                StatusCode::UNPROCESSABLE_ENTITY
            } else if matches!(reason, Failure::Deadline | Failure::OutputStall) {
                StatusCode::GATEWAY_TIMEOUT
            } else {
                StatusCode::BAD_GATEWAY
            },
            format!("audio normalization failed: {}", reason.code()),
        )
            .into_response(),
        PlaybackWaitError::Deadline => (
            StatusCode::GATEWAY_TIMEOUT,
            "audio normalization startup timed out",
        )
            .into_response(),
        PlaybackWaitError::Cancelled => (StatusCode::GONE, "stream stopped").into_response(),
        PlaybackWaitError::Stopped | PlaybackWaitError::Closed => (
            StatusCode::BAD_GATEWAY,
            "audio normalization stopped before readiness",
        )
            .into_response(),
    }
}

// Source failures must wake an idle body independently of pump cleanup. Raw
// sources have no watch channel and retain their existing media-only wait.
async fn next_media_event(
    sub: &mut crate::session::Subscription,
    state: &mut Option<crate::audio_normalize::PlaybackStateHandle>,
) -> Result<Option<StreamEvent>, crate::audio_normalize::Failure> {
    use crate::audio_normalize::{Failure, PlaybackPhase};
    loop {
        if let Some(state) = state.as_ref() {
            if let PlaybackPhase::Failed(reason) = state.snapshot().phase {
                return Err(reason);
            }
            if state.is_closed() && state.snapshot().phase != PlaybackPhase::Stopped {
                return Err(Failure::Pump);
            }
        }
        let event = tokio::select! {
            biased;
            _ = async {
                match state.as_mut() {
                    Some(state) if !state.is_closed() => { let _ = state.changed().await; },
                    _ => std::future::pending().await,
                }
            } => continue,
            event = sub.rx.recv() => event,
        };
        // Failure wins over queued old media and over a simultaneous EOF.
        if let Some(state) = state.as_ref() {
            if let PlaybackPhase::Failed(reason) = state.snapshot().phase {
                return Err(reason);
            }
        }
        if let (Ok(StreamEvent::NormalizedData { generation, .. }), Some(state)) =
            (&event, state.as_ref())
        {
            if *generation != state.snapshot().generation {
                continue;
            }
        }
        return match event {
            Ok(event) => Ok(Some(event)),
            Err(RecvError::Lagged(_)) => Ok(Some(StreamEvent::Discontinuity)),
            Err(RecvError::Closed) => Ok(None),
        };
    }
}
fn normalization_body_error(reason: crate::audio_normalize::Failure) -> std::io::Error {
    std::io::Error::other(format!("audio normalization failed: {}", reason.code()))
}

async fn stream_session_response(
    (session, sub): (Arc<StreamSession>, crate::session::Subscription),
    startup_timeout: std::time::Duration,
) -> Response {
    let state = session.playback_state();
    if let Some(state) = &state {
        let mut cancel = session.cancellation_receiver();
        if let Err(reason) = state
            .wait_ready(tokio::time::Instant::now() + startup_timeout, &mut cancel)
            .await
        {
            return normalization_error(reason);
        }
    }
    let mut gate =
        ace_media::mpegts::KeyframeGate::with_service_name(session.metadata().title.clone());
    if state.is_some() {
        gate.enable_mpeg2();
        gate.preserve_transport_identity();
    }
    let stream = futures::stream::unfold(
        (sub, gate, state, false),
        |(mut sub, mut gate, mut state, ended)| async move {
            if ended {
                return None;
            }
            loop {
                match next_media_event(&mut sub, &mut state).await {
                    Ok(Some(
                        StreamEvent::Data(chunk) | StreamEvent::NormalizedData { bytes: chunk, .. },
                    )) => {
                        let out = gate.push(&chunk);
                        if out.is_empty() {
                            continue;
                        }
                        return Some((Ok(Bytes::from(out)), (sub, gate, state, false)));
                    }
                    Ok(Some(StreamEvent::Discontinuity)) => reset_stream_keyframe_gate(&mut gate),
                    Ok(None) => return None,
                    Err(reason) => {
                        return Some((
                            Err(normalization_body_error(reason)),
                            (sub, gate, state, true),
                        ))
                    }
                }
            }
        },
    );
    Response::builder()
        .header(header::CONTENT_TYPE, "video/mp2t")
        .body(Body::from_stream(stream))
        .unwrap()
}

pub(super) async fn ace_stream_session_response(
    (session, sub): (Arc<StreamSession>, crate::session::Subscription),
    expires_at: Instant,
    mut cancel: tokio::sync::watch::Receiver<bool>,
    direct_lease: Option<AceDirectLeaseGuard>,
    startup_timeout: std::time::Duration,
) -> Response {
    let state = session.playback_state();
    if let Some(state) = &state {
        let deadline = (tokio::time::Instant::now() + startup_timeout)
            .min(tokio::time::Instant::from_std(expires_at));
        if let Err(reason) = state.wait_ready(deadline, &mut cancel).await {
            return normalization_error(reason);
        }
    }
    let mut gate =
        ace_media::mpegts::KeyframeGate::with_service_name(session.metadata().title.clone());
    if state.is_some() {
        gate.enable_mpeg2();
        gate.preserve_transport_identity();
    }
    let stream = futures::stream::unfold(
        (sub, gate, cancel, expires_at, direct_lease, state, false),
        |(mut sub, mut gate, mut cancel, expires_at, direct_lease, mut state, ended)| async move {
            if ended {
                return None;
            }
            loop {
                tokio::select! { biased;
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(expires_at)) => return None,
                    changed = cancel.changed() => { if changed.is_err() || *cancel.borrow() { return None; } },
                    event = next_media_event(&mut sub,&mut state) => match event {
                        Ok(Some(StreamEvent::Data(chunk) | StreamEvent::NormalizedData { bytes: chunk, .. })) => {
                            let out = gate.push(&chunk);
                            if out.is_empty() { continue; }
                            return Some((Ok(Bytes::from(out)),(sub,gate,cancel,expires_at,direct_lease,state,false)));
                        }
                        Ok(Some(StreamEvent::Discontinuity)) => reset_stream_keyframe_gate(&mut gate),
                        Ok(None) => return None,
                        Err(reason) => return Some((Err(normalization_body_error(reason)),(sub,gate,cancel,expires_at,direct_lease,state,true))),
                    }
                }
            }
        },
    );
    Response::builder()
        .header(header::CONTENT_TYPE, "video/mp2t")
        .body(Body::from_stream(stream))
        .unwrap()
}

pub(super) fn reset_stream_keyframe_gate(gate: &mut ace_media::mpegts::KeyframeGate) {
    gate.reset_for_discontinuity();
}

pub(super) fn capacity_error() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, "1")],
        "session capacity reached",
    )
        .into_response()
}

fn native_live_position_json(
    position: Option<&crate::provider::LivePositionStats>,
) -> serde_json::Value {
    let mut value = live_position_json(position);
    if let (Some(position), Some(object)) = (position, value.as_object_mut()) {
        object.insert("buffer_complete".into(), json!(position.buffer_complete));
    }
    value
}

#[cfg(test)]
mod buffer_contract_tests {
    use super::*;
    #[test]
    fn native_completion_is_absent_from_compatibility() {
        let p = crate::provider::LivePositionStats {
            pos: Some(1),
            last: Some(2),
            live_first: None,
            live_last: None,
            buffer_pieces: 0,
            buffer_complete: false,
        };
        assert_eq!(
            native_live_position_json(Some(&p))["buffer_complete"],
            false
        );
        assert!(live_position_json(Some(&p))
            .get("buffer_complete")
            .is_none());
        assert!(native_live_position_json(None).is_null());
    }
}

#[cfg(test)]
mod normalization_tests {
    use super::*;
    use crate::audio_normalize::{Failure, PlaybackPublisher, PlaybackStateHandle};
    use crate::provider::{SourceStats, TsSource};
    use async_trait::async_trait;
    use futures::StreamExt;
    use tokio::sync::{mpsc, watch};
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn retired_subscription_backlog_is_not_returned_after_recovery() {
        let (_input, session) = controlled();
        let mut sub = session.subscribe();
        let (sender, receiver) = tokio::sync::broadcast::channel(4);
        sub.rx = receiver;
        let (publisher, handle) = PlaybackPublisher::new();
        publisher.begin_generation(1);
        publisher.mark_ready(1);
        sender
            .send(StreamEvent::NormalizedData {
                bytes: Bytes::from_static(b"retired"),
                generation: 1,
            })
            .unwrap();
        publisher.begin_generation(2);
        publisher.mark_ready(2);
        sender
            .send(StreamEvent::NormalizedData {
                bytes: Bytes::from_static(b"current"),
                generation: 2,
            })
            .unwrap();
        let result = next_media_event(&mut sub, &mut Some(handle)).await;
        session.stop().await;
        assert!(matches!(
            result,
            Ok(Some(StreamEvent::NormalizedData { generation: 2, .. }))
        ));
    }

    #[tokio::test]
    async fn conversion_capacity_response_names_the_exhausted_resource() {
        let response = normalization_error(crate::audio_normalize::PlaybackWaitError::Failed(
            Failure::Admission,
        ));
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), b"audio conversion capacity reached");
    }

    struct Controlled {
        rx: mpsc::Receiver<Result<Bytes, Failure>>,
        publisher: PlaybackPublisher,
        handle: PlaybackStateHandle,
    }
    #[async_trait]
    impl TsSource for Controlled {
        async fn next(&mut self) -> Option<Bytes> {
            match self.rx.recv().await? {
                Ok(bytes) => {
                    self.publisher.mark_ready(1);
                    Some(bytes)
                }
                Err(reason) => {
                    self.publisher.fail(1, reason);
                    None
                }
            }
        }
        fn stats(&self) -> SourceStats {
            SourceStats::default()
        }
        fn playback_state(&self) -> Option<PlaybackStateHandle> {
            Some(self.handle.clone())
        }
    }
    fn controlled() -> (mpsc::Sender<Result<Bytes, Failure>>, Arc<StreamSession>) {
        let (publisher, handle) = PlaybackPublisher::new();
        publisher.begin_generation(1);
        let (tx, rx) = mpsc::channel(2);
        (
            tx,
            StreamSession::start(
                Box::new(Controlled {
                    rx,
                    publisher,
                    handle,
                }),
                4,
            ),
        )
    }
    async fn response(
        session: Arc<StreamSession>,
        compat: bool,
    ) -> (Response, Option<watch::Sender<bool>>) {
        let sub = session.subscribe();
        if compat {
            let (owner, cancel) = watch::channel(false);
            let response = ace_stream_session_response(
                (session, sub),
                Instant::now() + Duration::from_secs(30),
                cancel,
                None,
                Duration::from_secs(1),
            )
            .await;
            (response, Some(owner))
        } else {
            (
                stream_session_response((session, sub), Duration::from_secs(1)).await,
                None,
            )
        }
    }
    #[test]
    fn native_normalization_status_is_bounded_and_retains_the_terminal_code() {
        assert_eq!(normalization_status_json(None), serde_json::Value::Null);
        let (publisher, handle) = PlaybackPublisher::new();
        publisher.begin_generation(4);
        publisher.fail(4, Failure::ChildExit);
        assert_eq!(
            normalization_status_json(Some(handle)),
            json!({"generation":4,"state":"failed","failure":"child_exit"})
        );
    }

    #[tokio::test(start_paused = true)]
    async fn normalization_capacity_and_startup_deadline_release_their_readiness_subscription() {
        for compat in [false, true] {
            let (tx, session) = controlled();
            tx.send(Err(Failure::Admission)).await.unwrap();
            let (result, _lease) = response(session.clone(), compat).await;
            let subscribers = session.subscriber_count();
            session.stop().await;
            assert_eq!(result.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(result.headers()[header::RETRY_AFTER], "1");
            assert_eq!(subscribers, 0);

            let (_tx, session) = controlled();
            let start = tokio::time::Instant::now();
            let (result, _lease) = response(session.clone(), compat).await;
            let elapsed = start.elapsed();
            let subscribers = session.subscriber_count();
            session.stop().await;
            assert_eq!(result.status(), StatusCode::GATEWAY_TIMEOUT);
            assert_eq!(elapsed, Duration::from_secs(1));
            assert_eq!(subscribers, 0);
        }
    }

    #[tokio::test]
    async fn cancellation_wins_readiness_and_releases_native_or_compatibility_subscription() {
        for compat in [false, true] {
            let (_tx, session) = controlled();
            let sub = session.subscribe();
            let result = if compat {
                let (_owner, cancel) = watch::channel(true);
                ace_stream_session_response(
                    (session.clone(), sub),
                    Instant::now() + Duration::from_secs(30),
                    cancel,
                    None,
                    Duration::from_secs(1),
                )
                .await
            } else {
                session.cancel();
                stream_session_response((session.clone(), sub), Duration::from_secs(1)).await
            };
            let subscribers = session.subscriber_count();
            session.stop().await;
            assert_eq!(result.status(), StatusCode::GONE);
            assert_eq!(subscribers, 0);
        }
    }

    #[tokio::test]
    async fn normalization_refusal_before_headers_is_explicit_for_native_and_compat_ts() {
        for compat in [false, true] {
            let (tx, session) = controlled();
            tx.send(Err(Failure::Unsupported)).await.unwrap();
            let result = timeout(Duration::from_secs(2), response(session.clone(), compat)).await;
            session.stop().await;
            let (response, _lease) = result.unwrap();
            assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
            let bytes = axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap();
            assert_eq!(bytes.as_ref(), b"audio normalization failed: unsupported");
        }
    }
    #[tokio::test]
    async fn normalization_failure_after_headers_yields_one_body_error_and_remains_observable() {
        for compat in [false, true] {
            let (tx, session) = controlled();
            tx.send(Ok(
                crate::hls_audio::process::test_fixtures::transport_prefix().into(),
            ))
            .await
            .unwrap();
            let (response, _lease) = response(session.clone(), compat).await;
            assert_eq!(response.status(), StatusCode::OK);
            let mut body = response.into_body().into_data_stream();
            let first = timeout(Duration::from_secs(1), body.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(!first.is_empty());
            tx.send(Err(Failure::ChildExit)).await.unwrap();
            let failure = timeout(Duration::from_secs(1), body.next()).await;
            let end = timeout(Duration::from_secs(1), body.next()).await;
            let retained = session.playback_state().unwrap().snapshot();
            session.stop().await;
            assert!(
                matches!(failure, Ok(Some(Err(_)))),
                "post-header failure must not be silent EOF"
            );
            assert!(
                matches!(end, Ok(None)),
                "body error is terminal, not a repeating error loop"
            );
            assert_eq!(
                retained.phase,
                crate::audio_normalize::PlaybackPhase::Failed(Failure::ChildExit)
            );
        }
    }
}
