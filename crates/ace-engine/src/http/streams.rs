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
fn native_stream_id<'a>(network: &str, id: &'a str) -> Result<Cow<'a, str>, &'static str> {
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
fn native_public_stream_id<'a>(network: &str, id: &'a str) -> Cow<'a, str> {
    if network == "ace" {
        if let Some(hex) = ace_normalized_hex_id(id) {
            return Cow::Owned(format!("ih:{hex}"));
        }
    }
    Cow::Borrowed(id)
}

/// `DELETE /streams/{network}/{id}` — force-stop a running session (admin). 204 if it was
/// running, 404 otherwise. The `{id}` may carry a `.ts`/`.m3u8` suffix (mirroring the GET
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
    if s.manager.stop(&network, &id).await {
        StatusCode::NO_CONTENT.into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
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
                "livepos": live_position_json(stats.livepos.as_ref()),
                "recovery": stats.recovery,
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
/// that text. Capacity returns 503 with Retry-After; other failures retain bodiless 404.
fn stream_start_error(e: ProviderError) -> Response {
    match e {
        ProviderError::CapacityExhausted => capacity_error(),
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
            Err(e) => stream_start_error(e),
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
    let session = match s.manager.get_or_start(&network, &id).await {
        Ok(sess) => sess,
        Err(e) => return stream_start_error(e),
    };
    stream_session_response(session)
}

fn stream_session_response(session: Arc<StreamSession>) -> Response {
    let sub = session.subscribe();
    // Per-client keyframe gate: holds a mid-GOP joiner until the first clean keyframe (with
    // PAT/PMT — and, when the stream has a resolved title, a synthesized SDT — prepended).
    let gate = ace_media::mpegts::KeyframeGate::with_service_name(session.metadata().title.clone());
    // Bridge the broadcast receiver to an HTTP body stream; the Subscription rides along so
    // its Drop (decrementing the client count) fires when the client disconnects.
    let stream = futures::stream::unfold((sub, gate), |(mut sub, mut gate)| async move {
        loop {
            match sub.rx.recv().await {
                Ok(StreamEvent::Data(chunk)) => {
                    let out = gate.push(&chunk);
                    if out.is_empty() {
                        continue; // still waiting for the first keyframe
                    }
                    return Some((Ok::<_, std::io::Error>(Bytes::from(out)), (sub, gate)));
                }
                Ok(StreamEvent::Discontinuity) => {
                    reset_stream_keyframe_gate(&mut gate);
                    continue;
                }
                Err(RecvError::Lagged(_)) => {
                    reset_stream_keyframe_gate(&mut gate);
                    continue;
                }
                Err(RecvError::Closed) => return None,
            }
        }
    });
    Response::builder()
        .header(header::CONTENT_TYPE, "video/mp2t")
        .body(Body::from_stream(stream))
        .unwrap()
}

pub(super) fn ace_stream_session_response(
    session: Arc<StreamSession>,
    expires_at: Instant,
    cancel: tokio::sync::watch::Receiver<bool>,
    direct_lease: Option<AceDirectLeaseGuard>,
) -> Response {
    let sub = session.subscribe();
    let gate = ace_media::mpegts::KeyframeGate::with_service_name(session.metadata().title.clone());
    let stream = futures::stream::unfold(
        (sub, gate, cancel, expires_at, direct_lease),
        |(mut sub, mut gate, mut cancel, expires_at, direct_lease)| async move {
            loop {
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(expires_at)) => {
                        return None;
                    }
                    changed = cancel.changed() => {
                        if changed.is_err() || *cancel.borrow() { return None; }
                    }
                    received = sub.rx.recv() => match received {
                        Ok(StreamEvent::Data(chunk)) => {
                            let out = gate.push(&chunk);
                            if out.is_empty() { continue; }
                            return Some((Ok::<_, std::io::Error>(Bytes::from(out)), (sub, gate, cancel, expires_at, direct_lease)));
                        }
                        Ok(StreamEvent::Discontinuity) => {
                            reset_stream_keyframe_gate(&mut gate);
                        }
                        Err(RecvError::Lagged(_)) => { reset_stream_keyframe_gate(&mut gate); }
                        Err(RecvError::Closed) => return None,
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
