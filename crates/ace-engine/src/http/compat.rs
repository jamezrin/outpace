use super::*;

pub(super) async fn ace_getstream(
    State(s): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let mode = match params.get("format").map(String::as_str) {
        None | Some("") => AceGetstreamMode::Direct,
        Some("json") => AceGetstreamMode::Json,
        Some(_) => {
            return Json(json!({ "response": null, "error": "unsupported format" }))
                .into_response();
        }
    };
    let selection = match resolve_ace_selection(&s, &params).await {
        Ok(selection) => selection,
        Err(error) => {
            return Json(json!({ "response": null, "error": error })).into_response();
        }
    };
    let Some(network) = ace_network(&s) else {
        return Json(json!({ "response": null, "error": "no ace network registered" }))
            .into_response();
    };
    let token = s
        .ace_sessions
        .mint(selection.playback_id.clone(), selection.session_key.clone());
    let base = request_base(&headers);
    let playback_id = selection.playback_id;
    let public_id = selection.public_id;
    let metadata = selection.metadata;
    if mode == AceGetstreamMode::Json {
        return Json(json!({
            "response": {
                "infohash": public_id,
                "playback_url": format!("{base}/ace/r/{playback_id}/{token}"),
                "stat_url": format!("{base}/ace/stat/{playback_id}/{token}"),
                "command_url": format!("{base}/ace/cmd/{playback_id}/{token}"),
                "playback_session_id": token,
                "client_session_id": -1,
                "is_live": 1,
                "is_encrypted": 0,
                "metadata": stream_metadata_json(&metadata)
            },
            "error": null
        }))
        .into_response();
    }

    // A direct request owns an ordinary compatibility lease, just like a caller that follows
    // the JSON playback URL. The body holds exactly one StreamManager subscription, so dropping
    // it detaches this caller without stopping a shared source or another compatibility client.
    let Some((session_key, expires_at, cancel)) = s.ace_sessions.playback(&playback_id, &token)
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match s.manager.get_or_start(&network, &session_key).await {
        Ok(session) => ace_stream_session_response(
            session,
            expires_at,
            cancel,
            Some(AceDirectLeaseGuard {
                sessions: s.ace_sessions.clone(),
                playback_id,
                token,
            }),
        ),
        Err(_) => {
            s.ace_sessions.revoke(&playback_id, &token);
            StatusCode::NOT_FOUND.into_response()
        }
    }
}

/// `GET /ace/manifest.m3u8` — start one native live HLS session. The default/`redirect`
/// response points at a stable, token-authenticated playlist URL; `format=json` returns the
/// official documented control-URL shape. Both paths use the same native `HlsPackager`.
pub(super) async fn ace_manifest(
    State(s): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let format = params
        .get("format")
        .map(String::as_str)
        .unwrap_or("redirect");
    if !matches!(format, "" | "redirect" | "json") {
        return Json(json!({ "response": null, "error": "unsupported format" })).into_response();
    }
    let selection = match resolve_ace_selection(&s, &params).await {
        Ok(selection) => selection,
        Err(error) => {
            return Json(json!({ "response": null, "error": error })).into_response();
        }
    };
    let Some(network) = ace_network(&s) else {
        return Json(json!({ "response": null, "error": "no ace network registered" }))
            .into_response();
    };

    let (_pkg, session, pin) = match s
        .manager
        .get_or_start_compatibility_hls(&network, &selection.session_key)
        .await
    {
        Ok(started) => started,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    let token = s
        .ace_sessions
        .mint_hls(selection.playback_id.clone(), selection.session_key, pin);
    let base = request_base(&headers);
    let playback_url = format!("{base}/ace/m/{}/{}.m3u8", selection.playback_id, token);

    if format == "json" {
        return Json(json!({
            "response": {
                "playback_url": playback_url,
                "stat_url": format!("{base}/ace/stat/{}/{token}", selection.playback_id),
                "command_url": format!("{base}/ace/cmd/{}/{token}", selection.playback_id),
                "infohash": selection.public_id,
                "playback_session_id": token,
                "is_live": 1,
                "is_encrypted": 0,
                "client_session_id": -1,
                "metadata": stream_metadata_json(session.metadata())
            },
            "error": null
        }))
        .into_response();
    }

    // The documented engine default is a redirect. This also gives media players a stable URL
    // to reload, avoiding a fresh lease on every HLS playlist refresh.
    (
        StatusCode::FOUND,
        [
            (header::LOCATION, playback_url),
            (header::CACHE_CONTROL, "no-store".into()),
        ],
    )
        .into_response()
}

/// Tokenized HLS playlist returned by `/ace/manifest.m3u8` JSON/redirect modes.
pub(super) async fn ace_hls_playback(
    State(s): State<AppState>,
    Path((id, manifest)): Path<(String, String)>,
) -> Response {
    let timeout = s.manager.hls_startup_timeout();
    ace_hls_playback_with_timeout(State(s), Path((id, manifest)), timeout).await
}

pub(super) async fn ace_hls_playback_with_timeout(
    State(s): State<AppState>,
    Path((id, manifest)): Path<(String, String)>,
    timeout: Duration,
) -> Response {
    let Some(token) = manifest.strip_suffix(".m3u8") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some((session_key, expires_at, mut cancel)) = s.ace_sessions.playback(&id, token) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(network) = ace_network(&s) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(pkg) = s.manager.get_hls(&network, &session_key).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let ready = tokio::select! {
        ready = pkg.wait_ready(timeout) => ready,
        _ = cancel.changed() => return StatusCode::NOT_FOUND.into_response(),
        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(expires_at)) => {
            return StatusCode::NOT_FOUND.into_response();
        }
    };
    if s.ace_sessions.validate(&id, token).as_deref() != Some(session_key.as_str()) {
        return StatusCode::NOT_FOUND.into_response();
    }
    if !ready {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::RETRY_AFTER, "1")],
        )
            .into_response();
    }
    (
        [
            (header::CONTENT_TYPE, "application/vnd.apple.mpegurl"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        pkg.compatibility_playlist_with_segment_prefix(&format!("/ace/c/{token}")),
    )
        .into_response()
}

/// Authenticated view of a retained native live HLS segment. Probes never start work.
pub(super) async fn ace_hls_segment(
    State(s): State<AppState>,
    Path((token, segment)): Path<(String, String)>,
) -> Response {
    let Some(seq) = segment
        .strip_suffix(".ts")
        .and_then(|value| value.parse::<u64>().ok())
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some((_, session_key)) = s.ace_sessions.validate_token(&token) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(network) = ace_network(&s) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(pkg) = s.manager.get_hls(&network, &session_key).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match pkg.compatibility_segment(seq) {
        Some(bytes) => (
            [
                (header::CONTENT_TYPE, "video/mp2t"),
                (header::CACHE_CONTROL, "no-store"),
            ],
            bytes,
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

pub(super) async fn ace_playback(
    State(s): State<AppState>,
    Path((id, token)): Path<(String, String)>,
) -> Response {
    let Some(network) = ace_network(&s) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some((session_key, expires_at, cancel)) = s.ace_sessions.playback(&id, &token) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match s.manager.get_or_start(&network, &session_key).await {
        Ok(session) => ace_stream_session_response(session, expires_at, cancel, None),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

pub(super) fn live_position_json(
    position: Option<&crate::provider::LivePositionStats>,
) -> serde_json::Value {
    match position {
        Some(pos) => json!({
            "pos": pos.pos, "last": pos.last,
            "live_first": pos.live_first, "live_last": pos.live_last,
            "buffer_pieces": pos.buffer_pieces,
            "first_ts": null, "last_ts": null, "is_live": 1,
        }),
        None => serde_json::Value::Null,
    }
}

pub(super) async fn ace_stat(
    State(s): State<AppState>,
    Path((id, token)): Path<(String, String)>,
) -> Json<serde_json::Value> {
    let Some(network) = ace_network(&s) else {
        return Json(json!({ "response": null, "error": "no ace network registered" }));
    };
    let Some(session_key) = s.ace_sessions.validate(&id, &token) else {
        return Json(json!({ "response": null, "error": "invalid or expired playback session" }));
    };
    let public_id = ace_public_id(&id);
    let Some(session) = s.manager.get(&network, &session_key).await else {
        return Json(json!({
            "response": {
                "status": "idle",
                "speed_down": 0, "speed_up": 0, "livepos": null,
                "peers": 0,
                "downloaded": 0,
                "uploaded": 0,
                "infohash": public_id,
                "is_live": 1,
                "is_encrypted": 0,
                "playback_session_id": token,
                "client_session_id": -1,
                "metadata": stream_metadata_json(&StreamMetadata::default())
            },
            "error": null
        }));
    };

    let stats = session.stats().await;
    Json(json!({
        "response": {
            "status": "dl",
            "speed_down": stats.download_rate as f64 / 1024.0,
            "speed_up": stats.upload_rate as f64 / 1024.0,
            "livepos": live_position_json(stats.livepos.as_ref()),
            "peers": stats.peers,
            "downloaded": stats.downloaded,
            "uploaded": stats.uploaded,
            "infohash": public_id,
            "is_live": 1,
            "is_encrypted": 0,
            "playback_session_id": token,
            "client_session_id": -1,
            "metadata": stream_metadata_json(session.metadata())
        },
        "error": null
    }))
}

pub(super) async fn ace_command(
    State(s): State<AppState>,
    Path((id, token)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    if s.ace_sessions.validate(&id, &token).is_none() {
        return Json(json!({ "response": null, "error": "invalid or expired playback session" }));
    }
    if params.get("method").is_some_and(|m| m == "stop") {
        if !s.ace_sessions.revoke(&id, &token) {
            return Json(
                json!({ "response": null, "error": "invalid or expired playback session" }),
            );
        }
        return Json(json!({ "response": "ok", "error": null }));
    }
    Json(json!({ "response": null, "error": "missing method" }))
}

/// `GET /server/api` — the official-engine JSON control API (compatibility subset). Dispatches on
/// `?method=` and always answers HTTP 200 with a `{ "result", "error" }` envelope (note 10). Only
/// the methods in [`crate::server_api::Method`] are served; see `docs/protocol/compat-matrix.md`.
pub(super) async fn server_api(
    State(s): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    use crate::server_api as api;

    let method = params.get("method").map(String::as_str).unwrap_or("");
    let body = match api::parse_method(method) {
        api::Method::GetVersion => api::ok(api::version_result()),
        api::Method::GetStatus => api::ok(api::status_result(s.manager.list().await.len())),
        api::Method::GetNetworkConnectionStatus => api::ok(api::network_status_result(&s.networks)),
        // `get_content_id` only echoes an id the caller already gave; it can't derive one from a
        // bare infohash/url, so it never resolves.
        api::Method::GetContentId => match api::selector(&params) {
            api::Selector::ContentId(cid) => api::ok(api::content_id_result(&cid)),
            api::Selector::Missing => api::err("missing content_id"),
            _ => api::err("content_id unavailable for this selector"),
        },
        method @ (api::Method::AnalyzeContent | api::Method::GetMediaFiles) => {
            match resolve_server_api_selector(&s, api::selector(&params)).await {
                Ok(content) => match method {
                    api::Method::AnalyzeContent => api::ok(api::analyze_result(&content)),
                    api::Method::GetMediaFiles => api::ok(api::media_files_result(&content)),
                    _ => unreachable!("outer match restricts these two variants"),
                },
                Err(e) => api::err(e),
            }
        }
        api::Method::Unknown(name) => api::err(format!("unknown method: {name}")),
    };
    Json(body)
}

/// Resolve a `/server/api` content [`crate::server_api::Selector`] to its infohash. Direct
/// infohashes and magnets resolve offline; `content_id`/`url` need a catalog/transport fetch,
/// which is gated by the same `resolve_content_ids_in_getstream` switch tests use to stay offline
/// (production enables it). A best-effort failure is reported in-band, never as an HTTP error.
async fn resolve_server_api_selector(
    s: &AppState,
    sel: crate::server_api::Selector,
) -> Result<crate::server_api::ResolvedContent, String> {
    use crate::server_api::{ResolvedContent, Selector};

    if let Some(resolved) = crate::server_api::resolve_offline(&sel) {
        return resolved;
    }
    match sel {
        Selector::ContentId(cid) => {
            if !s.resolve_content_ids_in_getstream {
                return Err("content-id catalog resolution is disabled".to_string());
            }
            match resolve_via_catalog(&cid).await {
                Ok(info) => {
                    // `resolve_via_catalog` is the signed catalog, so this descriptor is indexable.
                    if let Some(network) = ace_network(s) {
                        s.manager.remember_live_descriptor(&network, &info);
                    }
                    Ok(ResolvedContent {
                        infohash: infohash_hex(&info.infohash),
                        content_id: Some(cid),
                        is_live: true,
                    })
                }
                Err(e) => Err(format!("content-id resolution failed: {e:?}")),
            }
        }
        Selector::Url(url) => {
            if !s.resolve_content_ids_in_getstream {
                return Err("transport-url resolution is disabled".to_string());
            }
            match stream_info_from_transport_url(&url).await {
                Ok(info) => Ok(ResolvedContent {
                    infohash: infohash_hex(&info.infohash),
                    content_id: None,
                    is_live: true,
                }),
                Err(e) => Err(format!("transport-url resolution failed: {e:?}")),
            }
        }
        Selector::Missing => Err("missing content_id/infohash/url/magnet".to_string()),
        Selector::Infohash(_) | Selector::Magnet(_) => {
            unreachable!("infohash/magnet selectors resolve offline")
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct AceStreamSelection {
    pub(super) public_id: String,
    pub(super) playback_id: String,
    pub(super) session_key: String,
    pub(super) content_id: Option<String>,
    pub(super) metadata: StreamMetadata,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AceGetstreamMode {
    Direct,
    Json,
}

impl AceStreamSelection {
    pub(super) fn with_resolved_stream(
        mut self,
        infohash: String,
        metadata: StreamMetadata,
    ) -> Self {
        self.public_id = infohash.clone();
        self.playback_id = infohash;
        self.content_id = None;
        self.metadata = metadata;
        self
    }
}

async fn resolve_ace_selection(
    s: &AppState,
    params: &HashMap<String, String>,
) -> Result<AceStreamSelection, String> {
    let mut selection = ace_selected_stream(params)?;
    let network = ace_network(s);
    if s.resolve_content_ids_in_getstream {
        if let Some(content_id) = selection.content_id.as_deref() {
            match resolve_via_catalog(content_id).await {
                Ok(info) => {
                    // A client may open this infohash directly later (#164). Signed catalog
                    // (`resolve_via_catalog`) only, so the descriptor is indexable.
                    if let Some(network) = network.as_deref() {
                        s.manager.remember_live_descriptor(network, &info);
                    }
                    selection =
                        selection.with_resolved_stream(infohash_hex(&info.infohash), info.metadata);
                }
                Err(e) => crate::alog!(
                    "[ace] content-id catalog resolution failed, falling back to cid: {e:?}"
                ),
            }
        }
    }
    // The provider may reject a selector synchronously. Cold Ace infohashes proceed to
    // asynchronous authenticated catalog resolution when the playback URL is opened.
    if let Some(network) = network.as_deref() {
        if let Err(ProviderError::Unresolvable(reason)) =
            s.manager.check_openable(network, &selection.session_key)
        {
            return Err(reason);
        }
    }
    Ok(selection)
}

pub(super) fn ace_selected_stream(
    params: &HashMap<String, String>,
) -> Result<AceStreamSelection, &'static str> {
    if let Some(content_id) = ace_nonempty_param(params, "content_id") {
        let content_id = ace_normalized_hex_id(content_id).ok_or("invalid content_id")?;
        return Ok(AceStreamSelection {
            public_id: content_id.clone(),
            playback_id: format!("cid:{content_id}"),
            session_key: format!("cid:{content_id}"),
            content_id: Some(content_id),
            metadata: StreamMetadata::default(),
        });
    }
    if let Some(infohash) = ace_nonempty_param(params, "infohash") {
        let infohash = ace_normalized_hex_id(infohash).ok_or("invalid infohash")?;
        return Ok(AceStreamSelection {
            public_id: infohash.clone(),
            playback_id: infohash.clone(),
            session_key: infohash,
            content_id: None,
            metadata: StreamMetadata::default(),
        });
    }
    // The legacy `id=` spelling denotes an AceStream content id, not a swarm infohash.
    if let Some(content_id) = ace_nonempty_param(params, "id") {
        let content_id = ace_normalized_hex_id(content_id).ok_or("invalid id")?;
        return Ok(AceStreamSelection {
            public_id: content_id.clone(),
            playback_id: format!("cid:{content_id}"),
            session_key: format!("cid:{content_id}"),
            content_id: Some(content_id),
            metadata: StreamMetadata::default(),
        });
    }
    if let Some(url) = ace_nonempty_param(params, "url") {
        // The id is a reversible, path-safe encoding of the URL, so it is both the route-safe
        // playback_id and the session_key the provider decodes — no server-side alias table, so
        // playback survives a restart or a direct `/ace/r/{id}` hit.
        let token =
            crate::transport_url::encode_transport_url(url).map_err(|_| "invalid transport url")?;
        return Ok(AceStreamSelection {
            public_id: token.clone(),
            playback_id: token.clone(),
            session_key: token,
            content_id: None,
            metadata: StreamMetadata::default(),
        });
    }
    if let Some(magnet) = ace_nonempty_param(params, "magnet") {
        let hex = crate::magnet::parse_magnet_infohash(magnet).map_err(|_| "invalid magnet")?;
        return Ok(AceStreamSelection {
            public_id: hex.clone(),
            playback_id: hex.clone(),
            session_key: hex,
            content_id: None,
            metadata: StreamMetadata::default(),
        });
    }
    Err("missing content_id/infohash/id/url/magnet")
}

pub(super) fn ace_normalized_hex_id(id: &str) -> Option<String> {
    (id.len() == 40 && id.bytes().all(|b| b.is_ascii_hexdigit())).then(|| id.to_ascii_lowercase())
}

fn ace_nonempty_param<'a>(params: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    params
        .get(key)
        .map(String::as_str)
        .filter(|id| !id.is_empty())
}

fn ace_public_id(id: &str) -> &str {
    id.strip_prefix("cid:").unwrap_or(id)
}

fn ace_network(s: &AppState) -> Option<String> {
    if s.networks.iter().any(|n| n == "ace") {
        Some("ace".to_string())
    } else {
        s.networks.first().cloned()
    }
}

fn request_base(headers: &HeaderMap) -> String {
    let host = headers
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .filter(|h| !h.is_empty())
        .unwrap_or("127.0.0.1:6878");
    format!("http://{host}")
}
