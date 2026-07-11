//! HTTP API (axum): the clean `/streams`/`/broadcast` surface plus the official-engine
//! compatibility `/ace/...` playback surface.

use crate::broadcast::BroadcastRegistry;
use crate::manager::StreamManager;
use crate::provider::{ProviderError, VodByteSource, VodContent};
use crate::session::StreamEvent;
use crate::session::StreamSession;
use ace_swarm::listen::SeedRegistry;
use ace_swarm::resolve::{infohash_hex, resolve_via_catalog, stream_info_from_transport_url};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{routing::get, routing::put, Json, Router};
use bytes::Bytes;
use futures::StreamExt;
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::broadcast::error::RecvError;

const ACE_TOKEN_TTL: Duration = Duration::from_secs(6 * 60 * 60);
const ACE_TOKEN_CAPACITY: usize = 4096;

#[derive(Clone)]
struct AceLease {
    playback_id: String,
    session_key: String,
    expires_at: Instant,
    cancel: tokio::sync::watch::Sender<bool>,
}

/// Bounded playback leases minted by `/ace/getstream`. A token is both authorization and the
/// compatibility client's ownership handle; revoking one never force-stops the shared source.
pub struct AceSessionStore {
    leases: Mutex<HashMap<String, AceLease>>,
    ttl: Duration,
    capacity: usize,
}

impl Default for AceSessionStore {
    fn default() -> Self {
        Self::new(ACE_TOKEN_TTL, ACE_TOKEN_CAPACITY)
    }
}

impl AceSessionStore {
    fn new(ttl: Duration, capacity: usize) -> Self {
        Self {
            leases: Mutex::new(HashMap::new()),
            ttl,
            capacity,
        }
    }

    fn mint(&self, playback_id: String, session_key: String) -> String {
        use rand::RngCore;
        let mut bytes = [0_u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        let token = hex::encode(bytes);
        self.insert_at(token.clone(), playback_id, session_key, Instant::now());
        token
    }

    fn insert_at(&self, token: String, playback_id: String, session_key: String, now: Instant) {
        let mut leases = self.leases.lock().unwrap();
        leases.retain(|_, lease| lease.expires_at > now);
        if leases.len() >= self.capacity {
            if let Some(oldest) = leases
                .iter()
                .min_by_key(|(_, lease)| lease.expires_at)
                .map(|(t, _)| t.clone())
            {
                leases.remove(&oldest);
            }
        }
        let (cancel, _) = tokio::sync::watch::channel(false);
        leases.insert(
            token,
            AceLease {
                playback_id,
                session_key,
                expires_at: now + self.ttl,
                cancel,
            },
        );
    }

    fn validate(&self, playback_id: &str, token: &str) -> Option<String> {
        self.validate_at(playback_id, token, Instant::now())
    }

    fn validate_at(&self, playback_id: &str, token: &str, now: Instant) -> Option<String> {
        let mut leases = self.leases.lock().unwrap();
        leases.retain(|_, lease| lease.expires_at > now);
        leases
            .get(token)
            .filter(|lease| lease.playback_id == playback_id)
            .map(|lease| lease.session_key.clone())
    }

    fn revoke(&self, playback_id: &str, token: &str) -> bool {
        let mut leases = self.leases.lock().unwrap();
        let matches = leases.get(token).is_some_and(|lease| {
            lease.playback_id == playback_id && lease.expires_at > Instant::now()
        });
        if matches {
            if let Some(lease) = leases.remove(token) {
                let _ = lease.cancel.send(true);
            }
        }
        matches
    }

    fn playback(
        &self,
        playback_id: &str,
        token: &str,
    ) -> Option<(String, Instant, tokio::sync::watch::Receiver<bool>)> {
        let now = Instant::now();
        let mut leases = self.leases.lock().unwrap();
        leases.retain(|_, lease| lease.expires_at > now);
        leases
            .get(token)
            .filter(|lease| lease.playback_id == playback_id)
            .map(|lease| {
                (
                    lease.session_key.clone(),
                    lease.expires_at,
                    lease.cancel.subscribe(),
                )
            })
    }
}

#[derive(Clone)]
pub struct AppState {
    pub manager: Arc<StreamManager>,
    pub networks: Vec<String>,
    /// Resolve `content_id=` to the official infohash before returning `/ace/getstream`
    /// URLs. Tests disable this to keep the compatibility route offline/deterministic.
    pub resolve_content_ids_in_getstream: bool,
    /// Official `/ace/getstream?content_id=` returns URLs keyed by the resolved infohash.
    /// Internally, keep using `cid:<content_id>` so playback gets the catalog-derived
    /// transport geometry/trackers instead of degrading to bare-infohash defaults.
    pub ace_sessions: Arc<AceSessionStore>,
    /// Experimental legacy Acestream HTTP compatibility surface. Native `/streams` and
    /// `/broadcast` routes remain available regardless of this flag.
    pub experimental_ace_compat: bool,
    /// B1: origination state. `None` disables `PUT /broadcast/{name}` (404) — e.g. in tests
    /// that don't need it.
    pub broadcasts: Option<BroadcastState>,
}

/// State `PUT /broadcast/{name}` needs: where minted broadcasts are tracked, the shared
/// piece-store registry the S1/S2 serve path already reads from, and minting parameters.
#[derive(Clone)]
pub struct BroadcastState {
    pub registry: Arc<BroadcastRegistry>,
    pub seed_registry: SeedRegistry,
    pub trackers: Vec<String>,
    pub store_bytes: u64,
    /// Inbound peer-listener port to advertise via tracker/DHT self-announce for a freshly
    /// minted broadcast — `None` when inbound serving is disabled, since announcing a port
    /// nobody's listening on would just misdirect real peers.
    pub inbound_peer_port: tokio::sync::watch::Receiver<Option<u16>>,
}

impl BroadcastState {
    /// Start the periodic tracker/DHT self-announce loops for `bc` (its infohash + content-id
    /// metadata swarm). A no-op without an inbound listener: advertising a port nobody serves
    /// on would misdirect real peers. Shared by fresh mint (`PUT`/RTMP) and startup reload, so
    /// each runs exactly once per broadcast.
    pub fn spawn_announce(&self, bc: &crate::broadcast::Broadcast) {
        if self.inbound_peer_port.borrow().is_none() {
            return;
        }
        let trackers = self.trackers.clone();
        tokio::spawn(crate::ace_provider::announce_infohash_periodically_dynamic(
            trackers.clone(),
            bc.infohash,
            self.inbound_peer_port.clone(),
        ));
        tokio::spawn(crate::ace_provider::announce_infohash_periodically_dynamic(
            trackers,
            bc.content_id,
            self.inbound_peer_port.clone(),
        ));
    }
}

pub fn router(state: AppState) -> Router {
    let compat = state.experimental_ace_compat;
    let mut router = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/networks", get(networks))
        .route("/streams", get(list_streams))
        .route(
            "/streams/:network/:file",
            get(stream_file).delete(delete_stream),
        )
        .route("/streams/:network/:id/status", get(stream_status))
        .route("/streams/:network/:id/seg/:seg", get(stream_segment))
        .route("/vod/:network/:id", get(vod_stream))
        .route("/vod/:network/:id/manifest.m3u8", get(vod_manifest))
        .route("/vod/:network/:id/seg/:seg", get(vod_hls_segment))
        .route(
            "/broadcast/:name",
            put(broadcast_ingest)
                .get(broadcast_transport)
                .delete(broadcast_delete),
        );
    if compat {
        router = router
            .route("/ace/getstream", get(ace_getstream))
            .route("/ace/r/:id/:token", get(ace_playback))
            .route("/ace/stat/:id/:token", get(ace_stat))
            .route("/ace/cmd/:id/:token", get(ace_command))
            .route("/server/api", get(server_api));
    }
    router.with_state(state)
}

/// `GET /vod/{network}/{id}` — resolve `id` as a single-file VOD, then stream its verified
/// bytes with a `Content-Length`. The resolved VOD is cached by the manager and shared with the
/// HLS routes, so a playback resolves the descriptor once and reuses downloaded pieces.
async fn vod_stream(
    State(s): State<AppState>,
    Path((network, id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let vod = match resolve_vod(&s, &network, &id).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let total = vod.content_length();
    if total == 0 {
        return empty_vod_response();
    }
    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    match parse_byte_range(range, total) {
        // Whole file (no usable Range): 200 with Accept-Ranges so players know they *can* seek.
        RangeOutcome::Full => match vod.open_range(0, total - 1).await {
            Ok(source) => vod_response_from_source(source),
            Err(e) => (StatusCode::BAD_GATEWAY, format!("{e:?}")).into_response(),
        },
        RangeOutcome::Satisfiable(start, end) => match vod.open_range(start, end).await {
            Ok(source) => vod_partial_response(source, start, end, total),
            Err(e) => (StatusCode::BAD_GATEWAY, format!("{e:?}")).into_response(),
        },
        // A well-formed range that can't be met: 416 with the resource's true length.
        RangeOutcome::Unsatisfiable => axum::http::Response::builder()
            .status(StatusCode::RANGE_NOT_SATISFIABLE)
            .header(header::ACCEPT_RANGES, "bytes")
            .header(header::CONTENT_RANGE, format!("bytes */{total}"))
            .body(Body::empty())
            .expect("valid 416 response")
            .into_response(),
    }
}

/// How a `Range` request header resolves against a known content length.
#[derive(Debug, PartialEq, Eq)]
enum RangeOutcome {
    /// No usable single-range spec — serve the whole file (`200`).
    Full,
    /// One satisfiable byte range `[start, end]` (inclusive) — serve `206`.
    Satisfiable(u64, u64),
    /// A well-formed range that lies outside the content — serve `416`.
    Unsatisfiable,
}

/// Interpret a `Range` header against `total` (> 0) content bytes. Only single byte-ranges are
/// honored; anything else (missing header, non-`bytes` unit, multi-range, or unparseable spec)
/// is ignored and treated as [`RangeOutcome::Full`] — RFC 7233 §3.1 lets a server ignore a
/// `Range` it doesn't act on. Only a syntactically valid but out-of-bounds range is
/// [`RangeOutcome::Unsatisfiable`].
fn parse_byte_range(header: Option<&str>, total: u64) -> RangeOutcome {
    let Some(spec) = header.and_then(|h| h.strip_prefix("bytes=")) else {
        return RangeOutcome::Full;
    };
    let spec = spec.trim();
    // Single range only; a comma means multiple ranges, which we decline to honor (serve 200).
    if spec.contains(',') {
        return RangeOutcome::Full;
    }
    let Some((first, last)) = spec.split_once('-') else {
        return RangeOutcome::Full;
    };
    let (first, last) = (first.trim(), last.trim());
    match (first.is_empty(), last.is_empty()) {
        // `-N`: the last N bytes (clamped to the whole file).
        (true, false) => match last.parse::<u64>() {
            Ok(0) => RangeOutcome::Unsatisfiable,
            Ok(n) => RangeOutcome::Satisfiable(total.saturating_sub(n), total - 1),
            Err(_) => RangeOutcome::Full,
        },
        // `N-`: from byte N to the end.
        (false, true) => match first.parse::<u64>() {
            Ok(start) if start < total => RangeOutcome::Satisfiable(start, total - 1),
            Ok(_) => RangeOutcome::Unsatisfiable,
            Err(_) => RangeOutcome::Full,
        },
        // `N-M`: an explicit range (end clamped to the last byte).
        (false, false) => match (first.parse::<u64>(), last.parse::<u64>()) {
            (Ok(start), Ok(end)) if start <= end && start < total => {
                RangeOutcome::Satisfiable(start, end.min(total - 1))
            }
            (Ok(_), Ok(_)) => RangeOutcome::Unsatisfiable,
            _ => RangeOutcome::Full,
        },
        // A bare `-`: not a valid range, ignore it.
        (true, true) => RangeOutcome::Full,
    }
}

/// Build an empty `200` for a zero-length VOD (no range possible), advertising `Accept-Ranges`.
fn empty_vod_response() -> Response {
    axum::http::Response::builder()
        .status(StatusCode::OK)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, 0)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::empty())
        .expect("valid empty vod response")
        .into_response()
}

/// Bridge a [`VodByteSource`] to an HTTP body stream.
fn vod_body(source: Box<dyn VodByteSource>) -> Body {
    let stream = futures::stream::unfold(source, |mut src| async move {
        src.next()
            .await
            .map(|chunk| (Ok::<Bytes, std::io::Error>(chunk), src))
    });
    Body::from_stream(stream)
}

/// Build a streaming `200` response for a whole-file [`VodByteSource`], advertising its length
/// and that the resource is range-seekable.
fn vod_response_from_source(source: Box<dyn VodByteSource>) -> Response {
    let total = source.content_length();
    axum::http::Response::builder()
        .status(StatusCode::OK)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, total)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(vod_body(source))
        .expect("valid vod response")
        .into_response()
}

/// Build a streaming `206 Partial Content` response for the inclusive byte range `[start, end]`
/// of a `total`-byte VOD.
fn vod_partial_response(
    source: Box<dyn VodByteSource>,
    start: u64,
    end: u64,
    total: u64,
) -> Response {
    axum::http::Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, end - start + 1)
        .header(
            header::CONTENT_RANGE,
            format!("bytes {start}-{end}/{total}"),
        )
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(vod_body(source))
        .expect("valid vod partial response")
        .into_response()
}

/// Resolve `id` on `network` to a single-file VOD via the manager's shared cache, or the HTTP
/// error response to return: `404` for an unknown network, `502` if resolution fails.
async fn resolve_vod(
    s: &AppState,
    network: &str,
    id: &str,
) -> Result<Arc<dyn VodContent>, Response> {
    match s.manager.resolve_vod(network, id).await {
        Ok(v) => Ok(v),
        Err(ProviderError::NotFound) => {
            Err((StatusCode::NOT_FOUND, "unknown network").into_response())
        }
        Err(e) => Err((StatusCode::BAD_GATEWAY, format!("{e:?}")).into_response()),
    }
}

/// `GET /vod/{network}/{id}/manifest.m3u8` — a static VOD HLS media playlist for the resolved
/// single-file VOD. The whole file's length is known, so the playlist lists every byte-range
/// segment up front and ends with `#EXT-X-ENDLIST`; segments are fetched lazily via
/// [`vod_hls_segment`]. Segment geometry comes from the shared HLS config (same knobs as live).
async fn vod_manifest(
    State(s): State<AppState>,
    Path((network, id)): Path<(String, String)>,
) -> Response {
    let vod = match resolve_vod(&s, &network, &id).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let layout = crate::hls::VodHlsLayout::new(vod.content_length(), s.manager.hls_config());
    (
        [(header::CONTENT_TYPE, "application/vnd.apple.mpegurl")],
        layout.playlist(&network, &id),
    )
        .into_response()
}

/// `GET /vod/{network}/{id}/seg/{n}.ts` — HLS segment `n` of the resolved VOD, served as the
/// verified byte range the [`VodHlsLayout`](crate::hls::VodHlsLayout) assigns to it. A segment
/// index past the last segment (or a malformed name) 404s.
async fn vod_hls_segment(
    State(s): State<AppState>,
    Path((network, id, seg)): Path<(String, String, String)>,
) -> Response {
    let Some(index) = seg.strip_suffix(".ts").and_then(|n| n.parse::<u64>().ok()) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let vod = match resolve_vod(&s, &network, &id).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let layout = crate::hls::VodHlsLayout::new(vod.content_length(), s.manager.hls_config());
    let Some((start, end)) = layout.segment_range(index) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match vod.open_range(start, end).await {
        Ok(source) => vod_hls_segment_response(source),
        Err(e) => (StatusCode::BAD_GATEWAY, format!("{e:?}")).into_response(),
    }
}

/// Build a streaming `200` response for one HLS VOD segment (MPEG-TS), advertising its length.
fn vod_hls_segment_response(source: Box<dyn VodByteSource>) -> Response {
    let len = source.content_length();
    axum::http::Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "video/mp2t")
        .header(header::CONTENT_LENGTH, len)
        .body(vod_body(source))
        .expect("valid vod segment response")
        .into_response()
}

/// `GET /broadcast/{name}` — the minted broadcast's raw transport-file bytes (the
/// `.acelive`-equivalent), if it's been `PUT` first. Lets any real Acestream node
/// (`--stream-support-node --url http://.../broadcast/{name}`) or our own tooling fetch the
/// descriptor by name instead of needing the infohash out of band. 404 if unminted.
async fn broadcast_transport(State(s): State<AppState>, Path(name): Path<String>) -> Response {
    let Some(bs) = &s.broadcasts else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match bs.registry.get(&name).await {
        Some(bc) => (
            [(header::CONTENT_TYPE, "application/octet-stream")],
            (*bc.transport_bytes).clone(),
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Whether `name` is safe to use as both a registry key and a persistence filename. Names
/// come straight from the URL path, so this closes a path-traversal vector: allow only
/// `[A-Za-z0-9._-]`, length 1..=64, and never `.`/`..`.
fn valid_broadcast_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// `PUT /broadcast/{name}` (B1) — accepts a chunked MPEG-TS body and originates it as an
/// Acestream-compatible live swarm. Responds immediately with the minted infohash (identical
/// name -> identical, already-minted broadcast; see `BroadcastRegistry::start_or_resume`)
/// while ingest continues in a background task — the request body may be a long-lived,
/// effectively-unbounded stream (a live source), so the handler can't wait for it to finish.
///
/// Piece numbering resumes from the broadcast's persisted cursor, so a second `PUT` to the
/// same name continues the sequence rather than restarting at 0 (issue #3).
async fn broadcast_ingest(
    State(s): State<AppState>,
    Path(name): Path<String>,
    body: Body,
) -> Response {
    if !valid_broadcast_name(&name) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Some(bs) = &s.broadcasts else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let (bc, freshly_minted) = bs
        .registry
        .start_or_resume(
            &name,
            &name,
            &bs.trackers,
            &bs.seed_registry,
            bs.store_bytes,
        )
        .await;
    let infohash = infohash_hex(&bc.infohash);
    let content_id = infohash_hex(&bc.content_id);
    crate::alog!("[broadcast] {name}: ingesting as infohash {infohash}");

    // Self-announce to tracker + DHT exactly once per freshly-minted name — resumed PUTs and
    // disk reloads must not spawn duplicate loops.
    if freshly_minted {
        bs.spawn_announce(&bc);
    }

    let store = bc.store.clone();
    let auth = bc.auth.clone();
    let cursor = bc.cursor.clone();
    tokio::spawn(async move {
        let mut ingest = crate::broadcast_ingest::BroadcastIngest::new(store, auth, cursor);
        let mut stream = body.into_data_stream();
        while let Some(chunk) = stream.next().await {
            let Ok(chunk) = chunk else { break };
            ingest.push_bytes(&chunk).await;
        }
        ingest.finish().await;
    });

    Json(json!({
        "name": name,
        "content_id": content_id,
        "infohash": infohash,
    }))
    .into_response()
}

/// `DELETE /broadcast/{name}` — purge a broadcast: drop it from the registry (and its persisted
/// record) and stop serving its infohash/content_id, so a subsequent `PUT` mints a fresh
/// identity. Idempotent: `204 No Content` whether or not the name existed.
async fn broadcast_delete(State(s): State<AppState>, Path(name): Path<String>) -> Response {
    if !valid_broadcast_name(&name) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Some(bs) = &s.broadcasts else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if bs.registry.delete(&name).await.is_some() {
        // Registry-entry eviction (and disk-dir cleanup) now ride the broadcast's SeedLease drop
        // inside `registry.delete` — no explicit seed_registry.remove / remove_cache_dir needed.
        crate::alog!("[broadcast] {name}: deleted");
    }
    StatusCode::NO_CONTENT.into_response()
}

async fn ace_getstream(
    State(s): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let Some(mut selection) = ace_selected_stream(&params) else {
        return Json(json!({ "response": null, "error": "missing content_id/infohash/id" }));
    };
    if ace_network(&s).is_none() {
        return Json(json!({ "response": null, "error": "no ace network registered" }));
    }
    if s.resolve_content_ids_in_getstream {
        if let Some(content_id) = selection.content_id.as_deref() {
            match resolve_via_catalog(content_id).await {
                Ok(info) => selection = selection.with_resolved_infohash(infohash_hex(&info.infohash)),
                Err(e) => crate::alog!(
                    "[ace] getstream content_id catalog resolution failed, falling back to cid: {e:?}"
                ),
            }
        }
    }

    let token = s
        .ace_sessions
        .mint(selection.playback_id.clone(), selection.session_key.clone());
    let base = request_base(&headers);
    let playback_id = selection.playback_id;
    let public_id = selection.public_id;
    Json(json!({
        "response": {
            "infohash": public_id,
            "playback_url": format!("{base}/ace/r/{playback_id}/{token}"),
            "stat_url": format!("{base}/ace/stat/{playback_id}/{token}"),
            "command_url": format!("{base}/ace/cmd/{playback_id}/{token}"),
            "playback_session_id": token,
            "client_session_id": -1,
            "is_live": 1,
            "is_encrypted": 0
        },
        "error": null
    }))
}

async fn ace_playback(
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
        Ok(session) => ace_stream_session_response(session, expires_at, cancel),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn ace_stat(
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
                "peers": 0,
                "downloaded": 0,
                "uploaded": 0,
                "infohash": public_id,
                "is_live": 1,
                "is_encrypted": 0,
                "playback_session_id": token,
                "client_session_id": -1
            },
            "error": null
        }));
    };

    let stats = session.stats().await;
    Json(json!({
        "response": {
            "status": "dl",
            "peers": stats.peers,
            "downloaded": stats.downloaded,
            "uploaded": stats.uploaded,
            "infohash": public_id,
            "is_live": 1,
            "is_encrypted": 0,
            "playback_session_id": token,
            "client_session_id": -1
        },
        "error": null
    }))
}

async fn ace_command(
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
async fn server_api(
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
                Ok(info) => Ok(ResolvedContent {
                    infohash: infohash_hex(&info.infohash),
                    content_id: Some(cid),
                    is_live: true,
                }),
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

struct AceStreamSelection {
    public_id: String,
    playback_id: String,
    session_key: String,
    content_id: Option<String>,
}

impl AceStreamSelection {
    fn with_resolved_infohash(mut self, infohash: String) -> Self {
        self.public_id = infohash.clone();
        self.playback_id = infohash;
        self.content_id = None;
        self
    }
}

fn ace_selected_stream(params: &HashMap<String, String>) -> Option<AceStreamSelection> {
    if let Some(content_id) = ace_nonempty_param(params, "content_id") {
        return Some(AceStreamSelection {
            public_id: content_id.to_string(),
            playback_id: format!("cid:{content_id}"),
            session_key: format!("cid:{content_id}"),
            content_id: Some(content_id.to_string()),
        });
    }
    if let Some(id) =
        ace_nonempty_param(params, "infohash").or_else(|| ace_nonempty_param(params, "id"))
    {
        return Some(AceStreamSelection {
            public_id: id.to_string(),
            playback_id: id.to_string(),
            session_key: id.to_string(),
            content_id: None,
        });
    }
    if let Some(url) = ace_nonempty_param(params, "url") {
        // The id is a reversible, path-safe encoding of the URL, so it is both the route-safe
        // playback_id and the session_key the provider decodes — no server-side alias table, so
        // playback survives a restart or a direct `/ace/r/{id}` hit.
        let token = crate::transport_url::encode_transport_url(url).ok()?;
        return Some(AceStreamSelection {
            public_id: token.clone(),
            playback_id: token.clone(),
            session_key: token,
            content_id: None,
        });
    }
    if let Some(magnet) = ace_nonempty_param(params, "magnet") {
        let hex = crate::magnet::parse_magnet_infohash(magnet).ok()?;
        return Some(AceStreamSelection {
            public_id: hex.clone(),
            playback_id: hex.clone(),
            session_key: hex,
            content_id: None,
        });
    }
    None
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

async fn networks(State(s): State<AppState>) -> Json<serde_json::Value> {
    Json(json!({ "networks": s.networks }))
}

/// `GET /streams` — active sessions and their client counts.
async fn list_streams(State(s): State<AppState>) -> Json<serde_json::Value> {
    let streams: Vec<_> = s
        .manager
        .list()
        .await
        .into_iter()
        .map(|(network, id, clients)| json!({ "network": network, "id": id, "clients": clients }))
        .collect();
    Json(json!({ "streams": streams }))
}

/// `DELETE /streams/{network}/{id}` — force-stop a running session (admin). 204 if it was
/// running, 404 otherwise. The `{id}` may carry a `.ts`/`.m3u8` suffix (mirroring the GET
/// URL); it's stripped so either form stops the same session.
async fn delete_stream(
    State(s): State<AppState>,
    Path((network, file)): Path<(String, String)>,
) -> Response {
    let id = file
        .strip_suffix(".ts")
        .or_else(|| file.strip_suffix(".m3u8"))
        .unwrap_or(&file);
    if s.manager.stop(&network, id).await {
        StatusCode::NO_CONTENT.into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

/// `GET /streams/{network}/{id}/status` — stats for a running session (404 if not active).
async fn stream_status(
    State(s): State<AppState>,
    Path((network, id)): Path<(String, String)>,
) -> Response {
    match s.manager.get(&network, &id).await {
        Some(session) => {
            let stats = session.stats().await;
            Json(json!({
                "network": network,
                "id": id,
                "clients": session.subscriber_count(),
                "peers": stats.peers,
                "bitrate": stats.bitrate,
                "buffer_ms": stats.buffer_ms,
                "uploaded": stats.uploaded,
                "peers_served": stats.peers_served,
            }))
            .into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// `GET /streams/{network}/{id}/seg/{n}.ts` — a retained HLS segment.
async fn stream_segment(
    State(s): State<AppState>,
    Path((network, id, seg)): Path<(String, String, String)>,
) -> Response {
    let Some(seq) = seg.strip_suffix(".ts").and_then(|n| n.parse::<u64>().ok()) else {
        return StatusCode::NOT_FOUND.into_response();
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

/// `GET /streams/{network}/{id}.ts` (continuous MPEG-TS) or `.m3u8` (live HLS playlist).
async fn stream_file(
    State(s): State<AppState>,
    Path((network, file)): Path<(String, String)>,
) -> Response {
    if let Some(id) = file.strip_suffix(".m3u8") {
        return match s.manager.get_or_start_hls(&network, id).await {
            Ok(pkg) => (
                [(header::CONTENT_TYPE, "application/vnd.apple.mpegurl")],
                pkg.playlist(&network, id),
            )
                .into_response(),
            Err(_) => StatusCode::NOT_FOUND.into_response(),
        };
    }
    let Some(id) = file.strip_suffix(".ts") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let session = match s.manager.get_or_start(&network, id).await {
        Ok(sess) => sess,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    stream_session_response(session)
}

fn stream_session_response(session: Arc<StreamSession>) -> Response {
    let sub = session.subscribe();
    // Per-client keyframe gate: a player joining mid-GOP is held until the first clean
    // keyframe (with PAT/PMT prepended) so it starts on a decodable picture, not garbage.
    let gate = ace_media::mpegts::KeyframeGate::new();
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

fn ace_stream_session_response(
    session: Arc<StreamSession>,
    expires_at: Instant,
    cancel: tokio::sync::watch::Receiver<bool>,
) -> Response {
    let sub = session.subscribe();
    let gate = ace_media::mpegts::KeyframeGate::new();
    let stream = futures::stream::unfold(
        (sub, gate, cancel, expires_at),
        |(mut sub, mut gate, mut cancel, expires_at)| async move {
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
                            return Some((Ok::<_, std::io::Error>(Bytes::from(out)), (sub, gate, cancel, expires_at)));
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
    (
        [(header::CONTENT_TYPE, "video/mp2t")],
        Body::from_stream(stream),
    )
        .into_response()
}

fn reset_stream_keyframe_gate(gate: &mut ace_media::mpegts::KeyframeGate) {
    *gate = ace_media::mpegts::KeyframeGate::new();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broadcast::{CHUNK_LENGTH, PIECE_LENGTH};
    use crate::provider::{
        ProviderError, ProviderRegistry, SourceStats, StreamProvider, TsSource, VodContent,
    };
    use crate::testprovider::TestProvider;
    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use bytes::Bytes;
    use tower::ServiceExt;

    struct FakeVod {
        chunks: std::collections::VecDeque<Bytes>,
        total: u64,
    }

    #[async_trait]
    impl VodByteSource for FakeVod {
        fn content_length(&self) -> u64 {
            self.total
        }
        async fn next(&mut self) -> Option<Bytes> {
            self.chunks.pop_front()
        }
    }

    #[tokio::test]
    async fn vod_response_streams_body_with_content_length() {
        let src = FakeVod {
            chunks: [Bytes::from_static(b"abc"), Bytes::from_static(b"de")]
                .into_iter()
                .collect(),
            total: 5,
        };
        let resp = vod_response_from_source(Box::new(src));
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()[header::CONTENT_LENGTH], "5");
        assert_eq!(resp.headers()[header::ACCEPT_RANGES], "bytes");
        let body = axum::body::to_bytes(resp.into_body(), 64).await.unwrap();
        assert_eq!(&body[..], b"abcde");
    }

    #[test]
    fn parse_byte_range_covers_the_range_header_forms() {
        use RangeOutcome::*;
        // No / non-bytes / unparseable / multi-range specs are ignored -> serve the whole file.
        assert!(matches!(parse_byte_range(None, 10), Full));
        assert!(matches!(parse_byte_range(Some("items=0-1"), 10), Full));
        assert!(matches!(parse_byte_range(Some("bytes=abc"), 10), Full));
        assert!(matches!(parse_byte_range(Some("bytes=-"), 10), Full));
        assert!(matches!(parse_byte_range(Some("bytes=0-1,3-4"), 10), Full));
        // Explicit range, clamped to the last byte.
        assert!(matches!(
            parse_byte_range(Some("bytes=2-4"), 10),
            Satisfiable(2, 4)
        ));
        assert!(matches!(
            parse_byte_range(Some("bytes=8-100"), 10),
            Satisfiable(8, 9)
        ));
        // Open-ended: from N to the end.
        assert!(matches!(
            parse_byte_range(Some("bytes=5-"), 10),
            Satisfiable(5, 9)
        ));
        // Suffix: the last N bytes (and a suffix longer than the file clamps to the whole file).
        assert!(matches!(
            parse_byte_range(Some("bytes=-3"), 10),
            Satisfiable(7, 9)
        ));
        assert!(matches!(
            parse_byte_range(Some("bytes=-100"), 10),
            Satisfiable(0, 9)
        ));
        // Out-of-bounds / inverted / zero-suffix are unsatisfiable -> 416.
        assert!(matches!(
            parse_byte_range(Some("bytes=100-200"), 10),
            Unsatisfiable
        ));
        assert!(matches!(
            parse_byte_range(Some("bytes=5-3"), 10),
            Unsatisfiable
        ));
        assert!(matches!(
            parse_byte_range(Some("bytes=-0"), 10),
            Unsatisfiable
        ));
    }

    // An in-memory VOD provider for exercising the HTTP `/vod` range contract without a swarm.
    // (Real SHA-1 verification of served ranges is covered by the ace-swarm vod tests.)
    struct MemVodProvider {
        data: Vec<u8>,
    }
    struct MemVod {
        data: Vec<u8>,
    }
    struct MemVodSource {
        chunk: Option<Bytes>,
        len: u64,
    }
    #[async_trait]
    impl VodByteSource for MemVodSource {
        fn content_length(&self) -> u64 {
            self.len
        }
        async fn next(&mut self) -> Option<Bytes> {
            self.chunk.take()
        }
    }
    #[async_trait]
    impl VodContent for MemVod {
        fn content_length(&self) -> u64 {
            self.data.len() as u64
        }
        async fn open_range(
            &self,
            start: u64,
            end: u64,
        ) -> Result<Box<dyn VodByteSource>, ProviderError> {
            let slice = self.data[start as usize..=end as usize].to_vec();
            let len = slice.len() as u64;
            Ok(Box::new(MemVodSource {
                chunk: Some(Bytes::from(slice)),
                len,
            }))
        }
    }
    #[async_trait]
    impl StreamProvider for MemVodProvider {
        fn network(&self) -> &'static str {
            "memvod"
        }
        async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
            Err(ProviderError::NotFound)
        }
        async fn resolve_vod(&self, _id: &str) -> Result<Box<dyn VodContent>, ProviderError> {
            Ok(Box::new(MemVod {
                data: self.data.clone(),
            }))
        }
    }

    fn memvod_router(data: Vec<u8>) -> Router {
        let mut r = ProviderRegistry::new();
        r.register(std::sync::Arc::new(MemVodProvider { data }));
        router(AppState {
            manager: StreamManager::new(r),
            networks: vec!["memvod".into()],
            resolve_content_ids_in_getstream: false,
            ace_sessions: Arc::new(AceSessionStore::default()),
            experimental_ace_compat: false,
            broadcasts: None,
        })
    }

    #[tokio::test]
    async fn vod_without_range_serves_200_and_advertises_accept_ranges() {
        let data: Vec<u8> = (0..20u8).collect();
        let resp = memvod_router(data.clone())
            .oneshot(Request::get("/vod/memvod/x").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()[header::ACCEPT_RANGES], "bytes");
        assert_eq!(resp.headers()[header::CONTENT_LENGTH], "20");
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        assert_eq!(&body[..], &data[..]);
    }

    #[tokio::test]
    async fn vod_range_serves_206_partial_content() {
        let data: Vec<u8> = (0..20u8).collect();
        let resp = memvod_router(data.clone())
            .oneshot(
                Request::get("/vod/memvod/x")
                    .header(header::RANGE, "bytes=5-9")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(resp.headers()[header::CONTENT_RANGE], "bytes 5-9/20");
        assert_eq!(resp.headers()[header::CONTENT_LENGTH], "5");
        assert_eq!(resp.headers()[header::ACCEPT_RANGES], "bytes");
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        assert_eq!(&body[..], &data[5..=9]);
    }

    #[tokio::test]
    async fn vod_suffix_range_serves_the_last_bytes() {
        let data: Vec<u8> = (0..20u8).collect();
        let resp = memvod_router(data.clone())
            .oneshot(
                Request::get("/vod/memvod/x")
                    .header(header::RANGE, "bytes=-4")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(resp.headers()[header::CONTENT_RANGE], "bytes 16-19/20");
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        assert_eq!(&body[..], &data[16..=19]);
    }

    #[tokio::test]
    async fn vod_unsatisfiable_range_returns_416_with_content_range() {
        let data: Vec<u8> = (0..20u8).collect();
        let resp = memvod_router(data)
            .oneshot(
                Request::get("/vod/memvod/x")
                    .header(header::RANGE, "bytes=50-60")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(resp.headers()[header::CONTENT_RANGE], "bytes */20");
    }

    // A memvod router whose HLS segments are `seg_packets` TS packets (188 bytes each), so small
    // fixtures still span several segments.
    fn memvod_hls_router(data: Vec<u8>, seg_packets: usize) -> Router {
        let mut r = ProviderRegistry::new();
        r.register(std::sync::Arc::new(MemVodProvider { data }));
        let hls = crate::config::HlsConfig {
            segment_packets: seg_packets,
            window_segments: 6,
            segment_duration_ms: 2000,
        };
        router(AppState {
            manager: StreamManager::with_config(r, 256, hls),
            networks: vec!["memvod".into()],
            resolve_content_ids_in_getstream: false,
            ace_sessions: Arc::new(AceSessionStore::default()),
            experimental_ace_compat: false,
            broadcasts: None,
        })
    }

    #[tokio::test]
    async fn vod_manifest_serves_a_terminated_vod_playlist() {
        // 450 bytes over 188-byte (1-packet) segments => 3 segments.
        let resp = memvod_hls_router(vec![0u8; 450], 1)
            .oneshot(
                Request::get("/vod/memvod/x/manifest.m3u8")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()[header::CONTENT_TYPE],
            "application/vnd.apple.mpegurl"
        );
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        assert!(text.contains("#EXT-X-PLAYLIST-TYPE:VOD"));
        assert!(text.contains("/vod/memvod/x/seg/0.ts"));
        assert!(text.contains("/vod/memvod/x/seg/2.ts"));
        assert!(!text.contains("/vod/memvod/x/seg/3.ts"));
        assert!(text.trim_end().ends_with("#EXT-X-ENDLIST"));
    }

    #[tokio::test]
    async fn vod_hls_segment_serves_its_verified_byte_range_as_ts() {
        let data: Vec<u8> = (0..255u8).cycle().take(450).collect();
        // Segment 1 covers bytes [188, 376).
        let resp = memvod_hls_router(data.clone(), 1)
            .oneshot(
                Request::get("/vod/memvod/x/seg/1.ts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()[header::CONTENT_TYPE], "video/mp2t");
        assert_eq!(resp.headers()[header::CONTENT_LENGTH], "188");
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        assert_eq!(&body[..], &data[188..376]);
    }

    #[tokio::test]
    async fn vod_hls_final_segment_is_clamped_to_the_last_byte() {
        let data: Vec<u8> = (0..255u8).cycle().take(450).collect();
        // 450 bytes => final segment 2 covers [376, 450): 74 bytes.
        let resp = memvod_hls_router(data.clone(), 1)
            .oneshot(
                Request::get("/vod/memvod/x/seg/2.ts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()[header::CONTENT_LENGTH], "74");
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        assert_eq!(&body[..], &data[376..450]);
    }

    #[tokio::test]
    async fn vod_hls_segment_past_the_end_404s() {
        let resp = memvod_hls_router(vec![0u8; 450], 1)
            .oneshot(
                Request::get("/vod/memvod/x/seg/99.ts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    fn params(pairs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn getstream_selects_magnet_as_infohash() {
        let ih = "0123456789abcdef0123456789abcdef01234567";
        let sel = ace_selected_stream(&params(&[("magnet", &format!("magnet:?xt=urn:btih:{ih}"))]))
            .unwrap();
        assert_eq!(sel.session_key, ih);
        assert_eq!(sel.playback_id, ih);
    }

    #[test]
    fn getstream_selects_transport_url_with_self_contained_playback_id() {
        let url = "https://example.com/a/b.acelive";
        let sel = ace_selected_stream(&params(&[("url", url)])).unwrap();
        // playback_id == session_key: no server-side alias table, so playback survives a restart
        // or a direct hit, and the id is path-safe (single segment, no '/'/':'/'?').
        assert_eq!(sel.playback_id, sel.session_key);
        assert!(sel.playback_id.starts_with("turl-"));
        assert!(!sel.playback_id.contains('/'));
        assert!(!sel.playback_id.contains(':'));
        assert!(!sel.playback_id.contains('?'));
        // ...and it decodes back to the URL the provider will fetch.
        assert_eq!(
            crate::transport_url::decode_transport_url(&sel.session_key).as_deref(),
            Some(url)
        );
    }

    #[test]
    fn getstream_precedence_content_id_over_infohash_over_url_over_magnet() {
        let ih = "0123456789abcdef0123456789abcdef01234567";
        let cid = "89abcdef0123456789abcdef0123456789abcdef";
        let sel = ace_selected_stream(&params(&[
            ("content_id", cid),
            ("infohash", ih),
            ("url", "https://e/x"),
            ("magnet", &format!("magnet:?xt=urn:btih:{ih}")),
        ]))
        .unwrap();
        assert_eq!(sel.session_key, format!("cid:{cid}"));

        let sel = ace_selected_stream(&params(&[
            ("infohash", ih),
            ("url", "https://e/x"),
            ("magnet", &format!("magnet:?xt=urn:btih:{ih}")),
        ]))
        .unwrap();
        assert_eq!(sel.session_key, ih);

        let sel = ace_selected_stream(&params(&[
            ("url", "https://e/x.acelive"),
            ("magnet", &format!("magnet:?xt=urn:btih:{ih}")),
        ]))
        .unwrap();
        assert_eq!(
            crate::transport_url::decode_transport_url(&sel.session_key).as_deref(),
            Some("https://e/x.acelive")
        );
    }

    #[test]
    fn getstream_rejects_bad_url_and_magnet() {
        assert!(ace_selected_stream(&params(&[("url", "file:///etc/passwd")])).is_none());
        assert!(ace_selected_stream(&params(&[("magnet", "magnet:?dn=noxt")])).is_none());
    }

    fn state() -> AppState {
        let mut r = ProviderRegistry::new();
        r.register(std::sync::Arc::new(TestProvider { chunks: 10 }));
        AppState {
            manager: StreamManager::new(r),
            networks: vec!["test".into()],
            resolve_content_ids_in_getstream: false,
            ace_sessions: Arc::new(AceSessionStore::default()),
            experimental_ace_compat: false,
            broadcasts: None,
        }
    }

    /// A real libx264 MPEG-TS (committed fixture). Video PID 0x100; keyframes at byte
    /// offsets 564 and 9400.
    const FIXTURE: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/vectors/media/h264-keyframes.ts"
    ));

    /// Provider that replays `FIXTURE` from byte `skip` (188-aligned), a few TS packets per
    /// chunk — used to prove the per-client keyframe gate on a genuine stream.
    struct FixtureProvider {
        skip: usize,
    }
    struct FixtureSource {
        pos: usize,
    }
    struct PacedFixtureProvider;
    struct PacedFixtureSource {
        pos: usize,
    }
    #[async_trait]
    impl TsSource for FixtureSource {
        async fn next(&mut self) -> Option<Bytes> {
            if self.pos >= FIXTURE.len() {
                std::future::pending().await
            }
            tokio::task::yield_now().await;
            let end = (self.pos + 188 * 3).min(FIXTURE.len());
            let chunk = Bytes::copy_from_slice(&FIXTURE[self.pos..end]);
            self.pos = end;
            Some(chunk)
        }
        fn stats(&self) -> SourceStats {
            SourceStats {
                peers: 1,
                bitrate: 0,
                buffer_ms: 0,
                downloaded: self.pos as u64,
                uploaded: 0,
                peers_served: 0,
            }
        }
    }
    #[async_trait]
    impl StreamProvider for FixtureProvider {
        fn network(&self) -> &'static str {
            "fix"
        }
        async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
            Ok(Box::new(FixtureSource { pos: self.skip }))
        }
    }
    #[async_trait]
    impl TsSource for PacedFixtureSource {
        async fn next(&mut self) -> Option<Bytes> {
            tokio::time::sleep(Duration::from_millis(2)).await;
            if self.pos >= FIXTURE.len() {
                self.pos = 0;
            }
            let end = (self.pos + 188 * 8).min(FIXTURE.len());
            let chunk = Bytes::copy_from_slice(&FIXTURE[self.pos..end]);
            self.pos = end;
            Some(chunk)
        }
        fn stats(&self) -> SourceStats {
            SourceStats::default()
        }
    }
    #[async_trait]
    impl StreamProvider for PacedFixtureProvider {
        fn network(&self) -> &'static str {
            "fix"
        }
        async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
            Ok(Box::new(PacedFixtureSource { pos: 0 }))
        }
    }

    fn fixture_state(skip: usize) -> AppState {
        let mut r = ProviderRegistry::new();
        r.register(std::sync::Arc::new(FixtureProvider { skip }));
        AppState {
            manager: StreamManager::new(r),
            networks: vec!["fix".into()],
            resolve_content_ids_in_getstream: false,
            ace_sessions: Arc::new(AceSessionStore::default()),
            experimental_ace_compat: false,
            broadcasts: None,
        }
    }

    fn ace_compat_state(skip: usize) -> AppState {
        let mut st = fixture_state(skip);
        st.experimental_ace_compat = true;
        st
    }

    #[test]
    fn content_id_selection_uses_resolved_infohash_when_available() {
        let content_id = "2123456789abcdef0123456789abcdef01234567";
        let resolved = "50e93529d3eb46a50506b14464185a15292d6e47";
        let mut params = HashMap::new();
        params.insert("content_id".to_string(), content_id.to_string());

        let selection = ace_selected_stream(&params)
            .unwrap()
            .with_resolved_infohash(resolved.to_string());

        assert_eq!(selection.public_id, resolved);
        assert_eq!(selection.playback_id, resolved);
        assert_eq!(selection.session_key, format!("cid:{content_id}"));
    }

    #[tokio::test]
    async fn stream_ts_starts_on_keyframe_when_joining_mid_gop() {
        use futures::StreamExt;
        const VIDEO_PID: u16 = 0x0100;
        const KEYFRAME2: usize = 9400;
        // Hold the app (and thus the manager/session) alive while we read the streamed body;
        // the session's pump is tied to the session's lifetime.
        let app = router(fixture_state(4136));
        // Join the stream mid-GOP (a non-keyframe video packet between the two keyframes).
        let resp = app
            .clone()
            .oneshot(
                Request::get("/streams/fix/x.ts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Collect a few KB of the served body.
        let mut stream = resp.into_body().into_data_stream();
        let mut body = Vec::new();
        while body.len() < 4096 {
            match stream.next().await {
                Some(Ok(chunk)) => body.extend_from_slice(&chunk),
                _ => break,
            }
        }
        // The first video packet the client sees must be the real keyframe at byte 9400,
        // not the mid-GOP packet we joined on.
        let first_video = body
            .chunks_exact(188)
            .find(|p| (((p[1] & 0x1F) as u16) << 8 | p[2] as u16) == VIDEO_PID)
            .expect("a video packet was served");
        assert_eq!(first_video, &FIXTURE[KEYFRAME2..KEYFRAME2 + 188]);
    }

    #[test]
    fn stream_ts_resets_keyframe_gate_after_lag() {
        let mut gate = ace_media::mpegts::KeyframeGate::new();
        assert!(
            !gate.push(FIXTURE).is_empty(),
            "fixture should lock the gate on a keyframe"
        );
        let mid_gop_packet = &FIXTURE[4136..4136 + 188];
        assert_eq!(
            gate.push(mid_gop_packet).len(),
            188,
            "locked gate passes through mid-GOP packets"
        );

        reset_stream_keyframe_gate(&mut gate);

        assert!(
            gate.push(mid_gop_packet).is_empty(),
            "after lag reset, mid-GOP packets are held until a keyframe"
        );
    }

    #[tokio::test]
    async fn healthz_ok() {
        let resp = router(state())
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn ace_session_store_expires_and_bounds_leases_deterministically() {
        let now = Instant::now();
        let store = AceSessionStore::new(Duration::from_secs(10), 2);
        store.insert_at("a".into(), "id-a".into(), "key-a".into(), now);
        store.insert_at(
            "b".into(),
            "id-b".into(),
            "key-b".into(),
            now + Duration::from_secs(1),
        );
        store.insert_at(
            "c".into(),
            "id-c".into(),
            "key-c".into(),
            now + Duration::from_secs(2),
        );

        assert_eq!(
            store.validate_at("id-a", "a", now + Duration::from_secs(2)),
            None
        );
        assert_eq!(
            store.validate_at("id-b", "b", now + Duration::from_secs(2)),
            Some("key-b".into())
        );
        assert_eq!(
            store.validate_at("wrong-id", "b", now + Duration::from_secs(2)),
            None
        );
        assert_eq!(
            store.validate_at("id-b", "b", now + Duration::from_secs(12)),
            None
        );
        assert_eq!(store.leases.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn ace_session_store_revokes_only_matching_client_lease() {
        let now = Instant::now();
        let store = AceSessionStore::new(Duration::from_secs(60), 4);
        store.insert_at("client-a".into(), "same-id".into(), "same-key".into(), now);
        store.insert_at("client-b".into(), "same-id".into(), "same-key".into(), now);

        let (_, _, mut client_a) = store.playback("same-id", "client-a").unwrap();
        let (_, _, client_b) = store.playback("same-id", "client-b").unwrap();
        assert!(store.revoke("same-id", "client-a"));
        client_a.changed().await.unwrap();
        assert!(*client_a.borrow());
        assert!(client_b.has_changed().is_ok_and(|changed| !changed));
        assert_eq!(store.validate("same-id", "client-a"), None);
        assert_eq!(
            store.validate("same-id", "client-b"),
            Some("same-key".into())
        );
    }

    #[tokio::test]
    async fn networks_lists_registered() {
        let resp = router(state())
            .oneshot(Request::get("/networks").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&body).contains("\"test\""));
    }

    #[tokio::test]
    async fn stream_ts_serves_mpegts_first_frame() {
        use futures::StreamExt;
        // Stream the real fixture from the start; the gate locks on its leading keyframe.
        // Keep `app` alive so the session/pump survives while we read the body.
        let app = router(fixture_state(0));
        let resp = app
            .clone()
            .oneshot(
                Request::get("/streams/fix/somechannel.ts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()[header::CONTENT_TYPE], "video/mp2t");
        // Live body is unbounded; read just the first TS chunk.
        let mut stream = resp.into_body().into_data_stream();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first[0], 0x47);
    }

    #[tokio::test]
    async fn ace_getstream_content_id_returns_a_playback_url_that_streams() {
        use futures::StreamExt;
        let content_id = "2123456789abcdef0123456789abcdef01234567";
        let state = ace_compat_state(0);
        let manager = state.manager.clone();
        let app = router(state);

        let resp = app
            .clone()
            .oneshot(
                Request::get(format!(
                    "/ace/getstream?format=json&content_id={content_id}"
                ))
                .header(header::HOST, "localhost:6900")
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], serde_json::Value::Null);
        assert_eq!(json["response"]["is_live"], 1);
        assert_eq!(json["response"]["is_encrypted"], 0);
        assert_eq!(json["response"]["infohash"], content_id);

        let playback_url = json["response"]["playback_url"].as_str().unwrap();
        let playback_path = playback_url
            .strip_prefix("http://localhost:6900")
            .expect("playback URL should point back at this daemon");
        assert!(playback_path.starts_with(&format!("/ace/r/cid:{content_id}/")));
        // Keep `app` alive while reading, as in the clean `/streams` tests: the test
        // manager owns the live session.
        let media = app
            .clone()
            .oneshot(Request::get(playback_path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(media.status(), StatusCode::OK);
        assert_eq!(media.headers()[header::CONTENT_TYPE], "video/mp2t");
        let mut stream = media.into_body().into_data_stream();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first[0], 0x47);
        assert!(manager
            .get("fix", &format!("cid:{content_id}"))
            .await
            .is_some());
        assert!(manager.get("fix", content_id).await.is_none());
    }

    #[tokio::test]
    async fn ace_getstream_mints_distinct_enforced_tokens() {
        let content_id = "2123456789abcdef0123456789abcdef01234567";
        let app = router(ace_compat_state(0));
        let mut tokens = Vec::new();
        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(
                    Request::get(format!("/ace/getstream?content_id={content_id}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let body = axum::body::to_bytes(response.into_body(), 1 << 20)
                .await
                .unwrap();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            tokens.push(
                value["response"]["playback_session_id"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            );
        }
        assert_ne!(tokens[0], tokens[1]);
        assert_eq!(tokens[0].len(), 64);
        assert!(!tokens[0].contains(content_id));

        let playback = app
            .clone()
            .oneshot(
                Request::get(format!("/ace/r/cid:{content_id}/invalid"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(playback.status(), StatusCode::NOT_FOUND);
        for path in [
            format!("/ace/stat/cid:{content_id}/invalid"),
            format!("/ace/cmd/cid:{content_id}/invalid?method=stop"),
            format!("/ace/cmd/cid:{content_id}/invalid?method=pause"),
            format!("/ace/cmd/cid:{content_id}/invalid"),
        ] {
            let response = app
                .clone()
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            let body = axum::body::to_bytes(response.into_body(), 1 << 20)
                .await
                .unwrap();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["response"], serde_json::Value::Null);
            assert_eq!(value["error"], "invalid or expired playback session");
        }
    }

    #[tokio::test]
    async fn active_ace_playback_ends_at_its_lease_deadline() {
        use futures::StreamExt;
        let content_id = "2123456789abcdef0123456789abcdef01234567";
        let mut state = ace_compat_state(0);
        let manager = state.manager.clone();
        state.ace_sessions = Arc::new(AceSessionStore::new(Duration::from_millis(30), 4));
        let app = router(state);
        let minted = app
            .clone()
            .oneshot(
                Request::get(format!("/ace/getstream?content_id={content_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(minted.into_body(), 1 << 20)
            .await
            .unwrap();
        let minted: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let path = minted["response"]["playback_url"]
            .as_str()
            .unwrap()
            .strip_prefix("http://127.0.0.1:6878")
            .unwrap()
            .to_owned();
        let playback = app
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let mut body = playback.into_body().into_data_stream();
        assert_eq!(
            manager
                .get("fix", &format!("cid:{content_id}"))
                .await
                .unwrap()
                .subscriber_count(),
            1
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(
            body.next().await.is_none(),
            "an active body must terminate when its lease expires"
        );
        assert_eq!(
            manager
                .get("fix", &format!("cid:{content_id}"))
                .await
                .unwrap()
                .subscriber_count(),
            0
        );
    }

    #[tokio::test]
    async fn ace_stat_and_stop_track_content_id_session() {
        use futures::StreamExt;
        let content_id = "2123456789abcdef0123456789abcdef01234567";
        let state = ace_compat_state(0);
        let manager = state.manager.clone();
        let app = router(state);
        let minted = app
            .clone()
            .oneshot(
                Request::get(format!("/ace/getstream?content_id={content_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(minted.into_body(), 1 << 20)
            .await
            .unwrap();
        let minted: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let token = minted["response"]["playback_session_id"].as_str().unwrap();
        let stat_path = format!("/ace/stat/cid:{content_id}/{token}");
        let idle = app
            .clone()
            .oneshot(Request::get(&stat_path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(idle.status(), StatusCode::OK);
        let body = axum::body::to_bytes(idle.into_body(), 1 << 20)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], serde_json::Value::Null);
        assert_eq!(json["response"]["status"], "idle");
        assert_eq!(json["response"]["infohash"], content_id);

        let media = app
            .clone()
            .oneshot(
                Request::get(format!("/ace/r/cid:{content_id}/{token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(media.status(), StatusCode::OK);
        let mut stream = media.into_body().into_data_stream();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first[0], 0x47);

        let running = app
            .clone()
            .oneshot(Request::get(&stat_path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(running.status(), StatusCode::OK);
        let body = axum::body::to_bytes(running.into_body(), 1 << 20)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], serde_json::Value::Null);
        assert_eq!(json["response"]["status"], "dl");
        assert_eq!(json["response"]["peers"], 1);
        assert!(json["response"]["downloaded"].as_u64().unwrap() > 0);

        let stop = app
            .oneshot(
                Request::get(format!("/ace/cmd/cid:{content_id}/{token}?method=stop"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stop.status(), StatusCode::OK);
        let body = axum::body::to_bytes(stop.into_body(), 1 << 20)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], serde_json::Value::Null);
        assert_eq!(json["response"], "ok");
        assert!(
            manager
                .get("fix", &format!("cid:{content_id}"))
                .await
                .is_some(),
            "compatibility stop must not force-stop the shared source"
        );
        assert!(manager.get("fix", content_id).await.is_none());
    }

    #[tokio::test]
    async fn ace_stop_isolates_two_compat_clients_and_preserves_native_consumer() {
        use futures::StreamExt;
        let id = "shared";
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(PacedFixtureProvider));
        let manager = StreamManager::new(registry);
        let app = router(AppState {
            manager: manager.clone(),
            networks: vec!["fix".into()],
            resolve_content_ids_in_getstream: false,
            ace_sessions: Arc::new(AceSessionStore::default()),
            experimental_ace_compat: true,
            broadcasts: None,
        });

        async fn mint(app: &Router, id: &str) -> serde_json::Value {
            let response = app
                .clone()
                .oneshot(
                    Request::get(format!("/ace/getstream?infohash={id}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let body = axum::body::to_bytes(response.into_body(), 1 << 20)
                .await
                .unwrap();
            serde_json::from_slice(&body).unwrap()
        }
        let a = mint(&app, id).await;
        let b = mint(&app, id).await;
        let a_path = a["response"]["playback_url"]
            .as_str()
            .unwrap()
            .strip_prefix("http://127.0.0.1:6878")
            .unwrap();
        let b_path = b["response"]["playback_url"]
            .as_str()
            .unwrap()
            .strip_prefix("http://127.0.0.1:6878")
            .unwrap();
        let stop_a = a["response"]["command_url"]
            .as_str()
            .unwrap()
            .strip_prefix("http://127.0.0.1:6878")
            .unwrap();

        let mut body_a = app
            .clone()
            .oneshot(Request::get(a_path).body(Body::empty()).unwrap())
            .await
            .unwrap()
            .into_body()
            .into_data_stream();
        let mut body_b = app
            .clone()
            .oneshot(Request::get(b_path).body(Body::empty()).unwrap())
            .await
            .unwrap()
            .into_body()
            .into_data_stream();
        let mut native = app
            .clone()
            .oneshot(
                Request::get(format!("/streams/fix/{id}.ts"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body()
            .into_data_stream();
        assert!(body_a.next().await.transpose().unwrap().is_some());
        assert!(body_b.next().await.transpose().unwrap().is_some());
        assert!(native.next().await.transpose().unwrap().is_some());
        assert_eq!(manager.get("fix", id).await.unwrap().subscriber_count(), 3);

        let stopped = app
            .clone()
            .oneshot(
                Request::get(format!("{stop_a}?method=stop"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stopped.status(), StatusCode::OK);
        assert!(body_a.next().await.is_none(), "stopped client A must close");
        assert_eq!(manager.get("fix", id).await.unwrap().subscriber_count(), 2);
        assert!(
            body_b.next().await.transpose().unwrap().is_some(),
            "client B must keep receiving"
        );
        assert!(
            native.next().await.transpose().unwrap().is_some(),
            "native consumer must keep receiving"
        );

        drop(body_b);
        drop(native);
        assert_eq!(
            manager.get("fix", id).await.unwrap().subscriber_count(),
            0,
            "the final consumers disappearing must leave the source eligible for idle cleanup"
        );
    }

    #[tokio::test]
    async fn ace_compat_routes_are_404_by_default() {
        for path in [
            "/ace/getstream?format=json&infohash=0123456789012345678901234567890123456789",
            "/ace/r/0123456789012345678901234567890123456789/outpace",
            "/ace/stat/0123456789012345678901234567890123456789/outpace",
            "/ace/cmd/0123456789012345678901234567890123456789/outpace?method=stop",
            "/server/api?method=get_version",
            "/ace/manifest.m3u8?infohash=0123456789012345678901234567890123456789",
            "/ace/c/session/0.ts",
        ] {
            let resp = router(fixture_state(0))
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{path}");
        }
    }

    #[tokio::test]
    async fn unknown_network_returns_404() {
        let resp = router(state())
            .oneshot(
                Request::get("/streams/nope/x.ts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn non_ts_extension_returns_404() {
        let resp = router(state())
            .oneshot(
                Request::get("/streams/test/x.foo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn status_404_until_started_then_reports() {
        let st = state();
        let app = router(st.clone());
        // Not started yet.
        let resp = app
            .clone()
            .oneshot(
                Request::get("/streams/test/chan/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        // Start it, then status reports it with clean keys.
        st.manager.get_or_start("test", "chan").await.unwrap();
        let resp = app
            .oneshot(
                Request::get("/streams/test/chan/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let txt = String::from_utf8_lossy(&body);
        assert!(txt.contains("\"clients\"") && txt.contains("\"peers\""));
    }

    #[tokio::test]
    async fn m3u8_serves_hls_playlist() {
        let resp = router(state())
            .oneshot(
                Request::get("/streams/test/chan.m3u8")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()[header::CONTENT_TYPE],
            "application/vnd.apple.mpegurl"
        );
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&body).starts_with("#EXTM3U"));
    }

    #[tokio::test]
    async fn delete_stops_running_session_then_404() {
        let st = state();
        let app = router(st.clone());
        st.manager.get_or_start("test", "z").await.unwrap();
        // First delete stops the running session.
        let resp = app
            .clone()
            .oneshot(
                Request::delete("/streams/test/z")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert!(st.manager.get("test", "z").await.is_none());
        // Second delete finds nothing to stop.
        let resp = app
            .oneshot(
                Request::delete("/streams/test/z")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn list_streams_reports_active() {
        let st = state();
        st.manager.get_or_start("test", "abc").await.unwrap();
        let resp = router(st)
            .oneshot(Request::get("/streams").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&body).contains("\"abc\""));
    }

    fn broadcast_state() -> AppState {
        let mut st = state();
        st.broadcasts = Some(BroadcastState {
            registry: crate::broadcast::BroadcastRegistry::new(),
            seed_registry: ace_swarm::listen::SeedRegistry::new(),
            trackers: vec![],
            // A few pieces' worth of headroom (piece_length is 1 MiB) — big enough that a
            // freshly-completed piece 0 isn't immediately evicted the instant piece 1's
            // first chunk arrives (PieceStore evicts the lowest piece once over budget).
            store_bytes: 4 << 20,
            // No inbound listener in tests -> self-announce is a no-op (see the field doc);
            // keeps these tests offline/instant instead of hitting the network.
            inbound_peer_port: tokio::sync::watch::channel(None).1,
        });
        st
    }

    #[tokio::test]
    async fn broadcast_put_returns_404_when_disabled() {
        let resp = router(state()) // plain `state()`: broadcasts: None
            .oneshot(
                Request::put("/broadcast/x")
                    .body(Body::from(vec![0u8; 4]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn broadcast_get_404s_until_minted_then_serves_the_transport_bytes() {
        let st = broadcast_state();
        let app = router(st);

        let resp = app
            .clone()
            .oneshot(Request::get("/broadcast/x").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND, "not minted yet");

        let put_resp = app
            .clone()
            .oneshot(
                Request::put("/broadcast/x")
                    .body(Body::from(vec![0x47u8; 8]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put_resp.status(), StatusCode::OK);

        let get_resp = app
            .oneshot(Request::get("/broadcast/x").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(get_resp.into_body(), 1 << 20)
            .await
            .unwrap();
        assert!(
            ace_wire::infohash::is_transport_file(&body),
            "GET /broadcast/{{name}} must serve real transport-file bytes"
        );
    }

    #[tokio::test]
    async fn broadcast_put_mints_and_serves_via_the_shared_seed_registry() {
        let st = broadcast_state();
        let seed_registry = st.broadcasts.as_ref().unwrap().seed_registry.clone();
        // Hold the `BroadcastRegistry` Arc alive for the test's duration too: its `by_name` map
        // now anchors each broadcast's `SeedLease` (Task 5), and `app.oneshot(..)` consumes (and
        // drops) the whole router — including its `AppState` clone — after each request.
        let _registry = st.broadcasts.as_ref().unwrap().registry.clone();
        let app = router(st);

        // A run of 188-byte packets each starting with the TS sync byte (0x47) — enough of
        // them, past TsResync's one-packet lookahead, to yield >= CHUNK_LENGTH bytes of
        // sync-locked output for the chunker.
        const TS_PACKET_LEN: usize = 188;
        let packets_needed = CHUNK_LENGTH.div_ceil(TS_PACKET_LEN as u64) as usize + 1;
        let mut body = Vec::with_capacity(packets_needed * TS_PACKET_LEN);
        for i in 0..packets_needed {
            body.push(0x47);
            body.extend(std::iter::repeat_n((i % 256) as u8, TS_PACKET_LEN - 1));
        }
        let resp = app
            .oneshot(
                Request::put("/broadcast/mychan")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let respbody = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&respbody).unwrap();
        let infohash_hex = json["infohash"].as_str().unwrap().to_string();
        assert_eq!(infohash_hex.len(), 40, "a 20-byte infohash, hex-encoded");
        let content_id_hex = json["content_id"].as_str().unwrap().to_string();
        assert_eq!(
            content_id_hex.len(),
            40,
            "a 20-byte content-id, hex-encoded"
        );
        assert_eq!(json["name"], "mychan");

        // The minted infohash must be immediately servable via the shared registry (S1/S2's
        // existing serve path) — confirming the wiring, not just the HTTP response shape.
        let mut infohash = [0u8; 20];
        for i in 0..20 {
            infohash[i] = u8::from_str_radix(&infohash_hex[i * 2..i * 2 + 2], 16).unwrap();
        }
        let mut content_id = [0u8; 20];
        for i in 0..20 {
            content_id[i] = u8::from_str_radix(&content_id_hex[i * 2..i * 2 + 2], 16).unwrap();
        }
        assert!(seed_registry.serves(&infohash));
        assert!(seed_registry.serves(&content_id));

        // Give the background ingest task a moment to process the body. It's short (well
        // under a full 1 MiB piece), so this reaches the store via `SigningChunker::flush`'s
        // short-final-piece path (see `ace_wire::signing_chunker`), not a full-piece `push`.
        for _ in 0..50 {
            if seed_registry
                .get(&infohash)
                .unwrap()
                .lock()
                .await
                .chunk(0, 0)
                .is_some()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            seed_registry
                .get(&infohash)
                .unwrap()
                .lock()
                .await
                .chunk(0, 0)
                .is_some(),
            "ingested bytes must reach the piece store as chunk (0, 0)"
        );
    }

    #[tokio::test]
    async fn ingested_piece_carries_a_real_verifiable_signature() {
        // A full 1 MiB piece's worth of body (piece_length - signature_len of real TS-shaped
        // content) so the ingest goes through SigningChunker's ordinary `push` path (a
        // complete, signed piece), not the short-final-piece `flush` path the other test
        // covers. Proves the actual B0 signing crack end to end through the real HTTP ingest
        // handler, not just the unit-level SigningChunker tests.
        let st = broadcast_state();
        let seed_registry = st.broadcasts.as_ref().unwrap().seed_registry.clone();
        let registry = st.broadcasts.as_ref().unwrap().registry.clone();
        let app = router(st);

        const TS_PACKET_LEN: usize = 188;
        let payload_capacity = (PIECE_LENGTH - 96) as usize; // 96 = 768-bit RSA signature_len
        let packets_needed = payload_capacity.div_ceil(TS_PACKET_LEN) + 1;
        let mut body = Vec::with_capacity(packets_needed * TS_PACKET_LEN);
        for i in 0..packets_needed {
            body.push(0x47);
            body.extend(std::iter::repeat_n((i % 256) as u8, TS_PACKET_LEN - 1));
        }
        let resp = app
            .oneshot(
                Request::put("/broadcast/bigchan")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let respbody = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&respbody).unwrap();
        let infohash_hex = json["infohash"].as_str().unwrap().to_string();
        let mut infohash = [0u8; 20];
        for i in 0..20 {
            infohash[i] = u8::from_str_radix(&infohash_hex[i * 2..i * 2 + 2], 16).unwrap();
        }

        // Wait for piece 0 to fully complete (all chunks present).
        for _ in 0..500 {
            if seed_registry
                .get(&infohash)
                .unwrap()
                .lock()
                .await
                .has_piece(0)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let store = seed_registry.get(&infohash).unwrap();
        let guard = store.lock().await;
        if !guard.has_piece(0) {
            let window = guard.window();
            panic!("piece 0 never completed; store window = {window:?}");
        }
        let piece_header = guard
            .piece_header(0)
            .expect("ingest records a live piece header");
        assert_ne!(
            piece_header, [0u8; 8],
            "broadcast-originated pieces must not serve the old zero header placeholder"
        );
        assert!(
            f64::from_be_bytes(piece_header) > 1_700_000_000.0,
            "live piece header should decode as a modern Unix timestamp"
        );
        let chunks_per_piece = guard.chunks_per_piece();
        let mut piece_bytes = Vec::with_capacity(PIECE_LENGTH as usize);
        for c in 0..chunks_per_piece {
            piece_bytes.extend_from_slice(&guard.chunk(0, c).unwrap());
        }
        drop(guard);

        let bc = registry.get("bigchan").await.unwrap();
        let sig_len = bc.auth.signature_len();
        let (payload, sig) = ace_wire::live_auth::split_piece(&piece_bytes, sig_len).unwrap();
        assert!(
            ace_wire::live_auth::verify_piece(&bc.auth.pubkey_der(), payload, sig),
            "the ingested piece's embedded signature must verify against the broadcast's own pubkey"
        );
    }

    #[test]
    fn broadcast_name_validation_rejects_traversal_and_junk() {
        assert!(valid_broadcast_name("news"));
        assert!(valid_broadcast_name("sports-2.hd_1"));
        assert!(!valid_broadcast_name(""));
        assert!(!valid_broadcast_name("."));
        assert!(!valid_broadcast_name(".."));
        assert!(!valid_broadcast_name("../etc/passwd"));
        assert!(!valid_broadcast_name("a/b"));
        assert!(!valid_broadcast_name("has space"));
        assert!(!valid_broadcast_name(&"x".repeat(65)));
    }

    #[tokio::test]
    async fn broadcast_put_and_delete_reject_invalid_names_with_400() {
        let app = router(broadcast_state());
        let put = app
            .clone()
            .oneshot(
                Request::put("/broadcast/bad%2Fname")
                    .body(Body::from(vec![0x47u8; 8]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put.status(), StatusCode::BAD_REQUEST);
        let del = app
            .oneshot(
                Request::delete("/broadcast/bad%2Fname")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(del.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn broadcast_delete_purges_and_is_idempotent() {
        let st = broadcast_state();
        let seed_registry = st.broadcasts.as_ref().unwrap().seed_registry.clone();
        let app = router(st);

        // Mint it.
        let put = app
            .clone()
            .oneshot(
                Request::put("/broadcast/gone")
                    .body(Body::from(vec![0x47u8; 8]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put.status(), StatusCode::OK);
        let body = axum::body::to_bytes(put.into_body(), 1 << 20)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let ih_hex = json["infohash"].as_str().unwrap();
        let mut infohash = [0u8; 20];
        for i in 0..20 {
            infohash[i] = u8::from_str_radix(&ih_hex[i * 2..i * 2 + 2], 16).unwrap();
        }
        assert!(seed_registry.serves(&infohash));

        // Delete it -> 204, no longer served, GET 404s.
        let del = app
            .clone()
            .oneshot(
                Request::delete("/broadcast/gone")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(del.status(), StatusCode::NO_CONTENT);
        assert!(
            !seed_registry.serves(&infohash),
            "no longer served after delete"
        );
        let get = app
            .clone()
            .oneshot(Request::get("/broadcast/gone").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(get.status(), StatusCode::NOT_FOUND);

        // Deleting again is idempotent.
        let del2 = app
            .oneshot(
                Request::delete("/broadcast/gone")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(del2.status(), StatusCode::NO_CONTENT);
    }

    async fn server_api_json(state: AppState, query: &str) -> (StatusCode, serde_json::Value) {
        let resp = router(state)
            .oneshot(
                Request::get(format!("/server/api?{query}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let json = if body.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&body).unwrap()
        };
        (status, json)
    }

    #[tokio::test]
    async fn server_api_get_version_returns_the_result_envelope() {
        let (status, json) = server_api_json(ace_compat_state(0), "method=get_version").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["error"], serde_json::Value::Null);
        assert_eq!(json["result"]["version"], env!("CARGO_PKG_VERSION"));
    }

    #[tokio::test]
    async fn server_api_analyze_content_echoes_a_direct_infohash_lowercased() {
        let upper = "50E93529D3EB46A50506B14464185A15292D6E47";
        let (status, json) = server_api_json(
            ace_compat_state(0),
            &format!("method=analyze_content&infohash={upper}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["error"], serde_json::Value::Null);
        assert_eq!(json["result"]["infohash"], upper.to_lowercase());
        assert_eq!(json["result"]["is_live"], 1);
    }

    #[tokio::test]
    async fn server_api_get_content_id_echoes_the_supplied_content_id() {
        let (_, json) = server_api_json(
            ace_compat_state(0),
            "method=get_content_id&content_id=abc123",
        )
        .await;
        assert_eq!(json["error"], serde_json::Value::Null);
        assert_eq!(json["result"]["content_id"], "abc123");
    }

    #[tokio::test]
    async fn server_api_get_content_id_errors_when_it_cannot_be_derived() {
        let (status, json) = server_api_json(
            ace_compat_state(0),
            "method=get_content_id&infohash=50e93529d3eb46a50506b14464185a15292d6e47",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["result"], serde_json::Value::Null);
        assert!(json["error"].is_string());
    }

    #[tokio::test]
    async fn server_api_get_media_files_lists_the_infohash_file() {
        let ih = "aa".repeat(20);
        let (_, json) = server_api_json(
            ace_compat_state(0),
            &format!("method=get_media_files&infohash={ih}"),
        )
        .await;
        assert_eq!(json["error"], serde_json::Value::Null);
        assert_eq!(json["result"]["files"][0]["infohash"], ih);
    }

    #[tokio::test]
    async fn server_api_get_network_connection_status_reports_connected() {
        let (_, json) =
            server_api_json(ace_compat_state(0), "method=get_network_connection_status").await;
        assert_eq!(json["error"], serde_json::Value::Null);
        assert_eq!(json["result"]["connected"], true);
    }

    #[tokio::test]
    async fn server_api_unknown_method_returns_error_with_http_200() {
        let (status, json) =
            server_api_json(ace_compat_state(0), "method=get_available_channels").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["result"], serde_json::Value::Null);
        assert!(json["error"].as_str().unwrap().contains("unknown method"));
    }

    #[tokio::test]
    async fn server_api_analyze_content_without_a_selector_errors() {
        let (_, json) = server_api_json(ace_compat_state(0), "method=analyze_content").await;
        assert_eq!(json["result"], serde_json::Value::Null);
        assert!(json["error"].is_string());
    }

    #[tokio::test]
    async fn server_api_content_id_resolution_errors_when_catalog_lookup_is_disabled() {
        // ace_compat_state keeps resolve_content_ids_in_getstream = false, so a content_id that
        // needs a live catalog lookup returns a structured error instead of touching the network.
        let (status, json) = server_api_json(
            ace_compat_state(0),
            "method=analyze_content&content_id=2123456789abcdef0123456789abcdef01234567",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["result"], serde_json::Value::Null);
        assert!(json["error"].is_string());
    }

    #[tokio::test]
    async fn server_api_is_gated_by_experimental_ace_compat() {
        // fixture_state leaves the compat surface off, so /server/api is not routed at all.
        let (status, _) = server_api_json(fixture_state(0), "method=get_version").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
}
