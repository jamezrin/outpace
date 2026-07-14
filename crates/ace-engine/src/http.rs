//! HTTP API (axum): the clean `/streams`/`/broadcast` surface plus the official-engine
//! compatibility `/ace/...` playback surface.

use crate::broadcast::BroadcastRegistry;
use crate::manager::StreamManager;
use crate::provider::{ProviderError, VodByteSource, VodContent};
use crate::session::{StreamEvent, StreamSession, Subscription};
use ace_swarm::listen::SeedRegistry;
use ace_swarm::resolve::{infohash_hex, resolve_via_catalog, stream_info_from_transport_url};
use ace_swarm::types::StreamMetadata;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{routing::get, routing::put, Json, Router};
use bytes::Bytes;
use futures::StreamExt;
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::broadcast::error::RecvError;

const ACE_TOKEN_TTL: Duration = Duration::from_secs(6 * 60 * 60);
const ACE_TOKEN_CAPACITY: usize = 4096;
const MAX_ICY_NAME_BYTES: usize = 256;

fn icy_name_header(metadata: &StreamMetadata) -> Option<HeaderValue> {
    let title = metadata.title.as_deref()?.trim();
    let sanitized: String = title.chars().filter(|ch| !ch.is_control()).collect();
    let sanitized = sanitized.trim();
    if sanitized.is_empty() {
        return None;
    }
    let mut end = sanitized.len().min(MAX_ICY_NAME_BYTES);
    while !sanitized.is_char_boundary(end) {
        end -= 1;
    }
    HeaderValue::from_bytes(&sanitized.as_bytes()[..end]).ok()
}

fn stream_metadata_json(metadata: &StreamMetadata) -> serde_json::Value {
    json!({
        "title": metadata.title,
        "bitrate": metadata.bitrate,
        "categories": metadata.categories,
    })
}

struct AceLease {
    playback_id: String,
    session_key: String,
    expires_at: Instant,
    cancel: tokio::sync::watch::Sender<bool>,
    /// An HLS client has no long-lived response body to own its subscriber count. Keep one
    /// receiver in the bounded lease instead, so the manager reaper sees the client until stop,
    /// expiry, or capacity eviction. The receiver is never read; the packager uses its own raw
    /// receiver and remains the sole segmenting pipeline.
    _hls_pin: Option<Subscription>,
}

/// Owns a hidden direct-playback lease for exactly as long as its HTTP body exists.
struct AceDirectLeaseGuard {
    sessions: Arc<AceSessionStore>,
    playback_id: String,
    token: String,
}

impl Drop for AceDirectLeaseGuard {
    fn drop(&mut self) {
        self.sessions.remove_owned(&self.playback_id, &self.token);
    }
}

/// Bounded playback leases minted by `/ace/getstream`. A token is both authorization and the
/// compatibility client's ownership handle; revoking one never force-stops the shared source.
pub struct AceSessionStore {
    leases: Mutex<HashMap<String, AceLease>>,
    ttl: Duration,
    capacity: usize,
    /// A single weak-reference reaper handles every HLS deadline for this store. Mutations bump
    /// the watch generation so capacity eviction, stop, and revoke cancel its current sleep and
    /// make it recalculate the next live deadline.
    hls_reaper_signal: tokio::sync::watch::Sender<u64>,
    hls_reaper_started: AtomicBool,
    hls_reaper_workers: Arc<AtomicUsize>,
}

struct HlsReaperGuard(Arc<AtomicUsize>);

impl Drop for HlsReaperGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Default for AceSessionStore {
    fn default() -> Self {
        Self::new(ACE_TOKEN_TTL, ACE_TOKEN_CAPACITY)
    }
}

impl AceSessionStore {
    fn new(ttl: Duration, capacity: usize) -> Self {
        let (hls_reaper_signal, _) = tokio::sync::watch::channel(0);
        Self {
            leases: Mutex::new(HashMap::new()),
            ttl,
            capacity,
            hls_reaper_signal,
            hls_reaper_started: AtomicBool::new(false),
            hls_reaper_workers: Arc::new(AtomicUsize::new(0)),
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

    fn mint_hls(
        self: &Arc<Self>,
        playback_id: String,
        session_key: String,
        pin: Subscription,
    ) -> String {
        use rand::RngCore;
        let mut bytes = [0_u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        let token = hex::encode(bytes);
        self.insert_hls_at(token.clone(), playback_id, session_key, Instant::now(), pin);
        self.ensure_hls_reaper();
        self.wake_hls_reaper();
        token
    }

    fn ensure_hls_reaper(self: &Arc<Self>) {
        if self
            .hls_reaper_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let weak_store = Arc::downgrade(self);
        let mut signal = self.hls_reaper_signal.subscribe();
        let workers = self.hls_reaper_workers.clone();
        workers.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move {
            let _guard = HlsReaperGuard(workers);
            loop {
                let Some(store) = weak_store.upgrade() else {
                    break;
                };
                let deadline = store.next_hls_expiry();
                drop(store);

                match deadline {
                    Some(deadline) => {
                        tokio::select! {
                            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                                if let Some(store) = weak_store.upgrade() {
                                    store.purge_expired(Instant::now());
                                } else {
                                    break;
                                }
                            }
                            changed = signal.changed() => {
                                if changed.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    None => {
                        if signal.changed().await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
    }

    fn next_hls_expiry(&self) -> Option<Instant> {
        self.leases
            .lock()
            .unwrap()
            .values()
            .filter(|lease| lease._hls_pin.is_some())
            .map(|lease| lease.expires_at)
            .min()
    }

    fn wake_hls_reaper(&self) {
        self.hls_reaper_signal
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }

    fn purge_expired(&self, now: Instant) {
        let mut leases = self.leases.lock().unwrap();
        leases.retain(|_, lease| {
            let keep = lease.expires_at > now;
            if !keep {
                let _ = lease.cancel.send(true);
            }
            keep
        });
    }

    fn insert_at(&self, token: String, playback_id: String, session_key: String, now: Instant) {
        self.insert_with_pin(token, playback_id, session_key, now, None);
    }

    fn insert_with_pin(
        &self,
        token: String,
        playback_id: String,
        session_key: String,
        now: Instant,
        hls_pin: Option<Subscription>,
    ) {
        let mut hls_schedule_changed = hls_pin.is_some();
        self.purge_expired(now);
        let mut leases = self.leases.lock().unwrap();
        if leases.len() >= self.capacity {
            if let Some(oldest) = leases
                .iter()
                .min_by_key(|(_, lease)| lease.expires_at)
                .map(|(t, _)| t.clone())
            {
                if let Some(lease) = leases.remove(&oldest) {
                    hls_schedule_changed |= lease._hls_pin.is_some();
                    let _ = lease.cancel.send(true);
                }
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
                _hls_pin: hls_pin,
            },
        );
        drop(leases);
        if hls_schedule_changed {
            self.wake_hls_reaper();
        }
    }

    fn insert_hls_at(
        &self,
        token: String,
        playback_id: String,
        session_key: String,
        now: Instant,
        pin: Subscription,
    ) {
        self.insert_with_pin(token, playback_id, session_key, now, Some(pin));
    }

    fn validate(&self, playback_id: &str, token: &str) -> Option<String> {
        self.validate_at(playback_id, token, Instant::now())
    }

    fn validate_token(&self, token: &str) -> Option<(String, String)> {
        let now = Instant::now();
        self.purge_expired(now);
        self.leases
            .lock()
            .unwrap()
            .get(token)
            .map(|lease| (lease.playback_id.clone(), lease.session_key.clone()))
    }

    fn validate_at(&self, playback_id: &str, token: &str, now: Instant) -> Option<String> {
        self.purge_expired(now);
        self.leases
            .lock()
            .unwrap()
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
        drop(leases);
        if matches {
            self.wake_hls_reaper();
        }
        matches
    }

    /// Remove a lease owned by an internal body guard even if its authorization deadline has
    /// just passed. Unlike `revoke`, this is not an authorization check: the guard was created
    /// with the exact id/token pair when the lease was minted and must always clean up its entry.
    fn remove_owned(&self, playback_id: &str, token: &str) -> bool {
        let mut leases = self.leases.lock().unwrap();
        let matches = leases
            .get(token)
            .is_some_and(|lease| lease.playback_id == playback_id);
        if matches {
            if let Some(lease) = leases.remove(token) {
                let _ = lease.cancel.send(true);
            }
        }
        drop(leases);
        if matches {
            self.wake_hls_reaper();
        }
        matches
    }

    fn playback(
        &self,
        playback_id: &str,
        token: &str,
    ) -> Option<(String, Instant, tokio::sync::watch::Receiver<bool>)> {
        let now = Instant::now();
        self.purge_expired(now);
        self.leases
            .lock()
            .unwrap()
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

    #[cfg(test)]
    fn active_count(&self) -> usize {
        self.leases.lock().unwrap().len()
    }

    #[cfg(test)]
    fn hls_reaper_workers(&self) -> Arc<AtomicUsize> {
        self.hls_reaper_workers.clone()
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
            .route("/ace/manifest.m3u8", get(ace_manifest))
            .route("/ace/m/:id/:manifest", get(ace_hls_playback))
            .route("/ace/c/:session/:segment", get(ace_hls_segment))
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
    if !crate::broadcast::valid_broadcast_name(&name) {
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
    if !crate::broadcast::valid_broadcast_name(&name) {
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
async fn ace_manifest(
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
async fn ace_hls_playback(
    State(s): State<AppState>,
    Path((id, manifest)): Path<(String, String)>,
) -> Response {
    let Some(token) = manifest.strip_suffix(".m3u8") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(session_key) = s.ace_sessions.validate(&id, token) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(network) = ace_network(&s) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(pkg) = s.manager.get_hls(&network, &session_key).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
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
async fn ace_hls_segment(
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
        Ok(session) => ace_stream_session_response(session, expires_at, cancel, None),
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

#[derive(Debug, PartialEq, Eq)]
struct AceStreamSelection {
    public_id: String,
    playback_id: String,
    session_key: String,
    content_id: Option<String>,
    metadata: StreamMetadata,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AceGetstreamMode {
    Direct,
    Json,
}

impl AceStreamSelection {
    fn with_resolved_stream(mut self, infohash: String, metadata: StreamMetadata) -> Self {
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
) -> Result<AceStreamSelection, &'static str> {
    let mut selection = ace_selected_stream(params)?;
    if s.resolve_content_ids_in_getstream {
        if let Some(content_id) = selection.content_id.as_deref() {
            match resolve_via_catalog(content_id).await {
                Ok(info) => {
                    selection =
                        selection.with_resolved_stream(infohash_hex(&info.infohash), info.metadata);
                }
                Err(e) => crate::alog!(
                    "[ace] content-id catalog resolution failed, falling back to cid: {e:?}"
                ),
            }
        }
    }
    Ok(selection)
}

fn ace_selected_stream(
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

fn ace_normalized_hex_id(id: &str) -> Option<String> {
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
        .map(|(network, id, clients, metadata)| {
            json!({
                "network": network,
                "id": id,
                "clients": clients,
                "metadata": stream_metadata_json(&metadata),
            })
        })
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
                "metadata": stream_metadata_json(session.metadata()),
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

/// `GET /streams/{network}/{id}` or `{id}.ts` (continuous MPEG-TS), or `.m3u8` (live HLS
/// playlist).
async fn stream_file(
    State(s): State<AppState>,
    Path((network, file)): Path<(String, String)>,
) -> Response {
    if let Some(id) = file.strip_suffix(".m3u8") {
        return match s.manager.get_or_start_hls(&network, id).await {
            Ok(pkg) => {
                let icy_name = s
                    .manager
                    .get(&network, id)
                    .await
                    .and_then(|session| icy_name_header(session.metadata()));
                let mut response = (
                    [(header::CONTENT_TYPE, "application/vnd.apple.mpegurl")],
                    pkg.playlist(&network, id),
                )
                    .into_response();
                if let Some(value) = icy_name {
                    response.headers_mut().insert("icy-name", value);
                }
                response
            }
            Err(_) => StatusCode::NOT_FOUND.into_response(),
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
    let session = match s.manager.get_or_start(&network, id).await {
        Ok(sess) => sess,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    stream_session_response(session)
}

fn stream_session_response(session: Arc<StreamSession>) -> Response {
    let icy_name = icy_name_header(session.metadata());
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
    let mut response = Response::builder().header(header::CONTENT_TYPE, "video/mp2t");
    if let Some(value) = icy_name {
        response = response.header("icy-name", value);
    }
    response.body(Body::from_stream(stream)).unwrap()
}

fn ace_stream_session_response(
    session: Arc<StreamSession>,
    expires_at: Instant,
    cancel: tokio::sync::watch::Receiver<bool>,
    direct_lease: Option<AceDirectLeaseGuard>,
) -> Response {
    let icy_name = icy_name_header(session.metadata());
    let sub = session.subscribe();
    let gate = ace_media::mpegts::KeyframeGate::new();
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
    let mut response = Response::builder().header(header::CONTENT_TYPE, "video/mp2t");
    if let Some(value) = icy_name {
        response = response.header("icy-name", value);
    }
    response.body(Body::from_stream(stream)).unwrap()
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
    use ace_swarm::types::StreamMetadata;
    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use bytes::Bytes;
    use tower::ServiceExt;

    #[test]
    fn icy_name_header_sanitizes_and_bounds_titles() {
        let metadata = StreamMetadata {
            title: Some("  Synthetic Demo\r\n\0 Channel  ".to_string()),
            bitrate: None,
            categories: vec![],
        };
        assert_eq!(
            icy_name_header(&metadata).unwrap().as_bytes(),
            b"Synthetic Demo Channel"
        );

        let metadata = StreamMetadata {
            title: Some("é".repeat(200)),
            bitrate: None,
            categories: vec![],
        };
        let value = icy_name_header(&metadata).unwrap();
        assert!(value.as_bytes().len() <= 256);
        assert!(std::str::from_utf8(value.as_bytes()).is_ok());
    }

    #[test]
    fn icy_name_header_omits_empty_titles() {
        assert!(icy_name_header(&StreamMetadata::default()).is_none());
        assert!(icy_name_header(&StreamMetadata {
            title: Some(" \r\n\t ".to_string()),
            bitrate: None,
            categories: vec![],
        })
        .is_none());
    }

    #[test]
    fn stream_metadata_json_has_stable_shape() {
        assert_eq!(
            stream_metadata_json(&StreamMetadata::default()),
            json!({ "title": null, "bitrate": null, "categories": [] })
        );
        assert_eq!(
            stream_metadata_json(&StreamMetadata {
                title: Some("Synthetic Demo Channel".to_string()),
                bitrate: Some(100_000),
                categories: vec!["sports".to_string()],
            }),
            json!({
                "title": "Synthetic Demo Channel",
                "bitrate": 100_000,
                "categories": ["sports"],
            })
        );
    }

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
        // 1500 bytes over 564-byte (3-packet) segments => 3 segments.
        let resp = memvod_hls_router(vec![0u8; 1500], 3)
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
        let data: Vec<u8> = (0..255u8).cycle().take(1500).collect();
        // Segment 1 covers bytes [564, 1128).
        let resp = memvod_hls_router(data.clone(), 3)
            .oneshot(
                Request::get("/vod/memvod/x/seg/1.ts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()[header::CONTENT_TYPE], "video/mp2t");
        assert_eq!(resp.headers()[header::CONTENT_LENGTH], "564");
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        assert_eq!(&body[..], &data[564..1128]);
    }

    #[tokio::test]
    async fn vod_hls_final_segment_is_clamped_to_the_last_byte() {
        let data: Vec<u8> = (0..255u8).cycle().take(1500).collect();
        // 1500 bytes => final segment 2 covers [1128, 1500): 372 bytes.
        let resp = memvod_hls_router(data.clone(), 3)
            .oneshot(
                Request::get("/vod/memvod/x/seg/2.ts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()[header::CONTENT_LENGTH], "372");
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        assert_eq!(&body[..], &data[1128..1500]);
    }

    #[tokio::test]
    async fn vod_hls_segment_past_the_end_404s() {
        let resp = memvod_hls_router(vec![0u8; 1500], 3)
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
            ("id", "fedcba9876543210fedcba9876543210fedcba98"),
            ("url", "https://e/x"),
            ("magnet", &format!("magnet:?xt=urn:btih:{ih}")),
        ]))
        .unwrap();
        assert_eq!(sel.session_key, format!("cid:{cid}"));

        let sel = ace_selected_stream(&params(&[
            ("infohash", ih),
            ("id", "fedcba9876543210fedcba9876543210fedcba98"),
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
    fn getstream_legacy_id_uses_content_id_resolver_key() {
        let id = "89ABCDEF0123456789ABCDEF0123456789ABCDEF";
        let sel = ace_selected_stream(&params(&[("id", id)])).unwrap();
        let normalized = id.to_ascii_lowercase();
        assert_eq!(sel.public_id, normalized);
        assert_eq!(sel.playback_id, format!("cid:{normalized}"));
        assert_eq!(sel.session_key, format!("cid:{normalized}"));
        assert_eq!(sel.content_id.as_deref(), Some(normalized.as_str()));
    }

    #[test]
    fn getstream_infohash_remains_a_direct_swarm_key() {
        let infohash = "89ABCDEF0123456789ABCDEF0123456789ABCDEF";
        let sel = ace_selected_stream(&params(&[("infohash", infohash)])).unwrap();
        let normalized = infohash.to_ascii_lowercase();
        assert_eq!(sel.playback_id, normalized);
        assert_eq!(sel.session_key, normalized);
        assert!(sel.content_id.is_none());
    }

    #[test]
    fn getstream_rejects_bad_url_and_magnet() {
        assert!(ace_selected_stream(&params(&[("url", "file:///etc/passwd")])).is_err());
        assert!(ace_selected_stream(&params(&[("magnet", "magnet:?dn=noxt")])).is_err());
    }

    #[test]
    fn getstream_rejects_malformed_hash_selectors_without_falling_through() {
        assert_eq!(
            ace_selected_stream(&params(&[("id", "short")])),
            Err("invalid id")
        );
        assert_eq!(
            ace_selected_stream(&params(&[("infohash", "not-hex")])),
            Err("invalid infohash")
        );
        assert_eq!(
            ace_selected_stream(&params(&[
                ("content_id", "bad"),
                ("infohash", "0123456789abcdef0123456789abcdef01234567",),
            ])),
            Err("invalid content_id")
        );
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
    fn fixture_metadata() -> StreamMetadata {
        StreamMetadata {
            title: Some("Synthetic Demo Channel".to_string()),
            bitrate: Some(100_000),
            categories: vec!["sports".to_string()],
        }
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
        fn metadata(&self) -> StreamMetadata {
            fixture_metadata()
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
        fn metadata(&self) -> StreamMetadata {
            fixture_metadata()
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

    struct PacedPacketProvider;
    struct PacedPacketSource {
        packet_index: u64,
    }

    fn paced_psi(pid: u16, section: &[u8]) -> Vec<u8> {
        let mut packet = vec![0xff; 188];
        packet[0] = 0x47;
        packet[1] = 0x40 | ((pid >> 8) as u8 & 0x1f);
        packet[2] = pid as u8;
        packet[3] = 0x10;
        packet[4] = 0;
        packet[5..5 + section.len()].copy_from_slice(section);
        packet
    }

    fn paced_pat(pmt_pid: u16) -> Vec<u8> {
        let mut section = vec![0x00, 0xb0, 13, 0x00, 0x01, 0xc1, 0, 0];
        section.extend_from_slice(&[
            0x00,
            0x01,
            0xe0 | ((pmt_pid >> 8) as u8 & 0x1f),
            pmt_pid as u8,
        ]);
        section.extend_from_slice(&[0; 4]);
        paced_psi(0, &section)
    }

    fn paced_pmt(pmt_pid: u16, video_pid: u16) -> Vec<u8> {
        let mut section = vec![0x02, 0xb0, 18, 0x00, 0x01, 0xc1, 0, 0];
        section.extend_from_slice(&[
            0xe0 | ((video_pid >> 8) as u8 & 0x1f),
            video_pid as u8,
            0xf0,
            0,
            0x1b,
            0xe0 | ((video_pid >> 8) as u8 & 0x1f),
            video_pid as u8,
            0xf0,
            0,
        ]);
        section.extend_from_slice(&[0; 4]);
        paced_psi(pmt_pid, &section)
    }

    fn paced_video_access(video_pid: u16, pcr: u64, marker: u8) -> Vec<u8> {
        let mut packet = vec![0xff; 188];
        packet[0] = 0x47;
        packet[1] = 0x40 | ((video_pid >> 8) as u8 & 0x1f);
        packet[2] = video_pid as u8;
        packet[3] = 0x30;
        packet[4] = 7;
        packet[5] = 0x50;
        packet[6] = (pcr >> 25) as u8;
        packet[7] = (pcr >> 17) as u8;
        packet[8] = (pcr >> 9) as u8;
        packet[9] = (pcr >> 1) as u8;
        packet[10] = ((pcr & 1) << 7) as u8 | 0x7e;
        packet[11] = 0;
        packet[12] = marker;
        packet
    }

    #[async_trait]
    impl TsSource for PacedPacketSource {
        async fn next(&mut self) -> Option<Bytes> {
            tokio::time::sleep(Duration::from_millis(2)).await;
            const PMT_PID: u16 = 0x0100;
            const VIDEO_PID: u16 = 0x0101;
            let packet = match self.packet_index {
                0 => paced_pat(PMT_PID),
                1 => paced_pmt(PMT_PID, VIDEO_PID),
                index => paced_video_access(
                    VIDEO_PID,
                    (index - 2) * 108_000,
                    index.wrapping_sub(2) as u8,
                ),
            };
            self.packet_index += 1;
            Some(Bytes::from(packet))
        }
        fn stats(&self) -> SourceStats {
            SourceStats::default()
        }
        fn metadata(&self) -> StreamMetadata {
            fixture_metadata()
        }
    }
    #[async_trait]
    impl StreamProvider for PacedPacketProvider {
        fn network(&self) -> &'static str {
            "hlsfix"
        }
        async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
            Ok(Box::new(PacedPacketSource { packet_index: 0 }))
        }
    }

    fn ace_hls_state(ttl: Duration, capacity: usize) -> AppState {
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(PacedPacketProvider));
        AppState {
            manager: StreamManager::with_config(
                registry,
                32,
                crate::config::HlsConfig {
                    segment_packets: 3,
                    window_segments: 3,
                    segment_duration_ms: 1000,
                },
            ),
            networks: vec!["hlsfix".into()],
            resolve_content_ids_in_getstream: false,
            ace_sessions: Arc::new(AceSessionStore::new(ttl, capacity)),
            experimental_ace_compat: true,
            broadcasts: None,
        }
    }

    async fn response_bytes(response: Response) -> Bytes {
        axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap()
    }

    async fn response_text(response: Response) -> String {
        let body = response_bytes(response).await;
        String::from_utf8(body.to_vec()).unwrap()
    }

    async fn wait_for_hls_playlist(app: &Router, path: &str) -> String {
        for _ in 0..50 {
            let response = app
                .clone()
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let playlist = response_text(response).await;
            if playlist.contains(".ts") {
                return playlist;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        panic!("HLS packager did not produce a segment");
    }

    fn playlist_segment_path(playlist: &str) -> &str {
        playlist
            .lines()
            .find(|line| line.ends_with(".ts"))
            .expect("playlist segment")
    }

    #[test]
    fn content_id_selection_uses_resolved_infohash_when_available() {
        let content_id = "2123456789abcdef0123456789abcdef01234567";
        let resolved = "50e93529d3eb46a50506b14464185a15292d6e47";
        let mut params = HashMap::new();
        params.insert("content_id".to_string(), content_id.to_string());

        let selection = ace_selected_stream(&params)
            .unwrap()
            .with_resolved_stream(resolved.to_string(), fixture_metadata());

        assert_eq!(selection.public_id, resolved);
        assert_eq!(selection.playback_id, resolved);
        assert_eq!(selection.session_key, format!("cid:{content_id}"));
        assert_eq!(selection.metadata, fixture_metadata());
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

    #[test]
    fn direct_lease_guard_removes_its_entry_after_authorization_expiry() {
        let now = Instant::now();
        let sessions = Arc::new(AceSessionStore::new(Duration::from_secs(1), 4));
        sessions.insert_at(
            "expired-token".into(),
            "same-id".into(),
            "same-key".into(),
            now - Duration::from_secs(2),
        );
        let guard = AceDirectLeaseGuard {
            sessions: sessions.clone(),
            playback_id: "same-id".into(),
            token: "expired-token".into(),
        };

        assert!(
            !sessions.revoke("same-id", "expired-token"),
            "normal revoke must retain its unexpired-authorization requirement"
        );
        assert_eq!(sessions.active_count(), 1);
        drop(guard);
        assert_eq!(
            sessions.active_count(),
            0,
            "the owner guard must remove its entry even after authorization expires"
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
        assert_eq!(resp.headers()["icy-name"], "Synthetic Demo Channel");
        // Live body is unbounded; read just the first TS chunk.
        let mut stream = resp.into_body().into_data_stream();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first[0], 0x47);
    }

    #[tokio::test]
    async fn stream_metadata_title_header_is_absent_without_metadata() {
        let resp = router(state())
            .oneshot(
                Request::get("/streams/test/somechannel.ts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(!resp.headers().contains_key("icy-name"));
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
        assert_eq!(
            json["response"]["metadata"],
            json!({ "title": null, "bitrate": null, "categories": [] })
        );

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
        assert_eq!(media.headers()["icy-name"], "Synthetic Demo Channel");
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
    async fn ace_getstream_without_format_directly_streams_legacy_id_as_mpegts() {
        use futures::StreamExt;
        let content_id = "2123456789abcdef0123456789abcdef01234567";
        let state = ace_compat_state(0);
        let manager = state.manager.clone();
        let app = router(state);

        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/ace/getstream?id={content_id}&use_api_events=1"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "video/mp2t");
        assert_eq!(response.headers()["icy-name"], "Synthetic Demo Channel");
        let mut body = response.into_body().into_data_stream();
        assert_eq!(body.next().await.unwrap().unwrap()[0], 0x47);
        assert!(manager
            .get("fix", &format!("cid:{content_id}"))
            .await
            .is_some());
        assert!(manager.get("fix", content_id).await.is_none());
    }

    #[tokio::test]
    async fn ace_direct_and_json_playback_share_source_and_disconnect_independently() {
        use futures::StreamExt;
        let infohash = "0123456789abcdef0123456789abcdef01234567";
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(PacedFixtureProvider));
        let manager = StreamManager::new(registry);
        let ace_sessions = Arc::new(AceSessionStore::default());
        let app = router(AppState {
            manager: manager.clone(),
            networks: vec!["fix".into()],
            resolve_content_ids_in_getstream: false,
            ace_sessions: ace_sessions.clone(),
            experimental_ace_compat: true,
            broadcasts: None,
        });

        let minted = app
            .clone()
            .oneshot(
                Request::get(format!("/ace/getstream?format=json&infohash={infohash}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let minted = axum::body::to_bytes(minted.into_body(), 1 << 20)
            .await
            .unwrap();
        let minted: serde_json::Value = serde_json::from_slice(&minted).unwrap();
        let playback_path = minted["response"]["playback_url"]
            .as_str()
            .unwrap()
            .strip_prefix("http://127.0.0.1:6878")
            .unwrap();
        let mut json_body = app
            .clone()
            .oneshot(Request::get(playback_path).body(Body::empty()).unwrap())
            .await
            .unwrap()
            .into_body()
            .into_data_stream();
        let mut direct_body = app
            .clone()
            .oneshot(
                Request::get(format!("/ace/getstream?infohash={infohash}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body()
            .into_data_stream();

        assert!(json_body.next().await.transpose().unwrap().is_some());
        assert!(direct_body.next().await.transpose().unwrap().is_some());
        assert_eq!(
            manager
                .get("fix", infohash)
                .await
                .unwrap()
                .subscriber_count(),
            2
        );
        assert_eq!(ace_sessions.active_count(), 2);
        drop(direct_body);
        assert_eq!(
            manager
                .get("fix", infohash)
                .await
                .unwrap()
                .subscriber_count(),
            1
        );
        assert_eq!(
            ace_sessions.active_count(),
            1,
            "disconnecting direct playback must revoke its hidden lease only"
        );
        assert!(
            json_body.next().await.transpose().unwrap().is_some(),
            "dropping the direct caller must not interrupt the JSON playback caller"
        );
    }

    #[tokio::test]
    async fn ace_getstream_errors_are_deterministic_and_do_not_start_sources() {
        async fn get_json(app: &Router, path: &str) -> serde_json::Value {
            let response = app
                .clone()
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = axum::body::to_bytes(response.into_body(), 1 << 20)
                .await
                .unwrap();
            serde_json::from_slice(&body).unwrap()
        }

        let state = ace_compat_state(0);
        let manager = state.manager.clone();
        let app = router(state);
        assert_eq!(
            get_json(&app, "/ace/getstream").await["error"],
            "missing content_id/infohash/id/url/magnet"
        );
        assert_eq!(
            get_json(
                &app,
                "/ace/getstream?content_id=bad&infohash=0123456789abcdef0123456789abcdef01234567",
            )
            .await["error"],
            "invalid content_id"
        );
        assert_eq!(
            get_json(
                &app,
                "/ace/getstream?format=xml&infohash=0123456789abcdef0123456789abcdef01234567",
            )
            .await["error"],
            "unsupported format"
        );
        assert!(manager.list().await.is_empty());
    }

    #[tokio::test]
    async fn ace_direct_start_failure_revokes_its_hidden_lease() {
        let sessions = Arc::new(AceSessionStore::default());
        let app = router(AppState {
            manager: StreamManager::new(ProviderRegistry::new()),
            networks: vec!["fix".into()],
            resolve_content_ids_in_getstream: false,
            ace_sessions: sessions.clone(),
            experimental_ace_compat: true,
            broadcasts: None,
        });
        let response = app
            .oneshot(
                Request::get("/ace/getstream?infohash=0123456789abcdef0123456789abcdef01234567")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(sessions.active_count(), 0);
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
                    Request::get(format!(
                        "/ace/getstream?format=json&content_id={content_id}"
                    ))
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
                Request::get(format!(
                    "/ace/getstream?format=json&content_id={content_id}"
                ))
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
                Request::get(format!(
                    "/ace/getstream?format=json&content_id={content_id}"
                ))
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
        assert_eq!(
            json["response"]["metadata"],
            json!({ "title": null, "bitrate": null, "categories": [] })
        );

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
        assert_eq!(
            json["response"]["metadata"],
            stream_metadata_json(&fixture_metadata())
        );
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
        let id = "0123456789abcdef0123456789abcdef01234567";
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
                    Request::get(format!("/ace/getstream?format=json&infohash={id}"))
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
    async fn ace_manifest_redirects_to_stable_authenticated_native_hls_view() {
        let id = "0123456789abcdef0123456789abcdef01234567";
        let state = ace_hls_state(Duration::from_secs(60), 8);
        let manager = state.manager.clone();
        let app = router(state);

        let start = app
            .clone()
            .oneshot(
                Request::get(format!("/ace/manifest.m3u8?infohash={id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(start.status(), StatusCode::FOUND);
        assert_eq!(start.headers()[header::CACHE_CONTROL], "no-store");
        let playback = start.headers()[header::LOCATION]
            .to_str()
            .unwrap()
            .strip_prefix("http://127.0.0.1:6878")
            .unwrap()
            .to_owned();
        assert!(playback.starts_with(&format!("/ace/m/{id}/")));

        let playlist = wait_for_hls_playlist(&app, &playback).await;
        let segment_path = playlist_segment_path(&playlist).to_owned();
        assert!(segment_path.starts_with("/ace/c/"));
        assert_eq!(segment_path.split('/').nth(3).unwrap().len(), 64);
        assert_eq!(
            manager.get("hlsfix", id).await.unwrap().subscriber_count(),
            1,
            "one HLS lease pins one native session subscriber"
        );

        let compat_segment = app
            .clone()
            .oneshot(Request::get(&segment_path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(compat_segment.status(), StatusCode::OK);
        assert_eq!(compat_segment.headers()[header::CONTENT_TYPE], "video/mp2t");
        assert_eq!(compat_segment.headers()[header::CACHE_CONTROL], "no-store");
    }

    #[tokio::test]
    async fn ace_hls_playlist_and_segment_reads_do_not_refresh_native_activity() {
        let id = "0123456789abcdef0123456789abcdef01234567";
        let state = ace_hls_state(Duration::from_secs(60), 8);
        let manager = state.manager.clone();
        let app = router(state);

        let start = app
            .clone()
            .oneshot(
                Request::get(format!("/ace/manifest.m3u8?infohash={id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let playback = start.headers()[header::LOCATION]
            .to_str()
            .unwrap()
            .strip_prefix("http://127.0.0.1:6878")
            .unwrap()
            .to_owned();
        let initial_playlist = wait_for_hls_playlist(&app, &playback).await;
        let segment_path = playlist_segment_path(&initial_playlist).to_owned();
        let pkg = manager.get_hls("hlsfix", id).await.unwrap();
        let stale = Instant::now() - Duration::from_secs(60);

        pkg.set_last_access_for_test(stale);
        let playlist = app
            .clone()
            .oneshot(Request::get(&playback).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(playlist.status(), StatusCode::OK);
        assert_eq!(pkg.last_access_for_test(), Some(stale));

        let segment = app
            .clone()
            .oneshot(Request::get(&segment_path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(segment.status(), StatusCode::OK);
        assert_eq!(pkg.last_access_for_test(), Some(stale));
    }

    #[tokio::test]
    async fn ace_manifest_json_returns_documented_hls_control_urls() {
        let id = "0123456789abcdef0123456789abcdef01234567";
        let app = router(ace_hls_state(Duration::from_secs(60), 8));
        let response = app
            .clone()
            .oneshot(
                Request::get(format!(
                    "/ace/manifest.m3u8?format=json&id={id}&use_api_events=1"
                ))
                .header(header::HOST, "localhost:6900")
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let value: serde_json::Value =
            serde_json::from_str(&response_text(response).await).unwrap();
        assert_eq!(value["error"], serde_json::Value::Null);
        assert_eq!(value["response"]["infohash"], id);
        assert_eq!(value["response"]["is_live"], 1);
        assert_eq!(
            value["response"]["metadata"],
            stream_metadata_json(&fixture_metadata())
        );
        let token = value["response"]["playback_session_id"].as_str().unwrap();
        assert_eq!(token.len(), 64);
        let playback = value["response"]["playback_url"]
            .as_str()
            .unwrap()
            .strip_prefix("http://localhost:6900")
            .unwrap();
        assert!(playback.starts_with(&format!("/ace/m/cid:{id}/")));
        assert!(playback.ends_with(".m3u8"));
        assert_eq!(
            value["response"]["stat_url"],
            format!("http://localhost:6900/ace/stat/cid:{id}/{token}")
        );
        assert_eq!(
            value["response"]["command_url"],
            format!("http://localhost:6900/ace/cmd/cid:{id}/{token}")
        );
        assert!(wait_for_hls_playlist(&app, playback)
            .await
            .contains("/ace/c/"));
    }

    #[tokio::test]
    async fn ace_and_native_hls_return_the_same_retained_segment_bytes() {
        let id = "0123456789abcdef0123456789abcdef01234567";
        let state = ace_hls_state(Duration::from_secs(60), 8);
        let manager = state.manager.clone();
        let app = router(state);

        let start = app
            .clone()
            .oneshot(
                Request::get(format!("/ace/manifest.m3u8?infohash={id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let playback = start.headers()[header::LOCATION]
            .to_str()
            .unwrap()
            .strip_prefix("http://127.0.0.1:6878")
            .unwrap();
        let playlist = wait_for_hls_playlist(&app, playback).await;
        let compat_path = playlist_segment_path(&playlist);
        let compat_packager = manager.get_hls("hlsfix", id).await.unwrap();
        let native_manifest = app
            .clone()
            .oneshot(
                Request::get(format!("/streams/hlsfix/{id}.m3u8"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(native_manifest.status(), StatusCode::OK);
        assert!(Arc::ptr_eq(
            &compat_packager,
            &manager.get_hls("hlsfix", id).await.unwrap()
        ));
        let seq = compat_path
            .rsplit('/')
            .next()
            .unwrap()
            .strip_suffix(".ts")
            .unwrap();
        let compat = app
            .clone()
            .oneshot(Request::get(compat_path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let native = app
            .clone()
            .oneshot(
                Request::get(format!("/streams/hlsfix/{id}/seg/{seq}.ts"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(compat.status(), StatusCode::OK);
        assert_eq!(native.status(), StatusCode::OK);
        assert_eq!(response_bytes(compat).await, response_bytes(native).await);
        assert_eq!(manager.list().await.len(), 1);
        assert!(manager.get_hls("hlsfix", id).await.is_some());
    }

    #[tokio::test]
    async fn ace_manifest_rejects_unknown_format_without_starting_work() {
        let id = "0123456789abcdef0123456789abcdef01234567";
        let state = ace_hls_state(Duration::from_secs(60), 8);
        let manager = state.manager.clone();
        let app = router(state);
        let response = app
            .oneshot(
                Request::get(format!(
                    "/ace/manifest.m3u8?format=mp4&infohash={id}&transcode_audio=1"
                ))
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let value: serde_json::Value =
            serde_json::from_str(&response_text(response).await).unwrap();
        assert_eq!(value["response"], serde_json::Value::Null);
        assert_eq!(value["error"], "unsupported format");
        assert!(manager.list().await.is_empty());
        assert!(manager.get_hls("hlsfix", id).await.is_none());
    }

    #[tokio::test]
    async fn ace_hls_stop_revokes_only_one_client_pin() {
        let id = "0123456789abcdef0123456789abcdef01234567";
        let state = ace_hls_state(Duration::from_secs(60), 8);
        let manager = state.manager.clone();
        let app = router(state);

        async fn mint(app: &Router, id: &str) -> serde_json::Value {
            let response = app
                .clone()
                .oneshot(
                    Request::get(format!("/ace/manifest.m3u8?format=json&infohash={id}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            serde_json::from_str(&response_text(response).await).unwrap()
        }
        let a = mint(&app, id).await;
        let b = mint(&app, id).await;
        assert_eq!(
            manager.get("hlsfix", id).await.unwrap().subscriber_count(),
            2
        );

        let stop_a = a["response"]["command_url"]
            .as_str()
            .unwrap()
            .strip_prefix("http://127.0.0.1:6878")
            .unwrap();
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
        assert_eq!(
            manager.get("hlsfix", id).await.unwrap().subscriber_count(),
            1
        );

        let a_playback = a["response"]["playback_url"]
            .as_str()
            .unwrap()
            .strip_prefix("http://127.0.0.1:6878")
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(Request::get(a_playback).body(Body::empty()).unwrap())
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        let b_playback = b["response"]["playback_url"]
            .as_str()
            .unwrap()
            .strip_prefix("http://127.0.0.1:6878")
            .unwrap();
        assert!(wait_for_hls_playlist(&app, b_playback)
            .await
            .contains(".ts"));
    }

    #[tokio::test]
    async fn ace_hls_churn_keeps_one_reaper_and_capacity_bounded_pins() {
        let id = "0123456789abcdef0123456789abcdef01234567";
        let state = ace_hls_state(Duration::from_secs(60), 4);
        let manager = state.manager.clone();
        let sessions = state.ace_sessions.clone();
        let workers = sessions.hls_reaper_workers();
        let app = router(state);
        let mut tokens = Vec::new();

        for _ in 0..100 {
            let response = app
                .clone()
                .oneshot(
                    Request::get(format!("/ace/manifest.m3u8?infohash={id}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FOUND);
            let token = response.headers()[header::LOCATION]
                .to_str()
                .unwrap()
                .rsplit('/')
                .next()
                .unwrap()
                .strip_suffix(".m3u8")
                .unwrap()
                .to_owned();
            tokens.push(token);
        }

        assert_eq!(workers.load(Ordering::SeqCst), 1);
        assert_eq!(sessions.active_count(), 4);
        assert_eq!(
            manager.get("hlsfix", id).await.unwrap().subscriber_count(),
            4
        );
        for token in tokens.iter().rev().take(4) {
            assert!(sessions.revoke(id, token));
        }
        assert_eq!(sessions.active_count(), 0);
        assert_eq!(
            manager.get("hlsfix", id).await.unwrap().subscriber_count(),
            0
        );

        drop(app);
        drop(sessions);
        for _ in 0..20 {
            if workers.load(Ordering::SeqCst) == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            workers.load(Ordering::SeqCst),
            0,
            "the weak-reference store reaper must exit when its store is dropped"
        );
    }

    #[tokio::test]
    async fn ace_hls_rejects_forged_future_evicted_and_expired_segments() {
        let id = "0123456789abcdef0123456789abcdef01234567";
        let state = ace_hls_state(Duration::from_millis(30), 1);
        let manager = state.manager.clone();
        let app = router(state);

        let first = app
            .clone()
            .oneshot(
                Request::get(format!("/ace/manifest.m3u8?infohash={id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let first_playback = first.headers()[header::LOCATION]
            .to_str()
            .unwrap()
            .strip_prefix("http://127.0.0.1:6878")
            .unwrap()
            .to_owned();
        let first_playlist = wait_for_hls_playlist(&app, &first_playback).await;
        let first_segment = playlist_segment_path(&first_playlist).to_owned();
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::get("/ace/c/ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff/0.ts")
                        .body(Body::empty())
                        .unwrap()
                )
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        let token = first_segment.split('/').nth(3).unwrap();
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::get(format!("/ace/c/{token}/18446744073709551615.ts"))
                        .body(Body::empty())
                        .unwrap()
                )
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );

        // Capacity one: a second client evicts and drops the first client's pin.
        let second = app
            .clone()
            .oneshot(
                Request::get(format!("/ace/manifest.m3u8?infohash={id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::FOUND);
        assert_eq!(
            manager.get("hlsfix", id).await.unwrap().subscriber_count(),
            1
        );
        assert_eq!(
            app.clone()
                .oneshot(Request::get(&first_segment).body(Body::empty()).unwrap())
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );

        let second_playback = second.headers()[header::LOCATION]
            .to_str()
            .unwrap()
            .strip_prefix("http://127.0.0.1:6878")
            .unwrap()
            .to_owned();
        let second_playlist = wait_for_hls_playlist(&app, &second_playback).await;
        let second_segment = playlist_segment_path(&second_playlist).to_owned();

        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(
            app.clone()
                .oneshot(Request::get(&second_playback).body(Body::empty()).unwrap())
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            app.clone()
                .oneshot(Request::get(&second_segment).body(Body::empty()).unwrap())
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            manager.get("hlsfix", id).await.unwrap().subscriber_count(),
            0
        );
    }

    #[tokio::test]
    async fn ace_hls_returns_404_after_a_segment_leaves_the_native_window() {
        let id = "0123456789abcdef0123456789abcdef01234567";
        let app = router(ace_hls_state(Duration::from_secs(60), 8));
        let start = app
            .clone()
            .oneshot(
                Request::get(format!("/ace/manifest.m3u8?infohash={id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let playback = start.headers()[header::LOCATION]
            .to_str()
            .unwrap()
            .strip_prefix("http://127.0.0.1:6878")
            .unwrap();
        let first = wait_for_hls_playlist(&app, playback).await;
        let old_segment = playlist_segment_path(&first).to_owned();
        let old_seq = old_segment
            .rsplit('/')
            .next()
            .unwrap()
            .strip_suffix(".ts")
            .unwrap()
            .parse::<u64>()
            .unwrap();

        for _ in 0..100 {
            let playlist = wait_for_hls_playlist(&app, playback).await;
            let media_seq = playlist
                .lines()
                .find_map(|line| line.strip_prefix("#EXT-X-MEDIA-SEQUENCE:"))
                .unwrap()
                .parse::<u64>()
                .unwrap();
            if media_seq > old_seq {
                break;
            }
            tokio::time::sleep(Duration::from_millis(3)).await;
        }

        assert_eq!(
            app.oneshot(Request::get(old_segment).body(Body::empty()).unwrap())
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
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
            "/ace/m/0123456789012345678901234567890123456789/token.m3u8",
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
    async fn extensionless_stream_is_a_direct_mpegts_alias() {
        let st = state();
        let app = router(st.clone());

        let extensionless = app
            .clone()
            .oneshot(
                Request::get("/streams/test/channel")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(extensionless.status(), StatusCode::OK);
        assert_eq!(extensionless.headers()[header::CONTENT_TYPE], "video/mp2t");
        assert!(!extensionless.headers().contains_key(header::LOCATION));

        let explicit = app
            .oneshot(
                Request::get("/streams/test/channel.ts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(explicit.status(), StatusCode::OK);
        assert_eq!(
            st.manager.list().await,
            vec![(
                "test".to_string(),
                "channel".to_string(),
                2,
                StreamMetadata::default(),
            )]
        );
    }

    #[tokio::test]
    async fn unsupported_dotted_suffixes_return_404_without_starting_sessions() {
        for path in [
            "/streams/test/x.mp4",
            "/streams/test/x.foo",
            "/streams/test/x.",
        ] {
            let st = state();
            let resp = router(st.clone())
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{path}");
            assert!(st.manager.list().await.is_empty(), "{path}");
        }
    }

    #[tokio::test]
    async fn empty_ts_id_returns_404_without_starting_a_session() {
        let st = state();
        let resp = router(st.clone())
            .oneshot(
                Request::get("/streams/test/.ts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert!(st.manager.list().await.is_empty());
    }

    #[tokio::test]
    async fn status_404_until_started_then_reports() {
        let st = fixture_state(0);
        let app = router(st.clone());
        // Not started yet.
        let resp = app
            .clone()
            .oneshot(
                Request::get("/streams/fix/chan/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        // Start it, then status reports it with clean keys.
        st.manager.get_or_start("fix", "chan").await.unwrap();
        let resp = app
            .oneshot(
                Request::get("/streams/fix/chan/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["metadata"], stream_metadata_json(&fixture_metadata()));
        assert_eq!(value["bitrate"], 0, "measured bitrate keeps its meaning");
    }

    #[tokio::test]
    async fn m3u8_serves_hls_playlist_with_stream_title() {
        let resp = router(fixture_state(0))
            .oneshot(
                Request::get("/streams/fix/chan.m3u8")
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
        assert_eq!(resp.headers()["icy-name"], "Synthetic Demo Channel");
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
        let st = fixture_state(0);
        st.manager.get_or_start("fix", "abc").await.unwrap();
        let resp = router(st)
            .oneshot(Request::get("/streams").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["streams"][0]["id"], "abc");
        assert_eq!(
            value["streams"][0]["metadata"],
            stream_metadata_json(&fixture_metadata())
        );
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
        assert!(crate::broadcast::valid_broadcast_name("news"));
        assert!(crate::broadcast::valid_broadcast_name("sports-2.hd_1"));
        assert!(!crate::broadcast::valid_broadcast_name(""));
        assert!(!crate::broadcast::valid_broadcast_name("."));
        assert!(!crate::broadcast::valid_broadcast_name(".."));
        assert!(!crate::broadcast::valid_broadcast_name("../etc/passwd"));
        assert!(!crate::broadcast::valid_broadcast_name("a/b"));
        assert!(!crate::broadcast::valid_broadcast_name("has space"));
        assert!(!crate::broadcast::valid_broadcast_name(&"x".repeat(65)));
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
