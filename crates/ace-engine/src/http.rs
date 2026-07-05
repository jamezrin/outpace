//! HTTP API (axum): the clean `/streams`/`/broadcast` surface plus the official-engine
//! compatibility `/ace/...` playback surface.

use crate::broadcast::BroadcastRegistry;
use crate::manager::StreamManager;
use crate::session::StreamSession;
use ace_swarm::listen::SeedRegistry;
use ace_swarm::resolve::resolve_via_catalog;
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
use tokio::sync::broadcast::error::RecvError;

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
    pub ace_session_aliases: Arc<Mutex<HashMap<String, String>>>,
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
    pub inbound_peer_port: Option<u16>,
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
        .route(
            "/broadcast/:name",
            put(broadcast_ingest).get(broadcast_transport),
        );
    if compat {
        router = router
            .route("/ace/getstream", get(ace_getstream))
            .route("/ace/r/:id/:token", get(ace_playback))
            .route("/ace/stat/:id/:token", get(ace_stat))
            .route("/ace/cmd/:id/:token", get(ace_command));
    }
    router.with_state(state)
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

/// `PUT /broadcast/{name}` (B1) — accepts a chunked MPEG-TS body and originates it as an
/// Acestream-compatible live swarm. Responds immediately with the minted infohash (identical
/// name -> identical, already-minted broadcast; see `BroadcastRegistry::start_or_resume`)
/// while ingest continues in a background task — the request body may be a long-lived,
/// effectively-unbounded stream (a live source), so the handler can't wait for it to finish.
///
/// KNOWN GAP: piece numbering restarts at 0 on every ingest task, even when resuming an
/// already-minted name (mirrors the real engine's `.restart` file semantics, which we don't
/// yet persist) — a second `PUT` to the same name after the first ingest ends would
/// overwrite piece indices rather than continuing them. Fine for a single continuous ingest
/// (the only case exercised so far); flagged for whoever adds ingest-reconnect support.
async fn broadcast_ingest(
    State(s): State<AppState>,
    Path(name): Path<String>,
    body: Body,
) -> Response {
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
    let infohash_hex = hex20_string(&bc.infohash);
    let content_id_hex = hex20_string(&bc.content_id);
    crate::alog!("[broadcast] {name}: ingesting as infohash {infohash_hex}");

    // Self-announce this broadcast and its content-id metadata swarm to tracker + DHT,
    // exactly once per freshly-minted name — resumed PUTs must not spawn duplicate loops.
    // A no-op without an inbound listener: advertising a port nobody's serving on would
    // misdirect real peers into a dead connection instead of outpace's own S1/S2 serve path.
    if freshly_minted {
        if let Some(port) = bs.inbound_peer_port {
            let trackers = bs.trackers.clone();
            let infohash = bc.infohash;
            let content_key = bc.content_id;
            tokio::spawn(crate::ace_provider::announce_infohash_periodically(
                trackers.clone(),
                infohash,
                port,
            ));
            tokio::spawn(crate::ace_provider::announce_infohash_periodically(
                trackers,
                content_key,
                port,
            ));
        }
    }

    let store = bc.store.clone();
    let auth = bc.auth.clone();
    tokio::spawn(async move {
        let mut ingest = crate::broadcast_ingest::BroadcastIngest::new(store, auth);
        let mut stream = body.into_data_stream();
        while let Some(chunk) = stream.next().await {
            let Ok(chunk) = chunk else { break };
            ingest.push_bytes(&chunk).await;
        }
        ingest.finish().await;
    });

    Json(json!({
        "name": name,
        "content_id": content_id_hex,
        "infohash": infohash_hex,
    }))
    .into_response()
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
                Ok(info) => selection = selection.with_resolved_infohash(hex20_string(&info.infohash)),
                Err(e) => crate::alog!(
                    "[ace] getstream content_id catalog resolution failed, falling back to cid: {e:?}"
                ),
            }
        }
    }

    let token = "outpace";
    let base = request_base(&headers);
    let playback_id = selection.playback_id;
    let session_key = selection.session_key;
    if playback_id != session_key {
        s.ace_session_aliases
            .lock()
            .unwrap()
            .insert(playback_id.clone(), session_key);
    }
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
    Path((id, _token)): Path<(String, String)>,
) -> Response {
    let Some(network) = ace_network(&s) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let session_key = ace_session_key(&s, &id);
    match s.manager.get_or_start(&network, &session_key).await {
        Ok(session) => stream_session_response(session),
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
    let public_id = ace_public_id(&id);
    let session_key = ace_session_key(&s, &id);
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
    Path((id, _token)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    if params.get("method").is_some_and(|m| m == "stop") {
        if let Some(network) = ace_network(&s) {
            let session_key = ace_session_key(&s, &id);
            s.manager.stop(&network, &session_key).await;
        }
        return Json(json!({ "response": "ok", "error": null }));
    }
    Json(json!({ "response": null, "error": "missing method" }))
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
    ace_nonempty_param(params, "infohash")
        .or_else(|| ace_nonempty_param(params, "id"))
        .map(|id| AceStreamSelection {
            public_id: id.to_string(),
            playback_id: id.to_string(),
            session_key: id.to_string(),
            content_id: None,
        })
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

fn ace_session_key(s: &AppState, id: &str) -> String {
    s.ace_session_aliases
        .lock()
        .unwrap()
        .get(id)
        .cloned()
        .unwrap_or_else(|| id.to_string())
}

fn hex20_string(bytes: &[u8; 20]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
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
                Ok(chunk) => {
                    let out = gate.push(&chunk);
                    if out.is_empty() {
                        continue; // still waiting for the first keyframe
                    }
                    return Some((Ok::<_, std::io::Error>(Bytes::from(out)), (sub, gate)));
                }
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    });
    Response::builder()
        .header(header::CONTENT_TYPE, "video/mp2t")
        .body(Body::from_stream(stream))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broadcast::{CHUNK_LENGTH, PIECE_LENGTH};
    use crate::provider::{ProviderError, ProviderRegistry, SourceStats, StreamProvider, TsSource};
    use crate::testprovider::TestProvider;
    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use bytes::Bytes;
    use tower::ServiceExt;

    fn state() -> AppState {
        let mut r = ProviderRegistry::new();
        r.register(std::sync::Arc::new(TestProvider { chunks: 10 }));
        AppState {
            manager: StreamManager::new(r),
            networks: vec!["test".into()],
            resolve_content_ids_in_getstream: false,
            ace_session_aliases: Arc::new(Mutex::new(HashMap::new())),
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

    fn fixture_state(skip: usize) -> AppState {
        let mut r = ProviderRegistry::new();
        r.register(std::sync::Arc::new(FixtureProvider { skip }));
        AppState {
            manager: StreamManager::new(r),
            networks: vec!["fix".into()],
            resolve_content_ids_in_getstream: false,
            ace_session_aliases: Arc::new(Mutex::new(HashMap::new())),
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

    #[tokio::test]
    async fn healthz_ok() {
        let resp = router(state())
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
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
        assert_eq!(
            playback_path,
            format!("/ace/r/cid:{content_id}/outpace"),
            "content_id playback must enter the provider through the cid: resolver path"
        );
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
    async fn ace_stat_and_stop_track_content_id_session() {
        use futures::StreamExt;
        let content_id = "2123456789abcdef0123456789abcdef01234567";
        let state = ace_compat_state(0);
        let manager = state.manager.clone();
        let app = router(state);

        let stat_path = format!("/ace/stat/cid:{content_id}/outpace");
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
                Request::get(format!("/ace/r/cid:{content_id}/outpace"))
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
                Request::get(format!("/ace/cmd/cid:{content_id}/outpace?method=stop"))
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
        assert!(manager
            .get("fix", &format!("cid:{content_id}"))
            .await
            .is_none());
        assert!(manager.get("fix", content_id).await.is_none());
    }

    #[tokio::test]
    async fn ace_compat_routes_are_404_by_default() {
        for path in [
            "/ace/getstream?format=json&infohash=0123456789012345678901234567890123456789",
            "/ace/r/0123456789012345678901234567890123456789/outpace",
            "/ace/stat/0123456789012345678901234567890123456789/outpace",
            "/ace/cmd/0123456789012345678901234567890123456789/outpace?method=stop",
            "/server/api?method=get_version",
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
            inbound_peer_port: None,
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
            piece_bytes.extend_from_slice(guard.chunk(0, c).unwrap());
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
}
