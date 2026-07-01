//! Clean HTTP API (axum). No `ace`/`acestream` tokens in paths or JSON keys — the only
//! `ace` on the surface is the `{network}` value, selecting a provider.

use crate::broadcast::{BroadcastRegistry, CHUNK_LENGTH, PIECE_LENGTH};
use crate::manager::StreamManager;
use ace_swarm::listen::SeedRegistry;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{routing::get, routing::put, Json, Router};
use bytes::Bytes;
use futures::StreamExt;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;

#[derive(Clone)]
pub struct AppState {
    pub manager: Arc<StreamManager>,
    pub networks: Vec<String>,
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
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/networks", get(networks))
        .route("/streams", get(list_streams))
        .route("/streams/:network/:file", get(stream_file).delete(delete_stream))
        .route("/streams/:network/:id/status", get(stream_status))
        .route("/streams/:network/:id/seg/:seg", get(stream_segment))
        .route("/broadcast/:name", put(broadcast_ingest).get(broadcast_transport))
        .with_state(state)
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
async fn broadcast_ingest(State(s): State<AppState>, Path(name): Path<String>, body: Body) -> Response {
    let Some(bs) = &s.broadcasts else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let (bc, freshly_minted) = bs
        .registry
        .start_or_resume(&name, &name, &bs.trackers, &bs.seed_registry, bs.store_bytes)
        .await;
    let infohash_hex: String = bc.infohash.iter().map(|b| format!("{b:02x}")).collect();
    eprintln!("[broadcast] {name}: ingesting as infohash {infohash_hex}");

    // Self-announce this broadcast to tracker + DHT (Task 7 discoverability), exactly once
    // per freshly-minted name — resumed PUTs must not spawn a second competing loop. A no-op
    // without an inbound listener: advertising a port nobody's serving on would misdirect
    // real peers into a dead connection instead of outpace's own S1/S2 serve path.
    if freshly_minted {
        if let Some(port) = bs.inbound_peer_port {
            let trackers = bs.trackers.clone();
            let infohash = bc.infohash;
            tokio::spawn(crate::ace_provider::announce_infohash_periodically(
                trackers, infohash, port,
            ));
        }
    }

    let store = bc.store.clone();
    let auth = bc.auth.clone();
    tokio::spawn(async move {
        let mut resync = ace_media::mpegts::TsResync::new();
        let sig_len = auth.signature_len() as u64;
        let mut chunker =
            ace_wire::signing_chunker::SigningChunker::new(PIECE_LENGTH, CHUNK_LENGTH, 0, sig_len);
        let mut stream = body.into_data_stream();
        while let Some(chunk) = stream.next().await {
            let Ok(chunk) = chunk else { break };
            let aligned = resync.push(&chunk);
            for out in chunker.push(&aligned, &auth) {
                store.lock().await.put_chunk(out.piece, out.chunk, &out.data);
            }
        }
        for out in chunker.flush(&auth) {
            store.lock().await.put_chunk(out.piece, out.chunk, &out.data);
        }
    });

    Json(json!({ "name": name, "infohash": infohash_hex })).into_response()
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
async fn delete_stream(State(s): State<AppState>, Path((network, file)): Path<(String, String)>) -> Response {
    let id = file.strip_suffix(".ts").or_else(|| file.strip_suffix(".m3u8")).unwrap_or(&file);
    if s.manager.stop(&network, id).await {
        StatusCode::NO_CONTENT.into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

/// `GET /streams/{network}/{id}/status` — stats for a running session (404 if not active).
async fn stream_status(State(s): State<AppState>, Path((network, id)): Path<(String, String)>) -> Response {
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
async fn stream_segment(State(s): State<AppState>, Path((network, id, seg)): Path<(String, String, String)>) -> Response {
    let Some(seq) = seg.strip_suffix(".ts").and_then(|n| n.parse::<u64>().ok()) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let pkg = match s.manager.get_or_start_hls(&network, &id).await {
        Ok(p) => p,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    match pkg.segment(seq) {
        Some(bytes) => ([(header::CONTENT_TYPE, "video/mp2t")], bytes).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// `GET /streams/{network}/{id}.ts` (continuous MPEG-TS) or `.m3u8` (live HLS playlist).
async fn stream_file(State(s): State<AppState>, Path((network, file)): Path<(String, String)>) -> Response {
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
        AppState { manager: StreamManager::new(r), networks: vec!["test".into()], broadcasts: None }
    }

    /// A real libx264 MPEG-TS (committed fixture). Video PID 0x100; keyframes at byte
    /// offsets 564 and 9400.
    const FIXTURE: &[u8] =
        include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/vectors/media/h264-keyframes.ts"));

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
                return None;
            }
            let end = (self.pos + 188 * 3).min(FIXTURE.len());
            let chunk = Bytes::copy_from_slice(&FIXTURE[self.pos..end]);
            self.pos = end;
            Some(chunk)
        }
        fn stats(&self) -> SourceStats {
            SourceStats { peers: 1, bitrate: 0, buffer_ms: 0, uploaded: 0, peers_served: 0 }
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
        AppState { manager: StreamManager::new(r), networks: vec!["fix".into()], broadcasts: None }
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
            .oneshot(Request::get("/streams/fix/x.ts").body(Body::empty()).unwrap())
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
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
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
            .oneshot(Request::get("/streams/fix/somechannel.ts").body(Body::empty()).unwrap())
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
    async fn unknown_network_returns_404() {
        let resp = router(state())
            .oneshot(Request::get("/streams/nope/x.ts").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn non_ts_extension_returns_404() {
        let resp = router(state())
            .oneshot(Request::get("/streams/test/x.foo").body(Body::empty()).unwrap())
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
            .oneshot(Request::get("/streams/test/chan/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        // Start it, then status reports it with clean keys.
        st.manager.get_or_start("test", "chan").await.unwrap();
        let resp = app
            .oneshot(Request::get("/streams/test/chan/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let txt = String::from_utf8_lossy(&body);
        assert!(txt.contains("\"clients\"") && txt.contains("\"peers\""));
    }

    #[tokio::test]
    async fn m3u8_serves_hls_playlist() {
        let resp = router(state())
            .oneshot(Request::get("/streams/test/chan.m3u8").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()[header::CONTENT_TYPE], "application/vnd.apple.mpegurl");
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
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
            .oneshot(Request::delete("/streams/test/z").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert!(st.manager.get("test", "z").await.is_none());
        // Second delete finds nothing to stop.
        let resp = app
            .oneshot(Request::delete("/streams/test/z").body(Body::empty()).unwrap())
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
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
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
            .oneshot(Request::put("/broadcast/x").body(Body::from(vec![0u8; 4])).unwrap())
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
            .oneshot(Request::put("/broadcast/x").body(Body::from(vec![0x47u8; 8])).unwrap())
            .await
            .unwrap();
        assert_eq!(put_resp.status(), StatusCode::OK);

        let get_resp = app
            .oneshot(Request::get("/broadcast/x").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(get_resp.into_body(), 1 << 20).await.unwrap();
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
            .oneshot(Request::put("/broadcast/mychan").body(Body::from(body)).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let respbody = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&respbody).unwrap();
        let infohash_hex = json["infohash"].as_str().unwrap().to_string();
        assert_eq!(infohash_hex.len(), 40, "a 20-byte infohash, hex-encoded");
        assert_eq!(json["name"], "mychan");

        // The minted infohash must be immediately servable via the shared registry (S1/S2's
        // existing serve path) — confirming the wiring, not just the HTTP response shape.
        let mut infohash = [0u8; 20];
        for i in 0..20 {
            infohash[i] = u8::from_str_radix(&infohash_hex[i * 2..i * 2 + 2], 16).unwrap();
        }
        assert!(seed_registry.serves(&infohash));

        // Give the background ingest task a moment to process the body. It's short (well
        // under a full 1 MiB piece), so this reaches the store via `SigningChunker::flush`'s
        // short-final-piece path (see `ace_wire::signing_chunker`), not a full-piece `push`.
        for _ in 0..50 {
            if seed_registry.get(&infohash).unwrap().lock().await.chunk(0, 0).is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            seed_registry.get(&infohash).unwrap().lock().await.chunk(0, 0).is_some(),
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
            .oneshot(Request::put("/broadcast/bigchan").body(Body::from(body)).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let respbody = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&respbody).unwrap();
        let infohash_hex = json["infohash"].as_str().unwrap().to_string();
        let mut infohash = [0u8; 20];
        for i in 0..20 {
            infohash[i] = u8::from_str_radix(&infohash_hex[i * 2..i * 2 + 2], 16).unwrap();
        }

        // Wait for piece 0 to fully complete (all chunks present).
        for _ in 0..500 {
            if seed_registry.get(&infohash).unwrap().lock().await.has_piece(0) {
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
