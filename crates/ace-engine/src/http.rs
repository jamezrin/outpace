//! HTTP API (axum): the clean `/streams`/`/broadcast` surface plus the official-engine
//! compatibility `/ace/...` playback surface.

use crate::broadcast::BroadcastRegistry;
use crate::hls::HlsPackager;
use crate::manager::StreamManager;
use crate::provider::{ProviderError, VodByteSource, VodContent};
use crate::session::{StreamEvent, StreamSession};
use ace_swarm::listen::SeedRegistry;
use ace_swarm::resolve::{infohash_hex, resolve_via_catalog, stream_info_from_transport_url};
use ace_swarm::types::StreamMetadata;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{routing::get, routing::put, Json, Router};
use bytes::Bytes;
use futures::StreamExt;
use serde_json::json;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast::error::RecvError;

mod broadcast;
mod compat;
mod streams;
mod vod;

use broadcast::*;
use compat::*;
use streams::*;
use vod::*;

mod leases;

use leases::AceDirectLeaseGuard;
pub use leases::AceSessionStore;

fn stream_metadata_json(metadata: &StreamMetadata) -> serde_json::Value {
    json!({
        "title": metadata.title,
        // Transport metadata stores bytes/s; public API rates are bits/s.
        "bitrate": metadata.bitrate.map(|rate| rate.saturating_mul(8)),
        "categories": metadata.categories,
    })
}

async fn await_hls_ready(pkg: &HlsPackager, timeout: Duration) -> Result<(), Box<Response>> {
    if pkg.wait_ready(timeout).await {
        Ok(())
    } else {
        Err(Box::new(
            (
                StatusCode::SERVICE_UNAVAILABLE,
                [(header::RETRY_AFTER, "1")],
            )
                .into_response(),
        ))
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
    /// transport geometry/trackers (a bare infohash would need an already-indexed descriptor).
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

/// `GET /debug/memstats` — diagnostic heap counters. With `--features profiling`
/// (jemalloc) this reports live-heap `allocated` vs `resident`/`retained`, which
/// separates a real leak (`allocated` grows without bound) from allocator
/// retention (`resident - allocated` grows). Without the feature it only reports
/// that profiling is off; process RSS is still observable via `/proc`.
async fn debug_memstats() -> Response {
    #[cfg(all(not(target_env = "msvc"), target_pointer_width = "64"))]
    let body = {
        use tikv_jemalloc_ctl::{epoch, stats};
        // Refresh the cached stats; report the failure rather than silently returning stale values.
        let epoch_err = epoch::advance().err().map(|e| e.to_string());
        // A failed counter read serializes as JSON null, not 0 — a zeroed probe must not look like
        // a valid "0 bytes" reading.
        let read = |r: Result<usize, tikv_jemalloc_ctl::Error>| r.ok().map(|v| v as u64);
        json!({
            "allocator": "jemalloc",
            "epoch_error": epoch_err,
            "allocated": read(stats::allocated::read()),
            "active": read(stats::active::read()),
            "resident": read(stats::resident::read()),
            "retained": read(stats::retained::read()),
            "mapped": read(stats::mapped::read()),
        })
    };
    #[cfg(not(all(not(target_env = "msvc"), target_pointer_width = "64")))]
    let body = json!({ "allocator": "system" });
    Json(body).into_response()
}

pub fn router(state: AppState) -> Router {
    let compat = state.experimental_ace_compat;
    let mut router = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/debug/memstats", get(debug_memstats))
        .route("/networks", get(networks))
        .route("/streams", get(list_streams))
        .route(
            "/streams/{network}/{file}",
            get(stream_file).delete(delete_stream),
        )
        .route("/streams/{network}/{id}/status", get(stream_status))
        .route("/streams/{network}/{id}/seg/{seg}", get(stream_segment))
        .route("/vod/{network}/{id}", get(vod_stream))
        .route("/vod/{network}/{id}/manifest.m3u8", get(vod_manifest))
        .route("/vod/{network}/{id}/seg/{seg}", get(vod_hls_segment))
        .route(
            "/broadcast/{name}",
            put(broadcast_ingest)
                .get(broadcast_transport)
                .delete(broadcast_delete),
        );
    if compat {
        router = router
            .route("/ace/getstream", get(ace_getstream))
            .route("/ace/r/{id}/{token}", get(ace_playback))
            .route("/ace/manifest.m3u8", get(ace_manifest))
            .route("/ace/m/{id}/{manifest}", get(ace_hls_playback))
            .route("/ace/c/{session}/{segment}", get(ace_hls_segment))
            .route("/ace/stat/{id}/{token}", get(ace_stat))
            .route("/ace/cmd/{id}/{token}", get(ace_command))
            .route("/server/api", get(server_api));
    }
    router.with_state(state)
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
    use std::sync::atomic::Ordering;
    use tower::ServiceExt;

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
                "bitrate": 800_000,
                "categories": ["sports"],
            })
        );
    }

    #[test]
    fn stream_metadata_json_saturates_rate_conversion() {
        let metadata = StreamMetadata {
            bitrate: Some(u64::MAX),
            ..Default::default()
        };
        assert_eq!(stream_metadata_json(&metadata)["bitrate"], u64::MAX);
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
            ..crate::config::HlsConfig::default()
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
                ..SourceStats::default()
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
            manager: StreamManager::with_config(
                r,
                256,
                crate::config::HlsConfig {
                    window_segments: 6,
                    segment_duration_ms: 1000,
                    startup_segments: 1,
                    ..crate::config::HlsConfig::default()
                },
            ),
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
    struct StalledProvider;
    struct StalledSource;

    #[async_trait]
    impl TsSource for StalledSource {
        async fn next(&mut self) -> Option<Bytes> {
            std::future::pending().await
        }

        fn stats(&self) -> SourceStats {
            SourceStats::default()
        }
    }

    #[async_trait]
    impl StreamProvider for StalledProvider {
        fn network(&self) -> &'static str {
            "stalled"
        }

        async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
            Ok(Box::new(StalledSource))
        }
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
                    // 4, not 3: PacedPacketSource's fixture metadata carries a title (Task 5,
                    // #135), so the injected per-segment table prefix is [PAT][PMT][SDT] (3
                    // packets) rather than [PAT][PMT] (2) -- one more packet of headroom is
                    // needed to still fit a keyframe alongside it.
                    segment_packets: 4,
                    window_segments: 3,
                    segment_duration_ms: 1000,
                    startup_segments: 1,
                    ..crate::config::HlsConfig::default()
                },
            ),
            networks: vec!["hlsfix".into()],
            resolve_content_ids_in_getstream: false,
            ace_sessions: Arc::new(AceSessionStore::new(ttl, capacity)),
            experimental_ace_compat: true,
            broadcasts: None,
        }
    }

    fn stalled_hls_state_with(ttl: Duration, capacity: usize) -> AppState {
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(StalledProvider));
        AppState {
            manager: StreamManager::with_config(
                registry,
                32,
                crate::config::HlsConfig {
                    segment_packets: 3,
                    window_segments: 3,
                    segment_duration_ms: 1000,
                    startup_segments: 1,
                    ..crate::config::HlsConfig::default()
                },
            ),
            networks: vec!["stalled".into()],
            resolve_content_ids_in_getstream: false,
            ace_sessions: Arc::new(AceSessionStore::new(ttl, capacity)),
            experimental_ace_compat: true,
            broadcasts: None,
        }
    }

    fn stalled_hls_state() -> AppState {
        stalled_hls_state_with(Duration::from_secs(60), 8)
    }

    async fn mint_stalled_hls(app: &Router, id: &str) -> (String, String, String) {
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
        let playback = start.headers()[header::LOCATION]
            .to_str()
            .unwrap()
            .strip_prefix("http://127.0.0.1:6878/ace/m/")
            .unwrap();
        let (playback_id, manifest) = playback.split_once('/').unwrap();
        let token = manifest.strip_suffix(".m3u8").unwrap();
        (
            playback_id.to_string(),
            manifest.to_string(),
            format!("/ace/cmd/{playback_id}/{token}?method=stop"),
        )
    }

    fn spawn_stalled_hls_request(
        state: AppState,
        playback_id: String,
        manifest: String,
    ) -> tokio::task::JoinHandle<Response> {
        tokio::spawn(async move {
            ace_hls_playback_with_timeout(
                State(state),
                Path((playback_id, manifest)),
                Duration::from_secs(5),
            )
            .await
        })
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
            .as_chunks::<188>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
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

        let resumed = gate.push(FIXTURE);
        let first_video = resumed
            .as_chunks::<188>()
            .0
            .iter()
            .map(|packet| packet.as_slice())
            .find(|packet| ace_media::mpegts::ts_pid(packet) == 0x0100)
            .expect("resumed direct stream has a video keyframe");
        let first_video: &[u8; 188] = first_video.try_into().unwrap();
        assert!(
            ace_media::mpegts::ts_timing(first_video).discontinuity,
            "resumed direct TS must expose the discontinuity to the decoder"
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
        assert!(!resp.headers().contains_key("icy-name"));
        // Live body is unbounded; read just the first TS chunk.
        let mut stream = resp.into_body().into_data_stream();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first[0], 0x47);
        assert_eq!(
            ace_media::mpegts::read_sdt_service_name(&first).as_deref(),
            Some("Synthetic Demo Channel")
        );
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
        assert!(!media.headers().contains_key("icy-name"));
        let mut stream = media.into_body().into_data_stream();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first[0], 0x47);
        assert_eq!(
            ace_media::mpegts::read_sdt_service_name(&first).as_deref(),
            Some("Synthetic Demo Channel")
        );
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
        assert!(!response.headers().contains_key("icy-name"));
        let mut body = response.into_body().into_data_stream();
        let first = body.next().await.unwrap().unwrap();
        assert_eq!(first[0], 0x47);
        assert_eq!(
            ace_media::mpegts::read_sdt_service_name(&first).as_deref(),
            Some("Synthetic Demo Channel")
        );
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
            json!({ "title": "Synthetic Demo Channel", "bitrate": 800_000, "categories": ["sports"] })
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
    async fn ace_hls_cold_playlist_200_contains_a_segment() {
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

        let response = app
            .oneshot(Request::get(playback).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "application/vnd.apple.mpegurl"
        );
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        let body = response_text(response).await;
        assert!(body.lines().any(|line| !line.starts_with('#')));
    }

    #[tokio::test]
    async fn ace_hls_stalled_playlist_returns_retryable_503() {
        let id = "0123456789abcdef0123456789abcdef01234567";
        let state = stalled_hls_state();
        let app = router(state.clone());
        let (playback_id, manifest, _) = mint_stalled_hls(&app, id).await;

        let response = ace_hls_playback_with_timeout(
            State(state),
            Path((playback_id, manifest)),
            Duration::from_millis(1),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");
    }

    #[tokio::test]
    async fn ace_hls_stop_cancels_a_stalled_playlist_wait_with_404() {
        let id = "0123456789abcdef0123456789abcdef01234567";
        let state = stalled_hls_state();
        let app = router(state.clone());
        let (playback_id, manifest, stop_path) = mint_stalled_hls(&app, id).await;
        let request = spawn_stalled_hls_request(state, playback_id, manifest);
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(
            !request.is_finished(),
            "playlist request must be waiting for readiness"
        );

        let stopped = app
            .oneshot(Request::get(stop_path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(stopped.status(), StatusCode::OK);
        let response = tokio::time::timeout(Duration::from_millis(100), request)
            .await
            .expect("revocation must wake the playlist wait")
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn ace_hls_expiry_cancels_a_stalled_playlist_wait_with_404() {
        let id = "0123456789abcdef0123456789abcdef01234567";
        let state = stalled_hls_state_with(Duration::from_millis(40), 8);
        let app = router(state.clone());
        let (playback_id, manifest, _) = mint_stalled_hls(&app, id).await;
        let request = spawn_stalled_hls_request(state, playback_id, manifest);
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(
            !request.is_finished(),
            "playlist request must be waiting for readiness"
        );

        let response = tokio::time::timeout(Duration::from_millis(100), request)
            .await
            .expect("expiry must wake the playlist wait")
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn ace_hls_capacity_eviction_cancels_a_stalled_playlist_wait_with_404() {
        let first_id = "0123456789abcdef0123456789abcdef01234567";
        let second_id = "1123456789abcdef0123456789abcdef01234567";
        let state = stalled_hls_state_with(Duration::from_secs(60), 1);
        let app = router(state.clone());
        let (playback_id, manifest, _) = mint_stalled_hls(&app, first_id).await;
        let request = spawn_stalled_hls_request(state, playback_id, manifest);
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(
            !request.is_finished(),
            "playlist request must be waiting for readiness"
        );

        let _newer_lease = mint_stalled_hls(&app, second_id).await;
        let response = tokio::time::timeout(Duration::from_millis(100), request)
            .await
            .expect("capacity eviction must wake the playlist wait")
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
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
    async fn live_hls_segment_carries_sdt_service_name() {
        // The `.ts` path is covered by several HTTP round-trip tests asserting the SDT; this is
        // the HLS-path equivalent, driven end-to-end through the actual served segment bytes
        // rather than at the `HlsPackager` unit level.
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
        let playlist = wait_for_hls_playlist(&app, &playback).await;
        let segment_path = playlist_segment_path(&playlist).to_owned();

        let seg = app
            .clone()
            .oneshot(Request::get(&segment_path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(seg.status(), StatusCode::OK);
        let body = axum::body::to_bytes(seg.into_body(), 1 << 20)
            .await
            .unwrap();
        assert_eq!(
            ace_media::mpegts::read_sdt_service_name(&body).as_deref(),
            Some("Synthetic Demo Channel")
        );
        // Keep `app`/`manager` alive through the reads above, mirroring the other live-HLS tests.
        drop((app, manager));
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
            json!({ "title": "Synthetic Demo Channel", "bitrate": 800_000, "categories": ["sports"] })
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
        assert_eq!(value["metadata"]["bitrate"], 800_000);
        assert_eq!(value["metadata"]["title"], "Synthetic Demo Channel");
        assert_eq!(value["bitrate"], 0, "measured bitrate keeps its meaning");
    }

    #[tokio::test]
    async fn m3u8_playlist_omits_icy_headers() {
        // An `icy-*` header on an HLS manifest makes VLC's HTTP access promote the URL to the
        // `icyx://` (Icecast) scheme and treat the body as a raw audio stream: the HLS demux
        // never loads, so VLC busy-loops the playlist and never fetches a segment. The manifest
        // must therefore carry no `icy-*` header even when the stream has a title (the title is
        // still exposed to clients via the JSON metadata endpoints). No `icy-*` header is emitted
        // on any path (direct `.ts` or HLS) any more; the title travels via the MPEG-TS SDT
        // `service_name` instead.
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
        assert!(
            !resp
                .headers()
                .keys()
                .any(|k| k.as_str().starts_with("icy-")),
            "HLS manifest must not carry icy-* headers (breaks VLC HLS playback)"
        );
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let body = String::from_utf8_lossy(&body);
        assert!(body.starts_with("#EXTM3U"));
        assert!(body.lines().any(|line| !line.starts_with('#')));
    }

    #[tokio::test]
    async fn m3u8_stalled_playlist_returns_retryable_503() {
        let response = stream_file_with_hls_timeout(
            State(stalled_hls_state()),
            Path(("stalled".to_string(), "chan.m3u8".to_string())),
            Duration::from_millis(1),
        )
        .await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");
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
            json!({ "title": "Synthetic Demo Channel", "bitrate": 800_000, "categories": ["sports"] })
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

    #[derive(Clone)]
    struct MeasuredFixture(SourceStats);
    impl crate::provider::StatsReader for MeasuredFixture {
        fn snapshot(&self) -> SourceStats {
            self.0.clone()
        }
    }
    #[async_trait]
    impl TsSource for MeasuredFixture {
        async fn next(&mut self) -> Option<Bytes> {
            std::future::pending().await
        }
        fn stats(&self) -> SourceStats {
            self.0.clone()
        }
        fn stats_handle(&self) -> Option<crate::provider::StatsHandle> {
            Some(Arc::new(self.clone()))
        }
    }
    #[async_trait]
    impl StreamProvider for MeasuredFixture {
        fn network(&self) -> &'static str {
            "fix"
        }
        async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
            Ok(Box::new(self.clone()))
        }
    }
    fn measured_state(position: bool) -> AppState {
        let mut st = ace_compat_state(0);
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(MeasuredFixture(SourceStats {
            bitrate: 12288,
            download_rate: 1536,
            upload_rate: 512,
            livepos: position.then_some(crate::provider::LivePositionStats {
                pos: Some(5),
                last: Some(9),
                live_first: Some(2),
                live_last: Some(9),
                buffer_pieces: 1,
            }),
            ..SourceStats::default()
        })));
        st.manager = StreamManager::new(registry);
        st
    }

    #[tokio::test]
    async fn measured_stats_schema_reports_units_positions_and_null_timestamps() {
        let st = measured_state(true);
        let _session = st.manager.get_or_start("fix", "stats").await.unwrap();
        let token = st.ace_sessions.mint("stats".into(), "stats".into());
        let Json(json) = ace_stat(State(st.clone()), Path(("stats".into(), token))).await;
        assert_eq!(json["response"]["speed_down"], 1.5);
        assert_eq!(json["response"]["speed_up"], 0.5);
        let pos = &json["response"]["livepos"];
        assert_eq!(pos["pos"], 5);
        assert_eq!(pos["last"], 9);
        assert_eq!(pos["buffer_pieces"], 1);
        assert_eq!(pos["is_live"], 1);
        assert_eq!(pos.get("first_ts"), Some(&serde_json::Value::Null));
        assert_eq!(pos.get("last_ts"), Some(&serde_json::Value::Null));
        let response = stream_status(State(st), Path(("fix".into(), "stats".into()))).await;
        let native: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(native["bitrate"], 12288);
        assert_eq!(native["livepos"], *pos);
    }

    #[tokio::test]
    async fn measured_stats_schema_idle_unknown_and_invalid_token_do_not_start() {
        let st = measured_state(false);
        let token = st.ace_sessions.mint("stats".into(), "stats".into());
        let Json(idle) = ace_stat(State(st.clone()), Path(("stats".into(), token.clone()))).await;
        assert_eq!(idle["response"]["speed_down"], 0);
        assert_eq!(
            idle["response"].get("livepos"),
            Some(&serde_json::Value::Null)
        );
        let Json(error) =
            ace_stat(State(st.clone()), Path(("stats".into(), "invalid".into()))).await;
        assert!(error["error"].is_string());
        assert!(st.manager.list().await.is_empty());
        let _session = st.manager.get_or_start("fix", "stats").await.unwrap();
        let Json(active) = ace_stat(State(st), Path(("stats".into(), token))).await;
        assert_eq!(
            active["response"].get("livepos"),
            Some(&serde_json::Value::Null)
        );
    }

    struct NativeAceFixture;

    #[async_trait]
    impl StreamProvider for NativeAceFixture {
        fn network(&self) -> &'static str {
            "ace"
        }
        async fn open(&self, id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
            if id != UNRESOLVED_IH && id != format!("cid:{UNRESOLVED_IH}") {
                return Err(ProviderError::NotFound);
            }
            Ok(Box::new(PacedFixtureSource { pos: 0 }))
        }
    }

    fn native_ace_fixture_state() -> AppState {
        let mut st = fixture_state(0);
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(NativeAceFixture));
        st.manager = StreamManager::with_config(
            registry,
            256,
            crate::config::HlsConfig {
                startup_segments: 1,
                segment_duration_ms: 1000,
                ..crate::config::HlsConfig::default()
            },
        );
        st.networks = vec!["ace".into()];
        st
    }

    #[tokio::test]
    async fn native_content_id_aliases_share_playback_status_and_delete() {
        let st = native_ace_fixture_state();
        let app = router(st.clone());
        let upper = UNRESOLVED_IH.to_ascii_uppercase();
        let canonical = format!("cid:{UNRESOLVED_IH}");
        let mut responses = Vec::new();
        for id in [upper.clone(), canonical.clone()] {
            let response = app
                .clone()
                .oneshot(
                    Request::get(format!("/streams/ace/{id}.ts"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            responses.push(response);
        }
        assert_eq!(st.manager.list().await.len(), 1);
        assert!(st.manager.get("ace", &canonical).await.is_some());
        for id in [upper.clone(), format!("cid:{upper}")] {
            let response = app
                .clone()
                .oneshot(
                    Request::get(format!("/streams/ace/{id}/status"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let status: serde_json::Value =
                serde_json::from_str(&body_text(response).await).unwrap();
            assert_eq!(status["id"], canonical);
            assert_eq!(status["clients"], 2);
        }
        let response = app
            .oneshot(
                Request::delete(format!("/streams/ace/{upper}.ts"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(st.manager.get("ace", &canonical).await.is_none());
        drop(responses);
    }

    #[tokio::test]
    async fn native_content_id_hls_aliases_share_segments_and_delete() {
        let st = native_ace_fixture_state();
        let app = router(st.clone());
        let upper = UNRESOLVED_IH.to_ascii_uppercase();
        let canonical = format!("cid:{UNRESOLVED_IH}");
        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/streams/ace/{upper}.m3u8"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let playlist = body_text(response).await;
        let segment = playlist_segment_path(&playlist);
        assert!(segment.starts_with(&format!("/streams/ace/{canonical}/seg/")));
        let alias_segment = segment.replace(&canonical, &upper);
        let mut bodies = Vec::new();
        for path in [segment, alias_segment.as_str()] {
            let response = app
                .clone()
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            bodies.push(response_bytes(response).await);
        }
        assert_eq!(bodies[0], bodies[1]);
        let response = app
            .oneshot(
                Request::delete(format!("/streams/ace/cid:{upper}.m3u8"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(st.manager.get("ace", &canonical).await.is_none());
    }

    #[tokio::test]
    async fn native_infohash_hls_emits_roundtrippable_explicit_urls() {
        let st = native_ace_fixture_state();
        let app = router(st);
        let explicit = format!("ih:{UNRESOLVED_IH}");
        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/streams/ace/{explicit}.m3u8"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let playlist = body_text(response).await;
        let path = playlist_segment_path(&playlist);
        assert!(path.starts_with(&format!("/streams/ace/{explicit}/seg/")));
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/streams/ace/{explicit}/status"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(status["id"], explicit);
        let response = app
            .oneshot(Request::get("/streams").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let list: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(list["streams"][0]["id"], explicit);
    }

    #[tokio::test]
    async fn native_malformed_explicit_identifiers_do_not_start_sessions() {
        let st = native_ace_fixture_state();
        let app = router(st.clone());
        for id in ["cid:bad", "ih:bad"] {
            for path in [
                format!("/streams/ace/{id}"),
                format!("/streams/ace/{id}.m3u8"),
                format!("/streams/ace/{id}/status"),
                format!("/streams/ace/{id}/seg/0.ts"),
            ] {
                let response = app
                    .clone()
                    .oneshot(Request::get(path).body(Body::empty()).unwrap())
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            }
            let response = app
                .clone()
                .oneshot(
                    Request::delete(format!("/streams/ace/{id}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
        assert!(st.manager.list().await.is_empty());
    }

    #[tokio::test]
    async fn native_explicit_infohash_uses_a_separate_session_key() {
        let st = native_ace_fixture_state();
        let app = router(st.clone());
        let upper = UNRESOLVED_IH.to_ascii_uppercase();
        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/streams/ace/ih:{upper}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(st.manager.get("ace", UNRESOLVED_IH).await.is_some());
        assert!(st
            .manager
            .get("ace", &format!("cid:{UNRESOLVED_IH}"))
            .await
            .is_none());
        let status = app
            .clone()
            .oneshot(
                Request::get(format!("/streams/ace/ih:{upper}/status"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);
        let deleted = app
            .oneshot(
                Request::delete(format!("/streams/ace/ih:{upper}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    }

    const UNRESOLVED_IH: &str = "0123456789abcdef0123456789abcdef01234567";

    /// An empty index and cached synthetic lookup failure keep native error tests offline.
    fn ace_provider_state() -> AppState {
        let mut registry = ProviderRegistry::new();
        let provider = crate::ace_provider::AceProvider::new(
            Arc::new(ace_wire::identity::Identity::generate()),
            0,
        );
        provider.cache_infohash_failure_for_test(UNRESOLVED_IH);
        registry.register(Arc::new(provider));
        AppState {
            manager: StreamManager::new(registry),
            networks: vec!["ace".into()],
            resolve_content_ids_in_getstream: false,
            ace_sessions: Arc::new(AceSessionStore::default()),
            experimental_ace_compat: true,
            broadcasts: None,
        }
    }

    async fn body_text(resp: Response) -> String {
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn native_routes_refuse_an_explicit_infohash_without_a_descriptor() {
        for path in [
            format!("/streams/ace/ih:{UNRESOLVED_IH}"),
            format!("/streams/ace/ih:{UNRESOLVED_IH}.ts"),
            format!("/streams/ace/ih:{UNRESOLVED_IH}.m3u8"),
        ] {
            let resp = router(ace_provider_state())
                .oneshot(Request::get(path.as_str()).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY, "{path}");
            let content_type = resp.headers()[header::CONTENT_TYPE].to_str().unwrap();
            assert!(
                content_type.starts_with("text/plain"),
                "{path}: {content_type}"
            );
            let body = body_text(resp).await;
            assert!(
                body.contains(&format!("cid:{UNRESOLVED_IH}")),
                "{path}: {body}"
            );
        }
    }

    #[tokio::test]
    async fn compat_routes_admit_cold_infohashes_for_resolution_at_playback_open() {
        let state = ace_provider_state();
        let sessions = state.ace_sessions.clone();
        let app = router(state);
        for query in [
            format!("/ace/getstream?format=json&infohash={UNRESOLVED_IH}"),
            format!(
                "/ace/getstream?format=json&magnet=magnet%3A%3Fxt%3Durn%3Abtih%3A{UNRESOLVED_IH}"
            ),
        ] {
            let resp = app
                .clone()
                .oneshot(Request::get(query.as_str()).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "{query}");
            let json: serde_json::Value = serde_json::from_str(&body_text(resp).await).unwrap();
            assert!(json["error"].is_null(), "{query}: {json}");
            assert_eq!(json["response"]["infohash"], UNRESOLVED_IH);
            assert!(json["response"]["playback_url"].is_string());
        }
        assert!(
            sessions.active_count() > 0,
            "cold selectors can mint playback leases"
        );
    }

    #[tokio::test]
    async fn compat_manifest_requires_successful_cold_lookup_before_minting_urls() {
        let state = ace_provider_state();
        let sessions = state.ace_sessions.clone();
        let response = router(state)
            .oneshot(
                Request::get(format!(
                    "/ace/manifest.m3u8?format=json&infohash={UNRESOLVED_IH}"
                ))
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(sessions.active_count(), 0);
    }

    #[tokio::test]
    async fn compat_getstream_mints_urls_for_an_infohash_with_a_verified_descriptor() {
        let state = ace_provider_state();
        let (transport, _) = crate::ace_provider::test_support::synthetic_live_transport(524_288);
        let info = ace_swarm::resolve::stream_info_from_transport(&transport).unwrap();
        state.manager.remember_live_descriptor("ace", &info);
        let ih = infohash_hex(&info.infohash);
        // JSON mode only mints URLs; it never opens the stream, so this stays offline.
        let resp = router(state)
            .oneshot(
                Request::get(format!("/ace/getstream?format=json&infohash={ih}").as_str())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_str(&body_text(resp).await).unwrap();
        assert!(json["error"].is_null(), "{json}");
        assert_eq!(json["response"]["infohash"], ih);
    }
}
