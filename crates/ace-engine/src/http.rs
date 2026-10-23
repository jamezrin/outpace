//! HTTP API (axum): the clean `/streams`/`/broadcast` surface plus the official-engine
//! compatibility `/ace/...` playback surface.

use crate::broadcast::BroadcastRegistry;
use crate::hls::{HlsPackager, HlsStatus};
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

#[cfg(test)]
tokio::task_local! {
    static HLS_AFTER_READY: Box<dyn Fn(&HlsPackager) + Send + Sync>;
}
use tokio::sync::broadcast::error::RecvError;

mod broadcast;
mod broadcast_source;
mod compat;
mod streams;
mod vod;

use broadcast::*;
use broadcast_source::*;
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

/// Read readiness and render under the same media lock. A recovery between a readiness
/// wake and the snapshot must spend only the remainder of this request's original budget.
async fn await_hls_playlist(
    pkg: &HlsPackager,
    timeout: Duration,
    prefix: &str,
    native_access: bool,
) -> Result<String, Box<Response>> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match pkg.playlist_snapshot(prefix, native_access) {
            Ok(playlist) => return Ok(playlist),
            Err(HlsStatus::Starting | HlsStatus::Recovering) => {}
            Err(_) => break,
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() || pkg.wait_available(remaining).await != HlsStatus::Ready {
            break;
        }
        #[cfg(test)]
        let _ = HLS_AFTER_READY.try_with(|hook| hook(pkg));
    }
    Err(Box::new(
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::RETRY_AFTER, "1")],
        )
            .into_response(),
    ))
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
    /// Supported, opt-in Acestream HTTP subset. Native `/streams` and
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
    pub async fn attach_source(
        &self,
        name: &str,
        config: crate::broadcast_pull::ValidatedSourceConfig,
    ) -> Result<
        (
            crate::broadcast::Broadcast,
            crate::broadcast_pull::SourceStatus,
        ),
        crate::broadcast::BroadcastError,
    > {
        self.registry
            .attach_source(
                name,
                config,
                &self.seed_registry,
                &self.trackers,
                self.store_bytes,
                self.inbound_peer_port.clone(),
            )
            .await
    }
    pub async fn source_status(
        &self,
        name: &str,
    ) -> Result<Option<crate::broadcast_pull::SourceStatus>, crate::broadcast::BroadcastError> {
        self.registry.source_status(name).await
    }
    pub async fn detach_source(&self, name: &str) -> Result<(), crate::broadcast::BroadcastError> {
        self.registry.detach_source(name).await
    }

    pub async fn acquire_producer(
        &self,
        name: &str,
        kind: crate::broadcast::ProducerKind,
    ) -> Result<
        (
            crate::broadcast::Broadcast,
            crate::broadcast::ProducerSession,
        ),
        crate::broadcast::BroadcastError,
    > {
        let pair = self
            .registry
            .acquire_producer(
                name,
                name,
                &self.trackers,
                &self.seed_registry,
                self.store_bytes,
                kind,
            )
            .await?;
        self.ensure_announcing(name).await?;
        Ok(pair)
    }

    /// Reserve identity/index space without attaching an ingress source.
    pub async fn mint(
        &self,
        name: &str,
    ) -> Result<crate::broadcast::Broadcast, crate::broadcast::BroadcastError> {
        self.registry
            .mint(
                name,
                name,
                &self.trackers,
                &self.seed_registry,
                self.store_bytes,
            )
            .await
    }

    pub async fn ensure_announcing(
        &self,
        name: &str,
    ) -> Result<(), crate::broadcast::BroadcastError> {
        self.registry
            .ensure_announcing(name, &self.trackers, self.inbound_peer_port.clone())
            .await
    }

    pub fn close_admissions(&self) {
        self.registry.close_admissions();
    }

    pub async fn delete(&self, name: &str) -> Result<(), crate::broadcast::BroadcastError> {
        self.registry.delete(name).await
    }

    pub async fn shutdown(&self) -> Result<(), crate::broadcast::BroadcastError> {
        self.registry.shutdown().await
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
            "/broadcast/{name}/source",
            put(broadcast_source_put)
                .get(broadcast_source_get)
                .delete(broadcast_source_delete),
        )
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
            .route("/server/api", get(server_api))
            .route("/webui/api/service", get(server_api));
    }
    router.with_state(state)
}

#[cfg(test)]
mod tests;
