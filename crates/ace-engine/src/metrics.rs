//! Opt-in Prometheus text exposition (`GET /metrics`, `OUTPACE_METRICS`).
//!
//! Hand-written over existing stats snapshots so no metrics crate is pulled in. Label hygiene
//! is a hard rule: no content id, infohash, stream or broadcast name, token or URL ever becomes
//! a label value. Per-stream values are summed per provider network, and HTTP request series
//! use the matched route template (`/streams/{network}/{id}/status`), never the raw path.

use crate::manager::StreamManager;
use crate::provider::SourceStats;
use axum::extract::{MatchedPath, Request, State};
use axum::http::{header, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// Prometheus text format content type (exposition format 0.0.4).
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Scrapes that may collect a snapshot at once. Further scrapes are refused, never queued,
/// so a misbehaving scraper cannot stack up session walks on the engine it observes.
const SCRAPE_CONCURRENCY: usize = 2;
/// Upper bound on collecting one snapshot. Prometheus records a refusal as a failed scrape.
const SNAPSHOT_DEADLINE: Duration = Duration::from_secs(2);
const RETRY_AFTER_SECS: &str = "1";

#[derive(Debug, Default, Clone, Copy, PartialEq)]
struct RequestTotals {
    count: u64,
    seconds: f64,
}

/// Clients may send arbitrary extension methods; collapse them so they cannot mint series.
fn method_label(method: &Method) -> &'static str {
    match *method {
        Method::GET => "GET",
        Method::HEAD => "HEAD",
        Method::POST => "POST",
        Method::PUT => "PUT",
        Method::DELETE => "DELETE",
        Method::OPTIONS => "OPTIONS",
        Method::PATCH => "PATCH",
        _ => "other",
    }
}

/// HTTP request counters keyed by `(route template, method, status code)`. Cardinality is
/// bounded by the router's route table (unmatched requests never reach `route_layer`), the
/// fixed method set of [`method_label`], and the status codes handlers return.
#[derive(Default)]
pub struct HttpMetrics {
    requests: Mutex<BTreeMap<(String, String, u16), RequestTotals>>,
}

impl HttpMetrics {
    fn record(&self, route: &str, method: &Method, status: u16, seconds: f64) {
        let mut requests = self.requests.lock().unwrap_or_else(|e| e.into_inner());
        let totals = requests
            .entry((route.to_owned(), method_label(method).to_owned(), status))
            .or_default();
        totals.count += 1;
        totals.seconds += seconds;
    }

    fn snapshot(&self) -> BTreeMap<(String, String, u16), RequestTotals> {
        self.requests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

#[derive(Clone)]
struct MetricsState {
    manager: Arc<StreamManager>,
    networks: Arc<[String]>,
    http: Arc<HttpMetrics>,
    started: Instant,
    scrapes: Arc<Semaphore>,
}

/// Add `GET /metrics` and per-route request accounting to a fully assembled router.
pub fn instrument(app: Router, manager: Arc<StreamManager>, networks: &[String]) -> Router {
    let state = MetricsState {
        manager,
        networks: networks.into(),
        http: Arc::default(),
        started: Instant::now(),
        scrapes: Arc::new(Semaphore::new(SCRAPE_CONCURRENCY)),
    };
    app.route("/metrics", get(metrics).with_state(state.clone()))
        .route_layer(axum::middleware::from_fn_with_state(state, track))
}

async fn track(State(state): State<MetricsState>, request: Request, next: Next) -> Response {
    let Some(route) = request
        .extensions()
        .get::<MatchedPath>()
        .map(|path| path.as_str().to_owned())
    else {
        return next.run(request).await;
    };
    let method = request.method().clone();
    let start = Instant::now();
    let response = next.run(request).await;
    // Streaming bodies (MPEG-TS, segments) keep flowing after this point: the duration is
    // time to response headers, not transfer time.
    state.http.record(
        &route,
        &method,
        response.status().as_u16(),
        start.elapsed().as_secs_f64(),
    );
    response
}

/// Run `collect` only with a free scrape permit and within `deadline`. `None` means the scrape
/// was refused or timed out; dropping `collect` cancels it, which is safe for read-only stats.
async fn bounded_collect<T>(
    scrapes: &Semaphore,
    deadline: Duration,
    collect: impl Future<Output = T>,
) -> Option<T> {
    let _permit = scrapes.try_acquire().ok()?;
    tokio::time::timeout(deadline, collect).await.ok()
}

fn scrape_unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, RETRY_AFTER_SECS)],
    )
        .into_response()
}

async fn metrics(State(state): State<MetricsState>) -> Response {
    let collect = async {
        Snapshot {
            version: env!("CARGO_PKG_VERSION"),
            uptime_seconds: state.started.elapsed().as_secs_f64(),
            memory: allocator_stats(),
            networks: state.networks.to_vec(),
            sessions: state.manager.session_stats().await,
            admission: state.manager.admission_status(),
            http: state.http.snapshot(),
        }
    };
    match bounded_collect(&state.scrapes, SNAPSHOT_DEADLINE, collect).await {
        Some(snapshot) => {
            ([(header::CONTENT_TYPE, CONTENT_TYPE)], render(&snapshot)).into_response()
        }
        None => scrape_unavailable(),
    }
}

#[cfg(all(
    not(target_env = "msvc"),
    not(target_os = "android"),
    target_pointer_width = "64"
))]
fn allocator_stats() -> Vec<(&'static str, u64)> {
    use tikv_jemalloc_ctl::{epoch, stats};
    if epoch::advance().is_err() {
        return Vec::new();
    }
    // A failed read is omitted rather than reported as a misleading zero.
    [
        ("allocated", stats::allocated::read()),
        ("active", stats::active::read()),
        ("resident", stats::resident::read()),
        ("retained", stats::retained::read()),
        ("mapped", stats::mapped::read()),
    ]
    .into_iter()
    .filter_map(|(kind, value)| value.ok().map(|v| (kind, v as u64)))
    .collect()
}

#[cfg(not(all(
    not(target_env = "msvc"),
    not(target_os = "android"),
    target_pointer_width = "64"
)))]
fn allocator_stats() -> Vec<(&'static str, u64)> {
    Vec::new()
}

struct Snapshot {
    version: &'static str,
    uptime_seconds: f64,
    memory: Vec<(&'static str, u64)>,
    networks: Vec<String>,
    sessions: Vec<(String, u64, SourceStats)>,
    admission: (usize, Option<usize>),
    http: BTreeMap<(String, String, u16), RequestTotals>,
}

#[derive(Default)]
struct NetworkTotals {
    sessions: u64,
    subscribers: u64,
    peers: u64,
    peers_served: u64,
    download_rate: u64,
    upload_rate: u64,
    downloaded: u64,
    uploaded: u64,
    active_upstreams: u64,
    upstream_reconnects: u64,
    stale_teardowns: u64,
    skipped_evicted_gaps: u64,
    signature_rejects: u64,
}

/// `(metric name, help text, value extractor)` for one per-network gauge.
type NetworkGauge = (&'static str, &'static str, fn(&NetworkTotals) -> u64);

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn header(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

fn render(s: &Snapshot) -> String {
    let mut out = String::new();

    header(
        &mut out,
        "outpace_build_info",
        "gauge",
        "Build version; always 1.",
    );
    let _ = writeln!(
        out,
        "outpace_build_info{{version=\"{}\"}} 1",
        escape_label(s.version)
    );
    header(
        &mut out,
        "outpace_uptime_seconds",
        "gauge",
        "Seconds since the HTTP server started.",
    );
    let _ = writeln!(out, "outpace_uptime_seconds {:.3}", s.uptime_seconds);

    if !s.memory.is_empty() {
        header(
            &mut out,
            "outpace_allocator_bytes",
            "gauge",
            "jemalloc allocator statistics by kind.",
        );
        for (kind, value) in &s.memory {
            let _ = writeln!(out, "outpace_allocator_bytes{{kind=\"{kind}\"}} {value}");
        }
    }

    let (occupied, limit) = s.admission;
    header(
        &mut out,
        "outpace_admission_occupied",
        "gauge",
        "Admitted source resources, including pending opens.",
    );
    let _ = writeln!(out, "outpace_admission_occupied {occupied}");
    if let Some(limit) = limit {
        header(
            &mut out,
            "outpace_admission_limit",
            "gauge",
            "Configured cap on concurrent sources (OUTPACE_MAX_SESSIONS).",
        );
        let _ = writeln!(out, "outpace_admission_limit {limit}");
    }

    // Every registered network gets a series so dashboards see zeros, not gaps.
    let mut totals: BTreeMap<&str, NetworkTotals> = s
        .networks
        .iter()
        .map(|n| (n.as_str(), NetworkTotals::default()))
        .collect();
    for (network, subscribers, stats) in &s.sessions {
        let t = totals.entry(network.as_str()).or_default();
        t.sessions += 1;
        t.subscribers += subscribers;
        t.peers += u64::from(stats.peers);
        t.peers_served += u64::from(stats.peers_served);
        t.download_rate += stats.download_rate;
        t.upload_rate += stats.upload_rate;
        t.downloaded += stats.downloaded;
        t.uploaded += stats.uploaded;
        if let Some(r) = &stats.recovery {
            t.active_upstreams += u64::from(r.active_upstreams);
            t.upstream_reconnects += r.upstream_reconnects;
            t.stale_teardowns += r.stale_teardowns;
            t.skipped_evicted_gaps += r.skipped_evicted_gaps;
            t.signature_rejects += r.signature_rejects;
        }
    }

    // Byte and recovery totals are summed over live sessions only, so they drop when a session
    // ends: they are gauges, not monotonic counters.
    let per_network: [NetworkGauge; 13] = [
        ("outpace_sessions", "Active stream sessions.", |t| {
            t.sessions
        }),
        (
            "outpace_subscribers",
            "Clients attached to active sessions.",
            |t| t.subscribers,
        ),
        (
            "outpace_peers",
            "Connected swarm peers across active sessions.",
            |t| t.peers,
        ),
        (
            "outpace_peers_served",
            "Distinct peers served at least one chunk, across active sessions.",
            |t| t.peers_served,
        ),
        (
            "outpace_active_upstreams",
            "Peers currently admitted as download upstreams.",
            |t| t.active_upstreams,
        ),
        (
            "outpace_download_bytes_per_second",
            "Measured reassembled download rate in bytes per second.",
            |t| t.download_rate,
        ),
        (
            "outpace_upload_bytes_per_second",
            "Measured upload rate in bytes per second.",
            |t| t.upload_rate,
        ),
        (
            "outpace_session_downloaded_bytes",
            "Bytes downloaded, summed over active sessions.",
            |t| t.downloaded,
        ),
        (
            "outpace_session_uploaded_bytes",
            "Bytes uploaded to peers, summed over active sessions.",
            |t| t.uploaded,
        ),
        (
            "outpace_session_upstream_reconnects",
            "Upstream reconnects, summed over active sessions.",
            |t| t.upstream_reconnects,
        ),
        (
            "outpace_session_stale_teardowns",
            "Stale upstream teardowns, summed over active sessions.",
            |t| t.stale_teardowns,
        ),
        (
            "outpace_session_skipped_evicted_gaps",
            "Evicted gaps skipped, summed over active sessions.",
            |t| t.skipped_evicted_gaps,
        ),
        (
            "outpace_session_signature_rejects",
            "Chunks rejected for bad signatures, summed over active sessions.",
            |t| t.signature_rejects,
        ),
    ];
    for (name, help, value) in per_network {
        header(&mut out, name, "gauge", help);
        for (network, t) in &totals {
            let _ = writeln!(
                out,
                "{name}{{network=\"{}\"}} {}",
                escape_label(network),
                value(t)
            );
        }
    }

    header(
        &mut out,
        "outpace_http_requests_total",
        "counter",
        "HTTP requests by matched route template, method and status code.",
    );
    for ((route, method, status), totals) in &s.http {
        let _ = writeln!(
            out,
            "outpace_http_requests_total{{route=\"{}\",method=\"{}\",status=\"{status}\"}} {}",
            escape_label(route),
            escape_label(method),
            totals.count
        );
    }
    header(
        &mut out,
        "outpace_http_response_header_seconds_total",
        "counter",
        "Cumulative seconds until response headers, by route template, method and status.",
    );
    for ((route, method, status), totals) in &s.http {
        let _ = writeln!(
            out,
            "outpace_http_response_header_seconds_total{{route=\"{}\",method=\"{}\",status=\"{status}\"}} {:.6}",
            escape_label(route),
            escape_label(method),
            totals.seconds
        );
    }
    out
}

#[cfg(test)]
mod tests;
