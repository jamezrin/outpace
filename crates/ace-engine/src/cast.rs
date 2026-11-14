//! Casting live streams to DLNA/UPnP media renderers on the LAN (`OUTPACE_ENABLE_CAST`).
//!
//! Discovery runs only when a client asks for targets: one bounded SSDP search, then one
//! bounded description fetch per replying device. There is no background loop; results are
//! cached for [`CACHE_TTL`]. Playing a stream tells the renderer, over AVTransport SOAP, to
//! fetch a per-cast URL on this engine's HTTP bind. That URL serves the ordinary shared live
//! session, so a renderer is just another HTTP client of it.
//!
//! Everything a renderer says is untrusted: see [`ssdp`], [`description`] and [`client`] for
//! the address, size, time and redirect limits.

pub mod client;
pub mod description;
pub mod soap;
pub mod ssdp;
pub mod url;

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use url::LanUrl;

/// How long a discovery result is reused before `GET /cast/targets` searches again.
pub const CACHE_TTL: Duration = Duration::from_secs(60);
const SEARCH: ssdp::SearchLimits = ssdp::SearchLimits {
    window: Duration::from_secs(2),
    max_replies: 64,
    max_locations: 16,
};
const DESCRIPTION_TIMEOUT: Duration = Duration::from_secs(2);
/// `GetTransportInfo` and `Stop`.
const SOAP_TIMEOUT: Duration = Duration::from_secs(5);
/// `SetAVTransportURI` and `Play`: many renderers open and probe the stream URL before they
/// answer, so this also covers the start of the stream body.
const PLAYBACK_SOAP_TIMEOUT: Duration = Duration::from_secs(15);
/// Renderers kept from one discovery, and so also the most concurrent casts.
const MAX_TARGETS: usize = 16;

/// Why a cast operation failed. Messages never carry renderer-supplied text beyond the
/// sanitized UPnP fault description.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CastError {
    UnknownTarget,
    /// `OUTPACE_BIND` is loopback: a renderer could not fetch the stream.
    LoopbackBind,
    /// The bind address cannot be reached from an IPv4 renderer.
    UnsupportedBind,
    Discovery,
    Unreachable,
    Timeout,
    Redirect,
    TooLarge,
    Malformed(&'static str),
    Forbidden(&'static str),
    RendererStatus(u16),
    Fault(soap::Fault),
}

impl fmt::Display for CastError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CastError::UnknownTarget => f.write_str("unknown cast target; list /cast/targets first"),
            CastError::LoopbackBind => f.write_str(
                "OUTPACE_BIND is a loopback address, so a renderer cannot fetch the stream; bind a LAN address such as 0.0.0.0:6878",
            ),
            CastError::UnsupportedBind => {
                f.write_str("OUTPACE_BIND is not reachable from an IPv4 renderer")
            }
            CastError::Discovery => f.write_str("ssdp search failed"),
            CastError::Unreachable => f.write_str("renderer unreachable"),
            CastError::Timeout => f.write_str("renderer timed out"),
            CastError::Redirect => f.write_str("renderer answered with a redirect"),
            CastError::TooLarge => f.write_str("renderer response too large"),
            CastError::Malformed(what) | CastError::Forbidden(what) => f.write_str(what),
            CastError::RendererStatus(status) => write!(f, "renderer answered http {status}"),
            CastError::Fault(fault) => match (&fault.code, &fault.description) {
                (Some(code), Some(text)) => write!(f, "renderer error {code}: {text}"),
                (Some(code), None) => write!(f, "renderer error {code}"),
                _ => f.write_str("renderer fault"),
            },
        }
    }
}

impl std::error::Error for CastError {}

impl CastError {
    /// Stable machine-readable code for API clients.
    pub fn code(&self) -> &'static str {
        match self {
            CastError::UnknownTarget => "unknown_target",
            CastError::LoopbackBind => "loopback_bind",
            CastError::UnsupportedBind => "unsupported_bind",
            CastError::Discovery => "discovery_failed",
            CastError::Unreachable => "renderer_unreachable",
            CastError::Timeout => "renderer_timeout",
            CastError::Redirect
            | CastError::TooLarge
            | CastError::Malformed(_)
            | CastError::Forbidden(_) => "renderer_invalid",
            CastError::RendererStatus(_) => "renderer_http_error",
            CastError::Fault(_) => "renderer_fault",
        }
    }
}

/// A discovered renderer with an AVTransport service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    /// Stable, path-safe id derived from the device UDN.
    pub id: String,
    pub name: String,
    pub manufacturer: Option<String>,
    pub model: Option<String>,
    pub address: Ipv4Addr,
    service_type: String,
    control: LanUrl,
}

impl Target {
    pub fn from_renderer(renderer: description::Renderer, location: &LanUrl) -> Self {
        use sha1::{Digest, Sha1};
        let key = renderer
            .udn
            .clone()
            .unwrap_or_else(|| format!("{location:?}"));
        let digest = Sha1::digest(key.as_bytes());
        Target {
            id: hex::encode(&digest[..8]),
            name: renderer.name,
            manufacturer: renderer.manufacturer,
            model: renderer.model,
            address: location.ip(),
            service_type: renderer.service_type,
            control: renderer.control,
        }
    }
}

/// An active cast: which stream a renderer was pointed at, and its open stream fetches.
#[derive(Clone, Debug)]
pub struct Cast {
    pub target: String,
    pub token: String,
    pub network: String,
    pub id: String,
    pub url: String,
    /// The MPEG-TS MIME type the renderer accepted; the stream route serves this type.
    pub mime: &'static str,
    connections: Arc<AtomicUsize>,
}

impl Cast {
    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::Relaxed)
    }
}

/// Counts one open stream response for a cast while it lives.
pub struct ConnectionGuard(Arc<AtomicUsize>);

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Snapshot returned by discovery.
pub struct Discovered {
    pub targets: Vec<Target>,
    pub age: Duration,
}

/// Renderer status: transport state plus whether it is still fetching our stream.
pub struct Status {
    pub transport: soap::TransportInfo,
    pub cast: Option<Cast>,
}

#[derive(Default)]
struct State {
    discovered_at: Option<Instant>,
    targets: Vec<Target>,
    casts: Vec<Cast>,
}

/// Shared cast state. Holds no tasks; every network operation runs inside a request.
#[derive(Default)]
pub struct CastService {
    http_bind: OnceLock<SocketAddr>,
    state: Mutex<State>,
    /// Single-flights discovery so concurrent `/cast/targets` calls share one search.
    discovery: tokio::sync::Mutex<()>,
}

impl CastService {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the HTTP listener's bound address; stream URLs are built from it.
    pub fn set_http_bind(&self, addr: SocketAddr) {
        let _ = self.http_bind.set(addr);
    }

    /// Cached targets when fresh, else one bounded discovery.
    pub async fn targets(&self, refresh: bool) -> Result<Discovered, CastError> {
        let _flight = self.discovery.lock().await;
        if !refresh {
            let state = self.state.lock().unwrap();
            if let Some(at) = state.discovered_at.filter(|at| at.elapsed() < CACHE_TTL) {
                return Ok(Discovered {
                    targets: state.targets.clone(),
                    age: at.elapsed(),
                });
            }
        }
        let targets = self.discover().await?;
        let mut state = self.state.lock().unwrap();
        state.discovered_at = Some(Instant::now());
        state.targets = targets.clone();
        Ok(Discovered {
            targets,
            age: Duration::ZERO,
        })
    }

    async fn discover(&self) -> Result<Vec<Target>, CastError> {
        let bind_ip = match self.http_bind.get().map(SocketAddr::ip) {
            Some(IpAddr::V4(ip)) if !ip.is_loopback() && !ip.is_unspecified() => ip,
            _ => Ipv4Addr::UNSPECIFIED,
        };
        let locations = ssdp::search(bind_ip, SEARCH).await?;
        let fetches = locations.iter().map(|location| async move {
            match fetch_renderer(location).await {
                Ok(renderer) => Some(Target::from_renderer(renderer, location)),
                Err(error) => {
                    crate::alog!("[dlna] skipped device at {}: {error}", location.ip());
                    None
                }
            }
        });
        let mut targets: Vec<Target> = Vec::new();
        for target in futures::future::join_all(fetches)
            .await
            .into_iter()
            .flatten()
        {
            if targets.len() < MAX_TARGETS && !targets.iter().any(|known| known.id == target.id) {
                targets.push(target);
            }
        }
        crate::alog!(
            "[dlna] discovery: {} devices replied, {} renderers usable",
            locations.len(),
            targets.len()
        );
        Ok(targets)
    }

    /// Whether `id` names a renderer from the last discovery.
    pub fn knows_target(&self, id: &str) -> bool {
        self.target(id).is_ok()
    }

    fn target(&self, id: &str) -> Result<Target, CastError> {
        self.state
            .lock()
            .unwrap()
            .targets
            .iter()
            .find(|target| target.id == id)
            .cloned()
            .ok_or(CastError::UnknownTarget)
    }

    /// The URL a renderer at `renderer` can fetch `/cast/s/<token>.ts` from.
    pub fn stream_url(&self, renderer: Ipv4Addr, token: &str) -> Result<String, CastError> {
        let bind = *self.http_bind.get().ok_or(CastError::UnsupportedBind)?;
        let ip = lan_address_for(bind.ip(), renderer)?;
        Ok(format!("http://{ip}:{}/cast/s/{token}.ts", bind.port()))
    }

    /// Check up front that the bind is reachable, before a stream is validated or touched.
    pub fn check_bind(&self) -> Result<(), CastError> {
        match self.http_bind.get().map(SocketAddr::ip) {
            Some(ip) if ip.is_loopback() => Err(CastError::LoopbackBind),
            Some(_) => Ok(()),
            None => Err(CastError::UnsupportedBind),
        }
    }

    /// Point `target_id` at a live stream and start playback.
    pub async fn play(
        &self,
        target_id: &str,
        network: &str,
        id: &str,
        title: &str,
    ) -> Result<Cast, CastError> {
        self.check_bind()?;
        let target = self.target(target_id)?;
        let token = hex::encode(rand::random::<[u8; 16]>());
        let url = self.stream_url(target.address, &token)?;
        let mut cast = Cast {
            target: target.id.clone(),
            token,
            network: network.to_string(),
            id: id.to_string(),
            url: url.clone(),
            mime: soap::TS_MIME_TYPES[0],
            connections: Arc::new(AtomicUsize::new(0)),
        };
        let previous = self.replace_cast(cast.clone());
        if previous.is_some() {
            // Some renderers refuse a new URI while playing; a failed Stop is not fatal.
            let _ = soap_call(
                &target,
                "Stop",
                soap::stop(&target.service_type),
                SOAP_TIMEOUT,
            )
            .await;
        }
        let result = async {
            let mut set = Err(CastError::UnknownTarget);
            for (attempt, mime) in soap::TS_MIME_TYPES.into_iter().enumerate() {
                if attempt > 0 {
                    // Renderers may check the stream's Content-Type too, so serve the new
                    // type before offering it.
                    cast.mime = mime;
                    self.replace_cast(cast.clone());
                }
                set = soap_call(
                    &target,
                    "SetAVTransportURI",
                    soap::set_av_transport_uri(&target.service_type, &url, title, mime),
                    PLAYBACK_SOAP_TIMEOUT,
                )
                .await;
                let illegal_mime = matches!(
                    &set,
                    Err(CastError::Fault(soap::Fault { code: Some(code), .. }))
                        if *code == soap::ILLEGAL_MIME_TYPE
                );
                if !illegal_mime {
                    break;
                }
            }
            set?;
            soap_call(
                &target,
                "Play",
                soap::play(&target.service_type),
                PLAYBACK_SOAP_TIMEOUT,
            )
            .await
        }
        .await;
        if let Err(error) = result {
            self.remove_cast(&target.id, Some(&cast.token));
            crate::alog!("[dlna] play on {:?} failed: {error}", target.name);
            return Err(error);
        }
        crate::alog!("[dlna] casting {network} stream to {:?}", target.name);
        Ok(cast)
    }

    /// Stop playback on `target_id` and forget its cast.
    pub async fn stop(&self, target_id: &str) -> Result<(), CastError> {
        let target = self.target(target_id)?;
        let removed = self.remove_cast(&target.id, None);
        let result = soap_call(
            &target,
            "Stop",
            soap::stop(&target.service_type),
            SOAP_TIMEOUT,
        )
        .await
        .map(drop);
        match &result {
            Ok(()) => crate::alog!("[dlna] stopped {:?} (cast active: {removed})", target.name),
            Err(error) => crate::alog!("[dlna] stop on {:?} failed: {error}", target.name),
        }
        result
    }

    /// `GetTransportInfo` plus the cast's open stream fetches.
    pub async fn status(&self, target_id: &str) -> Result<Status, CastError> {
        let target = self.target(target_id)?;
        let body = soap_call(
            &target,
            "GetTransportInfo",
            soap::get_transport_info(&target.service_type),
            SOAP_TIMEOUT,
        )
        .await?;
        let transport = soap::parse_transport_info(&body)?;
        let cast = self
            .state
            .lock()
            .unwrap()
            .casts
            .iter()
            .find(|cast| cast.target == target.id)
            .cloned();
        Ok(Status { transport, cast })
    }

    /// Resolve a stream token to its cast and count the connection until the guard drops.
    pub fn open_stream(&self, token: &str) -> Option<(Cast, ConnectionGuard)> {
        let state = self.state.lock().unwrap();
        let cast = state.casts.iter().find(|cast| cast.token == token)?.clone();
        cast.connections.fetch_add(1, Ordering::Relaxed);
        let guard = ConnectionGuard(cast.connections.clone());
        Some((cast, guard))
    }

    fn replace_cast(&self, cast: Cast) -> Option<Cast> {
        let mut state = self.state.lock().unwrap();
        let previous = state
            .casts
            .iter()
            .position(|known| known.target == cast.target)
            .map(|at| state.casts.swap_remove(at));
        if state.casts.len() >= MAX_TARGETS {
            state.casts.remove(0);
        }
        state.casts.push(cast);
        previous
    }

    fn remove_cast(&self, target: &str, token: Option<&str>) -> bool {
        let mut state = self.state.lock().unwrap();
        let before = state.casts.len();
        state
            .casts
            .retain(|cast| cast.target != target || token.is_some_and(|t| cast.token != t));
        state.casts.len() != before
    }

    #[cfg(test)]
    pub(crate) fn insert_cast_for_test(&self, target: &str, network: &str, id: &str) -> String {
        let token = hex::encode(rand::random::<[u8; 16]>());
        self.replace_cast(Cast {
            target: target.to_string(),
            token: token.clone(),
            network: network.to_string(),
            id: id.to_string(),
            url: String::new(),
            mime: soap::TS_MIME_TYPES[0],
            connections: Arc::new(AtomicUsize::new(0)),
        });
        token
    }

    #[cfg(test)]
    pub(crate) fn insert_target_for_test(&self, target: Target) {
        let mut state = self.state.lock().unwrap();
        state.discovered_at = Some(Instant::now());
        state.targets.push(target);
    }
}

/// Choose the engine address a renderer should fetch from.
fn lan_address_for(bind: IpAddr, renderer: Ipv4Addr) -> Result<Ipv4Addr, CastError> {
    if bind.is_loopback() {
        return Err(CastError::LoopbackBind);
    }
    match bind {
        IpAddr::V4(ip) if !ip.is_unspecified() => Ok(ip),
        ip if ip.is_unspecified() => {
            match ace_swarm::portmap::local_ip_toward(IpAddr::V4(renderer)) {
                Some(IpAddr::V4(local)) if !local.is_loopback() && !local.is_unspecified() => {
                    Ok(local)
                }
                _ => Err(CastError::UnsupportedBind),
            }
        }
        _ => Err(CastError::UnsupportedBind),
    }
}

async fn fetch_renderer(location: &LanUrl) -> Result<description::Renderer, CastError> {
    let response = client::send(client::Request {
        method: "GET",
        url: location,
        headers: &[],
        body: b"",
        max_body: description::MAX_DESCRIPTION,
        timeout: DESCRIPTION_TIMEOUT,
    })
    .await?;
    if response.status != 200 {
        return Err(CastError::RendererStatus(response.status));
    }
    description::parse(&response.body, location)
}

async fn soap_call(
    target: &Target,
    action: &str,
    body: String,
    timeout: Duration,
) -> Result<Vec<u8>, CastError> {
    let soap_action = soap::soap_action(&target.service_type, action);
    let response = client::send(client::Request {
        method: "POST",
        url: &target.control,
        headers: &[
            ("Content-Type", "text/xml; charset=\"utf-8\""),
            ("SOAPACTION", &soap_action),
        ],
        body: body.as_bytes(),
        max_body: soap::MAX_SOAP_RESPONSE,
        timeout,
    })
    .await?;
    if response.status == 200 {
        return Ok(response.body);
    }
    match soap::parse_fault(&response.body) {
        Some(fault) => Err(CastError::Fault(fault)),
        None => Err(CastError::RendererStatus(response.status)),
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    /// A target whose control URL is a local fake renderer (bypassing the LAN-only check,
    /// which applies at discovery).
    pub fn target(id: &str, control: std::net::SocketAddrV4) -> Target {
        Target {
            id: id.to_string(),
            name: "Fake Renderer".into(),
            manufacturer: None,
            model: None,
            address: Ipv4Addr::new(192, 168, 1, 40),
            service_type: "urn:schemas-upnp-org:service:AVTransport:1".into(),
            control: LanUrl {
                addr: control,
                path: "/ctl".into(),
            },
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
