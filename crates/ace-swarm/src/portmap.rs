//! Inbound-peer-port mapping on the home gateway (UPnP-IGD / NAT-PMP / PCP).
//!
//! Mirrors what the reference Acestream engine does for NAT reachability: its
//! `acestream.upnp` module runs a `__forward` thread that maps the peer port on the
//! gateway so unreachable-by-default home peers can be dialed. We do the standard,
//! swarm-neutral equivalent — **no** hole punching, STUN/ICE, or Acestream APIs
//! (see `docs/protocol/notes` and issue #19). Two backends:
//!
//! * **UPnP-IGD** via [`igd_next`] (async/tokio) — SSDP-discovers the gateway and calls
//!   `AddPortMapping`.
//! * **NAT-PMP / PCP** via [`crab_nat`] — a UDP fallback for gateways that speak NAT-PMP
//!   but not UPnP.
//!
//! Everything here is **best-effort and non-fatal**: any failure logs a warning
//! (mirroring the engine's `Failed to init port forward`) and returns `None`; the daemon
//! keeps running with a NAT-bound listener exactly as it does today. There are no
//! `.unwrap()`s on network results and nothing here panics.
//!
//! The resolved external endpoint is returned via [`PortMapHandle::endpoint`] so the
//! announce path (issue #21) can advertise the reachable address. This module stops at
//! producing + logging the endpoint; it does not wire announce.
//!
//! ## Gating
//!
//! The caller (`ace-engine`) only invokes [`spawn_port_mapping`] when **both**
//! `enable_inbound` and the new `enable_port_mapping` (default **off**) are set. With the
//! defaults unchanged, none of this code runs.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroU16;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

/// Which gateway backend(s) to use.
///
/// Parsed from config (`port_map_backend`) / env (`OUTPACE_PORT_MAP_BACKEND`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PortMapBackend {
    /// Try UPnP-IGD first, then fall back to NAT-PMP/PCP.
    #[default]
    Auto,
    /// UPnP-IGD only.
    Upnp,
    /// NAT-PMP/PCP only.
    Natpmp,
    /// Disable port mapping entirely.
    None,
}

impl PortMapBackend {
    /// The ordered list of concrete backends to attempt for this selection.
    fn candidates(self) -> &'static [Backend] {
        match self {
            PortMapBackend::Auto => &[Backend::Upnp, Backend::NatPmp],
            PortMapBackend::Upnp => &[Backend::Upnp],
            PortMapBackend::Natpmp => &[Backend::NatPmp],
            PortMapBackend::None => &[],
        }
    }
}

impl FromStr for PortMapBackend {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(PortMapBackend::Auto),
            "upnp" | "igd" => Ok(PortMapBackend::Upnp),
            "natpmp" | "nat-pmp" | "nat_pmp" | "pcp" => Ok(PortMapBackend::Natpmp),
            "none" | "off" | "disabled" => Ok(PortMapBackend::None),
            other => Err(format!(
                "invalid port-map backend {other:?} (expected auto|upnp|natpmp|none)"
            )),
        }
    }
}

impl fmt::Display for PortMapBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            PortMapBackend::Auto => "auto",
            PortMapBackend::Upnp => "upnp",
            PortMapBackend::Natpmp => "natpmp",
            PortMapBackend::None => "none",
        };
        f.write_str(s)
    }
}

/// A single concrete backend actually attempted (never `Auto`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Backend {
    Upnp,
    NatPmp,
}

/// Inputs for a mapping request.
#[derive(Debug, Clone)]
pub struct PortMapConfig {
    /// Which backend(s) to try.
    pub backend: PortMapBackend,
    /// The local listener port to map to (`config.peer_listen` port).
    pub internal_port: u16,
    /// Optional external-port override. When `None`, request the same port as `internal_port`.
    pub external_port: Option<u16>,
    /// Requested lease duration, in seconds. `0` means "infinite" for UPnP; NAT-PMP/PCP
    /// gateways clamp to their own maximum. The gateway may return a shorter lease, which the
    /// renewal task honours.
    pub lease_seconds: u32,
}

impl PortMapConfig {
    /// The external port we will *request*: the override if set, else the internal port.
    pub fn requested_external_port(&self) -> u16 {
        self.external_port.unwrap_or(self.internal_port)
    }
}

/// Default requested lease. UPnP IGDs commonly accept ~1–2 h; NAT-PMP recommends 2 h.
pub const DEFAULT_LEASE_SECONDS: u32 = 3600;

/// The resolved, externally-reachable endpoint produced by a successful mapping.
///
/// This is the value the announce path (#21) consumes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappedEndpoint {
    /// External IP as reported by the gateway. `None` when the backend does not report one
    /// (NAT-PMP does not return the external address as part of the map response; PCP does).
    pub external_ip: Option<IpAddr>,
    /// The external port the gateway actually mapped.
    pub external_port: u16,
    /// The lease the gateway granted. The renewal task refreshes before this elapses.
    pub lease_duration: Duration,
    /// Which backend produced the mapping (`"upnp"` | `"natpmp"`).
    pub backend: &'static str,
}

/// Errors from a mapping attempt. Always non-fatal to the daemon — logged and swallowed.
#[derive(Debug)]
pub enum PortMapError {
    /// UPnP gateway discovery / control call failed.
    Upnp(String),
    /// NAT-PMP/PCP request failed.
    NatPmp(String),
    /// Could not determine the local/gateway address needed for a NAT-PMP request.
    NoGateway(String),
    /// The requested port was invalid (e.g. zero for NAT-PMP, which forbids it).
    InvalidPort(String),
}

impl fmt::Display for PortMapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PortMapError::Upnp(e) => write!(f, "upnp: {e}"),
            PortMapError::NatPmp(e) => write!(f, "natpmp: {e}"),
            PortMapError::NoGateway(e) => write!(f, "gateway discovery: {e}"),
            PortMapError::InvalidPort(e) => write!(f, "invalid port: {e}"),
        }
    }
}

impl std::error::Error for PortMapError {}

/// A handle to a backgrounded port-mapping task.
///
/// The gateway discovery + mapping runs entirely in the spawned task, so obtaining a handle
/// never blocks daemon startup on slow SSDP / NAT-PMP retries. The resolved endpoint is
/// delivered asynchronously via a [`watch`] channel once (and if) established; read it with
/// [`PortMapHandle::endpoint`] or await it with [`PortMapHandle::wait_for_endpoint`].
///
/// Dropping the handle signals the task to delete the mapping on a best-effort basis; prefer
/// [`PortMapHandle::shutdown`] for a graceful, awaited teardown.
pub struct PortMapHandle {
    /// `None` until the background task establishes a mapping (and after the task ends the
    /// sender is dropped, so `changed()` returns `Err`).
    endpoint_rx: watch::Receiver<Option<MappedEndpoint>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl PortMapHandle {
    /// The resolved external endpoint if the background task has established one yet, else
    /// `None`. Non-blocking snapshot. Hand this to the announce path (#21).
    pub fn endpoint(&self) -> Option<MappedEndpoint> {
        self.endpoint_rx.borrow().clone()
    }

    /// A [`watch`] receiver for the resolved endpoint (`None` until established). The announce
    /// path (#21) can clone this and await changes without holding the handle.
    pub fn endpoint_receiver(&self) -> watch::Receiver<Option<MappedEndpoint>> {
        self.endpoint_rx.clone()
    }

    /// Await the endpoint being established. Returns `Some` once the mapping succeeds, or
    /// `None` if the background task ends first (all backends failed, or shutdown raced
    /// discovery).
    pub async fn wait_for_endpoint(&self) -> Option<MappedEndpoint> {
        let mut rx = self.endpoint_rx.clone();
        loop {
            if let Some(ep) = rx.borrow_and_update().clone() {
                return Some(ep);
            }
            if rx.changed().await.is_err() {
                return None;
            }
        }
    }

    /// Gracefully tear down: signal the background task to delete the gateway mapping (if it
    /// established one, or to abandon in-flight discovery) and wait for it to finish.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for PortMapHandle {
    fn drop(&mut self) {
        // Best-effort: nudge the renewal task to delete the mapping. If `shutdown` already ran
        // these are `None`. We can't await in `Drop`, so the task removes the mapping itself.
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// The trait behind the actual gateway call, so the non-fatal wrapper can be exercised with
/// an injected failure in unit tests (no real gateway required).
#[async_trait::async_trait]
trait MapAttempt: Send + Sync {
    /// Establish the mapping, returning the live session on success.
    async fn attempt(&self, cfg: &PortMapConfig) -> Result<ActiveMapping, PortMapError>;
    /// Concrete backend name, for logging.
    fn name(&self) -> &'static str;
}

/// Try each candidate backend in order; the first success wins. Every failure is logged and
/// swallowed. Returns `None` if none succeed (or there are no candidates) — the daemon
/// continues unmapped.
async fn establish(cfg: &PortMapConfig, attempts: &[Box<dyn MapAttempt>]) -> Option<ActiveMapping> {
    for attempt in attempts {
        match attempt.attempt(cfg).await {
            Ok(active) => return Some(active),
            Err(e) => {
                // Matches the reference engine's `Failed to init port forward` log line.
                crate::alog!(
                    "[portmap] {} failed to init port forward: {e} (continuing)",
                    attempt.name()
                );
            }
        }
    }
    None
}

/// Spawn a background task that maps the inbound peer port on the gateway, then renews the
/// lease until shutdown.
///
/// Returns immediately with a [`PortMapHandle`] — **all** gateway discovery and mapping happens
/// in the spawned task, so daemon startup never blocks on slow SSDP / NAT-PMP retries. Returns
/// `None` only when the backend is [`PortMapBackend::None`] (nothing to do). Best-effort and
/// non-fatal: mapping failures are logged and the daemon continues NAT-bound; never panics. The
/// caller must have already checked its gating (`enable_inbound` && `enable_port_mapping`).
///
/// Must be called from within a Tokio runtime.
pub fn spawn_port_mapping(cfg: PortMapConfig) -> Option<PortMapHandle> {
    let candidates = cfg.backend.candidates();
    if candidates.is_empty() {
        return None;
    }

    // Build the concrete attempts for the selected backends (owned, so they can move into the
    // background task).
    let attempts: Vec<Box<dyn MapAttempt>> = candidates
        .iter()
        .map(|b| match b {
            Backend::Upnp => Box::new(UpnpMapper) as Box<dyn MapAttempt>,
            Backend::NatPmp => Box::new(NatPmpMapper) as Box<dyn MapAttempt>,
        })
        .collect();

    Some(spawn_with_attempts(cfg, attempts))
}

/// Spawn the background discovery + renewal task over a set of (possibly injected) attempts.
/// Shared by the public entry point and unit tests.
fn spawn_with_attempts(cfg: PortMapConfig, attempts: Vec<Box<dyn MapAttempt>>) -> PortMapHandle {
    let (endpoint_tx, endpoint_rx) = watch::channel(None);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(run_port_mapping(cfg, attempts, endpoint_tx, shutdown_rx));
    PortMapHandle {
        endpoint_rx,
        shutdown_tx: Some(shutdown_tx),
        task: Some(task),
    }
}

/// The background task body: discover + map (cancellable by shutdown), publish the endpoint,
/// then renew until shutdown. Never panics.
async fn run_port_mapping(
    cfg: PortMapConfig,
    attempts: Vec<Box<dyn MapAttempt>>,
    endpoint_tx: watch::Sender<Option<MappedEndpoint>>,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    // Discovery/mapping can take tens of seconds against an unresponsive gateway; abandon it
    // cleanly if the daemon shuts down first (nothing is mapped yet, so nothing to remove).
    let active = tokio::select! {
        biased;
        _ = &mut shutdown_rx => return,
        active = establish(&cfg, &attempts) => active,
    };
    let Some(active) = active else {
        crate::alog!("[portmap] no backend could map the inbound port; continuing NAT-bound");
        return;
    };

    let endpoint = active.endpoint();
    crate::alog!(
        "[portmap] mapped external {}:{} -> local :{} via {} (lease {}s)",
        endpoint
            .external_ip
            .map(|ip| ip.to_string())
            .unwrap_or_else(|| "?".to_string()),
        endpoint.external_port,
        cfg.internal_port,
        endpoint.backend,
        endpoint.lease_duration.as_secs(),
    );
    // Publish for the announce path (#21). Ignored if all receivers were already dropped.
    let _ = endpoint_tx.send(Some(endpoint.clone()));

    renewal_loop(active, cfg, endpoint, endpoint_tx, shutdown_rx).await;
}

/// Refresh the mapping before its lease expires; delete it on shutdown signal.
async fn renewal_loop(
    mut active: ActiveMapping,
    cfg: PortMapConfig,
    endpoint: MappedEndpoint,
    endpoint_tx: watch::Sender<Option<MappedEndpoint>>,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    let lease = if endpoint.lease_duration.is_zero() {
        Duration::from_secs(30)
    } else {
        endpoint.lease_duration
    };
    let mut expires_at = tokio::time::Instant::now() + lease;
    loop {
        // Renew at ~half the lease so a single missed refresh does not drop the mapping.
        let refresh_in = lease / 2;
        tokio::select! {
            biased;
            _ = &mut shutdown_rx => {
                match active.remove(&cfg).await {
                    Ok(()) => crate::alog!(
                        "[portmap] removed mapping for external port {}",
                        endpoint.external_port
                    ),
                    Err(e) => crate::alog!(
                        "[portmap] failed to remove mapping for external port {}: {e}",
                        endpoint.external_port
                    ),
                }
                return;
            }
            _ = tokio::time::sleep_until(expires_at) => {
                crate::alog!("[portmap] mapping lease for external port {} expired; withdrawing endpoint", endpoint.external_port);
                let _ = endpoint_tx.send(None);
                return;
            }
            _ = tokio::time::sleep(refresh_in) => {
                match active.renew(&cfg).await {
                    Ok(()) => {
                        expires_at = tokio::time::Instant::now() + lease;
                        crate::alog!("[portmap] renewed mapping for external port {}", endpoint.external_port);
                    }
                    Err(e) => crate::alog!(
                        "[portmap] failed to renew mapping for external port {}: {e} (continuing)",
                        endpoint.external_port
                    ),
                }
            }
        }
    }
}

/// A live, backend-specific mapping capable of renew/remove.
enum ActiveMapping {
    Upnp(Box<UpnpActive>),
    NatPmp(Box<crab_nat::PortMapping>),
    /// A no-op mapping used by unit tests to exercise the full background flow (publish +
    /// renew + remove) without a real gateway.
    #[cfg(test)]
    Fake(MappedEndpoint),
    #[cfg(test)]
    FakeRenewFailure(MappedEndpoint),
}

impl ActiveMapping {
    fn endpoint(&self) -> MappedEndpoint {
        match self {
            ActiveMapping::Upnp(a) => MappedEndpoint {
                external_ip: a.external_ip,
                external_port: a.external_port,
                lease_duration: a.lease,
                backend: "upnp",
            },
            ActiveMapping::NatPmp(m) => MappedEndpoint {
                external_ip: natpmp_external_ip(m),
                external_port: m.external_port().get(),
                lease_duration: Duration::from_secs(u64::from(m.lifetime())),
                backend: "natpmp",
            },
            #[cfg(test)]
            ActiveMapping::Fake(ep) => ep.clone(),
            #[cfg(test)]
            ActiveMapping::FakeRenewFailure(ep) => ep.clone(),
        }
    }

    async fn renew(&mut self, cfg: &PortMapConfig) -> Result<(), PortMapError> {
        match self {
            ActiveMapping::Upnp(a) => a.renew(cfg).await,
            ActiveMapping::NatPmp(m) => m
                .renew()
                .await
                .map_err(|e| PortMapError::NatPmp(e.to_string())),
            #[cfg(test)]
            ActiveMapping::Fake(_) => Ok(()),
            #[cfg(test)]
            ActiveMapping::FakeRenewFailure(_) => {
                Err(PortMapError::Upnp("injected renewal failure".into()))
            }
        }
    }

    async fn remove(self, cfg: &PortMapConfig) -> Result<(), PortMapError> {
        match self {
            ActiveMapping::Upnp(a) => a.remove(cfg).await,
            ActiveMapping::NatPmp(m) => (*m)
                .try_drop()
                .await
                .map_err(|(e, _)| PortMapError::NatPmp(e.to_string())),
            #[cfg(test)]
            ActiveMapping::Fake(_) | ActiveMapping::FakeRenewFailure(_) => Ok(()),
        }
    }
}

/// PCP reports the external IP; NAT-PMP proper does not (as part of the map response).
fn natpmp_external_ip(m: &crab_nat::PortMapping) -> Option<IpAddr> {
    match m.mapping_type() {
        crab_nat::PortMappingType::Pcp { external_ip, .. } => Some(external_ip),
        crab_nat::PortMappingType::NatPmp => None,
    }
}

// ---------------------------------------------------------------------------------------------
// UPnP-IGD backend
// ---------------------------------------------------------------------------------------------

type TokioGateway = igd_next::aio::Gateway<igd_next::aio::tokio::Tokio>;

const MAPPING_DESCRIPTION: &str = "outpace peer";

struct UpnpMapper;

/// A live UPnP mapping: keeps the discovered gateway for renew/remove without re-searching.
struct UpnpActive {
    gateway: TokioGateway,
    local_addr: SocketAddr,
    external_port: u16,
    external_ip: Option<IpAddr>,
    lease: Duration,
}

impl UpnpActive {
    async fn renew(&self, cfg: &PortMapConfig) -> Result<(), PortMapError> {
        // Re-issue AddPortMapping with the same external port to refresh the lease.
        self.gateway
            .add_port(
                igd_next::PortMappingProtocol::TCP,
                self.external_port,
                self.local_addr,
                cfg.lease_seconds,
                MAPPING_DESCRIPTION,
            )
            .await
            .map_err(|e| PortMapError::Upnp(e.to_string()))
    }

    async fn remove(&self, _cfg: &PortMapConfig) -> Result<(), PortMapError> {
        self.gateway
            .remove_port(igd_next::PortMappingProtocol::TCP, self.external_port)
            .await
            .map_err(|e| PortMapError::Upnp(e.to_string()))
    }
}

#[async_trait::async_trait]
impl MapAttempt for UpnpMapper {
    fn name(&self) -> &'static str {
        "upnp"
    }

    async fn attempt(&self, cfg: &PortMapConfig) -> Result<ActiveMapping, PortMapError> {
        let gateway = igd_next::aio::tokio::search_gateway(igd_next::SearchOptions::default())
            .await
            .map_err(|e| PortMapError::Upnp(format!("gateway search: {e}")))?;

        // The local address the gateway should forward to. We bind the peer listener on all
        // interfaces, so tell the gateway to route to our LAN IP on that port.
        let local_ip = local_ip_toward_gateway(gateway.addr.ip())
            .ok_or_else(|| PortMapError::NoGateway("could not resolve local LAN IP".to_string()))?;
        let local_addr = SocketAddr::new(local_ip, cfg.internal_port);

        let external_port = cfg.requested_external_port();
        gateway
            .add_port(
                igd_next::PortMappingProtocol::TCP,
                external_port,
                local_addr,
                cfg.lease_seconds,
                MAPPING_DESCRIPTION,
            )
            .await
            .map_err(|e| PortMapError::Upnp(e.to_string()))?;

        // Best-effort external IP; failure here does not fail the mapping.
        let external_ip = gateway.get_external_ip().await.ok();

        Ok(ActiveMapping::Upnp(Box::new(UpnpActive {
            gateway,
            local_addr,
            external_port,
            external_ip,
            lease: Duration::from_secs(u64::from(cfg.lease_seconds)),
        })))
    }
}

// ---------------------------------------------------------------------------------------------
// NAT-PMP / PCP backend
// ---------------------------------------------------------------------------------------------

struct NatPmpMapper;

#[async_trait::async_trait]
impl MapAttempt for NatPmpMapper {
    fn name(&self) -> &'static str {
        "natpmp"
    }

    async fn attempt(&self, cfg: &PortMapConfig) -> Result<ActiveMapping, PortMapError> {
        let internal_port = NonZeroU16::new(cfg.internal_port)
            .ok_or_else(|| PortMapError::InvalidPort("internal port cannot be 0".to_string()))?;

        // NAT-PMP/PCP needs the gateway IP and (for PCP) our client IP. crab-nat does no
        // gateway discovery, so we derive both from the default route. This is a heuristic
        // (gateway assumed at `.1` of our LAN /24); a router at another address won't be found
        // by the NAT-PMP path. Documented as best-effort; UPnP discovers the real gateway.
        let (client_ip, gateway_ip) = default_route()
            .ok_or_else(|| PortMapError::NoGateway("no default route".to_string()))?;

        let external_port = cfg
            .external_port
            .and_then(NonZeroU16::new)
            .or_else(|| NonZeroU16::new(cfg.internal_port));
        let options = crab_nat::PortMappingOptions {
            external_port,
            lifetime_seconds: Some(cfg.lease_seconds),
            timeout_config: None,
        };

        let mapping = crab_nat::PortMapping::new(
            gateway_ip.into(),
            client_ip,
            crab_nat::InternetProtocol::Tcp,
            internal_port,
            options,
        )
        .await
        .map_err(|e| PortMapError::NatPmp(e.to_string()))?;

        Ok(ActiveMapping::NatPmp(Box::new(mapping)))
    }
}

// ---------------------------------------------------------------------------------------------
// Local/gateway address discovery (heuristic, best-effort)
// ---------------------------------------------------------------------------------------------

/// Resolve the local LAN IP used to reach `gateway`. Uses the "connect a UDP socket and read
/// its local address" trick — no packets are sent; the OS just resolves the outbound route.
fn local_ip_toward_gateway(gateway: IpAddr) -> Option<IpAddr> {
    let bind = match gateway {
        IpAddr::V4(_) => "0.0.0.0:0",
        IpAddr::V6(_) => "[::]:0",
    };
    let socket = std::net::UdpSocket::bind(bind).ok()?;
    socket.connect(SocketAddr::new(gateway, 65535)).ok()?;
    socket.local_addr().ok().map(|a| a.ip())
}

/// Best-effort default route discovery for the NAT-PMP path: returns `(local_ip, gateway_ip)`.
/// The gateway is assumed to sit at `.1` of the local IPv4 /24 — the common home-router layout.
/// Returns `None` for IPv6-only hosts (NAT-PMP is IPv4-oriented here).
fn default_route() -> Option<(IpAddr, Ipv4Addr)> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    // A routable public address so the OS picks the default-route interface. No packet is sent.
    socket.connect("8.8.8.8:80").ok()?;
    let local = socket.local_addr().ok()?.ip();
    match local {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            Some((IpAddr::V4(v4), Ipv4Addr::new(o[0], o[1], o[2], 1)))
        }
        IpAddr::V6(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn backend_parses_all_variants() {
        assert_eq!(
            "auto".parse::<PortMapBackend>().unwrap(),
            PortMapBackend::Auto
        );
        assert_eq!(
            "upnp".parse::<PortMapBackend>().unwrap(),
            PortMapBackend::Upnp
        );
        assert_eq!(
            "igd".parse::<PortMapBackend>().unwrap(),
            PortMapBackend::Upnp
        );
        assert_eq!(
            "natpmp".parse::<PortMapBackend>().unwrap(),
            PortMapBackend::Natpmp
        );
        assert_eq!(
            "nat-pmp".parse::<PortMapBackend>().unwrap(),
            PortMapBackend::Natpmp
        );
        assert_eq!(
            "pcp".parse::<PortMapBackend>().unwrap(),
            PortMapBackend::Natpmp
        );
        assert_eq!(
            "none".parse::<PortMapBackend>().unwrap(),
            PortMapBackend::None
        );
        assert_eq!(
            "off".parse::<PortMapBackend>().unwrap(),
            PortMapBackend::None
        );
    }

    #[test]
    fn backend_parse_is_case_and_space_insensitive() {
        assert_eq!(
            "  AUTO ".parse::<PortMapBackend>().unwrap(),
            PortMapBackend::Auto
        );
        assert_eq!(
            "UPnP".parse::<PortMapBackend>().unwrap(),
            PortMapBackend::Upnp
        );
    }

    #[test]
    fn backend_parse_rejects_invalid() {
        let err = "bogus".parse::<PortMapBackend>().unwrap_err();
        assert!(
            err.contains("bogus"),
            "error should name the bad value: {err}"
        );
        assert!("".parse::<PortMapBackend>().is_err());
    }

    #[test]
    fn backend_default_is_auto() {
        assert_eq!(PortMapBackend::default(), PortMapBackend::Auto);
    }

    #[test]
    fn backend_display_roundtrips_through_from_str() {
        for b in [
            PortMapBackend::Auto,
            PortMapBackend::Upnp,
            PortMapBackend::Natpmp,
            PortMapBackend::None,
        ] {
            assert_eq!(b.to_string().parse::<PortMapBackend>().unwrap(), b);
        }
    }

    #[test]
    fn backend_serde_roundtrips_as_lowercase() {
        let json = serde_json::to_string(&PortMapBackend::Natpmp).unwrap();
        assert_eq!(json, "\"natpmp\"");
        let back: PortMapBackend = serde_json::from_str("\"upnp\"").unwrap();
        assert_eq!(back, PortMapBackend::Upnp);
    }

    #[test]
    fn candidates_order_and_membership() {
        assert_eq!(
            PortMapBackend::Auto.candidates(),
            &[Backend::Upnp, Backend::NatPmp]
        );
        assert_eq!(PortMapBackend::Upnp.candidates(), &[Backend::Upnp]);
        assert_eq!(PortMapBackend::Natpmp.candidates(), &[Backend::NatPmp]);
        assert!(PortMapBackend::None.candidates().is_empty());
    }

    #[test]
    fn external_port_override_prefers_override() {
        let cfg = PortMapConfig {
            backend: PortMapBackend::Auto,
            internal_port: 8621,
            external_port: Some(9000),
            lease_seconds: DEFAULT_LEASE_SECONDS,
        };
        assert_eq!(cfg.requested_external_port(), 9000);
    }

    #[test]
    fn external_port_defaults_to_internal_when_no_override() {
        let cfg = PortMapConfig {
            backend: PortMapBackend::Auto,
            internal_port: 8621,
            external_port: None,
            lease_seconds: DEFAULT_LEASE_SECONDS,
        };
        assert_eq!(cfg.requested_external_port(), 8621);
    }

    fn test_cfg() -> PortMapConfig {
        PortMapConfig {
            backend: PortMapBackend::Auto,
            internal_port: 8621,
            external_port: None,
            lease_seconds: DEFAULT_LEASE_SECONDS,
        }
    }

    fn test_endpoint(port: u16) -> MappedEndpoint {
        MappedEndpoint {
            external_ip: None,
            external_port: port,
            lease_duration: Duration::from_secs(DEFAULT_LEASE_SECONDS as u64),
            backend: "fake",
        }
    }

    /// A backend whose gateway call always fails — used to prove the non-fatal path. The call
    /// counter is shared via `Arc` so the test can inspect it after the attempt is boxed.
    struct AlwaysFails {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl MapAttempt for AlwaysFails {
        fn name(&self) -> &'static str {
            "always-fails"
        }
        async fn attempt(&self, _cfg: &PortMapConfig) -> Result<ActiveMapping, PortMapError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(PortMapError::Upnp("injected failure".to_string()))
        }
    }

    /// A backend that succeeds after an optional delay, returning a fake (no-op) mapping.
    struct DelayedOk {
        delay: Duration,
        endpoint: MappedEndpoint,
    }

    struct RenewalFails {
        endpoint: MappedEndpoint,
    }

    #[async_trait::async_trait]
    impl MapAttempt for RenewalFails {
        fn name(&self) -> &'static str {
            "renewal-fails"
        }
        async fn attempt(&self, _cfg: &PortMapConfig) -> Result<ActiveMapping, PortMapError> {
            Ok(ActiveMapping::FakeRenewFailure(self.endpoint.clone()))
        }
    }

    #[async_trait::async_trait]
    impl MapAttempt for DelayedOk {
        fn name(&self) -> &'static str {
            "delayed-ok"
        }
        async fn attempt(&self, _cfg: &PortMapConfig) -> Result<ActiveMapping, PortMapError> {
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            Ok(ActiveMapping::Fake(self.endpoint.clone()))
        }
    }

    #[tokio::test]
    async fn establish_returns_none_on_injected_failure_without_panicking() {
        let cfg = test_cfg();
        let calls = Arc::new(AtomicUsize::new(0));
        let attempts: Vec<Box<dyn MapAttempt>> = vec![Box::new(AlwaysFails {
            calls: calls.clone(),
        })];
        // Must not panic; must return None; must have actually invoked the gateway call.
        let result = establish(&cfg, &attempts).await;
        assert!(result.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn establish_tries_all_candidates_in_order() {
        let cfg = test_cfg();
        let a = Arc::new(AtomicUsize::new(0));
        let b = Arc::new(AtomicUsize::new(0));
        let attempts: Vec<Box<dyn MapAttempt>> = vec![
            Box::new(AlwaysFails { calls: a.clone() }),
            Box::new(AlwaysFails { calls: b.clone() }),
        ];
        let result = establish(&cfg, &attempts).await;
        assert!(result.is_none());
        // Both candidates attempted once because the first failed.
        assert_eq!(a.load(Ordering::SeqCst), 1);
        assert_eq!(b.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn establish_with_no_candidates_returns_none() {
        let cfg = test_cfg();
        let attempts: Vec<Box<dyn MapAttempt>> = vec![];
        assert!(establish(&cfg, &attempts).await.is_none());
    }

    #[tokio::test]
    async fn spawn_with_none_backend_is_a_noop() {
        let cfg = PortMapConfig {
            backend: PortMapBackend::None,
            ..test_cfg()
        };
        assert!(spawn_port_mapping(cfg).is_none());
    }

    // Proves startup does not block on gateway discovery: `spawn_with_attempts` returns while
    // the (delayed) attempt is still in flight, so the endpoint is not yet available, and it is
    // then delivered asynchronously via the watch channel.
    #[tokio::test]
    async fn spawn_returns_before_discovery_completes_then_delivers_via_channel() {
        let cfg = test_cfg();
        let attempts: Vec<Box<dyn MapAttempt>> = vec![Box::new(DelayedOk {
            delay: Duration::from_millis(300),
            endpoint: test_endpoint(8621),
        })];
        let handle = spawn_with_attempts(cfg, attempts);
        // Returned immediately: the 300ms attempt cannot have completed synchronously.
        assert!(
            handle.endpoint().is_none(),
            "spawn must not await gateway discovery"
        );
        // Endpoint is delivered once the background task finishes discovery.
        let delivered = tokio::time::timeout(Duration::from_secs(2), handle.wait_for_endpoint())
            .await
            .expect("endpoint should be delivered promptly after discovery");
        assert_eq!(delivered.map(|e| e.external_port), Some(8621));
        // A snapshot read now also sees it.
        assert_eq!(handle.endpoint().map(|e| e.external_port), Some(8621));
        handle.shutdown().await;
    }

    // Shutting down while discovery is still in flight must not panic and must leave no endpoint.
    #[tokio::test]
    async fn shutdown_before_discovery_completes_does_not_panic() {
        let cfg = test_cfg();
        let attempts: Vec<Box<dyn MapAttempt>> = vec![Box::new(DelayedOk {
            delay: Duration::from_secs(30),
            endpoint: test_endpoint(8621),
        })];
        let handle = spawn_with_attempts(cfg, attempts);
        assert!(handle.endpoint().is_none());
        // Should return quickly (the task abandons the 30s discovery on the shutdown signal)
        // and no endpoint is ever produced.
        tokio::time::timeout(Duration::from_secs(2), handle.shutdown())
            .await
            .expect("shutdown must not wait for in-flight discovery");
    }

    // When every backend fails, the background task ends without an endpoint and the handle's
    // channel reports `None` rather than hanging.
    #[tokio::test]
    async fn all_backends_failing_yields_no_endpoint() {
        let cfg = test_cfg();
        let attempts: Vec<Box<dyn MapAttempt>> = vec![Box::new(AlwaysFails {
            calls: Arc::new(AtomicUsize::new(0)),
        })];
        let handle = spawn_with_attempts(cfg, attempts);
        let delivered = tokio::time::timeout(Duration::from_secs(2), handle.wait_for_endpoint())
            .await
            .expect("wait_for_endpoint must resolve when the task ends");
        assert!(delivered.is_none());
    }

    #[tokio::test]
    async fn renewal_failure_withdraws_endpoint_when_the_granted_lease_expires() {
        let endpoint = MappedEndpoint {
            lease_duration: Duration::from_millis(80),
            ..test_endpoint(48621)
        };
        let handle = spawn_with_attempts(test_cfg(), vec![Box::new(RenewalFails { endpoint })]);
        let mut endpoint_rx = handle.endpoint_receiver();
        endpoint_rx.changed().await.unwrap();
        assert_eq!(
            endpoint_rx.borrow().as_ref().map(|e| e.external_port),
            Some(48621)
        );
        tokio::time::timeout(Duration::from_millis(200), async {
            loop {
                endpoint_rx.changed().await.unwrap();
                if endpoint_rx.borrow().is_none() {
                    break;
                }
            }
        })
        .await
        .expect("expired mapping must be withdrawn");
        assert!(handle.endpoint().is_none());
        handle.shutdown().await;
    }
}
