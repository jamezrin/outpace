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
use tokio::sync::oneshot;
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

/// A live mapping plus its background renewal task.
///
/// Dropping the handle signals the renewal task to delete the mapping on a best-effort
/// basis; prefer [`PortMapHandle::shutdown`] for a graceful, awaited teardown.
pub struct PortMapHandle {
    endpoint: MappedEndpoint,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl PortMapHandle {
    /// The resolved external endpoint. Hand this to the announce path (#21).
    pub fn endpoint(&self) -> &MappedEndpoint {
        &self.endpoint
    }

    /// Gracefully tear down: signal the renewal task to delete the gateway mapping and wait
    /// for it to finish.
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
async fn establish(cfg: &PortMapConfig, attempts: &[&dyn MapAttempt]) -> Option<ActiveMapping> {
    for attempt in attempts {
        match attempt.attempt(cfg).await {
            Ok(active) => return Some(active),
            Err(e) => {
                // Matches the reference engine's `Failed to init port forward` log line.
                swarm_log!(
                    "[portmap] {} failed to init port forward: {e} (continuing)",
                    attempt.name()
                );
            }
        }
    }
    None
}

/// Attempt to map the inbound peer port on the gateway and, on success, spawn a renewal task.
///
/// Best-effort: returns `None` (after logging a warning) on any failure or when the backend is
/// [`PortMapBackend::None`]. Never panics. The caller must have already checked its gating
/// (`enable_inbound` && `enable_port_mapping`).
pub async fn spawn_port_mapping(cfg: PortMapConfig) -> Option<PortMapHandle> {
    let candidates = cfg.backend.candidates();
    if candidates.is_empty() {
        return None;
    }

    // Build the concrete attempts for the selected backends.
    let upnp = UpnpMapper;
    let natpmp = NatPmpMapper;
    let attempts: Vec<&dyn MapAttempt> = candidates
        .iter()
        .map(|b| match b {
            Backend::Upnp => &upnp as &dyn MapAttempt,
            Backend::NatPmp => &natpmp as &dyn MapAttempt,
        })
        .collect();

    let active = establish(&cfg, &attempts).await?;
    let endpoint = active.endpoint();
    swarm_log!(
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

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task_endpoint = endpoint.clone();
    let task = tokio::spawn(renewal_loop(active, cfg, task_endpoint, shutdown_rx));

    Some(PortMapHandle {
        endpoint,
        shutdown_tx: Some(shutdown_tx),
        task: Some(task),
    })
}

/// Refresh the mapping before its lease expires; delete it on shutdown signal.
async fn renewal_loop(
    mut active: ActiveMapping,
    cfg: PortMapConfig,
    endpoint: MappedEndpoint,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    loop {
        // Renew at ~half the lease so a single missed refresh does not drop the mapping.
        let lease = endpoint.lease_duration.max(Duration::from_secs(30));
        let refresh_in = lease / 2;
        tokio::select! {
            _ = &mut shutdown_rx => {
                match active.remove(&cfg).await {
                    Ok(()) => swarm_log!(
                        "[portmap] removed mapping for external port {}",
                        endpoint.external_port
                    ),
                    Err(e) => swarm_log!(
                        "[portmap] failed to remove mapping for external port {}: {e}",
                        endpoint.external_port
                    ),
                }
                return;
            }
            _ = tokio::time::sleep(refresh_in) => {
                match active.renew(&cfg).await {
                    Ok(()) => swarm_log!(
                        "[portmap] renewed mapping for external port {}",
                        endpoint.external_port
                    ),
                    Err(e) => swarm_log!(
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
        }
    }

    async fn renew(&mut self, cfg: &PortMapConfig) -> Result<(), PortMapError> {
        match self {
            ActiveMapping::Upnp(a) => a.renew(cfg).await,
            ActiveMapping::NatPmp(m) => m
                .renew()
                .await
                .map_err(|e| PortMapError::NatPmp(e.to_string())),
        }
    }

    async fn remove(self, cfg: &PortMapConfig) -> Result<(), PortMapError> {
        match self {
            ActiveMapping::Upnp(a) => a.remove(cfg).await,
            ActiveMapping::NatPmp(m) => (*m)
                .try_drop()
                .await
                .map_err(|(e, _)| PortMapError::NatPmp(e.to_string())),
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

    /// A backend whose gateway call always fails — used to prove the non-fatal path.
    struct AlwaysFails {
        calls: AtomicUsize,
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

    #[tokio::test]
    async fn establish_returns_none_on_injected_failure_without_panicking() {
        let cfg = test_cfg();
        let a = AlwaysFails {
            calls: AtomicUsize::new(0),
        };
        let attempts: Vec<&dyn MapAttempt> = vec![&a];
        // Must not panic; must return None; must have actually invoked the gateway call.
        let result = establish(&cfg, &attempts).await;
        assert!(result.is_none());
        assert_eq!(a.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn establish_tries_all_candidates_in_order() {
        let cfg = test_cfg();
        let a = AlwaysFails {
            calls: AtomicUsize::new(0),
        };
        let b = AlwaysFails {
            calls: AtomicUsize::new(0),
        };
        let attempts: Vec<&dyn MapAttempt> = vec![&a, &b];
        let result = establish(&cfg, &attempts).await;
        assert!(result.is_none());
        // Both candidates attempted once because the first failed.
        assert_eq!(a.calls.load(Ordering::SeqCst), 1);
        assert_eq!(b.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn establish_with_no_candidates_returns_none() {
        let cfg = test_cfg();
        let attempts: Vec<&dyn MapAttempt> = vec![];
        assert!(establish(&cfg, &attempts).await.is_none());
    }

    #[tokio::test]
    async fn spawn_with_none_backend_is_a_noop() {
        let cfg = PortMapConfig {
            backend: PortMapBackend::None,
            ..test_cfg()
        };
        assert!(spawn_port_mapping(cfg).await.is_none());
    }
}
