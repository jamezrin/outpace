//! Public-reachability observability (issue #22).
//!
//! Turns "can the swarm actually reach us?" — flagged as an open question in
//! `docs/protocol/notes/24-seeder-self-announce.md` — into an observable answer from signals
//! we already have, **without** any behavior change to connection logic, choking, or the wire
//! format:
//!
//! * **Observed public IP.** Peers echo the address they see us at in the BEP-10 `yourip`
//!   field of their extended handshake. On our OUTBOUND (leech) connections that value is the
//!   swarm telling us our public IP for free. [`ReachabilityMonitor::observe_yourip`] tallies
//!   these; a single peer can lie, so [`ReachabilityMonitor::observed_ip`] returns the
//!   most-corroborated value (ties broken by most-recent).
//! * **Inbound peers.** Every connection the `PeerListener` accepts is an unsolicited dial —
//!   the strongest possible proof we are reachable. [`ReachabilityMonitor::note_inbound_peer`]
//!   counts them.
//! * **Cross-check.** [`cross_check_ips`] compares the observed `yourip` against the external
//!   IP the gateway port-mapping (#20) reports; a mismatch hints double-NAT / CGNAT (UPnP
//!   mapped a private upstream). A missing side simply skips the check.
//!
//! The monitor is a cheap, shared, daemon-wide handle (a couple of atomics plus a tiny tally
//! map). The engine only creates one when `enable_inbound` is on; with inbound off nothing
//! holds a monitor, so harvesting and counting are entirely absent (see issue #22 task 5).

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Aggregates `yourip` observations and counts inbound peers. Cheap to share (`Arc`) across the
/// outbound harvest path and the inbound listener.
#[derive(Debug, Default)]
pub struct ReachabilityMonitor {
    /// Unsolicited inbound peer connections accepted so far.
    inbound_peers: AtomicU64,
    /// Tally of every public IP a peer has echoed to us in `yourip`.
    yourip: Mutex<YourIpTally>,
}

/// A snapshot of what we've observed about our public IP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservedIp {
    /// The most-corroborated address peers report seeing us at.
    pub ip: IpAddr,
    /// How many observations agreed on [`ObservedIp::ip`].
    pub agreeing: u64,
    /// Total `yourip` observations across all addresses (a low ratio of `agreeing`/`total`
    /// suggests disagreement worth noting).
    pub total: u64,
}

/// The result of cross-checking the observed `yourip` against the gateway's mapped external IP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrossCheck {
    /// Both are known and agree — the mapping targets the address peers actually reach us at.
    Match(IpAddr),
    /// Both are known but differ — hints double-NAT / CGNAT (the gateway mapped a private
    /// upstream address, not our real public one).
    Mismatch { observed: IpAddr, mapped: IpAddr },
    /// One side (or both) is unknown — no mapping, mapping disabled, backend reported no
    /// external IP, or no peer has echoed a `yourip` yet. The check is skipped.
    Skipped,
}

/// Pure cross-check decision, factored out so it is directly unit-testable: matching IPs →
/// [`CrossCheck::Match`]; differing → [`CrossCheck::Mismatch`]; either side absent →
/// [`CrossCheck::Skipped`].
pub fn cross_check_ips(observed: Option<IpAddr>, mapped: Option<IpAddr>) -> CrossCheck {
    match (observed, mapped) {
        (Some(o), Some(m)) if o == m => CrossCheck::Match(o),
        (Some(observed), Some(mapped)) => CrossCheck::Mismatch { observed, mapped },
        _ => CrossCheck::Skipped,
    }
}

#[derive(Debug, Default)]
struct YourIpTally {
    counts: HashMap<IpAddr, u64>,
    total: u64,
    last: Option<IpAddr>,
}

impl YourIpTally {
    fn observe(&mut self, ip: IpAddr) {
        *self.counts.entry(ip).or_insert(0) += 1;
        self.total += 1;
        self.last = Some(ip);
    }

    fn best(&self) -> Option<ObservedIp> {
        // Prefer the most-corroborated address; break ties in favour of the most recently seen
        // (`false < true`, so the last-seen address wins an equal count).
        self.counts
            .iter()
            .max_by_key(|(ip, &count)| (count, Some(**ip) == self.last))
            .map(|(&ip, &agreeing)| ObservedIp {
                ip,
                agreeing,
                total: self.total,
            })
    }
}

impl ReachabilityMonitor {
    pub fn new() -> Self {
        ReachabilityMonitor::default()
    }

    /// Record a public IP a peer echoed to us in `yourip` (from an outbound handshake).
    pub fn observe_yourip(&self, ip: IpAddr) {
        self.yourip.lock().unwrap().observe(ip);
    }

    /// Record that the `PeerListener` accepted one unsolicited inbound connection. Returns the
    /// running total (post-increment).
    pub fn note_inbound_peer(&self) -> u64 {
        self.inbound_peers.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Unsolicited inbound peer connections accepted so far.
    pub fn inbound_peers(&self) -> u64 {
        self.inbound_peers.load(Ordering::Relaxed)
    }

    /// The most-corroborated address peers report seeing us at, if any has been observed.
    pub fn observed(&self) -> Option<ObservedIp> {
        self.yourip.lock().unwrap().best()
    }

    /// Just the observed public IP, discarding the corroboration counts.
    pub fn observed_ip(&self) -> Option<IpAddr> {
        self.observed().map(|o| o.ip)
    }

    /// We treat ourselves as confirmed reachable only once at least one unsolicited inbound
    /// peer has dialed us — the strongest available signal (issue #22 task 4). An observed
    /// `yourip` alone proves we have a public address, not that anything can open a connection
    /// to it.
    pub fn is_reachable(&self) -> bool {
        self.inbound_peers() >= 1
    }

    /// Cross-check the observed `yourip` against the gateway's mapped external IP (#20).
    pub fn cross_check(&self, mapped: Option<IpAddr>) -> CrossCheck {
        cross_check_ips(self.observed_ip(), mapped)
    }

    /// A one-line, loggable reachability status, e.g.
    /// `externally reachable: yes (2 inbound peer(s), external 203.0.113.7:8621, observed yourip 203.0.113.7)`.
    /// `external` is the mapped endpoint from port-mapping (#20) when available; the observed
    /// `yourip` is reported separately since it may be present even without a mapping.
    pub fn status_line(&self, external: Option<(IpAddr, u16)>) -> String {
        let inbound = self.inbound_peers();
        let yes_no = if self.is_reachable() { "yes" } else { "no" };
        let mut s = format!("externally reachable: {yes_no} ({inbound} inbound peer(s)");
        if let Some((ip, port)) = external {
            s.push_str(&format!(", external {ip}:{port}"));
        }
        match self.observed_ip() {
            Some(ip) => s.push_str(&format!(", observed yourip {ip}")),
            None => s.push_str(", no yourip observed yet"),
        }
        s.push(')');
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn ip(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(a, b, c, d))
    }

    #[test]
    fn observed_ip_prefers_the_corroborated_value_over_a_single_liar() {
        let m = ReachabilityMonitor::new();
        m.observe_yourip(ip(203, 0, 113, 7));
        m.observe_yourip(ip(203, 0, 113, 7));
        m.observe_yourip(ip(203, 0, 113, 7));
        // One peer lies about a different address.
        m.observe_yourip(ip(198, 51, 100, 9));
        let obs = m.observed().unwrap();
        assert_eq!(obs.ip, ip(203, 0, 113, 7));
        assert_eq!(obs.agreeing, 3);
        assert_eq!(obs.total, 4);
    }

    #[test]
    fn observed_ip_breaks_ties_by_most_recent() {
        let m = ReachabilityMonitor::new();
        m.observe_yourip(ip(203, 0, 113, 7));
        m.observe_yourip(ip(198, 51, 100, 9));
        // Both seen once; the most recent wins the tie.
        assert_eq!(m.observed_ip(), Some(ip(198, 51, 100, 9)));
    }

    #[test]
    fn observed_ip_is_none_before_any_observation() {
        assert_eq!(ReachabilityMonitor::new().observed_ip(), None);
    }

    #[test]
    fn cross_check_matching_ip_is_a_match_no_warning() {
        let observed = ip(203, 0, 113, 7);
        assert_eq!(
            cross_check_ips(Some(observed), Some(observed)),
            CrossCheck::Match(observed)
        );
    }

    #[test]
    fn cross_check_differing_ip_is_a_mismatch_cgnat_hint() {
        let observed = ip(203, 0, 113, 7);
        let mapped = ip(10, 0, 0, 1); // a private upstream — the CGNAT/double-NAT signature
        assert_eq!(
            cross_check_ips(Some(observed), Some(mapped)),
            CrossCheck::Mismatch { observed, mapped }
        );
    }

    #[test]
    fn cross_check_missing_mapping_is_skipped() {
        assert_eq!(
            cross_check_ips(Some(ip(203, 0, 113, 7)), None),
            CrossCheck::Skipped
        );
        assert_eq!(
            cross_check_ips(None, Some(ip(203, 0, 113, 7))),
            CrossCheck::Skipped
        );
        assert_eq!(cross_check_ips(None, None), CrossCheck::Skipped);
    }

    #[test]
    fn monitor_cross_check_reads_the_observed_value() {
        let m = ReachabilityMonitor::new();
        m.observe_yourip(ip(203, 0, 113, 7));
        assert_eq!(
            m.cross_check(Some(ip(203, 0, 113, 7))),
            CrossCheck::Match(ip(203, 0, 113, 7))
        );
        assert_eq!(
            m.cross_check(Some(ip(10, 0, 0, 1))),
            CrossCheck::Mismatch {
                observed: ip(203, 0, 113, 7),
                mapped: ip(10, 0, 0, 1),
            }
        );
        assert_eq!(m.cross_check(None), CrossCheck::Skipped);
    }

    #[test]
    fn not_reachable_until_an_inbound_peer_connects() {
        let m = ReachabilityMonitor::new();
        // An observed public IP alone does NOT confirm reachability.
        m.observe_yourip(ip(203, 0, 113, 7));
        assert!(!m.is_reachable(), "0 inbound peers => not yet confirmed");
        assert_eq!(m.inbound_peers(), 0);

        assert_eq!(m.note_inbound_peer(), 1);
        assert!(m.is_reachable(), ">=1 inbound peer => reachable");
        assert_eq!(m.note_inbound_peer(), 2);
        assert_eq!(m.inbound_peers(), 2);
    }

    #[test]
    fn status_line_reflects_reachability_and_endpoint() {
        let m = ReachabilityMonitor::new();
        let unreachable = m.status_line(None);
        assert!(unreachable.contains("externally reachable: no"));
        assert!(unreachable.contains("0 inbound peer(s)"));
        assert!(unreachable.contains("no yourip observed yet"));

        m.observe_yourip(ip(203, 0, 113, 7));
        m.note_inbound_peer();
        let reachable = m.status_line(Some((ip(203, 0, 113, 7), 8621)));
        assert!(reachable.contains("externally reachable: yes"));
        assert!(reachable.contains("1 inbound peer(s)"));
        assert!(reachable.contains("external 203.0.113.7:8621"));
        assert!(reachable.contains("observed yourip 203.0.113.7"));
    }
}
