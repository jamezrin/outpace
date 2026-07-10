//! In-memory, daemon-session-scoped cache of recently-successful DHT routing nodes
//! (evaluate-first prototype for #42). A *routing node* here is a node that answered our
//! `get_peers` with a correlated, valid response carrying its own node id — i.e. one that
//! demonstrably participates in the DHT and can move a future walk closer to a target faster
//! than starting cold from the public bootstrap routers.
//!
//! This cache is deliberately conservative and DEFAULT-OFF (see `dht::routing_cache_enabled`):
//! an unproven startup optimization must never regress the cold path. When enabled it only
//! ever *adds* cached nodes to a walk's frontier alongside the bootstrap routers; it never
//! displaces the public bootstrap fallback and never blocks on a stale entry (a dead cached
//! node just shares the normal per-round response window like any other seed).
//!
//! Invariants enforced here (proven by the unit tests at the bottom):
//! - Only PUBLIC addresses are eligible; private/loopback/link-local/CGNAT/reserved are
//!   rejected. `values`/peer results are never routed through here — the walk only ever hands
//!   this cache a *responder* `(node id, source addr)`, never a peer from `r.values`.
//! - Bounded capacity and per-prefix diversity caps (no single /24 or /16 may dominate).
//! - Aggressive eviction/deprioritization of stale or failing nodes.
//!
//! Disk persistence is explicitly a separate future follow-up and is NOT implemented here.

use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::{Duration, Instant};

/// Maximum routing nodes retained across the daemon session.
pub(crate) const DEFAULT_CAPACITY: usize = 64;
/// Diversity cap: at most this many cached nodes may share a single /24.
pub(crate) const DEFAULT_MAX_PER_24: usize = 2;
/// Diversity cap: at most this many cached nodes may share a single /16.
pub(crate) const DEFAULT_MAX_PER_16: usize = 4;
/// A cached node older than this (since last success) is considered stale and is neither
/// seeded nor retained on prune.
pub(crate) const DEFAULT_STALE_AFTER: Duration = Duration::from_secs(15 * 60);
/// Consecutive failures at which a node is evicted outright (aggressive deprioritization).
pub(crate) const DEFAULT_MAX_FAILURES: u32 = 2;

/// One cached routing node with the freshness/success bookkeeping used for ranking, seeding,
/// and eviction.
#[derive(Clone, Debug)]
struct CachedNode {
    id: [u8; 20],
    addr: SocketAddrV4,
    /// Monotonic instant of the most recent correlated valid response from this node.
    last_success: Instant,
    successes: u32,
    /// Consecutive failures since the last success (reset to 0 on success).
    failures: u32,
}

/// Bounded, prefix-diverse cache of recently-successful DHT routing nodes. Not thread-safe on
/// its own; the session-global instance in `dht` wraps it in a `Mutex`.
pub(crate) struct RoutingNodeCache {
    capacity: usize,
    max_per_24: usize,
    max_per_16: usize,
    stale_after: Duration,
    max_failures: u32,
    /// When false (production default), only globally-routable IPv4 addresses are eligible.
    /// Tests flip this on to exercise seeding/harvest/eviction against loopback fake sockets.
    allow_non_global: bool,
    nodes: Vec<CachedNode>,
}

impl RoutingNodeCache {
    /// Production cache: default bounds, public-address-only.
    pub(crate) fn new() -> Self {
        RoutingNodeCache {
            capacity: DEFAULT_CAPACITY,
            max_per_24: DEFAULT_MAX_PER_24,
            max_per_16: DEFAULT_MAX_PER_16,
            stale_after: DEFAULT_STALE_AFTER,
            max_failures: DEFAULT_MAX_FAILURES,
            allow_non_global: false,
            nodes: Vec::new(),
        }
    }

    /// Record a node that returned a correlated, valid `get_peers` response carrying `id` from
    /// `addr`. Returns `true` if the node is eligible and now present (inserted or refreshed),
    /// `false` if rejected (non-public address under the default policy). Enforces the
    /// per-prefix diversity caps and the global capacity bound, evicting the weakest node(s)
    /// as needed.
    ///
    /// This is the ONLY ingestion path: the walk hands it a *responder* `(id, addr)`, never a
    /// `values`/peer record, so peer results can never be cached as routing nodes.
    pub(crate) fn record_success(
        &mut self,
        id: [u8; 20],
        addr: SocketAddrV4,
        now: Instant,
    ) -> bool {
        if !self.allow_non_global && !is_public_v4(addr.ip()) {
            return false;
        }
        if let Some(n) = self.nodes.iter_mut().find(|n| n.addr == addr) {
            n.id = id;
            n.last_success = now;
            n.successes = n.successes.saturating_add(1);
            n.failures = 0;
            return true;
        }
        // Keep each network prefix from dominating: before inserting a new node in a saturated
        // /24 or /16, evict the weakest existing node sharing that prefix. The newcomer just
        // succeeded, so it is at least as fresh as anything it replaces.
        self.enforce_prefix_cap(self.max_per_24, now, |a| slash24(a) == slash24(addr));
        self.enforce_prefix_cap(self.max_per_16, now, |a| slash16(a) == slash16(addr));
        if self.nodes.len() >= self.capacity {
            if let Some(idx) = self.weakest_index(now, |_| true) {
                self.nodes.remove(idx);
            }
        }
        self.nodes.push(CachedNode {
            id,
            addr,
            last_success: now,
            successes: 1,
            failures: 0,
        });
        true
    }

    /// Record that a cached node we seeded was queried but did not return a valid response.
    /// Increments its failure counter and evicts it once it reaches `max_failures` — a failing
    /// node is deprioritized fast so it cannot keep costing future walks.
    pub(crate) fn record_failure(&mut self, addr: SocketAddrV4, _now: Instant) {
        if let Some(pos) = self.nodes.iter().position(|n| n.addr == addr) {
            self.nodes[pos].failures = self.nodes[pos].failures.saturating_add(1);
            if self.nodes[pos].failures >= self.max_failures {
                self.nodes.remove(pos);
            }
        }
    }

    /// Fresh, non-failing cached nodes to seed a new walk with, best (freshest/most-successful)
    /// first, capped at `limit`. Stale or over-failed nodes are excluded — seeding never
    /// waits on them.
    pub(crate) fn seeds(&self, now: Instant, limit: usize) -> Vec<SocketAddrV4> {
        let mut live: Vec<&CachedNode> =
            self.nodes.iter().filter(|n| self.is_live(n, now)).collect();
        live.sort_by_key(|n| penalty(n, now));
        live.into_iter().take(limit).map(|n| n.addr).collect()
    }

    /// Drop stale and over-failed nodes. Cheap to call after every walk.
    pub(crate) fn prune_stale(&mut self, now: Instant) {
        let stale_after = self.stale_after;
        let max_failures = self.max_failures;
        self.nodes.retain(|n| {
            n.failures < max_failures && now.saturating_duration_since(n.last_success) < stale_after
        });
    }

    fn is_live(&self, n: &CachedNode, now: Instant) -> bool {
        n.failures < self.max_failures
            && now.saturating_duration_since(n.last_success) < self.stale_after
    }

    /// While `count(pred) >= cap`, evict the weakest node matching `pred`.
    fn enforce_prefix_cap(
        &mut self,
        cap: usize,
        now: Instant,
        pred: impl Fn(SocketAddrV4) -> bool + Copy,
    ) {
        while self.nodes.iter().filter(|n| pred(n.addr)).count() >= cap {
            match self.weakest_index(now, pred) {
                Some(idx) => {
                    self.nodes.remove(idx);
                }
                None => break,
            }
        }
    }

    /// Index of the weakest (highest-penalty) node matching `pred`, if any.
    fn weakest_index(&self, now: Instant, pred: impl Fn(SocketAddrV4) -> bool) -> Option<usize> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| pred(n.addr))
            .max_by_key(|(_, n)| penalty(n, now))
            .map(|(i, _)| i)
    }

    // --- test-only introspection / tunables ---

    #[cfg(test)]
    pub(crate) fn for_test(allow_non_global: bool) -> Self {
        RoutingNodeCache {
            allow_non_global,
            ..RoutingNodeCache::new()
        }
    }

    #[cfg(test)]
    pub(crate) fn with_params(
        capacity: usize,
        max_per_24: usize,
        max_per_16: usize,
        stale_after: Duration,
        max_failures: u32,
        allow_non_global: bool,
    ) -> Self {
        RoutingNodeCache {
            capacity,
            max_per_24,
            max_per_16,
            stale_after,
            max_failures,
            allow_non_global,
            nodes: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.nodes.len()
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, addr: SocketAddrV4) -> bool {
        self.nodes.iter().any(|n| n.addr == addr)
    }

    #[cfg(test)]
    pub(crate) fn failures_of(&self, addr: SocketAddrV4) -> Option<u32> {
        self.nodes
            .iter()
            .find(|n| n.addr == addr)
            .map(|n| n.failures)
    }

    #[cfg(test)]
    pub(crate) fn count_in_24(&self, addr: SocketAddrV4) -> usize {
        self.nodes
            .iter()
            .filter(|n| slash24(n.addr) == slash24(addr))
            .count()
    }
}

/// Weakness penalty used for ranking and eviction — higher means weaker. Failures dominate
/// (each failure outweighs any freshness/age difference), then staleness (older = weaker),
/// with a small credit for accumulated successes.
fn penalty(n: &CachedNode, now: Instant) -> u128 {
    let age = now.saturating_duration_since(n.last_success).as_secs() as u128;
    let credit = (n.successes as u128).saturating_mul(5);
    (n.failures as u128) * 1_000_000_000 + age.saturating_sub(credit)
}

fn slash24(a: SocketAddrV4) -> [u8; 3] {
    let o = a.ip().octets();
    [o[0], o[1], o[2]]
}

fn slash16(a: SocketAddrV4) -> [u8; 2] {
    let o = a.ip().octets();
    [o[0], o[1]]
}

/// True for globally-routable IPv4 addresses we are willing to cache as routing nodes. Rejects
/// loopback, private (RFC1918), link-local (incl. 169.254.0.0/16), CGNAT (100.64.0.0/10),
/// multicast, broadcast, unspecified, and documentation ranges.
pub(crate) fn is_public_v4(ip: &Ipv4Addr) -> bool {
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_broadcast()
        || ip.is_unspecified()
        || ip.is_documentation()
        || is_cgnat(ip))
}

/// RFC6598 carrier-grade NAT shared address space: 100.64.0.0/10.
fn is_cgnat(ip: &Ipv4Addr) -> bool {
    let o = ip.octets();
    o[0] == 100 && (o[1] & 0xC0) == 0x40
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pub_addr(a: u8, b: u8, c: u8, d: u8, port: u16) -> SocketAddrV4 {
        SocketAddrV4::new(Ipv4Addr::new(a, b, c, d), port)
    }

    #[test]
    fn only_public_addresses_are_eligible() {
        let mut c = RoutingNodeCache::new(); // production policy: public-only
        let now = Instant::now();
        // Public address is accepted.
        assert!(c.record_success([1u8; 20], pub_addr(87, 221, 96, 148, 8621), now));
        // Non-public ranges are all rejected.
        assert!(!c.record_success([1u8; 20], pub_addr(127, 0, 0, 1, 1000), now));
        assert!(!c.record_success([1u8; 20], pub_addr(10, 0, 0, 1, 1000), now));
        assert!(!c.record_success([1u8; 20], pub_addr(192, 168, 1, 1, 1000), now));
        assert!(!c.record_success([1u8; 20], pub_addr(172, 16, 0, 1, 1000), now));
        assert!(!c.record_success([1u8; 20], pub_addr(169, 254, 1, 1, 1000), now));
        assert!(!c.record_success([1u8; 20], pub_addr(100, 64, 0, 1, 1000), now)); // CGNAT
        assert!(!c.record_success([1u8; 20], pub_addr(0, 0, 0, 0, 1000), now));
        assert_eq!(c.len(), 1, "only the one public node should be cached");
    }

    #[test]
    fn capacity_is_bounded() {
        // capacity 4, generous prefix caps, distinct /16s so only capacity gates.
        let mut c =
            RoutingNodeCache::with_params(4, 8, 8, DEFAULT_STALE_AFTER, DEFAULT_MAX_FAILURES, true);
        let now = Instant::now();
        for i in 0..20u8 {
            assert!(c.record_success([i; 20], pub_addr(i + 1, i + 1, 0, 1, 6881), now));
        }
        assert!(c.len() <= 4, "capacity bound must hold, got {}", c.len());
    }

    #[test]
    fn per_24_diversity_cap_holds() {
        let mut c = RoutingNodeCache::with_params(
            64,
            2,
            64,
            DEFAULT_STALE_AFTER,
            DEFAULT_MAX_FAILURES,
            true,
        );
        let now = Instant::now();
        // Ten nodes all in 203.0.113.0/24 — the /24 cap must keep at most 2.
        for d in 1..=10u8 {
            c.record_success([d; 20], pub_addr(203, 0, 113, d, 6881), now);
        }
        assert_eq!(
            c.count_in_24(pub_addr(203, 0, 113, 1, 0)),
            2,
            "no single /24 may exceed its diversity cap"
        );
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn stale_nodes_are_not_seeded_and_are_pruned() {
        let mut c = RoutingNodeCache::with_params(
            64,
            8,
            8,
            Duration::from_secs(60),
            DEFAULT_MAX_FAILURES,
            true,
        );
        let now = Instant::now();
        let fresh = pub_addr(198, 51, 100, 1, 6881);
        let stale = pub_addr(198, 51, 100, 2, 6881);
        c.record_success([1; 20], fresh, now);
        // Insert `stale` as if it last succeeded well beyond the stale window.
        c.record_success([2; 20], stale, now - Duration::from_secs(120));
        let seeds = c.seeds(now, 16);
        assert!(seeds.contains(&fresh));
        assert!(!seeds.contains(&stale), "stale node must not be seeded");
        c.prune_stale(now);
        assert!(!c.contains(stale), "stale node must be pruned");
        assert!(c.contains(fresh));
    }

    #[test]
    fn failing_nodes_are_evicted_aggressively() {
        let mut c = RoutingNodeCache::with_params(
            64,
            8,
            8,
            DEFAULT_STALE_AFTER,
            2, // evict at 2 consecutive failures
            true,
        );
        let now = Instant::now();
        let addr = pub_addr(198, 51, 100, 9, 6881);
        c.record_success([1; 20], addr, now);
        c.record_failure(addr, now);
        assert_eq!(
            c.failures_of(addr),
            Some(1),
            "one failure recorded, not yet evicted"
        );
        c.record_failure(addr, now);
        assert!(
            !c.contains(addr),
            "node evicted once it reaches max_failures"
        );
        // A subsequent success re-admits it with a clean slate.
        c.record_success([1; 20], addr, now);
        assert_eq!(c.failures_of(addr), Some(0));
    }

    #[test]
    fn seeds_rank_fresher_and_more_successful_first() {
        let mut c = RoutingNodeCache::with_params(
            64,
            8,
            8,
            DEFAULT_STALE_AFTER,
            DEFAULT_MAX_FAILURES,
            true,
        );
        let now = Instant::now();
        let older = pub_addr(198, 51, 100, 1, 6881);
        let fresher = pub_addr(203, 0, 113, 1, 6881);
        c.record_success([1; 20], older, now - Duration::from_secs(120));
        c.record_success([2; 20], fresher, now);
        let seeds = c.seeds(now, 16);
        assert_eq!(seeds.first(), Some(&fresher), "fresher node ranks first");
    }

    #[test]
    fn public_classifier_matches_expectations() {
        // Globally-routable.
        assert!(is_public_v4(&Ipv4Addr::new(87, 221, 96, 148)));
        assert!(is_public_v4(&Ipv4Addr::new(1, 1, 1, 1)));
        // 100.63.0.1 sits just below the CGNAT block (100.64.0.0/10) and is routable.
        assert!(is_public_v4(&Ipv4Addr::new(100, 63, 0, 1)));
        // Reserved / non-routable ranges.
        assert!(!is_public_v4(&Ipv4Addr::new(127, 0, 0, 1))); // loopback
        assert!(!is_public_v4(&Ipv4Addr::new(10, 0, 0, 1))); // RFC1918
        assert!(!is_public_v4(&Ipv4Addr::new(169, 254, 0, 1))); // link-local
        assert!(!is_public_v4(&Ipv4Addr::new(100, 100, 0, 1))); // CGNAT
        assert!(!is_public_v4(&Ipv4Addr::new(203, 0, 113, 5))); // TEST-NET-3 (documentation)
        assert!(!is_public_v4(&Ipv4Addr::new(224, 0, 0, 1))); // multicast
    }
}
