//! Bounded, session-local upstream knowledge and retry timing.

use std::collections::HashSet;
use std::net::SocketAddrV4;
use std::time::{Duration, Instant};

const MAX_CANDIDATES: usize = 1024;
// A stream normally has one source. Bound priority announcements so bad sources cannot
// place an arbitrarily long run of timeouts in front of healthy PEX/discovery peers.
const MAX_SOURCES: usize = 8;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum CandidateKind {
    Source,
    Pex,
    Discovered,
}

struct Candidate {
    addr: SocketAddrV4,
    kind: CandidateKind,
    failures: u32,
    admissions: u64,
    explorations: u64,
    retry_at: Instant,
}

#[derive(Default)]
pub(super) struct SessionCandidates {
    entries: Vec<Candidate>,
}

impl SessionCandidates {
    pub(super) fn learn(&mut self, addr: SocketAddrV4, mut kind: CandidateKind) {
        if addr.port() == 0
            || addr.ip().is_unspecified()
            || addr.ip().is_multicast()
            || addr.ip().is_broadcast()
        {
            return;
        }
        if kind == CandidateKind::Source
            && self
                .entries
                .iter()
                .filter(|c| c.kind == kind && c.addr != addr)
                .count()
                >= MAX_SOURCES
        {
            // Preserve excess announcements as ordinary learned peers, without priority.
            kind = CandidateKind::Pex;
        }
        if let Some(candidate) = self.entries.iter_mut().find(|c| c.addr == addr) {
            candidate.kind = candidate.kind.min(kind);
            return; // Repeated gossip must not reset a failed connection's cooldown.
        }
        if self.entries.len() == MAX_CANDIDATES {
            let Some(index) = self
                .entries
                .iter()
                .enumerate()
                .rev()
                .filter(|(_, c)| c.kind > kind)
                .max_by_key(|(_, c)| c.kind)
                .map(|(i, _)| i)
            else {
                return;
            };
            self.entries.remove(index);
        }
        self.entries.push(Candidate {
            addr,
            kind,
            failures: 0,
            admissions: 0,
            explorations: 0,
            retry_at: Instant::now(),
        });
    }

    pub(super) fn eligible(&self, now: Instant) -> Vec<SocketAddrV4> {
        let mut eligible: Vec<_> = self.entries.iter().filter(|c| c.retry_at <= now).collect();
        eligible.sort_by_key(|c| (c.explorations, c.kind, c.admissions));
        eligible.into_iter().map(|c| c.addr).collect()
    }

    pub(super) fn eligible_learned(&self, now: Instant) -> Vec<SocketAddrV4> {
        self.eligible_kind(now, false)
    }

    pub(super) fn eligible_discovered(&self, now: Instant) -> Vec<SocketAddrV4> {
        self.eligible_kind(now, true)
    }

    fn eligible_kind(&self, now: Instant, discovered: bool) -> Vec<SocketAddrV4> {
        let mut eligible: Vec<_> = self
            .entries
            .iter()
            .filter(|c| c.retry_at <= now && (c.kind == CandidateKind::Discovered) == discovered)
            .collect();
        eligible.sort_by_key(|c| (c.explorations, c.kind, c.admissions));
        eligible.into_iter().map(|c| c.addr).collect()
    }

    pub(super) fn exploration_cohort(
        &self,
        now: Instant,
        active: &HashSet<SocketAddrV4>,
        pending: &HashSet<SocketAddrV4>,
    ) -> Option<u64> {
        self.entries
            .iter()
            .filter(|c| {
                !active.contains(&c.addr) && (c.retry_at <= now || pending.contains(&c.addr))
            })
            .map(|c| c.explorations)
            .min()
    }

    pub(super) fn into_learned(self) -> Vec<(SocketAddrV4, CandidateKind)> {
        self.entries.into_iter().map(|c| (c.addr, c.kind)).collect()
    }

    pub(super) fn all(&self) -> Vec<SocketAddrV4> {
        self.entries.iter().map(|c| c.addr).collect()
    }

    pub(super) fn explorations(&self, addr: SocketAddrV4) -> u64 {
        self.entries
            .iter()
            .find(|c| c.addr == addr)
            .map_or(0, |c| c.explorations)
    }

    // A completed opportunity advances exploration even when its window is stale.
    // Cancelled, duplicate and unselected transports consume no opportunity.
    pub(super) fn explored(&mut self, addr: SocketAddrV4) {
        if let Some(c) = self.entries.iter_mut().find(|c| c.addr == addr) {
            c.explorations = c.explorations.saturating_add(1);
        }
    }

    pub(super) fn admitted(&mut self, addr: SocketAddrV4) {
        self.explored(addr);
        if let Some(c) = self.entries.iter_mut().find(|c| c.addr == addr) {
            c.admissions = c.admissions.saturating_add(1);
        }
    }

    pub(super) fn attempting(&mut self, addr: SocketAddrV4, until: Instant) {
        if let Some(c) = self.entries.iter_mut().find(|c| c.addr == addr) {
            c.retry_at = until;
        }
    }

    pub(super) fn failed(&mut self, addr: SocketAddrV4, now: Instant) {
        if let Some(c) = self.entries.iter_mut().find(|c| c.addr == addr) {
            c.failures = c.failures.saturating_add(1);
            let cap = if c.kind == CandidateKind::Source {
                4
            } else {
                8
            };
            c.retry_at =
                now + Duration::from_secs((1u64 << c.failures.saturating_sub(1).min(3)).min(cap));
        }
    }

    pub(super) fn productive(&mut self, addr: SocketAddrV4) {
        // Real contiguous output ends nonproductive exploration. Restore source priority
        // for the next outage without resetting any other peer's failure/cooldown history.
        for candidate in &mut self.entries {
            candidate.admissions = 0;
            candidate.explorations = 0;
        }
        if let Some(c) = self.entries.iter_mut().find(|c| c.addr == addr) {
            c.failures = 0;
            c.retry_at = Instant::now();
        }
    }

    pub(super) fn retry_delay(&self, now: Instant) -> Duration {
        self.entries
            .iter()
            .map(|c| c.retry_at.saturating_duration_since(now))
            .min()
            .unwrap_or(Duration::from_secs(1))
    }

    pub(super) fn learned_counts(&self) -> (usize, usize) {
        (
            self.entries
                .iter()
                .filter(|c| c.kind == CandidateKind::Source)
                .count(),
            self.entries
                .iter()
                .filter(|c| c.kind == CandidateKind::Pex)
                .count(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn addr(port: u16) -> SocketAddrV4 {
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)
    }

    #[test]
    fn priority_dedup_upgrade_and_announcement_preserve_cooldown() {
        let mut candidates = SessionCandidates::default();
        candidates.learn(addr(1), CandidateKind::Discovered);
        candidates.learn(addr(2), CandidateKind::Pex);
        candidates.learn(addr(3), CandidateKind::Source);
        candidates.learn(addr(1), CandidateKind::Source);
        let now = Instant::now();
        assert_eq!(candidates.eligible(now), vec![addr(1), addr(3), addr(2)]);
        candidates.failed(addr(1), now);
        candidates.learn(addr(1), CandidateKind::Source);
        assert_eq!(candidates.eligible(now), vec![addr(3), addr(2)]);
        assert_eq!(
            candidates.eligible(now + Duration::from_secs(1)),
            vec![addr(1), addr(3), addr(2)]
        );
        for step in 1..10 {
            candidates.failed(addr(1), now + Duration::from_secs(step));
        }
        assert_eq!(
            candidates.entries[0].retry_at,
            now + Duration::from_secs(13)
        );
    }

    #[test]
    fn candidates_are_bounded_and_source_can_replace_discovery() {
        let mut candidates = SessionCandidates::default();
        for port in 1..=MAX_CANDIDATES as u16 {
            candidates.learn(addr(port), CandidateKind::Discovered);
        }
        candidates.learn(addr(2000), CandidateKind::Pex);
        candidates.learn(addr(2001), CandidateKind::Source);
        candidates.learn(
            SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 1),
            CandidateKind::Source,
        );
        candidates.learn(
            SocketAddrV4::new(Ipv4Addr::BROADCAST, 1),
            CandidateKind::Source,
        );
        candidates.learn(
            SocketAddrV4::new(Ipv4Addr::new(224, 0, 0, 1), 1),
            CandidateKind::Source,
        );
        candidates.learn(addr(0), CandidateKind::Source);
        assert_eq!(candidates.entries.len(), MAX_CANDIDATES);
        assert_eq!(
            &candidates.eligible(Instant::now())[..2],
            &[addr(2001), addr(2000)]
        );
        for port in 3000..3020 {
            candidates.learn(addr(port), CandidateKind::Source);
        }
        assert_eq!(
            candidates
                .entries
                .iter()
                .filter(|c| c.kind == CandidateKind::Source)
                .count(),
            MAX_SOURCES
        );
    }
}
