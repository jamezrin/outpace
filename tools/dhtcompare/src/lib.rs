//! Strict research-worker inputs. Parsing does not authorize network execution.
use std::fmt;
use std::net::SocketAddrV4;
use std::time::Duration;

const LOOKUP_BUDGET: Duration = Duration::from_secs(15);

pub struct WorkerInput {
    pub(crate) infohash: [u8; 20],
    pub(crate) seeds: Vec<SocketAddrV4>,
    pub(crate) budget: Duration,
    pub(crate) target: usize,
    pub(crate) result_cap: usize,
}

impl fmt::Debug for WorkerInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Neither target identity nor endpoint values belong in diagnostic output.
        f.debug_struct("WorkerInput")
            .field("seed_count", &self.seeds.len())
            .field("budget", &self.budget)
            .field("target", &self.target)
            .field("result_cap", &self.result_cap)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputError;
impl fmt::Display for InputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid_worker_input")
    }
}
impl std::error::Error for InputError {}

impl WorkerInput {
    /// Parse one target and one to four unique numeric IPv4 bootstrap endpoints.
    /// Loopback/private values permit confined tests; destination policy is separate.
    pub fn parse(hash: &str, seeds: &str) -> Result<Self, InputError> {
        if hash.len() != 40 || seeds.is_empty() || seeds.len() > 256 {
            return Err(InputError);
        }
        let mut infohash = [0; 20];
        for (out, pair) in infohash.iter_mut().zip(hash.as_bytes().as_chunks::<2>().0) {
            let digit = |b: u8| match b {
                b'0'..=b'9' => Ok(b - b'0'),
                b'a'..=b'f' => Ok(b - b'a' + 10),
                b'A'..=b'F' => Ok(b - b'A' + 10),
                _ => Err(InputError),
            };
            *out = digit(pair[0])? * 16 + digit(pair[1])?;
        }
        let mut parsed = Vec::with_capacity(4);
        for seed in seeds.split(',') {
            if parsed.len() == 4 || seed.trim() != seed {
                return Err(InputError);
            }
            let endpoint: SocketAddrV4 = seed.parse().map_err(|_| InputError)?;
            if endpoint.port() == 0 || parsed.contains(&endpoint) {
                return Err(InputError);
            }
            parsed.push(endpoint);
        }
        Ok(Self {
            infohash,
            seeds: parsed,
            budget: LOOKUP_BUDGET,
            target: 8,
            result_cap: 1024,
        })
    }

    /// Lend the target without including it in Debug or error messages.
    pub fn infohash(&self) -> &[u8; 20] {
        &self.infohash
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HASH: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn parses_only_explicit_numeric_seeds_without_exposing_hash() {
        let input = WorkerInput::parse(HASH, "127.0.0.1:9001,192.0.2.1:9002").unwrap();
        assert_eq!(input.seeds.len(), 2);
        assert_eq!(input.infohash()[0..2], [1, 35]);
        assert_eq!(
            WorkerInput::parse(&HASH.to_ascii_uppercase(), "127.0.0.1:9001")
                .unwrap()
                .infohash(),
            input.infohash()
        );
        assert!(!format!("{input:?}").contains(HASH));
        assert!(!format!("{input:?}").contains("127.0.0.1"));
        assert_eq!(input.budget.as_secs(), 15);
        assert_eq!(input.target, 8);
        assert_eq!(input.result_cap, 1024);
    }

    #[test]
    fn rejects_bad_hashes_with_a_sanitized_error() {
        for hash in [
            "",
            "secret",
            "0123456789abcdef0123456789abcdef0123456g",
            "0123456789abcdef0123456789abcdef0123456700",
        ] {
            let error = WorkerInput::parse(hash, "127.0.0.1:9001").unwrap_err();
            assert_eq!(error.to_string(), "invalid_worker_input");
            assert!(!format!("{error:?}").contains(hash) || hash.is_empty());
        }
    }

    #[test]
    fn rejects_implicit_duplicate_or_unbounded_seeds() {
        for seeds in [
            "",
            "localhost:9001",
            "127.0.0.1:0",
            "[::1]:9001",
            "127.0.0.1:9001,127.0.0.1:9001",
            "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003,127.0.0.1:9004,127.0.0.1:9005",
            "127.0.0.1:9001,",
            " 127.0.0.1:9001",
        ] {
            assert!(WorkerInput::parse(HASH, seeds).is_err(), "accepted {seeds}");
        }
        assert!(WorkerInput::parse(HASH, &"x".repeat(257)).is_err());
    }
}

#[cfg(test)]
mod accounting_tests {
    use super::*;
    fn peer(port: u16) -> SocketAddrV4 {
        SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, port)
    }
    #[test]
    fn target_is_checked_after_an_atomic_unique_batch() {
        let mut count = Accounting::new(2, 4);
        assert!(!count.observe(&[peer(1), peer(1)], Duration::from_millis(3)));
        assert!(count.observe(&[peer(2), peer(3)], Duration::from_millis(5)));
        assert_eq!(count.count(), 3);
        assert_eq!(count.first_peer(), Some(Duration::from_millis(3)));
    }
    #[test]
    fn cap_and_zero_port_are_enforced_before_retaining() {
        let mut count = Accounting::new(8, 2);
        assert!(count.observe(&[peer(0), peer(1), peer(2), peer(3)], Duration::ZERO));
        assert_eq!(count.count(), 2);
        assert_eq!(count.first_peer(), Some(Duration::ZERO));
    }
    #[test]
    fn emission_metadata_is_bounded_and_keeps_atomic_overshoot() {
        let mut count = Accounting::new(2, 1024);
        assert!(count.observe(&[peer(1), peer(1), peer(2), peer(3)], Duration::ZERO));
        assert_eq!(count.batch_sizes(), &[4]);
        assert_eq!(count.batches_observed(), 1);
        assert_eq!(count.target_overshoot(), 1);
        assert!(!count.batch_history_truncated());
        let mut empty = Accounting::new(8, 1024);
        for _ in 0..65 {
            assert!(!empty.observe(&[], Duration::ZERO));
        }
        assert_eq!(empty.batch_sizes(), &[0; 64]);
        assert_eq!(empty.batches_observed(), 65);
        assert!(empty.batch_history_truncated());
        assert_eq!(empty.count(), 0);
        assert!(empty.first_peer().is_none());
    }
    #[test]
    fn empty_observation_is_not_a_zero_first_peer_time() {
        let mut count = Accounting::new(8, 1024);
        assert!(!count.observe(&[], Duration::from_secs(1)));
        assert_eq!(count.first_peer(), None);
        assert_eq!(
            remaining_budget(Duration::from_secs(14)),
            Some(Duration::from_secs(1))
        );
        assert_eq!(remaining_budget(Duration::from_secs(15)), None);
        assert_eq!(remaining_budget(Duration::from_secs(20)), None);
    }
}

/// Bounded numeric accounting; addresses never appear in the emitted record.
pub struct Accounting {
    peers: std::collections::BTreeSet<SocketAddrV4>,
    first: Option<Duration>,
    target: usize,
    cap: usize,
    batch_sizes: Vec<usize>,
    batches_observed: usize,
}
impl Accounting {
    pub fn new(target: usize, cap: usize) -> Self {
        Self {
            peers: Default::default(),
            first: None,
            target: target.clamp(1, 1024),
            cap: cap.clamp(1, 1024),
            batch_sizes: Vec::new(),
            batches_observed: 0,
        }
    }
    pub fn observe(&mut self, batch: &[SocketAddrV4], elapsed: Duration) -> bool {
        self.batches_observed = self.batches_observed.saturating_add(1);
        if self.batch_sizes.len() < 64 {
            self.batch_sizes.push(batch.len());
        }
        for &peer in batch {
            if self.peers.len() >= self.cap {
                break;
            }
            if peer.port() != 0 && self.peers.insert(peer) {
                self.first.get_or_insert(elapsed);
            }
        }
        self.peers.len() >= self.target || self.peers.len() >= self.cap
    }
    /// Sizes are stream emissions, before this collector filters duplicates/zero ports.
    pub fn batch_sizes(&self) -> &[usize] {
        &self.batch_sizes
    }
    pub fn batches_observed(&self) -> usize {
        self.batches_observed
    }
    pub fn batch_history_truncated(&self) -> bool {
        self.batches_observed > self.batch_sizes.len()
    }
    pub fn target_overshoot(&self) -> usize {
        self.peers.len().saturating_sub(self.target)
    }
    pub fn count(&self) -> usize {
        self.peers.len()
    }
    pub fn first_peer(&self) -> Option<Duration> {
        self.first
    }
}

pub fn remaining_budget(elapsed: Duration) -> Option<Duration> {
    LOOKUP_BUDGET
        .checked_sub(elapsed)
        .filter(|left| !left.is_zero())
}

#[cfg(feature = "candidates")]
pub mod candidates;
