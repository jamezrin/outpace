//! Native stream start progress (`phase`) and stable failure codes for
//! `GET /streams/{network}/{id}/status` and the native start routes.
//!
//! The record of recent start attempts is bounded in size ([`START_ATTEMPT_CAPACITY`]) and in
//! time ([`START_ATTEMPT_TTL`] after a failure). It changes only at a few transitions per start
//! and is pruned lazily when touched: no background task, and nothing on the media path.

use crate::provider::ProviderError;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

/// Most start attempts remembered at once (in progress or recently failed).
pub const START_ATTEMPT_CAPACITY: usize = 64;
/// How long a failed attempt stays visible on the status route after it failed.
pub const START_ATTEMPT_TTL: Duration = Duration::from_secs(60);

type StreamKey = (String, String);

/// Where a live stream start currently is. Serialized as the lowercase `phase` string.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartPhase {
    /// Resolving the id to its descriptor.
    Resolving,
    /// Descriptor known; looking for and connecting to peers.
    Connecting,
    /// Connected to at least one peer; waiting for the first media output.
    Buffering,
    /// The session has produced media output.
    Playing,
    /// The start failed; see the accompanying [`StartError`].
    Failed,
}

impl StartPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Resolving => "resolving",
            Self::Connecting => "connecting",
            Self::Buffering => "buffering",
            Self::Playing => "playing",
            Self::Failed => "failed",
        }
    }
}

/// Stable, machine-readable failure code. Clients key off this; the message is free text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartErrorCode {
    /// Unknown network, or the start was refused without a more specific cause.
    NotFound,
    /// No peer answered metadata resolution, or none was found within the start budget.
    NoPeers,
    /// The network path (catalog hosts) was unreachable.
    Network,
    /// The id is well-formed but has no verified metadata to open it with.
    Unresolvable,
    /// Encrypted or invalid-protection content, which outpace does not play.
    Protected,
    /// The session (or conversion) capacity is exhausted; retry later.
    Capacity,
    /// Not enough free disk space to download the content (VOD).
    NoSpace,
    /// Any other engine failure.
    Internal,
}

impl StartErrorCode {
    pub const ALL: [StartErrorCode; 8] = [
        Self::NotFound,
        Self::NoPeers,
        Self::Network,
        Self::Unresolvable,
        Self::Protected,
        Self::Capacity,
        Self::NoSpace,
        Self::Internal,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::NoPeers => "no_peers",
            Self::Network => "network",
            Self::Unresolvable => "unresolvable",
            Self::Protected => "protected",
            Self::Capacity => "capacity",
            Self::NoSpace => "no_space",
            Self::Internal => "internal",
        }
    }

    /// Fixed user-facing text. Provider diagnostics may embed identifiers or URLs, so they
    /// stay in the operational log and never reach the response body.
    fn default_message(self) -> &'static str {
        match self {
            Self::NotFound => "stream not found",
            Self::NoPeers => "no peer is sharing this stream",
            Self::Network => "the network is unreachable",
            Self::Unresolvable => "stream metadata unavailable",
            Self::Protected => "protected content is not supported",
            Self::Capacity => "session capacity reached",
            Self::NoSpace => "not enough free disk space",
            Self::Internal => "stream start failed",
        }
    }
}

/// A typed start failure: `{"code": "...", "message": "..."}` on the wire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartError {
    pub code: StartErrorCode,
    pub message: String,
}

impl StartError {
    pub fn new(code: StartErrorCode) -> Self {
        Self {
            code,
            message: code.default_message().to_owned(),
        }
    }

    /// Classify a provider failure. Only messages documented as user-facing
    /// (`Unresolvable`, and the fixed protection texts) are passed through.
    pub fn from_provider(error: &ProviderError) -> Self {
        match error {
            ProviderError::NotFound => Self::new(StartErrorCode::NotFound),
            ProviderError::NoPeers(_) => Self::new(StartErrorCode::NoPeers),
            ProviderError::Network(_) | ProviderError::UnresolvableNetwork(_) => {
                Self::new(StartErrorCode::Network)
            }
            ProviderError::Unresolvable(reason) => Self {
                code: StartErrorCode::Unresolvable,
                message: reason.clone(),
            },
            ProviderError::ProtectedContent | ProviderError::InvalidProtection => Self {
                code: StartErrorCode::Protected,
                message: error.to_string(),
            },
            ProviderError::CapacityExhausted | ProviderError::ResolverBusy(_) => {
                Self::new(StartErrorCode::Capacity)
            }
            ProviderError::NoSpace(message) => Self {
                code: StartErrorCode::NoSpace,
                message: message.clone(),
            },
            ProviderError::Backend(_) | ProviderError::DirectHls(_) => {
                Self::new(StartErrorCode::Internal)
            }
        }
    }

    /// Classify an audio normalization failure that ended a session before any output:
    /// conversion admission is `capacity`, anything else `internal` with its fixed reason code.
    pub fn from_normalization(failure: crate::audio_normalize::Failure) -> Self {
        match failure {
            crate::audio_normalize::Failure::Admission => Self {
                code: StartErrorCode::Capacity,
                message: "audio conversion capacity reached".to_owned(),
            },
            other => Self {
                code: StartErrorCode::Internal,
                message: format!("audio normalization failed: {}", other.code()),
            },
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({ "code": self.code.as_str(), "message": self.message })
    }
}

/// A recorded start attempt that has no running session (yet, or any more).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttemptStatus {
    Resolving,
    Connecting,
    Failed(StartError),
}

impl AttemptStatus {
    pub fn phase(&self) -> StartPhase {
        match self {
            Self::Resolving => StartPhase::Resolving,
            Self::Connecting => StartPhase::Connecting,
            Self::Failed(_) => StartPhase::Failed,
        }
    }

    pub fn error(&self) -> Option<&StartError> {
        match self {
            Self::Failed(error) => Some(error),
            _ => None,
        }
    }
}

struct Attempt {
    key: StreamKey,
    seq: u64,
    status: AttemptStatus,
    failed_at: Option<Instant>,
}

#[derive(Default)]
struct AttemptLog {
    next_seq: u64,
    entries: VecDeque<Attempt>,
}

impl AttemptLog {
    fn prune(&mut self, now: Instant) {
        self.entries.retain(|attempt| {
            attempt
                .failed_at
                .is_none_or(|at| now.saturating_duration_since(at) < START_ATTEMPT_TTL)
        });
    }

    fn position(&self, key: &StreamKey, seq: u64) -> Option<usize> {
        self.entries
            .iter()
            .position(|attempt| attempt.seq == seq && attempt.key == *key)
    }
}

/// Bounded record of recent live start attempts, keyed like the manager's sessions.
#[derive(Default)]
pub struct StartAttempts {
    log: Mutex<AttemptLog>,
}

impl StartAttempts {
    /// Record a new attempt for `key` in [`AttemptStatus::Resolving`], replacing any earlier
    /// record for it. The returned guard removes the record when dropped unless
    /// [`StartAttempt::fail`] settled it, so success and cancellation leave nothing behind.
    pub fn begin(self: &Arc<Self>, key: StreamKey) -> StartAttempt {
        self.begin_at(key, Instant::now())
    }

    fn begin_at(self: &Arc<Self>, key: StreamKey, now: Instant) -> StartAttempt {
        let mut log = self.log.lock().unwrap();
        log.prune(now);
        log.entries.retain(|attempt| attempt.key != key);
        if log.entries.len() >= START_ATTEMPT_CAPACITY {
            // Evict the oldest failure first; in-progress attempts only when none is settled.
            let victim = log
                .entries
                .iter()
                .position(|attempt| attempt.failed_at.is_some())
                .unwrap_or(0);
            log.entries.remove(victim);
        }
        let seq = log.next_seq;
        log.next_seq = log.next_seq.wrapping_add(1);
        log.entries.push_back(Attempt {
            key: key.clone(),
            seq,
            status: AttemptStatus::Resolving,
            failed_at: None,
        });
        StartAttempt {
            attempts: self.clone(),
            key,
            seq,
            armed: true,
        }
    }

    /// The recorded attempt for `key`, if it is in progress or failed within the TTL.
    pub fn status(&self, key: &StreamKey) -> Option<AttemptStatus> {
        self.status_at(key, Instant::now())
    }

    fn status_at(&self, key: &StreamKey, now: Instant) -> Option<AttemptStatus> {
        let mut log = self.log.lock().unwrap();
        log.prune(now);
        log.entries
            .iter()
            .find(|attempt| attempt.key == *key)
            .map(|attempt| attempt.status.clone())
    }

    fn update(&self, key: &StreamKey, seq: u64, status: AttemptStatus, now: Instant) {
        let mut log = self.log.lock().unwrap();
        if let Some(index) = log.position(key, seq) {
            let attempt = &mut log.entries[index];
            // Failure is terminal: a late progress report must not revive it or its expiry.
            if attempt.failed_at.is_some() {
                return;
            }
            attempt.failed_at = matches!(status, AttemptStatus::Failed(_)).then_some(now);
            attempt.status = status;
        }
    }

    fn remove(&self, key: &StreamKey, seq: u64) {
        let mut log = self.log.lock().unwrap();
        if let Some(index) = log.position(key, seq) {
            log.entries.remove(index);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.log.lock().unwrap().entries.len()
    }
}

/// Owner of one in-progress attempt record.
pub struct StartAttempt {
    attempts: Arc<StartAttempts>,
    key: StreamKey,
    seq: u64,
    armed: bool,
}

impl StartAttempt {
    /// A handle the provider uses to report progress for this attempt.
    pub fn progress(&self) -> StartProgress {
        StartProgress {
            target: Some((Arc::downgrade(&self.attempts), self.key.clone(), self.seq)),
        }
    }

    /// Settle the attempt as failed; it stays visible for [`START_ATTEMPT_TTL`].
    pub fn fail(mut self, error: &ProviderError) {
        self.armed = false;
        self.attempts.update(
            &self.key,
            self.seq,
            AttemptStatus::Failed(StartError::from_provider(error)),
            Instant::now(),
        );
    }
}

impl Drop for StartAttempt {
    fn drop(&mut self) {
        if self.armed {
            self.attempts.remove(&self.key, self.seq);
        }
    }
}

/// Progress reporter passed to [`crate::provider::StreamProvider::open_with_progress`].
/// A default (detached) reporter ignores every report.
#[derive(Clone, Default)]
pub struct StartProgress {
    target: Option<(Weak<StartAttempts>, StreamKey, u64)>,
}

impl StartProgress {
    pub fn none() -> Self {
        Self::default()
    }

    /// The descriptor is known; the provider is now looking for and connecting to peers.
    pub fn resolved(&self) {
        if let Some((attempts, key, seq)) = &self.target {
            if let Some(attempts) = attempts.upgrade() {
                attempts.update(key, *seq, AttemptStatus::Connecting, Instant::now());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(id: &str) -> StreamKey {
        ("test".into(), id.into())
    }

    #[test]
    fn codes_and_phases_have_stable_strings() {
        let codes: Vec<_> = StartErrorCode::ALL.iter().map(|c| c.as_str()).collect();
        assert_eq!(
            codes,
            [
                "not_found",
                "no_peers",
                "network",
                "unresolvable",
                "protected",
                "capacity",
                "no_space",
                "internal"
            ]
        );
        let phases: Vec<_> = [
            StartPhase::Resolving,
            StartPhase::Connecting,
            StartPhase::Buffering,
            StartPhase::Playing,
            StartPhase::Failed,
        ]
        .iter()
        .map(|p| p.as_str())
        .collect();
        assert_eq!(
            phases,
            ["resolving", "connecting", "buffering", "playing", "failed"]
        );
        assert_eq!(
            StartError::new(StartErrorCode::NoSpace).to_json(),
            serde_json::json!({"code": "no_space", "message": "not enough free disk space"})
        );
    }

    #[test]
    fn provider_errors_map_to_codes_without_leaking_diagnostics() {
        let cases = [
            (ProviderError::NotFound, StartErrorCode::NotFound),
            (
                ProviderError::NoPeers("detail cid:secret".into()),
                StartErrorCode::NoPeers,
            ),
            (
                ProviderError::Network("detail http://host".into()),
                StartErrorCode::Network,
            ),
            (
                ProviderError::Unresolvable("use cid:<id>".into()),
                StartErrorCode::Unresolvable,
            ),
            (ProviderError::ProtectedContent, StartErrorCode::Protected),
            (ProviderError::InvalidProtection, StartErrorCode::Protected),
            (ProviderError::CapacityExhausted, StartErrorCode::Capacity),
            (
                ProviderError::Backend("detail http://host".into()),
                StartErrorCode::Internal,
            ),
            (
                ProviderError::ResolverBusy("detail resolver".into()),
                StartErrorCode::Capacity,
            ),
            (
                ProviderError::UnresolvableNetwork("detail http://host".into()),
                StartErrorCode::Network,
            ),
        ];
        for (error, code) in cases {
            let mapped = StartError::from_provider(&error);
            assert_eq!(mapped.code, code, "{error}");
            assert!(!mapped.message.contains("detail"), "{error}");
        }
        assert_eq!(
            StartError::from_provider(&ProviderError::Unresolvable("use cid:<id>".into())).message,
            "use cid:<id>"
        );
        assert_eq!(
            StartError::from_provider(&ProviderError::ProtectedContent).message,
            "encrypted content is not supported"
        );
        let no_space = StartError::from_provider(&ProviderError::NoSpace(
            "not enough free disk space: 356 MiB required, 300 MiB available".into(),
        ));
        assert_eq!(no_space.code, StartErrorCode::NoSpace);
        assert_eq!(
            no_space.message,
            "not enough free disk space: 356 MiB required, 300 MiB available"
        );
    }

    #[test]
    fn attempt_moves_through_phases_and_success_leaves_no_record() {
        let attempts = Arc::new(StartAttempts::default());
        let attempt = attempts.begin(key("a"));
        assert_eq!(attempts.status(&key("a")), Some(AttemptStatus::Resolving));
        attempt.progress().resolved();
        assert_eq!(attempts.status(&key("a")), Some(AttemptStatus::Connecting));
        drop(attempt);
        assert_eq!(attempts.status(&key("a")), None);
    }

    #[test]
    fn failure_is_retained_for_the_ttl_then_pruned() {
        let attempts = Arc::new(StartAttempts::default());
        let start = Instant::now();
        attempts
            .begin_at(key("a"), start)
            .fail(&ProviderError::NoPeers("x".into()));
        let failed = attempts.status(&key("a")).unwrap();
        assert_eq!(failed.phase(), StartPhase::Failed);
        assert_eq!(failed.error().unwrap().code, StartErrorCode::NoPeers);
        let later = Instant::now() + START_ATTEMPT_TTL;
        assert_eq!(attempts.status_at(&key("a"), later), None);
        assert_eq!(attempts.len(), 0);
    }

    #[test]
    fn a_new_attempt_replaces_the_previous_record_and_stale_guards_are_inert() {
        let attempts = Arc::new(StartAttempts::default());
        let first = attempts.begin(key("a"));
        let stale = first.progress();
        let second = attempts.begin(key("a"));
        stale.resolved();
        drop(first);
        assert_eq!(attempts.status(&key("a")), Some(AttemptStatus::Resolving));
        second.fail(&ProviderError::NotFound);
        assert_eq!(attempts.len(), 1);
    }

    #[test]
    fn late_progress_never_overwrites_a_failure() {
        let attempts = Arc::new(StartAttempts::default());
        let attempt = attempts.begin(key("a"));
        let progress = attempt.progress();
        attempt.fail(&ProviderError::NoPeers("x".into()));
        progress.resolved();
        let status = attempts.status(&key("a")).unwrap();
        assert_eq!(status.phase(), StartPhase::Failed);
        assert_eq!(status.error().unwrap().code, StartErrorCode::NoPeers);
        // Still expires: the failure time survived the late report.
        assert_eq!(
            attempts.status_at(&key("a"), Instant::now() + START_ATTEMPT_TTL),
            None
        );
    }

    #[test]
    fn record_is_bounded_and_evicts_failures_before_in_progress_attempts() {
        let attempts = Arc::new(StartAttempts::default());
        let mut live = Vec::new();
        attempts.begin(key("failed")).fail(&ProviderError::NotFound);
        for n in 0..START_ATTEMPT_CAPACITY - 1 {
            live.push(attempts.begin(key(&format!("live-{n}"))));
        }
        assert_eq!(attempts.len(), START_ATTEMPT_CAPACITY);
        live.push(attempts.begin(key("newest")));
        assert_eq!(attempts.len(), START_ATTEMPT_CAPACITY);
        assert_eq!(attempts.status(&key("failed")), None);
        assert!(attempts.status(&key("live-0")).is_some());
        live.push(attempts.begin(key("overflow")));
        assert_eq!(attempts.len(), START_ATTEMPT_CAPACITY);
        assert_eq!(attempts.status(&key("live-0")), None);
        drop(live);
        assert_eq!(attempts.len(), 0);
    }

    #[test]
    fn detached_progress_and_dropped_registry_are_ignored() {
        StartProgress::none().resolved();
        let attempts = Arc::new(StartAttempts::default());
        let attempt = attempts.begin(key("a"));
        let progress = attempt.progress();
        drop(attempt);
        drop(attempts);
        progress.resolved();
    }
}
