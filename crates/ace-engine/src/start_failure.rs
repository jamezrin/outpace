//! Per-attempt opening failures; diagnostics never determine retry policy.
use crate::provider::ProviderError;
use ace_swarm::resolve::{ResolveError, ResolveFailureKind};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartFailureKind {
    Resolution(ResolveFailureKind),
    Capacity,
    Cooldown,
    Discovery,
    Cancelled,
    CleanupUnconfirmed,
    Unclassified,
}
impl StartFailureKind {
    pub fn retryable(self) -> bool {
        matches!(
            self,
            Self::Capacity
                | Self::Cooldown
                | Self::Discovery
                | Self::Unclassified
                | Self::Resolution(
                    ResolveFailureKind::Network
                        | ResolveFailureKind::RejectedMetadata
                        | ResolveFailureKind::Unclassified
                )
        )
    }
}

#[derive(Debug)]
pub struct ProviderStartFailure {
    pub kind: StartFailureKind,
    pub error: ProviderError,
}
impl ProviderStartFailure {
    pub fn new(kind: StartFailureKind, error: ProviderError) -> Self {
        Self { kind, error }
    }
}
impl From<ProviderError> for ProviderStartFailure {
    fn from(error: ProviderError) -> Self {
        let kind = match &error {
            ProviderError::ProtectedContent => {
                StartFailureKind::Resolution(ResolveFailureKind::ProtectedContent)
            }
            ProviderError::InvalidProtection => {
                StartFailureKind::Resolution(ResolveFailureKind::InvalidProtection)
            }
            ProviderError::CapacityExhausted => StartFailureKind::Capacity,
            ProviderError::NoPeers(_) => StartFailureKind::Discovery,
            ProviderError::Network(_) => StartFailureKind::Resolution(ResolveFailureKind::Network),
            _ => StartFailureKind::Unclassified,
        };
        Self::new(kind, error)
    }
}
impl From<ResolveError> for ProviderStartFailure {
    fn from(error: ResolveError) -> Self {
        let kind = StartFailureKind::Resolution(error.failure_kind());
        Self::new(kind, ProviderError::from(error))
    }
}

impl From<ProviderStartFailure> for ProviderError {
    fn from(failure: ProviderStartFailure) -> Self {
        failure.error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::StreamProvider;
    use async_trait::async_trait;
    struct Unknown;
    #[async_trait]
    impl StreamProvider for Unknown {
        fn network(&self) -> &'static str {
            "synthetic"
        }
        async fn open(&self, _: &str) -> Result<Box<dyn crate::provider::TsSource>, ProviderError> {
            Err(ProviderError::Backend("same diagnostic".into()))
        }
    }
    #[tokio::test]
    async fn unknown_provider_errors_remain_bounded_retry_candidates() {
        let error = Unknown.open_classified("synthetic").await.err().unwrap();
        assert_eq!(error.kind, StartFailureKind::Unclassified);
        assert!(error.kind.retryable());
        assert_eq!(error.error.to_string(), "same diagnostic");
    }
    #[test]
    fn resolved_cause_is_retained_before_legacy_error_conversion() {
        let safety = ProviderStartFailure::from(ResolveError::Fetch(
            ace_swarm::http_fetch::FetchError::RedirectRejected,
        ));
        let network = ProviderStartFailure::from(ResolveError::Fetch(
            ace_swarm::http_fetch::FetchError::HttpStatus,
        ));
        assert_eq!(safety.error.to_string(), network.error.to_string());
        assert_eq!(
            safety.kind,
            StartFailureKind::Resolution(ResolveFailureKind::SafetyRefusal)
        );
        assert!(!safety.kind.retryable());
        assert_eq!(
            network.kind,
            StartFailureKind::Resolution(ResolveFailureKind::Network)
        );
        assert!(network.kind.retryable());
    }
    #[test]
    fn explicit_capacity_and_protection_keep_their_known_policies() {
        assert!(ProviderStartFailure::from(ProviderError::CapacityExhausted)
            .kind
            .retryable());
        assert_eq!(
            ProviderStartFailure::from(ProviderError::CapacityExhausted).kind,
            StartFailureKind::Capacity
        );
        assert!(!ProviderStartFailure::from(ProviderError::ProtectedContent)
            .kind
            .retryable());
        assert!(
            !ProviderStartFailure::from(ProviderError::InvalidProtection)
                .kind
                .retryable()
        );
        assert!(!StartFailureKind::Cancelled.retryable());
        assert!(!StartFailureKind::CleanupUnconfirmed.retryable());
    }
}
