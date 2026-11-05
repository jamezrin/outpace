use super::*;

fn manager() -> Arc<StreamManager> {
    let mut providers = ProviderRegistry::new();
    providers.register(Arc::new(crate::testprovider::TestProvider { chunks: 0 }));
    StreamManager::new(providers)
}

#[tokio::test]
async fn force_stop_disables_all_same_key_declarations_until_explicit_start() {
    let manager = manager();
    let first = manager.register_support("test", "synthetic").await.unwrap();
    let second = manager.register_support("test", "synthetic").await.unwrap();
    let other = manager.register_support("test", "other").await.unwrap();
    let updates = first.subscribe();
    let initial = first.intent();
    manager.stop_checked("test", "synthetic").await.unwrap();
    let stopped = first.intent();
    let notification = updates.has_changed().unwrap();
    let both_stopped = !stopped.running && !second.intent().running;
    manager.get_or_start("test", "synthetic").await.unwrap();
    let viewer_did_not_enable = !first.intent().running;
    let restarted = manager.set_support_running(&first, true).await.unwrap();
    let unrelated_unchanged = other.intent().running;
    assert!(manager.shutdown().await);
    assert!(both_stopped && viewer_did_not_enable && notification);
    assert!(stopped.epoch > initial.epoch && restarted.epoch > stopped.epoch);
    assert!(restarted.running && unrelated_unchanged);
}

#[tokio::test]
async fn registration_is_bounded_and_dropped_declarations_release_slots() {
    let manager = manager();
    let mut controls = Vec::new();
    for _ in 0..8 {
        controls.push(manager.register_support("test", "synthetic").await.unwrap());
    }
    let capped = matches!(
        manager.register_support("test", "ninth").await,
        Err(ProviderError::CapacityExhausted)
    );
    controls.pop();
    let replacement = manager
        .register_support("test", "replacement")
        .await
        .unwrap();
    assert!(manager.shutdown().await);
    assert!(
        capped,
        "ninth declaration must be rejected without growing registry"
    );
    assert!(!replacement.intent().running);
}

#[tokio::test]
async fn foreign_controls_and_post_shutdown_starts_are_rejected() {
    let first = manager();
    let second = manager();
    let control = first.register_support("test", "synthetic").await.unwrap();
    let foreign = second.set_support_running(&control, false).await.is_err();
    assert!(first.shutdown().await);
    let closed = first.register_support("test", "later").await.is_err();
    let restart = first.set_support_running(&control, true).await.is_err();
    assert!(second.shutdown().await);
    assert!(foreign && closed && restart);
}

#[tokio::test]
async fn declared_acquisition_rejects_stopped_stale_and_foreign_intents() {
    let manager = manager();
    let foreign = super::support_control_tests::manager();
    let control = manager.register_support("test", "synthetic").await.unwrap();
    let old = control.intent();
    manager.set_support_running(&control, false).await.unwrap();
    let stopped = manager
        .acquire_declared_support(&control, old.epoch)
        .await
        .is_err();
    let fresh = manager.set_support_running(&control, true).await.unwrap();
    let stale = manager
        .acquire_declared_support(&control, old.epoch)
        .await
        .is_err();
    let foreign_rejected = foreign
        .acquire_declared_support(&control, fresh.epoch)
        .await
        .is_err();
    let pin = manager
        .acquire_declared_support(&control, fresh.epoch)
        .await
        .unwrap();
    drop(pin);
    assert!(manager.shutdown().await);
    assert!(foreign.shutdown().await);
    assert!(stopped && stale && foreign_rejected);
}

struct ClassifiedProvider {
    legacy: std::sync::atomic::AtomicUsize,
    classified: std::sync::atomic::AtomicUsize,
}
#[async_trait::async_trait]
impl crate::provider::StreamProvider for ClassifiedProvider {
    fn network(&self) -> &'static str {
        "typed"
    }
    async fn open(&self, _: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        self.legacy
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(ProviderError::Backend("same diagnostic".into()))
    }
    async fn open_classified(
        &self,
        _: &str,
    ) -> Result<Box<dyn TsSource>, crate::start_failure::ProviderStartFailure> {
        self.classified
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(crate::start_failure::ProviderStartFailure::new(
            crate::start_failure::StartFailureKind::Resolution(
                ace_swarm::resolve::ResolveFailureKind::SafetyRefusal,
            ),
            ProviderError::Backend("same diagnostic".into()),
        ))
    }
}
#[tokio::test]
async fn declared_manager_start_preserves_actual_provider_cause_and_releases_capacity() {
    let provider = Arc::new(ClassifiedProvider {
        legacy: 0.into(),
        classified: 0.into(),
    });
    let mut registry = ProviderRegistry::new();
    registry.register(provider.clone());
    let manager = StreamManager::with_session_limit(registry, 1);
    let control = manager
        .register_support("typed", "synthetic")
        .await
        .unwrap();
    let error = manager
        .acquire_declared_support(&control, control.intent().epoch)
        .await
        .err()
        .unwrap();
    assert!(manager.shutdown().await);
    assert_eq!(manager.admission_status().0, 0);
    assert_eq!(
        error.kind,
        crate::start_failure::StartFailureKind::Resolution(
            ace_swarm::resolve::ResolveFailureKind::SafetyRefusal
        )
    );
    assert!(!error.kind.retryable());
    assert_eq!(error.error.to_string(), "same diagnostic");
    assert_eq!(
        provider
            .classified
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(provider.legacy.load(std::sync::atomic::Ordering::SeqCst), 0);
}
