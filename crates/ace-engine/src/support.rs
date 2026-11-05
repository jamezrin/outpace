//! Owned autonomous support supervision; status never contains source selectors.
use crate::start_failure::StartFailureKind;
use crate::{
    manager::{CleanupTracker, StreamManager, WorkerControl},
    provider::ProviderError,
    support_config::SupportSources,
    support_control::{SupportControl, SupportIntent},
};
use futures::FutureExt;
use serde::Serialize;
use std::time::Duration;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex as StdMutex,
    },
};
use tokio::sync::{watch, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportState {
    Starting,
    Running,
    Retrying,
    Stopped,
    Error,
}
#[derive(Clone, Debug, Serialize)]
pub struct SupportStatus {
    pub name: String,
    pub epoch: u64,
    pub state: SupportState,
    pub reason: Option<&'static str>,
    pub attempts: u64,
    pub downloaded: u64,
    pub retry_ms: u64,
    pub transition_pending: bool,
}
struct Entry {
    control: Arc<SupportControl>,
    status: watch::Sender<SupportStatus>,
    cleanup_failed: AtomicBool,
}
pub struct SupportService {
    manager: Arc<StreamManager>,
    entries: BTreeMap<String, Arc<Entry>>,
    workers: StdMutex<Vec<WorkerControl>>,
    cleanup: CleanupTracker,
    admission: Mutex<()>,
    closed: AtomicBool,
    failed: Arc<AtomicBool>,
}
impl SupportService {
    pub async fn spawn(
        manager: Arc<StreamManager>,
        sources: &SupportSources,
    ) -> Result<Arc<Self>, ProviderError> {
        let mut entries = BTreeMap::new();
        for (name, id) in sources.iter() {
            let control = manager.register_support("ace", id).await?;
            let (status, _) = watch::channel(SupportStatus {
                name: name.into(),
                epoch: control.intent().epoch,
                state: SupportState::Starting,
                reason: None,
                attempts: 0,
                downloaded: 0,
                retry_ms: 0,
                transition_pending: false,
            });
            entries.insert(
                name.into(),
                Arc::new(Entry {
                    control,
                    status,
                    cleanup_failed: AtomicBool::new(false),
                }),
            );
        }
        let service = Arc::new(Self {
            manager,
            entries,
            workers: StdMutex::new(Vec::new()),
            cleanup: CleanupTracker::default(),
            admission: Mutex::new(()),
            closed: AtomicBool::new(false),
            failed: Arc::new(AtomicBool::new(false)),
        });
        for entry in service.entries.values() {
            let manager = service.manager.clone();
            let entry = entry.clone();
            let failed = service.failed.clone();
            let worker = service.cleanup.spawn(move |cancel| async move {
                let result = std::panic::AssertUnwindSafe(run(
                    manager.clone(),
                    entry.clone(),
                    failed.clone(),
                    cancel,
                ))
                .catch_unwind()
                .await;
                if result.is_err() {
                    // A dropped logical pin is not proof of completed source cleanup.
                    // Keep the manager owner and expose the uncertainty; never restart.
                    fault(&entry, &failed);
                    let _ = manager.set_support_running(&entry.control, false).await;
                    let attempts = entry.status.borrow().attempts;
                    publish(
                        &entry,
                        entry.control.intent().epoch,
                        SupportState::Error,
                        Some("cleanup_unconfirmed"),
                        attempts,
                        0,
                    );
                    crate::alog!("[support] worker failed; cleanup is unconfirmed");
                }
            });
            service.workers.lock().unwrap().push(worker);
        }
        Ok(service)
    }
    pub(crate) fn declares(&self, name: &str) -> bool {
        self.entries.contains_key(name)
    }

    pub fn status(&self) -> Vec<SupportStatus> {
        self.entries
            .values()
            .map(|entry| {
                let mut status = entry.status.borrow().clone();
                let intent = entry.control.intent();
                if entry.cleanup_failed.load(Ordering::Acquire) {
                    status.state = SupportState::Error;
                    status.reason = Some("cleanup_unconfirmed");
                    status.epoch = intent.epoch;
                } else if status.epoch != intent.epoch {
                    status.epoch = intent.epoch;
                    status.state = if intent.running {
                        SupportState::Starting
                    } else {
                        SupportState::Stopped
                    };
                    status.reason = Some("transition_pending");
                    status.transition_pending = true;
                }
                status
            })
            .collect()
    }
    pub async fn stop(&self, name: &str) -> Result<SupportIntent, ProviderError> {
        let _admission = self.admission.lock().await;
        let entry = self.entries.get(name).ok_or(ProviderError::NotFound)?;
        self.manager
            .set_support_running(&entry.control, false)
            .await
    }
    pub async fn start(&self, name: &str) -> Result<SupportIntent, ProviderError> {
        let _admission = self.admission.lock().await;
        if self.closed.load(Ordering::Acquire) {
            return Err(ProviderError::NotFound);
        }
        let entry = self.entries.get(name).ok_or(ProviderError::NotFound)?;
        if entry.cleanup_failed.load(Ordering::Acquire) {
            return Err(ProviderError::Backend(
                "support cleanup is unconfirmed".into(),
            ));
        }
        let status = entry.status.borrow().clone();
        if status.state == SupportState::Error && status.epoch == entry.control.intent().epoch {
            self.manager
                .restart_support(&entry.control, status.epoch)
                .await
        } else {
            self.manager.set_support_running(&entry.control, true).await
        }
    }
    pub async fn shutdown(&self) -> bool {
        {
            let _admission = self.admission.lock().await;
            self.closed.store(true, Ordering::Release);
            let mut workers = self.workers.lock().unwrap();
            for worker in workers.iter() {
                worker.cancellation_signal().send_replace(true);
            }
            for mut worker in workers.drain(..) {
                worker.transfer();
            }
        }
        self.cleanup.drain().await && !self.failed.load(Ordering::Acquire)
    }
}

fn publish(
    entry: &Entry,
    epoch: u64,
    state: SupportState,
    reason: Option<&'static str>,
    attempts: u64,
    retry_ms: u64,
) {
    entry.status.send_modify(|status| {
        status.epoch = epoch;
        status.state = state;
        if state == SupportState::Starting {
            status.downloaded = 0;
        }
        status.reason = reason;
        status.attempts = attempts;
        status.retry_ms = retry_ms;
        status.transition_pending = false;
    });
}
fn reason(kind: StartFailureKind) -> &'static str {
    use ace_swarm::resolve::ResolveFailureKind as R;
    match kind {
        StartFailureKind::Capacity => "capacity",
        StartFailureKind::Cooldown => "cooldown",
        StartFailureKind::Discovery => "discovery",
        StartFailureKind::Cancelled | StartFailureKind::Resolution(R::Cancelled) => "cancelled",
        StartFailureKind::CleanupUnconfirmed => "cleanup_unconfirmed",
        StartFailureKind::Resolution(R::InvalidSelector) => "invalid_selector",
        StartFailureKind::Resolution(R::InvalidDescriptor) => "invalid_descriptor",
        StartFailureKind::Resolution(R::SafetyRefusal) => "safety_refusal",
        StartFailureKind::Resolution(R::ProtectedContent) => "protected_content",
        StartFailureKind::Resolution(R::InvalidProtection) => "invalid_protection",
        StartFailureKind::Resolution(R::Network) => "network",
        StartFailureKind::Resolution(R::RejectedMetadata) => "metadata_rejected",
        StartFailureKind::Unclassified | StartFailureKind::Resolution(R::Unclassified) => "unknown",
    }
}
async fn cancelled(cancel: &mut watch::Receiver<bool>) {
    loop {
        if *cancel.borrow_and_update() {
            return;
        }
        if cancel.changed().await.is_err() {
            return;
        }
    }
}
fn fault(entry: &Entry, failed: &AtomicBool) {
    entry.cleanup_failed.store(true, Ordering::Release);
    failed.store(true, Ordering::Release);
}
async fn finish_worker(manager: &StreamManager, entry: &Entry, attempts: u64) {
    let _ = manager.set_support_running(&entry.control, false).await;
    publish(
        entry,
        entry.control.intent().epoch,
        if entry.cleanup_failed.load(Ordering::Acquire) {
            SupportState::Error
        } else {
            SupportState::Stopped
        },
        entry
            .cleanup_failed
            .load(Ordering::Acquire)
            .then_some("cleanup_unconfirmed"),
        attempts,
        0,
    );
}
async fn run(
    manager: Arc<StreamManager>,
    entry: Arc<Entry>,
    failed: Arc<AtomicBool>,
    mut cancel: watch::Receiver<bool>,
) {
    let mut desired = entry.control.subscribe();
    let mut attempts = 0u64;
    let mut delay = 1u64;
    let mut previous_epoch = 0;
    loop {
        if *cancel.borrow() {
            finish_worker(&manager, &entry, attempts).await;
            return;
        }
        let intent = *desired.borrow_and_update();
        if intent.epoch != previous_epoch {
            delay = 1;
            previous_epoch = intent.epoch;
        }
        if !intent.running || entry.cleanup_failed.load(Ordering::Acquire) {
            publish(
                &entry,
                intent.epoch,
                if entry.cleanup_failed.load(Ordering::Acquire) {
                    SupportState::Error
                } else {
                    SupportState::Stopped
                },
                entry
                    .cleanup_failed
                    .load(Ordering::Acquire)
                    .then_some("cleanup_unconfirmed"),
                attempts,
                0,
            );
            tokio::select! { biased;
                _ = cancelled(&mut cancel) => { finish_worker(&manager, &entry, attempts).await; return; }
                result = desired.changed() => { if result.is_err() { return; } }
            }
            continue;
        }
        attempts = attempts.saturating_add(1);
        publish(
            &entry,
            intent.epoch,
            SupportState::Starting,
            None,
            attempts,
            0,
        );
        let opening = manager.acquire_declared_support(&entry.control, intent.epoch);
        tokio::pin!(opening);
        let opened = tokio::select! { biased;
            _ = cancelled(&mut cancel) => {
                let _ = manager.set_support_running(&entry.control, false).await;
                match opening.await {
                    Ok(pin) => { if manager.release_support(&entry.control.key.0, &entry.control.key.1, pin).await.is_err() { fault(&entry, &failed); } }
                    Err(error) => { if error.kind == StartFailureKind::CleanupUnconfirmed { fault(&entry, &failed); } }
                }
                finish_worker(&manager, &entry, attempts).await; return;
            }
            result = &mut opening => result,
        };
        let retry_reason = match opened {
            Err(error) => {
                if error.kind == StartFailureKind::CleanupUnconfirmed {
                    fault(&entry, &failed);
                }
                if entry.control.intent().epoch != intent.epoch {
                    continue;
                }
                if !error.kind.retryable() {
                    publish(
                        &entry,
                        intent.epoch,
                        SupportState::Error,
                        Some(reason(error.kind)),
                        attempts,
                        0,
                    );
                    tokio::select! { biased;
                        _ = cancelled(&mut cancel) => { finish_worker(&manager, &entry, attempts).await; return; }
                        result = desired.changed() => { if result.is_err() { return; } }
                    }
                    continue;
                }
                reason(error.kind)
            }
            Ok(pin) => {
                let session = pin.session().clone();
                let mut completion = session.completion_receiver();
                publish(
                    &entry,
                    intent.epoch,
                    SupportState::Running,
                    None,
                    attempts,
                    0,
                );
                let mut sample_at = tokio::time::Instant::now() + Duration::from_secs(1);
                let mut downloaded = 0;
                let mut healthy_since = None;
                loop {
                    if *completion.borrow()
                        || completion.has_changed().is_err()
                        || entry.control.intent().epoch != intent.epoch
                        || *cancel.borrow()
                    {
                        break;
                    }
                    tokio::select! { biased;
                        _ = cancelled(&mut cancel) => break,
                        _ = desired.changed() => break,
                        _ = completion.changed() => break,
                        _ = tokio::time::sleep_until(sample_at) => {
                            let stats = tokio::select! { biased;
                                _ = cancelled(&mut cancel) => break,
                                _ = desired.changed() => break,
                                stats = session.stats() => stats,
                            };
                            let now = tokio::time::Instant::now();
                            if stats.downloaded > downloaded {
                                let since = healthy_since.get_or_insert(now);
                                if now.duration_since(*since) >= Duration::from_secs(30) { delay = 1; }
                            } else { healthy_since = None; }
                            downloaded = stats.downloaded;
                            entry.status.send_modify(|status| status.downloaded = downloaded);
                            sample_at = now + Duration::from_secs(1);
                        }
                    }
                }
                let unconfirmed = !*completion.borrow() && completion.has_changed().is_err();
                if manager
                    .release_support(&entry.control.key.0, &entry.control.key.1, pin)
                    .await
                    .is_err()
                    || unconfirmed
                {
                    fault(&entry, &failed);
                }
                if *cancel.borrow() {
                    finish_worker(&manager, &entry, attempts).await;
                    return;
                }
                if entry.control.intent().epoch != intent.epoch {
                    continue;
                }
                if entry.cleanup_failed.load(Ordering::Acquire) {
                    continue;
                }
                "source_ended"
            }
        };
        let sleep = tokio::time::sleep(Duration::from_secs(delay));
        tokio::pin!(sleep);
        publish(
            &entry,
            intent.epoch,
            SupportState::Retrying,
            Some(retry_reason),
            attempts,
            delay * 1000,
        );
        delay = (delay * 2).min(30);
        tokio::select! { biased;
            _ = cancelled(&mut cancel) => { finish_worker(&manager, &entry, attempts).await; return; }
            _ = desired.changed() => {},
            _ = &mut sleep => {},
        }
    }
}

#[cfg(test)]
#[path = "support_tests.rs"]
mod tests;
