//! One shared download per stream, fanned out to many subscribers via a broadcast channel.
//! Exactly one [`TsSource`] is pulled regardless of how many clients are attached.

use crate::provider::{SourceStats, StatsHandle, TsSource};
use ace_swarm::types::StreamMetadata;
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, watch, Mutex};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamEvent {
    Data(Bytes),
    Discontinuity,
}

/// A live session: pulls from one `TsSource` and broadcasts TS chunks to subscribers.
pub struct StreamSession {
    tx: broadcast::WeakSender<StreamEvent>,
    subscribers: Arc<AtomicU64>,
    support_pins: AtomicU64,
    stats: Arc<Mutex<SourceStats>>,
    live_stats: Option<StatsHandle>,
    metadata: StreamMetadata,
    control: std::sync::Mutex<Option<crate::manager::WorkerControl>>,
    cancel: watch::Sender<bool>,
    completed: watch::Receiver<bool>,
    // Manager-created sessions share its cleanup owner. Standalone sessions retain
    // a private owner, destroyed only after cancellation has transferred their join.
    standalone_cleanup: Option<crate::manager::CleanupTracker>,
}

impl Drop for StreamSession {
    fn drop(&mut self) {
        self.cancel();
    }
}

/// A subscription that decrements the live subscriber count on drop.
pub struct Subscription {
    pub rx: broadcast::Receiver<StreamEvent>,
    count: Arc<AtomicU64>,
}

/// Logical support ownership, separate from playback subscriptions.
pub struct SupportPin {
    session: Arc<StreamSession>,
}

impl Drop for SupportPin {
    fn drop(&mut self) {
        self.session.support_pins.fetch_sub(1, Ordering::SeqCst);
    }
}

impl SupportPin {
    pub fn session(&self) -> &Arc<StreamSession> {
        &self.session
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::SeqCst);
    }
}

impl StreamSession {
    pub(crate) fn support_pin(self: &Arc<Self>) -> SupportPin {
        self.support_pins.fetch_add(1, Ordering::SeqCst);
        SupportPin {
            session: self.clone(),
        }
    }

    pub(crate) fn support_pin_count(&self) -> u64 {
        self.support_pins.load(Ordering::SeqCst)
    }

    /// Start pulling from `source` in a background task, broadcasting chunks. The pump stops
    /// when the source ends. `buffer` is the broadcast backlog (how far a slow/late receiver
    /// may lag before it drops old chunks).
    pub fn start(source: Box<dyn TsSource>, buffer: usize) -> Arc<StreamSession> {
        let cleanup = crate::manager::CleanupTracker::default();
        let mut session = Self::start_with_cleanup(source, buffer, &cleanup);
        Arc::get_mut(&mut session).unwrap().standalone_cleanup = Some(cleanup);
        session
    }

    pub(crate) fn start_with_cleanup(
        mut source: Box<dyn TsSource>,
        buffer: usize,
        cleanup: &crate::manager::CleanupTracker,
    ) -> Arc<Self> {
        let (tx, _rx) = broadcast::channel(buffer);
        let stats = Arc::new(Mutex::new(SourceStats::default()));
        let metadata = source.metadata();
        let live_stats = source.stats_handle();
        // Media handles must not keep a completed source's output channel alive.
        // The pump is the sole strong sender; late subscriptions observe EOF.
        let weak_tx = tx.downgrade();
        let pump_tx = tx;
        let pump_stats = stats.clone();
        let failed_sources = cleanup.failed_sources();
        let mut control = cleanup.spawn(move |mut cancel| async move {
            use futures::FutureExt;
            let pull_result = std::panic::AssertUnwindSafe(async {
                loop {
                    let chunk = tokio::select! {
                        biased;
                        _ = async {
                            if *cancel.borrow_and_update() { return; }
                            let _ = cancel.changed().await;
                        } => break,
                        chunk = source.next() => chunk,
                    };
                    let Some(chunk) = chunk else { break };
                    if *cancel.borrow() {
                        break;
                    }
                    *pump_stats.lock().await = source.stats();
                    if *cancel.borrow() {
                        break;
                    }
                    if source.take_discontinuity() {
                        let _ = pump_tx.send(StreamEvent::Discontinuity);
                    }
                    // Err just means no live receivers right now; keep pulling (live).
                    let _ = pump_tx.send(StreamEvent::Data(chunk));
                }
            })
            .catch_unwind()
            .await;
            // EOF is independent of potentially slow source cleanup. Published
            // broadcast backlog remains drainable by existing receivers.
            drop(pump_tx);
            if let Err(panic) = crate::manager::shutdown_source(source, &failed_sources).await {
                std::panic::resume_unwind(panic);
            }
            if pull_result.is_err() {
                crate::alog!("[session] source pull failed; owned cleanup completed");
            }
        });
        let cancel = control.cancellation_signal();
        let completed = control.completion_receiver();
        // Natural EOF must publish confirmed cleanup without waiting for media
        // owners to explicitly stop or drop the session.
        control.transfer();
        Arc::new(StreamSession {
            tx: weak_tx,
            subscribers: Arc::new(AtomicU64::new(0)),
            support_pins: AtomicU64::new(0),
            stats,
            live_stats,
            metadata,
            control: std::sync::Mutex::new(Some(control)),
            cancel,
            completed,
            standalone_cleanup: None,
        })
    }

    /// Fence new pump work and transfer its join synchronously, even if a later
    /// asynchronous stop waiter is canceled or media handles remain alive.
    pub(crate) fn cancel(&self) {
        self.cancel.send_replace(true);
        let control = self.control.lock().unwrap().take();
        drop(control);
    }

    pub(crate) fn completion_receiver(&self) -> watch::Receiver<bool> {
        self.completed.clone()
    }

    pub(crate) fn cancellation_requested(&self) -> bool {
        *self.cancel.borrow()
    }

    /// True only when owned source cleanup was confirmed.
    pub async fn stop(&self) -> bool {
        self.cancel();
        let mut completed = self.completed.clone();
        let mut success = crate::manager::wait_cleanup(&mut completed).await;
        if let Some(cleanup) = &self.standalone_cleanup {
            success &= cleanup.drain().await;
        }
        success
    }

    pub fn subscribe(&self) -> Subscription {
        self.subscribers.fetch_add(1, Ordering::SeqCst);
        Subscription {
            rx: self.raw_receiver(),
            count: self.subscribers.clone(),
        }
    }

    pub fn subscriber_count(&self) -> u64 {
        self.subscribers.load(Ordering::SeqCst)
    }

    /// A receiver that does NOT count as a client (used by internal consumers like the HLS
    /// packager, which must not pin the session open against idle teardown).
    pub fn raw_receiver(&self) -> broadcast::Receiver<StreamEvent> {
        if let Some(tx) = self.tx.upgrade() {
            tx.subscribe()
        } else {
            let (tx, rx) = broadcast::channel(1);
            drop(tx);
            rx
        }
    }

    pub async fn stats(&self) -> SourceStats {
        if let Some(reader) = &self.live_stats {
            return reader.snapshot();
        }
        self.stats.lock().await.clone()
    }

    pub fn metadata(&self) -> &StreamMetadata {
        &self.metadata
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::StreamProvider;
    use crate::testprovider::TestProvider;
    use ace_swarm::types::StreamMetadata;

    #[tokio::test]
    async fn cancellation_while_stats_lock_is_pending_prevents_publication() {
        struct ReadySource(Arc<tokio::sync::Notify>);
        #[async_trait::async_trait]
        impl TsSource for ReadySource {
            async fn next(&mut self) -> Option<Bytes> {
                self.0.notify_one();
                Some(Bytes::from_static(b"ready"))
            }
            fn stats(&self) -> SourceStats {
                SourceStats::default()
            }
        }
        let ready = Arc::new(tokio::sync::Notify::new());
        let session = StreamSession::start(Box::new(ReadySource(ready.clone())), 4);
        // Current-thread execution takes this uncontended lock before the pump can run.
        let held_stats = session.stats.lock().await;
        let mut receiver = session.raw_receiver();
        ready.notified().await;
        session.cancel();
        drop(held_stats);
        session.stop().await;
        assert!(
            matches!(
                receiver.try_recv(),
                Err(broadcast::error::TryRecvError::Closed)
            ),
            "chunk ready before cancellation was published after the stats await"
        );
    }

    #[tokio::test]
    async fn session_captures_source_metadata_before_pumping() {
        struct MetadataSource;
        #[async_trait::async_trait]
        impl TsSource for MetadataSource {
            async fn next(&mut self) -> Option<Bytes> {
                std::future::pending().await
            }

            fn stats(&self) -> SourceStats {
                SourceStats::default()
            }

            fn metadata(&self) -> StreamMetadata {
                StreamMetadata {
                    title: Some("Synthetic Demo Channel".to_string()),
                    bitrate: Some(100_000),
                    categories: vec!["sports".to_string()],
                }
            }
        }

        let session = StreamSession::start(Box::new(MetadataSource), 8);

        assert_eq!(
            session.metadata(),
            &StreamMetadata {
                title: Some("Synthetic Demo Channel".to_string()),
                bitrate: Some(100_000),
                categories: vec!["sports".to_string()],
            }
        );
    }

    #[tokio::test]
    async fn two_subscribers_share_one_source() {
        let source = TestProvider { chunks: 5 }.open("x").await.unwrap();
        let session = StreamSession::start(source, 16);
        // Subscribe both before any await, so the (current-thread) pump can't run first.
        let mut a = session.subscribe();
        let mut b = session.subscribe();
        assert_eq!(session.subscriber_count(), 2);
        let ca = a.rx.recv().await.unwrap();
        let cb = b.rx.recv().await.unwrap();
        assert_eq!(ca, cb);
        let StreamEvent::Data(ca) = ca else {
            panic!("expected data")
        };
        assert_eq!(ca[0], 0x47);
        drop(a);
        assert_eq!(session.subscriber_count(), 1);
    }

    #[tokio::test]
    async fn dropping_session_aborts_pump_and_drops_source() {
        use async_trait::async_trait;
        use std::sync::atomic::AtomicBool;
        use std::time::Duration;

        // A source that never yields (like a live swarm) but flags when it is dropped.
        struct BlockingSource(Arc<AtomicBool>);
        impl Drop for BlockingSource {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        #[async_trait]
        impl TsSource for BlockingSource {
            async fn next(&mut self) -> Option<Bytes> {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                None
            }
            fn stats(&self) -> SourceStats {
                SourceStats::default()
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let session = StreamSession::start(Box::new(BlockingSource(dropped.clone())), 16);
        assert!(!dropped.load(Ordering::SeqCst));
        drop(session); // last Arc gone → pump aborted → source dropped

        // The abort + drop run on the next runtime poll; give it a moment.
        for _ in 0..50 {
            if dropped.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            dropped.load(Ordering::SeqCst),
            "pump must be aborted and source dropped"
        );
    }

    #[tokio::test]
    async fn provider_discontinuity_precedes_first_post_gap_chunk() {
        use async_trait::async_trait;

        struct GapSource {
            step: u8,
            gap: bool,
        }
        #[async_trait]
        impl TsSource for GapSource {
            async fn next(&mut self) -> Option<Bytes> {
                self.step += 1;
                match self.step {
                    1 => Some(Bytes::from_static(b"before")),
                    2 => {
                        self.gap = true;
                        Some(Bytes::from_static(b"after"))
                    }
                    _ => None,
                }
            }
            fn take_discontinuity(&mut self) -> bool {
                std::mem::take(&mut self.gap)
            }
            fn stats(&self) -> SourceStats {
                SourceStats::default()
            }
        }

        let session = StreamSession::start(
            Box::new(GapSource {
                step: 0,
                gap: false,
            }),
            8,
        );
        let mut sub = session.subscribe();
        assert_eq!(
            sub.rx.recv().await.unwrap(),
            StreamEvent::Data(Bytes::from_static(b"before"))
        );
        assert_eq!(sub.rx.recv().await.unwrap(), StreamEvent::Discontinuity);
        assert_eq!(
            sub.rx.recv().await.unwrap(),
            StreamEvent::Data(Bytes::from_static(b"after"))
        );
    }
}
