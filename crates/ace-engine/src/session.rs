//! One shared download per stream, fanned out to many subscribers via a broadcast channel.
//! Exactly one [`TsSource`] is pulled regardless of how many clients are attached.

use crate::provider::{SourceStats, TsSource};
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamEvent {
    Data(Bytes),
    Discontinuity,
}

/// A live session: pulls from one `TsSource` and broadcasts TS chunks to subscribers.
pub struct StreamSession {
    tx: broadcast::Sender<StreamEvent>,
    subscribers: Arc<AtomicU64>,
    stats: Arc<Mutex<SourceStats>>,
    /// The background pull task; aborted when the session is dropped (last `Arc` gone, e.g.
    /// the manager's idle reaper or an explicit force-stop) so a live download doesn't keep
    /// running headless.
    pump: tokio::task::JoinHandle<()>,
}

impl Drop for StreamSession {
    fn drop(&mut self) {
        self.pump.abort();
    }
}

/// A subscription that decrements the live subscriber count on drop.
pub struct Subscription {
    pub rx: broadcast::Receiver<StreamEvent>,
    count: Arc<AtomicU64>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::SeqCst);
    }
}

impl StreamSession {
    /// Start pulling from `source` in a background task, broadcasting chunks. The pump stops
    /// when the source ends. `buffer` is the broadcast backlog (how far a slow/late receiver
    /// may lag before it drops old chunks).
    pub fn start(mut source: Box<dyn TsSource>, buffer: usize) -> Arc<StreamSession> {
        let (tx, _rx) = broadcast::channel(buffer);
        let stats = Arc::new(Mutex::new(SourceStats::default()));
        let pump_tx = tx.clone();
        let pump_stats = stats.clone();
        let pump = tokio::spawn(async move {
            while let Some(chunk) = source.next().await {
                *pump_stats.lock().await = source.stats();
                if source.take_discontinuity() {
                    let _ = pump_tx.send(StreamEvent::Discontinuity);
                }
                // Err just means no live receivers right now; keep pulling (live).
                let _ = pump_tx.send(StreamEvent::Data(chunk));
            }
        });
        Arc::new(StreamSession {
            tx,
            subscribers: Arc::new(AtomicU64::new(0)),
            stats,
            pump,
        })
    }

    pub fn subscribe(&self) -> Subscription {
        self.subscribers.fetch_add(1, Ordering::SeqCst);
        Subscription {
            rx: self.tx.subscribe(),
            count: self.subscribers.clone(),
        }
    }

    pub fn subscriber_count(&self) -> u64 {
        self.subscribers.load(Ordering::SeqCst)
    }

    /// A receiver that does NOT count as a client (used by internal consumers like the HLS
    /// packager, which must not pin the session open against idle teardown).
    pub fn raw_receiver(&self) -> broadcast::Receiver<StreamEvent> {
        self.tx.subscribe()
    }

    pub async fn stats(&self) -> SourceStats {
        self.stats.lock().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::StreamProvider;
    use crate::testprovider::TestProvider;

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
