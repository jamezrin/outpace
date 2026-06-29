//! One shared download per stream, fanned out to many subscribers via a broadcast channel.
//! Exactly one [`TsSource`] is pulled regardless of how many clients are attached.

use crate::provider::{SourceStats, TsSource};
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};

/// A live session: pulls from one `TsSource` and broadcasts TS chunks to subscribers.
pub struct StreamSession {
    tx: broadcast::Sender<Bytes>,
    subscribers: Arc<AtomicU64>,
    stats: Arc<Mutex<SourceStats>>,
}

/// A subscription that decrements the live subscriber count on drop.
pub struct Subscription {
    pub rx: broadcast::Receiver<Bytes>,
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
        let session = Arc::new(StreamSession {
            tx: tx.clone(),
            subscribers: Arc::new(AtomicU64::new(0)),
            stats: stats.clone(),
        });
        tokio::spawn(async move {
            while let Some(chunk) = source.next().await {
                *stats.lock().await = source.stats();
                // Err just means no live receivers right now; keep pulling (live).
                let _ = tx.send(chunk);
            }
        });
        session
    }

    pub fn subscribe(&self) -> Subscription {
        self.subscribers.fetch_add(1, Ordering::SeqCst);
        Subscription { rx: self.tx.subscribe(), count: self.subscribers.clone() }
    }

    pub fn subscriber_count(&self) -> u64 {
        self.subscribers.load(Ordering::SeqCst)
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
        assert_eq!(ca[0], 0x47);
        drop(a);
        assert_eq!(session.subscriber_count(), 1);
    }
}
