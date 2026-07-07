//! Registry of live sessions keyed by `(network, id)`. One shared session per key; lazy
//! start via the provider; teardown after the last subscriber leaves + grace.

use crate::config::HlsConfig;
use crate::hls::HlsPackager;
use crate::provider::{ProviderError, ProviderRegistry};
use crate::session::StreamSession;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct StreamManager {
    registry: ProviderRegistry,
    sessions: Mutex<HashMap<(String, String), Arc<StreamSession>>>,
    packagers: Mutex<HashMap<(String, String), Arc<HlsPackager>>>,
    /// Serializes session *creation* so concurrent first requests for the same id (e.g. the
    /// two connections VLC opens) trigger exactly ONE `provider.open()` — not duplicate
    /// discovery + duplicate peer connections from our same node_id (which real peers drop).
    /// Held only around start, never around the fast existing-session path.
    start_lock: Mutex<()>,
    buffer: usize,
    hls: HlsConfig,
    grace: Duration,
}

impl StreamManager {
    pub fn new(registry: ProviderRegistry) -> Arc<StreamManager> {
        Self::with_buffer(registry, 256)
    }

    pub fn with_buffer(registry: ProviderRegistry, buffer: usize) -> Arc<StreamManager> {
        Self::with_config(registry, buffer, HlsConfig::default())
    }

    pub fn with_config(
        registry: ProviderRegistry,
        buffer: usize,
        hls: HlsConfig,
    ) -> Arc<StreamManager> {
        Arc::new(StreamManager {
            registry,
            sessions: Mutex::new(HashMap::new()),
            packagers: Mutex::new(HashMap::new()),
            start_lock: Mutex::new(()),
            buffer,
            hls,
            grace: Duration::from_secs(30),
        })
    }

    #[cfg(test)]
    pub(crate) fn buffer(&self) -> usize {
        self.buffer
    }

    #[cfg(test)]
    pub(crate) fn hls_config(&self) -> HlsConfig {
        self.hls
    }

    /// Get the running session for `(network, id)` or start one via the provider. Returns
    /// `NotFound` if the network is unregistered.
    pub async fn get_or_start(
        self: &Arc<Self>,
        network: &str,
        id: &str,
    ) -> Result<Arc<StreamSession>, ProviderError> {
        let key = (network.to_string(), id.to_string());
        {
            let map = self.sessions.lock().await;
            if let Some(s) = map.get(&key) {
                return Ok(s.clone());
            }
        }
        // Serialize starts and re-check under the start lock: a concurrent request may have
        // started this exact session while we were waiting, so we must not open a second.
        let _starting = self.start_lock.lock().await;
        {
            let map = self.sessions.lock().await;
            if let Some(s) = map.get(&key) {
                return Ok(s.clone());
            }
        }
        let provider = self.registry.get(network).ok_or(ProviderError::NotFound)?;
        let source = provider.open(id).await?;
        let session = StreamSession::start(source, self.buffer);
        let mut map = self.sessions.lock().await;
        Ok(map.entry(key).or_insert(session).clone())
    }

    /// Peek at an already-running session without starting one.
    pub async fn get(&self, network: &str, id: &str) -> Option<Arc<StreamSession>> {
        self.sessions
            .lock()
            .await
            .get(&(network.to_string(), id.to_string()))
            .cloned()
    }

    /// Get (or lazily start) the HLS packager for `(network, id)`, starting the session too.
    pub async fn get_or_start_hls(
        self: &Arc<Self>,
        network: &str,
        id: &str,
    ) -> Result<Arc<HlsPackager>, ProviderError> {
        let session = self.get_or_start(network, id).await?;
        let key = (network.to_string(), id.to_string());
        let mut map = self.packagers.lock().await;
        Ok(map
            .entry(key)
            .or_insert_with(|| HlsPackager::start(&session, self.hls))
            .clone())
    }

    /// Peek at an already-running HLS packager without starting a session or packager.
    pub async fn get_hls(&self, network: &str, id: &str) -> Option<Arc<HlsPackager>> {
        self.packagers
            .lock()
            .await
            .get(&(network.to_string(), id.to_string()))
            .cloned()
    }

    /// Force-stop a session: remove it (and any HLS packager) so the shared download is torn
    /// down — the session's `Drop` aborts its background pull task. Returns `true` if a session
    /// for `(network, id)` existed. Connected clients see the stream end.
    pub async fn stop(&self, network: &str, id: &str) -> bool {
        let key = (network.to_string(), id.to_string());
        self.packagers.lock().await.remove(&key);
        self.sessions.lock().await.remove(&key).is_some()
    }

    /// Active sessions as `(network, id, subscriber_count)`.
    pub async fn list(&self) -> Vec<(String, String, u64)> {
        self.sessions
            .lock()
            .await
            .iter()
            .map(|((n, i), s)| (n.clone(), i.clone(), s.subscriber_count()))
            .collect()
    }

    /// Spawn the idle-teardown watcher: drops sessions with 0 subscribers after `grace`.
    pub fn spawn_reaper(self: &Arc<Self>) {
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(me.grace).await;
                let mut map = me.sessions.lock().await;
                map.retain(|_, s| s.subscriber_count() > 0);
                // Drop packagers whose session is gone (their background task ends on close).
                me.packagers.lock().await.retain(|k, _| map.contains_key(k));
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testprovider::TestProvider;

    fn registry() -> ProviderRegistry {
        let mut r = ProviderRegistry::new();
        r.register(Arc::new(TestProvider { chunks: 1000 }));
        r
    }

    #[test]
    fn with_buffer_sets_the_fanout_depth() {
        let reg = ProviderRegistry::new();
        let mgr = StreamManager::with_buffer(reg, 4);
        assert_eq!(mgr.buffer(), 4);
    }

    #[test]
    fn with_config_sets_buffer_and_hls_config() {
        let reg = ProviderRegistry::new();
        let hls = HlsConfig {
            segment_packets: 64,
            window_segments: 4,
            segment_duration_ms: 1500,
        };
        let mgr = StreamManager::with_config(reg, 8, hls);

        assert_eq!(mgr.buffer(), 8);
        assert_eq!(mgr.hls_config(), hls);
    }

    #[tokio::test]
    async fn same_key_returns_same_session() {
        let m = StreamManager::new(registry());
        let s1 = m.get_or_start("test", "abc").await.unwrap();
        let s2 = m.get_or_start("test", "abc").await.unwrap();
        assert!(Arc::ptr_eq(&s1, &s2));
        let s3 = m.get_or_start("test", "different").await.unwrap();
        assert!(!Arc::ptr_eq(&s1, &s3));
    }

    #[tokio::test]
    async fn concurrent_first_requests_start_the_session_once() {
        // A provider that counts how many times `open` is called.
        use crate::provider::{SourceStats, StreamProvider, TsSource};
        use async_trait::async_trait;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingProvider(Arc<AtomicUsize>);
        struct IdleSource;
        #[async_trait]
        impl TsSource for IdleSource {
            async fn next(&mut self) -> Option<bytes::Bytes> {
                // Never yields, never ends — like a live session being followed.
                std::future::pending().await
            }
            fn stats(&self) -> SourceStats {
                SourceStats::default()
            }
        }
        #[async_trait]
        impl StreamProvider for CountingProvider {
            fn network(&self) -> &'static str {
                "count"
            }
            async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                // A small yield so concurrent callers genuinely overlap inside `open`.
                tokio::task::yield_now().await;
                Ok(Box::new(IdleSource))
            }
        }

        let opens = Arc::new(AtomicUsize::new(0));
        let mut r = ProviderRegistry::new();
        r.register(Arc::new(CountingProvider(opens.clone())));
        let m = StreamManager::new(r);

        // Fire many concurrent first requests for the same id.
        let mut handles = Vec::new();
        for _ in 0..16 {
            let m = m.clone();
            handles.push(tokio::spawn(async move {
                m.get_or_start("count", "x")
                    .await
                    .map(|s| Arc::as_ptr(&s) as usize)
            }));
        }
        let mut ptrs = Vec::new();
        for h in handles {
            ptrs.push(h.await.unwrap().unwrap());
        }
        // Exactly one open, and everyone got the same session.
        assert_eq!(
            opens.load(Ordering::SeqCst),
            1,
            "session must be started exactly once"
        );
        assert!(
            ptrs.iter().all(|p| *p == ptrs[0]),
            "all callers share one session"
        );
    }

    #[tokio::test]
    async fn get_hls_only_returns_an_already_running_packager() {
        let m = StreamManager::new(registry());
        // Nothing running yet: peeking must not start a session or packager.
        assert!(m.get_hls("test", "a").await.is_none());
        assert!(
            m.get("test", "a").await.is_none(),
            "peeking must not have started a session"
        );
        // Once the packager exists (e.g. after a playlist fetch), peeking finds it.
        m.get_or_start_hls("test", "a").await.unwrap();
        assert!(m.get_hls("test", "a").await.is_some());
    }

    #[tokio::test]
    async fn unknown_network_is_not_found() {
        let m = StreamManager::new(registry());
        assert!(matches!(
            m.get_or_start("nope", "x").await,
            Err(ProviderError::NotFound)
        ));
    }

    #[tokio::test]
    async fn list_reports_active_sessions() {
        let m = StreamManager::new(registry());
        m.get_or_start("test", "a").await.unwrap();
        let list = m.list().await;
        assert!(list.iter().any(|(n, i, _)| n == "test" && i == "a"));
    }

    #[tokio::test]
    async fn stop_removes_session_and_is_idempotent() {
        let m = StreamManager::new(registry());
        m.get_or_start("test", "a").await.unwrap();
        assert!(
            m.stop("test", "a").await,
            "first stop removes the running session"
        );
        assert!(
            m.get("test", "a").await.is_none(),
            "session is gone afterwards"
        );
        assert!(
            !m.stop("test", "a").await,
            "stopping a missing session is a no-op"
        );
    }
}
