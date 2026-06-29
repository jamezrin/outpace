//! Registry of live sessions keyed by `(network, id)`. One shared session per key; lazy
//! start via the provider; teardown after the last subscriber leaves + grace.

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
    buffer: usize,
    grace: Duration,
}

impl StreamManager {
    pub fn new(registry: ProviderRegistry) -> Arc<StreamManager> {
        Arc::new(StreamManager {
            registry,
            sessions: Mutex::new(HashMap::new()),
            packagers: Mutex::new(HashMap::new()),
            buffer: 256,
            grace: Duration::from_secs(30),
        })
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
        let provider = self.registry.get(network).ok_or(ProviderError::NotFound)?;
        let source = provider.open(id).await?;
        let session = StreamSession::start(source, self.buffer);
        let mut map = self.sessions.lock().await;
        // Double-check: another task may have started it concurrently.
        Ok(map.entry(key).or_insert(session).clone())
    }

    /// Peek at an already-running session without starting one.
    pub async fn get(&self, network: &str, id: &str) -> Option<Arc<StreamSession>> {
        self.sessions.lock().await.get(&(network.to_string(), id.to_string())).cloned()
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
        Ok(map.entry(key).or_insert_with(|| HlsPackager::start(&session)).clone())
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
    async fn unknown_network_is_not_found() {
        let m = StreamManager::new(registry());
        assert!(matches!(m.get_or_start("nope", "x").await, Err(ProviderError::NotFound)));
    }

    #[tokio::test]
    async fn list_reports_active_sessions() {
        let m = StreamManager::new(registry());
        m.get_or_start("test", "a").await.unwrap();
        let list = m.list().await;
        assert!(list.iter().any(|(n, i, _)| n == "test" && i == "a"));
    }
}
