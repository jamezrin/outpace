//! Provider abstraction: the single seam between the generic engine and a network's
//! protocol. The `{network}` segment in the URL selects a `StreamProvider` via
//! `ProviderRegistry`. The generic engine never names a network.

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;

/// Stats snapshot for `/status` (clean field names only).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourceStats {
    pub peers: u32,
    pub bitrate: u64,   // bits/sec
    pub buffer_ms: u64, // buffered duration estimate
    /// Bytes downloaded from the source and emitted as MPEG-TS.
    pub downloaded: u64,
    /// Bytes we have uploaded (served to peers) on this source.
    pub uploaded: u64,
    /// Distinct peers we have served at least one chunk to.
    pub peers_served: u32,
}

/// A live MPEG-TS byte source for one stream.
#[async_trait]
pub trait TsSource: Send {
    /// Next contiguous MPEG-TS chunk, or None at end-of-stream.
    async fn next(&mut self) -> Option<Bytes>;
    fn stats(&self) -> SourceStats;
}

/// A finite, ordered VOD byte stream with a known total length. Unlike [`TsSource`] (an
/// unbounded live stream), a VOD source ends after exactly `content_length()` bytes.
#[async_trait]
pub trait VodByteSource: Send {
    /// Total content length in bytes (for a `Content-Length` header).
    fn content_length(&self) -> u64;
    /// Next verified, ordered chunk, or None at end-of-content.
    async fn next(&mut self) -> Option<Bytes>;
}

/// Adapter for one network (e.g. "ace").
#[async_trait]
pub trait StreamProvider: Send + Sync {
    fn network(&self) -> &'static str;
    /// Open a live TS source for `id` (the provider resolves/discovers internally).
    async fn open(&self, id: &str) -> Result<Box<dyn TsSource>, ProviderError>;
}

#[derive(Debug)]
pub enum ProviderError {
    NotFound,
    Backend(String),
}

/// Maps network name → provider.
#[derive(Default, Clone)]
pub struct ProviderRegistry {
    providers: HashMap<&'static str, Arc<dyn StreamProvider>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn register(&mut self, p: Arc<dyn StreamProvider>) {
        self.providers.insert(p.network(), p);
    }
    pub fn get(&self, network: &str) -> Option<Arc<dyn StreamProvider>> {
        self.providers.get(network).cloned()
    }
    pub fn networks(&self) -> Vec<&'static str> {
        let mut v: Vec<_> = self.providers.keys().copied().collect();
        v.sort();
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyProvider;
    #[async_trait]
    impl StreamProvider for DummyProvider {
        fn network(&self) -> &'static str {
            "dummy"
        }
        async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
            Err(ProviderError::NotFound)
        }
    }

    #[test]
    fn registry_registers_and_looks_up() {
        let mut r = ProviderRegistry::new();
        r.register(Arc::new(DummyProvider));
        assert!(r.get("dummy").is_some());
        assert!(r.get("nope").is_none());
        assert_eq!(r.networks(), vec!["dummy"]);
    }

    #[test]
    fn source_stats_has_upload_counters() {
        let s = SourceStats {
            peers: 1,
            bitrate: 0,
            buffer_ms: 0,
            downloaded: 8192,
            uploaded: 4096,
            peers_served: 2,
        };
        assert_eq!(s.downloaded, 8192);
        assert_eq!(s.uploaded, 4096);
        assert_eq!(s.peers_served, 2);
    }
}
