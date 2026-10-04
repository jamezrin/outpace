//! Provider abstraction: the single seam between the generic engine and a network's
//! protocol. The `{network}` segment in the URL selects a `StreamProvider` via
//! `ProviderRegistry`. The generic engine never names a network.

use ace_swarm::types::{StreamInfo, StreamMetadata};
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

/// A live MPEG-TS byte source for one stream. Source decorators may buffer chunks, but must
/// preserve metadata, statistics, and the discontinuity associated with each returned chunk.
#[async_trait]
pub trait TsSource: Send {
    /// Next contiguous MPEG-TS chunk, or None at end-of-stream.
    async fn next(&mut self) -> Option<Bytes>;
    /// Report a known gap immediately before the most recently returned chunk. Sources that
    /// recover by skipping unavailable media override this; ordinary sources stay contiguous.
    fn take_discontinuity(&mut self) -> bool {
        false
    }
    fn stats(&self) -> SourceStats;
    /// Descriptor metadata resolved alongside this exact source, if any.
    fn metadata(&self) -> StreamMetadata {
        StreamMetadata::default()
    }
}

/// A finite, ordered VOD byte stream with a known total length. Unlike [`TsSource`] (an
/// unbounded live stream), a VOD source ends after exactly `content_length()` bytes.
///
/// A source may cover the whole file or an arbitrary byte range (see [`VodContent::open_range`]);
/// in both cases `content_length()` is the number of bytes this source will emit.
#[async_trait]
pub trait VodByteSource: Send {
    /// Number of bytes this source will emit (the whole file, or a requested range's length).
    fn content_length(&self) -> u64;
    /// Next verified, ordered chunk, or None at end-of-content.
    async fn next(&mut self) -> Option<Bytes>;
}

/// A resolved single-file VOD: its total length is known, and verified byte ranges can be
/// opened on demand. Splitting resolution from range-opening lets the HTTP layer read the total
/// length once (to validate a `Range` header) before deciding which pieces to download, so seek
/// requests fetch only the covering pieces rather than the whole file.
#[async_trait]
pub trait VodContent: Send + Sync {
    /// Total content length of the whole file in bytes.
    fn content_length(&self) -> u64;
    /// Open a verified byte source for the inclusive byte range `[start, end]`. The returned
    /// source emits exactly those bytes, each SHA-1-verified (per covering piece) before
    /// emission. `start`/`end` must satisfy `start <= end < content_length()`.
    async fn open_range(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Box<dyn VodByteSource>, ProviderError>;
}

/// Adapter for one network (e.g. "ace").
#[async_trait]
pub trait StreamProvider: Send + Sync {
    fn network(&self) -> &'static str;
    /// Open a live TS source for `id` (the provider resolves/discovers internally).
    async fn open(&self, id: &str) -> Result<Box<dyn TsSource>, ProviderError>;
    /// Resolve `id` to a single-file VOD, returning its geometry (total length) and a handle for
    /// opening verified byte ranges. Defaults to unsupported; networks with a VOD path override
    /// this. Kept separate from [`open`](Self::open) because VOD is finite, length-known content
    /// (seekable), not an unbounded live stream.
    async fn resolve_vod(&self, _id: &str) -> Result<Box<dyn VodContent>, ProviderError> {
        Err(ProviderError::Backend(
            "this network does not support VOD".into(),
        ))
    }

    /// Offline check that [`open`](Self::open) would get past id resolution, for routes that
    /// hand out playback URLs before opening (compat `/ace/getstream?format=json`). Must not
    /// touch the network or start anything. Defaults to accepting every id.
    fn check_openable(&self, _id: &str) -> Result<(), ProviderError> {
        Ok(())
    }

    /// Record a live descriptor that was resolved outside [`open`](Self::open) (the compat
    /// routes resolve content ids for their JSON responses), so a later open by its infohash can
    /// use it (#164). Defaults to ignoring it.
    fn remember_live_descriptor(&self, _info: &StreamInfo) {}
}

#[derive(Debug)]
pub enum ProviderError {
    NotFound,
    Backend(String),
    /// The id is well-formed, but the provider holds no verified metadata to open it with and
    /// will not guess (#164: a bare infohash without a verified transport descriptor). The
    /// message is user-facing: it explains the refusal and how to open the stream instead, and
    /// is safe to return to HTTP clients and to print in the CLI.
    Unresolvable(String),
}

impl std::fmt::Display for ProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProviderError::NotFound => f.write_str("not found"),
            ProviderError::Backend(msg) | ProviderError::Unresolvable(msg) => f.write_str(msg),
        }
    }
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
    fn provider_error_display_is_the_user_facing_message() {
        assert_eq!(
            ProviderError::Unresolvable("use cid:<id>".into()).to_string(),
            "use cid:<id>"
        );
        assert_eq!(
            ProviderError::Backend("no peers".into()).to_string(),
            "no peers"
        );
        assert_eq!(ProviderError::NotFound.to_string(), "not found");
    }

    #[test]
    fn default_check_openable_accepts_any_id() {
        assert!(DummyProvider.check_openable("anything").is_ok());
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
