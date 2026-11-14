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
    pub bitrate: u64,       // bits/sec
    pub buffer_ms: u64,     // buffered duration estimate
    pub download_rate: u64, // measured reassembled payload bytes/sec
    pub upload_rate: u64,   // measured uploaded bytes/sec
    pub livepos: Option<LivePositionStats>,
    /// Source-session recovery observations; None for unsupported providers.
    pub recovery: Option<LiveRecoveryStats>,
    /// Bytes downloaded from the source and emitted as MPEG-TS.
    pub downloaded: u64,
    /// Bytes we have uploaded (served to peers) on this source.
    pub uploaded: u64,
    /// Distinct peers we have served at least one chunk to.
    pub peers_served: u32,
}

/// Historical recovery occurrences and the current admitted transport gauge.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct LiveRecoveryStats {
    pub upstream_reconnects: u64,
    pub stale_teardowns: u64,
    pub skipped_evicted_gaps: u64,
    pub signature_rejects: u64,
    pub active_upstreams: u32,
}

/// Provider piece observations, not a client's playhead or wall-clock timestamps.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LivePositionStats {
    pub pos: Option<u64>,
    pub last: Option<u64>,
    pub live_first: Option<u64>,
    pub live_last: Option<u64>,
    pub buffer_pieces: u64,
    pub buffer_complete: bool,
}

/// Independently readable; implementations must never lock the source across an await.
pub trait StatsReader: Send + Sync {
    fn snapshot(&self) -> SourceStats;
}
pub type StatsHandle = Arc<dyn StatsReader>;

/// A live MPEG-TS byte source for one stream. Source decorators may buffer chunks, but must
/// preserve metadata, statistics, and the discontinuity associated with each returned chunk.
#[async_trait]
pub trait TsSource: Send {
    /// Next contiguous MPEG-TS chunk, or None at end-of-stream.
    async fn next(&mut self) -> Option<Bytes>;
    /// Finish owned background work before this source is destroyed. Stateful sources
    /// and owning decorators override this; only stateless sources may use the no-op.
    /// Pending cleanup retains ownership rather than treating a timeout as completion.
    async fn shutdown(&mut self) {}
    /// Report a known gap immediately before the most recently returned chunk. Sources that
    /// recover by skipping unavailable media override this; ordinary sources stay contiguous.
    fn take_discontinuity(&mut self) -> bool {
        false
    }
    fn stats(&self) -> SourceStats;
    fn stats_handle(&self) -> Option<StatsHandle> {
        None
    }
    /// Optional normalization readiness and terminal state, independent of media EOF.
    /// Raw sources allocate no state channel.
    fn playback_state(&self) -> Option<crate::audio_normalize::PlaybackStateHandle> {
        None
    }
    /// Generation of the most recently returned chunk, not the latest asynchronous
    /// playback snapshot. Owning decorators preserve this ordered media metadata.
    fn chunk_generation(&self) -> Option<u64> {
        None
    }
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

/// Result of preparing a live content id without opening its media source.
#[derive(Debug)]
pub enum ContentIdPreparation {
    /// The provider leaves preparation to the caller's legacy catalog preflight.
    Unsupported,
    /// Metadata authenticated by the catalog, suitable for public infohash aliases.
    Catalog(StreamInfo),
    /// The provider resolved its own local or opted-in peer path, without catalog trust.
    ProviderResolved,
}

/// Adapter for one network (e.g. "ace").
#[async_trait]
pub trait StreamProvider: Send + Sync {
    fn network(&self) -> &'static str;
    /// Open a live TS source for `id` (the provider resolves/discovers internally).
    async fn open(&self, id: &str) -> Result<Box<dyn TsSource>, ProviderError>;

    /// Preserve a structured cause for supervised starts; legacy providers default to unknown.
    async fn open_classified(
        &self,
        id: &str,
    ) -> Result<Box<dyn TsSource>, crate::start_failure::ProviderStartFailure> {
        self.open(id)
            .await
            .map_err(crate::start_failure::ProviderStartFailure::from)
    }

    /// [`open_classified`](Self::open_classified) that also reports start progress (the
    /// descriptor is resolved) for the native status route. Providers without intermediate
    /// progress keep the default.
    async fn open_with_progress(
        &self,
        id: &str,
        progress: crate::start_status::StartProgress,
    ) -> Result<Box<dyn TsSource>, crate::start_failure::ProviderStartFailure> {
        let _ = progress;
        self.open_classified(id).await
    }

    /// Prepare a live content id using the same policy and cache as [`open`](Self::open).
    /// Only `Catalog` grants catalog provenance. A supported provider's error is terminal
    /// for this selection; `Unsupported` alone permits the caller's legacy preflight.
    async fn prepare_live_content_id(
        &self,
        _content_id: &str,
    ) -> Result<ContentIdPreparation, ProviderError> {
        Ok(ContentIdPreparation::Unsupported)
    }

    /// Resolve `id` to a single-file VOD, returning its geometry (total length) and a handle for
    /// opening verified byte ranges. Defaults to unsupported; networks with a VOD path override
    /// this. Kept separate from [`open`](Self::open) because VOD is finite, length-known content
    /// (seekable), not an unbounded live stream.
    async fn resolve_vod(&self, _id: &str) -> Result<Box<dyn VodContent>, ProviderError> {
        Err(ProviderError::Backend(
            "this network does not support VOD".into(),
        ))
    }

    /// The spelling of `id` that the manager keys sessions, VOD handles and stops on. Ids that
    /// differ only in a case-insensitive part must map to one canonical form, or two spellings of
    /// one stream would start two swarms and miss each other's caches. Defaults to `id` unchanged.
    fn canonical_id<'a>(&self, id: &'a str) -> std::borrow::Cow<'a, str> {
        std::borrow::Cow::Borrowed(id)
    }

    /// Offline check that [`open`](Self::open) would get past id resolution, for routes that
    /// hand out playback URLs before opening (compat `/ace/getstream?format=json`). Must not
    /// touch the network or start anything. Defaults to accepting every id.
    fn check_openable(&self, _id: &str) -> Result<(), ProviderError> {
        Ok(())
    }

    /// Record a live descriptor that was resolved outside [`open`](Self::open) (the compat
    /// routes resolve content ids for their JSON responses), so a later open by its infohash can
    /// use it (#164). Callers must pass only descriptors resolved through the signed catalog
    /// (never BEP-9 or a transport url): the index trusts them as-is. Defaults to ignoring it.
    fn remember_live_descriptor(&self, _info: &StreamInfo) {}

    /// Cache caller-supplied URL metadata without granting catalog provenance.
    fn remember_transport_url(&self, _id: &str, _info: &StreamInfo) {}
}

#[derive(Debug)]
pub enum ProviderError {
    ProtectedContent,
    InvalidProtection,
    CapacityExhausted,
    NotFound,
    Backend(String),
    /// No peer could be found or reached for the stream: no metadata peer answered content-id
    /// resolution, or initial swarm discovery found nobody. The message is a diagnostic for the
    /// log; HTTP clients get the stable `no_peers` code instead.
    NoPeers(String),
    /// A network step (catalog hosts) was unreachable, as distinct from [`Self::NoPeers`]. The
    /// message is a diagnostic for the log; HTTP clients get the stable `network` code.
    Network(String),
    /// A shared resolver (the catalog-resolution cache) was at capacity. HTTP statuses are those
    /// of [`Self::Backend`]; native clients get the stable `capacity` code. The message is a
    /// diagnostic for the log.
    ResolverBusy(String),
    /// The id is well-formed, but the provider holds no verified metadata to open it with and
    /// will not guess (#164: a bare infohash without a verified transport descriptor). The
    /// message is user-facing: it explains the refusal and how to open the stream instead, and
    /// is safe to return to HTTP clients and to print in the CLI.
    Unresolvable(String),
    /// [`Self::Unresolvable`] because the catalog lookup that would have verified the id could
    /// not reach the catalog. HTTP statuses and the user-facing message are those of
    /// `Unresolvable`; native clients get the stable `network` code.
    UnresolvableNetwork(String),
    /// The id resolved to an origin HLS source, which cannot be opened as raw TS. HTTP callers
    /// hand this URL to the player (or the opt-in managed relay); no local session or piece
    /// geometry is created. `Debug` and `Display` never include the (possibly signed) URL.
    DirectHls(Box<ace_swarm::resolve::HlsOrigin>),
}

impl std::fmt::Display for ProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProviderError::ProtectedContent => f.write_str("encrypted content is not supported"),
            ProviderError::InvalidProtection => f.write_str("invalid content protection metadata"),
            ProviderError::CapacityExhausted => f.write_str("session capacity reached"),
            ProviderError::NotFound => f.write_str("not found"),
            ProviderError::Backend(msg)
            | ProviderError::NoPeers(msg)
            | ProviderError::Network(msg)
            | ProviderError::ResolverBusy(msg)
            | ProviderError::Unresolvable(msg)
            | ProviderError::UnresolvableNetwork(msg) => f.write_str(msg),
            ProviderError::DirectHls(_) => {
                f.write_str("source is origin HLS; open it through an HTTP playback route")
            }
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

impl From<ace_swarm::resolve::ResolveError> for ProviderError {
    fn from(error: ace_swarm::resolve::ResolveError) -> Self {
        match error {
            ace_swarm::resolve::ResolveError::ProtectedContent => Self::ProtectedContent,
            ace_swarm::resolve::ResolveError::InvalidProtection => Self::InvalidProtection,
            ace_swarm::resolve::ResolveError::HlsOrigin(origin) => Self::DirectHls(origin),
            ace_swarm::resolve::ResolveError::Hls(reason) => Self::Unresolvable(reason.into()),
            other if other.failure_kind() == ace_swarm::resolve::ResolveFailureKind::Network => {
                Self::Network(format!("descriptor resolution: {other:?}"))
            }
            other => Self::Backend(format!("descriptor resolution: {other:?}")),
        }
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
            ..SourceStats::default()
        };
        assert_eq!(s.downloaded, 8192);
        assert_eq!(s.uploaded, 4096);
        assert_eq!(s.peers_served, 2);
    }
}
