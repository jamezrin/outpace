//! Opt-in, bounded HTTP-origin HLS relay. This is not a swarm, TS packager or DVR.
//!
//! Each viewer owns a private resource registry. Origin resource URLs are replaced
//! in local playlists and excluded from diagnostics and resource selectors. The player owns
//! rendition selection; this module retrieves and rewrites supported resources.

use crate::config::HlsHistoryConfig;
use crate::history::{HistoryBudget, HlsHistoryError};
use ace_media::hls_input::{self as playlist, PlaylistError, ResourceKind};
use ace_swarm::http_fetch::{self, FetchError, FetchLimits, FetchPolicy, FetchUrl};
use bytes::{Bytes, BytesMut};
use futures::Stream;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

mod compression;
mod history;
use history::{CaptureTicket, OriginHistory, Retained};
pub use history::{OriginHistoryStatus, OriginHistoryView, OriginSeekTarget};

#[cfg(test)]
mod tests;

const MAX_SESSIONS: usize = 64;
const MAX_RESOURCES: usize = 1024;
const MAX_URL_BYTES: usize = 512 * 1024;
const MAX_MEDIA_BYTES: usize = 16 * 1024 * 1024;
const MAX_CACHE_BYTES: usize = 16 * 1024 * 1024;
const MAX_CACHE_ENTRIES: usize = 1024;
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const HARD_TIMEOUT: Duration = Duration::from_secs(6 * 60 * 60);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const BODY_SLICE: usize = 64 * 1024;

struct RequestedResource {
    url: FetchUrl,
    kind: ResourceKind,
    fetch: Arc<tokio::sync::Mutex<()>>,
    cache_generation: u64,
    capture: Option<CaptureTicket>,
}

/// Fixed diagnostics deliberately exclude origin URLs and client error chains.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OriginError {
    InvalidSource,
    InvalidPlaylist,
    UnsupportedPlaylist,
    TooLarge,
    Busy,
    NotFound,
    InvalidRange,
    Timeout,
    Cancelled,
    History(HlsHistoryError),
    HistoryConflict,
    Fetch(FetchError),
}
impl OriginError {
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidSource => "invalid_hls_origin",
            Self::InvalidPlaylist => "invalid_origin_playlist",
            Self::UnsupportedPlaylist => "unsupported_origin_playlist",
            Self::TooLarge => "origin_resource_limit",
            Self::Busy => "origin_capacity",
            Self::NotFound => "origin_not_found",
            Self::InvalidRange => "invalid_origin_range",
            Self::Timeout => "origin_timeout",
            Self::Cancelled => "origin_cancelled",
            Self::History(HlsHistoryError::Disabled) => "origin_history_disabled",
            Self::History(HlsHistoryError::NotReady) => "origin_history_not_ready",
            Self::History(HlsHistoryError::Unavailable) => "origin_history_unavailable",
            Self::History(HlsHistoryError::Gone) => "origin_history_gone",
            Self::History(HlsHistoryError::InvalidTarget) => "invalid_origin_history_target",
            Self::HistoryConflict => "origin_history_generation_changed",
            Self::Fetch(FetchError::ResourceHttpStatus(status)) => match status {
                401 => "origin_upstream_unauthorized",
                403 => "origin_upstream_forbidden",
                404 => "origin_upstream_not_found",
                410 => "origin_upstream_gone",
                429 => "origin_upstream_rate_limited",
                503 => "origin_upstream_unavailable",
                _ => "origin_upstream_http_status",
            },
            Self::Fetch(error) => error.code(),
        }
    }
    pub const fn upstream_status(self) -> Option<u16> {
        match self {
            Self::Fetch(error) => error.upstream_status(),
            _ => None,
        }
    }
    pub fn status(&self) -> u16 {
        match self {
            Self::InvalidSource | Self::InvalidRange => 400,
            Self::UnsupportedPlaylist => 415,
            Self::Busy => 503,
            Self::NotFound | Self::Cancelled => 404,
            Self::History(HlsHistoryError::Gone) => 410,
            Self::History(HlsHistoryError::InvalidTarget) => 400,
            Self::History(HlsHistoryError::NotReady) => 503,
            Self::History(HlsHistoryError::Unavailable) => 416,
            Self::History(HlsHistoryError::Disabled) => 409,
            Self::HistoryConflict => 409,
            Self::Timeout => 504,
            Self::Fetch(
                FetchError::DnsTimeout
                | FetchError::ConnectTimeout
                | FetchError::HeadersTimeout
                | FetchError::BodyTimeout,
            ) => 504,
            _ => 502,
        }
    }
}
impl std::fmt::Display for OriginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for OriginError {}
impl From<PlaylistError> for OriginError {
    fn from(error: PlaylistError) -> Self {
        match error {
            PlaylistError::Invalid => Self::InvalidPlaylist,
            PlaylistError::Unsupported => Self::UnsupportedPlaylist,
            PlaylistError::TooLarge => Self::TooLarge,
        }
    }
}
impl From<FetchError> for OriginError {
    fn from(value: FetchError) -> Self {
        Self::Fetch(value)
    }
}
impl From<HlsHistoryError> for OriginError {
    fn from(value: HlsHistoryError) -> Self {
        Self::History(value)
    }
}

pub struct OriginPlayback {
    pub playback_url: String,
    pub stop_url: String,
    pub command_url: String,
}
#[derive(Clone)]
struct ResponseData {
    status: u16,
    content_type: String,
    content_range: Option<String>,
    body: Bytes,
    allocation: usize,
}

/// A response retains admission and viewer ownership until its body is consumed or dropped.
pub struct OriginResponse {
    pub status: u16,
    pub content_type: String,
    pub content_range: Option<String>,
    pub content_length: u64,
    pub body: Bytes,
    request: RequestGuard,
    _admission: OwnedSemaphorePermit,
    _fetch: Option<OwnedSemaphorePermit>,
    _history: Option<Arc<Retained>>,
    history_generation: Option<u64>,
}
impl OriginResponse {
    pub fn into_stream(self) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
        futures::stream::unfold((self, 0), |(response, offset)| async move {
            if offset >= response.body.len() {
                return None;
            }
            if *response.request.session.cancel.borrow() {
                let end = response.body.len();
                return Some((
                    Err(std::io::Error::other("origin_cancelled")),
                    (response, end),
                ));
            }
            if response.history_generation.is_some_and(|generation| {
                !response
                    .request
                    .session
                    .inner
                    .lock()
                    .unwrap()
                    .history
                    .has_view(generation)
            }) {
                let end = response.body.len();
                return Some((
                    Err(std::io::Error::other("origin_history_generation_changed")),
                    (response, end),
                ));
            }
            let end = (offset + BODY_SLICE).min(response.body.len());
            let bytes = response.body.slice(offset..end);
            Some((Ok(bytes), (response, end)))
        })
    }
}

struct Resource {
    url: FetchUrl,
    kind: ResourceKind,
    suffix: &'static str,
    owner: Option<MediaOwner>,
    playlist: PlaylistState,
    cache_generation: u64,
    retention: Duration,
    grace_until: Option<Instant>,
    active: usize,
    touched: u64,
    fetch: Arc<tokio::sync::Mutex<()>>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
struct MediaOwner {
    playlist: u64,
    epoch: u64,
}
#[derive(Default)]
struct PlaylistState {
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    discontinuity_sequence: Option<u64>,
    epoch: u64,
    refresh: u64,
    fingerprint: Option<[u8; 32]>,
    retention: Duration,
    media: HashSet<u64>,
}
struct SessionInner {
    resources: HashMap<u64, Resource>,
    next_id: u64,
    tick: u64,
    url_bytes: usize,
    pending: usize,
    idle: Instant,
    history: OriginHistory,
}
struct Session {
    inner: Mutex<SessionInner>,
    expires: Instant,
    cancel: watch::Sender<bool>,
}
impl Session {
    fn close(&self) {
        // Cache/registry publication takes this same lock before checking cancellation.
        let mut inner = self.inner.lock().unwrap();
        self.cancel.send_replace(true);
        inner.history.clear();
    }
    fn expired(&self, now: Instant) -> bool {
        let mut inner = self.inner.lock().unwrap();
        inner.history.prune(now);
        self.expires <= now || (inner.pending == 0 && inner.idle <= now)
    }
}
struct RequestGuard {
    session: Arc<Session>,
    id: u64,
}
impl Drop for RequestGuard {
    fn drop(&mut self) {
        let mut inner = self.session.inner.lock().unwrap();
        inner.pending -= 1;
        if let Some(resource) = inner.resources.get_mut(&self.id) {
            resource.active -= 1;
        }
        inner.idle = Instant::now() + IDLE_TIMEOUT;
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    token: String,
    id: u64,
    range: Option<String>,
}
struct Cached {
    data: ResponseData,
    expires: Instant,
    touched: u64,
}
#[derive(Default)]
struct Cache {
    entries: HashMap<CacheKey, Cached>,
    bytes: usize,
    tick: u64,
}
impl Cache {
    fn retain(&mut self, mut keep: impl FnMut(&CacheKey, &Cached) -> bool) {
        self.entries.retain(|key, value| keep(key, value));
        self.bytes = self
            .entries
            .values()
            .map(|c| c.data.allocation.max(c.data.body.len()))
            .sum();
    }
    fn get(&mut self, key: &CacheKey) -> Option<ResponseData> {
        self.retain(|_, value| value.expires > Instant::now());
        self.tick = self.tick.wrapping_add(1);
        self.entries.get_mut(key).map(|c| {
            c.touched = self.tick;
            c.data.clone()
        })
    }
    fn put(&mut self, key: CacheKey, data: ResponseData, ttl: Duration) {
        if data.allocation.max(data.body.len()) > MAX_CACHE_BYTES || ttl.is_zero() {
            return;
        }
        self.retain(|k, value| value.expires > Instant::now() && *k != key);
        while self.entries.len() >= MAX_CACHE_ENTRIES
            || self.bytes + data.allocation.max(data.body.len()) > MAX_CACHE_BYTES
        {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, v)| v.touched)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            let removed = self.entries.remove(&oldest).unwrap().data;
            self.bytes -= removed.allocation.max(removed.body.len());
        }
        self.tick = self.tick.wrapping_add(1);
        self.bytes += data.allocation.max(data.body.len());
        self.entries.insert(
            key,
            Cached {
                data,
                expires: Instant::now() + ttl.min(Duration::from_secs(30)),
                touched: self.tick,
            },
        );
    }
}

/// Runtime-owned relay; defaults are deliberately finite and independent of swarm sessions.
pub struct OriginHls {
    policy: FetchPolicy,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    cache: Mutex<Cache>,
    admission: Arc<Semaphore>,
    fetches: Arc<Semaphore>,
    resource_clients: http_fetch::ResourceClientPool,
    reaper_started: AtomicBool,
    lifetime: watch::Sender<bool>,
    history_config: HlsHistoryConfig,
    history_budget: HistoryBudget,
}
impl OriginHls {
    pub fn new(policy: FetchPolicy) -> Arc<Self> {
        Self::new_with_history(policy, HlsHistoryConfig::default(), HistoryBudget::new(0))
    }
    pub(crate) fn new_with_history(
        policy: FetchPolicy,
        history_config: HlsHistoryConfig,
        history_budget: HistoryBudget,
    ) -> Arc<Self> {
        let (lifetime, _) = watch::channel(false);
        Arc::new(Self {
            policy,
            sessions: Mutex::new(HashMap::new()),
            cache: Mutex::new(Cache::default()),
            admission: Arc::new(Semaphore::new(16)),
            fetches: Arc::new(Semaphore::new(4)),
            resource_clients: http_fetch::ResourceClientPool::default(),
            reaper_started: AtomicBool::new(false),
            lifetime,
            history_config,
            history_budget,
        })
    }
    /// Admit the resolved origin. Media remains lazy, so creating a playback does not
    /// fetch a manifest twice or claim that the origin is presently playable.
    pub async fn start(self: &Arc<Self>, raw_url: &str) -> Result<OriginPlayback, OriginError> {
        let url =
            FetchUrl::parse_source(raw_url, self.policy).map_err(|_| OriginError::InvalidSource)?;
        self.purge();
        let token = random_token();
        let now = Instant::now();
        let (cancel, _) = watch::channel(false);
        let bytes = url.serialized_len();
        let session = Arc::new(Session {
            inner: Mutex::new(SessionInner {
                resources: HashMap::from([(
                    0,
                    Resource {
                        url,
                        kind: ResourceKind::Playlist,
                        suffix: ".m3u8",
                        owner: None,
                        playlist: PlaylistState::default(),
                        cache_generation: 0,
                        retention: Duration::ZERO,
                        grace_until: None,
                        active: 0,
                        touched: 0,
                        fetch: Arc::new(tokio::sync::Mutex::new(())),
                    },
                )]),
                next_id: 1,
                tick: 0,
                url_bytes: bytes,
                pending: 0,
                idle: now + IDLE_TIMEOUT,
                history: OriginHistory::new(self.history_config, self.history_budget.clone()),
            }),
            expires: now + HARD_TIMEOUT,
            cancel,
        });
        let mut sessions = self.sessions.lock().unwrap();
        if sessions.len() >= MAX_SESSIONS {
            return Err(OriginError::Busy);
        }
        sessions.insert(token.clone(), session);
        drop(sessions);
        self.ensure_reaper();
        Ok(OriginPlayback {
            playback_url: format!("/origin-hls/{token}/0.m3u8"),
            stop_url: format!("/origin-hls/{token}"),
            command_url: format!("/origin-hls/{token}/cmd"),
        })
    }
    pub fn stop(&self, token: &str) -> bool {
        let session = self.sessions.lock().unwrap().remove(token);
        if let Some(session) = session {
            session.close();
            self.cache
                .lock()
                .unwrap()
                .retain(|key, _| key.token != token);
            true
        } else {
            false
        }
    }
    pub fn active_count(&self) -> usize {
        self.purge();
        self.sessions.lock().unwrap().len()
    }
    fn history_session(&self, token: &str) -> Result<Arc<Session>, OriginError> {
        if token.len() != 64 || !token.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(OriginError::NotFound);
        }
        self.purge();
        self.sessions
            .lock()
            .unwrap()
            .get(token)
            .cloned()
            .ok_or(OriginError::NotFound)
    }
    pub fn history_status(&self, token: &str) -> Result<OriginHistoryStatus, OriginError> {
        let session = self.history_session(token)?;
        let mut inner = session.inner.lock().unwrap();
        if *session.cancel.borrow() {
            return Err(OriginError::NotFound);
        }
        inner.idle = Instant::now() + IDLE_TIMEOUT;
        Ok(inner.history.status(token, Instant::now()))
    }
    pub fn seek(
        &self,
        token: &str,
        target: OriginSeekTarget,
        expected_generation: Option<u64>,
    ) -> Result<OriginHistoryView, OriginError> {
        let session = self.history_session(token)?;
        let mut inner = session.inner.lock().unwrap();
        if *session.cancel.borrow() {
            return Err(OriginError::NotFound);
        }
        let view = inner
            .history
            .seek(token, target, expected_generation, Instant::now())?;
        inner.idle = Instant::now() + IDLE_TIMEOUT;
        Ok(view)
    }
    pub fn live(
        &self,
        token: &str,
        expected_generation: Option<u64>,
    ) -> Result<OriginHistoryView, OriginError> {
        let session = self.history_session(token)?;
        let mut inner = session.inner.lock().unwrap();
        if *session.cancel.borrow() {
            return Err(OriginError::NotFound);
        }
        let view = inner.history.live(token, expected_generation)?;
        inner.idle = Instant::now() + IDLE_TIMEOUT;
        Ok(view)
    }
    pub async fn replay_resource(
        self: &Arc<Self>,
        token: &str,
        generation: u64,
        resource: &str,
    ) -> Result<OriginResponse, OriginError> {
        let session = self.history_session(token)?;
        let admission = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| OriginError::Busy)?;
        let (body, owner) = {
            let mut inner = session.inner.lock().unwrap();
            if *session.cancel.borrow() {
                return Err(OriginError::NotFound);
            }
            let data = inner
                .history
                .replay(token, generation, resource, Instant::now())?;
            inner.pending += 1;
            inner
                .resources
                .get_mut(&0)
                .ok_or(OriginError::NotFound)?
                .active += 1;
            data
        };
        Ok(OriginResponse {
            status: 200,
            content_type: if resource == "index.m3u8" {
                "application/vnd.apple.mpegurl"
            } else {
                "video/mp2t"
            }
            .into(),
            content_range: None,
            content_length: body.len() as u64,
            body,
            request: RequestGuard { session, id: 0 },
            _admission: admission,
            _fetch: None,
            _history: owner,
            history_generation: Some(generation),
        })
    }
    pub async fn resource(
        self: &Arc<Self>,
        token: &str,
        id: &str,
        range: Option<&str>,
    ) -> Result<OriginResponse, OriginError> {
        if token.len() != 64 || !token.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(OriginError::NotFound);
        }
        if range.is_some_and(|range| !valid_range(range)) {
            return Err(OriginError::InvalidRange);
        }
        let (number, suffix) = id
            .find('.')
            .map_or((id, None), |dot| (&id[..dot], Some(&id[dot..])));
        if number.is_empty() || !number.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(OriginError::NotFound);
        }
        let id = number.parse::<u64>().map_err(|_| OriginError::NotFound)?;
        let admission = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| OriginError::Busy)?;
        self.purge();
        let session = self
            .sessions
            .lock()
            .unwrap()
            .get(token)
            .cloned()
            .ok_or(OriginError::NotFound)?;
        let resource = {
            let mut inner = session.inner.lock().unwrap();
            if *session.cancel.borrow() {
                return Err(OriginError::NotFound);
            }
            inner.tick = inner.tick.wrapping_add(1);
            let tick = inner.tick;
            let entry = inner.resources.get_mut(&id).ok_or(OriginError::NotFound)?;
            if suffix.is_some_and(|suffix| suffix != entry.suffix) {
                return Err(OriginError::NotFound);
            }
            entry.active += 1;
            entry.touched = tick;
            let mut resource = RequestedResource {
                url: entry.url.clone(),
                kind: entry.kind,
                fetch: entry.fetch.clone(),
                cache_generation: entry.cache_generation,
                capture: None,
            };
            if resource.kind == ResourceKind::Media && matches!(range, None | Some("bytes=0-")) {
                resource.capture = inner.history.capture_ticket(id, Instant::now());
            }
            inner.pending += 1;
            resource
        };
        let request = RequestGuard {
            session: session.clone(),
            id,
        };
        let mut cancel = session.cancel.subscribe();
        let deadline = Instant::now() + REQUEST_TIMEOUT;
        let (result, fetch_permit) = tokio::select! {
            biased;
            _ = cancelled(&mut cancel) => Err(OriginError::Cancelled),
            result = tokio::time::timeout_at(deadline, self.load(token, id, &session, resource, range, deadline)) => {
                result.map_err(|_| OriginError::Timeout)?
            }
        }?;
        if *session.cancel.borrow() {
            return Err(OriginError::Cancelled);
        }
        Ok(OriginResponse {
            status: result.status,
            content_type: result.content_type,
            content_range: result.content_range,
            content_length: result.body.len() as u64,
            body: result.body,
            request,
            _admission: admission,
            _fetch: fetch_permit,
            _history: None,
            history_generation: None,
        })
    }
    async fn load(
        &self,
        token: &str,
        id: u64,
        session: &Arc<Session>,
        resource: RequestedResource,
        range: Option<&str>,
        deadline: Instant,
    ) -> Result<(ResponseData, Option<OwnedSemaphorePermit>), OriginError> {
        let RequestedResource {
            url,
            kind,
            fetch,
            cache_generation,
            capture,
        } = resource;
        let key = CacheKey {
            token: token.to_owned(),
            id,
            range: range.map(str::to_owned),
        };
        let _resource_fetch = fetch.lock().await;
        let fetch_permit = self
            .fetches
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| OriginError::Cancelled)?;
        if *session.cancel.borrow() {
            return Err(OriginError::Cancelled);
        }
        if kind == ResourceKind::Media {
            let cached = self.cache.lock().unwrap().get(&key);
            if let Some(data) = cached {
                Self::record(session, capture, &data);
                return Ok((data, Some(fetch_permit)));
            }
        }
        let opened_at = Instant::now();
        let limits = FetchLimits {
            connect_timeout: Duration::from_secs(5),
            headers_timeout: REQUEST_TIMEOUT,
            source_response: true,
        };
        let mut response = if kind == ResourceKind::Playlist {
            // Playlists are rewritten representations; byte offsets on the origin
            // cannot address them. HTTP permits ignoring a valid Range and returning
            // the entire representation (FFmpeg sends bytes=0- while probing).
            self.resource_clients
                .open_playlist_resource(&url, self.policy, limits, session.cancel.subscribe())
                .await
        } else {
            self.resource_clients
                .open_resource(&url, self.policy, limits, range, session.cancel.subscribe())
                .await
        }
        .map_err(|error| {
            if error == FetchError::InvalidSourceConfig && range.is_some() {
                OriginError::InvalidRange
            } else {
                OriginError::Fetch(error)
            }
        })?;
        let limit = if kind == ResourceKind::Playlist {
            playlist::MAX_PLAYLIST_BYTES
        } else {
            MAX_MEDIA_BYTES
        };
        if response
            .content_length
            .is_some_and(|len| len > limit as u64)
        {
            return Err(OriginError::TooLarge);
        }
        let mut bytes = BytesMut::new();
        while let Some(chunk) = response.body.next_chunk().await? {
            if chunk.len() > limit.saturating_sub(bytes.len()) {
                return Err(OriginError::TooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        if response
            .content_length
            .is_some_and(|len| len != bytes.len() as u64)
        {
            return Err(OriginError::Fetch(FetchError::BodyFailed));
        }
        if let Some(content_range) = &response.content_range {
            if range_length(content_range).is_none_or(|length| length != bytes.len() as u64) {
                return Err(OriginError::Fetch(FetchError::BodyFailed));
            }
        }
        if response.content_encoding == http_fetch::ResourceEncoding::Gzip {
            let decode_deadline = deadline.min(response.deadline);
            bytes = compression::decode_gzip(&bytes, limit, || {
                if *session.cancel.borrow() {
                    Err(OriginError::Cancelled)
                } else if Instant::now() >= decode_deadline {
                    Err(OriginError::Timeout)
                } else {
                    Ok(())
                }
            })?;
        }
        let allocation = bytes.capacity();
        let body = if kind == ResourceKind::Playlist {
            if response.status != 200 {
                return Err(OriginError::InvalidPlaylist);
            }
            self.rewrite(token, session, id, &response.final_url, &bytes)?
        } else {
            bytes.freeze()
        };
        if *session.cancel.borrow() {
            return Err(OriginError::Cancelled);
        }
        let data = ResponseData {
            status: response.status,
            content_type: if kind == ResourceKind::Playlist {
                "application/vnd.apple.mpegurl".to_owned()
            } else {
                media_content_type(response.content_type.as_deref()).to_owned()
            },
            content_range: response.content_range,
            body,
            allocation,
        };
        Self::record(session, capture, &data);
        if kind == ResourceKind::Media {
            if let Some(ttl) = response.cache_ttl {
                let ttl = ttl.saturating_sub(opened_at.elapsed());
                // Serialize publication against stop. No resource or cache enrollment
                // may occur after its ownership token has been revoked.
                let inner = session.inner.lock().unwrap();
                if *session.cancel.borrow() {
                    return Err(OriginError::Cancelled);
                }
                if inner
                    .resources
                    .get(&id)
                    .is_some_and(|resource| resource.cache_generation == cache_generation)
                {
                    self.cache.lock().unwrap().put(key, data.clone(), ttl);
                }
            }
        }
        // Retain the slot while the response is owned by a slow reader; otherwise
        // completed 16 MiB resources could accumulate beyond the fetch memory bound.
        Ok((data, Some(fetch_permit)))
    }
    fn record(session: &Session, ticket: Option<CaptureTicket>, data: &ResponseData) {
        if data.status != 200 || data.content_range.is_some() {
            return;
        }
        let Some(capture) = ticket.and_then(|ticket| {
            ticket.validate(&data.body, data.allocation, || *session.cancel.borrow())
        }) else {
            return;
        };
        let mut inner = session.inner.lock().unwrap();
        if !*session.cancel.borrow() {
            inner.history.capture_validated(capture, Instant::now());
        }
    }
    fn rewrite(
        &self,
        token: &str,
        session: &Session,
        parent_id: u64,
        base: &FetchUrl,
        bytes: &[u8],
    ) -> Result<Bytes, OriginError> {
        let playlist = playlist::Playlist::parse(bytes, HARD_TIMEOUT)?;
        let replay_supported = playlist.supports_simple_replay();
        let replay_segments = playlist.segments().to_vec();
        let mut refs = Vec::with_capacity(playlist.references().len());
        for reference in playlist.references() {
            refs.push((
                base.join_reference(&reference.uri, self.policy)
                    .map_err(|_| OriginError::InvalidSource)?,
                reference.kind,
            ));
        }
        for (index, reference) in playlist.references().iter().enumerate() {
            if let Some(previous) = reference.implicit_from {
                if refs[previous].0 != refs[index].0 {
                    return Err(OriginError::InvalidPlaylist);
                }
            }
        }
        let mut inner = session.inner.lock().unwrap();
        if *session.cancel.borrow() {
            return Err(OriginError::Cancelled);
        }
        let parent = inner
            .resources
            .get(&parent_id)
            .ok_or(OriginError::NotFound)?;
        if parent.kind != ResourceKind::Playlist {
            return Err(OriginError::InvalidPlaylist);
        }
        let mut epoch = parent.playlist.epoch;
        let timeline = playlist.media_timeline();
        let discontinuity_sequence = playlist.discontinuity_sequence();
        if let Some((first, last)) = timeline {
            if parent
                .playlist
                .first_sequence
                .is_some_and(|old| first < old)
                || last
                    .zip(parent.playlist.last_sequence)
                    .is_some_and(|(last, old)| last < old)
                || discontinuity_sequence
                    .zip(parent.playlist.discontinuity_sequence)
                    .is_some_and(|(current, old)| current < old)
            {
                epoch = epoch.checked_add(1).ok_or(OriginError::TooLarge)?;
            }
        }
        let previous_refresh = parent.playlist.refresh;
        let previous_fingerprint = parent.playlist.fingerprint;
        let retention = parent.playlist.retention.max(playlist.retention());
        let previous_media = parent.playlist.media.clone();
        let owner = MediaOwner {
            playlist: parent_id,
            epoch,
        };
        // URI identity remains stable for normal reloads and sliding overlap, including
        // repeated ranged segments and MAPs on the same resource. Cache generations
        // change separately. A rollback of either media or discontinuity sequence
        // starts a new resource epoch; normal discontinuity window slides do not.
        let mut selected: HashMap<(FetchUrl, ResourceKind), (u64, &'static str)> = HashMap::new();
        let mut required = HashSet::from([0, parent_id]);
        for (id, resource) in &inner.resources {
            if *id != parent_id {
                required.extend(resource.playlist.media.iter().copied());
            }
        }
        let mut current_media = HashSet::new();
        let mut new = Vec::new();
        let mut next = inner.next_id;
        let existing: HashMap<_, _> = inner
            .resources
            .iter()
            .filter(|(_, resource)| {
                resource.kind == ResourceKind::Playlist || resource.owner == Some(owner)
            })
            .map(|(id, resource)| {
                (
                    (resource.url.clone(), resource.kind),
                    (*id, resource.suffix),
                )
            })
            .collect();
        for (url, kind) in &refs {
            if selected.contains_key(&(url.clone(), *kind)) {
                continue;
            }
            let (id, suffix) = match existing.get(&(url.clone(), *kind)).copied() {
                Some(value) => value,
                None => {
                    let id = next;
                    next = next.checked_add(1).ok_or(OriginError::TooLarge)?;
                    let suffix = if *kind == ResourceKind::Playlist {
                        ".m3u8"
                    } else {
                        url.safe_media_extension().unwrap_or("")
                    };
                    new.push((id, url.clone(), *kind, suffix));
                    (id, suffix)
                }
            };
            selected.insert((url.clone(), *kind), (id, suffix));
            required.insert(id);
            if *kind == ResourceKind::Media {
                current_media.insert(id);
            }
        }
        let replacements: Vec<_> = refs
            .iter()
            .map(|key| {
                let (id, suffix) = selected[key];
                format!("/origin-hls/{token}/{id}{suffix}")
            })
            .collect();
        let replay_segments: Vec<_> = replay_segments
            .into_iter()
            .map(|segment| (selected[&refs[segment.reference_index]].0, segment))
            .collect();
        let rendered = playlist.render(&replacements)?;
        // Fingerprint both the source and the planned local representation. Resolved
        // URI changes (including a redirected relative base) change local resource IDs;
        // byte-identical reloads preserve media cache generations and original TTLs.
        let mut fingerprint = Sha256::new();
        fingerprint.update((bytes.len() as u64).to_be_bytes());
        fingerprint.update(bytes);
        fingerprint.update((rendered.len() as u64).to_be_bytes());
        fingerprint.update(rendered.as_bytes());
        let fingerprint: [u8; 32] = fingerprint.finalize().into();
        let changed = previous_fingerprint != Some(fingerprint);
        let refresh = if changed {
            previous_refresh
                .checked_add(1)
                .ok_or(OriginError::TooLarge)?
        } else {
            previous_refresh
        };
        // Stage removal deadlines before admission: this update must not evict a
        // just-removed segment before the client's previously served playlist expires.
        let now = Instant::now();
        let grace: HashMap<_, _> = previous_media
            .difference(&current_media)
            .filter_map(|id| {
                inner.resources.get(id).map(|resource| {
                    (
                        *id,
                        now.checked_add(resource.retention)
                            .unwrap_or(session.expires)
                            .min(session.expires),
                    )
                })
            })
            .collect();
        let added_bytes: usize = new.iter().map(|(_, url, _, _)| url.serialized_len()).sum();
        let mut count = inner.resources.len() + new.len();
        let mut total = inner.url_bytes + added_bytes;
        let mut candidates: Vec<_> = inner
            .resources
            .iter()
            // Master/rendition URLs remain usable throughout the viewer lease, even
            // while a selected media playlist rolls old segment resources out.
            .filter(|(id, r)| {
                r.kind == ResourceKind::Media
                    && r.active == 0
                    && !required.contains(id)
                    && grace
                        .get(id)
                        .copied()
                        .or(r.grace_until)
                        .is_some_and(|deadline| deadline <= now)
            })
            .map(|(id, r)| (*id, r.touched, r.url.serialized_len()))
            .collect();
        candidates.sort_by_key(|(id, touched, _)| (*touched, *id));
        let mut remove = Vec::new();
        for (id, _, bytes) in candidates {
            if count <= MAX_RESOURCES && total <= MAX_URL_BYTES {
                break;
            }
            count -= 1;
            total -= bytes;
            remove.push(id);
        }
        if count > MAX_RESOURCES || total > MAX_URL_BYTES {
            return Err(OriginError::TooLarge);
        }
        for id in &remove {
            inner.resources.remove(id);
        }
        inner.next_id = next;
        inner.url_bytes = total;
        inner.tick = inner.tick.wrapping_add(1);
        let tick = inner.tick;
        for (id, url, kind, suffix) in new {
            inner.resources.insert(
                id,
                Resource {
                    url,
                    kind,
                    suffix,
                    owner: (kind == ResourceKind::Media).then_some(owner),
                    playlist: PlaylistState::default(),
                    cache_generation: refresh,
                    retention,
                    grace_until: None,
                    active: 0,
                    touched: tick,
                    fetch: Arc::new(tokio::sync::Mutex::new(())),
                },
            );
        }
        for (id, deadline) in grace {
            if let Some(resource) = inner.resources.get_mut(&id) {
                resource.grace_until = Some(deadline);
            }
        }
        for id in &current_media {
            let resource = inner.resources.get_mut(id).expect("selected resource");
            resource.cache_generation = refresh;
            resource.retention = resource.retention.max(retention);
            resource.grace_until = None;
        }
        let parent = inner
            .resources
            .get_mut(&parent_id)
            .expect("protected parent");
        if let Some((first, last)) = timeline {
            parent.playlist.first_sequence = Some(first);
            parent.playlist.last_sequence = last;
        }
        parent.playlist.discontinuity_sequence = discontinuity_sequence;
        parent.playlist.epoch = epoch;
        parent.playlist.refresh = refresh;
        parent.playlist.fingerprint = Some(fingerprint);
        parent.playlist.retention = retention;
        parent.playlist.media = current_media.clone();
        if parent_id == 0 {
            inner
                .history
                .observe(epoch, replay_supported, &replay_segments, now);
        }
        for id in required {
            if let Some(resource) = inner.resources.get_mut(&id) {
                resource.touched = tick;
            }
        }
        if !remove.is_empty() || (changed && !current_media.is_empty()) {
            self.cache.lock().unwrap().retain(|key, _| {
                key.token != token
                    || (!remove.contains(&key.id) && (!changed || !current_media.contains(&key.id)))
            });
        }
        Ok(Bytes::from(rendered))
    }
    fn purge(&self) {
        let now = Instant::now();
        let mut sessions = self.sessions.lock().unwrap();
        sessions.retain(|_, session| {
            if session.expired(now) {
                session.close();
                false
            } else {
                true
            }
        });
        self.cache
            .lock()
            .unwrap()
            .retain(|key, value| value.expires > now && sessions.contains_key(&key.token));
    }
    fn ensure_reaper(self: &Arc<Self>) {
        if self.reaper_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let weak: Weak<Self> = Arc::downgrade(self);
        let mut lifetime = self.lifetime.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = lifetime.changed() => break,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        let Some(relay) = weak.upgrade() else { break; };
                        relay.purge();
                    }
                }
            }
        });
    }
}

fn valid_range(raw: &str) -> bool {
    if raw.len() > 64 {
        return false;
    }
    let Some((start, end)) = raw.strip_prefix("bytes=").and_then(|s| s.split_once('-')) else {
        return false;
    };
    let number = |value: &str| {
        (!value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
            .then(|| value.parse::<u64>().ok())
            .flatten()
    };
    match (number(start), number(end)) {
        (Some(start), Some(end)) => end
            .checked_sub(start)
            .and_then(|n| n.checked_add(1))
            .is_some(),
        (Some(_), None) => end.is_empty(),
        (None, Some(size)) => start.is_empty() && size > 0,
        _ => false,
    }
}
impl Drop for OriginHls {
    fn drop(&mut self) {
        self.lifetime.send_replace(true);
        for session in self.sessions.get_mut().unwrap().values() {
            session.close();
        }
    }
}
async fn cancelled(cancel: &mut watch::Receiver<bool>) {
    loop {
        if *cancel.borrow() {
            return;
        }
        if cancel.changed().await.is_err() {
            return;
        }
    }
}
fn random_token() -> String {
    use rand::TryRng;
    let mut bytes = [0; 32];
    rand::rngs::SysRng
        .try_fill_bytes(&mut bytes)
        .expect("OS randomness for HLS playback token");
    hex::encode(bytes)
}
fn range_length(value: &str) -> Option<u64> {
    let (bounds, _) = value.strip_prefix("bytes ")?.split_once('/')?;
    let (start, end) = bounds.split_once('-')?;
    end.parse::<u64>()
        .ok()?
        .checked_sub(start.parse().ok()?)?
        .checked_add(1)
}
fn media_content_type(value: Option<&str>) -> &str {
    let value = value.unwrap_or("").split(';').next().unwrap_or("").trim();
    match value {
        "video/mp2t" | "video/MP2T" => "video/mp2t",
        "video/mp4" => "video/mp4",
        "audio/mp4" => "audio/mp4",
        "audio/aac" => "audio/aac",
        "audio/mpeg" => "audio/mpeg",
        "text/vtt" => "text/vtt",
        _ => "application/octet-stream",
    }
}
