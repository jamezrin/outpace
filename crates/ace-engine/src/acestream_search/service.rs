use super::*;
use ace_swarm::http_fetch::{self, FetchLimits, FetchPolicy, FetchUrl};
use bytes::Bytes;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{watch, Semaphore},
    time::Instant,
};

const DEADLINE: Duration = Duration::from_secs(10);
const CACHE_TTL: Duration = Duration::from_secs(30);
const CACHE_ENTRIES: usize = 32;
const CACHE_BYTES: usize = 8 * MAX_RESPONSE_BYTES;

type FetchFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<u8>, SearchError>> + Send + 'a>>;
trait Transport: Send + Sync {
    fn fetch<'a>(&'a self, query: &'a SearchQuery, key: &'a str) -> FetchFuture<'a>;
}
struct HttpTransport;
impl Transport for HttpTransport {
    fn fetch<'a>(&'a self, query: &'a SearchQuery, key: &'a str) -> FetchFuture<'a> {
        Box::pin(async move {
            let policy = FetchPolicy::default();
            let url = FetchUrl::parse_source(&request_url(query, key), policy)
                .map_err(|_| SearchError::InvalidSettings)?;
            // Keep the sender alive until the finite body is read. The caller's
            // whole-operation timeout/drop also cancels DNS, connect and reads.
            let (_sender, cancel) = watch::channel(false);
            let mut body = http_fetch::open(
                &url,
                policy,
                FetchLimits {
                    connect_timeout: Duration::from_secs(3),
                    headers_timeout: Duration::from_secs(5),
                    source_response: false,
                },
                cancel,
            )
            .await
            .map_err(|_| SearchError::Upstream)?;
            // The guarded non-resource request refuses every redirect. Never send
            // the API key to a redirect destination, even another public address.
            let mut bytes = Vec::new();
            while let Some(chunk) = body.next_chunk().await.map_err(|_| SearchError::Upstream)? {
                if chunk.len() > MAX_RESPONSE_BYTES.saturating_sub(bytes.len()) {
                    return Err(SearchError::TooLarge);
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(bytes)
        })
    }
}
struct CacheEntry {
    query: SearchQuery,
    expires: Instant,
    bytes: Bytes,
}
struct State {
    entries: VecDeque<CacheEntry>,
    bytes: usize,
    next_start: Instant,
}
impl State {
    fn purge(&mut self) {
        let now = Instant::now();
        self.entries.retain(|entry| entry.expires > now);
        self.bytes = self.entries.iter().map(|entry| entry.bytes.len()).sum();
    }
    fn insert(&mut self, query: SearchQuery, bytes: Bytes) {
        self.purge();
        // This also bounds memory for cache entries, not only their count.
        while !self.entries.is_empty()
            && (self.entries.len() >= CACHE_ENTRIES || self.bytes + bytes.len() > CACHE_BYTES)
        {
            self.bytes -= self.entries.pop_front().unwrap().bytes.len();
        }
        if bytes.len() <= CACHE_BYTES {
            self.bytes += bytes.len();
            self.entries.push_back(CacheEntry {
                query,
                expires: Instant::now() + CACHE_TTL,
                bytes,
            });
        }
    }
}
pub struct SearchService {
    settings: SearchSettings,
    transport: Arc<dyn Transport>,
    state: Mutex<State>,
    permits: Semaphore,
}
impl SearchService {
    pub fn new(settings: SearchSettings) -> Result<Self, SearchError> {
        settings
            .validate(false)
            .map_err(|_| SearchError::InvalidSettings)?;
        if !settings.enabled {
            return Err(SearchError::InvalidSettings);
        }
        Ok(Self {
            settings,
            transport: Arc::new(HttpTransport),
            state: Mutex::new(State {
                entries: VecDeque::new(),
                bytes: 0,
                next_start: Instant::now(),
            }),
            permits: Semaphore::new(2),
        })
    }
    pub async fn search(&self, query: SearchQuery) -> Result<Bytes, SearchError> {
        // Public Rust callers receive the same admission bounds as HTTP callers.
        if !valid_text(&query.query, 256)
            || query.category.as_ref().is_some_and(|v| !valid_text(v, 64))
            || query.page > 10_000
            || !(1..=MAX_PAGE_SIZE).contains(&query.page_size)
        {
            return Err(SearchError::InvalidQuery);
        }
        {
            let mut state = self.state.lock().unwrap();
            state.purge();
            if let Some(entry) = state.entries.iter().find(|entry| entry.query == query) {
                return Ok(entry.bytes.clone());
            }
        }
        let _permit = self.permits.try_acquire().map_err(|_| SearchError::Busy)?;
        {
            let mut state = self.state.lock().unwrap();
            if Instant::now() < state.next_start {
                return Err(SearchError::RateLimited);
            }
            state.next_start = Instant::now() + Duration::from_secs(1);
        }
        let result = tokio::time::timeout(DEADLINE, async {
            let raw = self
                .transport
                .fetch(
                    &query,
                    self.settings
                        .key
                        .as_deref()
                        .ok_or(SearchError::InvalidSettings)?,
                )
                .await?;
            let result = normalize_response(&raw, &query)?;
            serde_json::to_vec(&result)
                .map(Bytes::from)
                .map_err(|_| SearchError::InvalidResponse)
        })
        .await
        .unwrap_or(Err(SearchError::Timeout));
        let mut state = self.state.lock().unwrap();
        match &result {
            Ok(bytes) => state.insert(query, bytes.clone()),
            Err(_) => state.next_start = Instant::now() + Duration::from_secs(30),
        }
        result
    }
}
fn request_url(query: &SearchQuery, key: &str) -> String {
    let mut params = form_urlencoded::Serializer::new(String::new());
    params
        .append_pair("method", "search")
        .append_pair("api_version", "1")
        .append_pair("api_key", key)
        .append_pair("group_by_channels", "1")
        .append_pair("show_epg", "0")
        .append_pair("query", &query.query)
        .append_pair("page", &query.page.to_string())
        .append_pair("page_size", &query.page_size.to_string());
    if let Some(category) = &query.category {
        params.append_pair("category", category);
    }
    format!("https://api.acestream.me/?{}", params.finish())
}
#[cfg(test)]
pub(crate) mod tests;
