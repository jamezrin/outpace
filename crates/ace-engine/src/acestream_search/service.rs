use super::*;
use ace_swarm::http_fetch::{self, FetchLimits, FetchPolicy, FetchUrl};
use bytes::Bytes;
use rand::seq::SliceRandom;
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
    fn fetch<'a>(&'a self, url: &'a str, budget: &'a mut usize) -> FetchFuture<'a>;
}
struct HttpTransport;
impl Transport for HttpTransport {
    fn fetch<'a>(&'a self, raw_url: &'a str, budget: &'a mut usize) -> FetchFuture<'a> {
        Box::pin(async move {
            if let Some(identity) = ace_swarm::engine_profile::identity() {
                return fetch_as_engine(raw_url, budget, &identity.user_agent).await;
            }
            let policy = FetchPolicy::default();
            let url = FetchUrl::parse_source(raw_url, policy)
                .map_err(|_| SearchError::InvalidSettings)?;
            // Whole-operation/per-host timeout or caller cancellation drops DNS,
            // connect and body futures. Never use environment proxies or redirects.
            let (_sender, cancel) = watch::channel(false);
            let mut body = http_fetch::open(
                &url,
                policy,
                FetchLimits {
                    connect_timeout: Duration::from_secs(2),
                    headers_timeout: Duration::from_secs(3),
                    source_response: false,
                },
                cancel,
            )
            .await
            .map_err(|_| SearchError::Upstream)?;
            let mut bytes = Vec::new();
            while let Some(chunk) = body.next_chunk().await.map_err(|_| SearchError::Upstream)? {
                if chunk.len() > *budget {
                    return Err(SearchError::TooLarge);
                }
                *budget -= chunk.len();
                bytes.extend_from_slice(&chunk);
            }
            Ok(bytes)
        })
    }
}
/// Under the AceStream identity, send the request with the engine's exact head instead of
/// the HTTP client's defaults. Search hosts are plain HTTP on port 80.
async fn fetch_as_engine(
    raw_url: &str,
    budget: &mut usize,
    user_agent: &str,
) -> Result<Vec<u8>, SearchError> {
    let rest = raw_url
        .strip_prefix("http://")
        .ok_or(SearchError::InvalidSettings)?;
    let (host, path) = rest
        .find('/')
        .map(|i| rest.split_at(i))
        .ok_or(SearchError::InvalidSettings)?;
    // Room for response headers on top of the body budget.
    let limit = budget.saturating_add(16 * 1024);
    let body = ace_swarm::engine_http::get(host, 80, path, user_agent, limit, HOST_DEADLINE)
        .await
        .map_err(|e| match e {
            ace_swarm::engine_http::EngineHttpError::TooLarge => SearchError::TooLarge,
            ace_swarm::engine_http::EngineHttpError::Timeout => SearchError::Timeout,
            _ => SearchError::Upstream,
        })?;
    if body.len() > *budget {
        return Err(SearchError::TooLarge);
    }
    *budget -= body.len();
    Ok(body)
}

const HOSTS: [&str; 3] = [
    "s5.torrentstream.net",
    "s5.torrentstream.org",
    "s5.torrentstream.info",
];
const HOST_DEADLINE: Duration = Duration::from_secs(3);
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
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| SearchError::InvalidSettings)?
                .as_secs();
            // The service expects a numeric nonce. Hexadecimal random tokens are
            // not interchangeable: server nonce coercion breaks response binding.
            let nonce = rand::random_range(1..=i64::MAX).to_string();
            let request = super::native::NativeRequest::for_client(
                &query,
                timestamp,
                nonce,
                ace_swarm::engine_profile::identity(),
            );
            let mut hosts = HOSTS;
            hosts.shuffle(&mut rand::rng());
            let mut budget = MAX_RESPONSE_BYTES;
            let mut last_error = SearchError::Upstream;
            for host in hosts {
                let url = request.url(host);
                let raw =
                    tokio::time::timeout(HOST_DEADLINE, self.transport.fetch(&url, &mut budget))
                        .await
                        .unwrap_or(Err(SearchError::Timeout));
                let result = raw
                    .and_then(|raw| super::native::decode_response(&raw, &request.nonce))
                    .and_then(|expanded| normalize_response(&expanded, &query))
                    .and_then(|result| {
                        serde_json::to_vec(&result)
                            .map(Bytes::from)
                            .map_err(|_| SearchError::InvalidResponse)
                    });
                match result {
                    Ok(bytes) => return Ok(bytes),
                    Err(SearchError::TooLarge) => return Err(SearchError::TooLarge),
                    Err(error) => last_error = error,
                }
                if budget == 0 {
                    return Err(SearchError::TooLarge);
                }
            }
            Err(last_error)
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
#[cfg(test)]
pub(crate) mod tests;
