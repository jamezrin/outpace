//! Bounded connection reuse after each request has independently passed DNS policy.
use super::*;
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::time::Instant;

const MAX_CLIENTS: usize = 16;
const CLIENT_LIFETIME: Duration = Duration::from_secs(60);
pub(super) const IDLE_LIFETIME: Duration = Duration::from_secs(15);

/// An instance-owned pool for finite resources. This stores clients, never DNS answers
/// or response bodies. The resource entry points resolve and validate every hop first.
/// Continuous sources and the unpooled free functions do not use this pool.
#[derive(Clone, Default)]
pub struct ResourceClientPool {
    inner: Arc<Mutex<HashMap<Key, Entry>>>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct Authority {
    allow_non_global: bool,
    scheme: String,
    host: String,
    port: u16,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct Key {
    authority: Authority,
    addr: SocketAddr,
    connect_timeout: Duration,
}

struct Entry {
    client: reqwest::Client,
    created: Instant,
    used: Instant,
}

impl ResourceClientPool {
    pub(super) fn client(
        &self,
        url: &FetchUrl,
        addr: SocketAddr,
        policy: FetchPolicy,
        connect_timeout: Duration,
        build: impl FnOnce() -> Result<reqwest::Client, FetchError>,
    ) -> Result<reqwest::Client, FetchError> {
        let key = Key {
            authority: Authority {
                allow_non_global: policy.allow_non_global,
                scheme: url.0.scheme().to_owned(),
                host: normalized_host(&url.0)?.to_owned(),
                port: url.0.port_or_known_default().ok_or(FetchError::NoPort)?,
            },
            addr,
            connect_timeout,
        };
        self.client_at(key, Instant::now(), build)
    }

    fn client_at(
        &self,
        key: Key,
        now: Instant,
        build: impl FnOnce() -> Result<reqwest::Client, FetchError>,
    ) -> Result<reqwest::Client, FetchError> {
        let mut entries = self
            .inner
            .lock()
            .map_err(|_| FetchError::ClientBuildFailed)?;
        entries.retain(|existing, entry| {
            now.saturating_duration_since(entry.created) < CLIENT_LIFETIME
                && (existing.authority != key.authority || existing.addr == key.addr)
        });
        if let Some(entry) = entries.get_mut(&key) {
            entry.used = now;
            return Ok(entry.client.clone());
        }
        // Construction is synchronous and bounded; holding the short map lock avoids
        // concurrent first requests creating separate pools for the same authority.
        // No DNS, network I/O, or await occurs while the lock is held.
        let client = build()?;
        if entries.len() >= MAX_CLIENTS {
            if let Some(oldest) = entries
                .iter()
                .min_by_key(|(_, entry)| entry.used)
                .map(|(key, _)| key.clone())
            {
                entries.remove(&oldest);
            }
        }
        entries.insert(
            key,
            Entry {
                client: client.clone(),
                created: now,
                used: now,
            },
        );
        Ok(client)
    }
}

#[cfg(test)]
mod tests;
