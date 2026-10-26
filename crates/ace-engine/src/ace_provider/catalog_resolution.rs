//! Bounded positive catalog cache. Fetches are polled only by attached callers.

use ace_swarm::resolve::{hex20, ResolveError};
use ace_swarm::types::StreamInfo;
use futures::future::{BoxFuture, Shared};
use futures::FutureExt;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

const MAX_ENTRIES: usize = 256;
const MAX_PENDING: usize = 32;
const MAX_WAITERS: usize = 64;

#[derive(Clone, Debug)]
pub(super) enum CatalogLookupError {
    Catalog(Arc<ResolveError>),
    Capacity,
}

type CatalogAttempt = Shared<BoxFuture<'static, Result<StreamInfo, CatalogLookupError>>>;

enum Entry {
    Ready {
        info: StreamInfo,
        completed_at: Instant,
        insertion: u64,
    },
    Pending {
        generation: u64,
        waiters: usize,
        future: CatalogAttempt,
    },
}

#[derive(Default)]
struct State {
    entries: HashMap<[u8; 20], Entry>,
    next_generation: u64,
    next_insertion: u64,
}

impl State {
    fn prune_expired(&mut self, now: Instant, ttl: Duration) {
        self.entries.retain(|_, entry| match entry {
            Entry::Ready { completed_at, .. } => now.duration_since(*completed_at) < ttl,
            Entry::Pending { .. } => true,
        });
    }

    /// Call only for a new key after pruning. Pending attempts are never evicted.
    fn make_room(&mut self) -> bool {
        if self.entries.len() < MAX_ENTRIES {
            return true;
        }
        let oldest = self
            .entries
            .iter()
            .filter_map(|(key, entry)| match entry {
                Entry::Ready { insertion, .. } => Some((*key, *insertion)),
                Entry::Pending { .. } => None,
            })
            .min_by_key(|(_, insertion)| *insertion)
            .map(|(key, _)| key);
        if let Some(key) = oldest {
            self.entries.remove(&key);
            true
        } else {
            false
        }
    }

    fn insert_ready(&mut self, key: [u8; 20], info: StreamInfo, completed_at: Instant) {
        self.next_insertion = self
            .next_insertion
            .checked_add(1)
            .expect("catalog cache insertion overflow");
        self.entries.insert(
            key,
            Entry::Ready {
                info,
                completed_at,
                insertion: self.next_insertion,
            },
        );
    }
}

pub(super) struct CatalogResolutionCache {
    state: Arc<Mutex<State>>,
    ttl: Duration,
}

impl CatalogResolutionCache {
    pub(super) fn new(ttl: Duration) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            ttl,
        }
    }

    #[cfg(test)]
    pub(super) fn pending_waiters_for_test(&self, cid: &str) -> Option<usize> {
        let key = hex20(cid).ok()?;
        let state = self.state.lock().unwrap();
        match state.entries.get(&key) {
            Some(Entry::Pending { waiters, .. }) => Some(*waiters),
            _ => None,
        }
    }

    pub(super) fn get(&self, cid: &str) -> Option<StreamInfo> {
        let key = hex20(cid).ok()?;
        let mut state = self.state.lock().unwrap();
        match state.entries.get(&key) {
            Some(Entry::Ready {
                info, completed_at, ..
            }) if completed_at.elapsed() < self.ttl => Some(info.clone()),
            Some(Entry::Ready { .. }) => {
                state.entries.remove(&key);
                None
            }
            _ => None,
        }
    }

    pub(super) async fn resolve_with<F>(
        &self,
        cid: &str,
        fetch: F,
    ) -> Result<StreamInfo, CatalogLookupError>
    where
        F: Future<Output = Result<StreamInfo, ResolveError>> + Send + 'static,
    {
        let key = hex20(cid).map_err(|error| CatalogLookupError::Catalog(Arc::new(error)))?;
        let (future, _waiter) = {
            let mut state = self.state.lock().unwrap();
            match state.entries.get_mut(&key) {
                Some(Entry::Ready {
                    info, completed_at, ..
                }) if completed_at.elapsed() < self.ttl => return Ok(info.clone()),
                Some(Entry::Pending {
                    generation,
                    waiters,
                    future,
                }) => {
                    if *waiters >= MAX_WAITERS {
                        return Err(CatalogLookupError::Capacity);
                    }
                    *waiters += 1;
                    (
                        future.clone(),
                        WaiterGuard {
                            state: Arc::clone(&self.state),
                            key,
                            generation: *generation,
                        },
                    )
                }
                _ => {
                    state.prune_expired(Instant::now(), self.ttl);
                    let pending = state
                        .entries
                        .values()
                        .filter(|entry| matches!(entry, Entry::Pending { .. }))
                        .count();
                    if pending >= MAX_PENDING || !state.make_room() {
                        return Err(CatalogLookupError::Capacity);
                    }
                    state.next_generation = state
                        .next_generation
                        .checked_add(1)
                        .expect("catalog cache generation overflow");
                    let generation = state.next_generation;
                    // A weak publisher prevents State -> Shared -> State ownership cycles.
                    let publisher = Arc::downgrade(&self.state);
                    let future = async move {
                        let result = fetch
                            .await
                            .map_err(|error| CatalogLookupError::Catalog(Arc::new(error)));
                        let completed_at = Instant::now();
                        if let Some(state) = publisher.upgrade() {
                            let retired = {
                                let mut state = state.lock().unwrap();
                                if matches!(
                                    state.entries.get(&key),
                                    Some(Entry::Pending { generation: current, .. })
                                        if *current == generation
                                ) {
                                    let retired = state.entries.remove(&key);
                                    if let Ok(info) = &result {
                                        state.insert_ready(key, info.clone(), completed_at);
                                    }
                                    retired
                                } else {
                                    None
                                }
                            };
                            // Never drop a pending future while holding the state mutex.
                            drop(retired);
                        }
                        result
                    }
                    .boxed()
                    .shared();
                    state.entries.insert(
                        key,
                        Entry::Pending {
                            generation,
                            waiters: 1,
                            future: future.clone(),
                        },
                    );
                    (
                        future,
                        WaiterGuard {
                            state: Arc::clone(&self.state),
                            key,
                            generation,
                        },
                    )
                }
            }
        };
        // The guard lives across this await, but no mutex guard does. Other callers can
        // poll the same attempt even after the first caller is canceled.
        future.await
    }

    /// Trusted fixture seeding only. Production publication follows authenticated fetches.
    #[cfg(test)]
    pub(super) fn put(&self, cid: &str, info: StreamInfo) {
        let key = hex20(cid).expect("synthetic content-id");
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        state.prune_expired(now, self.ttl);
        if matches!(state.entries.get(&key), Some(Entry::Pending { .. })) {
            return;
        }
        if state.entries.contains_key(&key) || state.make_room() {
            state.insert_ready(key, info, now);
        }
    }
}

/// Each attached caller owns one reservation, independent of who first polled the fetch.
struct WaiterGuard {
    state: Arc<Mutex<State>>,
    key: [u8; 20],
    generation: u64,
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        let retired = {
            let mut state = self.state.lock().unwrap();
            match state.entries.get_mut(&self.key) {
                Some(Entry::Pending {
                    generation,
                    waiters,
                    ..
                }) if *generation == self.generation => {
                    *waiters -= 1;
                    if *waiters == 0 {
                        state.entries.remove(&self.key)
                    } else {
                        None
                    }
                }
                _ => None,
            }
        };
        drop(retired);
    }
}

#[cfg(test)]
mod tests;
