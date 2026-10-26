use super::*;
use ace_swarm::types::StreamMetadata;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const CID_A: &str = "0123456789abcdef0123456789abcdef01234567";
const CID_B: &str = "1123456789abcdef0123456789abcdef01234567";

fn info(marker: u8) -> StreamInfo {
    StreamInfo {
        infohash: [marker; 20],
        piece_length: 524_288,
        chunk_length: 16_384,
        trackers: Vec::new(),
        metadata: StreamMetadata::default(),
        sig_len: 0,
        source_pubkey: Vec::new(),
    }
}

fn waiters(cache: &CatalogResolutionCache, cid: &str) -> usize {
    let key = hex20(cid).unwrap();
    let state = cache.state.lock().unwrap();
    match state.entries.get(&key) {
        Some(Entry::Pending { waiters, .. }) => *waiters,
        _ => 0,
    }
}

fn pending_keys(cache: &CatalogResolutionCache) -> usize {
    cache
        .state
        .lock()
        .unwrap()
        .entries
        .values()
        .filter(|entry| matches!(entry, Entry::Pending { .. }))
        .count()
}

async fn wait_for_waiters(cache: &CatalogResolutionCache, cid: &str, expected: usize) -> bool {
    for _ in 0..10_000 {
        if waiters(cache, cid) == expected {
            return true;
        }
        tokio::task::yield_now().await;
    }
    false
}

async fn wait_for_pending(cache: &CatalogResolutionCache, expected: usize) -> bool {
    for _ in 0..10_000 {
        if pending_keys(cache) == expected {
            return true;
        }
        tokio::task::yield_now().await;
    }
    false
}

fn blocked_call(
    cache: Arc<CatalogResolutionCache>,
    cid: String,
    polls: Arc<AtomicUsize>,
    marker: u8,
) -> (
    JoinHandle<Result<StreamInfo, CatalogLookupError>>,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
) {
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        cache
            .resolve_with(&cid, async move {
                polls.fetch_add(1, Ordering::SeqCst);
                let _ = started_tx.send(());
                let _ = release_rx.await;
                Ok(info(marker))
            })
            .await
    });
    (task, started_rx, release_tx)
}

async fn started(receiver: oneshot::Receiver<()>) {
    tokio::time::timeout(Duration::from_secs(5), receiver)
        .await
        .expect("catalog fetch did not poll within watchdog")
        .expect("catalog fetch dropped before signaling its first poll");
}

#[tokio::test]
async fn catalog_resolution_same_key_coalesces_and_other_keys_progress() {
    let cache = Arc::new(CatalogResolutionCache::new(Duration::from_secs(300)));
    let polls = Arc::new(AtomicUsize::new(0));
    let (leader, leader_started, release) =
        blocked_call(cache.clone(), CID_A.into(), polls.clone(), 1);
    started(leader_started).await;

    let upper = CID_A.to_ascii_uppercase();
    let join_polls = polls.clone();
    let join_cache = cache.clone();
    let joiner = tokio::spawn(async move {
        join_cache
            .resolve_with(&upper, async move {
                join_polls.fetch_add(1, Ordering::SeqCst);
                Ok(info(9))
            })
            .await
    });
    let attached = wait_for_waiters(&cache, CID_A, 2).await;

    let distinct_polls = polls.clone();
    let distinct = cache
        .resolve_with(CID_B, async move {
            distinct_polls.fetch_add(1, Ordering::SeqCst);
            Ok(info(2))
        })
        .await;
    let _ = release.send(());
    let leader_result = leader.await.unwrap();
    let join_result = joiner.await.unwrap();

    assert!(
        attached,
        "differently-cased key did not join the pending lookup"
    );
    assert_eq!(distinct.unwrap().infohash, [2; 20]);
    assert_eq!(leader_result.unwrap().infohash, [1; 20]);
    assert_eq!(join_result.unwrap().infohash, [1; 20]);
    assert_eq!(polls.load(Ordering::SeqCst), 2, "one poll per distinct key");
}

#[tokio::test]
async fn catalog_resolution_errors_fan_out_without_retry() {
    let error_cache = Arc::new(CatalogResolutionCache::new(Duration::from_secs(300)));
    let error_polls = Arc::new(AtomicUsize::new(0));
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let calls = error_polls.clone();
    let first_cache = error_cache.clone();
    let first_error = tokio::spawn(async move {
        first_cache
            .resolve_with(CID_A, async move {
                calls.fetch_add(1, Ordering::SeqCst);
                let _ = started_tx.send(());
                let _ = release_rx.await;
                Err(ResolveError::Catalog("scripted catalog failure"))
            })
            .await
    });
    started(started_rx).await;
    let calls = error_polls.clone();
    let second_cache = error_cache.clone();
    let second_error = tokio::spawn(async move {
        second_cache
            .resolve_with(CID_A, async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(info(3))
            })
            .await
    });
    let error_attached = wait_for_waiters(&error_cache, CID_A, 2).await;
    let _ = release_tx.send(());
    let first_error = first_error.await.unwrap().unwrap_err();
    let second_error = second_error.await.unwrap().unwrap_err();
    let same_error = match (&first_error, &second_error) {
        (CatalogLookupError::Catalog(first), CatalogLookupError::Catalog(second)) => {
            Arc::ptr_eq(first, second)
        }
        _ => false,
    };
    let retry_calls = error_polls.clone();
    let retry = error_cache
        .resolve_with(CID_A, async move {
            retry_calls.fetch_add(1, Ordering::SeqCst);
            Ok(info(4))
        })
        .await;
    assert!(error_attached, "second failure caller was not attached");
    assert!(
        same_error,
        "waiters did not receive the same shared catalog error"
    );
    assert!(matches!(
        first_error,
        CatalogLookupError::Catalog(ref error) if **error == ResolveError::Catalog("scripted catalog failure")
    ));
    assert_eq!(retry.unwrap().infohash, [4; 20]);
    assert_eq!(
        error_polls.load(Ordering::SeqCst),
        2,
        "retry is explicit and single"
    );
}

#[tokio::test]
async fn catalog_resolution_cancel_leader_keeps_waiter() {
    let cache = Arc::new(CatalogResolutionCache::new(Duration::from_secs(300)));
    let polls = Arc::new(AtomicUsize::new(0));
    let (leader, leader_started, release) =
        blocked_call(cache.clone(), CID_A.into(), polls.clone(), 1);
    started(leader_started).await;
    let join_polls = polls.clone();
    let join_cache = cache.clone();
    let waiter = tokio::spawn(async move {
        join_cache
            .resolve_with(CID_A, async move {
                join_polls.fetch_add(1, Ordering::SeqCst);
                Ok(info(2))
            })
            .await
    });
    let attached = wait_for_waiters(&cache, CID_A, 2).await;
    leader.abort();
    let leader_was_cancelled = leader.await.unwrap_err().is_cancelled();
    let _ = release.send(());
    let result = waiter.await.unwrap().unwrap();
    assert!(attached && leader_was_cancelled);
    assert_eq!(result.infohash, [1; 20]);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn catalog_resolution_cancel_waiter_keeps_leader() {
    let cache = Arc::new(CatalogResolutionCache::new(Duration::from_secs(300)));
    let polls = Arc::new(AtomicUsize::new(0));
    let (leader, leader_started, release) =
        blocked_call(cache.clone(), CID_A.into(), polls.clone(), 1);
    started(leader_started).await;
    let join_polls = polls.clone();
    let join_cache = cache.clone();
    let waiter = tokio::spawn(async move {
        join_cache
            .resolve_with(CID_A, async move {
                join_polls.fetch_add(1, Ordering::SeqCst);
                Ok(info(2))
            })
            .await
    });
    let attached = wait_for_waiters(&cache, CID_A, 2).await;
    waiter.abort();
    let waiter_was_cancelled = waiter.await.unwrap_err().is_cancelled();
    let _ = release.send(());
    let result = leader.await.unwrap().unwrap();
    assert!(attached && waiter_was_cancelled);
    assert_eq!(result.infohash, [1; 20]);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
}

struct DropSignal(Option<oneshot::Sender<()>>);
impl Drop for DropSignal {
    fn drop(&mut self) {
        if let Some(signal) = self.0.take() {
            let _ = signal.send(());
        }
    }
}

#[tokio::test]
async fn catalog_resolution_cancel_last_drops_fetch_and_releases_slot() {
    let cache = Arc::new(CatalogResolutionCache::new(Duration::from_secs(300)));
    let polls = Arc::new(AtomicUsize::new(0));
    let (started_tx, started_rx) = oneshot::channel();
    let (drop_tx, dropped_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let fetch_drop = DropSignal(Some(drop_tx));
    let first_cache = cache.clone();
    let first_polls = polls.clone();
    let first = tokio::spawn(async move {
        first_cache
            .resolve_with(CID_A, async move {
                let _fetch_drop = fetch_drop;
                first_polls.fetch_add(1, Ordering::SeqCst);
                let _ = started_tx.send(());
                let _ = release_rx.await;
                Ok(info(1))
            })
            .await
    });
    started(started_rx).await;
    let second_cache = cache.clone();
    let second_polls = polls.clone();
    let second = tokio::spawn(async move {
        second_cache
            .resolve_with(CID_A, async move {
                second_polls.fetch_add(1, Ordering::SeqCst);
                Ok(info(2))
            })
            .await
    });
    let attached = wait_for_waiters(&cache, CID_A, 2).await;
    first.abort();
    second.abort();
    let first_cancelled = first.await.unwrap_err().is_cancelled();
    let second_cancelled = second.await.unwrap_err().is_cancelled();
    let fetch_dropped = tokio::time::timeout(Duration::from_secs(5), dropped_rx)
        .await
        .is_ok();
    let new_attempt = cache
        .resolve_with(CID_B, async { Ok(info(3)) })
        .await
        .unwrap();

    assert!(attached && first_cancelled && second_cancelled);
    assert!(
        fetch_dropped,
        "last waiter did not release the shared producer"
    );
    assert_eq!(pending_keys(&cache), 0);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(new_attempt.infohash, [3; 20]);
    drop(release_tx);
}

#[tokio::test]
async fn catalog_resolution_old_guard_cannot_remove_new_generation() {
    let cache = Arc::new(CatalogResolutionCache::new(Duration::from_secs(300)));
    let key = hex20(CID_A).unwrap();
    let old_generation = 4;
    let new_generation = 5;
    let fresh = info(5);
    let future = futures::future::ready(Ok(fresh.clone())).boxed().shared();
    {
        let mut state = cache.state.lock().unwrap();
        state.entries.insert(
            key,
            Entry::Pending {
                generation: new_generation,
                waiters: 1,
                future,
            },
        );
    }
    let stale_guard = WaiterGuard {
        state: Arc::clone(&cache.state),
        key,
        generation: old_generation,
    };
    drop(stale_guard);
    let generation_preserved_after_stale_drop = matches!(
        cache.state.lock().unwrap().entries.get(&key),
        Some(Entry::Pending { generation, waiters: 1, .. }) if *generation == new_generation
    );
    let result = cache
        .resolve_with(CID_A, async {
            panic!("stale guard removed the current generation")
        })
        .await
        .unwrap();
    assert_eq!(result, fresh);
    assert!(generation_preserved_after_stale_drop);
}

#[tokio::test(start_paused = true)]
async fn catalog_resolution_ttl_expires_at_300_seconds_without_sliding() {
    let cache = Arc::new(CatalogResolutionCache::new(Duration::from_secs(300)));
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let first_cache = cache.clone();
    let first = tokio::spawn(async move {
        first_cache
            .resolve_with(CID_A, async move {
                let _ = started_tx.send(());
                let _ = release_rx.await;
                Ok(info(1))
            })
            .await
    });
    started(started_rx).await;
    tokio::time::advance(Duration::from_secs(60)).await;
    let _ = release_tx.send(());
    assert_eq!(first.await.unwrap().unwrap().infohash, [1; 20]);

    tokio::time::advance(Duration::from_secs(299)).await;
    assert_eq!(cache.get(CID_A).unwrap().infohash, [1; 20]);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(
        cache.get(CID_A).is_none(),
        "300-second boundary must be a miss"
    );
    let polls = Arc::new(AtomicUsize::new(0));
    let count = polls.clone();
    let refreshed = cache
        .resolve_with(CID_A, async move {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(info(2))
        })
        .await
        .unwrap();
    assert_eq!(refreshed.infohash, [2; 20]);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn catalog_resolution_capacity_evicts_ready_not_pending() {
    let cache = CatalogResolutionCache::new(Duration::from_secs(300));
    for index in 0..MAX_ENTRIES {
        cache.put(&format!("{index:040x}"), info(index as u8));
    }
    let new_id = format!("{:040x}", MAX_ENTRIES);
    let inserted = cache
        .resolve_with(&new_id, async { Ok(info(9)) })
        .await
        .unwrap();
    assert_eq!(inserted.infohash, [9; 20]);
    assert!(cache.get(&format!("{:040x}", 0)).is_none());
    assert!(cache.get(&format!("{:040x}", 1)).is_some());

    let pending_cache = Arc::new(CatalogResolutionCache::new(Duration::from_secs(300)));
    let polls = Arc::new(AtomicUsize::new(0));
    let pending_id = format!("{:040x}", MAX_ENTRIES + 1);
    let (pending, pending_started, release) =
        blocked_call(pending_cache.clone(), pending_id.clone(), polls.clone(), 4);
    started(pending_started).await;
    for index in 0..(MAX_ENTRIES - 1) {
        pending_cache.put(&format!("{:040x}", index), info(index as u8));
    }
    let ready_hit = pending_cache.get(&format!("{:040x}", 1));
    let join_cache = pending_cache.clone();
    let join_polls = polls.clone();
    let joined_id = pending_id.clone();
    let joiner = tokio::spawn(async move {
        join_cache
            .resolve_with(&joined_id, async move {
                join_polls.fetch_add(1, Ordering::SeqCst);
                Ok(info(8))
            })
            .await
    });
    let full_cache_joined = wait_for_waiters(&pending_cache, &pending_id, 2).await;
    let full_miss_id = format!("{:040x}", MAX_ENTRIES + 2);
    let full_miss = pending_cache
        .resolve_with(&full_miss_id, async { Ok(info(8)) })
        .await
        .unwrap();
    let pending_preserved = pending_cache
        .state
        .lock()
        .unwrap()
        .entries
        .get(&hex20(&pending_id).unwrap())
        .is_some_and(|entry| matches!(entry, Entry::Pending { .. }));
    let _ = release.send(());
    let pending_result = pending.await.unwrap().unwrap();
    let join_result = joiner.await.unwrap().unwrap();
    assert_eq!(full_miss.infohash, [8; 20]);
    assert!(pending_preserved && ready_hit.is_some() && full_cache_joined);
    assert_eq!(pending_result.infohash, [4; 20]);
    assert_eq!(join_result.infohash, [4; 20]);
    assert_eq!(polls.load(Ordering::SeqCst), 1);

    let expired_cache = Arc::new(CatalogResolutionCache::new(Duration::from_secs(300)));
    expired_cache.put(CID_A, info(3));
    tokio::time::advance(Duration::from_secs(300)).await;
    let after_expiry = expired_cache
        .resolve_with(CID_B, async { Ok(info(5)) })
        .await
        .unwrap();
    assert_eq!(after_expiry.infohash, [5; 20]);
    assert!(expired_cache.get(CID_A).is_none());
}

#[tokio::test]
async fn catalog_resolution_pending_and_waiter_limits_do_not_fetch() {
    let cache = Arc::new(CatalogResolutionCache::new(Duration::from_secs(300)));
    let polls = Arc::new(AtomicUsize::new(0));
    let mut pending = Vec::new();
    let mut releases = Vec::new();
    let mut started_receivers = Vec::new();
    for index in 0..MAX_PENDING {
        let cid = format!("{:040x}", index + 1000);
        let (task, started_rx, release_tx) =
            blocked_call(cache.clone(), cid, polls.clone(), index as u8);
        pending.push(task);
        started_receivers.push(started_rx);
        releases.push(release_tx);
    }
    for receiver in started_receivers {
        started(receiver).await;
    }
    let pending_full = wait_for_pending(&cache, MAX_PENDING).await;
    let overflow_polls = Arc::new(AtomicUsize::new(0));
    let never = overflow_polls.clone();
    let overflow_id = format!("{:040x}", 9000);
    let overflow = cache
        .resolve_with(&overflow_id, async move {
            never.fetch_add(1, Ordering::SeqCst);
            Ok(info(90))
        })
        .await;
    assert!(matches!(overflow, Err(CatalogLookupError::Capacity)));
    assert_eq!(overflow_polls.load(Ordering::SeqCst), 0);
    let _ = releases
        .into_iter()
        .map(|release| release.send(()))
        .collect::<Vec<_>>();
    for task in pending {
        assert!(task.await.unwrap().is_ok());
    }
    assert!(pending_full);
    assert_eq!(polls.load(Ordering::SeqCst), MAX_PENDING);

    let waiter_cache = Arc::new(CatalogResolutionCache::new(Duration::from_secs(300)));
    let waiter_polls = Arc::new(AtomicUsize::new(0));
    let (leader, leader_started, release) =
        blocked_call(waiter_cache.clone(), CID_A.into(), waiter_polls.clone(), 7);
    started(leader_started).await;
    let mut joiners: Vec<JoinHandle<Result<StreamInfo, CatalogLookupError>>> = Vec::new();
    for _ in 0..(MAX_WAITERS - 1) {
        let cache = waiter_cache.clone();
        let polls = waiter_polls.clone();
        joiners.push(tokio::spawn(async move {
            cache
                .resolve_with(CID_A, async move {
                    polls.fetch_add(1, Ordering::SeqCst);
                    Ok(info(8))
                })
                .await
        }));
    }
    let all_attached = wait_for_waiters(&waiter_cache, CID_A, MAX_WAITERS).await;
    let denied_polls = Arc::new(AtomicUsize::new(0));
    let denied = denied_polls.clone();
    let overflow = waiter_cache
        .resolve_with(CID_A, async move {
            denied.fetch_add(1, Ordering::SeqCst);
            Ok(info(9))
        })
        .await;
    assert!(matches!(overflow, Err(CatalogLookupError::Capacity)));
    assert_eq!(denied_polls.load(Ordering::SeqCst), 0);

    let joiner = joiners.pop().expect("one waiter to cancel");
    joiner.abort();
    let cancelled = joiner.await.unwrap_err().is_cancelled();
    let extra_cache = waiter_cache.clone();
    let extra_polls = waiter_polls.clone();
    let extra = tokio::spawn(async move {
        extra_cache
            .resolve_with(CID_A, async move {
                extra_polls.fetch_add(1, Ordering::SeqCst);
                Ok(info(10))
            })
            .await
    });
    let reservation_restored = wait_for_waiters(&waiter_cache, CID_A, MAX_WAITERS).await;
    let _ = release.send(());
    let leader_result = leader.await.unwrap().unwrap();
    let extra_result = extra.await.unwrap().unwrap();
    let mut all_results_ok = true;
    for joiner in joiners {
        all_results_ok &= joiner.await.unwrap().is_ok();
    }
    assert!(all_attached && cancelled && reservation_restored);
    assert!(all_results_ok);
    assert_eq!(leader_result.infohash, [7; 20]);
    assert_eq!(extra_result.infohash, [7; 20]);
    assert_eq!(waiter_polls.load(Ordering::SeqCst), 1);
}
