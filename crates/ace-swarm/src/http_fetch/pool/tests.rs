use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

fn key(port: u16) -> Key {
    Key {
        authority: Authority {
            allow_non_global: false,
            scheme: "http".into(),
            host: "example.invalid".into(),
            port,
        },
        addr: SocketAddr::from(([127, 0, 0, 1], port)),
        connect_timeout: Duration::from_secs(1),
    }
}

fn build(counter: &AtomicUsize) -> Result<reqwest::Client, FetchError> {
    counter.fetch_add(1, Ordering::SeqCst);
    let _ = rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .map_err(|_| FetchError::ClientBuildFailed)
}

#[test]
fn bounded_pool_evicts_lru_and_expires_without_extending_fixed_lifetime() {
    let pool = ResourceClientPool::default();
    let builds = AtomicUsize::new(0);
    let start = Instant::now();
    for port in 1..=MAX_CLIENTS as u16 {
        pool.client_at(
            key(port),
            start + Duration::from_millis(port as u64),
            || build(&builds),
        )
        .unwrap();
    }
    pool.client_at(key(1), start + Duration::from_secs(1), || build(&builds))
        .unwrap();
    pool.client_at(key(100), start + Duration::from_secs(2), || build(&builds))
        .unwrap();
    {
        let entries = pool.inner.lock().unwrap();
        assert_eq!(entries.len(), MAX_CLIENTS);
        assert!(entries.contains_key(&key(1)));
        assert!(!entries.contains_key(&key(2)));
    }
    assert_eq!(builds.load(Ordering::SeqCst), MAX_CLIENTS + 1);
    pool.client_at(key(1), start + Duration::from_secs(59), || build(&builds))
        .unwrap();
    assert_eq!(builds.load(Ordering::SeqCst), MAX_CLIENTS + 1);
    pool.client_at(
        key(1),
        start + CLIENT_LIFETIME + Duration::from_secs(1),
        || build(&builds),
    )
    .unwrap();
    assert_eq!(builds.load(Ordering::SeqCst), MAX_CLIENTS + 2);
    assert_eq!(pool.inner.lock().unwrap().len(), 2);
}

#[test]
fn changed_pin_is_retired_but_policy_scheme_authority_and_limits_remain_separate() {
    let pool = ResourceClientPool::default();
    let builds = AtomicUsize::new(0);
    let now = Instant::now();
    let original = key(80);
    pool.client_at(original.clone(), now, || build(&builds))
        .unwrap();
    let mut other_pin = original.clone();
    other_pin.addr.set_ip(IpAddr::from([127, 0, 0, 2]));
    pool.client_at(other_pin.clone(), now, || build(&builds))
        .unwrap();
    assert!(!pool.inner.lock().unwrap().contains_key(&original));
    pool.client_at(original.clone(), now, || build(&builds))
        .unwrap();
    assert!(!pool.inner.lock().unwrap().contains_key(&other_pin));
    assert_eq!(builds.load(Ordering::SeqCst), 3);
    let mut variants = Vec::new();
    let mut variant = original.clone();
    variant.authority.allow_non_global = true;
    variants.push(variant);
    let mut variant = original.clone();
    variant.authority.scheme = "https".into();
    variants.push(variant);
    let mut variant = original.clone();
    variant.authority.host = "other.invalid".into();
    variants.push(variant);
    let mut variant = original.clone();
    variant.authority.port = 81;
    variants.push(variant);
    let mut variant = original.clone();
    variant.connect_timeout = Duration::from_secs(2);
    variants.push(variant);
    for variant in variants {
        pool.client_at(variant, now, || build(&builds)).unwrap();
    }
    assert_eq!(builds.load(Ordering::SeqCst), 8);
    assert_eq!(pool.inner.lock().unwrap().len(), 6);
}

#[test]
fn signed_paths_and_queries_are_not_part_of_pool_keys() {
    let pool = ResourceClientPool::default();
    let builds = AtomicUsize::new(0);
    for url in [
        "http://example.invalid/a?token=first",
        "http://example.invalid/b?token=second",
    ] {
        let url = FetchUrl::parse_source(url, FetchPolicy::default()).unwrap();
        pool.client(
            &url,
            SocketAddr::from(([127, 0, 0, 1], 80)),
            FetchPolicy::default(),
            Duration::from_secs(1),
            || build(&builds),
        )
        .unwrap();
    }
    assert_eq!(builds.load(Ordering::SeqCst), 1);
}

#[test]
fn concurrent_first_use_constructs_only_one_client_and_map_stays_bounded() {
    let pool = ResourceClientPool::default();
    let builds = AtomicUsize::new(0);
    let barrier = std::sync::Barrier::new(8);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                barrier.wait();
                pool.client_at(key(80), Instant::now(), || build(&builds))
                    .unwrap();
            });
        }
    });
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    std::thread::scope(|scope| {
        for worker in 0..8 {
            let pool = &pool;
            let builds = &builds;
            scope.spawn(move || {
                for index in 0..8 {
                    pool.client_at(key(1000 + worker * 8 + index), Instant::now(), || {
                        build(builds)
                    })
                    .unwrap();
                    assert!(pool.inner.lock().unwrap().len() <= MAX_CLIENTS);
                }
            });
        }
    });
}
