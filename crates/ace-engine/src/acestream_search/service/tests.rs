use super::*;
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicUsize, Ordering},
};

struct Fake {
    replies: Mutex<VecDeque<Result<Vec<u8>, SearchError>>>,
    calls: AtomicUsize,
    delay: Duration,
}
impl Transport for Fake {
    fn fetch<'a>(&'a self, url: &'a str, budget: &'a mut usize) -> FetchFuture<'a> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            let reply = self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Err(SearchError::Upstream))?;
            let reply = if serde_json::from_slice::<Value>(&reply)
                .is_ok_and(|v| v.get("results").is_some())
            {
                signed_reply(url, &reply)
            } else {
                reply
            };
            if reply.len() > *budget {
                return Err(SearchError::TooLarge);
            }
            *budget -= reply.len();
            Ok(reply)
        })
    }
}
fn signed_reply(url: &str, inner: &[u8]) -> Vec<u8> {
    use base64ct::{Base64, Encoding};
    use sha1::{Digest, Sha1};
    let params: std::collections::BTreeMap<_, _> =
        form_urlencoded::parse(url.split_once('?').unwrap().1.as_bytes())
            .into_owned()
            .collect();
    assert_eq!(params["_p"], "outpace");
    assert!(params["_r"].parse::<i64>().is_ok_and(|n| n > 0));
    let data = Base64::encode_string(inner);
    let mut hash = Sha1::new();
    hash.update(data.as_bytes());
    hash.update(params["_r"].as_bytes());
    hash.update(super::super::native::RESPONSE_KEY);
    serde_json::to_vec(&json!({"data":data,"sign":hex::encode(hash.finalize())})).unwrap()
}

fn settings() -> SearchSettings {
    SearchSettings { enabled: true }
}
fn service(
    replies: Vec<Result<Vec<u8>, SearchError>>,
    delay: Duration,
) -> (SearchService, Arc<Fake>) {
    let fake = Arc::new(Fake {
        replies: Mutex::new(replies.into()),
        calls: AtomicUsize::new(0),
        delay,
    });
    (
        SearchService {
            transport: fake.clone(),
            state: Mutex::new(State {
                entries: VecDeque::new(),
                bytes: 0,
                next_start: Instant::now(),
            }),
            permits: Semaphore::new(2),
        },
        fake,
    )
}
fn empty() -> Result<Vec<u8>, SearchError> {
    Ok(br#"{"total":0,"time":0.0,"results":[]}"#.to_vec())
}
fn query() -> SearchQuery {
    SearchQuery::parse(vec![]).unwrap()
}

#[test]
fn opt_in_requires_no_operator_key_and_private_mode_stays_blocked() {
    let settings = SearchSettings { enabled: true };
    assert!(settings.validate(false).is_ok());
    assert!(settings.validate(true).is_err());
    assert!(SearchService::new(SearchSettings::default()).is_err());
}
#[tokio::test(start_paused = true)]
async fn validated_empty_results_are_cached_per_page_and_expire() {
    let (service, fake) = service(vec![empty(), empty(), empty()], Duration::ZERO);
    let first = service.search(query()).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&first).unwrap(),
        json!({"result":{"total":0,"results":[]},"error":null})
    );
    assert_eq!(service.search(query()).await.unwrap(), first);
    assert_eq!(fake.calls.load(Ordering::SeqCst), 1);
    let mut next = query();
    next.page = 1;
    assert_eq!(
        service.search(next.clone()).await,
        Err(SearchError::RateLimited)
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    service.search(next).await.unwrap();
    tokio::time::advance(Duration::from_secs(30)).await;
    service.search(query()).await.unwrap();
    assert_eq!(fake.calls.load(Ordering::SeqCst), 3);
}
#[tokio::test(start_paused = true)]
async fn errors_are_not_cached_as_empty_and_impose_cooldown() {
    for reply in [
        Err(SearchError::Upstream),
        Ok(b"invalid JSON".to_vec()),
        Ok(vec![b' '; MAX_RESPONSE_BYTES + 1]),
    ] {
        let mut replies = vec![reply.clone(); 3];
        let attempts = if reply.as_ref().is_ok_and(|r| r.len() > MAX_RESPONSE_BYTES) {
            1
        } else {
            3
        };
        if attempts == 1 {
            replies.truncate(1);
        }
        replies.push(empty());
        let (service, fake) = service(replies, Duration::ZERO);
        assert!(service.search(query()).await.is_err());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(service.search(query()).await, Err(SearchError::RateLimited));
        tokio::time::advance(Duration::from_secs(30)).await;
        service.search(query()).await.unwrap();
        assert_eq!(fake.calls.load(Ordering::SeqCst), attempts + 1);
    }
}
#[tokio::test(start_paused = true)]
async fn timeout_and_dropping_requests_release_admission() {
    let (service, _) = service(vec![empty()], Duration::from_secs(60));
    assert_eq!(service.search(query()).await, Err(SearchError::Timeout));
    assert_eq!(service.permits.available_permits(), 2);
    tokio::time::advance(Duration::from_secs(30)).await;
    let mut future = Box::pin(service.search(query()));
    tokio::select! { result=&mut future => panic!("unexpected {result:?}"), _=tokio::time::sleep(Duration::from_millis(1)) => {} }
    assert_eq!(service.permits.available_permits(), 1);
    drop(future);
    assert_eq!(service.permits.available_permits(), 2);
}
#[tokio::test(start_paused = true)]
async fn no_more_than_two_requests_can_be_in_flight() {
    let (service, fake) = service(vec![empty(), empty()], Duration::from_secs(5));
    let mut a = Box::pin(service.search(query()));
    tokio::select! { result=&mut a=>panic!("unexpected {result:?}"), _=tokio::time::sleep(Duration::from_secs(1))=>{} }
    let mut q = query();
    q.page = 1;
    let mut b = Box::pin(service.search(q));
    tokio::select! { result=&mut b=>panic!("unexpected {result:?}"), _=tokio::time::sleep(Duration::from_secs(1))=>{} }
    let mut q = query();
    q.page = 2;
    assert_eq!(service.search(q).await, Err(SearchError::Busy));
    assert_eq!(fake.calls.load(Ordering::SeqCst), 2);
    drop((a, b));
    assert_eq!(service.permits.available_permits(), 2);
}
#[tokio::test(start_paused = true)]
async fn cache_entry_and_byte_budgets_evict_old_results() {
    let (service, _) = service((0..34).map(|_| empty()).collect(), Duration::ZERO);
    for page in 0..34 {
        let mut q = query();
        q.page = page;
        service.search(q).await.unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
    }
    assert!(service.state.lock().unwrap().entries.len() <= 32);
    // Populate retained values near the byte ceiling through the real insertion path.
    let mut state = service.state.lock().unwrap();
    state.entries.clear();
    state.bytes = 0;
    for page in 0..10 {
        let mut q = query();
        q.page = page;
        state.insert(q, Bytes::from(vec![b' '; MAX_RESPONSE_BYTES]));
    }
    assert!(state.bytes <= 8 * MAX_RESPONSE_BYTES);
    assert!(state.entries.len() <= 8);
}

pub(crate) fn offline_service(reply: Result<Vec<u8>, SearchError>) -> Arc<SearchService> {
    Arc::new(service(vec![reply], Duration::ZERO).0)
}

/// Discovery-only probe; no source registry, account or operator API key.
#[tokio::test]
#[ignore = "requires explicit OUTPACE_TEST_NATIVE_SEARCH=1 and public HTTP access"]
async fn live_search_uses_guarded_production_transport_without_publishing_metadata() {
    assert_eq!(
        std::env::var("OUTPACE_TEST_NATIVE_SEARCH").as_deref(),
        Ok("1")
    );
    let service = SearchService::new(settings()).unwrap();
    let q = SearchQuery::parse(vec![
        ("query".into(), "outpace_synthetic_search_probe_406".into()),
        ("page_size".into(), "1".into()),
    ])
    .unwrap();
    let response = service
        .search(q)
        .await
        .expect("guarded signed native request must succeed");
    let response: Value = serde_json::from_slice(&response).unwrap();
    assert!(response["error"].is_null());
    assert!(response["result"]["results"].as_array().unwrap().len() <= 1);
}

#[tokio::test(start_paused = true)]
async fn malformed_first_host_fails_over_to_a_valid_signed_page() {
    let (service, fake) = service(vec![Ok(b"unsigned".to_vec()), empty()], Duration::ZERO);
    assert!(service.search(query()).await.is_ok());
    assert_eq!(fake.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn all_three_bad_hosts_fail_without_caching_an_empty_page() {
    let (service, fake) = service(vec![Ok(b"unsigned".to_vec()); 3], Duration::ZERO);
    assert_eq!(
        service.search(query()).await,
        Err(SearchError::InvalidResponse)
    );
    assert_eq!(fake.calls.load(Ordering::SeqCst), 3);
    assert!(service.state.lock().unwrap().entries.is_empty());
}

#[tokio::test(start_paused = true)]
async fn native_host_timeouts_are_bounded_and_fail_over() {
    let (service, fake) = service(vec![empty(); 3], Duration::from_secs(60));
    let start = Instant::now();
    assert_eq!(service.search(query()).await, Err(SearchError::Timeout));
    assert_eq!(fake.calls.load(Ordering::SeqCst), 3);
    assert!(start.elapsed() <= Duration::from_secs(10));
    assert_eq!(service.permits.available_permits(), 2);
}

#[tokio::test(start_paused = true)]
async fn failover_shares_one_wire_byte_budget_and_stops_at_the_limit() {
    let (service, fake) = service(
        vec![Ok(vec![b' '; 600_000]), Ok(vec![b' '; 600_000]), empty()],
        Duration::ZERO,
    );
    assert_eq!(service.search(query()).await, Err(SearchError::TooLarge));
    assert_eq!(fake.calls.load(Ordering::SeqCst), 2);
    assert!(service.state.lock().unwrap().entries.is_empty());
}
