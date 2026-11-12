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
    fn fetch<'a>(&'a self, _query: &'a SearchQuery, _key: &'a str) -> FetchFuture<'a> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            self.replies
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Err(SearchError::Upstream))
        })
    }
}
fn settings() -> SearchSettings {
    SearchSettings::from_env(true, Ok("synthetic-secret".into())).unwrap()
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
            settings: settings(),
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
    Ok(br#"{"total":0,"results":[]}"#.to_vec())
}
fn query() -> SearchQuery {
    SearchQuery::parse(vec![]).unwrap()
}

#[test]
fn key_is_required_only_for_opt_in_and_never_debugged_or_serialized() {
    let absent = Err(std::env::VarError::NotPresent);
    assert!(SearchSettings::from_env(false, absent.clone())
        .unwrap()
        .validate(false)
        .is_ok());
    assert!(SearchSettings::from_env(true, absent).is_err());
    for key in ["", "contains spaces", "\n", &"x".repeat(513)] {
        assert!(SearchSettings::from_env(true, Ok(key.into())).is_err());
    }
    let settings = settings();
    assert!(!format!("{settings:?}").contains("synthetic-secret"));
    assert!(!serde_json::to_string(&settings)
        .unwrap()
        .contains("synthetic-secret"));
    assert!(settings.validate(true).is_err());
    assert!(settings.validate(false).is_ok());
}
#[test]
fn fixed_request_encodes_each_value_and_disables_epg() {
    let query = SearchQuery::parse(vec![
        ("query".into(), "a&key=+é".into()),
        ("page".into(), "2".into()),
        ("category".into(), "tv & radio".into()),
    ])
    .unwrap();
    let url = request_url(&query, "key&value");
    assert!(url.starts_with("https://api.acestream.me/?"));
    assert!(url.contains("query=a%26key%3D%2B%C3%A9"));
    assert!(url.contains("api_key=key%26value"));
    assert!(url.contains("category=tv+%26+radio"));
    assert!(url.contains("group_by_channels=1"));
    assert!(url.contains("show_epg=0"));
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
        let (service, fake) = service(vec![reply, empty()], Duration::ZERO);
        assert!(service.search(query()).await.is_err());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(service.search(query()).await, Err(SearchError::RateLimited));
        tokio::time::advance(Duration::from_secs(30)).await;
        service.search(query()).await.unwrap();
        assert_eq!(fake.calls.load(Ordering::SeqCst), 2);
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

/// This probes discovery only. It requires an operator-provided key, never a source registry.
#[tokio::test]
#[ignore = "requires OUTPACE_TEST_SEARCH_API_KEY and public HTTPS access"]
async fn live_search_uses_guarded_production_transport_without_publishing_metadata() {
    let key =
        std::env::var("OUTPACE_TEST_SEARCH_API_KEY").expect("set OUTPACE_TEST_SEARCH_API_KEY");
    let service = SearchService::new(SearchSettings::from_env(true, Ok(key)).unwrap()).unwrap();
    let q = SearchQuery::parse(vec![
        ("query".into(), "outpace_synthetic_search_probe_406".into()),
        ("page_size".into(), "1".into()),
    ])
    .unwrap();
    let response = service
        .search(q)
        .await
        .expect("guarded search request must succeed");
    let response: Value = serde_json::from_slice(&response).unwrap();
    assert!(response["error"].is_null());
    assert!(response["result"]["results"].as_array().unwrap().len() <= 1);
}
