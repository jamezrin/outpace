use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

struct KeepAliveOrigin {
    addr: SocketAddr,
    connections: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<String>>>,
    headers_started: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for KeepAliveOrigin {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl KeepAliveOrigin {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connections = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let headers_started = Arc::new(Notify::new());
        let task_connections = connections.clone();
        let task_requests = requests.clone();
        let task_started = headers_started.clone();
        let task = tokio::spawn(async move {
            let mut readers = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.unwrap();
                        task_connections.fetch_add(1, Ordering::SeqCst);
                        let requests = task_requests.clone();
                        let started = task_started.clone();
                        readers.spawn(async move {
                            for _ in 0..32 {
                                let mut request = Vec::new();
                                while !request.ends_with(b"\r\n\r\n") {
                                    let mut byte = [0];
                                    if request.len() >= 32768 || socket.read_exact(&mut byte).await.is_err() {
                                        return;
                                    }
                                    request.extend_from_slice(&byte);
                                }
                                let request = String::from_utf8(request).unwrap();
                                let path = request.split_whitespace().nth(1).unwrap().to_owned();
                                requests.lock().unwrap().push(request);
                                if path == "/slow-headers" {
                                    started.notify_one();
                                    tokio::time::sleep(Duration::from_secs(30)).await;
                                }
                                let response = match path.as_str() {
                                    "/first?sig=one" => "HTTP/1.1 200 OK\r\nContent-Length: 3\r\nSet-Cookie: synthetic=one; Path=/\r\n\r\nabc",
                                    "/redirect" => "HTTP/1.1 302 Found\r\nContent-Length: 0\r\nLocation: /ok\r\n\r\n",
                                    "/slow-body" | "/cut" => "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\na",
                                    "/close" => "HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc",
                                    _ => "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc",
                                };
                                if socket.write_all(response.as_bytes()).await.is_err() {
                                    return;
                                }
                                if path == "/cut" || path == "/close" {
                                    return;
                                }
                                if path == "/slow-body" {
                                    tokio::time::sleep(Duration::from_secs(30)).await;
                                    return;
                                }
                            }
                        });
                    }
                    _ = readers.join_next(), if !readers.is_empty() => {}
                }
            }
        });
        Self {
            addr,
            connections,
            requests,
            headers_started,
            task,
        }
    }

    fn url(&self, path: &str) -> FetchUrl {
        FetchUrl::parse_source(&format!("http://{}{path}", self.addr), policy()).unwrap()
    }
}

fn policy() -> FetchPolicy {
    FetchPolicy {
        allow_non_global: true,
    }
}

async fn drain(mut response: ResourceResponse) -> Result<Vec<u8>, FetchError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.body.next_chunk().await? {
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

async fn with_dns(
    pool: &ResourceClientPool,
    url: &FetchUrl,
    hooks: test_support::TestHooks,
) -> Result<ResourceResponse, FetchError> {
    let (_tx, cancel) = watch::channel(false);
    open_resource_inner(
        url,
        policy(),
        limits(),
        None,
        cancel,
        ResourceOptions {
            pool: Some(pool.clone()),
            hooks: Some(hooks),
            ..Default::default()
        },
    )
    .await
}

#[tokio::test]
async fn finite_pool_reuses_one_socket_while_free_function_stays_unpooled() {
    let origin = KeepAliveOrigin::start().await;
    let pool = ResourceClientPool::default();
    let (_tx, cancel) = watch::channel(false);
    for path in ["/first?sig=one", "/second?sig=two"] {
        let response = pool
            .open_resource(&origin.url(path), policy(), limits(), None, cancel.clone())
            .await
            .unwrap();
        assert_eq!(drain(response).await.unwrap(), b"abc");
    }
    assert_eq!(origin.connections.load(Ordering::SeqCst), 1);
    for _ in 0..2 {
        let response = open_resource(
            &origin.url("/free"),
            policy(),
            limits(),
            None,
            cancel.clone(),
        )
        .await
        .unwrap();
        assert_eq!(drain(response).await.unwrap(), b"abc");
    }
    assert_eq!(origin.connections.load(Ordering::SeqCst), 3);
    let requests = origin.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert!(requests[0].starts_with("GET /first?sig=one HTTP/1.1"));
    assert!(requests[1].starts_with("GET /second?sig=two HTTP/1.1"));
    for request in requests.iter() {
        let request = request.to_ascii_lowercase();
        assert!(request.contains("accept-encoding: identity\r\n"));
        assert!(
            !request.contains("cookie:")
                && !request.contains("authorization:")
                && !request.contains("referer:")
        );
    }
}

#[tokio::test]
async fn fresh_dns_retires_old_pins_and_never_falls_back_after_blocked_or_failed_lookup() {
    let first = KeepAliveOrigin::start().await;
    let second = KeepAliveOrigin::start().await;
    let pool = ResourceClientPool::default();
    // No explicit port: the test resolver can choose distinct local endpoints.
    let url = FetchUrl::parse_source("http://pooled.invalid/ok", policy()).unwrap();
    let hooks = test_support::TestHooks::new()
        .with_dns_answers(vec![first.addr])
        .then_dns_answers(vec![first.addr])
        .then_dns_answers(vec![second.addr])
        .then_dns_answers(vec![first.addr])
        .then_dns_answers(vec!["169.254.169.254:80".parse().unwrap()]);
    for _ in 0..4 {
        assert_eq!(
            drain(with_dns(&pool, &url, hooks.clone()).await.unwrap())
                .await
                .unwrap(),
            b"abc"
        );
    }
    assert_eq!(first.connections.load(Ordering::SeqCst), 2);
    assert_eq!(second.connections.load(Ordering::SeqCst), 1);
    let (_tx, cancel) = watch::channel(false);
    let strict = open_resource_inner(
        &url,
        FetchPolicy::default(),
        limits(),
        None,
        cancel,
        ResourceOptions {
            pool: Some(pool.clone()),
            hooks: Some(test_support::TestHooks::new().with_dns_answers(vec![first.addr])),
            ..Default::default()
        },
    )
    .await;
    assert!(matches!(strict, Err(FetchError::AddressBlocked)));
    assert!(matches!(
        with_dns(&pool, &url, hooks.clone()).await,
        Err(FetchError::AddressBlocked)
    ));
    assert!(matches!(
        with_dns(&pool, &url, hooks).await,
        Err(FetchError::DnsFailed)
    ));
    assert_eq!(first.requests.lock().unwrap().len(), 3);
    assert_eq!(second.requests.lock().unwrap().len(), 1);
    for origin in [&first, &second] {
        assert!(origin.requests.lock().unwrap().iter().all(|request| request
            .to_ascii_lowercase()
            .contains("host: pooled.invalid\r\n")));
    }
}

#[tokio::test]
async fn pooled_authorities_are_separate_even_when_the_endpoint_matches() {
    let origin = KeepAliveOrigin::start().await;
    let pool = ResourceClientPool::default();
    for host in ["one.invalid", "two.invalid", "one.invalid"] {
        let url = FetchUrl::parse_source(&format!("http://{host}/ok"), policy()).unwrap();
        let hooks = test_support::TestHooks::new().with_dns_answers(vec![origin.addr]);
        assert_eq!(
            drain(with_dns(&pool, &url, hooks).await.unwrap())
                .await
                .unwrap(),
            b"abc"
        );
    }
    assert_eq!(origin.connections.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn every_redirect_hop_resolves_again_before_pool_lookup() {
    let first = KeepAliveOrigin::start().await;
    let second = KeepAliveOrigin::start().await;
    let pool = ResourceClientPool::default();
    let url = FetchUrl::parse_source("http://redirect.invalid/redirect", policy()).unwrap();
    let hooks = test_support::TestHooks::new()
        .with_dns_answers(vec![first.addr])
        .then_dns_answers(vec![second.addr]);
    let response = with_dns(&pool, &url, hooks).await.unwrap();
    assert_eq!(response.final_url.0.path(), "/ok");
    assert_eq!(drain(response).await.unwrap(), b"abc");
    assert_eq!(first.requests.lock().unwrap().len(), 1);
    assert_eq!(second.requests.lock().unwrap().len(), 1);
    let blocked = test_support::TestHooks::new()
        .with_dns_answers(vec![first.addr])
        .then_dns_answers(vec!["169.254.169.254:80".parse().unwrap()]);
    assert!(matches!(
        with_dns(&pool, &url, blocked).await,
        Err(FetchError::AddressBlocked)
    ));
    assert_eq!(second.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn cancellation_and_deadlines_survive_reuse_and_release_partial_connections() {
    let origin = KeepAliveOrigin::start().await;
    let pool = ResourceClientPool::default();
    let (_tx, cancel) = watch::channel(false);
    let response = pool
        .open_resource(&origin.url("/ok"), policy(), limits(), None, cancel.clone())
        .await
        .unwrap();
    assert_eq!(drain(response).await.unwrap(), b"abc");

    let (tx, rx) = watch::channel(false);
    let mut response = pool
        .open_resource(&origin.url("/slow-body"), policy(), limits(), None, rx)
        .await
        .unwrap();
    assert_eq!(response.body.next_chunk().await.unwrap().unwrap(), b"a"[..]);
    tx.send(true).unwrap();
    assert_eq!(
        response.body.next_chunk().await.unwrap_err(),
        FetchError::Cancelled
    );
    drop(response);

    let short = FetchLimits {
        headers_timeout: Duration::from_millis(250),
        ..limits()
    };
    let response = pool
        .open_resource(
            &origin.url("/slow-body"),
            policy(),
            short,
            None,
            cancel.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(
        drain(response).await,
        Err(FetchError::BodyTimeout)
    ));

    let (tx, rx) = watch::channel(false);
    let open_pool = pool.clone();
    let url = origin.url("/slow-headers");
    let open = tokio::spawn(async move {
        open_pool
            .open_resource(&url, policy(), limits(), None, rx)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), origin.headers_started.notified())
        .await
        .unwrap();
    tx.send(true).unwrap();
    assert!(matches!(open.await.unwrap(), Err(FetchError::Cancelled)));

    assert!(matches!(
        pool.open_resource(
            &origin.url("/slow-headers"),
            policy(),
            short,
            None,
            cancel.clone()
        )
        .await,
        Err(FetchError::HeadersTimeout)
    ));
    let response = pool
        .open_resource(&origin.url("/ok"), policy(), limits(), None, cancel)
        .await
        .unwrap();
    assert_eq!(drain(response).await.unwrap(), b"abc");
}

#[tokio::test]
async fn pending_fresh_dns_can_be_cancelled_despite_a_warm_connection() {
    let origin = KeepAliveOrigin::start().await;
    let pool = ResourceClientPool::default();
    let url = FetchUrl::parse_source("http://pending.invalid/ok", policy()).unwrap();
    let hooks = test_support::TestHooks::new().with_dns_answers(vec![origin.addr]);
    assert_eq!(
        drain(with_dns(&pool, &url, hooks).await.unwrap())
            .await
            .unwrap(),
        b"abc"
    );
    let (tx, cancel) = watch::channel(false);
    let request = open_resource_inner(
        &url,
        policy(),
        limits(),
        None,
        cancel,
        ResourceOptions {
            pool: Some(pool),
            hooks: Some(test_support::TestHooks::new().with_pending_dns()),
            ..Default::default()
        },
    );
    let (result, ()) = tokio::join!(request, async {
        tokio::task::yield_now().await;
        tx.send(true).unwrap();
    });
    assert!(matches!(result, Err(FetchError::Cancelled)));
    assert_eq!(origin.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn closed_or_truncated_responses_do_not_poison_subsequent_requests_or_retry() {
    let origin = KeepAliveOrigin::start().await;
    let pool = ResourceClientPool::default();
    let (_tx, cancel) = watch::channel(false);
    for path in ["/close", "/ok"] {
        let response = pool
            .open_resource(&origin.url(path), policy(), limits(), None, cancel.clone())
            .await
            .unwrap();
        assert_eq!(drain(response).await.unwrap(), b"abc");
    }
    assert_eq!(origin.connections.load(Ordering::SeqCst), 2);
    let response = pool
        .open_resource(
            &origin.url("/cut"),
            policy(),
            limits(),
            None,
            cancel.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(drain(response).await, Err(FetchError::BodyFailed)));
    let response = pool
        .open_resource(&origin.url("/ok"), policy(), limits(), None, cancel)
        .await
        .unwrap();
    assert_eq!(drain(response).await.unwrap(), b"abc");
    assert_eq!(
        origin
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.starts_with("GET /cut "))
            .count(),
        1
    );
    assert_eq!(origin.connections.load(Ordering::SeqCst), 3);
}
