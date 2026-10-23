use super::*;
use bytes::Bytes;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::process::Command;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;

const ROOT_CERT: &[u8] = include_bytes!("tests/fixtures/source-test-root.der");
const ROOT_KEY: &[u8] = include_bytes!("tests/fixtures/source-test-key.der");

fn policy(allow_non_global: bool) -> FetchPolicy {
    FetchPolicy { allow_non_global }
}

fn limits(source_response: bool) -> FetchLimits {
    FetchLimits {
        connect_timeout: Duration::from_secs(60),
        headers_timeout: Duration::from_secs(15),
        source_response,
    }
}

fn local_url(addr: SocketAddr, path: &str) -> FetchUrl {
    FetchUrl::parse_source(
        &format!("http://127.0.0.1:{}{path}", addr.port()),
        policy(true),
    )
    .expect("test policy permits loopback")
}

fn literal_url(host: &str) -> String {
    if host.contains(':') {
        format!("http://[{host}]/source")
    } else {
        format!("http://{host}/source")
    }
}

fn assert_code<T>(result: Result<T, FetchError>, code: &str) {
    let error = result.err().expect("operation should be rejected");
    assert_eq!(error.code(), code);
}

#[test]
fn pull206_policy_and_source_url_admission() {
    let deny_by_default = [
        "0.0.0.0",
        "10.0.0.1",
        "100.64.0.1",
        "127.0.0.1",
        "169.254.169.254",
        "172.16.0.1",
        "192.168.1.1",
        "192.0.2.1",
        "198.51.100.1",
        "203.0.113.1",
        "224.0.0.1",
        "255.255.255.255",
        "::",
        "::1",
        "::ffff:127.0.0.1",
        "::ffff:192.0.2.1",
        "fc00::1",
        "fe80::1",
        "ff02::1",
    ];
    for host in deny_by_default {
        let source = literal_url(host);
        assert!(
            FetchUrl::parse_source(&source, policy(false)).is_err(),
            "{host} must be denied by default"
        );
    }

    let opt_in_subset = [
        "10.0.0.1",
        "100.64.0.1",
        "127.0.0.1",
        "172.16.0.1",
        "192.168.1.1",
        "::1",
        "fc00::1",
        "::ffff:127.0.0.1",
        "::ffff:10.0.0.1",
    ];
    for host in opt_in_subset {
        assert!(
            FetchUrl::parse_source(&literal_url(host), policy(true)).is_ok(),
            "{host} is in the approved non-global opt-in subset"
        );
    }
    for host in [
        "169.254.169.254",
        "192.0.2.1",
        "198.51.100.1",
        "203.0.113.1",
        "0.0.0.0",
        "224.0.0.1",
        "255.255.255.255",
        "::",
        "fe80::1",
        "ff02::1",
        "::ffff:169.254.169.254",
    ] {
        assert!(
            FetchUrl::parse_source(&literal_url(host), policy(true)).is_err(),
            "{host} must remain blocked with opt-in"
        );
    }

    for raw in [
        "",
        "http://@source.example.invalid/live",
        "http://source.example.invalid/live#fragment",
        "ftp://source.example.invalid/live",
        "file:///tmp/source",
        "http:///missing-host",
        "http://source.example.invalid:0/live",
        "http://source.example.invalid/live\nX-Injected: yes",
    ] {
        assert!(
            FetchUrl::parse_source(raw, policy(false)).is_err(),
            "{raw:?}"
        );
    }
    let prefix = "http://example.invalid/";
    let at_limit = format!("{prefix}{}", "x".repeat(8_192 - prefix.len()));
    let over_limit = format!("{prefix}{}", "x".repeat(8_193 - prefix.len()));
    assert!(FetchUrl::parse_source(&at_limit, policy(false)).is_ok());
    assert!(FetchUrl::parse_source(&over_limit, policy(false)).is_err());

    let a = FetchUrl::parse_source("HTTP://Example.Invalid:80/a?x=1&y=2", policy(false)).unwrap();
    let b = FetchUrl::parse_source("http://example.invalid/a?x=1&y=2", policy(false)).unwrap();
    assert_eq!(a, b, "scheme/host case and default port normalize");
    assert_ne!(
        a,
        FetchUrl::parse_source("http://example.invalid/a?y=2&x=1", policy(false)).unwrap(),
        "query ordering remains significant"
    );
    assert_ne!(
        a,
        FetchUrl::parse_source("http://example.invalid/a?x=1&y=3", policy(false)).unwrap(),
        "query tokens remain significant"
    );
    assert_ne!(
        a,
        FetchUrl::parse_source("http://example.invalid/b?x=1&y=2", policy(false)).unwrap(),
        "path remains significant"
    );

    let marker = "do-not-print-this-token";
    let rejected = FetchUrl::parse_source(
        &format!("http://user:{marker}@127.0.0.1/private?token={marker}"),
        policy(true),
    )
    .expect_err("userinfo is never accepted");
    assert!(!format!("{rejected:?}{rejected}").contains(marker));

    // A syntactically valid hostname with no usable DNS record proves URL parsing itself is
    // side-effect free; admission must not perform name resolution.
    assert!(FetchUrl::parse_source("http://not-resolved.invalid/live", policy(false)).is_ok());
}

async fn serve_response(response: Vec<u8>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((mut socket, _)) = listener.accept().await {
            let mut request = [0u8; 4096];
            let _ = socket.read(&mut request).await;
            let _ = socket.write_all(&response).await;
            let _ = socket.shutdown().await;
        }
    });
    addr
}

fn response(status: &str, extra_headers: &str, body: &[u8]) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n{extra_headers}\r\n",
        body.len()
    )
    .into_bytes()
    .into_iter()
    .chain(body.iter().copied())
    .collect()
}

async fn open_local(addr: SocketAddr) -> Result<GuardedBody, FetchError> {
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    open(
        &local_url(addr, "/media?token=synthetic"),
        policy(true),
        limits(true),
        cancel_rx,
    )
    .await
}

#[tokio::test]
async fn pull206_response_and_frame_bounds() {
    for (status, expected) in [
        ("204 No Content", "unsupported_response"),
        ("205 Reset Content", "unsupported_response"),
        ("206 Partial Content", "unsupported_response"),
        ("101 Switching Protocols", "unsupported_response"),
        ("302 Found", "redirect_rejected"),
        ("404 Not Found", "http_status"),
    ] {
        let addr = serve_response(response(status, "", b"x")).await;
        assert_code(open_local(addr).await, expected);
    }

    for headers in [
        "Content-Range: bytes 0-1/2\r\n",
        "Content-Encoding: gzip\r\n",
    ] {
        let addr = serve_response(response("200 OK", headers, b"x")).await;
        assert_code(open_local(addr).await, "unsupported_response");
    }

    let ordinary = serve_response(response("200 OK", "", b"ordinary body")).await;
    let mut body = open_local(ordinary)
        .await
        .expect("missing MIME is accepted");
    assert_eq!(
        body.next_chunk().await.unwrap(),
        Some(Bytes::from_static(b"ordinary body"))
    );
    assert_eq!(body.next_chunk().await.unwrap(), None);

    let mut many_headers = String::new();
    for index in 0..65 {
        many_headers.push_str(&format!("X-{index}: v\r\n"));
    }
    let addr = serve_response(response("200 OK", &many_headers, b"x")).await;
    assert_code(open_local(addr).await, "unsupported_response");

    let oversized_headers = format!("X-Large: {}\r\n", "h".repeat(33 * 1024));
    let addr = serve_response(response("200 OK", &oversized_headers, b"x")).await;
    assert_code(open_local(addr).await, "unsupported_response");

    let malformed = b"HTTP/1.1 200 OK\r\nnot a header\r\n\r\nx".to_vec();
    let addr = serve_response(malformed).await;
    assert_code(open_local(addr).await, "unsupported_response");

    let mut body = test_support::body_with_frames(vec![Bytes::from(vec![0x47; 256 * 1024 + 1])]);
    assert_code(body.next_chunk().await, "body_frame_too_large");
}

#[tokio::test]
async fn pull206_fetch_waits_cancel() {
    // Cancellation while DNS is pending drops the in-flight resolver wait.
    let dns_url = FetchUrl::parse_source("http://dns-cancel.test/source", policy(false)).unwrap();
    let (dns_cancel_tx, dns_cancel_rx) = watch::channel(false);
    let dns_task = tokio::spawn({
        let dns_url = dns_url.clone();
        async move {
            test_support::open_with(
                &dns_url,
                policy(false),
                limits(true),
                dns_cancel_rx,
                test_support::TestHooks::new().with_pending_dns(),
            )
            .await
        }
    });
    tokio::task::yield_now().await;
    dns_cancel_tx.send(true).unwrap();
    assert_code(
        tokio::time::timeout(Duration::from_secs(1), dns_task)
            .await
            .unwrap()
            .unwrap(),
        "cancelled",
    );

    // Cancellation during TLS handshake drops the real loopback connection. The server accepts
    // and reads the ClientHello, then waits for the client's close as the cancellation result.
    let tls_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tls_addr = tls_listener.local_addr().unwrap();
    let (hello_tx, hello_rx) = tokio::sync::oneshot::channel();
    let tls_server = tokio::spawn(async move {
        let (mut socket, _) = tls_listener.accept().await.unwrap();
        let mut client_hello = [0u8; 4096];
        let count = socket.read(&mut client_hello).await.unwrap();
        assert!(count > 0, "client reached the TLS handshake");
        let _ = hello_tx.send(());
        let mut after_hello = [0u8; 1];
        socket.read(&mut after_hello).await.unwrap()
    });
    let tls_url = FetchUrl::parse_source("https://tls-cancel.test/source", policy(true)).unwrap();
    let (tls_cancel_tx, tls_cancel_rx) = watch::channel(false);
    let tls_open = tokio::spawn({
        let tls_url = tls_url.clone();
        async move {
            test_support::open_with(
                &tls_url,
                policy(true),
                limits(true),
                tls_cancel_rx,
                test_support::TestHooks::new().with_dns_answers(vec![tls_addr]),
            )
            .await
        }
    });
    tokio::time::timeout(Duration::from_secs(1), hello_rx)
        .await
        .unwrap()
        .unwrap();
    tls_cancel_tx.send(true).unwrap();
    assert_code(
        tokio::time::timeout(Duration::from_secs(1), tls_open)
            .await
            .unwrap()
            .unwrap(),
        "cancelled",
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), tls_server)
            .await
            .unwrap()
            .unwrap(),
        0,
        "cancellation closes the TLS attempt"
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut byte = [0u8; 1];
        while !request.ends_with(b"\r\n\r\n") {
            if socket.read_exact(&mut byte).await.is_err() {
                return 0;
            }
            request.push(byte[0]);
        }
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut pending = [0u8; 1];
        socket.read(&mut pending).await.unwrap()
    });

    let (cancel_tx, mut cancel_rx) = watch::channel(false);
    let url = local_url(addr, "/stall");
    let mut body = open(&url, policy(true), limits(true), cancel_rx.clone())
        .await
        .unwrap();
    let (reading_tx, reading_rx) = tokio::sync::oneshot::channel();
    let read_task = tokio::spawn(async move {
        let _ = reading_tx.send(());
        body.next_chunk().await
    });
    tokio::time::timeout(Duration::from_secs(1), reading_rx)
        .await
        .unwrap()
        .unwrap();
    tokio::task::yield_now().await;
    cancel_tx.send(true).unwrap();
    assert_code(
        tokio::time::timeout(Duration::from_secs(1), read_task)
            .await
            .unwrap()
            .unwrap(),
        "cancelled",
    );
    tokio::time::timeout(Duration::from_secs(1), cancel_rx.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap(),
        0,
        "cancellation closes the body attempt after accepted headers"
    );
}

#[tokio::test(start_paused = true)]
async fn pull206_dns_and_header_deadlines_are_independent_of_connect_timeout() {
    let url = FetchUrl::parse_source("http://dns-stall.test/live", policy(false)).unwrap();
    let (_tx, cancel) = watch::channel(false);
    let hooks = test_support::TestHooks::new().with_pending_dns();
    assert_code(
        test_support::open_with(&url, policy(false), limits(true), cancel.clone(), hooks).await,
        "dns_timeout",
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 2048];
        let _ = socket.read(&mut request).await;
        let _ = accepted_tx.send(());
        // Hold the connected socket without returning response headers.
        tokio::time::sleep(Duration::from_secs(60)).await;
    });
    let header_url = local_url(addr, "/wait-for-headers");
    let open = open(&header_url, policy(true), limits(true), cancel);
    tokio::pin!(open);
    tokio::select! {
        result = &mut open => assert_code(result, "headers_timeout"),
        accepted = accepted_rx => {
            accepted.unwrap();
            assert_code(open.await, "headers_timeout");
        }
    }
}

#[tokio::test]
async fn pull206_rebinding_rechecks_and_pins() {
    // Test-only open seam uses a scripted resolver but still exercises the production connector.
    // First result is mixed; the second contains only blocked addresses. The first request must
    // reach the one selected loopback target, while the second must not dial any target.
    let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first_addr = first.local_addr().unwrap();
    let (request_tx, request_rx) = tokio::sync::oneshot::channel();
    let (check_second_tx, check_second_rx) = tokio::sync::oneshot::channel();
    let target_seen = tokio::spawn(async move {
        let (mut socket, _) = first.accept().await.unwrap();
        let mut request = [0u8; 2048];
        let count = socket.read(&mut request).await.unwrap();
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let _ = request_tx.send(String::from_utf8_lossy(&request[..count]).into_owned());
        let _ = check_second_rx.await;
        tokio::time::timeout(Duration::from_millis(150), first.accept())
            .await
            .is_ok()
    });
    let hooks = test_support::TestHooks::new()
        .with_dns_answers(vec!["169.254.169.254:8080".parse().unwrap(), first_addr])
        .then_dns_answers(vec!["169.254.169.254:8080".parse().unwrap()]);
    let url = FetchUrl::parse_source(
        &format!("http://source.test:{}/live", first_addr.port()),
        policy(true),
    )
    .unwrap();
    let (_tx, cancel) = watch::channel(false);
    let opened = test_support::open_with(
        &url,
        policy(true),
        limits(true),
        cancel.clone(),
        hooks.clone(),
    )
    .await;
    assert!(opened.is_ok(), "mixed answers select one allowed target");
    let request = tokio::time::timeout(Duration::from_secs(1), request_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(request.starts_with("GET /live HTTP/1.1\r\n"));
    assert!(request
        .to_ascii_lowercase()
        .contains(&format!("host: source.test:{}\r\n", first_addr.port())));
    for forbidden in [
        "authorization:",
        "cookie:",
        "accept-encoding:",
        "range:",
        "if-range:",
        "if-none-match:",
        "referer:",
    ] {
        assert!(
            !request.to_ascii_lowercase().contains(forbidden),
            "unexpected forwarded header {forbidden}"
        );
    }

    drop(opened);
    let err = test_support::open_with(&url, policy(true), limits(true), cancel, hooks).await;
    assert_code(err, "address_blocked");
    check_second_tx.send(()).unwrap();
    assert!(
        !tokio::time::timeout(Duration::from_secs(1), target_seen)
            .await
            .unwrap()
            .unwrap(),
        "the all-blocked next resolution opens no second socket"
    );
}

#[tokio::test]
async fn pull206_proxy_redirect_tls_guards() {
    let redirect_target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let redirect_addr = redirect_target.local_addr().unwrap();
    let redirect_sentinel = redirect_target;
    let redirect_seen = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_millis(100), redirect_sentinel.accept())
            .await
            .is_ok()
    });
    let addr = serve_response(response(
        "302 Found",
        &format!(
            "Location: http://127.0.0.1:{}/redirected\r\n",
            redirect_addr.port()
        ),
        b"redirect",
    ))
    .await;
    assert_code(open_local(addr).await, "redirect_rejected");
    assert!(
        !redirect_seen.await.unwrap(),
        "Location target receives no request"
    );

    // The child gets proxy variables only in its own environment. The target and proxy are
    // distinct loopback listeners, so a request routed through the proxy is observable.
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_addr = target.local_addr().unwrap();
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy.local_addr().unwrap();
    let proxy_seen = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_millis(150), proxy.accept())
            .await
            .is_ok()
    });
    let target_seen = tokio::spawn(async move {
        let (mut socket, _) = target.accept().await.unwrap();
        let mut request = [0u8; 1024];
        let count = socket.read(&mut request).await.unwrap();
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        request[..count].to_vec()
    });
    let child = tokio::task::spawn_blocking(move || {
        Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("http_fetch::tests::pull206_proxy_child_probe")
            .arg("--nocapture")
            .env("PULL206_PROXY_CHILD", "1")
            .env("PULL206_TARGET_PORT", target_addr.port().to_string())
            .env("HTTP_PROXY", format!("http://{proxy_addr}"))
            .env("HTTPS_PROXY", format!("http://{proxy_addr}"))
            .env("ALL_PROXY", format!("http://{proxy_addr}"))
            .env("NO_PROXY", "")
            .env("no_proxy", "")
            .output()
            .expect("start isolated proxy test child")
    });
    let output = tokio::time::timeout(Duration::from_secs(20), child)
        .await
        .expect("proxy child finished within its bounded HTTP deadline")
        .unwrap();
    assert!(
        output.status.success(),
        "child test failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let request = tokio::time::timeout(Duration::from_secs(1), target_seen)
        .await
        .unwrap()
        .unwrap();
    assert!(request.starts_with(b"GET /probe HTTP/1.1\r\n"));
    assert!(
        !proxy_seen.await.unwrap(),
        "environment proxy receives no request"
    );

    // The local TLS server presents only the synthetic source.test certificate. A trusted-root
    // attempt succeeds with the original Host name; an absent root and a wrong URL hostname fail.
    let tls = spawn_tls_once().await;
    let https = FetchUrl::parse_source("https://source.test/live", policy(true)).unwrap();
    let trusted = test_support::TestHooks::new()
        .with_dns_answers(vec![tls.addr])
        .with_tls_root(ROOT_CERT.to_vec());
    let (_tx, cancel) = watch::channel(false);
    let mut body =
        test_support::open_with(&https, policy(true), limits(true), cancel.clone(), trusted)
            .await
            .unwrap();
    assert_eq!(
        body.next_chunk().await.unwrap(),
        Some(Bytes::from_static(b"tls-ok"))
    );
    let host = tls.host_rx.await.unwrap();
    assert_eq!(host, "source.test");
    assert_eq!(tls.sni_rx.await.unwrap().as_deref(), Some("source.test"));

    let untrusted = test_support::TestHooks::new().with_dns_answers(vec![tls.addr]);
    let untrusted_url =
        FetchUrl::parse_source("https://source.test/untrusted", policy(true)).unwrap();
    assert_code(
        test_support::open_with(
            &untrusted_url,
            policy(true),
            limits(true),
            cancel.clone(),
            untrusted,
        )
        .await,
        "tls_failed",
    );

    let wrong_host = FetchUrl::parse_source("https://other.test/wrong-host", policy(true)).unwrap();
    let wrong_name = test_support::TestHooks::new()
        .with_dns_answers(vec![tls.addr])
        .with_tls_root(ROOT_CERT.to_vec());
    assert_code(
        test_support::open_with(&wrong_host, policy(true), limits(true), cancel, wrong_name).await,
        "tls_failed",
    );
}

#[test]
fn pull206_proxy_child_probe() {
    if std::env::var("PULL206_PROXY_CHILD").as_deref() != Ok("1") {
        return;
    }
    let port: u16 = std::env::var("PULL206_TARGET_PORT")
        .unwrap()
        .parse()
        .unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async move {
        let url = FetchUrl::parse_source("http://proxy-test.invalid/probe", policy(true)).unwrap();
        let hooks = test_support::TestHooks::new()
            .with_dns_answers(vec![SocketAddr::from(([127, 0, 0, 1], port))]);
        let (_tx, cancel) = watch::channel(false);
        let mut body = test_support::open_with(&url, policy(true), limits(true), cancel, hooks)
            .await
            .unwrap();
        assert_eq!(body.next_chunk().await.unwrap(), None);
    });
}

struct TlsFixture {
    addr: SocketAddr,
    host_rx: tokio::sync::oneshot::Receiver<String>,
    sni_rx: tokio::sync::oneshot::Receiver<Option<String>>,
}

#[derive(Debug)]
struct RecordingCertificateResolver {
    key: std::sync::Arc<rustls::sign::CertifiedKey>,
    sni_tx: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<Option<String>>>>,
}

impl rustls::server::ResolvesServerCert for RecordingCertificateResolver {
    fn resolve(
        &self,
        client_hello: rustls::server::ClientHello<'_>,
    ) -> Option<std::sync::Arc<rustls::sign::CertifiedKey>> {
        if let Some(sender) = self.sni_tx.lock().unwrap().take() {
            let _ = sender.send(client_hello.server_name().map(str::to_owned));
        }
        Some(self.key.clone())
    }
}

async fn spawn_tls_once() -> TlsFixture {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(false).unwrap();
    let addr = listener.local_addr().unwrap();
    let (host_tx, host_rx) = tokio::sync::oneshot::channel();
    let (sni_tx, sni_rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let mut host_tx = Some(host_tx);
        let mut sni_tx = Some(sni_tx);
        for _ in 0..3 {
            let (socket, _) = listener.accept().unwrap();
            let cert = rustls::pki_types::CertificateDer::from(ROOT_CERT.to_vec());
            let key = rustls::pki_types::PrivateKeyDer::try_from(ROOT_KEY.to_vec()).unwrap();
            let signing_key = rustls::crypto::ring::sign::any_supported_type(&key).unwrap();
            let certified_key =
                std::sync::Arc::new(rustls::sign::CertifiedKey::new(vec![cert], signing_key));
            let mut config = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_cert_resolver(std::sync::Arc::new(RecordingCertificateResolver {
                    key: certified_key,
                    sni_tx: std::sync::Mutex::new(sni_tx.take()),
                }));
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            let connection = rustls::ServerConnection::new(std::sync::Arc::new(config)).unwrap();
            let mut stream = rustls::StreamOwned::new(connection, socket);
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            while request.len() < 32 * 1024 {
                if stream.read_exact(&mut byte).is_err() {
                    break;
                }
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let text = String::from_utf8_lossy(&request);
            let host = text
                .lines()
                .find_map(|line| {
                    line.strip_prefix("Host: ")
                        .or_else(|| line.strip_prefix("host: "))
                })
                .unwrap_or_default()
                .to_string();
            if let Some(sender) = host_tx.take() {
                let _ = sender.send(host);
            }
            let _ = stream.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\ntls-ok",
            );
            let _ = stream.flush();
        }
    });
    TlsFixture {
        addr,
        host_rx,
        sni_rx,
    }
}
