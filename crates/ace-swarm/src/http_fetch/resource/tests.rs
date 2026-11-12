use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn response(status: u16, headers: &[(&str, &str)]) -> reqwest::Response {
    let mut response = hyper::Response::builder().status(status);
    for (name, value) in headers {
        response = response.header(*name, *value);
    }
    response.body(Vec::<u8>::new()).unwrap().into()
}

#[test]
fn relative_references_preserve_signed_query_and_revalidate_authority() {
    let policy = FetchPolicy::default();
    let base =
        FetchUrl::parse_source("https://example.invalid/a/entry?parent=secret", policy).unwrap();
    assert_eq!(
        base.join_reference("child?sig=a%2Fb&n=1", policy)
            .unwrap()
            .0
            .as_str(),
        "https://example.invalid/a/child?sig=a%2Fb&n=1"
    );
    assert_eq!(
        base.join_reference("child", policy).unwrap().0.as_str(),
        "https://example.invalid/a/child"
    );
    for value in [
        "http://@example.invalid/x",
        "//user@example.invalid/x",
        "///bad",
        "http:///bad",
        "file:///x",
        "child#fragment",
        "child\\x",
        "",
    ] {
        assert!(base.join_reference(value, policy).is_err(), "{value}");
    }
    assert_eq!(
        base.join_reference("segment.ts?next=https://example.invalid/x", policy)
            .unwrap()
            .0
            .as_str(),
        "https://example.invalid/a/segment.ts?next=https://example.invalid/x"
    );
    let mut map = std::collections::HashMap::new();
    map.insert(base.clone(), 1);
    assert_eq!(map.get(&base), Some(&1));
    assert_eq!(base.serialized_len(), base.0.as_str().len());
}

#[test]
fn resource_ranges_validate_geometry_and_response() {
    for (range, value, length) in [
        ("bytes=2-4", "bytes 2-4/10", "3"),
        ("bytes=8-99", "bytes 8-9/10", "2"),
        ("bytes=7-", "bytes 7-9/10", "3"),
        ("bytes=-3", "bytes 7-9/10", "3"),
    ] {
        assert!(validate_response(
            &response(206, &[("content-range", value), ("content-length", length)]),
            Some(range)
        )
        .is_ok());
    }
    for bad in [
        "bytes=1-0",
        "bytes=-0",
        "bytes=0-1,3-4",
        "bytes=+1-2",
        "bytes=0-18446744073709551615",
        "bytes= 0-2",
    ] {
        assert!(Range::parse(bad).is_err());
    }
    for (value, length) in [
        ("bytes 3-4/10", "2"),
        ("bytes 2-5/10", "4"),
        ("bytes 2-4/0", "3"),
        ("bytes 2-4/4", "3"),
        ("bytes 2-4/10", "2"),
        ("bytes 2-4/+10", "3"),
    ] {
        assert!(validate_response(
            &response(206, &[("content-range", value), ("content-length", length)]),
            Some("bytes=2-4")
        )
        .is_err());
    }
    assert!(validate_response(&response(206, &[("content-range", "bytes 0-1/2")]), None).is_err());
    assert!(validate_response(&response(200, &[("content-range", "bytes 0-1/2")]), None).is_err());
    assert!(validate_response(&response(204, &[]), None).is_err());
    assert!(validate_response(&response(200, &[("content-encoding", "gzip")]), None).is_err());
}

#[test]
fn playlist_gzip_is_explicit_and_rejects_ambiguous_encodings_and_partial_bodies() {
    for value in ["gzip", "GZIP", " \tgZip\t ", "identity", " IDENTITY "] {
        assert!(validate_playlist_response(&response(200, &[("content-encoding", value)])).is_ok());
    }
    for value in [
        "",
        "gzip, identity",
        "gzip, gzip",
        "br",
        "deflate",
        "zstd",
        "x-gzip",
    ] {
        assert_eq!(
            validate_playlist_response(&response(200, &[("content-encoding", value)])),
            Err(FetchError::UnsupportedResponse)
        );
    }
    for value in ["gzip", "identity"] {
        assert_eq!(
            validate_playlist_response(&response(
                200,
                &[("content-encoding", value), ("content-encoding", value)]
            )),
            Err(FetchError::UnsupportedResponse)
        );
    }
    assert_eq!(
        validate_playlist_response(&response(
            206,
            &[
                ("content-encoding", "gzip"),
                ("content-range", "bytes 0-9/10"),
                ("content-length", "10")
            ]
        )),
        Err(FetchError::UnsupportedResponse)
    );
    assert_eq!(
        validate_response(
            &response(
                206,
                &[
                    ("content-encoding", "gzip"),
                    ("content-range", "bytes 0-9/10"),
                    ("content-length", "10")
                ]
            ),
            Some("bytes=0-9")
        ),
        Err(FetchError::UnsupportedResponse)
    );
}

#[tokio::test]
async fn playlist_redirects_preserve_raw_encoding_and_wire_length() {
    let mut origin = fixture_origin(vec![
        ("HTTP/1.1 302 Found\r\nLocation: /final?sig=a%2Fb\r\nContent-Encoding: gzip\r\nContent-Length: 0\r\n\r\n".into(), Duration::ZERO),
        ("HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 3\r\n\r\nraw".into(), Duration::ZERO),
    ]).await;
    let (_tx, cancel) = watch::channel(false);
    let mut result = open_playlist_resource(
        &origin.url,
        FetchPolicy {
            allow_non_global: true,
        },
        limits(),
        cancel,
    )
    .await
    .unwrap();
    assert!(result.final_url.0.as_str().ends_with("/final?sig=a%2Fb"));
    assert_eq!(result.content_encoding, ResourceEncoding::Gzip);
    assert_eq!(result.content_length, Some(3));
    let mut bytes = Vec::new();
    while let Some(chunk) = result.body.next_chunk().await.unwrap() {
        bytes.extend_from_slice(&chunk);
    }
    assert_eq!(bytes, b"raw");
    for request in (&mut origin.task).await.unwrap() {
        let request = request.to_ascii_lowercase();
        assert!(request.contains("accept-encoding: gzip, identity\r\n"));
        assert!(!request.contains("\r\nrange:"));
    }
}

#[tokio::test]
async fn gzip_playlist_body_retains_cancellation_and_transport_deadline() {
    for should_cancel in [false, true] {
        let origin = fixture_origin(vec![(
            "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 1\r\n\r\n".into(),
            Duration::from_secs(2),
        )])
        .await;
        let (tx, cancel) = watch::channel(false);
        let mut result = open_playlist_resource(
            &origin.url,
            FetchPolicy {
                allow_non_global: true,
            },
            FetchLimits {
                headers_timeout: Duration::from_millis(100),
                ..limits()
            },
            cancel,
        )
        .await
        .unwrap();
        if should_cancel {
            tx.send_replace(true);
        }
        let error = tokio::time::timeout(Duration::from_millis(500), result.body.next_chunk())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error,
            if should_cancel {
                FetchError::Cancelled
            } else {
                FetchError::BodyTimeout
            }
        );
        assert!(result.body.response.is_none());
    }
}

#[test]
fn caching_requires_explicit_fresh_unpersonalized_response() {
    let ttl = |headers: &[(&str, &str)]| cache_ttl(response(200, headers).headers());
    assert_eq!(
        ttl(&[("cache-control", "public, max-age=100"), ("age", "80")]),
        Some(Duration::from_secs(20))
    );
    assert_eq!(
        ttl(&[("cache-control", "max-age=100")]),
        Some(Duration::from_secs(30))
    );
    assert_eq!(
        ttl(&[("cache-control", "max-age=\"12\"")]),
        Some(Duration::from_secs(12))
    );
    for headers in [
        vec![],
        vec![("cache-control", "immutable")],
        vec![("cache-control", "max-age=1"), ("age", "2")],
        vec![("cache-control", "max-age=30, no-cache")],
        vec![("cache-control", "max-age=30, private=\"x\"")],
        vec![("cache-control", "max-age=30, no-store")],
        vec![("cache-control", "max-age=30, max-age=30")],
        vec![("cache-control", "max-age=30"), ("vary", "Accept-Encoding")],
        vec![("cache-control", "max-age=30"), ("set-cookie", "fixture=1")],
        vec![("cache-control", "max-age=30"), ("age", "invalid")],
    ] {
        assert_eq!(ttl(&headers), None, "{headers:?}");
    }
}

struct Origin {
    url: FetchUrl,
    task: tokio::task::JoinHandle<Vec<String>>,
}
impl Drop for Origin {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn fixture_origin(responses: Vec<(String, Duration)>) -> Origin {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = FetchUrl::parse_source(
        &format!("http://{}/initial", listener.local_addr().unwrap()),
        FetchPolicy {
            allow_non_global: true,
        },
    )
    .unwrap();
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for (response, delay) in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                socket.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
                assert!(request.len() < 8192);
            }
            requests.push(String::from_utf8(request).unwrap());
            socket.write_all(response.as_bytes()).await.unwrap();
            tokio::time::sleep(delay).await;
        }
        requests
    });
    Origin { url, task }
}

fn limits() -> FetchLimits {
    FetchLimits {
        connect_timeout: Duration::from_secs(1),
        headers_timeout: Duration::from_secs(2),
        source_response: false,
    }
}

#[tokio::test]
async fn redirect_base_range_and_original_source_policy_are_distinct() {
    let mut origin=fixture_origin(vec![("HTTP/1.1 302 Found\r\nLocation: /final/index?sig=a%2Fb\r\nContent-Length: 0\r\n\r\n".into(),Duration::ZERO),("HTTP/1.1 206 Partial Content\r\nContent-Type: video/mp2t\r\nContent-Range: bytes 1-3/5\r\nContent-Length: 3\r\n\r\nbcd".into(),Duration::ZERO)]).await;
    let (_tx, cancel) = watch::channel(false);
    let mut result = open_resource(
        &origin.url,
        FetchPolicy {
            allow_non_global: true,
        },
        limits(),
        Some("bytes=1-3"),
        cancel,
    )
    .await
    .unwrap();
    assert!(result
        .final_url
        .0
        .as_str()
        .ends_with("/final/index?sig=a%2Fb"));
    assert_eq!(result.status, 206);
    assert_eq!(result.content_length, Some(3));
    assert_eq!(result.cache_ttl, None);
    let mut bytes = Vec::new();
    while let Some(chunk) = result.body.next_chunk().await.unwrap() {
        bytes.extend_from_slice(&chunk);
    }
    assert_eq!(bytes, b"bcd");
    let requests = (&mut origin.task).await.unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].starts_with("GET /final/index?sig=a%2Fb HTTP/1.1"));
    for request in requests {
        let request = request.to_ascii_lowercase();
        assert!(request.contains("range: bytes=1-3"));
        assert!(
            !request.contains("authorization:")
                && !request.contains("cookie:")
                && !request.contains("referer:")
        );
    }

    let origin = fixture_origin(vec![(
        "HTTP/1.1 302 Found\r\nLocation: /next\r\nContent-Length: 0\r\n\r\n".into(),
        Duration::ZERO,
    )])
    .await;
    let (_tx, cancel) = watch::channel(false);
    assert!(matches!(
        open(
            &origin.url,
            FetchPolicy {
                allow_non_global: true
            },
            FetchLimits {
                source_response: true,
                ..limits()
            },
            cancel
        )
        .await,
        Err(FetchError::RedirectRejected)
    ));
}

#[tokio::test]
async fn redirect_limit_and_body_deadline_are_bounded() {
    let origin = fixture_origin(
        (0..4)
            .map(|_| {
                (
                    "HTTP/1.1 302 Found\r\nLocation: /again\r\nContent-Length: 0\r\n\r\n".into(),
                    Duration::ZERO,
                )
            })
            .collect(),
    )
    .await;
    let (_tx, cancel) = watch::channel(false);
    assert!(matches!(
        open_resource(
            &origin.url,
            FetchPolicy {
                allow_non_global: true
            },
            limits(),
            None,
            cancel
        )
        .await,
        Err(FetchError::RedirectRejected)
    ));
    drop(origin);
    let origin = fixture_origin(vec![(
        "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n".into(),
        Duration::from_secs(2),
    )])
    .await;
    let (_tx, cancel) = watch::channel(false);
    let mut response = open_resource(
        &origin.url,
        FetchPolicy {
            allow_non_global: true,
        },
        FetchLimits {
            headers_timeout: Duration::from_millis(100),
            ..limits()
        },
        None,
        cancel,
    )
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(400), response.body.next_chunk())
            .await
            .unwrap()
            .is_err()
    );
}

#[tokio::test]
async fn resource_cancellation_releases_open_body() {
    let origin = fixture_origin(vec![(
        "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n".into(),
        Duration::from_secs(2),
    )])
    .await;
    let (tx, cancel) = watch::channel(false);
    let mut response = open_resource(
        &origin.url,
        FetchPolicy {
            allow_non_global: true,
        },
        limits(),
        None,
        cancel,
    )
    .await
    .unwrap();
    tx.send_replace(true);
    assert_eq!(response.body.next_chunk().await, Err(FetchError::Cancelled));
    assert!(response.body.response.is_none());
}

#[tokio::test]
async fn public_policy_refuses_non_global_destinations_and_redirect_targets() {
    let policy = FetchPolicy::default();
    let base = FetchUrl::parse_source("https://example.invalid/live/index.m3u8", policy).unwrap();
    for target in [
        "http://127.0.0.1/x",
        "http://10.0.0.1/x",
        "http://172.16.0.1/x",
        "http://192.168.1.1/x",
        "http://169.254.169.254/latest/meta-data/",
        "http://100.64.0.1/x",
        "http://0.0.0.0/x",
        "http://[::1]/x",
        "http://[fe80::1]/x",
        "http://[fd00::1]/x",
        "http://[::ffff:127.0.0.1]/x",
    ] {
        assert_eq!(
            FetchUrl::parse_source(target, policy).err(),
            Some(FetchError::SourceAddressBlocked),
            "{target}"
        );
        // A redirect Location or playlist reference is admitted under the same policy.
        assert_eq!(
            base.join_reference(target, policy).err(),
            Some(FetchError::SourceAddressBlocked),
            "{target}"
        );
    }
    // Names are re-checked after DNS, before any connection.
    let (_tx, cancel) = watch::channel(false);
    let named = FetchUrl::parse_source("http://localhost:9/x", policy).unwrap();
    assert!(matches!(
        open_resource(&named, policy, limits(), None, cancel).await,
        Err(FetchError::AddressBlocked | FetchError::DnsFailed)
    ));
}

#[test]
fn shared_cache_honors_shared_directive_and_apparent_date_age() {
    let ttl = |headers: &[(&str, &str)]| cache_ttl(response(200, headers).headers());
    assert_eq!(ttl(&[("cache-control", "max-age=100, s-maxage=0")]), None);
    assert_eq!(
        ttl(&[("cache-control", "max-age=100, s-maxage=20"), ("age", "5")]),
        Some(Duration::from_secs(15))
    );
    assert_eq!(
        ttl(&[("cache-control", "s-maxage=\"20\""), ("age", "5")]),
        Some(Duration::from_secs(15))
    );
    for directive in [
        "s-maxage=1,s-maxage=2",
        "s-maxage=invalid",
        "max-age=100,s-maxage=-1",
    ] {
        assert_eq!(ttl(&[("cache-control", directive)]), None);
    }
    assert_eq!(
        ttl(&[
            ("cache-control", "max-age=30"),
            ("date", "Sun, 06 Nov 1994 08:49:37 GMT"),
            ("age", "0")
        ]),
        None
    );
    assert_eq!(
        ttl(&[("cache-control", "max-age=30"), ("date", "invalid")]),
        None
    );
}
