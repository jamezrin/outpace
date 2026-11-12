use super::*;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{extract::State, routing::any, Router};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::net::TcpListener;

struct Fixture {
    address: std::net::SocketAddr,
    abort: tokio::task::AbortHandle,
    calls: Arc<Mutex<Vec<String>>>,
}
impl Fixture {
    async fn new(handler: impl Fn(&str, &HeaderMap) -> Response + Send + Sync + 'static) -> Self {
        type Handler = dyn Fn(&str, &HeaderMap) -> Response + Send + Sync;
        let calls = Arc::new(Mutex::new(Vec::new()));
        let captured = calls.clone();
        let handler: Arc<Handler> = Arc::new(move |path, headers| {
            captured.lock().unwrap().push(path.to_owned());
            handler(path, headers)
        });
        let app = Router::new()
            .fallback(any(
                |State(handler): State<Arc<Handler>>, request: Request<Body>| async move {
                    handler(&request.uri().to_string(), request.headers())
                },
            ))
            .with_state(handler);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            address,
            abort: server.abort_handle(),
            calls,
        }
    }
    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.address)
    }
    fn count(&self, path: &str) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.as_str() == path)
            .count()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.abort.abort();
    }
}
fn relay() -> Arc<OriginHls> {
    OriginHls::new(FetchPolicy {
        allow_non_global: true,
    })
}
fn token(playback: &OriginPlayback) -> &str {
    playback.stop_url.rsplit('/').next().unwrap()
}
fn resource_id(url: &str) -> &str {
    url.rsplit('/').next().unwrap()
}
fn numeric_id(url: &str) -> u64 {
    resource_id(url).split('.').next().unwrap().parse().unwrap()
}
fn uris(body: &[u8]) -> Vec<String> {
    std::str::from_utf8(body)
        .unwrap()
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .map(str::to_owned)
        .collect()
}
fn manifest(text: &'static str) -> Response {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "application/vnd.apple.mpegurl",
        )],
        text,
    )
        .into_response()
}
fn media(bytes: &'static [u8], cache: &'static str) -> Response {
    (
        [
            (axum::http::header::CONTENT_TYPE, "video/mp2t"),
            (axum::http::header::CACHE_CONTROL, cache),
        ],
        Bytes::from_static(bytes),
    )
        .into_response()
}

#[tokio::test]
async fn signed_origin_is_lazy_and_relative_queries_remain_exact() {
    let origin = Fixture::new(|path, _| match path {
        "/dir/index?root=a%2Fb" => manifest(
            "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2,\nseg.ts?sig=c%2Fd\n#EXT-X-ENDLIST\n",
        ),
        "/dir/seg.ts?sig=c%2Fd" => media(b"synthetic media", "no-store"),
        _ => StatusCode::NOT_FOUND.into_response(),
    })
    .await;
    let relay = relay();
    let playback = relay
        .start(&origin.url("/dir/index?root=a%2Fb"))
        .await
        .unwrap();
    assert!(origin.calls.lock().unwrap().is_empty());
    let playlist = relay.resource(token(&playback), "0", None).await.unwrap();
    assert_eq!(playlist.content_type, "application/vnd.apple.mpegurl");
    assert!(!std::str::from_utf8(&playlist.body)
        .unwrap()
        .contains("sig="));
    let segment = uris(&playlist.body).remove(0);
    drop(playlist);
    let response = relay
        .resource(token(&playback), resource_id(&segment), None)
        .await
        .unwrap();
    assert_eq!(response.body, "synthetic media");
    assert_eq!(origin.count("/dir/seg.ts?sig=c%2Fd"), 1);
    assert_eq!(relay.sessions.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn redirected_master_and_map_ranges_preserve_resource_identity() {
    let origin = Fixture::new(|path, headers| match path {
        "/entry" => (StatusCode::FOUND, [(axum::http::header::LOCATION,"/cdn/master?root=private")]).into_response(),
        "/cdn/master?root=private" => manifest("#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"a\",NAME=\"audio\",URI=\"audio?sig=a%2Fb\"\n#EXT-X-STREAM-INF:BANDWIDTH=1000000,AUDIO=\"a\"\nvideo?sig=v%2Fx\n"),
        "/cdn/video?sig=v%2Fx" => manifest("#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MAP:URI=\"file.mp4?sig=s%2Ft\",BYTERANGE=\"4@0\"\n#EXTINF:2,\n#EXT-X-BYTERANGE:4@4\nfile.mp4?sig=s%2Ft\n#EXT-X-ENDLIST\n"),
        "/cdn/audio?sig=a%2Fb" => manifest("#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2,\na.aac\n#EXT-X-ENDLIST\n"),
        "/cdn/file.mp4?sig=s%2Ft" => {
            let (range, body): (&str,&[u8]) = match headers.get(axum::http::header::RANGE).unwrap().to_str().unwrap() {
                "bytes=0-3" => ("bytes 0-3/8",b"init"), "bytes=4-7" => ("bytes 4-7/8",b"data"), _ => panic!("unexpected range"),
            };
            (StatusCode::PARTIAL_CONTENT, [(axum::http::header::CONTENT_RANGE,range),(axum::http::header::CONTENT_TYPE,"video/mp4")],Bytes::copy_from_slice(body)).into_response()
        }
        _ => StatusCode::NOT_FOUND.into_response(),
    }).await;
    let relay = relay();
    let playback = relay.start(&origin.url("/entry")).await.unwrap();
    let master = relay.resource(token(&playback), "0", None).await.unwrap();
    let text = std::str::from_utf8(&master.body).unwrap();
    let audio = text
        .lines()
        .find(|l| l.starts_with("#EXT-X-MEDIA"))
        .unwrap()
        .split("URI=\"")
        .nth(1)
        .unwrap()
        .trim_end_matches('"')
        .to_owned();
    let video = uris(&master.body).remove(0);
    drop(master);
    let audio_response = relay
        .resource(token(&playback), resource_id(&audio), None)
        .await
        .unwrap();
    drop(audio_response);
    let video_response = relay
        .resource(token(&playback), resource_id(&video), None)
        .await
        .unwrap();
    let text = std::str::from_utf8(&video_response.body).unwrap();
    let map = text
        .lines()
        .find(|l| l.starts_with("#EXT-X-MAP"))
        .unwrap()
        .split("URI=\"")
        .nth(1)
        .unwrap()
        .split('"')
        .next()
        .unwrap()
        .to_owned();
    let segment = uris(&video_response.body).remove(0);
    assert_eq!(map, segment);
    drop(video_response);
    let init = relay
        .resource(token(&playback), resource_id(&map), Some("bytes=0-3"))
        .await
        .unwrap();
    assert_eq!(init.status, 206);
    assert_eq!(init.body, "init");
    assert_eq!(init.content_range.as_deref(), Some("bytes 0-3/8"));
    drop(init);
    let media = relay
        .resource(token(&playback), resource_id(&segment), Some("bytes=4-7"))
        .await
        .unwrap();
    assert_eq!(media.body, "data");
    assert_eq!(origin.count("/cdn/audio?sig=a%2Fb"), 1);
}

#[tokio::test]
async fn cached_media_is_shared_only_within_its_playlist_snapshot() {
    let generation = Arc::new(AtomicUsize::new(0));
    let current = generation.clone();
    let origin = Fixture::new(move |path, _| match path {
        "/playlist" => {
            let old = current.fetch_add(1, Ordering::SeqCst) == 0;
            if old {
                manifest("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:20\n#EXTINF:1,\nsegment.ts\n")
            } else {
                manifest("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:1,\nsegment.ts\n")
            }
        }
        "/segment.ts" => {
            if current.load(Ordering::SeqCst) == 1 {
                media(b"first", "max-age=30")
            } else {
                media(b"reset", "max-age=30")
            }
        }
        _ => StatusCode::NOT_FOUND.into_response(),
    })
    .await;
    let relay = relay();
    let playback = relay.start(&origin.url("/playlist")).await.unwrap();
    let first = relay.resource(token(&playback), "0", None).await.unwrap();
    let old = uris(&first.body).remove(0);
    drop(first);
    let (a, b) = tokio::join!(
        relay.resource(token(&playback), resource_id(&old), None),
        relay.resource(token(&playback), resource_id(&old), None)
    );
    assert_eq!(a.unwrap().body, "first");
    assert_eq!(b.unwrap().body, "first");
    assert_eq!(origin.count("/segment.ts"), 1);
    let second = relay.resource(token(&playback), "0", None).await.unwrap();
    let new = uris(&second.body).remove(0);
    drop(second);
    assert_ne!(new, old);
    assert_eq!(
        relay
            .resource(token(&playback), resource_id(&new), None)
            .await
            .unwrap()
            .body,
        "reset"
    );
    assert_eq!(origin.count("/playlist"), 2);
    assert_eq!(origin.count("/segment.ts"), 2);
}

#[tokio::test]
async fn identical_reload_and_sliding_overlap_preserve_media_and_map_urls() {
    let relay = relay();
    let playback = relay.start("https://example.invalid/master").await.unwrap();
    let session = relay
        .sessions
        .lock()
        .unwrap()
        .get(token(&playback))
        .cloned()
        .unwrap();
    let base = FetchUrl::parse_source("https://example.invalid/master", relay.policy).unwrap();
    let first = b"#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:1,\na.ts\n#EXTINF:1,\nb.ts\n";
    let original = relay
        .rewrite(token(&playback), &session, 0, &base, first)
        .unwrap();
    let repeat = relay
        .rewrite(token(&playback), &session, 0, &base, first)
        .unwrap();
    assert_eq!(original, repeat);
    let later = b"#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:11\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:1,\nb.ts\n#EXTINF:1,\nc.ts\n";
    let advanced = relay
        .rewrite(token(&playback), &session, 0, &base, later)
        .unwrap();
    assert_eq!(uris(&original)[1], uris(&advanced)[0]);
    let map = |body: &Bytes| {
        std::str::from_utf8(body)
            .unwrap()
            .lines()
            .find(|line| line.starts_with("#EXT-X-MAP"))
            .unwrap()
            .to_owned()
    };
    assert_eq!(map(&original), map(&advanced));
    assert!(map(&original).contains(".mp4\""));
    assert!(uris(&original)[0].ends_with(".ts"));
    let rollback = relay
        .rewrite(token(&playback), &session, 0, &base, first)
        .unwrap();
    assert_ne!(uris(&original)[1], uris(&rollback)[1]);
    assert_ne!(map(&original), map(&rollback));
}

#[tokio::test]
async fn refresh_prevents_older_inflight_body_from_repopulating_the_cache() {
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let reads = Arc::new(AtomicUsize::new(0));
    let notify = started.clone();
    let unblock = release.clone();
    let count = reads.clone();
    let origin = Fixture::new(move |path, _| {
        if path == "/playlist" {
            return manifest("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:1\n#EXTINF:1,\nsegment.ts\n");
        }
        if count.fetch_add(1, Ordering::SeqCst) == 0 {
            notify.notify_one();
            let unblock = unblock.clone();
            return Response::builder()
                .header(axum::http::header::CACHE_CONTROL, "max-age=30")
                .body(Body::from_stream(futures::stream::once(async move {
                    unblock.notified().await;
                    Ok::<_, std::io::Error>(Bytes::from_static(b"old"))
                })))
                .unwrap();
        }
        media(b"new", "max-age=30")
    })
    .await;
    let relay = relay();
    let playback = relay.start(&origin.url("/playlist")).await.unwrap();
    let playlist = relay.resource(token(&playback), "0", None).await.unwrap();
    let segment = uris(&playlist.body).remove(0);
    drop(playlist);
    let worker = relay.clone();
    let viewer = token(&playback).to_owned();
    let id = resource_id(&segment).to_owned();
    let old = tokio::spawn(async move { worker.resource(&viewer, &id, None).await });
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    let refreshed = relay.resource(token(&playback), "0", None).await.unwrap();
    assert_eq!(uris(&refreshed.body)[0], segment);
    drop(refreshed);
    release.notify_one();
    assert_eq!(old.await.unwrap().unwrap().body, "old");
    assert!(relay.cache.lock().unwrap().entries.is_empty());
    assert_eq!(
        relay
            .resource(token(&playback), resource_id(&segment), None)
            .await
            .unwrap()
            .body,
        "new"
    );
    assert_eq!(reads.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn no_store_media_is_never_cached_and_viewers_stop_independently() {
    let origin = Fixture::new(|path, _| {
        if path == "/playlist" {
            manifest("#EXTM3U\n#EXTINF:1,\nsegment.ts\n")
        } else {
            media(b"data", "no-store")
        }
    })
    .await;
    let relay = relay();
    let first = relay.start(&origin.url("/playlist")).await.unwrap();
    let second = relay.start(&origin.url("/playlist")).await.unwrap();
    let manifest = relay.resource(token(&first), "0", None).await.unwrap();
    let segment = uris(&manifest.body).remove(0);
    drop(manifest);
    for _ in 0..2 {
        drop(
            relay
                .resource(token(&first), resource_id(&segment), None)
                .await
                .unwrap(),
        );
    }
    assert_eq!(origin.count("/segment.ts"), 2);
    assert!(relay.stop(token(&first)));
    assert!(!relay.stop(token(&first)));
    assert!(matches!(
        relay.resource(token(&first), "0", None).await,
        Err(OriginError::NotFound)
    ));
    assert!(relay.resource(token(&second), "0", None).await.is_ok());
}

#[tokio::test]
async fn stop_cancels_pending_body_and_prevents_late_registry_enrollment() {
    let started = Arc::new(tokio::sync::Notify::new());
    let notify = started.clone();
    let origin = Fixture::new(move |_, _| {
        notify.notify_one();
        Response::new(Body::from_stream(futures::stream::pending::<
            Result<Bytes, std::io::Error>,
        >()))
    })
    .await;
    let relay = relay();
    let playback = relay.start(&origin.url("/pending")).await.unwrap();
    let id = token(&playback).to_owned();
    let worker = relay.clone();
    let task = tokio::spawn(async move { worker.resource(&id, "0", None).await });
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    assert!(relay.stop(token(&playback)));
    let result = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        result,
        Err(OriginError::Cancelled) | Err(OriginError::Fetch(FetchError::Cancelled))
    ));
    assert!(relay.sessions.lock().unwrap().is_empty());
    assert!(relay.cache.lock().unwrap().entries.is_empty());
    assert_eq!(relay.admission.available_permits(), 16);
    assert_eq!(relay.fetches.available_permits(), 4);
}

#[tokio::test]
async fn cancelling_request_releases_all_owned_permits() {
    let started = Arc::new(tokio::sync::Notify::new());
    let notify = started.clone();
    let origin = Fixture::new(move |_, _| {
        notify.notify_one();
        Response::new(Body::from_stream(futures::stream::pending::<
            Result<Bytes, std::io::Error>,
        >()))
    })
    .await;
    let relay = relay();
    let playback = relay.start(&origin.url("/pending")).await.unwrap();
    let id = token(&playback).to_owned();
    let worker = relay.clone();
    let task = tokio::spawn(async move { worker.resource(&id, "0", None).await });
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    task.abort();
    match task.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("request should have been cancelled"),
    }
    let session = relay
        .sessions
        .lock()
        .unwrap()
        .get(token(&playback))
        .cloned()
        .unwrap();
    assert_eq!(session.inner.lock().unwrap().pending, 0);
    assert_eq!(relay.admission.available_permits(), 16);
    assert_eq!(relay.fetches.available_permits(), 4);
}

#[tokio::test(start_paused = true)]
async fn lease_capacity_idle_expiry_and_hard_deadline_are_bounded() {
    let relay = relay();
    let mut playbacks = Vec::new();
    for _ in 0..MAX_SESSIONS {
        playbacks.push(relay.start("http://example.invalid/media").await.unwrap());
    }
    assert!(matches!(
        relay.start("http://example.invalid/media").await,
        Err(OriginError::Busy)
    ));
    let held = relay
        .sessions
        .lock()
        .unwrap()
        .get(token(&playbacks[0]))
        .cloned()
        .unwrap();
    {
        let mut inner = held.inner.lock().unwrap();
        inner.pending = 1;
        inner.resources.get_mut(&0).unwrap().active = 1;
    }
    let guard = RequestGuard {
        session: held.clone(),
        id: 0,
    };
    tokio::task::yield_now().await;
    tokio::time::advance(IDLE_TIMEOUT + Duration::from_secs(1)).await;
    relay.purge();
    assert_eq!(relay.sessions.lock().unwrap().len(), 1);
    assert!(!*held.cancel.borrow());
    tokio::time::advance(HARD_TIMEOUT).await;
    relay.purge();
    assert!(relay.sessions.lock().unwrap().is_empty());
    assert!(*held.cancel.borrow());
    drop(guard);
}

#[tokio::test(start_paused = true)]
async fn waiting_for_same_resource_uses_one_request_deadline() {
    let relay = relay();
    let playback = relay
        .start("http://example.invalid/manifest")
        .await
        .unwrap();
    let session = relay
        .sessions
        .lock()
        .unwrap()
        .get(token(&playback))
        .cloned()
        .unwrap();
    let lock = session.inner.lock().unwrap().resources[&0].fetch.clone();
    let held = lock.lock().await;
    let cloned = relay.clone();
    let id = token(&playback).to_owned();
    let task = tokio::spawn(async move { cloned.resource(&id, "0", None).await });
    tokio::task::yield_now().await;
    tokio::time::advance(REQUEST_TIMEOUT + Duration::from_secs(1)).await;
    assert!(matches!(task.await.unwrap(), Err(OriginError::Timeout)));
    drop(held);
    assert_eq!(session.inner.lock().unwrap().pending, 0);
    assert_eq!(relay.admission.available_permits(), 16);
}

#[tokio::test(start_paused = true)]
async fn playlist_admission_is_atomic_and_long_live_registry_evicts_unowned_old_ids() {
    let relay = relay();
    let playback = relay
        .start("http://example.invalid/manifest")
        .await
        .unwrap();
    let session = relay
        .sessions
        .lock()
        .unwrap()
        .get(token(&playback))
        .cloned()
        .unwrap();
    let base = FetchUrl::parse_source("http://example.invalid/manifest", relay.policy).unwrap();
    let body = relay
        .rewrite(
            token(&playback),
            &session,
            0,
            &base,
            b"#EXTM3U\n#EXTINF:1,\na.ts\n",
        )
        .unwrap();
    let pinned = resource_id(&uris(&body)[0])
        .split('.')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    {
        let mut inner = session.inner.lock().unwrap();
        inner.pending += 1;
        inner.resources.get_mut(&pinned).unwrap().active += 1;
    }
    let guard = RequestGuard {
        session: session.clone(),
        id: pinned,
    };
    for number in 0..MAX_RESOURCES + 5 {
        tokio::time::advance(Duration::from_secs(3)).await;
        relay
            .rewrite(
                token(&playback),
                &session,
                0,
                &base,
                format!("#EXTM3U\n#EXTINF:1,\nsegment-{number}.ts\n").as_bytes(),
            )
            .unwrap();
    }
    let inner = session.inner.lock().unwrap();
    assert_eq!(inner.resources.len(), MAX_RESOURCES);
    assert!(inner.resources.contains_key(&0));
    assert!(inner.resources.contains_key(&pinned));
    assert!(!inner.resources.contains_key(&2));
    drop(inner);
    drop(guard);
    let before = session.inner.lock().unwrap().next_id;
    assert!(relay
        .rewrite(
            token(&playback),
            &session,
            0,
            &base,
            b"#EXTM3U\n#EXTINF:1,\na.ts\n#EXT-X-MAP:URI=\"file:///private\"\n"
        )
        .is_err());
    assert_eq!(session.inner.lock().unwrap().next_id, before);
    relay.stop(token(&playback));
    assert!(matches!(
        relay.rewrite(
            token(&playback),
            &session,
            0,
            &base,
            b"#EXTM3U\n#EXTINF:1,\na.ts\n"
        ),
        Err(OriginError::Cancelled)
    ));
}

#[tokio::test]
async fn declared_oversize_media_is_rejected_before_reading_body() {
    let origin = Fixture::new(|path, _| {
        if path == "/playlist" {
            manifest("#EXTM3U\n#EXTINF:1,\na.ts\n")
        } else {
            Response::builder()
                .header(axum::http::header::CONTENT_LENGTH, MAX_MEDIA_BYTES + 1)
                .body(Body::from_stream(futures::stream::pending::<
                    Result<Bytes, std::io::Error>,
                >()))
                .unwrap()
        }
    })
    .await;
    let relay = relay();
    let playback = relay.start(&origin.url("/playlist")).await.unwrap();
    let playlist = relay.resource(token(&playback), "0", None).await.unwrap();
    let uri = uris(&playlist.body).remove(0);
    drop(playlist);
    assert!(matches!(
        relay
            .resource(token(&playback), resource_id(&uri), None)
            .await,
        Err(OriginError::TooLarge)
    ));
}

#[tokio::test]
async fn origin_url_policy_and_unknown_resources_do_not_fetch() {
    let relay = OriginHls::new(FetchPolicy::default());
    assert!(matches!(
        relay.start("http://127.0.0.1/private").await,
        Err(OriginError::InvalidSource)
    ));
    assert!(matches!(
        relay.start("file:///private").await,
        Err(OriginError::InvalidSource)
    ));
    let playback = relay
        .start("https://example.invalid/signed?token=secret")
        .await
        .unwrap();
    assert!(matches!(
        relay.resource(token(&playback), "999", None).await,
        Err(OriginError::NotFound)
    ));
    assert!(matches!(
        relay
            .resource(token(&playback), "0", Some("bytes=1-0"))
            .await,
        Err(OriginError::InvalidRange)
    ));
    assert!(!OriginError::InvalidSource.to_string().contains("secret"));
}

#[tokio::test]
async fn cache_lifetime_does_not_restart_after_a_slow_body() {
    let origin = Fixture::new(|path, _| {
        if path == "/playlist" {
            manifest("#EXTM3U\n#EXTINF:1,\nsegment.ts\n")
        } else {
            Response::builder()
                .header(axum::http::header::CACHE_CONTROL, "max-age=1")
                .body(Body::from_stream(futures::stream::once(async {
                    tokio::time::sleep(Duration::from_millis(1100)).await;
                    Ok::<_, std::io::Error>(Bytes::from_static(b"slow"))
                })))
                .unwrap()
        }
    })
    .await;
    let relay = relay();
    let playback = relay.start(&origin.url("/playlist")).await.unwrap();
    let playlist = relay.resource(token(&playback), "0", None).await.unwrap();
    let uri = uris(&playlist.body).remove(0);
    drop(playlist);
    for _ in 0..2 {
        assert_eq!(
            relay
                .resource(token(&playback), resource_id(&uri), None)
                .await
                .unwrap()
                .body,
            "slow"
        );
    }
    assert_eq!(origin.count("/segment.ts"), 2);
    assert!(relay.cache.lock().unwrap().entries.is_empty());
}

#[tokio::test]
async fn response_stream_holds_admission_until_drop_and_stop_cancels_it() {
    use futures::StreamExt;
    let origin = Fixture::new(|_, _| manifest("#EXTM3U\n#EXTINF:1,\nsegment.ts\n")).await;
    let relay = relay();
    let playback = relay.start(&origin.url("/playlist")).await.unwrap();
    let response = relay.resource(token(&playback), "0", None).await.unwrap();
    assert_eq!(relay.admission.available_permits(), 15);
    assert_eq!(relay.fetches.available_permits(), 3);
    let mut stream = Box::pin(response.into_stream());
    relay.stop(token(&playback));
    assert!(stream.next().await.unwrap().is_err());
    assert!(stream.next().await.is_none());
    drop(stream);
    assert_eq!(relay.admission.available_permits(), 16);
    assert_eq!(relay.fetches.available_permits(), 4);
}

#[tokio::test]
async fn implicit_byte_ranges_follow_resolved_resource_identity() {
    let relay = relay();
    let playback = relay
        .start("http://example.invalid/manifest")
        .await
        .unwrap();
    let session = relay
        .sessions
        .lock()
        .unwrap()
        .get(token(&playback))
        .cloned()
        .unwrap();
    let base = FetchUrl::parse_source("http://example.invalid/manifest", relay.policy).unwrap();
    let same = b"#EXTM3U\n#EXTINF:1,\n#EXT-X-BYTERANGE:4@0\nfile.ts\n#EXTINF:1,\n#EXT-X-BYTERANGE:4\n./file.ts\n";
    let rendered = relay
        .rewrite(token(&playback), &session, 0, &base, same)
        .unwrap();
    let resources = uris(&rendered);
    assert_eq!(resources[0], resources[1]);
    let before = session.inner.lock().unwrap().next_id;
    let different = b"#EXTM3U\n#EXTINF:1,\n#EXT-X-BYTERANGE:4@0\nfile.ts\n#EXTINF:1,\n#EXT-X-BYTERANGE:4\nother.ts\n";
    assert!(matches!(
        relay.rewrite(token(&playback), &session, 0, &base, different),
        Err(OriginError::InvalidPlaylist)
    ));
    assert_eq!(session.inner.lock().unwrap().next_id, before);
}

#[tokio::test]
async fn malformed_media_range_is_a_client_error_without_upstream_fetch() {
    let origin = Fixture::new(|path, _| match path {
        "/playlist" => manifest("#EXTM3U\n#EXTINF:1,\nsegment.ts\n"),
        _ => panic!("invalid range must not reach the origin"),
    })
    .await;
    let relay = relay();
    let playback = relay.start(&origin.url("/playlist")).await.unwrap();
    let playlist = relay.resource(token(&playback), "0", None).await.unwrap();
    let segment = uris(&playlist.body).remove(0);
    drop(playlist);
    for range in ["bytes=-0", "bytes=2-1", "bytes=1-2,3-4", "invalid"] {
        let result = relay
            .resource(token(&playback), resource_id(&segment), Some(range))
            .await;
        assert!(matches!(result, Err(OriginError::InvalidRange)));
    }
    assert_eq!(origin.count("/segment.ts"), 0);
}

#[tokio::test]
async fn playlist_probe_ranges_return_the_full_rewritten_representation() {
    let origin = Fixture::new(|_, headers| {
        assert!(!headers.contains_key(axum::http::header::RANGE));
        manifest("#EXTM3U\n#EXTINF:1,\nsegment.ts\n")
    })
    .await;
    let relay = relay();
    let playback = relay.start(&origin.url("/playlist")).await.unwrap();
    for range in ["bytes=0-", "bytes=0-512", "bytes=-8"] {
        let response = relay
            .resource(token(&playback), "0", Some(range))
            .await
            .unwrap();
        assert_eq!(response.status, 200);
        assert!(response.content_range.is_none());
        assert_eq!(uris(&response.body).len(), 1);
    }
}

#[tokio::test]
async fn master_rendition_urls_survive_long_media_playlist_eviction() {
    let origin = Fixture::new(|path, _| match path {
        "/master" => manifest(
            "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\na\n#EXT-X-STREAM-INF:BANDWIDTH=2\nb\n",
        ),
        "/a" | "/b" => manifest("#EXTM3U\n#EXTINF:1,\nsegment.ts\n"),
        _ => StatusCode::NOT_FOUND.into_response(),
    })
    .await;
    let relay = relay();
    let playback = relay.start(&origin.url("/master")).await.unwrap();
    let manifest = relay.resource(token(&playback), "0", None).await.unwrap();
    let variants = uris(&manifest.body);
    drop(manifest);
    let session = relay
        .sessions
        .lock()
        .unwrap()
        .get(token(&playback))
        .cloned()
        .unwrap();
    let base = FetchUrl::parse_source(&origin.url("/a"), relay.policy).unwrap();
    tokio::time::pause();
    session.inner.lock().unwrap().idle = Instant::now() + HARD_TIMEOUT;
    for update in 0..8 {
        tokio::time::advance(Duration::from_secs(202)).await;
        let mut text = String::from("#EXTM3U\n");
        for segment in 0..200 {
            text.push_str(&format!("#EXTINF:1,\nsegment-{update}-{segment}.ts\n"));
        }
        relay
            .rewrite(token(&playback), &session, 0, &base, text.as_bytes())
            .unwrap();
    }
    assert!(session.inner.lock().unwrap().resources.len() <= MAX_RESOURCES);
    tokio::time::resume();
    for variant in variants {
        let response = relay
            .resource(token(&playback), resource_id(&variant), None)
            .await
            .unwrap();
        assert_eq!(uris(&response.body).len(), 1);
    }
    assert_eq!(origin.count("/b"), 1);
}

#[tokio::test(start_paused = true)]
async fn every_current_rendition_snapshot_is_pinned_and_overflow_is_atomic() {
    let relay = relay();
    let playback = relay.start("https://example.invalid/master").await.unwrap();
    let session = relay
        .sessions
        .lock()
        .unwrap()
        .get(token(&playback))
        .cloned()
        .unwrap();
    let base = FetchUrl::parse_source("https://example.invalid/master", relay.policy).unwrap();
    let master = relay.rewrite(token(&playback), &session, 0, &base, b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\na.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=2\nb.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=3\nc.m3u8\n").unwrap();
    let parents: Vec<_> = uris(&master).iter().map(|uri| numeric_id(uri)).collect();
    let playlist = |count, sequence| {
        let mut text = format!("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:{sequence}\n");
        for n in 0..count {
            text.push_str(&format!("#EXTINF:1,\n{n}.ts\n"));
        }
        text
    };
    let a = relay
        .rewrite(
            token(&playback),
            &session,
            parents[0],
            &base,
            playlist(500, 0).as_bytes(),
        )
        .unwrap();
    let b = relay
        .rewrite(
            token(&playback),
            &session,
            parents[1],
            &base,
            playlist(500, 0).as_bytes(),
        )
        .unwrap();
    let before = session.inner.lock().unwrap().next_id;
    assert!(matches!(
        relay.rewrite(
            token(&playback),
            &session,
            parents[2],
            &base,
            playlist(30, 0).as_bytes()
        ),
        Err(OriginError::TooLarge)
    ));
    {
        let inner = session.inner.lock().unwrap();
        assert_eq!(inner.next_id, before);
        for id in uris(&a).iter().chain(uris(&b).iter()) {
            assert!(inner.resources.contains_key(&numeric_id(id)));
        }
    }
    relay
        .rewrite(
            token(&playback),
            &session,
            parents[0],
            &base,
            playlist(1, 500).as_bytes(),
        )
        .unwrap();
    assert!(matches!(
        relay.rewrite(
            token(&playback),
            &session,
            parents[2],
            &base,
            playlist(30, 0).as_bytes()
        ),
        Err(OriginError::TooLarge)
    ));
    session.inner.lock().unwrap().idle = Instant::now() + HARD_TIMEOUT;
    tokio::time::advance(Duration::from_secs(502)).await;
    relay
        .rewrite(
            token(&playback),
            &session,
            parents[2],
            &base,
            playlist(30, 0).as_bytes(),
        )
        .unwrap();
    let inner = session.inner.lock().unwrap();
    for id in uris(&b) {
        assert!(inner.resources.contains_key(&numeric_id(&id)));
    }
}

#[tokio::test]
async fn media_suffixes_are_bounded_and_cannot_select_another_resource() {
    for (path, expected) in [
        ("part.TS?sig=.mp4", Some(".ts")),
        ("part.m4s", Some(".m4s")),
        ("part.mp4", Some(".mp4")),
        ("part?file=.ts", None),
        ("part.html", None),
        ("ts", None),
    ] {
        let url = FetchUrl::parse_source(
            &format!("https://example.invalid/{path}"),
            FetchPolicy::default(),
        )
        .unwrap();
        assert_eq!(url.safe_media_extension(), expected);
    }
    let relay = relay();
    let playback = relay.start("https://example.invalid/master").await.unwrap();
    assert!(playback.playback_url.ends_with("/0.m3u8"));
    for id in ["0.ts", "0.m3u8.ts", "+0.m3u8", "0.html"] {
        assert!(matches!(
            relay.resource(token(&playback), id, None).await,
            Err(OriginError::NotFound)
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn removed_media_and_maps_keep_grace_without_mutating_failed_admission() {
    let relay = relay();
    let playback = relay
        .start("https://example.invalid/playlist")
        .await
        .unwrap();
    let session = relay
        .sessions
        .lock()
        .unwrap()
        .get(token(&playback))
        .cloned()
        .unwrap();
    session.inner.lock().unwrap().idle = Instant::now() + HARD_TIMEOUT;
    let base = FetchUrl::parse_source("https://example.invalid/playlist", relay.policy).unwrap();
    let playlist = |prefix, sequence| {
        let mut text = format!(
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:{sequence}\n#EXT-X-MAP:URI=\"{prefix}-init.mp4\"\n"
        );
        for n in 0..400 {
            text.push_str(&format!("#EXTINF:1,\n{prefix}-{n}.m4s\n"));
        }
        text
    };
    let first = relay
        .rewrite(
            token(&playback),
            &session,
            0,
            &base,
            playlist("a", 0).as_bytes(),
        )
        .unwrap();
    relay
        .rewrite(
            token(&playback),
            &session,
            0,
            &base,
            playlist("b", 400).as_bytes(),
        )
        .unwrap();
    let before = session.inner.lock().unwrap().next_id;
    let deadline = session
        .inner
        .lock()
        .unwrap()
        .resources
        .get(&1)
        .unwrap()
        .grace_until
        .unwrap();
    for seconds in [0, 400] {
        tokio::time::advance(Duration::from_secs(seconds)).await;
        assert!(matches!(
            relay.rewrite(
                token(&playback),
                &session,
                0,
                &base,
                playlist("c", 800).as_bytes()
            ),
            Err(OriginError::TooLarge)
        ));
        let inner = session.inner.lock().unwrap();
        assert_eq!(inner.next_id, before);
        assert_eq!(inner.resources.get(&1).unwrap().grace_until, Some(deadline));
        assert_eq!(
            inner.resources.get(&0).unwrap().playlist.first_sequence,
            Some(400)
        );
        for uri in uris(&first) {
            assert!(inner.resources.contains_key(&numeric_id(&uri)));
        }
    }
    tokio::time::advance(Duration::from_secs(2)).await;
    relay
        .rewrite(
            token(&playback),
            &session,
            0,
            &base,
            playlist("c", 800).as_bytes(),
        )
        .unwrap();
    let inner = session.inner.lock().unwrap();
    assert!(inner.resources.len() <= MAX_RESOURCES);
    assert!(!inner.resources.contains_key(&1));
}

#[tokio::test]
async fn url_registry_limit_rejects_an_entire_update_atomically() {
    let relay = relay();
    let playback = relay
        .start("http://example.invalid/manifest")
        .await
        .unwrap();
    let session = relay
        .sessions
        .lock()
        .unwrap()
        .get(token(&playback))
        .cloned()
        .unwrap();
    let base = FetchUrl::parse_source("http://example.invalid/manifest", relay.policy).unwrap();
    let mut playlist = String::from("#EXTM3U\n");
    for n in 0..70 {
        playlist.push_str(&format!("#EXTINF:1,\n/{n}{}\n", "x".repeat(8000)));
    }
    assert!(matches!(
        relay.rewrite(token(&playback), &session, 0, &base, playlist.as_bytes()),
        Err(OriginError::TooLarge)
    ));
    let inner = session.inner.lock().unwrap();
    assert_eq!(inner.resources.len(), 1);
    assert_eq!(inner.next_id, 1);
}

#[test]
fn cache_bounds_entries_and_memory_including_zero_length_resources() {
    let mut cache = Cache::default();
    for id in 0..MAX_CACHE_ENTRIES + 2 {
        cache.put(
            CacheKey {
                token: "synthetic".into(),
                id: id as u64,
                range: None,
            },
            ResponseData {
                status: 200,
                content_type: "video/mp2t".into(),
                content_range: None,
                body: Bytes::new(),
            },
            Duration::from_secs(10),
        );
    }
    assert_eq!(cache.entries.len(), MAX_CACHE_ENTRIES);
    let data = Bytes::from(vec![0_u8; 4 * 1024 * 1024]);
    for id in 0..6 {
        cache.put(
            CacheKey {
                token: "large".into(),
                id,
                range: None,
            },
            ResponseData {
                status: 200,
                content_type: "video/mp2t".into(),
                content_range: None,
                body: data.clone(),
            },
            Duration::from_secs(10),
        );
    }
    assert!(cache.bytes <= MAX_CACHE_BYTES);
    assert_eq!(cache.bytes, MAX_CACHE_BYTES);
}

struct TempMedia(std::path::PathBuf);
impl TempMedia {
    fn new() -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("outpace-origin-hls-{}-{nonce}", std::process::id()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for TempMedia {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn ffmpeg(args: &[String]) -> std::process::Output {
    let program = std::env::var_os("FFMPEG").unwrap_or_else(|| "ffmpeg".into());
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::process::Command::new(program)
            .args(args)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("bounded FFmpeg fixture deadline")
    .expect("FFmpeg executable is available");
    assert!(
        result.status.success(),
        "FFmpeg failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(result.stdout.len() < 1024 * 1024 && result.stderr.len() < 1024 * 1024);
    result
}

#[tokio::test]
#[ignore = "requires FFmpeg; release HLS qualification runs this on generated loopback media"]
async fn generated_origin_hls_ts_and_fmp4_decode() {
    use crate::http::{router, AceSessionStore, AppState};
    use crate::manager::StreamManager;
    use crate::provider::ProviderRegistry;
    for (container, suffix) in [("mpegts", "ts"), ("fmp4", "m4s")] {
        let files = TempMedia::new();
        let manifest_path = files.0.join("index.m3u8");
        let segments = files.0.join(format!("segment%03d.{suffix}"));
        let mut args: Vec<String> = [
            "-hide_banner",
            "-v",
            "error",
            "-nostdin",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=160x90:rate=10",
            "-t",
            "3",
            "-an",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-pix_fmt",
            "yuv420p",
            "-g",
            "10",
            "-keyint_min",
            "10",
            "-sc_threshold",
            "0",
            "-f",
            "hls",
            "-hls_time",
            "1",
            "-hls_playlist_type",
            "vod",
            "-hls_segment_type",
            container,
            "-hls_flags",
            "independent_segments",
            "-hls_segment_filename",
        ]
        .iter()
        .map(|s| (*s).into())
        .collect();
        args.push(segments.to_str().unwrap().into());
        args.push(manifest_path.to_str().unwrap().into());
        ffmpeg(&args).await;
        let resources: HashMap<String, Bytes> = std::fs::read_dir(&files.0)
            .unwrap()
            .map(|file| {
                let file = file.unwrap();
                assert!(file.file_type().unwrap().is_file());
                let bytes = std::fs::read(file.path()).unwrap();
                assert!(bytes.len() < MAX_MEDIA_BYTES);
                (
                    format!("/{}", file.file_name().to_str().unwrap()),
                    Bytes::from(bytes),
                )
            })
            .collect();
        let origin = Fixture::new(move |path, _| match resources.get(path) {
            Some(bytes) => (
                [(
                    axum::http::header::CONTENT_TYPE,
                    if path.ends_with("m3u8") {
                        "application/vnd.apple.mpegurl"
                    } else if path.ends_with("ts") {
                        "video/mp2t"
                    } else {
                        "video/mp4"
                    },
                )],
                bytes.clone(),
            )
                .into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        })
        .await;
        let relay = relay();
        let playback = relay.start(&origin.url("/index.m3u8")).await.unwrap();
        let state = AppState {
            peer_policy: Default::default(),
            manager: StreamManager::new(ProviderRegistry::new()),
            networks: vec![],
            resolve_content_ids_in_getstream: false,
            ace_sessions: Arc::new(AceSessionStore::default()),
            experimental_ace_compat: false,
            broadcasts: None,
            origin_hls: Some(relay.clone()),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move { axum::serve(listener, router(state)).await.unwrap() });
        struct Abort(tokio::task::AbortHandle);
        impl Drop for Abort {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _server = Abort(task.abort_handle());
        let input = format!("http://{address}{}", playback.playback_url);
        let decode: Vec<String> = [
            "-hide_banner",
            "-v",
            "error",
            "-nostdin",
            "-i",
            &input,
            "-map",
            "0:v:0",
            "-frames:v",
            "30",
            "-progress",
            "pipe:1",
            "-nostats",
            "-f",
            "null",
            "-",
        ]
        .iter()
        .map(|s| (*s).into())
        .collect();
        let result = ffmpeg(&decode).await;
        let progress = String::from_utf8(result.stdout).unwrap();
        let frames = progress
            .lines()
            .filter_map(|line| line.strip_prefix("frame=")?.trim().parse::<u32>().ok())
            .max()
            .unwrap_or(0);
        assert_eq!(
            frames, 30,
            "all generated {container} frames decode via the managed HTTP relay"
        );
        assert!(relay.stop(token(&playback)));
        assert!(relay.sessions.lock().unwrap().is_empty());
        eprintln!("managed origin HLS {container}: {frames} decoded frames");
    }
}
