use super::*;
use flate2::{write::GzEncoder, Compression};
use std::io::Write;

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

fn compressed(bytes: Vec<u8>) -> Response {
    Response::builder()
        .header(axum::http::header::CONTENT_ENCODING, "gzip")
        .header(axum::http::header::CONTENT_LENGTH, bytes.len())
        .body(Body::from(bytes))
        .unwrap()
}

#[tokio::test]
async fn gzip_redirected_master_and_child_rewrite_to_identity_and_keep_signed_urls() {
    let origin = Fixture::new(|path, headers| {
        assert_eq!(
            headers[axum::http::header::ACCEPT_ENCODING],
            if path.contains(".ts") {
                "identity"
            } else {
                "gzip, identity"
            }
        );
        assert!(headers.get(axum::http::header::RANGE).is_none());
        match path {
            "/entry" => (
                StatusCode::FOUND,
                [
                    (axum::http::header::LOCATION, "/cdn/master?root=private"),
                    (axum::http::header::CONTENT_ENCODING, "gzip"),
                ],
            )
                .into_response(),
            "/cdn/master?root=private" => compressed(gzip(
                b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nchild?sig=a%2Fb\n",
            )),
            "/cdn/child?sig=a%2Fb" => compressed(gzip(
                b"#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2,\nseg.ts?sig=c%2Fd\n#EXT-X-ENDLIST\n",
            )),
            "/cdn/seg.ts?sig=c%2Fd" => media(b"synthetic media", "no-store"),
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    })
    .await;
    let relay = relay();
    let playback = relay.start(&origin.url("/entry")).await.unwrap();
    let master = relay
        .resource(token(&playback), "0", Some("bytes=0-"))
        .await
        .unwrap();
    assert!(master.body.starts_with(b"#EXTM3U\n"));
    assert_eq!(master.content_length, master.body.len() as u64);
    assert_eq!(master.content_type, "application/vnd.apple.mpegurl");
    let child = uris(&master.body).remove(0);
    drop(master);
    let playlist = relay
        .resource(token(&playback), resource_id(&child), None)
        .await
        .unwrap();
    assert_eq!(playlist.content_length, playlist.body.len() as u64);
    let segment = uris(&playlist.body).remove(0);
    drop(playlist);
    let data = relay
        .resource(token(&playback), resource_id(&segment), None)
        .await
        .unwrap();
    assert_eq!(data.body, "synthetic media");
    assert_eq!(origin.count("/cdn/child?sig=a%2Fb"), 1);
    assert_eq!(origin.count("/cdn/seg.ts?sig=c%2Fd"), 1);
}

#[tokio::test]
async fn corrupt_gzip_never_enrolls_resources_and_a_later_valid_reload_recovers() {
    let good = gzip(b"#EXTM3U\n#EXTINF:2,\nseg.ts\n");
    let mut bad = good.clone();
    let index = bad.len() - 8;
    bad[index] ^= 1;
    let requests = AtomicUsize::new(0);
    let origin = Fixture::new(move |_, _| {
        compressed(if requests.fetch_add(1, Ordering::SeqCst) == 0 {
            bad.clone()
        } else {
            good.clone()
        })
    })
    .await;
    let relay = relay();
    let playback = relay.start(&origin.url("/manifest")).await.unwrap();
    assert!(matches!(
        relay.resource(token(&playback), "0", None).await,
        Err(OriginError::InvalidPlaylist)
    ));
    let session = relay.sessions.lock().unwrap()[token(&playback)].clone();
    assert_eq!(session.inner.lock().unwrap().resources.len(), 1);
    assert_eq!(session.inner.lock().unwrap().pending, 0);
    assert_eq!(relay.admission.available_permits(), 16);
    assert_eq!(relay.fetches.available_permits(), 4);
    assert!(relay.cache.lock().unwrap().entries.is_empty());
    let result = relay.resource(token(&playback), "0", None).await.unwrap();
    assert_eq!(uris(&result.body).len(), 1);
    assert_eq!(session.inner.lock().unwrap().resources.len(), 2);
}

#[tokio::test]
async fn gzip_wire_and_decoded_caps_are_independent_with_and_without_content_length() {
    let bomb = gzip(&vec![b'x'; playlist::MAX_PLAYLIST_BYTES + 1]);
    assert!(bomb.len() < playlist::MAX_PLAYLIST_BYTES);
    let oversized_wire = vec![0; playlist::MAX_PLAYLIST_BYTES + 1];
    let origin = Fixture::new(move |path, _| match path {
        "/decoded" => compressed(bomb.clone()),
        "/wire" => compressed(oversized_wire.clone()),
        "/declared" => Response::builder()
            .header(axum::http::header::CONTENT_ENCODING, "gzip")
            .header(
                axum::http::header::CONTENT_LENGTH,
                playlist::MAX_PLAYLIST_BYTES + 1,
            )
            .body(Body::from_stream(futures::stream::pending::<
                Result<Bytes, std::io::Error>,
            >()))
            .unwrap(),
        _ => Response::builder()
            .header(axum::http::header::CONTENT_ENCODING, "gzip")
            .body(Body::from_stream(futures::stream::iter(
                oversized_wire
                    .chunks(16384)
                    .map(|bytes| Ok::<_, std::io::Error>(Bytes::copy_from_slice(bytes)))
                    .collect::<Vec<_>>(),
            )))
            .unwrap(),
    })
    .await;
    let relay = relay();
    for path in ["/decoded", "/wire", "/declared", "/chunked"] {
        let playback = relay.start(&origin.url(path)).await.unwrap();
        assert!(
            matches!(
                relay.resource(token(&playback), "0", None).await,
                Err(OriginError::TooLarge)
            ),
            "{path}"
        );
        let session = relay.sessions.lock().unwrap()[token(&playback)].clone();
        assert_eq!(session.inner.lock().unwrap().resources.len(), 1);
        assert_eq!(relay.admission.available_permits(), 16);
        assert_eq!(relay.fetches.available_permits(), 4);
    }
}

#[tokio::test]
async fn gzip_media_is_rejected_even_after_a_valid_gzip_playlist() {
    let origin = Fixture::new(|path, _| {
        if path == "/manifest" {
            compressed(gzip(b"#EXTM3U\n#EXTINF:2,\nseg.ts\n"))
        } else {
            compressed(gzip(b"synthetic media"))
        }
    })
    .await;
    let relay = relay();
    let playback = relay.start(&origin.url("/manifest")).await.unwrap();
    let manifest = relay.resource(token(&playback), "0", None).await.unwrap();
    let segment = uris(&manifest.body).remove(0);
    drop(manifest);
    for range in [None, Some("bytes=0-")] {
        assert!(matches!(
            relay
                .resource(token(&playback), resource_id(&segment), range)
                .await,
            Err(OriginError::Fetch(FetchError::UnsupportedResponse))
        ));
    }
}
