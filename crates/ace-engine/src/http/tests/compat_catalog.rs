use super::*;
use crate::ace_provider::{AceProvider, CatalogFetch};
use crate::broadcast::BroadcastRegistry;
use crate::config::{LiveRecoveryConfig, StartupBufferConfig};
use crate::manager::StreamManager;
use crate::provider::{ProviderError, TsSource};
use ace_peer::session::PeerSession;
use ace_swarm::listen::{PeerListener, SeedRegistry};
use ace_swarm::resolve::{stream_info_from_transport, ResolveError};
use ace_swarm::types::StreamInfo;
use ace_wire::bencode::Bencode;
use ace_wire::extended::{LivePosition, NodeFields, OutgoingExtendedHandshake};
use ace_wire::handshake::random_peer_id;
use ace_wire::identity::Identity;
use ace_wire::infohash::transport_file_hash;
use ace_wire::live_auth::LiveSourceAuth;
use ace_wire::live_codec::build_piece;
use ace_wire::message::PeerMessage;
use async_trait::async_trait;
use axum::http::header;
use futures::future::BoxFuture;
use futures::StreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;

const SYNTHETIC_CID: &str = "0123456789abcdef0123456789abcdef01234567";

struct MetadataPeerGuard(tokio::task::JoinHandle<()>);

impl Drop for MetadataPeerGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn serve_metadata_peer(
    key: [u8; 20],
    transport: Vec<u8>,
) -> (std::net::SocketAddrV4, MetadataPeerGuard) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!("test metadata peer must bind IPv4 loopback");
    };
    let registry = SeedRegistry::new();
    registry.register_metadata(key, transport);
    let server = tokio::spawn(PeerListener::serve(
        listener,
        registry,
        random_peer_id(),
        [0; 8],
        4,
        Arc::new(Identity::generate()),
        4,
        None,
    ));
    (addr, MetadataPeerGuard(server))
}

fn ace_state(provider: AceProvider, resolve_content_ids_in_getstream: bool) -> AppState {
    ace_app_state(Arc::new(provider), resolve_content_ids_in_getstream)
}

fn ace_app_state(
    provider: Arc<dyn StreamProvider>,
    resolve_content_ids_in_getstream: bool,
) -> AppState {
    let mut registry = ProviderRegistry::new();
    registry.register(provider);
    AppState {
        manager: StreamManager::new(registry),
        networks: vec!["ace".into()],
        resolve_content_ids_in_getstream,
        ace_sessions: Arc::new(AceSessionStore::default()),
        experimental_ace_compat: true,
        broadcasts: None,
    }
}

fn synthetic_transport_with_auth(piece_length: i64) -> (Vec<u8>, LiveSourceAuth) {
    synthetic_transport_with_auth_named(piece_length, "Synthetic Live")
}

fn synthetic_transport_with_auth_named(piece_length: i64, name: &str) -> (Vec<u8>, LiveSourceAuth) {
    let auth = LiveSourceAuth::generate();
    let mut descriptor = std::collections::BTreeMap::new();
    descriptor.insert(b"name".to_vec(), Bencode::Bytes(name.as_bytes().to_vec()));
    descriptor.insert(b"piece_length".to_vec(), Bencode::Int(piece_length));
    descriptor.insert(b"chunk_length".to_vec(), Bencode::Int(16_384));
    descriptor.insert(b"bitrate".to_vec(), Bencode::Int(1_000_000));
    descriptor.insert(b"authmethod".to_vec(), Bencode::Bytes(b"RSA".to_vec()));
    descriptor.insert(b"pubkey".to_vec(), Bencode::Bytes(auth.pubkey_der()));
    descriptor.insert(
        b"trackers".to_vec(),
        Bencode::List(vec![Bencode::Bytes(b"udp://tracker.invalid:80".to_vec())]),
    );
    (
        ace_wire::transport::encode_transport(&Bencode::Dict(descriptor)),
        auth,
    )
}

async fn wait_for_catalog_waiters(provider: &AceProvider, expected: usize) -> bool {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if provider.catalog_pending_waiters_for_test(SYNTHETIC_CID) == Some(expected) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok()
}

fn returned_playback_path(response: &serde_json::Value) -> Result<String, String> {
    response["response"]["playback_url"]
        .as_str()
        .ok_or_else(|| "JSON response did not include a playback URL".to_string())?
        .strip_prefix("http://localhost:6900")
        .map(str::to_owned)
        .ok_or_else(|| "playback URL did not use the test router origin".to_string())
}

fn assert_valid_fixture_ts(media: &[u8]) {
    assert!(media.len() >= 188);
    let (packets, remainder) = media.as_chunks::<188>();
    assert!(remainder.is_empty());
    assert!(packets.iter().all(|packet| packet[0] == 0x47));
    assert!(
        packets
            .iter()
            .any(|packet| packet == &super::FIXTURE[9_400..9_588]),
        "media did not contain the fixture access-point packet"
    );
}

fn signed_wire_piece(media: &[u8], auth: &LiveSourceAuth, piece_length: usize) -> Vec<u8> {
    let payload_length = piece_length - auth.signature_len();
    let mut wire_piece = vec![0; piece_length];
    for (index, byte) in wire_piece[..payload_length].iter_mut().enumerate() {
        *byte = media[index % media.len()];
    }
    let signature = auth.sign(&wire_piece[..payload_length]);
    wire_piece[payload_length..].copy_from_slice(&signature);
    wire_piece
}

async fn collect_until_fixture_marker(
    stream: &mut axum::body::BodyDataStream,
    marker: &[u8],
) -> Result<Bytes, String> {
    let mut observed = Vec::new();
    loop {
        let chunk = stream
            .next()
            .await
            .ok_or_else(|| "surviving JSON URL ended before post-drop media".to_string())?
            .map_err(|error| error.to_string())?;
        observed.extend_from_slice(&chunk);
        if observed
            .windows(marker.len())
            .any(|window| window == marker)
        {
            return Ok(Bytes::from(observed));
        }
    }
}

async fn consume_open_post_drop_fixture_media(
    mut direct_stream: axum::body::BodyDataStream,
    mut playback_stream: axum::body::BodyDataStream,
    direct_session: Arc<crate::session::StreamSession>,
    playback_session: Arc<crate::session::StreamSession>,
    post_drop_piece_requested: tokio::sync::oneshot::Receiver<()>,
    release_post_drop_piece_flag: Arc<std::sync::atomic::AtomicBool>,
) -> Result<(Bytes, Bytes, Bytes, u64, u64, bool), String> {
    let same_session = Arc::ptr_eq(&direct_session, &playback_session);
    let subscribers_before_drop = direct_session.subscriber_count();
    let direct_initial = tokio::time::timeout(Duration::from_secs(5), direct_stream.next())
        .await
        .map_err(|_| "direct media exceeded its watchdog")?
        .ok_or_else(|| "direct response ended before initial media".to_string())?
        .map_err(|error| error.to_string())?;
    let playback_initial = tokio::time::timeout(Duration::from_secs(5), playback_stream.next())
        .await
        .map_err(|_| "JSON playback media exceeded its watchdog")?
        .ok_or_else(|| "JSON playback ended before initial media".to_string())?
        .map_err(|error| error.to_string())?;
    drop(direct_stream);
    let subscribers_after_drop = playback_session.subscriber_count();
    release_post_drop_piece_flag.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(5), post_drop_piece_requested)
        .await
        .map_err(|_| "ACE source did not request the released post-drop piece")?
        .map_err(|error| format!("loopback post-drop request barrier closed: {error}"))?;
    let marker = &super::FIXTURE[9_588..9_776];
    let post_drop_media = tokio::time::timeout(
        Duration::from_secs(5),
        collect_until_fixture_marker(&mut playback_stream, marker),
    )
    .await
    .map_err(|_| "surviving JSON URL post-drop media exceeded its 5-second watchdog")??;
    drop(playback_stream);
    Ok((
        direct_initial,
        playback_initial,
        post_drop_media,
        subscribers_before_drop,
        subscribers_after_drop,
        same_session,
    ))
}

async fn open_and_consume_post_drop_fixture_media(
    app: axum::Router,
    fetch: CatalogFetch,
    manager: Arc<StreamManager>,
    direct_path: String,
    playback_path: String,
    post_drop_piece_requested: tokio::sync::oneshot::Receiver<()>,
    release_post_drop_piece_flag: Arc<std::sync::atomic::AtomicBool>,
) -> Result<(Bytes, Bytes, Bytes, u64, u64, bool), String> {
    let direct_stream =
        direct_media_stream_request(app.clone(), fetch.clone(), direct_path).await?;
    let playback_stream = direct_media_stream_request(app.clone(), fetch, playback_path).await?;
    let direct_session = manager
        .get("ace", &format!("cid:{SYNTHETIC_CID}"))
        .await
        .ok_or_else(|| "direct media did not create its cid session".to_string())?;
    let playback_session = manager
        .get("ace", &format!("cid:{SYNTHETIC_CID}"))
        .await
        .ok_or_else(|| "JSON playback did not use the cid session".to_string())?;
    consume_open_post_drop_fixture_media(
        direct_stream,
        playback_stream,
        direct_session,
        playback_session,
        post_drop_piece_requested,
        release_post_drop_piece_flag,
    )
    .await
}

async fn serve_media_peer(
    listener: tokio::net::TcpListener,
    media: Vec<u8>,
    auth: LiveSourceAuth,
    accepted: Arc<AtomicUsize>,
) {
    serve_media_peer_controlled(listener, media, auth, accepted, 524_288, None).await;
}

struct PostDropPiece {
    piece: u32,
    media: Vec<u8>,
    requested: Option<tokio::sync::oneshot::Sender<()>>,
    release_flag: Arc<std::sync::atomic::AtomicBool>,
    sent_chunks: Arc<AtomicUsize>,
    release_observed: Arc<AtomicUsize>,
    released: bool,
}

async fn serve_media_peer_with_post_drop_piece(
    listener: tokio::net::TcpListener,
    auth: LiveSourceAuth,
    accepted: Arc<AtomicUsize>,
    requested: tokio::sync::oneshot::Sender<()>,
    release_flag: Arc<std::sync::atomic::AtomicBool>,
    sent_chunks: Arc<AtomicUsize>,
    release_observed: Arc<AtomicUsize>,
) {
    serve_media_peer_controlled(
        listener,
        super::FIXTURE[..9_588].to_vec(),
        auth,
        accepted,
        16_384,
        Some(PostDropPiece {
            piece: 11,
            media: super::FIXTURE[..9_776].to_vec(),
            requested: Some(requested),
            release_flag,
            sent_chunks,
            release_observed,
            released: false,
        }),
    )
    .await;
}

async fn serve_media_peer_controlled(
    listener: tokio::net::TcpListener,
    media: Vec<u8>,
    auth: LiveSourceAuth,
    accepted: Arc<AtomicUsize>,
    piece_length: usize,
    mut post_drop_piece: Option<PostDropPiece>,
) {
    let Ok((stream, _)) = listener.accept().await else {
        return;
    };
    accepted.fetch_add(1, Ordering::SeqCst);
    let mut session = PeerSession::new(stream);
    if session.accept_handshake([0; 20], |_| true).await.is_err() {
        return;
    }
    if session
        .send_extended_handshake(&OutgoingExtendedHandshake {
            ace_metadata_version: 1,
            ut_metadata_id: 2,
            mi: Some(LivePosition {
                min_piece: 7,
                max_piece: 10,
                position: -1,
                distance_from_source: 1,
            }),
            node: NodeFields::default(),
            peer_ip: None,
            metadata_size: None,
        })
        .await
        .is_err()
    {
        return;
    }
    if session.send(&PeerMessage::Unchoke).await.is_err() {
        return;
    }
    let wire_piece = signed_wire_piece(&media, &auth, piece_length);
    let post_drop_wire_piece = post_drop_piece
        .as_ref()
        .map(|piece| signed_wire_piece(&piece.media, &auth, piece_length));
    loop {
        if let Some(control) = post_drop_piece.as_mut() {
            if !control.released && control.release_flag.load(Ordering::SeqCst) {
                if session
                    .send(&ace_wire::live_codec::live_have(control.piece))
                    .await
                    .is_err()
                {
                    return;
                }
                control.released = true;
                control.release_observed.fetch_add(1, Ordering::SeqCst);
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(2)) => {}
            extra = listener.accept() => {
                if let Ok((stream, _)) = extra {
                    accepted.fetch_add(1, Ordering::SeqCst);
                    drop(stream);
                }
            }
            message = session.read_message() => {
                let Ok(message) = message else { return; };
                if let PeerMessage::Unknown { id: 6, payload } = message {
                    if payload.len() < 10 {
                        return;
                    }
                    let piece = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                    let chunk = u16::from_be_bytes(payload[8..10].try_into().unwrap());
                    let post_drop = match (&post_drop_piece, &post_drop_wire_piece) {
                        (Some(control), Some(post_wire)) if piece >= control.piece && control.released => post_wire,
                        _ => &wire_piece,
                    };
                    let start = usize::from(chunk) * 16_384;
                    if session
                        .send(&build_piece(0, piece, chunk, [0; 8], &post_drop[start..start + 16_384]))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    if piece >= 11 {
                        if let Some(control) = post_drop_piece.as_mut().filter(|control| control.released) {
                            control.sent_chunks.fetch_add(1, Ordering::SeqCst);
                            if let Some(requested) = control.requested.take() {
                                let _ = requested.send(());
                            }
                        }
                    }
                }
            }
        }
    }
}

fn counting_catalog(info: StreamInfo, polls: Arc<AtomicUsize>) -> CatalogFetch {
    Arc::new(move |_cid: String| {
        let info = info.clone();
        let polls = polls.clone();
        Box::pin(async move {
            polls.fetch_add(1, Ordering::SeqCst);
            Ok::<StreamInfo, ResolveError>(info)
        })
    })
}

async fn direct_media_request(
    app: axum::Router,
    fetch: CatalogFetch,
    path: String,
) -> Result<Bytes, String> {
    direct_media_stream_request(app, fetch, path)
        .await?
        .next()
        .await
        .ok_or_else(|| "direct response ended before media".to_string())?
        .map_err(|error| error.to_string())
}

async fn direct_media_stream_request(
    app: axum::Router,
    fetch: CatalogFetch,
    path: String,
) -> Result<axum::body::BodyDataStream, String> {
    let request = Request::get(path)
        .header(header::HOST, "localhost:6900")
        .body(Body::empty())
        .map_err(|error| error.to_string())?;
    crate::ace_provider::with_catalog_fetch_for_test(fetch, async move {
        let response = app
            .oneshot(request)
            .await
            .map_err(|error| error.to_string())?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "unexpected direct response status: {}",
                response.status()
            ));
        }
        if !response.headers()[header::CONTENT_TYPE]
            .to_str()
            .map_err(|error| error.to_string())?
            .starts_with("video/mp2t")
        {
            return Err("direct response did not use video/mp2t".to_string());
        }
        Ok(response.into_body().into_data_stream())
    })
    .await
}

async fn direct_response_headers(
    app: axum::Router,
    fetch: CatalogFetch,
    path: String,
) -> Result<(StatusCode, String), String> {
    let request = Request::get(path)
        .header(header::HOST, "localhost:6900")
        .body(Body::empty())
        .map_err(|error| error.to_string())?;
    crate::ace_provider::with_catalog_fetch_for_test(fetch, async move {
        let response = app
            .oneshot(request)
            .await
            .map_err(|error| error.to_string())?;
        let status = response.status();
        let content_type = response.headers()[header::CONTENT_TYPE]
            .to_str()
            .map_err(|error| error.to_string())?
            .to_string();
        Ok((status, content_type))
    })
    .await
}

async fn json_request(
    app: axum::Router,
    fetch: CatalogFetch,
    path: String,
) -> Result<(StatusCode, serde_json::Value), String> {
    let request = Request::get(path)
        .header(header::HOST, "localhost:6900")
        .body(Body::empty())
        .map_err(|error| error.to_string())?;
    crate::ace_provider::with_catalog_fetch_for_test(fetch, async move {
        let response = app
            .oneshot(request)
            .await
            .map_err(|error| error.to_string())?;
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .map_err(|error| error.to_string())?;
        let json = serde_json::from_slice(&body).map_err(|error| error.to_string())?;
        Ok((status, json))
    })
    .await
}

struct DefaultHookProvider {
    remembered: Arc<AtomicUsize>,
}

#[async_trait]
impl StreamProvider for DefaultHookProvider {
    fn network(&self) -> &'static str {
        "ace"
    }

    async fn open(&self, _id: &str) -> Result<Box<dyn TsSource>, ProviderError> {
        Err(ProviderError::NotFound)
    }

    fn remember_live_descriptor(&self, _info: &StreamInfo) {
        self.remembered.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn compat_catalog_default_hook_preserves_legacy_preflight() {
    let (transport, _) = synthetic_transport_with_auth(524_288);
    let info = stream_info_from_transport(&transport).unwrap();
    let remembered = Arc::new(AtomicUsize::new(0));
    let state = ace_app_state(
        Arc::new(DefaultHookProvider {
            remembered: remembered.clone(),
        }),
        true,
    );
    let app = router(state);
    let polls = Arc::new(AtomicUsize::new(0));
    let (status, json) = tokio::time::timeout(
        Duration::from_secs(5),
        json_request(
            app,
            counting_catalog(info.clone(), polls.clone()),
            format!("/ace/getstream?format=json&content_id={SYNTHETIC_CID}"),
        ),
    )
    .await
    .expect("default-hook request exceeded its 5-second watchdog")
    .expect("default-hook request failed");
    assert_eq!(status, StatusCode::OK);
    assert!(json["response"]["playback_url"].is_string(), "{json}");
    assert_eq!(remembered.load(Ordering::SeqCst), 1);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn compat_catalog_error_does_not_retry_or_mint_lease() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(addr) => addr,
        _ => unreachable!("loopback listener must be IPv4"),
    };
    let (transport, auth) = synthetic_transport_with_auth(524_288);
    let info = stream_info_from_transport(&transport).unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let server = tokio::spawn(serve_media_peer(
        listener,
        super::FIXTURE.to_vec(),
        auth,
        accepted.clone(),
    ));
    let provider =
        AceProvider::new(Arc::new(Identity::generate()), 0).with_bootstrap_peers(vec![addr]);
    let state = ace_state(provider, true);
    let sessions = state.ace_sessions.clone();
    let app = router(state);
    let polls = Arc::new(AtomicUsize::new(0));
    let first_polls = polls.clone();
    let first_info = info.clone();
    let first_fetch: CatalogFetch = Arc::new(move |_cid| {
        let polls = first_polls.clone();
        let info = first_info.clone();
        Box::pin(async move {
            let call = polls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                Err(ResolveError::Catalog("synthetic catalog failure"))
            } else {
                Ok(info)
            }
        })
    });
    let first_result = tokio::time::timeout(
        Duration::from_secs(5),
        json_request(
            app.clone(),
            first_fetch,
            format!("/ace/getstream?content_id={SYNTHETIC_CID}"),
        ),
    )
    .await;
    let failed_attempt_polls = polls.load(Ordering::SeqCst);
    let failed_attempt_leases = sessions.active_count();
    let second_result = tokio::time::timeout(
        Duration::from_secs(5),
        json_request(
            app,
            counting_catalog(info, polls.clone()),
            format!("/ace/getstream?format=json&content_id={SYNTHETIC_CID}"),
        ),
    )
    .await;
    server.abort();
    let server_shutdown = server.await;

    let first = first_result
        .expect("failed cold request exceeded its 5-second watchdog")
        .expect("failed cold request did not return its error envelope");
    assert_eq!(first.0, StatusCode::OK);
    assert!(first.1["error"].is_string(), "{}", first.1);
    assert_eq!(failed_attempt_polls, 1, "hidden retry occurred");
    assert_eq!(
        failed_attempt_leases, 0,
        "failed attempt minted a playback lease"
    );
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        0,
        "failure opened a source"
    );

    let second = second_result
        .expect("independent retry exceeded its 5-second watchdog")
        .expect("independent retry failed");
    assert_eq!(second.0, StatusCode::OK);
    assert!(second.1["error"].is_null(), "{}", second.1);
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    assert_eq!(sessions.active_count(), 1);
    assert_eq!(accepted.load(Ordering::SeqCst), 0);
    assert!(server_shutdown.is_ok() || server_shutdown.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn compat_catalog_attached_error_requests_share_one_failure() {
    let state = ace_state(AceProvider::new(Arc::new(Identity::generate()), 0), true);
    let sessions = state.ace_sessions.clone();
    let app = router(state);
    let polls = Arc::new(AtomicUsize::new(0));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let started_tx = Arc::new(std::sync::Mutex::new(Some(started_tx)));
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let release_rx = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));
    let fetch: CatalogFetch = {
        let polls = polls.clone();
        Arc::new(move |_cid| {
            let polls = polls.clone();
            let started = started_tx.lock().unwrap().take();
            let release = release_rx.clone();
            Box::pin(async move {
                polls.fetch_add(1, Ordering::SeqCst);
                if let Some(started) = started {
                    let _ = started.send(());
                }
                if let Some(release) = release.lock().await.take() {
                    let _ = release.await;
                }
                Err::<StreamInfo, _>(ResolveError::Catalog("synthetic shared failure"))
            })
        })
    };
    let first_app = app.clone();
    let first_fetch = fetch.clone();
    let mut first = tokio::spawn(async move {
        json_request(
            first_app,
            first_fetch,
            format!("/ace/getstream?format=json&content_id={SYNTHETIC_CID}"),
        )
        .await
    });
    let started = tokio::time::timeout(Duration::from_secs(5), started_rx).await;
    let second_app = app;
    let second_fetch = fetch;
    let mut second = tokio::spawn(async move {
        json_request(
            second_app,
            second_fetch,
            format!("/ace/getstream?format=json&content_id={SYNTHETIC_CID}"),
        )
        .await
    });
    let started_ok = matches!(&started, Ok(Ok(())));
    if started_ok {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = release_tx.send(());
    let first_result = tokio::time::timeout(Duration::from_secs(5), &mut first).await;
    let second_result = tokio::time::timeout(Duration::from_secs(5), &mut second).await;
    let first_clean = if first_result.is_err() {
        first.abort();
        matches!(first.await, Err(error) if error.is_cancelled())
    } else {
        true
    };
    let second_clean = if second_result.is_err() {
        second.abort();
        matches!(second.await, Err(error) if error.is_cancelled())
    } else {
        true
    };
    let first_result = first_result.ok().and_then(Result::ok).and_then(Result::ok);
    let second_result = second_result.ok().and_then(Result::ok).and_then(Result::ok);

    assert!(started_ok, "catalog failure future did not start");
    assert!(first_clean && second_clean, "request task cleanup failed");
    let first = first_result.expect("first attached request did not finish");
    let second = second_result.expect("second attached request did not finish");
    assert_eq!(first.0, StatusCode::OK);
    assert_eq!(second.0, StatusCode::OK);
    assert!(first.1["error"].is_string(), "{}", first.1);
    assert!(second.1["error"].is_string(), "{}", second.1);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(sessions.active_count(), 0);
}

#[tokio::test]
async fn compat_catalog_peer_and_local_results_keep_cid_alias() {
    let local_seed = SeedRegistry::new();
    let local_broadcasts = BroadcastRegistry::new();
    let local_broadcast = local_broadcasts
        .mint("synthetic-local", &[], &local_seed, 1 << 20)
        .await
        .unwrap();
    let local_cid = ace_swarm::resolve::infohash_hex(&local_broadcast.content_id);
    let local_provider = Arc::new(
        AceProvider::new(Arc::new(Identity::generate()), 0).with_seed_registry(local_seed),
    );
    let local_state = ace_app_state(local_provider.clone(), true);
    let local_sessions = local_state.ace_sessions.clone();
    let local_polls = Arc::new(AtomicUsize::new(0));
    let count_local = local_polls.clone();
    let local_fetch: CatalogFetch = Arc::new(move |_cid| {
        let count = count_local.clone();
        Box::pin(async move {
            count.fetch_add(1, Ordering::SeqCst);
            Err(ResolveError::Catalog("local id should bypass catalog"))
        })
    });
    let local_request = tokio::time::timeout(
        Duration::from_secs(5),
        json_request(
            router(local_state),
            local_fetch.clone(),
            format!("/ace/getstream?format=json&content_id={local_cid}"),
        ),
    )
    .await
    .expect("local catalog alias request exceeded its 5-second watchdog")
    .expect("local catalog alias request failed");
    let local_prepare_again = tokio::time::timeout(
        Duration::from_secs(5),
        crate::ace_provider::with_catalog_fetch_for_test(
            local_fetch,
            StreamProvider::prepare_live_content_id(&*local_provider, &local_cid),
        ),
    )
    .await
    .expect("local native preparation exceeded its 5-second watchdog")
    .expect("local native preparation failed");

    let (transport, _) = synthetic_transport_with_auth(524_288);
    let peer_key = transport_file_hash(&transport);
    let peer_cid = ace_swarm::resolve::infohash_hex(&peer_key);
    let (addr, mut peer_server) = serve_metadata_peer(peer_key, transport).await;
    let peer_provider = Arc::new(
        AceProvider::new(Arc::new(Identity::generate()), 0)
            .with_bep9_content_id_fallback(true)
            .with_bootstrap_peers(vec![addr]),
    );
    let peer_state = ace_app_state(peer_provider.clone(), true);
    let peer_sessions = peer_state.ace_sessions.clone();
    let peer_polls = Arc::new(AtomicUsize::new(0));
    let count_peer = peer_polls.clone();
    let peer_fetch: CatalogFetch = Arc::new(move |_cid| {
        let count = count_peer.clone();
        Box::pin(async move {
            count.fetch_add(1, Ordering::SeqCst);
            Err(ResolveError::Catalog("synthetic catalog miss"))
        })
    });
    let peer_request = tokio::time::timeout(
        Duration::from_secs(5),
        json_request(
            router(peer_state),
            peer_fetch.clone(),
            format!("/ace/getstream?format=json&content_id={peer_cid}"),
        ),
    )
    .await
    .expect("BEP-9 catalog alias request exceeded its 5-second watchdog")
    .expect("BEP-9 catalog alias request failed");
    let peer_prepare_again = tokio::time::timeout(
        Duration::from_secs(5),
        crate::ace_provider::with_catalog_fetch_for_test(
            peer_fetch,
            StreamProvider::prepare_live_content_id(&*peer_provider, &peer_cid),
        ),
    )
    .await
    .expect("BEP-9 native preparation exceeded its 5-second watchdog")
    .expect("BEP-9 native preparation failed");
    peer_server.0.abort();
    let peer_shutdown = (&mut peer_server.0).await;

    assert_eq!(local_request.0, StatusCode::OK);
    assert!(local_request.1["error"].is_null(), "{}", local_request.1);
    assert_eq!(local_request.1["response"]["infohash"], local_cid);
    assert_eq!(
        local_request.1["response"]["playback_url"]
            .as_str()
            .unwrap()
            .split("/ace/r/")
            .nth(1)
            .unwrap()
            .split('/')
            .next()
            .unwrap(),
        format!("cid:{local_cid}")
    );
    let local_token = local_request.1["response"]["playback_session_id"]
        .as_str()
        .unwrap();
    assert_eq!(
        local_sessions.validate_token(local_token),
        Some((format!("cid:{local_cid}"), format!("cid:{local_cid}")))
    );
    assert_eq!(local_polls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        local_prepare_again,
        crate::provider::ContentIdPreparation::ProviderResolved
    ));

    assert_eq!(peer_request.0, StatusCode::OK);
    assert!(peer_request.1["error"].is_null(), "{}", peer_request.1);
    assert_eq!(peer_request.1["response"]["infohash"], peer_cid);
    assert_eq!(
        peer_request.1["response"]["playback_url"]
            .as_str()
            .unwrap()
            .split("/ace/r/")
            .nth(1)
            .unwrap()
            .split('/')
            .next()
            .unwrap(),
        format!("cid:{peer_cid}")
    );
    let peer_token = peer_request.1["response"]["playback_session_id"]
        .as_str()
        .unwrap();
    assert_eq!(
        peer_sessions.validate_token(peer_token),
        Some((format!("cid:{peer_cid}"), format!("cid:{peer_cid}")))
    );
    assert_eq!(peer_polls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        peer_prepare_again,
        crate::provider::ContentIdPreparation::ProviderResolved
    ));
    assert!(peer_shutdown.is_ok() || peer_shutdown.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn compat_catalog_concurrent_requests_share_resolution_and_source() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(addr) => addr,
        _ => unreachable!("loopback listener must be IPv4"),
    };
    let (transport, auth) = synthetic_transport_with_auth(16_384);
    let info = stream_info_from_transport(&transport).unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let (post_drop_piece_tx, post_drop_piece_rx) = tokio::sync::oneshot::channel();
    let release_post_drop_piece_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let post_drop_chunks_sent = Arc::new(AtomicUsize::new(0));
    let post_drop_release_observed = Arc::new(AtomicUsize::new(0));
    let server = tokio::spawn(serve_media_peer_with_post_drop_piece(
        listener,
        auth,
        accepted.clone(),
        post_drop_piece_tx,
        release_post_drop_piece_flag.clone(),
        post_drop_chunks_sent.clone(),
        post_drop_release_observed.clone(),
    ));
    let provider = Arc::new(
        AceProvider::new(Arc::new(Identity::generate()), 0)
            .with_bootstrap_peers(vec![addr])
            .with_live_recovery(LiveRecoveryConfig {
                max_active_upstreams: 1,
                max_parallel_connect: 1,
                ..LiveRecoveryConfig::default()
            })
            .with_startup_buffer(StartupBufferConfig {
                target_ms: 0,
                ..StartupBufferConfig::default()
            }),
    );
    let state = ace_app_state(provider.clone(), true);
    let manager = state.manager.clone();
    let app = router(state);
    let polls = Arc::new(AtomicUsize::new(0));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let started_tx = Arc::new(std::sync::Mutex::new(Some(started_tx)));
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let release_rx = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));
    let fetch: CatalogFetch = {
        let info = info.clone();
        let polls = polls.clone();
        Arc::new(move |_cid| {
            let info = info.clone();
            let polls = polls.clone();
            let started = started_tx.lock().unwrap().take();
            let release = release_rx.clone();
            Box::pin(async move {
                polls.fetch_add(1, Ordering::SeqCst);
                if let Some(started) = started {
                    let _ = started.send(());
                }
                if let Some(release) = release.lock().await.take() {
                    let _ = release.await;
                }
                Ok::<_, ResolveError>(info)
            })
        })
    };
    let mut direct = tokio::spawn(direct_media_stream_request(
        app.clone(),
        fetch.clone(),
        format!("/ace/getstream?content_id={SYNTHETIC_CID}"),
    ));
    let mut json = tokio::spawn(json_request(
        app.clone(),
        fetch.clone(),
        format!("/ace/getstream?format=json&content_id={SYNTHETIC_CID}"),
    ));
    let setup = tokio::time::timeout(Duration::from_secs(5), started_rx).await;
    let both_attached = if matches!(setup, Ok(Ok(()))) {
        wait_for_catalog_waiters(&provider, 2).await
    } else {
        false
    };
    let _ = release_tx.send(());
    let direct_result = tokio::time::timeout(Duration::from_secs(5), &mut direct).await;
    let json_result = tokio::time::timeout(Duration::from_secs(5), &mut json).await;
    let direct_clean = if direct_result.is_err() {
        direct.abort();
        matches!(direct.await, Err(error) if error.is_cancelled())
    } else {
        true
    };
    let json_clean = if json_result.is_err() {
        json.abort();
        matches!(json.await, Err(error) if error.is_cancelled())
    } else {
        true
    };

    let observation = async {
        let direct_stream = direct_result
            .map_err(|_| "direct request exceeded its watchdog")?
            .map_err(|_| "direct request task failed")?
            .map_err(|_| "direct request failed")?;
        let (status, response) = json_result
            .map_err(|_| "JSON request exceeded its watchdog")?
            .map_err(|_| "JSON request task failed")?
            .map_err(|_| "JSON request failed")?;
        if status != StatusCode::OK || !response["error"].is_null() {
            return Err("JSON preparation did not succeed".to_string());
        }
        let path = returned_playback_path(&response)?;
        let playback_stream = tokio::time::timeout(
            Duration::from_secs(5),
            direct_media_stream_request(app.clone(), fetch, path),
        )
        .await
        .map_err(|_| "returned JSON playback URL exceeded its watchdog")?
        .map_err(|_| "returned JSON playback URL failed")?;
        let direct_session = manager
            .get("ace", &format!("cid:{SYNTHETIC_CID}"))
            .await
            .ok_or("direct playback did not create a cid session")?;
        let playback_session = manager
            .get("ace", &format!("cid:{SYNTHETIC_CID}"))
            .await
            .ok_or("JSON playback lost the cid session")?;
        let media = consume_open_post_drop_fixture_media(
            direct_stream,
            playback_stream,
            direct_session,
            playback_session,
            post_drop_piece_rx,
            release_post_drop_piece_flag.clone(),
        )
        .await?;
        Ok::<_, String>((response, media))
    }
    .await;
    drop(app);
    server.abort();
    let server_shutdown = server.await;

    assert!(setup.is_ok(), "catalog fetch did not reach its gate");
    assert!(
        both_attached,
        "both requests did not attach to the pending catalog fetch"
    );
    assert!(direct_clean, "direct task did not finish cleanly");
    assert!(json_clean, "JSON task did not finish cleanly");
    assert!(server_shutdown.is_ok() || server_shutdown.unwrap_err().is_cancelled());
    let (
        response,
        (
            direct_initial,
            playback_initial,
            post_drop_media,
            subscribers,
            remaining_subscribers,
            same_session,
        ),
    ) = observation.unwrap_or_else(|error| {
        panic!(
            "{error}; release flag={}, signed post-drop chunks sent={}, live HAVE updates={}",
            release_post_drop_piece_flag.load(Ordering::SeqCst),
            post_drop_chunks_sent.load(Ordering::SeqCst),
            post_drop_release_observed.load(Ordering::SeqCst)
        )
    });
    assert_valid_fixture_ts(&direct_initial);
    assert_valid_fixture_ts(&playback_initial);
    assert_valid_fixture_ts(&post_drop_media);
    assert!(post_drop_media
        .windows(188)
        .any(|packet| packet == &super::FIXTURE[9_588..9_776]));
    assert!(post_drop_chunks_sent.load(Ordering::SeqCst) > 0);
    assert_eq!(post_drop_release_observed.load(Ordering::SeqCst), 1);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        1,
        "both media consumers must share one ACE source"
    );
    assert!(
        same_session,
        "direct and URL playback must share the cid session"
    );
    assert_eq!(
        subscribers, 2,
        "both response bodies must subscribe to one session"
    );
    assert_eq!(
        remaining_subscribers, 1,
        "dropping the direct body must preserve the URL consumer"
    );
    assert!(response["response"]["playback_url"].is_string());
}

#[tokio::test]
async fn compat_catalog_cancelled_direct_request_keeps_json_waiter_live() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(addr) => addr,
        _ => unreachable!("loopback listener must be IPv4"),
    };
    let (transport, auth) = synthetic_transport_with_auth(16_384);
    let info = stream_info_from_transport(&transport).unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let (post_drop_piece_tx, post_drop_piece_rx) = tokio::sync::oneshot::channel();
    let release_post_drop_piece_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let post_drop_chunks_sent = Arc::new(AtomicUsize::new(0));
    let post_drop_release_observed = Arc::new(AtomicUsize::new(0));
    let server = tokio::spawn(serve_media_peer_with_post_drop_piece(
        listener,
        auth,
        accepted.clone(),
        post_drop_piece_tx,
        release_post_drop_piece_flag.clone(),
        post_drop_chunks_sent.clone(),
        post_drop_release_observed.clone(),
    ));
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_bootstrap_peers(vec![addr])
        .with_live_recovery(LiveRecoveryConfig {
            max_active_upstreams: 1,
            max_parallel_connect: 1,
            ..LiveRecoveryConfig::default()
        })
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let provider = Arc::new(provider);
    let state = ace_app_state(provider.clone(), true);
    let manager = state.manager.clone();
    let sessions = state.ace_sessions.clone();
    let app = router(state);
    let polls = Arc::new(AtomicUsize::new(0));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let started_tx = Arc::new(std::sync::Mutex::new(Some(started_tx)));
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let release_rx = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));
    let fetch: CatalogFetch = {
        let info = info.clone();
        let polls = polls.clone();
        Arc::new(move |_cid| {
            let info = info.clone();
            let polls = polls.clone();
            let started = started_tx.lock().unwrap().take();
            let release = release_rx.clone();
            Box::pin(async move {
                polls.fetch_add(1, Ordering::SeqCst);
                if let Some(started) = started {
                    let _ = started.send(());
                }
                if let Some(release) = release.lock().await.take() {
                    let _ = release.await;
                }
                Ok::<_, ResolveError>(info)
            })
        })
    };
    let direct = tokio::spawn(direct_media_request(
        app.clone(),
        fetch.clone(),
        format!("/ace/getstream?content_id={SYNTHETIC_CID}"),
    ));
    let started = tokio::time::timeout(Duration::from_secs(5), started_rx).await;
    let mut json = tokio::spawn(json_request(
        app.clone(),
        fetch.clone(),
        format!("/ace/getstream?format=json&content_id={SYNTHETIC_CID}"),
    ));
    let started_ok = matches!(&started, Ok(Ok(())));
    let both_attached = if started_ok {
        wait_for_catalog_waiters(&provider, 2).await
    } else {
        false
    };
    direct.abort();
    let direct_cancelled = matches!(direct.await, Err(error) if error.is_cancelled());
    let _ = release_tx.send(());
    let json_result = tokio::time::timeout(Duration::from_secs(5), &mut json).await;
    if json_result.is_err() {
        json.abort();
    }
    let json_cancelled = if json_result.is_err() {
        matches!(json.await, Err(error) if error.is_cancelled())
    } else {
        false
    };
    let json_result = json_result.ok().and_then(Result::ok).and_then(Result::ok);
    let json_finished = json_result.is_some();
    let playback_result = match json_result.as_ref() {
        Some((StatusCode::OK, body)) if body["error"].is_null() => {
            returned_playback_path(body).map(|path| (path, body.clone()))
        }
        _ => Err("JSON waiter did not return a successful playback URL".to_string()),
    };
    let media_result = match playback_result.as_ref() {
        Ok((path, _)) => Some(
            tokio::time::timeout(
                Duration::from_secs(20),
                open_and_consume_post_drop_fixture_media(
                    app.clone(),
                    fetch,
                    manager,
                    format!("/ace/getstream?content_id={SYNTHETIC_CID}"),
                    path.clone(),
                    post_drop_piece_rx,
                    release_post_drop_piece_flag.clone(),
                ),
            )
            .await,
        ),
        Err(_) => None,
    };
    drop(app);
    server.abort();
    let server_shutdown = server.await;

    assert!(started_ok, "catalog fetch did not start");
    assert!(
        both_attached,
        "JSON request did not attach before direct cancellation"
    );
    assert!(direct_cancelled, "direct request task was not canceled");
    assert!(!json_cancelled, "JSON waiter task was canceled");
    assert!(json_finished, "JSON waiter did not finish");
    assert!(server_shutdown.is_ok() || server_shutdown.unwrap_err().is_cancelled());
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    let (
        direct_media,
        playback_media,
        post_drop_media,
        subscribers,
        remaining_subscribers,
        same_session,
    ) = media_result
        .expect("surviving JSON playback URL was not opened")
        .expect("surviving JSON playback URL exceeded its watchdog")
        .unwrap_or_else(|error| {
            panic!(
                "surviving JSON playback URL failed: {error}; source accepts={}, signed post-drop chunks={}, live HAVE updates={}",
                accepted.load(Ordering::SeqCst),
                post_drop_chunks_sent.load(Ordering::SeqCst),
                post_drop_release_observed.load(Ordering::SeqCst),
            )
        });
    assert_valid_fixture_ts(&direct_media);
    assert_valid_fixture_ts(&playback_media);
    assert_valid_fixture_ts(&post_drop_media);
    assert!(post_drop_media
        .windows(188)
        .any(|packet| packet == &super::FIXTURE[9_588..9_776]));
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    assert!(post_drop_chunks_sent.load(Ordering::SeqCst) > 0);
    assert_eq!(post_drop_release_observed.load(Ordering::SeqCst), 1);
    assert!(same_session, "JSON URL must share the cid session");
    assert_eq!(subscribers, 2);
    assert_eq!(remaining_subscribers, 1);
    let (_, body) = json_result.expect("JSON waiter response missing");
    let token = body["response"]["playback_session_id"].as_str().unwrap();
    assert_eq!(
        sessions.validate_token(token),
        Some((
            body["response"]["infohash"].as_str().unwrap().to_string(),
            format!("cid:{SYNTHETIC_CID}")
        )),
        "the surviving JSON lease must retain its cid session key"
    );
    let (path, _) = playback_result.expect("surviving playback URL missing");
    assert!(path.starts_with("/ace/r/"));
}

#[tokio::test]
async fn compat_catalog_cold_direct_open_fetches_once() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(addr) => addr,
        _ => unreachable!("loopback listener must be IPv4"),
    };
    let (transport, auth) = synthetic_transport_with_auth(524_288);
    let info = stream_info_from_transport(&transport).unwrap();
    let media = super::FIXTURE.to_vec();
    let accepted = Arc::new(AtomicUsize::new(0));
    let server = tokio::spawn(serve_media_peer(listener, media, auth, accepted.clone()));
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_bootstrap_peers(vec![addr])
        .with_live_recovery(LiveRecoveryConfig {
            max_active_upstreams: 1,
            max_parallel_connect: 1,
            ..LiveRecoveryConfig::default()
        })
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let state = ace_state(provider, true);
    let manager = state.manager.clone();
    let catalog_calls = Arc::new(AtomicUsize::new(0));
    let calls = catalog_calls.clone();
    let catalog_info = info.clone();
    let fetch: CatalogFetch = Arc::new(move |_cid: String| {
        let calls = calls.clone();
        let info = catalog_info.clone();
        Box::pin(async move {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok::<StreamInfo, ResolveError>(info)
        }) as BoxFuture<'static, Result<StreamInfo, ResolveError>>
    });
    let path = format!("/ace/getstream?content_id={SYNTHETIC_CID}");
    let request = Request::get(path).body(Body::empty()).unwrap();
    let mut request_task = tokio::spawn(crate::ace_provider::with_catalog_fetch_for_test(
        fetch,
        async move {
            let response = router(state)
                .oneshot(request)
                .await
                .map_err(|e| e.to_string())?;
            if response.status() != StatusCode::OK {
                return Err(format!(
                    "unexpected direct response status: {}",
                    response.status()
                ));
            }
            let content_type = response.headers()[header::CONTENT_TYPE]
                .to_str()
                .map_err(|e| e.to_string())?;
            if !content_type.starts_with("video/mp2t") {
                return Err(format!("unexpected content type: {content_type}"));
            }
            let mut body = response.into_body().into_data_stream();
            let media = body
                .next()
                .await
                .ok_or_else(|| "direct response ended before media".to_string())?
                .map_err(|e| e.to_string())?;
            let has_cid_session = manager
                .get("ace", &format!("cid:{SYNTHETIC_CID}"))
                .await
                .is_some();
            Ok::<_, String>((media, has_cid_session))
        },
    ));
    let request_outcome = tokio::time::timeout(Duration::from_secs(5), &mut request_task).await;
    let (response_and_bytes, request_task_shutdown) = match request_outcome {
        Ok(Ok(result)) => (result, Ok(())),
        Ok(Err(error)) => (
            Err(format!("catalog-scoped request task failed: {error}")),
            Err(error.to_string()),
        ),
        Err(_) => {
            request_task.abort();
            let shutdown = request_task.await;
            let shutdown = match shutdown {
                Err(error) if error.is_cancelled() => Ok(()),
                Err(error) => Err(error.to_string()),
                Ok(_) => Err("request completed after its watchdog expired".to_string()),
            };
            (
                Err("direct cold compat request exceeded its 5-second watchdog".to_string()),
                shutdown,
            )
        }
    };
    server.abort();
    let server_result = server.await;

    let (media, has_cid_session) = response_and_bytes.expect("catalog scope request failed");
    assert!(
        request_task_shutdown.is_ok(),
        "catalog-scoped request task did not shut down cleanly"
    );
    assert!(
        server_result.is_ok() || server_result.unwrap_err().is_cancelled(),
        "loopback media peer did not shut down cleanly"
    );
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    assert!(media.len() >= 188, "response did not contain a TS packet");
    assert_eq!(media.len() % 188, 0, "response is not TS packet aligned");
    assert!(media
        .as_chunks::<188>()
        .0
        .iter()
        .all(|packet| packet[0] == 0x47));
    assert!(
        media
            .windows(188)
            .any(|packet| packet == &super::FIXTURE[9_400..9_588]),
        "gated response did not contain the fixture access-point packet"
    );
    assert!(
        has_cid_session,
        "compat cold open did not retain its cid session key"
    );
    assert_eq!(
        catalog_calls.load(Ordering::Relaxed),
        1,
        "cold compat direct open should share its catalog descriptor with AceProvider"
    );
}

#[tokio::test]
async fn compat_catalog_gate_disabled_never_fetches() {
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0);
    let state = ace_state(provider, false);
    let fetch: CatalogFetch = Arc::new(|cid: String| {
        let future: BoxFuture<'static, Result<StreamInfo, ResolveError>> = Box::pin(async move {
            panic!("resolution-disabled compat route fetched catalog for {cid}")
        });
        future
    });
    let path = format!("/ace/getstream?format=json&content_id={SYNTHETIC_CID}");
    let request = Request::get(path).body(Body::empty()).unwrap();
    let response = tokio::time::timeout(Duration::from_secs(5), async {
        crate::ace_provider::with_catalog_fetch_for_test(fetch, router(state).oneshot(request))
            .await
            .unwrap()
    })
    .await
    .expect("resolution-disabled compat route exceeded its 5-second watchdog");
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
    assert!(body["error"].is_null(), "{body}");
    assert!(body["response"]["playback_url"].is_string(), "{body}");
}

#[tokio::test]
async fn compat_catalog_warm_reuse_preserves_urls_and_cid_key() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(addr) => addr,
        _ => unreachable!("loopback listener must be IPv4"),
    };
    let (transport, auth) = synthetic_transport_with_auth(524_288);
    let info = stream_info_from_transport(&transport).unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let server = tokio::spawn(serve_media_peer(
        listener,
        super::FIXTURE.to_vec(),
        auth,
        accepted.clone(),
    ));
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_bootstrap_peers(vec![addr])
        .with_live_recovery(LiveRecoveryConfig {
            max_active_upstreams: 1,
            max_parallel_connect: 1,
            ..LiveRecoveryConfig::default()
        })
        .with_startup_buffer(StartupBufferConfig {
            target_ms: 0,
            ..StartupBufferConfig::default()
        });
    let state = ace_state(provider, true);
    let manager = state.manager.clone();
    let sessions = state.ace_sessions.clone();
    let app = router(state);
    let polls = Arc::new(AtomicUsize::new(0));
    let fetch = counting_catalog(info.clone(), polls.clone());
    let stage = Arc::new(AtomicUsize::new(0));
    let request_stage = stage.clone();
    let request_result = tokio::time::timeout(Duration::from_secs(5), async {
        request_stage.store(1, Ordering::SeqCst);
        let cold_media = direct_media_request(
            app.clone(),
            fetch.clone(),
            format!("/ace/getstream?content_id={SYNTHETIC_CID}"),
        )
        .await?;
        request_stage.store(2, Ordering::SeqCst);
        let first_session = manager
            .get("ace", &format!("cid:{SYNTHETIC_CID}"))
            .await
            .ok_or_else(|| "cold direct request did not create its cid session".to_string())?;
        request_stage.store(3, Ordering::SeqCst);
        let warm_response = direct_response_headers(
            app.clone(),
            fetch.clone(),
            format!("/ace/getstream?content_id={SYNTHETIC_CID}"),
        )
        .await?;
        request_stage.store(4, Ordering::SeqCst);
        let warm_session = manager
            .get("ace", &format!("cid:{SYNTHETIC_CID}"))
            .await
            .ok_or_else(|| "warm direct request lost its cid session".to_string())?;
        request_stage.store(5, Ordering::SeqCst);
        let (status, json) = json_request(
            app,
            fetch,
            format!("/ace/getstream?format=json&content_id={SYNTHETIC_CID}"),
        )
        .await?;
        request_stage.store(6, Ordering::SeqCst);
        Ok::<_, String>((
            cold_media,
            warm_response,
            Arc::ptr_eq(&first_session, &warm_session),
            status,
            json,
        ))
    })
    .await;
    server.abort();
    let server_result = server.await;

    let (cold_media, warm_response, same_session, status, json) = request_result
        .unwrap_or_else(|_| {
            panic!(
                "warm compat sequence exceeded its 5-second watchdog at stage {}",
                stage.load(Ordering::SeqCst)
            )
        })
        .expect("warm compat sequence failed");
    assert!(server_result.is_ok() || server_result.unwrap_err().is_cancelled());
    assert_eq!(status, StatusCode::OK);
    assert_eq!(warm_response.0, StatusCode::OK);
    assert!(warm_response.1.starts_with("video/mp2t"));
    assert!(cold_media.len() >= 188);
    assert!(cold_media
        .as_chunks::<188>()
        .0
        .iter()
        .all(|packet| packet[0] == 0x47));
    assert!(
        same_session,
        "warm direct request must reuse the manager's cid session"
    );
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "warm routes must use the cached catalog descriptor"
    );
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        1,
        "warm direct must not open a second ACE source"
    );

    let response = &json["response"];
    let infohash = ace_swarm::resolve::infohash_hex(&info.infohash);
    let token = response["playback_session_id"].as_str().unwrap();
    assert_eq!(response["infohash"], infohash);
    assert_eq!(
        response["playback_url"],
        format!("http://localhost:6900/ace/r/{infohash}/{token}")
    );
    assert_eq!(
        response["stat_url"],
        format!("http://localhost:6900/ace/stat/{infohash}/{token}")
    );
    assert_eq!(
        response["command_url"],
        format!("http://localhost:6900/ace/cmd/{infohash}/{token}")
    );
    assert_eq!(
        response["metadata"]["title"],
        serde_json::json!(info.metadata.title)
    );
    assert_eq!(response["metadata"]["bitrate"], 8_000_000);
    assert_eq!(response["metadata"]["categories"], serde_json::json!([]));
    assert!(manager
        .get("ace", &format!("cid:{SYNTHETIC_CID}"))
        .await
        .is_some());
    assert_eq!(
        sessions.validate_token(token),
        Some((infohash, format!("cid:{SYNTHETIC_CID}"))),
        "tokenized alias must retain the cid session key"
    );
}

#[tokio::test(start_paused = true)]
async fn compat_catalog_ttl_expiry_refetches_without_rekeying() {
    let (first_transport, _) = synthetic_transport_with_auth_named(524_288, "Synthetic Live first");
    let first_info = stream_info_from_transport(&first_transport).unwrap();
    let (fresh_transport, _) =
        synthetic_transport_with_auth_named(524_288, "Synthetic Live refreshed");
    let fresh_info = stream_info_from_transport(&fresh_transport).unwrap();
    let first_alias = ace_swarm::resolve::infohash_hex(&first_info.infohash);
    let fresh_alias = ace_swarm::resolve::infohash_hex(&fresh_info.infohash);
    assert_ne!(first_alias, fresh_alias);

    let provider = Arc::new(AceProvider::new(Arc::new(Identity::generate()), 0));
    let state = ace_app_state(provider, true);
    let sessions = state.ace_sessions.clone();
    let manager = state.manager.clone();
    let app = router(state);
    let polls = Arc::new(AtomicUsize::new(0));
    let fetch: CatalogFetch = {
        let first_info = first_info.clone();
        let fresh_info = fresh_info.clone();
        let polls = polls.clone();
        Arc::new(move |_cid| {
            let call = polls.fetch_add(1, Ordering::SeqCst);
            let info = if call == 0 {
                first_info.clone()
            } else {
                fresh_info.clone()
            };
            Box::pin(async move { Ok::<StreamInfo, ResolveError>(info) })
        })
    };
    let get_json = |app: axum::Router, fetch: CatalogFetch| async move {
        tokio::time::timeout(
            Duration::from_secs(5),
            json_request(
                app,
                fetch,
                format!("/ace/getstream?format=json&content_id={SYNTHETIC_CID}"),
            ),
        )
        .await
        .expect("catalog JSON request exceeded its watchdog")
        .expect("catalog JSON request failed")
    };

    let (first_status, first_body) = get_json(app.clone(), fetch.clone()).await;
    assert_eq!(first_status, StatusCode::OK);
    assert!(first_body["error"].is_null(), "{first_body}");
    assert_eq!(first_body["response"]["infohash"], first_alias);
    assert_eq!(
        first_body["response"]["metadata"]["title"],
        "Synthetic Live first"
    );
    assert_eq!(polls.load(Ordering::SeqCst), 1);

    tokio::time::advance(Duration::from_secs(299)).await;
    let (warm_status, warm_body) = get_json(app.clone(), fetch.clone()).await;
    assert_eq!(warm_status, StatusCode::OK);
    assert!(warm_body["error"].is_null(), "{warm_body}");
    assert_eq!(warm_body["response"]["infohash"], first_alias);
    assert_eq!(polls.load(Ordering::SeqCst), 1, "entry expired before 300s");

    tokio::time::advance(Duration::from_secs(1)).await;
    let (fresh_status, fresh_body) = get_json(app, fetch).await;
    assert_eq!(fresh_status, StatusCode::OK);
    assert!(fresh_body["error"].is_null(), "{fresh_body}");
    assert_eq!(fresh_body["response"]["infohash"], fresh_alias);
    assert_eq!(
        fresh_body["response"]["playback_url"],
        format!(
            "http://localhost:6900/ace/r/{fresh_alias}/{}",
            fresh_body["response"]["playback_session_id"]
                .as_str()
                .unwrap()
        )
    );
    assert_eq!(
        fresh_body["response"]["metadata"]["title"],
        "Synthetic Live refreshed"
    );
    assert_eq!(
        polls.load(Ordering::SeqCst),
        2,
        "expiry must fetch once again"
    );

    for body in [&first_body, &warm_body, &fresh_body] {
        let token = body["response"]["playback_session_id"].as_str().unwrap();
        assert_eq!(
            sessions.validate_token(token),
            Some((
                body["response"]["infohash"].as_str().unwrap().to_string(),
                format!("cid:{SYNTHETIC_CID}"),
            )),
            "catalog refresh must keep each lease bound to the original cid key"
        );
    }
    assert!(
        manager
            .get("ace", &format!("cid:{SYNTHETIC_CID}"))
            .await
            .is_none(),
        "metadata preparation alone must not start or rekey a media session"
    );
    assert!(manager.get("ace", &first_alias).await.is_none());
    assert!(manager.get("ace", &fresh_alias).await.is_none());
}
