use super::*;

const SYNTHETIC: &str = "0123456789abcdef0123456789abcdef01234567";

fn private_provider() -> AceProvider {
    AceProvider::new(Arc::new(Identity::generate()), 0).with_peer_policy(
        ace_swarm::peer_policy::PeerPolicy::from_private_peers(Some("127.0.0.1:1")).unwrap(),
    )
}

#[tokio::test]
async fn private_live_and_vod_never_poll_public_catalog() {
    let provider = private_provider();
    let live = provider
        .resolve_content_id_with_catalog(SYNTHETIC, async {
            panic!("private live polled public catalog")
        })
        .await;
    assert!(
        matches!(live, Err(ProviderError::Unresolvable(ref reason)) if reason == "catalog_disabled_in_private_mode")
    );
    let vod = provider
        .resolve_vod_info_with_catalog(&format!("cid:{SYNTHETIC}"), async {
            panic!("private VOD polled public catalog")
        })
        .await;
    assert!(
        matches!(vod, Err(ProviderError::Unresolvable(ref reason)) if reason == "catalog_disabled_in_private_mode")
    );
    let bare = provider
        .resolve_infohash_with_catalog(SYNTHETIC, async {
            panic!("private bare identity polled public catalog")
        })
        .await;
    assert!(
        matches!(bare, Err(ProviderError::Unresolvable(ref reason)) if reason == "catalog_disabled_in_private_mode")
    );
}

#[tokio::test]
async fn private_opted_in_raw_metadata_fallback_never_polls_catalog() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let policy =
        ace_swarm::peer_policy::PeerPolicy::from_private_peers(Some(&endpoint.to_string()))
            .unwrap();
    let provider = private_provider()
        .with_peer_policy(policy)
        .with_bep9_content_id_fallback(true);
    let mut server = tokio::spawn(async move {
        for _ in 0..2 {
            let (socket, _) = listener.accept().await?;
            drop(socket);
        }
        Ok::<(), std::io::Error>(())
    });
    let calls = Arc::new(AtomicU32::new(0));
    let live_calls = calls.clone();
    let vod_calls = calls.clone();
    let observation = tokio::time::timeout(Duration::from_secs(5), async {
        let live = provider
            .resolve_content_id_with_catalog(SYNTHETIC, async move {
                live_calls.fetch_add(1, Ordering::Relaxed);
                Err(ResolveError::Catalog("forbidden fixture catalog"))
            })
            .await;
        let vod = provider
            .resolve_vod_info_with_catalog(&format!("cid:{SYNTHETIC}"), async move {
                vod_calls.fetch_add(1, Ordering::Relaxed);
                Err(ResolveError::Catalog("forbidden fixture catalog"))
            })
            .await;
        (live, vod)
    })
    .await;
    let closed = tokio::time::timeout(Duration::from_secs(2), &mut server).await;
    if closed.is_err() {
        server.abort();
        let _ = server.await;
    }
    assert!(
        matches!(closed, Ok(Ok(Ok(())))),
        "owned metadata peer did not finish: {closed:?}"
    );
    let (live, vod) = observation.expect("bounded raw metadata controls");
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert!(
        matches!(live, Err(ProviderError::NoPeers(ref reason)) if reason == "content-id resolution: no metadata peer responded")
    );
    assert!(
        matches!(vod, Err(ProviderError::NoPeers(ref reason)) if reason == "vod content-id resolution: no metadata peer responded")
    );
}

#[tokio::test]
async fn private_http_transport_urls_are_rejected_before_fetch() {
    let provider = private_provider();
    let target =
        crate::transport_url::encode_transport_url("http://127.0.0.1:1/descriptor").unwrap();
    let live = provider.resolve_live_info(&target).await;
    assert!(
        matches!(live, Err(ProviderError::Unresolvable(ref reason)) if reason == "remote_descriptor_disabled_in_private_mode")
    );
    let vod = provider
        .resolve_vod_info_with_catalog(&target, async { panic!("unexpected catalog") })
        .await;
    assert!(
        matches!(vod, Err(ProviderError::Unresolvable(ref reason)) if reason == "remote_descriptor_disabled_in_private_mode")
    );
}

#[test]
fn private_provider_builders_cannot_restore_public_advertisement() {
    let (_, port) = tokio::sync::watch::channel(Some(1234));
    let provider = private_provider()
        .with_inbound_announce_port(Some(4321))
        .with_inbound_announce_port_receiver(port)
        .with_reachability(Some(Arc::new(ReachabilityMonitor::new())))
        .with_cache(CacheType::Memory, PathBuf::new());
    assert_eq!(provider.discovery_announce_port(), 0);
    assert_eq!(provider.seeder_announce_port(), None);
    assert!(provider.reachability.is_none());
}

#[tokio::test]
async fn private_local_aliases_resolve_without_catalog_and_do_not_grant_bare_identity_trust() {
    let (bytes, _) = test_support::synthetic_live_transport(32768);
    let map =
        crate::local_descriptors::LocalDescriptors::from_test_bytes("fixture", &bytes).unwrap();
    let provider = private_provider().with_local_descriptors(map);
    let live = provider.resolve_live_info("local:fixture").await.unwrap();
    assert_eq!(live.piece_length, 32768);
    assert!(live.sig_len > 0);
    assert!(!live.source_pubkey.is_empty());
    let bare = ace_swarm::resolve::infohash_hex(&live.infohash);
    assert!(provider.verified_info_for_infohash(&bare).is_err());
    assert!(provider.check_openable("local:fixture").is_ok());
    assert!(provider.check_openable("local:/etc/passwd").is_err());
    assert!(provider.resolve_live_info("local:missing").await.is_err());
    let bytes = test_support::protection_vod_bytes(0, false);
    let map =
        crate::local_descriptors::LocalDescriptors::from_test_bytes("fixture", &bytes).unwrap();
    let provider = private_provider().with_local_descriptors(map);
    let vod = provider
        .resolve_vod_info_with_catalog("local:fixture", async {
            panic!("local alias polled catalog")
        })
        .await
        .unwrap();
    assert_eq!(vod.total_length, 1);
    assert!(provider.resolve_live_info("local:fixture").await.is_err());
}

#[tokio::test]
async fn private_allowed_metadata_resolves_live_and_vod_without_catalog_trust() {
    use sha1::{Digest, Sha1};
    let (live_bytes, _) = test_support::synthetic_live_transport(32768);
    let vod_bytes = test_support::protection_vod_bytes(0, false);
    let live_key: [u8; 20] = Sha1::digest(&live_bytes).into();
    let vod_key: [u8; 20] = Sha1::digest(&vod_bytes).into();
    let registry = SeedRegistry::new();
    registry.register_metadata(live_key, live_bytes);
    registry.register_metadata(vod_key, vod_bytes);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let admission = ace_swarm::listen::InboundPolicy {
        max_inbound: 1,
        max_per_stream: 1,
        max_unchoked: 1,
    }
    .admission();
    let mut server = tokio::spawn(async move {
        let identity = Arc::new(Identity::generate());
        for _ in 0..2 {
            let (socket, _) = listener.accept().await?;
            let result = admission
                .accept(
                    socket,
                    registry.clone(),
                    random_peer_id(),
                    [0; 8],
                    identity.clone(),
                    [127, 0, 0, 1],
                )
                .expect("one owned metadata connection")
                .await;
            if !matches!(result, Err(ace_peer::PeerError::Closed)) {
                return result;
            }
        }
        Ok::<(), ace_peer::PeerError>(())
    });
    let policy =
        ace_swarm::peer_policy::PeerPolicy::from_private_peers(Some(&endpoint.to_string()))
            .unwrap();
    let provider = private_provider()
        .with_peer_policy(policy)
        .with_bep9_content_id_fallback(true);
    let calls = Arc::new(AtomicU32::new(0));
    let live_calls = calls.clone();
    let vod_calls = calls.clone();
    let observation = tokio::time::timeout(Duration::from_secs(5), async {
        let live = provider
            .resolve_content_id_with_catalog(&hex::encode(live_key), async move {
                live_calls.fetch_add(1, Ordering::Relaxed);
                Err(ResolveError::Catalog("forbidden fixture catalog"))
            })
            .await;
        let vod = provider
            .resolve_vod_info_with_catalog(&format!("cid:{}", hex::encode(vod_key)), async move {
                vod_calls.fetch_add(1, Ordering::Relaxed);
                Err(ResolveError::Catalog("forbidden fixture catalog"))
            })
            .await;
        (live, vod)
    })
    .await;
    let closed = tokio::time::timeout(Duration::from_secs(2), &mut server).await;
    if closed.is_err() {
        server.abort();
        let _ = server.await;
    }
    assert!(
        matches!(closed, Ok(Ok(Ok(())))),
        "metadata serving task failed: {closed:?}"
    );
    let (live, vod) = observation.expect("bounded allowed metadata resolution");
    let live = live.unwrap();
    let vod = vod.unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert!(live.sig_len > 0 && !live.source_pubkey.is_empty());
    assert_eq!(vod.total_length, 1);
    assert!(provider.infohash_index.get(&live.infohash).is_none());
    assert!(provider
        .verified_info_for_infohash(&hex::encode(live.infohash))
        .is_err());
}
