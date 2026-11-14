use super::*;
use ace_swarm::listen::PeerListener;
use ace_wire::infohash::transport_file_hash;
use std::future::{ready, Future};

const CID: &str = "0123456789abcdef0123456789abcdef01234567";
const RAW_METAINFO: &[u8] =
    include_bytes!("../../../../tests/vectors/metainfo/single-file-unsorted.torrent");

fn catalog_failure<T>() -> impl Future<Output = Result<T, ResolveError>> {
    ready(Err(ResolveError::Catalog("synthetic catalog unavailable")))
}

fn provider() -> AceProvider {
    AceProvider::new(Arc::new(Identity::generate()), 0)
}

// A real metadata server exercises the complete TCP + BEP-9 path. Abort on drop keeps
// the server bounded even when an assertion fails.
struct MetadataPeer(tokio::task::JoinHandle<()>);
impl Drop for MetadataPeer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn metadata_peer(key: [u8; 20], bytes: Vec<u8>) -> (SocketAddrV4, MetadataPeer) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let registry = SeedRegistry::new();
    registry.register_metadata(key, bytes);
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
    (addr, MetadataPeer(server))
}

fn vod_transport() -> Vec<u8> {
    use ace_wire::bencode::Bencode;
    let descriptor = Bencode::Dict(std::collections::BTreeMap::from([
        (b"name".to_vec(), Bencode::Bytes(b"Synthetic VOD".to_vec())),
        (b"authmethod".to_vec(), Bencode::Bytes(b"None".to_vec())),
        (b"pubkey".to_vec(), Bencode::Bytes(vec![])),
        (b"bitrate".to_vec(), Bencode::Int(100_000)),
        (b"piece_length".to_vec(), Bencode::Int(16_384)),
        (b"chunk_length".to_vec(), Bencode::Int(16_384)),
        (b"length".to_vec(), Bencode::Int(16_384)),
        (b"pieces".to_vec(), Bencode::Bytes(vec![0; 20])),
    ]));
    ace_wire::transport::encode_transport(&descriptor)
}

#[tokio::test]
async fn disabled_live_and_vod_fail_before_peer_connections_or_discovery() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    // Empty bootstrap peers would trigger discovery if the policy gate were bypassed.
    for peers in [vec![addr], vec![]] {
        let p = provider().with_bootstrap_peers(peers);
        let live = tokio::time::timeout(
            Duration::from_millis(100),
            p.resolve_content_id_with_catalog(CID, catalog_failure()),
        )
        .await
        .unwrap()
        .unwrap_err();
        let vod = tokio::time::timeout(
            Duration::from_millis(100),
            p.resolve_vod_info_with_catalog(&format!("cid:{CID}"), catalog_failure()),
        )
        .await
        .unwrap()
        .unwrap_err();
        for error in [live, vod] {
            let message = error.to_string();
            assert!(message.contains("synthetic catalog unavailable"));
            assert!(message.contains("fallback is disabled"));
            assert!(!message.contains(CID));
        }
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn catalog_raw_metainfo_is_vod_only_at_the_provider_boundary() {
    let p = provider();
    let vod = p
        .resolve_vod_info_with_catalog(&format!("cid:{CID}"), ready(Ok(RAW_METAINFO.to_vec())))
        .await
        .unwrap();
    assert_eq!(vod.total_length, 16_385);
    assert_eq!(vod.piece_hashes, vec![[0xA5; 20], [0x5A; 20]]);

    let live = p
        .resolve_content_id_with_catalog(&infohash_hex(&hex20(CID).unwrap()), async {
            stream_info_from_transport(RAW_METAINFO)
                .map_err(|_| ResolveError::Transport("raw metainfo is not live transport"))
        })
        .await;
    assert!(
        live.is_err(),
        "raw metainfo must not become a live StreamInfo"
    );
}

#[tokio::test]
async fn enabled_live_peer_resolution_stays_out_of_catalog_trust() {
    let (bytes, _) = test_support::synthetic_live_transport(524_288);
    let key = transport_file_hash(&bytes);
    let expected = stream_info_from_transport(&bytes).unwrap();
    let (addr, _server) = metadata_peer(key, bytes).await;
    let p = provider()
        .with_bep9_content_id_fallback(true)
        .with_bootstrap_peers(vec![addr]);
    let cid = infohash_hex(&key);
    let actual = p
        .resolve_content_id_with_catalog(&cid, catalog_failure())
        .await
        .unwrap();
    assert_eq!(actual, expected);
    assert!(p.resolve_cache.get(&cid).is_none());
    assert!(p.peer_resolve_cache.get(&cid).is_some());
    assert!(p
        .verified_info_for_infohash(&infohash_hex(&expected.infohash))
        .is_err());
}

#[tokio::test]
async fn enabled_live_preparation_keeps_peer_result_out_of_catalog_trust() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (bytes, _) = test_support::synthetic_live_transport(524_288);
    let key = transport_file_hash(&bytes);
    let expected = stream_info_from_transport(&bytes).unwrap();
    let (addr, _server) = metadata_peer(key, bytes).await;
    let p = provider()
        .with_bep9_content_id_fallback(true)
        .with_bootstrap_peers(vec![addr]);
    let cid = infohash_hex(&key);
    let polls = Arc::new(AtomicUsize::new(0));
    let count = polls.clone();
    let fetch: CatalogFetch = Arc::new(move |_cid| {
        let count = count.clone();
        Box::pin(async move {
            count.fetch_add(1, Ordering::SeqCst);
            Err(ResolveError::Catalog("synthetic catalog unavailable"))
        })
    });

    let prepared = with_catalog_fetch_for_test(
        fetch.clone(),
        StreamProvider::prepare_live_content_id(&p, &cid),
    )
    .await
    .unwrap();
    let native = with_catalog_fetch_for_test(fetch, p.resolve_live_info(&format!("cid:{cid}")))
        .await
        .unwrap();

    assert!(matches!(
        prepared,
        crate::provider::ContentIdPreparation::ProviderResolved
    ));
    assert_eq!(native, expected);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert!(p.resolve_cache.get(&cid).is_none());
    assert!(p.peer_resolve_cache.get(&cid).is_some());
    assert!(p
        .verified_info_for_infohash(&infohash_hex(&expected.infohash))
        .is_err());
}

#[tokio::test]
async fn enabled_live_and_vod_reject_mismatched_raw_transport_hash() {
    let (live, _) = test_support::synthetic_live_transport(524_288);
    let key = hex20(CID).unwrap();
    for bytes in [live, vod_transport()] {
        assert_ne!(transport_file_hash(&bytes), key);
        let (addr, _server) = metadata_peer(key, bytes).await;
        let p = provider()
            .with_bep9_content_id_fallback(true)
            .with_bootstrap_peers(vec![addr]);
        assert!(p
            .resolve_content_id_with_catalog(CID, catalog_failure())
            .await
            .is_err());
        assert!(p
            .resolve_vod_info_with_catalog(&format!("cid:{CID}"), catalog_failure())
            .await
            .is_err());
        assert!(p.peer_resolve_cache.get(CID).is_none());
    }
}

#[tokio::test]
async fn enabled_vod_resolves_raw_hash_metadata() {
    let bytes = vod_transport();
    let key = transport_file_hash(&bytes);
    let (addr, _server) = metadata_peer(key, bytes).await;
    let p = provider()
        .with_bep9_content_id_fallback(true)
        .with_bootstrap_peers(vec![addr]);
    let result = p
        .resolve_vod_info_with_catalog(&format!("cid:{}", infohash_hex(&key)), catalog_failure())
        .await
        .unwrap();
    assert_eq!(result.total_length, 16_384);
    assert!(p.resolve_cache.get(&infohash_hex(&key)).is_none());
    assert!(p.infohash_index.get(&result.infohash).is_none());
}

#[tokio::test]
async fn local_broadcast_content_id_resolves_without_catalog_or_opt_in() {
    let seed = SeedRegistry::new();
    let broadcasts = crate::broadcast::BroadcastRegistry::new();
    let broadcast = broadcasts
        .mint("policy-test", &[], &seed, 1 << 20)
        .await
        .unwrap();
    let p = provider().with_seed_registry(seed);
    let cid = infohash_hex(&broadcast.content_id);
    let info = p
        .resolve_content_id_with_catalog(&cid, async {
            panic!("local broadcast must not fetch catalog")
        })
        .await
        .unwrap();
    assert_eq!(info.infohash, broadcast.infohash);
    assert!(p.resolve_cache.get(&cid).is_none());
    assert!(p.peer_resolve_cache.get(&cid).is_none());
    assert!(p.infohash_index.get(&info.infohash).is_none());

    let fetch: CatalogFetch = Arc::new(|cid| {
        Box::pin(async move { panic!("local content id unexpectedly fetched catalog for {cid}") })
    });
    let prepared = with_catalog_fetch_for_test(
        fetch.clone(),
        StreamProvider::prepare_live_content_id(&p, &cid),
    )
    .await
    .unwrap();
    let native = with_catalog_fetch_for_test(fetch, p.resolve_live_info(&format!("cid:{cid}")))
        .await
        .unwrap();
    assert!(matches!(
        prepared,
        crate::provider::ContentIdPreparation::ProviderResolved
    ));
    assert_eq!(native.infohash, broadcast.infohash);
    assert!(p.resolve_cache.get(&cid).is_none());
    assert!(p.infohash_index.get(&info.infohash).is_none());
}

#[tokio::test]
async fn generic_seed_metadata_does_not_gain_local_broadcast_trust() {
    let (bytes, _) = test_support::synthetic_live_transport(524_288);
    let key = transport_file_hash(&bytes);
    let registry = SeedRegistry::new();
    registry.register_metadata(key, bytes);
    let p = provider().with_seed_registry(registry);
    let cid = infohash_hex(&key);
    let error = p
        .resolve_content_id_with_catalog(&cid, catalog_failure())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("fallback is disabled"));
}

#[tokio::test]
async fn disabled_fallback_preserves_catalog_resolution_and_cached_trust() {
    let (bytes, _) = test_support::synthetic_live_transport(524_288);
    let expected = stream_info_from_transport(&bytes).unwrap();
    let p = provider();
    assert_eq!(
        p.resolve_content_id_with_catalog(CID, ready(Ok(expected.clone())))
            .await
            .unwrap(),
        expected
    );
    assert_eq!(
        p.resolve_content_id_with_catalog(CID, async {
            panic!("cached catalog result must not refetch")
        })
        .await
        .unwrap(),
        expected
    );
    assert!(p
        .verified_info_for_infohash(&infohash_hex(&expected.infohash))
        .is_ok());
}

#[tokio::test]
async fn catalog_resolution_capacity_never_enables_peer_fallback() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (bytes, _) = test_support::synthetic_live_transport(524_288);
    let peer_key = transport_file_hash(&bytes);
    let (addr, peer_server) = metadata_peer(peer_key, bytes.clone()).await;
    let provider = Arc::new(
        provider()
            .with_bep9_content_id_fallback(true)
            .with_bootstrap_peers(vec![addr]),
    );
    let polls = Arc::new(AtomicUsize::new(0));
    let mut blockers = Vec::new();
    let mut releases = Vec::new();
    let mut started_receivers = Vec::new();
    for index in 0..32 {
        let cid = format!("{:040x}", index + 500);
        let current = provider.clone();
        let calls = polls.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        blockers.push(tokio::spawn(async move {
            current
                .resolve_cache
                .resolve_with(&cid, async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let _ = started_tx.send(());
                    let _ = release_rx.await;
                    Err(ResolveError::Catalog("held synthetic lookup"))
                })
                .await
        }));
        releases.push(release_tx);
        started_receivers.push(started_rx);
    }
    for started in started_receivers {
        tokio::time::timeout(Duration::from_secs(5), started)
            .await
            .expect("pending catalog lookup did not start")
            .expect("pending catalog lookup was dropped");
    }

    let cid = infohash_hex(&peer_key);
    let unpolled = polls.clone();
    let fallback_descriptor = stream_info_from_transport(&bytes).unwrap();
    let outcome = provider
        .resolve_content_id_with_catalog(&cid, async move {
            unpolled.fetch_add(1, Ordering::SeqCst);
            Ok(fallback_descriptor)
        })
        .await;
    let _ = releases
        .into_iter()
        .map(|release| release.send(()))
        .collect::<Vec<_>>();
    for blocker in blockers {
        let _ = blocker.await;
    }
    let mut peer_server = peer_server;
    peer_server.0.abort();
    let peer_shutdown = (&mut peer_server.0).await;

    assert!(matches!(
        outcome,
        Err(ProviderError::ResolverBusy(message))
            if message == "content-id catalog resolution capacity exceeded"
    ));
    assert_eq!(polls.load(Ordering::SeqCst), 32);
    assert!(
        peer_shutdown.is_ok() || peer_shutdown.unwrap_err().is_cancelled(),
        "loopback BEP-9 peer did not shut down cleanly"
    );
}

#[tokio::test]
async fn catalog_resolution_cid_namespace_does_not_seed_arbitrary_infohash() {
    let (bytes, _) = test_support::synthetic_live_transport(524_288);
    let catalog_info = stream_info_from_transport(&bytes).unwrap();
    let p = provider();
    p.resolve_cache.put(CID, catalog_info.clone());

    assert!(p.resolve_cache.get(CID).is_some());
    assert!(
        p.verified_info_for_infohash(CID).is_err(),
        "a content-id cache entry must not authenticate a bare infohash"
    );
    assert!(p
        .verified_info_for_infohash(&infohash_hex(&catalog_info.infohash))
        .is_err());
}

#[tokio::test]
async fn protected_cached_descriptor_cannot_open_or_prepare() {
    use ace_wire::protection::Protection;
    let p = provider();
    let mut info = stream_info_from_transport(&vod_transport()).unwrap();
    info.protection = Some(Protection::Protected);
    p.resolve_cache.put(CID, info.clone());
    p.infohash_index.put(info.clone());
    assert_eq!(
        p.check_openable(&infohash_hex(&info.infohash))
            .unwrap_err()
            .to_string(),
        "encrypted content is not supported"
    );
    assert_eq!(
        p.open(&format!("cid:{CID}"))
            .await
            .err()
            .unwrap()
            .to_string(),
        "encrypted content is not supported"
    );
    assert_eq!(
        p.resolve_content_id_prepared(CID)
            .await
            .err()
            .unwrap()
            .to_string(),
        "encrypted content is not supported"
    );
}

#[tokio::test]
async fn malformed_catalog_descriptor_preserves_reason_without_peer_fallback() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    for fallback in [false, true] {
        for reason in ["unexpected chunk_length", "piece_length too large"] {
            let p = provider()
                .with_bep9_content_id_fallback(fallback)
                .with_bootstrap_peers(vec![addr]);
            let result = tokio::time::timeout(
                Duration::from_millis(100),
                p.resolve_content_id_with_catalog(CID, ready(Err(ResolveError::Transport(reason)))),
            )
            .await
            .expect("malformed descriptor tried peer fallback");
            assert_eq!(
                result.unwrap_err().to_string(),
                format!("invalid descriptor metadata: {reason}")
            );
        }
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn typed_protection_catalog_errors_are_terminal_for_vod_fallback() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        unreachable!()
    };
    let p = provider()
        .with_bep9_content_id_fallback(true)
        .with_bootstrap_peers(vec![addr]);
    for error in [
        ResolveError::ProtectedContent,
        ResolveError::InvalidProtection,
    ] {
        let expected = if error == ResolveError::ProtectedContent {
            "encrypted content is not supported"
        } else {
            "invalid content protection metadata"
        };
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            p.resolve_vod_info_with_catalog(&format!("cid:{CID}"), ready(Err(error))),
        )
        .await;
        assert_eq!(
            result
                .expect("terminal metadata error tried peer fallback")
                .unwrap_err()
                .to_string(),
            expected
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn parsed_vod_protection_refuses_before_store_with_public_control() {
    for disk in [false, true] {
        for premium in [1, 2, 0] {
            let bytes = test_support::protection_vod_bytes(premium, disk);
            // Disk-store creation is a durable first-forbidden-boundary sentinel:
            // even a transient lease would create this directory.
            let cache = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
                "../../target/acceptance279-{}-{disk}-{premium}",
                std::process::id()
            ));
            assert!(!cache.exists());
            let mut p = provider().with_bep9_content_id_fallback(true);
            // Isolate the store boundary: with_cache also starts an independent
            // warm-peer writer that creates its directory before any resolution.
            p.cache_type = CacheType::Disk;
            p.cache_dir = cache.clone();
            let hash = ace_swarm::resolve::inspect_descriptor(&bytes)
                .ok()
                .map(|v| v.infohash);
            let result = p
                .resolve_vod_inner_with_catalog(&format!("cid:{CID}"), ready(Ok(bytes)))
                .await;
            if premium == 0 {
                let content = result.unwrap();
                assert_eq!(content.content_length(), 1);
                assert!(cache.is_dir(), "public control crosses store sentinel");
                assert!(p.seed_registry.get(&hash.unwrap()).is_some());
            } else {
                assert_eq!(
                    result.err().unwrap().to_string(),
                    if premium == 1 {
                        "encrypted content is not supported"
                    } else {
                        "invalid content protection metadata"
                    }
                );
                if let Some(hash) = hash {
                    assert!(p.seed_registry.get(&hash).is_none());
                }
                assert!(!cache.exists(), "refusal crossed disk store sentinel");
            }
            drop(p);
        }
    }
}

#[tokio::test]
async fn url_cache_metadata_does_not_authenticate_bare_hash_with_fallback() {
    let p = provider().with_bep9_content_id_fallback(true);
    let (bytes, _) = test_support::synthetic_live_transport(524_288);
    let info = stream_info_from_transport(&bytes).unwrap();
    let url =
        crate::transport_url::encode_transport_url("https://example.invalid/synthetic").unwrap();
    p.remember_transport_url(&url, &info);
    assert_eq!(
        p.resolve_live_info(&url).await.unwrap().infohash,
        info.infohash
    );
    assert!(p
        .verified_info_for_infohash(&infohash_hex(&info.infohash))
        .is_err());
    assert!(p.infohash_index.get(&info.infohash).is_none());
}

#[tokio::test]
async fn disk_vod_start_is_refused_with_no_space_when_the_reserve_cannot_fit() {
    for (cache_type, refused) in [(CacheType::Disk, true), (CacheType::Memory, false)] {
        let bytes = test_support::protection_vod_bytes(0, true);
        let cache = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "../../target/no-space-485-{}-{refused}",
            std::process::id()
        ));
        let mut p = provider().with_bep9_content_id_fallback(true);
        p.cache_type = cache_type;
        p.cache_dir = cache.clone();
        // No filesystem has u64::MAX bytes free; a memory store never asks.
        p.vod_disk_reserve = u64::MAX;
        let result = p
            .resolve_vod_inner_with_catalog(&format!("cid:{CID}"), ready(Ok(bytes)))
            .await;
        if refused {
            let error = result.err().expect("disk start refused");
            assert!(matches!(error, ProviderError::NoSpace(_)), "{error}");
            let start = crate::start_status::StartError::from_provider(&error);
            assert_eq!(start.code.as_str(), "no_space");
            assert!(start.message.contains("required"), "{}", start.message);
            assert!(start.message.contains("available"), "{}", start.message);
        } else {
            assert_eq!(result.unwrap().content_length(), 1);
        }
        drop(p);
        let _ = std::fs::remove_dir_all(&cache);
    }
}
