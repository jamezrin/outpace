use super::*;

#[tokio::test]
async fn network_is_ace() {
    let p = AceProvider::new(Arc::new(Identity::generate()), 6878);
    assert_eq!(p.network(), "ace");
}

#[test]
fn startup_policy_builders_preserve_absent_and_explicit_values() {
    let startup = crate::config::StartupBufferConfig {
        target_ms: 12_000,
        max_bytes: 33_554_432,
        timeout_ms: 9_000,
    };
    let default = AceProvider::new(Arc::new(Identity::generate()), 6878);
    assert_eq!(default.prefetch_policy(), None);
    assert_eq!(
        default.startup_buffer_config(),
        crate::config::StartupBufferConfig::default()
    );

    let configured = AceProvider::new(Arc::new(Identity::generate()), 6878)
        .with_prefetch_pieces(Some(8))
        .with_startup_buffer(startup);
    assert_eq!(configured.prefetch_policy(), Some(8));
    assert_eq!(configured.startup_buffer_config(), startup);
}

#[test]
fn prefetch_derives_history_from_target_bitrate_and_payload_with_two_piece_margin() {
    assert_eq!(
        derived_prefetch_pieces(30_000, Some(1_000_000), 524_288, 128),
        60
    );
}

#[test]
fn prefetch_positive_fractional_byte_still_requires_one_history_piece() {
    assert_eq!(derived_prefetch_pieces(1, Some(1), 1_048_576, 96), 3);
}

#[test]
fn prefetch_fallback_depends_on_whether_prebuffer_is_enabled() {
    assert_eq!(derived_prefetch_pieces(30_000, None, 1_048_576, 96), 32);
    assert_eq!(derived_prefetch_pieces(0, None, 1_048_576, 96), 8);
}

#[test]
fn prefetch_disabled_with_known_bitrate_preserves_eight_pieces() {
    assert_eq!(derived_prefetch_pieces(0, Some(1_000_000), 524_288, 128), 8);
    assert_eq!(derived_prefetch_pieces(0, Some(0), 524_288, 128), 8);
}

#[test]
fn prefetch_zero_bitrate_uses_unknown_history_fallback() {
    assert_eq!(derived_prefetch_pieces(30_000, Some(0), 524_288, 128), 32);
}

#[test]
fn prefetch_bounds_huge_rates_payload_and_peer_window() {
    assert_eq!(
        derived_prefetch_pieces(u64::MAX, Some(u64::MAX), 0, 128),
        u64::MAX
    );
    assert_eq!(derived_prefetch_pieces(1_000, Some(1), 128, 128), 3);
    let mut descriptor = info();
    descriptor.metadata.bitrate = Some(u64::MAX);
    let recovery = LiveRecoveryConfig {
        max_reasm_pieces_ahead: 64,
        ..Default::default()
    };
    let provider =
        AceProvider::new(Arc::new(Identity::generate()), 6878).with_live_recovery(recovery);
    let depth = provider.prefetch_policy_for(&descriptor);
    assert_eq!(depth, 64);
}

#[test]
fn prefetch_explicit_override_is_not_reinterpreted() {
    let provider =
        AceProvider::new(Arc::new(Identity::generate()), 6878).with_prefetch_pieces(Some(3));
    assert_eq!(provider.prefetch_policy_for(&info()), 3);
}

#[tokio::test]
async fn leech_lease_evicts_registry_entry_when_dropped() {
    let reg = ace_swarm::listen::SeedRegistry::new();
    let ih = [9u8; 20];
    {
        let (_store, _lease) = reg.lease_store(ih, || {
            ace_swarm::store::PieceStore::new(1 << 20, 1 << 14, 1 << 20)
        });
        assert!(
            reg.serves(&ih),
            "served while the leech loop holds its lease"
        );
    }
    assert!(
        !reg.serves(&ih),
        "entry evicted after the leech loop drops its lease"
    );
}

#[test]
fn disk_store_dir_is_process_unique_per_instance() {
    let tmp = std::env::temp_dir().join(format!("outpace-uniqdir-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    let ih = [7u8; 20];
    // Two stores for the SAME infohash must land in DIFFERENT directories.
    let _s1 = build_piece_store(1 << 20, 1 << 14, 1 << 20, None, CacheType::Disk, &tmp, &ih);
    let _s2 = build_piece_store(1 << 20, 1 << 14, 1 << 20, None, CacheType::Disk, &tmp, &ih);
    let dirs: Vec<_> = std::fs::read_dir(&tmp)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(
        dirs.len(),
        2,
        "each store instance owns its own dir: {dirs:?}"
    );
    assert!(
        dirs.iter().all(|d| d.starts_with(&infohash_hex(&ih))),
        "dir names keep the readable infohash prefix: {dirs:?}"
    );
    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn disk_construction_failure_keeps_playback_store_at_zero_retention() {
    let invalid_root = std::env::temp_dir().join(format!(
        "outpace-cache-failure-{}-{}",
        std::process::id(),
        DISK_STORE_FAILURES.load(Ordering::Relaxed)
    ));
    let _ = std::fs::remove_file(&invalid_root);
    std::fs::write(&invalid_root, b"file blocks child directory creation").unwrap();
    let mut store = build_piece_store(
        8,
        4,
        128 * 1024 * 1024,
        None,
        CacheType::Disk,
        &invalid_root,
        &[0x38; 20],
    );

    store.put_chunk(0, 0, &[1, 2, 3, 4]);
    assert!(
        store.chunk(0, 0).is_none(),
        "disk failure policy must not retain the configured disk budget in RAM"
    );
    assert!(store.have_pieces().is_empty());
    std::fs::remove_file(invalid_root).unwrap();
}

#[tokio::test]
async fn unrecognized_id_shape_is_backend_error() {
    let p = AceProvider::new(Arc::new(Identity::generate()), 6878);
    // Neither a 40-hex infohash nor a cid:<40hex> content-id.
    assert!(matches!(
        p.open("not-a-hex-infohash").await,
        Err(ProviderError::Backend(_))
    ));
}

const PLACEHOLDER_ID: &str = "0123456789abcdef0123456789abcdef01234567";

fn test_provider() -> AceProvider {
    AceProvider::new(Arc::new(Identity::generate()), 0)
}

#[test]
fn cold_infohash_is_admitted_for_async_catalog_resolution() {
    let p = test_provider();
    assert!(
        p.check_openable(PLACEHOLDER_ID).is_ok(),
        "the synchronous pre-check must allow the asynchronous catalog lookup"
    );
}

#[tokio::test]
async fn cold_infohash_catalog_result_is_verified_and_indexed() {
    let (transport, _) = test_support::synthetic_live_transport(524_288);
    let expected = stream_info_from_transport(&transport).unwrap();
    let id = infohash_hex(&expected.infohash).to_ascii_uppercase();
    let p = test_provider();
    let actual = p
        .resolve_infohash_with_catalog(&id, std::future::ready(Ok(expected.clone())))
        .await
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(p.verified_info_for_infohash(&id).unwrap(), expected);
    assert_eq!(
        p.resolve_infohash_with_catalog(&id, async {
            panic!("verified index hit must not query the catalog again")
        })
        .await
        .unwrap(),
        expected
    );
}

#[tokio::test]
async fn cold_infohash_rejects_mismatched_catalog_descriptor_without_indexing_it() {
    let (transport, _) = test_support::synthetic_live_transport(524_288);
    let foreign = stream_info_from_transport(&transport).unwrap();
    let foreign_id = infohash_hex(&foreign.infohash);
    let p = test_provider();
    assert!(matches!(
        p.resolve_infohash_with_catalog(PLACEHOLDER_ID, std::future::ready(Ok(foreign)))
            .await,
        Err(ProviderError::Unresolvable(_))
    ));
    assert!(p.verified_info_for_infohash(PLACEHOLDER_ID).is_err());
    assert!(p.verified_info_for_infohash(&foreign_id).is_err());
}

#[tokio::test]
async fn cold_infohash_failed_lookup_is_short_cached() {
    let p = test_provider();
    assert!(p
        .resolve_infohash_with_catalog(
            PLACEHOLDER_ID,
            std::future::ready(Err(ResolveError::Catalog("not found"))),
        )
        .await
        .is_err());
    assert!(p
        .resolve_infohash_with_catalog(PLACEHOLDER_ID, async {
            panic!("a recent failure must suppress another catalog request")
        })
        .await
        .is_err());
}

#[tokio::test]
async fn cold_infohash_failure_expires_and_verified_metadata_wins() {
    let (transport, _) = test_support::synthetic_live_transport(524_288);
    let expected = stream_info_from_transport(&transport).unwrap();
    let id = infohash_hex(&expected.infohash);
    let p = test_provider();
    p.infohash_failures.lock().unwrap().push_back((
        expected.infohash,
        Instant::now() - INFOHASH_FAILURE_TTL - Duration::from_secs(1),
    ));
    assert_eq!(
        p.resolve_infohash_with_catalog(&id, std::future::ready(Ok(expected.clone())))
            .await
            .unwrap(),
        expected
    );
    p.remember_infohash_failure(expected.infohash);
    assert_eq!(
        p.resolve_infohash_with_catalog(&id, async {
            panic!("a verified descriptor must win over an earlier failure")
        })
        .await
        .unwrap(),
        expected
    );
}

#[test]
fn cold_infohash_failure_cache_is_bounded_and_deduplicated() {
    let p = test_provider();
    for n in 0..=INFOHASH_FAILURE_CAPACITY {
        p.remember_infohash_failure([n as u8; 20]);
    }
    p.remember_infohash_failure([INFOHASH_FAILURE_CAPACITY as u8; 20]);
    let entries = p.infohash_failures.lock().unwrap();
    assert_eq!(entries.len(), INFOHASH_FAILURE_CAPACITY);
    assert!(!entries.iter().any(|(key, _)| *key == [0; 20]));
    assert_eq!(
        entries
            .iter()
            .filter(|(key, _)| *key == [INFOHASH_FAILURE_CAPACITY as u8; 20])
            .count(),
        1
    );
}

#[tokio::test]
async fn unavailable_infohash_fails_closed_after_catalog_lookup() {
    let p = test_provider();
    let err = p
        .resolve_infohash_with_catalog(
            PLACEHOLDER_ID,
            std::future::ready(Err(ResolveError::Catalog("not found"))),
        )
        .await
        .expect_err("missing catalog metadata must not invent stream geometry");
    match err {
        ProviderError::Unresolvable(msg) => {
            assert!(msg.contains(&format!("cid:{PLACEHOLDER_ID}")), "{msg}");
        }
        other => panic!("expected Unresolvable, got {other:?}"),
    }
    assert!(p.check_openable(PLACEHOLDER_ID).is_ok());
    assert!(p.verified_info_for_infohash(PLACEHOLDER_ID).is_err());
}

#[tokio::test]
async fn bare_infohash_uses_the_verified_descriptors_geometry_and_pubkey() {
    let (transport, pubkey) = test_support::synthetic_live_transport(524_288);
    let verified = ace_swarm::resolve::stream_info_from_transport(&transport).unwrap();
    let p = test_provider();
    p.remember_live_descriptor(&verified);

    // Hex ids are case-insensitive: an upper-case infohash must hit the same entry.
    let id = infohash_hex(&verified.infohash).to_ascii_uppercase();
    assert!(p.check_openable(&id).is_ok());
    let info = p.resolve_live_info(&id).await.unwrap();
    assert_eq!(
        info.infohash,
        ace_wire::infohash::infohash_of_transport(&transport)
    );
    assert_eq!(info.piece_length, 524_288, "not the old 1 MiB guess");
    assert_eq!(info.chunk_length, 16_384);
    assert_eq!(info.sig_len, 96);
    assert_eq!(
        info.source_pubkey, pubkey,
        "pubkey enables RSA piece verification"
    );
    assert_eq!(info.metadata.title.as_deref(), Some("Synthetic Live"));
}

#[tokio::test]
async fn warm_infohash_vector_preserves_non_default_geometry() {
    let transport =
        include_bytes!("../../../../../tests/vectors/transport/synthetic-live-512k.bin");
    let descriptor = ace_wire::transport::decode_transport(transport).unwrap();
    let verified = ace_swarm::resolve::stream_info_from_transport(transport).unwrap();
    let p = test_provider();
    p.resolve_cache.put(PLACEHOLDER_ID, verified.clone());
    p.resolve_live_info(&format!("cid:{PLACEHOLDER_ID}"))
        .await
        .unwrap();

    let info = p
        .resolve_live_info(&infohash_hex(&verified.infohash))
        .await
        .unwrap();
    assert_eq!(info.piece_length, 524_288);
    assert_eq!(info.chunk_length, 16_384);
    assert_eq!(info.chunks_per_piece(), 32);
    assert_eq!(info.sig_len, 96);
    assert_eq!(info.source_pubkey, descriptor.pubkey);
    assert!(!info.source_pubkey.is_empty());
    assert_eq!(info.trackers, vec!["udp://tracker.invalid:80"]);
}

#[tokio::test]
async fn warm_infohash_continuity_authenticates_pieces_before_emitting() {
    let auth = ace_wire::live_auth::LiveSourceAuth::generate();
    let (transport, _) = test_support::synthetic_live_transport(524_288);
    // Keep the descriptor and its computed infohash bound to this test's signing key.
    let decoded = ace_wire::transport::decode_transport(&transport).unwrap();
    let ace_wire::bencode::Bencode::Dict(mut fields) = decoded.raw else {
        panic!("synthetic transport must be a dict");
    };
    fields.insert(
        b"pubkey".to_vec(),
        ace_wire::bencode::Bencode::Bytes(auth.pubkey_der()),
    );
    let transport =
        ace_wire::transport::encode_transport(&ace_wire::bencode::Bencode::Dict(fields));
    let verified = ace_swarm::resolve::stream_info_from_transport(&transport).unwrap();
    let p = test_provider();
    p.resolve_cache.put(PLACEHOLDER_ID, verified.clone());
    p.resolve_live_info(&format!("cid:{PLACEHOLDER_ID}"))
        .await
        .unwrap();
    let info = p
        .resolve_live_info(&infohash_hex(&verified.infohash))
        .await
        .unwrap();
    let start = 7;
    let mut reasm = PieceReassembler::new(info.piece_length, start)
        .with_piece_trailer(info.sig_len as u64)
        .with_source_pubkey(info.source_pubkey.clone())
        .with_max_pieces_ahead(default_live_recovery().max_reasm_pieces_ahead);
    let payload = vec![0x47; info.piece_length as usize - auth.signature_len()];
    let mut piece = payload.clone();
    piece.extend(auth.sign(&payload));
    let mut corrupt = piece.clone();
    corrupt[0] ^= 1;
    let chunk_length = info.chunk_length as usize;
    for (index, block) in corrupt.chunks(chunk_length).enumerate() {
        let result = reasm.add_block(start, (index * chunk_length) as u64, block);
        if index + 1 == info.chunks_per_piece() as usize {
            assert!(result.is_err(), "bad RSA signature must reject the piece");
        } else {
            result.unwrap();
        }
    }
    assert!(reasm.take_ready().is_empty());
    assert_eq!(reasm.next_needed(), start);

    for (index, block) in piece.chunks(chunk_length).enumerate() {
        reasm
            .add_block(start, (index * chunk_length) as u64, block)
            .unwrap();
    }
    assert_eq!(reasm.take_ready(), payload);
    assert_eq!(reasm.next_needed(), start + 1);
}

#[tokio::test]
async fn content_id_resolution_records_the_descriptor_under_its_infohash() {
    let (transport, _) = test_support::synthetic_live_transport(524_288);
    let verified = ace_swarm::resolve::stream_info_from_transport(&transport).unwrap();
    let p = test_provider();
    let ih = infohash_hex(&verified.infohash);
    assert!(p.verified_info_for_infohash(&ih).is_err());

    // A cached content-id resolution (no network) must also feed the infohash index.
    p.resolve_cache.put(PLACEHOLDER_ID, verified.clone());
    let via_cid = p
        .resolve_live_info(&format!("cid:{PLACEHOLDER_ID}"))
        .await
        .unwrap();
    assert_eq!(via_cid, verified);
    assert_eq!(p.resolve_live_info(&ih).await.unwrap(), verified);

    // The content id itself is not an infohash: the two namespaces stay separate (#165).
    assert!(matches!(
        p.verified_info_for_infohash(PLACEHOLDER_ID),
        Err(ProviderError::Unresolvable(_))
    ));
}

#[tokio::test]
async fn own_broadcast_opens_by_infohash_with_its_minted_geometry() {
    let seed = SeedRegistry::new();
    let broadcasts = crate::broadcast::BroadcastRegistry::new();
    let bc = broadcasts
        .mint(
            "t164",
            "T164",
            &["udp://tracker.invalid:80".into()],
            &seed,
            1 << 20,
        )
        .await
        .unwrap();
    let id = infohash_hex(&bc.infohash);

    // A provider that does not share the broadcast's registry still refuses it.
    assert!(matches!(
        test_provider().verified_info_for_infohash(&id),
        Err(ProviderError::Unresolvable(_))
    ));

    let p = test_provider().with_seed_registry(seed);
    assert!(p.check_openable(&id).is_ok());
    let info = p.resolve_live_info(&id).await.unwrap();
    assert_eq!(info.infohash, bc.infohash);
    assert_eq!(info.piece_length, crate::broadcast::PIECE_LENGTH);
    assert_eq!(info.source_pubkey, bc.auth.pubkey_der());
}

#[tokio::test]
async fn disabled_bep9_policy_does_not_read_peer_cache() {
    let (transport, _) = test_support::synthetic_live_transport(524_288);
    let info = stream_info_from_transport(&transport).unwrap();
    let p = test_provider();
    p.peer_resolve_cache.put(PLACEHOLDER_ID, info);
    let result = tokio::time::timeout(
        Duration::from_millis(100),
        p.resolve_content_id_with_catalog(
            PLACEHOLDER_ID,
            std::future::ready(Err(ResolveError::Catalog("synthetic catalog failure"))),
        ),
    )
    .await;
    let error = result
        .expect("disabled policy must fail before peer I/O")
        .unwrap_err();
    assert!(error.to_string().contains("fallback is disabled"));
}

#[tokio::test]
async fn peer_resolved_content_id_does_not_make_its_infohash_openable() {
    let (transport, _) = test_support::synthetic_live_transport(524_288);
    let verified = ace_swarm::resolve::stream_info_from_transport(&transport).unwrap();
    let p = test_provider().with_bep9_content_id_fallback(true);
    p.peer_resolve_cache.put(PLACEHOLDER_ID, verified.clone());

    // The caller who chose this content id gets its descriptor...
    let via_cid = p
        .resolve_live_info(&format!("cid:{PLACEHOLDER_ID}"))
        .await
        .unwrap();
    assert_eq!(via_cid, verified);

    // ...but BEP-9 binds the blob only to that content id, so the infohash stays closed.
    let ih = infohash_hex(&verified.infohash);
    assert!(matches!(
        p.verified_info_for_infohash(&ih),
        Err(ProviderError::Unresolvable(_))
    ));
    assert!(matches!(
        p.resolve_infohash_with_catalog(
            &ih,
            std::future::ready(Err(ResolveError::Catalog("not found")))
        )
        .await,
        Err(ProviderError::Unresolvable(_))
    ));
}

#[tokio::test]
async fn own_broadcast_wins_over_a_conflicting_index_entry() {
    use ace_wire::bencode::Bencode;
    let seed = SeedRegistry::new();
    let broadcasts = crate::broadcast::BroadcastRegistry::new();
    let bc = broadcasts
        .mint(
            "t164b",
            "T164B",
            &["udp://own.invalid:80".into()],
            &seed,
            1 << 20,
        )
        .await
        .unwrap();
    let own = seed.broadcast_transport_for_infohash(&bc.infohash).unwrap();

    // Same infohash, different (unbound) trackers.
    let decoded = ace_wire::transport::decode_transport(&own).unwrap();
    let Bencode::Dict(mut dict) = decoded.raw else {
        panic!("transport is not a dict");
    };
    dict.insert(
        b"trackers".to_vec(),
        Bencode::List(vec![Bencode::Bytes(b"udp://evil.invalid:80".to_vec())]),
    );
    let patched = ace_wire::transport::encode_transport(&Bencode::Dict(dict));
    assert_eq!(
        ace_wire::infohash::infohash_of_transport(&patched),
        bc.infohash
    );

    let p = test_provider().with_seed_registry(seed);
    let evil = ace_swarm::resolve::stream_info_from_transport(&patched).unwrap();
    assert_eq!(evil.trackers, vec!["udp://evil.invalid:80".to_string()]);
    p.remember_live_descriptor(&evil);

    let info = p
        .resolve_live_info(&infohash_hex(&bc.infohash))
        .await
        .unwrap();
    assert_eq!(info.trackers, vec!["udp://own.invalid:80".to_string()]);
}

#[test]
fn check_openable_leaves_non_infohash_ids_to_open() {
    let p = test_provider();
    assert!(p.check_openable(&format!("cid:{PLACEHOLDER_ID}")).is_ok());
    let turl =
        crate::transport_url::encode_transport_url("https://example.invalid/x.acelive").unwrap();
    assert!(p.check_openable(&turl).is_ok());
}

#[tokio::test]
async fn content_id_with_bad_hex_is_rejected_without_network() {
    let p = AceProvider::new(Arc::new(Identity::generate()), 6878);
    // `cid:` dispatch reaches resolution but the hex is invalid → immediate Backend error,
    // no discovery/connect attempted.
    assert!(matches!(
        p.open("cid:nothex").await,
        Err(ProviderError::Backend(_))
    ));
}

#[tokio::test]
async fn open_rejects_transport_url_with_blocked_host() {
    let p = AceProvider::new(Arc::new(Identity::generate()), 6878);
    // A valid transport-url id whose host is loopback is SSRF-blocked, so open() fails closed
    // via the decode+fetch branch (proven by the "transport url" message, not the generic
    // unrecognized-id fallthrough).
    let id = crate::transport_url::encode_transport_url("http://127.0.0.1:1/x").unwrap();
    let err = p.open(&id).await.err().expect("blocked host must error");
    match err {
        ProviderError::Backend(msg) => assert!(msg.contains("transport url"), "{msg}"),
        other => panic!("expected Backend error, got {other:?}"),
    }
}

#[tokio::test]
async fn open_rejects_transport_url_id_with_bad_scheme() {
    // Defense in depth: even a hand-crafted transport-url id encoding a non-http scheme is
    // rejected at the fetch layer, not just at the input surface.
    use base64ct::{Base64UrlUnpadded, Encoding};
    let p = AceProvider::new(Arc::new(Identity::generate()), 6878);
    let id = format!(
        "turl-{}",
        Base64UrlUnpadded::encode_string(b"file:///etc/passwd")
    );
    let err = p.open(&id).await.err().expect("bad scheme must error");
    match err {
        ProviderError::Backend(msg) => assert!(msg.contains("transport url"), "{msg}"),
        other => panic!("expected Backend error, got {other:?}"),
    }
}

#[cfg(unix)]
#[tokio::test]
async fn warm_peer_hints_never_grant_descriptor_authority() {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    let dir = std::env::temp_dir().join(format!(
        "outpace-190-authority-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
    let path = dir.join("recent-peers-v1");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    std::fs::write(
        &path,
        format!("OUTPACE-WARM-1\n{PLACEHOLDER_ID} {now} S 1.2.3.4 23456\n"),
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let provider = AceProvider::new(Arc::new(Identity::generate()), 0)
        .with_cache(CacheType::Memory, dir.clone());
    provider.warm_peers.initialized().await;
    let hash = hex20(PLACEHOLDER_ID).unwrap();
    assert_eq!(
        provider.warm_peers.candidate_addresses(&hash),
        vec!["1.2.3.4:23456".parse::<SocketAddrV4>().unwrap()]
    );
    assert!(provider.verified_info_for_infohash(PLACEHOLDER_ID).is_err());
    assert!(provider
        .resolve_infohash_with_catalog(
            PLACEHOLDER_ID,
            std::future::ready(Err(ResolveError::Catalog("synthetic catalog miss")))
        )
        .await
        .is_err());
    drop(provider);
    // Cache drop requests the owned writer to stop without blocking the async caller.
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => break,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => {
                    tokio::task::yield_now().await
                }
                Err(error) => panic!("temporary cache cleanup failed: {error}"),
            }
        }
    })
    .await
    .unwrap();
}
