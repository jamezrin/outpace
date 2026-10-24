//! Config and startup rejection must precede identity, cache, and registry effects.

use crate::broadcast_options::BroadcastOptions;
use crate::broadcast_options::{parse_broadcast_options, BroadcastOptionsError};
use crate::config::Config;
use std::collections::BTreeMap;

fn profile_map() -> BTreeMap<String, BroadcastOptions> {
    BTreeMap::from([(
        "synthetic-origin".to_owned(),
        BroadcastOptions {
            title: Some("Synthetic title".to_owned()),
            bitrate_bytes_per_sec: Some(500_000),
            piece_length: Some(131_072),
            quality: Some("HD".to_owned()),
            categories: Some(vec![]),
        },
    )])
}

fn assert_selected_profile(broadcast: &crate::broadcast::Broadcast) {
    let decoded = ace_wire::transport::decode_transport(&broadcast.transport_bytes).unwrap();
    assert_eq!(decoded.name, "Synthetic title");
    assert_eq!(decoded.bitrate, Some(500_000));
    assert_eq!(decoded.piece_length, 131_072);
    assert_eq!(
        decoded.raw.get(b"quality").unwrap().as_bytes(),
        Some(b"HD".as_slice())
    );
    assert!(matches!(
        decoded.raw.get(b"categories"),
        Some(ace_wire::bencode::Bencode::List(items)) if items.is_empty()
    ));
}

fn temp_dir(label: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "outpace-options51-{label}-{}",
        rand::random::<u64>()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn options51_config_env_boundary_and_direct_config_validation() {
    let mut config = Config::default();
    crate::runtime::apply_broadcast_options_env(&mut config, Err(std::env::VarError::NotPresent))
        .unwrap();
    assert!(config.broadcast_options.is_empty());
    assert_eq!(
        crate::runtime::apply_broadcast_options_env(
            &mut config,
            Err(std::env::VarError::NotUnicode("secret".into()))
        )
        .unwrap_err()
        .to_string(),
        "invalid_broadcast_options"
    );
    assert_eq!(
        parse_broadcast_options(Some("  ")).unwrap_err().to_string(),
        "invalid_broadcast_options"
    );
    let invalid = parse_broadcast_options(Some(r#"{"synthetic-origin":{"piece_length":98304}}"#))
        .unwrap_err();
    assert_eq!(invalid.to_string(), "invalid_broadcast_options");
    let _: BroadcastOptionsError = invalid;
}

#[tokio::test]
async fn options51_preflight_precedes_runtime_effects() {
    let dir = std::env::temp_dir().join(format!(
        "outpace-options51-runtime-{}",
        rand::random::<u64>()
    ));
    let sentinel = dir.join("cache-sentinel");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(&sentinel, b"keep").unwrap();
    let config = Config {
        data_dir: dir.clone(),
        cache_dir: dir.join("cache"),
        seed_store_bytes: 131_071,
        broadcast_options: std::collections::BTreeMap::from([(
            "synthetic-origin".into(),
            crate::broadcast_options::BroadcastOptions {
                piece_length: Some(131_072),
                ..Default::default()
            },
        )]),
        ..Config::default()
    };
    let result = crate::runtime::build_runtime(config, vec![]).await;
    let error = match result {
        Err(error) => error.to_string(),
        Ok(runtime) => {
            runtime.broadcasts.shutdown().await.unwrap();
            runtime.manager.shutdown().await;
            panic!("undersized profile unexpectedly built a runtime");
        }
    };
    assert_eq!(error, "broadcast_store_too_small");
    assert!(!dir.join("identity.seed").exists());
    assert!(!dir.join("broadcasts").exists());
    assert_eq!(std::fs::read(&sentinel).unwrap(), b"keep");
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn options51_runtime_mint_and_startup_pull_use_the_named_profile() {
    let mint_dir = temp_dir("runtime-mint");
    let runtime = crate::runtime::build_runtime(
        Config {
            data_dir: mint_dir.clone(),
            networks: vec![],
            enable_inbound: false,
            broadcast_options: profile_map(),
            ..Config::default()
        },
        vec![],
    )
    .await
    .unwrap();
    let minted = crate::runtime::mint_broadcast(&runtime, "synthetic-origin")
        .await
        .unwrap();
    assert_selected_profile(&minted);
    runtime.broadcasts.shutdown().await.unwrap();
    runtime.manager.shutdown().await;
    let _ = std::fs::remove_dir_all(&mint_dir);

    let source_dir = temp_dir("startup-pull");
    let mut sources = BTreeMap::new();
    sources.insert(
        "synthetic-origin".to_owned(),
        crate::broadcast_pull::BroadcastSourceConfig {
            url: "http://127.0.0.1:1/live".to_owned(),
            connect_timeout_secs: 5,
            read_timeout_secs: 15,
            reconnect_initial_secs: 1,
            reconnect_max_secs: 30,
        },
    );
    let runtime = crate::runtime::build_runtime(
        Config {
            data_dir: source_dir.clone(),
            bind: "127.0.0.1:0".parse().unwrap(),
            rtmp_bind: "127.0.0.1:0".parse().unwrap(),
            networks: vec![],
            enable_inbound: false,
            allow_non_global_broadcast_source: true,
            broadcast_sources: sources,
            broadcast_options: profile_map(),
            seed_ttl_secs: 0,
            ..Config::default()
        },
        vec![],
    )
    .await
    .unwrap();
    let registry = runtime.broadcasts.registry.clone();
    let (opened_tx, mut opened_rx) = tokio::sync::mpsc::unbounded_channel();
    let open: crate::broadcast_pull::TestSourceOpen =
        std::sync::Arc::new(move |_, _, _, mut cancel| {
            let opened = opened_tx.clone();
            Box::pin(async move {
                let _ = opened.send(());
                let _ = cancel.changed().await;
                Err(ace_swarm::http_fetch::FetchError::Cancelled)
            })
        });
    registry.set_source_test_hooks(crate::broadcast_pull::SourceTestHooks {
        open: Some(open),
        ..Default::default()
    });
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = crate::runtime::serve_http_until(runtime, async move {
        let _ = shutdown_rx.await;
    });
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => panic!("runtime server returned before startup source opened: {result:?}"),
        opened = tokio::time::timeout(std::time::Duration::from_secs(5), opened_rx.recv()) => {
            opened.expect("startup-declared source should reach the real worker")
                .expect("fake source opener should notify");
        }
    }
    let broadcast = registry.get("synthetic-origin").await.unwrap();
    assert_selected_profile(&broadcast);
    shutdown_tx.send(()).unwrap();
    server.await.unwrap();
    let _ = std::fs::remove_dir_all(source_dir);
}

#[tokio::test]
async fn options51_unused_profile_does_not_mint_and_conflict_without_source_fails_preflight() {
    let unused_dir = temp_dir("unused-profile");
    let runtime = crate::runtime::build_runtime(
        Config {
            data_dir: unused_dir.clone(),
            networks: vec![],
            enable_inbound: false,
            broadcast_options: profile_map(),
            ..Config::default()
        },
        vec![],
    )
    .await
    .unwrap();
    assert!(runtime.broadcasts.registry.names().await.is_empty());
    assert!(!unused_dir.join("broadcasts").exists());
    runtime.broadcasts.shutdown().await.unwrap();
    runtime.manager.shutdown().await;
    let _ = std::fs::remove_dir_all(&unused_dir);

    let conflict_dir = temp_dir("profile-conflict");
    let registry = crate::broadcast::BroadcastRegistry::with_persist(
        &conflict_dir,
        crate::config::CacheType::Memory,
        std::path::PathBuf::new(),
    );
    let seed = ace_swarm::listen::SeedRegistry::new();
    registry
        .mint("synthetic-origin", &[], &seed, 8 << 20)
        .await
        .unwrap();
    registry.shutdown().await.unwrap();
    let error = crate::runtime::build_runtime(
        Config {
            data_dir: conflict_dir.clone(),
            networks: vec![],
            enable_inbound: false,
            broadcast_options: profile_map(),
            ..Config::default()
        },
        vec![],
    )
    .await
    .err()
    .expect("conflict must fail without a declared source")
    .to_string();
    assert_eq!(error, "broadcast_options_conflict");
    assert!(!conflict_dir.join("identity.seed").exists());
    assert!(conflict_dir
        .join("broadcasts/synthetic-origin.json")
        .exists());
    let _ = std::fs::remove_dir_all(conflict_dir);
}

#[tokio::test]
async fn options51_invalid_enumeration_and_legacy_capacity_fail_before_identity() {
    let invalid_dir = temp_dir("configured-invalid");
    let records = invalid_dir.join("broadcasts");
    std::fs::create_dir_all(&records).unwrap();
    std::fs::write(records.join("synthetic-origin.json"), b"not-json").unwrap();
    let invalid_error = crate::runtime::build_runtime(
        Config {
            data_dir: invalid_dir.clone(),
            broadcast_options: profile_map(),
            networks: vec![],
            enable_inbound: false,
            ..Config::default()
        },
        vec![],
    )
    .await
    .err()
    .expect("a configured corrupt record must fail startup")
    .to_string();
    assert_eq!(invalid_error, "broadcast_state_invalid");
    assert!(!invalid_dir.join("identity.seed").exists());
    let _ = std::fs::remove_dir_all(&invalid_dir);

    let enumeration_dir = temp_dir("enumeration-failure");
    std::fs::write(enumeration_dir.join("broadcasts"), b"not-a-directory").unwrap();
    let enumeration_error = crate::runtime::build_runtime(
        Config {
            data_dir: enumeration_dir.clone(),
            networks: vec![],
            enable_inbound: false,
            ..Config::default()
        },
        vec![],
    )
    .await
    .err()
    .expect("record enumeration failure must fail startup")
    .to_string();
    assert_eq!(enumeration_error, "broadcast_persistence_failed");
    assert!(!enumeration_dir.join("identity.seed").exists());
    let _ = std::fs::remove_dir_all(&enumeration_dir);

    let capacity_dir = temp_dir("legacy-capacity");
    let registry = crate::broadcast::BroadcastRegistry::with_persist_and_options(
        &capacity_dir,
        crate::config::CacheType::Memory,
        std::path::PathBuf::new(),
        profile_map(),
    )
    .unwrap();
    let seed = ace_swarm::listen::SeedRegistry::new();
    registry
        .mint("synthetic-origin", &[], &seed, 1 << 20)
        .await
        .unwrap();
    registry.shutdown().await.unwrap();
    let capacity_error = crate::runtime::build_runtime(
        Config {
            data_dir: capacity_dir.clone(),
            seed_store_bytes: 131_071,
            networks: vec![],
            enable_inbound: false,
            ..Config::default()
        },
        vec![],
    )
    .await
    .err()
    .expect("valid persisted 128 KiB geometry must fit the budget")
    .to_string();
    assert_eq!(capacity_error, "broadcast_store_too_small");
    assert!(!capacity_dir.join("identity.seed").exists());
    let _ = std::fs::remove_dir_all(capacity_dir);
}

#[tokio::test]
async fn options51_reload_rechecks_raw_profile_after_successful_preflight() {
    let dir = temp_dir("reload-recheck");
    let registry = crate::broadcast::BroadcastRegistry::with_persist_and_options(
        &dir,
        crate::config::CacheType::Memory,
        std::path::PathBuf::new(),
        profile_map(),
    )
    .unwrap();
    let seed = ace_swarm::listen::SeedRegistry::new();
    let original = registry
        .mint("synthetic-origin", &[], &seed, 8 << 20)
        .await
        .unwrap();
    registry.shutdown().await.unwrap();
    let persist = crate::broadcast_persist::BroadcastPersist::new(&dir);
    let before = persist.load("synthetic-origin").unwrap().unwrap();
    let config = Config {
        data_dir: dir.clone(),
        broadcast_options: profile_map(),
        networks: vec![],
        enable_inbound: false,
        ..Config::default()
    };
    crate::runtime::preflight_broadcasts(&config).unwrap();

    let mut raw = ace_wire::transport::decode_transport(&before.transport)
        .unwrap()
        .raw;
    if let ace_wire::bencode::Bencode::Dict(fields) = &mut raw {
        fields.insert(
            b"name".to_vec(),
            ace_wire::bencode::Bencode::Bytes(b"Changed synthetic title".to_vec()),
        );
    }
    let mut changed = before.clone();
    changed.transport = ace_wire::transport::encode_transport(&raw);
    persist.save("synthetic-origin", &changed).unwrap();

    let reload_registry = crate::broadcast::BroadcastRegistry::with_persist_and_options(
        &dir,
        crate::config::CacheType::Memory,
        std::path::PathBuf::new(),
        profile_map(),
    )
    .unwrap();
    let reload_seed = ace_swarm::listen::SeedRegistry::new();
    let error = match reload_registry
        .reload_persisted(
            &reload_seed,
            8 << 20,
            &std::collections::BTreeSet::from(["synthetic-origin".to_owned()]),
        )
        .await
    {
        Ok(_) => panic!("modified persisted profile unexpectedly reloaded"),
        Err(error) => error,
    };
    assert_eq!(error.code(), "broadcast_options_conflict");
    let boxed_error: Box<dyn std::error::Error> = Box::new(error);
    let boxed_debug = format!("{boxed_error:?}");
    assert!(!reload_seed.serves(&original.infohash));
    assert!(!reload_seed.serves(&original.content_id));
    let after = persist.load("synthetic-origin").unwrap().unwrap();
    assert_eq!(after.transport, changed.transport);
    assert_eq!(after.key_pkcs1_pem, before.key_pkcs1_pem);
    assert_eq!(after.next_piece, before.next_piece);
    assert_eq!(after.reserved_until, before.reserved_until);
    reload_registry.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(dir);
    assert_eq!(
        boxed_debug, "broadcast_options_conflict",
        "the reload error is boxed directly for runtime/CLI propagation"
    );
}

#[tokio::test]
async fn options51_configured_invalid_reload_error_has_sanitized_boxed_debug() {
    let dir = temp_dir("reload-invalid-presentation");
    let original_registry = crate::broadcast::BroadcastRegistry::with_persist(
        &dir,
        crate::config::CacheType::Memory,
        std::path::PathBuf::new(),
    );
    let original_seed = ace_swarm::listen::SeedRegistry::new();
    let original = original_registry
        .mint("synthetic-origin", &[], &original_seed, 8 << 20)
        .await
        .unwrap();
    original_registry.shutdown().await.unwrap();

    let config = Config {
        data_dir: dir.clone(),
        broadcast_sources: BTreeMap::from([(
            "synthetic-origin".to_owned(),
            crate::broadcast_pull::BroadcastSourceConfig {
                url: "https://source.synthetic.invalid/live".to_owned(),
                connect_timeout_secs: 5,
                read_timeout_secs: 15,
                reconnect_initial_secs: 1,
                reconnect_max_secs: 30,
            },
        )]),
        networks: vec![],
        enable_inbound: false,
        ..Config::default()
    };
    crate::runtime::preflight_broadcasts(&config).unwrap();

    let persist = crate::broadcast_persist::BroadcastPersist::new(&dir);
    let before = persist.load("synthetic-origin").unwrap().unwrap();
    let mut raw = ace_wire::transport::decode_transport(&before.transport)
        .unwrap()
        .raw;
    if let ace_wire::bencode::Bencode::Dict(fields) = &mut raw {
        fields.insert(
            b"outpace_name".to_vec(),
            ace_wire::bencode::Bencode::Bytes(b"synthetic-other".to_vec()),
        );
    }
    let mut changed = before.clone();
    changed.transport = ace_wire::transport::encode_transport(&raw);
    persist.save("synthetic-origin", &changed).unwrap();

    let reload_registry = crate::broadcast::BroadcastRegistry::with_persist(
        &dir,
        crate::config::CacheType::Memory,
        std::path::PathBuf::new(),
    );
    let reload_seed = ace_swarm::listen::SeedRegistry::new();
    let error = match reload_registry
        .reload_persisted(
            &reload_seed,
            8 << 20,
            &std::collections::BTreeSet::from(["synthetic-origin".to_owned()]),
        )
        .await
    {
        Ok(_) => panic!("configured malformed record unexpectedly reloaded"),
        Err(error) => error,
    };
    assert_eq!(error.code(), "broadcast_state_invalid");
    let boxed_error: Box<dyn std::error::Error> = Box::new(error);
    let boxed_debug = format!("{boxed_error:?}");
    assert!(!reload_seed.serves(&original.infohash));
    assert!(!reload_seed.serves(&original.content_id));
    let after = persist.load("synthetic-origin").unwrap().unwrap();
    assert_eq!(after.transport, changed.transport);
    assert_eq!(after.key_pkcs1_pem, before.key_pkcs1_pem);
    assert_eq!(after.next_piece, before.next_piece);
    assert_eq!(after.reserved_until, before.reserved_until);
    reload_registry.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(dir);
    assert_eq!(
        boxed_debug, "broadcast_state_invalid",
        "configured invalid reload errors are boxed directly for runtime/CLI propagation"
    );
}
