//! Profile identity, raw resume, geometry, and announcement behavior through the real registry.

use super::{BroadcastRegistry, ProducerKind};
use crate::broadcast_options::BroadcastOptions;
use crate::broadcast_persist::BroadcastPersist;
use crate::config::CacheType;
use ace_swarm::listen::SeedRegistry;
use ace_wire::bencode::Bencode;
use ace_wire::infohash::{infohash_of_descriptor, infohash_of_transport, transport_file_hash};
use ace_wire::transport::{decode_transport, encode_transport};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, watch};

fn temp_dir() -> PathBuf {
    let path = std::env::temp_dir().join(format!("outpace-options51-{}", rand::random::<u64>()));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn profile() -> BTreeMap<String, BroadcastOptions> {
    let mut options = BTreeMap::new();
    options.insert(
        "synthetic-origin".into(),
        BroadcastOptions {
            title: Some("Synthetic title".into()),
            bitrate_bytes_per_sec: Some(500_000),
            piece_length: Some(131_072),
            quality: Some("HD".into()),
            categories: Some(vec![]),
        },
    );
    options
}

#[test]
fn options51_raw_resume_matching() {
    let mut fields = BTreeMap::new();
    fields.insert(
        b"name".to_vec(),
        Bencode::Bytes(b"raw title bytes".to_vec()),
    );
    fields.insert(b"bitrate".to_vec(), Bencode::Int(500_000));
    fields.insert(b"piece_length".to_vec(), Bencode::Int(131_072));
    fields.insert(b"quality".to_vec(), Bencode::Bytes(b"HD".to_vec()));
    fields.insert(b"categories".to_vec(), Bencode::List(vec![]));
    fields.insert(
        b"opaque_existing_field".to_vec(),
        Bencode::Bytes(vec![0, 255]),
    );
    let descriptor = Bencode::Dict(fields);
    let selected = profile()["synthetic-origin"].clone();
    assert!(!selected.matches_descriptor(&descriptor));

    let mut fields = BTreeMap::new();
    fields.insert(
        b"name".to_vec(),
        Bencode::Bytes(b"Synthetic title".to_vec()),
    );
    fields.insert(b"bitrate".to_vec(), Bencode::Int(500_000));
    fields.insert(b"piece_length".to_vec(), Bencode::Int(131_072));
    fields.insert(b"quality".to_vec(), Bencode::Bytes(b"HD".to_vec()));
    fields.insert(b"categories".to_vec(), Bencode::List(vec![]));
    fields.insert(
        b"opaque_existing_field".to_vec(),
        Bencode::Bytes(vec![0, 255]),
    );
    let descriptor = Bencode::Dict(fields);
    assert!(selected.matches_descriptor(&descriptor));

    let mut omitted = BroadcastOptions::default();
    assert!(omitted.matches_descriptor(&Bencode::Dict(BTreeMap::new())));
    omitted.quality = Some("auto".into());
    assert!(!omitted.matches_descriptor(&Bencode::Dict(BTreeMap::new())));
    assert!(!omitted.matches_descriptor(&Bencode::Dict(BTreeMap::from([(
        b"quality".to_vec(),
        Bencode::Bytes(b"Auto".to_vec())
    ),]))));
    let ordered = BroadcastOptions {
        categories: Some(vec!["first".into(), "second".into()]),
        ..Default::default()
    };
    assert!(!ordered.matches_descriptor(&Bencode::Dict(BTreeMap::from([(
        b"categories".to_vec(),
        Bencode::List(vec![
            Bencode::Bytes(b"second".to_vec()),
            Bencode::Bytes(b"first".to_vec())
        ]),
    )]))));
}

#[tokio::test]
async fn options51_legacy_bytes_and_identity() {
    let stores = 8 << 20;
    let seed = SeedRegistry::new();
    let base = BroadcastRegistry::new();
    let unprofiled = base
        .mint("synthetic-origin", &[], &seed, stores)
        .await
        .unwrap();
    let decoded = decode_transport(&unprofiled.transport_bytes).unwrap();
    assert_eq!(decoded.piece_length, 65_536);
    assert_eq!(decoded.chunk_length, 16_384);
    assert_eq!(decoded.bitrate, Some(8_375));
    assert_eq!(
        decoded.raw.get(b"quality").and_then(Bencode::as_bytes),
        Some(b"auto".as_slice())
    );
    assert!(
        matches!(decoded.raw.get(b"categories"), Some(Bencode::List(items)) if items == &[Bencode::Bytes(b"other".to_vec())])
    );
    let mut legacy = BTreeMap::new();
    legacy.insert(
        b"name".to_vec(),
        Bencode::Bytes(b"synthetic-origin".to_vec()),
    );
    legacy.insert(b"piece_length".to_vec(), Bencode::Int(65_536));
    legacy.insert(b"chunk_length".to_vec(), Bencode::Int(16_384));
    legacy.insert(b"bitrate".to_vec(), Bencode::Int(8_375));
    legacy.insert(b"quality".to_vec(), Bencode::Bytes(b"auto".to_vec()));
    legacy.insert(
        b"categories".to_vec(),
        Bencode::List(vec![Bencode::Bytes(b"other".to_vec())]),
    );
    legacy.insert(b"authmethod".to_vec(), Bencode::Bytes(b"RSA".to_vec()));
    legacy.insert(b"pubkey".to_vec(), Bencode::Bytes(decoded.pubkey.clone()));
    legacy.insert(b"trackers".to_vec(), Bencode::List(vec![]));
    legacy.insert(b"allow_public_trackers".to_vec(), Bencode::Int(1));
    legacy.insert(
        b"outpace_name".to_vec(),
        Bencode::Bytes(b"synthetic-origin".to_vec()),
    );
    let expected_legacy_transport = encode_transport(&Bencode::Dict(legacy));
    assert_eq!(
        unprofiled.transport_bytes.as_slice(),
        expected_legacy_transport.as_slice()
    );
    assert_eq!(
        unprofiled.infohash,
        infohash_of_transport(&unprofiled.transport_bytes)
    );
    assert_eq!(
        unprofiled.content_id,
        transport_file_hash(&unprofiled.transport_bytes)
    );

    let registry = BroadcastRegistry::new_with_options(profile()).unwrap();
    let selected = registry
        .mint("synthetic-origin", &[], &seed, stores)
        .await
        .unwrap();
    let decoded = decode_transport(&selected.transport_bytes).unwrap();
    assert_eq!(decoded.name, "Synthetic title");
    assert_eq!(decoded.bitrate, Some(500_000));
    assert_eq!(decoded.piece_length, 131_072);
    assert_eq!(
        decoded.raw.get(b"quality").and_then(Bencode::as_bytes),
        Some(b"HD".as_slice())
    );
    assert!(
        matches!(decoded.raw.get(b"categories"), Some(Bencode::List(items)) if items.is_empty())
    );
    assert_ne!(selected.infohash, unprofiled.infohash);

    let desc = decode_transport(&selected.transport_bytes).unwrap().raw;
    let original_infohash = infohash_of_descriptor(&desc).unwrap();
    let mut changed_quality = desc.clone();
    if let Bencode::Dict(fields) = &mut changed_quality {
        fields.insert(b"quality".to_vec(), Bencode::Bytes(b"SD".to_vec()));
    }
    let mut changed_categories = desc.clone();
    if let Bencode::Dict(fields) = &mut changed_categories {
        fields.insert(
            b"categories".to_vec(),
            Bencode::List(vec![Bencode::Bytes(b"other".to_vec())]),
        );
    }
    assert_eq!(
        original_infohash,
        infohash_of_descriptor(&changed_quality).unwrap()
    );
    assert_eq!(
        original_infohash,
        infohash_of_descriptor(&changed_categories).unwrap()
    );
    assert_ne!(
        transport_file_hash(&encode_transport(&desc)),
        transport_file_hash(&encode_transport(&changed_quality))
    );
    assert_ne!(
        transport_file_hash(&encode_transport(&desc)),
        transport_file_hash(&encode_transport(&changed_categories))
    );
}

#[tokio::test]
async fn options51_raw_resume_matching_preserves_transport_key_and_reservation() {
    let dir = temp_dir();
    let seed = SeedRegistry::new();
    let registry = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
    let first = registry
        .mint("synthetic-origin", &[], &seed, 8 << 20)
        .await
        .unwrap();
    let before = BroadcastPersist::new(&dir)
        .load("synthetic-origin")
        .unwrap()
        .unwrap();

    let mut raw = decode_transport(&before.transport).unwrap().raw;
    if let Bencode::Dict(fields) = &mut raw {
        fields.remove(b"quality".as_slice());
        fields.insert(
            b"opaque_existing_field".to_vec(),
            Bencode::Bytes(b"preserve raw".to_vec()),
        );
    }
    let mut noncanonical = before.clone();
    noncanonical.transport = encode_transport(&raw);
    BroadcastPersist::new(&dir)
        .save("synthetic-origin", &noncanonical)
        .unwrap();

    let resumed = BroadcastRegistry::with_persist_and_options(
        &dir,
        CacheType::Memory,
        PathBuf::new(),
        BTreeMap::new(),
    )
    .unwrap();
    let loaded = resumed
        .reload_persisted(&seed, 8 << 20, &BTreeSet::new())
        .await
        .unwrap();
    let after = BroadcastPersist::new(&dir)
        .load("synthetic-origin")
        .unwrap()
        .unwrap();
    assert_eq!(after.transport, noncanonical.transport);
    assert_eq!(after.key_pkcs1_pem, noncanonical.key_pkcs1_pem);
    assert_eq!(
        loaded[0].transport_bytes.as_slice(),
        noncanonical.transport.as_slice()
    );
    assert_eq!(first.infohash, loaded[0].infohash);
    assert_eq!(loaded[0].cursor.start_piece(), 128);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn options51_legacy_geometry_and_cursor() {
    let dir = temp_dir();
    let registry = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
    let seed = SeedRegistry::new();
    let _broadcast = registry
        .mint("synthetic-origin", &[], &seed, 8 << 20)
        .await
        .unwrap();
    let rec = BroadcastPersist::new(&dir)
        .load("synthetic-origin")
        .unwrap()
        .unwrap();
    let mut decoded = decode_transport(&rec.transport).unwrap();
    if let Bencode::Dict(fields) = &mut decoded.raw {
        fields.insert(b"piece_length".to_vec(), Bencode::Int(98_304));
    }
    let mut legacy = rec.clone();
    legacy.transport = encode_transport(&decoded.raw);
    BroadcastPersist::new(&dir)
        .save("synthetic-origin", &legacy)
        .unwrap();

    let resumed = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
    let (broadcast, _producer) = resumed
        .acquire_producer("synthetic-origin", &[], &seed, 8 << 20, ProducerKind::Pull)
        .await
        .unwrap();
    assert_eq!(broadcast.geometry.piece_length, 98_304);
    assert_eq!(broadcast.cursor.start_piece(), 128);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn options51_preflight_precedes_effects_and_enforces_full_piece_budget() {
    for cache_type in [CacheType::Memory, CacheType::Disk] {
        let dir = temp_dir();
        let mut config = crate::config::Config {
            data_dir: dir.clone(),
            cache_type,
            cache_dir: dir.join("cache"),
            seed_store_bytes: 131_071,
            ..crate::config::Config::default()
        };
        config.broadcast_options = profile();
        assert_eq!(
            crate::broadcast::preflight_broadcasts(&config)
                .unwrap_err()
                .to_string(),
            "broadcast_store_too_small"
        );
        assert!(!dir.join("identity.seed").exists());
        assert!(!dir.join("broadcasts").exists());
        let _ = std::fs::remove_dir_all(dir);
    }
}

#[tokio::test]
async fn options51_transport_trackers_are_authoritative() {
    let dir = temp_dir();
    let registry = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = captured.clone();
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    registry.set_test_announcer(Arc::new(move |key, trackers| {
        sink.lock().unwrap().push((key, trackers));
        let notify = started_tx.clone();
        Box::pin(async move {
            let _ = notify.send(());
            std::future::pending::<()>().await;
        })
    }));
    let seed = SeedRegistry::new();
    let trackers = vec![
        "udp://synthetic.invalid/announce".to_owned(),
        "https://synthetic.invalid/announce".to_owned(),
    ];
    let broadcast = registry
        .mint("synthetic-origin", &trackers, &seed, 8 << 20)
        .await
        .unwrap();
    assert_eq!(broadcast.trackers.as_slice(), trackers.as_slice());
    let (_port_tx, port) = watch::channel(Some(4661));
    registry
        .ensure_announcing("synthetic-origin", port)
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), started_rx.recv())
        .await
        .unwrap()
        .unwrap();
    {
        let observed = captured.lock().unwrap();
        assert!(!observed.is_empty());
        assert!(observed
            .iter()
            .any(|(key, value)| *key == broadcast.infohash && value == &trackers));
    }
    registry.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn options51_resumed_announcer_uses_stored_trackers_for_both_keys() {
    async fn resume_with_trackers(stored: Vec<String>, process: Vec<String>) {
        let dir = temp_dir();
        let first_registry =
            BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
        let first_seed = SeedRegistry::new();
        let first = first_registry
            .mint("synthetic-origin", &stored, &first_seed, 8 << 20)
            .await
            .unwrap();
        first_registry.shutdown().await.unwrap();

        let registry = BroadcastRegistry::with_persist(&dir, CacheType::Memory, PathBuf::new());
        let seed = SeedRegistry::new();
        let loaded = registry
            .reload_persisted(&seed, 8 << 20, &BTreeSet::new())
            .await
            .unwrap();
        assert_eq!(loaded[0].trackers.as_slice(), stored.as_slice());
        assert_eq!(loaded[0].infohash, first.infohash);
        assert_eq!(loaded[0].content_id, first.content_id);

        let captured = Arc::new(Mutex::new(Vec::new()));
        let sink = captured.clone();
        registry.set_test_announcer(Arc::new(move |key, trackers| {
            sink.lock().unwrap().push((key, trackers));
            Box::pin(std::future::pending::<()>())
        }));
        let (_port_tx, port) = watch::channel(Some(4661));
        let state = crate::http::BroadcastState {
            registry: registry.clone(),
            seed_registry: seed,
            trackers: process,
            store_bytes: 8 << 20,
            inbound_peer_port: port,
        };
        state.ensure_announcing("synthetic-origin").await.unwrap();
        {
            let observed = captured.lock().unwrap();
            assert_eq!(observed.len(), 2);
            for key in [first.infohash, first.content_id] {
                assert!(observed.iter().any(|(captured_key, trackers)| {
                    *captured_key == key && trackers.as_slice() == stored.as_slice()
                }));
            }
        }
        state.shutdown().await.unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }

    resume_with_trackers(
        vec![
            "udp://stored.synthetic.invalid/announce".to_owned(),
            "https://stored.synthetic.invalid/announce".to_owned(),
        ],
        vec!["udp://changed-process.synthetic.invalid/announce".to_owned()],
    )
    .await;
    resume_with_trackers(
        Vec::new(),
        vec!["udp://changed-process.synthetic.invalid/announce".to_owned()],
    )
    .await;
}
