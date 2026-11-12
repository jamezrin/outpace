use std::path::Path;

type Target = (&'static str, fn(&[u8]));

#[test]
fn replay_committed_synthetic_inputs() {
    let targets: [Target; 4] = [
        ("metadata", ace_fuzz::metadata),
        ("peer_frames", ace_fuzz::peer_frames),
        ("peer_extensions", ace_fuzz::peer_extensions),
        ("media_resync", ace_fuzz::media_resync),
    ];
    for (name, target) in targets {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("corpus")
            .join(name);
        let mut seeds: Vec<_> = std::fs::read_dir(root)
            .unwrap()
            .map(Result::unwrap)
            .collect();
        seeds.sort_by_key(|seed| seed.file_name());
        assert!(seeds.len() >= 4);
        for seed in seeds {
            assert!(seed.file_type().unwrap().is_file());
            let data = std::fs::read(seed.path()).unwrap();
            assert!(data.len() <= ace_fuzz::MAX_INPUT);
            target(&data);
        }
    }
}

#[test]
fn positive_controls_reach_accepted_paths() {
    use ace_wire::bencode::Bencode;
    use ace_wire::message::PeerMessage;
    let metadata = include_bytes!("../corpus/metadata/single-piece");
    assert!(ace_wire::metainfo::decode_metainfo(metadata).is_ok());
    assert!(Bencode::parse(metadata).is_ok());
    assert_eq!(
        PeerMessage::decode(&[0, 0, 0, 1, 2]).unwrap(),
        Some((PeerMessage::Interested, 5))
    );
    let message = include_bytes!("../corpus/peer_extensions/metadata-data");
    assert!(ace_wire::ut_metadata::MetadataMessage::parse(message).is_some());
    let peers = include_bytes!("../corpus/peer_extensions/peer-windows");
    assert!(!ace_wire::peer_exchange::parse_peer_exchange_detailed(peers).is_empty());
    let ts = include_bytes!("../corpus/media_resync/aligned-packets");
    let output = ace_media::mpegts::TsResync::new().push(ts);
    assert!(!output.is_empty() && ace_media::mpegts::is_aligned(&output));
}
