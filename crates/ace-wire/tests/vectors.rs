use ace_wire::infohash::{infohash_of_transport, is_transport_file};
use std::path::PathBuf;

fn vec_bytes(rel: &str) -> Vec<u8> {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/vectors").join(rel);
    std::fs::read(&p).unwrap_or_else(|e| panic!("read {:?}: {e}", p))
}

#[test]
fn infohash_matches_engine_ground_truth() {
    assert_eq!(hex::encode(infohash_of_transport(&vec_bytes("transport-01.bin"))),
               "34df422b80a4bd94ac1e51be9ede60364ec7a7dd");
    assert_eq!(hex::encode(infohash_of_transport(&vec_bytes("transport-02.bin"))),
               "ed2c05b3b022e9cc7b7c1ca46d20f10839dc4108");
}

#[test]
fn detects_transport_magic() {
    assert!(is_transport_file(&vec_bytes("transport-01.bin")));
    assert!(!is_transport_file(b"not a transport file"));
    assert!(!is_transport_file(b"AceStream")); // too short / partial
}
