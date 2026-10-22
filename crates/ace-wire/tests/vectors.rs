use ace_wire::handshake::{Handshake, HandshakeProtocol, PSTR};
use ace_wire::infohash::{infohash_of_transport, is_transport_file, transport_file_hash};
use ace_wire::transport::{decode_transport, TransportDescriptor};
use std::path::PathBuf;

fn vec_bytes(rel: &str) -> Vec<u8> {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/vectors")
        .join(rel);
    std::fs::read(&p).unwrap_or_else(|e| panic!("read {:?}: {e}", p))
}

#[test]
fn infohash_matches_engine_ground_truth() {
    assert_eq!(
        hex::encode(infohash_of_transport(&vec_bytes("transport-01.bin"))),
        "50e93529d3eb46a50506b14464185a15292d6e47"
    );
    assert_eq!(
        hex::encode(infohash_of_transport(&vec_bytes("transport-02.bin"))),
        "685edf209ccfdf88977c0d317e1407baca486067"
    );
}

#[test]
fn transport_file_hash_matches_cached_transport_names() {
    assert_eq!(
        hex::encode(transport_file_hash(&vec_bytes("transport-01.bin"))),
        "34df422b80a4bd94ac1e51be9ede60364ec7a7dd"
    );
    assert_eq!(
        hex::encode(transport_file_hash(&vec_bytes("transport-02.bin"))),
        "ed2c05b3b022e9cc7b7c1ca46d20f10839dc4108"
    );
}

#[test]
fn detects_transport_magic() {
    assert!(is_transport_file(&vec_bytes("transport-01.bin")));
    assert!(!is_transport_file(b"not a transport file"));
    assert!(!is_transport_file(b"AceStream")); // too short / partial
}

#[test]
fn decodes_captured_handshake() {
    let bytes = vec_bytes("messages/encrypter-handshake-peer-in.bin");
    let hs = Handshake::decode(&bytes).unwrap();
    assert_eq!(PSTR, b"AceStreamProtocol");
    assert_eq!(
        hex::encode(hs.infohash),
        "50e93529d3eb46a50506b14464185a15292d6e47"
    );
    assert_eq!(&hs.peer_id, b"R30------Ef2V8QOgmt4");
    assert_eq!(hs.reserved, [0u8; 8]);
    // re-encode must be byte-identical to the captured 66 bytes
    assert_eq!(hs.encode().to_vec(), bytes);
}

#[test]
fn random_peer_id_has_acestream_prefix() {
    let id = ace_wire::handshake::random_peer_id();
    assert_eq!(&id[..9], b"R30------");
    assert_eq!(id.len(), 20);
}

#[test]
fn rejects_wrong_pstr() {
    let mut bytes = vec_bytes("messages/encrypter-handshake-peer-in.bin");
    bytes[1] = b'X'; // corrupt pstr
    assert!(Handshake::decode(&bytes).is_err());
}

fn fixed_bittorrent_handshake(infohash: [u8; 20], peer_id: [u8; 20]) -> Vec<u8> {
    let mut bytes = b"\x13BitTorrent protocol".to_vec();
    bytes.extend([0u8; 8]);
    bytes.extend(infohash);
    bytes.extend(peer_id);
    bytes
}

#[test]
fn bittorrent_handshake_roundtrips_fixed_standard_wire_bytes() {
    let infohash = [0x42; 20];
    let peer_id = *b"-UT0001-123456789012";
    let bytes = fixed_bittorrent_handshake(infohash, peer_id);

    let decoded = Handshake::decode_with_protocol(&bytes, HandshakeProtocol::BitTorrent).unwrap();

    assert_eq!(bytes.len(), 68);
    assert_eq!(decoded.infohash, infohash);
    assert_eq!(decoded.peer_id, peer_id);
    assert_eq!(decoded.reserved, [0; 8]);
    assert_eq!(
        Handshake::new(infohash, peer_id).encode_with_protocol(HandshakeProtocol::BitTorrent),
        bytes
    );
}

#[test]
fn bittorrent_handshake_matches_synthetic_golden_vector() {
    let bytes = vec_bytes("handshakes/bittorrent-standard.bin");
    let decoded = Handshake::decode_with_protocol(&bytes, HandshakeProtocol::BitTorrent).unwrap();

    assert_eq!(decoded.infohash, std::array::from_fn(|index| index as u8));
    assert_eq!(decoded.peer_id, *b"-UT0001-123456789012");
    assert_eq!(
        Handshake::new(decoded.infohash, decoded.peer_id)
            .encode_with_protocol(HandshakeProtocol::BitTorrent),
        bytes
    );
}

#[test]
fn bittorrent_handshake_decoder_rejects_wrong_protocol_and_truncation() {
    let bytes = fixed_bittorrent_handshake([0x42; 20], *b"-UT0001-123456789012");
    let mut wrong_protocol = bytes.clone();
    wrong_protocol[1] = b'X';

    assert!(
        Handshake::decode_with_protocol(&wrong_protocol, HandshakeProtocol::BitTorrent).is_err()
    );
    assert!(Handshake::decode_with_protocol(&bytes[..67], HandshakeProtocol::BitTorrent).is_err());
    assert!(Handshake::decode_with_protocol(&bytes, HandshakeProtocol::AceStream).is_err());
}

#[test]
fn decodes_live_transport_syntheticchannel() {
    let d: TransportDescriptor = decode_transport(&vec_bytes("transport-01.bin")).unwrap();
    assert_eq!(d.piece_length, 1048576);
    assert_eq!(d.chunk_length, 16384);
    assert!(d.is_live); // live: no static piece hashes
    assert!(d.pieces.is_empty());
    assert!(d.name.starts_with("Synthetic Live Channel 1080"));
    assert_eq!(d.pubkey.len(), 124); // RSA DER
    assert_eq!(d.trackers.len(), 1);
    assert!(d.trackers[0].contains("tracker1.invalid"));
}

#[test]
fn decodes_live_transport_promo() {
    let d = decode_transport(&vec_bytes("transport-02.bin")).unwrap();
    assert_eq!(d.piece_length, 131072);
    assert_eq!(d.chunk_length, 16384);
    assert!(d.is_live);
    assert_eq!(d.name, "Synthetic Demo Channel");
    assert_eq!(
        d.trackers,
        vec!["udp://t1.torrentstream.org:2710/announce".to_string()]
    );
}

#[test]
fn rejects_non_transport() {
    assert!(decode_transport(b"not a transport file").is_err());
}
