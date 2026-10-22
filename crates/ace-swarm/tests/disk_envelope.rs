use ace_swarm::resolve::{
    stream_info_from_transport, vod_info_from_transport, DEFAULT_CHUNK_LENGTH,
};
use ace_wire::disk_envelope::{decode_disk_metainfo, is_disk_envelope};
use ace_wire::handshake::HandshakeProtocol;
use ace_wire::WireError;
use cbc::cipher::{block_padding::NoPadding, block_padding::Pkcs7, BlockModeEncrypt, KeyIvInit};
use sha1::{Digest, Sha1};

const BARE: &[u8] = include_bytes!("../../../tests/vectors/metainfo/single-file-unsorted.torrent");
const ENCRYPTED_VECTOR: &[u8] =
    include_bytes!("../../../tests/vectors/metainfo/disk-envelope-unsorted.acestream");
const MARKER: &[u8; 8] = b"\x01\x02\x03\x04\x11\x02e.";
// Independent reference constant: derived from the protocol's documented static MD5 input.
// This intentionally duplicates the expected constant rather than importing production code.
const REFERENCE_DISK_KEY: [u8; 16] = [
    0x82, 0x4e, 0x0b, 0xc1, 0x14, 0xf5, 0x8c, 0xdb, 0x5a, 0x49, 0xa0, 0xc0, 0xf1, 0x42, 0xb4, 0x24,
];

type Encryptor = cbc::Encryptor<aes::Aes128>;

fn reference_encrypt_padded(plain: &[u8]) -> Vec<u8> {
    let encrypted = Encryptor::new(&REFERENCE_DISK_KEY.into(), &[0u8; 16].into())
        .encrypt_padded_vec::<Pkcs7>(plain);
    [MARKER.as_slice(), encrypted.as_slice()].concat()
}

fn reference_encrypt_raw(aligned_plain: &[u8]) -> Vec<u8> {
    let encrypted = Encryptor::new(&REFERENCE_DISK_KEY.into(), &[0u8; 16].into())
        .encrypt_padded_vec::<NoPadding>(aligned_plain);
    [MARKER.as_slice(), encrypted.as_slice()].concat()
}

fn expected_infohash(bytes: &[u8]) -> [u8; 20] {
    let marker = b"4:info";
    let start = bytes
        .windows(marker.len())
        .position(|w| w == marker)
        .unwrap()
        + marker.len();
    let end = bytes.len() - 1;
    Sha1::digest(&bytes[start..end]).into()
}

// Add a valid ignored root key, keeping the original info span unchanged, until the
// encoded dictionary has the requested length modulo one AES block.
fn metainfo_with_root_value_len(value_len: usize) -> Vec<u8> {
    let mut candidate = BARE[..BARE.len() - 1].to_vec();
    candidate.extend_from_slice(b"3:pad");
    candidate.extend_from_slice(format!("{value_len}:").as_bytes());
    candidate.extend(std::iter::repeat_n(b'x', value_len));
    candidate.push(b'e');
    candidate
}

fn metainfo_with_residue(residue: usize) -> Vec<u8> {
    for value_len in 0..64 {
        let candidate = metainfo_with_root_value_len(value_len);
        if candidate.len() % 16 == residue {
            return candidate;
        }
    }
    panic!("a small ignored root field should reach every AES-block residue");
}

fn metainfo_with_exact_len(target_len: usize) -> Vec<u8> {
    let low = target_len.saturating_sub(BARE.len() + 32);
    let high = target_len;
    (low..=high)
        .map(metainfo_with_root_value_len)
        .find(|candidate| candidate.len() == target_len)
        .expect("root padding can reach the requested metainfo length")
}

#[test]
fn reference_encrypted_vector_resolves_with_original_identity_and_standard_bt() {
    let got = vod_info_from_transport(ENCRYPTED_VECTOR)
        .expect("the public VOD resolver decodes the independent synthetic disk vector");
    let bare = vod_info_from_transport(BARE).expect("the plaintext vector remains supported");

    assert_eq!(got.infohash, expected_infohash(BARE));
    assert_eq!(got, bare);
    assert_eq!(got.peer_protocol, HandshakeProtocol::BitTorrent);
    assert_eq!(got.chunk_length, DEFAULT_CHUNK_LENGTH);
    assert_eq!(got.total_length, 16_385);
    assert_eq!(got.piece_length, 16_384);
    assert_eq!(got.piece_hashes, vec![[0xA5; 20], [0x5A; 20]]);
    assert_eq!(got.trackers, vec!["https://tracker.invalid/announce"]);
}

#[test]
fn valid_one_and_sixteen_byte_padding_preserve_the_metainfo_identity() {
    for pad_len in [1u8, 16] {
        let plain = metainfo_with_residue((16 - pad_len as usize) % 16);
        let padded = [plain.as_slice(), &vec![pad_len; pad_len as usize]].concat();
        let decoded = vod_info_from_transport(&reference_encrypt_raw(&padded)).unwrap();
        assert_eq!(decoded.infohash, expected_infohash(BARE));
        assert_eq!(decoded.total_length, 16_385);
    }
}

#[test]
fn recognizes_only_the_exact_complete_marker() {
    assert!(is_disk_envelope(MARKER));
    assert!(!is_disk_envelope(&MARKER[..7]));
    for index in 0..MARKER.len() {
        let mut changed = *MARKER;
        changed[index] ^= 1;
        assert!(!is_disk_envelope(&changed));
    }
}

#[test]
fn rejects_every_short_header_and_every_single_byte_marker_mutation() {
    for len in 0..24 {
        let bytes = if len <= MARKER.len() {
            MARKER[..len].to_vec()
        } else {
            [MARKER.as_slice(), &[0u8; 16][..len - MARKER.len()]].concat()
        };
        assert!(
            vod_info_from_transport(&bytes).is_err(),
            "accepted length {len}"
        );
    }
    for index in 0..MARKER.len() {
        let mut changed = *MARKER;
        changed[index] ^= 1;
        let bytes = [changed.as_slice(), &[0u8; 16][..]].concat();
        assert!(
            vod_info_from_transport(&bytes).is_err(),
            "accepted changed marker byte {index}"
        );
    }
}

#[test]
fn accepts_the_largest_aligned_envelope_below_the_cap_and_rejects_the_next() {
    // Plaintext length is one less than the maximum aligned ciphertext body, so strict
    // PKCS#7 adds one byte and the complete envelope lands exactly at 1 MiB minus 8.
    let max_plain = metainfo_with_exact_len(1_048_559);
    assert_eq!(max_plain.len(), 1_048_559);
    let largest = reference_encrypt_padded(&max_plain);
    assert_eq!(largest.len(), 1_048_568);
    let decoded = vod_info_from_transport(&largest).expect("largest in-cap envelope is accepted");
    assert_eq!(decoded.infohash, expected_infohash(BARE));

    // The next aligned ciphertext length puts total framed input 8 bytes over the cap.
    let next_plain = metainfo_with_exact_len(1_048_575);
    let above_cap = reference_encrypt_padded(&next_plain);
    assert_eq!(above_cap.len(), 1_048_584);
    assert_eq!(
        decode_disk_metainfo(&above_cap),
        Err(WireError::Invalid("disk envelope too large"))
    );

    let mut non_aligned = MARKER.to_vec();
    non_aligned.extend_from_slice(&[0u8; 17]);
    assert_eq!(
        decode_disk_metainfo(&non_aligned),
        Err(WireError::Invalid("bad disk envelope body length"))
    );

    let mut exact_cap = MARKER.to_vec();
    exact_cap.resize(1_048_576, 0);
    assert_eq!(
        decode_disk_metainfo(&exact_cap),
        Err(WireError::Invalid("bad disk envelope body length"))
    );
    let mut cap_plus_one = MARKER.to_vec();
    cap_plus_one.resize(1_048_577, 0);
    assert_eq!(
        decode_disk_metainfo(&cap_plus_one),
        Err(WireError::Invalid("disk envelope too large"))
    );
}

#[test]
fn rejects_zero_oversized_and_mixed_pkcs7_padding() {
    let cases: [(usize, &[u8]); 3] = [(1, &[0]), (1, &[17]), (2, &[1, 2])];
    for (pad_space, tail) in cases {
        let body = metainfo_with_residue(16 - pad_space);
        let aligned = [body.as_slice(), tail].concat();
        assert_eq!(aligned.len() % 16, 0);
        assert_eq!(
            decode_disk_metainfo(&reference_encrypt_raw(&aligned)),
            Err(WireError::Invalid("disk aes/pad"))
        );
    }
}

#[test]
fn encrypted_metainfo_keeps_duplicate_trailing_and_multifile_rejections() {
    let duplicate =
        b"d4:infod6:lengthi1e6:lengthi1e12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaaee";
    let trailing = b"d4:infod6:lengthi1e12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaaeex";
    let multifile = [
        b"d4:infod5:filesle6:lengthi1e12:piece lengthi16384e6:pieces20:".as_slice(),
        &[b'a'; 20],
        b"ee",
    ]
    .concat();
    let bad_geometry = b"d4:infod6:lengthi1e12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee";
    for bytes in [
        duplicate.as_slice(),
        trailing.as_slice(),
        multifile.as_slice(),
        bad_geometry.as_slice(),
    ] {
        assert!(vod_info_from_transport(&reference_encrypt_padded(bytes)).is_err());
    }
    assert_eq!(
        decode_disk_metainfo(&reference_encrypt_padded(multifile.as_slice())),
        Err(WireError::Invalid("multi-file metainfo is not supported"))
    );
    assert_eq!(
        decode_disk_metainfo(&reference_encrypt_padded(bad_geometry)),
        Err(WireError::Invalid("unsupported metainfo piece geometry"))
    );

    let mut excessive_depth = BARE[..BARE.len() - 1].to_vec();
    excessive_depth.extend_from_slice(b"5:extra");
    excessive_depth.extend(std::iter::repeat_n(b'l', 34));
    excessive_depth.extend(std::iter::repeat_n(b'e', 34));
    excessive_depth.push(b'e');
    assert!(vod_info_from_transport(&reference_encrypt_padded(&excessive_depth)).is_err());
    assert_eq!(
        decode_disk_metainfo(&reference_encrypt_padded(&excessive_depth)),
        Err(WireError::Invalid("metainfo nesting too deep"))
    );

    let mut excessive_values = BARE[..BARE.len() - 1].to_vec();
    excessive_values.extend_from_slice(b"5:extra");
    excessive_values.push(b'l');
    for _ in 0..65_537 {
        excessive_values.extend_from_slice(b"0:");
    }
    excessive_values.extend_from_slice(b"ee");
    assert_eq!(
        decode_disk_metainfo(&reference_encrypt_padded(&excessive_values)),
        Err(WireError::Invalid("too many metainfo values"))
    );

    let nested = reference_encrypt_padded(ENCRYPTED_VECTOR);
    assert!(vod_info_from_transport(&nested).is_err());
}

#[test]
fn live_transport_decoder_does_not_accept_a_vod_disk_envelope() {
    assert!(stream_info_from_transport(ENCRYPTED_VECTOR).is_err());
}
