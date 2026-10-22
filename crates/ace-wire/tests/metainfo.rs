use ace_wire::bencode::Bencode;
use ace_wire::metainfo::decode_metainfo;
use sha1::{Digest, Sha1};
use std::collections::BTreeMap;

const VALID: &[u8] = include_bytes!("../../../tests/vectors/metainfo/single-file-unsorted.torrent");

fn bstr(bytes: &[u8]) -> Vec<u8> {
    let mut out = format!("{}:", bytes.len()).into_bytes();
    out.extend_from_slice(bytes);
    out
}

fn dict(entries: &[(&[u8], Vec<u8>)]) -> Vec<u8> {
    let mut out = vec![b'd'];
    for (key, value) in entries {
        out.extend(bstr(key));
        out.extend(value);
    }
    out.push(b'e');
    out
}

fn int(value: i64) -> Vec<u8> {
    format!("i{value}e").into_bytes()
}

fn list(items: &[Vec<u8>]) -> Vec<u8> {
    let mut out = vec![b'l'];
    for item in items {
        out.extend(item);
    }
    out.push(b'e');
    out
}

fn info(length: i64, piece_length: i64, pieces: &[u8]) -> Vec<u8> {
    dict(&[
        (b"length", int(length)),
        (b"piece length", int(piece_length)),
        (b"pieces", bstr(pieces)),
        (b"name", bstr(b"synthetic")),
    ])
}

#[test]
fn decodes_binary_piece_hashes_and_hashes_the_original_unsorted_info_span() {
    let metainfo = decode_metainfo(VALID).expect("valid synthetic metainfo");
    let marker = b"4:info";
    let start = VALID
        .windows(marker.len())
        .position(|w| w == marker)
        .unwrap()
        + marker.len();
    let end = VALID.len() - 1;
    let expected: [u8; 20] = Sha1::digest(&VALID[start..end]).into();
    let canonical_info = Bencode::Dict(BTreeMap::from([
        (b"length".to_vec(), Bencode::Int(16_385)),
        (b"name".to_vec(), Bencode::Bytes(b"synthetic".to_vec())),
        (b"piece length".to_vec(), Bencode::Int(16_384)),
        (
            b"pieces".to_vec(),
            Bencode::Bytes([vec![0xA5; 20], vec![0x5A; 20]].concat()),
        ),
    ]));
    let reencoded_hash: [u8; 20] = Sha1::digest(canonical_info.encode()).into();

    assert_eq!(metainfo.infohash, expected);
    assert_ne!(metainfo.infohash, reencoded_hash);
    assert_eq!(metainfo.piece_length, 16_384);
    assert_eq!(metainfo.total_length, 16_385);
    assert_eq!(metainfo.piece_hashes, vec![[0xA5; 20], [0x5A; 20]]);
    assert_eq!(metainfo.trackers, vec!["https://tracker.invalid/announce"]);
}

#[test]
fn accepts_the_final_piece_shorter_than_piece_length() {
    let metainfo = decode_metainfo(VALID).unwrap();
    assert_eq!(metainfo.total_length % metainfo.piece_length, 1);
    assert_eq!(metainfo.piece_hashes.len(), 2);
}

#[test]
fn rejects_duplicate_keys_at_every_dictionary_depth() {
    let valid_info = info(1, 16_384, &[0x11; 20]);
    let dup_info = dict(&[(
        b"info",
        dict(&[
            (b"length", int(1)),
            (b"length", int(1)),
            (b"piece length", int(16_384)),
            (b"pieces", bstr(&[0x11; 20])),
        ]),
    )]);
    let dup_root = dict(&[(b"info", valid_info.clone()), (b"info", valid_info)]);
    let nested = dict(&[
        (b"info", info(1, 16_384, &[0x11; 20])),
        (b"metadata", dict(&[(b"x", bstr(b"a")), (b"x", bstr(b"b"))])),
    ]);
    assert!(decode_metainfo(&dup_info).is_err());
    assert!(decode_metainfo(&dup_root).is_err());
    assert!(decode_metainfo(&nested).is_err());
}

#[test]
fn rejects_trailing_bytes_and_noncanonical_integer_and_string_lengths() {
    for bytes in [
        [VALID, b"x"].concat(),
        b"d4:infod6:lengthi01e12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaaee".to_vec(),
        b"d4:infod6:lengthi1e12:piece lengthi16384e6:pieces020:aaaaaaaaaaaaaaaaaaaaee".to_vec(),
        b"d4:infod6:lengthi-0e12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaaee".to_vec(),
        b"d4:infod6:lengthi+1e12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaaee".to_vec(),
        b"d4:infod6:lengthie12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaaee".to_vec(),
        b"d4:infod6:lengthi9223372036854775808e12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaaee".to_vec(),
        b"d4:infod6:lengthi1e12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaa".to_vec(),
    ] {
        assert!(decode_metainfo(&bytes).is_err(), "accepted {bytes:?}");
    }
}

#[test]
fn rejects_invalid_single_file_geometry_and_piece_digest_count() {
    for bytes in [
        dict(&[(b"info", info(0, 16_384, &[0x11; 20]))]),
        dict(&[(b"info", info(-1, 16_384, &[0x11; 20]))]),
        dict(&[(b"info", info(1, 0, &[0x11; 20]))]),
        dict(&[(b"info", info(1, 16_383, &[0x11; 20]))]),
        dict(&[(b"info", info(1, 16_384, &[0x11; 19]))]),
        dict(&[(b"info", info(16_385, 16_384, &[0x11; 20]))]),
        dict(&[(b"info", info(1, 16_384, &[]))]),
        dict(&[(b"info", info(1, 16_384, &[0x11; 40]))]),
        dict(&[(
            b"info",
            dict(&[
                (b"piece length", int(16_384)),
                (b"pieces", bstr(&[0x11; 20])),
            ]),
        )]),
        dict(&[(
            b"info",
            dict(&[(b"length", int(1)), (b"pieces", bstr(&[0x11; 20]))]),
        )]),
        dict(&[(
            b"info",
            dict(&[(b"length", int(1)), (b"piece length", int(16_384))]),
        )]),
        dict(&[(
            b"info",
            dict(&[
                (b"length", int(1)),
                (b"piece length", int(16_384)),
                (b"pieces", bstr(&[0x11; 20])),
                (b"files", b"le".to_vec()),
            ]),
        )]),
        dict(&[(
            b"info",
            dict(&[
                (b"length", int(1)),
                (b"piece length", int(16_384)),
                (b"pieces", bstr(&[0x11; 20])),
                (b"files", int(1)),
            ]),
        )]),
    ] {
        assert!(decode_metainfo(&bytes).is_err(), "accepted {bytes:?}");
    }
}

#[test]
fn rejects_oversized_input_and_piece_length() {
    assert!(decode_metainfo(&vec![b'x'; 1_048_577]).is_err());
    let bytes = dict(&[(b"info", info(1, 8 * 1_048_576 + 16_384, &[0x11; 20]))]);
    assert!(decode_metainfo(&bytes).is_err());
}

#[test]
fn collects_and_deduplicates_announce_and_tier_trackers_in_order() {
    let info = info(1, 16_384, &[0x11; 20]);
    let announce_list = list(&[
        list(&[bstr(b"https://one.invalid/")]),
        list(&[bstr(b"udp://two.invalid:80")]),
    ]);
    let bytes = dict(&[
        (b"announce-list", announce_list),
        (b"info", info),
        (b"announce", bstr(b"https://one.invalid/")),
    ]);
    let got = decode_metainfo(&bytes).unwrap();
    assert_eq!(
        got.trackers,
        vec!["https://one.invalid/", "udp://two.invalid:80"]
    );
}

#[test]
fn rejects_malformed_and_unsupported_tracker_values() {
    for announce in [
        b"ftp://tracker.invalid/".to_vec(),
        b"https://bad tracker/".to_vec(),
        vec![0xff],
        b"https://bad\ntracker/".to_vec(),
        [b"https://".as_slice(), &vec![b'a'; 2_041]].concat(),
    ] {
        let bytes = dict(&[
            (b"announce", bstr(&announce)),
            (b"info", info(1, 16_384, &[0x11; 20])),
        ]);
        assert!(decode_metainfo(&bytes).is_err());
    }
    let wrong_tier = dict(&[
        (b"announce-list", bstr(b"https://tracker.invalid/")),
        (b"info", info(1, 16_384, &[0x11; 20])),
    ]);
    assert!(decode_metainfo(&wrong_tier).is_err());
    let wrong_inner_tier = dict(&[
        (b"announce-list", list(&[bstr(b"https://tracker.invalid/")])),
        (b"info", info(1, 16_384, &[0x11; 20])),
    ]);
    assert!(decode_metainfo(&wrong_inner_tier).is_err());
    let tier = list(
        &(0..129)
            .map(|_| bstr(b"udp://tracker.invalid:80"))
            .collect::<Vec<_>>(),
    );
    let too_many = dict(&[
        (b"announce-list", list(&[tier])),
        (b"info", info(1, 16_384, &[0x11; 20])),
    ]);
    assert!(decode_metainfo(&too_many).is_err());
}

#[test]
fn rejects_depth_and_node_budget_exhaustion() {
    let deep = [vec![b'l'; 34], vec![b'e'; 34]].concat();
    let deep = dict(&[(b"extra", deep), (b"info", info(1, 16_384, &[0x11; 20]))]);
    assert!(decode_metainfo(&deep).is_err());

    let mut many_nodes = b"d5:extral".to_vec();
    for _ in 0..65_537 {
        many_nodes.extend_from_slice(b"0:");
    }
    many_nodes.extend_from_slice(b"e4:info");
    many_nodes.extend(info(1, 16_384, &[0x11; 20]));
    many_nodes.push(b'e');
    assert!(decode_metainfo(&many_nodes).is_err());
}
