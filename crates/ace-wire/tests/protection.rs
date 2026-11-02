use ace_wire::{bencode::Bencode, metainfo::decode_metainfo, protection::Protection, transport};

fn set(root: &mut Bencode, key: &[u8], value: Bencode) {
    let Bencode::Dict(fields) = root else {
        panic!("synthetic dictionary")
    };
    fields.insert(key.to_vec(), value);
}

#[test]
fn wrapped_protection_uses_root_and_strict_values() {
    for (value, expected) in [
        (None, Protection::Public),
        (Some(0), Protection::Public),
        (Some(1), Protection::Protected),
    ] {
        let mut root = Bencode::parse(b"d12:chunk_lengthi16384e12:piece_lengthi131072ee").unwrap();
        if let Some(value) = value {
            set(&mut root, b"premium", Bencode::Int(value));
        }
        set(
            &mut root,
            b"info",
            Bencode::Dict([(b"premium".to_vec(), Bencode::Int(1))].into()),
        );
        let bytes = transport::encode_transport_with_key(
            &root,
            &transport::TRANSPORT_KEY,
            &transport::TRANSPORT_IV,
        );
        let descriptor = transport::decode_transport(&bytes).unwrap();
        assert_eq!(descriptor.protection().unwrap(), expected);
    }
}

#[test]
fn malformed_protection_is_not_public() {
    for value in [
        Bencode::Int(-1),
        Bencode::Int(2),
        Bencode::Bytes(b"1".to_vec()),
        Bencode::List(vec![]),
    ] {
        assert!(ace_wire::protection::classify(Some(&value)).is_err());
    }
}

#[test]
fn metainfo_protection_uses_nested_authority() {
    let fixture = include_bytes!("../../../tests/vectors/metainfo/single-file-unsorted.torrent");
    for (value, expected) in [
        (None, Protection::Public),
        (Some(0), Protection::Public),
        (Some(1), Protection::Protected),
    ] {
        let mut root = Bencode::parse(fixture).unwrap();
        set(&mut root, b"premium", Bencode::Int(1));
        let Bencode::Dict(fields) = &mut root else {
            unreachable!()
        };
        if let Some(value) = value {
            set(
                fields.get_mut(b"info".as_slice()).unwrap(),
                b"premium",
                Bencode::Int(value),
            );
        }
        assert_eq!(
            decode_metainfo(&root.encode()).unwrap().protection,
            expected
        );
    }
}

#[test]
fn malformed_authoritative_fields_are_typed_errors() {
    let fixture = include_bytes!("../../../tests/vectors/metainfo/single-file-unsorted.torrent");
    for value in [
        Bencode::Int(-1),
        Bencode::Int(2),
        Bencode::Bytes(b"1".to_vec()),
        Bencode::List(vec![]),
        Bencode::Dict(Default::default()),
    ] {
        let mut root = Bencode::parse(fixture).unwrap();
        set(&mut root, b"premium", Bencode::Int(0));
        let Bencode::Dict(fields) = &mut root else {
            unreachable!()
        };
        set(
            fields.get_mut(b"info".as_slice()).unwrap(),
            b"premium",
            value.clone(),
        );
        assert!(matches!(
            decode_metainfo(&root.encode()),
            Err(ace_wire::WireError::InvalidProtection)
        ));
        let mut wrapped =
            Bencode::parse(b"d12:chunk_lengthi16384e12:piece_lengthi131072ee").unwrap();
        set(&mut wrapped, b"premium", value);
        let descriptor =
            transport::decode_transport(&transport::encode_transport(&wrapped)).unwrap();
        assert!(matches!(
            descriptor.protection(),
            Err(ace_wire::WireError::InvalidProtection)
        ));
    }
}

#[test]
fn protected_metainfo_hashes_original_info_span() {
    use sha1::{Digest, Sha1};
    // Deliberately unsorted original info dictionary: re-encoding changes its identity.
    let info = b"d7:premiumi1e4:name9:synthetic6:lengthi1e12:piece lengthi16384e6:pieces20:abcdefghijklmnopqrste";
    let mut bytes = b"d4:info".to_vec();
    bytes.extend_from_slice(info);
    bytes.push(b'e');
    let decoded = decode_metainfo(&bytes).unwrap();
    assert_eq!(decoded.protection, Protection::Protected);
    assert_eq!(decoded.infohash, <[u8; 20]>::from(Sha1::digest(info)));
    assert_ne!(
        decoded.infohash,
        <[u8; 20]>::from(Sha1::digest(Bencode::parse(info).unwrap().encode()))
    );
}

#[test]
fn disk_envelope_preserves_nested_protection_and_original_hash() {
    use cbc::cipher::{block_padding::Pkcs7, BlockModeEncrypt, KeyIvInit};
    let fixture = include_bytes!("../../../tests/vectors/metainfo/single-file-unsorted.torrent");
    for value in [
        None,
        Some(Bencode::Int(0)),
        Some(Bencode::Int(1)),
        Some(Bencode::Int(2)),
        Some(Bencode::Bytes(vec![])),
    ] {
        let mut root = Bencode::parse(fixture).unwrap();
        set(&mut root, b"premium", Bencode::Int(1));
        let Bencode::Dict(fields) = &mut root else {
            unreachable!()
        };
        if let Some(value) = value {
            set(
                fields.get_mut(b"info".as_slice()).unwrap(),
                b"premium",
                value,
            );
        }
        let raw = root.encode();
        let key = [
            0x82, 0x4e, 0x0b, 0xc1, 0x14, 0xf5, 0x8c, 0xdb, 0x5a, 0x49, 0xa0, 0xc0, 0xf1, 0x42,
            0xb4, 0x24,
        ];
        let mut wrapped = vec![1, 2, 3, 4, 0x11, 2, 0x65, 0x2e];
        wrapped.extend(
            cbc::Encryptor::<aes::Aes128>::new_from_slices(&key, &[0; 16])
                .unwrap()
                .encrypt_padded_vec::<Pkcs7>(&raw),
        );
        match decode_metainfo(&raw) {
            Ok(expected) => {
                let actual = ace_wire::disk_envelope::decode_disk_metainfo(&wrapped).unwrap();
                assert_eq!(actual.protection, expected.protection);
                assert_eq!(actual.infohash, expected.infohash);
            }
            Err(ace_wire::WireError::InvalidProtection) => assert!(matches!(
                ace_wire::disk_envelope::decode_disk_metainfo(&wrapped),
                Err(ace_wire::WireError::InvalidProtection)
            )),
            other => panic!("unexpected synthetic fixture result: {other:?}"),
        }
    }
}
