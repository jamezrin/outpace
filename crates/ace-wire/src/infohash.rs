//! Acestream identifier math.
//!
//! The swarm infohash is not the SHA-1 of the wrapped `.acelive` transport file.
//! The official engine hashes a bencoded, ordered subset of the decoded live
//! descriptor. It also computes SHA-1 over the raw transport bytes separately
//! for transport-file identity/cache naming.

use crate::bencode::Bencode;
use crate::transport::TransportDescriptor;
use crate::{Result, WireError};
use sha1::{Digest, Sha1};

/// The magic header every Acestream transport file starts with.
pub const TRANSPORT_MAGIC: &[u8] = b"AceStreamTransport";

fn sha1_bytes(bytes: &[u8]) -> [u8; 20] {
    let mut h = Sha1::new();
    h.update(bytes);
    h.finalize().into()
}

/// SHA-1 of the whole wrapped transport file, including the `AceStreamTransport`
/// magic and encrypted body. This is a transport-file identifier, not the swarm
/// infohash used in peer handshakes.
pub fn transport_file_hash(bytes: &[u8]) -> [u8; 20] {
    sha1_bytes(bytes)
}

/// Fallible form of [`infohash_of_transport`].
pub fn try_infohash_of_transport(bytes: &[u8]) -> Result<[u8; 20]> {
    let d = crate::transport::decode_transport(bytes)?;
    infohash_of_decoded_transport(&d)
}

/// The official Acestream swarm infohash for a live transport.
///
/// Hashes an ordered pair list of the six required base fields followed by any
/// present optional selector fields; see [`infohash_of_descriptor`].
/// Legacy wrapped VOD retains the six-field hash via decoded classification.
pub fn infohash_of_transport(bytes: &[u8]) -> [u8; 20] {
    try_infohash_of_transport(bytes).expect("valid Acestream transport descriptor")
}

/// Compute the official live-BT swarm infohash from a decoded descriptor dict.
///
/// The base six fields remain required. Optional fields follow in the original
/// BT selector's order and retain their raw values, including zero or empty values.
pub fn infohash_of_descriptor(descriptor: &Bencode) -> Result<[u8; 20]> {
    hash_descriptor(descriptor, true)
}

/// Hash a decoded transport using its existing live/VOD classification.
///
/// Live descriptors include present optional selector fields; legacy wrapped
/// VOD retains its required six-field identity.
pub fn infohash_of_decoded_transport(descriptor: &TransportDescriptor) -> Result<[u8; 20]> {
    hash_descriptor(&descriptor.raw, descriptor.is_live)
}

fn hash_descriptor(descriptor: &Bencode, include_optional: bool) -> Result<[u8; 20]> {
    let mut pairs = Vec::new();
    for key in [
        b"name".as_slice(),
        b"authmethod".as_slice(),
        b"pubkey".as_slice(),
        b"piece_length".as_slice(),
        b"chunk_length".as_slice(),
        b"bitrate".as_slice(),
    ] {
        let value = descriptor
            .get(key)
            .ok_or(WireError::Invalid("missing descriptor infohash field"))?;
        pairs.push(Bencode::List(vec![
            Bencode::Bytes(key.to_vec()),
            value.clone(),
        ]));
    }
    if include_optional {
        for key in [
            b"premium".as_slice(),
            b"commerce".as_slice(),
            b"provider_key".as_slice(),
            b"provider_sid".as_slice(),
            b"access".as_slice(),
            b"sharing".as_slice(),
            b"private".as_slice(),
            b"tns".as_slice(),
            b"license".as_slice(),
        ] {
            if let Some(value) = descriptor.get(key) {
                pairs.push(Bencode::List(vec![
                    Bencode::Bytes(key.to_vec()),
                    value.clone(),
                ]));
            }
        }
    }
    Ok(sha1_bytes(&Bencode::List(pairs).encode()))
}

/// True iff `bytes` begins with the Acestream transport magic.
pub fn is_transport_file(bytes: &[u8]) -> bool {
    bytes.starts_with(TRANSPORT_MAGIC)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha1::{Digest, Sha1};

    const SYNTHETIC_LIVE: &[u8] =
        include_bytes!("../../../tests/vectors/transport/synthetic-live-512k.bin");

    // Recovered from the original BT constructor's ordered selector. This test-side
    // list is deliberately independent of infohash_of_descriptor's implementation.
    const ORIGINAL_BT_SELECTOR: [&[u8]; 15] = [
        b"name",
        b"authmethod",
        b"pubkey",
        b"piece_length",
        b"chunk_length",
        b"bitrate",
        b"premium",
        b"commerce",
        b"provider_key",
        b"provider_sid",
        b"access",
        b"sharing",
        b"private",
        b"tns",
        b"license",
    ];

    const LEGACY_VOD_SELECTOR: [&[u8]; 6] = [
        b"name",
        b"authmethod",
        b"pubkey",
        b"piece_length",
        b"chunk_length",
        b"bitrate",
    ];

    fn synthetic_positive_descriptor() -> Bencode {
        crate::transport::decode_transport(SYNTHETIC_LIVE)
            .expect("accepted synthetic positive fixture")
            .raw
    }

    fn independent_selector_hash(descriptor: &Bencode) -> ([u8; 20], Vec<Vec<u8>>) {
        let pairs: Vec<Bencode> = ORIGINAL_BT_SELECTOR
            .iter()
            .filter_map(|key| {
                descriptor
                    .get(key)
                    .map(|value| Bencode::List(vec![Bencode::Bytes(key.to_vec()), value.clone()]))
            })
            .collect();
        let included = pairs
            .iter()
            .map(|pair| match pair {
                Bencode::List(items) => items[0].as_bytes().unwrap().to_vec(),
                _ => unreachable!("selector pairs are constructed as lists"),
            })
            .collect();
        let digest = Sha1::digest(Bencode::List(pairs).encode()).into();
        (digest, included)
    }

    fn independent_legacy_vod_hash(descriptor: &Bencode) -> [u8; 20] {
        let pairs: Vec<Bencode> = LEGACY_VOD_SELECTOR
            .iter()
            .map(|key| {
                Bencode::List(vec![
                    Bencode::Bytes(key.to_vec()),
                    descriptor
                        .get(key)
                        .expect("synthetic VOD has each legacy field")
                        .clone(),
                ])
            })
            .collect();
        Sha1::digest(Bencode::List(pairs).encode()).into()
    }

    fn synthetic_wrapped_vod_with_provider_selectors() -> (Vec<u8>, Bencode) {
        use std::collections::BTreeMap;

        let descriptor = Bencode::Dict(BTreeMap::from([
            (
                b"name".to_vec(),
                Bencode::Bytes(b"synthetic VOD fixture".to_vec()),
            ),
            (
                b"authmethod".to_vec(),
                Bencode::Bytes(b"synthetic-auth".to_vec()),
            ),
            (b"pubkey".to_vec(), Bencode::Bytes(Vec::new())),
            (b"piece_length".to_vec(), Bencode::Int(131_072)),
            (b"chunk_length".to_vec(), Bencode::Int(16_384)),
            (b"bitrate".to_vec(), Bencode::Int(100_000)),
            (b"length".to_vec(), Bencode::Int(300_000)),
            (b"pieces".to_vec(), Bencode::Bytes(vec![0x5a; 60])),
            (
                b"provider_key".to_vec(),
                Bencode::Bytes(b"synthetic-provider-key".to_vec()),
            ),
            (
                b"provider_sid".to_vec(),
                Bencode::Bytes(b"synthetic-provider-session".to_vec()),
            ),
        ]));
        let wrapped = crate::transport::encode_transport(&descriptor);
        (wrapped, descriptor)
    }

    fn insert_ascii_field(descriptor: &mut Bencode, key: &[u8], value: &[u8]) {
        let Bencode::Dict(fields) = descriptor else {
            panic!("synthetic descriptor is a dictionary");
        };
        fields.insert(key.to_vec(), Bencode::Bytes(value.to_vec()));
    }

    fn insert_field(descriptor: &mut Bencode, key: &[u8], value: Bencode) {
        let Bencode::Dict(fields) = descriptor else {
            panic!("synthetic descriptor is a dictionary");
        };
        fields.insert(key.to_vec(), value);
    }

    #[test]
    fn original_bt_hash_includes_present_provider_selectors_in_order() {
        let base = synthetic_positive_descriptor();

        // The accepted six-field positive remains unchanged, and tracker metadata
        // stays outside the original selector even when its value changes.
        let (base_expected, base_keys) = independent_selector_hash(&base);
        assert_eq!(
            base_keys,
            [
                b"name".to_vec(),
                b"authmethod".to_vec(),
                b"pubkey".to_vec(),
                b"piece_length".to_vec(),
                b"chunk_length".to_vec(),
                b"bitrate".to_vec(),
            ]
        );
        assert_eq!(infohash_of_descriptor(&base).unwrap(), base_expected);

        let mut tracker_changed = base.clone();
        let Bencode::Dict(fields) = &mut tracker_changed else {
            unreachable!();
        };
        let Some(Bencode::List(trackers)) = fields.get_mut(b"trackers".as_slice()) else {
            panic!("synthetic positive fixture retains tracker metadata");
        };
        trackers.push(Bencode::Bytes(b"udp://second.tracker.invalid:80".to_vec()));
        assert_eq!(independent_selector_hash(&tracker_changed).0, base_expected);
        assert_eq!(
            infohash_of_descriptor(&tracker_changed).unwrap(),
            base_expected
        );

        // Add only the two statically recovered string selectors. Values are
        // invented ASCII bytes and do not encode an identity or endpoint.
        let mut extended = base;
        insert_ascii_field(
            &mut extended,
            b"provider_key",
            b"synthetic-provider-key-alpha",
        );
        insert_ascii_field(
            &mut extended,
            b"provider_sid",
            b"synthetic-provider-sid-beta",
        );

        let (expected, included) = independent_selector_hash(&extended);
        assert_eq!(
            included,
            [
                b"name".to_vec(),
                b"authmethod".to_vec(),
                b"pubkey".to_vec(),
                b"piece_length".to_vec(),
                b"chunk_length".to_vec(),
                b"bitrate".to_vec(),
                b"provider_key".to_vec(),
                b"provider_sid".to_vec(),
            ]
        );
        assert_eq!(infohash_of_descriptor(&extended).unwrap(), expected);
        let wrapped_live = crate::transport::encode_transport(&extended);
        assert_eq!(try_infohash_of_transport(&wrapped_live).unwrap(), expected);
    }

    #[test]
    fn all_fifteen_present_selector_fields_are_hashed_in_exact_order() {
        let mut descriptor = synthetic_positive_descriptor();
        for (index, key) in ORIGINAL_BT_SELECTOR[6..].iter().enumerate() {
            insert_ascii_field(
                &mut descriptor,
                key,
                format!("synthetic-optional-value-{index}").as_bytes(),
            );
        }

        let (expected, included) = independent_selector_hash(&descriptor);
        assert_eq!(
            included,
            ORIGINAL_BT_SELECTOR
                .iter()
                .map(|key| key.to_vec())
                .collect::<Vec<_>>()
        );
        assert_eq!(infohash_of_descriptor(&descriptor).unwrap(), expected);
    }

    #[test]
    fn every_optional_selector_preserves_present_zero_and_empty_raw_values() {
        let base = synthetic_positive_descriptor();
        let raw_values = [
            Bencode::Int(0),
            Bencode::Bytes(Vec::new()),
            Bencode::List(Vec::new()),
            Bencode::Dict(std::collections::BTreeMap::new()),
        ];

        for key in &ORIGINAL_BT_SELECTOR[6..] {
            for value in &raw_values {
                let mut descriptor = base.clone();
                insert_field(&mut descriptor, key, value.clone());
                let (expected, included) = independent_selector_hash(&descriptor);
                let expected_keys: Vec<Vec<u8>> = ORIGINAL_BT_SELECTOR
                    .iter()
                    .filter(|candidate| descriptor.get(candidate).is_some())
                    .map(|candidate| candidate.to_vec())
                    .collect();
                assert_eq!(included, expected_keys, "selector presence for {key:?}");
                assert_eq!(
                    infohash_of_descriptor(&descriptor).unwrap(),
                    expected,
                    "raw optional value for {key:?} must be retained"
                );
            }
        }
    }

    #[test]
    fn each_missing_required_field_keeps_the_existing_error_even_with_suffixes() {
        let base = synthetic_positive_descriptor();
        for key in LEGACY_VOD_SELECTOR {
            let mut descriptor = base.clone();
            let Bencode::Dict(fields) = &mut descriptor else {
                unreachable!();
            };
            fields.remove(key);
            fields.insert(
                b"provider_key".to_vec(),
                Bencode::Bytes(b"synthetic-provider-key".to_vec()),
            );
            assert!(
                matches!(
                    infohash_of_descriptor(&descriptor),
                    Err(WireError::Invalid("missing descriptor infohash field"))
                ),
                "missing required selector {key:?} must retain its error"
            );
        }
    }

    #[test]
    fn unlisted_metadata_does_not_change_extended_live_identity() {
        let mut descriptor = synthetic_positive_descriptor();
        for (index, key) in ORIGINAL_BT_SELECTOR[6..].iter().enumerate() {
            insert_ascii_field(
                &mut descriptor,
                key,
                format!("synthetic-optional-value-{index}").as_bytes(),
            );
        }
        let expected = independent_selector_hash(&descriptor).0;

        insert_field(
            &mut descriptor,
            b"quality",
            Bencode::Bytes(b"synthetic-unlisted-quality".to_vec()),
        );
        insert_field(
            &mut descriptor,
            b"categories",
            Bencode::List(vec![Bencode::Bytes(b"synthetic-category".to_vec())]),
        );
        assert_eq!(independent_selector_hash(&descriptor).0, expected);
        assert_eq!(infohash_of_descriptor(&descriptor).unwrap(), expected);
    }

    #[test]
    fn wrapped_vod_transport_keeps_legacy_hash_when_provider_selectors_are_present() {
        let (wrapped, descriptor) = synthetic_wrapped_vod_with_provider_selectors();
        let decoded = crate::transport::decode_transport(&wrapped).unwrap();
        assert!(
            !decoded.is_live,
            "synthetic descriptor must take the VOD path"
        );
        assert_eq!(
            decoded.pieces.len(),
            3,
            "synthetic VOD has valid piece hashes"
        );

        let expected = independent_legacy_vod_hash(&descriptor);
        assert_eq!(
            try_infohash_of_transport(&wrapped).unwrap(),
            expected,
            "decoded VOD transports must retain the legacy six-field selector"
        );
    }
}
