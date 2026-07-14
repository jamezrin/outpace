//! Descriptor tracker patching.
//!
//! Rewrites the `trackers` field of an Acestream `.acelive` transport descriptor so
//! swarm peers announce to the harness's in-process tracker instead of the public
//! ones, WITHOUT disturbing any other field. Because the swarm infohash is computed
//! over a fixed subset of fields (name, authmethod, pubkey, piece_length,
//! chunk_length, bitrate — see [`ace_wire::infohash`]) that EXCLUDES `trackers`, a
//! patched descriptor keeps the same infohash and therefore the same swarm.

use ace_wire::bencode::Bencode;
use ace_wire::transport::{decode_transport, encode_transport};
use anyhow::{anyhow, bail, Result};

/// Replace only the `trackers` field of a transport descriptor and re-encode it.
///
/// Decodes `descriptor_bytes` (validating the transport magic + AES body), swaps the
/// `trackers` list for `new_trackers`, then re-encodes under the global transport
/// key/IV. All other keys are preserved verbatim.
pub fn patch_trackers(descriptor_bytes: &[u8], new_trackers: &[String]) -> Result<Vec<u8>> {
    // decode_transport validates the magic + AES body and hands back the full dict.
    let decoded = decode_transport(descriptor_bytes)
        .map_err(|e| anyhow!("decoding transport descriptor: {e:?}"))?;

    let Bencode::Dict(mut dict) = decoded.raw else {
        bail!("transport descriptor is not a bencode dict");
    };

    let trackers = Bencode::List(
        new_trackers
            .iter()
            .map(|t| Bencode::Bytes(t.clone().into_bytes()))
            .collect(),
    );
    dict.insert(b"trackers".to_vec(), trackers);

    Ok(encode_transport(&Bencode::Dict(dict)))
}

/// Compute the Acestream swarm infohash (lowercase hex) of a transport descriptor.
pub fn infohash_of(descriptor_bytes: &[u8]) -> Result<String> {
    let hash = ace_wire::infohash::try_infohash_of_transport(descriptor_bytes)
        .map_err(|e| anyhow!("computing swarm infohash: {e:?}"))?;
    Ok(hex::encode(hash))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn fixture(name: &str) -> Vec<u8> {
        // Fixtures live at the workspace-root tests/vectors/ (two levels up from the crate).
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/vectors")
            .join(name);
        std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
    }

    fn roundtrip_case(name: &str) {
        let original = fixture(name);
        let before = decode_transport(&original).expect("decode original");
        let infohash_before = infohash_of(&original).expect("infohash original");

        let new_trackers = vec!["udp://10.0.0.1:7001/announce".to_string()];
        let patched = patch_trackers(&original, &new_trackers).expect("patch");

        // Patched bytes still decode.
        let after = decode_transport(&patched).expect("decode patched");

        // Only the trackers field changed.
        assert_eq!(after.trackers, new_trackers, "trackers must be replaced");
        assert_ne!(
            before.trackers, after.trackers,
            "trackers must actually differ from the original"
        );
        assert_eq!(after.name, before.name, "name preserved");
        assert_eq!(
            after.piece_length, before.piece_length,
            "piece_length preserved"
        );
        assert_eq!(
            after.chunk_length, before.chunk_length,
            "chunk_length preserved"
        );
        assert_eq!(after.bitrate, before.bitrate, "bitrate preserved");
        assert_eq!(after.pubkey, before.pubkey, "pubkey preserved");

        // The swarm infohash EXCLUDES trackers, so it must be unchanged.
        let infohash_after = infohash_of(&patched).expect("infohash patched");
        assert_eq!(
            infohash_before, infohash_after,
            "swarm infohash must be unchanged after patching trackers"
        );
    }

    #[test]
    fn patch_trackers_roundtrip_transport_01() {
        roundtrip_case("transport-01.bin");
    }

    #[test]
    fn patch_trackers_roundtrip_transport_02() {
        roundtrip_case("transport-02.bin");
    }

    #[test]
    fn patch_trackers_supports_multiple_urls() {
        let original = fixture("transport-02.bin");
        let new_trackers = vec![
            "udp://10.0.0.1:7001/announce".to_string(),
            "udp://10.0.0.2:7001/announce".to_string(),
        ];
        let patched = patch_trackers(&original, &new_trackers).unwrap();
        let after = decode_transport(&patched).unwrap();
        assert_eq!(after.trackers, new_trackers);
    }
}
