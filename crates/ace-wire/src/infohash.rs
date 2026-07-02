//! Acestream identifier math.
//!
//! The swarm infohash is not the SHA-1 of the wrapped `.acelive` transport file.
//! The official engine hashes a bencoded, ordered subset of the decoded live
//! descriptor. It also computes SHA-1 over the raw transport bytes separately
//! for transport-file identity/cache naming.

use crate::bencode::Bencode;
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
    infohash_of_descriptor(&d.raw)
}

/// The official Acestream swarm infohash for a live transport.
///
/// Ground truth from `Transport.load_transport_file_from_string(...).get_infohash()`:
/// `SHA1(bencode([[name, v], [authmethod, v], [pubkey, v], [piece_length, v],
/// [chunk_length, v], [bitrate, v]]))`.
pub fn infohash_of_transport(bytes: &[u8]) -> [u8; 20] {
    try_infohash_of_transport(bytes).expect("valid Acestream transport descriptor")
}

/// Compute the official swarm infohash from a decoded descriptor dict.
pub fn infohash_of_descriptor(descriptor: &Bencode) -> Result<[u8; 20]> {
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
    Ok(sha1_bytes(&Bencode::List(pairs).encode()))
}

/// True iff `bytes` begins with the Acestream transport magic.
pub fn is_transport_file(bytes: &[u8]) -> bool {
    bytes.starts_with(TRANSPORT_MAGIC)
}
