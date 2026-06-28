//! Acestream identifier math. infohash = SHA1(entire transport file bytes).
//! See docs/protocol/transport-file.md (validated against engine ground truth).

use sha1::{Digest, Sha1};

/// The magic header every Acestream transport file starts with.
pub const TRANSPORT_MAGIC: &[u8] = b"AceStreamTransport";

/// The BitTorrent infohash of an Acestream transport = SHA1 of the whole file,
/// including the `AceStreamTransport` magic.
pub fn infohash_of_transport(bytes: &[u8]) -> [u8; 20] {
    let mut h = Sha1::new();
    h.update(bytes);
    h.finalize().into()
}

/// True iff `bytes` begins with the Acestream transport magic.
pub fn is_transport_file(bytes: &[u8]) -> bool {
    bytes.starts_with(TRANSPORT_MAGIC)
}
