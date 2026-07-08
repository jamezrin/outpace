//! Single-file VOD download over vanilla BitTorrent: request blocks in order, verify each
//! assembled piece against the transport's SHA-1 `pieces`, and emit verified bytes in order.
//!
//! VOD is standard BitTorrent (`Request`/`Piece`/`Bitfield`/`Have`), unlike the live path
//! which reuses those message IDs with custom `[stream]` payloads and in-band RSA signatures.
//! This module therefore shares only the low-level connect/handshake primitives with live and
//! is deterministically testable against a local mock seeder (no live swarm).

use sha1::{Digest, Sha1};

/// True iff `bytes` hashes (SHA-1) to `expected` — standard BitTorrent piece integrity.
pub fn verify_piece(expected: &[u8; 20], bytes: &[u8]) -> bool {
    let digest = Sha1::digest(bytes);
    digest.as_slice() == expected
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_piece_accepts_matching_and_rejects_tampered() {
        let data = b"hello vod piece";
        let hash: [u8; 20] = Sha1::digest(data).into();
        assert!(verify_piece(&hash, data));
        let mut bad = data.to_vec();
        bad[0] ^= 0xff;
        assert!(!verify_piece(&hash, &bad));
    }
}
