//! Acestream transport-file descriptor decoder.
//!
//! A transport file is:
//!   `"AceStreamTransport"` (18-byte magic)
//!   + `00 02` (2-byte version)
//!   + body = AES-128-CBC(PKCS#7(bencode(descriptor dict)))
//!
//! See `docs/protocol/transport-file.md` for the full spec.

use crate::bencode::Bencode;
use crate::{Result, WireError};

use cbc::cipher::{BlockModeDecrypt, KeyIvInit, block_padding::Pkcs7};

/// AES-128-CBC key for transport-file decryption.
///
/// Extracted from the engine's `core/src/Crypto.pyx` (`m2_AES_decrypt` /
/// `block_decrypt`).  Fixed global constant — the same key decrypts all
/// Acestream transport files, making it a required protocol interop constant.
pub const TRANSPORT_KEY: [u8; 16] = [
    0xa5, 0x0c, 0x4e, 0x33, 0xa2, 0xf4, 0x8c, 0xc5,
    0x0c, 0xe2, 0x75, 0xc9, 0xff, 0x3a, 0x31, 0xbf,
];

/// AES-128-CBC initialisation vector for transport-file decryption.
///
/// Paired with [`TRANSPORT_KEY`]; both were captured from the engine source.
pub const TRANSPORT_IV: [u8; 16] = [
    0x74, 0xe9, 0xcd, 0xd6, 0x39, 0x1b, 0xcb, 0xd5,
    0x65, 0xf9, 0x95, 0x03, 0x31, 0x33, 0x29, 0xa3,
];

/// Decoded Acestream transport descriptor.
#[derive(Debug)]
pub struct TransportDescriptor {
    /// Channel / content name (lossy UTF-8 of the `name` bytes).
    pub name: String,
    /// Bytes per piece (e.g. 1 048 576 for live, 131 072 for some streams).
    pub piece_length: u64,
    /// Bytes per chunk (typically 16 384 = 16 KiB).
    pub chunk_length: u64,
    /// Media bitrate hint, if present.
    pub bitrate: Option<i64>,
    /// Tracker announce URLs (lossy UTF-8 of each `trackers` list element).
    pub trackers: Vec<String>,
    /// RSA DER public key of the broadcaster (124 bytes); empty if absent.
    pub pubkey: Vec<u8>,
    /// VOD piece SHA-1 hashes (20 bytes each); empty for live streams.
    pub pieces: Vec<[u8; 20]>,
    /// `true` iff no `pieces` key is present (i.e. this is a live stream).
    pub is_live: bool,
    /// Full decoded bencode dict for accessing other / future keys.
    pub raw: crate::bencode::Bencode,
}

type Dec = cbc::Decryptor<aes::Aes128>;

/// Decode an Acestream transport file using the global protocol key/IV.
pub fn decode_transport(bytes: &[u8]) -> Result<TransportDescriptor> {
    decode_transport_with_key(bytes, &TRANSPORT_KEY, &TRANSPORT_IV)
}

/// Decode an Acestream transport file with an explicit AES key and IV.
///
/// Useful for testing with synthetic test vectors under a known key.
pub fn decode_transport_with_key(
    bytes: &[u8],
    key: &[u8; 16],
    iv: &[u8; 16],
) -> Result<TransportDescriptor> {
    // 1. Magic check.
    if !crate::infohash::is_transport_file(bytes) {
        return Err(WireError::Invalid("not a transport file"));
    }

    // 2. Body = skip 18-byte magic + 2-byte version.
    let body = &bytes[20..];
    if body.is_empty() || body.len() % 16 != 0 {
        return Err(WireError::Invalid("bad transport body length"));
    }

    // 3. AES-128-CBC decrypt + PKCS#7 unpad.
    let pt = Dec::new_from_slices(key, iv)
        .map_err(|_| WireError::Invalid("aes key/iv length"))?
        .decrypt_padded_vec::<Pkcs7>(body)
        .map_err(|_| WireError::Invalid("aes/pad"))?;

    // 4. Bencode parse — must be a Dict.
    let raw = Bencode::parse(&pt)?;
    if !matches!(raw, Bencode::Dict(_)) {
        return Err(WireError::Invalid("descriptor not a dict"));
    }

    // 5. Extract fields.
    let name = raw
        .get(b"name")
        .and_then(|v| v.as_bytes())
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default();

    let piece_length = raw
        .get(b"piece_length")
        .and_then(|v| v.as_int())
        .map(|i| i as u64)
        .unwrap_or(0);

    let chunk_length = raw
        .get(b"chunk_length")
        .and_then(|v| v.as_int())
        .map(|i| i as u64)
        .unwrap_or(0);

    let bitrate = raw.get(b"bitrate").and_then(|v| v.as_int());

    let pubkey = raw
        .get(b"pubkey")
        .and_then(|v| v.as_bytes())
        .map(|b| b.to_vec())
        .unwrap_or_default();

    let trackers: Vec<String> = match raw.get(b"trackers") {
        Some(Bencode::List(list)) => list
            .iter()
            .filter_map(|item| item.as_bytes())
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .collect(),
        _ => Vec::new(),
    };

    // 6. VOD pieces: concatenated 20-byte SHA-1 hashes; absent on live streams.
    let pieces: Vec<[u8; 20]> = match raw.get(b"pieces") {
        Some(Bencode::Bytes(p)) if p.len() % 20 == 0 && !p.is_empty() => p
            .chunks_exact(20)
            .map(|c| {
                let mut arr = [0u8; 20];
                arr.copy_from_slice(c);
                arr
            })
            .collect(),
        _ => Vec::new(),
    };
    let is_live = pieces.is_empty();

    Ok(TransportDescriptor {
        name,
        piece_length,
        chunk_length,
        bitrate,
        trackers,
        pubkey,
        pieces,
        is_live,
        raw,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_short_and_non_magic_input() {
        // too short
        assert!(decode_transport_with_key(b"short", &TRANSPORT_KEY, &TRANSPORT_IV).is_err());
        // non-magic
        assert!(
            decode_transport_with_key(b"not a transport file", &TRANSPORT_KEY, &TRANSPORT_IV)
                .is_err()
        );
        // magic but empty body
        let magic_only = b"AceStreamTransport\x00\x02";
        assert!(
            decode_transport_with_key(magic_only, &TRANSPORT_KEY, &TRANSPORT_IV).is_err()
        );
    }
}
