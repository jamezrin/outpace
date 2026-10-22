//! Bounded decoding of the exact original-disk single-file VOD envelope.
//!
//! The recovered bytes go unchanged to the strict metainfo decoder. CBC provides no
//! authentication: valid padding and metainfo do not prove the file's provenance.

use crate::metainfo::{decode_metainfo, Metainfo};
use crate::{Result, WireError};
use cbc::cipher::{block_padding::Pkcs7, BlockModeDecrypt, KeyIvInit};

const DISK_MARKER: [u8; 8] = [0x01, 0x02, 0x03, 0x04, 0x11, 0x02, 0x65, 0x2e];
const MAX_DISK_BYTES: usize = 1_048_576;

// Protocol constant for the exact-marker branch in engine 3.2.11: raw MD5 of
// the compiled 59-byte argument to m2_AES_decrypt (Transport.so source VA
// 0xc3560, file offset 0xc2560). Independently matched to four captured calls.
// String decoding indexes the XOR schedule by decoded u16 word, not byte offset.
// This is distinct from the live transport key; no runtime MD5 dependency is needed.
const DISK_KEY: [u8; 16] = [
    0x82, 0x4e, 0x0b, 0xc1, 0x14, 0xf5, 0x8c, 0xdb, 0x5a, 0x49, 0xa0, 0xc0, 0xf1, 0x42, 0xb4, 0x24,
];
const DISK_IV: [u8; 16] = [0; 16];

/// Recognize only the complete fixed eight-byte disk marker.
pub fn is_disk_envelope(bytes: &[u8]) -> bool {
    bytes.starts_with(&DISK_MARKER)
}

/// Decode exactly one disk envelope containing one complete single-file metainfo.
///
/// Framing is checked before allocating a decryption buffer. PKCS#7 validates every
/// padding byte and permits only lengths 1 through 16. Inner parser limits and
/// original-info-byte identity are preserved; nested envelopes are not decoded.
pub fn decode_disk_metainfo(bytes: &[u8]) -> Result<Metainfo> {
    if bytes.len() > MAX_DISK_BYTES {
        return Err(WireError::Invalid("disk envelope too large"));
    }
    if !is_disk_envelope(bytes) {
        return Err(WireError::Invalid("not a disk envelope"));
    }
    if bytes.len() < DISK_MARKER.len() + 16 {
        return Err(WireError::Truncated);
    }
    let body = &bytes[DISK_MARKER.len()..];
    if !body.len().is_multiple_of(16) {
        return Err(WireError::Invalid("bad disk envelope body length"));
    }
    let plaintext = cbc::Decryptor::<aes::Aes128>::new_from_slices(&DISK_KEY, &DISK_IV)
        .map_err(|_| WireError::Invalid("disk aes key/iv length"))?
        .decrypt_padded_vec::<Pkcs7>(body)
        .map_err(|_| WireError::Invalid("disk aes/pad"))?;
    decode_metainfo(&plaintext)
}
