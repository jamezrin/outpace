//! Parse a `magnet:` link's `xt=urn:btih:` info-hash into a 40-hex string (BitTorrent v1 only).
//! Accepts the two standard btih encodings — 40-char hex and 32-char RFC-4648 base32 — and
//! rejects everything else (notably v2 `urn:btmh:`), so magnet inputs reduce to the existing
//! bare-infohash playback path.

/// Extract the v1 info-hash from a magnet URI or a raw `magnet=` value, as a lowercase 40-hex
/// string. Returns a human-readable error for anything unsupported.
pub(crate) fn parse_magnet_infohash(magnet: &str) -> Result<String, String> {
    let query = magnet
        .trim()
        .strip_prefix("magnet:?")
        .or_else(|| magnet.trim().strip_prefix("magnet:"))
        .unwrap_or(magnet.trim());
    for (key, value) in query.split('&').filter_map(|p| p.split_once('=')) {
        if key != "xt" {
            continue;
        }
        if let Some(hash) = value.strip_prefix("urn:btih:") {
            return normalize_btih(hash);
        }
    }
    Err("magnet has no supported urn:btih: info-hash".into())
}

fn normalize_btih(hash: &str) -> Result<String, String> {
    if hash.len() == 40 && hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Ok(hash.to_ascii_lowercase());
    }
    if hash.len() == 32 {
        if let Some(bytes) = base32_decode(hash) {
            return Ok(bytes.iter().map(|b| format!("{b:02x}")).collect());
        }
    }
    Err("unsupported btih info-hash encoding".into())
}

/// Decode a 32-char RFC-4648 base32 string into 20 bytes (case-insensitive). `None` on any
/// invalid character or length.
fn base32_decode(s: &str) -> Option<Vec<u8>> {
    const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    if s.len() != 32 {
        return None;
    }
    let mut buffer: u16 = 0;
    let mut bits = 0u32;
    let mut out = Vec::with_capacity(20);
    for c in s.bytes() {
        let up = c.to_ascii_uppercase();
        let val = ALPHABET.iter().position(|&a| a == up)? as u16;
        buffer = (buffer << 5) | val;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
        }
    }
    if out.len() == 20 {
        Some(out)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_40_hex_btih() {
        let m = "magnet:?xt=urn:btih:0123456789ABCDEF0123456789abcdef01234567&dn=x";
        assert_eq!(
            parse_magnet_infohash(m).unwrap(),
            "0123456789abcdef0123456789abcdef01234567"
        );
    }

    #[test]
    fn parses_32_char_base32_btih() {
        // The full 32-char base32 alphabet decodes to the canonical RFC-4648 20-byte sequence
        // (indices 0..31 packed 5 bits each), so magnet base32 maps to the same infohash the
        // hex form of those bytes would.
        let m = "magnet:?xt=urn:btih:ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
        let expected: String = [
            0x00u8, 0x44, 0x32, 0x14, 0xc7, 0x42, 0x54, 0xb6, 0x35, 0xcf, 0x84, 0x65, 0x3a, 0x56,
            0xd7, 0xc6, 0x75, 0xbe, 0x77, 0xdf,
        ]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
        assert_eq!(parse_magnet_infohash(m).unwrap(), expected);
    }

    #[test]
    fn rejects_btmh_v2_and_missing() {
        assert!(parse_magnet_infohash("magnet:?xt=urn:btmh:1220abcd").is_err());
        assert!(parse_magnet_infohash("magnet:?dn=noxt").is_err());
        assert!(parse_magnet_infohash("magnet:?xt=urn:btih:tooshort").is_err());
    }
}
