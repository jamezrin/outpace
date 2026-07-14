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

use cbc::cipher::{block_padding::Pkcs7, BlockModeDecrypt, KeyIvInit};

/// AES-128-CBC key for transport-file decryption.
///
/// Extracted from the engine's `core/src/Crypto.pyx` (`m2_AES_decrypt` /
/// `block_decrypt`).  Fixed global constant — the same key decrypts all
/// Acestream transport files, making it a required protocol interop constant.
pub const TRANSPORT_KEY: [u8; 16] = [
    0xa5, 0x0c, 0x4e, 0x33, 0xa2, 0xf4, 0x8c, 0xc5, 0x0c, 0xe2, 0x75, 0xc9, 0xff, 0x3a, 0x31, 0xbf,
];

/// AES-128-CBC initialisation vector for transport-file decryption.
///
/// Paired with [`TRANSPORT_KEY`]; both were captured from the engine source.
pub const TRANSPORT_IV: [u8; 16] = [
    0x74, 0xe9, 0xcd, 0xd6, 0x39, 0x1b, 0xcb, 0xd5, 0x65, 0xf9, 0x95, 0x03, 0x31, 0x33, 0x29, 0xa3,
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
    /// Bounded content categories advertised by the descriptor.
    pub categories: Vec<String>,
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

impl TransportDescriptor {
    /// Total content byte length for a single-file VOD (`length` key), if present.
    ///
    /// SYNTHESIZED SCHEMA: no public VOD fixture is available, so the single-file `length`
    /// key is assumed from standard BitTorrent conventions. Reconcile against a real capture.
    pub fn vod_total_length(&self) -> Option<u64> {
        self.raw
            .get(b"length")
            .and_then(|v| v.as_int())
            .filter(|&i| i > 0)
            .map(|i| i as u64)
    }

    /// Whether this descriptor advertises a multi-file layout (`files` list). Multi-file VOD is
    /// intentionally unsupported; callers reject it. SYNTHESIZED SCHEMA (see [`Self::vod_total_length`]).
    pub fn is_multifile(&self) -> bool {
        matches!(self.raw.get(b"files"), Some(Bencode::List(_)))
    }
}

type Dec = cbc::Decryptor<aes::Aes128>;

const MAX_CATEGORIES: usize = 32;
const MAX_CATEGORY_BYTES: usize = 128;

fn bounded_text(bytes: &[u8], max_bytes: usize) -> Option<String> {
    let text = String::from_utf8_lossy(bytes);
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    let mut end = text.len().min(max_bytes);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    Some(text[..end].to_string())
}

fn decode_categories(raw: &Bencode) -> Vec<String> {
    match raw.get(b"categories") {
        Some(Bencode::List(items)) => items
            .iter()
            .filter_map(Bencode::as_bytes)
            .filter_map(|value| bounded_text(value, MAX_CATEGORY_BYTES))
            .take(MAX_CATEGORIES)
            .collect(),
        Some(Bencode::Bytes(value)) => bounded_text(value, MAX_CATEGORY_BYTES)
            .into_iter()
            .collect(),
        _ => Vec::new(),
    }
}

/// Read a required descriptor int that must be strictly positive.
///
/// These fields size piece/chunk requests, so a missing field or a negative value
/// (which would otherwise wrap to a huge `u64`) is a decode error, not a silent 0.
fn required_positive(raw: &Bencode, key: &[u8]) -> Result<u64> {
    match raw.get(key).and_then(|v| v.as_int()) {
        Some(i) if i > 0 => Ok(i as u64),
        _ => Err(WireError::Invalid(
            "missing or non-positive descriptor field",
        )),
    }
}

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
    if body.is_empty() || !body.len().is_multiple_of(16) {
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

    let piece_length = required_positive(&raw, b"piece_length")?;
    let chunk_length = required_positive(&raw, b"chunk_length")?;

    let bitrate = raw.get(b"bitrate").and_then(|v| v.as_int());

    let categories = decode_categories(&raw);

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
        Some(Bencode::Bytes(p)) if p.len().is_multiple_of(20) && !p.is_empty() => p
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
        categories,
        trackers,
        pubkey,
        pieces,
        is_live,
        raw,
    })
}

/// Encode an Acestream transport file from a bencode descriptor dict — the inverse of
/// [`decode_transport`]: PKCS#7 + AES-128-CBC under the global key/IV, prefixed with the
/// 18-byte magic + 2-byte version. Round-trips with the decoder. `descriptor` should be a
/// `Bencode::Dict`.
pub fn encode_transport(descriptor: &Bencode) -> Vec<u8> {
    encode_transport_with_key(descriptor, &TRANSPORT_KEY, &TRANSPORT_IV)
}

/// Like [`encode_transport`] but with an explicit key/IV (for tests / synthetic vectors).
pub fn encode_transport_with_key(descriptor: &Bencode, key: &[u8; 16], iv: &[u8; 16]) -> Vec<u8> {
    use cbc::cipher::{block_padding::Pkcs7, BlockModeEncrypt, KeyIvInit};
    type Enc = cbc::Encryptor<aes::Aes128>;
    let ct = Enc::new_from_slices(key, iv)
        .expect("16-byte key/iv")
        .encrypt_padded_vec::<Pkcs7>(&descriptor.encode());
    let mut out = b"AceStreamTransport\x00\x02".to_vec();
    out.extend_from_slice(&ct);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Wrap raw bencode `plaintext` into a transport file under the global key/IV.
    fn make_transport(plaintext: &[u8]) -> Vec<u8> {
        encode_transport_with_key(
            &crate::bencode::Bencode::parse(plaintext).unwrap(),
            &TRANSPORT_KEY,
            &TRANSPORT_IV,
        )
    }

    #[test]
    fn rejects_negative_piece_length() {
        // A negative bencode int must not be silently cast to a huge u64.
        let tf = make_transport(b"d12:chunk_lengthi16384e12:piece_lengthi-1ee");
        assert!(decode_transport(&tf).is_err());
    }

    #[test]
    fn rejects_missing_required_length_field() {
        // piece_length is required to size piece requests; absence is a decode error,
        // not a silent default of 0.
        let tf = make_transport(b"d12:chunk_lengthi16384ee");
        assert!(decode_transport(&tf).is_err());
    }

    #[test]
    fn accepts_valid_lengths() {
        // Sanity check the synthetic-vector helper against the happy path.
        let tf = make_transport(b"d12:chunk_lengthi16384e4:name2:hi12:piece_lengthi131072ee");
        let d = decode_transport(&tf).unwrap();
        assert_eq!(d.piece_length, 131072);
        assert_eq!(d.chunk_length, 16384);
        assert_eq!(d.name, "hi");
    }

    #[test]
    fn encode_then_decode_roundtrips() {
        use crate::bencode::Bencode;
        use std::collections::BTreeMap;
        let mut d = BTreeMap::new();
        d.insert(b"name".to_vec(), Bencode::Bytes(b"my broadcast".to_vec()));
        d.insert(b"piece_length".to_vec(), Bencode::Int(1_048_576));
        d.insert(b"chunk_length".to_vec(), Bencode::Int(16_384));
        d.insert(
            b"categories".to_vec(),
            Bencode::List(vec![
                Bencode::Bytes(b" sports ".to_vec()),
                Bencode::Int(7),
                Bencode::Bytes(Vec::new()),
                Bencode::Bytes(b"live".to_vec()),
            ]),
        );
        d.insert(
            b"trackers".to_vec(),
            Bencode::List(vec![Bencode::Bytes(
                b"udp://t1.example:2710/announce".to_vec(),
            )]),
        );
        let dict = Bencode::Dict(d);

        let bytes = encode_transport(&dict);
        assert!(
            crate::infohash::is_transport_file(&bytes),
            "must carry the transport magic"
        );
        assert_eq!(
            bytes.len() % 16,
            (20 % 16),
            "magic(20) + ciphertext(mult of 16)"
        );

        let got = decode_transport(&bytes).unwrap();
        assert_eq!(got.name, "my broadcast");
        assert_eq!(got.piece_length, 1_048_576);
        assert_eq!(got.chunk_length, 16_384);
        assert_eq!(got.categories, vec!["sports", "live"]);
        assert_eq!(
            got.trackers,
            vec!["udp://t1.example:2710/announce".to_string()]
        );
        assert!(got.is_live, "no pieces key => live");
    }

    #[test]
    fn category_bytes_decode_as_one_bounded_value() {
        let category = "é".repeat(100);
        let plaintext = format!(
            "d10:categories{}:{}12:chunk_lengthi16384e12:piece_lengthi131072ee",
            category.len(),
            category
        );
        let got = decode_transport(&make_transport(plaintext.as_bytes())).unwrap();

        assert_eq!(got.categories.len(), 1);
        assert!(got.categories[0].len() <= 128);
        assert!(got.categories[0].is_char_boundary(got.categories[0].len()));
    }

    #[test]
    fn category_list_preserves_order_and_stops_at_the_count_limit() {
        use std::collections::BTreeMap;

        let mut descriptor = BTreeMap::new();
        descriptor.insert(b"piece_length".to_vec(), Bencode::Int(131_072));
        descriptor.insert(b"chunk_length".to_vec(), Bencode::Int(16_384));
        descriptor.insert(
            b"categories".to_vec(),
            Bencode::List(
                (0..40)
                    .map(|index| Bencode::Bytes(format!("category-{index}").into_bytes()))
                    .collect(),
            ),
        );

        let transport = encode_transport(&Bencode::Dict(descriptor));
        let got = decode_transport(&transport).unwrap();

        assert_eq!(got.categories.len(), MAX_CATEGORIES);
        assert_eq!(
            got.categories.first().map(String::as_str),
            Some("category-0")
        );
        assert_eq!(
            got.categories.last().map(String::as_str),
            Some("category-31")
        );
    }

    #[test]
    fn single_file_vod_exposes_length_and_pieces() {
        use crate::bencode::Bencode;
        use std::collections::BTreeMap;
        // 2 pieces worth of SHA-1 hashes (40 bytes) + a length key => single-file VOD.
        let mut d = BTreeMap::new();
        d.insert(b"name".to_vec(), Bencode::Bytes(b"movie".to_vec()));
        d.insert(b"piece_length".to_vec(), Bencode::Int(131072));
        d.insert(b"chunk_length".to_vec(), Bencode::Int(16384));
        d.insert(b"length".to_vec(), Bencode::Int(200000));
        d.insert(b"pieces".to_vec(), Bencode::Bytes(vec![0u8; 40]));
        let tf = encode_transport(&Bencode::Dict(d));
        let got = decode_transport(&tf).unwrap();
        assert!(!got.is_live);
        assert_eq!(got.pieces.len(), 2);
        assert_eq!(got.vod_total_length(), Some(200000));
        assert!(!got.is_multifile());
    }

    #[test]
    fn files_key_marks_multifile() {
        use crate::bencode::Bencode;
        use std::collections::BTreeMap;
        let mut d = BTreeMap::new();
        d.insert(b"piece_length".to_vec(), Bencode::Int(131072));
        d.insert(b"chunk_length".to_vec(), Bencode::Int(16384));
        d.insert(b"pieces".to_vec(), Bencode::Bytes(vec![0u8; 20]));
        d.insert(b"files".to_vec(), Bencode::List(vec![]));
        let tf = encode_transport(&Bencode::Dict(d));
        let got = decode_transport(&tf).unwrap();
        assert!(got.is_multifile());
        assert_eq!(got.vod_total_length(), None);
    }

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
        assert!(decode_transport_with_key(magic_only, &TRANSPORT_KEY, &TRANSPORT_IV).is_err());
    }

    // Diagnostic tool: decode a real captured .acelive transport file and dump its fields
    // (esp. `pubkey`, in hex, for cross-checking against an RSA key file — B0 ground truth).
    //   ACE_TRANSPORT_FILE=/path/to/test.acelive cargo test -p ace-wire dump_real_transport -- --ignored --nocapture
    #[test]
    #[ignore]
    fn dump_real_transport() {
        let path = std::env::var("ACE_TRANSPORT_FILE").expect("set ACE_TRANSPORT_FILE=path");
        let bytes = std::fs::read(&path).expect("read transport file");
        let d = decode_transport(&bytes).expect("decode");
        println!("name: {:?}", d.name);
        println!("piece_length: {}", d.piece_length);
        println!("chunk_length: {}", d.chunk_length);
        println!("bitrate: {:?}", d.bitrate);
        println!("trackers: {:?}", d.trackers);
        println!("is_live: {}", d.is_live);
        println!("pieces: {} entries", d.pieces.len());
        println!(
            "pubkey ({} bytes): {}",
            d.pubkey.len(),
            hex_encode(&d.pubkey)
        );
        if let crate::bencode::Bencode::Dict(map) = &d.raw {
            println!("all top-level keys + values:");
            for (k, v) in map.iter() {
                println!("  {:?} = {:?}", String::from_utf8_lossy(k), v);
            }
        }
    }

    fn hex_encode(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }
}
