//! Resolve a stream identifier to a downloadable [`StreamInfo`].
//!
//! Two identifier shapes:
//!   * **infohash** — a 40-hex BitTorrent infohash; usable directly with default live
//!     geometry (peers advertise their piece range in the handshake `mi`).
//!   * **content-id** — locates an `AceStreamTransport` metadata file which, once fetched
//!     over the network (BEP-9 ut_metadata) and decoded, yields the infohash + geometry +
//!     trackers. The fetch is network-native (no Acestream API); see [`stream_info_from_transport`]
//!     for the pure decode half. The ut_metadata exchange is the remaining live-gated step
//!     (documented in the design spec).

use crate::types::StreamInfo;
use ace_wire::infohash::infohash_of_transport;
use ace_wire::transport::decode_transport;

/// Default live geometry when only an infohash is known: 1 MiB pieces / 16 KiB chunks.
pub const DEFAULT_PIECE_LENGTH: u64 = 1_048_576;
pub const DEFAULT_CHUNK_LENGTH: u64 = 16_384;

#[derive(Debug, PartialEq, Eq)]
pub enum ResolveError {
    BadInfohash,
    Transport(&'static str),
}

/// Build a [`StreamInfo`] from raw `AceStreamTransport` file bytes (the pure half of
/// content-id resolution): infohash = SHA1(file), geometry + trackers from the descriptor.
pub fn stream_info_from_transport(bytes: &[u8]) -> Result<StreamInfo, ResolveError> {
    let d = decode_transport(bytes).map_err(|_| ResolveError::Transport("decode failed"))?;
    Ok(StreamInfo {
        infohash: infohash_of_transport(bytes),
        piece_length: d.piece_length,
        chunk_length: d.chunk_length,
        trackers: d.trackers,
    })
}

/// Build a [`StreamInfo`] from a 40-char hex infohash with default live geometry. `trackers`
/// are supplied separately (config defaults / DHT), since a bare infohash carries none.
pub fn stream_info_from_infohash(hex: &str, trackers: Vec<String>) -> Result<StreamInfo, ResolveError> {
    let bytes = decode_hex20(hex).ok_or(ResolveError::BadInfohash)?;
    Ok(StreamInfo {
        infohash: bytes,
        piece_length: DEFAULT_PIECE_LENGTH,
        chunk_length: DEFAULT_CHUNK_LENGTH,
        trackers,
    })
}

fn decode_hex20(hex: &str) -> Option<[u8; 20]> {
    if hex.len() != 40 {
        return None;
    }
    let mut out = [0u8; 20];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cbc::cipher::{block_padding::Pkcs7, BlockModeEncrypt, KeyIvInit};

    // Wrap raw bencode into a transport file under the global key/IV (mirrors transport.rs).
    fn make_transport(plaintext: &[u8]) -> Vec<u8> {
        type Enc = cbc::Encryptor<aes::Aes128>;
        let body = Enc::new_from_slices(&ace_wire::transport::TRANSPORT_KEY, &ace_wire::transport::TRANSPORT_IV)
            .unwrap()
            .encrypt_padded_vec::<Pkcs7>(plaintext);
        let mut out = b"AceStreamTransport\x00\x02".to_vec();
        out.extend_from_slice(&body);
        out
    }

    #[test]
    fn transport_yields_stream_info() {
        let tf = make_transport(
            b"d12:chunk_lengthi16384e12:piece_lengthi1048576e8:trackersl18:udp://t.example:80ee",
        );
        let si = stream_info_from_transport(&tf).unwrap();
        assert_eq!(si.piece_length, 1_048_576);
        assert_eq!(si.chunk_length, 16_384);
        assert_eq!(si.chunks_per_piece(), 64);
        assert_eq!(si.trackers, vec!["udp://t.example:80".to_string()]);
        assert_eq!(si.infohash, infohash_of_transport(&tf));
    }

    #[test]
    fn infohash_form_uses_default_geometry() {
        let hex = "0123456789abcdef0123456789abcdef01234567";
        let si = stream_info_from_infohash(hex, vec!["udp://x:1".into()]).unwrap();
        assert_eq!(si.piece_length, DEFAULT_PIECE_LENGTH);
        assert_eq!(si.chunk_length, DEFAULT_CHUNK_LENGTH);
        assert_eq!(si.infohash[0], 0x01);
        assert_eq!(si.infohash[19], 0x67);
    }

    #[test]
    fn bad_infohash_rejected() {
        assert_eq!(stream_info_from_infohash("xyz", vec![]), Err(ResolveError::BadInfohash));
        assert_eq!(stream_info_from_infohash(&"z".repeat(40), vec![]), Err(ResolveError::BadInfohash));
    }
}
