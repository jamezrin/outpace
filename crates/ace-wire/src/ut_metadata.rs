//! BEP-9 `ut_metadata` message codec (pure). A ut_metadata message is the payload that
//! follows the `<ext_id>` byte of an `Extended` peer message: a bencoded control dict
//! (`{msg_type, piece[, total_size]}`) optionally followed by raw metadata bytes.
//!
//! For Acestream the assembled metadata blob is the `AceStreamTransport` file, which
//! `ace_wire::transport` decodes into the stream descriptor.
use crate::bencode::Bencode;

/// Metadata is transferred in 16 KiB blocks (BEP-9).
pub const METADATA_BLOCK_LEN: usize = 16384;

const MSG_TYPE_REQUEST: i64 = 0;
const MSG_TYPE_DATA: i64 = 1;
const MSG_TYPE_REJECT: i64 = 2;

/// Build the request payload for metadata `piece` (the bytes after the ext_id byte):
/// `bencode({msg_type: 0, piece: n})`.
pub fn request_piece(piece: i64) -> Vec<u8> {
    control_dict(MSG_TYPE_REQUEST, piece, None)
}

/// A decoded ut_metadata message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataMessage {
    /// A peer asking us for a metadata piece (we don't serve, but parse it cleanly).
    Request { piece: i64 },
    /// A metadata block: its index, the advertised total size, and the raw bytes.
    Data { piece: i64, total_size: Option<i64>, data: Vec<u8> },
    /// The peer has no metadata / declines piece.
    Reject { piece: i64 },
}

impl MetadataMessage {
    /// Parse a ut_metadata Extended payload: a leading bencode control dict, with any data
    /// bytes (for `Data`) trailing the dict. Returns `None` if the dict is missing/invalid.
    pub fn parse(payload: &[u8]) -> Option<MetadataMessage> {
        let (dict, consumed) = Bencode::parse_prefix(payload).ok()?;
        let msg_type = dict.get(b"msg_type")?.as_int()?;
        let piece = dict.get(b"piece")?.as_int()?;
        match msg_type {
            MSG_TYPE_REQUEST => Some(MetadataMessage::Request { piece }),
            MSG_TYPE_REJECT => Some(MetadataMessage::Reject { piece }),
            MSG_TYPE_DATA => Some(MetadataMessage::Data {
                piece,
                total_size: dict.get(b"total_size").and_then(Bencode::as_int),
                data: payload[consumed..].to_vec(),
            }),
            _ => None,
        }
    }
}

/// Number of 16 KiB metadata pieces for a blob of `total_size` bytes.
pub fn piece_count(total_size: usize) -> usize {
    total_size.div_ceil(METADATA_BLOCK_LEN)
}

fn control_dict(msg_type: i64, piece: i64, total_size: Option<i64>) -> Vec<u8> {
    use std::collections::BTreeMap;
    let mut d: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
    d.insert(b"msg_type".to_vec(), Bencode::Int(msg_type));
    d.insert(b"piece".to_vec(), Bencode::Int(piece));
    if let Some(ts) = total_size {
        d.insert(b"total_size".to_vec(), Bencode::Int(ts));
    }
    Bencode::Dict(d).encode()
}

/// Build a `Data` payload (used by tests / a future seeder): control dict then raw `data`.
pub fn data_piece(piece: i64, total_size: i64, data: &[u8]) -> Vec<u8> {
    let mut out = control_dict(MSG_TYPE_DATA, piece, Some(total_size));
    out.extend_from_slice(data);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_is_canonical_bencode() {
        assert_eq!(request_piece(0), b"d8:msg_typei0e5:piecei0ee".to_vec());
        assert_eq!(request_piece(3), b"d8:msg_typei0e5:piecei3ee".to_vec());
    }

    #[test]
    fn parses_request() {
        let m = MetadataMessage::parse(b"d8:msg_typei0e5:piecei7ee").unwrap();
        assert_eq!(m, MetadataMessage::Request { piece: 7 });
    }

    #[test]
    fn parses_reject() {
        let m = MetadataMessage::parse(b"d8:msg_typei2e5:piecei1ee").unwrap();
        assert_eq!(m, MetadataMessage::Reject { piece: 1 });
    }

    #[test]
    fn parses_data_with_trailing_bytes() {
        let payload = data_piece(2, 40000, &[0xAA, 0xBB, 0xCC]);
        let m = MetadataMessage::parse(&payload).unwrap();
        assert_eq!(
            m,
            MetadataMessage::Data { piece: 2, total_size: Some(40000), data: vec![0xAA, 0xBB, 0xCC] }
        );
    }

    #[test]
    fn non_bencode_is_none() {
        assert!(MetadataMessage::parse(b"not bencode").is_none());
    }

    #[test]
    fn piece_count_rounds_up() {
        assert_eq!(piece_count(0), 0);
        assert_eq!(piece_count(1), 1);
        assert_eq!(piece_count(METADATA_BLOCK_LEN), 1);
        assert_eq!(piece_count(METADATA_BLOCK_LEN + 1), 2);
    }
}
