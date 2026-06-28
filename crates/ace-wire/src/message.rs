//! BitTorrent peer-message framing: <u32 be length><u8 id><payload>; len 0 = keep-alive.
use crate::{Result, WireError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerMessage {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Vec<u8>),
    Request { index: u32, begin: u32, length: u32 },
    Piece { index: u32, begin: u32, block: Vec<u8> },
    Cancel { index: u32, begin: u32, length: u32 },
    Extended { ext_id: u8, payload: Vec<u8> },
}

impl PeerMessage {
    pub fn encode(&self) -> Vec<u8> {
        let mut body = Vec::new();
        match self {
            PeerMessage::KeepAlive => {}
            PeerMessage::Choke => body.push(0),
            PeerMessage::Unchoke => body.push(1),
            PeerMessage::Interested => body.push(2),
            PeerMessage::NotInterested => body.push(3),
            PeerMessage::Have(i) => { body.push(4); body.extend_from_slice(&i.to_be_bytes()); }
            PeerMessage::Bitfield(b) => { body.push(5); body.extend_from_slice(b); }
            PeerMessage::Request { index, begin, length } => {
                body.push(6); body.extend_from_slice(&index.to_be_bytes());
                body.extend_from_slice(&begin.to_be_bytes()); body.extend_from_slice(&length.to_be_bytes());
            }
            PeerMessage::Piece { index, begin, block } => {
                body.push(7); body.extend_from_slice(&index.to_be_bytes());
                body.extend_from_slice(&begin.to_be_bytes()); body.extend_from_slice(block);
            }
            PeerMessage::Cancel { index, begin, length } => {
                body.push(8); body.extend_from_slice(&index.to_be_bytes());
                body.extend_from_slice(&begin.to_be_bytes()); body.extend_from_slice(&length.to_be_bytes());
            }
            PeerMessage::Extended { ext_id, payload } => {
                body.push(20); body.push(*ext_id); body.extend_from_slice(payload);
            }
        }
        let mut out = (body.len() as u32).to_be_bytes().to_vec();
        out.extend_from_slice(&body);
        out
    }

    /// Decode one frame from the front of `buf`.
    /// Returns Ok(None) if more bytes are needed (incomplete frame).
    pub fn decode(buf: &[u8]) -> Result<Option<(PeerMessage, usize)>> {
        if buf.len() < 4 { return Ok(None); }
        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let total = 4 + len;
        if buf.len() < total { return Ok(None); }
        let body = &buf[4..total];
        if len == 0 { return Ok(Some((PeerMessage::KeepAlive, total))); }
        let id = body[0];
        let p = &body[1..];
        let msg = match id {
            0 => PeerMessage::Choke,
            1 => PeerMessage::Unchoke,
            2 => PeerMessage::Interested,
            3 => PeerMessage::NotInterested,
            4 => PeerMessage::Have(be32(p, 0)?),
            5 => PeerMessage::Bitfield(p.to_vec()),
            6 => PeerMessage::Request { index: be32(p, 0)?, begin: be32(p, 4)?, length: be32(p, 8)? },
            7 => {
                if p.len() < 8 { return Err(WireError::Invalid("short piece")); }
                PeerMessage::Piece { index: be32(p, 0)?, begin: be32(p, 4)?, block: p[8..].to_vec() }
            }
            8 => PeerMessage::Cancel { index: be32(p, 0)?, begin: be32(p, 4)?, length: be32(p, 8)? },
            20 => {
                if p.is_empty() { return Err(WireError::Invalid("short extended")); }
                PeerMessage::Extended { ext_id: p[0], payload: p[1..].to_vec() }
            }
            _ => return Err(WireError::Invalid("unknown message id")),
        };
        Ok(Some((msg, total)))
    }
}

fn be32(p: &[u8], off: usize) -> Result<u32> {
    let s = p.get(off..off + 4).ok_or(WireError::Invalid("short u32"))?;
    Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keepalive_roundtrip() {
        assert_eq!(PeerMessage::KeepAlive.encode(), vec![0, 0, 0, 0]);
        assert_eq!(PeerMessage::decode(&[0, 0, 0, 0]).unwrap(), Some((PeerMessage::KeepAlive, 4)));
    }

    #[test]
    fn request_roundtrip() {
        let m = PeerMessage::Request { index: 1, begin: 16384, length: 16384 };
        let enc = m.encode();
        // len = 13 (1 id + 12 payload)
        assert_eq!(&enc[..5], &[0, 0, 0, 13, 6]);
        assert_eq!(PeerMessage::decode(&enc).unwrap(), Some((m, enc.len())));
    }

    #[test]
    fn extended_roundtrip() {
        let m = PeerMessage::Extended { ext_id: 0, payload: b"de".to_vec() };
        let enc = m.encode();
        assert_eq!(&enc[..6], &[0, 0, 0, 4, 20, 0]); // len=4, id=20, ext_id=0
        assert_eq!(PeerMessage::decode(&enc).unwrap(), Some((m, enc.len())));
    }

    #[test]
    fn partial_frame_returns_none() {
        // length prefix says 13 bytes follow, but we only have a few
        assert_eq!(PeerMessage::decode(&[0, 0, 0, 13, 6, 0, 0]).unwrap(), None);
        // not even a full length prefix
        assert_eq!(PeerMessage::decode(&[0, 0]).unwrap(), None);
    }
}
