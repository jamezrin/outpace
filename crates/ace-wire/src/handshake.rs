//! Acestream peer handshake: 66 bytes, BitTorrent layout with a custom pstr.
//! `0x11 "AceStreamProtocol" + 8 reserved + infohash(20) + peer_id(20)`.
use crate::{Result, WireError};
use rand::Rng;

/// Protocol string (length 17). Replaces BitTorrent's "BitTorrent protocol".
pub const PSTR: &[u8] = b"AceStreamProtocol";
/// Total handshake length: 1 + 17 + 8 + 20 + 20.
pub const HANDSHAKE_LEN: usize = 66;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    pub reserved: [u8; 8],
    pub infohash: [u8; 20],
    pub peer_id: [u8; 20],
}

impl Handshake {
    /// Build an outgoing handshake with zero reserved bits.
    pub fn new(infohash: [u8; 20], peer_id: [u8; 20]) -> Self {
        Handshake { reserved: [0; 8], infohash, peer_id }
    }

    pub fn encode(&self) -> [u8; HANDSHAKE_LEN] {
        let mut out = [0u8; HANDSHAKE_LEN];
        out[0] = PSTR.len() as u8;
        out[1..18].copy_from_slice(PSTR);
        out[18..26].copy_from_slice(&self.reserved);
        out[26..46].copy_from_slice(&self.infohash);
        out[46..66].copy_from_slice(&self.peer_id);
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Handshake> {
        if buf.len() < HANDSHAKE_LEN { return Err(WireError::Truncated); }
        if buf[0] as usize != PSTR.len() || &buf[1..18] != PSTR {
            return Err(WireError::Invalid("not an AceStreamProtocol handshake"));
        }
        let mut reserved = [0u8; 8];
        reserved.copy_from_slice(&buf[18..26]);
        let mut infohash = [0u8; 20];
        infohash.copy_from_slice(&buf[26..46]);
        let mut peer_id = [0u8; 20];
        peer_id.copy_from_slice(&buf[46..66]);
        Ok(Handshake { reserved, infohash, peer_id })
    }
}

/// Generate an ephemeral peer_id: `R30------` + 11 random ASCII alphanumerics.
pub fn random_peer_id() -> [u8; 20] {
    const ALPHANUM: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut id = [0u8; 20];
    id[..9].copy_from_slice(b"R30------");
    let mut rng = rand::thread_rng();
    for b in &mut id[9..] {
        *b = ALPHANUM[rng.gen_range(0..ALPHANUM.len())];
    }
    id
}
