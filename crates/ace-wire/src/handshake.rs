//! Acestream peer handshake: BitTorrent layout with a custom pstr.
//! `0x11 "AceStreamProtocol" + 8 reserved + infohash(20) + peer_id(20)`.
//!
//! Most peers use the full 66-byte form, but the official engine as an inbound leecher has
//! also been observed opening with only the 46-byte prefix through `infohash`; responders
//! must accept that short form and reply with their full handshake.
//! Exported torrent metainfo selects the separate standard 68-byte BitTorrent form explicitly.
use crate::{Result, WireError};
use rand::Rng;

/// Protocol string (length 17). Replaces BitTorrent's "BitTorrent protocol".
pub const PSTR: &[u8] = b"AceStreamProtocol";
/// Prefix length through the infohash: 1 + 17 + 8 + 20.
pub const HANDSHAKE_PREFIX_LEN: usize = 46;
/// Total handshake length: 1 + 17 + 8 + 20 + 20.
pub const HANDSHAKE_LEN: usize = 66;
/// Standard BitTorrent protocol string used by exported torrent metainfo.
pub const BITTORRENT_PSTR: &[u8] = b"BitTorrent protocol";
/// Standard BitTorrent handshake length, including the peer id.
pub const BITTORRENT_HANDSHAKE_LEN: usize = 68;

/// Selected from the transport descriptor before exchanging any peer bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HandshakeProtocol {
    /// Legacy wrapped VOD and live/metadata sessions retain the AceStream handshake.
    #[default]
    AceStream,
    /// Exported torrent metainfo uses the standard BitTorrent handshake.
    BitTorrent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    pub reserved: [u8; 8],
    pub infohash: [u8; 20],
    pub peer_id: [u8; 20],
}

impl Handshake {
    /// Build an outgoing handshake with zero reserved bits.
    pub fn new(infohash: [u8; 20], peer_id: [u8; 20]) -> Self {
        Handshake {
            reserved: [0; 8],
            infohash,
            peer_id,
        }
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

    /// Encode the explicitly selected protocol; [`Self::encode`] remains AceStream.
    pub fn encode_with_protocol(&self, protocol: HandshakeProtocol) -> Vec<u8> {
        if protocol == HandshakeProtocol::AceStream {
            return self.encode().to_vec();
        }
        let mut out = Vec::with_capacity(BITTORRENT_HANDSHAKE_LEN);
        out.push(BITTORRENT_PSTR.len() as u8);
        out.extend_from_slice(BITTORRENT_PSTR);
        out.extend_from_slice(&self.reserved);
        out.extend_from_slice(&self.infohash);
        out.extend_from_slice(&self.peer_id);
        out
    }

    /// Decode only the selected protocol, without guessing or falling back.
    pub fn decode_with_protocol(buf: &[u8], protocol: HandshakeProtocol) -> Result<Handshake> {
        if protocol == HandshakeProtocol::AceStream {
            return Self::decode(buf);
        }
        if buf.len() < BITTORRENT_HANDSHAKE_LEN {
            return Err(WireError::Truncated);
        }
        if buf[0] as usize != BITTORRENT_PSTR.len() || &buf[1..20] != BITTORRENT_PSTR {
            return Err(WireError::Invalid("not a BitTorrent protocol handshake"));
        }
        let mut reserved = [0u8; 8];
        reserved.copy_from_slice(&buf[20..28]);
        let mut infohash = [0u8; 20];
        infohash.copy_from_slice(&buf[28..48]);
        let mut peer_id = [0u8; 20];
        peer_id.copy_from_slice(&buf[48..68]);
        Ok(Handshake {
            reserved,
            infohash,
            peer_id,
        })
    }

    pub fn decode(buf: &[u8]) -> Result<Handshake> {
        if buf.len() < HANDSHAKE_LEN {
            return Err(WireError::Truncated);
        }
        let mut hs = Self::decode_prefix(&buf[..HANDSHAKE_PREFIX_LEN])?;
        hs.peer_id.copy_from_slice(&buf[46..66]);
        Ok(hs)
    }

    pub fn decode_prefix(buf: &[u8]) -> Result<Handshake> {
        if buf.len() < HANDSHAKE_PREFIX_LEN {
            return Err(WireError::Truncated);
        }
        if buf[0] as usize != PSTR.len() || &buf[1..18] != PSTR {
            return Err(WireError::Invalid("not an AceStreamProtocol handshake"));
        }
        let mut reserved = [0u8; 8];
        reserved.copy_from_slice(&buf[18..26]);
        let mut infohash = [0u8; 20];
        infohash.copy_from_slice(&buf[26..46]);
        Ok(Handshake {
            reserved,
            infohash,
            peer_id: [0u8; 20],
        })
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
