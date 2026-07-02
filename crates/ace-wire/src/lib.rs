//! ace-wire: pure protocol primitives for the Acestream (BitTorrent/BitTornado) wire.
//! No I/O. Encode/decode only. See docs/protocol/wire-protocol.md.

pub mod bencode;
pub mod chunker;
pub mod extended;
pub mod handshake;
pub mod identity;
pub mod infohash;
pub mod live;
pub mod live_auth;
pub mod live_codec;
pub mod message;
pub mod reassembly;
pub mod signing_chunker;
pub mod transport;
pub mod ut_metadata;

/// Errors produced while decoding untrusted wire bytes.
#[derive(Debug, PartialEq, Eq)]
pub enum WireError {
    /// Buffer ended before a complete structure was parsed.
    Truncated,
    /// Bytes did not match the expected format.
    Invalid(&'static str),
}

pub type Result<T> = std::result::Result<T, WireError>;
