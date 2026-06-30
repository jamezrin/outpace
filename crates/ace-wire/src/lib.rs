//! ace-wire: pure protocol primitives for the Acestream (BitTorrent/BitTornado) wire.
//! No I/O. Encode/decode only. See docs/protocol/wire-protocol.md.

pub mod bencode;
pub mod extended;
pub mod handshake;
pub mod infohash;
pub mod identity;
pub mod live;
pub mod live_codec;
pub mod ut_metadata;
pub mod chunker;
pub mod reassembly;
pub mod message;
pub mod transport;

/// Errors produced while decoding untrusted wire bytes.
#[derive(Debug, PartialEq, Eq)]
pub enum WireError {
    /// Buffer ended before a complete structure was parsed.
    Truncated,
    /// Bytes did not match the expected format.
    Invalid(&'static str),
}

pub type Result<T> = std::result::Result<T, WireError>;
