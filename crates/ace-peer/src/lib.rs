//! ace-peer: async Acestream peer session built on ace-wire codecs.
pub mod session;

#[derive(Debug)]
pub enum PeerError {
    /// A protocol decode failed.
    Wire(ace_wire::WireError),
    /// Peer presented a handshake for a different infohash.
    InfohashMismatch,
    /// Connection closed before a full structure arrived.
    Closed,
    /// Underlying I/O.
    Io(std::io::Error),
}

impl From<std::io::Error> for PeerError {
    fn from(e: std::io::Error) -> Self { PeerError::Io(e) }
}
impl From<ace_wire::WireError> for PeerError {
    fn from(e: ace_wire::WireError) -> Self { PeerError::Wire(e) }
}

pub type Result<T> = std::result::Result<T, PeerError>;
