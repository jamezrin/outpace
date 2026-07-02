//! ace-tracker: BitTorrent UDP tracker client (BEP-15) for Acestream infohashes.
pub mod client;
pub mod codec;

#[derive(Debug)]
pub enum TrackerError {
    /// Response too short / malformed.
    Malformed(&'static str),
    /// Transaction id in the response did not match the request.
    TransactionMismatch,
    /// Tracker returned an error action (3) with this message.
    Tracker(String),
    /// Underlying I/O or timeout.
    Io(std::io::Error),
    /// Operation timed out.
    Timeout,
}

impl From<std::io::Error> for TrackerError {
    fn from(e: std::io::Error) -> Self {
        TrackerError::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, TrackerError>;
