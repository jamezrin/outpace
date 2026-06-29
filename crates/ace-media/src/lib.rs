//! ace-media: turn an ordered byte stream into MPEG-TS / HLS for player consumption.
//!
//! Acestream live content is already an MPEG-TS stream chopped into pieces; the
//! reassembled byte stream (see `ace_wire::reassembly`) is therefore MPEG-TS. This crate
//! provides the pure logic to validate/align TS and segment it for HLS. No I/O.

pub mod hls;
pub mod mpegts;

/// Errors from media framing.
#[derive(Debug, PartialEq, Eq)]
pub enum MediaError {
    /// Buffer is not aligned to 188-byte TS packets starting with the sync byte.
    NotTsAligned,
    /// A required parameter was out of range (e.g. zero packets per segment).
    BadParam(&'static str),
}

pub type Result<T> = std::result::Result<T, MediaError>;
