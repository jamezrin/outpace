//! Pure MPEG-TS alignment, timing, keyframe/access-point, and HLS segment helpers.
//!
//! Acestream live content is already an MPEG-TS stream chopped into pieces; the
//! reassembled byte stream is therefore MPEG-TS. [`mpegts`] provides packet alignment,
//! resynchronization, PCR tracking, and keyframe/access-point detection; [`hls`] provides
//! packet-count segmentation and a basic media-playlist renderer. This crate performs no I/O.
//!
//! The daemon's `ace-engine` crate owns live packaging tasks, session lifecycle, retained
//! segment windows, live playlists, and HTTP serving. Those policies are outside this crate.

pub mod audio;
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
