//! Optional SRT ingest configuration and owned transport integration.
pub mod config;

#[cfg(feature = "srt")]
mod native;

#[cfg(feature = "srt")]
mod owner;

#[cfg(feature = "srt")]
mod ingest;

mod service;
pub(crate) use service::SrtService;
