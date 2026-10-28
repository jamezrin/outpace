//! ace-engine: the **outpace** daemon. A provider-agnostic streaming engine that fans
//! one shared live MPEG-TS download out to many HTTP clients, behind a clean
//! `/streams/{network}/{id}` API. All network-specific protocol lives behind the
//! [`provider::StreamProvider`] adapter (see the v1 design spec).
//!
//! [`ace_media`] supplies pure MPEG-TS alignment, timing, keyframe/access-point, and
//! segment helpers without I/O. This crate owns their live integration: [`session`]
//! broadcasts source data and discontinuities, [`manager`] manages sessions and shared
//! packagers, and [`hls`] maintains the live segment window, readiness, and playlists.
//! [`http`] handles playlist and segment requests; [`routes`] parses the compatibility
//! HTTP routes rather than serving them.

// The workspace's one logging macro, re-exported so `crate::alog!` resolves here.
// Operational events use it; user-facing CLI/banner lines stay plain `eprintln!`
// (see the `ace_log` crate docs for the distinction).
pub use ace_log::alog;

pub mod ace_provider;
mod admission;
pub mod broadcast;
pub mod broadcast_ingest;
pub mod broadcast_options;
#[cfg(test)]
#[path = "broadcast_options/tests.rs"]
mod broadcast_options_tests;
pub mod broadcast_persist;
pub mod broadcast_pull;
#[cfg(test)]
#[path = "broadcast_pull/tests.rs"]
mod broadcast_pull_tests;
pub mod cli;
pub mod config;
pub mod hls;
pub mod hls_audio;
pub mod http;
mod live_stats;
pub mod magnet;
pub mod manager;
pub mod provider;
pub mod routes;
pub mod rtmp;
pub mod rtmp_ts;
pub mod runtime;
pub mod server_api;
pub mod session;
pub mod startup_buffer;
pub mod testprovider;
pub mod transport_url;
