//! ace-engine: the **outpace** daemon. A provider-agnostic streaming engine that fans
//! one shared live MPEG-TS download out to many HTTP clients, behind a clean
//! `/streams/{network}/{id}` API. All network-specific protocol lives behind the
//! [`provider::StreamProvider`] adapter (see the v1 design spec).

#[macro_use]
pub mod logts;
pub mod ace_provider;
pub mod broadcast;
pub mod broadcast_ingest;
pub mod cli;
pub mod config;
pub mod hls;
pub mod http;
pub mod manager;
pub mod provider;
pub mod routes;
pub mod rtmp;
pub mod rtmp_ts;
pub mod runtime;
pub mod session;
pub mod testprovider;
