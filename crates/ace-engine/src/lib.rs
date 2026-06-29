//! ace-engine: the **outpace** daemon. A provider-agnostic streaming engine that fans
//! one shared live MPEG-TS download out to many HTTP clients, behind a clean
//! `/streams/{network}/{id}` API. All network-specific protocol lives behind the
//! [`provider::StreamProvider`] adapter (see the v1 design spec).

pub mod provider;
pub mod routes;
pub mod testprovider;
