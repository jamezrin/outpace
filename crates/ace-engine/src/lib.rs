//! ace-engine: the outpace daemon — a 6878-compatible HTTP subset that joins the
//! Acestream swarm, pulls a stream, and re-exposes it as MPEG-TS / HLS / m3u.
//!
//! This currently provides the pure HTTP route surface ([`routes`]); the session
//! manager, swarm wiring, and HTTP server are added as the live data path lands
//! (gated on the node-identity work — see `docs/protocol/notes/14-16`).

pub mod routes;
