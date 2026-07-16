//! ace-swarm: multi-peer live download orchestration.
//!
//! Connects to peers, performs the **signed** Acestream handshake
//! ([`ace_wire::identity`] + `PeerSession::send_signed_extended_handshake`), and pulls
//! live pieces, feeding them to [`ace_wire::reassembly::PieceReassembler`] for the media
//! layer. The piece-distribution decision core is [`scheduler::Scheduler`] (pure and
//! tested); the async peer I/O loop wraps it.
//!
//! The async driver is validated against a live channel that is actually delivering data
//! (see `README.md` and `docs/protocol/notes/20-vlc-playback.md`); the scheduler is
//! validated here.

// The workspace's one logging macro, re-exported so `crate::alog!` resolves here.
// Operational events use it; user-facing CLI/banner lines stay plain `eprintln!`
// (see the `ace_log` crate docs for the distinction).
pub use ace_log::alog;

pub mod dht;
pub mod discover;
pub mod driver;
pub mod listen;
pub mod live;
pub mod portmap;
pub mod reachability;
pub mod resolve;
pub mod scheduler;
pub mod seed;
pub mod store;
pub mod types;
pub mod vod;
