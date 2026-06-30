//! ace-swarm: multi-peer live download orchestration.
//!
//! Connects to peers, performs the **signed** Acestream handshake
//! ([`ace_wire::identity`] + `PeerSession::send_signed_extended_handshake`), and pulls
//! live pieces, feeding them to [`ace_wire::reassembly::PieceReassembler`] for the media
//! layer. The piece-distribution decision core is [`scheduler::Scheduler`] (pure and
//! tested); the async peer I/O loop wraps it.
//!
//! The async driver is validated against a live channel that is actually delivering data
//! (see `docs/RESUME.md` — currently environment-gated); the scheduler is validated here.

pub mod dht;
pub mod discover;
pub mod driver;
pub mod listen;
pub mod live;
pub mod resolve;
pub mod scheduler;
pub mod seed;
pub mod store;
pub mod types;
