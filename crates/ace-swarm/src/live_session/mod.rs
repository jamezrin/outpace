//! Boundary for the production live follower. The runner is being relocated here;
//! legacy single-peer entrypoints remain in [`crate::live`] and [`crate::driver`].
mod observation;
mod options;

pub use observation::{LiveCounters, LiveObserver, LivePosition};
pub use options::{
    LiveRecoveryOptions, MAX_LIVE_RECOVERY_ACTIVE_UPSTREAMS, MAX_LIVE_RECOVERY_PARALLEL_CONNECT,
    MAX_LIVE_RECOVERY_PIECE_ADVANCE, MAX_LIVE_RECOVERY_REASM_PIECES_AHEAD,
};

/// Contiguous authenticated media with an optional gap immediately before it.
pub struct LiveOutput {
    pub bytes: bytes::Bytes,
    pub discontinuity: bool,
}

/// Engine-configured store construction, including cache backend and fallback policy.
pub type StoreFactory = std::sync::Arc<dyn Fn([u8; 20]) -> crate::store::PieceStore + Send + Sync>;
