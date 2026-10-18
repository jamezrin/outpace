use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Arc;

/// Position and active coverage published by the follower.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LivePosition {
    pub pos: Option<u64>,
    pub last: Option<u64>,
    pub live_first: Option<u64>,
    pub live_last: Option<u64>,
    pub buffer_pieces: u64,
}

/// Observation sink independent of engine statistics presentation.
pub trait LiveObserver: Send + Sync {
    fn received_bytes(&self, count: u64);
    fn publish_position(&self, position: Option<LivePosition>, buffer_ms: u64);
    /// Clear active coverage while preserving historical position fields.
    fn clear_active_coverage(&self);
}

/// Shared counters; received wire bytes are reported separately through the observer.
#[derive(Clone)]
pub struct LiveCounters {
    pub peers: Arc<AtomicU32>,
    pub downloaded: Arc<AtomicU64>,
    pub uploaded: Arc<AtomicU64>,
    pub peers_served: Arc<AtomicU32>,
}
