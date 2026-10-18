use std::time::Duration;

pub const MAX_LIVE_RECOVERY_ACTIVE_UPSTREAMS: usize = 256;
pub const MAX_LIVE_RECOVERY_PARALLEL_CONNECT: usize = 1024;
pub const MAX_LIVE_RECOVERY_PIECE_ADVANCE: u64 = 16_384;
pub const MAX_LIVE_RECOVERY_REASM_PIECES_AHEAD: u64 = 65_536;

/// Validated limits and deadlines for the production follower.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveRecoveryOptions {
    pub request_timeout_ms: u64,
    pub stale_upstream_timeout_ms: u64,
    pub request_check_interval_ms: u64,
    pub max_active_upstreams: usize,
    pub max_parallel_connect: usize,
    pub max_piece_advance: u64,
    pub max_reasm_pieces_ahead: u64,
}

impl LiveRecoveryOptions {
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }

    pub fn stale_upstream_timeout(&self) -> Duration {
        Duration::from_millis(self.stale_upstream_timeout_ms)
    }

    pub fn request_check_interval(&self) -> Duration {
        Duration::from_millis(self.request_check_interval_ms)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.request_timeout_ms == 0 {
            return Err("OUTPACE_REQUEST_TIMEOUT_MS must be >= 1".into());
        }
        if self.stale_upstream_timeout_ms == 0 {
            return Err("OUTPACE_STALE_UPSTREAM_TIMEOUT_MS must be >= 1".into());
        }
        if self.request_check_interval_ms == 0 {
            return Err("OUTPACE_REQUEST_CHECK_INTERVAL_MS must be >= 1".into());
        }
        if self.request_timeout_ms >= self.stale_upstream_timeout_ms {
            return Err(
                "OUTPACE_REQUEST_TIMEOUT_MS must be < OUTPACE_STALE_UPSTREAM_TIMEOUT_MS".into(),
            );
        }
        if self.request_check_interval_ms > self.request_timeout_ms {
            return Err(
                "OUTPACE_REQUEST_CHECK_INTERVAL_MS must be <= OUTPACE_REQUEST_TIMEOUT_MS".into(),
            );
        }
        if self.max_active_upstreams == 0 {
            return Err("OUTPACE_MAX_ACTIVE_UPSTREAMS must be >= 1".into());
        }
        if self.max_parallel_connect == 0 {
            return Err("OUTPACE_MAX_PARALLEL_CONNECT must be >= 1".into());
        }
        if self.max_piece_advance == 0 {
            return Err("OUTPACE_MAX_PIECE_ADVANCE must be >= 1".into());
        }
        if self.max_active_upstreams > MAX_LIVE_RECOVERY_ACTIVE_UPSTREAMS {
            return Err(format!(
                "OUTPACE_MAX_ACTIVE_UPSTREAMS must be <= {MAX_LIVE_RECOVERY_ACTIVE_UPSTREAMS}"
            ));
        }
        if self.max_parallel_connect > MAX_LIVE_RECOVERY_PARALLEL_CONNECT {
            return Err(format!(
                "OUTPACE_MAX_PARALLEL_CONNECT must be <= {MAX_LIVE_RECOVERY_PARALLEL_CONNECT}"
            ));
        }
        if self.max_piece_advance > MAX_LIVE_RECOVERY_PIECE_ADVANCE {
            return Err(format!(
                "OUTPACE_MAX_PIECE_ADVANCE must be <= {MAX_LIVE_RECOVERY_PIECE_ADVANCE}"
            ));
        }
        if self.max_reasm_pieces_ahead > MAX_LIVE_RECOVERY_REASM_PIECES_AHEAD {
            return Err(format!(
                "OUTPACE_MAX_REASM_PIECES_AHEAD must be <= {MAX_LIVE_RECOVERY_REASM_PIECES_AHEAD}"
            ));
        }
        if self.max_reasm_pieces_ahead < self.max_piece_advance {
            return Err(
                "OUTPACE_MAX_REASM_PIECES_AHEAD must be >= OUTPACE_MAX_PIECE_ADVANCE".into(),
            );
        }
        Ok(())
    }
}
