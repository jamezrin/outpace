//! Minimal timestamped stderr logging (no external deps); see the ace-engine twin.
//!
//! Adds a wall-clock UTC `HH:MM:SS.mmm` prefix to the `[tag] message` stderr lines so
//! swarm events can be timed and interleaved with the engine's own logs.

use std::time::{SystemTime, UNIX_EPOCH};

/// Wall-clock UTC `HH:MM:SS.mmm` for a log-line prefix.
pub fn now() -> String {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = d.as_secs();
    let (h, m, s) = ((secs / 3600) % 24, (secs / 60) % 60, secs % 60);
    format!("{:02}:{:02}:{:02}.{:03}", h, m, s, d.subsec_millis())
}

/// `eprintln!` with a leading UTC timestamp. Takes the same format arguments as `eprintln!`.
#[macro_export]
macro_rules! swarm_log {
    ($($arg:tt)*) => {
        eprintln!("{} {}", $crate::logts::now(), format_args!($($arg)*))
    };
}
