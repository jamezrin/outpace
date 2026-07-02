//! Minimal timestamped stderr logging for the daemon (no external deps).
//!
//! All operational logging goes to stderr as `[tag] message` lines. This adds a wall-clock
//! UTC `HH:MM:SS.mmm` prefix so freezes/stalls can be timed and correlated across lines.

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
macro_rules! alog {
    ($($arg:tt)*) => {
        eprintln!("{} {}", $crate::logts::now(), format_args!($($arg)*))
    };
}
