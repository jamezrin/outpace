//! The workspace's single operational logger: timestamped stderr, no external deps.
//!
//! [`alog!`] is the **only** logging macro in the workspace. It writes
//! `HH:MM:SS.mmm [tag] message` to stderr so freezes and stalls can be timed and
//! correlated across crates. Deliberately dependency-free: no `log`, no `tracing`.
//!
//! # Operational logging vs. user-facing output
//!
//! Two kinds of text go to the console, and they are not the same thing:
//!
//! - **Operational events** — what the daemon is doing while it runs (peer churn, HLS
//!   segment decisions, ingest failures, shutdown). These are read after the fact, out of
//!   a log file, and need a timestamp and a `[tag]` to be correlated. **Use [`alog!`].**
//! - **User-facing CLI and startup-banner output** — the `outpace: listening on …` boot
//!   lines, `outpace play: …` progress, and test-skip notices. A human is reading these
//!   live, and a `12:34:56.789 [x]` prefix would be noise. **Use plain `eprintln!`.**
//!
//! When in doubt, ask who reads the line: an operator grepping a log tomorrow wants
//! [`alog!`]; a person watching the terminal right now wants `eprintln!`.
//!
//! `tools/swarmtest` is exempt from this crate entirely — it is an operator-driven test
//! harness whose entire output is the user-facing kind, so it uses plain `eprintln!`
//! and does not depend on `ace-log`.
//!
//! The library crates (`ace-media`, `ace-peer`, `ace-tracker`, `ace-wire`) log nothing:
//! they return errors and let the caller decide. Keep it that way.

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
///
/// For operational events only; see the module docs for the CLI/banner distinction.
#[macro_export]
macro_rules! alog {
    ($($arg:tt)*) => {
        eprintln!("{} {}", $crate::now(), format_args!($($arg)*))
    };
}

#[cfg(test)]
mod tests {
    use super::now;

    #[test]
    fn now_is_fixed_width_hms_millis() {
        let s = now();
        assert_eq!(s.len(), 12, "expected HH:MM:SS.mmm, got {s:?}");
        let (hms, millis) = s.split_once('.').expect("millisecond separator");
        assert_eq!(millis.len(), 3);
        assert!(millis.chars().all(|c| c.is_ascii_digit()), "{s:?}");

        let parts: Vec<&str> = hms.split(':').collect();
        assert_eq!(parts.len(), 3, "{s:?}");
        assert!(
            parts
                .iter()
                .all(|p| p.len() == 2 && p.chars().all(|c| c.is_ascii_digit())),
            "{s:?}"
        );
    }

    #[test]
    fn now_fields_are_in_range() {
        let s = now();
        let nums: Vec<u32> = s
            .split([':', '.'])
            .map(|p| p.parse().expect("numeric field"))
            .collect();
        assert!(nums[0] < 24, "hour out of range: {s:?}");
        assert!(nums[1] < 60, "minute out of range: {s:?}");
        assert!(nums[2] < 60, "second out of range: {s:?}");
        assert!(nums[3] < 1000, "millis out of range: {s:?}");
    }
}
