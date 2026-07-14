//! Resolved run configuration and the per-run output directory allocator.
//!
//! [`Config`] is the fully-resolved settings for one `swarmtest run` invocation,
//! built from CLI args plus defaults. Each run gets its own timestamped directory
//! under `target/swarmtest/<UTC-timestamp>/` (which is already gitignored).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

/// Default UDP address the in-process tracker binds to.
pub const DEFAULT_TRACKER_ADDR: &str = "0.0.0.0:7001";
/// Default TCP address the static descriptor HTTP server binds to.
pub const DEFAULT_HTTPD_ADDR: &str = "0.0.0.0:7002";
/// Default engine tarball download URL (Ubuntu 22.04 / py3.10 build of 3.2.11).
pub const DEFAULT_ENGINE_URL: &str =
    "https://download.acestream.media/linux/acestream_3.2.11_ubuntu_22.04_x86_64_py3.10.tar.gz";
/// Default warmup window (seconds) before payload assertions begin.
pub const DEFAULT_WARMUP_SECS: u64 = 60;
/// Default measurement window (seconds).
pub const DEFAULT_WINDOW_SECS: u64 = 75;

/// One interop scenario. `all` at the CLI expands into the full ordered set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scenario {
    /// Real engine broadcasts + 3 real-engine consumers.
    Baseline,
    /// Real engine broadcasts + 2 engine + 2 outpace consumers.
    Mixed,
    /// Outpace broadcasts + 2 engine + 2 outpace consumers.
    OutpaceSource,
}

impl Scenario {
    /// Stable machine-readable name (also the directory / report key).
    pub fn as_str(self) -> &'static str {
        match self {
            Scenario::Baseline => "baseline",
            Scenario::Mixed => "mixed",
            Scenario::OutpaceSource => "outpace-source",
        }
    }

    /// The full ordered scenario set, as run for `--scenario all`.
    pub fn all() -> Vec<Scenario> {
        vec![Scenario::Baseline, Scenario::Mixed, Scenario::OutpaceSource]
    }
}

/// Fully-resolved configuration for a single `swarmtest run`.
#[derive(Debug, Clone)]
pub struct Config {
    /// Scenarios to run, in order.
    pub scenarios: Vec<Scenario>,
    /// Warmup window in seconds.
    pub warmup_secs: u64,
    /// Measurement window in seconds.
    pub window_secs: u64,
    /// Explicit engine directory override, if provided.
    pub engine_dir: Option<PathBuf>,
    /// Engine tarball download URL.
    pub engine_url: String,
    /// Keep the run directory / containers after completion.
    pub keep: bool,
    /// Capture packet traces (pcap) during the run.
    pub pcap: bool,
    /// This run's output directory (already created on disk).
    pub run_dir: PathBuf,
    /// Workspace root (the directory containing `target/` and the root `Dockerfile`).
    pub workspace_root: PathBuf,
    /// UDP address the in-process tracker binds to.
    pub tracker_addr: SocketAddr,
    /// TCP address the static HTTP server binds to.
    pub httpd_addr: SocketAddr,
}

/// Allocate and create a fresh `target/swarmtest/<UTC-timestamp>/` run directory.
///
/// `workspace_root` is the directory containing `target/` (the repo root when run
/// from a checkout). The timestamp is a compact UTC `YYYYMMDDThhmmssZ` string.
pub fn allocate_run_dir(workspace_root: &Path) -> Result<PathBuf> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_secs();
    let dir = workspace_root
        .join("target")
        .join("swarmtest")
        .join(format_utc_timestamp(now));
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("creating run directory {}", dir.display()))?;
    Ok(dir)
}

/// Format a unix timestamp (seconds) as a compact UTC `YYYYMMDDThhmmssZ` string.
///
/// Pure and self-contained (no date crate) so it can be unit tested against known
/// epoch values. Uses Howard Hinnant's `civil_from_days` algorithm for the date.
pub fn format_utc_timestamp(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (hh, mm, ss) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (y, mo, d) = civil_from_days(days);
    format!("{y:04}{mo:02}{d:02}T{hh:02}{mm:02}{ss:02}Z")
}

/// Convert days-since-unix-epoch into a `(year, month, day)` civil date (UTC).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    // Howard Hinnant, "chrono-Compatible Low-Level Date Algorithms".
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenario_names_are_stable() {
        assert_eq!(Scenario::Baseline.as_str(), "baseline");
        assert_eq!(Scenario::Mixed.as_str(), "mixed");
        assert_eq!(Scenario::OutpaceSource.as_str(), "outpace-source");
        assert_eq!(Scenario::all().len(), 3);
    }

    #[test]
    fn format_utc_timestamp_known_epochs() {
        // 0 = 1970-01-01T00:00:00Z
        assert_eq!(format_utc_timestamp(0), "19700101T000000Z");
        // 1_600_000_000 = 2020-09-13T12:26:40Z (well-known value)
        assert_eq!(format_utc_timestamp(1_600_000_000), "20200913T122640Z");
        // A leap-year date to exercise civil_from_days: 2020-02-29T00:00:00Z
        // 2020-02-29 is 18321 days after epoch.
        assert_eq!(format_utc_timestamp(18_321 * 86_400), "20200229T000000Z");
    }

    #[test]
    fn allocate_run_dir_creates_timestamped_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = allocate_run_dir(tmp.path()).unwrap();
        assert!(dir.exists());
        assert!(dir.starts_with(tmp.path().join("target").join("swarmtest")));
    }
}
