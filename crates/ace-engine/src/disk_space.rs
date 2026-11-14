//! Free-space check before a VOD download spills pieces to the disk cache.
//!
//! One filesystem query per VOD resolution ([`available_bytes`]); nothing on the piece path and
//! no background work. The fit decision ([`check_fit`]) is a pure function so it is unit-tested
//! without touching a real filesystem.

use std::path::Path;

/// Default free space kept in reserve on the cache filesystem (`OUTPACE_VOD_DISK_RESERVE_BYTES`).
pub const DEFAULT_VOD_DISK_RESERVE_BYTES: u64 = 256 * 1024 * 1024;

/// Proportional safety margin on top of the bytes a download still needs: 1/20 (5%).
const MARGIN_DIVISOR: u64 = 20;

/// A VOD download that would not fit on the cache filesystem.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NoSpace {
    /// Free bytes the download needs, including the safety margin.
    pub required: u64,
    /// Free bytes the filesystem reports as available to this process.
    pub available: u64,
}

impl NoSpace {
    /// User-facing text for the `no_space` error body: sizes only, never a path or identifier.
    pub fn message(&self) -> String {
        format!(
            "not enough free disk space: {} required, {} available",
            format_bytes(self.required.div_ceil(MIB_BYTES), self.required),
            format_bytes(self.available / MIB_BYTES, self.available)
        )
    }
}

/// Decide whether a VOD download fits on the cache filesystem.
///
/// - `total`: the content's total size from its descriptor.
/// - `budget`: the piece store's byte ceiling; the store never holds more than this on disk, so
///   the download needs at most `min(total, budget)`.
/// - `held`: bytes the store already holds for this content (a resumed or seeded file), which
///   need no new space.
/// - `available`: free bytes on the cache filesystem, or `None` when unknown (unsupported
///   platform or a failed query), which always allows the start.
/// - `reserve`: free space to keep regardless of size; the margin is the larger of this and 5%
///   of the bytes still needed.
pub fn check_fit(
    total: u64,
    budget: u64,
    held: u64,
    available: Option<u64>,
    reserve: u64,
) -> Result<(), NoSpace> {
    let need = total.min(budget).saturating_sub(held);
    let Some(available) = available else {
        return Ok(());
    };
    if need == 0 {
        return Ok(());
    }
    let margin = (need / MARGIN_DIVISOR).max(reserve);
    let required = need.saturating_add(margin);
    if available >= required {
        Ok(())
    } else {
        Err(NoSpace {
            required,
            available,
        })
    }
}

/// True for the write errors that mean the disk (or the user's quota) is full.
pub fn is_no_space(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::StorageFull | std::io::ErrorKind::QuotaExceeded
    )
}

/// Free bytes available to this process on the filesystem holding `path`. A missing `path` is
/// resolved through its nearest existing ancestor (the cache directory may not exist yet).
/// `None` when the platform has no supported query or the query fails.
pub fn available_bytes(path: &Path) -> Option<u64> {
    let existing = path.ancestors().find(|p| p.exists())?;
    query_available(existing)
}

#[cfg(unix)]
#[allow(clippy::unnecessary_cast)] // statvfs field widths differ between unix targets.
fn query_available(path: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is a valid NUL-terminated string and `stat` is a writable statvfs buffer.
    if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: statvfs returned 0, so it initialized `stat`.
    let stat = unsafe { stat.assume_init() };
    let block = if stat.f_frsize != 0 {
        stat.f_frsize as u64
    } else {
        stat.f_bsize as u64
    };
    Some((stat.f_bavail as u64).saturating_mul(block))
}

#[cfg(windows)]
fn query_available(path: &Path) -> Option<u64> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;
    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut available = 0u64;
    // SAFETY: `wide` is NUL-terminated; the two unused out-pointers may be null.
    let ok = unsafe {
        GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &mut available,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    (ok != 0).then_some(available)
}

#[cfg(not(any(unix, windows)))]
fn query_available(_path: &Path) -> Option<u64> {
    None
}

const MIB_BYTES: u64 = 1024 * 1024;

/// `mib` (rounded by the caller: up for a requirement, down for free space) once `bytes` reaches
/// a mebibyte, so the message never overstates what is free.
fn format_bytes(mib: u64, bytes: u64) -> String {
    if bytes >= MIB_BYTES {
        format!("{mib} MiB")
    } else {
        format!("{bytes} bytes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;

    #[test]
    fn fits_when_free_space_covers_need_plus_reserve() {
        assert_eq!(check_fit(GIB, 4 * GIB, 0, Some(2 * GIB), 256 * MIB), Ok(()));
    }

    #[test]
    fn refuses_when_the_reserve_floor_does_not_fit() {
        // Need 100 MiB; 5% is 5 MiB, so the 256 MiB reserve is the margin.
        let err = check_fit(100 * MIB, GIB, 0, Some(300 * MIB), 256 * MIB).unwrap_err();
        assert_eq!(err.required, 356 * MIB);
        assert_eq!(err.available, 300 * MIB);
    }

    #[test]
    fn proportional_margin_wins_for_large_downloads() {
        // Need 20 GiB; 5% (1 GiB) exceeds the 256 MiB reserve.
        let err = check_fit(20 * GIB, u64::MAX, 0, Some(21 * GIB - 1), 256 * MIB).unwrap_err();
        assert_eq!(err.required, 21 * GIB);
        assert_eq!(
            check_fit(20 * GIB, u64::MAX, 0, Some(21 * GIB), 256 * MIB),
            Ok(())
        );
    }

    #[test]
    fn need_is_capped_by_the_store_budget() {
        // A 10 GiB file in a 128 MiB store never needs more than 128 MiB on disk.
        assert_eq!(
            check_fit(10 * GIB, 128 * MIB, 0, Some(384 * MIB), 256 * MIB),
            Ok(())
        );
        let err = check_fit(10 * GIB, 128 * MIB, 0, Some(383 * MIB), 256 * MIB).unwrap_err();
        assert_eq!(err.required, 384 * MIB);
    }

    #[test]
    fn already_cached_bytes_need_no_new_space() {
        // 900 MiB of a 1 GiB file is held: only 124 MiB plus the reserve is needed.
        assert_eq!(
            check_fit(GIB, 4 * GIB, 900 * MIB, Some(380 * MIB), 256 * MIB),
            Ok(())
        );
        // A fully held file starts even on a full disk.
        assert_eq!(check_fit(GIB, 4 * GIB, GIB, Some(0), 256 * MIB), Ok(()));
        assert_eq!(check_fit(GIB, 4 * GIB, 2 * GIB, Some(0), 256 * MIB), Ok(()));
    }

    #[test]
    fn unknown_free_space_allows_the_start() {
        assert_eq!(check_fit(u64::MAX, u64::MAX, 0, None, u64::MAX), Ok(()));
    }

    #[test]
    fn zero_reserve_keeps_the_proportional_margin() {
        assert!(check_fit(100 * MIB, GIB, 0, Some(104 * MIB), 0).is_err());
        assert_eq!(check_fit(100 * MIB, GIB, 0, Some(105 * MIB), 0), Ok(()));
    }

    #[test]
    fn saturates_instead_of_overflowing() {
        let err = check_fit(u64::MAX, u64::MAX, 0, Some(GIB), u64::MAX).unwrap_err();
        assert_eq!(err.required, u64::MAX);
    }

    #[test]
    fn message_reports_both_sizes_without_paths() {
        let err = NoSpace {
            required: 356 * MIB,
            available: 300 * MIB,
        };
        assert_eq!(
            err.message(),
            "not enough free disk space: 356 MiB required, 300 MiB available"
        );
        assert_eq!(
            NoSpace {
                required: 10,
                available: 0
            }
            .message(),
            "not enough free disk space: 10 bytes required, 0 bytes available"
        );
    }

    #[test]
    fn full_disk_and_quota_errors_are_no_space() {
        assert!(is_no_space(std::io::ErrorKind::StorageFull));
        assert!(is_no_space(std::io::ErrorKind::QuotaExceeded));
        assert!(!is_no_space(std::io::ErrorKind::PermissionDenied));
        assert!(!is_no_space(std::io::ErrorKind::Other));
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn queries_a_real_filesystem_through_a_missing_child() {
        let missing = std::env::temp_dir().join("outpace-disk-space-missing/child");
        assert!(available_bytes(&missing).is_some());
    }
}
