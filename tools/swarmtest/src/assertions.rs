//! Pure, unit-tested verdict logic for interop scenarios.
//!
//! Everything here is deterministic and network-free: it consumes samples the
//! [`crate::peers`] drivers collect (per-peer stats series, per-second playback byte
//! buckets, an MPEG-TS head buffer) and reduces them to pass/fail verdicts. The
//! docker glue in [`crate::scenario`] does the sampling; the judgement lives here so
//! it can be tested exhaustively against synthetic series.

use serde::Serialize;

/// One normalized stats sample for a swarm peer, plus the raw stat JSON it came from.
///
/// Different sources populate different fields (an engine `/ace/stat` gives
/// `downloaded`; an outpace `/status` gives `uploaded`/`peers_served`). The driver
/// fills what it knows and carries the raw JSON so a human can audit exact fields.
#[derive(Debug, Clone, Default, Serialize)]
pub struct PeerStats {
    /// Coarse activity status. `"dl"` means actively downloading (the healthy state).
    pub status: String,
    /// Number of swarm peers this node currently sees.
    pub peers: u64,
    /// Cumulative payload bytes downloaded (monotonic while healthy).
    pub downloaded: u64,
    /// Cumulative payload bytes uploaded to other peers.
    pub uploaded: u64,
    /// Expected stat fields that were ABSENT from the raw payload (as opposed to present
    /// with value 0). Surfaced in `report.json` so a first-run field-name mismatch is
    /// unambiguous instead of silently read as 0. Verdict logic ignores it.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub missing_fields: Vec<String>,
    /// The full stat JSON the sample was derived from (for human audit).
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub raw: serde_json::Value,
}

impl PeerStats {
    /// Construct a sample with the four normalized fields, no missing fields, and a null
    /// raw payload.
    pub fn new(status: &str, peers: u64, downloaded: u64, uploaded: u64) -> Self {
        Self {
            status: status.to_string(),
            peers,
            downloaded,
            uploaded,
            missing_fields: Vec::new(),
            raw: serde_json::Value::Null,
        }
    }
}

/// Fraction of post-warmup samples that must report `status == "dl"`.
pub const MIN_DL_RATIO: f64 = 0.9;
/// A playback second delivering fewer than this many bytes counts as stalled.
pub const STALL_BYTES_PER_SEC: u64 = 188;
/// The longest run of stalled seconds tolerated before a stream fails.
pub const MAX_STALL_SECS: usize = 5;
/// Fraction of the expected byte total a stream must deliver to be stable.
pub const MIN_THROUGHPUT_RATIO: f64 = 0.8;
/// MPEG-TS packet size; sync byte `0x47` must appear at every multiple of it.
pub const TS_PACKET_LEN: usize = 188;
/// Consecutive aligned packets required to accept a capture as genuine MPEG-TS.
pub const MIN_TS_PACKETS: usize = 4;

/// Swarm-health verdict for one peer, with the component checks exposed for reporting.
#[derive(Debug, Clone, Serialize)]
pub struct HealthVerdict {
    /// Fraction of post-warmup samples with `status == "dl"`.
    pub dl_ratio: f64,
    /// Whether [`Self::dl_ratio`] meets [`MIN_DL_RATIO`].
    pub status_ok: bool,
    /// Whether every post-warmup sample saw at least one peer.
    pub peers_ok: bool,
    /// Whether `downloaded` is non-decreasing and strictly higher at the end.
    pub downloaded_rising: bool,
    /// Whether any post-warmup sample reported a non-zero upload.
    pub upload_positive: bool,
    /// Number of post-warmup samples considered.
    pub samples: usize,
    /// Overall pass = all component checks pass over a non-empty post-warmup window.
    pub passed: bool,
}

/// Reduce a stats series to a swarm-health verdict, ignoring the first `warmup` samples.
///
/// Healthy (for a consumer) = actively downloading in ≥[`MIN_DL_RATIO`] of samples,
/// always seeing a peer, and a strictly rising download counter. `upload_positive` is
/// computed and reported but does NOT gate the verdict: in a swarm whose source
/// satisfies everyone, a consumer legitimately never gets pulled from, so per-peer
/// upload proves nothing about the peer's ability to seed. Seeding is asserted at the
/// swarm level instead (see [`swarm_reciprocates`]) and, for outpace specifically, by
/// the outpace-source scenario where real engine peers download from it. An empty
/// post-warmup window fails.
pub fn swarm_health(samples: &[PeerStats], warmup: usize) -> HealthVerdict {
    let post = samples.get(warmup.min(samples.len())..).unwrap_or(&[]);
    if post.is_empty() {
        return HealthVerdict {
            dl_ratio: 0.0,
            status_ok: false,
            peers_ok: false,
            downloaded_rising: false,
            upload_positive: false,
            samples: 0,
            passed: false,
        };
    }

    let dl_count = post.iter().filter(|s| s.status == "dl").count();
    let dl_ratio = dl_count as f64 / post.len() as f64;
    let status_ok = dl_ratio >= MIN_DL_RATIO;
    let peers_ok = post.iter().all(|s| s.peers >= 1);
    let downloaded_rising = is_rising(post.iter().map(|s| s.downloaded));
    let upload_positive = post.iter().any(|s| s.uploaded > 0);

    // upload_positive is reported but intentionally not part of `passed` — seeding is a
    // swarm-level property (see the module note on `swarm_health`).
    let passed = status_ok && peers_ok && downloaded_rising;
    HealthVerdict {
        dl_ratio,
        status_ok,
        peers_ok,
        downloaded_rising,
        upload_positive,
        samples: post.len(),
        passed,
    }
}

/// Swarm-level reciprocation: at least one peer served upload during the window, proving
/// the swarm does real peer-to-peer relaying rather than pure source→leech fan-out.
///
/// This is the per-role seeding assertion for consumers: individual consumers are not
/// required to upload (a source-satisfied swarm may never pull from them), but the swarm
/// as a whole must show reciprocation.
pub fn swarm_reciprocates(verdicts: &[HealthVerdict]) -> bool {
    verdicts.iter().any(|v| v.upload_positive)
}

/// True iff the sequence is non-decreasing and ends strictly above where it started.
///
/// This is the operative meaning of "download counter is rising": a stalled series
/// (flat, so net-zero) fails, while a series that climbs — even with brief plateaus —
/// passes.
fn is_rising(values: impl Iterator<Item = u64>) -> bool {
    let mut first = None;
    let mut prev = 0u64;
    let mut last = 0u64;
    let mut count = 0usize;
    for v in values {
        if first.is_none() {
            first = Some(v);
        } else if v < prev {
            return false; // a decrease is never "rising"
        }
        prev = v;
        last = v;
        count += 1;
    }
    match first {
        Some(f) if count >= 2 => last > f,
        _ => false,
    }
}

/// Stream-stability verdict from a per-second playback byte-bucket series.
#[derive(Debug, Clone, Serialize)]
pub struct StabilityVerdict {
    /// Total bytes delivered across the window.
    pub total_bytes: u64,
    /// Bytes the window was expected to deliver (`expected_bps × seconds`).
    pub expected_bytes: u64,
    /// Whether delivered ≥ [`MIN_THROUGHPUT_RATIO`] of expected.
    pub throughput_ok: bool,
    /// Longest run of consecutive stalled seconds observed.
    pub max_stall_secs: usize,
    /// Whether [`Self::max_stall_secs`] stays within [`MAX_STALL_SECS`].
    pub stall_ok: bool,
    /// Number of one-second buckets considered.
    pub seconds: usize,
    /// Overall pass = adequate throughput and no over-long stall.
    pub passed: bool,
}

/// Judge a per-second byte-bucket series against an expected byte rate.
///
/// `buckets[i]` is the payload bytes delivered during playback second `i`;
/// `expected_bps` is the nominal stream rate in bytes per second. A stream is stable
/// when it delivers at least [`MIN_THROUGHPUT_RATIO`] of the expected total AND never
/// stalls (a second under [`STALL_BYTES_PER_SEC`]) for more than [`MAX_STALL_SECS`] in
/// a row. An empty series fails.
pub fn stream_stability(buckets: &[u64], expected_bps: u64) -> StabilityVerdict {
    let seconds = buckets.len();
    let total_bytes: u64 = buckets.iter().sum();
    let expected_bytes = expected_bps.saturating_mul(seconds as u64);

    let throughput_ok =
        seconds > 0 && (total_bytes as f64) >= MIN_THROUGHPUT_RATIO * (expected_bytes as f64);

    let mut run = 0usize;
    let mut max_stall = 0usize;
    for &b in buckets {
        if b < STALL_BYTES_PER_SEC {
            run += 1;
            max_stall = max_stall.max(run);
        } else {
            run = 0;
        }
    }
    let stall_ok = seconds > 0 && max_stall <= MAX_STALL_SECS;

    StabilityVerdict {
        total_bytes,
        expected_bytes,
        throughput_ok,
        max_stall_secs: max_stall,
        stall_ok,
        seconds,
        passed: throughput_ok && stall_ok,
    }
}

/// Check MPEG-TS packet alignment with resync: there exists a start offset in
/// `[0, TS_PACKET_LEN)` from which the sync byte `0x47` lands on every
/// [`TS_PACKET_LEN`]-byte boundary through the rest of `buf`.
///
/// A live HTTP playback stream is joined mid-packet, so the capture's first byte is
/// rarely a sync byte; requiring alignment at offset 0 would spuriously fail. Instead we
/// look for any packet phase that stays aligned, requiring at least [`MIN_TS_PACKETS`]
/// whole packets from the start so a lone stray `0x47` cannot pass. A corrupt or
/// unaligned stream (no phase stays aligned) fails.
pub fn ts_contiguity(buf: &[u8]) -> bool {
    // Need enough bytes that some phase can contain MIN_TS_PACKETS packets.
    if buf.len() < TS_PACKET_LEN * MIN_TS_PACKETS {
        return false;
    }
    // For each packet phase, find the longest run of consecutive aligned sync bytes.
    // A run of MIN_TS_PACKETS proves genuine TS framing while tolerating a leading
    // partial packet or a reconnect splice elsewhere in the capture.
    for start in 0..TS_PACKET_LEN {
        let mut run = 0usize;
        let mut offset = start;
        while offset < buf.len() {
            if buf[offset] == 0x47 {
                run += 1;
                if run >= MIN_TS_PACKETS {
                    return true;
                }
            } else {
                run = 0;
            }
            offset += TS_PACKET_LEN;
        }
    }
    false
}

/// Build a synthetic, sync-aligned MPEG-TS buffer of `packets` packets (test helper).
#[cfg(test)]
pub(crate) fn synthetic_ts(packets: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(packets * TS_PACKET_LEN);
    for i in 0..packets {
        out.push(0x47);
        out.extend(std::iter::repeat_n((i % 251) as u8, TS_PACKET_LEN - 1));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rising_series() -> Vec<PeerStats> {
        (0..10)
            .map(|i| PeerStats::new("dl", 2, 1000 * (i + 1), 500 * i))
            .collect()
    }

    #[test]
    fn healthy_series_passes() {
        let v = swarm_health(&rising_series(), 3);
        assert!(v.passed, "{v:?}");
        assert!(v.status_ok && v.peers_ok && v.downloaded_rising && v.upload_positive);
        assert_eq!(v.samples, 7);
    }

    #[test]
    fn stalled_download_fails_health() {
        // Downloaded counter flat after warmup -> not rising.
        let mut s = rising_series();
        for sample in s.iter_mut().skip(3) {
            sample.downloaded = 4000;
        }
        let v = swarm_health(&s, 3);
        assert!(!v.downloaded_rising);
        assert!(!v.passed);
    }

    #[test]
    fn download_only_consumer_passes_health_upload_reported() {
        // A consumer that downloads a stable stream but is never pulled from passes
        // health (per-role: seeding is judged at the swarm level), while upload_positive
        // is still reported so the absence of reciprocation is visible.
        let s: Vec<_> = (0..8)
            .map(|i| PeerStats::new("dl", 1, 1000 * (i + 1), 0))
            .collect();
        let v = swarm_health(&s, 2);
        assert!(!v.upload_positive);
        assert!(v.passed);
    }

    #[test]
    fn swarm_reciprocation_requires_one_uploader() {
        let mk = |up: u64| {
            swarm_health(
                &(0..6)
                    .map(|i| PeerStats::new("dl", 1, 1000 * (i + 1), up * (i + 1)))
                    .collect::<Vec<_>>(),
                2,
            )
        };
        let leech = mk(0);
        let seeder = mk(500);
        assert!(!swarm_reciprocates(&[leech.clone(), leech.clone()]));
        assert!(swarm_reciprocates(&[leech, seeder]));
    }

    #[test]
    fn idle_status_below_ratio_fails_health() {
        let mut s = rising_series();
        // Half the post-warmup window is "idle" -> ratio 0.5 < 0.9.
        for sample in s.iter_mut().skip(3).step_by(2) {
            sample.status = "idle".to_string();
        }
        let v = swarm_health(&s, 3);
        assert!(!v.status_ok);
        assert!(!v.passed);
    }

    #[test]
    fn zero_peers_fails_health() {
        let s: Vec<_> = (0..6)
            .map(|i| PeerStats::new("dl", 0, 1000 * (i + 1), 100))
            .collect();
        let v = swarm_health(&s, 2);
        assert!(!v.peers_ok);
        assert!(!v.passed);
    }

    #[test]
    fn empty_post_warmup_window_fails() {
        let v = swarm_health(&rising_series(), 100);
        assert_eq!(v.samples, 0);
        assert!(!v.passed);
    }

    #[test]
    fn stable_stream_passes() {
        // 20s at ~175 KB/s, expecting 175_000 bps.
        let buckets = vec![175_000u64; 20];
        let v = stream_stability(&buckets, 175_000);
        assert!(v.throughput_ok && v.stall_ok && v.passed, "{v:?}");
        assert_eq!(v.max_stall_secs, 0);
    }

    #[test]
    fn long_stall_fails_stability() {
        // 6 consecutive stalled seconds > MAX_STALL_SECS even though total is fine.
        let mut buckets = vec![300_000u64; 14];
        for b in buckets.iter_mut().take(6) {
            *b = 0;
        }
        let v = stream_stability(&buckets, 175_000);
        assert_eq!(v.max_stall_secs, 6);
        assert!(!v.stall_ok);
        assert!(!v.passed);
    }

    #[test]
    fn short_stall_within_budget_passes() {
        let mut buckets = vec![300_000u64; 20];
        for b in buckets.iter_mut().skip(4).take(5) {
            *b = 0; // exactly MAX_STALL_SECS stalled seconds
        }
        let v = stream_stability(&buckets, 175_000);
        assert_eq!(v.max_stall_secs, MAX_STALL_SECS);
        assert!(v.stall_ok);
        assert!(v.passed);
    }

    #[test]
    fn low_throughput_fails_stability() {
        let buckets = vec![50_000u64; 20]; // well under 0.8 * expected
        let v = stream_stability(&buckets, 175_000);
        assert!(!v.throughput_ok);
        assert!(!v.passed);
    }

    #[test]
    fn empty_bucket_series_fails() {
        let v = stream_stability(&[], 175_000);
        assert!(!v.passed);
    }

    #[test]
    fn aligned_ts_buffer_is_contiguous() {
        assert!(ts_contiguity(&synthetic_ts(10)));
    }

    #[test]
    fn corrupt_ts_buffer_fails_contiguity() {
        // Clobber every other packet's sync byte so no phase ever reaches a run of
        // MIN_TS_PACKETS consecutive aligned packets.
        let mut buf = synthetic_ts(12);
        for p in (0..12).step_by(2) {
            buf[TS_PACKET_LEN * p] = 0x00;
        }
        assert!(!ts_contiguity(&buf));
    }

    #[test]
    fn non_ts_buffer_fails_contiguity() {
        assert!(!ts_contiguity(&[0u8; TS_PACKET_LEN * 6]));
    }

    #[test]
    fn mid_packet_start_resyncs() {
        // A live capture joined mid-packet: 53 junk bytes then aligned TS. Offset 0 is
        // not a sync byte, but the resync must still find the aligned phase.
        let mut buf = vec![0xABu8; 53];
        buf.extend(synthetic_ts(10));
        assert_ne!(buf[0], 0x47);
        assert!(ts_contiguity(&buf));
    }

    #[test]
    fn short_buffer_is_not_contiguous() {
        assert!(!ts_contiguity(&[0x47; 100]));
    }

    #[test]
    fn trailing_garbage_after_aligned_run_still_passes() {
        // A reconnect splice can append non-aligned bytes; a clean leading run of
        // MIN_TS_PACKETS is still accepted as genuine TS.
        let mut buf = synthetic_ts(6);
        buf.extend(std::iter::repeat_n(0xABu8, 200));
        assert!(ts_contiguity(&buf));
    }
}
