//! Bounded retained HLS media for explicitly requested, finite local replay (timeshift).
//!
//! This is local media history of segments this engine has already packaged: not
//! peer-network history and not a wall-clock (UTC) archive. Positions are accumulated
//! media duration on one packager's lifetime. A [`HlsHistorySnapshot`] holds coordinates
//! only, so a viewer's replay can never pin pruned media or exceed the configured budgets.

use crate::config::HlsHistoryConfig;
use bytes::Bytes;
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Hard cap on retained entries per source, independent of the byte and time limits.
pub(crate) const MAX_HISTORY_SEGMENTS: usize = 4096;
/// A segment longer than this is not a trustworthy seek coordinate.
const MAX_SEGMENT_SECONDS: f32 = 3600.0;

/// An immutable replay range: `first_seq` is an independently decodable entry and
/// `last_seq` is the completed head observed when the snapshot was prepared.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HlsHistorySnapshot {
    pub first_seq: u64,
    pub last_seq: u64,
    pub start_ms: u64,
    pub end_ms: u64,
}

/// One contiguous retained run of sequence numbers within a single timing epoch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HlsHistoryInterval {
    pub first_seq: u64,
    pub last_seq: u64,
    pub start_ms: u64,
    pub end_ms: u64,
    pub epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HlsHistoryStatus {
    pub enabled: bool,
    pub kind: &'static str,
    pub time_basis: &'static str,
    pub first_seq: Option<u64>,
    pub last_seq: Option<u64>,
    pub start_ms: Option<u64>,
    pub end_ms: Option<u64>,
    /// Earliest independent entry contiguous with the current retained head. `None` when
    /// only non-decodable continuations or a stale retained tail exist.
    pub seekable_start_ms: Option<u64>,
    pub retained_bytes: usize,
    pub max_bytes: usize,
    pub max_seconds: u64,
    pub intervals: Vec<HlsHistoryInterval>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HlsHistoryError {
    /// History retention is not configured.
    Disabled,
    /// No completed segment has been retained yet.
    NotReady,
    /// The requested position is not retained, not decodable, or not contiguous with live.
    Unavailable,
    /// A previously prepared view no longer matches retained media.
    Gone,
    /// The request itself is malformed.
    InvalidTarget,
}

impl std::fmt::Display for HlsHistoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Disabled => "history is disabled",
            Self::NotReady => "history is not ready",
            Self::Unavailable => "requested history is unavailable",
            Self::Gone => "history has expired",
            Self::InvalidTarget => "invalid history target",
        })
    }
}

impl std::error::Error for HlsHistoryError {}

/// Byte budget shared by every packager of one manager. It counts retained backing
/// allocations, conservatively charging an allocation shared with the live window once
/// per history entry.
#[derive(Clone, Debug)]
pub(crate) struct HistoryBudget {
    used: Arc<AtomicUsize>,
    max: usize,
}

impl HistoryBudget {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            used: Arc::new(AtomicUsize::new(0)),
            max,
        }
    }

    pub(crate) fn reserve(&self, bytes: usize) -> Option<Charge> {
        self.used
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|total| *total <= self.max)
            })
            .ok()?;
        Some(Charge {
            used: self.used.clone(),
            bytes,
        })
    }

    #[cfg(test)]
    pub(crate) fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
}

/// Returns its reservation to the shared budget when the retained entry is dropped.
#[derive(Debug)]
pub(crate) struct Charge {
    used: Arc<AtomicUsize>,
    bytes: usize,
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

struct Segment {
    seq: u64,
    bytes: Bytes,
    charge: Charge,
    duration: f32,
    start_ms: u64,
    end_ms: u64,
    epoch: u64,
    discontinuity: bool,
    independent: bool,
    created: Instant,
}

/// One completed segment offered to history by the packager.
pub(crate) struct HistoryEntry {
    pub(crate) seq: u64,
    pub(crate) bytes: Bytes,
    /// Backing allocation actually retained, at least `bytes.len()`.
    pub(crate) allocation: usize,
    pub(crate) duration: f32,
    pub(crate) discontinuity: bool,
    /// Starts at a video access point with fresh tables, so a replay may begin here.
    pub(crate) independent: bool,
}

pub(crate) struct HistoryStore {
    config: HlsHistoryConfig,
    budget: HistoryBudget,
    segments: VecDeque<Segment>,
    used: usize,
    /// Accumulated accepted media duration: the replay coordinate timeline.
    media_ms: u64,
    epoch: u64,
    /// Sequence number of the newest completed segment, retained or not.
    previous_seq: Option<u64>,
}

impl HistoryStore {
    pub(crate) fn new(config: HlsHistoryConfig, budget: HistoryBudget) -> Self {
        Self {
            config,
            budget,
            segments: VecDeque::new(),
            used: 0,
            media_ms: 0,
            epoch: 0,
            previous_seq: None,
        }
    }

    /// A store that never retains anything.
    pub(crate) fn disabled() -> Self {
        Self::new(HlsHistoryConfig::default(), HistoryBudget::new(0))
    }

    fn pop_front(&mut self) {
        if let Some(segment) = self.segments.pop_front() {
            self.used -= segment.charge.bytes;
        }
    }

    fn clear(&mut self) {
        while !self.segments.is_empty() {
            self.pop_front();
        }
    }

    fn prune(&mut self, now: Instant) {
        let age = Duration::from_secs(self.config.max_seconds);
        let media_span = self.config.max_seconds.saturating_mul(1000);
        while self.segments.front().is_some_and(|s| {
            now.saturating_duration_since(s.created) >= age
                || self.media_ms.saturating_sub(s.start_ms) > media_span
        }) {
            self.pop_front();
        }
    }

    pub(crate) fn push(&mut self, entry: HistoryEntry, now: Instant) {
        if !self.config.enabled() {
            return;
        }
        let HistoryEntry {
            seq,
            bytes,
            allocation,
            duration,
            discontinuity,
            independent,
        } = entry;
        // Unsafe or ambiguous timing must not become a seek coordinate.
        if !duration.is_finite() || duration <= 0.0 || duration > MAX_SEGMENT_SECONDS {
            self.clear();
            self.previous_seq = Some(seq);
            self.epoch = self.epoch.saturating_add(1);
            return;
        }
        let duration_ms = (f64::from(duration) * 1000.0).round().max(1.0) as u64;
        let Some(end_ms) = self.media_ms.checked_add(duration_ms) else {
            self.clear();
            self.previous_seq = Some(seq);
            return;
        };
        let start_ms = self.media_ms;
        self.media_ms = end_ms;
        if discontinuity
            || self
                .previous_seq
                .is_some_and(|previous| previous.checked_add(1) != Some(seq))
        {
            self.epoch = self.epoch.saturating_add(1);
        }
        self.previous_seq = Some(seq);
        self.prune(now);
        let allocation = allocation.max(bytes.len());
        if allocation > self.config.max_bytes {
            // Not retained: the old tail is no longer the live edge (see `current_head`).
            return;
        }
        while self.used > self.config.max_bytes - allocation
            || self.segments.len() >= MAX_HISTORY_SEGMENTS
        {
            self.pop_front();
        }
        // Reclaim this source's own oldest history before refusing growth under the
        // manager-wide cap.
        let charge = loop {
            if let Some(charge) = self.budget.reserve(allocation) {
                break Some(charge);
            }
            if self.segments.is_empty() {
                break None;
            }
            self.pop_front();
        };
        let Some(charge) = charge else {
            return;
        };
        self.used += allocation;
        self.segments.push_back(Segment {
            seq,
            bytes,
            charge,
            duration,
            start_ms,
            end_ms,
            epoch: self.epoch,
            discontinuity,
            independent,
            created: now,
        });
    }

    pub(crate) fn status(&mut self, now: Instant) -> HlsHistoryStatus {
        self.prune(now);
        let mut seekable_start_ms = None;
        let mut expected = self.previous_seq;
        for segment in self.segments.iter().rev() {
            if expected != Some(segment.seq) {
                break;
            }
            if segment.independent {
                seekable_start_ms = Some(segment.start_ms);
            }
            expected = segment.seq.checked_sub(1);
        }
        let mut intervals: Vec<HlsHistoryInterval> = Vec::new();
        for s in &self.segments {
            if let Some(last) = intervals
                .last_mut()
                .filter(|p| p.epoch == s.epoch && p.last_seq.checked_add(1) == Some(s.seq))
            {
                last.last_seq = s.seq;
                last.end_ms = s.end_ms;
            } else {
                intervals.push(HlsHistoryInterval {
                    first_seq: s.seq,
                    last_seq: s.seq,
                    start_ms: s.start_ms,
                    end_ms: s.end_ms,
                    epoch: s.epoch,
                });
            }
        }
        HlsHistoryStatus {
            enabled: self.config.enabled(),
            kind: "local_hls_replay",
            time_basis: "retained_media_duration",
            first_seq: self.segments.front().map(|s| s.seq),
            last_seq: self.segments.back().map(|s| s.seq),
            start_ms: self.segments.front().map(|s| s.start_ms),
            end_ms: self.segments.back().map(|s| s.end_ms),
            seekable_start_ms,
            retained_bytes: self.used,
            max_bytes: self.config.max_bytes,
            max_seconds: self.config.max_seconds,
            intervals,
        }
    }

    /// Prepare a replay starting at least `back` behind the completed retained head.
    pub(crate) fn prepare(
        &mut self,
        back: Duration,
        now: Instant,
    ) -> Result<HlsHistorySnapshot, HlsHistoryError> {
        if !self.config.enabled() {
            return Err(HlsHistoryError::Disabled);
        }
        self.prune(now);
        let back_ms: u64 = back
            .as_millis()
            .try_into()
            .map_err(|_| HlsHistoryError::InvalidTarget)?;
        if back_ms == 0 {
            return Err(HlsHistoryError::InvalidTarget);
        }
        let last = self.current_head()?;
        let target = last
            .end_ms
            .checked_sub(back_ms)
            .ok_or(HlsHistoryError::Unavailable)?;
        self.snapshot_at(target, last)
    }

    /// Prepare a replay from an absolute position on this packager's media-duration
    /// timeline. The target is not a wall-clock timestamp and does not move with live.
    pub(crate) fn prepare_at(
        &mut self,
        position_ms: u64,
        now: Instant,
    ) -> Result<HlsHistorySnapshot, HlsHistoryError> {
        if !self.config.enabled() {
            return Err(HlsHistoryError::Disabled);
        }
        self.prune(now);
        let last = self.current_head()?;
        // The completed head is an exclusive boundary: no media exists at or after it.
        if position_ms >= last.end_ms {
            return Err(HlsHistoryError::Unavailable);
        }
        self.snapshot_at(position_ms, last)
    }

    fn current_head(&self) -> Result<&Segment, HlsHistoryError> {
        let last = self.segments.back().ok_or(HlsHistoryError::NotReady)?;
        // If growth was refused, the old retained tail is not the live edge.
        if Some(last.seq) != self.previous_seq {
            return Err(HlsHistoryError::Unavailable);
        }
        Ok(last)
    }

    fn snapshot_at(
        &self,
        position_ms: u64,
        last: &Segment,
    ) -> Result<HlsHistorySnapshot, HlsHistoryError> {
        let first = self
            .segments
            .iter()
            .rev()
            .find(|s| s.start_ms <= position_ms && s.independent)
            .ok_or(HlsHistoryError::Unavailable)?;
        let snapshot = HlsHistorySnapshot {
            first_seq: first.seq,
            last_seq: last.seq,
            start_ms: first.start_ms,
            end_ms: last.end_ms,
        };
        // A hole between the access point and the head is not replayable.
        self.range(&snapshot)
            .map_err(|_| HlsHistoryError::Unavailable)?;
        Ok(snapshot)
    }

    /// Every entry of `view`, exactly as prepared, or `Gone`. Never a truncated range.
    fn range(&self, view: &HlsHistorySnapshot) -> Result<Vec<&Segment>, HlsHistoryError> {
        let count = view
            .last_seq
            .checked_sub(view.first_seq)
            .and_then(|n| n.checked_add(1))
            .ok_or(HlsHistoryError::Gone)?;
        if count > MAX_HISTORY_SEGMENTS as u64 {
            return Err(HlsHistoryError::Gone);
        }
        let selected: Vec<_> = self
            .segments
            .iter()
            .filter(|s| s.seq >= view.first_seq && s.seq <= view.last_seq)
            .collect();
        if selected.len() as u64 != count
            || selected
                .first()
                .is_none_or(|s| !s.independent || s.start_ms != view.start_ms)
            || selected.last().is_none_or(|s| s.end_ms != view.end_ms)
            || selected
                .iter()
                .enumerate()
                .any(|(i, s)| s.seq != view.first_seq + i as u64)
        {
            return Err(HlsHistoryError::Gone);
        }
        Ok(selected)
    }

    /// Render an immutable VOD playlist for `view`. It ends with `EXT-X-ENDLIST`, so a
    /// replay never silently grows into live playback.
    pub(crate) fn playlist(
        &mut self,
        view: &HlsHistorySnapshot,
        prefix: &str,
        now: Instant,
    ) -> Result<String, HlsHistoryError> {
        self.prune(now);
        let segments = self.range(view)?;
        let target = segments
            .iter()
            .map(|s| s.duration.ceil() as u64)
            .max()
            .unwrap_or(1)
            .max(1);
        let mut out = format!(
            "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXT-X-TARGETDURATION:{target}\n\
             #EXT-X-MEDIA-SEQUENCE:{}\n#EXT-X-START:TIME-OFFSET=0.000,PRECISE=YES\n",
            view.first_seq
        );
        for s in segments {
            if s.discontinuity {
                out.push_str("#EXT-X-DISCONTINUITY\n");
            }
            out.push_str(&format!(
                "#EXTINF:{:.3},\n{prefix}/{}.ts\n",
                s.duration, s.seq
            ));
        }
        out.push_str("#EXT-X-ENDLIST\n");
        Ok(out)
    }

    pub(crate) fn segment(
        &mut self,
        view: &HlsHistorySnapshot,
        seq: u64,
        now: Instant,
    ) -> Result<Bytes, HlsHistoryError> {
        self.prune(now);
        if seq < view.first_seq || seq > view.last_seq {
            return Err(HlsHistoryError::Gone);
        }
        // A player may already have consumed an aged-out prefix of this immutable VOD.
        // Only the requested fragment must still exist; requiring the whole old range
        // here would end long replays while their unread fragments remain retained.
        self.segments
            .iter()
            .find(|s| s.seq == seq && s.start_ms >= view.start_ms && s.end_ms <= view.end_ms)
            .map(|s| s.bytes.clone())
            .ok_or(HlsHistoryError::Gone)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> HlsHistoryConfig {
        HlsHistoryConfig {
            max_seconds: 60,
            max_bytes: 1024,
            global_max_bytes: 2048,
        }
    }

    fn entry(seq: u64, bytes: Bytes, allocation: usize, discontinuity: bool) -> HistoryEntry {
        HistoryEntry {
            seq,
            bytes,
            allocation,
            duration: 5.0,
            discontinuity,
            independent: true,
        }
    }

    fn push(store: &mut HistoryStore, seq: u64, independent: bool, now: Instant) {
        store.push(
            HistoryEntry {
                independent,
                ..entry(seq, Bytes::from(vec![seq as u8; 100]), 100, false)
            },
            now,
        );
    }

    #[test]
    fn finite_replay_starts_at_earlier_access_point_and_is_immutable() {
        let now = Instant::now();
        let mut store = HistoryStore::new(config(), HistoryBudget::new(2048));
        push(&mut store, 0, true, now);
        push(&mut store, 1, false, now);
        push(&mut store, 2, true, now);
        let view = store.prepare(Duration::from_secs(8), now).unwrap();
        assert_eq!(view.first_seq, 0);
        assert_eq!(view.last_seq, 2);
        let before = store.playlist(&view, "/synthetic", now).unwrap();
        push(&mut store, 3, true, now);
        assert_eq!(store.playlist(&view, "/synthetic", now).unwrap(), before);
        assert!(before.ends_with("#EXT-X-ENDLIST\n"));
        assert!(before.contains("#EXT-X-PLAYLIST-TYPE:VOD\n"));
        assert!(before.contains("#EXT-X-START:TIME-OFFSET=0.000"));
        assert!(before.contains("#EXT-X-MEDIA-SEQUENCE:0\n"));
        assert_eq!(store.segment(&view, 1, now).unwrap()[0], 1);
        assert_eq!(store.segment(&view, 3, now), Err(HlsHistoryError::Gone));
    }

    #[test]
    fn absolute_position_stays_stable_as_the_completed_head_advances() {
        let now = Instant::now();
        let mut store = HistoryStore::new(config(), HistoryBudget::new(2048));
        push(&mut store, 0, true, now);
        push(&mut store, 1, false, now);
        push(&mut store, 2, true, now);
        let view = store.prepare_at(7500, now).unwrap();
        assert_eq!(
            (view.first_seq, view.last_seq, view.start_ms, view.end_ms),
            (0, 2, 0, 15000)
        );
        let playlist = store.playlist(&view, "/synthetic", now).unwrap();

        push(&mut store, 3, true, now);
        let later = store.prepare_at(7500, now).unwrap();
        assert_eq!(
            (
                later.first_seq,
                later.last_seq,
                later.start_ms,
                later.end_ms
            ),
            (0, 3, 0, 20000)
        );
        assert_eq!(
            store
                .prepare(Duration::from_millis(7500), now)
                .unwrap()
                .first_seq,
            2
        );
        assert_eq!(store.playlist(&view, "/synthetic", now).unwrap(), playlist);
        assert_eq!(store.prepare_at(0, now).unwrap().first_seq, 0);
        assert_eq!(store.prepare_at(19_999, now).unwrap().first_seq, 3);
        for position in [20_000, 20_001, u64::MAX] {
            assert_eq!(
                store.prepare_at(position, now),
                Err(HlsHistoryError::Unavailable)
            );
        }
    }

    #[test]
    fn absolute_position_rejects_expiry_holes_and_unretained_head() {
        let now = Instant::now();
        let mut store = HistoryStore::new(config(), HistoryBudget::new(2048));
        assert_eq!(store.prepare_at(0, now), Err(HlsHistoryError::NotReady));
        push(&mut store, 0, true, now);
        push(&mut store, 2, true, now);
        assert_eq!(
            store.prepare_at(1000, now),
            Err(HlsHistoryError::Unavailable)
        );
        let view = store.prepare_at(5000, now).unwrap();
        assert_eq!(view.first_seq, 2);
        store.push(entry(3, Bytes::from_static(b"tiny"), 1025, false), now);
        assert_eq!(
            store.prepare_at(5000, now),
            Err(HlsHistoryError::Unavailable)
        );
        assert!(store.playlist(&view, "/synthetic", now).is_ok());
        assert_eq!(
            store.prepare_at(5000, now + Duration::from_secs(61)),
            Err(HlsHistoryError::NotReady)
        );

        let mut disabled = HistoryStore::disabled();
        assert_eq!(disabled.prepare_at(0, now), Err(HlsHistoryError::Disabled));
    }

    #[test]
    fn holes_unavailable_and_expired_views_are_never_truncated() {
        let now = Instant::now();
        let mut store = HistoryStore::new(config(), HistoryBudget::new(2048));
        push(&mut store, 0, true, now);
        push(&mut store, 2, true, now);
        assert_eq!(
            store.prepare(Duration::from_secs(8), now),
            Err(HlsHistoryError::Unavailable)
        );
        let view = store.prepare(Duration::from_secs(3), now).unwrap();
        assert_eq!(
            store.playlist(&view, "/x", now + Duration::from_secs(61)),
            Err(HlsHistoryError::Gone)
        );
    }

    #[test]
    fn consumed_prefix_can_expire_without_invalidating_unread_replay_segments() {
        let now = Instant::now();
        let mut store = HistoryStore::new(config(), HistoryBudget::new(2048));
        push(&mut store, 0, true, now);
        push(&mut store, 1, true, now + Duration::from_secs(5));
        push(&mut store, 2, true, now + Duration::from_secs(10));
        let view = store
            .prepare(Duration::from_secs(12), now + Duration::from_secs(10))
            .unwrap();
        let later = now + Duration::from_secs(61);
        assert_eq!(store.segment(&view, 0, later), Err(HlsHistoryError::Gone));
        assert_eq!(store.segment(&view, 1, later).unwrap()[0], 1);
        assert_eq!(store.segment(&view, 2, later).unwrap()[0], 2);
        assert_eq!(
            store.playlist(&view, "/x", later),
            Err(HlsHistoryError::Gone)
        );
        assert_eq!(store.segment(&view, 3, later), Err(HlsHistoryError::Gone));
    }

    #[test]
    fn global_budget_bounds_multiple_sources_and_is_released_on_drop() {
        let now = Instant::now();
        let budget = HistoryBudget::new(300);
        let mut a = HistoryStore::new(config(), budget.clone());
        let mut b = HistoryStore::new(config(), budget.clone());
        push(&mut a, 0, true, now);
        push(&mut a, 1, true, now);
        push(&mut b, 0, true, now);
        push(&mut b, 1, true, now);
        assert_eq!(budget.used(), 300);
        assert_eq!(b.status(now).first_seq, Some(1));
        drop(a);
        assert_eq!(budget.used(), 100);
        drop(b);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn per_source_budget_and_segment_cap_evict_oldest_first() {
        let now = Instant::now();
        let mut store = HistoryStore::new(config(), HistoryBudget::new(2048));
        for seq in 0..20 {
            push(&mut store, seq, true, now);
        }
        let status = store.status(now);
        assert_eq!(status.retained_bytes, 1000);
        assert_eq!(status.first_seq, Some(10));
        assert_eq!(status.last_seq, Some(19));
    }

    #[test]
    fn backing_capacity_is_charged_and_oversized_tail_is_not_called_live() {
        let now = Instant::now();
        let budget = HistoryBudget::new(2048);
        let mut store = HistoryStore::new(config(), budget.clone());
        store.push(entry(0, Bytes::from_static(b"tiny"), 900, false), now);
        assert_eq!(store.status(now).retained_bytes, 900);
        assert_eq!(budget.used(), 900);
        store.push(entry(1, Bytes::from_static(b"tiny"), 1025, false), now);
        assert_eq!(
            store.prepare(Duration::from_secs(1), now),
            Err(HlsHistoryError::Unavailable)
        );
        assert_eq!(store.status(now).seekable_start_ms, None);
    }

    #[test]
    fn duration_bound_discontinuity_and_disabled_configuration() {
        let now = Instant::now();
        let mut cfg = config();
        cfg.max_seconds = 10;
        let mut store = HistoryStore::new(cfg, HistoryBudget::new(2048));
        push(&mut store, 0, true, now);
        push(&mut store, 1, true, now);
        store.push(entry(2, Bytes::from(vec![2; 100]), 100, true), now);
        let state = store.status(now);
        assert_eq!(state.first_seq, Some(1));
        assert_eq!(state.intervals.len(), 2);
        let view = store.prepare(Duration::from_secs(9), now).unwrap();
        assert!(store
            .playlist(&view, "/x", now)
            .unwrap()
            .contains("#EXT-X-DISCONTINUITY"));
        let mut disabled = HistoryStore::new(HlsHistoryConfig::default(), HistoryBudget::new(2048));
        push(&mut disabled, 0, true, now);
        assert_eq!(disabled.status(now).retained_bytes, 0);
        assert!(!disabled.status(now).enabled);
        assert_eq!(
            disabled.prepare(Duration::from_secs(1), now),
            Err(HlsHistoryError::Disabled)
        );
        cfg.max_seconds = 3601;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn untrustworthy_timing_drops_history_and_starts_a_new_epoch() {
        let now = Instant::now();
        let mut store = HistoryStore::new(config(), HistoryBudget::new(2048));
        push(&mut store, 0, true, now);
        store.push(
            HistoryEntry {
                duration: f32::NAN,
                ..entry(1, Bytes::from_static(b"x"), 1, false)
            },
            now,
        );
        assert_eq!(store.status(now).retained_bytes, 0);
        push(&mut store, 2, true, now);
        assert_eq!(store.status(now).intervals[0].epoch, 1);
        assert!(store.prepare(Duration::from_secs(1), now).is_ok());
    }

    #[test]
    fn zero_back_is_an_invalid_target() {
        let now = Instant::now();
        let mut store = HistoryStore::new(config(), HistoryBudget::new(2048));
        push(&mut store, 0, true, now);
        assert_eq!(
            store.prepare(Duration::ZERO, now),
            Err(HlsHistoryError::InvalidTarget)
        );
    }
}
