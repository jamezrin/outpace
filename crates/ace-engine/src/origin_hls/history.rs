//! Explicit recording of completed, independent origin TS segments. No network I/O.
use super::*;
use crate::config::HlsHistoryConfig;
use crate::history::{
    Charge, HistoryBudget, HlsHistoryError, HlsHistoryInterval, HlsHistorySnapshot,
    HlsHistoryStatus,
};
use ace_media::mpegts::{self, VideoAccessPointState, TS_PACKET_LEN};
use serde::Serialize;
use std::collections::BTreeMap;

const MAX_RECORDS: usize = 4096;

#[derive(Clone, Copy, Debug)]
pub enum OriginSeekTarget {
    Back(Duration),
    PositionMs(u64),
}

#[derive(Clone, Debug, Serialize)]
pub struct OriginHistoryView {
    pub generation: u64,
    pub mode: &'static str,
    pub snapshot: Option<HlsHistorySnapshot>,
    pub playback_url: String,
}

#[derive(Serialize)]
pub struct OriginHistoryStatus {
    pub enabled: bool,
    pub supported: bool,
    pub reason: Option<&'static str>,
    pub kind: &'static str,
    pub time_basis: &'static str,
    pub persistent: bool,
    pub utc_seek: bool,
    pub peer_archive: bool,
    pub view: OriginHistoryView,
    pub history: HlsHistoryStatus,
}

pub(super) struct Retained {
    pub(super) bytes: Bytes,
    allocation: usize,
    _charge: Charge,
}
struct ReplayBytes(Arc<Retained>);
impl AsRef<[u8]> for ReplayBytes {
    fn as_ref(&self) -> &[u8] {
        &self.0.bytes
    }
}
/// Request-time identity, independent of cache freshness and seek/live generation.
#[derive(Clone, Copy)]
pub(super) struct CaptureTicket {
    incarnation: u64,
    sequence: u64,
    id: u64,
    max_bytes: usize,
}
pub(super) struct ValidatedCapture {
    ticket: CaptureTicket,
    bytes: Bytes,
    allocation: usize,
}
impl CaptureTicket {
    /// Linear validation runs without the session mutex held.
    pub(super) fn validate(
        self,
        bytes: &Bytes,
        allocation: usize,
        cancelled: impl FnMut() -> bool,
    ) -> Option<ValidatedCapture> {
        let allocation = allocation.max(bytes.len());
        if allocation > self.max_bytes || !independent_ts_until(bytes, cancelled) {
            return None;
        }
        Some(ValidatedCapture {
            ticket: self,
            bytes: bytes.clone(),
            allocation,
        })
    }
}
struct Record {
    id: u64,
    duration: Duration,
    discontinuity: u64,
    start_ms: u64,
    end_ms: u64,
    created: Instant,
    body: Option<Arc<Retained>>,
}

pub(super) struct OriginHistory {
    config: HlsHistoryConfig,
    budget: HistoryBudget,
    records: BTreeMap<u64, Record>,
    current: HashMap<u64, u64>,
    used: usize,
    epoch: Option<u64>,
    tail: Option<(u64, u64)>,
    window_start: Option<(u64, u64)>,
    retired_through: Option<u64>,
    generation: u64,
    incarnation: u64,
    snapshot: Option<HlsHistorySnapshot>,
    reason: Option<&'static str>,
}

impl OriginHistory {
    pub(super) fn new(config: HlsHistoryConfig, budget: HistoryBudget) -> Self {
        Self {
            config,
            budget,
            records: BTreeMap::new(),
            current: HashMap::new(),
            used: 0,
            epoch: None,
            tail: None,
            window_start: None,
            retired_through: None,
            generation: 0,
            incarnation: 0,
            snapshot: None,
            reason: Some("manifest_not_observed"),
        }
    }
    fn remove_first(&mut self) {
        if let Some((sequence, record)) = self.records.pop_first() {
            self.used -= record.body.map_or(0, |body| body.allocation);
            self.retired_through = Some(
                self.retired_through
                    .map_or(sequence, |old| old.max(sequence)),
            );
        }
    }
    pub(super) fn clear(&mut self) {
        // Equal descriptors after reset must never revive old tickets.
        self.incarnation = self.incarnation.saturating_add(1);
        self.records.clear();
        self.current.clear();
        self.used = 0;
        self.tail = None;
        self.window_start = None;
        self.retired_through = None;
        self.snapshot = None;
    }
    fn reset(&mut self) {
        self.clear();
        // Exhaustion permanently makes further view transitions unavailable.
        self.generation = self.generation.saturating_add(1);
    }
    pub(super) fn prune(&mut self, now: Instant) {
        let age = Duration::from_secs(self.config.max_seconds);
        let span = self.config.max_seconds.saturating_mul(1000);
        while self.records.first_key_value().is_some_and(|(_, record)| {
            now.saturating_duration_since(record.created) >= age
                || self
                    .tail
                    .is_some_and(|(_, end)| end.saturating_sub(record.start_ms) > span)
                || self.records.len() > MAX_RECORDS
        }) {
            self.remove_first();
        }
    }
    /// The caller commits this only after a complete guarded manifest rewrite succeeds.
    pub(super) fn observe(
        &mut self,
        epoch: u64,
        supported: bool,
        segments: &[(u64, playlist::MediaSegment)],
        now: Instant,
    ) {
        if self.config.max_seconds == 0 {
            return;
        }
        let unique: HashMap<_, _> = segments
            .iter()
            .map(|(id, segment)| (*id, segment.sequence))
            .collect();
        let supported = supported
            && unique.len() == segments.len()
            && segments.iter().all(|(_, segment)| {
                !segment.duration.is_zero()
                    && segment.duration <= Duration::from_secs(3600)
                    && segment.duration.as_millis() > 0
            });
        if !supported {
            if self.reason.is_none() {
                self.reset();
            }
            self.reason = Some("requires_single_independent_ts_media_playlist");
            self.current.clear();
            return;
        }
        let reset = self.epoch.is_some_and(|old| old != epoch)
            || segments.iter().any(|(id, segment)| {
                self.records.get(&segment.sequence).is_some_and(|old| {
                    old.id != *id
                        || old.duration != segment.duration
                        || old.discontinuity != segment.discontinuity_sequence
                })
            })
            // A mutable origin URL reused at another sequence cannot identify
            // the bytes of an already admitted request. Start a fresh timeline.
            || self.records.iter().any(|(sequence, record)| {
                unique.get(&record.id).is_some_and(|new_sequence| new_sequence != sequence)
            });
        if reset {
            self.reset();
        }
        self.epoch = Some(epoch);
        self.reason = None;
        let Some((_, first)) = segments.first() else {
            self.current.clear();
            self.prune(now);
            return;
        };
        let mut preceding = 0_u64;
        let mut anchor = None;
        for (_, segment) in segments {
            if let Some(record) = self.records.get(&segment.sequence) {
                anchor = record.start_ms.checked_sub(preceding);
                break;
            }
            preceding = preceding.saturating_add(segment.duration.as_millis() as u64);
        }
        let start = if let Some(start) = anchor.or_else(|| {
            self.window_start
                .filter(|(seq, _)| *seq == first.sequence)
                .map(|(_, start)| start)
        }) {
            start
        } else if let Some((_, end)) = self
            .tail
            .filter(|(last, _)| last.checked_add(1) == Some(first.sequence))
        {
            end
        } else {
            if self.tail.is_some() {
                self.reset();
            }
            0
        };
        let mut position = start;
        self.window_start = Some((first.sequence, start));
        self.current.clear();
        for (id, segment) in segments {
            let Ok(duration_ms) = u64::try_from(segment.duration.as_millis()) else {
                self.reset();
                return;
            };
            let Some(end_ms) = position.checked_add(duration_ms) else {
                self.reset();
                return;
            };
            if self
                .retired_through
                .is_some_and(|retired| segment.sequence <= retired)
            {
                position = end_ms;
                continue;
            }
            self.records
                .entry(segment.sequence)
                .or_insert_with(|| Record {
                    id: *id,
                    duration: segment.duration,
                    discontinuity: segment.discontinuity_sequence,
                    start_ms: position,
                    end_ms,
                    created: now,
                    body: None,
                });
            self.current.insert(*id, segment.sequence);
            position = end_ms;
        }
        self.tail = segments
            .last()
            .map(|(_, segment)| (segment.sequence, position));
        self.prune(now);
    }
    pub(super) fn capture_ticket(&mut self, id: u64, now: Instant) -> Option<CaptureTicket> {
        self.prune(now);
        if self.config.max_seconds == 0 || self.reason.is_some() || self.incarnation == u64::MAX {
            return None;
        }
        let sequence = *self.current.get(&id)?;
        let record = self.records.get(&sequence)?;
        if record.body.is_some() {
            return None;
        }
        Some(CaptureTicket {
            incarnation: self.incarnation,
            sequence,
            id,
            max_bytes: self.config.max_bytes,
        })
    }
    pub(super) fn capture_validated(&mut self, capture: ValidatedCapture, now: Instant) {
        self.prune(now);
        let ValidatedCapture {
            ticket,
            bytes,
            allocation,
        } = capture;
        let seq = ticket.sequence;
        if self.config.max_seconds == 0
            || self.reason.is_some()
            || self.incarnation != ticket.incarnation
            || self
                .records
                .get(&seq)
                .is_none_or(|record| record.id != ticket.id || record.body.is_some())
        {
            return;
        }
        while self.used > self.config.max_bytes - allocation {
            self.remove_first();
        }
        // Never discard this segment's descriptor merely to record it later as another sequence.
        if !self.records.contains_key(&seq) {
            return;
        }
        let charge = loop {
            if let Some(charge) = self.budget.reserve(allocation) {
                break charge;
            }
            if self
                .records
                .first_key_value()
                .is_none_or(|(old, _)| *old >= seq)
            {
                return;
            }
            self.remove_first();
        };
        let Some(record) = self.records.get_mut(&seq) else {
            return;
        };
        record.body = Some(Arc::new(Retained {
            bytes,
            allocation,
            _charge: charge,
        }));
        self.used += allocation;
    }
    #[cfg(test)]
    fn capture(&mut self, id: u64, bytes: &Bytes, allocation: usize, now: Instant) {
        if let Some(capture) = self
            .capture_ticket(id, now)
            .and_then(|ticket| ticket.validate(bytes, allocation, || false))
        {
            self.capture_validated(capture, now);
        }
    }
    fn available(&self) -> Result<(), OriginError> {
        if self.config.max_seconds == 0 {
            return Err(HlsHistoryError::Disabled.into());
        }
        if self.reason.is_some() {
            return Err(OriginError::UnsupportedPlaylist);
        }
        Ok(())
    }
    fn check_generation(&self, expected: Option<u64>) -> Result<(), OriginError> {
        if expected.is_some_and(|expected| expected != self.generation) {
            return Err(OriginError::HistoryConflict);
        }
        Ok(())
    }
    pub(super) fn has_view(&self, generation: u64) -> bool {
        self.generation == generation && self.snapshot.is_some()
    }
    pub(super) fn view(&self, token: &str) -> OriginHistoryView {
        OriginHistoryView {
            generation: self.generation,
            mode: if self.snapshot.is_some() {
                "replay"
            } else {
                "live"
            },
            snapshot: self.snapshot.clone(),
            playback_url: if self.snapshot.is_some() {
                format!("/origin-hls/{token}/replay/{}/index.m3u8", self.generation)
            } else {
                format!("/origin-hls/{token}/0.m3u8")
            },
        }
    }
    pub(super) fn seek(
        &mut self,
        token: &str,
        target: OriginSeekTarget,
        expected: Option<u64>,
        now: Instant,
    ) -> Result<OriginHistoryView, OriginError> {
        self.check_generation(expected)?;
        self.available()?;
        self.prune(now);
        let (&last_seq, last) = self
            .records
            .iter()
            .rev()
            .find(|(_, r)| r.body.is_some())
            .ok_or(HlsHistoryError::NotReady)?;
        let position = match target {
            OriginSeekTarget::Back(back) => {
                let back = u64::try_from(back.as_millis())
                    .ok()
                    .filter(|back| *back > 0)
                    .ok_or(HlsHistoryError::InvalidTarget)?;
                last.end_ms
                    .checked_sub(back)
                    .ok_or(HlsHistoryError::Unavailable)?
            }
            OriginSeekTarget::PositionMs(position) => position,
        };
        let (&first_seq, first) = self
            .records
            .iter()
            .find(|(_, r)| r.start_ms <= position && position < r.end_ms)
            .ok_or(HlsHistoryError::Unavailable)?;
        let snapshot = HlsHistorySnapshot {
            first_seq,
            last_seq,
            start_ms: first.start_ms,
            end_ms: last.end_ms,
        };
        self.range(&snapshot)
            .map_err(|_| HlsHistoryError::Unavailable)?;
        let generation = self
            .generation
            .checked_add(1)
            .ok_or(OriginError::TooLarge)?;
        self.generation = generation;
        self.snapshot = Some(snapshot);
        Ok(self.view(token))
    }
    pub(super) fn live(
        &mut self,
        token: &str,
        expected: Option<u64>,
    ) -> Result<OriginHistoryView, OriginError> {
        self.check_generation(expected)?;
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or(OriginError::TooLarge)?;
        self.snapshot = None;
        Ok(self.view(token))
    }
    fn range(&self, snapshot: &HlsHistorySnapshot) -> Result<Vec<(u64, &Record)>, HlsHistoryError> {
        let count = snapshot
            .last_seq
            .checked_sub(snapshot.first_seq)
            .and_then(|n| n.checked_add(1))
            .ok_or(HlsHistoryError::Gone)?;
        if count > MAX_RECORDS as u64 {
            return Err(HlsHistoryError::Gone);
        }
        let records: Vec<_> = self
            .records
            .range(snapshot.first_seq..=snapshot.last_seq)
            .map(|(seq, r)| (*seq, r))
            .collect();
        if records.len() != count as usize
            || records
                .first()
                .is_none_or(|(_, r)| r.start_ms != snapshot.start_ms)
            || records
                .last()
                .is_none_or(|(_, r)| r.end_ms != snapshot.end_ms)
            || records.iter().any(|(_, r)| r.body.is_none())
        {
            return Err(HlsHistoryError::Gone);
        }
        // Bound the generated representation before a seek commits its new view.
        // The estimate includes the fixed token and maximum decimal selectors.
        let mut estimated = 512 + records.len() * 200;
        for pair in records.windows(2) {
            let changes = pair[1]
                .1
                .discontinuity
                .checked_sub(pair[0].1.discontinuity)
                .and_then(|count| usize::try_from(count).ok())
                .and_then(|count| count.checked_mul(21))
                .ok_or(HlsHistoryError::Gone)?;
            estimated = estimated
                .checked_add(changes)
                .ok_or(HlsHistoryError::Gone)?;
        }
        if estimated > playlist::MAX_PLAYLIST_BYTES {
            return Err(HlsHistoryError::Gone);
        }
        Ok(records)
    }
    pub(super) fn replay(
        &mut self,
        token: &str,
        generation: u64,
        resource: &str,
        now: Instant,
    ) -> Result<(Bytes, Option<Arc<Retained>>), OriginError> {
        self.prune(now);
        if generation != self.generation {
            return Err(HlsHistoryError::Gone.into());
        }
        let snapshot = self.snapshot.as_ref().ok_or(HlsHistoryError::Gone)?;
        if resource == "index.m3u8" {
            let records = self.range(snapshot)?;
            let target = records
                .iter()
                .map(|(_, r)| r.duration.as_secs_f64().ceil() as u64)
                .max()
                .unwrap_or(1)
                .max(1);
            let base = records[0].1.discontinuity;
            let mut output = format!("#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXT-X-INDEPENDENT-SEGMENTS\n#EXT-X-TARGETDURATION:{target}\n#EXT-X-MEDIA-SEQUENCE:{}\n#EXT-X-DISCONTINUITY-SEQUENCE:{base}\n#EXT-X-START:TIME-OFFSET=0.000,PRECISE=YES\n", snapshot.first_seq);
            let mut discontinuity = base;
            for (seq, record) in records {
                for _ in discontinuity..record.discontinuity {
                    output.push_str("#EXT-X-DISCONTINUITY\n");
                }
                discontinuity = record.discontinuity;
                output.push_str(&format!(
                    "#EXTINF:{:.9},\n/origin-hls/{token}/replay/{generation}/{seq}.ts\n",
                    record.duration.as_secs_f64()
                ));
            }
            output.push_str("#EXT-X-ENDLIST\n");
            if output.len() > playlist::MAX_PLAYLIST_BYTES {
                return Err(OriginError::TooLarge);
            }
            Ok((Bytes::from(output), None))
        } else {
            let sequence = resource
                .strip_suffix(".ts")
                .filter(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or(OriginError::NotFound)?;
            if !(snapshot.first_seq..=snapshot.last_seq).contains(&sequence) {
                return Err(HlsHistoryError::Gone.into());
            }
            let record = self
                .records
                .get(&sequence)
                .filter(|r| r.start_ms >= snapshot.start_ms && r.end_ms <= snapshot.end_ms)
                .ok_or(HlsHistoryError::Gone)?;
            let body = record.body.clone().ok_or(HlsHistoryError::Gone)?;
            // Every emitted/sliced Bytes keeps the shared charge, even after the
            // HTTP stream itself and the archive record have both been dropped.
            Ok((Bytes::from_owner(ReplayBytes(body.clone())), Some(body)))
        }
    }
    pub(super) fn status(&mut self, token: &str, now: Instant) -> OriginHistoryStatus {
        self.prune(now);
        let retained: Vec<_> = self
            .records
            .iter()
            .filter(|(_, r)| r.body.is_some())
            .collect();
        let mut intervals: Vec<HlsHistoryInterval> = Vec::new();
        for (&seq, record) in &retained {
            if let Some(last) = intervals.last_mut().filter(|last| {
                last.last_seq.checked_add(1) == Some(seq) && last.epoch == record.discontinuity
            }) {
                last.last_seq = seq;
                last.end_ms = record.end_ms;
            } else {
                intervals.push(HlsHistoryInterval {
                    first_seq: seq,
                    last_seq: seq,
                    start_ms: record.start_ms,
                    end_ms: record.end_ms,
                    epoch: record.discontinuity,
                });
            }
        }
        let mut seekable = None;
        let mut previous: Option<u64> = None;
        for (&seq, record) in retained.iter().rev() {
            if previous.is_some_and(|previous| seq.checked_add(1) != Some(previous)) {
                break;
            }
            seekable = Some(record.start_ms);
            previous = Some(seq);
        }
        let enabled = self.config.max_seconds != 0;
        OriginHistoryStatus {
            enabled,
            supported: enabled && self.reason.is_none(),
            reason: if enabled {
                self.reason
            } else {
                Some("history_disabled")
            },
            kind: "retained_origin_ts",
            time_basis: "observed_media_duration",
            persistent: false,
            utc_seek: false,
            peer_archive: false,
            view: self.view(token),
            history: HlsHistoryStatus {
                enabled,
                kind: "retained_origin_ts",
                time_basis: "observed_media_duration",
                first_seq: retained.first().map(|(seq, _)| **seq),
                last_seq: retained.last().map(|(seq, _)| **seq),
                start_ms: retained.first().map(|(_, r)| r.start_ms),
                end_ms: retained.last().map(|(_, r)| r.end_ms),
                seekable_start_ms: seekable,
                retained_bytes: self.used,
                max_bytes: self.config.max_bytes,
                max_seconds: self.config.max_seconds,
                intervals,
            },
        }
    }
}

/// Keep the byte representation intact. A declaration alone cannot make arbitrary
/// or mid-GOP bytes a replay entry; validated fresh tables and a first video access
/// point are also required. Audio-only and unknown video layouts remain unsupported.
#[cfg(test)]
fn independent_ts(bytes: &[u8]) -> bool {
    independent_ts_until(bytes, || false)
}
fn independent_ts_until(bytes: &[u8], mut cancelled: impl FnMut() -> bool) -> bool {
    if bytes.is_empty() || !bytes.len().is_multiple_of(TS_PACKET_LEN) {
        return false;
    }
    let mut access = VideoAccessPointState::new();
    access.enable_mpeg2();
    let mut first_video = None;
    for (index, packet) in bytes.as_chunks::<TS_PACKET_LEN>().0.iter().enumerate() {
        if (index % 256 == 0 && cancelled()) || packet[0] != mpegts::TS_SYNC {
            return false;
        }
        if packet[1] & 0x80 != 0 || packet[3] & 0xc0 != 0 || packet[3] & 0x30 == 0 {
            return false;
        }
        match (packet[3] >> 4) & 3 {
            2 if packet[4] != 183 => return false,
            3 if packet[4] > 182 => return false,
            _ => {}
        }
        let point = access.observe(packet);
        if first_video.is_none()
            && access.video_pid() == Some(mpegts::ts_pid(packet))
            && packet[3] & 0x10 != 0
        {
            first_video = Some(
                point
                    && access.table_prefix().is_some_and(|tables| {
                        tables
                            .as_chunks::<TS_PACKET_LEN>()
                            .0
                            .iter()
                            .all(|packet| valid_table_crc(packet))
                    }),
            );
        }
    }
    first_video == Some(true) && !cancelled()
}

fn valid_table_crc(packet: &[u8]) -> bool {
    let payload = 4 + if packet[3] & 0x20 != 0 {
        1 + packet[4] as usize
    } else {
        0
    };
    let Some(pointer) = packet.get(payload) else {
        return false;
    };
    let start = payload + 1 + *pointer as usize;
    let (Some(high), Some(low)) = (packet.get(start + 1), packet.get(start + 2)) else {
        return false;
    };
    let end = start + 3 + (((*high & 15) as usize) << 8 | *low as usize);
    packet
        .get(start..end)
        .is_some_and(|section| section.len() >= 4 && mpegts::mpeg_crc32(section) == 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn bytes() -> Bytes {
        Bytes::from_static(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/vectors/media/h264-keyframes.ts"
        )))
    }
    fn config() -> HlsHistoryConfig {
        HlsHistoryConfig {
            max_seconds: 60,
            max_bytes: 1024 * 1024,
            global_max_bytes: 2 * 1024 * 1024,
        }
    }
    fn metadata(first: u64, count: u64) -> Vec<(u64, playlist::MediaSegment)> {
        (first..first + count)
            .map(|seq| {
                (
                    seq + 1,
                    playlist::MediaSegment {
                        reference_index: (seq - first) as usize,
                        sequence: seq,
                        duration: Duration::from_secs(2),
                        discontinuity_sequence: 0,
                    },
                )
            })
            .collect()
    }
    #[test]
    fn independent_ts_requires_fresh_tables_clear_packets_and_first_video_access() {
        let valid = bytes();
        assert!(independent_ts(&valid));
        assert!(!independent_ts(&valid[376..]));
        let mut corrupt = valid.to_vec();
        corrupt[188 + 12] ^= 1;
        assert!(!independent_ts(&corrupt));
        let mut scrambled = valid.to_vec();
        scrambled[3] |= 0xc0;
        assert!(!independent_ts(&scrambled));
        let mut continuation = valid.to_vec();
        let late = &valid[4136..4136 + 188];
        continuation[564..564 + 188].copy_from_slice(late);
        assert!(!independent_ts(&continuation));
    }
    #[test]
    fn parallel_completion_keeps_positions_and_frozen_snapshots_without_fetching() {
        let now = Instant::now();
        let cfg = config();
        let mut history = OriginHistory::new(cfg, HistoryBudget::new(cfg.global_max_bytes));
        history.observe(0, true, &metadata(10, 3), now);
        let bytes = bytes();
        history.capture(13, &bytes, bytes.len(), now);
        history.capture(11, &bytes, bytes.len(), now);
        assert!(history
            .seek(
                "viewer",
                OriginSeekTarget::Back(Duration::from_secs(5)),
                Some(0),
                now
            )
            .is_err());
        assert_eq!(history.generation, 0);
        history.capture(12, &bytes, bytes.len(), now);
        let view = history
            .seek("viewer", OriginSeekTarget::PositionMs(1), Some(0), now)
            .unwrap();
        let snapshot = view.snapshot.unwrap();
        assert_eq!(
            (
                snapshot.first_seq,
                snapshot.last_seq,
                snapshot.start_ms,
                snapshot.end_ms
            ),
            (10, 12, 0, 6000)
        );
        let frozen = history
            .replay("viewer", view.generation, "index.m3u8", now)
            .unwrap()
            .0;
        assert!(std::str::from_utf8(&frozen)
            .unwrap()
            .ends_with("#EXT-X-ENDLIST\n"));
        history.observe(0, true, &metadata(11, 3), now);
        history.capture(14, &bytes, bytes.len(), now);
        assert_eq!(
            history
                .replay("viewer", view.generation, "index.m3u8", now)
                .unwrap()
                .0,
            frozen
        );
        assert_eq!(
            history
                .replay("viewer", view.generation, "11.ts", now)
                .unwrap()
                .0,
            bytes
        );
        assert!(history
            .seek(
                "viewer",
                OriginSeekTarget::PositionMs(u64::MAX),
                Some(1),
                now
            )
            .is_err());
        assert_eq!(history.generation, 1);
        assert!(matches!(
            history.live("viewer", Some(0)),
            Err(OriginError::HistoryConflict)
        ));
        history.live("viewer", Some(1)).unwrap();
        assert!(matches!(
            history.replay("viewer", 1, "11.ts", now),
            Err(OriginError::History(HlsHistoryError::Gone))
        ));
    }
    #[test]
    fn pruning_epoch_reset_and_unobserved_gaps_revoke_snapshots_and_memory() {
        let now = Instant::now();
        let cfg = config();
        let budget = HistoryBudget::new(cfg.global_max_bytes);
        let mut history = OriginHistory::new(cfg, budget.clone());
        let bytes = bytes();
        history.observe(0, true, &metadata(1, 2), now);
        for id in [2, 3] {
            history.capture(id, &bytes, bytes.len(), now);
        }
        let view = history
            .seek(
                "viewer",
                OriginSeekTarget::Back(Duration::from_secs(2)),
                None,
                now,
            )
            .unwrap();
        history.observe(1, true, &metadata(3, 2), now);
        assert!(history
            .replay("viewer", view.generation, "index.m3u8", now)
            .is_err());
        assert_eq!(history.used, 0);
        history.capture(4, &bytes, bytes.len(), now);
        history.capture(5, &bytes, bytes.len(), now);
        let view = history
            .seek(
                "viewer",
                OriginSeekTarget::Back(Duration::from_secs(3)),
                None,
                now,
            )
            .unwrap();
        history.observe(1, true, &metadata(20, 2), now);
        assert!(history
            .replay("viewer", view.generation, "index.m3u8", now)
            .is_err());
        history.capture(21, &bytes, bytes.len(), now);
        history.prune(now + Duration::from_secs(61));
        assert_eq!(history.used, 0);
        assert!(budget.reserve(cfg.global_max_bytes).is_some());
    }
    #[test]
    fn shared_budget_counts_live_replay_response_owners_after_pruning() {
        let now = Instant::now();
        let bytes = bytes();
        let mut cfg = config();
        cfg.max_bytes = bytes.len();
        cfg.global_max_bytes = bytes.len();
        let budget = HistoryBudget::new(cfg.global_max_bytes);
        let mut a = OriginHistory::new(cfg, budget.clone());
        let mut b = OriginHistory::new(cfg, budget.clone());
        for h in [&mut a, &mut b] {
            h.observe(0, true, &metadata(1, 1), now);
        }
        a.capture(2, &bytes, bytes.len(), now);
        b.capture(2, &bytes, bytes.len(), now);
        assert_eq!(b.used, 0);
        let view = a
            .seek("a", OriginSeekTarget::PositionMs(0), None, now)
            .unwrap();
        let (body, owner) = a.replay("a", view.generation, "1.ts", now).unwrap();
        let held = body.slice(..188);
        drop(body);
        drop(owner);
        a.clear();
        b.capture(2, &bytes, bytes.len(), now);
        assert_eq!(b.used, 0);
        drop(held);
        b.capture(2, &bytes, bytes.len(), now);
        assert_eq!(b.used, bytes.len());
    }

    #[test]
    fn per_source_budget_uses_backing_allocation_and_does_not_pin_old_snapshots() {
        let now = Instant::now();
        let bytes = bytes();
        let mut cfg = config();
        cfg.max_bytes = bytes.len() * 2;
        let mut history = OriginHistory::new(cfg, HistoryBudget::new(cfg.global_max_bytes));
        history.observe(0, true, &metadata(1, 3), now);
        history.capture(2, &bytes, cfg.max_bytes + 1, now);
        assert_eq!(history.used, 0, "spare backing capacity is also charged");
        history.capture(2, &bytes, bytes.len(), now);
        history.capture(3, &bytes, bytes.len(), now);
        let view = history
            .seek("viewer", OriginSeekTarget::PositionMs(0), None, now)
            .unwrap();
        history.capture(4, &bytes, bytes.len(), now);
        assert_eq!(history.used, bytes.len() * 2);
        assert!(history
            .replay("viewer", view.generation, "index.m3u8", now)
            .is_err());
        assert!(
            history
                .replay("viewer", view.generation, "2.ts", now)
                .is_ok(),
            "consumed-prefix pruning does not revoke retained unread segments"
        );
        history.observe(0, true, &metadata(1, 3), now);
        history.capture(2, &bytes, bytes.len(), now);
        assert!(
            history
                .replay("viewer", view.generation, "index.m3u8", now)
                .is_err(),
            "an identical manifest must not revive a pruned snapshot prefix"
        );
        assert!(history
            .replay("viewer", view.generation, "1.ts", now)
            .is_err());
    }
    #[test]
    fn capture_tickets_survive_sliding_windows_and_view_changes() {
        let now = Instant::now();
        let cfg = config();
        let mut history = OriginHistory::new(cfg, HistoryBudget::new(cfg.global_max_bytes));
        let bytes = bytes();
        history.observe(0, true, &metadata(10, 3), now);
        let removed = history.capture_ticket(11, now).unwrap();
        let overlap = history.capture_ticket(12, now).unwrap();
        history.capture(13, &bytes, bytes.len(), now);
        let view = history
            .seek("viewer", OriginSeekTarget::PositionMs(4000), None, now)
            .unwrap();
        history.live("viewer", Some(view.generation)).unwrap();
        history.observe(0, true, &metadata(11, 3), now);
        for ticket in [overlap, removed] {
            history.capture_validated(ticket.validate(&bytes, bytes.len(), || false).unwrap(), now);
        }
        let view = history
            .seek("viewer", OriginSeekTarget::PositionMs(0), None, now)
            .unwrap();
        assert_eq!(view.snapshot.unwrap().first_seq, 10);
        assert_eq!(history.used, bytes.len() * 3);
        history.capture_validated(
            removed.validate(&bytes, bytes.len(), || false).unwrap(),
            now,
        );
        assert_eq!(
            history.used,
            bytes.len() * 3,
            "duplicate completion must not charge twice"
        );
    }

    #[test]
    fn capture_tickets_reject_reset_aba_descriptor_changes_and_pruning() {
        let now = Instant::now();
        let cfg = config();
        let bytes = bytes();
        for mode in 0..7 {
            let mut history = OriginHistory::new(cfg, HistoryBudget::new(cfg.global_max_bytes));
            history.observe(0, true, &metadata(10, 1), now);
            let ticket = history.capture_ticket(11, now).unwrap();
            let mut changed = metadata(10, 1);
            match mode {
                0 => {
                    history.observe(0, false, &changed, now);
                    history.observe(0, true, &changed, now);
                }
                1 => {
                    history.observe(1, true, &changed, now);
                }
                2 => {
                    changed[0].1.duration = Duration::from_secs(3);
                    history.observe(0, true, &changed, now);
                }
                3 => {
                    changed[0].0 = 99;
                    history.observe(0, true, &changed, now);
                }
                4 => {
                    history.prune(now + Duration::from_secs(61));
                    history.observe(0, true, &changed, now + Duration::from_secs(61));
                }
                5 => {
                    changed[0].1.sequence += 1;
                    history.observe(0, true, &changed, now);
                }
                _ => {
                    history.clear();
                    history.observe(0, true, &changed, now);
                }
            }
            history.capture_validated(ticket.validate(&bytes, bytes.len(), || false).unwrap(), now);
            assert_eq!(history.used, 0, "stale completion accepted in case {mode}");
        }
    }

    #[test]
    fn capture_validation_is_cancellable_without_holding_history_lock() {
        let now = Instant::now();
        let cfg = config();
        let history = Mutex::new(OriginHistory::new(
            cfg,
            HistoryBudget::new(cfg.global_max_bytes),
        ));
        let ticket = {
            let mut guard = history.lock().unwrap();
            guard.observe(0, true, &metadata(10, 1), now);
            guard.capture_ticket(11, now).unwrap()
        };
        let bytes = bytes();
        let mut polls = 0;
        assert!(ticket
            .validate(&bytes, bytes.len(), || {
                polls += 1;
                let mut guard = history
                    .try_lock()
                    .expect("validation does not hold history lock");
                guard.clear();
                true
            })
            .is_none());
        assert_eq!(polls, 1);
        let mut guard = history.lock().unwrap();
        guard.incarnation = u64::MAX;
        guard.observe(0, true, &metadata(10, 1), now);
        assert!(
            guard.capture_ticket(11, now).is_none(),
            "ticket identity never wraps"
        );
    }
}
