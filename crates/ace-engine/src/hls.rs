//! Live HLS packaging over a session's TS broadcast. PCR is used as the media clock and segments
//! start only at PMT-declared video access points, prefixed with the latest PAT and PMT. Runs that
//! cannot reach another clean boundary within the configured packet ceiling are discarded.

use crate::config::HlsConfig;
use crate::session::{StreamEvent, StreamSession};
use ace_media::mpegts::VideoAccessPointState;
use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const TS_PACKET: usize = 188;
const PCR_HZ: u64 = 90_000;
const PCR_MODULUS: u64 = 1 << 33;
const SDT_PID: u16 = 0x0011;

pub struct HlsPackager {
    state: Mutex<HlsState>,
    ready: tokio::sync::Notify,
    /// Hard packet ceiling for every segment. PCR/random-access timing may cut earlier, but never
    /// later, so configuration-time retained-memory calculations match the runtime invariant.
    max_segment_bytes: usize,
    /// Number of segments retained in the sliding window.
    window: usize,
    /// Requested segment duration (seconds).
    seg_duration: f32,
}

struct HlsState {
    /// Sequence number of the first retained segment.
    media_seq: u64,
    segments: VecDeque<HlsSegment>,
    cur: Vec<u8>,
    scanned_packets: usize,
    segment_start_pcr: Option<u64>,
    last_pcr: Option<u64>,
    pcr_pid: Option<u16>,
    max_segment_duration: f32,
    discontinuity_pending: bool,
    last_access: Option<Instant>,
    access: VideoAccessPointState,
    awaiting_access_point: bool,
    /// Set when `access` carries a title, so upstream SDT-PID packets must be dropped from
    /// segment bodies to avoid duplicating (or conflicting with) our synthesized SDT.
    filter_sdt: bool,
    /// One packet outside the configured segment buffer, used only to classify a full segment's
    /// next boundary without appending past `max_segment_bytes`.
    lookahead: [u8; TS_PACKET],
    lookahead_len: usize,
}

struct HlsSegment {
    bytes: Bytes,
    duration: f32,
    discontinuity: bool,
}

impl HlsPackager {
    fn new(config: HlsConfig, service_name: Option<String>) -> Arc<HlsPackager> {
        let seg_packets = config.segment_packets.max(3);
        let max_segment_bytes = seg_packets
            .checked_mul(TS_PACKET)
            .expect("HLS config must be validated before packager construction");
        let access = VideoAccessPointState::with_service_name(service_name);
        let filter_sdt = access.has_service_name();
        Arc::new(HlsPackager {
            state: Mutex::new(HlsState {
                media_seq: 0,
                segments: VecDeque::new(),
                cur: Vec::new(),
                scanned_packets: 0,
                segment_start_pcr: None,
                last_pcr: None,
                pcr_pid: None,
                max_segment_duration: config.segment_duration_secs(),
                discontinuity_pending: false,
                last_access: None,
                access,
                awaiting_access_point: true,
                filter_sdt,
                lookahead: [0; TS_PACKET],
                lookahead_len: 0,
            }),
            ready: tokio::sync::Notify::new(),
            max_segment_bytes,
            window: config.window_segments.max(1),
            seg_duration: config.segment_duration_secs(),
        })
    }

    /// Start packaging `session`'s TS into segments in the background (uncounted receiver).
    pub fn start(session: &StreamSession, config: HlsConfig) -> Arc<HlsPackager> {
        let me = Self::new(config, session.metadata().title.clone());
        let pkg = me.clone();
        let mut rx = session.raw_receiver();
        tokio::spawn(async move {
            use tokio::sync::broadcast::error::RecvError;
            loop {
                match rx.recv().await {
                    Ok(StreamEvent::Data(chunk)) => pkg.feed(&chunk),
                    Ok(StreamEvent::Discontinuity) | Err(RecvError::Lagged(_)) => {
                        pkg.discontinuity()
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        });
        me
    }

    /// Drop media accumulated before a known gap and mark the first complete segment after it.
    fn discontinuity(&self) {
        let mut st = self.state.lock().unwrap();
        st.cur.clear();
        st.scanned_packets = 0;
        st.segment_start_pcr = None;
        st.last_pcr = None;
        st.pcr_pid = None;
        st.discontinuity_pending = true;
        st.access.reset();
        st.awaiting_access_point = true;
        st.lookahead_len = 0;
    }

    /// Append contiguous TS bytes, emitting PCR-timed, preferably random-access-aligned segments.
    fn feed(&self, data: &[u8]) {
        let mut st = self.state.lock().unwrap();
        let mut remaining = data;
        while !remaining.is_empty() {
            if st.cur.len() == self.max_segment_bytes {
                let take = (TS_PACKET - st.lookahead_len).min(remaining.len());
                let start = st.lookahead_len;
                st.lookahead[start..start + take].copy_from_slice(&remaining[..take]);
                st.lookahead_len += take;
                remaining = &remaining[take..];
                if st.lookahead_len == TS_PACKET {
                    let packet = st.lookahead;
                    st.lookahead_len = 0;
                    self.scan_lookahead(&mut st, &packet);
                }
                continue;
            }
            let room = self.max_segment_bytes - st.cur.len();
            let packet_room = TS_PACKET - st.cur.len() % TS_PACKET;
            let take = room.min(packet_room).min(remaining.len());
            st.cur.extend_from_slice(&remaining[..take]);
            remaining = &remaining[take..];
            self.scan(&mut st);
        }
    }

    fn scan(&self, st: &mut HlsState) {
        while st.scanned_packets < st.cur.len() / TS_PACKET {
            let packet_offset = st.scanned_packets * TS_PACKET;
            let mut packet = [0; TS_PACKET];
            packet.copy_from_slice(&st.cur[packet_offset..packet_offset + TS_PACKET]);
            let mut timing = ts_timing(&packet);
            let is_access_point = st.access.observe(&packet);

            if st.filter_sdt && ts_pid(&packet) == SDT_PID {
                st.cur.drain(packet_offset..packet_offset + TS_PACKET);
                continue; // scanned_packets unchanged: the next packet slides into this offset
            }

            if timing.discontinuity {
                st.access.reset();
                st.cur.drain(..packet_offset + TS_PACKET);
                st.scanned_packets = 0;
                st.segment_start_pcr = None;
                st.last_pcr = None;
                st.pcr_pid = None;
                st.discontinuity_pending = true;
                st.awaiting_access_point = true;
                continue;
            }

            if st.awaiting_access_point {
                let Some(prefix) = is_access_point.then(|| st.access.table_prefix()).flatten()
                else {
                    st.cur.drain(..packet_offset + TS_PACKET);
                    st.scanned_packets = 0;
                    continue;
                };
                if prefix.len() + TS_PACKET > self.max_segment_bytes {
                    st.cur.drain(..packet_offset + TS_PACKET);
                    st.scanned_packets = 0;
                    st.discontinuity_pending = true;
                    continue;
                }
                let trailing = st.cur.split_off(packet_offset + TS_PACKET);
                st.cur.clear();
                st.cur.extend_from_slice(&prefix);
                st.cur.extend_from_slice(&packet);
                st.cur.extend_from_slice(&trailing);
                st.scanned_packets = prefix.len() / TS_PACKET;
                st.awaiting_access_point = false;
                continue;
            }

            // A transport stream may carry several programs, each with its own PCR clock. Until
            // PAT/PMT metadata is available here, lock each uninterrupted run to the first PCR
            // PID instead of combining unrelated clocks. Random-access flags remain useful on a
            // separate video PID.
            if timing.pcr.is_some() {
                match st.pcr_pid {
                    Some(pid) if pid != timing.pid => timing.pcr = None,
                    None => st.pcr_pid = Some(timing.pid),
                    _ => {}
                }
            }

            if pcr_went_backwards(st.last_pcr, timing.pcr) {
                st.access.reset();
                st.cur.drain(..packet_offset + TS_PACKET);
                st.scanned_packets = 0;
                st.segment_start_pcr = None;
                st.last_pcr = None;
                st.pcr_pid = None;
                st.discontinuity_pending = true;
                st.awaiting_access_point = true;
                continue;
            }

            if st.segment_start_pcr.is_none() {
                st.segment_start_pcr = timing.pcr;
            }
            if timing.pcr.is_some() {
                st.last_pcr = timing.pcr;
            }

            let elapsed = match (st.segment_start_pcr, st.last_pcr) {
                (Some(start), Some(now)) => pcr_delta(start, now).map(pcr_seconds),
                _ => None,
            };
            let target_reached = elapsed.is_some_and(|d| d >= self.seg_duration);
            if packet_offset > 0 && target_reached && is_access_point {
                let duration = elapsed.unwrap_or(self.seg_duration);
                let boundary_pcr = st.last_pcr;
                self.emit(st, packet_offset, duration);
                let prefix = st
                    .access
                    .table_prefix()
                    .expect("an observed video access point has PAT/PMT state");
                st.cur.splice(0..0, prefix.iter().copied());
                st.scanned_packets = prefix.len() / TS_PACKET;
                st.segment_start_pcr = boundary_pcr;
                st.last_pcr = boundary_pcr;
                continue;
            }
            st.scanned_packets += 1;
        }
    }

    /// Classify the packet after a ceiling-full run. A clean access point completes the retained
    /// run; any other packet proves that run cannot end cleanly within its configured bound.
    fn scan_lookahead(&self, st: &mut HlsState, packet: &[u8; TS_PACKET]) {
        let mut timing = ts_timing(packet);
        let is_access_point = st.access.observe(packet);

        if st.filter_sdt && ts_pid(packet) == SDT_PID {
            return;
        }

        if timing.discontinuity {
            st.access.reset();
            self.discard_incomplete_run(st);
            return;
        }

        if timing.pcr.is_some() {
            match st.pcr_pid {
                Some(pid) if pid != timing.pid => timing.pcr = None,
                None => st.pcr_pid = Some(timing.pid),
                _ => {}
            }
        }

        if pcr_went_backwards(st.last_pcr, timing.pcr) {
            st.access.reset();
            self.discard_incomplete_run(st);
            return;
        }

        if st.segment_start_pcr.is_none() {
            st.segment_start_pcr = timing.pcr;
        }
        if timing.pcr.is_some() {
            st.last_pcr = timing.pcr;
        }
        let elapsed = match (st.segment_start_pcr, st.last_pcr) {
            (Some(start), Some(now)) => pcr_delta(start, now).map(pcr_seconds),
            _ => None,
        };

        // Once a full target duration has elapsed, emit the ceiling-full segment even if this
        // boundary packet is not a keyframe. On a peaky or long-GOP stream (e.g. 2160p HEVC in a
        // high-motion scene) the next keyframe can land just past the byte ceiling; discarding
        // the run there would stall the playlist. Emitting keeps playback advancing. A run forced
        // out this way does not end on a keyframe, so the continuation segment starts mid-GOP --
        // seamless for sequential live playback, with the next keyframe re-aligning boundaries.
        // It is not a transport discontinuity, so no #EXT-X-DISCONTINUITY is emitted.
        if elapsed.is_some_and(|duration| duration >= self.seg_duration) {
            let duration = elapsed.unwrap_or(self.seg_duration);
            let boundary_pcr = st.last_pcr;
            let boundary_pcr_pid = st.pcr_pid;
            self.emit(st, self.max_segment_bytes, duration);
            let prefix = st
                .access
                .table_prefix()
                .expect("an accumulated run past the ceiling has cached PAT/PMT state");
            self.seed_clean_run(st, &prefix, packet, timing);
            st.segment_start_pcr = boundary_pcr;
            st.last_pcr = boundary_pcr;
            st.pcr_pid = boundary_pcr_pid;
            return;
        }

        self.discard_incomplete_run(st);
        if is_access_point {
            let prefix = st
                .access
                .table_prefix()
                .expect("an observed video access point has PAT/PMT state");
            self.seed_clean_run(st, &prefix, packet, ts_timing(packet));
        }
    }

    fn seed_clean_run(
        &self,
        st: &mut HlsState,
        prefix: &[u8],
        packet: &[u8; TS_PACKET],
        timing: TsTiming,
    ) {
        debug_assert!(prefix.len() + TS_PACKET <= self.max_segment_bytes);
        st.cur.clear();
        st.cur.extend_from_slice(prefix);
        st.cur.extend_from_slice(packet);
        st.scanned_packets = st.cur.len() / TS_PACKET;
        st.segment_start_pcr = timing.pcr;
        st.last_pcr = timing.pcr;
        st.pcr_pid = timing.pcr.map(|_| timing.pid);
        st.awaiting_access_point = false;
    }

    fn discard_incomplete_run(&self, st: &mut HlsState) {
        st.cur.clear();
        st.scanned_packets = 0;
        st.segment_start_pcr = None;
        st.last_pcr = None;
        st.pcr_pid = None;
        st.discontinuity_pending = true;
        st.awaiting_access_point = true;
        st.lookahead_len = 0;
    }

    fn emit(&self, st: &mut HlsState, end: usize, duration: f32) {
        let first_segment = st.segments.is_empty();
        // Evict before splitting/pushing so emission never transiently retains `window + 1`
        // completed segments in addition to the current segment. Configuration accounts for
        // exactly `window` completed segments plus `cur`.
        while st.segments.len() >= self.window {
            st.segments.pop_front();
            st.media_seq += 1;
        }
        let rest = st.cur.split_off(end);
        let seg = std::mem::replace(&mut st.cur, rest);
        let discontinuity = std::mem::take(&mut st.discontinuity_pending);
        st.segments.push_back(HlsSegment {
            bytes: Bytes::from(seg),
            duration,
            discontinuity,
        });
        if first_segment {
            self.ready.notify_waiters();
        }
        st.max_segment_duration = st.max_segment_duration.max(duration);
        st.scanned_packets = 0;
    }

    /// Whether at least one independently joinable live segment is available.
    pub fn is_ready(&self) -> bool {
        !self.state.lock().unwrap().segments.is_empty()
    }

    /// Wait up to `timeout` for the first independently joinable live segment.
    pub async fn wait_ready(&self, timeout: Duration) -> bool {
        if self.is_ready() {
            return true;
        }
        tokio::time::timeout(timeout, async {
            loop {
                let notified = self.ready.notified();
                if self.is_ready() {
                    break;
                }
                notified.await;
            }
        })
        .await
        .is_ok()
    }

    /// Render the live media playlist; segment URIs are absolute under the given prefix.
    pub fn playlist(&self, network: &str, id: &str) -> String {
        self.playlist_with_segment_prefix(&format!("/streams/{network}/{id}/seg"))
    }

    /// Record native HLS demand before a newly created packager is returned to its caller.
    pub(crate) fn record_native_access(&self) {
        self.state.lock().unwrap().last_access = Some(Instant::now());
    }

    /// Render a native live playlist with a caller-provided segment route and refresh activity.
    pub fn playlist_with_segment_prefix(&self, segment_prefix: &str) -> String {
        let mut st = self.state.lock().unwrap();
        st.last_access = Some(Instant::now());
        Self::render_playlist(&st, segment_prefix)
    }

    /// Render the retained live window for a compatibility route without refreshing native HLS
    /// activity. Compatibility lifetime is controlled by its explicit subscription pin.
    pub fn compatibility_playlist_with_segment_prefix(&self, segment_prefix: &str) -> String {
        Self::render_playlist(&self.state.lock().unwrap(), segment_prefix)
    }

    fn render_playlist(st: &HlsState, segment_prefix: &str) -> String {
        let mut out = String::from("#EXTM3U\n#EXT-X-VERSION:3\n");
        out.push_str(&format!(
            "#EXT-X-TARGETDURATION:{}\n",
            st.max_segment_duration.ceil() as u64
        ));
        out.push_str(&format!("#EXT-X-MEDIA-SEQUENCE:{}\n", st.media_seq));
        for (i, segment) in st.segments.iter().enumerate() {
            if segment.discontinuity {
                out.push_str("#EXT-X-DISCONTINUITY\n");
            }
            out.push_str(&format!("#EXTINF:{:.3},\n", segment.duration));
            out.push_str(&format!(
                "{segment_prefix}/{}.ts\n",
                st.media_seq + i as u64
            ));
        }
        out
    }

    /// Fetch a retained segment by its absolute sequence number.
    pub fn segment(&self, seq: u64) -> Option<Bytes> {
        let mut st = self.state.lock().unwrap();
        let bytes = Self::retained_segment(&st, seq);
        if bytes.is_some() {
            st.last_access = Some(Instant::now());
        }
        bytes
    }

    /// Fetch a retained segment for a compatibility route without refreshing native HLS activity.
    pub fn compatibility_segment(&self, seq: u64) -> Option<Bytes> {
        Self::retained_segment(&self.state.lock().unwrap(), seq)
    }

    fn retained_segment(st: &HlsState, seq: u64) -> Option<Bytes> {
        if seq < st.media_seq {
            return None;
        }
        st.segments
            .get((seq - st.media_seq) as usize)
            .map(|segment| segment.bytes.clone())
    }

    pub(crate) fn was_accessed_within(&self, now: Instant, grace: Duration) -> bool {
        self.state
            .lock()
            .unwrap()
            .last_access
            .is_some_and(|last_access| now.saturating_duration_since(last_access) < grace)
    }

    #[cfg(test)]
    pub(crate) fn set_last_access_for_test(&self, last_access: Instant) {
        self.state.lock().unwrap().last_access = Some(last_access);
    }

    #[cfg(test)]
    pub(crate) fn last_access_for_test(&self) -> Option<Instant> {
        self.state.lock().unwrap().last_access
    }
}

#[derive(Default)]
struct TsTiming {
    pid: u16,
    pcr: Option<u64>,
    discontinuity: bool,
}

/// A packet's PID, independent of `ts_timing`'s adaptation-field gate. `ts_timing` returns a
/// zeroed default (including `pid: 0`) for payload-only packets -- the common shape for PSI
/// sections such as SDT -- so it cannot be used to identify the SDT PID for filtering.
fn ts_pid(packet: &[u8; TS_PACKET]) -> u16 {
    (((packet[1] & 0x1f) as u16) << 8) | packet[2] as u16
}

fn ts_timing(packet: &[u8]) -> TsTiming {
    if packet.len() != TS_PACKET || packet[0] != 0x47 || packet[3] & 0x20 == 0 {
        return TsTiming::default();
    }
    let pid = (((packet[1] & 0x1f) as u16) << 8) | packet[2] as u16;
    let adaptation_len = packet[4] as usize;
    if adaptation_len == 0 || adaptation_len > 183 {
        return TsTiming::default();
    }
    let flags = packet[5];
    let pcr = if flags & 0x10 != 0 && adaptation_len >= 7 {
        Some(
            ((packet[6] as u64) << 25)
                | ((packet[7] as u64) << 17)
                | ((packet[8] as u64) << 9)
                | ((packet[9] as u64) << 1)
                | ((packet[10] as u64) >> 7),
        )
    } else {
        None
    };
    TsTiming {
        pid,
        pcr,
        discontinuity: flags & 0x80 != 0,
    }
}

fn pcr_delta(start: u64, end: u64) -> Option<u64> {
    let delta = (end + PCR_MODULUS - start) % PCR_MODULUS;
    (delta <= PCR_MODULUS / 2).then_some(delta)
}

fn pcr_went_backwards(previous: Option<u64>, current: Option<u64>) -> bool {
    matches!((previous, current), (Some(old), Some(new)) if pcr_delta(old, new).is_none())
}

fn pcr_seconds(ticks: u64) -> f32 {
    ticks as f32 / PCR_HZ as f32
}

/// Byte-range HLS layout for a finite, length-known VOD.
///
/// Unlike [`HlsPackager`] (a live sliding window fed by a broadcast), a VOD's whole geometry is
/// known up front, so segmentation is a pure function of the total length and configured segment
/// size — no state, no retained window. Each segment maps to an inclusive byte range that the
/// HTTP layer serves via `VodContent::open_range` (which SHA-1-verifies the covering pieces).
///
/// Segmentation is byte-based and aligned to whole 188-byte TS packets, exactly like the live
/// packager: it assumes MPEG-TS content and is not keyframe-aware (fine for tolerant players;
/// precise GOP segmentation is a documented follow-up).
pub struct VodHlsLayout {
    total: u64,
    /// Segment size in bytes (a whole number of 188-byte TS packets, >= 188).
    seg_bytes: u64,
    /// Nominal duration of a full segment (seconds).
    seg_duration: f32,
}

impl VodHlsLayout {
    pub fn new(total: u64, config: HlsConfig) -> VodHlsLayout {
        VodHlsLayout {
            total,
            seg_bytes: (config.segment_packets.max(3) * TS_PACKET) as u64,
            seg_duration: config.segment_duration_secs(),
        }
    }

    /// Number of segments covering the whole file (ceil division). Zero-length → 0 segments.
    pub fn segment_count(&self) -> u64 {
        self.total.div_ceil(self.seg_bytes)
    }

    /// Inclusive byte range `[start, end]` for segment `index`, or `None` if `index` is past the
    /// last segment. The final segment is clamped to the last byte, so it may be shorter than a
    /// full segment.
    pub fn segment_range(&self, index: u64) -> Option<(u64, u64)> {
        if index >= self.segment_count() {
            return None;
        }
        let start = index * self.seg_bytes;
        let end = ((index + 1) * self.seg_bytes).min(self.total) - 1;
        Some((start, end))
    }

    /// Render the VOD media playlist: a static `#EXT-X-PLAYLIST-TYPE:VOD` list of every segment,
    /// terminated by `#EXT-X-ENDLIST`. Per-segment `#EXTINF` durations are the nominal segment
    /// duration, with the trailing partial segment scaled by its byte fraction.
    pub fn playlist(&self, network: &str, id: &str) -> String {
        let count = self.segment_count();
        let mut out = String::from("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-PLAYLIST-TYPE:VOD\n");
        out.push_str(&format!(
            "#EXT-X-TARGETDURATION:{}\n",
            self.seg_duration.ceil() as u64
        ));
        out.push_str("#EXT-X-MEDIA-SEQUENCE:0\n");
        for i in 0..count {
            let (start, end) = self.segment_range(i).expect("i < count");
            let dur = self.seg_duration * (end - start + 1) as f32 / self.seg_bytes as f32;
            out.push_str(&format!("#EXTINF:{dur:.3},\n"));
            out.push_str(&format!("/vod/{network}/{id}/seg/{i}.ts\n"));
        }
        out.push_str("#EXT-X-ENDLIST\n");
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HlsConfig;

    fn pkg() -> Arc<HlsPackager> {
        HlsPackager::new(
            HlsConfig {
                segment_packets: 6,
                window_segments: 3,
                segment_duration_ms: 1000,
            },
            None,
        )
    }

    fn packets(n: usize) -> Vec<u8> {
        let mut v = vec![0u8; n * TS_PACKET];
        for k in 0..n {
            v[k * TS_PACKET] = 0x47;
            v[k * TS_PACKET + 3] = 0x10;
        }
        v
    }

    fn timed_pkg(duration_ms: u64) -> Arc<HlsPackager> {
        HlsPackager::new(
            HlsConfig {
                segment_packets: 100,
                window_segments: 6,
                segment_duration_ms: duration_ms,
            },
            None,
        )
    }

    fn random_access_packet(marker: u8) -> Vec<u8> {
        let mut packet = packets(1);
        packet[1] = 0x40 | ((VIDEO_PID >> 8) as u8 & 0x1f);
        packet[2] = VIDEO_PID as u8;
        packet[3] = 0x30;
        packet[4] = 1;
        packet[5] = 0x40;
        packet[6] = marker;
        packet
    }

    fn with_pid(mut packet: Vec<u8>, pid: u16) -> Vec<u8> {
        packet[1] = (packet[1] & 0xe0) | ((pid >> 8) as u8 & 0x1f);
        packet[2] = pid as u8;
        packet
    }

    fn psi(pid: u16, section: &[u8]) -> Vec<u8> {
        let mut packet = vec![0xff; TS_PACKET];
        packet[0] = 0x47;
        packet[1] = 0x40 | ((pid >> 8) as u8 & 0x1f);
        packet[2] = pid as u8;
        packet[3] = 0x10;
        packet[4] = 0;
        packet[5..5 + section.len()].copy_from_slice(section);
        packet
    }

    fn pat(pmt_pid: u16) -> Vec<u8> {
        let section_length = 5 + 4 + 4;
        let mut section = vec![0x00, 0xb0, section_length as u8, 0x00, 0x01, 0xc1, 0, 0];
        section.extend_from_slice(&[
            0x00,
            0x01,
            0xe0 | ((pmt_pid >> 8) as u8 & 0x1f),
            pmt_pid as u8,
        ]);
        section.extend_from_slice(&[0; 4]);
        psi(0, &section)
    }

    fn pmt(pmt_pid: u16, video_pid: u16) -> Vec<u8> {
        let section_length = 5 + 4 + 5 + 4;
        let mut section = vec![0x02, 0xb0, section_length as u8, 0x00, 0x01, 0xc1, 0, 0];
        section.extend_from_slice(&[
            0xe0 | ((video_pid >> 8) as u8 & 0x1f),
            video_pid as u8,
            0xf0,
            0,
            0x1b,
            0xe0 | ((video_pid >> 8) as u8 & 0x1f),
            video_pid as u8,
            0xf0,
            0,
        ]);
        section.extend_from_slice(&[0; 4]);
        psi(pmt_pid, &section)
    }

    fn pcr_packet(
        pid: u16,
        pcr: u64,
        random_access: bool,
        discontinuity: bool,
        marker: u8,
    ) -> Vec<u8> {
        let mut packet = vec![0xff; TS_PACKET];
        packet[0] = 0x47;
        packet[1] = 0x40 | ((pid >> 8) as u8 & 0x1f);
        packet[2] = pid as u8;
        packet[3] = 0x30;
        packet[4] = 7;
        packet[5] =
            0x10 | if random_access { 0x40 } else { 0 } | if discontinuity { 0x80 } else { 0 };
        packet[6] = (pcr >> 25) as u8;
        packet[7] = (pcr >> 17) as u8;
        packet[8] = (pcr >> 9) as u8;
        packet[9] = (pcr >> 1) as u8;
        packet[10] = ((pcr & 1) << 7) as u8 | 0x7e;
        packet[11] = 0;
        packet[12] = marker;
        packet
    }

    fn video_access_packet(pid: u16, pcr: u64) -> Vec<u8> {
        pcr_packet(pid, pcr, true, false, (pcr / 90_000) as u8)
    }

    fn clean_pkg(segment_packets: usize) -> Arc<HlsPackager> {
        HlsPackager::new(
            HlsConfig {
                segment_packets,
                window_segments: 4,
                segment_duration_ms: 1000,
            },
            None,
        )
    }

    const PMT_PID: u16 = 0x0100;
    const VIDEO_PID: u16 = 0x0101;
    const AUDIO_PID: u16 = 0x0102;

    fn feed_one_clean_segment(p: &HlsPackager) {
        feed_clean_run(p, 0, 1);
    }

    fn feed_clean_run(p: &HlsPackager, start_pcr: u64, segment_count: usize) {
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, start_pcr));
        for i in 1..=segment_count {
            p.feed(&video_access_packet(
                VIDEO_PID,
                start_pcr + i as u64 * 108_000,
            ));
        }
    }

    #[test]
    fn clean_first_segment_discards_partial_gop_and_starts_with_tables_and_video_access() {
        let p = clean_pkg(12);
        let partial_gop_packet = with_pid(packets(1), VIDEO_PID);
        let pat = pat(PMT_PID);
        let pmt = pmt(PMT_PID, VIDEO_PID);
        let first_access = video_access_packet(VIDEO_PID, 0);

        p.feed(&partial_gop_packet);
        p.feed(&pat);
        p.feed(&pmt);
        p.feed(&first_access);
        p.feed(&pcr_packet(VIDEO_PID, 45_000, false, false, 7));
        p.feed(&video_access_packet(VIDEO_PID, 108_000));

        let segment = p.segment(0).expect("first clean segment");
        assert_eq!(&segment[..TS_PACKET], &pat);
        assert_eq!(&segment[TS_PACKET..TS_PACKET * 2], &pmt);
        assert_eq!(&segment[TS_PACKET * 2..TS_PACKET * 3], &first_access);
        assert!(!segment
            .windows(TS_PACKET)
            .any(|packet| packet == partial_gop_packet));
    }

    #[test]
    fn clean_subsequent_segment_repeats_tables_and_ignores_non_video_random_access() {
        let p = clean_pkg(12);
        let pat = pat(PMT_PID);
        let pmt = pmt(PMT_PID, VIDEO_PID);
        let non_video_access = pcr_packet(AUDIO_PID, 108_000, true, false, 8);
        let second_access = video_access_packet(VIDEO_PID, 216_000);

        p.feed(&pat);
        p.feed(&pmt);
        p.feed(&video_access_packet(VIDEO_PID, 0));
        p.feed(&pcr_packet(VIDEO_PID, 108_000, false, false, 7));
        p.feed(&non_video_access);
        p.feed(&second_access);
        p.feed(&pcr_packet(VIDEO_PID, 270_000, false, false, 9));
        p.feed(&video_access_packet(VIDEO_PID, 324_000));

        let first = p.segment(0).expect("first clean segment");
        assert!(first
            .chunks_exact(TS_PACKET)
            .any(|packet| packet == non_video_access));
        let second = p.segment(1).expect("second clean segment");
        assert_eq!(&second[..TS_PACKET], &pat);
        assert_eq!(&second[TS_PACKET..TS_PACKET * 2], &pmt);
        assert_eq!(&second[TS_PACKET * 2..TS_PACKET * 3], &second_access);
    }

    #[test]
    fn clean_discontinuity_requires_fresh_tables_before_restarting() {
        let p = clean_pkg(12);
        let stale_access = video_access_packet(VIDEO_PID, 0);
        let fresh_pat = pat(PMT_PID);
        let fresh_pmt = pmt(PMT_PID, VIDEO_PID);
        let fresh_access = video_access_packet(VIDEO_PID, 90_000);

        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, 0));
        p.discontinuity();
        p.feed(&stale_access);
        p.feed(&fresh_pat);
        p.feed(&fresh_pmt);
        p.feed(&fresh_access);
        p.feed(&video_access_packet(VIDEO_PID, 198_000));

        let segment = p.segment(0).expect("first post-gap clean segment");
        assert_eq!(&segment[..TS_PACKET], &fresh_pat);
        assert_eq!(&segment[TS_PACKET..TS_PACKET * 2], &fresh_pmt);
        assert_eq!(&segment[TS_PACKET * 2..TS_PACKET * 3], &fresh_access);
        assert!(!segment
            .chunks_exact(TS_PACKET)
            .any(|packet| packet == stale_access));
    }

    #[test]
    fn clean_hard_ceiling_discards_incomplete_run_and_restarts_at_next_access_point() {
        let p = clean_pkg(5);
        let pat = pat(PMT_PID);
        let pmt = pmt(PMT_PID, VIDEO_PID);

        p.feed(&pat);
        p.feed(&pmt);
        p.feed(&video_access_packet(VIDEO_PID, 0));
        p.feed(&pcr_packet(VIDEO_PID, 9_000, false, false, 1));
        p.feed(&pcr_packet(VIDEO_PID, 18_000, false, false, 2));
        p.feed(&pcr_packet(VIDEO_PID, 27_000, false, false, 3));

        assert!(p.segment(0).is_none());
        assert!(p.state.lock().unwrap().cur.is_empty());

        let restarted_access = video_access_packet(VIDEO_PID, 108_000);
        p.feed(&restarted_access);
        p.feed(&video_access_packet(VIDEO_PID, 216_000));

        let segment = p.segment(0).expect("clean segment after ceiling reset");
        assert_eq!(segment.len(), 3 * TS_PACKET);
        assert_eq!(&segment[..TS_PACKET], &pat);
        assert_eq!(&segment[TS_PACKET..TS_PACKET * 2], &pmt);
        assert_eq!(&segment[TS_PACKET * 2..], &restarted_access);
        assert!(p
            .playlist("test", "ceiling")
            .contains("#EXT-X-DISCONTINUITY"));
        let st = p.state.lock().unwrap();
        assert!(st.cur.len() <= p.max_segment_bytes);
        assert!(st
            .segments
            .iter()
            .all(|segment| segment.bytes.len() <= p.max_segment_bytes));
    }

    #[test]
    fn hard_ceiling_emits_oversized_segment_instead_of_stalling_on_peaky_gop() {
        // A peaky / long-GOP stream can fill the byte ceiling after a full target duration has
        // elapsed but before the next keyframe arrives. Discarding the run there stalls the
        // playlist (the #137 4K-HEVC bug); instead the packager emits the ceiling-full segment
        // and continues. The forced cut is not a transport discontinuity, so no
        // #EXT-X-DISCONTINUITY is introduced.
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 5,
                window_segments: 4,
                segment_duration_ms: 100,
            },
            None,
        );
        let pat = pat(PMT_PID);
        let pmt = pmt(PMT_PID, VIDEO_PID);
        p.feed(&pat);
        p.feed(&pmt);
        p.feed(&video_access_packet(VIDEO_PID, 0));
        p.feed(&pcr_packet(VIDEO_PID, 45_000, false, false, 1));
        p.feed(&pcr_packet(VIDEO_PID, 90_000, false, false, 2));
        // Ceiling is full; this packet arrives past the target duration with no keyframe.
        p.feed(&pcr_packet(VIDEO_PID, 135_000, false, false, 3));

        let segment = p
            .segment(0)
            .expect("ceiling-full segment is emitted, not discarded");
        assert_eq!(segment.len(), 5 * TS_PACKET);
        assert_eq!(&segment[..TS_PACKET], &pat);
        assert_eq!(&segment[TS_PACKET..TS_PACKET * 2], &pmt);
        assert!(!p.playlist("test", "peaky").contains("#EXT-X-DISCONTINUITY"));
    }

    #[test]
    fn clean_three_packet_ceiling_uses_next_access_point_as_boundary_lookahead() {
        let p = clean_pkg(3);
        let pat = pat(PMT_PID);
        let pmt = pmt(PMT_PID, VIDEO_PID);
        let first_access = video_access_packet(VIDEO_PID, 0);

        p.feed(&pat);
        p.feed(&pmt);
        p.feed(&first_access);
        let boundary = video_access_packet(VIDEO_PID, 108_000);
        p.feed(&boundary[..100]);
        assert!(p.segment(0).is_none());
        assert_eq!(p.state.lock().unwrap().cur.len(), 3 * TS_PACKET);
        p.feed(&boundary[100..]);

        let segment = p.segment(0).expect("three-packet clean segment");
        assert_eq!(segment.len(), 3 * TS_PACKET);
        assert_eq!(&segment[..TS_PACKET], &pat);
        assert_eq!(&segment[TS_PACKET..TS_PACKET * 2], &pmt);
        assert_eq!(&segment[TS_PACKET * 2..], &first_access);
        let st = p.state.lock().unwrap();
        assert!(st.cur.len() <= 3 * TS_PACKET);
        assert!(st
            .segments
            .iter()
            .all(|segment| segment.bytes.len() <= 3 * TS_PACKET));
    }

    #[test]
    fn payload_only_packet_fixtures_have_valid_afc() {
        let partial_gop = with_pid(packets(1), VIDEO_PID);
        let pcr_disappeared = with_pid(packets(1), VIDEO_PID);

        assert_eq!(partial_gop[3] & 0x30, 0x10);
        assert_eq!(pcr_disappeared[3] & 0x30, 0x10);
    }

    #[tokio::test]
    async fn readiness_waits_for_first_completed_segment_and_then_stays_ready() {
        let p = clean_pkg(6);
        let stale = Instant::now() - Duration::from_secs(60);
        p.state.lock().unwrap().last_access = Some(stale);

        assert!(!p.is_ready());
        assert!(!p.wait_ready(Duration::from_millis(1)).await);
        assert_eq!(p.state.lock().unwrap().last_access, Some(stale));

        feed_one_clean_segment(&p);
        assert!(p.wait_ready(Duration::from_millis(50)).await);
        assert!(p.is_ready());
        assert_eq!(p.state.lock().unwrap().last_access, Some(stale));
    }

    #[test]
    fn playlist_and_valid_segment_reads_refresh_activity() {
        let p = pkg();
        feed_one_clean_segment(&p);
        let stale = Instant::now() - Duration::from_secs(60);

        p.state.lock().unwrap().last_access = Some(stale);
        let _playlist = p.playlist("test", "active");
        assert!(p
            .state
            .lock()
            .unwrap()
            .last_access
            .is_some_and(|at| at > stale));

        p.state.lock().unwrap().last_access = Some(stale);
        assert!(p.segment(0).is_some());
        assert!(p
            .state
            .lock()
            .unwrap()
            .last_access
            .is_some_and(|at| at > stale));
    }

    #[test]
    fn evicted_and_future_segment_probes_do_not_refresh_activity() {
        let p = pkg();
        feed_clean_run(&p, 0, 5);
        let stale = Instant::now() - Duration::from_secs(60);
        p.state.lock().unwrap().last_access = Some(stale);

        assert!(p.segment(1).is_none());
        assert_eq!(p.state.lock().unwrap().last_access, Some(stale));
        assert!(p.segment(99).is_none());
        assert_eq!(p.state.lock().unwrap().last_access, Some(stale));
    }

    #[test]
    fn pcr_duration_cuts_before_random_access_packet_and_drives_extinf() {
        let p = timed_pkg(1000);
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        for (pcr, key, marker) in [
            (0, true, 1),
            (45_000, false, 2),
            (108_000, true, 3),
            (153_000, false, 4),
            (216_000, true, 5),
        ] {
            p.feed(&pcr_packet(VIDEO_PID, pcr, key, false, marker));
        }

        assert_eq!(p.segment(0).unwrap().len(), 4 * TS_PACKET);
        assert_eq!(p.segment(1).unwrap()[2 * TS_PACKET + 12], 3);
        let playlist = p.playlist("test", "timed");
        assert_eq!(playlist.matches("#EXTINF:1.200,").count(), 2);
        assert!(playlist.contains("#EXT-X-TARGETDURATION:2"));
    }

    #[test]
    fn default_ceiling_allows_a_timed_keyframe_beyond_256_packets() {
        let p = HlsPackager::new(HlsConfig::default(), None);
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        let input: Vec<u8> = (0..=300)
            .flat_map(|i| pcr_packet(VIDEO_PID, i * 360, matches!(i, 0 | 300), false, i as u8))
            .collect();

        p.feed(&input);

        assert_eq!(p.segment(0).unwrap().len(), 302 * TS_PACKET);
        assert!(p
            .playlist("test", "default-ceiling")
            .contains("#EXTINF:1.200,"));
    }

    #[test]
    fn pcr_mode_never_exceeds_the_hard_packet_ceiling() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 5,
                window_segments: 4,
                segment_duration_ms: 60_000,
            },
            None,
        );
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, 0));
        p.feed(&pcr_packet(VIDEO_PID, 9_000, false, false, 1));
        p.feed(&pcr_packet(VIDEO_PID, 18_000, false, false, 2));
        p.feed(&pcr_packet(VIDEO_PID, 27_000, false, false, 3));

        assert!(p.segment(0).is_none());
        let st = p.state.lock().unwrap();
        assert!(st.cur.is_empty());
        assert!(st
            .segments
            .iter()
            .all(|segment| segment.bytes.len() <= p.max_segment_bytes));
    }

    #[test]
    fn hard_ceiling_preserves_post_discontinuity_boundary() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 6,
                window_segments: 4,
                segment_duration_ms: 1000,
            },
            None,
        );
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, 0));
        p.discontinuity();
        feed_clean_run(&p, 90_000, 1);

        let first = p.segment(0).unwrap();
        assert_eq!(first.len(), 3 * TS_PACKET);
        assert_eq!(&first[..TS_PACKET], &pat(PMT_PID));
        assert_eq!(p.state.lock().unwrap().cur.len(), 3 * TS_PACKET);
        assert_eq!(
            p.playlist("test", "bounded-gap")
                .matches("#EXT-X-DISCONTINUITY")
                .count(),
            1
        );
    }

    #[test]
    fn random_access_packet_need_not_carry_the_pcr_it_is_aligned_against() {
        let p = timed_pkg(1000);
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&pcr_packet(VIDEO_PID, 0, true, false, 1));
        p.feed(&pcr_packet(VIDEO_PID, 108_000, false, false, 2));
        p.feed(&random_access_packet(3));
        p.feed(&pcr_packet(VIDEO_PID, 216_000, false, false, 4));
        p.feed(&random_access_packet(5));

        assert_eq!(p.segment(0).unwrap().len(), 4 * TS_PACKET);
        assert_eq!(p.segment(1).unwrap()[2 * TS_PACKET + 6], 3);
        assert_eq!(
            p.playlist("test", "split-pcr")
                .matches("#EXTINF:1.200,")
                .count(),
            2
        );
    }

    #[test]
    fn pcr_wrap_is_measured_as_forward_time() {
        let p = timed_pkg(1000);
        let start = PCR_MODULUS - 45_000;
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&pcr_packet(VIDEO_PID, start, true, false, 1));
        p.feed(&pcr_packet(VIDEO_PID, 63_000, true, false, 2));
        p.feed(&pcr_packet(VIDEO_PID, 171_000, true, false, 3));

        let playlist = p.playlist("test", "wrap");
        assert!(playlist.contains("#EXTINF:1.200,"));
        assert_eq!(playlist.matches("#EXT-X-DISCONTINUITY").count(), 0);
    }

    #[test]
    fn transport_discontinuity_discards_partial_segment_and_marks_next_one() {
        let p = timed_pkg(1000);
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&pcr_packet(VIDEO_PID, 0, true, false, 1));
        p.feed(&pcr_packet(VIDEO_PID, 45_000, false, false, 2));
        p.feed(&pcr_packet(VIDEO_PID, 900_000, true, true, 3));
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&pcr_packet(VIDEO_PID, 900_000, true, false, 4));
        p.feed(&pcr_packet(VIDEO_PID, 1_008_000, true, false, 5));

        assert_eq!(p.segment(0).unwrap()[2 * TS_PACKET + 12], 4);
        let playlist = p.playlist("test", "disc");
        assert!(playlist.contains("#EXT-X-DISCONTINUITY\n#EXTINF:1.200,"));
    }

    #[test]
    fn packet_ceiling_drops_a_run_when_pcr_disappears() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 5,
                window_segments: 6,
                segment_duration_ms: 1000,
            },
            None,
        );
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&pcr_packet(VIDEO_PID, 0, true, false, 1));
        p.feed(&with_pid(packets(1), VIDEO_PID));
        p.feed(&with_pid(packets(1), VIDEO_PID));
        p.feed(&with_pid(packets(1), VIDEO_PID));

        assert!(p.segment(0).is_none());
        assert!(p.state.lock().unwrap().cur.is_empty());
    }

    #[test]
    fn unrelated_pcr_pid_does_not_corrupt_the_selected_clock() {
        let p = timed_pkg(1000);
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, 100));
        p.feed(&pcr_packet(100, 0, true, false, 1));
        // A numerically unrelated clock on another program must not look like a backwards jump.
        p.feed(&pcr_packet(200, PCR_MODULUS - 1, false, false, 2));
        p.feed(&pcr_packet(100, 108_000, true, false, 3));
        p.feed(&pcr_packet(100, 216_000, true, false, 4));

        let playlist = p.playlist("test", "multiprogram");
        assert_eq!(playlist.matches("#EXT-X-DISCONTINUITY").count(), 0);
        assert_eq!(playlist.matches("#EXTINF:1.200,").count(), 2);
    }

    #[test]
    fn no_pcr_lookahead_boundary_keeps_the_selected_clock_pid() {
        let p = clean_pkg(4);
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&pcr_packet(VIDEO_PID, 0, true, false, 1));
        p.feed(&pcr_packet(VIDEO_PID, 108_000, false, false, 2));

        // This access point completes the full run using elapsed time from VIDEO_PID, but it
        // carries no PCR of its own. The next clean run must retain VIDEO_PID as its clock.
        p.feed(&random_access_packet(3));
        assert!(p.segment(0).is_some());

        // A backwards-looking PCR from another program must remain filtered. The selected video
        // clock can then advance to the next access point without a false discontinuity.
        p.feed(&pcr_packet(AUDIO_PID, 0, false, false, 4));
        p.feed(&pcr_packet(VIDEO_PID, 216_000, true, false, 5));

        let playlist = p.playlist("test", "lookahead-clock");
        assert_eq!(playlist.matches("#EXT-X-DISCONTINUITY").count(), 0);
        assert_eq!(playlist.matches("#EXTINF:1.200,").count(), 2);
        assert!(p.segment(1).is_some());
    }

    #[test]
    fn target_duration_never_decreases_as_segments_slide() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 100,
                window_segments: 1,
                segment_duration_ms: 1000,
            },
            None,
        );
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&pcr_packet(VIDEO_PID, 0, true, false, 1));
        p.feed(&pcr_packet(VIDEO_PID, 180_000, true, false, 2));
        assert!(p
            .playlist("test", "target")
            .contains("#EXT-X-TARGETDURATION:2"));

        p.feed(&pcr_packet(VIDEO_PID, 270_000, true, false, 3));
        assert!(p
            .playlist("test", "target")
            .contains("#EXT-X-TARGETDURATION:2"));
    }

    #[test]
    fn fills_segments_and_slides_window() {
        let p = pkg();
        feed_clean_run(&p, 0, 5);
        let pl = p.playlist("test", "abc");
        assert!(pl.contains("#EXT-X-MEDIA-SEQUENCE:2"));
        assert!(pl.contains("/streams/test/abc/seg/2.ts"));
        assert!(pl.contains("/streams/test/abc/seg/4.ts"));
        assert!(!pl.contains("seg/1.ts"));
        // Evicted + future segments are gone; retained ones are PAT + PMT + access point.
        assert!(p.segment(1).is_none());
        assert_eq!(p.segment(3).unwrap().len(), 3 * TS_PACKET);
        assert!(p.segment(99).is_none());
    }

    #[test]
    fn alternate_playlist_prefix_reuses_native_sequence_and_discontinuity_state() {
        let p = pkg();
        feed_one_clean_segment(&p);
        p.discontinuity();
        feed_clean_run(&p, 1_000_000, 1);

        let native = p.playlist("test", "abc");
        let compat = p.compatibility_playlist_with_segment_prefix("/ace/c/client-token");

        assert!(native.contains("/streams/test/abc/seg/0.ts"));
        assert!(compat.contains("/ace/c/client-token/0.ts"));
        assert!(compat.contains("/ace/c/client-token/1.ts"));
        let native_metadata: Vec<_> = native
            .lines()
            .filter(|line| line.starts_with('#'))
            .collect();
        let compat_metadata: Vec<_> = compat
            .lines()
            .filter(|line| line.starts_with('#'))
            .collect();
        assert_eq!(
            native_metadata, compat_metadata,
            "changing the URI prefix must preserve every playlist metadata, sequence, duration, and discontinuity line"
        );
    }

    #[test]
    fn partial_packets_buffer_until_full() {
        let p = pkg();
        let stream = [
            pat(PMT_PID),
            pmt(PMT_PID, VIDEO_PID),
            video_access_packet(VIDEO_PID, 0),
            video_access_packet(VIDEO_PID, 108_000),
        ]
        .concat();
        p.feed(&stream[..stream.len() - 1]);
        assert!(p.playlist("test", "x").lines().all(|l| !l.contains("seg/")));
        p.feed(&stream[stream.len() - 1..]);
        assert!(p.playlist("test", "x").contains("seg/0.ts"));
    }

    #[test]
    fn receiver_lag_discards_partial_media_and_marks_only_first_post_gap_segment() {
        let p = pkg();
        let pre_gap = vec![0x11; TS_PACKET];
        p.feed(&pre_gap);
        p.discontinuity();
        feed_clean_run(&p, 0, 2);

        assert_eq!(p.segment(0).unwrap().len(), 3 * TS_PACKET);
        assert_eq!(p.segment(1).unwrap().len(), 3 * TS_PACKET);
        let playlist = p.playlist("test", "gap");
        assert_eq!(playlist.matches("#EXT-X-DISCONTINUITY").count(), 1);
        assert!(
            playlist.contains("#EXT-X-DISCONTINUITY\n#EXTINF:1.200,\n/streams/test/gap/seg/0.ts")
        );
    }

    #[test]
    fn discontinuity_metadata_slides_with_its_segment() {
        let p = pkg();
        feed_one_clean_segment(&p);
        p.discontinuity();
        feed_clean_run(&p, 1_000_000, 3);
        let retained = p.playlist("test", "slide");
        assert!(retained.contains("#EXT-X-MEDIA-SEQUENCE:1"));
        assert_eq!(retained.matches("#EXT-X-DISCONTINUITY").count(), 1);

        for i in 4..=6 {
            p.feed(&video_access_packet(VIDEO_PID, 1_000_000 + i * 108_000));
        }
        let evicted = p.playlist("test", "slide");
        assert!(evicted.contains("#EXT-X-MEDIA-SEQUENCE:4"));
        assert_eq!(evicted.matches("#EXT-X-DISCONTINUITY").count(), 0);
    }

    #[test]
    fn repeated_gaps_keep_distinct_markers_and_monotonic_sequences() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 6,
                window_segments: 5,
                segment_duration_ms: 1000,
            },
            None,
        );
        feed_clean_run(&p, 0, 1);
        p.discontinuity();
        feed_clean_run(&p, 1_000_000, 1);
        p.discontinuity();
        feed_clean_run(&p, 2_000_000, 1);

        let playlist = p.playlist("test", "repeat");
        assert_eq!(playlist.matches("#EXT-X-DISCONTINUITY").count(), 2);
        assert!(playlist.contains("seg/0.ts"));
        assert!(playlist.contains("seg/1.ts"));
        assert!(playlist.contains("seg/2.ts"));
        assert_eq!(p.segment(0).unwrap().len(), 3 * TS_PACKET);
        assert_eq!(p.segment(2).unwrap().len(), 3 * TS_PACKET);
    }

    #[test]
    fn configured_hls_settings_control_playlist_and_segments() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 6,
                window_segments: 2,
                segment_duration_ms: 2500,
            },
            None,
        );
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, 0));
        for i in 1..=3 {
            p.feed(&video_access_packet(VIDEO_PID, i * 270_000));
        }

        let pl = p.playlist("test", "abc");

        assert!(pl.contains("#EXT-X-TARGETDURATION:3"));
        assert!(pl.contains("#EXTINF:3.000,"));
        assert!(pl.contains("#EXT-X-MEDIA-SEQUENCE:1"));
        assert!(p.segment(0).is_none());
        assert_eq!(p.segment(2).unwrap().len(), 3 * TS_PACKET);
    }

    fn vod_config() -> HlsConfig {
        // Minimum 3 packets/segment => 564-byte segments, 2.0s nominal per full segment.
        HlsConfig {
            segment_packets: 3,
            window_segments: 6,
            segment_duration_ms: 2000,
        }
    }

    #[test]
    fn vod_segment_count_is_ceil_division_of_total_over_segment_bytes() {
        // 564-byte segments.
        assert_eq!(VodHlsLayout::new(0, vod_config()).segment_count(), 0);
        assert_eq!(VodHlsLayout::new(1, vod_config()).segment_count(), 1);
        assert_eq!(VodHlsLayout::new(564, vod_config()).segment_count(), 1);
        assert_eq!(VodHlsLayout::new(565, vod_config()).segment_count(), 2);
        assert_eq!(VodHlsLayout::new(564 * 3, vod_config()).segment_count(), 3);
    }

    #[test]
    fn vod_segment_range_is_aligned_with_a_clamped_final_segment() {
        // 1500 bytes over 564-byte segments => [0,564), [564,1128), [1128,1500).
        let layout = VodHlsLayout::new(1500, vod_config());
        assert_eq!(layout.segment_range(0), Some((0, 563)));
        assert_eq!(layout.segment_range(1), Some((564, 1127)));
        assert_eq!(layout.segment_range(2), Some((1128, 1499))); // clamped, shorter
        assert_eq!(layout.segment_range(3), None); // past the last segment
    }

    #[test]
    fn vod_playlist_is_a_terminated_vod_list_of_every_segment() {
        // 1500 bytes => 3 segments; last is 372/564 of a full 2.0s segment.
        let pl = VodHlsLayout::new(1500, vod_config()).playlist("memvod", "abc");
        assert!(pl.contains("#EXT-X-PLAYLIST-TYPE:VOD"));
        assert!(pl.contains("#EXT-X-TARGETDURATION:2"));
        assert!(pl.contains("#EXT-X-MEDIA-SEQUENCE:0"));
        assert!(pl.contains("/vod/memvod/abc/seg/0.ts"));
        assert!(pl.contains("/vod/memvod/abc/seg/2.ts"));
        assert!(!pl.contains("/vod/memvod/abc/seg/3.ts"));
        assert!(pl.trim_end().ends_with("#EXT-X-ENDLIST"));
        // Full segments advertise the nominal 2.0s; the trailing partial is scaled by bytes.
        assert!(pl.contains("#EXTINF:2.000,"));
        assert!(pl.contains(&format!("#EXTINF:{:.3},", 2.0 * 372.0 / 564.0)));
    }

    #[test]
    fn minimum_segment_packets_is_preserved_at_construction() {
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 3,
                window_segments: 2,
                segment_duration_ms: 1000,
            },
            None,
        );

        assert_eq!(p.max_segment_bytes, 3 * TS_PACKET);
    }

    #[test]
    fn segments_replace_upstream_sdt_with_titled_sdt() {
        use ace_media::mpegts::read_sdt_service_name;
        let p = HlsPackager::new(
            HlsConfig {
                segment_packets: 100,
                window_segments: 4,
                segment_duration_ms: 1000,
            },
            Some("HLS Title".to_string()),
        );
        p.feed(&pat(PMT_PID));
        p.feed(&pmt(PMT_PID, VIDEO_PID));
        p.feed(&video_access_packet(VIDEO_PID, 0)); // opens segment 0 with [PAT][PMT][SDT]
        p.feed(&with_pid(packets(1), 0x0011)); // upstream SDT-PID packet -> must be filtered
        p.feed(&video_access_packet(VIDEO_PID, 108_000)); // 1.2s elapsed -> emits segment 0
        let seg = p.segment(0).expect("a segment should be emitted");
        // The injected SDT carries the title.
        assert_eq!(read_sdt_service_name(&seg).as_deref(), Some("HLS Title"));
        // Exactly one SDT-PID packet in the segment: ours. The upstream one was dropped.
        let sdt_packets = seg
            .chunks(TS_PACKET)
            .filter(|c| {
                c.len() == TS_PACKET && ((((c[1] & 0x1f) as u16) << 8) | c[2] as u16) == 0x0011
            })
            .count();
        assert_eq!(sdt_packets, 1);
    }
}
