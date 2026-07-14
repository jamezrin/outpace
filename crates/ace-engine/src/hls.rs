//! Live HLS packaging over a session's TS broadcast. PCR is used as the media clock and cuts
//! prefer packets carrying the random-access indicator (normally the start of a video GOP).
//! Packet-count segmentation remains as a bounded fallback for streams without usable PCR.

use crate::config::HlsConfig;
use crate::session::{StreamEvent, StreamSession};
use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const TS_PACKET: usize = 188;
const PCR_HZ: u64 = 90_000;
const PCR_MODULUS: u64 = 1 << 33;

pub struct HlsPackager {
    state: Mutex<HlsState>,
    /// Hard packet ceiling for every segment. PCR/random-access timing may cut earlier, but never
    /// later, so configuration-time retained-memory calculations match the runtime invariant.
    seg_packets: usize,
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
}

struct HlsSegment {
    bytes: Bytes,
    duration: f32,
    discontinuity: bool,
}

impl HlsPackager {
    fn new(config: HlsConfig) -> Arc<HlsPackager> {
        let seg_packets = config.segment_packets.max(1);
        let max_segment_bytes = seg_packets
            .checked_mul(TS_PACKET)
            .expect("HLS config must be validated before packager construction");
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
            }),
            seg_packets,
            max_segment_bytes,
            window: config.window_segments.max(1),
            seg_duration: config.segment_duration_secs(),
        })
    }

    /// Start packaging `session`'s TS into segments in the background (uncounted receiver).
    pub fn start(session: &StreamSession, config: HlsConfig) -> Arc<HlsPackager> {
        let me = Self::new(config);
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
    }

    /// Append contiguous TS bytes, emitting PCR-timed, preferably random-access-aligned segments.
    fn feed(&self, data: &[u8]) {
        let mut st = self.state.lock().unwrap();
        let mut remaining = data;
        while !remaining.is_empty() {
            // Never duplicate more than one configured segment into `cur`, even when a source
            // supplies a very large chunk in one call. `scan` always emits when the buffer reaches
            // this ceiling, so there is room again before the next iteration.
            let room = self.max_segment_bytes - st.cur.len();
            let take = room.min(remaining.len());
            st.cur.extend_from_slice(&remaining[..take]);
            remaining = &remaining[take..];
            self.scan(&mut st);
        }
    }

    fn scan(&self, st: &mut HlsState) {
        while st.scanned_packets < st.cur.len() / TS_PACKET {
            let packet_offset = st.scanned_packets * TS_PACKET;
            let mut timing = ts_timing(&st.cur[packet_offset..packet_offset + TS_PACKET]);

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

            if timing.discontinuity || pcr_went_backwards(st.last_pcr, timing.pcr) {
                if packet_offset > 0 {
                    st.cur.drain(..packet_offset);
                }
                // Keep the flagged packet as the first packet after the gap, but do not inspect
                // its discontinuity flag again on the next loop iteration.
                st.scanned_packets = 1;
                st.segment_start_pcr = timing.pcr;
                st.last_pcr = timing.pcr;
                st.pcr_pid = timing.pcr.map(|_| timing.pid);
                st.discontinuity_pending = true;
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
            // Waiting forever for a broken/missing random-access flag is worse than a less ideal
            // boundary. At twice the target, cut at the next PCR-bearing packet.
            let pcr_fallback =
                timing.pcr.is_some() && elapsed.is_some_and(|d| d >= self.seg_duration * 2.0);
            if packet_offset > 0 && ((target_reached && timing.random_access) || pcr_fallback) {
                let duration = elapsed.unwrap_or(self.seg_duration);
                let boundary_pcr = st.last_pcr;
                self.emit(st, packet_offset, duration);
                st.segment_start_pcr = boundary_pcr;
                st.last_pcr = boundary_pcr;
                continue;
            }
            st.scanned_packets += 1;
            if st.scanned_packets >= self.seg_packets {
                // PCR mode is allowed to wait for a preferable boundary only up to the configured
                // packet ceiling. This is also the fallback for streams with no usable clock.
                let duration = elapsed.unwrap_or(self.seg_duration);
                self.emit(st, self.seg_packets * TS_PACKET, duration);
                st.segment_start_pcr = None;
                st.last_pcr = None;
                st.pcr_pid = None;
            }
        }
    }

    fn emit(&self, st: &mut HlsState, end: usize, duration: f32) {
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
        st.max_segment_duration = st.max_segment_duration.max(duration);
        st.scanned_packets = 0;
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
    random_access: bool,
    discontinuity: bool,
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
        random_access: flags & 0x40 != 0,
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
            seg_bytes: (config.segment_packets.max(1) * TS_PACKET) as u64,
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
        HlsPackager::new(HlsConfig {
            segment_packets: 2,
            window_segments: 3,
            segment_duration_ms: 1000,
        }) // 2 packets/segment, window 3
    }

    fn packets(n: usize) -> Vec<u8> {
        let mut v = vec![0u8; n * TS_PACKET];
        for k in 0..n {
            v[k * TS_PACKET] = 0x47;
        }
        v
    }

    fn timed_packet(pcr: u64, random_access: bool, discontinuity: bool, marker: u8) -> Vec<u8> {
        let mut packet = vec![0xff; TS_PACKET];
        packet[0] = 0x47;
        packet[1] = 0;
        packet[2] = 1;
        packet[3] = 0x20;
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

    fn timed_pkg(duration_ms: u64) -> Arc<HlsPackager> {
        HlsPackager::new(HlsConfig {
            segment_packets: 100,
            window_segments: 6,
            segment_duration_ms: duration_ms,
        })
    }

    fn random_access_packet(marker: u8) -> Vec<u8> {
        let mut packet = packets(1);
        packet[1] = marker & 0x1f;
        packet[3] = 0x20;
        packet[4] = 1;
        packet[5] = 0x40;
        packet
    }

    fn with_pid(mut packet: Vec<u8>, pid: u16) -> Vec<u8> {
        packet[1] = (packet[1] & 0xe0) | ((pid >> 8) as u8 & 0x1f);
        packet[2] = pid as u8;
        packet
    }

    #[test]
    fn playlist_and_valid_segment_reads_refresh_activity() {
        let p = pkg();
        p.feed(&packets(2));
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
        p.feed(&packets(10));
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
        for (pcr, key, marker) in [
            (0, true, 1),
            (45_000, false, 2),
            (108_000, true, 3),
            (153_000, false, 4),
            (216_000, true, 5),
        ] {
            p.feed(&timed_packet(pcr, key, false, marker));
        }

        assert_eq!(p.segment(0).unwrap().len(), 2 * TS_PACKET);
        assert_eq!(p.segment(1).unwrap()[12], 3);
        let playlist = p.playlist("test", "timed");
        assert_eq!(playlist.matches("#EXTINF:1.200,").count(), 2);
        assert!(playlist.contains("#EXT-X-TARGETDURATION:2"));
    }

    #[test]
    fn default_ceiling_allows_a_timed_keyframe_beyond_256_packets() {
        let p = HlsPackager::new(HlsConfig::default());
        let input: Vec<u8> = (0..=300)
            .flat_map(|i| timed_packet(i * 360, i == 300, false, i as u8))
            .collect();

        p.feed(&input);

        assert_eq!(p.segment(0).unwrap().len(), 300 * TS_PACKET);
        assert!(p
            .playlist("test", "default-ceiling")
            .contains("#EXTINF:1.200,"));
    }

    #[test]
    fn pcr_mode_never_exceeds_the_hard_packet_ceiling() {
        let p = HlsPackager::new(HlsConfig {
            segment_packets: 3,
            window_segments: 4,
            segment_duration_ms: 60_000,
        });
        let input: Vec<u8> = (0..7)
            .flat_map(|i| timed_packet(i * 9_000, false, false, i as u8))
            .collect();
        p.feed(&input);

        assert_eq!(p.segment(0).unwrap().len(), 3 * TS_PACKET);
        assert_eq!(p.segment(1).unwrap().len(), 3 * TS_PACKET);
        let st = p.state.lock().unwrap();
        assert_eq!(st.cur.len(), TS_PACKET);
        assert!(st
            .segments
            .iter()
            .all(|segment| segment.bytes.len() <= p.max_segment_bytes));
    }

    #[test]
    fn hard_ceiling_preserves_post_discontinuity_boundary() {
        let p = HlsPackager::new(HlsConfig {
            segment_packets: 2,
            window_segments: 4,
            segment_duration_ms: 60_000,
        });
        p.feed(&timed_packet(0, false, false, 1));
        p.discontinuity();
        let post_gap: Vec<u8> = (0..3)
            .flat_map(|i| timed_packet(i * 9_000, false, false, 10 + i as u8))
            .collect();
        p.feed(&post_gap);

        let first = p.segment(0).unwrap();
        assert_eq!(first.len(), 2 * TS_PACKET);
        assert_eq!(first[12], 10);
        assert_eq!(p.state.lock().unwrap().cur[12], 12);
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
        p.feed(&timed_packet(0, false, false, 1));
        p.feed(&timed_packet(108_000, false, false, 2));
        p.feed(&random_access_packet(3));
        p.feed(&timed_packet(216_000, false, false, 4));
        p.feed(&random_access_packet(5));

        assert_eq!(p.segment(0).unwrap().len(), 2 * TS_PACKET);
        assert_eq!(p.segment(1).unwrap()[1] & 0x1f, 3);
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
        p.feed(&timed_packet(start, true, false, 1));
        p.feed(&timed_packet(63_000, true, false, 2));
        p.feed(&timed_packet(171_000, true, false, 3));

        let playlist = p.playlist("test", "wrap");
        assert!(playlist.contains("#EXTINF:1.200,"));
        assert_eq!(playlist.matches("#EXT-X-DISCONTINUITY").count(), 0);
    }

    #[test]
    fn transport_discontinuity_discards_partial_segment_and_marks_next_one() {
        let p = timed_pkg(1000);
        p.feed(&timed_packet(0, true, false, 1));
        p.feed(&timed_packet(45_000, false, false, 2));
        p.feed(&timed_packet(900_000, true, true, 3));
        p.feed(&timed_packet(1_008_000, true, false, 4));

        assert_eq!(p.segment(0).unwrap()[12], 3);
        let playlist = p.playlist("test", "disc");
        assert!(playlist.contains("#EXT-X-DISCONTINUITY\n#EXTINF:1.200,"));
    }

    #[test]
    fn packet_fallback_resumes_when_pcr_disappears() {
        let p = HlsPackager::new(HlsConfig {
            segment_packets: 3,
            window_segments: 6,
            segment_duration_ms: 1000,
        });
        p.feed(&timed_packet(0, false, false, 1));
        p.feed(&packets(6));

        assert_eq!(p.segment(0).unwrap().len(), 3 * TS_PACKET);
        assert_eq!(p.segment(1).unwrap().len(), 3 * TS_PACKET);
    }

    #[test]
    fn unrelated_pcr_pid_does_not_corrupt_the_selected_clock() {
        let p = timed_pkg(1000);
        p.feed(&with_pid(timed_packet(0, false, false, 1), 100));
        // A numerically unrelated clock on another program must not look like a backwards jump.
        p.feed(&with_pid(
            timed_packet(PCR_MODULUS - 1, false, false, 2),
            200,
        ));
        p.feed(&with_pid(timed_packet(108_000, true, false, 3), 100));
        p.feed(&with_pid(timed_packet(216_000, true, false, 4), 100));

        let playlist = p.playlist("test", "multiprogram");
        assert_eq!(playlist.matches("#EXT-X-DISCONTINUITY").count(), 0);
        assert_eq!(playlist.matches("#EXTINF:1.200,").count(), 2);
    }

    #[test]
    fn target_duration_never_decreases_as_segments_slide() {
        let p = HlsPackager::new(HlsConfig {
            segment_packets: 100,
            window_segments: 1,
            segment_duration_ms: 1000,
        });
        p.feed(&timed_packet(0, false, false, 1));
        p.feed(&timed_packet(180_000, true, false, 2));
        assert!(p
            .playlist("test", "target")
            .contains("#EXT-X-TARGETDURATION:2"));

        p.feed(&timed_packet(270_000, true, false, 3));
        assert!(p
            .playlist("test", "target")
            .contains("#EXT-X-TARGETDURATION:2"));
    }

    #[test]
    fn fills_segments_and_slides_window() {
        let p = pkg();
        // 10 packets -> 5 segments of 2; window keeps last 3 (seq 2,3,4).
        p.feed(&packets(10));
        let pl = p.playlist("test", "abc");
        assert!(pl.contains("#EXT-X-MEDIA-SEQUENCE:2"));
        assert!(pl.contains("/streams/test/abc/seg/2.ts"));
        assert!(pl.contains("/streams/test/abc/seg/4.ts"));
        assert!(!pl.contains("seg/1.ts"));
        // evicted + future segments are gone; retained ones are 2*188 bytes.
        assert!(p.segment(1).is_none());
        assert_eq!(p.segment(3).unwrap().len(), 2 * TS_PACKET);
        assert!(p.segment(99).is_none());
    }

    #[test]
    fn alternate_playlist_prefix_reuses_native_sequence_and_discontinuity_state() {
        let p = pkg();
        p.feed(&packets(2));
        p.discontinuity();
        p.feed(&packets(2));

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
        p.feed(&packets(1)); // not enough for a 2-packet segment
        assert!(p.playlist("test", "x").lines().all(|l| !l.contains("seg/")));
        p.feed(&packets(1));
        assert!(p.playlist("test", "x").contains("seg/0.ts"));
    }

    #[test]
    fn receiver_lag_discards_partial_media_and_marks_only_first_post_gap_segment() {
        let p = pkg();
        let pre_gap = vec![0x11; TS_PACKET];
        p.feed(&pre_gap);
        p.discontinuity();
        let post_gap = vec![0x22; 4 * TS_PACKET];
        p.feed(&post_gap);

        assert_eq!(p.segment(0).unwrap().as_ref(), &post_gap[..2 * TS_PACKET]);
        assert_eq!(p.segment(1).unwrap().as_ref(), &post_gap[2 * TS_PACKET..]);
        let playlist = p.playlist("test", "gap");
        assert_eq!(playlist.matches("#EXT-X-DISCONTINUITY").count(), 1);
        assert!(
            playlist.contains("#EXT-X-DISCONTINUITY\n#EXTINF:1.000,\n/streams/test/gap/seg/0.ts")
        );
    }

    #[test]
    fn discontinuity_metadata_slides_with_its_segment() {
        let p = pkg();
        p.feed(&packets(2));
        p.discontinuity();
        p.feed(&packets(6));
        let retained = p.playlist("test", "slide");
        assert!(retained.contains("#EXT-X-MEDIA-SEQUENCE:1"));
        assert_eq!(retained.matches("#EXT-X-DISCONTINUITY").count(), 1);

        p.feed(&packets(6));
        let evicted = p.playlist("test", "slide");
        assert!(evicted.contains("#EXT-X-MEDIA-SEQUENCE:4"));
        assert_eq!(evicted.matches("#EXT-X-DISCONTINUITY").count(), 0);
    }

    #[test]
    fn repeated_gaps_keep_distinct_markers_and_monotonic_sequences() {
        let p = HlsPackager::new(HlsConfig {
            segment_packets: 1,
            window_segments: 5,
            segment_duration_ms: 1000,
        });
        p.feed(&packets(1));
        p.discontinuity();
        p.feed(&packets(1));
        p.discontinuity();
        p.feed(&packets(1));

        let playlist = p.playlist("test", "repeat");
        assert_eq!(playlist.matches("#EXT-X-DISCONTINUITY").count(), 2);
        assert!(playlist.contains("seg/0.ts"));
        assert!(playlist.contains("seg/1.ts"));
        assert!(playlist.contains("seg/2.ts"));
        assert_eq!(p.segment(0).unwrap().len(), TS_PACKET);
        assert_eq!(p.segment(2).unwrap().len(), TS_PACKET);
    }

    #[test]
    fn configured_hls_settings_control_playlist_and_segments() {
        let p = HlsPackager::new(HlsConfig {
            segment_packets: 1,
            window_segments: 2,
            segment_duration_ms: 2500,
        });
        p.feed(&packets(3));

        let pl = p.playlist("test", "abc");

        assert!(pl.contains("#EXT-X-TARGETDURATION:3"));
        assert!(pl.contains("#EXTINF:2.500,"));
        assert!(pl.contains("#EXT-X-MEDIA-SEQUENCE:1"));
        assert!(p.segment(0).is_none());
        assert_eq!(p.segment(2).unwrap().len(), TS_PACKET);
    }

    fn vod_config() -> HlsConfig {
        // 1 packet/segment => 188-byte segments, 2.0s nominal per full segment.
        HlsConfig {
            segment_packets: 1,
            window_segments: 6,
            segment_duration_ms: 2000,
        }
    }

    #[test]
    fn vod_segment_count_is_ceil_division_of_total_over_segment_bytes() {
        // 188-byte segments.
        assert_eq!(VodHlsLayout::new(0, vod_config()).segment_count(), 0);
        assert_eq!(VodHlsLayout::new(1, vod_config()).segment_count(), 1);
        assert_eq!(VodHlsLayout::new(188, vod_config()).segment_count(), 1);
        assert_eq!(VodHlsLayout::new(189, vod_config()).segment_count(), 2);
        assert_eq!(VodHlsLayout::new(188 * 3, vod_config()).segment_count(), 3);
    }

    #[test]
    fn vod_segment_range_is_aligned_with_a_clamped_final_segment() {
        // 500 bytes over 188-byte segments => segments [0,188), [188,376), [376,500).
        let layout = VodHlsLayout::new(500, vod_config());
        assert_eq!(layout.segment_range(0), Some((0, 187)));
        assert_eq!(layout.segment_range(1), Some((188, 375)));
        assert_eq!(layout.segment_range(2), Some((376, 499))); // clamped, shorter
        assert_eq!(layout.segment_range(3), None); // past the last segment
    }

    #[test]
    fn vod_playlist_is_a_terminated_vod_list_of_every_segment() {
        // 500 bytes => 3 segments; last is 124/188 of a full 2.0s segment.
        let pl = VodHlsLayout::new(500, vod_config()).playlist("memvod", "abc");
        assert!(pl.contains("#EXT-X-PLAYLIST-TYPE:VOD"));
        assert!(pl.contains("#EXT-X-TARGETDURATION:2"));
        assert!(pl.contains("#EXT-X-MEDIA-SEQUENCE:0"));
        assert!(pl.contains("/vod/memvod/abc/seg/0.ts"));
        assert!(pl.contains("/vod/memvod/abc/seg/2.ts"));
        assert!(!pl.contains("/vod/memvod/abc/seg/3.ts"));
        assert!(pl.trim_end().ends_with("#EXT-X-ENDLIST"));
        // Full segments advertise the nominal 2.0s; the trailing partial is scaled by bytes.
        assert!(pl.contains("#EXTINF:2.000,"));
        assert!(pl.contains(&format!("#EXTINF:{:.3},", 2.0 * 124.0 / 188.0)));
    }

    #[test]
    fn zero_segment_packets_is_normalized_at_construction() {
        let p = HlsPackager::new(HlsConfig {
            segment_packets: 0,
            window_segments: 2,
            segment_duration_ms: 1000,
        });

        assert_eq!(p.seg_packets, 1);
    }
}
