//! Minimal live HLS packaging over a session's TS broadcast: slice the contiguous TS into
//! fixed-size segments (aligned to 188-byte packets), keep a sliding window in memory, and
//! render a live media playlist. Segmentation is byte-based (not keyframe-aware) — fine for
//! tolerant players; precise GOP segmentation is a documented follow-up.

use crate::config::HlsConfig;
use crate::session::StreamSession;
use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

const TS_PACKET: usize = 188;

pub struct HlsPackager {
    state: Mutex<HlsState>,
    /// Packets per segment (segment size = `seg_packets * 188`).
    seg_packets: usize,
    /// Number of segments retained in the sliding window.
    window: usize,
    /// Nominal segment duration advertised in the playlist (seconds).
    seg_duration: f32,
}

struct HlsState {
    /// Sequence number of the first retained segment.
    media_seq: u64,
    segments: VecDeque<Bytes>,
    cur: Vec<u8>,
}

impl HlsPackager {
    fn new(config: HlsConfig) -> Arc<HlsPackager> {
        Arc::new(HlsPackager {
            state: Mutex::new(HlsState {
                media_seq: 0,
                segments: VecDeque::new(),
                cur: Vec::new(),
            }),
            seg_packets: config.segment_packets.max(1),
            window: config.window_segments,
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
                    Ok(chunk) => pkg.feed(&chunk),
                    Err(RecvError::Lagged(_)) => continue,
                    Err(RecvError::Closed) => break,
                }
            }
        });
        me
    }

    /// Append contiguous TS bytes, emitting segments as they fill.
    fn feed(&self, data: &[u8]) {
        let mut st = self.state.lock().unwrap();
        st.cur.extend_from_slice(data);
        let seg_bytes = self.seg_packets * TS_PACKET;
        while st.cur.len() >= seg_bytes {
            let rest = st.cur.split_off(seg_bytes);
            let seg = std::mem::replace(&mut st.cur, rest);
            st.segments.push_back(Bytes::from(seg));
            while st.segments.len() > self.window {
                st.segments.pop_front();
                st.media_seq += 1;
            }
        }
    }

    /// Render the live media playlist; segment URIs are absolute under the given prefix.
    pub fn playlist(&self, network: &str, id: &str) -> String {
        let st = self.state.lock().unwrap();
        let mut out = String::from("#EXTM3U\n#EXT-X-VERSION:3\n");
        out.push_str(&format!(
            "#EXT-X-TARGETDURATION:{}\n",
            self.seg_duration.ceil() as u64
        ));
        out.push_str(&format!("#EXT-X-MEDIA-SEQUENCE:{}\n", st.media_seq));
        for i in 0..st.segments.len() as u64 {
            out.push_str(&format!("#EXTINF:{:.3},\n", self.seg_duration));
            out.push_str(&format!(
                "/streams/{network}/{id}/seg/{}.ts\n",
                st.media_seq + i
            ));
        }
        out
    }

    /// Fetch a retained segment by its absolute sequence number.
    pub fn segment(&self, seq: u64) -> Option<Bytes> {
        let st = self.state.lock().unwrap();
        if seq < st.media_seq {
            return None;
        }
        st.segments.get((seq - st.media_seq) as usize).cloned()
    }
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
    fn partial_packets_buffer_until_full() {
        let p = pkg();
        p.feed(&packets(1)); // not enough for a 2-packet segment
        assert!(p.playlist("test", "x").lines().all(|l| !l.contains("seg/")));
        p.feed(&packets(1));
        assert!(p.playlist("test", "x").contains("seg/0.ts"));
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
