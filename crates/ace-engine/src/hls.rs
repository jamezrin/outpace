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
