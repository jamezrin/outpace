//! HLS segmentation (pure): split a TS-packet-aligned byte stream into segments at
//! packet boundaries and render the `.m3u8` media playlist.
//!
//! MVP segments by packet count (a coarse size-based proxy for duration); a later pass
//! can cut on PCR / keyframe boundaries. Segment naming/serving is the engine's job.

use crate::mpegts::{is_aligned, TS_PACKET_LEN};
use crate::{MediaError, Result};

/// Split TS-aligned `ts` into segments of `packets_per_segment` packets each (the final
/// segment may be shorter). Errors if `ts` isn't packet-aligned or the count is zero.
pub fn segment(ts: &[u8], packets_per_segment: usize) -> Result<Vec<&[u8]>> {
    if packets_per_segment == 0 {
        return Err(MediaError::BadParam("packets_per_segment must be > 0"));
    }
    if !is_aligned(ts) {
        return Err(MediaError::NotTsAligned);
    }
    let seg_len = packets_per_segment * TS_PACKET_LEN;
    Ok(ts.chunks(seg_len).collect())
}

/// Render an HLS media playlist. `durations` is per-segment seconds (one per segment);
/// `media_sequence` is the sequence number of the first listed segment; `live` controls
/// whether `#EXT-X-ENDLIST` is appended (omit for a live sliding window). Segments are
/// named `seg{media_sequence + i}.ts`.
pub fn media_playlist(durations: &[f32], media_sequence: u64, live: bool) -> String {
    let target = durations
        .iter()
        .cloned()
        .fold(0.0f32, f32::max)
        .ceil()
        .max(1.0) as u64;
    let mut m = String::new();
    m.push_str("#EXTM3U\n");
    m.push_str("#EXT-X-VERSION:3\n");
    m.push_str(&format!("#EXT-X-TARGETDURATION:{target}\n"));
    m.push_str(&format!("#EXT-X-MEDIA-SEQUENCE:{media_sequence}\n"));
    for (i, d) in durations.iter().enumerate() {
        m.push_str(&format!("#EXTINF:{:.3},\n", d));
        m.push_str(&format!("seg{}.ts\n", media_sequence + i as u64));
    }
    if !live {
        m.push_str("#EXT-X-ENDLIST\n");
    }
    m
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mpegts::TS_SYNC;

    fn ts(packets: usize) -> Vec<u8> {
        let mut v = Vec::new();
        for i in 0..packets {
            let mut p = vec![i as u8; TS_PACKET_LEN];
            p[0] = TS_SYNC;
            v.extend(p);
        }
        v
    }

    #[test]
    fn splits_evenly_at_packet_boundaries() {
        let t = ts(4);
        let segs = segment(&t, 2).unwrap();
        assert_eq!(segs.len(), 2);
        assert!(segs.iter().all(|s| s.len() == 2 * TS_PACKET_LEN));
    }

    #[test]
    fn final_segment_may_be_shorter() {
        let t = ts(5);
        let segs = segment(&t, 2).unwrap();
        assert_eq!(segs.len(), 3);
        assert_eq!(segs[2].len(), TS_PACKET_LEN); // leftover single packet
    }

    #[test]
    fn rejects_unaligned_input() {
        assert_eq!(
            segment(&[0x47, 0x00, 0x01], 2),
            Err(MediaError::NotTsAligned)
        );
    }

    #[test]
    fn rejects_zero_packets_per_segment() {
        assert_eq!(
            segment(&ts(2), 0),
            Err(MediaError::BadParam("packets_per_segment must be > 0"))
        );
    }

    #[test]
    fn live_playlist_has_no_endlist_and_correct_headers() {
        let m = media_playlist(&[4.0, 3.5], 7, true);
        assert!(m.starts_with("#EXTM3U\n"));
        assert!(m.contains("#EXT-X-TARGETDURATION:4\n"));
        assert!(m.contains("#EXT-X-MEDIA-SEQUENCE:7\n"));
        assert!(m.contains("#EXTINF:4.000,\nseg7.ts\n"));
        assert!(m.contains("#EXTINF:3.500,\nseg8.ts\n"));
        assert!(!m.contains("#EXT-X-ENDLIST"));
    }

    #[test]
    fn vod_playlist_ends_with_endlist() {
        let m = media_playlist(&[2.0], 0, false);
        assert!(m.trim_end().ends_with("#EXT-X-ENDLIST"));
    }
}
