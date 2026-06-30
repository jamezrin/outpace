//! MPEG-TS packet basics (pure).
//!
//! An MPEG-TS stream is a sequence of fixed 188-byte packets, each starting with the
//! sync byte `0x47`. Before re-exposing or segmenting we must confirm the byte stream is
//! packet-aligned (and find the alignment offset if it isn't).

/// MPEG-TS sync byte at the start of every packet.
pub const TS_SYNC: u8 = 0x47;
/// MPEG-TS packet length in bytes.
pub const TS_PACKET_LEN: usize = 188;

/// Number of consecutive packets to check when probing for sync alignment.
const PROBE_PACKETS: usize = 4;

/// Find the smallest offset `0..188` at which `buf` is TS-packet-aligned — i.e. the sync
/// byte `0x47` recurs every 188 bytes for up to [`PROBE_PACKETS`] packets. Returns `None`
/// if no alignment is found (not a TS stream / too short).
pub fn find_sync_offset(buf: &[u8]) -> Option<usize> {
    for off in 0..TS_PACKET_LEN {
        if off >= buf.len() {
            break;
        }
        let mut ok = true;
        let mut checked = 0;
        let mut pos = off;
        while pos < buf.len() && checked < PROBE_PACKETS {
            if buf[pos] != TS_SYNC {
                ok = false;
                break;
            }
            pos += TS_PACKET_LEN;
            checked += 1;
        }
        if ok && checked > 0 {
            return Some(off);
        }
    }
    None
}

/// True iff `buf` is a whole number of TS packets and every packet starts with `0x47`.
pub fn is_aligned(buf: &[u8]) -> bool {
    if buf.is_empty() || !buf.len().is_multiple_of(TS_PACKET_LEN) {
        return false;
    }
    buf.chunks_exact(TS_PACKET_LEN).all(|p| p[0] == TS_SYNC)
}

/// Number of complete TS packets in `buf` (truncating any trailing partial packet).
pub fn packet_count(buf: &[u8]) -> usize {
    buf.len() / TS_PACKET_LEN
}

/// Stateful TS packet re-aligner for a continuous byte stream that may contain misaligned
/// regions (e.g. Acestream live pieces, which are each internally 188-aligned but don't
/// byte-chain — ~one partial packet of junk at each piece boundary). Feed arbitrary byte
/// runs; it emits only sync-locked 188-byte packets, discarding bytes as needed to re-lock,
/// and buffers the unconfirmed tail until more data arrives.
#[derive(Default)]
pub struct TsResync {
    buf: Vec<u8>,
}

impl TsResync {
    pub fn new() -> Self {
        TsResync { buf: Vec::new() }
    }

    /// Append `data`; return all newly emittable sync-locked packets (a whole number of
    /// 188-byte packets, each starting with `0x47`). Sync is confirmed with one packet of
    /// lookahead (`0x47` at the candidate AND at +188), so output trails input by ≤1 packet.
    pub fn push(&mut self, data: &[u8]) -> Vec<u8> {
        self.buf.extend_from_slice(data);
        let n = self.buf.len();
        let mut out = Vec::new();
        let mut i = 0;
        while i + 2 * TS_PACKET_LEN <= n {
            if self.buf[i] == TS_SYNC && self.buf[i + TS_PACKET_LEN] == TS_SYNC {
                out.extend_from_slice(&self.buf[i..i + TS_PACKET_LEN]);
                i += TS_PACKET_LEN;
            } else {
                // Lost lock: scan forward to the next confirmable packet start.
                let mut j = i + 1;
                while j + TS_PACKET_LEN < n
                    && !(self.buf[j] == TS_SYNC && self.buf[j + TS_PACKET_LEN] == TS_SYNC)
                {
                    j += 1;
                }
                if j + TS_PACKET_LEN < n {
                    i = j;
                } else {
                    break; // can't confirm yet; keep from i for the next push
                }
            }
        }
        self.buf.drain(0..i);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(fill: u8) -> Vec<u8> {
        let mut p = vec![fill; TS_PACKET_LEN];
        p[0] = TS_SYNC;
        p
    }

    #[test]
    fn aligned_stream_has_sync_offset_zero() {
        let mut s = packet(0);
        s.extend(packet(1));
        assert_eq!(find_sync_offset(&s), Some(0));
        assert!(is_aligned(&s));
        assert_eq!(packet_count(&s), 2);
    }

    #[test]
    fn finds_offset_when_stream_is_shifted() {
        let mut s = vec![0xAA, 0xBB, 0xCC]; // 3 junk bytes before the first packet
        s.extend(packet(0));
        s.extend(packet(1));
        assert_eq!(find_sync_offset(&s), Some(3));
        assert!(!is_aligned(&s)); // length isn't a packet multiple from offset 0
    }

    #[test]
    fn rejects_non_ts_garbage() {
        let junk = vec![0u8; 600];
        assert_eq!(find_sync_offset(&junk), None);
        assert!(!is_aligned(&junk));
    }

    #[test]
    fn is_aligned_false_on_partial_packet() {
        let mut s = packet(0);
        s.truncate(100);
        assert!(!is_aligned(&s));
    }

    #[test]
    fn resync_passes_through_already_aligned_stream() {
        let mut r = TsResync::new();
        let mut s = packet(1);
        s.extend(packet(2));
        s.extend(packet(3)); // 3 packets; lookahead holds back the last
        let out = r.push(&s);
        assert!(is_aligned(&out));
        assert_eq!(packet_count(&out), 2);
        // the held-back final packet flushes once another arrives
        let out2 = r.push(&packet(4));
        assert!(is_aligned(&out2));
    }

    #[test]
    fn resync_drops_junk_between_aligned_runs() {
        // Two aligned packets, then 96 bytes of boundary junk, then aligned packets again
        // (mimics the Acestream −per-piece drift).
        let mut r = TsResync::new();
        let mut s = packet(1);
        s.extend(packet(2));
        s.extend(vec![0x00u8; 96]); // misaligned boundary bytes
        for k in 0..4 {
            s.extend(packet(10 + k));
        }
        let out = r.push(&s);
        assert!(is_aligned(&out), "output must be packet-aligned");
        // every emitted packet starts with sync; junk was discarded
        assert!(out.chunks_exact(TS_PACKET_LEN).all(|p| p[0] == TS_SYNC));
        // we keep most packets (6 in, minus ≤1 lookahead) — junk doesn't corrupt the run
        assert!(packet_count(&out) >= 4);
    }

    #[test]
    fn resync_handles_split_pushes() {
        let mut r = TsResync::new();
        let mut s = packet(1);
        s.extend(packet(2));
        s.extend(packet(3));
        // feed in arbitrary fragments
        let mut out = r.push(&s[..50]);
        out.extend(r.push(&s[50..200]));
        out.extend(r.push(&s[200..]));
        out.extend(r.push(&packet(4))); // flush trailer
        assert!(is_aligned(&out));
        assert!(packet_count(&out) >= 3);
    }
}
