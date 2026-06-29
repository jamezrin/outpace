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
}
