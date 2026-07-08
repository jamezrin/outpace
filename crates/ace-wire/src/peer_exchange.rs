//! Acestream peer-exchange gossip (custom peer message `id=12`).
//!
//! A connected peer periodically pushes a list of other peers it knows about. Layout
//! (recovered from live captures — see `tests/vectors/peer-exchange/`):
//!
//! ```text
//! header (16 bytes): [u8 stream][u16 count][u32 =17][u32 record_size=108][5 bytes]
//! then `count` fixed-size records; each record carries, at fixed offsets:
//!   [4-byte IPv4][u16 port]   (offset 11)   the peer's R30-… peer-id, and its live-window
//!   [u32 min_piece]           (offset 27)   piece positions: the oldest piece it still
//!   [u32 position]            (offset 83)   retains and the newest piece it holds.
//! ```
//!
//! [`parse_peer_exchange`] extracts just `(ip, port)` — a free, swarm-sourced supply of fresh
//! upstreams near the current live edge. [`parse_peer_exchange_detailed`] additionally reads
//! each record's `[min_piece, position]` live window so [`rank_by_window_coverage`] can try
//! peers that can already serve the current playback cursor first. The window here is only a
//! ranking hint; the authoritative window is re-read from a peer's extended handshake once we
//! actually connect.

use std::net::{Ipv4Addr, SocketAddrV4};

/// Byte offset of the fields we read within a peer-exchange message.
const HEADER_LEN: usize = 16;
const COUNT_OFFSET: usize = 1; // u16
const RECORD_SIZE_OFFSET: usize = 7; // u32
const IP_OFFSET_IN_RECORD: usize = 11; // 4 bytes, then u16 port

// The live-window piece positions each record also carries (see module docs; recovered from
// the `tests/vectors/peer-exchange/` captures). Both are big-endian u32 at fixed offsets:
//   `min_piece` — the oldest piece still in the peer's live window (its window start).
//   `position`  — the newest piece the peer currently holds (its window end).
const MIN_PIECE_OFFSET_IN_RECORD: usize = 27;
const POSITION_OFFSET_IN_RECORD: usize = 83;
/// Smallest record that still contains the `position` field; shorter records have no window.
const WINDOW_END_IN_RECORD: usize = POSITION_OFFSET_IN_RECORD + 4;

/// The inclusive piece range `[min, max]` a peer advertises it can currently serve, read from
/// its `id=12` peer-exchange record: `min` is the window start (oldest retained piece), `max`
/// is the peer's current download position (newest piece it holds).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PieceWindow {
    pub min: u64,
    pub max: u64,
}

impl PieceWindow {
    /// Whether this window already covers `piece` — i.e. the peer can serve it right now.
    pub fn covers(&self, piece: u64) -> bool {
        self.min <= piece && piece <= self.max
    }
}

/// A peer advertised in an `id=12` peer-exchange message, with the live window its record
/// carries. `window` is `None` when the record advertises no usable window (a `0` or
/// `0xffffffff` sentinel, an inverted range, or a record too short to hold the field) — the
/// peer is still a valid candidate, just without a ranking hint. The authoritative window is
/// re-read from the peer's extended handshake once we actually connect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PexPeer {
    pub addr: SocketAddrV4,
    pub window: Option<PieceWindow>,
}

/// Parse an `id=12` peer-exchange payload into the peer addresses it advertises. Returns an
/// empty vec for anything that doesn't match the expected self-describing layout (never
/// panics on malformed input). Addresses with a zero IP or port are skipped.
pub fn parse_peer_exchange(payload: &[u8]) -> Vec<SocketAddrV4> {
    parse_peer_exchange_detailed(payload)
        .into_iter()
        .map(|p| p.addr)
        .collect()
}

/// Like [`parse_peer_exchange`], but also extracts each record's advertised live window so
/// callers can rank candidates by which peers can already serve the current playback cursor
/// (see [`rank_by_window_coverage`]).
pub fn parse_peer_exchange_detailed(payload: &[u8]) -> Vec<PexPeer> {
    if payload.len() < HEADER_LEN {
        return Vec::new();
    }
    let count = u16::from_be_bytes([payload[COUNT_OFFSET], payload[COUNT_OFFSET + 1]]) as usize;
    let record_size = u32::from_be_bytes([
        payload[RECORD_SIZE_OFFSET],
        payload[RECORD_SIZE_OFFSET + 1],
        payload[RECORD_SIZE_OFFSET + 2],
        payload[RECORD_SIZE_OFFSET + 3],
    ]) as usize;
    // A record must at least reach the IP+port field, and the message must actually hold the
    // advertised records — otherwise the layout isn't what we think and we bail rather than
    // misread arbitrary bytes as addresses.
    if record_size < IP_OFFSET_IN_RECORD + 6 {
        return Vec::new();
    }
    let available = (payload.len() - HEADER_LEN) / record_size;
    let n = count.min(available);
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let record = &payload[HEADER_LEN + i * record_size..HEADER_LEN + (i + 1) * record_size];
        let ip = Ipv4Addr::new(
            record[IP_OFFSET_IN_RECORD],
            record[IP_OFFSET_IN_RECORD + 1],
            record[IP_OFFSET_IN_RECORD + 2],
            record[IP_OFFSET_IN_RECORD + 3],
        );
        let port = u16::from_be_bytes([
            record[IP_OFFSET_IN_RECORD + 4],
            record[IP_OFFSET_IN_RECORD + 5],
        ]);
        if ip.is_unspecified() || port == 0 {
            continue;
        }
        out.push(PexPeer {
            addr: SocketAddrV4::new(ip, port),
            window: parse_window(record),
        });
    }
    out
}

/// Read the `[min_piece, position]` live window from a fixed-size peer-exchange record, or
/// `None` when the record is too short or advertises the `0` / `0xffffffff` "unknown" sentinel
/// (or an inverted range) rather than a real window.
fn parse_window(record: &[u8]) -> Option<PieceWindow> {
    if record.len() < WINDOW_END_IN_RECORD {
        return None;
    }
    let read = |at: usize| {
        u32::from_be_bytes([record[at], record[at + 1], record[at + 2], record[at + 3]]) as u64
    };
    let min = read(MIN_PIECE_OFFSET_IN_RECORD);
    let max = read(POSITION_OFFSET_IN_RECORD);
    let unknown = |v: u64| v == 0 || v == u32::MAX as u64;
    if unknown(min) || unknown(max) || max < min {
        return None;
    }
    Some(PieceWindow { min, max })
}

/// Order peer-exchange candidates so the ones most likely to serve the current playback cursor
/// come first. Peers whose advertised window already covers `next_needed` rank ahead of peers
/// with no usable window metadata (still eligible as fallback — the window is only re-read
/// authoritatively once connected), which in turn rank ahead of peers whose window is known but
/// does not cover `next_needed`. Ordering is stable within each group, preserving the swarm's
/// original advertised order. No candidate is dropped.
pub fn rank_by_window_coverage(peers: &[PexPeer], next_needed: u64) -> Vec<SocketAddrV4> {
    let mut covering = Vec::new();
    let mut unknown = Vec::new();
    let mut not_covering = Vec::new();
    for p in peers {
        match p.window {
            Some(w) if w.covers(next_needed) => covering.push(p.addr),
            Some(_) => not_covering.push(p.addr),
            None => unknown.push(p.addr),
        }
    }
    covering.reserve(unknown.len() + not_covering.len());
    covering.extend(unknown);
    covering.extend(not_covering);
    covering
}

/// Byte offset of the IPv4 in a single-peer announce (`id=36`); the port `u16` follows it.
const ANNOUNCE_IP_OFFSET: usize = 8;

/// Parse an `id=36` single-peer announce (the stream's source-node descriptor) into its
/// address. Returns `None` if the buffer is too short or the address is a zero IP/port.
pub fn parse_peer_announce(payload: &[u8]) -> Option<SocketAddrV4> {
    let base = ANNOUNCE_IP_OFFSET;
    if payload.len() < base + 6 {
        return None;
    }
    let ip = Ipv4Addr::new(
        payload[base],
        payload[base + 1],
        payload[base + 2],
        payload[base + 3],
    );
    let port = u16::from_be_bytes([payload[base + 4], payload[base + 5]]);
    if ip.is_unspecified() || port == 0 {
        return None;
    }
    Some(SocketAddrV4::new(ip, port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_source_node_from_a_real_announce() {
        // A real 80-byte id=36 (source-node descriptor): IPv4 at offset 8, port after it.
        let bytes =
            hex("00020034003f004005e7198b272a5233302d2d2d2d2d2d4c00000000000000000000000000000000");
        assert_eq!(
            parse_peer_announce(&bytes).map(|a| a.to_string()),
            Some("5.231.25.139:10026".to_string())
        );
        assert!(parse_peer_announce(&[0u8; 6]).is_none());
    }

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn parses_addresses_from_a_real_capture() {
        // A live 556-byte id=12 message (5 records) captured from a real swarm peer.
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/vectors/peer-exchange/id12-556.bin"
        );
        let bytes = std::fs::read(path).expect("vector present");
        let peers = parse_peer_exchange(&bytes);
        let got: Vec<String> = peers.iter().map(|a| a.to_string()).collect();
        assert_eq!(
            got,
            vec![
                "37.11.110.121:8621",
                "90.173.16.56:8621",
                "87.217.156.180:8621",
                "90.77.1.216:8621",
                "88.26.18.27:8621",
            ]
        );
    }

    #[test]
    fn rejects_truncated_or_nonsense_input() {
        assert!(parse_peer_exchange(&[]).is_empty());
        assert!(parse_peer_exchange(&[0u8; 10]).is_empty());
        // count says 50 records but the buffer only holds a header: parse what's there (none).
        let mut hdr = vec![0u8; 16];
        hdr[1..3].copy_from_slice(&50u16.to_be_bytes());
        hdr[7..11].copy_from_slice(&108u32.to_be_bytes());
        assert!(parse_peer_exchange(&hdr).is_empty());
    }

    #[test]
    fn skips_zero_addresses() {
        // One well-formed record whose IP/port are zero -> skipped.
        let mut msg = vec![0u8; 16 + 108];
        msg[1..3].copy_from_slice(&1u16.to_be_bytes());
        msg[7..11].copy_from_slice(&108u32.to_be_bytes());
        assert!(parse_peer_exchange(&msg).is_empty());
    }

    fn real_capture() -> Vec<u8> {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/vectors/peer-exchange/id12-556.bin"
        );
        std::fs::read(path).expect("vector present")
    }

    #[test]
    fn extracts_live_windows_from_a_real_capture() {
        // The same 5-record capture used above, now read through the detailed parser: each
        // record carries a per-peer live window at fixed offsets (min_piece @27, position @83).
        let peers = parse_peer_exchange_detailed(&real_capture());
        let got: Vec<(String, Option<(u64, u64)>)> = peers
            .iter()
            .map(|p| (p.addr.to_string(), p.window.map(|w| (w.min, w.max))))
            .collect();
        assert_eq!(
            got,
            vec![
                ("37.11.110.121:8621".to_string(), Some((7428906, 7428963))),
                ("90.173.16.56:8621".to_string(), Some((7406250, 7428942))),
                ("87.217.156.180:8621".to_string(), Some((7428906, 7428963))),
                ("90.77.1.216:8621".to_string(), Some((7428906, 7428970))),
                // Record 4 advertises a 0xffffffff (unknown/stale) position: no usable window.
                ("88.26.18.27:8621".to_string(), None),
            ]
        );
    }

    #[test]
    fn detailed_parse_returns_the_same_addresses_as_the_plain_parser() {
        let bytes = real_capture();
        let detailed: Vec<SocketAddrV4> = parse_peer_exchange_detailed(&bytes)
            .into_iter()
            .map(|p| p.addr)
            .collect();
        assert_eq!(detailed, parse_peer_exchange(&bytes));
    }

    fn peer(addr: &str, window: Option<(u64, u64)>) -> PexPeer {
        PexPeer {
            addr: addr.parse().unwrap(),
            window: window.map(|(min, max)| PieceWindow { min, max }),
        }
    }

    #[test]
    fn window_covers_only_within_its_inclusive_range() {
        let w = PieceWindow { min: 100, max: 200 };
        assert!(!w.covers(99));
        assert!(w.covers(100));
        assert!(w.covers(150));
        assert!(w.covers(200));
        assert!(!w.covers(201));
    }

    #[test]
    fn ranking_prefers_peers_whose_window_covers_next_needed() {
        // next_needed = 150. Only "b" and "d" cover it; "a" is behind (already evicted 150),
        // "c" is ahead (window starts after 150). Covering peers must come first.
        let peers = vec![
            peer("10.0.0.1:1", Some((10, 20))),   // behind
            peer("10.0.0.2:2", Some((100, 200))), // covers
            peer("10.0.0.3:3", Some((300, 400))), // ahead
            peer("10.0.0.4:4", Some((150, 160))), // covers (edge)
        ];
        let ranked = rank_by_window_coverage(&peers, 150);
        assert_eq!(ranked[0].to_string(), "10.0.0.2:2");
        assert_eq!(ranked[1].to_string(), "10.0.0.4:4");
        // Non-covering peers are retained after the covering ones (never dropped).
        let rest: Vec<String> = ranked[2..].iter().map(|a| a.to_string()).collect();
        assert!(rest.contains(&"10.0.0.1:1".to_string()));
        assert!(rest.contains(&"10.0.0.3:3".to_string()));
        assert_eq!(ranked.len(), 4);
    }

    #[test]
    fn ranking_keeps_unknown_window_peers_as_fallback_ahead_of_known_non_covering() {
        // A peer with no window metadata stays eligible: ordered after known-covering peers
        // but ahead of peers we know cannot currently serve next_needed.
        let peers = vec![
            peer("10.0.0.1:1", Some((10, 20))),   // known: does not cover 150
            peer("10.0.0.2:2", None),             // unknown: fallback
            peer("10.0.0.3:3", Some((100, 200))), // known: covers 150
        ];
        let ranked = rank_by_window_coverage(&peers, 150);
        assert_eq!(
            ranked.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
            vec!["10.0.0.3:3", "10.0.0.2:2", "10.0.0.1:1"],
        );
    }

    #[test]
    fn ranking_preserves_order_when_no_window_metadata_is_available() {
        // Fallback: with no usable windows at all, ordering is unchanged from the input.
        let peers = vec![
            peer("10.0.0.1:1", None),
            peer("10.0.0.2:2", None),
            peer("10.0.0.3:3", None),
        ];
        let ranked = rank_by_window_coverage(&peers, 150);
        assert_eq!(
            ranked.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
            vec!["10.0.0.1:1", "10.0.0.2:2", "10.0.0.3:3"],
        );
    }

    #[test]
    fn ranking_is_stable_within_each_group() {
        // Two covering peers keep their advertised order relative to each other.
        let peers = vec![
            peer("10.0.0.1:1", Some((100, 200))), // covers
            peer("10.0.0.2:2", Some((100, 200))), // covers
        ];
        let ranked = rank_by_window_coverage(&peers, 150);
        assert_eq!(
            ranked.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
            vec!["10.0.0.1:1", "10.0.0.2:2"],
        );
    }
}
