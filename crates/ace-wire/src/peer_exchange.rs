//! Acestream peer-exchange gossip (custom peer message `id=12`).
//!
//! A connected peer periodically pushes a list of other peers it knows about. Layout
//! (recovered from live captures — see `tests/vectors/peer-exchange/`):
//!
//! ```text
//! header (16 bytes): [u8 stream][u16 count][u32 =17][u32 record_size=108][5 bytes]
//! then `count` fixed-size records; each record carries, at a fixed offset:
//!   [4-byte IPv4][u16 port]   (offset 11)   plus the peer's R30-… peer-id and its
//!                                            live-window piece positions (unused here)
//! ```
//!
//! We only extract `(ip, port)` — the peer's live window is re-read from its extended
//! handshake once we actually connect, so the address is the whole value here: a free,
//! swarm-sourced supply of fresh upstreams near the current live edge.

use std::net::{Ipv4Addr, SocketAddrV4};

/// Byte offset of the fields we read within a peer-exchange message.
const HEADER_LEN: usize = 16;
const COUNT_OFFSET: usize = 1; // u16
const RECORD_SIZE_OFFSET: usize = 7; // u32
const IP_OFFSET_IN_RECORD: usize = 11; // 4 bytes, then u16 port

/// Parse an `id=12` peer-exchange payload into the peer addresses it advertises. Returns an
/// empty vec for anything that doesn't match the expected self-describing layout (never
/// panics on malformed input). Addresses with a zero IP or port are skipped.
pub fn parse_peer_exchange(payload: &[u8]) -> Vec<SocketAddrV4> {
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
        let base = HEADER_LEN + i * record_size + IP_OFFSET_IN_RECORD;
        let ip = Ipv4Addr::new(
            payload[base],
            payload[base + 1],
            payload[base + 2],
            payload[base + 3],
        );
        let port = u16::from_be_bytes([payload[base + 4], payload[base + 5]]);
        if ip.is_unspecified() || port == 0 {
            continue;
        }
        out.push(SocketAddrV4::new(ip, port));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
