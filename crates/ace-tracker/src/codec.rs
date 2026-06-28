//! BEP-15 UDP tracker wire codec. Pure build/parse, no I/O.
use crate::{Result, TrackerError};
use std::net::SocketAddrV4;

/// Magic protocol id for the initial connect handshake (BEP-15).
pub const PROTOCOL_ID: u64 = 0x41727101980;
pub const ACTION_CONNECT: u32 = 0;
pub const ACTION_ANNOUNCE: u32 = 1;
pub const ACTION_ERROR: u32 = 3;
pub const EVENT_STARTED: u32 = 2;

pub fn build_connect_request(txid: u32) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[0..8].copy_from_slice(&PROTOCOL_ID.to_be_bytes());
    b[8..12].copy_from_slice(&ACTION_CONNECT.to_be_bytes());
    b[12..16].copy_from_slice(&txid.to_be_bytes());
    b
}

pub fn parse_connect_response(buf: &[u8], txid: u32) -> Result<u64> {
    if buf.len() < 16 { return Err(TrackerError::Malformed("connect resp < 16")); }
    let action = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let rtxid = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if rtxid != txid { return Err(TrackerError::TransactionMismatch); }
    if action != ACTION_CONNECT { return Err(TrackerError::Malformed("not a connect action")); }
    let mut c = [0u8; 8];
    c.copy_from_slice(&buf[8..16]);
    Ok(u64::from_be_bytes(c))
}

/// BEP-15 transfer counters advertised in an announce.
///
/// Caller-provided so the client reports its real state instead of a hardcoded
/// value. The [`Default`] (all zeros) matches the value validated against the live
/// Acestream tracker during Phase 2 — note `left = 0` advertises "complete" in
/// BEP-15 terms, so revisit once real piece accounting exists.
#[derive(Debug, Clone, Copy, Default)]
pub struct TransferState {
    pub downloaded: u64,
    pub left: u64,
    pub uploaded: u64,
}

#[allow(clippy::too_many_arguments)]
pub fn build_announce_request(
    connection_id: u64, txid: u32, infohash: &[u8; 20], peer_id: &[u8; 20],
    port: u16, num_want: i32, transfer: &TransferState,
) -> Vec<u8> {
    let mut b = Vec::with_capacity(98);
    b.extend_from_slice(&connection_id.to_be_bytes());
    b.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
    b.extend_from_slice(&txid.to_be_bytes());
    b.extend_from_slice(infohash);
    b.extend_from_slice(peer_id);
    b.extend_from_slice(&transfer.downloaded.to_be_bytes());
    b.extend_from_slice(&transfer.left.to_be_bytes());
    b.extend_from_slice(&transfer.uploaded.to_be_bytes());
    b.extend_from_slice(&EVENT_STARTED.to_be_bytes());
    b.extend_from_slice(&0u32.to_be_bytes()); // ip (default)
    b.extend_from_slice(&0u32.to_be_bytes()); // key
    b.extend_from_slice(&num_want.to_be_bytes());
    b.extend_from_slice(&port.to_be_bytes());
    b
}

/// Returns (interval_seconds, peers).
pub fn parse_announce_response(buf: &[u8], txid: u32) -> Result<(u32, Vec<SocketAddrV4>)> {
    if buf.len() < 8 { return Err(TrackerError::Malformed("announce resp < 8")); }
    let action = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let rtxid = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if rtxid != txid { return Err(TrackerError::TransactionMismatch); }
    if action == ACTION_ERROR {
        let msg = String::from_utf8_lossy(&buf[8..]).into_owned();
        return Err(TrackerError::Tracker(msg));
    }
    if action != ACTION_ANNOUNCE { return Err(TrackerError::Malformed("not an announce action")); }
    if buf.len() < 20 { return Err(TrackerError::Malformed("announce header < 20")); }
    let interval = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
    let mut peers = Vec::new();
    let mut i = 20;
    while i + 6 <= buf.len() {
        let ip = std::net::Ipv4Addr::new(buf[i], buf[i + 1], buf[i + 2], buf[i + 3]);
        let port = u16::from_be_bytes([buf[i + 4], buf[i + 5]]);
        peers.push(SocketAddrV4::new(ip, port));
        i += 6;
    }
    Ok((interval, peers))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddrV4;

    #[test]
    fn connect_request_layout() {
        let req = build_connect_request(0x1122_3344);
        assert_eq!(&req[0..8], &0x41727101980u64.to_be_bytes()); // magic protocol id
        assert_eq!(&req[8..12], &0u32.to_be_bytes());            // action = connect
        assert_eq!(&req[12..16], &0x1122_3344u32.to_be_bytes()); // txid
    }

    #[test]
    fn parse_connect_roundtrip() {
        let txid: u32 = 0xAABB_CCDD;
        let mut resp = Vec::new();
        resp.extend_from_slice(&0u32.to_be_bytes());          // action connect
        resp.extend_from_slice(&txid.to_be_bytes());          // txid
        resp.extend_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes()); // conn id
        assert_eq!(parse_connect_response(&resp, txid).unwrap(), 0x0102_0304_0506_0708);
        // wrong txid rejected
        assert!(parse_connect_response(&resp, txid ^ 1).is_err());
    }

    #[test]
    fn announce_request_layout() {
        let req = build_announce_request(0x0102_0304_0506_0708, 0x1111_2222,
            &[0xAB; 20], &[0xCD; 20], 6881, 50, &TransferState::default());
        assert_eq!(req.len(), 98);
        assert_eq!(&req[0..8], &0x0102_0304_0506_0708u64.to_be_bytes()); // conn id
        assert_eq!(&req[8..12], &1u32.to_be_bytes());                    // action announce
        assert_eq!(&req[16..36], &[0xABu8; 20]);                         // infohash
        assert_eq!(&req[36..56], &[0xCDu8; 20]);                         // peer id
        assert_eq!(&req[96..98], &6881u16.to_be_bytes());                // port
    }

    #[test]
    fn announce_request_encodes_caller_transfer_counters() {
        let t = TransferState { downloaded: 0x1122, left: 0x3344, uploaded: 0x5566 };
        let req = build_announce_request(0x0102_0304_0506_0708, 0x1111_2222,
            &[0xAB; 20], &[0xCD; 20], 6881, 50, &t);
        assert_eq!(&req[56..64], &0x1122u64.to_be_bytes()); // downloaded
        assert_eq!(&req[64..72], &0x3344u64.to_be_bytes()); // left
        assert_eq!(&req[72..80], &0x5566u64.to_be_bytes()); // uploaded
    }

    #[test]
    fn parse_announce_peers() {
        let txid: u32 = 0x1111_2222;
        let mut resp = Vec::new();
        resp.extend_from_slice(&1u32.to_be_bytes());   // action announce
        resp.extend_from_slice(&txid.to_be_bytes());   // txid
        resp.extend_from_slice(&1800u32.to_be_bytes()); // interval
        resp.extend_from_slice(&0u32.to_be_bytes());    // leechers
        resp.extend_from_slice(&2u32.to_be_bytes());    // seeders
        resp.extend_from_slice(&[5, 252, 161, 218]); resp.extend_from_slice(&2710u16.to_be_bytes());
        resp.extend_from_slice(&[1, 2, 3, 4]);          resp.extend_from_slice(&8621u16.to_be_bytes());
        let (interval, peers) = parse_announce_response(&resp, txid).unwrap();
        assert_eq!(interval, 1800);
        assert_eq!(peers, vec![
            "5.252.161.218:2710".parse::<SocketAddrV4>().unwrap(),
            "1.2.3.4:8621".parse::<SocketAddrV4>().unwrap(),
        ]);
    }

    #[test]
    fn parse_tracker_error_action() {
        let txid: u32 = 7;
        let mut resp = Vec::new();
        resp.extend_from_slice(&3u32.to_be_bytes()); // action = error
        resp.extend_from_slice(&txid.to_be_bytes());
        resp.extend_from_slice(b"nope");
        assert!(matches!(parse_announce_response(&resp, txid), Err(crate::TrackerError::Tracker(_))));
    }
}
