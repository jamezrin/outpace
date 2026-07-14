//! BEP-15 UDP tracker wire codec. Pure build/parse, no I/O.
use crate::{Result, TrackerError};
use std::net::SocketAddrV4;

/// Magic protocol id for the initial connect handshake (BEP-15).
pub const PROTOCOL_ID: u64 = 0x41727101980;
pub const ACTION_CONNECT: u32 = 0;
pub const ACTION_ANNOUNCE: u32 = 1;
pub const ACTION_ERROR: u32 = 3;

/// BEP-15 announce `event` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnounceEvent {
    None = 0,
    Completed = 1,
    Started = 2,
    Stopped = 3,
}

pub fn build_connect_request(txid: u32) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[0..8].copy_from_slice(&PROTOCOL_ID.to_be_bytes());
    b[8..12].copy_from_slice(&ACTION_CONNECT.to_be_bytes());
    b[12..16].copy_from_slice(&txid.to_be_bytes());
    b
}

pub fn parse_connect_response(buf: &[u8], txid: u32) -> Result<u64> {
    if buf.len() < 16 {
        return Err(TrackerError::Malformed("connect resp < 16"));
    }
    let action = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let rtxid = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if rtxid != txid {
        return Err(TrackerError::TransactionMismatch);
    }
    if action != ACTION_CONNECT {
        return Err(TrackerError::Malformed("not a connect action"));
    }
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
    connection_id: u64,
    txid: u32,
    infohash: &[u8; 20],
    peer_id: &[u8; 20],
    port: u16,
    num_want: i32,
    transfer: &TransferState,
    event: AnnounceEvent,
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
    b.extend_from_slice(&(event as u32).to_be_bytes());
    b.extend_from_slice(&0u32.to_be_bytes()); // ip (default)
    b.extend_from_slice(&0u32.to_be_bytes()); // key
    b.extend_from_slice(&num_want.to_be_bytes());
    b.extend_from_slice(&port.to_be_bytes());
    b
}

/// Returns (interval_seconds, peers).
pub fn parse_announce_response(buf: &[u8], txid: u32) -> Result<(u32, Vec<SocketAddrV4>)> {
    if buf.len() < 8 {
        return Err(TrackerError::Malformed("announce resp < 8"));
    }
    let action = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let rtxid = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if rtxid != txid {
        return Err(TrackerError::TransactionMismatch);
    }
    if action == ACTION_ERROR {
        let msg = String::from_utf8_lossy(&buf[8..]).into_owned();
        return Err(TrackerError::Tracker(msg));
    }
    if action != ACTION_ANNOUNCE {
        return Err(TrackerError::Malformed("not an announce action"));
    }
    if buf.len() < 20 {
        return Err(TrackerError::Malformed("announce header < 20"));
    }
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

/// Parsed BEP-15 connect request (server side).
#[derive(Debug, Clone, Copy)]
pub struct ConnectRequest {
    pub txid: u32,
}

pub fn parse_connect_request(buf: &[u8]) -> Result<ConnectRequest> {
    if buf.len() < 16 {
        return Err(TrackerError::Malformed("connect req < 16"));
    }
    let mut magic = [0u8; 8];
    magic.copy_from_slice(&buf[0..8]);
    if u64::from_be_bytes(magic) != PROTOCOL_ID {
        return Err(TrackerError::Malformed("bad protocol id"));
    }
    let action = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
    if action != ACTION_CONNECT {
        return Err(TrackerError::Malformed("not a connect action"));
    }
    let txid = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
    Ok(ConnectRequest { txid })
}

pub fn build_connect_response(txid: u32, connection_id: u64) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[0..4].copy_from_slice(&ACTION_CONNECT.to_be_bytes());
    b[4..8].copy_from_slice(&txid.to_be_bytes());
    b[8..16].copy_from_slice(&connection_id.to_be_bytes());
    b
}

/// Parsed BEP-15 announce request (server side).
#[derive(Debug, Clone, Copy)]
pub struct AnnounceRequest {
    pub connection_id: u64,
    pub txid: u32,
    pub infohash: [u8; 20],
    pub peer_id: [u8; 20],
    pub transfer: TransferState,
    pub event: AnnounceEvent,
    pub key: u32,
    pub num_want: i32,
    pub port: u16,
}

pub fn parse_announce_request(buf: &[u8]) -> Result<AnnounceRequest> {
    if buf.len() < 98 {
        return Err(TrackerError::Malformed("announce req < 98"));
    }
    let mut cid = [0u8; 8];
    cid.copy_from_slice(&buf[0..8]);
    let connection_id = u64::from_be_bytes(cid);
    let action = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
    if action != ACTION_ANNOUNCE {
        return Err(TrackerError::Malformed("not an announce action"));
    }
    let txid = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
    let mut infohash = [0u8; 20];
    infohash.copy_from_slice(&buf[16..36]);
    let mut peer_id = [0u8; 20];
    peer_id.copy_from_slice(&buf[36..56]);
    let mut d = [0u8; 8];
    d.copy_from_slice(&buf[56..64]);
    let mut l = [0u8; 8];
    l.copy_from_slice(&buf[64..72]);
    let mut u = [0u8; 8];
    u.copy_from_slice(&buf[72..80]);
    let transfer = TransferState {
        downloaded: u64::from_be_bytes(d),
        left: u64::from_be_bytes(l),
        uploaded: u64::from_be_bytes(u),
    };
    // event is advisory: unknown values coerce to None instead of dropping the announce
    let event = match u32::from_be_bytes([buf[80], buf[81], buf[82], buf[83]]) {
        1 => AnnounceEvent::Completed,
        2 => AnnounceEvent::Started,
        3 => AnnounceEvent::Stopped,
        _ => AnnounceEvent::None,
    };
    // buf[84..88] ip (ignored)
    let key = u32::from_be_bytes([buf[88], buf[89], buf[90], buf[91]]);
    let num_want = i32::from_be_bytes([buf[92], buf[93], buf[94], buf[95]]);
    let port = u16::from_be_bytes([buf[96], buf[97]]);
    Ok(AnnounceRequest {
        connection_id,
        txid,
        infohash,
        peer_id,
        transfer,
        event,
        key,
        num_want,
        port,
    })
}

pub fn build_announce_response(
    txid: u32,
    interval: u32,
    leechers: u32,
    seeders: u32,
    peers: &[SocketAddrV4],
) -> Vec<u8> {
    let mut b = Vec::with_capacity(20 + peers.len() * 6);
    b.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
    b.extend_from_slice(&txid.to_be_bytes());
    b.extend_from_slice(&interval.to_be_bytes());
    b.extend_from_slice(&leechers.to_be_bytes());
    b.extend_from_slice(&seeders.to_be_bytes());
    for peer in peers {
        b.extend_from_slice(&peer.ip().octets());
        b.extend_from_slice(&peer.port().to_be_bytes());
    }
    b
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddrV4;

    #[test]
    fn connect_request_layout() {
        let req = build_connect_request(0x1122_3344);
        assert_eq!(&req[0..8], &0x41727101980u64.to_be_bytes()); // magic protocol id
        assert_eq!(&req[8..12], &0u32.to_be_bytes()); // action = connect
        assert_eq!(&req[12..16], &0x1122_3344u32.to_be_bytes()); // txid
    }

    #[test]
    fn parse_connect_roundtrip() {
        let txid: u32 = 0xAABB_CCDD;
        let mut resp = Vec::new();
        resp.extend_from_slice(&0u32.to_be_bytes()); // action connect
        resp.extend_from_slice(&txid.to_be_bytes()); // txid
        resp.extend_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes()); // conn id
        assert_eq!(
            parse_connect_response(&resp, txid).unwrap(),
            0x0102_0304_0506_0708
        );
        // wrong txid rejected
        assert!(parse_connect_response(&resp, txid ^ 1).is_err());
    }

    #[test]
    fn announce_request_layout() {
        let req = build_announce_request(
            0x0102_0304_0506_0708,
            0x1111_2222,
            &[0xAB; 20],
            &[0xCD; 20],
            6881,
            50,
            &TransferState::default(),
            AnnounceEvent::Started,
        );
        assert_eq!(req.len(), 98);
        assert_eq!(&req[0..8], &0x0102_0304_0506_0708u64.to_be_bytes()); // conn id
        assert_eq!(&req[8..12], &1u32.to_be_bytes()); // action announce
        assert_eq!(&req[16..36], &[0xABu8; 20]); // infohash
        assert_eq!(&req[36..56], &[0xCDu8; 20]); // peer id
        assert_eq!(&req[96..98], &6881u16.to_be_bytes()); // port
    }

    #[test]
    fn announce_request_encodes_caller_transfer_counters() {
        let t = TransferState {
            downloaded: 0x1122,
            left: 0x3344,
            uploaded: 0x5566,
        };
        let req = build_announce_request(
            0x0102_0304_0506_0708,
            0x1111_2222,
            &[0xAB; 20],
            &[0xCD; 20],
            6881,
            50,
            &t,
            AnnounceEvent::Started,
        );
        assert_eq!(&req[56..64], &0x1122u64.to_be_bytes()); // downloaded
        assert_eq!(&req[64..72], &0x3344u64.to_be_bytes()); // left
        assert_eq!(&req[72..80], &0x5566u64.to_be_bytes()); // uploaded
    }

    #[test]
    fn announce_event_is_encoded_at_the_documented_offset() {
        let t = TransferState {
            downloaded: 0,
            left: 0,
            uploaded: 0,
        };
        let req = build_announce_request(
            1,
            2,
            &[0u8; 20],
            &[0u8; 20],
            6881,
            -1,
            &t,
            AnnounceEvent::Completed,
        );
        assert_eq!(&req[80..84], &1u32.to_be_bytes()); // Completed = 1
    }

    #[test]
    fn parse_announce_peers() {
        let txid: u32 = 0x1111_2222;
        let mut resp = Vec::new();
        resp.extend_from_slice(&1u32.to_be_bytes()); // action announce
        resp.extend_from_slice(&txid.to_be_bytes()); // txid
        resp.extend_from_slice(&1800u32.to_be_bytes()); // interval
        resp.extend_from_slice(&0u32.to_be_bytes()); // leechers
        resp.extend_from_slice(&2u32.to_be_bytes()); // seeders
        resp.extend_from_slice(&[5, 252, 161, 218]);
        resp.extend_from_slice(&2710u16.to_be_bytes());
        resp.extend_from_slice(&[1, 2, 3, 4]);
        resp.extend_from_slice(&8621u16.to_be_bytes());
        let (interval, peers) = parse_announce_response(&resp, txid).unwrap();
        assert_eq!(interval, 1800);
        assert_eq!(
            peers,
            vec![
                "5.252.161.218:2710".parse::<SocketAddrV4>().unwrap(),
                "1.2.3.4:8621".parse::<SocketAddrV4>().unwrap(),
            ]
        );
    }

    #[test]
    fn parse_tracker_error_action() {
        let txid: u32 = 7;
        let mut resp = Vec::new();
        resp.extend_from_slice(&3u32.to_be_bytes()); // action = error
        resp.extend_from_slice(&txid.to_be_bytes());
        resp.extend_from_slice(b"nope");
        assert!(matches!(
            parse_announce_response(&resp, txid),
            Err(crate::TrackerError::Tracker(_))
        ));
    }

    #[test]
    fn connect_server_roundtrip_preserves_txid() {
        let txid: u32 = 0x1122_3344;
        // client build -> server parse
        let req = build_connect_request(txid);
        assert_eq!(parse_connect_request(&req).unwrap().txid, txid);
        // server build -> client parse
        let resp = build_connect_response(txid, 0x0102_0304_0506_0708);
        assert_eq!(
            parse_connect_response(&resp, txid).unwrap(),
            0x0102_0304_0506_0708
        );
    }

    #[test]
    fn parse_connect_request_rejects_bad_magic_and_action() {
        let mut bad_magic = build_connect_request(1);
        bad_magic[0] ^= 0xFF;
        assert!(parse_connect_request(&bad_magic).is_err());
        let mut bad_action = build_connect_request(1);
        bad_action[8..12].copy_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
        assert!(parse_connect_request(&bad_action).is_err());
    }

    #[test]
    fn parse_connect_request_rejects_truncated() {
        assert!(parse_connect_request(&[0u8; 15]).is_err());
    }

    #[test]
    fn announce_server_roundtrip_preserves_all_fields() {
        for event in [
            AnnounceEvent::None,
            AnnounceEvent::Completed,
            AnnounceEvent::Started,
            AnnounceEvent::Stopped,
        ] {
            let transfer = TransferState {
                downloaded: 0x1122,
                left: 0x3344,
                uploaded: 0x5566,
            };
            let req = build_announce_request(
                0x0102_0304_0506_0708,
                0x1111_2222,
                &[0xAB; 20],
                &[0xCD; 20],
                6881,
                -1,
                &transfer,
                event,
            );
            let got = parse_announce_request(&req).unwrap();
            assert_eq!(got.connection_id, 0x0102_0304_0506_0708);
            assert_eq!(got.txid, 0x1111_2222);
            assert_eq!(got.infohash, [0xAB; 20]);
            assert_eq!(got.peer_id, [0xCD; 20]);
            assert_eq!(got.transfer.downloaded, 0x1122);
            assert_eq!(got.transfer.left, 0x3344);
            assert_eq!(got.transfer.uploaded, 0x5566);
            assert_eq!(got.event, event);
            assert_eq!(got.key, 0);
            assert_eq!(got.num_want, -1);
            assert_eq!(got.port, 6881);
        }
    }

    #[test]
    fn parse_announce_request_coerces_unknown_event_to_none() {
        let mut req = build_announce_request(
            1,
            2,
            &[0u8; 20],
            &[0u8; 20],
            6881,
            -1,
            &TransferState::default(),
            AnnounceEvent::None,
        );
        req[80..84].copy_from_slice(&7u32.to_be_bytes()); // out-of-range event
        assert_eq!(
            parse_announce_request(&req).unwrap().event,
            AnnounceEvent::None
        );
    }

    #[test]
    fn parse_announce_request_reads_key_at_documented_offset() {
        // Hand-rolled buffer: the client builder hardcodes key=0, so plant
        // sentinels to catch an offset slip between ip (84..88) and key (88..92).
        let mut req = vec![0u8; 98];
        req[8..12].copy_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
        req[84..88].copy_from_slice(&0xDEAD_BEEFu32.to_be_bytes()); // ip (ignored)
        req[88..92].copy_from_slice(&0x1357_9BDFu32.to_be_bytes()); // key
        let got = parse_announce_request(&req).unwrap();
        assert_eq!(got.key, 0x1357_9BDF);
    }

    #[test]
    fn parse_announce_request_rejects_bad_action() {
        let mut req = build_announce_request(
            1,
            2,
            &[0u8; 20],
            &[0u8; 20],
            6881,
            -1,
            &TransferState::default(),
            AnnounceEvent::None,
        );
        req[8..12].copy_from_slice(&ACTION_CONNECT.to_be_bytes());
        assert!(parse_announce_request(&req).is_err());
    }

    #[test]
    fn parse_announce_request_rejects_truncated() {
        let req = build_announce_request(
            1,
            2,
            &[0u8; 20],
            &[0u8; 20],
            6881,
            -1,
            &TransferState::default(),
            AnnounceEvent::None,
        );
        assert!(parse_announce_request(&req[..97]).is_err());
    }

    #[test]
    fn announce_response_server_roundtrip_zero_one_n_peers() {
        let txid: u32 = 0x1111_2222;
        let none: Vec<SocketAddrV4> = vec![];
        let one: Vec<SocketAddrV4> = vec!["1.2.3.4:8621".parse().unwrap()];
        let many: Vec<SocketAddrV4> = vec![
            "5.252.161.218:2710".parse().unwrap(),
            "1.2.3.4:8621".parse().unwrap(),
            "9.9.9.9:1234".parse().unwrap(),
        ];
        for peers in [none, one, many] {
            let resp = build_announce_response(txid, 1800, 3, 7, &peers);
            // client parser skips these, so check them at the byte level
            assert_eq!(&resp[12..16], &3u32.to_be_bytes()); // leechers
            assert_eq!(&resp[16..20], &7u32.to_be_bytes()); // seeders
            let (interval, got) = parse_announce_response(&resp, txid).unwrap();
            assert_eq!(interval, 1800);
            assert_eq!(got, peers);
        }
    }
}
