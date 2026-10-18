use super::*;
use crate::scheduler::{ActivePeers, PeerAssignment};

mod continuity;
mod pool;
mod seeding;
fn default_live_recovery() -> LiveRecoveryConfig {
    LiveRecoveryConfig::default()
}

fn win(min: i64, max: i64) -> WireLivePosition {
    WireLivePosition {
        min_piece: min,
        max_piece: max,
        position: -1,
        distance_from_source: 1,
    }
}

fn test_addr() -> SocketAddrV4 {
    use std::net::Ipv4Addr;
    SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8621)
}

fn info() -> StreamInfo {
    StreamInfo {
        infohash: [0; 20],
        piece_length: 4,
        chunk_length: 2,
        trackers: vec![],
        metadata: Default::default(),
        sig_len: 0,
        source_pubkey: vec![],
    }
}

fn addrs(ports: &[u16]) -> Vec<SocketAddrV4> {
    ports
        .iter()
        .map(|&p| SocketAddrV4::new([1, 2, 3, 4].into(), p))
        .collect()
}

fn live_pos(min_piece: i64, max_piece: i64) -> WireLivePosition {
    WireLivePosition {
        min_piece,
        max_piece,
        position: max_piece,
        distance_from_source: 1,
    }
}

fn single_active_peer(min_piece: i64, max_piece: i64) -> ActivePeers {
    let mut active = ActivePeers::new();
    let addr: SocketAddrV4 = "1.2.3.4:8621".parse().unwrap();
    active.insert(SINGLE_PEER_ID, addr, live_pos(min_piece, max_piece));
    active.set_unchoked(SINGLE_PEER_ID, true);
    active
}

mod authentication;

mod credit;
