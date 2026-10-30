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

// A synthetic peer handle used only by scheduler fixtures.
const SINGLE_PEER_ID: u64 = 0;

fn schedule_piece_requests(
    scheduler: &mut Scheduler,
    active_peers: &mut ActivePeers,
    next_needed: u64,
    head: u64,
) -> Vec<u64> {
    schedule_piece_assignments(scheduler, active_peers, next_needed, head)
        .into_iter()
        .filter_map(|assignment| (assignment.peer_id == SINGLE_PEER_ID).then_some(assignment.piece))
        .collect()
}

mod store_progress;

mod publication_schedules;

mod read_progress;

mod read_schedules;

mod activation_progress;
pub(crate) mod output_progress;

mod output_causality;
