//! Piece-request scheduling across a pool of peers (pure, no I/O).
//!
//! The async peer loop (connect → signed handshake → read/write) lives in the driver;
//! this is the decision core it consults: *given the pieces we still need and what each
//! peer offers, which `(peer, piece)` requests should we issue right now?* It enforces a
//! per-peer in-flight cap, never double-requests a piece, only targets pieces inside a
//! peer's advertised live window, and skips choked peers. Pairs with
//! [`ace_wire::live::LivePicker`] / [`ace_wire::reassembly::PieceReassembler`].

use std::collections::BTreeSet;

/// A peer as the scheduler sees it this tick.
#[derive(Debug, Clone, Copy)]
pub struct PeerView {
    /// Local handle the driver uses to address this peer.
    pub id: u64,
    /// Lowest piece index this peer advertises (live window `min_piece`).
    pub min_piece: u64,
    /// Highest piece index this peer advertises (live window `max_piece`).
    pub max_piece: u64,
    /// Whether the peer has unchoked us (we may only request when true).
    pub unchoked: bool,
    /// Requests already outstanding to this peer.
    pub in_flight: usize,
}

impl PeerView {
    fn covers(&self, piece: u64) -> bool {
        self.min_piece <= piece && piece <= self.max_piece
    }
}

/// Tracks outstanding piece requests and assigns new ones across peers.
pub struct Scheduler {
    max_in_flight: usize,
    requested: BTreeSet<u64>,
}

impl Scheduler {
    /// `max_in_flight` is the per-peer cap on outstanding requests.
    pub fn new(max_in_flight: usize) -> Self {
        Scheduler { max_in_flight: max_in_flight.max(1), requested: BTreeSet::new() }
    }

    /// Decide requests to issue this tick. Considers pieces in `[next_needed, head]` (the
    /// part of the live window we still want, lowest-first for in-order playback) and
    /// assigns each not-yet-requested piece to an unchoked peer that covers it and still
    /// has spare in-flight capacity. Returns `(peer_id, piece)` pairs and records them as
    /// in-flight.
    pub fn assign(&mut self, next_needed: u64, head: u64, peers: &[PeerView]) -> Vec<(u64, u64)> {
        // Drop bookkeeping for pieces we've moved past.
        self.requested.retain(|&p| p >= next_needed);

        // Remaining capacity per peer for this tick (only unchoked peers can serve).
        let mut cap: Vec<(PeerView, usize)> = peers
            .iter()
            .filter(|p| p.unchoked)
            .map(|p| (*p, self.max_in_flight.saturating_sub(p.in_flight)))
            .collect();

        let mut out = Vec::new();
        let mut piece = next_needed;
        while piece <= head {
            if !self.requested.contains(&piece) {
                // Prefer the peer with the most spare capacity that covers this piece —
                // spreads load and keeps the rarest-window peers in reserve.
                if let Some(slot) = cap
                    .iter_mut()
                    .filter(|(p, c)| *c > 0 && p.covers(piece))
                    .max_by_key(|(_, c)| *c)
                {
                    out.push((slot.0.id, piece));
                    slot.1 -= 1;
                    self.requested.insert(piece);
                }
            }
            piece += 1;
        }
        out
    }

    /// A request completed: free its slot (the reassembler now owns the bytes).
    pub fn on_complete(&mut self, piece: u64) {
        self.requested.remove(&piece);
    }

    /// A request failed / the peer dropped: requeue the piece for reassignment.
    pub fn on_drop(&mut self, piece: u64) {
        self.requested.remove(&piece);
    }

    /// Pieces currently outstanding (for tests / metrics).
    pub fn in_flight_count(&self) -> usize {
        self.requested.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(id: u64, min: u64, max: u64, unchoked: bool, in_flight: usize) -> PeerView {
        PeerView { id, min_piece: min, max_piece: max, unchoked, in_flight }
    }

    #[test]
    fn assigns_up_to_in_flight_cap_for_one_peer() {
        let mut s = Scheduler::new(3);
        let got = s.assign(0, 100, &[peer(1, 0, 100, true, 0)]);
        assert_eq!(got, vec![(1, 0), (1, 1), (1, 2)]); // capped at 3
    }

    #[test]
    fn skips_choked_peers() {
        let mut s = Scheduler::new(5);
        assert!(s.assign(0, 10, &[peer(1, 0, 10, false, 0)]).is_empty());
    }

    #[test]
    fn respects_peer_window() {
        let mut s = Scheduler::new(10);
        // Peer only covers [5,7]; we need from 0.
        let got = s.assign(0, 20, &[peer(1, 5, 7, true, 0)]);
        assert_eq!(got, vec![(1, 5), (1, 6), (1, 7)]);
    }

    #[test]
    fn never_double_requests_a_piece() {
        let mut s = Scheduler::new(2);
        let first = s.assign(0, 100, &[peer(1, 0, 100, true, 0)]);
        assert_eq!(first, vec![(1, 0), (1, 1)]);
        // Same pieces still in flight (peer reports in_flight=2): nothing new for piece 0/1.
        let second = s.assign(0, 100, &[peer(1, 0, 100, true, 2)]);
        assert!(second.is_empty());
    }

    #[test]
    fn distributes_across_peers_by_spare_capacity() {
        let mut s = Scheduler::new(2);
        // p1 already has 1 in flight (1 spare); p2 idle (2 spare). p2 should get the first.
        let got = s.assign(0, 100, &[peer(1, 0, 100, true, 1), peer(2, 0, 100, true, 0)]);
        // 3 total spare slots → 3 assignments for pieces 0,1,2
        assert_eq!(got.len(), 3);
        let pieces: Vec<u64> = got.iter().map(|(_, pc)| *pc).collect();
        assert_eq!(pieces, vec![0, 1, 2]);
        // p2 (more spare) takes the first piece.
        assert_eq!(got[0].0, 2);
    }

    #[test]
    fn on_drop_requeues_and_on_complete_frees() {
        let mut s = Scheduler::new(1);
        assert_eq!(s.assign(0, 10, &[peer(1, 0, 10, true, 0)]), vec![(1, 0)]);
        assert_eq!(s.in_flight_count(), 1);
        // Dropped → can be reassigned (peer now idle again).
        s.on_drop(0);
        assert_eq!(s.assign(0, 10, &[peer(1, 0, 10, true, 0)]), vec![(1, 0)]);
        // Completed → advancing next_needed past it, no re-request.
        s.on_complete(0);
        let next = s.assign(1, 10, &[peer(1, 0, 10, true, 0)]);
        assert_eq!(next, vec![(1, 1)]);
    }

    #[test]
    fn drops_bookkeeping_below_next_needed() {
        let mut s = Scheduler::new(5);
        s.assign(0, 4, &[peer(1, 0, 4, true, 0)]); // requests 0..=4
        assert_eq!(s.in_flight_count(), 5);
        // Window advanced to 3 (live edge moved); stale entries pruned.
        s.assign(3, 4, &[peer(1, 0, 4, true, 5)]);
        assert_eq!(s.in_flight_count(), 2); // only 3,4 remain
    }
}
