//! Piece-request scheduling across a pool of peers (pure, no I/O).
//!
//! The async peer loop (connect → signed handshake → read/write) lives in the driver;
//! this is the decision core it consults: *given the pieces we still need and what each
//! peer offers, which `(peer, piece)` requests should we issue right now?* It enforces a
//! per-peer in-flight cap, never double-requests a piece, only targets pieces inside a
//! peer's advertised live window, and skips choked peers. Pairs with
//! [`ace_wire::live::LivePicker`] / [`ace_wire::reassembly::PieceReassembler`].

use ace_wire::extended::LivePosition;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddrV4;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerAssignment {
    pub peer_id: u64,
    pub piece: u64,
}

#[derive(Debug, Clone)]
struct ActivePeer {
    min_piece: u64,
    max_piece: u64,
    unchoked: bool,
    in_flight: BTreeSet<u64>,
}

/// Pure active-peer state paired with [`Scheduler`]. It tracks each peer's current
/// advertised window, choke state, and pieces assigned to that peer, while the scheduler
/// keeps global "already requested" ownership.
#[derive(Debug, Default, Clone)]
pub struct ActivePeers {
    peers: BTreeMap<u64, ActivePeer>,
}

impl ActivePeers {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, id: u64, _addr: SocketAddrV4, window: LivePosition) {
        self.peers.insert(
            id,
            ActivePeer {
                min_piece: window.min_piece.max(0) as u64,
                max_piece: window.max_piece.max(0) as u64,
                unchoked: false,
                in_flight: BTreeSet::new(),
            },
        );
    }

    pub fn set_unchoked(&mut self, id: u64, unchoked: bool) {
        if let Some(peer) = self.peers.get_mut(&id) {
            peer.unchoked = unchoked;
        }
    }

    pub fn update_window(&mut self, id: u64, min_piece: u64, max_piece: u64) {
        if let Some(peer) = self.peers.get_mut(&id) {
            peer.min_piece = min_piece;
            peer.max_piece = max_piece.max(min_piece);
        }
    }

    pub fn assign(
        &mut self,
        scheduler: &mut Scheduler,
        next_needed: u64,
        head: u64,
    ) -> Vec<PeerAssignment> {
        let views: Vec<PeerView> = self
            .peers
            .iter()
            .map(|(&id, peer)| PeerView {
                id,
                min_piece: peer.min_piece,
                max_piece: peer.max_piece,
                unchoked: peer.unchoked,
                in_flight: peer.in_flight.len(),
            })
            .collect();
        let assigned = scheduler.assign(next_needed, head, &views);
        let mut out = Vec::with_capacity(assigned.len());
        for (peer_id, piece) in assigned {
            if let Some(peer) = self.peers.get_mut(&peer_id) {
                peer.in_flight.insert(piece);
                out.push(PeerAssignment { peer_id, piece });
            } else {
                scheduler.on_drop(piece);
            }
        }
        out
    }

    pub fn remove(&mut self, id: u64) -> Vec<u64> {
        self.peers
            .remove(&id)
            .map(|peer| peer.in_flight.into_iter().collect())
            .unwrap_or_default()
    }

    pub fn in_flight_count(&self, id: u64) -> usize {
        self.peers
            .get(&id)
            .map(|peer| peer.in_flight.len())
            .unwrap_or(0)
    }

    /// A piece finished: clear it from every peer's in-flight set (it may be tracked against
    /// more than one peer after a timeout re-request). Keeps a slow peer's slot occupied until
    /// the piece it was asked for actually lands, so [`assign`](Self::assign) keeps steering
    /// new work to faster peers.
    pub fn complete_everywhere(&mut self, piece: u64) {
        for peer in self.peers.values_mut() {
            peer.in_flight.remove(&piece);
        }
    }

    /// Drop in-flight bookkeeping for pieces below `floor` from every peer — call after the
    /// playback cursor jumps forward (a skip past an evicted gap) so stale slots don't wrongly
    /// suppress a peer's capacity forever.
    pub fn prune_below(&mut self, floor: u64) {
        for peer in self.peers.values_mut() {
            peer.in_flight.retain(|&p| p >= floor);
        }
    }

    /// Whether any unchoked peer's advertised window currently covers `piece` (can serve it).
    pub fn any_unchoked_covers(&self, piece: u64) -> bool {
        self.peers
            .values()
            .any(|p| p.unchoked && p.min_piece <= piece && piece <= p.max_piece)
    }

    /// The lowest piece index any unchoked peer can currently serve (min of their windows), or
    /// `None` if no peer is unchoked — the skip target when the needed piece has been evicted
    /// from every window.
    pub fn lowest_covered_piece(&self) -> Option<u64> {
        self.peers
            .values()
            .filter(|p| p.unchoked)
            .map(|p| p.min_piece)
            .min()
    }
}

impl Scheduler {
    /// `max_in_flight` is the per-peer cap on outstanding requests.
    pub fn new(max_in_flight: usize) -> Self {
        Scheduler {
            max_in_flight: max_in_flight.max(1),
            requested: BTreeSet::new(),
        }
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

    /// Every outstanding request was tied to a peer that dropped. Requeue all pieces so
    /// another peer can be assigned from the same playback cursor.
    pub fn clear_in_flight(&mut self) {
        self.requested.clear();
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
        PeerView {
            id,
            min_piece: min,
            max_piece: max,
            unchoked,
            in_flight,
        }
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
        let got = s.assign(
            0,
            100,
            &[peer(1, 0, 100, true, 1), peer(2, 0, 100, true, 0)],
        );
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
    fn clear_in_flight_requeues_every_outstanding_piece_after_peer_drop() {
        let mut s = Scheduler::new(3);
        assert_eq!(
            s.assign(0, 10, &[peer(1, 0, 10, true, 0)]),
            vec![(1, 0), (1, 1), (1, 2)]
        );
        assert_eq!(s.in_flight_count(), 3);

        s.clear_in_flight();

        assert_eq!(s.in_flight_count(), 0);
        assert_eq!(
            s.assign(0, 10, &[peer(2, 0, 10, true, 0)]),
            vec![(2, 0), (2, 1), (2, 2)]
        );
    }

    fn addr() -> SocketAddrV4 {
        use std::net::Ipv4Addr;
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)
    }

    fn win(min: i64, max: i64) -> LivePosition {
        LivePosition {
            min_piece: min,
            max_piece: max,
            position: -1,
            distance_from_source: 1,
        }
    }

    #[test]
    fn timeout_rerequest_prefers_a_different_peer_via_spare_capacity() {
        // Peer 1 was asked for piece 5 but never delivered (still in-flight). On re-request,
        // the scheduler must NOT pick peer 1 again while idle peer 2 also covers the piece.
        let mut ap = ActivePeers::new();
        ap.insert(1, addr(), win(0, 100));
        ap.set_unchoked(1, true);
        let mut s = Scheduler::new(4);
        // Only peer 1 is present, so it gets piece 5.
        let first = ap.assign(&mut s, 5, 5);
        assert_eq!(
            first,
            vec![PeerAssignment {
                peer_id: 1,
                piece: 5
            }]
        );
        // A second, idle peer joins that also covers the piece.
        ap.insert(2, addr(), win(0, 100));
        ap.set_unchoked(2, true);
        // Piece 5 timed out: requeue globally but keep peer 1's slot occupied.
        s.on_drop(5);
        // Re-assign: peer 1 still shows piece 5 in-flight (less spare) -> peer 2 gets it.
        let retry = ap.assign(&mut s, 5, 5);
        assert_eq!(
            retry,
            vec![PeerAssignment {
                peer_id: 2,
                piece: 5
            }]
        );
        // When it finally lands, both peers' slots for piece 5 clear.
        ap.complete_everywhere(5);
        assert_eq!(ap.in_flight_count(1), 0);
        assert_eq!(ap.in_flight_count(2), 0);
    }

    #[test]
    fn lowest_covered_and_coverage_reflect_unchoked_windows() {
        let mut ap = ActivePeers::new();
        ap.insert(1, addr(), win(50, 100));
        ap.insert(2, addr(), win(80, 130));
        assert!(!ap.any_unchoked_covers(90)); // nobody unchoked yet
        assert_eq!(ap.lowest_covered_piece(), None);
        ap.set_unchoked(2, true);
        assert!(ap.any_unchoked_covers(90)); // peer 2 covers 90
        assert!(!ap.any_unchoked_covers(60)); // 60 only in peer 1's window, still choked
        assert_eq!(ap.lowest_covered_piece(), Some(80));
        ap.set_unchoked(1, true);
        assert_eq!(ap.lowest_covered_piece(), Some(50));
    }

    #[test]
    fn prune_below_clears_stale_slots_after_a_skip() {
        let mut ap = ActivePeers::new();
        ap.insert(1, addr(), win(0, 100));
        ap.set_unchoked(1, true);
        let mut s = Scheduler::new(4);
        ap.assign(&mut s, 0, 3); // pieces 0..=3 in flight on peer 1
        assert_eq!(ap.in_flight_count(1), 4);
        ap.prune_below(2); // cursor skipped to 2
        assert_eq!(ap.in_flight_count(1), 2); // only 2,3 remain
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
