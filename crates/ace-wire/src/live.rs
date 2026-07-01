//! Live piece-window selection (pure, no I/O).
//!
//! Mirrors the `mi` window semantics observed from real peers
//! (`docs/protocol/notes/11-live-extended-handshake.md`): a live stream is a sliding
//! window of piece indices `[min_piece, max_piece]` with the broadcaster head at
//! `position`. A fresh client must **start near the head** — piece 0 (and anything
//! `< min_piece`) is long evicted — request only within the window, and advance toward
//! the head as it slides.

use crate::bencode::Bencode;

/// A snapshot of a peer's advertised live window (from its extended-handshake `mi`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveWindow {
    /// Oldest piece still available in the swarm (older pieces are evicted).
    pub min_piece: u64,
    /// Newest piece available (at/just behind the live head).
    pub max_piece: u64,
    /// The broadcaster's current head piece.
    pub position: u64,
}

impl LiveWindow {
    /// Piece index to begin a fresh live download: `buffer` pieces behind the head to
    /// build a little playback cushion, but never below `min_piece` (evicted).
    pub fn start_piece(&self, buffer: u64) -> u64 {
        self.position.saturating_sub(buffer).max(self.min_piece)
    }

    /// Recognize a live-window ("myinfo") update inside a raw peer-message payload, by
    /// **content** rather than by message id. Acestream advertises the advancing live
    /// edge in a periodic `myinfo` message (`got_myinfo` in the engine's `live` module —
    /// distinct from the one-shot extended handshake) carrying the same
    /// `min_piece`/`max_piece`/`position` bencode shape as the handshake's `mi` sub-dict
    /// (see `docs/protocol/notes/22-live-edge-never-advances.md`). The window keys may sit
    /// at the dict root or under an `mi` sub-dict; either is accepted. Returns `None` for
    /// any payload that isn't a bencode dict exposing a non-negative `max_piece`, so it
    /// can be tried speculatively on every otherwise-unhandled message without false
    /// positives driving requests.
    pub fn from_myinfo_payload(payload: &[u8]) -> Option<LiveWindow> {
        // parse_prefix tolerates trailing bytes after the dict (some messages append a
        // suffix); a non-bencode or non-dict payload simply yields None.
        let (root, _) = Bencode::parse_prefix(payload).ok()?;
        let dict: &Bencode = if root.get(b"max_piece").is_some() {
            &root
        } else {
            root.get(b"mi")?
        };
        let max = dict.get(b"max_piece").and_then(Bencode::as_int)?;
        if max < 0 {
            return None;
        }
        let max = max as u64;
        // min/position are advisory; default to the head when a partial update omits them.
        let min = dict
            .get(b"min_piece")
            .and_then(Bencode::as_int)
            .map(|v| v.max(0) as u64)
            .unwrap_or(0)
            .min(max);
        let position = dict
            .get(b"position")
            .and_then(Bencode::as_int)
            .map(|v| v.max(0) as u64)
            .unwrap_or(max)
            .clamp(min, max);
        Some(LiveWindow { min_piece: min, max_piece: max, position })
    }
}

/// Sequential live piece picker. Advances toward the head; never hands out an evicted
/// piece (`< min_piece`) or an unavailable one (`> max_piece`), and skips forward to
/// re-sync if it falls behind a sliding window.
#[derive(Debug, Clone)]
pub struct LivePicker {
    next: u64,
}

impl LivePicker {
    /// Start the picker at an explicit piece index.
    pub fn starting_at(piece: u64) -> Self {
        LivePicker { next: piece }
    }

    /// Start a fresh live download `buffer` pieces behind `window`'s head.
    pub fn new(window: &LiveWindow, buffer: u64) -> Self {
        LivePicker { next: window.start_piece(buffer) }
    }

    /// The next piece index to request given the current window, or `None` if we've
    /// caught up to the head (nothing newer is available yet — wait for the window to
    /// advance). If we've fallen behind a sliding window, skip the evicted gap.
    pub fn next_request(&mut self, window: &LiveWindow) -> Option<u64> {
        if self.next < window.min_piece {
            self.next = window.min_piece; // fell behind; skip evicted pieces
        }
        if self.next > window.max_piece {
            return None; // caught up to the live head
        }
        let piece = self.next;
        self.next += 1;
        Some(piece)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn win(min: u64, max: u64, pos: u64) -> LiveWindow {
        LiveWindow { min_piece: min, max_piece: max, position: pos }
    }

    #[test]
    fn start_piece_is_buffer_behind_head() {
        assert_eq!(win(100, 200, 200).start_piece(5), 195);
    }

    #[test]
    fn start_piece_floors_at_min() {
        // position - buffer would be 92, but min is 100 (older is evicted).
        assert_eq!(win(100, 200, 102).start_piece(10), 100);
    }

    #[test]
    fn picker_yields_sequential_pieces_within_window() {
        let w = win(100, 200, 200);
        let mut p = LivePicker::new(&w, 3); // starts at 197
        assert_eq!(p.next_request(&w), Some(197));
        assert_eq!(p.next_request(&w), Some(198));
        assert_eq!(p.next_request(&w), Some(199));
        assert_eq!(p.next_request(&w), Some(200));
    }

    #[test]
    fn picker_returns_none_when_caught_up_to_head() {
        let w = win(100, 200, 200);
        let mut p = LivePicker::starting_at(200);
        assert_eq!(p.next_request(&w), Some(200));
        assert_eq!(p.next_request(&w), None); // 201 > max_piece
    }

    #[test]
    fn picker_skips_evicted_pieces_when_behind() {
        // We lagged at piece 50 but the window has slid forward to [100, 200].
        let w = win(100, 200, 200);
        let mut p = LivePicker::starting_at(50);
        assert_eq!(p.next_request(&w), Some(100)); // jumped past the evicted gap
        assert_eq!(p.next_request(&w), Some(101));
    }

    #[test]
    fn myinfo_recognized_at_dict_root() {
        // A standalone myinfo update: bencode dict with the window keys at the root.
        let payload = b"d9:max_piecei14718275e9:min_piecei14718220e8:positioni14718275ee";
        let w = LiveWindow::from_myinfo_payload(payload).expect("recognized");
        assert_eq!(w.max_piece, 14718275);
        assert_eq!(w.min_piece, 14718220);
        assert_eq!(w.position, 14718275);
    }

    #[test]
    fn myinfo_recognized_under_mi_subdict() {
        // The same window nested under `mi` (as in a re-sent extended handshake).
        let payload = b"d2:mid9:max_piecei205e9:min_piecei100e8:positioni200eee";
        let w = LiveWindow::from_myinfo_payload(payload).expect("recognized");
        assert_eq!((w.min_piece, w.max_piece, w.position), (100, 205, 200));
    }

    #[test]
    fn partial_myinfo_defaults_missing_fields_to_head() {
        // `send_partial_myinfo` may carry only max_piece; min/position default sanely.
        let payload = b"d9:max_piecei500ee";
        let w = LiveWindow::from_myinfo_payload(payload).expect("recognized");
        assert_eq!(w.max_piece, 500);
        assert_eq!(w.position, 500);
        assert!(w.min_piece <= w.max_piece);
    }

    #[test]
    fn non_window_payloads_are_not_mistaken_for_myinfo() {
        // Not bencode, an unrelated dict, and a negative head must all be rejected so a
        // speculative attempt on every message never drives a bogus request.
        assert!(LiveWindow::from_myinfo_payload(b"\x00\x01\x02not-bencode").is_none());
        assert!(LiveWindow::from_myinfo_payload(b"d3:fooi1ee").is_none());
        assert!(LiveWindow::from_myinfo_payload(b"d9:max_piecei-1ee").is_none());
        assert!(LiveWindow::from_myinfo_payload(b"").is_none());
    }
}
