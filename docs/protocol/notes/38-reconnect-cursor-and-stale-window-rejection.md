# 38 - Reconnect uses the emit cursor and rejects stale windows

Follow-up to note 37. This is another bounded improvement on the single-upstream live path:
after a stale peer drops, outpace now resumes from the piece it still needs to emit, and
it rejects a reconnected peer whose advertised window is already behind that piece.

## Root cause

`Continuity::resume` used `requested_to + 1` as the reconnect position. That value only
means "we asked the previous peer up through this piece"; it does not prove the pieces were
received or emitted. If a peer died after a request burst but before delivery, the next peer
could be asked to start after the missing piece, leaving the reassembler blocked forever.

The same boundary also allowed the known static upstream to be retried even when its
advertised `max_piece` was lower than the next piece the stream still needed. In the Synthetic Live Channel
target, this produced repeated 12 s stale-follow cycles against the same old window:

- needed next: `14718270`
- peer window: `min=14718220`, `max=14718269`

That peer cannot satisfy the next piece.

## Implemented behavior

`Continuity::resume` now uses `PieceReassembler::next_needed()` as the source of truth for
the reconnect position. `requested_to` remains only the request frontier for the current
connection and is reset to just before the resume point, so the new peer is asked for the
first still-missing piece.

Before handing a reconnected peer to `follow_one_peer`, `follow_live` now checks whether the
peer's advertised `mi.max_piece` covers `reasm.next_needed()`. If not, it logs the stale
window, excludes that peer for the current discovery cycle, drops the connection, and keeps
searching.

Regression tests:

- `resume_retries_from_the_reassembler_cursor_not_the_old_request_frontier`
- `peer_window_must_cover_the_next_needed_piece_on_reconnect`
- updated `resume_continues_seamlessly_when_the_new_window_still_covers_our_position` to
  model emitted pieces, not merely requested pieces.

Verification:

- `cargo test -p ace-engine resume_ -- --nocapture`
- `cargo test -p ace-engine peer_window_ -- --nocapture`
- `cargo test -p ace-engine`
- `cargo clippy -p ace-engine --all-targets -- -D warnings`
- `rustfmt --edition 2021 --check crates/ace-engine/src/ace_provider.rs`
- `git diff --check`

## Live smoke

Target:

- infohash `50e93529d3eb46a50506b14464185a15292d6e47`
- daemon `127.0.0.1:6900`
- capture length: 75 s curl timeout

Result:

- `HTTP 200`
- first byte: `7.667642` s
- downloaded: `8,308,472` bytes
- MPEG-TS sync at offsets `0`, `188`, `376`; first 1000 packets had zero sync errors.

Relevant daemon log:

```text
[ace] 85.87.156.75:8621: connected + handshaked (window max=14718269)
[ace] 85.87.156.75:8621: window min=14718220 max=14718269 -> start=14718261 head=14718269
[ace] 85.87.156.75:8621: UNCHOKE -> requesting pieces 14718261..=14718269
[ace] 85.87.156.75:8621: served 6 MiB (head=14718269, next piece needed=14718268)
[ace] 85.87.156.75:8621: stale upstream - no live progress for 12s; reconnecting
[ace] no reachable peer among 16 known; rediscovery found 16, added 1 (known now 17)
[ace] no reachable peer among 17 known; rediscovery found 14, added 0 (known now 17)
[ace] 85.87.156.75:8621: connected + handshaked (window max=14718269)
[ace] 85.87.156.75:8621: stale advertised window min=14718220 max=14718269 cannot cover next needed piece 14718270; trying another peer
[ace] no reachable peer among 17 known; rediscovery found 15, added 1 (known now 18)
```

The live byte result is unchanged for this target, but the post-window behavior is better
bounded: the stale upstream is no longer followed for another 12 s once its advertised
window is known to be behind the stream's next needed piece.

## Current blocker

This does not prove continuous playback. The blocker remains:

> Outpace needs more than one active single upstream retry path. It must keep several
> handshaked peers alive, observe which windows advance over time, and request missing
> chunks from any peer with coverage.

The new cursor/window gate makes that future active-peer scheduler safer: it gives the
scheduler a correct "next needed" cursor and a precise test for whether a peer can currently
contribute to that cursor.
