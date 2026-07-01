# 23 — Peer reconnects spliced/gapped the stream (the "jittery/stuttery" report)

**Status: ROOT-CAUSED + FIXED + LIVE-VERIFIED with quantitative evidence (not just "it played").**

## Symptom (operator report)

After note 22 fixed the live-edge freeze, the operator reported the video still looked
"a bit jittery/stuttery" during playback, distinct from the earlier "frozen after a burst"
bug (which was confirmed fixed by that point).

## Root cause

All piece-continuity state — the `PieceReassembler`, the `TsResync` filter, the request
frontier (`requested_to`), and the live `head` — was **local to `follow_one_peer`**, torn
down and recreated from scratch on every call. `follow_live`'s retry loop calls
`follow_one_peer` again on every peer loss (`FollowEnd::PeerLost`), and **real swarm
connections drop and get replaced routinely** — verified live in this exact session (the
node-identity daemon reconnected within its first ~15 seconds in one run).

`PieceReassembler` only ever emits **strictly contiguously** from its cursor
(`crates/ace-wire/src/reassembly.rs`: `take_ready` requires an exact match at `next_emit`).
Recreating it fresh from the *new* peer's own window on every reconnect meant:
- If the new peer's computed `start` (its own `head - PREFETCH_PIECES`) landed at or before
  where we'd already emitted through, the new reassembler doesn't know that — it re-requests
  and **re-emits already-served pieces**, splicing a duplicate/backward chunk into the one
  continuous broadcast byte stream every client shares.
- If the new peer's `start` landed *after* where we'd left off, the reassembler's cursor
  would sit at the old (lower) index forever, waiting for a piece that will never arrive —
  a silent stall at the reassembly layer (the same failure class as note 22's freeze, just
  one layer down).

Either way, **every peer reconnect was a guaranteed splice or stall**, not an edge case —
live swarm peers reconnect on a timescale of tens of seconds to a few minutes, so a viewer
watching for any length of time was likely to hit this repeatedly, perceived as jitter/stutter.

## The fix

Extracted the continuity state into a `Continuity` struct (`crates/ace-engine/src/ace_provider.rs`)
owned by `follow_live` and threaded through `follow_one_peer` via `&mut Option<Continuity>`,
so it survives across reconnects instead of being recreated:

- **First connection** (`Continuity::fresh`): unchanged behavior — bootstrap `start`/`head`
  from that peer's window, `PREFETCH_PIECES` behind the head, clamped to `min_piece`.
- **Reconnect** (`Continuity::resume`): keep going from `requested_to + 1` (or the
  reassembler's own `next_needed()` if we hadn't started requesting yet), **regardless of
  the new peer's own head** — the new peer is just a new upstream for the *same* ongoing
  live position, not a reason to re-anchor. Only if the new peer's `min_piece` has already
  evicted the piece we still needed (a genuine, unavoidable gap from being disconnected too
  long) does it skip forward — logged (`reconnect gap — peer's window already evicted
  pieces X..Y; skipping ahead`) and realized via a new `PieceReassembler::skip_to` method
  (`crates/ace-wire/src/reassembly.rs`) that forcibly advances the emit cursor and discards
  now-stale buffered data, so the reassembler can't get stuck waiting for a piece that will
  never come. `head` is a `max()` — it can only advance, never regress, even if the new peer
  happens to report a smaller window.

Unit-tested at the pure-logic level (no network needed):
- `ace-wire`: `skip_to_advances_cursor_past_an_unrecoverable_gap`,
  `skip_to_drops_stale_buffered_data_below_the_new_cursor`,
  `skip_to_never_moves_the_cursor_backward`.
- `ace-engine`: `fresh_starts_prefetch_pieces_behind_head_clamped_to_min`,
  `resume_continues_seamlessly_when_the_new_window_still_covers_our_position`,
  `resume_skips_forward_over_an_unrecoverable_eviction_gap`, `resume_head_never_regresses`.

## Live verification (this session, same host, real swarm)

Ran the fixed daemon against the Synthetic Live Channel channel (`cid2`)
for ~150 s and captured a real reconnect organically:

```
[ace] 5.231.25.139:10022: connected + handshaked
[ace] 5.231.25.139:10022: window min=5384532 max=5384630 -> start=5384622 head=5384630
   (never sent UNCHOKE; lost)
[ace] 92.191.196.38:8621: connected + handshaked
[ace] 92.191.196.38:8621: reconnected; window min=5384576 max=5384639 -> resuming from 5384622 head=5384639
[ace] 92.191.196.38:8621: UNCHOKE -> requesting pieces 5384622..=5384639
[ace] 92.191.196.38:8621: served 1 MiB ... served 116 MiB (head climbing steadily, no resets)
```

The resumed request starts at **5384622** — our own already-committed target from the first
(failed) connection — not the second peer's independently-computed `5384631`
(`5384639 - PREFETCH_PIECES`). That's the fix operating correctly: on the previous
(pre-fix) code, this reconnect would have silently re-anchored to 5384631, either
re-requesting/re-emitting 5384622–5384630 as a duplicate splice, or (if the reassembler's
stale cursor were below that) stalling.

**Quantitative confirmation, not just "it decoded":** captured 133 MB / 172.3 s of
continuous stream spanning the reconnect. Extracted all 4,301 video frame presentation
timestamps (`ffprobe -show_entries frame=pts_time`) and scanned for backward jumps or
gaps > 1 s (50 fps → ~0.02 s is normal spacing): **zero anomalies**. A pre-fix capture
spanning the same kind of reconnect would be expected to show either a backward PTS jump
(the duplicate-splice case) or a multi-second gap (the stall case) at the reconnect
boundary; neither appeared.

Also re-confirmed (same live run) everything from note 22 still holds: `unhandled msg
id=4/10` never appear, `served N MiB` climbs continuously, real 1920×1080 H.264 + 48 kHz
AAC decodes cleanly (`ffprobe`/`ffmpeg`), two different decoded frames pulled from the start
and end of an earlier capture show genuinely different live content (not a frozen loop),
and driving the stream through **VLC's own internal demuxer** (not just curl/ffmpeg) shows
it finding PAT/PMT, locking H.264 SPS/PPS, hitting the `KeyframeGate`'s SEI recovery point,
and detecting `AAC channels: 2 samplerate: 48000`.

## What's still open

The daemon reconnects to the same "never unchokes" peer (`5.231.25.139:10022`) repeatedly
across separate `open` calls in these logs. `connect_any`'s `exclude` only skips the single
*most recently* lost peer, not a cumulative blacklist, so a consistently bad peer can be
re-picked on a later reconnect within the same session. Not addressed here (didn't cause
incorrect output, just a wasted connect+handshake round-trip); worth a small follow-up if it
turns out to matter for reconnect latency.

## Reproduce / verify
```sh
cargo test -p ace-wire reassembly::    # skip_to
cargo test -p ace-engine ace_provider::  # Continuity::fresh / resume
# live (operator), watch for a natural reconnect in the log and confirm it says
# "reconnected; ... resuming from <X>" where X continues your own prior position, not a
# fresh recompute from the new peer's window:
OUTPACE_BIND=127.0.0.1:6900 cargo run -p ace-engine --bin outpace
curl -N http://127.0.0.1:6900/streams/ace/<id>.ts -o cap.ts &
# after a while, check for PTS anomalies across the whole capture:
ffprobe -v error -select_streams v:0 -show_entries frame=pts_time -of csv=p=0 cap.ts \
  | awk 'NR>1{d=$1-p; if (d<-0.001 || d>1.0) print NR, p, $1, d} {p=$1}'
```
