# 22 — The live edge never advanced (download froze after the initial window)

**Status: ROOT-CAUSED + FIXED in code; fix is live-gated for final confirmation.**

## Symptom (operator report, VLC)

Playing a known-good live id: the stream takes a long time to load, then renders only a
brief burst of **the same choppy frames and goes essentially idle**, and **no audio plays**
even though the stream carries an audio PID. Re-opening the same id reproduces the same
brief-then-frozen behavior.

## Root cause

`follow_one_peer` (`crates/ace-engine/src/ace_provider.rs`) downloaded the initial prefetch
window **exactly once** and then sat forever in `read_message().await`, never requesting
another piece:

- On `UNCHOKE` it requested pieces `start..=head` where `head = window.max_piece` and
  `start = head − PREFETCH_PIECES(8)` — about **9 pieces ≈ 8.3 MB**.
- After that, the **only** code path that issued new requests was the `PeerMessage::Have(p)`
  arm. **Real Acestream live peers never send a standard BT `Have` (id=4, 4-byte payload)
  to advance the live head.** So `head`/`requested_to` froze and no further `request_range`
  ever fired.
- Every other inbound message — a re-sent extended handshake, and Acestream's custom
  message ids (10/11/34/36, kept as `Unknown`) — fell into `_ => {}` and was ignored.

This was masked because it was **never tested as a *continuous* stream**. The `live_recon_unchoke`
harness requests a fixed range *once* too (`// request once`), and notes 19/20/21 all read
"≈8.3 MB downloaded" as success — but **8.3 MB is exactly the static initial window**. No
live capture in notes 19/20/21 ever shows an inbound `Have`.

### Why each symptom follows
- **Same choppy frames / idle:** you get one ~8 MB window (a few seconds), then the feed
  freezes. The manager reuses the session by `(network,id)` and a tokio `broadcast` channel
  does not replay backlog to a new subscriber, so re-opening either rejoins the frozen
  session (no new data) or, after the idle reaper fires, pulls another small static window
  near a barely-moved head — indistinguishable brief-freeze each time.
- **No audio:** *not* a muxing bug. `KeyframeGate` locks on the video keyframe and then
  passes **everything** through, including the intact PMT (which still declares the audio
  PID) and subsequent audio packets. `ffprobe` (note 20) confirms AAC is present in the
  bytes. VLC simply never receives enough *continuous* data with a running clock to start
  the audio output — a downstream consequence of the freeze, not a separate defect.

## Ground truth: how the live edge actually advances

The extracted engine's `live` module (`re/engine/lib/acestreamengine/live.so`, a Cython
build) still exposes its symbol names. The relevant ones:

- `core/src/live/MessageID.pyx`, `Messages.pyx`, `PiecePickerClient.pyx`, `Rerequester.pyx`
- handlers: **`got_myinfo`**, `send_myinfo`, `send_partial_myinfo`, **`got_have_bitfield`**,
  `got_have_bitfield_with_timestamps`, `send_have_array`, `got_have`
- client loop: `check_outstanding_pieces`, `get_last_requested_piece`,
  `get_last_received_piece`, `get_external_max_piece`, `get_external_min_piece`

Key facts:
- **`got_myinfo` is a separate handler from `got_extend_handshake`.** The `mi` dict we read
  once from the BEP-10 extended handshake is just the *bootstrap*; the live window is then
  advanced by a periodic **`myinfo`** message (and `send_partial_myinfo` partial updates),
  plus **have-bitfield arrays** — not by per-piece standard `Have`.
- The client keeps a request frontier topped up (`check_outstanding_pieces` +
  `PiecePickerClient`) toward the advertised `external_max_piece`.

### Wire message ids — DECODED from a live operator run (2026-06-30)

An operator run with the once-per-id diagnostics produced real payloads, which decode
cleanly and pin the advancement signal:

| id | size | payload | meaning |
|----|------|---------|---------|
| **4** | 8 B | `[u32 stream=0][u32 piece]` | **Live HAVE — the advancing head.** Captured `…51cb63`=5360483 vs window max 5360481; subsequent ones were max+1 each. **One per new piece. THE advancement signal.** Note it's an *8-byte* HAVE — standard BT HAVE is 4 bytes, so our decoder routed it to `Unknown{id:4}` and `from_myinfo_payload` (bencode-only) couldn't see it. |
| 10 | 8 B | `[u32 stream=0][u32 piece]` | Trailing edge: `…51cade`=5360350 = `min_piece+1`. The advancing eviction pointer (oldest still-available piece). |
| 11 | 182 B | bencode `d1:ai1e1:bi…e1:ci…e…` | Per-source stats dict (single-char keys). Not the window. |
| 12 | 5416 B | binary, leading `05 0032 …` | Large; ≈ `have_bitfield_with_timestamps` (full-window bitfield bootstrap). Not needed once id=4 increments drive us. |
| 34 | 8 B | `[u32=1][u32]` | Varying non-piece values (rate/timer). Undetermined; harmless. |
| 36 | 80 B | binary containing `R30------` | Peer-exchange / peer announce (carries a peer_id). Not the window. |

So the live edge is driven by **id=4 (8-byte binary HAVE)**, not a bencode `myinfo`. The
content-recognizer (`from_myinfo_payload`) remains as a belt-and-suspenders path for any peer
that re-advertises the window as bencode, but **id=4 is the primary fix**.

## The fix (committed)

Advance the request frontier on **every** signal we can recognize, and recognize the
`myinfo` window update by **content, not id** (so we don't need to pin the custom id):

- `ace_wire::live::LiveWindow::from_myinfo_payload` — bencode-decodes a payload and extracts
  a `LiveWindow` if it exposes a non-negative `max_piece` (at the dict root or under an `mi`
  sub-dict). Returns `None` for anything else, so it is safe to try on every otherwise-
  unhandled message without false positives. (Unit-tested.)
- `ace_engine::ace_provider`:
  - `next_request_range` / `advance_requests` — the single forward-progress primitive every
    signal funnels through; bounded by `MAX_PIECE_ADVANCE(256)` so a bogus head can't burst.
  - The loop now advances `head` and re-requests on: `UNCHOKE`, `Have`, a re-sent
    `Extended` handshake, **and any `Unknown` custom message whose payload is recognized as
    a `myinfo` window dict**.
  - Diagnostics: each unmodelled message id is logged **once** (`unhandled msg id=… (N bytes) <hex>`),
    and a recognized window update logs `live window update (msg id=…) head A -> B`. One live
    run thus reveals exactly which id carries the window if content-recognition needs
    tightening.

After the live capture above, the **primary** advancement handler is the binary HAVE:
`PeerMessage::Unknown { id: 4, payload } if payload.len() == 8` reads the head piece from
`payload[4..8]` and advances. `id=10` (trailing edge) is matched and ignored.

This is **non-regressing by construction**: it only ever requests pieces a peer has
advertised as existing (so it can't stall the in-order `PieceReassembler` on a not-yet-
existent piece), and it adds no new *outbound* message types — only ordinary chunk requests
— so it does not repeat the note-21 proactive-`Have` regression.

## Slow time-to-first-byte (the "Acestream getstream was almost instant" report)

The operator's log showed every `open` serially trying two unreachable peers
(`connect failed/timed out`) at `CONNECT_TIMEOUT`=3 s **each** before reaching the one live
peer — ≈6 s wasted before the first byte. And because the stream then stalled (id=4 ignored),
VLC repeatedly reconnected, re-paying that cost each time (the log shows `open` looping).

**Fix:** `connect_any` races connect+handshake across the first `MAX_PARALLEL_CONNECT`(12)
peers concurrently (`tokio::JoinSet`) and follows the first that succeeds; the rest are
aborted. Dead peers no longer serialize the time to first byte. `follow_live` re-races on
real peer loss (excluding the just-lost peer). Combined with the id=4 fix (no more stalls →
no more reconnect storms), time-to-first-byte should drop to roughly one peer's
connect+handshake RTT, matching the engine's parallel-dial behavior.

## Duplicate `open` per id (a second live-operator run)

A follow-up live run (still with `id=4` newly wired) showed the connect now instant
(parallel dial working — no dead-peer timeouts) but the log showed **`[dht] seeded` /
`open cf53…` firing a second time** for the same id shortly after the first, with no piece
data or `id=4` HAVE logged in between — i.e. the session looked like it was being torn down
and restarted rather than settling into continuous flow.

**Root cause:** `StreamManager::get_or_start` (`crates/ace-engine/src/manager.rs`) checked
for an existing session, and if absent, released the lock and called the **slow**
`provider.open()` (tracker/DHT discovery + spawning `follow_live`) before re-acquiring the
lock to store the result. Two near-simultaneous first requests for the same id — which is
exactly what VLC does (it opens more than one connection when probing a stream) — both miss
the initial check and **both** run a full `open()`: duplicate discovery, and **two outbound
connections to the same peer using our identical node_id**, which a real peer's own
anti-abuse heuristics may treat as anomalous and drop (the same class of "peer goes quiet on
anomalous behavior" seen in note 21's reverted `Have`-advertisement experiment).

**Fix:** added a `start_lock: Mutex<()>` held only across the *creation* path (never around
the fast existing-session lookup), with a re-check of the session map after acquiring it —
the standard double-checked-locking shape. Concurrent first requests for the same id now
provoke exactly **one** `provider.open()`; all callers converge on the one session that
resulted. Regression test:
`manager::tests::concurrent_first_requests_start_the_session_once` (16 concurrent
`get_or_start` calls against a counting provider assert exactly 1 open + all callers sharing
one `Arc`).

### Throughput visibility added

Because "is it actually still downloading" was previously only inferable from silence,
`follow_one_peer` now logs served throughput as data is emitted to the broadcast channel:
`[ace] {addr}: served N MiB (head=…, next piece needed=…)` — first at 1 MiB, then every
4 MiB. This turns "does it silently freeze again" into an observable, avoiding another round
of "why is it slow / did it stop" reports without a live capture.

## What still needs the operator (live-gated)

The pure logic is unit-tested and the workspace is green + clippy-clean, but the actual
swarm interaction can only be confirmed from a non-WARP host:

1. Run `OUTPACE_BIND=127.0.0.1:6900 cargo run -p ace-engine --bin outpace` on a live
   id and `curl -N …/streams/ace/<id>.ts | wc -c` for ~60 s. **Pass = far more than ~8 MB
   and still growing** (continuous), vs. the old frozen ~8.3 MB.
2. Time to first byte should now be a few seconds (parallel connect), not ~6 s+ per the
   serial dead-peer timeouts before.
3. The `unhandled msg id=4/10` lines should be **gone** (now handled); ids 11/34/36/12 may
   still log once each (harmless stats/PEX). The stream should keep flowing as long as id=4
   HAVEs arrive.
4. `[dht] seeded` / `open <id>` should log **once** per id per cold start, not repeatedly —
   if it still repeats, that's a *different* trigger than the fixed race (e.g. the session
   really did end — check for a preceding `PeerLost`/error line).
5. Watch for `[ace] {addr}: served N MiB (head=…, next piece needed=…)` recurring — this is
   the new, direct evidence the stream is still advancing (not silence-by-inference).
6. Confirm in VLC: continuous video **and** audio (audio should follow once the stream no
   longer freezes).

## Reproduce / verify
```sh
cargo test -p ace-wire live::            # window-update recognizer
cargo test -p ace-engine ace_provider::  # request-frontier advancement
cargo test -p ace-engine manager::       # single-open-per-id under concurrent requests
# live (operator):
OUTPACE_BIND=127.0.0.1:6900 cargo run -p ace-engine --bin outpace
curl -N http://127.0.0.1:6900/streams/ace/<40hex-or-cid>.ts | wc -c   # expect >> 8 MB, growing
```
