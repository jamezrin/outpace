# 26 — B1: broadcast ingest + origination, loopback milestone reached

**Status: DONE (outpace-to-outpace).** Official-engine interop remains gated on B0
(live-source-auth signing, note 25 — not yet cracked). Part of the `/goal` push toward full
leech+seed+broadcast parity.

## What was built

- `ace_wire::live_auth::LiveSourceAuth` — RSA identity for the broadcast source: generate,
  load/save PKCS#1 PEM (matching a real `.sauth` file, note 25), and the DER
  SubjectPublicKeyInfo public key the transport's `pubkey` field carries. Unit-tested
  **against the actual captured real-engine key** from note 25 — byte-for-byte match.
  768-bit / `e=3` by default, matching the captured key's parameters (not a protocol
  requirement, just avoids an easy implementation fingerprint).
- `ace_engine::broadcast::BroadcastRegistry` — mints (or resumes) a named broadcast:
  builds the descriptor (`name`, `piece_length`, `chunk_length`, `authmethod=RSA`, our real
  `pubkey`, trackers, etc. — the same field set as a real source-node transport, note 25),
  encodes it (`encode_transport`), computes the infohash, and registers an empty
  `PieceStore` for it in the **existing** `SeedRegistry` — the same registry S1/S2 already
  read from. No new serving code needed; a minted broadcast is immediately servable.
- `PUT /broadcast/{name}` (`http.rs`) — accepts the request body (any size; may be a
  long-lived live stream), responds immediately with the infohash (mint/resume is fast,
  in-memory), and processes the body in a background task: `TsResync` (packet realignment)
  → `TsChunker` (splits into piece/chunk-addressed blocks) → `PieceStore::put_chunk`.

## A real, pre-existing gap found and fixed along the way

`SeederSession::serve` (S2's inbound path, from an earlier session) **never sent an
extended handshake at all** — it went straight from the plaintext BT handshake to
`Have`-advertising + serving chunk-requests. This meant **our own `AceProvider` leech
client could never download from our own inbound listener**: `follow_one_peer` blocks on
`read_peer_window`, which waits specifically for the peer's `Extended{ext_id:0}` message
carrying the `mi` live window — a message the real Acestream engine always sends
unsolicited (notes 11/19) but our S2 serve path silently never did. Undetected until now
because S2's own tests use a raw mock `PeerSession` that doesn't wait for it either.

**Fixed**: `SeederSession::serve` now sends a signed extended handshake (our Ed25519
identity + the store's current `(min, max)` window as `mi`, `distance_from_source: 0` for
an origin) before advertising `Have`s. Threaded an `Arc<Identity>` through
`PeerListener::serve`/`handle_inbound` (previously had no identity access at all) and the
connecting peer's IP (for `yourip`) from the accept loop. New regression test:
`accepted_peer_gets_a_signed_extended_handshake_with_the_live_window`
(`ace-swarm/tests/inbound_seeder.rs`) — connects, reads the first message, asserts it's the
extended handshake with the right `mi.min_piece`/`max_piece` and a `node_id`.

This is exactly the kind of gap the v2 design spec's "bidirectional interop is a test gate"
principle exists to catch — it just hadn't been exercised end-to-end (outpace leeching
from outpace's own inbound path) until this B1 pass needed it.

## Live-verified loopback (this session, same host)

Two daemon instances, no official engine involved:

```
# Daemon A — origin, inbound listener enabled
OUTPACE_BIND=127.0.0.1:6910 OUTPACE_ENABLE_INBOUND=1 OUTPACE_PEER_LISTEN=127.0.0.1:6911 \
  cargo run -p ace-engine --bin outpace

# Ingest a real H.264 TS (finite, ~6 MB / multiple 1 MiB pieces so more than piece 0 completes)
curl -X PUT --data-binary @big-broadcast.ts http://127.0.0.1:6910/broadcast/bigchan
# -> {"infohash":"cafe036789abcdef0123456789abcdef01234567","name":"bigchan"}

# Daemon B — leeches from daemon A as a bootstrap peer, same infohash
OUTPACE_BIND=127.0.0.1:6920 OUTPACE_ACE_PEERS=127.0.0.1:6911 \
  cargo run -p ace-engine --bin outpace
curl -N http://127.0.0.1:6920/streams/ace/cafe036789abcdef0123456789abcdef01234567.ts -o out.ts
```

Daemon B's log:
```
[ace] 127.0.0.1:6911: connected + handshaked
[ace] 127.0.0.1:6911: window min=0 max=5 -> start=0 head=5
[ace] 127.0.0.1:6911: UNCHOKE -> requesting pieces 0..=5
[ace] 127.0.0.1:6911: served 1 MiB (head=5, next piece needed=2)
```
5.2 MB captured; `ffprobe` confirms `h264 128x96` (matching the ingested fixture exactly);
`ffmpeg` decodes a clean frame (the fixture's SMPTE test pattern, visually verified, not
corrupted). **This is the spec's B1 "reachable milestone"**, done via two real daemon
processes over a real TCP connection — not a mock/duplex test.

### A real ffmpeg gotcha hit along the way (unrelated to outpace, worth recording)

`ffmpeg -f mpegts pipe:1` buffers its own muxer output and does not flush to the pipe/file
in real time by default — piping it live into `curl -T -` for a genuinely continuous ingest
test showed zero bytes reaching the server for 15+ seconds even though ffmpeg's own
progress counter was advancing. Fix: `-flush_packets 1`. Confirmed the buffering (not
axum/hyper/curl) was the cause by writing directly to a plain file first (also 0 bytes for
several seconds without the flag, populated instantly with it). For this session's
milestone, sidestepped entirely by ingesting a large-enough **finite** file instead of a
live pipe — proves the same ingest → chunk → store → serve → download → decode path
without depending on live real-time pacing. A genuinely continuous ingest test (matching
the design spec's ultimate use case, e.g. OBS pushing indefinitely) should use
`-flush_packets 1` (or an equivalent non-ffmpeg live source that flushes per-write).

## What's still open (in priority order)

1. **Known ingest-resume gap** (documented in `http.rs`): piece numbering restarts at 0 on
   every ingest task, even resuming an already-minted name — a second `PUT` after the first
   ingest ends overwrites piece indices rather than continuing them. Matches the real
   engine's `.restart`-file persistence need (note 25's `relocating-the-source-node.md`) —
   not implemented. Fine for one continuous ingest (the only case exercised); needed for
   ingest-reconnect support.
2. **Official-engine interop** — gated on B0 (per-piece signing preimage, note 25). Our
   transport carries a real, valid RSA identity and `authmethod=RSA`, but pieces are served
   with the same placeholder `piece_header` S1/S2 already use.
3. **Transport persistence / `ut_metadata` serving** — a minted broadcast's transport bytes
   are held in memory (`Broadcast.transport_bytes`) but not yet written to disk or served
   over BEP-9 `ut_metadata` to a peer that only has our infohash and wants the descriptor.
   Not needed for infohash-direct interop (this session's loopback used the infohash
   directly, matching how every other test in this project's history has worked).
