# 21 — Seeder ground truth: piece-header structure + official-engine interop proof

**Status: PARTIAL.** Captured the real 8-byte piece-header structure from the live swarm
(structural finding, semantics of the varying half still open). Separately, proved
**bidirectional protocol acceptance against the official engine itself** (not just real
swarm peers) running in the `re/` Docker sandbox — outpace downloaded 9 MB of real live
data through a direct connection to the official engine's peer port. The REVERSE direction
(engine downloading FROM outpace) was not exercised in this session — see "What's left."

Both experiments ran with WARP off, Docker available, against the documented known-good
live channel (`docs/protocol/notes/02-test-streams.md`): content_id
`cid1`, infohash (at capture time)
`50e93529d3eb46a50506b14464185a15292d6e47`.

## 1. Piece-header structure (the `[8B header][2B chunk][data]` block)

Used the existing `ace-peer` `live_recon_unchoke` `#[ignore]`d harness (already prints
`head=hex::encode(&block[..24])` per received `Piece`) against a live, currently-unchoked
real swarm peer (`85.87.156.75:8621`). Sampled chunk 0 across 6 consecutive pieces:

```
piece 14718263: 41da9074 c08689ae | chunk=0000 ...
piece 14718264: 41da9074 c0873860 | chunk=0000 ...
piece 14718265: 41da9074 c1106df0 | chunk=0000 ...
piece 14718266: 41da9074 c19070f0 | chunk=0000 ...
piece 14718267: 41da9074 c19156d4 | chunk=0000 ...
piece 14718268: 41da9074 c2247832 | chunk=0000 ...
```

And all 8 chunks of pieces 14718267/14718268 (chunk index at `block[8..10]` matches the
requested chunk exactly in every case — confirms the existing `[8B hdr][2B chunk]` layout
assumption from note 19 is correct):

```
piece 14718267, chunks 0..7: header = 41da9074c19156d4 (CONSTANT across all 8 chunks)
piece 14718268, chunks 0..7: header = 41da9074c2247832 (CONSTANT across all 8 chunks)
```

**Findings:**
- The 8-byte header is **constant across every chunk of one piece**, confirming it's a
  genuine **per-piece** header (not per-chunk) — exactly the role our `piece_header: [u8;8]`
  parameter in `build_piece`/`SeederSession::serve` already assumed.
- `header[0..4]` (`41da9074`) was **constant across all 6 sampled pieces** (and across both
  peers' worth of earlier capture in this session) — a session/stream-wide value, not
  piece-specific.
- `header[4..8]` varies per piece (`c08689ae`, `c0873860`, `c1106df0`, `c19070f0`,
  `c19156d4`, `c2247832`) with **irregular deltas** between consecutive piece indices
  (44,722 then ~8.99M then far smaller jumps) — **not a simple linear counter**. Consistent
  with either a hash/checksum of piece-specific content, or a fine-grained timestamp with
  non-uniform real-world piece-arrival jitter (live video bitrate isn't constant). Did not
  converge on which within this session's time budget.
- **Not captured:** whether `header[4..8]` is reproducible across *different peers* for the
  *same* piece (would distinguish "source-derived, swarm-wide" from "relay-peer-specific").
  Planned but not completed — see "What's left."

**Practical implication:** our own decode path (`LiveChunk::from_message`) has *never*
inspected these 8 bytes — it only reads the chunk index at `block[8..10]` and treats
`block[0..8]` as opaque. Since note 19/20 already proved full live download → VLC playback
end-to-end while completely ignoring this header, **the header's content is not validated
by any consumer we've tested** (ourselves, including against the official engine in part 2
below). This means the current `[0u8;8]` placeholder in `build_piece`/`SeederSession::serve`
is not *known* to be rejected by anything — but we have not yet proven a real consumer
*accepts* served pieces with a wrong/placeholder header either (that requires part 2's
reverse direction, not yet achieved).

## 2. Official-engine interop: outpace downloads from the real engine

Stood up the existing `re/sandbox` Docker engine (image already built from a prior
session: `docker compose -f re/sandbox/docker-compose.yml up -d acestream`). Confirmed
reachable on its bridge IP (`172.23.0.2`) with peer port `8621` open (confirmed via
`/proc/net/tcp` inside the container — matches note 01's "LM listening on port 8621").

Started the engine on the **same live channel** via its local control API (a call to our
own sandboxed test instance, not the production discovery path — distinct from the
project's "never call Acestream's discovery/index API" constraint, which is about
*outpace's own* swarm discovery):

```
curl "http://127.0.0.1:6878/ace/getstream?content_id=cid1&format=json"
curl -N "http://127.0.0.1:6878/ace/r/<infohash>/<playback_session_id>"   # nudge it into active streaming
```

Then ran outpace against the *same* infohash with the engine's container address as a
bootstrap peer:

```
OUTPACE_ACE_PEERS=172.23.0.2:8621 cargo run -p ace-engine --bin outpace
curl -N http://127.0.0.1:<port>/streams/ace/<infohash>.ts
```

**Result (outpace's log):**
```
[ace] 172.23.0.2:8621: connected + handshaked
[ace] 172.23.0.2:8621: window min=14718260 max=14718269 -> start=14718261 head=14718269
[ace] 172.23.0.2:8621: UNCHOKE -> requesting pieces 14718261..=14718269
```

**Result (engine's own `/ace/stat` — note `uploaded` and `peers`):**
```json
{"peers": 3, "speed_up": 377, "uploaded": 9437184, ...}
```

The official engine **accepted outpace's full signed handshake** (Ed25519 node
identity + signature, BEP-10 extended handshake, live `mi` window per note 17/19),
**unchoked it, and served ~9 MB** of real live MPEG-TS over the connection — exactly the
same protocol outpace already uses against real swarm peers, now confirmed against the
**reference implementation itself**, in a fully controlled, reproducible local setup (no
reliance on which real swarm peers happen to be reachable on a given day).

This is the harder, more rigorous half of the wire-compatibility question: it proves the
official engine's *leecher-acceptance* path treats outpace indistinguishably from a real
peer. It does **not** yet prove the *seeder* direction.

## What's left (not RE-blocked — just not done this session)

1. **Reverse direction (engine downloads FROM outpace) — root cause fixed, but the
   specific scenario tested still doesn't trigger it; a genuine window-lead is needed.**
   Root cause identified: `follow_one_peer` (the outbound leecher loop) mirrors the peer's
   own advertised window in our `mi` and never proactively sends `Have` for newly-acquired
   pieces — so a peer (including the engine) has no signal that outpace holds anything
   it might want. Fixed and merged to `main`: `follow_one_peer` now sends `Have(piece)`
   over the existing connection the moment a piece completes (`ace_provider.rs`, commit
   `02f6d05`).

   **Re-ran the same experiment with the fix in place** (same infohash, same container):
   outpace connected, was unchoked, requested+received pieces, and (confirmed via the
   new code path) sent `Have` for each one back to the engine. `/status` on outpace's
   side stayed `uploaded: 0` — **as architecturally predicted, not a bug**: because we
   mirror the engine's own reported window at connect time (`start`/`head` computed from
   *their* `mi`), every `Have` we send is for a piece the engine, by construction, already
   has. The fix is necessary infrastructure and is now real and tested, but this exact
   scenario (both clients freshly joining the same live edge of the same swarm at nearly
   the same time) never gives outpace a genuine surplus to advertise. Proving the
   reverse direction needs either: a real window-lead (timing-dependent, not forceable with
   the current single-peer-at-a-time `follow_live` architecture — getting one would need
   S2's multi-peer work, out of scope here), or one of item 3's two routes below.
2. **Cross-peer header reproducibility** (does `header[4..8]` match across two different
   peers serving the same piece?) — would resolve whether it's source-derived or
   relay-specific. One query was attempted but the daemon reconnected to the same peer
   (`85.87.156.75`) both times; need to explicitly target a second peer address.
3. **Engine-as-inbound-target**: getting the engine to dial outpace directly (rather than
   outpace dialing the engine) would more directly test the `PeerListener`/
   `SeederSession::serve` path end-to-end against the official engine. Requires either (a)
   the engine discovering outpace organically via DHT/tracker self-announce (not yet
   wired — `announce_seeder` exists since the v2-offline-foundations branch but has no
   production caller), or (b) the engine's I2I instance-coordination API on port 62062
   (briefly probed — not HTTP, no quick win; would need Frida/binary RE to characterize,
   same effort class as the node-identity crack in note 15/17).
4. **`header[4..8]` semantics** (hash vs. timestamp vs. something else) — open.

None of items 1-4 are blocked on environment access (Docker + non-WARP networking both
confirmed available and used in this session) — they're scoped follow-up work.

## Reproduce

```bash
# 1. Sandbox engine (image already built; see note 01 if rebuilding)
docker compose -f re/sandbox/docker-compose.yml up -d acestream
sleep 3
docker inspect sandbox-acestream-1 --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'

# 2. Start the engine on the test channel
curl "http://127.0.0.1:6878/ace/getstream?content_id=cid1&format=json"
# (grab playback_url/infohash/playback_session_id from the response, nudge with a short curl -N on playback_url)

# 3. Point outpace at the engine's container IP for the same infohash
OUTPACE_ACE_PEERS=<container_ip>:8621 cargo run -p ace-engine --bin outpace
curl -N http://127.0.0.1:6878/streams/ace/<infohash>.ts   # (use a different OUTPACE_BIND if 6878 is taken by the sandbox)

# 4. Header capture (any currently-live peer ip:port + the live infohash)
ACE_PEER=<ip:port> ACE_INFOHASH=<40hex> ACE_PIECES=6 ACE_CHUNKS=1 \
  cargo test -p ace-peer live_recon_unchoke -- --ignored --nocapture
```
