# 24 — Seeder self-announce wired (tracker + DHT), live-verified

**Status: DONE.** Part of the push toward full leech+seed parity with the official
engine; see the v2 compliance design and issue #4 for remaining productization work.

## What changed

1. **`announce_seeder` wired into the session lifecycle.** It existed in `ace-swarm::discover`
   since the v2-offline-foundations branch but had no production caller. `ace_provider::open`
   now spawns a periodic self-announce (`announce_seeder_periodically`, every
   `SEEDER_ANNOUNCE_INTERVAL` = 4 min) alongside `follow_live`, via `tokio::select!` — whichever
   ends first (normally the download loop, on consumer-drop or no-reachable-peer) tears down
   the other, so there's no separate lifecycle to manage. Gated by `enable_seeding` (if
   disabled, the announce future is `std::future::pending()` — never fires, since we shouldn't
   claim to be a seeder while deliberately not serving).
   **Superseded by note 52:** the self-announce now advertises the *peer* port (not the HTTP
   API port) and is gated on having an inbound listener (`enable_inbound`) rather than on
   `enable_seeding`.

2. **DHT `announce_peer` (BEP-5) implemented — this didn't exist at all before.**
   `ace_swarm::dht` only ever implemented `get_peers` (the read half of the DHT). Real
   Acestream swarms are largely DHT-populated (per our own docs), so tracker-only
   self-announce under-serves discoverability. Added:
   - `build_announce_peer` — the KRPC query, carrying the opaque `token` a node handed us in
     its own `get_peers` response (BEP-5 requires echoing a token a node itself issued —
     anti-spoofing).
   - `GetPeersResponse.token` — `parse_response` now extracts `r.token` alongside the
     existing peers/nodes.
   - `dht_walk` — factored the iterative bootstrap/frontier/query-response loop out of
     `dht_get_peers` (previously inlined) so `dht_announce_peer` can reuse it instead of
     duplicating ~50 lines of protocol-level logic. Same external behavior for
     `dht_get_peers` (unit-tested, unchanged).
   - `dht_announce_peer(infohash, peer_port, budget) -> usize` — walks toward `infohash`
     exactly like `dht_get_peers`, and for every node that hands us a token, sends it
     `announce_peer` for our port. Returns how many were sent (best-effort — DHT is UDP,
     fire-and-forget, same as the real engine).

   Deliberately **not** folded into `announce_seeder` itself: a first attempt did this and it
   turned `announce_seeder`'s previously-instant offline unit test into a live, ~5-second
   network-dependent one (caught immediately by re-running the test suite — see
   `superpowers:systematic-debugging` discipline: verify after every change, not just at the
   end). `dht_announce_peer` is a separate, composable primitide; `ace_provider`'s periodic
   loop calls both explicitly.

## Live verification (this session, same host, real network)

```
[ace] seeder self-announce for cid2: 0 tracker peer(s)
seen, DHT announce_peer sent to 8 node(s)
```
Captured while the daemon was actively downloading the Synthetic Live Channel channel. The tracker returned
0 peers (plausible — a single UDP tracker isn't always well-populated); the DHT walk found 8
real nodes, obtained tokens from them, and sent `announce_peer`. Also ran the DHT announce
path standalone (`dht_live_announce_sends_without_erroring`, `#[ignore]`d,
`ACE_INFOHASH=... cargo test -p ace-swarm dht_live_announce -- --ignored --nocapture`):
completed in 3.1 s, 8 nodes announced to.

**What this proves:** outpace now actively tells the DHT (and trackers) "I am seeding
this infohash," which is the prerequisite for any real peer or the official engine to ever
discover and dial outpace on its own. **What this does NOT yet prove:** that anything
actually dials us back as a result — DHT/tracker announce is fire-and-forget; we have no way
to directly observe who queried the DHT for our infohash afterward. That's the next step
(Task 7, note 25).

## Tests
- `ace-swarm`: `announce_peer_query_roundtrips`, `parses_token_needed_to_announce_back`,
  `no_token_parses_as_none`, plus the existing `get_peers`/`dht_get_peers` tests (unchanged,
  confirmed still fast/offline after the `dht_walk` refactor).
- `ace-engine`: `seeder_announce_never_fires_when_seeding_disabled`.
- Full workspace `cargo test` green, clippy clean, both before and after.
