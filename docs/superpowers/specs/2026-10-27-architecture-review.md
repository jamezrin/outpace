# Architecture Review

**Date:** 2026-10-27
**Baseline:** `origin/main` at `7d77f41` (workspace version 0.8.0). Line references below are pinned to this commit.
**Scope:** Whole-workspace architecture review covering correctness, performance and robustness, assessed against the open roadmap (#195, #208, #212, #253, #246, #279, #213) and the permanent non-goals in #212.
**Depth:** The live path was read closely: the `ace-swarm::live_session` pool actor and peer workers, the seed store, seeding/reciprocal serving, startup discovery and connect, the startup buffer, and session fan-out. The HLS packager and audio normalization, broadcast/RTMP origination, the VOD coordinator, DHT internals and the tracker codec were only skimmed.

## Summary

The crate boundaries are sound since #189. Trust handling is strong: signed catalog response checks, descriptor/infohash binding, SSRF limits on transport URLs, and identifier hygiene. The serious risks are concentrated in one place, the live pool actor (`follow_peer_pool_with_discovery`). Both high-severity findings live there, and its shape (a single ~840-line async function) makes this class of defect easy to miss.

Already solid:
- `PeerSession::read_message` is cancel-safe;
- disk store I/O runs in an actor off the async runtime;
- one shared session fans out to subscribers through a broadcast channel, so a slow viewer lags and resyncs at a keyframe instead of stalling the download;
- inbound peers are capped globally and per stream;
- the HTTP and RTMP listeners default to loopback;
- a warm-peer cache exists for restarts.

## Findings

Severity is ranked. "Verified" means the defect was confirmed by reading the code path end to end. "Hypothesis" means the mechanism is traced but not reproduced.

### H1. Pieces that fail signature verification are stored, advertised and served (verified) — #285

- `live_session/pool.rs:~719` writes each chunk to the seed store before `reasm.add_block_with_contribution` (`pool.rs:~728`) verifies the completed piece.
- `Continuity::release_rejected_piece` (`continuity.rs:140`) does not purge the store.
- The store counts a piece as available once all its chunks are present (`store.rs` `has_piece`/`have_pieces`). The seeder advertises from that list (`seed.rs:251` bitfield, `seed.rs:288` live-HAVE), and `seed/protocol.rs` `prepare_reply` serves it to inbound and reciprocal peers.
- **Impact:** outpace relays pieces it has itself rejected to official engines and other nodes. A skipped piece stays served until it is evicted.
- **Direction:** commit to the store only after verification, or purge on rejection and track a verified bit.

### H2. The live pool actor can deadlock on its bounded channels (hypothesis) — #284

- The central loop awaits sends into 64-slot per-peer command channels (`pool.rs:75`; `pool_support.rs` `advance_pool_requests`, `send_peer_command`).
- Peer workers await sends into the shared event channel of `max_active_upstreams × 32` = 128 entries (`peer.rs:30/37/47/52`).
- Nothing keeps a peer's command queue below 64: the scheduler allows 256 pieces in flight per peer (`continuity.rs:78`), and reciprocal id=6 replies are unbounded (`pool.rs:860–873`).
- An upstream that floods id=6 and stops reading can block its worker in a write (20 s timeout) while the event channel fills. The worker then blocks forever reporting `Lost`, and the central loop blocks forever on that worker's queue.
- **Impact:** the stale timer and retransmits never run; clients stay attached and receive nothing. Seeding is on by default, so a remote peer can trigger this. A large catch-up burst after a reconnect meets the same precondition (see #246).
- **Direction:** never await on a single peer from the actor (`try_send`, and treat a full queue as an overloaded peer), cap queued replies per peer, and add a non-reading duplex peer test.

### M1. The connect budget structure may explain slow starts (hypothesis) — #253

- `CONNECT_TIMEOUT` = 3 s covers TCP, the BitTorrent handshake and the window exchange (`live_session/mod.rs:54`). The inner TCP timeout is also 3 s (`peer.rs:105`).
- A slow TCP connect can therefore consume the whole budget, and a dropped SYN is reported as `total_timeout`.
- With 12 parallel connects, 98 attempts take about 25 s in the worst case, consistent with the observed 5–21 s first byte.
- Per-phase timing already shipped (#276), so the next step is to rerun the A/B with the breakdown by phase. Only then tune the TCP sub-budget and cold-start fan-out.
- The 30 s prebuffer is not the time-to-first-byte cause: the prefetch is derived from it to fill the reservoir quickly.

### M2. Concurrent sessions are unbounded (verified) — #286

- No stream-count or global memory setting exists.
- Each session can hold a 128 MiB seed store, up to 128 MiB of startup reservoir, and up to 512 reassembly pieces.
- The Docker images bind `0.0.0.0:6878`.
- This is resource exhaustion, not missing authentication: the official engine is unauthenticated too, and `docs/deployment.md` already recommends a proxy.
- **Direction:** a configurable session limit that still lets clients join running sessions, with 503 or the compat error envelope on refusal.

### M3. Inline waits stall every upstream (verified) — #287

- Stale-gossip harvesting is awaited inline for up to 750 ms (`pool.rs:~518`), and each chunk awaits the store mutex.
- Each of these pauses event handling for all peers.

### M4. The pool actor is monolithic (maintainability) — #287

- `follow_peer_pool_with_discovery` spans `pool.rs:158–1000`.
- `spawn_candidate_connects` is called 9 times with identical arguments, and the entry points carry 12–18 positional arguments under `too_many_arguments` allows.
- **Direction:** a `PoolActor` struct with one handler per message, a single refill helper and pure decision functions, with no behaviour change. Land it after #284 and #285, and before the #210 soak.

### Low — #288

- `read_message` zeroes a 16 KiB stack buffer and memmoves its `Vec` on each message.
- A live chunk is copied about 3 times on receipt and again when served.
- The startup reservoir scans data byte by byte.
- `rand` and `sha1` each appear in two versions.
- None of this matters at today's bitrates. It becomes relevant for support-node fan-out (#270).

### Product note — #210

The default 30 s prebuffer with derived prefetch starts playback roughly that far behind the live edge. Record latency to live alongside first-byte time in the soak sign-off.

## Ruled out

- Direct-TS HTTP output resets its keyframe gate on `StreamEvent::Discontinuity` and on broadcast lag (`http/streams.rs:~238`), so a missing gate reset there does not explain #246.
- `PeerMessage` framing is capped at 2 MiB (`ace-wire/src/message.rs`), and `read_message` buffers safely under `select!`.

## Roadmap changes made

- The v0.5.0 milestone was renamed to **v0.9.0**, because v0.6.0–v0.8.0 shipped first. It is the last milestone before v1.0.0. v0.8.0 was closed.
- v0.9.0 order: #284 → #285 → #253 → #286 → #287 → #246 → soak evidence for #210.
- The #208 gate gained a "Robustness invariants" section (#284, #285, #286).
- #269, #271 and #272 were moved to Post-1.0. #270 also moved to Post-1.0 and is blocked by #285. #288 is Post-1.0.
- Scope is unchanged. #279 (refuse protected streams before joining the swarm) remains a v1.0 blocker, and nothing here expands beyond #212.
