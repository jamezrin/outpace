# RESUME — start here in a fresh session

This is the single entry point for picking up the **outpace** project (an
open-source, from-scratch reimplementation of the **Acestream P2P streaming
engine**) in a new Claude Code session with no prior context.

## 30-second orientation
- **Goal:** a CLI daemon that joins the Acestream network, pulls a stream, and
  re-exposes it (MPEG-TS / HLS / m3u) so VLC/Jellyfin/dispatcharr can play it.
  No closed-source blobs. Engine only — Android/player apps out of scope.
- **Viability:** PROVEN (GO). An independent client built here was accepted by live
  swarm peers. See `docs/superpowers/specs/2026-06-28-phase0-findings.md`.
- **Read next, in order:** this file → `docs/superpowers/specs/2026-06-28-acestream-engine-reimplementation-design.md`
  (design) → `docs/protocol/wire-protocol.md` (the protocol) →
  `docs/protocol/transport-file.md`. The `docs/protocol/notes/` folder has the
  detailed reverse-engineering findings (numbered 00–13).

## Current state (what's on `main`, all committed + tests green)
Run `cargo test` — should be all green (live-network tests are `#[ignore]`d).

> **⚠️ Correction (note 22):** the "downloaded 8.3 / 9.4 MB" results below were read as
> *success* but were actually the bug — that is exactly the static initial prefetch window
> (~9 pieces). The live download **never advanced past it** because the loop only requested
> more pieces on a standard BT `Have`, which real Acestream peers don't send. **Fixed** in
> three parts, all confirmed against a live operator run: (1) the real advancement signal is
> an 8-byte custom `id=4` message (`[u32 stream][u32 piece]`), now decoded and driving the
> request frontier; (2) peer connect is now raced in parallel (`connect_any`) instead of
> serially trying dead peers, fixing a multi-second time-to-load; (3) `StreamManager` had a
> start-race where concurrent first requests for the same id (VLC opens more than one
> connection) each ran a full `provider.open()` — duplicate discovery + two connections from
> our same node_id, which a real peer may drop — now serialized via a start lock. Throughput
> is now logged (`served N MiB`) so silent freezes are directly observable. See
> `docs/protocol/notes/22-live-edge-never-advances.md`. **Live-verified in-session** (this
> host, real swarm): 42–133 MB continuous captures, `ffprobe`/`ffmpeg` decode real
> 1920×1080 H.264 + 48 kHz AAC with genuinely different content at the start vs. end of a
> capture (not a frozen loop), and VLC's own internal demuxer locks PAT/PMT/SPS/PPS and the
> `KeyframeGate`'s SEI recovery point end-to-end against the live daemon.
>
> **Follow-up (note 23):** a related but distinct bug — every peer reconnect (which real
> swarm connections do routinely) recreated the piece-reassembly/resync state from scratch,
> **splicing a duplicate chunk or silently stalling the reassembler** depending on where the
> new peer's own window happened to land relative to where we'd left off. This is what the
> operator then reported as "jittery/stuttery" playback after the freeze was already fixed.
> **Fixed** by making that continuity state (`Continuity` in `ace_provider.rs`) survive
> reconnects — resume from our own prior position, only skipping forward (via the new
> `PieceReassembler::skip_to`) on a genuine, unavoidable eviction gap. **Live-verified**: a
> real reconnect captured mid-session correctly resumed from the pre-reconnect target
> (not the new peer's own computed start), and a 172 s / 133 MB capture spanning that
> reconnect showed **zero PTS anomalies** across 4,301 video frames (no backward jumps, no
> gaps). Also fixed a smaller related issue found in the same pass: `connect_any`'s exclude
> list only remembered the single most-recently-lost peer, so a session could flip-flop back
> to a peer already proven bad; it's now a cumulative per-session blacklist (falling back to
> the full peer list only if excluding everyone would leave nothing to try). See
> `docs/protocol/notes/23-reconnect-continuity.md`.

| Phase | Status | Deliverable |
|---|---|---|
| 0 Protocol recovery | ✅ done | Specs + vectors + go/no-go memo (GO) |
| 1 `ace-wire` | ✅ done | infohash, bencode, handshake, msg framing, extended HS, **transport decoder** |
| 2 `ace-tracker` + `ace-peer` | ✅ done | BEP-15 UDP tracker; async peer session (handshake + read) — both verified live |
| 3 piece download → media → engine | ✅ done | **live HD video downloaded from the real swarm** (note 19: full signed handshake → UNCHOKE → Acestream piece requests → MPEG-TS → ffmpeg decodes 1920×1080 H.264+AAC). Protocol promoted into `ace-wire`/`ace-swarm` |
| 4 productization (outpace daemon) | ✅ built + **live-proven** | **Autonomous DHT discovery → connect → 9.4 MB live MPEG-TS downloaded from just an infohash (verified 2026-06-29).** Clean provider-abstracted daemon: `StreamProvider`/`TsSource` + `ProviderRegistry`, shared `StreamSession` broadcast fan-out (one download → many clients, **acexy-parity out of the box**), `StreamManager`, axum HTTP API, live HLS, config + persistent identity, tracker discovery, transport→`StreamInfo` resolve, `AceProvider`. All tests green + clippy clean; daemon boots & serves the clean API. Muxing drift (Task 19) fixed (`TsResync`); per-client start-on-keyframe done (`KeyframeGate`); content-id→infohash resolution over `ut_metadata` done (Task 18 — `ace_wire::ut_metadata`, `PeerSession::fetch_metadata`, `ace_swarm::resolve::resolve_via_peer`, offline mock-peer integration test); DELETE force-stop + abort-pump-on-teardown. **Spec complete, incl. live E2E (Task 20, verified 2026-06-30 — see `docs/protocol/notes/20-vlc-playback.md`):** daemon autonomously discovered 25 peers, downloaded 8.3 MB live MPEG-TS, ffmpeg decoded a 1280×720 H.264 frame from the served output, the served stream starts on a keyframe, and two clients shared one session (`/streams` → `clients:2`). Plan: `docs/superpowers/plans/2026-06-29-outpace-daemon.md`; spec: `docs/superpowers/specs/2026-06-29-outpace-daemon-design.md` |

Crates: `crates/{ace-wire,ace-tracker,ace-peer,ace-media,ace-engine}`. Workspace root `Cargo.toml`.
**Pure Phase 3/4 logic done (no live data needed):** `ace_wire::live` (LiveWindow/LivePicker),
`ace_wire::reassembly` (PieceReassembler: chunks→ordered bytes), `ace_media::{mpegts,hls}`
(TS align + HLS segment/manifest), `ace_engine::routes` (6878 URL surface). What's left needs
the live byte path: peer download loop / `ace-swarm`, then wire `ace-media`+`ace-engine` to it.

## Next: v2 — P2P compliance, seeding & broadcasting
The v1 daemon was **download-only (a pure leecher)**. v2 fixes that and adds origination. Spec:
**`docs/superpowers/specs/2026-06-30-compliance-seeding-broadcasting-design.md`**. Four phases:
**S1** reciprocal upload, **S2** inbound seeder, **B0** live-source-auth RE spike, **B1** HTTP
broadcast ingest. **Hard constraint throughout:** full **wire compatibility** with the official
engine — outpace and Acestream peers must interoperate transparently (an Acestream peer can't
tell it's talking to outpace). RTMP/SRT ingest is a documented future follow-up, out of scope.

### S1 — DONE (merged to main 2026-06-30, branch `seeding-s1-compliance`, plan `docs/superpowers/plans/2026-06-30-seeding-s1-compliance.md`)
Reciprocal upload over connections we already hold. Built + reviewed (spec + code-quality + a
final whole-branch pass; all green, clippy clean):
- `ace-swarm::store::PieceStore` — pure bounded rolling chunk store (FIFO evict lowest piece).
- `ace-wire::live_codec::build_piece` — send side of the live piece codec (inverse of `LiveChunk`).
- `ace-swarm::seed::SeederSession::serve` (advertise Haves → unchoke on `Interested` → answer
  `Unknown{id:6}` chunk-requests with `build_piece`).
- `ace-engine::ace_provider::follow_one_peer` — feeds a per-connection `PieceStore` from
  downloaded pieces and serves the peer's chunk-requests inline; `uploaded`/`peers_served`
  atomics surfaced at `/status`.

### v2 offline foundations — DONE (merged to main, plan
`docs/superpowers/plans/2026-06-30-v2-offline-foundations.md`)
Everything from S2/B1 that's buildable and testable WITHOUT a live Acestream swarm or RE sandbox.
All green, clippy clean, full workspace:
- `ace-wire::transport::encode_transport` — production transport-file encoder (inverse of
  `decode_transport`); unblocks B1 minting. Round-trips with the decoder.
- `ace-wire::chunker::TsChunker` — pure live-stream chunker (inverse of `PieceReassembler`);
  unblocks B1's `PUT /broadcast` ingest. Chunk-then-reassemble proven as the identity.
- `ace-peer::session::PeerSession::accept_handshake` — inbound/responder-side BT handshake
  (reads peer's handshake first, replies only if we serve the requested infohash).
- `ace-tracker`/`ace-swarm::discover::announce_seeder` — parameterized the tracker announce
  `event` field; added a seeder-announce helper (`left=0`, `Completed`). **Now wired** (note
  24, 2026-07-01): `ace_provider::open` runs a periodic tracker+DHT self-announce alongside
  the download loop, gated by `enable_seeding`. Also added `ace_swarm::dht::dht_announce_peer`
  — BEP-5's `announce_peer`, which didn't exist before (only `get_peers`/read-side DHT existed).
  Live-verified against the real mainline DHT and real trackers.
- `ace-engine::config::Config` gained inbound-seeder knobs: `peer_listen`, `seed_store_bytes`,
  `max_unchoked`, `max_inbound_peers`, `enable_seeding`, `enable_inbound`. `enable_seeding` is
  now wired (gates the three S1 serve arms in `follow_one_peer` — `Interested`, the chunk-serve
  arm, and `Have`-on-completion; `false` makes the provider a pure leecher). **`max_unchoked` is
  still accepted but not wired** — it configures `Choker`, which has no production caller until
  S2's multi-peer serve coordinator exists (a single outbound connection or a single inbound
  `SeederSession::serve` call has nobody to choose between).
- `ace-swarm::listen::{SeedRegistry, PeerListener}` — inbound seeder plumbing: a registry
  mapping infohash → shared `PieceStore`, and a bounded-concurrency TCP accept loop handing
  accepted peers to `SeederSession::serve`. Real-TCP loopback-tested (accept+handshake+serve,
  and refusal of an infohash we don't serve). **Known gap:** `SeedRegistry` entries are never
  evicted — unbounded by infohash count (each store is itself byte-bounded) for the process's
  lifetime; add eviction (e.g. tied to `StreamSession` teardown) if this becomes a real concern.
- Wired into the live engine: `follow_one_peer`'s store now comes from the shared registry (so
  downloaded pieces become servable to inbound peers too); `main.rs` optionally starts
  `PeerListener` — **gated OFF by default** (`enable_inbound=false`) so operators opt in to
  exposing a peer listener. Verified live: a default `cargo run` does NOT start the inbound
  listener; `OUTPACE_ENABLE_INBOUND=1` does.
- **Task 7's wire-compatibility blocker is closed in note 33.** Whether to flip
  `enable_inbound`'s default to `true` is now an exposure/product decision, not a known
  protocol blocker.

### Task 7 — WIRE-COMPATIBILITY PROVEN (`docs/protocol/notes/21`, `24`, `25`, `33`)
**2026-07-01 update (note 25):** discovered `references/ace-network-docs/docs/broadcasting/`
already documents `start-engine --stream-source-node`, letting the **official engine** run as
a broadcast origin locally against our own tiny HTTP MPEG-TS loop — deterministic,
reproducible ground truth (no live-swarm dependency). Reproduced note 21's full-protocol
interop proof this way (signed handshake accepted, `UNCHOKE`, real `Piece` data, same
`[8B header][2B chunk]` structure). Also confirmed the RSA key format for B0 (see note 25).
At that point, the infohash formula and reverse direction (engine downloads FROM outpace)
were still open; both are now superseded by notes 29 and 33.

With Docker + a non-WARP host (confirmed available 2026-06-30), ran real verification against
both the live swarm and the official engine itself (sandboxed):
- **Captured real piece-header structure** from a live peer: the 8-byte header is constant
  across every chunk of one piece (confirms it's per-piece, matching our `piece_header:[u8;8]`
  assumption) — `header[0..4]` is constant across pieces (session-wide), `header[4..8]` varies
  per piece with irregular deltas (hash? fine timestamp? undetermined).
- **Proved the official engine accepts outpace's full protocol stack** — signed handshake,
  Ed25519 identity, BEP-10 extended handshake, Acestream request/piece codec — by connecting
  outpace directly to the engine's peer port inside its own Docker sandbox and downloading
  9 MB of real live video from it. This is the harder, more rigorous half of wire-compatibility
  (the *reference implementation* treats outpace indistinguishably from a real peer).
- **Tried, broke, and reverted a fix for the reverse direction (engine downloads FROM
  outpace).** Added proactive `Have`-advertisement to `follow_one_peer` to give peers a
  signal to request from us — **live testing (prompted by VLC showing no video) found this
  makes a real swarm peer go silent**, confirmed by bisection (works before the commit, hangs
  with it, works again reverted). Reverted outright (no demonstrated benefit either way — see
  note 21's "⚠️ Update" section for the full account). **Lesson banked: any new behavior on
  the outbound leecher path needs a live download smoke test, not just mock/duplex tests,
  before being trusted.** This was later superseded by the controlled local-tracker proof in
  note 33: the official engine now downloads from outpace's inbound `SeederSession` path
  and returns media bytes.
- The `id=7` header semantics are now known: a big-endian `f64` Unix timestamp, constant per
  piece. Outpace preserves upstream headers and generates timestamp headers for B1
  originated pieces (note 33).

### B1 — DONE, including official-engine consumer interop (2026-07-01, notes 26/33)
`ace_wire::live_auth::LiveSourceAuth` (real RSA identity, byte-for-byte validated against a
captured real engine key — note 25), `ace_engine::broadcast::BroadcastRegistry` (mint/resume
a named broadcast; registers its `PieceStore` in the **existing** `SeedRegistry`, so it's
immediately servable via S1/S2 with no new serving code), and `PUT /broadcast/{name}`
(`http.rs`: responds instantly with the infohash, ingests the body — `TsResync` → `TsChunker`
→ `PieceStore` — in a background task). **Live-verified, two real daemon processes, real
TCP:** daemon A originates via `PUT /broadcast/bigchan`; daemon B, given daemon A's peer
address as a bootstrap peer and the returned infohash, downloads via the **unmodified**
`AceProvider` leech path; `ffprobe`/`ffmpeg` decode the served output to the real ingested
video. This is the design spec's "reachable milestone" for B1.

Along the way, found and fixed a real pre-existing gap: `SeederSession::serve` (S2, inbound)
never sent an extended handshake at all, so **outpace could never leech from its own
inbound listener** — our own `AceProvider` blocks waiting for that message. Fixed by signing
and sending it (identity + the store's live window) before advertising `Have`s; see note 26.

**Update (note 27, same day): B1 now signs pieces for real**, not a placeholder — see the B0
entry below. `ace_engine::broadcast::Broadcast` carries its `LiveSourceAuth`; the ingest
handler uses `ace_wire::signing_chunker::SigningChunker` (real RSA-PKCS1v15-SHA1 sign,
embedded as each piece's trailing bytes). New integration test
`ingested_piece_carries_a_real_verifiable_signature` proves this through the actual HTTP
ingest path (not just unit-level).

**Update (note 28, same day): B1 origination now self-announces for real.** Found a real
bug: a minted broadcast never called `announce_seeder`/`dht_announce_peer` at all — only
the leech path did. Fixed (`ace_provider::announce_infohash_periodically`, spawned once per
freshly-minted name from `http.rs`'s ingest handler, gated on an inbound listener actually
existing). Live-verified: minting now immediately produces a real DHT `announce_peer` to the
public mainline DHT — previously nothing happened until this fix.

**Update (note 29, same day): the infohash wall is cracked and fixed.** The earlier
`SHA1(entire transport-file bytes)` formula was the engine's separate transport-file/cache
hash, **not** the peer-wire swarm infohash. Importing the official `Transport.so` inside the
sandbox (with stubbed `ACEStream.*` imports and real bencode helpers) and wrapping
`hashlib.sha1` showed `Transport.load_transport_file_from_string(...).get_infohash()` hashes
this exact selected-field preimage:
`bencode([[name,v],[authmethod,v],[pubkey,v],[piece_length,v],[chunk_length,v],[bitrate,v]])`.
Implemented in `ace_wire::infohash` (`infohash_of_transport` now returns the official swarm
infohash; `transport_file_hash` preserves the raw wrapped-file SHA1), then wired through
content-id resolve and B1 broadcast minting. Regression vectors now match official engine
ground truth: `transport-01.bin` → `50e93529d3eb46a50506b14464185a15292d6e47`,
`transport-02.bin` → `685edf209ccfdf88977c0d317e1407baca486067`.

**Update (note 30, same day): official-engine-as-consumer was rerun.** The official engine
now returns the exact outpace infohash for a fresh `url=http://.../broadcast/{name}`
transport (`cafe006789abcdef0123456789abcdef01234567` in the proof run), proving the hash
fix in the live path. Playback still times out with `status=prebuf`, `peers=0`,
`downloaded=0`; outpace sees no inbound official peer. Docker container -> host peer-port
TCP reachability is proven separately, and experiments with descriptor `startup_nodes` plus
an ad hoc `peers=` query did not force `/ace/getstream` to dial. The current blocker is
therefore **peer discovery / direct peer injection for the official engine**, not transport
hashing or piece serving.

**Update (note 31, same day): the peer-discovery blocker is superseded.** A deterministic
local UDP tracker in the descriptor makes the official engine discover and dial outpace:
tracker announce → inbound TCP accept → Acestream handshake → signed extended handshake →
official extended handshake → outpace live bitfield. Official stats now show
`status=prebuf`, `peers=1`, `downloaded=0`, and outpace sees no `Interested` or `id=6`
chunk requests, only `id=11` stats and `id=34` telemetry. Implemented the pcap-derived live
bitfield (`id=5`) and fixed a real source-advertisement inconsistency: `mi.max_piece` /
`live_window_size` now describe complete pieces only, not a partial head. This did **not**
make the official consumer request data. The current blocker is therefore **source live-window
/ source-advertisement semantics after connection**, not infohashing or peer discovery. Next
highest-leverage lead: run/capture an official local source-node consumer path and mirror its
exact `mi` fields, bitfield range/timing, and initial piece numbering.

**Update (note 32, same day): the source-advertisement blocker is superseded.** Matching the
official source-node profile (64 KiB pieces, bitrate 8375, `distance_from_source=-1`,
`node_state=1`, `live_window_size=115`) plus removing the standard BT `Have` burst after
`Interested` gets the official consumer to request and receive pieces. Proof
`proof-no-bt-have-big-1` (`cafe016789abcdef0123456789abcdef01234567`): official
`getstream` returned the exact outpace infohash, outpace logged
`Interested -> Unchoke -> id=6 requests -> id=7 Piece replies`, and official stats showed
`peers=1`, nonzero `speed_down`, and a large `downloaded` counter. Playback still timed out
with zero bytes, and the official consumer repeatedly re-requested pieces `100..102` instead
of advancing. That narrowed the blocker to **piece acceptance after delivery**, not
discovery, infohashing, source-window advertisement, or request routing. The suspected
`[0u8;8]` live piece header was confirmed and fixed in note 33.

**Update (note 33, same day): the piece-acceptance blocker is resolved.** Capturing the
local official source node showed the `id=7` 8-byte live piece header is a **big-endian
IEEE-754 `f64` Unix timestamp**, constant across all chunks of a piece
(`41da91522634c2ee` → `1782925464.8243976`, etc.). Implemented header preservation and
generation end-to-end: `LiveChunk` parses it, `PieceStore` stores it per piece, relay paths
serve the stored header, and B1 broadcast ingest generates one timestamp header per piece.
Live proof `proof-header-1` (`cafe026789abcdef0123456789abcdef01234567`) with a controlled
local tracker now gets the official engine past prebuffer: official stats moved to
`status=dl`, `peers=1`, `downloaded=196608`; following the official playback URL returned
`HTTP 200` and `196228` bytes, `ffprobe` identified H.264 video, and `ffmpeg` decoded a
frame from the short capture. Reverse-direction official-engine-as-consumer interop is now
proven through media output.

Also still open: ingest-resume continuity (piece numbering restarts at 0 on every ingest
task, a known gap); transport persistence / `ut_metadata` serving for a minted broadcast.

**Update (note 34, 2026-07-02; corrected by note 44): the Acestream-style HTTP
content-id surface is wired.** Outpace serves
`/ace/getstream?format=json&content_id=<40hex>` and returns Acestream-shaped
`playback_url`, `stat_url`, and `command_url` values under `/ace/...`. The first
implementation passed `content_id` directly as the playback/session swarm key; note 43's
official-engine comparison proved that was wrong for real `acestream://` ids because the
official engine maps content id cid1 to infohash `50e935...6e47`. As of note 44,
`content_id=` returns URLs keyed as `/ace/.../cid:<content_id>/outpace`, so playback
enters the `ut_metadata` resolver path. `infohash=` and `id=` remain direct swarm-infohash
inputs. Full HTTP API parity (`/ace/manifest.m3u8`, `/server/api`, `getstream?url=...`)
remains follow-up work.

**Update (note 35, 2026-07-02): startup improved, but continuous playback is still not
proven on the Synthetic Live Channel public id.** Official engine `3.2.11` maps
`content_id=cid1` to
`infohash=50e93529d3eb46a50506b14464185a15292d6e47`, but in this session the official
engine itself stayed at `status=prebuf`, `peers=1`, `downloaded=0` for that target.
Outpace can serve the valid initial MPEG-TS window (`8,308,472` bytes, H.264/AAC,
188-byte aligned), and DHT/discovery startup was improved (`dht_live_finds_peers` went
from ~21.3 s to ~7.8 s; playback first byte from ~15.1 s to ~7.7 s, with one traced run at
4.6 s). The remaining precise blocker is live continuity after the initial window:
`85.87.156.75:8621` stays connected and sends `id=11`/`id=34` plus one large `id=12`, but
does not send advancing `id=4` or moving window values. The new stale-upstream guard now
reconnects after 12 s without live progress instead of waiting forever; in this swarm
snapshot all other discovered peers failed connect+handshake, so playback still stopped at
the initial window. Full `id=12` was captured via `strace` (raw tcpdump lacked
`CAP_NET_RAW`) and is documented in `docs/protocol/notes/35-live-startup-static-upstream-and-id12-capture.md`.
Next lead: refresh discovery after stale-peer exhaustion or follow multiple peers; only
promote `id=11`/`id=12` parsing after a capture where those values actually advance.

**Update (note 36, 2026-07-02): refresh-after-exhaustion is now implemented.** When
`follow_live` exhausts the current peer candidates after a stale upstream, it now runs
`discover_peers` again, merges newly discovered peers, and keeps the client in a retrying
prebuffer state instead of ending the source. If rediscovery finds no new peers, the
previously excluded peers are allowed back into rotation after the discovery cycle, so a
reachable peer can be re-evaluated if its window later advances. Live smoke on the same
infohash showed the loop working (`rediscovery found 16, added 1`, then later retried
`85.87.156.75:8621`), but playback still capped at the same `8,308,472` byte initial
window because no alternate peer completed the live follow path and the known peer's window
remained static. Remaining no-stutter lead: multi-peer following/scheduling, preferring
upstreams whose windows advance and filling missing chunks from any peer that has them.

**Update (note 37, 2026-07-02): initial upstream selection now ranks advertised live
windows.** `connect_any` no longer returns the first peer that only completes connect +
handshake; a candidate must also send its extended handshake/live `mi` window. After the
first usable candidate, outpace waits a short `250 ms` grace window for other
near-complete candidates from the same batch and chooses the best advertised window by
`max_piece`, then `position`, then lower `distance_from_source`. The selected window is
passed into `follow_one_peer`, so the initial extended handshake is not consumed twice.
Tests cover preferring a fresher head and using distance only as a tiebreaker. Live smoke on
`50e93529d3eb46a50506b14464185a15292d6e47` still capped at `8,308,472` bytes with valid TS
sync and first byte `7.605448` s: the selection step was active, but the swarm again yielded
only `85.87.156.75:8621` as a usable upstream, with the same static
`max_piece=14718269`. Remaining blocker: a real small active-peer set that keeps several
handshaked peers alive, tracks which windows advance over time, and fills missing chunks
from any peer advertising coverage.

**Update (note 38, 2026-07-02): reconnect now resumes from the real emit cursor and rejects
advertised windows behind it.** `Continuity::resume` no longer treats `requested_to + 1` as
proof that prior pieces were received; it uses `PieceReassembler::next_needed()` as the
source of truth and resets the request frontier so a new peer is asked for the first
still-missing piece. `follow_live` also rejects a reconnected peer before `follow_one_peer`
if `mi.max_piece` is lower than that next needed piece. Live smoke on the same infohash
again capped at `8,308,472` bytes with first byte `7.667642` s, but the post-window loop is
tighter: the retried `85.87.156.75:8621` advertised `max_piece=14718269` while outpace
needed `14718270`, so it was logged as a stale advertised window and dropped immediately
instead of burning another 12 s stale-follow interval. This is still not continuous
playback; the next real step remains a small active-peer scheduler that keeps multiple
handshaked peers alive and fills the next-needed cursor from any peer with coverage.

**Update (note 39, 2026-07-02): the production provider now uses the shared scheduler as
its request frontier.** `AceProvider` no longer has a private single-peer `requested_to`
piece frontier. `Continuity` owns `ace_swarm::scheduler::Scheduler` plus per-piece chunk
receipt bookkeeping, schedules from `PieceReassembler::next_needed()` toward the head, and
marks scheduler requests complete when all chunks for a piece arrive. On reconnect it calls
the new `Scheduler::clear_in_flight()` so requests assigned to the dropped peer are
requeueable. Live smoke still capped at `8,308,472` bytes (`HTTP 200`, first byte
`6.239245` s, valid TS sync), but the log now shows `UNCHOKE -> scheduling from piece ...`
and preserves the stale-window rejection. This is a structural step toward the remaining
blocker, not a playback fix by itself: the next step is a real active-peer I/O coordinator
that keeps several `ConnectedUpstream` sessions alive and feeds their events into this
shared `Continuity` + `Scheduler`.

**Update (note 40, 2026-07-02): active-peer state now sits in the provider request path.**
`ace_swarm::scheduler` gained `ActivePeers`, which tracks each active peer's advertised
window, choke state, and per-peer in-flight piece set, and returns a dropped peer's assigned
pieces so they can be requeued with `Scheduler::on_drop`. `AceProvider::Continuity` now owns
`ActivePeers`; the current single-peer loop registers the connected peer, updates its
unchoke/window state from messages, and fills requests through `ActivePeers::assign` instead
of a one-off `PeerView`. Live smoke again served the valid initial window only
(`8,308,472` bytes, first byte `7.600419` s, clean TS sync) and preserved stale-window
rejection. This still does not run multiple sessions concurrently. Remaining blocker:
spawn/own several handshaked `ConnectedUpstream` sessions, feed their events into one
shared `Continuity`, and send chunk-request commands back to whichever peer
`ActivePeers::assign` selected.

**Update (note 41, 2026-07-02): the multi-upstream coordinator is now wired, but the
Synthetic Live Channel live target still produced only one usable upstream.** `connect_pool` now returns a
bounded set of handshaked peers that have advertised live windows, and production
`follow_live` feeds that pool into a new `follow_peer_pool` coordinator. The coordinator
spawns one worker task per upstream, processes peer messages centrally, schedules through
the shared `Continuity` + `ActivePeers` + `Scheduler`, dispatches chunk-request commands
back to the selected peer, and requeues only a dropped peer's in-flight pieces. Focused
worker tests, `cargo test -p ace-engine`, `cargo test -p ace-swarm`, clippy, targeted
`rustfmt --check`, build, and `git diff --check` all passed. Live smoke on
`50e93529d3eb46a50506b14464185a15292d6e47` still served exactly the same valid initial
window (`8,308,472` bytes, first byte `7.605502` s, clean TS sync) and then stalled: the
new log showed `upstream pool (1 peer(s))` with only `85.87.156.75:8621`, window
`14718220..14718269`, followed by the same stale-window rejection at needed piece
`14718270`. Remaining blocker is now narrower: outpace can coordinate multiple
handshaked upstreams, but this run still obtains only one window-advertising upstream.
Next lead: keep filling the active pool in the background while the first peer serves, and
log why other discovered peers fail (TCP connect vs 66-byte handshake vs window read vs
signed-handshake acceptance).

**Update (note 42, 2026-07-02): background pool filling and peer-acquisition failure
classes are now wired.** While the first upstream serves, outpace now keeps trying the
remaining discovered candidates and can add successful `ConnectedUpstream`s to the active
coordinator without delaying first byte. Connection attempts are classified as `tcp`,
`handshake`, `window`, or `connected`, and tests cover the refill candidate filter plus
failure summary formatting. Live smoke on the same infohash still capped at the same valid
initial MPEG-TS window (`8,308,472` bytes, first byte `7.714653` s, clean TS sync), but it
now explains why: initial selection completed `attempted=3 connected=1 tcp=2`, then the
background refill tried the other 12 candidates and finished `attempted=12 tcp=11
handshake=1`. No alternate peer reached the live-window stage. Remaining blocker is now
peer acquisition quality for this public target, not the multi-peer coordinator: the
current discovery set has only one usable upstream, and that upstream's advertised window
stays static behind the next-needed piece. Next lead: compare official-engine peer
acquisition for the same infohash on the same network, or deepen background discovery
without delaying first byte and feed newly found peers into the refill path.

**Update (note 43, 2026-07-02): deeper background discovery now runs concurrently with
known-peer refill, and the official engine also stalls on this target.** `ace-swarm`
gained `DiscoveryOptions` / `discover_peers_with_options`, plus a public targeted DHT
lookup, so background refill can ask for a deeper peer set without changing fast-start
discovery. `AceProvider` now starts that deeper discovery immediately while it retries
known candidates; the discovery budget is 8 s, below the 12 s stale-upstream timeout.
Live smoke still capped at the same valid initial outpace window (`8,308,472` bytes),
but startup first byte improved to `3.012109` s in the proof run and the new log showed
the deeper discovery completed before stale reconnect (`background upstream discovery:
found 1, added 0`; refill still `tcp=13 handshake=1`). A fresh official-engine 3.2.11
sandbox comparison on the same content id resolved the same infohash and also found only
`peers=1`; official stats reached `status=dl` / `downloaded=10485760`, and following the
official playback URL served `10,484,740` clean MPEG-TS bytes before the same 75 s timeout.
Conclusion: this Synthetic Live Channel content id is still useful as a startup/protocol regression smoke
test, but it is not a healthy continuous-playback proof target in the current network
snapshot. Next lead: find a public content id where the official engine actually advances
continuously, or use the controlled official source-node/local-tracker setup as the live
continuity proof target.

**Update (note 44, 2026-07-02): `/ace/getstream?content_id=` now routes to the real
content-id resolver.** The compatibility handler no longer treats a 40-hex `content_id` as
a direct swarm infohash. It returns playback/stat/command URLs containing
`cid:<content_id>`, so `/ace/r/...` calls `AceProvider::open("cid:<content_id>")` and
attempts content-id→infohash resolution through BEP-9 `ut_metadata`. The public JSON
still reports the caller's id without the internal `cid:` prefix. Regression tests assert
that `content_id=` creates only the `cid:<id>` manager session, while `infohash=`/`id=`
remain raw direct keys. A live smoke on the known public id returned the expected `cid:`
URLs, then playback returned `HTTP 404` after `37.598872` s; daemon logs showed DHT found
14 metadata peers and reached `85.87.156.75:8621`, but that peer sent no
`metadata_size`. So the HTTP compatibility bug is fixed; the remaining content-id startup
blocker is live metadata-resolution coverage for public ids. For regression smoke against
the known Synthetic Live Channel target, `infohash=50e93529d3eb46a50506b14464185a15292d6e47` still
exercises the direct swarm playback path.

**Update (note 45, 2026-07-02): the content-id startup blocker is fixed with the official
signed catalog path.** Fresh official-engine tracing showed the engine resolves public
content ids through `GET /gettorrent` on the catalog service, not only through peer
`ut_metadata`. The request signature is now reproduced exactly (including the important
literal-backslash secret bytes), and `ace_swarm::resolve::resolve_via_catalog` fetches the
base64 transport, verifies the returned checksum against `SHA1(transport_bytes)`, and feeds
the existing transport decoder. `AceProvider::resolve_content_id` now tries this catalog
path first and keeps peer `ut_metadata` as fallback. Production `/ace/getstream?content_id=`
also resolves before responding, so the public JSON/URLs now match the official engine's
resolved-infohash shape (`50e93529d3eb46a50506b14464185a15292d6e47` for the Synthetic Live Channel
cid1 id); internally the route aliases that public infohash URL back to
`cid:<content_id>` so playback keeps the catalog-derived transport trackers/geometry.
Live smoke: `getstream?content_id=cid1 returned an `/ace/r/47eda3.../outpace`
URL, playback returned `HTTP 200`, first byte in `0.369436` s, and downloaded `8,308,472`
TS-aligned bytes before the bounded curl timeout; logs showed `resolved cid:... via catalog`
and `open cid:...`. `ffprobe` identified H.264 video plus AAC audio. The remaining Synthetic Live Channel
limitation is unchanged from notes 41-43: the reachable upstream still serves only the
initial window and then advertises a stale window behind the next needed piece.

**Update (note 46, 2026-07-02): a separate live pause/stall cause is fixed.** On the
healthier `cid2` target, outpace and the official
engine now agree on the resolved infohash (`c123456789abcdef0123456789abcdef01234567`).
The official engine returned first byte in `0.003322` s and `57,666,380` bytes over a 60 s
bounded curl. Before this fix, outpace started quickly (`0.363953` s) but stalled at
`23,795,724` HTTP bytes; the newly exposed `/ace/stat` `downloaded` counter plateaued at
`24,110,624` bytes from 25 s through 40 s while daemon logs never declared the pool stale.
Root cause: after playback started, `follow_peer_pool` still refreshed its stale deadline
on non-output activity (piece chunks that did not close the next contiguous gap, live-window
updates, unchokes), so a busy but visibly stalled upstream masked the pause. The stale
deadline now refreshes on `made_output || (emitted == 0 && made_activity)`: protocol
activity can keep startup alive before first output, but only successful contiguous
MPEG-TS output keeps playback alive after that. The same rule is applied to the legacy
`follow_one_peer` path, and `/ace/stat/...` now reports real emitted bytes via
`SourceStats.downloaded`. Live after the fix: outpace logged
`upstream pool stale — no live progress for 12s; reconnecting 3 peer(s)`, reconnected, and
resumed from the prior cursor; a 75 s bounded curl received `69,777,140` bytes
(`time_starttransfer=0.369128`), and a stat-correct 45 s run advanced `/ace/stat`
`downloaded` from 0 to `46,124,672` while the HTTP client received `45,282,432` bytes
(`time_starttransfer=0.083484`). `ffprobe` identified 1920x1080 H.264 plus AAC. Remaining
work: longer VLC/ffmpeg soak with PTS-gap reporting on this or another healthy target; the
Synthetic Live Channel cid1 id remains a poor continuity target because the reachable upstream is
stale for the official engine too.

**Update (note 47, 2026-07-02): the per-piece frame drops are fixed — the B0 signature was
being fed into the TS path.** Reported as "stuttery / drops frames" on the healthy target
`content_id=cid3` (catalog-resolves to infohash
`d123456789abcdef0123456789abcdef01234567`). Root cause found by dumping the raw pre-`TsResync`
piece stream: **every Acestream live piece is `[piece_length − sig_len bytes of continuous
MPEG-TS][sig_len-byte trailing RSA source signature]`** (B0/note 27; `sig_len=96` for the
standard 768-bit key). The download path was reassembling the *whole* piece, signature
included, and handing it to `TsResync`, which re-locked by discarding bytes at each 1 MiB
boundary — losing **one straddling video packet per piece** (measured: 42/44 CC discontinuities
on the video PID, one per piece). Proven fix: stripping the trailing `sig_len` bytes per piece
makes the pieces byte-chain into clean 188-aligned TS (0 CC discontinuities, verified). This is
the same "−4 B/piece drift / junk at piece boundary" `TsResync` was originally built to paper
over — but dropping the "junk" also cost a real packet. Implemented as
`PieceReassembler::with_piece_trailer(sig_len)` (strips on emit only — the seeding `PieceStore`
still keeps full signed pieces for relay/wire-compat), `sig_len` derived from the transport
`pubkey`'s RSA modulus (`ace_wire::live_auth::signature_len_from_pubkey_der`) and carried on
`StreamInfo.sig_len` (bare-infohash path defaults to the standard 96). **Live-verified:** a 60 s
capture went 41.5 MB → clean, `CC_disc 44→0`, `MB decode errors 18→1` (the lone remaining one is
the mid-stream join point), `PTS gaps>0.1s 1→0`, 1920×1080 H.264 + 48 kHz AAC. cid3
is now a good continuous-playback smoke target.

**Update (note 48, 2026-07-02): live-playback robustness — timestamped logs, stale-pool
recovery, and per-piece request retransmission.** After note 47 the stream plays clean but can
still freeze intermittently after the initial window: the connected peers keep the TCP session
alive (`id=11`/`id=12`/`id=34`) but stop delivering the *next contiguous* piece, the 12 s stale
guard tears the whole pool down, and re-acquisition sometimes hits a batch where the other known
peers are unreachable (`no usable upstreams: tcp=22`). Three changes:
- **Timestamped stderr logs.** All `[ace]`/`[dht]`/`[seed]`/`[listen]` lines now carry a
  `HH:MM:SS.mmm` UTC prefix (dependency-free `logts` module + `alog!`/`swarm_log!` macros
  wrapping `eprintln!`), so stall durations and the gap between "stale" and recovery are
  measurable.
- **Stale ≠ bad peer (`FollowEnd::PoolStale`).** A stalled pool's peers are reachable — a stall
  usually means we fell behind the live edge, which `Continuity::resume`/`skip_to` is designed to
  skip. They're now **retried, not excluded** (bounded by an emitted-bytes watermark so a
  genuinely frozen source still gives up: `retry_stalled_pool`). Previously they were lumped into
  the cumulative `excluded` set, so recovery skipped the only reachable peers and burned on dead
  ones.
- **Per-piece request timeout + mid-session skip.** The pool loop now sweeps every ≤1 s
  (`REQUEST_CHECK_INTERVAL`): a piece outstanding past `REQUEST_TIMEOUT` (4 s) is requeued and
  re-requested — routed to a faster peer because the timed-out piece is dropped only from the
  scheduler's global set while the slow peer keeps its in-flight slot, so `ActivePeers::assign`
  (prefers most-spare) steers the retry elsewhere; and if the cursor is stuck >4 s on a piece
  evicted from every upstream window, `Continuity::skip_evicted_gap` skips to the lowest covered
  piece without a full teardown (the mid-session analogue of the reconnect-gap skip). New pure
  primitives `ActivePeers::{complete_everywhere,prune_below,any_unchoked_covers,lowest_covered_piece}`
  and `Continuity::{timed_out_requests,skip_evicted_gap}`, all unit-tested. This shrinks a single
  stuck piece from a 12 s whole-pool teardown to a ~4–5 s in-place retransmit. Regression-checked
  live (90 s / 81 MB, 0 CC discontinuities); the retransmit/skip paths stayed dormant on a healthy
  stream (as designed) so they're unit-tested + reasoned, not yet observed firing live. Remaining:
  observe the retransmit/skip paths during an actual field stall via the new timestamped logs.

### B0 — CRACKED (note 27, 2026-07-01)
**The per-piece signing scheme is fully cracked and implemented**, not just researched.
Scheme: `SHA1(piece_bytes[0 .. piece_length - sig_len])`, signed with standard **RSASSA-
PKCS1-v1_5** (textbook RFC 8017 §8.2, no custom padding), with the signature embedded
**in-band as the piece's own trailing `sig_len` bytes** (`sig_len` = the RSA modulus's byte
length) — not a separate wire message. This means the pure P2P relay path (S1/S2) never
needed to change; only the *source* (B1 origination) needed real signing.

Found by hooking the official engine's own hash calls live while it signed real pieces
(`references/ace-network-docs/docs/broadcasting/`'s local `--stream-source-node` setup from
note 25) — two real obstacles hit and resolved: the engine uses **PyCryptodome, not
OpenSSL** for hashing (confirmed via `/proc/<pid>/maps`, hooking the wrong library the first
time round found nothing); and Frida's `setTimeout`-based polling proved unreliable for this
target (fixed by hooking `dlopen` and installing hooks synchronously in its callback, no
timers). Confirmed the crack three independent ways: (1) the captured hash's input length
(`piece_length - 96`) matched a 768-bit RSA signature exactly; (2) `SHA1` of an independently
downloaded real piece (via our own recon client) matched a captured digest byte-for-byte;
(3) `pow(signature, e, n)` on a real piece's trailing bytes recovered **exact** standard
PKCS#1 v1.5 padding around the matching SHA1 digest.

Implemented in `ace_wire::live_auth` (`sign`/`verify_piece`/`split_piece`) and
`ace_wire::signing_chunker::SigningChunker` (buffers payload, signs, appends signature,
feeds an ordinary `TsChunker`) — a real bug in the first `flush()` draft (forgot to flush
the *inner* chunker too, silently dropping a finite broadcast's last few bytes) was caught
by its own unit test before it shipped. Verified against real captured ground truth
(`tests/vectors/live-source-auth/piece-{0,1}.bin`): our verifier accepts the real engine's
actual signatures, and re-signing the same payload with the same key reproduces the
**exact** signature bytes the real engine produced (PKCS#1 v1.5's padding is deterministic,
so this is a legitimate byte-for-byte check). See note 27 for the full account, including
what's still open (a previously-attempted Ghidra static-analysis route hit an unrelated
environment limitation — Python scripting not configured — and was abandoned in favor of
the live capture, which succeeded).

## Run the daemon + verify VLC (live, operator-run)
The daemon binary is `outpace` (`cargo run -p ace-engine --bin outpace`). The clean API:
`GET /healthz`, `/networks`, `/streams`, `/streams/{network}/{id}.ts` (live MPEG-TS),
`/streams/{network}/{id}.m3u8` (+ `/seg/{n}.ts`), `/streams/{network}/{id}/status`,
`DELETE /streams/{network}/{id}` (force-stop a session). `{network}` is the provider key
(`ace` today). There is also an Acestream-compatible HTTP surface for existing clients:
`/ace/getstream`, `/ace/r`, `/ace/stat`, and `/ace/cmd`. (Session teardown — idle-reaper or
force-stop — now aborts the background pull task, so a stopped stream stops downloading.)

Default bind is `127.0.0.1:6878` (collides with a running official engine — override with
`OUTPACE_BIND`). **Peer discovery is autonomous via mainline DHT** (`ace_swarm::dht`) +
the UDP tracker — no peer list needed. **VERIFIED LIVE (2026-06-29):** from just an infohash
the daemon discovers ~44 peers, connects, and downloads 9.4 MB of live MPEG-TS (188-aligned,
H.264) with no hand-fed peers.

```sh
# Fully autonomous — just an infohash:
OUTPACE_BIND=127.0.0.1:6900 cargo run -p ace-engine --bin outpace
vlc http://127.0.0.1:6900/streams/ace/<40-hex-infohash>.ts

# Acestream-compatible content_id startup:
resp=$(curl -s "http://127.0.0.1:6900/ace/getstream?format=json&content_id=<40-hex-content-id>")
vlc "$(printf '%s' "$resp" | jq -r '.response.playback_url')"
```

The served stream is **clean, packet-aligned MPEG-TS** — the −4 B/piece drift (Task 19) is
fixed by `ace_media::mpegts::TsResync` (Acestream's 1 MiB live pieces are each internally
188-aligned but don't byte-chain; the resync filter drops the ~1 partial packet of junk per
piece boundary and re-locks). **VERIFIED:** `ffmpeg` decodes the daemon's output to
1280×720 50 fps H.264.

**Start-on-keyframe (done).** Each `.ts` client is fed through a per-client
`ace_media::mpegts::KeyframeGate`: a player joining mid-GOP is held until the first clean
keyframe — it parses PAT→PMT to find the video PID, detects the random-access point (the
adaptation-field RAI bit or an H.264 IDR/SPS NAL), then emits the cached PAT+PMT followed by
the keyframe and passes everything through after. A safety budget falls back to passthrough
if no keyframe appears. So players start on a decodable picture instead of garbage. Validated
against a committed real-encoder fixture (`tests/vectors/media/h264-keyframes.ts`): joining
mid-GOP, the gate locks exactly on the real keyframe and ffmpeg decodes the output I-frame
first. Applied per-client (the broadcast is unchanged), so every joiner benefits, not just the
first.

**Content-id playback has two paths.** For Acestream-compatible clients, use the engine-style
route above: `/ace/getstream?format=json&content_id=<40hex>` returns `/ace/.../cid:<id>/...`
URLs and runs the metadata-resolution path. The clean API can run the same path by passing
an `acestream://` content-id as `cid:<40hex>`:
```sh
vlc http://127.0.0.1:6900/streams/ace/cid:cid1.ts
```
The daemon announces the content-id to the tracker/DHT, connects to a metadata-swarm peer,
fetches the `AceStreamTransport` file over **BEP-9 ut_metadata** (`ace_wire::ut_metadata` +
`PeerSession::fetch_metadata`), and decodes it to the real infohash + geometry + trackers
(`ace_swarm::resolve::resolve_via_peer`, TTL-cached). A bare `<40hex>` is still treated as an
infohash directly (proven path). The full resolve flow is offline-proven by an in-memory
mock-peer integration test (`crates/ace-swarm/tests/resolve_metadata.rs`); the live discovery
half shares the same environment gate as the download path. Current live smoke on the known
Synthetic Live Channel public id reached 14 metadata peers but failed because the only responsive peer sent
no `metadata_size`; for direct swarm regression testing use the official-resolved infohash
`50e93529d3eb46a50506b14464185a15292d6e47`.

`OUTPACE_ACE_PEERS=ip:port,…` still exists as an optional manual-peer override.

Multi-client check: open the same URL from two players — `GET /streams` shows ONE session
with `clients: 2` (single shared swarm download).

## The protocol in one screen (all reverse-engineered + validated)
- Acestream is a **BitTornado (BitTorrent) fork**.
- **Discovery:** standard BitTorrent — UDP tracker (`t1.torrentstream.org:2710`) +
  Mainline DHT + LSD. (DHT not yet implemented; plan is to borrow a vetted DHT crate.)
- **Peer handshake (66 bytes, plaintext):** `0x11` + `"AceStreamProtocol"` + 8 zero
  reserved + infohash(20) + peer_id(20). peer_id = `R30------` + 11 random (ephemeral).
- **After handshake:** standard BT framing `<u32 len><u8 id><payload>` + a BEP-10
  extended handshake whose `mi` dict carries the **live piece window**
  (`min_piece`/`max_piece`/`position`/`live_window_size`).
- **Identifiers:** `infohash = SHA1` of the official ordered selected-field descriptor
  bencode (`name`, `authmethod`, `pubkey`, `piece_length`, `chunk_length`, `bitrate`).
  `SHA1(whole transport file)` is only a transport-file/cache hash. `content_id` is what
  `acestream://` links carry; the engine resolves content_id→infohash.
- **Transport file:** `"AceStreamTransport"` + `00 02` + **AES-128-CBC(fixed key/IV) →
  PKCS#7 → bencode** descriptor (name, piece_length, chunk_length, trackers, RSA
  pubkey; VOD adds a `pieces` SHA1 list). Key/IV are embedded constants in
  `ace-wire::transport`.
- **No account/identity needed** for the public swarm. Public content is
  `is_encrypted=0` (no DRM); the AES `Encrypter` is only for premium (out of scope).

## THE NEXT STEP (Phase 3.2 — piece download) — BLOCKER LOCATED
The blocker to "play in VLC" is now **precisely characterized** (see
`docs/protocol/notes/14-live-unchoke-recon.md`). `ace-peer` can now build + send its own
BEP-10 extended handshake (`ace_wire::extended::OutgoingExtendedHandshake`,
`PeerSession::send_extended_handshake`) and a recon harness (`live_recon_unchoke`,
`#[ignore]`d, with env knobs) drove the full exchange against live peers.

**Finding:** live peers accept our 66-byte handshake and send their extended handshake,
then **insta-close (~0.1 s) the moment we send ours** — regardless of `mi`/`interested`/
distance, and even with the full key set if `node_id`/`signature` are dummies. The same
peers serve the official engine. So the gate is a **valid node identity**: `node_id`
(32 B) + `signature` (64 B) — Ed25519-shaped. We must mint/sign a real one.

**Identity scheme CRACKED (notes 15–16):** `node_id` = an **Ed25519 public key** derived
from `/root/.ACEStream/device.key` (a 32-byte seed in hex) — **confirmed by exact pubkey
match**; `signature` = a 64-byte **Ed25519** signature. The identity is **self-generated**,
so we can mint our own keypair — no engine key needed. Signer = **`LiveSourceAuth.sign`**
(`core/src/live/LiveSourceAuth.pyx`), a Python orchestrator that delegates the actual sign.

1. **Preimage — SOLVED & verified (note 17).** The signature is computed **per-connection**:
   ```
   digest32  = SHA256( bencode(handshake_dict, signature := 64 × 0x00) )   # canonical sorted-key bencode
   signature = Ed25519_detached_sign(secret_key, digest32)
   ```
   `node_id` = the Ed25519 public key (self-generated; mint our own). Confirmed by
   `Ed25519_verify(node_id, signature, digest32)` passing **6/6** on live engine handshakes.
   (Note 16's "once at startup" was wrong — an attach-timing artifact; cracked by
   `docker restart` + **race-attach during boot** so `crypto_sign_detached` is hooked before
   the node signs.) Vectors: committed verify-only set in `tests/vectors/node-identity/`;
   full RE artifacts in `re/captures/node-identity/` (gitignored).
2. **Mint + sign:** Ed25519 identity in `ace_wire` + canonical bencode encoder; extend
   `OutgoingExtendedHandshake` to carry `node_id`/`signature`/`ts`/`v`/`pv`/`p`/`platform`/`nt`
   and sign (zero-sig → SHA256 → detached sign) before send; re-run `live_recon_unchoke`
   → expect acceptance + unchoke.
3. Then: live piece loop (request within `[min_piece, max_piece]`), `ace-swarm` → wire
   `ace-media` (MPEG-TS/HLS — built) + `ace-engine` (routes built) to it. Verify in VLC.

The engine listens for peers on **TCP 8621** (container IP `172.23.0.2`): connecting there
with our client + the current infohash makes it reply as a peer with its full signed
handshake — the ground-truth probe used to crack the above.

**Identity gate is now PASSED (notes 17–18).** Hooking the engine's
`crypto_sign_verify_detached` while our signed client connects shows `ret=0` with
`pk = our node_id` and `m = the digest the engine recomputed` — the official engine accepts
our minted identity. The engine-as-peer still closes us afterward, but that is **not**
identity rejection: in the current sandbox the channel is `status=prebuf, downloaded=0`
(the engine itself is pulling no data), so there is nothing to transact. **Validating live
unchoke + piece flow needs a live channel that is actually delivering data** (and likely
real swarm peers reachable from the host with WARP off) — an environmental gate, not a code
one. A VOD acestream id (static piece-hash list) would let the piece loop be validated
offline.

This step is RE-ish (a live experiment + binary RE), like the handshake/transport spikes —
not clean coding. A **VOD acestream id** would also help (static piece-hash list to
validate against; we've only had live fixtures).

## Environment & gotchas (HARD-WON — don't relearn these)
- **Cloudflare WARP breaks P2P** (drops inbound UDP, exits in-country). Keep it OFF for
  swarm testing. The Spain network itself does NOT block the swarm once WARP is off.
- **Engine sandbox:** the official closed engine runs in Docker for capture/RE:
  `docker compose -f re/sandbox/docker-compose.yml up -d acestream` (HTTP API on
  `127.0.0.1:6878`; `frida-tools` installed inside the container; SYS_PTRACE+seccomp
  unconfined so Frida attaches). `re/sandbox/` is git-ignored.
- **API quirks:** read `/ace/stat` fields under **`.response`** (not top-level — all
  top-level keys are null). Use **`content_id=`** for `acestream://` ids, not
  `infohash=`. A `status=prebuf, peers=0` forever = a **dead live channel** (the old
  docs' promo `685e…6067` is dead — don't use it).
- **Known-good LIVE test:** content_id `cid1`
  (resolve to the current infohash via `analyze_content` — it rotates).
- **`re/` is git-ignored and local-only** (NOT in version control): the extracted
  engine, Ghidra decompilation (`re/decompiled/ghidra/*.so.c`), Frida scripts +
  captures (`re/harness/`, `re/captures/`), the transport key file, the throwaway
  Python probes, and the interop spike. These persist on THIS machine but won't exist
  in a fresh clone elsewhere. The AES key/IV are already baked into `ace-wire` (committed),
  so the code doesn't depend on `re/`.
- **References:** original engine binaries + related repos are in `references/`
  (git-ignored). The Linux engine tarball + an Android APK + a Windows exe are there.

## Workflow notes
- This project followed the superpowers skills: brainstorm → writing-plans →
  subagent-driven-development. Plans are in `docs/superpowers/plans/`. The pattern that
  worked: do live recon/RE inline (controller), then hand clean, fully-specified TDD
  tasks to subagents; validate everything against captured ground-truth vectors in
  `tests/vectors/`.
- RE spikes (Frida/Ghidra) need the Docker engine running and a non-WARP network.
