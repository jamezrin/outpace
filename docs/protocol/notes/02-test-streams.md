# Task 2 — Known-good test streams + environment findings

## Working live test stream (verified end-to-end)
- **content_id:** `cid1`  (this is what `acestream://` links carry)
- **infohash:** `50e93529d3eb46a50506b14464185a15292d6e47` (engine-resolved from the content_id)
- **name:** "Synthetic Live Channel 1080 …" — `type=live`, `transport_type=bt`, `is_encrypted=0`
- Verified playback via the **official engine**: `status=dl`, `peers` up to ~32,
  `speed_down` ~1.2–2.8 MB/s, `downloaded` climbing 11MB→33MB over ~30s.

### Critical methodology notes
- Use **`content_id=`** (not `infohash=`) for `acestream://` IDs. A 40-hex
  `acestream://` value is a **content_id**; the engine resolves it to the BT
  `infohash`. `analyze_content?query=<id>` classifies any identifier.
- `/ace/stat/...` fields are nested under **`.response`** (e.g. `.response.peers`),
  NOT top-level. Reading top-level yields all-`null` (a reading bug, not a dead session).
- A `status=prebuf` with `peers=0` forever = the **live channel has no active
  broadcaster** (dead channel), even when the swarm is reachable. The old docs'
  synthetic demo channel `685edf…6067` is dead — do not use it as a liveness test.

## Ground-truth vector for Task 8 (identifier math)
- `infohash 685edf209ccfdf88977c0d317e1407baca486067` → `content_id cid4`
  (from the working metadata API; matches the value in the official docs).
- `content_id cid1` → `infohash 50e93529d3eb46a50506b14464185a15292d6e47`.

## Environment / network findings (resolved the "blocked" investigation)
- **No country-level block of the swarm from Spain at the network layer.** With a
  direct connection (no WARP), the Acestream tracker `t1.torrentstream.org`
  (`5.252.161.218:2710`, UDP BitTorrent tracker) **replies**, and inbound peer UDP
  flows freely (≈156 packets in a short window; bidirectional).
- **Cloudflare WARP breaks this.** WARP exits in-country (`colo=MAD, loc=ES`) AND
  its NAT drops inbound P2P/UDP return traffic: under WARP the tracker never replied
  and inbound peer UDP was ~1 packet. **Do not use WARP for P2P testing.**
- The engine's UPnP port-forward fails inside Docker (expected); outbound-initiated
  peer connections still work, so streaming succeeds without an inbound port.
- `TrafficStatsSender` telemetry returns HTTP 451 — irrelevant to streaming.

## Trackers observed (from DNS + pcap)
- `t1.torrentstream.org` → `5.252.161.218:2710` — **Acestream's own UDP tracker** (live, replies).
- `tracker.coppersurfer.tk`, `tracker.leechers-paradise.org`, `9.rarbg.me` — generic
  public BT trackers, **long dead** (shut down years ago). Not Acestream-specific.
