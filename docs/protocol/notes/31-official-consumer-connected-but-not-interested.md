# 31 — Official consumer now connects to outpace, but never becomes interested

> Superseded by note 32: after matching the official source-node profile and removing the
> standard BT `Have` burst, the official consumer does become interested and requests
> pieces. The current blocker is piece acceptance after delivery.

Follow-up to note 30. The previous blocker was not precise enough: with a controlled local
UDP tracker in the transport descriptor, the official engine does discover and dial
outpace. The failure has moved one step deeper into peer-wire source-advertisement
semantics.

## Controlled setup

- Official engine: `sandbox-acestream-1`, HTTP API on `127.0.0.1:6878`.
- Outpace: HTTP `0.0.0.0:6900`, peer listener `0.0.0.0:8621`, inbound enabled,
  `OUTPACE_SEED_DEBUG=1`.
- Local tracker: UDP `0.0.0.0:7001`, returning compact peer `172.23.0.1:8621`.
- Transport rewrite: changed only `trackers` to `udp://172.23.0.1:7001/announce`.
  The selected-field swarm infohash stayed unchanged.

Proof infohashes from this pass:

- `cafe066789abcdef0123456789abcdef01234567` — port-match test (`8621` everywhere).
- `cafe076789abcdef0123456789abcdef01234567` — full id=11 diagnostics capture.
- `cafe086789abcdef0123456789abcdef01234567` — complete-piece `mi` window test.

In each case, official `getstream?url=...` returned the exact outpace infohash.

## What now works

The official engine now reaches outpace through a deterministic local tracker:

```text
connect ('172.23.0.2', 34579)
announce ('172.23.0.2', 34579) cafe086789abcdef0123456789abcdef01234567
[seed-listener] accepted connection from 172.23.0.2:58640
```

Outpace completes the inbound Acestream handshake, sends its signed extended handshake,
receives the official engine's extended handshake, and sends Acestream's live bitfield:

```text
[seed-session] peer=172.23.0.2 advertise window min=0 max=18 position=18 distance=0
[seed-session] <- ExtendedHandshake bytes ace_metadata=Some(1) ut_metadata=Some(2)
  ts=Some(114862) p=Some(8621) mi[min=Some(-1) max=Some(-1) pos=Some(-1) dist=Some(-1)]
[seed-session] -> live Bitfield for 19 complete piece(s)
```

The official stat endpoint confirms it counts outpace as a peer:

```json
{
  "status": "prebuf",
  "peers": 1,
  "downloaded": 0,
  "infohash": "cafe086789abcdef0123456789abcdef01234567",
  "is_live": 1
}
```

So discovery, TCP reachability, infohash agreement, and the initial peer handshake are no
longer the blocker.

## What still fails

The official engine never sends standard `Interested` or Acestream chunk requests (`id=6`).
After receiving the signed extended handshake and live bitfield, it only repeats:

- `id=11` bencoded stats/status dict.
- `id=34` 8-byte telemetry.
- occasional keep-alive.

The playback read still times out with zero bytes:

```text
curl --max-time 25 .../ace/r/cafe086789abcdef0123456789abcdef01234567/...
curl: (28) Operation timed out after 25002 milliseconds with 0 bytes received
http=000 bytes=0 time=25.002469
```

Decoded `id=11` from the official consumer before interest:

```text
{a:0, b:0, c:0, d:0, e:0, f:0, g:-1, h:0, i:-1, j:-1,
 k:-1, l:-1, m:-1, n:0, o:0, p:0, q:0/1, r:-1, s:-1, t:-1}
```

Existing good pcap sessions show these id=11 messages are normal stats traffic. In good
sessions the successful sequence is:

```text
source extended handshake -> source live bitfield id=5 -> leecher Interested ->
source Unchoke -> leecher id=6 chunk requests -> source id=7 pieces
```

Outpace reaches the first two source-side steps but the official consumer never performs
the third.

## Code changes from this pass

- Added `ace_wire::live_codec::live_have` and `live_bitfield`.
- `SeederSession::serve` now sends the live bitfield after the peer's extended handshake.
- Standard BT `Have` advertisements remain delayed until the peer sends `Interested`.
- Added gated `OUTPACE_SEED_DEBUG=1` peer-wire diagnostics.
- Fixed a real consistency bug: the source `mi` window now uses complete pieces only, so a
  partial head piece is not advertised in `max_piece`/`live_window_size` when the live
  bitfield cannot advertise it. Regression:
  `serve_advertises_complete_piece_window_in_extended_handshake`.

The consistency fix is necessary but not sufficient: the official consumer still stays
`prebuf`/`downloaded=0` after the corrected `min=0 max=18` window.

## Current precise blocker

The official consumer connects to outpace and receives a plausible source bootstrap, but
its live picker does not consider outpace an interesting source.

Most likely remaining mismatches:

1. Source extended-handshake `mi` semantics: real serving peers advertise nonzero rates,
   `download_window_end`, `lsp`, `is_accessible=1`, and a `position` behind `max_piece`; our
   source advertises mostly zeros/`-1`, `is_accessible=0`, `lsp=-1`, and `position=max`.
2. Initial source piece numbering / restart semantics. Outpace B1 starts each fresh ingest
   at piece 0; official source nodes persist `.restart`, and real swarms use mature live
   windows. Low indices are not disproven, but they remain suspect.
3. True source-node ground truth is still missing for the reverse direction. The existing pcap
   is a real serving peer, not necessarily an official origin.

## Next lead

Run the official engine as a local source node (`start-engine --stream-source-node`, note 25)
and capture the exact source-to-leecher bootstrap from an official consumer or from a minimal
outpace leecher:

- signed extended-handshake field values,
- id=5 bitfield timing and range,
- first id=11 dictionaries,
- first `Unchoke`/request timing,
- initial piece number and `.restart` behavior.

Do not spend more time on discovery or infohash unless the controlled local tracker proof
regresses; those layers are now proven past the previous blocker.
