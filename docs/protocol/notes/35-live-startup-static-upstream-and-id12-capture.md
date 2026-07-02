# 35 — Live startup improved; static-upstream blocker narrowed

Follow-up to note 34. The Acestream-compatible `/ace/getstream` surface works, but the
known Synthetic Live Channel public id is currently a poor continuous-playback proof target: outpace can
pull the initial MPEG-TS window, while the official engine itself stayed in prebuffer on the
same content during this session.

## Live runs

Target:

- content_id: `cid1`
- official-resolved infohash: `50e93529d3eb46a50506b14464185a15292d6e47`

Official engine comparison (`acestream` sandbox, engine `3.2.11`):

- `/ace/getstream?content_id=f8b0...` returned a playback URL under
  `/ace/r/50e93529d3eb46a50506b14464185a15292d6e47/<token>`.
- Following the official playback URL for 35 s returned 0 bytes.
- `/ace/stat/...` reported `status=prebuf`, `peers=1`, `downloaded=0`,
  `infohash=50e93529d3eb46a50506b14464185a15292d6e47`.

Outpace before the fixes in this note:

- `/ace/getstream?format=json&content_id=f8b0...` returned in about 1 ms.
- `/ace/r/f8b0.../outpace` produced `8,308,472` bytes in a 45 s curl run.
- First byte was `15.110996` s.
- Output was 188-byte MPEG-TS aligned; `ffprobe` saw H.264 1280x720 + AAC.
- The provider connected to `85.87.156.75:8621`, requested the initial window
  `14718261..=14718269`, served the 8.3 MB window, then never advanced.

After the DHT/discovery startup fixes:

- Public DHT lookup for this infohash improved from `elapsed_ms=21258` to `elapsed_ms=7784`
  in the ignored live DHT test.
- Playback first byte improved to `7.7` s in normal daemon runs, and to `4.616304` s in the
  traced run.
- Byte count still capped at `8,308,472` bytes for this target.

## Startup fixes

Two peer-discovery issues were fixed in `ace-swarm`:

- `dht_get_peers` now stops once it has a useful peer target instead of waiting for the full
  15 s budget whenever the public DHT returns fewer than 30 peers.
- Tracker and DHT discovery now run concurrently. The first source wins only if it has enough
  peers; if it returns too few (for example one bad tracker peer), outpace waits for and
  merges the other source.

Regression tests:

- `dht_lookup_stops_once_peer_target_is_met`
- `peer_discovery_returns_fast_nonempty_source_without_waiting_for_slow_source`
- `peer_discovery_waits_for_second_source_when_first_has_too_few_peers`

## Full `id=12` capture

Raw packet capture was blocked by missing `CAP_NET_RAW`, so the daemon was run under
`strace -f -yy -s 65535` and the incoming peer byte stream was reconstructed from
`recvfrom(13<TCP:[192.168.1.2:33660->85.87.156.75:8621]>, ...)`.

Decoded incoming peer stream:

- 66-byte `AceStreamProtocol` handshake.
- 610 length-prefixed peer messages.
- Message id counts: `id=7` x576, `id=11` x29, `id=34` x3, `id=1` x1, `id=12` x1.
- `id=7` count is exactly 9 pieces x 64 chunks: the initial window only.
- `id=12` frame: message length `3365`, payload length `3364`.
- First 64 payload bytes:
  `05001f000000110000006c1a80b6320000000031e13539000000004de7e08321ad0100052164ff0000000000dfff0d0000000000e021a7`
- Last 64 payload bytes:
  `2d2d2d2d7343363076357334594848ffffffffffffffffffffffff0000000000e0953d0000000000e095360000000000e0953d02002e160c00031b5045534553`

The `id=12` payload is not a simple `[first_piece][bit_count][bits]` equivalent. It contains
repeated peer ids (`R30------...`) plus per-peer piece-range-looking values. Piece-ish
big-endian u32s in the payload include the same static range visible elsewhere:
`14718220`, `14718260..14718270`, with no value beyond `14718270` in this capture.

`id=11` is also present and bencoded, for example:

```text
d1:ai1e1:bi0e1:ci2199e1:di2138041e1:ei5542682e1:fi0e1:gi14718270e1:hi100e1:ii14718220e1:ji14718269e1:ki1e1:li0e1:mi-1e1:ni0e1:oi0e1:pi0e1:qi1e1:ri14718269e1:si14718269e1:ti-1ee
```

Fields `i`, `j`, `r`, and `s` line up with the advertised window
`min=14718220`, `max=14718269`; field `g=14718270` is one piece ahead. Across the 30 s
trace, `g` stayed fixed at `14718270`. Parsing this as an advancement signal would at most
request one additional piece in this run; it is not yet evidence of a complete live-edge
mechanism.

## Static-upstream guard

The connected peer kept the TCP session alive and sent `id=11`/`id=34`, but produced no new
pieces and no recognized live-head update after the initial window. `follow_one_peer` now
treats that as a stale upstream after 12 s without forward progress and returns
`PeerLost`, letting the existing reconnect path try another discovered peer instead of
waiting forever.

Live result after this guard:

```text
[ace] 85.87.156.75:8621: stale upstream — no live progress for 12s; reconnecting
[ace] no reachable peer among 15 discovered
```

So this is a bounded-failure improvement, not a full continuity fix. In this particular
swarm snapshot, DHT rediscovery still returned the same 14-15 peers and only
`85.87.156.75:8621` completed the connection+handshake path.

## Content-id derivation check

The official mapping for the SYNTHETICCHANNEL transport remains:

- content_id `cid1`
- infohash `50e93529d3eb46a50506b14464185a15292d6e47`

Using the decoded transport pubkey from `tests/vectors/transport-01.bin`, simple pubkey
hash candidates do not match the content id:

- `SHA1(pubkey)` = `3fe25f036fa7d30550a2c0e566a6c1005ac86906`
- stripping common DER prefixes / modulus-like slices did not match either.

The exact native content_id derivation remains open.

## Current blocker

The blocker is no longer "which hash function is imported natively" or "can `/ace/getstream`
start playback". It is:

> For this public live swarm, outpace can connect to one peer and pull a valid 8.3 MB
> initial MPEG-TS window, but that peer is static/chatty and no alternative discovered peer
> is reachable. Outpace needs a better live-upstream strategy: refreshed peer discovery,
> multi-peer following, or exact handling of `id=11`/`id=12` if a future capture shows those
> values moving on an actually advancing source.

Next concrete leads:

1. DONE in note 36: do not end `follow_live` permanently when every initially discovered
   peer fails after a stale upstream. Refresh discovery and merge new peers before giving up.
2. Move from single-peer following to a small active peer set, using whichever peer advances
   the live head and has the needed chunks.
3. Re-capture `id=11`/`id=12` on a content id where the official engine reaches `status=dl`
   and bytes advance continuously; only then promote compact-field parsing into protocol
   logic.
