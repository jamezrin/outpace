# 49 - Peer exchange and custom message classification

Date: 2026-07-02

## `id=12` is Acestream peer exchange

The frequently logged `unhandled msg id=12` is Acestream peer-exchange gossip (PEX). Connected
peers periodically broadcast a list of other peers they know.

Recovered layout from live vector `tests/vectors/peer-exchange/id12-556.bin`:

```text
header (16 bytes):
  [u8 stream][u16 count][u32 = 17][u32 record_size = 108][5 bytes]

then count fixed 108-byte records:
  IPv4 at record offset 11
  port at record offset 15
  R30-... peer id and live-window positions elsewhere in the record
```

`ace_wire::peer_exchange::parse_peer_exchange` extracts advertised IPv4 socket addresses and
returns an empty list for malformed messages. The real 556-byte / 5-peer capture parses to:

- `37.11.110.121:8621`
- `90.173.16.56:8621`
- `87.217.156.180:8621`
- `90.77.1.216:8621`
- `88.26.18.27:8621`

## Provider wiring

`follow_peer_pool` now handles `id=12`: while the pool is below `MAX_ACTIVE_UPSTREAMS`, newly
advertised peers are deduped against the per-session tried set, skipped if already active, and
connect-raced into the existing refill -> pool-add path through a live-held refill channel
clone. The send is `try_send`, so a full pool simply drops extra opportunities instead of
blocking the peer message loop.

Live proof: a real `id=12` from `5.231.25.139` logged
`peer-exchange from ...: 4 new peer(s) to try (of 5 advertised)`, and the active pool grew.

## Other custom message ids

The remaining custom ids are classified and de-noised:

- `id=36`: source-node descriptor, an 80-byte single-peer announce. The peer address is encoded
  at `IPv4@8:port@12`, with an `R30-...` id. The observed source was
  `5.231.25.139:10026`, a non-8621 port. It is harvested once through the same deduped path and
  skipped if already active.
- `id=11`: bencoded stats dict with single-letter counter keys. No action needed.
- `id=34`: 8-byte telemetry payload `[u32=1][u32 counter]`. No action needed.
- `id=13`: empty keepalive. No action needed.

There should be no more `unhandled msg` spam for these ids.

## Follow-up

PEX records also appear to carry live-window piece positions. We currently reconnect and read
the real window from the extended handshake. A useful next optimization is to parse those PEX
window fields and prefer advertised peers that already cover `Continuity::next_needed()`.
