# Seeding and Peers

AceStream is peer-to-peer: everyone watching a stream also helps distribute it. By default
outpace behaves like the original engine and is a full participant. It downloads from peers,
uploads to them, accepts inbound connections and announces itself.

## The three modes

| Mode | `OUTPACE_ENABLE_INBOUND` | `OUTPACE_ENABLE_SEEDING` | What happens |
| --- | --- | --- | --- |
| **Full participant** (default) | `1` | `1` | Listens on TCP 8621, accepts inbound peers, uploads to them and to the peers you download from, and announces to trackers and the DHT. |
| **Leecher with reciprocal upload** | `0` | `1` | No listener and no announcements. You still upload to peers you connected to. |
| **Pure leecher** | `0` | `0` | Download only. No listener, no announcements, no upload. |

To switch to pure leecher mode:

```bash
OUTPACE_ENABLE_INBOUND=0
OUTPACE_ENABLE_SEEDING=0
```

Why stay a full participant:

- streams you watch get healthier, which benefits everyone;
- swarms tend to treat peers that upload better;
- [broadcasts](Broadcasting) and [support sources](Support-Sources-and-Operations) need it.

Why you might not:

- upload bandwidth or data caps;
- policy reasons;
- a network that can't accept inbound connections anyway (see
  [NAT and Port Mapping](NAT-and-Port-Mapping)).

Playback works in every mode.

## The peer port

- `OUTPACE_PEER_LISTEN` (default `0.0.0.0:8621`) is a **TCP** listener on all interfaces.
- outpace announces this same port to trackers and the DHT, so whatever forwards traffic to it
  must use the same external port.
- There is **no UDP listener**. DHT and tracker traffic use outbound UDP from random ports, so
  allow outbound UDP and its replies, but don't forward UDP 8621.
- outpace doesn't implement the legacy port 62062 protocol.
- The listener only runs while inbound serving is on.

## What gets seeded

outpace keeps recently downloaded pieces of each stream so it can serve them to peers:

| Setting | Default | Effect |
| --- | --- | --- |
| `OUTPACE_CACHE_TYPE` | `memory` | Keep pieces in RAM, or on disk with `disk`. |
| `OUTPACE_SEED_RETENTION_SECS` | `45` | Live streams keep about this many seconds of pieces. Memory then follows the bitrate instead of filling the byte cap. `0` = byte cap only. |
| `OUTPACE_SEED_STORE_BYTES` | 128 MiB | Hard ceiling **per stream**. VOD always uses this cap. |
| `OUTPACE_SEED_TTL_SECS` | `300` | Safety net that frees a stream's store if nothing owns it any more. |

A stream's store is released when the stream is torn down, about 30 seconds after its last
viewer leaves.

### Memory or disk

- **Memory** is fast and simple. The per-stream cost is roughly bitrate × 45 seconds: around
  30 MB for a 5 Mbit/s stream.
- **Disk** spills pieces to `OUTPACE_CACHE_DIR`, which helps on small devices. The cache is
  temporary: it is wiped at startup, and each stream's directory is removed at teardown. See
  [Configuration](Configuration#how-the-disk-cache-wipe-works) for the safety rules.

### Process memory after playback

On Linux, outpace returns freed memory to the system in the background, so resident memory drops
back after a stream ends. On macOS memory is released more lazily. Steady growth while streams
are running is mostly the per-stream seeding store and buffers, which are bounded by the
settings above.

## Inbound limits

Each limit is checked when a peer connects:

| Setting | Default | Meaning |
| --- | --- | --- |
| `OUTPACE_MAX_INBOUND` | `64` | Total inbound connections, including ones still handshaking. Extra connections are closed at once. `0` acts as `1`. |
| `OUTPACE_MAX_INBOUND_PER_STREAM` | `16` | Connections admitted per stream. At the default, a stream's 17th connection is closed before the handshake. `0` removes the per-stream limit (the global limit still applies). |
| `OUTPACE_MAX_UNCHOKED` | `8` | How many peers per stream may download from you at once. |

### Unchoke slots

Among the peers that want data from a stream, outpace lets the first `OUTPACE_MAX_UNCHOKED`
download, plus one rotating "optimistic" slot that moves to another peer about every 10
seconds. That gives newcomers a chance. With `0`, only the optimistic slot is used. Metadata
requests (peers asking for the descriptor) don't need a slot.

### What the limits don't do

- There is no bandwidth limit or fair-share guarantee. Use your OS or router to shape upload
  traffic.
- Peers that only ask for metadata still count against the per-stream limit, and half-open
  handshakes count against the global limit.
- Upload to the peers **you** connected to (reciprocal seeding) isn't counted against these
  inbound limits.
- Stopping a stream doesn't cut peers that are already connected. They can keep reading the
  pieces they already had access to until they disconnect.

## Checking that peers can reach you

Every two minutes, outpace logs whether peers can reach it:

```text
[reachability] externally reachable: yes (2 inbound peer(s), ...)
[reachability] externally reachable: no (0 inbound peer(s) ...)
```

If you see "no" for more than 10 minutes, a hint follows that names the advertised port. See
[NAT and Port Mapping](NAT-and-Port-Mapping) and
[Troubleshooting](Troubleshooting#no-inbound-peers).

## Related pages

- [NAT and Port Mapping](NAT-and-Port-Mapping)
- [Private Peer Mode](Private-Peer-Mode): restrict peers to an allowlist.
- [Configuration](Configuration#seeding-and-inbound-peers)
