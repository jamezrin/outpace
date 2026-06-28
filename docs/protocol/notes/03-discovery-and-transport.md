# Task 3/4 — Peer discovery + transport observations (live stream capture)

Captured against the working live stream (content_id `f8b0…`, infohash `50e935…2d6e47`).

## Peer discovery = standard BitTorrent (three mechanisms, all documented)
1. **UDP tracker** — `t1.torrentstream.org` → `5.252.161.218:2710`. Classic
   BitTorrent UDP tracker protocol (connect/announce). Replies normally (no WARP).
2. **Mainline DHT** — bencode KRPC over UDP observed, e.g. payload
   `64 31 3a 61 64 32 3a 69 64 32 30 3a …` = `d1:ad2:id20:<node_id>…` to peer
   `136.243.44.126`. Standard BT DHT — decentralized peer discovery.
3. **Local Service Discovery (LSD)** — multicast `239.255.17.18`, payload begins
   `01 <infohash 50e935…2d6e47> 21ad52 33302d2d2d2d2d2d494b5554364a`
   where the trailing ASCII is `30------IKUT6J` (a peer_id). Local peer announce.

## Peer wire transport
- **TCP dominates** (≈1710 TCP vs ≈79 UDP in a 20s window) → piece transfer is TCP.
- **No cleartext BitTorrent handshake**: zero matches for the `BitTorrent protocol`
  pstr signature. Peer links use Acestream's **`Encrypter`** (custom pstr and/or
  MSE-like obfuscation/encryption). THIS is the gating RE delta for an independent
  client. Reference: BitTornado's `Encrypter`/MSE implementation to diff against.

## Content-level encryption
- This public channel reports `is_encrypted=0` — no DRM/content encryption. So once
  the peer-link `Encrypter` handshake is handled, received pieces are directly usable.

## Bootstrap summary (Task 4, answers Unknown #2)
- Hardcoded Acestream tracker: `t1.torrentstream.org` (UDP :2710).
- Plus Mainline DHT bootstrap (standard BT DHT bootstrap nodes) and LSD.
- Generic public trackers in the transport file (coppersurfer/leechers-paradise/
  rarbg) are long-dead and irrelevant.
- OPEN: whether the engine also queries an HTTP "meta-tracker"/supernode list
  (`get_meta_trackers` symbol exists) beyond the UDP tracker + DHT.
