# Acestream Wire Protocol (consolidated spec — public/free swarm)

Scope: everything an independent client needs to join the public swarm and pull a
public (`is_encrypted=0`) live/VOD stream. Premium/encrypted content is out of scope.
This consolidates `notes/03-discovery-and-transport.md`, `notes/05-crypto.md`,
`notes/08-key-finding-bittornado-lineage.md`, and `transport-file.md`.

**One-line summary:** Acestream is a **BitTornado (BitTorrent) fork**. Discovery and
the peer wire are standard BitTorrent with three deltas: a custom handshake pstr
(`AceStreamProtocol`), a custom transport-file container, and BEP-10 extended
messages carrying live-streaming metadata.

## 1. Identifiers
- **infohash** (20 bytes) = `SHA1(entire transport file)` — see `transport-file.md` (validated).
- **content_id** (20 bytes, hex in `acestream://`) — stable per channel; resolves to
  an infohash via the engine. Native derivation OPEN; metadata API resolves today.
- **transport file** = `"AceStreamTransport"` magic + version + packed body (body
  layout OPEN).

## 2. Peer discovery (standard BitTorrent — use existing libraries)
1. **UDP tracker** — `t1.torrentstream.org:2710` (BEP-15 UDP tracker: connect → announce
   with the infohash → peer list). Generic public trackers in transports are dead; ignore.
2. **Mainline DHT** (BEP-5) — bencode KRPC over UDP; observed `get_peers`/`announce`.
3. **Local Service Discovery (LSD)** — multicast `239.255.17.18`, payload `01 <infohash>
   <peer_id>`.
Any subset yields peer `IP:port` endpoints. DHT + the UDP tracker are sufficient for
public content.

## 3. Peer wire protocol (TCP)
Standard BitTorrent peer protocol with a custom protocol string. **Plaintext** for
public content (no encryption layer).

### 3.1 Handshake (66 bytes, both directions)
| Off | Len | Field | Value |
|---|---|---|---|
| 0 | 1 | pstrlen | `0x11` (17) |
| 1 | 17 | pstr | `AceStreamProtocol` |
| 18 | 8 | reserved | `00*8` (extended messages used regardless) |
| 26 | 20 | infohash | target infohash |
| 46 | 20 | peer_id | `R30------` + 11 random chars (ephemeral) |

Vectors: `tests/vectors/messages/encrypter-handshake-*.bin`. A peer that shares the
infohash responds with the same-shape handshake; mismatched infohash → drop.

### 3.2 Messages (after handshake)
Standard length-prefixed framing: `<u32 length><u8 id><payload>`; length 0 = keep-alive.
IDs are the BitTorrent set: `0 choke, 1 unchoke, 2 interested, 3 not_interested,
4 have, 5 bitfield, 6 request, 7 piece, 8 cancel, 20 extended`.
- `request` payload = `index(u32) begin(u32) length(u32)`.
- `piece`   payload = `index(u32) begin(u32) block-bytes`.
- These are vanilla BitTorrent; piece integrity is the transport's piece hashes
  (inside the OPEN transport body).

### 3.3 Extended handshake (BEP-10, id 20, sub-id 0)
First post-handshake message is a bencoded extended handshake. Observed keys
(vector `encrypter-handshake-ext-msg-trunc.bin`):
```
ace_metadata_version=1, m={ut_metadata:2}, asn, asn_country, geoip_country, lsp,
mi={distance_from_source, down_rate, download_window…}   # live-streaming metrics
```
`ut_metadata` (BEP-9) fetches the transport metadata from peers. The `mi` sub-dict
and other Acestream keys drive live piece selection (live source distance, rates).
Exact live-extension message semantics: OPEN (Phase-1 `ace-peer` work).

## 4. Minimal client recipe (for the interop spike / `ace-peer`)
1. Resolve `content_id`→`infohash` (engine API for now).
2. Get peers (UDP tracker announce and/or DHT) for the infohash.
3. TCP-connect a peer; send the 66-byte `AceStreamProtocol` handshake; read theirs.
4. Send/receive the BEP-10 extended handshake; (optionally) `ut_metadata` to fetch
   the transport; exchange `bitfield`/`have`.
5. `interested` → on `unchoke`, `request` blocks → receive `piece`.
6. (Live) follow the live piece picker semantics from the `mi`/live extension.

## 5. Known-good test fixtures
- Live: content_id `cid1` → infohash
  `50e93529d3eb46a50506b14464185a15292d6e47` (verified streaming).
- Tracker: `t1.torrentstream.org` → `5.252.161.218:2710`.

## 6. OPEN items (not blockers for public playback)
- Transport-file inner body layout (piece length, piece hashes, file list, live params).
- content_id native derivation.
- Acestream live-extension message details beyond the handshake.
- Premium `is_encrypted=1` Encrypter (AES) — intentionally out of scope.
