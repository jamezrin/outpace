# Transport File & Identifier Math (spec)

Status: infohash derivation **VALIDATED** against engine ground truth; inner body
layout and content_id derivation PARTIAL/OPEN.

## File container
An Acestream transport file (`*.torrent` in the engine's
`~/.ACEStream/collected_torrent_files/`) is **not** a raw bencoded BitTorrent
torrent. It is:

```
"AceStreamTransport"   (18-byte ASCII magic)
<2-byte version>        00 02
<body>                  AES-128-CBC(fixed key/IV) of PKCS#7( bencode(descriptor) )
```

### Body decoding — RESOLVED (validated, see `notes/13-transport-descriptor.md`)
```
plaintext = PKCS7_unpad( AES128_CBC_decrypt(body, KEY, IV) )
descriptor = bencode_parse(plaintext)          # a single dict
```
- **KEY/IV** are 16-byte **fixed global constants** embedded in the engine
  (`core/src/Crypto.pyx`: `m2_AES_decrypt`/`block_decrypt`). The same pair decrypts
  all transports → they are effectively protocol constants required for interop.
  Captured value lives git-ignored at `re/captures/transport_keys.txt` (not committed
  as raw bytes); it must be embedded in `ace-wire`'s transport decoder to interoperate.
- Validated: decoding both committed vectors yields well-formed bencode with valid
  PKCS#7 padding and sensible fields (below).

### Descriptor fields (bencode dict)
| key | meaning |
|---|---|
| `name` | channel/content name |
| `piece_length` | bytes per piece (e.g. 1048576 or 131072) |
| `chunk_length` | bytes per chunk (16384 = 16 KiB) |
| `bitrate`, `quality` | media hints |
| `authmethod` | `RSA` (live integrity via source signature) |
| `pubkey` | 124-byte RSA DER public key of the broadcaster |
| `trackers` | list of `udp://…/announce` tracker URLs |
| `categories`, `allow_public_trackers`, `permanent` | metadata/flags |
| `pieces` | **VOD only** — concatenated 20-byte SHA1 piece hashes (see OPEN) |

Validated decode:
- `transport-01.bin` → name "Synthetic Live Channel 1080 …", `piece_length=1048576`, RSA, 2 trackers, **live (no `pieces`)**.
- `transport-02.bin` → name "Synthetic Demo Channel", `piece_length=131072`, tracker `udp://t1.torrentstream.org:2710/announce`, **live (no `pieces`)**.

## Infohash derivation — VALIDATED, corrected 2026-07-01
```
infohash = SHA1(bencode([
  ["name",         descriptor["name"]],
  ["authmethod",   descriptor["authmethod"]],
  ["pubkey",       descriptor["pubkey"]],
  ["piece_length", descriptor["piece_length"]],
  ["chunk_length", descriptor["chunk_length"]],
  ["bitrate",      descriptor["bitrate"]],
]))
```
Confirmed by importing the official `Transport.so` in the sandbox and calling
`load_transport_file_from_string(...).get_infohash()` while wrapping `hashlib.sha1`.
Ground-truth vectors:
- `tests/vectors/transport-01.bin` → official infohash =
  `50e93529d3eb46a50506b14464185a15292d6e47`
- `tests/vectors/transport-02.bin` → official infohash =
  `685edf209ccfdf88977c0d317e1407baca486067`

The engine also computes `SHA1(entire wrapped transport-file bytes)`, but that is a
separate transport-file hash/cache identifier, not the swarm infohash used in peer
handshakes:
- `tests/vectors/transport-01.bin` → transport-file hash =
  `34df422b80a4bd94ac1e51be9ede60364ec7a7dd`
- `tests/vectors/transport-02.bin` → transport-file hash =
  `ed2c05b3b022e9cc7b7c1ca46d20f10839dc4108`

## content_id — PARTIAL
- The engine maps content_id ⇄ infohash (verified): content_id
  `cid1` → infohash
  `50e93529d3eb46a50506b14464185a15292d6e47`; infohash
  `685edf209ccfdf88977c0d317e1407baca486067` → content_id
  `cid4`.
- For live broadcasts the content_id is stable while the infohash rotates, so
  content_id is almost certainly derived from the broadcaster **`pubkey`** in the
  descriptor (now decodable — see above), but the exact preimage is not simple
  `SHA1(pubkey)`: for `transport-01.bin`, `SHA1(pubkey)` is
  `3fe25f036fa7d30550a2c0e566a6c1005ac86906`, not the official content_id
  `cid1`. Common DER-prefix / modulus-like slices
  checked in note 35 also did not match.
- `OPEN:` confirm the exact content_id algorithm from the decoded `pubkey`.
  `get_content_id` (HTTP API) resolves it today, so it is not an MVP blocker.

## VOD piece-hash list and file layout (implemented #47)
Both captured fixtures are **live** (no `pieces`). A **VOD** transport carries `pieces`
= a concatenated list of 20-byte SHA1 piece hashes (standard BitTorrent piece integrity).
The decoder surfaces `pieces` and `is_live`, and outpace now downloads, verifies, and serves
single-file VOD content over vanilla BitTorrent (`ace_swarm::vod::download_vod`,
`GET /vod/:network/:id`, `outpace play --vod`). Each assembled piece is SHA1-checked against
its `pieces` entry before any bytes are emitted.

**SYNTHESIZED SCHEMA (still OPEN — reconcile against a real VOD capture).** No public VOD
transport was available, so the file-layout keys are assumed from standard BitTorrent
conventions and encoded that way in the synthetic test fixtures:
- Single-file VOD: a `length` key = total content bytes (the final piece is truncated to it).
- Multi-file VOD: a `files` list. Multi-file is detected (`TransportDescriptor::is_multifile`)
  and **intentionally rejected** with a clear error; only single-file VOD is supported.

If a real capture shows a different encoding (e.g. length carried inside a `files`/`info`
sub-dict), only `vod_total_length()` / `is_multifile()` in `transport.rs` need to change; the
download/verify/serve path is standard BitTorrent and is unaffected.

## Implementation note
`ace-wire` can: (1) compute the official descriptor-derived swarm infohash and the
separate raw transport-file hash; (2) **decode the descriptor**
via AES-128-CBC(fixed key/IV) → PKCS#7 → bencode, exposing `piece_length`,
`chunk_length`, `trackers`, `pubkey`, and (VOD) `pieces`. The AES key/IV must be
embedded as protocol constants. Live integrity uses the RSA `pubkey` + per-piece
source signatures; live piece selection uses the peer `mi` window (see
`notes/11-live-extended-handshake.md`).
