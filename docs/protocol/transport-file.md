# Transport File & Identifier Math (spec)

Status: infohash derivation **VALIDATED** against engine ground truth; inner body
layout and content_id derivation PARTIAL/OPEN.

## File container
An Acestream transport file (`*.torrent` in the engine's
`~/.ACEStream/collected_torrent_files/`) is **not** a raw bencoded BitTorrent
torrent. It is:

```
"AceStreamTransport"  (18-byte ASCII magic)
<2-byte version?>      e.g. 00 02
<binary body>          serialized TransportDescriptor (packed binary; not bencode,
                       not zlib — see OPEN below)
```

Example body start (vector `transport-01.bin`): `00 02 93 a4 be a2 72 88 07 26 …`.

## Infohash derivation — VALIDATED
```
infohash = SHA1( entire transport-file bytes, including the "AceStreamTransport" magic )
```
Confirmed byte-for-byte against engine ground truth (the engine names each cached
transport file by its infohash):
- `tests/vectors/transport-01.bin` → SHA1 = `34df422b80a4bd94ac1e51be9ede60364ec7a7dd` ✓
- `tests/vectors/transport-02.bin` → SHA1 = `ed2c05b3b022e9cc7b7c1ca46d20f10839dc4108` ✓

This matches the Ghidra finding that `TorrentDef.finalize()` computes a SHA1
(`hash_sha1`) used as the 20-byte infohash (plus secondary `hash_md5`/`hash_crc32`).
NOTE: this is SHA1 over the **whole wrapped file**, not over a BitTorrent info-dict.

## content_id — PARTIAL / OPEN
- The engine maps content_id ⇄ infohash (verified): content_id
  `cid1` → infohash
  `50e93529d3eb46a50506b14464185a15292d6e47`; infohash
  `685edf209ccfdf88977c0d317e1407baca486067` → content_id
  `cid4`.
- For live broadcasts the content_id is stable while the infohash can rotate, so
  content_id is almost certainly derived from a field **inside** the transport body
  (e.g. the broadcaster public key), not from the file hash.
- `OPEN:` exact content_id algorithm + the inner body field layout (the packed
  TransportDescriptor after the `00 02` version). Recovering it needs either
  decoding the binary body or hooking `TorrentDef`/`Transport` getters at runtime.
- `get_content_id` (HTTP API) resolves it today, so an MVP can rely on the engine-
  equivalent lookup; native derivation is a later refinement, not a blocker.

## Implementation note
`ace-wire` can compute/verify the infohash immediately (SHA1 of the transport
bytes). Parsing the inner descriptor (piece length, file list, trackers, live
params) requires the body layout — tracked as OPEN and best recovered alongside
Task 9/Phase-1 work.
