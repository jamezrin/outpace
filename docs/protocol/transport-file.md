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
| `bitrate` | advertised media rate in bytes per second |
| `quality` | media quality hint |
| `authmethod` | `RSA` (live integrity via source signature) |
| `pubkey` | 124-byte RSA DER public key of the broadcaster |
| `trackers` | list of `udp://…/announce` tracker URLs |
| `categories`, `allow_public_trackers`, `permanent` | metadata/flags |
| `pieces` | **VOD only (legacy synthetic root schema)** — concatenated 20-byte SHA1 piece hashes |

The bitrate unit is bytes per second, matching the original engine 3.2.11. Its torrent
metadata duration fallback computes media byte length divided by seconds: a synthetic
10,000,000-byte file with a 10-second duration reports 1,000,000. The transport descriptor
getter returns the raw `bitrate` value. Outpace keeps that unit internally and converts it
to bits per second for public API rate fields.


Validated decode:
- `transport-01.bin` → name "Synthetic Live Channel 1080 …", `piece_length=1048576`, RSA, 2 trackers, **live (no `pieces`)**.
- `transport-02.bin` → name "Synthetic Demo Channel", `piece_length=131072`, tracker `udp://t1.torrentstream.org:2710/announce`, **live (no `pieces`)**.

## Infohash derivation — VALIDATED, corrected 2026-07-01

The complete live-BT selector, recovered in issue #253, is ordered as follows:

```text
keys = [name, authmethod, pubkey, piece_length, chunk_length, bitrate,
        premium, commerce, provider_key, provider_sid, access,
        sharing, private, tns, license]
pairs = [[key, descriptor[key]] for key in keys if key in descriptor]
infohash = SHA1(bencode(pairs))
```

Selection uses raw decoded Bencode values and exact key membership. Present zero
or empty values are included; absent suffix fields supply no defaults. The pair
list follows selector order, not dictionary order, and excludes unlisted metadata.
Outpace retains its existing error when any of the first six fields is missing.

This suffix extension applies only to live transports. Transport consumers use
the decoder's existing `is_live` classification; legacy wrapped VOD retains the
required six-field selector, even when selected suffix keys are present. Bare or
disk-enveloped torrent metainfo retains its standard encoded-`info` identity.
The raw `infohash_of_descriptor` helper remains a live-BT entrypoint.

Static recovery of the original BT selector and hash consumer establishes this
fifteen-key rule. An offline, wholly synthetic BT descriptor with invented
`provider_key` and `provider_sid` strings reproduced eight original pairs versus
the previous six-field implementation; all six shared values and encoded spans
agreed. This does not identify discarded production fields or establish historical
playback causality. Issue #253 remains open pending validation and review.

The earlier ground-truth vectors cover the six-field case, with no selected suffix
fields. It was confirmed by importing the official `Transport.so` in the sandbox and calling
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

## content_id — OPEN

The engine can resolve public content ids to descriptors and expose a content id for
some existing infohashes. Historical observations of stable content ids alongside rotating
live infohashes motivated a broadcaster-key derivation conjecture; neither that conjecture
nor a general derivation algorithm has been established. The candidate key hashes examined
in note 35 did not establish such a binding.

An offline synthetic probe of engine 3.2.11 found that `TransportFile.dump()` preserves the
raw wrapped bytes, while `get_hash()`, `get_infohash()` and `get_def().get_hash()` agree on
a value different from SHA-1 of those bytes. A live `TransportDescriptorBT` has no
`get_content_id` method; `TorrentDef.get_content_id()` initially returns `None`, and an
explicitly set synthetic id roundtrips. The engine's transport-creation wrapper has a
separate content-id acquisition path through `_update_torrent`; that path has not been
fully traced. These observations establish neither a deterministic public derivation nor
arbitrary server assignment.

Outpace-originated broadcasts deliberately use `SHA1(raw wrapped transport bytes)` as
their content id. This convention must not be imposed on public catalog responses.
Content-id resolution is catalog-only by default (plus the daemon's trusted local broadcast
registry); raw-hash BEP-9 fallback requires `OUTPACE_BEP9_CONTENT_ID_FALLBACK=1`.
See [note 63](notes/63-bep9-content-id-policy.md) for the evidence and trust boundaries.

## Single-file VOD metainfo (#80)

The original engine can **export** ordinary bencoded torrent metainfo. Outpace's VOD
resolver accepts that complete exported file, starting at its root dictionary, through
`outpace play <http(s)-metainfo-url> --vod` and the native `/vod/{network}/{id}` routes
with a transport-URL id. URL fetches keep the existing SSRF guard and size cap. Catalog
responses still pass authentication before decoding; a caller-supplied metainfo file
does not acquire catalog provenance or enter the live infohash index.

The supported `info` dictionary contains positive `length` and `piece length` integers
and a nonempty binary `pieces` string of concatenated 20-byte SHA-1 digests. `files` is
rejected regardless of its value: only single-file VOD is supported. Piece length must
be a multiple of the existing 16 KiB request block size and at most 8 MiB. Digest count
must equal `ceil(length / piece length)`; the final piece may be short. Length is bounded
by the signed 64-bit bencode integer range and, together with the digest list and piece
limit, by exact piece geometry. Every downloaded piece is SHA-1 verified before emission.

Swarm identity is **SHA1 of the original encoded `info` byte span**, including its original
key ordering and unknown fields. It never uses dictionary reserialization or the live
transport identity formula. The dedicated decoder accepts unique out-of-order dictionary
keys, rejects duplicate keys at every depth, malformed/noncanonical integers and byte-string
lengths, and trailing data. Input is limited to 1 MiB, nesting to 32 containers, and parsed
values (including dictionary keys) to 65,536.

Optional `announce` and `announce-list` tracker fields are decoded as UTF-8 byte strings
and lists of tiers respectively, with ordered deduplication. At most 128 entries are
accepted, each at most 2,048 bytes, with a nonempty authority, an `http`, `https`, or `udp`
scheme and no ASCII whitespace/control characters. Existing tracker policy still applies
after extraction; decoding does not contact trackers or authorize new network targets.

Encrypted `AceStreamTransport` decoding and live identity remain unchanged. The earlier
synthetic ASCII-wrapped VOD schema (`pieces`, `length`, and geometry at the descriptor
root) remains a historical compatibility path, not a verified original-engine schema.
Original-engine VOD uses the ordinary `info` dictionary described above, implemented
in `metainfo` and `disk_envelope`.
The VOD resolver also accepts the original-disk envelope with the exact eight-byte
marker `01 02 03 04 11 02 65 2e`. Its suffix is AES-128-CBC using a distinct fixed
disk key (raw MD5 of the compiled 59-byte loader argument in engine 3.2.11) and a
zero IV. Total input is capped at 1 MiB before decryption allocation; the suffix
must contain at least one complete 16-byte block. Every PKCS#7 padding byte is
validated, with padding lengths limited to 1 through 16. The unchanged unpadded
bytes pass directly to the strict metainfo decoder above, preserving its limits,
single-file policy, original-info identity, and standard BitTorrent handshakes.
Only one envelope layer is accepted. A recognized marker makes any framing,
padding, or metainfo error terminal; alternate markers, nesting, and prefix
scanning are unsupported. Live decoding does not accept this envelope. CBC is
unauthenticated: padding and metainfo validation do not detect every ciphertext
change or establish catalog authenticity. Catalog authentication, URL/tracker
policy, and network defaults are unchanged.

Tracked metainfo vectors are synthetic and carry no content ids or recorded infohashes.
Original-engine exported metainfo and production download/range validation artifacts
remain private; decoder tests alone do not establish original-engine peer interoperability.

### Empirical acceptance (2026-10-22)

The operator validation for #80 used legally unencumbered generated synthetic media.
Engine 3.2.11 created a 1,944-byte original disk descriptor and exported ordinary
metainfo. An independent cold official-engine consumer and the production cold Outpace
CLI each recovered all 2,767,449 source bytes, with all 85 piece hashes matching and
exact source byte equality. Both exported metainfo and the original disk envelope were
validated in production ([PR #260](https://github.com/jamezrin/outpace/pull/260),
[PR #261](https://github.com/jamezrin/outpace/pull/261)); the original-disk CLI exited 0
in about six seconds.

A fresh production daemon served a full 200 response, a cross-piece 206 range, a suffix
206 covering the final short piece, and an invalid-range 416, with exact expected
headers and bodies. The full request started cold; the suffix reused downloaded data.
Runs used normal mode, `max_cache_size=0`, internal Docker networking, an explicit
private peer, and process-scoped non-global transport-URL opt-in. Cleanup of run-owned
processes, listeners, containers, and networks was verified.

These results establish single-file schema, download, and byte-range interoperability
with the original engine. Public swarm discovery was not exercised; no public-catalog
or multi-file ground-truth claim is made. Multi-file VOD remains unsupported.

## Implementation note
`ace-wire` can: (1) compute the official descriptor-derived swarm infohash and the
separate raw transport-file hash; (2) **decode the descriptor**
via AES-128-CBC(fixed key/IV) → PKCS#7 → bencode, exposing `piece_length`,
`chunk_length`, `trackers`, `pubkey`, and (VOD) `pieces`. The AES key/IV must be
embedded as protocol constants. Live integrity uses the RSA `pubkey` + per-piece
source signatures; live piece selection uses the peer `mi` window (see
`notes/11-live-extended-handshake.md`).
