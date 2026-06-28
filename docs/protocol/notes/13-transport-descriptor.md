# 13 – Transport descriptor: body cipher CRACKED + field layout

Status: **SOLVED** for the body transform; field layout recovered and **validated**
by decoding both known vectors offline. Live-only caveat noted (no static piece-hash
list observed — see §4).

This resolves the Phase-3 gate documented in `12-transport-body-blocker.md`.

## 1. Body encoding — AES-128-CBC, fixed key + fixed IV, then bencode

A transport file is:

```
"AceStreamTransport" (18 bytes) + version 00 02 (u16 BE) + BODY
```

The BODY (everything after byte 20) is:

```
plaintext = AES-128-CBC-decrypt(BODY, key=K, iv=IV)
descriptor = bencode_decode( pkcs7_unpad(plaintext) )
```

- **Cipher: AES-128-CBC.** Block size 16; both example bodies are block-aligned
  (512 and 384 bytes). Mode confirmed dynamically: only pycryptodome's `_raw_cbc`
  module fired (ECB/CTR modules were never invoked).
- **Key: 16 bytes (AES-128). FIXED global constant.**
- **IV: 16 bytes. FIXED global constant.**
- **Padding: PKCS#7** (both vectors padded with 5 × `0x05` to the next 16-byte
  boundary: 507→512, 379→384).

The key and IV are the **same for every transport** — proven because one captured
(key, IV) pair decrypts **two different** transport files (`transport-01`,
`transport-02`) into valid bencode. They are therefore global constants embedded in
the engine, not per-file material. A client hardcodes them.

> The raw key/IV bytes are intentionally **not** committed here. They are stored
> gitignored at `re/captures/transport_keys.txt` and consumed by the decoder
> `re/harness/decode_transport.py`. Sizes: key = 16 bytes, IV = 16 bytes.

### Key/IV origin
The bytes are **not stored raw** in `Transport.so` / `node.so` (a byte-search for
both 16-byte sequences misses). They are produced at runtime by the Cython crypto
layer `core/src/Crypto.pyx` — functions `m2_AES_decrypt` / `m2_AES_encrypt` /
`block_decrypt` (M2Crypto-style AES wrappers). `m2_AES_decrypt` takes up to **3
positional args** (data, key, iv) — i.e. the caller (the transport loader) passes
the key and IV in explicitly; they are likely derived from a constant
passphrase/KDF held in the loader. `OPEN:` the exact derivation (passphrase + KDF)
was not traced — but it is irrelevant for a client since the resulting key/IV are
fixed and were captured directly.

## 2. How it was cracked (evidence)

Dynamic, via Frida against the running engine (PID 9 in `sandbox-acestream-1`):

- Script: `re/harness/transport_crypto.js` — hooks pycryptodome exported C symbols
  `AES_start_operation` / `AESNI_start_operation` (key), `CBC_start_operation` (IV),
  and `CBC_decrypt` (in→out). Driver: `re/harness/run_frida.py`.
- The engine runs on an AES-NI CPU, so the key is set via **`AESNI_start_operation`**
  in `_raw_aesni.abi3.so` (not `_raw_aes`) — hooking only the portable module
  initially missed the key; adding the AES-NI hook captured it.
- Trigger: `GET /server/api?api_version=3&method=get_media_files&content_id=cid1`.
  This forces the descriptor decrypt. (`method=...&infohash=2183d180…` returned
  "cannot get transport file"; the **content_id** flow is the working trigger.)
- The `CBC_decrypt` whose INPUT equals `transport-01`'s body (`93a4bea27288…`)
  produced OUTPUT beginning `64 32 31 3a` = `d21:` → a bencoded dict. Captured
  in-container log: `/tmp/tc3.log` (key+IV lines), reproduced offline below.

## 3. Descriptor field layout (bencoded dict, evidence-backed)

The plaintext is a single bencoded dictionary. Fields observed across the two
vectors (a superset of the `TransportDescriptor` getters in `07-transport.md`):

| Key | Type | Example | Notes |
|---|---|---|---|
| `name` | bytes (utf-8) | `Synthetic Live Channel 1080 example.invalid …` | stream title (matches API `name`) |
| `quality` | bytes | `HD` | |
| `bitrate` | int | `902408` | bits/s |
| `piece_length` | int | `1048576` / `131072` | **BitTorrent piece size** |
| `chunk_length` | int | `16384` | sub-piece / live chunk size |
| `authmethod` | bytes | `RSA` | integrity = RSA signing |
| `pubkey` | bytes | 124-byte RSA DER | source public key (live integrity) |
| `trackers` | list[bytes] | `udp://…/announce …` | space-joined UDP tracker URLs |
| `categories` | list[bytes] | `amateur` / `entertaining` | |
| `allow_public_trackers` | int (bool) | `1` | |
| `permanent` | int (bool) | `1` | (transport-02 only) |

`OPEN:` fields not present in these two (live) samples but expected per the
getter inventory in `07-transport.md`: `pieces` (the concatenated 20-byte SHA1
piece-hash list — see §4), `signature`, `infohash`/`content_id`, `countries`,
`languages`, `startup_nodes`, `meta_trackers`, DASH/manifest fields. These appear
only when the corresponding mode/content type is used.

## 4. Live vs VOD — no static piece-hash list here

Both available vectors are **live** streams (`get_media_files` reports
`"type":"live","transport_type":"bt"`). Their descriptors contain **no `pieces`
key** — consistent with the hypothesis in `12-…`: live piece integrity comes from
the **source RSA key** (`authmethod=RSA`, 124-byte `pubkey`) and per-piece
signatures, not a fixed SHA1 list. They DO carry `piece_length` and `chunk_length`,
so the live piece grid is fully derivable.

`OPEN:` We have **no VOD transport** to confirm the on-disk shape of a static
per-piece SHA1 list. Expectation (from the `pieces`/`sha1` references in
`Transport.so`): a VOD descriptor would add a `pieces` key = concatenated 20-byte
SHA1 hashes (length = 20 × ceil(file_size / piece_length)), exactly as BitTorrent.
This is unverified — obtaining a VOD `.acestream`/`.torrent` transport is the next
fixture to confirm it.

## 5. Validated decode output

Decoder `re/harness/decode_transport.py` (AES-128-CBC + PKCS#7 + bencode). Real
output:

```
transport-01.bin  sha1=34df422b80a4bd94ac1e51be9ede60364ec7a7dd
  format version : 2
  body bytes     : 512  -> plaintext 507 (bencode)
  piece_length   : 1048576
  chunk_length   : 16384
  bitrate        : 902408
  authmethod     : RSA   pubkey: 124-byte RSA DER
  pieces         : NONE (live)
  name           : Synthetic Live Channel 1080
  trackers       : udp://tracker1.invalid:9006/announce udp://tracker.opentrackr.org:1337/announce

transport-02.bin  sha1=ed2c05b3b022e9cc7b7c1ca46d20f10839dc4108
  format version : 2
  body bytes     : 384  -> plaintext 379 (bencode)
  piece_length   : 131072
  chunk_length   : 16384
  bitrate        : 100000
  authmethod     : RSA   pubkey: 124-byte RSA DER
  pieces         : NONE (live)
  name           : Synthetic Demo Channel
  trackers       : udp://t1.torrentstream.org:2710/announce
```

Both decode to well-formed bencode with valid PKCS#7 padding — the transform is
confirmed correct.

## 6. Client implementation recipe

1. Read file; assert magic `AceStreamTransport`, version u16 BE.
2. `plaintext = AES128_CBC_decrypt(body, K, IV)` with the fixed 16-byte K, IV.
3. Strip PKCS#7 padding.
4. `bdecode` the result → descriptor dict.
5. Use `piece_length`, `chunk_length`, `trackers`, `pubkey` for the swarm/live
   logic. (For VOD, expect a `pieces` SHA1 list — `OPEN`, see §4.)

## 7. Evidence index
- Decoder: `re/harness/decode_transport.py` (validated, output in §5).
- Frida hook: `re/harness/transport_crypto.js`; driver `re/harness/run_frida.py`.
- Captured key/IV (gitignored): `re/captures/transport_keys.txt`.
- Vectors: `tests/vectors/transport-01.bin`, `tests/vectors/transport-02.bin`.
- Cython crypto wrappers: `core/src/Crypto.pyx` → `m2_AES_decrypt` /
  `block_decrypt` (Ghidra: `Transport.so.c` ~L52304; `node.so.c` ~L35017).
