# 07 – Transport.so: transport-file format and hashing

Ghidra 12.1.2 headless analysis completed successfully.
Binary: `re/engine/lib/acestreamengine/Transport.so` (878 KiB, x86-64 ELF shared object, stripped symbol table).
Decompiled output: `re/decompiled/ghidra/Transport.so.c` (2.8 MiB, ~95k lines), function index: `Transport.so.index.txt`.
analyzeHeadless path: `/opt/ghidra/support/analyzeHeadless`

## Source structure (recovered from embedded strings)

Compiled from `core/Transport.pyx` with Cython 0.29.22. Sub-modules incorporated:

| Embedded source file | Purpose |
|---|---|
| `core/Transport.pyx` | Main module; module-level factory functions |
| `core/src/TorrentDef.pyx` | TorrentDef class; finalize / infohash |
| `core/src/live/TransportDescriptor.pyx` | Base live-stream descriptor (fields: infohash, trackers, chunk_length, piece_length, categories, countries, languages, etc.) |
| `core/src/live/TransportDescriptorBT.pyx` | BitTorrent-mode descriptor |
| `core/src/live/TransportDescriptorDASH.pyx` | DASH-mode descriptor |
| `core/src/Crypto.pyx` | RSA helpers (pub key serialization) |
| `core/src/import_crypto.pyx`, `core/src/common.pyx`, `core/src/logging.pyx`, `core/src/build_target.pyx` | Utilities |

## Exported Python types

String table (.rodata, VA `0xb46c9`–`0xb4b1c`) confirms these types:

- `Transport.TransportFile` – the on-disk / serialized transport file object
- `Transport.TransportDescriptor` – live-stream content descriptor (pickle checksum `0x815ee8a`)
- `Transport.TransportDescriptorBase` – abstract base (pickle checksum `0xd41d8cd`)
- `Transport.MultiTransportDescriptor`
- `Transport.TransportDescriptorBT` – BitTorrent-backed variant
- `Transport.TransportDescriptorDASH` – DASH-backed variant
- `Transport.TorrentDef` – extended torrent metainfo object

Module-level functions (strings in .rodata, VA `0xb4688`–`0xb4eb1`):
```
TorrentDef_load              TorrentDef_load_from_dict
load_transport_file          load_transport_file_from_url
load_transport_file_from_file
create_transport_file        create_hls_transport_file
create_multi_transport_file  test_split
RSA_keypair_to_pub_key_in_der   RSA_keypair_to_pub_key_in_pem
RSA_pub_key_from_der         unpad
get_python_version           get_build_target
load_download_pstate         get_ts_bitrate_from_metainfo
qualities_sort_func
```

## TransportDescriptor field layout

### What the decompilation reveals

The `__pyx_unpickle_TransportDescriptor` function (`FUN_001886a0` range, around VA `0x169722`) contains a checksum comparison at `0x815ee8a`. This checksum is a Cython-generated hash of the serialized field-name tuple. The function calls `FUN_0011def0(param_3, &PTR_DAT_001ca2e0, ...)` to parse keyword arguments from the state dict; the pointer `PTR_DAT_001ca2e0` points to a `PyArg_ParseTupleAndKeywords`-style keyword list, but the actual string pointers are in `.data` as interned objects initialized at module startup and could not be resolved without dynamic analysis.

The `__setstate__`-like helper `FUN_00131430` (VA `0x131430`) shows:

```c
// Field layout deduced from __setstate__ helper:
*(long **)(param_1 + 0x20) = state_tuple[0];          // field 0: Python object
*(int *)(param_1 + 0x18)   = int(state_tuple[1]);     // field 1: C int
// field 2: accessed via __setstate__ attribute call
```

This matches a 2–3 field minimal class (e.g., `TransportDescriptorBase` which has checksum `0xd41d8cd`).

`TransportDescriptor` itself has far more fields (evidenced by the large method table). The full field offsets are not recoverable without resolving interned strings.

### Field inventory from method names

The following fields are inferred from the getter/setter method names present in .rodata:

**Identity / addressing**
- `infohash` (20-byte SHA1; read via `get_infohash`, written via `set_infohash`)
- `content_id` (via `get_content_id`, `set_content_id`)
- `transport_version` (integer; `get_transport_version`)

**Hashing**
- `hash` – primary hash; `get_hash`, `get_hash_hex`
- `secondary_hashes` – list; `get_secondary_hashes`
- `checksum` / `checksum_hex` – `get_checksum`, `get_checksum_hex`
- `manifest_url_hash`, `manifest_hostname_hash`, `provider_key_hash`, `provider_sid_hash`, `retranslator_key_hash` – derived hashes
- `all_hashes` – aggregate; `get_all_hashes`

**Streaming / content**
- `trackers` – list of tracker URLs; `get_trackers`, `add_trackers`
- `meta_trackers` – list; `get_meta_trackers`
- `startup_nodes` – list; `get_startup_nodes`
- `chunk_length` (integer; `get_chunk_length`)
- `piece_length` (integer; `get_piece_length`, `set_piece_length`)
- `timeshift_offset` (integer; `get_timeshift_offset`)

**Manifest / CDN**
- `manifest_url`, `runtime_manifest_url`, `hidden_manifest_url`
- `manifest_type`, `provider`, `provider_name`
- `download_url`
- `allow_public_trackers` (boolean)
- `allow_manifest_from_p2p`, `allow_manifest_segments_from_p2p`

**Auth / crypto**
- `authmethod`, `pubkey`, `private_flag`, `signature`
- `sharing`

**Metadata**
- `categories`, `countries`, `languages`
- `name`, `name_as_unicode`
- `bitrate`, `date_start`, `date_end`, `permanent`
- `media_params`, `extra_data`, `urllist`
- `qualities` (for multi-quality streams)
- `streams`, `type`
- `commerce`, `premium`, `tns_enabled`, `license`
- `retranslator`

**Flags**
- `is_finalized`, `is_new_format`, `is_auto_generated`, `is_wrapper`
- `has_manifest_url`

### TransportDescriptorBT.__init__ (FUN_001400b0 @ 0x1400b0)

This `__init__` (from `core/src/live/TransportDescriptorBT.pyx` line 18) takes up to 4 positional arguments (plus kwargs). The argument parsing switch covers cases 0–4 and processes kwargs at keys `DAT_002001f8`, `DAT_00201db8`, `DAT_00200af8`, `DAT_00200ad8`. The first two positional args are converted to integers via `FUN_00124530` (a C-int conversion helper) and stored as `local_88` and `local_84` respectively.

OPEN: The two integer constructor args likely encode mode/version and piece_length; their exact semantic meaning is not confirmed.

## TorrentDef.finalize() and infohash computation (FUN_001886a0 @ 0x1886a0)

### What is confirmed

`FUN_001886a0` is a very large function (~5500 lines of decompiled C). It contains error-path strings `"local variable 'hash_sha1' referenced before assignment"`, `"local variable 'hash_md5' referenced before assignment"`, and `"local variable 'hash_crc32' referenced before assignment"` (lines 75386, 75106, 75262 in `Transport.so.c`). These strings appear in Cython's `UnboundLocalError` guards, confirming that:

1. **Three parallel hash computations run** before the infohash is finalized: `hash_sha1`, `hash_md5`, `hash_crc32`.
2. After the hashes are computed, the code calls `FUN_0011d710(local_e0, DAT_001d8668)` where `local_e0` holds the `hash_sha1` object and `DAT_001d8668` is an interned string (likely `"digest"` — this is a standard Python hashlib pattern for `hashlib.sha1().digest()`).
3. The result of `hash_sha1.digest()` (20 bytes) is then used (via `FUN_001311e0` and `FUN_00121540` — the Cython method-call helpers) as the infohash.
4. Accumulation of hash input uses `PyNumber_InPlaceAdd` on `local_158`, `local_100`, and `local_188` (which accumulate per-piece and total-size data), consistent with SHA1 computed over concatenated piece hashes.

### Infohash algorithm reconstruction (partial)

The SHA1 hash is computed over some serialized form of the content descriptor. Based on patterns in `FUN_001886a0`:

- The function begins by accessing `PyObject_GetItem(param_1, DAT_001d9588)` — likely fetching the `"files"` or `"pieces"` key from a metainfo dict.
- A loop accumulates data using `PyNumber_InPlaceAdd` over a sequence of items (likely pieces or file records).
- For each item, the function accesses `.digest()` on `hash_sha1` and appends the result.
- The line numbers trace to `core/src/TorrentDef.pyx` around lines 1802–1804 (`0x70a`–`0x70c`), in a section that matches the standard BitTorrent `finalize()` which SHA1-hashes the concatenated pieces metadata.

**Most likely algorithm** (OPEN: not fully confirmed):
```
infohash = SHA1( bencode( torrent_metainfo_dict ) )
```
This is standard BitTorrent infohash. The evidence (SHA1 over a dict structure consistent with bencode metainfo) is consistent with this. Alternatively, the SHA1 may be over a custom serialized payload.

OPEN: The exact byte sequence fed to SHA1 is not confirmed. It is unclear whether `infohash` = SHA1(bencoded metainfo info-dict) as in vanilla BitTorrent, or SHA1 over a custom Acestream-specific wire format. Resolving this requires dynamic instrumentation or finding the `bencode` call in the Python call chain.

OPEN: The role of `hash_md5` and `hash_crc32` is not determined. They may be stored as secondary hashes in the descriptor (`get_secondary_hashes`, `set_add_md5hash`, `set_add_sha1hash`, `set_add_crc32`) for data-integrity verification rather than for peer identification.

### content_id derivation

OPEN: The relationship between `infohash` (20 bytes) and `content_id` (returned by `get_content_id`) is not determined from the decompilation. They may be identical, or `content_id` may be a different encoding (hex string, base64, custom).

## create_transport_file (FUN_0019b910 @ 0x19b910)

This factory function (`core/Transport.pyx` line 0xb4 = 180, error message at `create_transport_file` with "exactly 2 positional arguments") takes 2 required arguments. The function:

1. Resolves the TransportDescriptor class via interned strings (VA `0x1d8ce0` in .data).
2. Calls `PyObject_GetAttr(class_obj, DAT_001d8c98)` followed by `PyObject_GetAttr(result, DAT_001d9538)` — these look like accessing `.infohash` then `.hex()` or similar.
3. Constructs and serializes a transport file dict.

OPEN: The on-disk/on-wire serialization format of the transport file (`.acelive` / `.acestream` extension) is not reconstructible from this decompilation. The structure is a Python dict that likely gets bencoded, but the key names are not resolvable without dynamic analysis.

## TorrentDef methods (hash-related)

| Method | Evidence | Notes |
|---|---|---|
| `set_add_md5hash(h)` | String in .rodata | Stores extra MD5 hash |
| `get_add_md5hash()` | String in .rodata | Returns stored MD5 |
| `set_add_sha1hash(h)` | String in .rodata | Stores extra SHA1 hash |
| `get_add_sha1hash()` | String in .rodata | Returns stored SHA1 |
| `set_add_crc32(h)` | String in .rodata | Stores CRC32 |
| `get_add_crc32()` | String in .rodata | Returns CRC32 |
| `set_piece_length(n)` | String in .rodata | Sets piece size |
| `finalize()` | FUN_001886a0 | Computes infohash via SHA1 |
| `get_metainfo()` | String in .rodata | Returns full metainfo dict |

The presence of `set_create_merkle_torrent` / `get_create_merkle_torrent` suggests Merkle tree hashing is also supported as an optional mode.

## Summary of confirmed vs. open findings

| Topic | Status | Evidence |
|---|---|---|
| SHA1 used for infohash | CONFIRMED | UnboundLocalError guard for `hash_sha1`; `.digest()` call pattern |
| MD5 and CRC32 secondary hashes | CONFIRMED (exist) | UnboundLocalError guards in same function |
| TransportDescriptor has trackers, meta_trackers, startup_nodes | CONFIRMED | Getter strings in .rodata |
| TransportDescriptor has piece_length, chunk_length | CONFIRMED | Getter strings in .rodata |
| Source file: SHA1 over bencoded metainfo | OPEN | Consistent but not confirmed |
| Exact infohash input bytes | OPEN | Requires dynamic analysis |
| content_id derivation | OPEN | Not found in decompilation |
| Transport file serialization format | OPEN | Dict structure visible but key names unresolved |
| TransportDescriptor full field layout at C level | OPEN | Interned strings not resolvable statically |
