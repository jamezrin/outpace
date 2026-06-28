# 07 – node.so: peer + tracker logic

Ghidra 12.1.2 headless analysis completed successfully.
Binary: `re/engine/lib/acestreamengine/node.so` (1.0 MiB, x86-64 ELF shared object, stripped symbol table).
Decompiled output: `re/decompiled/ghidra/node.so.c` (3.3 MiB, ~104k lines), function index: `node.so.index.txt`.
analyzeHeadless path: `/opt/ghidra/support/analyzeHeadless`

## Source structure (recovered from embedded strings)

The binary was compiled from `core/node.pyx` with Cython 0.29.22. It also incorporates (as cimports) several sub-modules that are compiled into the same .so:

| Embedded source file | Purpose |
|---|---|
| `core/node.pyx` | Main module; exports `StreamSupportNode` |
| `core/src/live/TransportDescriptor.pyx` | Base live descriptor |
| `core/src/live/TransportDescriptorBT.pyx` | BitTorrent-style descriptor |
| `core/src/live/TransportDescriptorDASH.pyx` | DASH descriptor |
| `core/src/utils/TransportFileManager.pyx` | Transport-file management |
| `core/src/Statistics/TrafficStatistics.pyx` | Traffic stats |
| `core/src/Statistics/RemoteSettings.pyx` | Remote config |
| `core/src/Crypto.pyx` | Crypto helpers |
| `core/src/common.pyx`, `core/src/parse_addr.pyx`, `core/src/import_crypto.pyx`, `core/src/build_target.pyx`, `core/src/logging.pyx` | Utilities |

## Exported Python types

Strings in .rodata confirm these Python type objects are registered at module init via `PyModuleDef_Init(&DAT_001fb660)`:

- `node.StreamSupportNode` – the main P2P node class
- `node.TransportFileManager`
- `node.RemoteSettings`
- `node.TrafficStatistics`
- `node.TransportDescriptor`, `node.TransportDescriptorBase`, `node.MultiTransportDescriptor`
- `node.TransportDescriptorBT`, `node.TransportDescriptorDASH`

## StreamSupportNode method table

All method names recovered from .rodata (PyMethodDef strings, Ghidra VA `0x1d80b5`–`0x1d8197`):

```
on_error            shutdown            create_pid_file     delete_pid_file
state_callback      init                parse_args          update_prefs
set_default_http_handler
remote_access_get_node_info
remote_access_update_node
remote_access_update_single_param
update_prefs_from_remote_settings
get_allow_manifest_segments_from_p2p
get_stats           get_peers           sync_time
get_config          set_config
run                 send_request        send_event
save                save_download_pstate
upload_transport_file_task
upload_transport_file_callback
vod_callback        shutdown_callback   signal_handler
check_remote_settings
update_traffic_stats
__reduce_cython__   __setstate_cython__
```

## get_config (FUN_00133d50 @ 0x133d50)

The `get_config` method (`core/node.pyx` line 1177) creates a `_PyDict_NewPresized(9)` dictionary and fills it with 9 fields from `self`. Fields are read from offsets `param_1 + 0x30` through `param_1 + 0x78` and populated via `PyDict_SetItem`. The string keys are Python-interned objects (DAT references). The specific key names are not directly readable from the decompilation without resolving the interned string pointers in `.data`.

OPEN: Field names in the 9-entry config dict could not be resolved; resolving requires tracing `PyUnicode_InternFromString` calls during module init.

## Encryption subsystem (from .rodata strings)

Four low-level encryption functions are embedded:

| Function name | Ghidra VA | Notes |
|---|---|---|
| `block_encrypt` | string at 0x1d7e3d | AES or XOR block cipher |
| `block_decrypt` | string at 0x1d7e2f | inverse of block_encrypt |
| `m2_AES_encrypt` | string at 0x1d7e65 | AES-based (M2Crypto) |
| `m2_AES_decrypt` | string at 0x1d7e4b | AES-based (M2Crypto) |
| `xor_encrypt`   | string at 0x1d7e74 | XOR stream |

These appear in `core/src/Crypto.pyx` (string at 0x1d7d08). Block size and key derivation are not visible in the decompiled output.

## TransportDescriptor method table (in-node copy)

Because the descriptor types are cimported into `node.so`, their method tables also appear in the binary's .rodata section. Observed methods (Ghidra VA range `0x1d821f`–`0x1d8592`):

```
copy                set_infohash        get_streams
get_type            is_new_format       is_finalized
get_sharing         get_transport_version
get_infohash        get_hash            get_secondary_hashes
get_hash_hex        get_checksum        get_checksum_hex
get_manifest_url    has_manifest_url    get_runtime_manifest_url
get_provider        get_provider_name   get_hidden_manifest_url
get_manifest_type   get_manifest_url_hash   get_manifest_hostname_hash
get_trackers        get_meta_trackers   get_startup_nodes
get_chunk_length    get_permanent       get_date_start    get_date_end
get_name            get_categories      get_countries     get_languages
get_bitrate         allow_public_trackers
get_name_as_unicode
get_provider_key_hash  get_provider_sid_hash  get_retranslator_key_hash
get_signature       get_commerce        get_premium
get_tns_enabled     get_license         get_media_params
get_download_url    is_auto_generated   get_allow_manifest_from_p2p
add_trackers        get_timeshift_offset   get_piece_length
get_authmethod      get_pubkey          get_private_flag
get_tdef            get_qualities       get_extra_data    get_urllist
get_all_hashes      get_python_version  get_build_target
```

## RemoteSettings.parse_response (FUN_0013efb0 @ 0x13efb0)

This function (from `core/src/Statistics/RemoteSettings.pyx`, line ~0x1a6 = 422) is not the P2P wire-protocol message parser. It processes remote configuration responses from an HTTP/settings server. It takes 2 arguments: the response content and a type indicator. It accesses attributes via `PyObject_GetAttr` on a module-level settings object (`DAT_00202ab0`).

OPEN: The HTTP/settings protocol format for remote settings responses is not resolved.

## Handshake and peer message dispatch

The function symbols `parse_response`, `send_request`, and `send_event` each appear in **different** source files (RemoteSettings.pyx, TrafficStatistics.pyx) and do NOT correspond to the P2P peer wire-protocol handler. The actual P2P wire-protocol handshake and message dispatch is in `StreamSupportNode` but could not be isolated in the decompilation because:

1. All Cython functions carry the same boilerplate (ref-counting, error propagation, frame tracking).
2. The `run()` method (`core/node.pyx` line 0x4d2 = 1234, `FUN_0013b760`) calls `FUN_00131930(DAT_002029e0, puVar10)` and then accesses `.update` on the result; this suggests it drives an event loop but the socket I/O layer sits below the Cython level.
3. No switch statement on a message-type byte was found in the decompiled output; the dispatch is likely implemented in the Python layer (via dict/handler dispatch) rather than a compiled C switch.

OPEN: Peer handshake byte sequence and message-type dispatch table not identified. A human RE should search for `struct.pack` / `struct.unpack` call patterns (or equivalent `PyBytes_FromStringAndSize` with fixed lengths) to find the wire framing. No such pattern was visible at the Cython level; the framing likely lives in pure Python code outside these .so files.

OPEN: Tracker announce message format not identifiable from the decompilation. The tracker URLs are stored in the transport descriptor (accessible via `get_trackers()`/`get_meta_trackers()`), but the HTTP announce request format is not in node.so.

## piece_length / piece indexing

The field `piece_length` (and `chunk_length`) is stored in the TransportDescriptor object. The method `get_piece_length()` returns it directly. The infohash is computed in `TorrentDef.finalize()` in Transport.so (see 07-transport.md). The piece index scheme follows the standard SHA1-per-piece layout inherited from BitTorrent (confirmed by the presence of `set_add_sha1hash`, `get_add_sha1hash` methods).

OPEN: The exact integer encoding of piece_length (little-endian vs. big-endian) and the on-wire piece request message format are not confirmed from this decompilation.
