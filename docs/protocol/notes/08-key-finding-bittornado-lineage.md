# Key finding — Acestream P2P is a BitTornado fork

## Evidence
- The Linux engine ships **no pure `.pyc`** — the entire engine is Cython-compiled
  `.so` (only a 0-byte `lib/acestreamengine/__init__.py` exists). So "find the pure
  Python wire handler" (original Task 6 premise) does not apply to this build.
- The **peer wire protocol lives in `live.so`** (and partly `CoreApp.so`), not in
  `node.so`/`Transport.so`. Symbols recovered via `strings`:
  - `live.so`: `Connecter`, `ConnecterConnection`, `Encrypter`, `Downloader`,
    `DownloaderTransporter`, `DownloaderFeedback`, `Rerequester`, `rerequesters`,
    `handshake`, `handshaked_connection_made`, `peer_id`.
  - `CoreApp.so`: `Encrypter`, `handshake`.
- These class names are **verbatim BitTornado / BitTorrent-5.x** module names
  (`Connecter`, `Encrypter`, `Downloader`, `Uploader`, `Rerequester`,
  `ConnecterConnection`). Acestream/TorrentStream is a fork of that codebase.

## Why this matters (huge RE accelerator)
- The **base wire protocol is standard BitTorrent peer protocol** (BitTornado
  variant): handshake (`pstr`, reserved, infohash, peer_id), bitfield/have,
  request/piece/cancel, and the tracker `Rerequester`. **BitTornado source is
  public**, so the skeleton is already documented — we don't RE it from scratch.
- We only need to reverse-engineer the **Acestream-specific deltas**:
  1. **Encryption layer** (`Encrypter`): `m2_AES_encrypt/decrypt`, `xor_encrypt`,
     `block_encrypt/decrypt` from `core/src/Crypto.pyx`. Likely an MSE-like or
     custom scheme. Key size/derivation not visible statically (Task 5 / Frida).
  2. **Live-streaming extensions** in `live.so`: live piece picker
     (`PiecePickerSource`/`PiecePickerClient`), `livepos` semantics, buffer mgmt.
  3. **Transport-file / TorrentDef format** (`Transport.so`): `TorrentDef.finalize()`
     computes `hash_sha1` (20-byte infohash) + `hash_md5` + `hash_crc32`. Most likely
     SHA1 over a bencoded info-dict (standard BT), exact input bytes still OPEN.
  4. **content_id ↔ infohash** relationship (engine resolves content_id→infohash via
     metadata; see 02-test-streams.md vectors).

## Impact on the plan
- **Task 6 (legacy pyc):** demote — BitTornado upstream source is the reference
  instead of hunting an old pyc release. Keep only if we want exact constants.
- **Task 9 (wire protocol):** anchor the spec on the BitTorrent/BitTornado peer
  protocol; document only the Acestream deltas (handshake reserved bits, Encrypter).
- **`ace-wire` / `ace-peer` (Phase 1–2):** can be scaffolded from the documented BT
  peer protocol immediately; the encryption handshake (Task 5) is the gating unknown.
- **Reframes Unknown #1:** "signed identity?" → look at the `Encrypter` handshake in
  `live.so` + Frida-dumped keys to see whether keys are ephemeral (MSE-style) or tied
  to a server-issued identity.
