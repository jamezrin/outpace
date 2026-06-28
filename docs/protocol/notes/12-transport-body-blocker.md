# Phase 3 gate — the transport body is encrypted/obfuscated

> **RESOLVED — see `13-transport-descriptor.md`.** The body is **AES-128-CBC**
> with a **fixed global key + fixed IV**, then PKCS#7 + **bencode**. Cracked via
> Frida (pycryptodome `AESNI_start_operation`/`CBC_start_operation`/`CBC_decrypt`
> hooks) and validated by decoding both vectors offline. The notes below are the
> historical pre-crack analysis.


## What we know
- A transport file = `"AceStreamTransport"` (18B) + version `00 02` + **body**.
- infohash = SHA1(whole file) — already validated. The body's *content* is NOT
  needed for the infohash, but IS needed to know piece length, the per-piece SHA1
  list, the file list, and live params (to verify/pick pieces).
- The body is **not** plaintext: high entropy, no bencode, not zlib/gzip, no readable
  strings. Two different transports (`transport-01.bin`, `transport-02.bin`) share an
  **identical body prefix** (`00 02 93 a4 be a2 72 88 07 26 …`) — a fixed-key
  transform signature (likely the engine's `xor_encrypt` / `m2_AES` / `block_encrypt`
  from `Crypto.pyx`, or a fixed-IV block cipher over a common serialization header).
- Ghidra (`Transport.so`) shows the body is produced by `create_transport_file` via a
  `_Pack` routine + Cython `pickle` of `TransportDescriptor`, and references `pieces`
  and `sha1`.

## Second blocker
- BEP-9 `ut_metadata` is advertised (`m={ut_metadata:2}`) but **peers do not serve it
  to a fresh, choked leecher** — the TCP connection closes right after the extended
  handshake. So we can't trivially pull the metadata from the swarm to study it.

## Implication
Phase 3 (piece download + `ace-swarm`) is **RE-gated** on decoding the transport
descriptor. This is different from Phases 1–2, which were clean implementation against
already-reversed specs. We need a focused reversing spike before writing parser code.

## Candidate approaches (in order of expected payoff)
1. **Frida-hook the engine's transport parser** (proven technique — see `05-crypto.md`).
   Hook the function in `Transport.so` that returns the *decoded* `TransportDescriptor`
   (after de-obfuscation), and the de-obfuscation routine itself, to dump: the body
   transform (key/algorithm), `piece_length`, the piece SHA1 list, file list, and live
   params. The engine is running and frida is available in the container.
2. **Deeper Ghidra on `create_transport_file` / `_Pack_001c1408`** in `Transport.so`
   to recover the serialization + the obfuscation key statically.
3. **Engine API leverage:** `get_media_files` already returns parsed `name`/`files`/
   `type`; check `mode=full` / `analyze_content` for any exposed `piece_length` to
   bound the search (gives values, not the wire format).

## Note on live vs VOD
Live streams may not carry a static per-piece SHA1 list at all — live piece integrity
likely comes from the **source signature** (the `signature`/`node_id` in the extended
handshake) rather than fixed hashes. VOD transports are the right target for a static
piece-hash list. We currently only have a verified-live infohash; a VOD infohash would
be a cleaner reversing fixture.

## Status
Phase 3 is paused at this gate pending a transport-body reversing spike (approach 1
recommended). Phases 0–2 remain complete and green on `main`.
