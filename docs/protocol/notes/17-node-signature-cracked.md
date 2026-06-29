# 17 — Node signature preimage CRACKED (Phase 3.2 unblocked)

**Status: SOLVED & cryptographically verified (6/6 live samples).** This removes the last
unknown blocking the live piece path.

## The scheme

The BEP-10 extended handshake carries a node identity: `node_id` (32 B) + `signature`
(64 B), both Ed25519. The signature is computed as:

```
digest32  = SHA256( bencode(handshake_dict  with  signature := 64 × 0x00) )
signature = Ed25519_detached_sign(secret_key, digest32)
```

- **`bencode(...)` is canonical** — dict keys sorted lexicographically by raw bytes (the
  standard bencode rule). The dict signed is the *entire* extended-handshake dict the node
  will send, with the `signature` value present but set to 64 zero bytes (NOT removed).
- **`node_id` = the Ed25519 public key.** Self-generated from a 32-byte seed
  (`/root/.ACEStream/device.key`, hex). We can mint our own keypair — no engine key needed.
- The signer is libsodium `crypto_sign_detached` (via PyNaCl); `LiveSourceAuth.sign`
  (note 16) is the Python orchestrator. The digest is SHA-256, **not** SHA-512.
- **It is per-connection, not once-at-startup.** Note 16's "once at startup" conclusion was
  WRONG — it was an artifact of attaching Frida *after* the engine had already signed. The
  fix that cracked it: `docker restart` + **race-attach during boot** with a lazy
  per-module hook, so `crypto_sign*` is hooked before the node signs. Each handshake has a
  fresh `ts`, so each `digest32` and `signature` differ.

## `ts`

`ts` increments by ~1 per second (observed 749872, 749873, … consecutive). It is a counter
/ uptime-like value, **not** a unix timestamp. For our own client any self-consistent `ts`
works — peers verify the signature against the handshake's own `ts`, so we just sign
whatever dict we send.

## How it was captured (reproducible)

1. `docker restart sandbox-acestream-1`.
2. Immediately race-attach Frida (`re/captures/node-identity/fa_det.py`) by process name in
   a tight retry loop, loading `hook_det.js` which lazily hooks `crypto_sign_detached`
   (+ `_ed25519_detached`, `crypto_sign`) on `_sodium.abi3.so`, recording `msg`→`sig`.
3. Start the stream (`/ace/getstream?content_id=…`) and connect to the engine's peer port
   (TCP 8621, container IP 172.23.0.2) several times with our client (`/tmp/ehprobe`), which
   prints the engine's outgoing extended-handshake dict (hex).
4. Match each handshake's `signature` field to a recorded `sig` → recover its `digest32`.
5. Brute the formula: `SHA256(bencode(dict, signature=zeros))` matched **6/6**; then
   `Ed25519_verify(node_id, signature, digest32)` passed **6/6**.

Artifacts (gitignored): `re/captures/node-identity/` (handshakes, sign records, hook + driver).
Committed verify-only vector (no secret): `tests/vectors/node-identity/`.

## Ruled-out earlier (don't repeat)

Brute force of raw field concatenations (node_id/infohash/ts in every encoding) — the
preimage is the *whole bencoded dict*, not a field tuple. `crypto_sign` (combined mode) is
NOT used for the handshake; it's `crypto_sign_detached`. Attaching after boot misses it.

## Next (now unblocked)

1. Rust: `ace_wire` Ed25519 identity (`from_seed`/`generate`, `node_id`) + canonical
   bencode encoder + `sign_handshake` (zero-sig → SHA256 → detached sign). Prove against the
   committed vector (verify) and the engine seed (byte-exact reproduce, local-only).
2. Extend `OutgoingExtendedHandshake` to carry `node_id`/`signature`/`ts`/`v`/`pv`/`p`/`nt`/
   `platform`; sign before send. Re-run `live_recon_unchoke` → expect acceptance + unchoke.
3. Live piece loop → `ace-swarm` → wire `ace-media`/`ace-engine` → VLC.
