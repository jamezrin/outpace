# 16 — Ghidra: the node-handshake signer is `LiveSourceAuth.sign` (static Ed25519)

Follow-up to note 15. Goal: recover the exact bytes the node-handshake `signature`
(64 B Ed25519) covers. This note records what the Ghidra dive into `live.so` proved and
the precise remaining task.

## Confirmed
- The signing logic lives in **`core/src/live/LiveSourceAuth.pyx`** (the source path is
  embedded as a traceback string referenced by all 33 functions of that module). The
  method is **`sign`** (Cython `FUN_00286730`; `get_signature` getters belong to the
  *transport descriptor* / broadcaster RSA key, a different concern).
- The node signing is **NOT PyNaCl/libsodium**. Hooking every `crypto_sign*` /
  `crypto_sign_*_seed_keypair` in `_sodium.abi3.so` across a **full engine restart** plus
  8 fresh handshake builds produced **zero** calls. `_sodium` is loaded but used elsewhere.
  → the Ed25519 sign is **statically linked inside `live.so`** (own ref10/donna impl).
- This means the dynamic shortcut (hook libsodium, read the `m`/`mlen` arg) does **not**
  work; and black-box brute force over node_id/infohash/ts (all encodings) does not
  reproduce the signature (note 15). The preimage has structure only visible in the code.

## Reproduce the decompilation
Ghidra 12 headless (Jython is gone — use **Java** post-scripts; PyGhidra otherwise):
```
/opt/ghidra/support/analyzeHeadless <proj> livep -import re/engine/lib/acestreamengine/live.so   # ~3.5 min analysis
/opt/ghidra/support/analyzeHeadless <proj> livep -process live.so -noanalysis \
   -scriptPath tools/ghidra -postScript find_funcs.java   # locates funcs via PyMethodDef name->ptr
/opt/ghidra/support/analyzeHeadless <proj> livep -process live.so -noanalysis \
   -scriptPath tools/ghidra -postScript find_auth.java     # dumps all LiveSourceAuth.pyx funcs
```
Scripts are committed in `tools/ghidra/`. A copy of the decompiled `sign` wrapper is in
`tools/ghidra/LiveSourceAuth.sign.decompiled.txt`. (`live.so` itself is under git-ignored
`re/`.)

## Ruled out (do not repeat)
- **Brute force** of Ed25519 over the seed for node_id/infohash/ts in every encoding
  (LE/BE 2/4/8, ASCII, hex) and orderings — no match (note 15).
- **bencode-dict hypotheses**: `sign(bencode(handshake_dict))` minus `signature`, minus
  `signature`+`yourip`, with `signature` set to `b""`/64 zeros — none match.
- **libsodium** (`_sodium.abi3.so`): hooked every `crypto_sign*`/`*_seed_keypair` across an
  engine restart + fresh handshake builds — **zero** calls.
- **OpenSSL** (`libcrypto.so.3`): hooked `EVP_DigestSign`/`EVP_DigestSignUpdate`/
  `EVP_DigestSignFinal`/`EVP_PKEY_sign` (and tried `ED25519_sign`, not exported) across a
  restart + 6 fresh handshakes — **zero** calls.
  → conclusion: the Ed25519 (and almost certainly its SHA-512) are **fully vendored inside
  `live.so`** (no external crypto calls during signing). `LiveSourceAuth.sign`
  (`FUN_00286730`) is just a Python-level orchestrator (attribute calls, returns a
  `PyList_New(2)` — likely `[signature, ...]`); the byte assembly + sign are in a callee.

## CORRECTION / key insight (most important for next session)
`live.so` contains **no** SHA-512/SHA-256/ed25519 constants (searched IV `6a09e667…`,
K `428a2f98…`, ed25519 `L`). So the Ed25519 is **not vendored in `live.so`** either.
Reconciling with "no `_sodium`/OpenSSL calls fired during handshake builds": the signature
is almost certainly computed **once at engine startup (or on a background timer), then
cached** — every peer handshake just emits the cached `signature`/`ts`. The earlier
observation that it "changes with `ts` over minutes" fits a periodic/lazy re-sign, not a
per-handshake one. **All my hooks attached *after* boot, so they missed the startup sign.**
→ This *reopens* the dynamic capture (likely PyNaCl `crypto_sign` or OpenSSL after all).
**Next step: frida SPAWN-GATE the engine** (`frida -f /app/acestreamengine <args>` inside the
container, or attach within the first ~100 ms of process start) with the `_sodium`
`crypto_sign*` + OpenSSL `EVP_DigestSign`/`ED25519_sign` hooks installed *before* it runs,
so the startup signing is captured with its `m`/`mlen`. Engine argv (PID 1 `start-engine`):
`/app/acestreamengine --client-console --bind-all --log-stderr --log-stderr-level debug
--log-modules root:D`. Confirm whether re-signs are periodic (hook + idle-wait several
minutes) as a simpler alternative to spawn-gating.

## The remaining task (two viable routes)
1. **Read the code (deterministic).** Trace `LiveSourceAuth`'s message assembly: the
   method that builds the bytes passed to the static Ed25519 sign. Obstacle: Cython
   attribute names are interned `PyObject*` globals (`__pyx_n_s_*`, null at rest), so the
   decompiled C shows opaque `DAT_*`. Resolve them via Cython's string-init table
   (`__Pyx_InitStrings`) — note: the naive `{p,s,n}` 40-byte layout scan found 0 entries
   on this build (CPython 3.10 / newer Cython uses a different layout); find the real
   layout (look at `__Pyx_InitString`/`__Pyx_CreateStringTabAndInitStrings` in the init
   function) or map globals by cross-referencing the `__Pyx_StringTabEntry` array.
2. **Dynamic, but hook the *static* signer (recommended).** In Ghidra, find the Ed25519
   sign function `LiveSourceAuth.sign` calls (the callee taking a `(msg, mlen, sk)`-shaped
   buffer; identify by the ed25519 group-order/`L` constant or by being called right after
   the message buffer is assembled). Take its file offset, then Frida-hook that **address
   in `live.so`** (`Process.getModuleByName('live.so').base.add(0x...)`) and dump the
   message argument — exactly the preimage. This sidesteps the Cython-string problem.
   *Concrete sub-step:* find the vendored **SHA-512** inside `live.so` (search for the
   SHA-512 round-constant table starting `428a2f98d728ae22…` / IV `6a09e667f3bcc908`), then
   Frida-hook the SHA-512 update at `live.so.base + offset`. Ed25519 sign hashes
   `prefix(32) || message` and then `R(32) || A(32) || message`, so the **message bytes
   appear in the SHA-512 input** — read them off directly. Likely the fastest finish.

Once the preimage is known: mint our own Ed25519 keypair, replicate the signature over the
same byte layout (with our own `node_id`/`ts`), put `node_id`/`signature`/`ts`/`v`/`pv`/`p`/
`platform`/`nt` into `OutgoingExtendedHandshake`, and re-run `live_recon_unchoke`.

## Ground-truth probe (still works)
Engine peer port = **TCP 8621** (container `172.23.0.2`). Connecting with our client + the
current infohash makes the engine reply as a peer with a freshly-signed handshake; the
signature changes with `ts` over ~minutes (lazily re-signed). `device.key` (the 32-byte
hex seed) → Ed25519 pubkey == the engine's `node_id` (verified).
