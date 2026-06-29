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

Once the preimage is known: mint our own Ed25519 keypair, replicate the signature over the
same byte layout (with our own `node_id`/`ts`), put `node_id`/`signature`/`ts`/`v`/`pv`/`p`/
`platform`/`nt` into `OutgoingExtendedHandshake`, and re-run `live_recon_unchoke`.

## Ground-truth probe (still works)
Engine peer port = **TCP 8621** (container `172.23.0.2`). Connecting with our client + the
current infohash makes the engine reply as a peer with a freshly-signed handshake; the
signature changes with `ts` over ~minutes (lazily re-signed). `device.key` (the 32-byte
hex seed) → Ed25519 pubkey == the engine's `node_id` (verified).
