# 15 — Node identity: Ed25519 from `device.key` (the unchoke gate, largely cracked)

**Status: STRUCTURE CRACKED; exact signature preimage still open.** This resolves
*what* the `node_id` (32 B) + `signature` (64 B) gate from note 14 is, and how to mint
our own identity. Only the precise bytes covered by the signature remain to confirm.

## The identity is Ed25519, seeded by `device.key`
- The engine stores `/root/.ACEStream/device.key` = **64 ASCII hex chars = a 32-byte
  Ed25519 seed** (here `5e788f8995bee89acab02cf39d62ccd3ac40aa35673bcc4f1f7b334c7ffa7ea9`).
- `node_id` = the **Ed25519 public key** derived from that seed. **Confirmed by exact
  match**: deriving the Ed25519 pubkey from the seed yields
  `40efe2ebc9eb5d73a5426a4725051800f0f533764938c0c01ca8baa24197615a`, which is byte-for-byte
  the `node_id` the engine sends in its extended handshake (probed directly against the
  engine's own peer port — see below).
- `signature` = a 64-byte **Ed25519 signature**. The engine uses **PyNaCl**
  (`_sodium.abi3.so` is loaded in the process; `crypto_sign*` exports present).
- Implication: identity is **self-generated** (no central authority — consistent with
  "no account needed"). We can mint our own: random 32-byte seed → Ed25519 keypair →
  `node_id` = pubkey, and sign with the secret key. We do **not** need the engine's key.

## The signature is time-based and lazily cached
- Probing the engine repeatedly: within a few seconds the `signature` (and `ts`) are
  identical; over ~minutes `ts` increments and the `signature` changes with it. So `ts`
  (or a value tied to it) is part of the signed message, and the engine **re-signs lazily
  when it builds a handshake after the cached value goes stale** — not on a fixed timer.
- Datapoints (same `node_id`, `lsp`/`pos` frozen because the test channel stalled):
  - `ts=732265` → `sig=dbacaa99…fce00a`
  - `ts=732318` → `sig=afb31e76…1d2100`
  - `ts=732422` → `sig=90d2bb25…71e2b08`

## What the signature covers — STILL OPEN
Brute-forcing Ed25519 over the seed for many concatenations of `node_id` / infohash /
`ts` (LE/BE 2/4/8-byte, ASCII, hex-string forms, with/without separators, all orderings)
did **not** reproduce the observed signatures. So the preimage has more structure
(extra fields, a domain-separation prefix, or a canonical serialization). Two ways to
finish it:
1. **Dynamic (preferred):** hook `crypto_sign`/`crypto_sign_detached` in
   `_sodium.abi3.so` and read the `m`/`mlen` argument — this dumps the exact preimage.
   The hook is written (`scratchpad/hook_sign.js`) and attaches fine, but the call only
   fires on a **re-sign**, which needs `ts` to advance — i.e. an **actively progressing
   live stream** (the test channel had stalled, `ts` frozen, so only the cached sig was
   served). Re-run with a live channel whose position is advancing, connecting every
   ~10 s to force a stale-cache rebuild across a `ts` boundary.
2. **Static:** Ghidra-decompile `live.so` (the live-handshake module; not in the prior
   pass) and read the message assembly before the `crypto_sign` call.

## How to probe the engine's own handshake (ground-truth tool)
The engine listens for peers on **TCP 8621** (from `/proc/net/tcp` in its netns; the
container IP is `172.23.0.2`, reachable from the host). Connecting there with our client
+ the current infohash makes the engine respond **as a peer**, sending its full extended
handshake (incl. `node_id` + `signature`). The throwaway probe lives at
`/tmp/.../ehprobe` this session; fold a permanent version into `ace-peer` if useful.

## Next step
Once the preimage is known: implement Ed25519 identity minting + handshake signing in a
new `ace-identity` (or in `ace-wire::extended`), have `OutgoingExtendedHandshake` carry
`node_id`/`signature`/`ts`/`v`/`pv`/`p`/`platform`/`nt`, and re-run `live_recon_unchoke`
— the peer should then accept us and (hopefully) unchoke. Then the live piece loop.
