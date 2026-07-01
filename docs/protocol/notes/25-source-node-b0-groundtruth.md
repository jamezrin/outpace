# 25 — Official source-node broadcasting: B0 ground truth + an infohash formula bug

**Status: SUBSTANTIAL GROUND TRUTH GATHERED, signing scheme NOT fully cracked.** Part of the
`/goal` push toward full leech+seed parity, specifically B0 (live-source-auth) and Task 7.

## The big unlock: `references/ace-network-docs/docs/broadcasting/`

This repo already had, uninvestigated, the **official Acestream broadcasting reference docs**
— `start-engine --stream-source-node` lets us run the **real, official engine** as a
broadcast origin locally, in the Docker sandbox, against our own tiny local MPEG-TS HTTP
source (looped via `ffmpeg -re -stream_loop -1 ... -listen 1 http://0.0.0.0:8090`). This is
categorically better ground truth than live-swarm captures: **deterministic, reproducible,
fully under our control**, no dependence on ephemeral real-world peers or timing.

```
docker run -d --network sandbox_default sandbox-acestream:latest /app/start-engine \
  --stream-source-node --source http://172.23.0.1:8090/ --name test --bitrate 8375 \
  --quality SD --category entertaining --metadata-dir /meta --publish-dir /pub \
  --cache-dir /cache --state-dir /state --log-stderr --log-stderr-level debug
```
Peer port defaults to **7764** for source nodes (not 8621 — that's the *support*-node
default). Produces, immediately and deterministically:
- `<publish-dir>/test.acelive` — a real transport file.
- `<metadata-dir>/test.sauth` — **the broadcaster's signing private key, in the clear.**
- `<metadata-dir>/test.restart` — plain ASCII decimal, the last piece number.
- `<cache-dir>/live.<infohash>.{0,1}` — cache files **named by the engine's own computed
  infohash** (the same validation trick `transport-file.md` used for transport-01/02).

## B0 findings (confirmed, no RE required — it's just... there)

- **`test.sauth` is a plain PEM `RSA PRIVATE KEY`** (PKCS#1, `-----BEGIN RSA PRIVATE KEY-----`).
  This session's key: **768-bit modulus, public exponent `e=3`** (not the usual 65537 — a
  deliberate, cheap-verification choice; `LiveSourceAuth`/our future `live_auth` module must
  read `e` from the key, not hardcode 65537).
- **The transport's `pubkey` field is the exact DER `SubjectPublicKeyInfo` (X.509) encoding**
  of that RSA public key — confirmed **byte-for-byte** via `openssl rsa -pubout -outform DER`
  against our own `decode_transport`'s output. This is a strong, independent validation of
  `ace_wire::transport::decode_transport` against a real engine-produced file (previously
  only round-trip-tested against our own encoder). 124 bytes, matching the doc's existing
  comment exactly.
- **`authmethod` field, previously undocumented in our decoder, = literal ASCII `"RSA"`.**
  `TransportDescriptor` doesn't currently surface it as a typed field (only via `raw`) — worth
  adding when B1 needs to read/write it.
- **Full descriptor key set observed** (a freshly-created live MPEG-TS source-node
  transport): `allow_public_trackers`, `authmethod`, `bitrate`, `categories`, `chunk_length`,
  `name`, `piece_length`, `pubkey`, `quality`, `trackers`. (No `pieces` — live, as expected.)
- **Real signed-piece capture, reproduced from note 21's live-swarm finding but now in a
  fully controlled setting**: connecting our existing `live_recon_unchoke` harness (unchanged
  — full signed Ed25519 handshake, same as talking to a real swarm peer) to the source
  node's peer port gets `UNCHOKE` and real `Piece` messages. The `[8B header][2B chunk][data]`
  structure holds: `header[0..4]` constant across pieces in one session (`41da9131` this run),
  `header[4..8]` varies per piece with irregular deltas (`e839d56a`, `eaad22f5`, `ed2d1e2f` for
  pieces 60/61/62) — **exact same pattern as note 21's live capture**, now reproducible on
  demand instead of depending on a live swarm peer staying connected.
  - Quick-checked (bounded effort): `header[4..8]` is **not** a CRC32, SHA1[:4], or MD5[:4] of
    the piece's first chunk alone. Doesn't rule out a hash of the *whole* piece, a signature
    fragment, or a timestamp — just rules out the cheapest hypotheses.

## B0 NOT cracked: the exact `header[4..8]` semantics and signing preimage

Attempted a live Frida hook (same technique as notes 15–18's node-identity crack) on the
running source-node process, hooking OpenSSL's `EVP_DigestUpdate`/`EVP_DigestFinal_ex` via
`frida`'s Python bindings to capture every hash computed at runtime. **Did not get a clean
capture this session** — pure Frida/tooling friction, not a sign the technique is unworkable:
- `frida.spawn()` needs the actual ELF (`/app/acestreamengine`), not the `/app/start-engine`
  shell wrapper (`ExecutableNotSupportedError` otherwise) — needs `LD_LIBRARY_PATH=/app/lib`
  passed via `spawn(..., env=...)`, since the wrapper script normally sets it before exec'ing
  the real binary.
- A JS top-level **blocking** wait (`Thread.sleep` in a loop) deadlocks `script.load()` itself
  (that Python call is a synchronous RPC with its own timeout) — don't block at top level.
- An **async** `setTimeout`-based poll-for-module-then-hook avoids that, but needs the target
  process to survive long enough for the timer to fire; short-lived targets (`--get-infohash`,
  `-v`) exit before a 100–500 ms timer ever gets scheduled — pointless to hook those, use the
  long-running `--stream-source-node` process instead (seconds of runway: bitrate detection,
  tracker startup, etc.).
- Even with a long-running target and async polling, this session's runs didn't yield a
  hooked digest — likely leftover container/port state (a previous `--name test2` instance's
  process kept squatting on port 7764 across supposedly-fresh `docker run`s of the same
  container) killing later spawns silently. A genuinely clean container (no prior entrypoint
  process) is required.
- **Next attempt should**: use a fresh container per attempt (or `frida -f` CLI instead of
  raw Python bindings — the CLI handles spawn/resume/stdio plumbing that tripped this
  session up), hook broadly (`EVP_DigestUpdate`/`Final` **and** a generic RSA sign hook, e.g.
  `RSA_private_encrypt`/`EVP_PKEY_sign`, since the per-piece signature is presumably produced
  by the same private key we already have in cleartext from `.sauth` — meaning once a
  candidate preimage is captured, it can be verified **offline** with that key, no live
  engine needed for confirmation, exactly like note 17's Ed25519 crack).

## Wire-compatibility win, independent of B0

Regardless of the signing mystery, this session **proved outpace interoperates with an
official-engine-*originated* broadcast**, not just real-swarm content: our existing signed
handshake (Ed25519 node identity, BEP-10 extended handshake, live `mi` window) was accepted,
we got `UNCHOKE`, and downloaded real `Piece` data — using the **same, unmodified** client
code path as talking to real swarm peers. This is a stronger, fully-reproducible version of
note 21's engine-interop proof (that one needed the engine's HTTP `getstream`/nudge API
against a real live channel; this one is self-contained, no real swarm dependency at all).

## Infohash formula bug found — IMPORTANT, needs fixing before B1

`docs/protocol/transport-file.md` states (as "VALIDATED"): `infohash = SHA1(entire
transport-file bytes, including the magic)`. **This is wrong for at least the freshly-minted
source-node case**: `SHA1(test.acelive)` = `8123456789abcdef0123456789abcdef01234567`, but the
engine's own `--get-infohash test.acelive` reports `f123456789abcdef0123456789abcdef01234567`
— **and only the latter is accepted at the wire level** (tried both, plus `SHA1(ciphertext
body)`, `SHA1(plaintext bencode)`, `SHA1(magic+ciphertext, no version)`, MD5 variants, and a
brute-force over all 2^10 bencode field subsets in sorted-key order — **none matched**).
`f123456789abcdef0123456789abcdef01234567` is confirmed correct by two independent signals:
the engine's own cache filename (`live.f4e1fdc7...`) and a real BT handshake accepted only
under that value.

**This does not contradict the *existing*, repeatedly live-verified path** (resolving a
content-id's transport via `ut_metadata`, then using `infohash_of_transport = SHA1(fetched
bytes)` to rejoin the *same* real swarm — this has worked in every live test this session and
in notes 19–21). The discrepancy is specific to **freshly `--stream-source-node`-minted**
transport files. Hypotheses not yet tested: the file may be re-serialized once between
creation and disk-write in a way that changes bytes without changing meaning (unlikely for
deterministic AES-CBC, but not disproven), or `--get-infohash` might not simply hash the file
handed to it. **Not resolved this session** — flagging clearly so B1 doesn't silently mint
transports with a self-consistent-but-wrong infohash that no real peer would compute the same
way. `docs/protocol/transport-file.md` should get a correction pass before B1 ships.

## Recommended next steps (priority order)
1. **B1 can still proceed** using the existing, proven `SHA1(bytes)` formula for anything we
   *originate ourselves* end-to-end (our own encoder + our own resolver, self-consistent by
   construction) — the bug only bites when a *different* implementation (the real engine, or
   a real client) needs to independently arrive at the same infohash for OUR minted file. Ship
   B1's loopback (outpace → outpace) proof first; gate real-engine interop on resolving
   this.
2. Retry the Frida capture with a clean container + `frida -f` CLI + hook both
   `EVP_Digest*` and RSA sign functions in one pass.
3. Once a candidate preimage is captured, verify offline against the `.sauth` private key
   (already have it) — no live engine dependency for confirmation.
