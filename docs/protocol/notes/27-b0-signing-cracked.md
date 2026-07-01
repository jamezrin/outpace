# 27 — B0 CRACKED: live-source piece signing, fully implemented and verified

**Status: DONE.** The per-piece signing scheme (B0) is cracked, implemented in
`ace_wire::live_auth`/`ace_wire::signing_chunker`, and wired into B1's broadcast ingest so
outpace-originated broadcasts now carry **real, valid RSA signatures** — not a
placeholder. Verified against real captured ground truth at the unit level and through the
full production HTTP ingest → sign → store → reconstruct → verify path at the integration
level.

## The scheme

- **Preimage:** `SHA1(piece_bytes[0 .. piece_length - sig_len])` — the piece's own payload,
  excluding its trailing signature-sized region.
- **Signature:** standard **RSASSA-PKCS1-v1_5 with SHA1**. Textbook RFC 8017 §8.2, no custom
  padding, no surprises.
- **Placement:** the signature is embedded **in-band as the last `sig_len` bytes of the
  piece itself** (`sig_len` = the RSA modulus's byte length — 96 for a 768-bit key) — not a
  separate wire message. A "piece" a peer downloads *is* `payload || signature`, always
  exactly `piece_length` bytes.
- This means **the pure P2P relay path never needed to change**: `PieceStore`/
  `SeederSession`/`PeerListener` (S1/S2) already move piece bytes verbatim between peers,
  so the signature rides along for free. Only the **source** (origination — B1) needed this
  cracked to produce a real signature instead of a placeholder tail.

## How it was found

Static approaches first, but the actual crack came from a live capture:

1. **Ghidra decompilation of `live.so`** (the module containing `LiveSourceAuth`, per its
   retained Cython symbol table — see note 25) was attempted for static analysis, mirroring
   how notes 15–17 originally characterized the node-identity scheme. The import/analysis
   succeeded, but the export postScript failed in this environment
   (`Ghidra was not started with PyGhidra. Python is not available`) — an unrelated Ghidra
   scripting-environment limitation, not a sign this path is unworkable; just not available
   this session. `live.so` remains imported in `re/ghidra/proj/` for a future attempt.
2. **Frida, live, on the official engine's own `--stream-source-node` process** (note 25's
   local-sandbox setup) — this is what actually cracked it. Two real obstacles hit and
   resolved along the way:
   - The engine uses **PyCryptodome** (`Crypto.Hash._SHA1.abi3.so` etc.), **not** Python's
     OpenSSL-backed `hashlib`/`libcrypto.so.3` — confirmed by reading the running process's
     `/proc/<pid>/maps`. An earlier attempt hooking `libcrypto`'s `EVP_Digest*` functions
     found nothing because it was watching the wrong library entirely.
   - `setTimeout`-based polling for "has the target library loaded yet" proved unreliable in
     this Frida/target combination (confirmed with a minimal repro: a scheduled timeout
     never fired even against a process alive well past the delay). Fixed by hooking
     `dlopen` itself and installing hash hooks **synchronously** in its `onLeave`, plus a
     synchronous "is it already resident" check at script-load time — no timers at all.
3. Hooked `SHA1_update`/`SHA1_digest` (PyCryptodome's actual C symbol names) this way while
   the source node produced real pieces. Every piece triggered exactly one
   `SHA1_update(state, piece_bytes, 65440)` + `SHA1_digest` — for a `piece_length=65536`
   test broadcast, `65536 - 65440 = 96` immediately suggested "one RSA-768 signature's worth
   of trailing bytes excluded."
4. **Confirmed empirically, not just by suggestive arithmetic**: connected our own
   `live_recon_unchoke` harness to the *same* running broadcast, downloaded real pieces, and
   found `SHA1(downloaded_piece[0..65440])` **exactly matched** two of the captured digests
   (pieces 0 and 1 — matched by re-running against a fresh instance of the same looped test
   fixture, so piece content — and thus its hash — was reproducible across runs).
5. **Confirmed the actual signature, not just the hash stage**: took the real piece's
   trailing 96 bytes as `sig`, computed `pow(int(sig), e, n)` using the broadcast's own
   `.sauth` private key's public parameters (`e=3`), and the recovered bytes were **exactly**
   standard PKCS#1 v1.5 padding: `00 01 FF...FF 00 <SHA1 DigestInfo ASN.1 prefix> <digest>`
   — with `<digest>` matching the SHA1 computed in step 4. This is unambiguous: there is
   nothing custom here at all, it's RFC 8017 §8.2 to the letter.

## Implementation

- **`ace_wire::live_auth`** (extended from note 25's identity-only version):
  - `LiveSourceAuth::sign(payload) -> Vec<u8>` — SHA1 then RSASSA-PKCS1-v1_5.
  - `LiveSourceAuth::signature_len()` — the RSA modulus's byte length.
  - `verify_piece(pubkey_der, payload, signature) -> bool` — standalone verification given a
    transport's `pubkey` DER bytes (no private key needed).
  - `split_piece(piece, sig_len) -> Option<(payload, signature)>` — the inverse of "append
    signature to payload," for a consumer that wants to check a downloaded piece.
  - Needed `sha1 = { version = "0.10", features = ["oid"] }` in `ace-wire`'s `Cargo.toml` —
    the `rsa` crate's `Pkcs1v15Sign::new::<Sha1>()` requires `Sha1: AssociatedOid`, which
    `sha1` only implements behind its (non-default) `oid` feature.
- **`ace_wire::signing_chunker::SigningChunker`** — combines `TsChunker` with signing:
  buffers raw ingest bytes up to `piece_length - sig_len`, signs, appends the signature,
  hands the resulting full-`piece_length` block to an ordinary `TsChunker`. A genuine bug
  was caught by its own unit test during development: the first `flush()` implementation
  forgot to also flush the *inner* `TsChunker`, silently dropping the last few bytes of any
  finite/VOD-style broadcast (a true unbounded live stream never hits this path, so it would
  have been a silent, hard-to-notice data-loss bug specifically for finite content).
- **`ace_engine::broadcast::Broadcast`** now carries its `LiveSourceAuth` (previously
  generated only transiently to build the transport's `pubkey`, then discarded). The `PUT
  /broadcast/{name}` ingest handler (`http.rs`) uses `SigningChunker` instead of the bare
  `TsChunker` it had before.

## Verification

**Unit level** (`ace-wire`), against real captured ground truth
(`tests/vectors/live-source-auth/piece-{0,1}.bin` — real pieces from a real official-engine
source-node broadcast, note 25/this session):
- `verify_piece_passes_on_real_captured_pieces` — our verifier accepts the real engine's
  actual signatures.
- `resigning_the_real_payload_reproduces_the_exact_captured_signature` — re-signing the same
  payload with the same key (loaded from the real captured `.sauth`) reproduces the **exact
  same signature bytes** the real engine produced (PKCS#1 v1.5 has no randomized padding, so
  this is a legitimate byte-for-byte determinism check, not approximate).
- `verify_piece_rejects_a_tampered_payload` — a flipped payload bit fails verification.

**Integration level** (`ace-engine`), through the real production HTTP path:
- `ingested_piece_carries_a_real_verifiable_signature` — `PUT`s a full piece_length of
  TS-shaped content through the actual `broadcast_ingest` handler, waits for the piece to
  complete in the real `PieceStore`, reconstructs it from real stored chunks, and verifies
  the embedded signature against the broadcast's own `pubkey_der()` — the same code path a
  real downstream consumer (our own or, if B0's other open items land, the official engine)
  would exercise.

Full workspace `cargo test` green (20/20 binaries), clippy clean, both before and after.

## What's still open

- **Live interop with the official engine as a *consumer* of a outpace-originated,
  now-really-signed broadcast** — not attempted this session (would need the local
  `--stream-support-node`/client mode pointed at our infohash, per note 25's broadcasting
  docs). Everything needed for it now exists (real signatures, real transport, real serving
  path); this is a live verification step, not further RE.
- **Whether real-swarm 1 MiB live pieces use the *same* scheme** — this session's crack used
  a `--stream-source-node` MPEG-TS test broadcast with `piece_length=65536` (auto-selected
  for its low bitrate). The scheme (SHA1+PKCS1v1.5, signature-in-tail) is presumably
  identical for any piece_length/key size — nothing in the mechanism is bitrate- or
  geometry-specific — but this hasn't been independently reconfirmed against a real 1 MiB
  live-swarm piece with a different-sized (real) RSA key.
- **`header[4..8]`** (the 8-byte per-chunk wire header from notes 21/23, distinct from this
  signature) remains uninterpreted — confirmed unrelated to signing by this session's work
  (a 4-byte field can't hold a 96+-byte RSA signature). Still just "not needed" per note 21:
  no consumer we've built or tested checks it.
