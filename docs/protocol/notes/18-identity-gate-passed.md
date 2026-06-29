# 18 — Node-identity gate PASSED live (signature accepted by the real engine)

**Milestone:** our minted Ed25519 identity + handshake signature is **accepted** by the
official engine's verifier. Phase 3.2's hard blocker is removed.

## Proof

With the running engine hooked on `crypto_sign_verify_detached`, we connected our Rust
client (`live_recon_unchoke`) sending a SIGNED extended handshake with a freshly-minted
identity. The engine called verify on our handshake:

```
VERIFY crypto_sign_verify_detached         ret=0  m=663e966d…7255  pk=c1af5944…d2bc
VERIFY crypto_sign_ed25519_verify_detached ret=0  m=663e966d…7255  pk=c1af5944…d2bc
```

- `ret=0` → **signature valid**.
- `pk` = exactly **our** `node_id` (the pubkey we minted that run).
- `m` = the 32-byte digest the engine **recomputed** from our handshake dict — confirming
  our `SHA256(bencode(dict, signature=zeros))` preimage matches the verifier's, and our
  Ed25519 signature checks out against our own pubkey.

This validates the whole implementation end-to-end against the real verifier:
`ace_wire::identity` + `OutgoingExtendedHandshake::sign_and_encode` +
`PeerSession::send_signed_extended_handshake`.

## Remaining: the post-handshake close is NOT identity rejection

After verifying us, the engine-as-peer still closes the connection. Since the signature is
accepted, this is downstream peer behavior, not the identity gate. Likely causes to
investigate next:
- the engine-as-peer has nothing to serve a host client (it's leeching the same live
  stream; no/low upload slots), vs. real swarm peers which are the actual piece sources;
- a missing post-handshake step we owe the peer (bitfield / have-none / request pacing);
- handshake fields a *connecting client* is expected to include that our minimal dict omits
  (the captured engine dict also had `asn`/`geoip_country`/`stream_statuses`/`yourip`/`tt`/
  `lsp`, several of which are server-populated).

## Next

Test against **real swarm peers** (not the engine-as-peer) for actual unchoke + piece flow,
then drive the live piece loop (`ace_wire::live::LivePicker` + `reassembly::PieceReassembler`)
→ `ace-swarm` → wire `ace-media`/`ace-engine`.
