# 14 — Phase 3.2 live recon: the unchoke blocker is a node-identity gate

**Status: BLOCKER PRECISELY CHARACTERIZED (not yet cracked).** Our `ace-peer` client
can now send its own BEP-10 extended handshake (encoder added in `ace-wire`), and a
configurable recon harness (`live_recon_unchoke`, `#[ignore]`d) drove the full
post-handshake exchange against live distance-1 peers on the Synthetic Live Channel channel
(content_id cid1 → infohash `50e935…2d6e47`).

## What we did
Started the engine streaming the channel (so it joined the swarm), captured the hot
peer set from the engine's network namespace (`tcpdump` on TCP 8621–8623, per note 10),
and ran the recon harness against the busiest peers (e.g. `146.158.146.137:8621`, which
is simultaneously the engine's #1 piece source).

Each peer: **accepted our 66-byte handshake**, sent its full extended handshake
(`mi` window near the live head, e.g. min≈14707122 / max≈14707185 / position≈14707184,
`distance_from_source=1`), and then **closed the connection within ~0.05–0.5 s** the
moment we sent our own extended handshake.

## Experiments that isolate the trigger
Knobs on the harness (`ACE_NO_MI`, `ACE_DIST`, `ACE_NO_INTERESTED`) plus a throwaway
probe sending the full key set:

| Variation | Result |
|---|---|
| mirror peer's `mi` + `interested` | insta-close |
| **no `mi`, no `interested`** (bare handshake, just hold) | **insta-close** |
| `distance_from_source=8` + `interested` | insta-close |
| no `mi` + `interested` | insta-close |
| **full key set** (`node_id`=dummy 32B, `signature`=zero 64B, `v`,`tt`,`p`,`pv`,`platform`,`nt`,`mi`) + `interested` | **insta-close** |

**Conclusion:** the close is **not** triggered by `interested`, by `mi` content, or by
the advertised distance. The peer **rejects our extended handshake itself**. Since the
very same peer serves pieces to the official engine, the discriminator is the
**node identity** we cannot yet forge.

## The gate: `node_id` (32B) + `signature` (64B)
Full real extended-handshake key set observed from a live peer:
```
ace_metadata_version=1  geoip_country(2B)  lsp(int)  m={ut_metadata:2}
mi={17 live metrics}    node_id=bytes[32]  nt=1  p=8621  platform=3  pv=2
signature=bytes[64]     stream_statuses={"-1":…}  ts(int)  tt="bt"  v=3021900  yourip(4B)
```
`node_id` (32 bytes) + `signature` (64 bytes) is the shape of an **Ed25519** identity
(32-byte public key, 64-byte signature). A peer keeps connections only from nodes that
present a valid identity; a dummy/zero signature is rejected.

## Next RE leads (for whoever cracks this)
- The live handshake + signing lives in **`re/engine/lib/acestreamengine/live.so`**
  (contains `signature`, `min_piece`, `position`, `distance_from_source`). It was **not**
  in the prior Ghidra pass (only `node.so` / `Transport.so` were decompiled) — decompile
  `live.so` next.
- `live.so` links **RSA** primitives (`rsa_sign_data`, `rsa_verify_data_pubkeyobj`,
  `RSA_keypair_to_pub_key_in_der/pem`, `RSA_pub_key_from_der`). The transport descriptor
  already carries a 124-byte RSA `pubkey` (note 13) — that is the **broadcaster** key,
  likely for **live piece authenticity** (per-piece signatures), a *separate* concern
  from the 32/64-byte **peer node identity**. Confirm which primitive signs the handshake.
- Open question: what bytes does `signature` cover (node_id? a nonce? the infohash?),
  and is the identity self-generated per session (consistent with "no account needed")
  so we can mint our own and sign correctly.

## Tooling left in place
- `ace_wire::extended::OutgoingExtendedHandshake` (+ `LivePosition`) — builds our handshake.
- `ace_peer::session::PeerSession::send_extended_handshake`.
- `ace-peer` `live_recon_unchoke` `#[ignore]` test — the recon instrument, with env knobs.
- Reproduce: resolve infohash + capture peers (note 10 recipe), then
  `ACE_PEER=<ip:port> ACE_INFOHASH=<40hex> cargo test -p ace-peer live_recon -- --ignored --nocapture`.
