# 52 — Seeder self-announce advertises the peer port, not the HTTP port

**Status: DONE.** Fixes issue #21 (part of the NAT-reachability work, #19). Shippable on
its own, ahead of the port-mapping client (#20).

## The bug

There were two self-announce paths and they advertised **different** ports:

- **Broadcast path** (`BroadcastState::inbound_peer_port`, wired in `runtime::build_runtime`)
  announced `config.peer_listen.port()` → **8621**. Correct.
- **Leech/seeder path** (`AceProvider`) announced `self.port`, which was constructed from
  `config.bind.port()` → **6878, the HTTP API port**.

So a leeching outpace told trackers **and** the DHT (`announce_infohash_periodically` →
`announce_seeder` + `dht_announce_peer`) to dial it on 6878, which serves HTTP, not the
AceStream peer protocol. A correctness bug independent of NAT: even a fully reachable peer
that connected there would speak the wrong protocol. And because the leech path self-announce
was gated on `enable_seeding` (default true) rather than on actually running a listener, a
node with inbound serving off still advertised a dial-able seeder endpoint that nothing was
serving.

## What changed

- **`AceProvider` now carries a `peer_port` (the local peer-listener port) and an
  `announce_peer_port: Option<u16>`.** `peer_port` is what every tracker/DHT **discovery**
  announce sends (discovery also registers us as a peer) — always the peer endpoint, never
  the HTTP port. `announce_peer_port` is the dial-able endpoint the periodic **seeder
  self-announce** advertises; `None` disables the self-announce entirely.
- **A single resolved endpoint is threaded into both paths.** `build_runtime` computes
  `inbound_peer_port = config.enable_inbound.then_some(config.peer_listen.port())` once and
  passes it to both `AceProvider::with_inbound_announce_port` and
  `BroadcastState::inbound_peer_port`, so tracker + DHT + PEX advertise one identical
  endpoint (the mapped external port once #20 lands).
- **The self-announce is now gated on having an inbound port, not on `enable_seeding`.**
  This mirrors the broadcast path (`BroadcastState::spawn_announce` no-ops when
  `inbound_peer_port` is `None`). `enable_seeding` still gates *outbound* reciprocal serving
  (`SeedConfig::enabled`); it no longer decides whether we invite peers to dial in. When
  `enable_inbound` is on, the inbound `PeerListener` serves regardless of `enable_seeding`,
  so self-announcing is honest. See note 24 for the original (now-superseded) gating.
- **`outpace play`** (one-shot CLI leech to stdout) runs no listener, so it discovers on the
  peer port and does not self-announce (`announce_peer_port` defaults to `None`).
- **`enable_inbound` now defaults to ON.** With the self-announce gate moved onto the inbound
  listener, the daemon out of the box now behaves like the original Acestream engine: a full
  P2P participant that binds its peer port (`0.0.0.0:8621`), accepts inbound peers, seeds, and
  self-announces the peer port to trackers + DHT. This was always the original design intent
  (`compliance-seeding-broadcasting-design.md` specced `enable_inbound` default true; the v2
  foundations plan only held it off until the piece-header acceptance gap closed — note 33,
  which explicitly deferred the flip as "a product/exposure decision, not a technical gap").
  Only the HTTP API `bind` stays on localhost by default; the exposed surface is the peer
  port, as with Acestream. `OUTPACE_ENABLE_INBOUND=0` restores a pure-leecher deployment.

## Tests

- `ace-engine`: `leech_self_announce_uses_the_peer_port_never_the_http_port` (regression:
  advertises 8621, never 6878; leech-only default advertises nothing) and
  `seeder_announce_never_fires_without_an_inbound_port` (replaces the old
  `..._when_seeding_disabled`).
- Full `ace-engine` lib suite green, clippy clean.

## Still open

- The external port only differs from `peer_listen.port()` once **#20** (UPnP-IGD /
  NAT-PMP/PCP port mapping) resolves a mapped port; the `Option<u16>` seam is already in
  place to carry it into both announce paths without further plumbing.
