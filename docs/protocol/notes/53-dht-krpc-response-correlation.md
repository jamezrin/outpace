# 53 — DHT lookups correlate KRPC responses by source and transaction id

**Status: DONE.** Fixes issue #40 (part of the DHT discovery/safety workstream, #46).
Follow-up from #9's `mainline` evaluation: while the custom `ace_swarm::dht` lookup stays the
default path, it has to defend against untrusted UDP the way a DHT library would.

## The problem

The iterative `get_peers` walk (`dht_walk_frontier`) trusted any KRPC-shaped packet that
landed on its socket while a round's collection window was open:

- Every outbound `get_peers` query went out with the **same** hardcoded transaction id
  (`b"sc"`).
- `parse_response` pulled `r.values` / `r.nodes` / `r.token` **without** checking the
  response's transaction id or that the sender was a node we had actually queried.

The socket is an unconnected `0.0.0.0:0` UDP socket, so any host on the internet can send it a
packet. A well-formed but unsolicited (or forged) response could therefore inject peers, grow
the frontier toward attacker-chosen nodes, or hand us an announce token — purely by looking
like a `get_peers` reply. This is the standard off-path-injection concern for DHT clients.

## What changed

All in `crates/ace-swarm/src/dht.rs`:

- **Distinct transaction id per query.** `dht_walk_frontier` now carries a per-walk `u16`
  counter and encodes it big-endian into a 2-byte `t` for each outbound `get_peers`. The
  fire-and-forget `announce_peer` sends in `dht_announce_peer` likewise get distinct txids
  instead of the shared `b"sc"`.
- **Inflight `(source, txid)` tracking.** When a query is sent, `(dest_addr, txid)` is recorded
  in an `inflight` map with an expiry deadline (`INFLIGHT_TTL = 3s`).
- **Responses are correlated before use.** A reply is accepted only if `(src, resp.txid)`
  matches an inflight entry; the entry is then removed, so a duplicate/replayed packet for the
  same query is not processed twice. Unmatched packets are **dropped and skipped** — not
  treated as end-of-window — so a spoofed packet can neither inject data nor cut the walk short.
- **Inflight entries expire.** Each round prunes entries past their TTL, so an abandoned
  (unanswered) query can't be matched by an unrelated packet arriving much later in the walk.
  The TTL is generous enough (3s ≈ two collection windows) to still admit a genuine reply that
  lands a round or two late.
- **`parse_response` now surfaces `t`.** `GetPeersResponse` gained a `txid: Vec<u8>` field,
  populated from the top-level transaction id (it lives alongside `r`/`y`, not inside `r`).

Public API is unchanged: `dht_get_peers`, `dht_get_peers_with_target`, and `dht_announce_peer`
keep their signatures. `parse_response`/`GetPeersResponse` are only used inside `dht.rs`.

## Tests

All deterministic, loopback-only (no live network):

- `parse_response_extracts_transaction_id` — the top-level `t` is captured.
- `dht_ignores_response_with_wrong_transaction_id` — the queried node replies with a txid we
  never sent; the peer is not harvested.
- `dht_ignores_response_from_wrong_source` — a spoofer socket replies with the *correct*
  observed txid from a source we never queried; still ignored.
- `dht_lookup_stops_once_peer_target_is_met` — updated so the mock node echoes the query's
  transaction id (as a real node does), proving the matching-source/matching-txid reply is
  accepted.
- Full `cargo test -p ace-swarm` green (94 lib tests + integration), clippy clean, `dht.rs`
  fmt-clean. The two `#[ignore]`d live DHT tests remain ignored.

## Still open

- Response validation is intentionally minimal (source + txid). Broader malformed/hostile KRPC
  hardening — partial compact records, wrong `y`/message-type fields, oversized payloads at the
  receive-buffer boundary — is tracked separately in **#41**.
