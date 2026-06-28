# Live extended-handshake `mi` dict (captured by ace-peer in Phase 2)

Our own `ace-peer` library connected to a real swarm peer (`146.158.146.137:8621`)
for the live infohash `50e935…2d6e47`, completed the `AceStreamProtocol` handshake,
and decoded the peer's BEP-10 extended handshake. Beyond the keys in
`05-crypto.md`, the **`mi` sub-dict carries the live piece-window state** — the
information a client needs to drive the live piece picker (Phase 3).

## `mi` keys observed (live stream)
- **`min_piece`, `max_piece`** — the available live window in piece indices
  (observed e.g. `min_piece≈14694515`, `max_piece≈14694578`).
- **`position`** — current live head piece (≈`max_piece`).
- **`live_window_size`** — window width in pieces (observed `100`).
- **`distance_from_source`** — hops from the broadcaster (source = 0).
- **`download_window_end`**, **`time_from_source`**, **`ping_from_source`** — live timing.
- **`up_rate`, `down_rate`, `top_up_rate`, `top_session_up_rate`, `upload_rating`,
  `mam`, `lsp`, `peer_type`, `is_accessible`** — peer rate/topology metrics.

## Top-level extended-handshake keys (live)
`tt=bt`, `v=3021900` (engine 3.2.19 build), `platform=3`, plus `node_id` (32 bytes),
`signature` (64 bytes), `yourip` (our public IP echoed), `pi` (peer port),
`stream_statuses`, `m={ut_metadata:2}` (and the `05-crypto.md` keys).

## Implications for Phase 3
- The live piece picker should: read `min_piece`/`max_piece`/`position`/
  `live_window_size` from peers, target pieces near the live head, and request within
  `[min_piece, max_piece]`. A fresh client cannot request piece 0 (long evicted) — it
  must start near `position`.
- `signature`/`node_id` are present but did **not** gate our unsigned client from
  receiving the handshake — consistent with "no identity required" (Unknown #1). They
  appear to be source-authenticity metadata, not an entry gate.
- `OPEN:` exact units of the rate fields and whether `signature` must be valid to be
  *unchoked* for piece data (we were not unchoked as a fresh leecher — normal BT).
