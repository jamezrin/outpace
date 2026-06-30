# Security Review Follow-ups - 2026-06-30

Scope: follow-up review of the current outpace daemon implementation after the
external review notes. P2 (HTTP control authorization) is intentionally not tracked
here per maintainer decision; the relevant issue is the untrusted metadata/resource
exhaustion path.

## Accepted

### P1: cap peer-advertised `metadata_size`

`cid:<40hex>` resolution trusts the metadata-swarm peer's extended-handshake
`metadata_size`:

- `crates/ace-swarm/src/resolve.rs:121` accepts any positive value and returns it as
  `usize`.
- `crates/ace-peer/src/session.rs:95` derives the number of BEP-9 requests from that
  size.
- `crates/ace-peer/src/session.rs:103` allocates `vec![0u8; metadata_size]`.

A malicious metadata peer can therefore force a large request fan-out and a large
allocation before any transport descriptor is decoded. Fix direction: define a
transport-file metadata ceiling close to observed/expected Acestream transport sizes
and reject advertised sizes above it before calling `fetch_metadata`.

## Additional Findings

### P1: cap decoded transport geometry before streaming

The fetched `AceStreamTransport` descriptor is also untrusted. The decoder currently
requires positive `piece_length` and `chunk_length`, but does not cap them or require
the live chunk geometry to fit the wire codec:

- `crates/ace-wire/src/transport.rs` accepts any positive descriptor lengths.
- `crates/ace-swarm/src/types.rs` casts `piece_length / chunk_length` to `u16`.
- `crates/ace-engine/src/ace_provider.rs:248` constructs a `PieceReassembler` from
  the decoded `piece_length`.
- `crates/ace-wire/src/reassembly.rs:52` allocates a per-piece buffer of
  `piece_length` bytes on the first accepted block for a piece.

A malicious transport served during content-id resolution can advertise an enormous
piece size and cause memory exhaustion when streaming starts. It can also advertise a
chunk count that truncates to `u16`, making request generation inconsistent with the
descriptor. Fix direction: validate transport geometry when building `StreamInfo`:
reasonable max piece length, fixed/allowed chunk length, exact divisibility, and
`chunks_per_piece <= u16::MAX`.

### P2: bound peer-driven live request expansion

While following a live peer, incoming `Have(u32)` messages can expand `head` and
immediately trigger `request_range(from, head, chunks_per_piece)` when unchoked:

- `crates/ace-engine/src/ace_provider.rs:271` updates `head` directly from the peer's
  `Have` piece index.
- `crates/ace-engine/src/ace_provider.rs:276` requests every piece from the previous
  requested point through the new `head`.
- `crates/ace-engine/src/ace_provider.rs:310` sends `pieces * chunks_per_piece`
  requests without a per-advance cap.

A hostile peer can send a very large `Have` and make the daemon spend a long time
sending requests, tying up the session task and network writes. Fix direction: treat
peer live windows as bounded moving windows, cap per-tick request fan-out, and reject
or ignore implausible jumps beyond the configured prefetch/window size.

### P2: coalesce concurrent first opens for the same stream

`StreamManager::get_or_start` checks the session map, releases the lock, runs
`provider.open(id)`, then inserts with a double-check:

- `crates/ace-engine/src/manager.rs` checks for an existing session before opening.
- `provider.open(id)` can perform discovery, connect to peers, and spawn provider
  background work before a `StreamSession` exists in the map.
- The final `entry(key).or_insert(session)` keeps only one session, but concurrent
  losers have already paid the network/task cost.

Many simultaneous first requests for the same stream can therefore amplify discovery
and peer connection work, even though the intended steady-state is one download per
`(network, id)`. Fix direction: store an in-flight/opening state per key, or hold a
per-key async lock, so only one opener runs and other callers await its result.

### P1: verify live piece authenticity before serving bytes

The transport descriptor exposes `authmethod=RSA` and the broadcaster `pubkey`, and
the protocol notes describe live integrity as source RSA signatures. The playback path
does not currently verify any live-source signature before reassembling and serving
peer-supplied TS bytes:

- `crates/ace-wire/src/transport.rs` decodes `pubkey`, but `StreamInfo` drops it.
- `crates/ace-engine/src/ace_provider.rs:283` accepts incoming live `Piece` messages.
- `crates/ace-engine/src/ace_provider.rs:286` feeds their payload into the reassembler.
- `crates/ace-media` gates/resyncs MPEG-TS structure, but does not authenticate source
  data.

A malicious peer in the swarm can inject arbitrary MPEG-TS payloads that will be
proxied to clients if they satisfy the loose chunk/reassembly path. Fix direction:
carry the transport auth fields into `StreamInfo` and implement live-source signature
verification before a piece can be marked complete or emitted.

### P1: reject unsolicited live chunks outside the requested window

`PieceReassembler` accepts any piece index at or above `next_emit`; it does not know
what the caller requested or what live window is plausible:

- `crates/ace-engine/src/ace_provider.rs:283` passes every decoded live chunk to
  `reasm.add_block`.
- `crates/ace-wire/src/reassembly.rs:40` only drops pieces below `next_emit` or
  already-complete pieces.
- `crates/ace-wire/src/reassembly.rs:52` allocates a full `piece_length` buffer for
  each new piece index.

A hostile peer can send chunks for arbitrary far-future piece numbers and force one
full piece allocation per index, retained in `partial` or `complete` until all earlier
pieces arrive. Fix direction: track requested `(piece, chunk)` ranges in the live
driver and ignore any data outside the requested window; also cap the reassembler's
accepted distance ahead of `next_emit`.

### P1: cap `TsResync`'s unconfirmed tail

`TsResync` is meant to discard junk until it can confirm MPEG-TS packet alignment, but
when no confirmable sync point exists it keeps the entire buffer for the next push:

- `crates/ace-media/src/mpegts.rs:73` appends every ready byte run to `self.buf`.
- `crates/ace-media/src/mpegts.rs:91` breaks when no confirmable packet start exists.
- `crates/ace-media/src/mpegts.rs:96` drains only bytes before `i`; in this case `i`
  is still `0`.
- `crates/ace-engine/src/ace_provider.rs:291` feeds reassembled peer data into this
  resync stage before sending to clients.

A peer that supplies complete pieces of non-TS data can make this buffer grow by every
piece while emitting nothing. Fix direction: keep at most the minimum lookahead needed
to find packet sync (for example `2 * TS_PACKET_LEN - 1`, or a small bounded scan
window), and abort the peer/session after too much consecutive unsynchronized data.

### P2: bound bencode nesting depth

The shared bencode parser recursively parses lists and dicts without a depth limit:

- `crates/ace-wire/src/bencode.rs:49` dispatches recursively by token.
- `crates/ace-wire/src/bencode.rs:77` parses lists by recursively calling
  `parse_value`.
- `crates/ace-wire/src/bencode.rs:88` parses dict values the same way.
- Peer extended handshakes and DHT replies both feed untrusted network bytes through
  this parser.

Frame and UDP receive sizes cap total bytes, but a small deeply nested bencode value
can still drive deep recursion and risk stack exhaustion. Fix direction: thread a
remaining-depth budget through parsing and reject inputs above a conservative nesting
limit.

### P2: do not start streams from HLS segment fetches

`GET /streams/{network}/{id}/seg/{n}.ts` currently calls `get_or_start_hls`, so asking
for any segment number can start provider resolution/discovery:

- `crates/ace-engine/src/http.rs:84` starts or fetches the HLS packager from a segment
  request.
- `crates/ace-engine/src/hls.rs:41` starts a background HLS receiver for the session.
- If the requested segment is not retained, `crates/ace-engine/src/http.rs:90` returns
  404 after the startup work has already happened.

This lets arbitrary segment probes trigger the expensive stream-open path even when the
caller never fetched a playlist and the segment cannot exist yet. Fix direction:
segment endpoints should only use an already-running packager/session (`get`, not
`get_or_start_hls`) and return 404 for unknown streams.

### P2: constrain tracker URLs from decoded transports

For `cid:<40hex>`, the transport descriptor is fetched from an untrusted metadata peer.
Its tracker list is copied into `StreamInfo` and later used for DNS resolution and UDP
announce traffic:

- `crates/ace-wire/src/transport.rs:123` collects every byte-string in `trackers`.
- `crates/ace-swarm/src/resolve.rs:50` copies those tracker URLs into `StreamInfo`.
- `crates/ace-engine/src/ace_provider.rs:150` passes the tracker list to
  `discover_peers`.
- `crates/ace-swarm/src/discover.rs:16` accepts either `udp://host:port/...` or any
  raw `host:port` string, then `lookup_host`s it.
- `crates/ace-swarm/src/discover.rs:48` sends UDP tracker announces to the resolved
  addresses.

A malicious metadata peer can therefore make the daemon perform DNS lookups and UDP
traffic to arbitrary internal or external hosts when a content-id stream is opened.
Fix direction: require `udp://` tracker URLs, cap tracker count and string length,
reject private/link-local/multicast destinations unless explicitly configured, and
consider using only trusted/default trackers for content-id resolution until transport
auth is implemented.
