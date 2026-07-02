# 32 — Official consumer now requests outpace pieces, but does not accept them

> Superseded by note 33: the 8-byte `id=7` live piece header is a big-endian `f64` Unix
> timestamp. After outpace preserved/generated it instead of serving `[0u8;8]`, the
> official consumer moved to `status=dl` and returned media bytes.

Follow-up to note 31. The previous blocker was "official connects but does not request."
That is superseded: with the source-node profile and without a standard BT `Have` burst,
the official engine sends `Interested`, sends Acestream `id=6` chunk requests, and receives
outpace `id=7` pieces. Playback still does not start.

## Ground-truth source profile applied

This pass first ran the official engine as a local source node:

- `piece_length = 65536`
- `chunk_length = 16384`
- `bitrate = 8375`
- source `mi`: `distance_from_source=-1`, `is_accessible=0`, `download_window_end=max_piece`,
  `lsp=max_piece`, `live_window_size=115`, rates all zero, `node_state=1`

Outpace now mints broadcasts with that geometry and advertises inbound source windows
using that profile. This replaced the earlier pcap-relay guesses (`is_accessible=1`,
nonzero rates, lagged `position`), which did not make the official consumer request data.

## Standard BT Have burst is harmful

Proof `proof-official-source-profile-1`:

- outpace infohash: `cafe096789abcdef0123456789abcdef01234567`
- official `getstream` returned the same infohash
- outpace advertised `min=538 max=2584 position=2584 distance=-1`
- after the live bitfield, official sent `Interested`
- outpace replied `Unchoke + 2047 Have advertisement(s)`
- official reset/broke the connection repeatedly; playback timed out with zero bytes

This matches note 21's earlier negative result on the outbound path: standard BT `Have`
messages are anomalous in this live protocol path. Real live availability is carried by
Acestream `id=5` bitfield / `id=4` have, not standard BT `Have`.

Code change from this proof:

- `SeederSession::serve` still sends the Acestream live bitfield after the peer extended
  handshake.
- On `Interested`, it now sends only `Unchoke`; it does not send a standard BT `Have` burst.
- Regression: `serve_advertises_live_bitfield_before_peer_is_interested`.

## Official now requests pieces

The first no-`Have` proof (`proof-no-bt-have-1`,
`cafe0a6789abcdef0123456789abcdef01234567`) used the small 16 KiB test vector and was not
meaningful: it is smaller than one 64 KiB signed piece, so outpace had no complete piece
to advertise.

The meaningful proof used a repeated MPEG-TS body:

- name: `proof-no-bt-have-big-1`
- body: 6,768,000 bytes (`tests/vectors/media/h264-keyframes.ts` repeated 400 times)
- outpace infohash: `cafe016789abcdef0123456789abcdef01234567`
- descriptor tracker rewritten only to `udp://172.23.0.1:7001/announce`
- official `getstream` returned the exact same infohash
- official playback curl still timed out after 25s with `0` bytes

Outpace log:

```text
[seed-session] peer=172.23.0.2 advertise window min=0 max=102 position=102 distance=-1
[seed-session] <- ExtendedHandshake ... mi[min=-1 max=-1 pos=-1 dist=-1]
[seed-session] -> live Bitfield for 103 complete piece(s)
[seed-session] <- Interested
[seed-session] -> Unchoke
[seed-session] <- ACE Request stream=0 piece=100 chunk=0 bytes=10
[seed-session] -> Piece stream=0 piece=100 chunk=0 bytes=16384
...
[seed-session] <- ACE Request stream=0 piece=102 chunk=3 bytes=10
[seed-session] -> Piece stream=0 piece=102 chunk=3 bytes=16384
```

The official engine repeated requests for pieces `100..102`, chunks `0..3`, for the whole
playback window. Its stat endpoint stayed `status=prebuf`, `peers=1`, with nonzero
`speed_down` and a large/inflated `downloaded` counter from the repeated transfers:

```json
{
  "status": "prebuf",
  "speed_down": 82906,
  "peers": 1,
  "downloaded": 7508574208,
  "infohash": "cafe016789abcdef0123456789abcdef01234567",
  "is_live": 1
}
```

## Current precise blocker

The official consumer now gets all the way through:

```text
tracker discovery -> inbound TCP -> BT handshake -> signed extended handshake ->
official extended handshake -> live bitfield -> Interested -> Unchoke ->
id=6 chunk requests -> id=7 piece replies
```

It still does not release playback and instead re-requests the same edge pieces. The
remaining blocker is therefore **piece acceptance after delivery**, not discovery,
infohashing, source-window advertisement, or request routing.

The leading suspect at this point was the inbound listener's `[0u8;8]` live piece header,
because notes 21/25 showed the real header is per-piece and nonzero:

- `header[0..4]` is constant within a source session.
- `header[4..8]` varies per piece.
- semantics are still unknown.

This is an inference, not proof. Piece signing is already independently validated against
real official-source pieces (note 27), but the failing behavior could still involve a
header/signature interaction, live-edge advancement (`id=4` for newly completed pieces), or
another source-side acceptance check.

## Next lead

Use the local official source node to capture the exact `id=7` headers for controlled pieces
and derive the 8-byte header semantics, or instrument the official consumer's piece-accept
path to see which check rejects outpace's replies.

Concrete options:

1. Minimal client against official `--stream-source-node`: send signed extended handshake,
   `Interested`, and `id=6` requests; save full `id=7` blocks including the 8-byte header for
   several pieces.
2. Compare those headers against full piece bytes, piece index, stream id, source auth key,
   and timing to test hash/checksum/timestamp hypotheses.
3. If static derivation stalls, hook the official consumer around live piece validation
   (CPython frame-eval or PyGhidra-assisted symbol targeting) and observe the exact reject
   branch for outpace's pieces.

Do not spend more time on the now-proven layers unless they regress: official infohash
agreement, local tracker discovery, inbound handshake, live bitfield, interest, request
parsing, and piece reply routing are all past their prior blockers.
