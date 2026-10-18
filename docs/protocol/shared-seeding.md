# Shared seeding replies

`seed::protocol::prepare_reply` handles request decoding, shared-store lookup and
wire reply construction for inbound serving and production live-session
reciprocal serving. Connection loops retain eligibility, handshakes,
advertisements, transport writes and accounting.

The explicit profiles preserve the existing wire differences:

| Profile | Requests | Reply stream | Zero stored piece header |
| --- | --- | --- | --- |
| Inbound | Live id=6 and standard BT Request | Echo live request stream | Use session fallback header |
| Reciprocal | Live id=6 only | Zero | Preserve zero |

Live requests accept at least ten payload bytes, including trailing bytes.
Unavailable chunks remain unanswered. Standard BT requests retain the store's
block-range validation and stored bytes, including signature bytes.

Inbound replies remain gated by the connection's unchoked state and, when
configured, `ServeCoordinator`. Reciprocal serving remains outside that inbound
coordinator budget: it is eligible whenever seeding is enabled, including before
the remote peer sends `Interested`. This extraction does not unify those policies.

The prepared reply carries its media-byte count for caller accounting. The pool
counts after successful command enqueue and records its current peer count.
Inbound serving retains no upload counters. Coordinator join/watch/leave and
public `SeederSession` APIs are unchanged. The legacy single-peer loop and older
download-only driver/live Rust APIs were removed in #193; `live_session::follow_live`
is the production live entrypoint.
