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

Inbound media replies remain gated by the connection's unchoked state. The listener
always supplies the exact key's `ServeCoordinator`; only direct `SeederSession::serve`
callers retain the optional-coordinator compatibility behavior. Metadata replies bypass
Interested/Unchoke. Reciprocal serving remains outside the inbound admission and coordinator
budgets: it is eligible whenever seeding is enabled, including before the remote peer sends
`Interested`. These policies are intentionally separate.

## Inbound admission and lifetime

The daemon defaults to G=64 retained inbound connections, N=16 identified sessions per canonical
stream, and U=8 stable desired unchokes per exact key. G counts unidentified/incomplete
handshakes; configured G=0 acts as one. Ordinary registry keys are self-canonical. Broadcast
registration explicitly associates media and metadata aliases; both consume the same N,
including metadata-only, uninterested and choked occupants. At defaults the 17th combined
admission closes before any handshake response. Unknown, empty, conflicting and full entries
also close without a response. Knowing a served key establishes eligibility, not authentication.

`OUTPACE_MAX_INBOUND_PER_STREAM` (`Config::max_inbound_per_stream`) selects N. N=0 restores
global-only admission without changing U; positive N greater than G is accepted without
clamping. Absence defaults to 16; invalid/empty/negative/overflow/non-Unicode environment inputs
fail startup. Restart/recreate is required. See [deployment](../deployment.md#inbound-occupancy-policy)
for the explicit Compose service environment override needed to forward this new variable;
putting it in `.env` alone is insufficient.

The listener reserves G nonblocking before spawning. Inside the handshake predicate it resolves
the registry entry, checks the complete canonical association, reserves N, captures store and
metadata Arcs, obtains the required coordinator, and touches activity as one synchronous
registry-locked decision. Capacity and live alias bindings share a separate accounting mutex,
always acquired after the registry mutex. No await, disk/store construction, signing or socket
I/O occurs under these locks. Failed admission leaves no partial reservation.

The non-Clone canonical permit releases only against its stable accounting owner, never a
replacement registry generation. Live alias bindings and counts survive remove/recreate; even
N=0 rejects redirection of a live alias into another canonical allowance. A media-only occupant
retains its metadata alias binding. The last connection release removes idle accounting buckets
and bindings. Producer leases retain their existing generation checks and ownership semantics.
Registration also pins newly-associated aliases for an already-occupied canonical stream; weak
accounting references let it do so without making the registry own connection accounting.

The connection future owns G throughout handshake and serving; its snapshot owns N across
optional peer-id stages, handshake response, signing, and all serving awaits. Read/write errors,
EOF, serving errors and task cancellation release both. Backpressured replies retain both until
completion/error or cancellation. Admission before removal retains a coherent resource snapshot;
removal first rejects. DELETE is not revocation: existing sessions can continue serving retained
data/coordinators, and their old coordinators no longer receive registry-driven rotations.
Aborting the listener alone does not cancel its detached handlers.

For I interested peers the exact-key coordinator desires `min(I, U + 1)` selections in
interest-entry order: the first U peers plus one rotating tail peer on nominal 10-second daemon
ticks. U=0 still selects one optimistic peer. Watch observation and pending socket writes mean
this does not bound instantaneous wire-visible unchokes. Media and metadata keys keep independent
coordinators despite sharing N. Admission grants no bandwidth limit, equal share or class
reservation; metadata-only clients can crowd out media, and silent handshakes can exhaust G.
It does not bound accept churn, kernel backlog, CPU or host egress.

TTL=300 (zero disables the reaper) concerns ownerless registry entries, not connection lifetime.
The existing 250 ms serve polling can cancel pending reads and their inner I/O timeouts;
per-operation timeouts therefore do not establish a whole-session idle deadline. Listener
addresses, exposure, enable flags, cache paths and outbound reciprocal serving remain unchanged.

The prepared reply carries its media-byte count for caller accounting. The pool
counts after successful command enqueue and records its current peer count.
Inbound serving retains no upload counters. Coordinator join/watch/leave and
public `SeederSession` APIs are unchanged. The legacy single-peer loop and older
download-only driver/live Rust APIs were removed in #193; `live_session::follow_live`
is the production live entrypoint.
