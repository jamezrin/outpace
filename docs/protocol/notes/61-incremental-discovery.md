# Incremental live peer discovery

Live open resolves its transport descriptor before discovery. The first candidate starts the
existing connection path immediately; one owned discovery generation remains with the session
through active pools and reconnects. Later candidates enter the existing bounded cohort scheduler
even while its active pool is full. Consumer cancellation closes discovery and connection work.
An observation timeout does not create a replacement discovery generation.

Tracker jobs resolve and announce concurrently, with at most four jobs and one two-second
budget per tracker covering resolution and both BEP-15 exchanges. The tracker list remains
bounded to 64 URLs of at most 256 bytes and retains its destination policy. Correlated DHT
responses publish unique values immediately. Bootstrap resolution and response windows share
its total budget. Default discovery keeps its target of eight and 15-second DHT budget;
background discovery uses an eight-second budget, below the default 12-second stale timeout.

Tracker and DHT hostname resolution share one process-wide service with at most four dedicated
native resolver workers and eight queued requests. A worker retains its physical capacity until
its OS call returns, even after the caller times out or cancels. Queued cancellation removes the
request; overload fails promptly without another queue, waiter or thread. Literal addresses
bypass native resolution, preserving destination policy. Partial thread initialization retains
only the successfully started workers and never replaces a stalled one. No successful workers
means hostname resolution is unavailable while literals remain usable. Resolved results retain
at most sixteen socket addresses.

The process owner retains those fixed worker handles; stream cancellation and Tokio runtime
shutdown do not wait on an OS resolver call. A genuinely stalled NSS call can occupy a worker
indefinitely, reducing available resolution capacity. No hard cancellation or joined-shutdown
guarantee is made for such a call. Unlike an async permit around Tokio lookup, this service bounds
actual executing and queued work across repeated discovery sessions. Other hostname users
outside tracker/DHT discovery are outside this facility's bound.

Bootstrap completions enter one owning DHT frontier immediately, while other resolutions remain
outstanding. Its original absolute deadline covers all seed queries and response windows. Later
useful seeds are retained without restarting the walk; interrupted response rounds preserve
correlation and completed rounds retain their original exhaustion and timeout accounting.

The combined discovery feed retains at most 1024 unique peers. Reaching that cap or losing the
consumer cancels its remaining source futures. Normal source completion drains its buffered
final candidates. Reaching one source's target does not cancel the other incremental source.
Aggregate discovery for metadata, VOD and seeding retains its existing target completion policy.

All candidate transports use one scheduler. Running connections and queued ready receipts
share the configured parallel limit; active transports retain their separate active limit.
Source and PEX provenance, exploration cohorts, real admission accounting, productive-history
reset and failure cooldowns remain authoritative. A duplicate discovered hint cannot release
another producer's reservation. Active peer loss and consumed/rejected ready receipts wake
released capacity even when other upstreams survive. Completed failures and the earliest
unobserved cooldown deadline wake useful work; each deadline is consumed once, including
when it elapsed during another event. Active and pending peers are excluded from retry wakes;
ordinary request ticks do not repeatedly fan out old-window transports. Aborted attempts do
not count as completed exploration. The one-second live-window gate and current-piece floor,
per-chunk worker floor, ordered transport recovery and descriptor authentication remain intact.

When the last upstream that emitted usable media is lost, an announced Source transport whose
window ends below the next-needed cursor is refreshed without a false network failure or added
exploration. Current-cursor-capable fallback and ready transports remain usable. Fresh runtime
ids distinguish the new session at the same endpoint; queued blocks and loss events from retired
workers cannot affect its progress or ownership. A window behind the head remains usable when
it covers the cursor; productive-history reset does not discard such prepared peers.

An id=4 announcement advances its own peer window independently of the shared head. A faster
gossip peer cannot suppress another producer announcing the same or a lower head. The shared
head remains monotonic; request scheduling wakes only when that peer window or the shared head
actually grows. Cursor, prefetch and per-chunk floors still constrain usable media.

## Durable productive-peer hints

A nonempty configured cache directory stores private productive-peer hints in `recent-peers-v1`,
including when the piece-store backend is Memory. An empty directory keeps hints in memory.
The existing daemon cache-directory configuration selects this location; there is no new
network default or environment setting. The private file contains runtime swarm keys and
endpoints and must never be committed or included in public reports.

Credit requires a complete authenticated contiguous piece from one actual producer and usable
media output. The session retains that producer's provenance, including an id36-learned source.
Mixed-producer pieces, unsigned output, invalid signatures, incomplete pieces, announcements
and stale chunks do not earn credit. Attribution follows the buffered reassembly lifetime,
including partial and completed ahead pieces retained through reconnect. Actually written
blocks update it; completed-piece duplicate no-ops cannot establish another producer's credit.
Attribution is bounded by the reassembly window and pruned on rejection, actual skip and emission. Hints never populate the verified-descriptor index or
confirm a current live head; fresh peer validation and the original descriptor remain required.

Hints expire after 300 seconds. Retention is bounded to 256 swarms, 16 peers per swarm, eight
source peers per swarm and 3000 rows overall. The additional total-row cap bounds the serialized
snapshot below 256 KiB; it can evict entries before every swarm's individual maximum is filled.
Eviction favors older records. Rows over 128 bytes, corrupt/unknown versions, invalid or
nonpublic endpoints, duplicate or excessive rows and expired/future timestamps are rejected.

One coalesced writer owns each normalized cache path across provider handles. Loading occurs
on that worker alongside already started fresh discovery, with at most 100 ms of startup wait.
New productive records merge with loaded records without replacing newer progress. Streaming
changes only bounded memory and signals the writer; snapshots normally coalesce over five
seconds. Clean writers wait without a timer until notification; dirty/retry work retains its
coalescing deadline and explicit flush/stop wakes it. Session shutdown requests a flush without blocking stream cancellation. Private files
must be owned regular single-link files with mode 0600. Directory descriptors, nofollow and
nonblocking opens, inode validation, private temporary files and atomic replacement avoid
symlink, FIFO and path-replacement hazards. A directory inode lock excludes alias writers.

At most 16 active or retired writer handles are owned globally. Last-owner teardown signals
flush/stop and retires its handle; only finished handles are joined. A stalled OS filesystem
syscall can delay its worker indefinitely. Async teardown does not wait on that syscall, and
an unfinished retired writer still occupies its bounded slot and excludes a same-path writer.
A two-second validation flush timeout means persistence was not observed; it does not mean
that disk I/O was cancelled or that a stalled writer was joined. I/O failures leave streaming
functional. Platforms without Unix inode validation use memory hints.

## Validation and acceptance

Offline controls exercise the actual provider/follower with real loopback peer sessions, the
shared production discovery combiner, real UDP tracker/DHT responses, authenticated pieces,
fresh provider reconstruction and real private filesystem checks. They cover first BT attempt
within two seconds despite a slow second source, later bounded refill, full-pool candidate
retention, running/ready capacity, consumer cancellation, correlation, normal final-batch drain,
cache credit boundaries and terminal writer ownership.

Operational stage logs distinguish request, resolution, first candidates, TCP attempt/connect,
BT handshake, authenticated contiguous progress and initial source output. HTTP startup buffering
remains unchanged. Full issue acceptance requires a simultaneous original/candidate comparison
with default settings: first live connection within three seconds of resolution and first body
within original plus five seconds, jointly with issue 166. Offline fast connection controls alone
do not establish default first-body or media parity. Live artifacts and identifiers remain private.
