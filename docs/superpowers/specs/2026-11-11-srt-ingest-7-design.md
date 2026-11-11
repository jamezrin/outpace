# Optional bounded SRT broadcast ingest (#7)

Status: design draft, not implemented. Routine design decisions are delegated by
the milestone goal. #7 remains open until encrypted publisher interoperability,
signed playable output, persisted identity/cursor continuity and all gates pass.

## Intent and approaches

Accept a common SRT caller publishing MPEG-TS and feed the existing exclusive
ProducerSession / BroadcastIngest path. Preserve HTTP PUT, RTMP, raw broadcasts,
persisted identity and continuous piece reservations. No new media transcoder,
SRT caller/rendezvous/source-fetch mode, protocol implementation or public listener
by default. This is transport origination, not official-engine API parity.

Considered pure Rust SRT: the older srt-tokio repository explicitly warns against
production use; a second candidate's audited receive window charges occupied
packets while sparse storage grows by sequence distance. Adopting that snapshot
would require protocol hardening beyond an ingest adapter. Considered native Rust
wrappers: sampled wrappers bundle native crypto/SRT plus their reactor or wider
pipeline abstractions. They are not selected merely because their API is async.

Selected proposal: maintained reference libsrt through a small private C ABI shim
and Rust RAII boundary, optional Cargo `srt` feature. Use system libsrt >=1.5.7
with encryption support; fail configuration/build explicitly if prerequisites are
missing. The shim uses installed headers for enum/socket-option ABI and sockaddr
conversion; no hand-copied struct layout or guessed constants. No closed binary
or private artifact enters the repository. MPL-2.0 reference-library provenance
and native/TLS packaging obligations must be documented alongside AGPL application.
Default builds and releases do not require or link libsrt. Enabled container/custom
builds must explicitly provide its development/runtime packages. Linux first;
other enabled targets must compile/test before being claimed supported.

## Operator contract and security

OUTPACE_SRT_BIND is unset by default: no UDP socket, native startup, thread or
secret lookup. An explicit numeric SocketAddr enables a listener; a build without
`srt` rejects that configuration, never silently ignores it. Bind defaults are not
expanded to wildcard. Documentation shows loopback and calls out UDP firewall/NAT
and exposed binds; the daemon never changes firewall/routing/VPN settings.

Require OUTPACE_SRT_PASSPHRASE_FILE when enabled, bounded regular non-symlink file
owned by the operator and inaccessible to other users, passphrase 10..79 bytes.
Do not accept secrets in URLs/stream IDs or log native errors containing secrets.
SRT encryption is mandatory (enforced encryption, with the sender selecting a
reference-supported AES-128/192/256 key length), not
an optional plaintext fallback. Shared PSK grants publishing authority for this
listener; it is not individual-user authentication or payload integrity beyond
what the negotiated reference protocol provides. Document that trust boundary.

The caller Stream ID is exactly one existing valid_broadcast_name, <=64 ASCII
characters; no arbitrary command/query/URL routing. Reject invalid IDs before
creating or reopening broadcast state. Existing producer conflicts are explicit
and must not overwrite another HTTP/RTMP/SRT producer. Library logging is bounded
and redacted through the application's operational logging policy, without a new
application logging framework or secret-bearing raw stderr.

OUTPACE_SRT_LATENCY_MS defaults120, validated20..2000; this is a receiver policy,
not a latency guarantee. OUTPACE_SRT_MAX_CONNECTIONS defaults4, bounded1..16.
Set native receive/send buffers explicitly (1MiB each per socket), bounded flow
window consistent with MTU, live/message mode, enforced encryption, connect/peer
idle deadlines and finite close behavior; verify effective options after setting.
Do not infer total native RSS from these buffers. Measure actual fixed/native
thread counts, memory, CPU and publisher-to-signed-piece latency before acceptance.

## Ownership and backpressure

One owned native polling thread per listener services nonblocking accept/receive
with finite waits <=10ms and a finite per-turn batch. It owns every native socket
and poll descriptor. Accepted sockets and native listen backlog are bounded by the
configured connection cap; no task per handshake or unbounded pending map. Audit
reference backlog/handshake limits before implementation acceptance.

Each admitted connection owns a bounded media channel of four <=64KiB messages
and one retained pending message. Polling continues cancellation/control and other
connections while a media channel is full. No blocking send on the native thread
or Tokio event loop; disable that socket's read interest until capacity returns.
Native receive buffers then supply explicit finite backpressure; timeout/drop
policy reports disconnect rather than silently growing memory. Oversized or
invalid message framing is rejected, never truncated into a seemingly valid TS.

An owned async connection worker validates 188-byte TS framing across message
boundaries and feeds BroadcastIngest. Existing signed-piece geometry, verified
publication and durable cursor reservation are authoritative. At most one ingest
attempt per producer; no duplicate identity/store implementation. The transport
worker's producer reservation remains owned through pending publication and exact
socket retirement. A disconnected publisher drops incomplete TS/piece staging per
existing ingest semantics, checkpoints reservations, and releases producer ownership
only after completion. Reconnect to the same name reuses persisted identity/cursor;
it does not reuse an unresolved producer claim.

Control requests use bounded storage and priority service. Cancellation is latched
independently of media queue capacity. Drop requests termination; explicit shutdown
waits for native thread join, socket/poll destruction and async ingest workers.
A canceled join waiter transfers to the existing cleanup owner. Native global
startup/cleanup is reference-counted and cannot race active sockets; no unconditional
process-global cleanup from one connection's Drop. Never run potentially blocking
native close or thread join on Tokio's critical event loop. Ownership uncertainty
retains admission and surfaces an error; it is not counted as successful cleanup.

## Required proof and acceptance

1. Default/off/build-feature/config/secret validation RED/GREEN, including wildcard
   opt-in, absent libsrt, bad passphrase permissions and invalid names.
2. Actual library nonblocking options, effective buffer limits and wrong-key/plain
   rejection; cap/backlog pressure, oversized/fragmented TS and slow signed-store
   backpressure. Prove control/cancellation progress with full media channels.
3. Exact socket/poll/thread retirement, cancellation during startup/admission/write/
   close, and failed cleanup retaining ownership; no detached work or slot reuse.
4. Exclusive HTTP/RTMP/SRT producer conflicts plus signed-piece verification and
   persisted identity/monotone piece numbering across disconnect/reconnect/restart.
5. One bounded loopback FFmpeg SRT publisher carrying generated public-free media;
   consume through normal broadcast playback and decode with a standard tool.
   Include wrong key, no media and reconnect controls, no external-content tests.
6. Measure synthetic one/four publishers for throughput, latency, RSS, native threads
   and cancellation; distinguish limits from observations. Full normal/default
   gates and enabled-feature gates; independent architecture/security/test review.
7. Document native prerequisites/license, URL with nonsecret stream ID, passphrase
   delivery, finite bounds, supported codecs/TS input, default-off behavior and
   manual smoke. Update #7 only for proved original criteria; no readiness release
   or milestone closure from this feature alone.

## Remaining builder decisions

Confirm native handshake/backlog and receive-message size enforcement from exact
reference sources. Set a precise flow-window/buffer pair with option readback.
Choose a cancellation-safe native-owner/async-cleanup handshake using existing
CleanupTracker rather than making joins discardable. Resolve these in the plan
before product code; source-level assumptions are not execution evidence.

## Reference API clarification during Task 1

The reference v1.5.7 PBKEYLEN contract requires a unidirectional receiver to leave
PBKEYLEN at zero and accept the sender's advertised key length. The listener must
not force a conflicting key length. Accepted sockets must prove a reference-supported
encrypted key state (16/24/32-byte key) before producer admission. This follows the
reference API, avoids undefined negotiation with common publishers and retains
mandatory shared-secret encryption; it does not add an AES-256-only guarantee.

Native listen backlog is additional to admitted connections, not included magically
in the application's active count. The implementation must bound/document both
terms and prove handshake/retirement limits before advertising a total bound.
