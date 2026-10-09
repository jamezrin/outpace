# Selecting a live start from window evidence

A fresh pool with one advertised window processes peer messages immediately but
withholds media requests and output for up to one second. It can connect source
announcements, PEX candidates and existing background candidates during this
interval. An independently connected current window releases the decision early.
Source announcements contain addresses; PEX ranges rank connection candidates.
Neither alone establishes a new authoritative live head.

The deadline uses monotonic time and does not restart on hints or receipts. It
wakes independently of the configured request-check interval. A stale refill's
bootstrap gossip harvest shares the remaining deadline; ordinary reconnect
harvesting keeps its existing 750 ms bound. If no independent current evidence
arrives, the provider starts from the best known window. That fallback keeps
single-peer streams usable but cannot prove freshness when the only available
peer is stale. Several independently connected initial windows already supply
comparison evidence and require no additional decision interval.

During this initial decision, a strictly newer connected window can replace the
provisional transport even with one active slot. Candidate handshake/interest
writes must succeed before the old runtime closes. Retiring the nonproducing,
older transport applies its existing bounded retry cooldown once, without an
additional exploration count or a fabricated physical-loss event. An independent
matching window releases the decision while retaining a full existing pool.
Productive established pools retain their existing admission/recovery policy.
The session's bounded learned tasks, queued receipts and producer ownership stay
in effect; a consumer close cancels owned work.

Before assignments, retries and piece publication, the cursor advances to at
least `known_head - configured_prefetch`. The provider drops older buffered
partials/completions, releases their request slots and ignores late stale blocks
before caching or publishing them. Workers share a monotonic session-local floor
and check it before each queued chunk write. A write already underway when new
evidence arrives cannot be recalled; no subsequent stale queued chunk is issued.
Already published bytes cannot be retroactively removed when a future window
reveals a newer head.

A forward skip after published output retains the fresh unknown-loss transport
gate. Initial provisional positioning has no published prefix to interrupt.
The existing startup reservoir policy and transport-loss metadata recovery are
unchanged. Zero-prebuffer validation therefore observes provider output directly.

Windows use the existing absolute unsigned 32-bit wire piece domain. Negative,
inverted or out-of-domain handshake windows are rejected. Arithmetic near its
upper boundary is saturating; a low counter observation never moves the cursor
backward. This does not implement modular epochs for a genuine counter wrap or
source reset. Broader discovery latency, startup reservoir timing and SDT
continuity remain separate work.
