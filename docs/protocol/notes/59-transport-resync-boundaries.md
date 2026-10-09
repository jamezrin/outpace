# Transport loss boundaries and cached access-point recovery

`TsResync::push_report` returns aligned packets and ordered loss boundaries. Each
boundary contains the offset in that push's aligned output immediately before the
loss and the number of discarded input bytes. Offsets are packet aligned, including
zero and the end of the output. A trailing boundary can arm recovery before any
post-loss packets arrive; more discarded bytes during later pushes remain visible.
Initial unsynchronized input does not count as loss of previously synchronized media.
The unconfirmed tail remains bounded to 375 bytes.

After acquiring sync, a complete packet beginning at its expected sync offset is
preserved even when the following packet is misaligned. This avoids discarding the
last valid packet before boundary junk. Sync geometry cannot prove that the packet's
payload is intact if bytes were inserted inside that packet; this is transport
alignment, not payload authentication or decoding. Authentication of live pieces
still runs before the resync path, with its existing geometry/capacity checks.

The provider processes each aligned region in order. Already-valid prefix packets
pass through the existing gate before it is rearmed. Each resumed region becomes
a separate `LiveOutput`; its gap flag means discontinuity immediately before that
chunk. Multiple losses can therefore produce multiple ordered chunks/markers from
one input batch. A second loss while still gated retains the recovery accounting
and learned metadata instead of restarting a fresh PSI search.

The provider's initial passthrough gate observes PAT/PMT changes continuously.
`KeyframeGate::rearm_for_discontinuity` preserves complete current cached tables,
video PID and codec for same-stream transport loss, clears partial input and scan
state, and marks the first resumed packet. The next recognized video access point
can open the gate without repeated PSI; cached PAT/PMT precede the resumed packet.
PAT program/PMT identity changes invalidate dependent metadata, and current PMTs
replace the video PID/codec or remove video. Incomplete, non-current or mismatched
program tables do not replace the cache. Single-packet PSI remains the supported
parser scope; no multi-packet PSI assembler is introduced.

Fresh resets retain their previous semantics. Whole-piece skips still require
fresh tables; `reset_for_discontinuity` used by HTTP and subscriber lag still clears
learned metadata. The packet-budget fallback remains unchanged and is reported as
`ScanBudget`, rather than being confused with a recognized `AccessPoint`. There is
no small-byte-count bypass: a short alignment loss alone proves no decoder safety.

Operational `[mpegts]` boundary events report `discarded_bytes`, the aligned batch
offset, and the provider's output position before the loss. The latter counts
provider bytes and is not automatically an HTTP capture offset: startup/client
gates, repeated tables and subscriber timing can change that relationship.
Recovery events report total `discarded_bytes`, actual aligned `withheld_bytes`,
`loss_events`, resume reason and monotonic `duration_ms`. Withheld bytes exclude
inserted tables/markers and the resumed packet. Duration measures wall time between
detection and filtering the resumed packet, not lost source PCR duration or network
delivery delay. A piece skip superseding a pending transport recovery is logged
explicitly with its accumulated measurements.

`AceSource` and an already-released `StartupBufferedSource` preserve the prefix and
resumed-chunk ordering. While collecting, the startup reservoir retains its existing
gap policy: discard the pre-gap reservoir and restart its first-clean timer. This
change does not establish startup cushion/timing acceptance for #166, nor native
HLS quality. If a collecting-phase source loss causes more than one GOP of loss
relative to the original engine, #169's live criterion remains unmet and requires
concrete interface/retention analysis before changing that policy.

Tests use synthetic transport bytes and actual provider/source callers, including
all split points, multiple losses, capped tails, pending recovery and metadata
changes. Live #169 acceptance additionally requires an actual source misalignment
event in simultaneous optimized outpace/original-engine captures, identified with
stable media anchors and measured original-relative loss of at most one GOP.
Uneventful capture, ambiguous repeated PCR identity or a decoder's zero exit do not
prove that criterion.
