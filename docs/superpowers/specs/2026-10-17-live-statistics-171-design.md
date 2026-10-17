# Live statistics for issue #171

Status: proposed specification; approach approved, implementation not started.
Base: `main` after #228. Scope: native live status and the existing experimental
Ace statistics endpoint. No playback, discovery, buffering-limit or listener
defaults change.

## Outcome

Operators must see measured activity while a source is collecting its startup
buffer, emitting media or stalled. Native `bitrate` must become nonzero within
five seconds of sustained incoming media. `buffer_ms` must track the actual
startup reservoir rather than remain at its last emitted-chunk snapshot.
Compat statistics must expose measured download/upload rates and known live
piece positions, with explicit units and no invented wall-clock timestamps.

The current session pump copies `source.stats()` only after `next()` returns.
`StartupBufferedSource::next()` can collect many chunks before returning, so
improving `AceSource::stats()` alone cannot satisfy startup visibility. Similarly,
an emitted-chunk-only rate estimator cannot decay while output is stalled.

## Snapshot contract

Add an optional, independently readable statistics handle to `TsSource`, with a
default implementation for existing providers. The handle reads shared metrics
without borrowing or locking the source across `next().await`. `StreamSession`
captures it before starting the pump and uses it for status reads. Sources without
a handle retain the existing cached-statistics behavior.

The Ace handle combines existing atomic counters with bounded rate history and a
small position/buffer snapshot. The startup decorator returns a composite handle:
it reads the underlying source statistics and overlays its reservoir duration
while collecting or draining. It must not recursively acquire the same lock.
All shared-state critical sections are short and synchronous; no network, source
or timer await occurs while holding a metrics lock.

The handle does not retain the source, session, subscription or producer task.
Dropping a source/session must retain existing cancellation behavior. A retained
reader after end-of-stream must report rates decaying to zero, not a permanently
cached positive rate. Metrics must not keep an idle stream alive.

## Rates and counters

Use a nominal five-second rolling measurement window, sampled at one-second
intervals independently of HTTP polling and output delivery. Fixed-capacity history
must bound memory independently of stream duration, packet rate and poll count.
Snapshot calculation takes the current monotonic time so rates expire during
inactivity, including after a sampler stops. Timer-boundary precision is limited
by the sampling interval and documented as such.

Download rate counts contiguous reassembled media payload, excluding piece
signatures and protocol framing, before output gating or channel backpressure.
Count each accepted payload once; duplicate/retransmitted blocks and injected
PAT/PMT/SDT bytes must not inflate the rate. Both single-peer and pooled paths use
the same accounting boundary. Discarded, unauthenticated pieces do not count.

Upload rate follows the bytes accounted by the source's existing `uploaded`
counter, not an estimate from download rate. Preserve existing `downloaded`,
`uploaded`, `peers` and `peers_served` meanings. In particular, do not silently
redefine the delivered MPEG-TS `downloaded` total as the new received-payload
measurement. Document the rate/total accounting difference.

Internally retain integer byte rates and counters. Native `bitrate` is measured
download bytes/second multiplied by eight, saturating on overflow. Compat
`speed_down` and `speed_up` are bytes/second divided by 1,024, retaining fractional
KiB/s where useful. A source with no measured traffic reports zero. Rates do not
come from descriptor bitrate hints. Counter reset/overflow handling must never
manufacture a large positive delta.

## Buffered duration

While the startup reservoir contains media, report its existing measured duration
and draining estimate. Update the shared observation after enqueue, dequeue,
release and discontinuity, including changes made before `next()` returns.
Prefer the existing selected-PCR calculation; retain the existing descriptor-rate
fallback when timing is unavailable. Do not introduce a new release or pacing rule.

After reservoir release, report the contiguous complete media ready at the
reassembler's delivery cursor. A missing piece terminates the measured prefix;
partial pieces and complete pieces beyond a hole do not count. Ready media
removed for delivery no longer belongs to that reassembler estimate. A small
read-only reassembler query may be added rather than consuming media to inspect
it. Reuse timing helpers and document any descriptor-rate duration estimate.

This is a stage-specific observation, not a sum of every allocation in the
daemon: exclude socket buffers, the provider handoff channel, session broadcast
backlog, retained HLS segments and client buffers. Do not double-count the same
media in the startup reservoir and the reassembler. Zero after draining can be
correct even when a player has a large buffer. Reset discontinuous timing epochs
and discarded-media observations when recovery skips or reconnects.

## Live positions and JSON

Extend the internal snapshot with an optional live-position record. Populate it
from existing cursor, advertised window/head and contiguous-ready observations;
do not infer a client's playhead from subscriber activity or elapsed wall time.
Invalid/negative wire positions become unavailable values, never wrapped integers.

Compat `livepos` uses the following documented mapping:

| Field | Meaning |
| --- | --- |
| `pos` | Next piece needed by the provider's reassembly cursor. |
| `last` | Most recently observed provider live head. |
| `live_first`, `live_last` | Available advertised window bounds, when known. |
| `buffer_pieces` | Complete contiguous locally ready pieces at the cursor. |
| `first_ts`, `last_ts` | Null: no verified mapping to engine wall-clock timestamps. |
| `is_live` | 1 for a live source. |

These are provider observations, not authenticated source-clock assertions or
exact AceStream client-playhead parity. Last-known observations must be described
as such; clear unavailable active-window bounds when there is no active window.
Before any position is learned, `livepos` is null. Idle compat responses include
zero speed fields and null `livepos`; they must not start a provider or refresh a
playback lease. Existing invalid-token error behavior stays unchanged.

Native status retains all existing fields and adds the same optional position
record under `livepos`, so operators can inspect the observations behind compat
mapping. Timestamp fields stay null rather than fabricated zero timestamps.
Other optional engine fields such as disk-cache details, selection indices and
progress percentages are outside this issue. Startup state/release-reason fields
are also deferred; the existing operational release event remains available.

## Implementation boundaries

Keep the rate/snapshot helper in a focused engine module. `provider.rs` defines
the optional reader and snapshot data; `session.rs` selects live versus cached
reads. `ace_provider.rs` publishes producer rates and positions;
`startup_buffer.rs` supplies the reservoir overlay. `http.rs` serializes the
native and compat fields. Any `ace-wire` addition only observes reassembler
readiness and remains silent and deterministic.

Update `docs/native-api.md` and `docs/protocol/compat-matrix.md` with units,
sampling, buffer exclusions, nullability and live-position differences. No new
dependency or configurable telemetry service is needed.

## Validation and completion

Use injected/paused monotonic clocks for rate tests: startup activity, a full
window, sustained flow, idle decay, upload-only flow, counter reset and bounded
history. Repeated reads at one time must not change the result.

Use fake sources to prove that session status sees reservoir growth while the
first outer `next()` remains pending, draining decreases the observation, and
discontinuity clears the old timing epoch. Cover ended sources, cancellation and
the fallback for providers without a reader. Test reassembly holes and partial
pieces without consuming or changing the scheduling cursor.

HTTP schema tests cover active/idle compat responses, native status, unavailable
positions/timestamps and existing token behavior. Check the real Ace producer
accounting in both its pooled and single-peer paths, including retransmissions.
Run the pinned-toolchain workspace test, Clippy and formatting gates, the relevant
comparison-harness tests and identifier hygiene once the implementation is ready.

Make one bounded, simultaneous local reference/candidate comparison using a
current public registry source supplied manually through the environment. Use
fresh state, unchanged playback settings, WARP off with restoration, and private
artifacts. Request 90 seconds: retain startup observations, then compare mean
`speed_down` over the predefined final 60-second common window. Report the ratio
and raw numeric summary; do not remove inconvenient samples or repeat to win.
Both sides must have media activity for the comparison to be valid.

The issue's live acceptance is a candidate mean within 20% of the reference.
If the source, unit comparison or measurement window is invalid, record why and
leave that acceptance unverified. Offline correctness or a successful PR cannot
substitute for the numeric live criterion. Never put real content ids, infohashes,
stream names or raw status/media into tracked artifacts or PR text.
