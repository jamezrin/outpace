# Optional shared live MPEG-TS normalization (#49)

Status: the extracted boundary is merged; shared TS output/source, configuration,
HTTP errors and synthetic codec acceptance are implemented in the current draft.
Synthetic codec tests, workspace gates and bounded before/after performance
validation pass. Whole-feature review and its HLS generation-lifecycle fix pass final gates; PR
integration remains pending. Routine design choices are delegated by the milestone goal. Existing
HLS-only normalization is not #49 completion; keep the issue open until every
acceptance criterion is backed by evidence.

## Decision and scope

Use the existing supervised FFmpeg subprocess, one optional source decoration
before StreamSession fanout. Per-client children duplicate CPU and ownership; a
second representation/session map adds unnecessary leases and accounting for this
first fixed target. All viewers share the operator-selected representation.

A separate default-disabled live-TS switch enables normalization. The existing
HLS-only option retains its scope. Both enabled must not create two children.
Disabled live mode returns the original source directly, without executable
resolution, probing, copying, classification or tasks. VOD byte ranges and raw
live defaults stay unchanged. No new listeners, URLs, arbitrary FFmpeg arguments,
video transcode, per-client profile/language selection or library bindings.

The concrete target is selected MPEG audio or AC3 to AAC-LC stereo, 48 kHz,
128 kbit/s, with copied MPEG-2/H.264/HEVC video in MPEG-TS. Reuse existing strict
selection: existing supported ADTS AAC and video-only may pass without a child.
AAC passthrough does not promise resampling. Unsupported layouts fail clearly;
LATM/EAC3/DTS-only and multi-program/multi-track parity are not promised.

## Shared ownership

One helper decorates provider-returned live sources in both manager creation
paths, after existing startup buffering/admission and before session fanout. Do
not wait for media under session-map or finalization locks. The tracked worker
owns the source (and its provider reservation), charged staging, child/pumps and
recovery state. The adapter owns bounded output and latest state, cancellation
and completion handles. Drop transfers cancellation to existing cleanup ownership;
shutdown waits for confirmed completion. Never free a child permit before reap or
pretend a source shutdown panic released its reservation.

Preserve provider metadata and source statistics. Downloaded/rate/live position
remain transport observations, not transformed-output byte counts. Publish source
snapshots when no independent stats handle exists. Transformation state is a
separate bounded latest-value handle, not unbounded history.

## Reusable process boundary

Extract the fixed-profile process core with narrow internal HLS and TS output
adapters. Reuse command validation, pipe-only protocol restriction, four global
child slots, 1 MiB charged input/replay/writer storage, bounded stderr, pumps,
cancellation and confirmed reap. No copied process supervisor or public codec
framework. Preserve existing HLS failure mapping and completed-segment readiness.

Separate pure output packet validation from publication. At most three accepted
188-byte packets arise from one input packet (cached PAT, PMT, opening packet).
Collect a fixed-size batch before publication; asynchronous publication must never
hold a state/publication mutex across channel capacity waits. Preserve generation
fencing and existing HLS callback order during extraction.

TS output chunks are at most 32 KiB, with four queued chunks, plus explicitly
accounted current/held chunks and fixed framing. Byte bounds include sliced backing
owners; message count alone is insufficient. Preserve session service metadata:
the HLS validator currently suppresses SDT because its packager synthesizes it.
The TS sink must have an explicit policy and must not silently drop information
needed by the existing per-client title synthesis.

A full output queue backpressures stdout, stdin and the single upstream producer.
All waits select cancellation. Pause appropriate child-progress clocks during
intentional output backpressure without disabling real stall detection. Slow HTTP
viewers remain isolated by the existing bounded broadcast/keyframe gates.

## Readiness, errors and recovery

TS readiness requires accepted validated PAT/PMT and a clean copied-video opening,
not any stdout bytes or an HLS segment. Keep HLS readiness unchanged. Add an optional
read-only source playback-state handle with default None, delegated by wrappers
and retained by StreamSession. Latest states: classifying, starting, ready,
recovering, failed and stopped, with generation and a stable bounded failure code.
Failure is published before output closes and stopped cannot overwrite failure.

Subscribe/pin before waiting for enabled normalization readiness in native and
compatibility TS responses. Bound/cancel that wait; raw mode adds no wait. Pre-ready
failures return explicit errors (including 503/Retry-After for child capacity).
After headers, failure becomes a body I/O error and inspectable native status;
it cannot retroactively replace HTTP 200. Native HLS must inherit terminal source
failure. Retain bounded terminal status in the ordinary session entry until
stop/reap, without repeated automatic child starts or a permanent error map.

Enabled startup uses one shared bounded FFmpeg capability preflight. Disabled
modes never probe it. Reuse bounded generation recovery: fence old stdout and
queued data, cancel/reap before replacement, fresh tables/opening and one downstream
discontinuity. Preserve the existing three-invalidations-per-minute policy and
absolute startup deadline. Epoch-tagged output discards invalidated queued data.
Both normalization options enabled use the native HLS path on normalized TS,
retaining errors/discontinuities without duplicate classifiers/children.

## Verification and performance

Each slice needs meaningful RED/GREEN, original HLS regressions and independent
review. First extract bounded validation output without changing HLS policy. Then
add the TS adapter and readiness/error handle, manager/HTTP integration and config.

Prove raw byte preservation/no probe/no child; actual MP2 and AC3 synthetic input
producing decodable AAC TS with video copy; AAC/video-only no child; multiple
viewers/support/HLS one provider admission and at most one child; full queue
cancellation; failure/late generation fencing; correct pre/post-header errors;
reap before slot reuse and no abandoned pumps/source reservation. Preserve HLS
readiness, stall, source cleanup and generation controls.

Measure a bounded synthetic throughput/latency workload before/after relevant
process changes and across one/multiple viewers. Record queue/storage/task/process
bounds and distinguish source-limited observations from capacity or smoothness.
Default-path source reasoning is not a throughput measurement. Normal repository
test/Clippy/fmt/hygiene gates and a synthetic FFmpeg decode/manual-player procedure
complete the implementation. No new live internet/content observation is authorized
by this design; use only synthetic media for these proofs.
