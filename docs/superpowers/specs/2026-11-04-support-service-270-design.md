# Autonomous MPEG-TS support service (#270)

This document defines the staged design for Post-1.0 milestone issue #270. The merged baseline proves
signed acquisition and verified reseeding with a finite service-owned receiver;
it does not supply daemon configuration, restart or stop ownership. Root selected
the following design under delegated routine design authority.

## Goal and boundary

A daemon follows explicitly configured public ACE live sources and serves verified
pieces to peers without local playback. Real viewers may join the same provider
resource. Viewer counts remain actual viewers. Preserve protection/provenance,
inbound caps, cache bounds and unlimited-by-default shared session admission.
No private-peer support, HLS transport service, UI, transcoding, SRT or official
engine parity is introduced. Existing HLS/native viewers still share live sources.
No live Internet observation is necessary to establish the synthetic contract.

## Approaches

A direct provider worker per declaration duplicates downloads and bypasses manager
admission/sharing. A fake viewer obscures ownership and viewer accounting. Retaining
an Arc alone lets the reaper remove the map entry while the old pump stays alive,
so a later viewer can start a second download. Reject all three.

Use explicit headless pins on manager-owned live generations, with cancel/join
ownership separate from media handles. Reuse the existing session pump, which
already pulls its source with zero subscribers. Do not create a discard receiver
or another media queue. A store-only actor could avoid parsing but would duplicate
cursor/recovery invariants; defer it unless measurements justify a separate change.

## Deliver in reviewable stages

1. Own live source/session cancellation and completion through the provider wrappers.
   Establish stop/join with retained media handles and provider capacity invariants.
   This stage has its own tests/review/PR; #270 stays open.
2. Add bounded configuration, manager headless pins and daemon supervisor/controls.
3. Complete synthetic source/service/leecher validation, measurements and operator
   documentation; close #270 only when all acceptance evidence is retained.

No stage may claim the service exists merely from a reaper pin or detached drain.

## Stage 1: source and session lifetime

Add explicit source shutdown to `TsSource`; a stateless source may use a no-op
implementation. Every owning wrapper must forward it. `StartupBufferedSource`
and admission `Source` cannot hide the inner shutdown. `AceSource` must retain its
follower/announce owner handle and close its output receiver before joining that
owner; sampler cleanup remains owned. Source opening cancellation before a source
is returned must not orphan a newly spawned worker.

`StreamSession` owns a cancellation signal and one pump completion, independent of
external Arc references. A stop cancels pending `source.next`, terminates output,
finishes source shutdown, destroys the source and its admission reservation, then
publishes completion. Concurrent stop/shutdown callers share the same completion.
Caller cancellation must transfer the join to manager-owned cleanup, not detach it.
Do not hold manager/session locks while awaiting provider work or cleanup.

Retained handles must not keep an explicitly stopped pump alive. This strengthens
force-stop completion, without weakening #286: a reservation stays held through
pending cancellation and actual source destruction, not merely map removal. The
existing retained-pump test must be migrated with an explicit paused-cleanup witness,
not replaced by an immediate-zero assertion. Retained VOD contents/ranges keep their
existing independent ownership semantics and are out of scope.

Already admitted peer readers may own immutable store snapshots after a producer
lease disappears. Join and report the owned outbound follower and its workers;
do not claim producer removal revokes independent inbound readers or completion of
unrelated store work. Audit `shutdown_peer_runtimes` (currently abort without await)
before treating follower leader completion as descendant completion. Retain worker
joins in the existing owner/cleanup path; no unbounded new cleanup queue. Never
claim successful cleanup merely from aborting a task. If cleanup cannot finish,
keep completion pending and capacity held; shutdown must expose that condition
rather than report success. Existing dependency I/O deadlines bound normal paths;
do not invent a timeout that silently frees uncertain work.

The terminal output contract must reach subscribers already waiting and late
subscriptions on retained handles. Keep public subscription APIs where practical;
choose a bounded terminal state/notification rather than keeping an open sender
with no pump. EOF follows cancellation; no buffered bytes are published afterward.
An explicit new start may create a new generation only after the old ownership
transition is fenced, with capacity still enforcing overlapping cleanup.

## Declarations and inputs

Add `OUTPACE_SUPPORT_SOURCES`, a JSON object of stable operator names to existing
ACE live selector strings. Default unset is empty/off. Maximum encoded input is
64 KiB; at most eight declarations; names are 1–64 ASCII alphanumeric, underscore
or hyphen, selectors are nonempty and at most 4096 bytes. Reject duplicates and
invalid selectors transactionally before identity/cache/workers. Reuse current
provider/CLI selector grammar and resolution protection checks; do not add request
filesystem access or read the private source registry. VOD is rejected, not streamed
as a support source. Persist declarations only in operator configuration, never
transient workers or source selectors in daemon state files.

This service is public-mode only. Reject support configuration when inbound serving
or seeding is disabled, and when private peer policy is selected; do not silently
widen settings. Inbound limits, peer-listen binding, cache configuration, tracker/DHT
and public-reachability behavior remain existing explicit daemon controls. Public
catalog, metadata and peer resolution use the existing authenticated paths.

## Shared ownership and generation algorithm

The manager tracks headless pins separately from subscribers/HLS. Pin acquisition
uses the existing per-key start lock and provider admission, rechecking shutdown
before and after awaits. A same-key running viewer/HLS source is reused at capacity;
two declarations for the same key hold two logical pins on one provider. Reaper
retains a live generation while any headless pin exists. Releasing a pin never
cancels other pins/viewers/HLS. Release with no remaining owners starts normal
owned teardown without an artificial viewer or indefinite cache entry.

Pins identify the exact session generation. Dropping/releasing an old pin cannot
remove, stop or mutate a replacement. Source completion ends that generation and
notifies its pins. There is no hidden always-ready polling loop.

Each declaration owns a desired-running flag, monotonic generation, supervisor
control and at most one pin/open/backoff future. Supervisor states are `starting`,
`running`, `retrying`, `stopped`, `error`. Capacity/network failures retry with
1-second initial exponential delay capped at 30 seconds. Reset backoff only after
30 seconds of healthy source progress; opening alone does not reset it. Permanently invalid/protected selectors enter `error` until explicit start/config
restart. Retry classification must carry structured cause: existing `Unresolvable`
and `Backend` text also wraps transient catalog and discovery failures, so the
supervisor must not classify by message text or blanket error-variant matching. No overlapping attempts for a declaration. Cancel a pending open or sleep
before joining; no delayed publication after stop. Healthy progress is source
acquisition/output statistics, not decoded playback or readiness.

Explicit manager force-stop disables support desired-running for that key's current
service generations, as well as stopping the shared stream. It must not immediately
restart behind the operator. Other declarations/keys remain live. A new ordinary
viewer request can start a stream but does not reenable stopped support ownership.
An explicit support start reenables it. A transient source EOF permits supervised
retry; existing viewers of the ended generation observe EOF and reconnect normally.

## Runtime and controls

Bind listeners successfully before admitting configured support supervisors. Runtime
owns the service; startup failure and shutdown first close new support admission,
then cancel opens/retries/pins and join workers outside lifecycle locks. Manager
shutdown is the final fence and cleanup owner. No daemon worker may depend on a
local media request. A failed source must not block other declarations or controls.

Native controls: `GET /support` returns declaration name/state/generation, static
reason code and numeric progress/ownership fields; never raw selectors, descriptor
paths or credentials. `POST /support/{name}/start` and `/stop` operate only on declared
names, with an empty body. Unknown names return 404; accepted/idempotent operations
return their current state. Pending stop must not be labeled complete before its
owned join. Do not add arbitrary remote source creation or change existing bind/auth
privileges. Status says support enabled/running, not additional viewers or readiness.
No new UI or compatibility API is required.

## Resource and performance requirements

Eight declaration controls and their bounded map are the only new service-level
retention. No new payload queue, duplicate provider source, per-frame allocation,
periodic busy polling, reaper self-Arc cycle or unbounded cleanup backlog. Reuse
existing verified piece-store and recovery limits. Shared admission includes pending
opens and retained cleanup; unlimited default stays unlimited. Evaluate parser CPU,
RSS/store retention, FD/thread/task and cancellation latency in bounded synthetic
comparisons against the merged baseline. Separate measured results from structural
reasoning; do not claim parser avoidance or official interoperability.

## Required tests and completion evidence

- Source/session cancellation while next/open/cleanup is paused; retained handles;
  concurrent stop/shutdown; terminal output and no late generation publication.
- Pause actual cleanup: capacity remains held; completion/release frees it once,
  and retained VOD-range behavior is unchanged. Caller cancellation cannot lose joins.
- Zero-viewer support survives reaper; viewer/HLS join shares one provider slot;
  two declarations share one source; stopped/old pins cannot affect replacement.
- Retry deadlines/backoff/reset, capacity refusal, permanent refusal, force-stop
  suppression, config parsing/conflicts, and independent declaration progress.
- Actual signed source → daemon-owned support → separate leecher, exact whole-piece
  bytes/header/signature, verified-only serving before decoder readiness, no viewer.
  Stop/EOF and producer/worker cleanup checked before assertions, with independent
  admitted snapshot lifetimes explicitly controlled.
- Bounded source restart/cache lifecycle, inbound limits, control status and task/
  allocation/resource measurements with no unsupported sustained/readiness claim.
- Independent architecture/correctness/performance/test-fidelity review and repository
  gates, CI and merge for every stage. Operator guide documents public topology,
  peer ports/discovery, persistence, cancellation, cache and interoperability limits.

## Stage-1 failure and operator contract

Output closes independently of source teardown, so clients can drain already
published backlog and reach EOF while cleanup remains pending. Successful stop
requires confirmed owned cleanup. Native DELETE returns 204 for confirmed stop,
404 for absence, and 500 with a generic failure message when cleanup is unknown.
It never exposes a panic payload or private source details.

A source shutdown panic retains the complete admitted source owner in the
manager's failed-owner collection. Its admission remains occupied for that
manager's lifetime; a failure does not create reusable capacity. Arbitrary
partially executed shutdown is not retried automatically. Manager shutdown
returns an unsuccessful outcome, and operational logs identify the retained
reservation. Destroying a failed manager eventually drops these owners; this
fallback is not evidence that their descendants were successfully joined. An
operator must treat it as a failed shutdown requiring intervention, rather than
readiness or a successful clean stop. The collection adds no payload queue or
per-failure worker and is bounded by admitted resources when a cap is configured.
The unlimited default remains unchanged.
