# First usable source-output progress

Design only; awaiting root acceptance before Sol implementation. Base requested: `72f8070`. Read AGENTS.md, the private RED report and the actual modified discovery tests; preserve Luna's existing edits. Routine policy choices below are designer rulings, not evidence of a public root cause.

## Evidence and scope

`ace_provider/discovery_tests.rs::startup_progress_recovery` uses actual loopback open_resolved, generated source signatures, reassembly and aligned TS output. The reported cap-1 advancing-head case fails: A consumes requests, emits no media and blocks B despite a valid 2-second stale configuration. B is published 1.2 seconds after A's first request, beyond lone-peer corroboration. Healthy-A, silent-A recovery and cap-4 **spare-capacity** controls passed. That last control has only one A; it is not evidence about a full default four-peer pool. These are reported results, not rerun in this design assignment.

Fix only active-pool first-output liveness. No whole-open/HTTP/first-decode budget, new environment/default value, catalog/discovery changes, generic framework or extra queue.

## Alternatives and decision

1. Remove every pre-output activity refresh: small, but changes the existing startup-progress rule and does not itself define a full request opportunity around corroboration/activation.
2. **Chosen:** one non-sliding first-output deadline per active-pool invocation, using its existing `stale_upstream_timeout()`. Preserve legacy activity bookkeeping, but it cannot extend this first-output window.
3. Extend on useful verified assembly progress: more state and ambiguous usefulness. Partial chunks are not independently source-verified; verified ahead pieces may never close the output gap. This can still postpone output indefinitely and is deferred.

## Contract

Let T be the configured stale_upstream_timeout; its current production default remains 12 seconds. No new numeric production policy is introduced.

- Scope is a successfully activated pool in follow_peer_pool_with_discovery while `Continuity.emitted == 0` (this session has never emitted). Keep one local `Option<Instant>` for the current invocation, initially None; do not put it in persistent Continuity.
- Arm once at the first control-loop budget check **after initial activation and its scheduling pass have completed and the existing startup_deadline has cleared**. Sample now after any awaited scheduling in that turn. A fresh multi-peer pool arms after initial scheduling; a fresh lone pool first retains the existing up-to-one-second corroboration decision and its release scheduling, then gets a full T. Earlier independent corroboration may open that gate sooner. The timer does not wait for a peer to unchoke or actually answer requests.
- Thus valid T shorter than one second cannot expire solely while requests are intentionally withheld. A lone pool's bound is the remaining existing corroboration opportunity followed by T, measured after activation/scheduling; descriptor/connect/activation I/O time is outside it. Do not validate T against one second or clamp/tune T.
- Additional admissions/replacements, head/window/Unchoke/gossip, duplicate/partial/invalid chunks and even completed verified pieces without output cannot move the armed deadline. Existing retry/reassembly/signature/chunk limits stay intact. No fresh T for each peer.
- Usable output means nonempty aligned LiveOutput from the existing authenticated-contiguous/reassembly/resync path, successfully accepted by the existing output channel. It does not require video, a keyframe, an HTTP body, prebuffer release or decode. Audio-only TS counts. Preserve existing `made_output`/emitted accounting order; do not weaken validation to satisfy the deadline.
- After the first successful output, disarm this deadline and use the existing output-based sliding stale policy. A resumed continuity with any previous emitted bytes never arms it: existing post-output timeout/recovery applies, including its current per-pool last_progress initialization.
- Rebuilding before any output creates a fresh per-pool window after that new pool's activation/gate. A pending original corroboration deadline, if retained in continuity, keeps its existing absolute expiry; do not grant another one-second grace. Rebuilds are not a session-wide startup bound.
- At `now >= deadline` on a pool budget check, use exactly the existing PoolStale teardown, StaleTeardown observation and return path. The follower preserves candidates, marks stalled peers failed through existing cooldowns, and explores retained B normally. Do not bypass active/parallel caps or special-case B.
- Include remaining first-output time in the existing event-wait calculation. Keep the current cooperative loop semantics: already-running activation writes, output backpressure and an event already being processed are not preempted by a new timeout. Check at the next control turn, using a fresh timestamp. This is a non-sliding progress policy, not a hard real-time wall-clock/late-payload rejection guarantee. Preserve the current event/deadline race behavior and consumer cancellation; do not add per-operation cancellation wrappers.

## Minimal implementation interface

Modify only `ace-swarm/src/live_session/{pool.rs,support.rs,mod.rs}` for production. Add a private support helper and import it into the existing module:

```rust
pub(super) fn pool_progress_budget(
    first_output_deadline: &mut Option<Instant>,
    emitted: u64,
    startup_deadline: Option<Instant>,
    last_progress: Instant,
    now: Instant,
    stale_timeout: Duration,
) -> Option<Duration>;
```

`None` means stale. If emitted > 0, clear the local deadline and delegate to unchanged stale_upstream_budget. Otherwise an already-armed deadline returns its strictly positive remainder or None at expiry, independent of last_progress. If unarmed and corroboration is still present, return its remaining duration (zero permitted so the existing release branch runs); otherwise arm now+T and return T. The pool must run its existing corroboration-release/scheduling branch before querying this helper, resampling now afterward. No new public interface, ownership object, clock type or Continuity field. Use the existing std::time::Instant for production and explicit instants for pure unit controls.

Replace the current stale_upstream_budget(...).or_else(startup_deadline...) calculation with this helper. Preserve the existing teardown block, initial scheduling and all corroboration-release branches. Budget resampling must not retroactively change their decisions. Keep should_refresh_stale_deadline unchanged, including its true startup-activity unit assertion; that assertion now describes sliding bookkeeping, not permission to evade the independent bound. Add a combined control proving chatter changes last_progress but not the first-output expiry.

## Required evidence and risks

Keep the four existing startup_progress cases. Add a genuinely full four-A pool before asserting a default-capacity counterpart: barrier all handshakes/windows within initial selection grace, observe four successful activations/peer count, and publish B only afterward. Do not require all four peers to receive a piece when the advertised scheduling window may not contain four assignments. Use the same valid accelerated 2-second test stale interval; this proves default **capacity**, not default 12-second startup timing.

Add healthy-A just-before-bound, low-T corroboration, head-chatter-after-gate, first-output-to-post-output and resumed-continuity controls, authenticated video/audio output, and drop-before-expiry cleanup. Extend no other liveness policy. A legitimate peer that cannot produce usable output within configured T can now lose the whole pool even while making assembly progress; this is the intended bounded tradeoff. A newly admitted peer near expiry inherits the remainder. Existing cooperative I/O can overshoot the logical deadline; such stalls require separate evidence/fixes. The linked one-task plan lists exact tests and future commands.
