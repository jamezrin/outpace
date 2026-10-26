# Reconnect window selection

Date: 2026-10-26. Design only, for `test/reconnect-window-selection-246` at supplied base `82e4d6a`. Do not change that base because PR #280 changes status. Preserve others' existing test edits.

## Contract and evidence

During reconnect selection, a protocol-valid candidate may count toward the usable pool and start the 250 ms selection grace only if the existing continuity predicate accepts its advertised window. With no continuity, initial selection stays unchanged. Per-attempt timeout remains 3 seconds; parallel/active caps, exploration cohorts, cooldowns, sorting, peer wire/authentication and buffering defaults remain unchanged. No new overall recovery deadline, primary-selection policy, or special all-stale recovery strategy.

The supplied RED has prior source output and cursor 9: stale A completes around 50 ms, starts grace and cancels covering B delayed 400 ms. The final assertion finds no selected peer accepted by `Continuity::window_can_resume`. B alone works, and cancelled B is eligible for a later retry. This proves premature same-round selection, not permanent stall, decode failure or deployed causality.

## Interfaces and predicate

Choose a private, copyable value snapshot rather than a callback or borrowed Continuity:

- In `live_session/continuity.rs`, `#[derive(Clone, Copy)] pub(super) struct ResumeCursor { next_needed: u64, head: u64 }`, with private fields.
- `Continuity::resume_cursor(&self) -> ResumeCursor` captures the reassembler cursor and known head.
- `ResumeCursor::window_can_resume(&self, window: &WireLivePosition) -> bool` owns the unchanged predicate; `Continuity::window_can_resume` delegates to it.
- Append `resume: Option<ResumeCursor>` to `connect_pool(infohash: [u8; 20], candidates: &mut SessionCandidates, live_recovery: LiveRecoveryConfig, discovery: &mut Option<DiscoveryRun>, resume: Option<ResumeCursor>) -> Vec<ConnectedUpstream>`. Import the type explicitly from the sibling continuity module.

Exactly preserve both saturating conversions via `i64::try_from(value).unwrap_or(i64::MAX)`. With converted cursor N and head H, accept `window.max_piece >= N || (N > H && window.max_piece >= H)`. Do not add a minimum-window containment requirement: future windows can legitimately force forward resync. Existing wire-window syntax validation stays in `read_peer_window`.

The snapshot is fixed for one awaited selection; that future does not advance continuity. `follow_live_session` in `mod.rs` captures `continuity.as_ref().map(Continuity::resume_cursor)` before its existing cancellation select. All rebuilds with retained Continuity pass Some, including never-emitted continuity; first acquisition passes None. Retain the outer defensive predicate/gossip branch; it must not double-account candidates excluded inside selection. The three existing direct test calls migrate: startup fallback passes None, reconnect A/B and B-alone pass Some.

## Selection and accounting

In the Connected match arm, record the successful handshake timing once. Before pushing into `connected` or creating its deadline, evaluate the optional snapshot. On rejection, call `candidates.explored(addr)` and `candidates.failed(addr, Instant::now())` once, then drop the transport and continue the selection loop. This reuses existing stale-window exploration/cooldown semantics, not a bad-wire classification: do not construct `PeerConnectFailure`, increment handshake/window/timeout failures, or call admitted/productive. Preserve `connected` diagnostics as protocol successes; the returned vector alone means selected usable peers. No new logging is required.

Rejected transports consume no retained selection capacity and do not start, extend or reset grace. Their completed attempt releases its existing slot; pending candidates and discovery retain their ordinary opportunities. Keep the per-round attempted set, so a rejected address is not immediately retried after cooldown. First accepted completion starts grace once; existing active-cap and pending-set termination still apply. No success means no grace clock. A finite all-stale set exhausts normally, returns empty, and leaves the caller's retry/rediscovery policy intact; newly excluded transports are closed rather than held for gossip. The existing outer gossip helper remains unchanged for its defensive path. Do not retain an unbounded stale-socket collection or add inline gossip latency to selection.

## Alternatives and validation

A concrete optional cursor/head pair plus a shared free predicate would also work, but named snapshot fields avoid argument reversal. A generic filter callback adds unnecessary borrowing/interface complexity; post-selection filtering is the demonstrated defect.

Keep the final normative RED assertion text and predicate intact. Separate the initial-None mechanism control from desired reconnect assertions. Required controls: A-only stale/all-stale cleanup and cooldown, delayed B, B alone, first-usable grace, unchanged initial selection, live-edge/future-window predicate equivalence, cancellation, caps/cohort fairness and existing timeout fallback. These are loopback/synthetic controls, not public reliability acceptance.
