# Live Recovery Configuration and Reset Behavior - Design

**Date:** 2026-07-07
**Status:** Implemented by PR for review
**Issue:** #68

## Goal

Promote outpace's live lag-recovery thresholds and HLS packaging constants into validated runtime configuration while preserving today's defaults. Also make discontinuous playback recovery cleaner by regating direct TS clients after fan-out lag and regating provider output after unrecoverable live skips.

## Current Shape

The existing seams are suitable for a small, incremental change:

- `Config` in `crates/ace-engine/src/config.rs` already owns operator-facing settings.
- `runtime::config_from_env` parses env vars and rejects invalid values.
- `AceProvider` is configured with builder methods before registration in `build_runtime`.
- `StreamManager` owns the per-session fan-out buffer and the HLS packager map.
- `HlsPackager::start` currently hardcodes `256` packets per segment, `6` retained segments, and `1.0` advertised seconds.
- Direct TS clients have a per-client `KeyframeGate`, but `RecvError::Lagged(_)` currently leaves an already-locked gate in passthrough mode.
- The provider skips evicted live gaps through `Continuity::resume` and `Continuity::skip_evicted_gap`, but post-skip bytes are currently passed downstream after TS resync only.

## Approach

Add two focused config structs under `Config`:

- `LiveRecoveryConfig` for request timeout, stale upstream timeout, request sweep interval, active upstream count, parallel connect count, max piece advance, and max reassembler-ahead pieces.
- `HlsConfig` for HLS segment packet count, retained window, and advertised segment duration in milliseconds.

Both structs use scalar fields (`*_ms`, counts) to keep env parsing and serde simple. Methods convert millisecond fields to `Duration` and seconds as needed by the runtime.

Validation happens once in `config_from_env` after all env overrides are parsed:

- `request_timeout_ms > 0`
- `stale_upstream_timeout_ms > 0`
- `request_check_interval_ms > 0`
- `request_timeout_ms < stale_upstream_timeout_ms`
- `request_check_interval_ms <= request_timeout_ms`
- `max_active_upstreams >= 1`
- `max_parallel_connect >= 1`
- `max_piece_advance >= 1`
- `max_reasm_pieces_ahead >= max_piece_advance`
- `hls.segment_packets >= 1`
- `hls.window_segments >= 1`
- `hls.segment_duration_ms >= 1`

Defaults remain identical to the current constants.

## Runtime Threading

`build_runtime` passes `config.live_recovery` into `AceProvider::with_live_recovery` and creates the `StreamManager` with both `session_buffer` and `hls`.

`AceProvider` stores the policy and passes it into:

- initial connect races (`connect_pool`)
- `follow_live`
- `follow_peer_pool`
- background refill
- peer-exchange harvesting
- `Continuity`

`Continuity` stores the policy because timeout checks, reassembler bounds, and scheduler bounds all belong to per-stream continuity state.

## Reset Behavior

Direct TS responses reset their per-client `KeyframeGate` on `RecvError::Lagged(_)`. A client that outruns the fan-out buffer then waits for the next clean keyframe instead of resuming mid-GOP.

Provider-level discontinuities also reset a `KeyframeGate` held by `Continuity`. The gate is armed when:

- `Continuity::resume` skips because a reconnect peer has already evicted the needed piece.
- `Continuity::skip_evicted_gap` skips an in-session gap that all active upstream windows have evicted.

While armed, provider output passes through that gate before entering the shared session channel. Once the gate locks, it remains in passthrough mode until another skip re-arms it. This keeps all downstream consumers cleaner, including direct TS and HLS, without changing the session message type.

HLS discontinuity tags remain out of scope for this PR because the existing HLS packager has no timestamp or event-boundary model. The source-level keyframe regate is the minimal safe recovery behavior now; explicit `#EXT-X-DISCONTINUITY` should be added with a future segment-boundary event model.

## Env Vars

New env vars:

- `OUTPACE_REQUEST_TIMEOUT_MS`, default `4000`
- `OUTPACE_STALE_UPSTREAM_TIMEOUT_MS`, default `12000`
- `OUTPACE_REQUEST_CHECK_INTERVAL_MS`, default `1000`
- `OUTPACE_MAX_ACTIVE_UPSTREAMS`, default `4`
- `OUTPACE_MAX_PARALLEL_CONNECT`, default `12`
- `OUTPACE_MAX_PIECE_ADVANCE`, default `256`
- `OUTPACE_MAX_REASM_PIECES_AHEAD`, default `512`
- `OUTPACE_HLS_SEGMENT_PACKETS`, default `256`
- `OUTPACE_HLS_WINDOW_SEGMENTS`, default `6`
- `OUTPACE_HLS_SEGMENT_DURATION_MS`, default `1000`

Existing env vars retained and documented with the same defaults:

- `OUTPACE_PREFETCH_PIECES`, default `8`
- `OUTPACE_SESSION_BUFFER`, default `256`

## Tests

Unit coverage should prove:

- Defaults match the old hardcoded values.
- Env parsing populates every new knob.
- Invalid zero values and invalid relationships are rejected.
- `AceProvider` and `StreamManager` receive configured policies.
- HLS defaults and overrides preserve existing behavior unless configured.
- Direct TS `RecvError::Lagged(_)` resets a previously locked gate.
- Provider skip paths arm source-level keyframe regating.

Full workspace tests and clippy should pass before the PR is opened.
