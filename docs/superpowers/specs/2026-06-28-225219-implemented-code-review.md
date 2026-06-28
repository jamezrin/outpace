# Implemented Code Review

**Date:** 2026-06-28 22:52:19 Europe/Madrid
**Scope:** Rust workspace implementation in `crates/{ace-wire,ace-tracker,ace-peer}`.
**Review mode:** Behavioral/code-risk review of implemented code, not protocol docs.

## Summary

The implemented crates are small, readable, and aligned with the documented architecture:

- `ace-wire`: bencode, handshake, peer-message framing, extended handshake parsing,
  infohash math, transport descriptor decode.
- `ace-tracker`: BEP-15 UDP tracker codec and async announce.
- `ace-peer`: async peer session over generic streams plus TCP connect helper.

The main risks are early-stage network-protocol hardening gaps: unbounded peer frame
buffering, permissive malformed-message acceptance, insufficient descriptor validation,
and missing peer I/O timeouts.

## Findings

### 1. Unbounded peer frame buffering

**Severity:** High

`PeerSession::read_message` repeatedly appends to `self.buf` until
`PeerMessage::decode` returns a complete frame. `PeerMessage::decode` trusts the
incoming 32-bit length prefix and returns `Ok(None)` until `buf.len() >= 4 + len`.

A hostile peer can advertise a huge frame length and keep the connection open, forcing
memory growth until the process is under pressure or killed.

**Locations:**

- `crates/ace-peer/src/session.rs:40`
- `crates/ace-wire/src/message.rs:55`

**Recommendation:**

Introduce a protocol maximum frame size and enforce it before buffering more data.
Apply the check in `PeerMessage::decode` and/or in `PeerSession::read_message`.
For current BitTorrent-style traffic, a conservative default can be based on the max
block size expected for piece messages, with room for extended handshakes.

### 2. Fixed-size peer messages accept trailing bytes

**Severity:** Medium

The peer message decoder accepts malformed frames where fixed-size messages have extra
payload bytes:

- `choke`, `unchoke`, `interested`, `not_interested` should have zero payload bytes.
- `have` should have exactly 4 payload bytes.
- `request` and `cancel` should have exactly 12 payload bytes.

Today the decoder reads the fields it needs and ignores trailing bytes. That makes
malformed peer traffic indistinguishable from valid protocol events.

**Location:**

- `crates/ace-wire/src/message.rs:62`

**Recommendation:**

Validate exact payload lengths for all fixed-size messages. Keep variable-size handling
only for `bitfield`, `piece`, and `extended`.

### 3. Transport descriptor numeric fields are not validated

**Severity:** Medium

`piece_length` and `chunk_length` are parsed as signed bencode integers and cast to
`u64`. Negative values become very large unsigned values, and missing or invalid fields
silently default to `0`.

This can later break piece selection, request sizing, cache allocation, or arithmetic
in `ace-swarm`/media assembly.

**Location:**

- `crates/ace-wire/src/transport.rs:101`

**Recommendation:**

Reject negative numeric fields with `try_into` or explicit range checks. Treat required
transport fields as decode errors instead of defaulting to `0`, at least for fields
needed to request pieces safely.

### 4. Tracker announce reports `left = 0`

**Severity:** Medium

`build_announce_request` currently sends:

- `downloaded = 0`
- `left = 0`
- `uploaded = 0`

In BEP-15 terms, `left = 0` advertises the client as complete/seeding. That is
misleading for the current client before piece download/upload exists and may affect
tracker behavior or peer expectations.

**Location:**

- `crates/ace-tracker/src/codec.rs:42`

**Recommendation:**

Make transfer counters caller-provided. If the stream size is unknown for live content,
represent that choice explicitly in the API and confirm the exact value expected by
Acestream trackers during live interop.

### 5. Peer TCP operations have no timeout

**Severity:** Medium

`connect`, `perform_handshake`, and `read_message` can wait indefinitely on slow or
stalled peers. This will matter once swarm orchestration opens many concurrent peer
attempts.

**Locations:**

- `crates/ace-peer/src/session.rs:23`
- `crates/ace-peer/src/session.rs:59`

**Recommendation:**

Add configurable timeouts around TCP connect, handshake read/write, and per-message
read loops. Keep the generic `PeerSession<S>` testable by applying timeouts in a
wrapper/helper layer or by accepting timeout configuration on session methods.

## Tooling Notes

`cargo test` passes outside the sandbox:

- `ace-peer`: 2 passed, 1 ignored live-network test.
- `ace-tracker`: 6 passed, 1 ignored live-network test.
- `ace-wire`: 10 unit tests passed.
- `ace-wire` vectors: 8 passed.

The first sandboxed `cargo test` run failed because the sandbox blocked a local UDP
socket bind in `announce_against_local_fake_tracker`; rerunning outside the sandbox
passed.

`cargo clippy --all-targets --all-features -- -D warnings` currently fails on one
style lint:

- `crates/ace-wire/src/transport.rs:78`: replace `body.len() % 16 != 0` with
  `!body.len().is_multiple_of(16)`.

## Resolution (2026-06-29)

All findings verified against the code and addressed via TDD; `cargo test` and
`cargo clippy --all-targets --all-features -- -D warnings` are both clean.

| # | Finding | Commit | Notes |
|---|---------|--------|-------|
| 1 | Unbounded frame buffering | `feb8f86` | `MAX_FRAME_LEN` = 2 MiB, rejected on the length prefix. |
| 5 | No peer I/O timeouts | `7cee478` | `DEFAULT_PEER_TIMEOUT` (20s) wraps connect/handshake/send/read; `PeerSession::with_timeout`. |
| 2 | Trailing bytes on fixed-size msgs | `36771c5` | Exact payload-length checks for choke/unchoke/interested/not-interested/have/request/cancel. |
| 3 | Unvalidated descriptor numerics | `311fb21` | `piece_length`/`chunk_length` now required and strictly positive. |
| 4 | Tracker `left = 0` | `c6c66b3` | Counters caller-provided via `TransferState`; all-zero default kept (validated live) with a doc caveat. |
| — | Clippy `manual_is_multiple_of` | `ca1913f` | Fixed both occurrences (the review only spotted the first). |

## Suggested Fix Order

1. Add max peer frame size enforcement and tests for malicious oversized length
   prefixes.
2. Enforce exact peer-message payload lengths and add malformed-frame tests.
3. Add timeouts to TCP connect/handshake/message reads.
4. Validate transport descriptor required fields and numeric ranges.
5. Rework tracker announce counters into explicit caller-provided state.
6. Fix the Clippy lint and run `cargo test` plus Clippy again.
