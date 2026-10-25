# Bounded ACE handshake reply diagnostics

Approved in chat on 2026-10-25 for implementation through merge. This diagnoses the remaining #253 reply deadlines; it does not change connection policy or establish their public-runtime cause.

## Selected approach

Add `HandshakeProgress::{Sent, ReplyBytes { received: usize }}` and `PeerSession::perform_handshake_with_progress_observer(infohash, peer_id, impl FnMut(HandshakeProgress)) -> Result<Handshake>`. Preserve both existing public handshake APIs and genuinely FnOnce-only callbacks. Sent occurs once after the complete local write. ReplyBytes is ACE-only, cumulative, strictly increasing, and bounded by the fixed 66-byte reply. The standard BitTorrent/VOD branch retains its existing reader and sent callback.

A private borrowed AsyncRead adapter counts successful ReadBuf growth synchronously during polling around the existing read_exact. Keep one existing operation timeout around the entire reply, without resetting it on progress. This retains consumed partial-byte counts in the caller-owned progress record even if the outer three-second future is dropped. It does not make resuming a canceled read safe. A manual counted read loop was considered, but would replace existing EOF/read_exact behavior unnecessarily.

Keep libraries silent. In swarm diagnostics retain an optional reply snapshot: absent before Sent, zero bytes after Sent, updated scalar count during reading. Classify existing typed results into `valid`, `timeout`, `eof`, `reset`, `io_other`, `decode`, or `hash_mismatch`. Closed maps to EOF and Protocol to decode if required by exhaustive matching. An OS I/O timeout remains io_other. Preserve existing stage categories. An outer reply deadline finalizes pending cause as timeout; a later window timeout must retain valid basic-reply state. Complete bytes alone never imply validation succeeded.

Append `reply_bytes`, `reply_state=zero|partial|complete`, and `reply_cause` to existing completed-attempt lines only when a snapshot exists. Add fixed 7-by-3 cause/state aggregate counters with nonzero fixed keys such as `reply_timeout_partial`. Do not redefine attempted/stage totals, emit per-read logs, retain histories, or expose payloads, endpoints, identifiers, arbitrary errors or private URLs.

## Constraints and validation

No changes to wire bytes, handshake order, 66-byte ACE or 68-byte standard BT framing, three-second overall deadline, operation timeouts, fairness, scheduling, verification, public status schema, configuration or dependencies. Keep PR275's state preservation intact. Parent task abortion remains outside completed-attempt accounting.

Luna tests actual reader/cancellation behavior with synthetic duplex or scripted transports: silence; seven-byte prefix then outer/inner timeout; partial EOF; reset and other I/O; complete malformed reply; valid-format wrong hash; success followed by a framed message. Assert exact bytes and original errors, no overread, fixed cause and unchanged stage totals. Verify legacy FnOnce compatibility, blocked-write omission, valid reply retained on window timeout, standard BT behavior, count bounds and aggregate one-count-per-completed-outcome.

Sol implements and reviews; Luna writes and runs all tests/checks/hooks. All live identifiers/captures remain private. A later public investigation, if needed, uses the diagnostic without claiming that synthetic tests resolve #253.
