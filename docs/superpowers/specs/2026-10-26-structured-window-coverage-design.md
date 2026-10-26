# Structured window coverage design

## Intent

Fix the reproduced retained-producer gap in #246/#195: a supported structured live-window update can advance one producer's advertised coverage while another peer has already established the same or a higher shared head. The stats observation currently sees the update, but the scheduler never requests the newly covered piece from that producer.

## Design

The user has accepted this minimal design and authorized autonomous implementation; there are no open material choices or separate approval gate. This handoff recovers an existing partial implementation after a capacity failure, preserving valid production changes and Luna's test edits.

Keep `LiveWindow::from_myinfo_payload` as the sole structured-window parser and retain its current acceptance rules, including prefix parsing with trailing bytes, non-negative maximum, partial fields, and the existing `u32::MAX` maximum bound at the live-session seam. Read a validated maximum independently of the shared head. In both `Extended` and otherwise-unhandled `Unknown` message paths, advance the shared head monotonically and update the sending peer's scheduler coverage when either the shared head or that peer's maximum advances. Run the existing request advancement and loss accounting after such progress. Keep `advance_head_from_window` available for its existing callers and tests.

An identical or lower maximum for both shared head and sending peer does no work. Valid structured messages in the `Unknown` path remain recognized even when only peer coverage advances; the existing head-advance log retains its meaning and format. This change does not alter minimum-window handling, message-ID interpretation, Bitfield handling, deadlines, budgets, defaults, authentication, fairness, caps, gates, or log schema.

## Approach and handoff

Use the accepted parser-helper extraction and existing scheduling path. Passing a fake shared head into the old helper would obscure validation; changing the wire parser or minimum-window policy would exceed this fix. Retain the old head-advance helper and its existing test interface under `cfg(test)` once production callers use the independent maximum reader.

This implementation handoff runs no tests, builds, formatting, checks, Git mutations, network activity, or subagents. Luna owns GREEN and any additional controls, followed by fresh independent review. Source self-review is not a passing test result.

## Evidence and limits

The retained-producer loopback RED demonstrates a valid `Extended` update from 8 to 10 after another peer has established shared head 10 and cursor 9: stats observe it, yet no piece-9 request or media follows. Existing id=4 same/lower-head and structured new-head controls pass. The fix addresses this local handler gap; it does not establish a cause for any public or deployed playback plateau.
