# Persistent peer receive buffer (#288, first increment)

Status: implemented, independently reviewed and merged through PR #315 after successful CI. This increment leaves the other #288 items open.

## Intent and scope

Remove avoidable receive-buffer copying without changing messages, wire framing, cancellation, timeouts or public configuration. Limit this increment to `ace-peer` receive buffering. Preserve verified-store and actor ownership unchanged. End-to-end shared payloads, store/disk changes, startup MPEG-TS scanning and dependency unification are separate work.

## Current behavior and decision

`PeerSession` retains a `Vec<u8>` across reads, decodes one message, drains its prefix, and reads into a zeroed 16 KiB temporary array before copying into the persistent buffer. `PeerMessage::decode` returns owned payloads and rejects declared frame bodies larger than 2 MiB. Buffered fragments survive cancellation of a pending `read_message` future.

Use `BytesMut` for the persistent buffer and Tokio `read_buf` for direct reads. Decode through the existing borrowed-slice interface; advance the consumed prefix only after successful decoding. Keep message payload ownership unchanged. Do not create and immediately discard frozen shared frames: sharing is deferred until message ownership actually changes.

Limit each underlying read to 16 KiB, even when spare capacity is larger. Keep the initial 32 KiB capacity. Compute actual spare capacity as `capacity() - len()` rather than `remaining_mut()` (which includes growable space). `reserve(n)` requests at least `n` additional bytes beyond the current length, not `n` minus existing spare; ensure at least the intended 16 KiB allowance only when actual spare is insufficient; avoid reserving on a path that can return an already-buffered message. Preserve the current declared-frame ceiling and error behavior. No unsafe initialization shortcuts.

An offset into `Vec` would avoid some prefix movement but retain temporary-read initialization and copying. Changing message/store types to shared `Bytes` could remove more copies but expands ownership and retention risks beyond this reviewable increment. `BytesMut` may still compact/reallocate during reserve; do not claim all copying or allocation disappears.

## Invariants

- Byte-for-byte message order and owned payloads remain identical, including keepalive and unknown messages.
- Cancel after receiving a partial prefix/body, then resume the same session: every byte appears exactly once. Exercise both standalone reads and command-versus-read selection.
- Per-read ceiling remains 16 KiB; declared frame-body cap remains 2 MiB (2 MiB + four prefix bytes in the complete frame). A complete maximum frame succeeds; an oversized declaration fails at the next decode without further body reads. The initial coalesced read may already contain body bytes.
- Preserve timeout granularity per I/O await, EOF/error propagation, and any buffered tail for subsequent messages.
- Silent library crates remain silent; no new runtime flags, network behavior or store/actor concurrency changes.

## Evidence and acceptance

Before changing production code, retain deterministic functional guards and a baseline using fixed synthetic frames. Cover every split of a small frame, coalesced frames, partial body cancellation, timeout/resume, EOF, malformed/maximum/oversized frames, and actual command/read selection. Existing vectors and assertions remain intact.

Use a bounded reproducible local benchmark for small coalesced frames, fragmented frames and larger piece frames. Record throughput, read calls, largest requested read and peak visible buffer capacity before/after, sampling capacity before/after reserve and after each read as well as after prefix advancement (which changes visible capacity). Treat visible capacity as a buffer metric, not allocator RSS or total allocated bytes. Record these metrics with build/source provenance. Use a fixed three-run sequence after warmup; report variability. Allocation/copy removal inferred from source must be distinguished from measured allocator totals. Investigate a material regression rather than weakening guards. Benchmarking and compilation wait until the active sustained validation ends.

Run affected peer/wire tests, all-target Clippy, formatting, hygiene and whitespace checks. Independent review covers cancellation, capacity growth, frame bounds and measurement fidelity. Broader tests are justified by changed dependencies/interfaces, not repeated automatically. Merge only after review and CI succeed. No live validation is needed for this internal buffer increment.

## Local validation receipt

Six receive guards passed before and after the buffer change. The workspace passed 1,613 tests with no failures and 8 ignored tests (including the explicit synthetic benchmark). Workspace all-target Clippy, formatting, identifier hygiene and whitespace checks passed. Independent review found no P1/P2 issue.

Three warmed release trials on one host favored the new buffer in all three synthetic cases; read counts, the 16 KiB request ceiling and the measured 32 KiB peak visible capacity were unchanged. The large case uses Unknown messages with piece-sized payloads, not a Piece-codec benchmark. No CPU affinity, statistical significance, allocator-total, RSS or live throughput claim is made. `BytesMut` has a larger fixed handle than `Vec`; visible capacity is not total allocation. Private evidence retains exact timings, source pins and logs.
