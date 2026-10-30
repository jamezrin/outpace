# Piece-bounded pool output and exact bounded statistics

Date: 2026-10-30. Approved implementation contract; issue 287 remains open.

Reserve actual consumer capacity before moving one authenticated ready piece. Preserve the existing bulk extraction API. Continuity owns at most one prepared piece, parser carry, causal producer tokens, a fixed admission fence, and typed effective playback clocks across generation loss. Process at most one preparation and one enqueue per actor turn across prefix and select. Publication gates new extraction independently of older pending media. Peerless provider drainage precedes discovery and resume mutation.

Preserve concatenated media bytes and discontinuity offsets across partitions using the existing resynchronizer and gate loops. Reports describe selected original spans; cached tables do not create producer credit. Credit each contributing piece once on successful causal enqueue. Emitted and downloaded counters advance at enqueue, correcting attempted-send accounting. Enqueue does not establish downstream receipt.

Output hold freezes scheduling, floor/skip/resume and playback budget consumption, while real corroboration, activation, publication, control, reads and cancellation remain responsive. First output has explicit AwaitCorroboration, Armed and Done states. Retained clocks cannot acquire a fresh budget on generation recreation.

Exact metrics use an incremental scanner with at most 2048 packets and 64 piece transitions per actual actor turn. Preserve SelectedPcrClock and independent per-piece alignment. Publish unavailable synchronously on cursor/completion invalidation; partial arrivals, duplicates and head updates do not invalidate. Continue only unfinished useful work when an observer exists. Native buffer_complete distinguishes unavailable numeric zeros from completed empty measurements; compatibility keys/types remain unchanged. Metrics never admit media.

No resource default, verified-store contract or unlimited session default changes. Maximum allowed piece geometry requires measured metrics-enabled backlog latency and operation counts; complexity alone establishes no latency bound.
