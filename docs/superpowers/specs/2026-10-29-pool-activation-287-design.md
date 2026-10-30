# Pool activation responsiveness (#287)

Status: implementation locally tested. Workspace, clippy, formatting and hygiene pass. Independent review, commit and CI pending; #287/v0.9 remain open.

The binding root rulings in the private implementation evidence select one persistent actor-owned write future. Preparation signs the existing handshake using the at-start cursor/head policy. The future owns the candidate and messages, borrowing neither identity nor mutable pool state. Initial setup retains an awaiting compatibility wrapper. Actor refill starts a job without awaiting transport I/O; refill consumption pauses while occupied. Poll completion before closed-peer prefix processing and in select. Consumer cancellation drops the socket with the actor.

Commit is synchronous and follows successful writes only. Recheck current coverage, duplicate address, exploration cohort, capacity and startup corroboration, and exact replacement ID. An obsolete candidate closes without admission counters or retiring a provisional. Failed writes retain exploration/loss accounting. Every connection budget subtracts the held activation slot in addition to gossip. No new defaults, timeouts, dependencies, wire policy or output backpressure changes.

Interface choice: prepared activation returns the owned ConnectedUpstream on success; actor metadata retains peer ID and optional exact replacement ID. Monotonic peer IDs distinguish replacements even at identical addresses. Commit shares the initial-setup accounting implementation.

The echoed public-IP observation occurs exactly once during preparation, before writes can fail or cancel, preserving existing policy. Completion also checks consumer closure immediately before admission. A replacement is admissible only when current capacity is exactly full, never already exceeded.

Completion retains an exact provisional that caught up during the writes and releases the matching-window hold. A planned replacement whose pool now has spare capacity becomes obsolete; it does not retire an unnecessary provisional or change the original admission plan.

Completion explicitly reconsiders refill work after releasing its held transport slot, since the earlier reservation decrement may already have been observed on a busy turn.
