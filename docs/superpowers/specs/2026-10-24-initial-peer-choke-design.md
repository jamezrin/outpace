# Initial peer choke-state preservation

Approved in chat on 2026-10-24 for implementation through review, validation and merge.

The production follower loses an Unchoke consumed before the initial live-window message. A paired loopback reproduction observes a real piece request for window-then-Unchoke, but none for Unchoke-then-window. This establishes an ordering defect, without establishing the public relay's ordering or explaining basic-handshake reply deadlines.

Preserve the latest Choke/Unchoke state while reading the initial window, carry it in the connection result, and initialize incoming request eligibility in the scheduler during successful activation, while retaining the independent default for our reciprocal-Unchoke runtime flag. Default to choked if neither message arrived. Subsequent control messages continue through the existing worker/event path. State is connection-local and cannot leak across reconnects or peers.

## Approaches

The selected approach retains one Boolean state, with the last observed control message winning. Replaying all preceding messages would broaden ownership and buffering contracts; changing timeout or scheduling policies would not repair the reproduced loss. Neither alternative is included.

Window parsing, message-count bound, deadlines, ranking, fairness, verification, activation writes, replacement behavior, and cancellation ownership remain unchanged. No new dependencies, configuration, public API, logging framework, or network defaults.

Validate real request eligibility for window-first and early-Unchoke orderings; no request for no control or Unchoke-then-Choke; request for Choke-then-Unchoke; later normal control handling remains effective. Keep public identifiers, media and raw run notes private. This bounded fix does not close the broader #253 investigation by itself.
