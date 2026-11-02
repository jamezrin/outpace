# Lazy wire fallback decoding (#288)

Status: decoder and encoder implemented, tested and independently reviewed; PR/CI integration pending.

Typed peer decoding currently evaluates `Unknown { payload: p.to_vec() }` eagerly before `Option::unwrap_or` chooses a typed message. Avoid this discarded allocation/copy with lazy fallback construction. Preserve the public owned PeerMessage API, exact decoded values, frame lengths, incomplete/oversized behavior, and unknown/malformed payload ownership. No network, configuration, dependency or authentication changes.

Approaches: a lazy fallback is the smallest semantic-preserving correction. Rewriting the decoder match to return concrete messages also works but unnecessarily expands the diff. Shared Bytes messages require broader ownership and compatibility work and are deferred. Selected by the implementer under routine design delegation: lazy fallback.

A dedicated integration-test binary uses a thread-local allocation-request counter forwarding to System. Only synchronous decoding is measured; fixtures and assertions stay outside the counting scope. Prove zero allocation requests for fixed-size typed messages and at most one owned payload allocation for nonempty typed Piece/Bitfield/Extended messages, while unknown and malformed frames retain their payload allocation and exact bytes. Count allocations, not inferred RSS or end-to-end performance. Test allocator unsafe operations only forward the original pointer/layout to System; production adds no unsafe code.

First observe the allocation contract fail on the eager implementation. Then make fallback lazy and run wire/vector tests, workspace Clippy, formatting and hygiene. Independent review covers decoder semantics and allocator measurement isolation. Keep #288 open for remaining copies and dependency work.

## Single-buffer encoding extension

The existing encoder builds a body Vec and then copies it into a second prefixed Vec. Compute the exact body length from the variant and allocate the final prefix/body buffer once, preserving all message bytes and the existing u32 length-prefix conversion. No public encode API changes. All variants need independent golden header/payload checks, exact-length allocation and decode roundtrips. Use the same isolated thread-local counting test binary, with input setup outside measurement. This extends the unpublished wire-allocation increment; no deployed behavior/default changes.
