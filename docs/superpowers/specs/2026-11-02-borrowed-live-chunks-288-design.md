# Borrowed live chunk inspection (#288)

Status: implemented and independently reviewed; local validation complete. PR/CI integration remains pending.

The pool receives an owned PeerMessage, converts it to an owned LiveChunk payload copy, then immediately lends those bytes to the bounded reassembler. Remove the intermediate allocation/copy with an additive borrowed view. Do not replace the public message enum, ingress buffers, reassembler or authentication policy.

Add LiveChunkRef<'a> with the same piece/header/chunk fields and a borrowed data slice. Add LiveChunk::borrow_from_message(&PeerMessage) returning that view; retain from_message's owned Vec API by copying the parsed view only when ownership is requested. Parsing still accepts only Piece variants with at least ten block bytes; stream index handling and empty-data behavior remain identical. This is framing inspection of untrusted data, not authentication or geometry validation.

The pool's on_piece uses the view only to inspect metadata and feed add_live_chunk. The message owner already lives in that method; no new task, queue, lifetime extension, shared allocation or unsafe code is introduced. Existing cancellation, publication, producer credit and failed-signature handling remain unchanged. Copies into the authenticated piece buffer and cache/wire encoding remain explicit follow-ups; do not claim end-to-end zero-copy.

Before implementation, add contracts for every short-header length, empty data, wrong variant, exact metadata and payload pointer identity. Preserve owned parsing and independent-owned-data semantics. Record a bounded warmed release baseline for the current inspect-and-borrow hot-path use, then paired current-binary owned/borrowed measurements with alternating trial order. Retain historical measurements separately where the original harness was not independently preserved. Measurements must distinguish borrowed inspection from callers that actually require ownership; no live or end-to-end throughput claim.

Run wire and relevant pool/authentication tests and repository gates. Obtain independent review before PR/CI integration. No deployment or new live validation is needed. Keep #288 open for remaining copies and dependency work.
