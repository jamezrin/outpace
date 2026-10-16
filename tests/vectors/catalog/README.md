# Synthetic catalog response vectors

Both XML files use request nonce 123 and a synthetic transport descriptor.
`response.xml` is signed using the engine's response key and is accepted by the
original 3.2.11 native response verifier. `substituted.xml` replaces the tracker
and updates the transport checksum, retaining the old signature; the same native
verifier rejects it. The descriptor's geometry, name and pubkey are synthetic,
and both tracker hosts use the reserved `example.invalid` domain.

The fixtures are text, including the XML declaration and final newline. Do not
reformat them: the response signature covers those bytes. Protocol note
[62](../../../docs/protocol/notes/62-catalog-response-authentication.md) describes
construction, engine evidence and the embedded-key security limitation. Neither
fixture is a captured public descriptor or contains a real content id/infohash.
