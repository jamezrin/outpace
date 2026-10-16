# 62 — Catalog response signatures and request nonces

Date: 2026-10-16. Issue: #178; related content-id derivation work: #177.

## Finding

The official Linux engine 3.2.11 has a response verification step that outpace's
catalog resolver omitted. A checksum supplied alongside the transport detects
corruption; it is not the engine's complete authentication mechanism. The
request signing described in note 45 is separate from response verification.

The engine's `CoreApp.TSService.check_response` parses the XML document and
requires a `response` root with a `sig` attribute. Its signature formula is:

```text
unsigned = response_text.replace(' sig="' + signature + '"', '')
expected = SHA1(UTF8(unsigned) + TSService.get_key(2)).hexdigest()
require expected == signature
require integer(response_root.r) == request_random
```

`get_key(2)` returns a 116-byte protocol constant distinct from the request key
(`get_key(1)`). Neither is a server-only secret. As with the pre-existing request
key and transport AES key, outpace includes the response constant for protocol
interoperability. XML declarations, whitespace and trailing newlines participate
in the hash; parsing and reserializing the document would change signed bytes.
The engine removes the literal double-quoted signature attribute, including its
preceding space, rather than hashing canonical XML.

## Evidence and reproduction

Inspection used the locally available official engine image in a disposable
container with `--network none`. Engine code and decompiler output remained in
private scratch space outside the repository. No registry, public stream,
network capture or running viewer was needed.

In the inspected `CoreApp.so`, `TSService.get_key` is at ELF-relative `0x12e530`
and `check_response` at `0x5599a0`. These offsets are build-specific, not public
interfaces. Runtime initialization resolves the Cython vtable; local disassembly
and decompilation show signature removal, SHA-1 with key mode 2, signature
comparison and optional request-random comparison. The catalog caller supplies
its request nonce to this verification step.

Calling that actual native verifier under the Python runtime, with its GIL held
and a Cython optional-argument structure containing request nonce 123, produced:

- Correctly signed synthetic XML: accepted as a DOM document.
- Modified body retaining its old signature: `Bad signature`.
- Correctly signed XML with request nonce 124 instead: `Bad random`.
- Missing signature: `Missing signature`.
- Forty-zero signature: `Bad signature`.

The committed `tests/vectors/catalog/response.xml` contains an entirely synthetic
transport (name `Synthetic`, piece length 65536, chunk length 16384, bitrate
1000000, synthetic pubkey, and a tracker under `example.invalid`). The companion
`substituted.xml` changes its tracker and updates its checksum while retaining the
original response signature. The original native verifier accepts the first
fixture and rejects the second with `Bad signature`. Their literal signature and
checksum values are vouched for as synthetic digests by the hygiene allowlist.

To reproduce the vectors, bencode the synthetic descriptor, wrap it using the
existing transport AES key/IV and version 2 header, base64-encode the resulting
bytes, and place their SHA-1 in `checksum`. Sign the exact XML bytes using the
formula above. For the negative vector, change the tracker, re-encode and update
its checksum, but keep the old signature. This requires no real identifiers.

## Outpace behavior

`fetch_catalog_transport` carries its generated request nonce into response
verification. The entire document must parse, with DTD/entity expansion disabled
and a 4096-node limit within the existing 1 MiB response bound. A valid response
root, canonical signature attribute, signature and matching nonce are required
before extracting or decoding any transport. Missing or duplicate fields, nested
transport/checksum elements, malformed XML, wrong signatures and replayed nonces
fail closed for that catalog attempt. The existing catalog-host fallback remains.
Checksum validation still runs after authentication.

The local HTTP regression exercises the actual fetch path: a fake catalog reads
the generated nonce from the request and returns a signed response using either
that nonce or a different nonce. Only the matching response is accepted. The fixed
engine-checked fixture independently pins the key and signature algorithm, rather
than deriving every positive expectation from outpace's signer.

## Security limits and scope

This matches the observed engine verification mechanism; it is not proof that
only the catalog operator could have generated a response. An adversary who
extracts the shared response key can forge both the body and signature and echo
an observed nonce. SHA-1 and plain HTTP remain protocol limitations. No TLS
endpoint or certificate deployment was established by this offline investigation,
and HTTPS availability is not ruled out. No transport encryption, tracker
policy, bind address, cache path or environment setting changes here.

The response nonce binds a reply to an outstanding request. It does **not** prove
that the transport derives the requested content id. Public catalog content ids
must not be assumed to equal the SHA-1 of the transport bytes; that unresolved
question is tracked in #177. As updated by [note 63](63-bep9-content-id-policy.md),
catalog failure returns an error by default. Raw transport-hash BEP-9 fallback requires
`OUTPACE_BEP9_CONTENT_ID_FALLBACK=1`, and its descriptors remain outside the shared
catalog-provenance index. The daemon's own broadcast ids resolve through a separate
trusted local broadcast registry without this opt-in. A stronger server trust anchor or
verified content-id binding would be needed to remove the remaining forgery risk. The question of
third-party catalog registration is also unresolved.

Verification uses offline Rust regression tests, the workspace test suite,
Clippy, formatting and identifier hygiene. No live playback, public catalog
availability or startup-performance claim follows from these checks.
