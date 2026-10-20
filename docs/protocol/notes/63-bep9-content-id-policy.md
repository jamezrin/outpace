# 63 - Content-id resolution and explicit raw-hash BEP-9 fallback

Date: 2026-10-16. Issue: #177. Related catalog response authentication: #178,
[note 62](62-catalog-response-authentication.md).

## Policy

Live and VOD content-id resolution uses the catalog by default. A catalog failure returns
an error instead of automatically querying BEP-9 `ut_metadata` peers with the requested
content id. The daemon also resolves its own broadcast content ids through a trusted local
broadcast registry. That path uses descriptors owned by the broadcaster in this process;
it requires neither a catalog success nor peer fallback opt-in.

`OUTPACE_BEP9_CONTENT_ID_FALLBACK=1` explicitly enables BEP-9 fallback after catalog failure
for identifiers using the raw transport-hash convention. The setting defaults to false and
is read by both the daemon runtime and standalone `outpace play`, including `play --vod`.
The resolver still requires:

```text
SHA1(complete raw wrapped transport bytes) == requested raw-hash identifier
```

The check covers the whole `AceStreamTransport` wrapper, not just the decoded descriptor,
the source public key or the descriptor-derived swarm infohash. A mismatch is rejected.
Opt-in does not remove this binding check or promote peer metadata to catalog provenance.

## Evidence and limits

Outpace broadcasts deliberately identify their minted wrapped descriptor by its raw SHA-1.
They announce that metadata key and serve the wrapped bytes through BEP-9 before a follower
joins the descriptor-derived broadcast swarm. Existing synthetic resolver tests establish
that convention. They do not establish the derivation of a public catalog content id.
The earlier `fix/bep9-bind-metadata-hash` branch was already an ancestor of `main` when
this work began; it was not an unmerged fix for public content-id derivation.

An independent engine probe used `sandbox-acestream:latest` with `--network none` and a
synthetic AES-wrapped descriptor. It observed:

- `TransportFile.dump()` returned the original raw blob.
- `get_hash()`, `get_infohash()` and `get_def().get_hash()` agreed with one another and
  differed from `SHA1(raw blob)`.
- The live `TransportDescriptorBT` object exposed no `get_content_id` method.
- `TorrentDef.get_content_id()` initially returned `None`; setting an obviously synthetic
  placeholder content id and reading it back preserved the value.

A targeted inspection of `CoreApp.create_transport_file_wrapper` at ELF offset `0x651c60`
found a `create_content_id` option, a call into `_update_torrent`, handling for
`error while getting content id`, and separate `tfile` and `content_id` return fields.
This is evidence of a separate content-id acquisition path. `_update_torrent` was not
fully traced, so it proves neither deterministic public derivation nor arbitrary catalog
server assignment. The synthetic probe also does not reproduce public catalog registration.

The earlier broadcaster-key conjecture in [transport-file.md](../transport-file.md) remains
unestablished. Neither the raw transport SHA-1 nor the swarm infohash may be substituted
for the public catalog content id as a general binding rule. No public peer sampling was
performed for this investigation; it does not establish that BEP-9 never works for public
content ids.

## Trust boundaries

The catalog path verifies the engine-compatible response signature, matching request nonce
and transport checksum. Its shared embedded response key is not a server-only trust anchor
over plain HTTP. The nonce ties a response to a request; it does not prove a mathematical
relationship between the requested content id and the descriptor. Note 62 describes the
remaining forgery risk. This policy preserves those checks without adding an unsupported
raw-hash requirement to catalog responses.

A raw-hash BEP-9 result binds the requested identifier to the complete transport bytes.
That does not establish that it is a public catalog content id. Peer-derived results remain
outside `InfohashIndex`, the shared catalog-provenance index. Resolving such a content id
must not make subsequent bare-infohash or magnet playback succeed. Transport-URL descriptors
likewise do not acquire catalog provenance.

The local broadcast registry is a separate trust source: the same daemon minted and owns
the descriptor. It must not be populated by a peer result or used to bypass raw-hash checking
for a remote broadcast. Own broadcasts retain their existing trusted bare-infohash behavior.
Metadata serving and broadcast raw-hash identifiers remain supported.

## Migration

Existing deployments that relied on automatic BEP-9 fallback now receive an error when
catalog resolution fails. To receive an outpace broadcast Ace link from another daemon,
set `OUTPACE_BEP9_CONTENT_ID_FALLBACK=1` on the receiving daemon or standalone player:

```bash
OUTPACE_BEP9_CONTENT_ID_FALLBACK=1 outpace play 'acestream://<content-id>' > live.ts
```

The originating daemon needs no flag to play its own broadcast content id. A standalone
`play` process does not share that daemon's trusted broadcast registry, even on the same
machine. Alternatively, use the broadcast's transport URL:

```bash
outpace play 'https://stream.example/broadcast/demo' > live.ts
```

The receiver's transport-URL access restrictions still apply. Enabling the fallback is a
receiver-side choice; a broadcaster serving metadata does not enable it for clients.
The `/server/api` content-analysis methods remain catalog-only; the flag governs playback
resolution and does not add peer or local-broadcast lookup to those analysis methods.
No bind address, tracker policy, transport encryption or cache path changes with this policy.

## Offline validation and scope

The intended offline regression coverage exercises default-disabled and explicitly enabled
configuration in both runtime and CLI; catalog failure without a peer attempt by default;
raw-hash success and mismatch rejection with opt-in; live and VOD resolution; separation
of peer results from catalog `InfohashIndex`; and own-broadcast content-id resolution without
peer fallback. Existing catalog signature, nonce and checksum checks must remain covered.
Use synthetic descriptors and identifiers throughout.

Relevant validation is the focused resolver/configuration/provider/CLI tests, followed by
`cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo fmt --all --check`, and `python3 tools/hygiene/check_identifiers.py`. These are the
validation targets, not a claim that every command has passed merely because this note
lists it; the change's verification report records actual results.

Offline checks cannot establish current public catalog availability, public peer metadata
coverage, third-party catalog registration, live playback continuity or startup performance.
The exact public content-id binding remains open.
