# Optional live-BT hash fields (#253)

## Status and evidence

Implementation, tests, documentation, independent review and genuine offline synthetic validation are complete. CI and merge remain pending; no deployment or historical playback cause is claimed.

Static recovery of the original BT constructor and hash consumer establishes an ordered fifteen-key selector filtered by membership in its stored mapping. A retained same-buffer comparison independently established an original eight-pair versus candidate six-pair mismatch, with agreement on the shared six values/spans. The two discarded production suffix names remain unknown. The provider-field example is a source-backed synthetic reproducer, not an identification of those production fields or a historical playback explanation.

## Required hash contract

For a supported live descriptor, select these exact raw Bencode keys in this order:

```text
name, authmethod, pubkey, piece_length, chunk_length, bitrate,
premium, commerce, provider_key, provider_sid, access,
sharing, private, tns, license
```

Retain the existing requirement that all first six fields exist. Missing any of them still returns `WireError::Invalid("missing descriptor infohash field")`. Do not broaden acceptance to the original loader's more general missing-field behavior.

For each remaining key, append `[key, raw_value]` only if that exact key exists. Compute SHA-1 of the Bencode-encoded ordered pair list. Absent optionals supply no pair/default. Present integer zero (including Bencode's false representation), empty bytes, empty list or empty dictionary remain present. Clone raw values without display-title normalization, text conversion, coercion or new field-type validation. Original schema acceptance remains separate from this pure hash operation.

Do not sort the selector, iterate every dictionary field, recognize aliases, substitute a transport-file checksum or hash normalized metadata. Unlisted keys, including tracker/category/quality metadata, remain excluded. The existing six-field case must retain its identity.

## Minimal implementation boundary

Keep `infohash_of_descriptor(&Bencode) -> Result<[u8; 20]>` as the raw live-BT entrypoint, extending its selection to the contract above. Keep the selection table private and fixed; add no configuration or dependency.

The existing function also feeds legacy wrapped VOD through transport consumers. Preserve that behavior explicitly rather than inferring that static BT evidence applies to every descriptor:

- Add `infohash_of_decoded_transport(&TransportDescriptor) -> Result<[u8; 20]>` in `ace-wire::infohash` to select the live rule only when the decoder's existing `is_live` is true. Otherwise retain the required six-field rule.
- Reuse one private hash-building routine for these two fixed policies. No public caller-supplied selector or duplicated decoder classification.
- Delegate `try_infohash_of_transport` and the hash assignments in both `stream_info_from_transport` and legacy `vod_info_from_transport` to that decoded-descriptor entrypoint. Preserve existing decode and validation ordering.
- Existing live broadcast callers of `infohash_of_descriptor` remain live callers; do not change broadcast options or lifecycle code.

This one shared dispatch boundary avoids copying `pieces` classification into the hash module. Preserve the decoder's actual `is_live` semantics, including its existing treatment of empty/malformed piece arrays. Preserve `stream_info_from_transport` accepting a legacy wrapped VOD descriptor where it already does so.

Raw torrent and disk-enveloped VOD keep their existing metainfo hashing. No wrapper/version/padding/parser, VOD routing, signature/public-key validation, geometry limit, catalog authentication, metadata binding, index admission, cache policy or network behavior changes. Existing APIs retain their signatures; the decoded-descriptor helper is additive.

## Acceptance and ownership

1. Luna/root first validate one invented live-BT descriptor built from the accepted synthetic positive plus ASCII `provider_key` and `provider_sid`. The original schema's recovered string categories justify those two values. Its selected sequence must be base six, then `provider_key`, then `provider_sid`; absent `premium`/`commerce` must not be defaulted. The original must accept the BT object and independently select eight pairs while the current candidate selects six. If that prediction fails, stop at the loader/stored-value boundary; do not fetch another stream or guess other mutations.
2. Root delivers the actual synthetic RED, expected ordered encoding, original-control evidence and test ownership boundary before implementation. The original's derived hash stays private; tracked tests derive synthetic expected digests at runtime from independent expected pair lists.
3. Pure tests pin all fifteen positions, absent versus zero/empty membership, required-six errors, raw-value preservation and ignored metadata. They need not claim that the genuine loader accepts arbitrary types for every optional schema field.
4. Pure transport/StreamInfo tests require independent expected digest agreement and unchanged geometry/source-key/signature/index behavior. Legacy wrapped VOD with selected-looking optionals must keep its six-field identity across wire and swarm entrypoints; raw/disk VOD controls remain passing.
5. Focused tests, workspace tests, formatting, Clippy and identifier hygiene pass; fresh review and existing required CI precede root's merge.

## Fixture and publication hygiene

Use the existing wholly synthetic positive and `encode_transport` for in-memory fixtures. Reuse its generated synthetic public key or generate fresh synthetic material; never copy a production descriptor, title, public key, content identifier, identity, endpoint or observed suffix value. Use reserved endpoints only where syntax requires one. Neither code nor harness may read the content registry. Add no recorded forty-hex digest to tracked docs/tests: compute expectations from synthetic bytes. A binary fixture is unnecessary unless Luna's RED needs one; any new vector then requires explicit synthetic provenance in its README.

Keep original binaries, disassembly, oracle protocols and raw comparison material private. The ignored SDD handoff carries private evidence locations. Later protocol documentation should qualify the historical six-field case and state the fifteen-key live selector and compatibility boundary without copying private evidence or identity values.

## Design decision

Use the fixed raw selector plus existing decoded live/VOD dispatch. A global fifteen-key replacement would also change legacy VOD identities; duplicating classification from raw fields would risk disagreement with the decoder. Hashing all fields or adding defaulted optionals would violate the recovered rule. No broader framework, acquisition, deployment or product feature is part of this fix.
