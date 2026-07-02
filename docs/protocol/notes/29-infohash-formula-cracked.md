# 29 — Transport infohash formula cracked

Follow-up to note 28. The precisely scoped blocker was correct: the official engine and
outpace computed different infohashes for the same transport bytes. Root cause: we were
using the engine's **raw transport-file hash**, not its peer-wire **swarm infohash**.

## What cracked it

`pyghidra` is available on this machine, but the faster path was to import the official
`Transport.so` directly inside the Docker sandbox's Python 3.10 runtime. `Transport.so`
normally expects the engine's embedded `ACEStream.*` package namespace, so
`re/harness/import_transport_stub.py` provides permissive stub modules plus real bencode and
utility helpers. With that, this call path works outside the launcher:

```python
Transport.load_transport_file_from_string(raw_transport).get_infohash()
```

Wrapping `Transport.hashlib.sha1` captured both hashes the official module computes:

- `SHA1(raw wrapped transport bytes)` — the cache/transport-file identifier.
- `SHA1(selected descriptor preimage)` — the actual swarm infohash returned by
  `get_infohash()` and used in peer handshakes.

## Actual formula

For live BT transports, the official swarm infohash is:

```text
SHA1(bencode([
  ["name",         descriptor["name"]],
  ["authmethod",   descriptor["authmethod"]],
  ["pubkey",       descriptor["pubkey"]],
  ["piece_length", descriptor["piece_length"]],
  ["chunk_length", descriptor["chunk_length"]],
  ["bitrate",      descriptor["bitrate"]],
]))
```

The order above is engine order, not lexicographic dict order. Example captured preimage for
`transport-02.bin`:

```text
ll4:name13:Synthetic Demo Channelel10:authmethod3:RSAel6:pubkey124:<124 bytes>el12:piece_lengthi131072eel12:chunk_lengthi16384eel7:bitratei100000eee
```

Ground truth:

| Transport | Official swarm infohash | Raw transport-file hash |
|---|---|---|
| `tests/vectors/transport-01.bin` | `50e93529d3eb46a50506b14464185a15292d6e47` | `34df422b80a4bd94ac1e51be9ede60364ec7a7dd` |
| `tests/vectors/transport-02.bin` | `685edf209ccfdf88977c0d317e1407baca486067` | `ed2c05b3b022e9cc7b7c1ca46d20f10839dc4108` |

This also explains the old confusion: the raw hashes were real engine-computed values, but
they identify/cache the transport file. They are not the infohash a peer handshakes,
announces, or discovers under.

## Code changes

- `ace_wire::infohash::infohash_of_transport` now returns the official descriptor-derived
  swarm infohash.
- `ace_wire::infohash::transport_file_hash` preserves `SHA1(raw transport bytes)` for the
  separate cache/transport-file identity.
- `ace_swarm::resolve::stream_info_from_transport` now computes the stream infohash from the
  decoded descriptor, so content-id/`ut_metadata` resolution returns the peer-wire infohash.
- `ace_engine::broadcast::BroadcastRegistry::start_or_resume` now mints B1 broadcasts under
  the official descriptor hash rather than `SHA1(raw transport bytes)`.
- Regression coverage:
  - `ace-wire` vectors assert both official infohashes and raw transport-file hashes.
  - `ace-swarm` resolver fixtures include the six selected fields.
  - B1 broadcast tests assert the minted transport bytes decode and hash consistently under
    the official formula.

## Follow-up

Done in note 30. The official engine now returns outpace's exact infohash for a freshly
minted `url=http://.../broadcast/{name}` transport, so the formula fix is live-verified.
Playback still stays at `prebuf` with `peers=0`; the next blocker is peer discovery / direct
peer injection, not transport hashing.
