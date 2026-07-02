# 30 — Official engine consumes outpace after infohash fix: hash interop proven, peer discovery still blocked

> **2026-07-01 update:** superseded by note 31 for the current blocker. A controlled local
> tracker in the descriptor now makes the official engine dial outpace (`peers=1`). The
> remaining failure is deeper: after handshake + live bitfield, the official consumer never
> sends `Interested` or `id=6` chunk requests. Keep this note for the infohash proof and the
> failed `startup_nodes`/`peers=` experiments.

Follow-up to note 29. The old blocker ("official engine computes a different infohash than
outpace for the same transport bytes") is fixed and live-verified. The remaining failure
has moved to peer discovery / direct peer injection: the official engine accepts the
outpace transport, computes the same swarm infohash, but does not discover or dial
outpace as a peer through the normal playback path.

## Runtime setup

- Official engine sandbox: `sandbox-acestream-1`, HTTP API on `127.0.0.1:6878`, Docker
  gateway from the container is `172.23.0.1`.
- Outpace:

```bash
OUTPACE_BIND=0.0.0.0:6900 \
OUTPACE_ENABLE_INBOUND=1 \
OUTPACE_PEER_LISTEN=0.0.0.0:8622 \
OUTPACE_DATA_DIR=/tmp/outpace-proof-data \
cargo run -p ace-engine --bin outpace
```

- Ingest source: repeated `tests/vectors/media/h264-keyframes.ts` into
  `PUT /broadcast/{name}`.

The official engine container can fetch outpace HTTP transport bytes:

```text
http://172.23.0.1:6900/healthz -> ok
http://172.23.0.1:6900/broadcast/proof-compat-1 -> 420 bytes
```

The container can also open raw TCP to outpace's peer port:

```text
connected ('172.23.0.2', 34268) -> ('172.23.0.1', 8622)
[seed-listener] accepted connection from 172.23.0.2:34268
[seed-listener] peer error from 172.23.0.2:34268: Io(Custom { kind: UnexpectedEof, error: "early eof" })
```

So local TCP reachability is not the blocker.

## Infohash proof: fixed

Minted broadcast:

```text
PUT /broadcast/proof-compat-1
outpace infohash: cafe006789abcdef0123456789abcdef01234567
```

Official engine request:

```bash
curl -G http://127.0.0.1:6878/ace/getstream \
  --data-urlencode format=json \
  --data-urlencode url=http://172.23.0.1:6900/broadcast/proof-compat-1
```

Official response:

```json
{
  "response": {
    "infohash": "cafe006789abcdef0123456789abcdef01234567",
    "playback_url": "http://127.0.0.1:6878/ace/r/cafe006789abcdef0123456789abcdef01234567/269e6c9cb28db04d1f0a5fbd76f2519e",
    "is_live": 1,
    "is_encrypted": 0
  },
  "error": null
}
```

That is the exact outpace infohash. This is independent live validation that
`ace_wire::infohash` now matches official `Transport.so` for outpace-minted transports.

## Playback still fails at zero peers

Opening the returned `playback_url`:

```text
curl --max-time 45 .../ace/r/cafe006789abcdef0123456789abcdef01234567/269e6c9cb28db04d1f0a5fbd76f2519e
curl: (28) Operation timed out after 45002 milliseconds with 0 bytes received
HTTP/write-out: 000 0 45.002749
```

Official stat stayed at prebuffer with no peer:

```json
{
  "status": "prebuf",
  "peers": 0,
  "downloaded": 0,
  "uploaded": 0,
  "infohash": "cafe006789abcdef0123456789abcdef01234567",
  "is_live": 1
}
```

Outpace logged self-announce, but no accepted official peer during playback:

```text
[ace] seeder self-announce for cafe006789abcdef0123456789abcdef01234567:
  0 tracker peer(s) seen, DHT announce_peer sent to 8 node(s)
```

Interpretation: the official engine parsed the outpace transport and entered playback
for the right swarm, but its normal tracker/DHT discovery did not yield a reachable
outpace peer within the test window. This is a new, narrower blocker than note 28's
hash wall.

## `startup_nodes` experiment

`TransportDescriptorBT.get_startup_nodes()` exists, so I tested whether embedding a direct
local peer in the descriptor would make the official engine dial outpace.

Method:

1. Fetch outpace's transport from `GET /broadcast/{name}`.
2. Decode with the fixed AES-CBC transport key/IV.
3. Add `startup_nodes` in several bencoded forms.
4. Re-encode the transport.
5. Import official `Transport.so` in the sandbox via `re/harness/import_transport_stub.py`.

Official parser results:

```text
startup_nodes [b'172.23.0.1:8622']
startup_nodes [[b'172.23.0.1', 8622]]
startup_nodes [{b'host': b'172.23.0.1', b'port': 8622}]
startup_nodes [b'\xac\x17\x00\x01!\xae']
```

All variants parsed and all kept the same official infohash
(`startup_nodes` is not part of the selected-field infohash preimage from note 29).

Playback test with a fresh infohash and the likely BitTorrent-style shape
`startup_nodes = [[b"172.23.0.1", 8622]]`:

```text
outpace infohash: cafe046789abcdef0123456789abcdef01234567
official getstream returned: cafe046789abcdef0123456789abcdef01234567
playback: timed out after 20s, 0 bytes
stat: status=prebuf, peers=0, downloaded=0
```

No outpace inbound connection was logged for this startup-node playback attempt. Either
the HTTP playback path does not consume `startup_nodes`, the field is only for a different
mode, or the value shape still is not the runtime shape expected by the downloader.

## Hidden `peers=` query experiment

Also tested an undocumented direct-peer hint:

```bash
curl -G http://127.0.0.1:6878/ace/getstream \
  --data-urlencode format=json \
  --data-urlencode url=http://172.23.0.1:6900/broadcast/proof-peers-param-1 \
  --data-urlencode peers=172.23.0.1:8622
```

Fresh outpace infohash:

```text
cafe056789abcdef0123456789abcdef01234567
```

Official response returned the same infohash, but playback still timed out with:

```text
status=prebuf, peers=0, downloaded=0
```

No inbound connection was logged. The public docs do not list such a parameter; treat this
as ignored for `/ace/getstream`.

## Current precise blocker

The official engine now accepts a outpace-minted transport and agrees on the swarm
infohash, but the normal consumer path does not learn a outpace peer:

- `url=` transport fetch works.
- official `Transport.so` computes the same infohash.
- official playback remains `prebuf` with `peers=0`.
- outpace self-announces to the public tracker/DHT, but the tracker returns zero peers
  and DHT announce alone did not produce a local Docker peer.
- Docker container -> host peer-port TCP reachability is proven.
- Descriptor `startup_nodes` and an ad hoc `peers=` query did not force a dial through
  `/ace/getstream`.

This is not currently evidence of bad piece signing, bad piece headers, or bad live chunk
serving. The official engine never got far enough to request data.

## Next leads

1. Find a reliable direct-peer injection route for the official engine. The best candidates
   are the old I2I/API path on port `62062`, support-node mode (`source_ip`/`source_port` in
   `references/streaming-utils`), or CPython frame-eval/Frida hooks around the downloader's
   peer-list construction.
2. Alternatively, make discovery controlled: run a local tracker/supernode that returns
   `172.23.0.1:8622`, put that tracker URL in the outpace descriptor before minting, and
   check whether official `/ace/getstream` dials it. Public tracker/DHT is too indirect for
   this local Docker proof.
3. Only after the official engine actually connects to outpace should the next blocker be
   assumed to involve `SeederSession`, piece headers, or live-signature semantics.
