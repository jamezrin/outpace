# 57 - Deprecated port 62062 fails the production value gate

Issue #111 proposed two possible reasons to implement the old TCP engine API: direct-peer
injection for deterministic official-engine interoperability, or compatibility with a
named legacy client. Neither currently justifies a production listener. Outpace should
continue to omit and not expose port 62062.

## Provenance

The conclusions below use these sources:

- Official Ace Network documentation, commit
  `54a185dd0993f7f4eca774fb36c751a098f47f00`:
  `docs/developers/old-engine-api.md` and
  `docs/developers/engine-command-line-options.md` in the ignored
  `references/ace-network-docs` checkout.
- Official Android engine repositories at the commits pinned by
  `acestream-engine-android` commit `88ddcc93ae71a4a256b57757abdfad5e75b46cb1`:
  `acestream-engine-client` `e65c982a588414f23bed22615cbeb80fc59fd4c9`,
  `acestream-android-sdk` `b33f3640b54ad727de61854200ad2b6689b2ca7e`, and
  `acestream-engine-android-core` `ced69f36873ffbef9806f29321dbf9c68d4e52bf`.
- The isolated official Linux engine 3.2.11 sandbox described in note 01.
- ValdikSS/AceProxy commit `73da4c4f1b9a20586ffc87502ab6a3c177c21634`
  (2015-11-11), inspected in a temporary checkout as a historical client example.
- Outpace interoperability notes 30 through 33, with the later notes superseding the
  earlier discovery blocker.

No proprietary binary, private capture, product key, or sandbox state is included here.

## What the protocol is

The official documentation calls 62062 the old, deprecated **engine API**. It is a
CRLF-delimited player-control protocol:

```text
client -> HELLOBG ...
engine -> HELLOTS ...
client -> READY ...
engine -> AUTH ...
client -> LOADASYNC ... / START ... / STOP / playback events
engine -> LOADRESP ... / START <HTTP URL> / STATUS ... / PAUSE / RESUME
```

Its documented content selectors are torrent URL, infohash, content/player ID, base64
torrent data, and direct URL. The full documented incoming-command inventory contains no
peer address, tracker override, source-node endpoint, peer-list mutation, or other command
that could direct the engine to `host:port` on the peer wire.

The engine log class names (`Instance2InstanceThread` and `APIServer`) led earlier notes to
call this an I2I API. That name does not imply engine-to-engine discovery or coordination:
the public contract and the historical client both show an application-to-engine playback
control socket. `START INFOHASH` selects content; it does not select a peer.

**Conclusion:** port 62062 is the wrong layer for explicit-peer injection. No additional
binary reverse engineering or production parser is warranted for that use case.

## The interop use case is already superseded

Note 30 listed port 62062 only because official-consumer testing then lacked deterministic
peer discovery. Notes 31 through 33 replaced that need with a controlled local UDP tracker
embedded in the transport descriptor. That route made the official engine:

1. announce to the local tracker and dial Outpace;
2. complete the signed peer handshake and live-window exchange;
3. send `Interested` and live chunk requests;
4. accept Outpace's timestamped pieces; and
5. return MPEG-TS media over its HTTP playback URL.

The original test-harness value case is therefore not merely unproven; it is obsolete.
The local-tracker method exercises the normal production discovery and peer-wire path and
already provides the deterministic official-consumer proof that issue #111 sought.

## Pinned official Android sources use HTTP

The pinned official Android sources retain old-API plumbing but do not provide evidence of
a current socket-protocol consumer:

- `IAceStreamEngine.aidl` still exposes both `getEngineApiPort()` and
  `getHttpApiPort()` for compatibility.
- `AceStreamEngineService` passes `--api-port 62062` to the embedded engine.
- `ConnectTask` opens and immediately closes a socket to `127.0.0.1:62062`; it sends no
  command and uses the successful connect only as an engine-readiness check.
- The Android SDK's `EngineApi` obtains `getHttpApiPort()` and performs Retrofit HTTP calls
  to `/server/api` and per-session command URLs.
- Across the pinned engine client, SDK, and core trees, the old wire commands (`HELLOBG`,
  `HELLOTS`, `LOADASYNC`) occur nowhere. The only 62062 socket use is the readiness check.

Outpace already has a native health endpoint, so emulating a deprecated control server
only to make a connect-and-close readiness probe succeed would not provide useful Android
integration. Android bound-service compatibility is a separate platform concern.

## Historical client found: AceProxy

AceProxy is a concrete historical consumer. Its `aceclient` sends protocol version 3,
performs the challenge/key exchange, sends user demographics, loads and starts PID or
torrent content, consumes status and live-position events, and proxies the engine's HTTP
playback URL on its own port.

It does not satisfy the issue's present compatibility value gate:

- its last repository commit is from 2015 and its entry point requires Python 2;
- Python 2 is absent from the current validation environment, so an end-to-end supported
  client reproduction was not possible;
- its primary outcome—HTTP playback by content ID or torrent—is already a native Outpace
  surface, without the extra proxy process;
- no Outpace user, deployment, or supported application has reported a dependency on it;
- meaningful emulation would include challenge authentication, content loading, session
  lifecycle, asynchronous state, pause/resume, and possibly live seek. A port-open stub or
  HELLO-only listener would falsely advertise compatibility.

AceProxy is useful evidence for how the protocol evolved beyond the abbreviated official
document, but existence of abandoned client code is not enough reason to add a remotely
reachable parser and state machine. A future issue can reconsider a minimal subset only
with a reproducible client version and user workflow that cannot migrate to the HTTP API.

## Isolated engine 3.2.11 probe

The existing sandbox was started without publishing port 62062 to the host. A probe from
inside its network namespace observed:

- startup bound the API to `0.0.0.0:62062`;
- connect alone produced no banner;
- `HELLOBG version=3\r\n` returned
  `HELLOTS version=3.2.11 version_code=3021100 key=<challenge> http_port=6878 bmode=0`;
- plain `READY\r\n` returned `NOTREADY`;
- the challenge response used by the historical client returned `AUTH 0` when supplied
  with its product key.

The checked-in `tools/probe_old_engine_api.py` reproduces only this bounded handshake. It:

- accepts loopback IP literals only;
- caps each response line at 4096 bytes;
- caps its timeout at 10 seconds;
- sends no content, playback, file, URL, peer, or shutdown command;
- reads an optional legacy product key from `ACE_OLD_API_PRODUCT_KEY` and never prints it.

For the Docker sandbox, copy the probe into the container so its loopback restriction is
preserved:

```bash
docker cp tools/probe_old_engine_api.py sandbox-acestream-1:/tmp/
docker exec sandbox-acestream-1 \
  python /tmp/probe_old_engine_api.py
```

The unkeyed expected result is `HELLOTS ...`, then `NOTREADY`. Keyed probing is optional
and should use a locally supplied, authorized product key.

## Security and scope decision

The sandbox binds 62062 on all interfaces, and the historical challenge scheme relies on
a product key embedded in client software. It should not be treated as strong network
authentication. Outpace must not copy the reference container's unconditional port
publication.

Decision for issue #111:

- do not add a 62062 listener, configuration flag, `EXPOSE`, or Compose mapping;
- use the controlled local tracker for deterministic official-consumer interop;
- direct legacy users to the HTTP compatibility routes;
- keep account, advertising, demographics, arbitrary URL/file control, and general remote
  player control outside Outpace's scope;
- reopen research only for a named, reproducible, currently needed client workflow with an
  explicitly bounded command subset and a security review.

This is a no-production-implementation outcome. The durable note and bounded probe satisfy
the research need without creating a new default attack surface.
