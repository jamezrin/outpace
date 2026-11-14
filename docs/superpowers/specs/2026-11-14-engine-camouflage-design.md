# AceStream identity mode design

Status: implemented on `feat/engine-camouflage`. Identity mode is off unless
`OUTPACE_ACESTREAM_IDENTITY=1`. The ASN lookup is one HTTPS read of RIPEstat
(`prefix-overview` and `geoloc` on `stat.ripe.net`) after two peers agree on a public
address; setting `OUTPACE_ACESTREAM_IDENTITY_ASN` and `_COUNTRY` skips it. Redirects are
not followed. `live_window_size`, `down_rate`, `stream_statuses` and omitting `node_state`
on consumers are still the pre-existing handshake behavior.

## Problem

Peers, trackers, the search service and the catalog service can tell outpace apart
from the official AceStream engine. Some of the giveaways are labels (search sends
`_p=outpace`), others are field values or protocol shapes that no real engine
produces (extended-handshake `ts=0`, `node_state` on a consumer, a DHT without the
`v` tag). This is a precaution: nothing has been seen treating outpace differently.

Goal: an opt-in mode in which every value a remote party can observe matches one
coherent official-engine identity, while the default behavior stays as it is, except
for a set of protocol fixes that are harmless for everyone.

Out of scope:

- Anything only the operator's own clients see: the `/server` API, metrics, logs,
  the startup banner.
- LAN-only behavior (the engine's multicast discovery on `239.255.17.18:32968`, mDNS,
  SSDP browsing).
- Emulating the engine's background chatter with AceStream's servers. See
  [Open questions](#open-questions).

## Evidence

All values come from a capture of the official Linux engine 3.2.11
(`py3@445e0ed`, built 2025-09-22) running in the swarmtest sandbox image. During
the capture it ran a search, resolved content ids through the catalog, and played
live streams for several minutes. Bytecode from the engine's `modules.zip` filled in
derivations the wire alone does not show. Raw captures stay private. Values below
are shapes and constants, never identifiers.

### HTTP clients

| Endpoint | User-Agent | Other headers (in order) |
|---|---|---|
| Search (`s5.torrentstream.org/a`) and `ts.acestream.net`, `r.acestream.net`, `s*.torrentstream.*` | `Mozilla/5.0 (Linux) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36 AceStream/3.2.11` (fixed) | `sentry-trace`, `Host`, `User-Agent`, `Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8`, `Accept-Language: en-US,en;q=0.5`, `Accept-Encoding: gzip`, `Connection: close` |
| Catalog (`<ip>:8081/gettorrent`, `/get`, `/preload`, `/a`, `/b`, `/c`) | `Mozilla/5.0 (Linux) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/<M>.0.<B>.<P> Safari/537.36`, randomized per request, no `AceStream/` suffix. Observed over 70 requests: M 60–117, B 14–2908, P 1–100 | Same set and order as above |
| RDAP (`rdap.db.ripe.net/ip/<own ip>`) | `Python-urllib/3.10` | `Accept-Encoding: identity`, `sentry-trace`, `Host`, `User-Agent`, `Accept: application/rdap+json`, `Connection: close` |

- Every request is HTTP/1.1 and starts with `sentry-trace: <32 hex>-<16 hex>-`
  (random trace and span ids, empty sampled flag).
- `Host` carries `:port` only when the port isn't 80. The catalog sends
  `Host: <ip>:8081`, as outpace already does.

### Search request parameters

Real: `_a _b _n _p _r _t _v a [c] [h] i m _s`. outpace today:
`_n _p _r _t _v a h i m s t [c] _s`.

- `_n=3.2.11`, `_v=3021100`, `_p=linux`.
- `_a`: 40-hex **device id**. Random per installation and stable for the life of the
  install (it differed between fresh containers and stayed fixed across requests).
- `_b`: 40-hex **hardware key**, from `ACEStream/Utilities/HardwareIdentity`. On
  Linux it is `sha1(sha1(b"".join(parts)).hexdigest().encode()).hexdigest()`, where
  `parts` is the disk ids from `/dev/disk/by-id` (matching `sd*`/`hd*` only), then
  `str(MemTotal_kB * 1024)`. With no SATA/IDE disks visible (containers, NVMe-only
  hosts) it is just the memory term. This was reproduced exactly from the captured
  value.
- `h` (page) is omitted for page 0 and `h=1` is sent for page 1. `s` and `t` are
  never sent, and `c` is sent only with a category.
- `_r` is a decimal 63-bit random. outpace already matches.

### Peer wire (`AceStreamProtocol` handshake + BEP-10 extended handshake)

- Reserved bytes all zero. Peer id `R30------` plus 11 random bytes. outpace already
  matches.
- Extended-handshake keys sent by a 3.2.11 consumer, every time (31/31):
  `ace_metadata_version asn asn_country geoip_country lsp m mi node_id nt p platform pv
  signature stream_statuses ts tt v yourip`. **No `node_state`.**
- `v=3021100`, `pv=2`, `nt=1`, `platform=2`, `tt=bt`, `m={ut_metadata: 2}`.
- `p` = the engine's real listening port (8621 by default; other peers send
  8622–8663).
- `ts` = host uptime in whole seconds (it matched `/proc/uptime` to within the
  capture's age). It is not a counter.
- `asn` (int), `asn_country`, `geoip_country` (ISO code) are filled from a lookup of
  the engine's own public IP.
- `mi` is always present with all 17 keys, including before playback starts (every
  position field is `-1` then). `live_window_size` is a **per-stream** value that
  peers of the same stream share (686 on one stream, 1197 on another). It is not a
  constant. `down_rate` carries the real download rate, and the upload rates are 0
  for a consumer that isn't uploading.
- `stream_statuses` is `{}` before playback and `{"-1": 1}` once playing.
- Message order after the handshake: `ext0`, `interested`, then `request`s.

Platform ids, from the peers of the same capture: `1` = Windows (v 3.1.32–3.2.8),
`2` = Linux (3.2.11, the current Linux release), `3` = Android (3.2.13–3.2.22).
Peers that carry `node_state` are Linux *source* nodes (`nt` 2/3), never consumers.

### Tracker (BEP-15 UDP)

`key=0`, `num_want=200`, `ip=0`, `port` = listening port, standard events. **outpace
already matches**, including `key=0`. The earlier idea of randomizing the key is
dropped, because it would have created a giveaway.

### DHT (BEP-5)

- Every message carries `v = b"NS\x01\x18"` (pymdht `NEXTSHARE_VERSION`).
- One node id for the whole session (10,488 of 10,493 messages).
- 2-byte transaction ids.
- `announce_peer` args are `id info_hash port token`. **No `implied_port`.**

### UPnP

Mapping description is `"AS " + device_id[:4]`.

## Design

### 1. Protocol fixes for everyone

These apply whether camouflage is on or off. None of them touches a hot path. They
are values computed once at startup or once per connection.

| Fix | Where | Change |
|---|---|---|
| `p` | `ace-swarm/peer_policy.rs` `node_fields` | The actual peer-listener port, not the 8621 constant. Private mode keeps `p=0`. |
| `ts` | `ace-wire/extended.rs` | Host uptime in seconds (`/proc/uptime` on Linux, read once per handshake; process uptime elsewhere). |
| `mi` always present | `extended.rs` `sign_and_encode` | Emit the 17-key dict with `-1` placeholders when there's no position. |
| `node_state` | `extended.rs` | Omit for consumers. Keep it only where we act as a broadcast source (matches real `nt` 2/3 source nodes). |
| `live_window_size` | live session → `LivePosition` | Mirror the value this stream's peers advertise (the first one received). Fall back to today's computation until one arrives. |
| `down_rate` | live session → `LivePosition` | Current session download rate in bytes/s, from existing stats. |
| `stream_statuses` | `extended.rs` | `{"-1": 1}` once playing. |
| DHT `v` | `ace-swarm/dht.rs` builders | Add `v = NS\x01\x18` to every query. |
| DHT node id | `dht.rs` | One random id per process instead of one per lookup or announce. |
| DHT `implied_port` | `dht.rs` `build_announce_peer` | Drop the key (the explicit `port` stays). |

`asn`/`asn_country`/`geoip_country` stay `0`/empty when camouflage is off, because
filling them needs a network lookup (see below).

### 2. Camouflage mode (opt-in)

Enabled with `OUTPACE_ACESTREAM_IDENTITY=1`. An **identity profile** is resolved
once at startup and threaded to every outbound client. Each value has a good
default and its own override variable.

```text
EngineProfile {
  version_name: "3.2.11", version_code: 3021100,   // _n/_v/UA suffix/handshake v
  platform: Linux,                                  // _p, handshake platform, UA "(Linux)"
  device_id: [u8; 20],                              // _a, UPnP "AS xxxx"
  hardware_key: [u8; 20],                           // _b
  asn: Option<(u32, country)>,                      // handshake asn/asn_country/geoip_country
}
```

| Variable | Default | Meaning |
|---|---|---|
| `OUTPACE_ACESTREAM_IDENTITY` | `0` | `1` turns the mode on. The overrides below are ignored unless it is on. |
| `OUTPACE_ACESTREAM_IDENTITY_VERSION` | `3.2.11` | Claimed engine version (`X.Y.Z`). The code is derived as `X*1e6 + Y*1e4 + Z*100`, the encoding the engine uses (3.2.11 → 3021100). |
| `OUTPACE_ACESTREAM_IDENTITY_PLATFORM` | `linux` | Claimed platform. Only `linux` is accepted. Others fail at startup until captured (see decisions). |
| `OUTPACE_ACESTREAM_IDENTITY_DEVICE_ID` | random, persisted | 40-hex device id (`_a`, UPnP label). |
| `OUTPACE_ACESTREAM_IDENTITY_HARDWARE_KEY` | derived from this host | 40-hex hardware key (`_b`). |
| `OUTPACE_ACESTREAM_IDENTITY_ASN` / `OUTPACE_ACESTREAM_IDENTITY_COUNTRY` | looked up | ASN (integer) and ISO country for the handshake. Setting both skips the lookup. |
| `OUTPACE_ACESTREAM_IDENTITY_USER_AGENT` | engine UA for the version | Fixed User-Agent sent to search and AceStream web hosts. |

Every override is validated at startup. A bad value fails startup with an error
naming the variable. Nothing is silently replaced by a guess.

Startup rejects a malformed version or unsupported platform with a clear error. It
never guesses a value.

What changes with the mode on:

- **Search**: `_n/_v/_p` from the profile. Adds `_a` (device id) and `_b` (hardware
  key). Drops `s` and `t`, omits `h` on page 0. Sends the fixed engine UA with
  `AceStream/<version>`, the full header set in engine order, and a fresh
  `sentry-trace`. The request signature already covers every parameter, so `_a` and
  `_b` are signed like the rest. A live check must confirm the server accepts them.
- **Catalog**: the randomized Chrome UA (M 60–117, B 1–3000, P 1–100, uniform),
  plus the same header set and `sentry-trace`. `_n/_v/_p` in both the query and its
  signature come from the profile instead of the hard-coded `3.2.11`/`linux`.
- **Peer wire**: handshake `v`/`platform` from the profile. Adds real
  `asn`/`asn_country`/`geoip_country`.
- **Broadcasts**: descriptors omit `outpace_name`. The name stays in local state so
  operators can still find their broadcasts.
- **UPnP**: description `AS <first 4 hex of device id>` instead of `outpace peer`.

Derivations:

- **Device id**: 20 random bytes, persisted under `OUTPACE_DATA_DIR` next to the
  existing node identity, created on first camouflage start. It is stable across
  restarts, like a real install.
- **Hardware key**: computed exactly as the engine does on Linux (disk ids matching
  `sd*`/`hd*` from `/dev/disk/by-id`, then `MemTotal_kB*1024`). It needs no new
  dependency. Hosts without SATA disks, containers included, produce the
  memory-only form, which is what the real engine sends there too.
- **ASN/country**: the public IP is learned from peers' `yourip` field (majority of
  the first few handshakes). One lookup follows, cached for the process lifetime.
  Until it resolves, the fields are sent the way the engine sends them while its own
  lookup is pending. The lookup source is an open question.

### Defaults and compatibility

- With `OUTPACE_ACESTREAM_IDENTITY` unset, the only visible changes are the protocol fixes
  in section 1. Search still says `outpace`, the UPnP label stays `outpace peer`,
  and broadcasts keep `outpace_name`.
- The node identity (`node_id` signing key) and peer id format are unchanged.

## Testing

- Vectors in `tests/vectors/` for: the extended handshake with no position (`mi`
  placeholders, no `node_state`), DHT `get_peers` and `announce_peer` bytes with `v`
  and without `implied_port`, and the hardware-key derivation (memory-only and
  disk+memory) against the captured formula with synthetic inputs.
- Unit tests: profile parsing (good and bad version/platform), search parameter set
  and order per mode, catalog header set, UA generator bounds.
- Live (ignored, WARP off, content id resolved from the registry at runtime):
  a camouflaged search returns results, a catalog resolve succeeds, and a play
  starts. A packet capture of outpace in camouflage mode is diffed against the
  engine capture with the same scripts used for the evidence above.

## Performance

No hot-path change. Per handshake: one `/proc/uptime` read and a few extra bencode
keys, about 40 bytes. Per process: one DHT node id, one device-id file read, one
meminfo read and one ASN lookup when camouflage is on. No new background loop, task
or dependency. The PR will include `receive_buffer_measurement` before/after numbers
because `ace-wire`'s handshake encoder changes.

## Wiki

`Environment-Variables.md` gets the new variables. A new `Engine-Camouflage.md` page,
linked from `_Sidebar.md` and `Home.md`, covers what is and isn't disguised and the
supported profiles. `Seeding-and-Peers.md` and `NAT-and-Port-Mapping.md` get the
handshake-port and UPnP-label notes.

## Decisions

Taken as recommended, pending maintainer review: (1) HTTPS lookup with env
override; (2) `linux` only until other platforms are captured; (3) background
chatter is a follow-up issue.

1. **ASN lookup source.** The engine uses Python `ipwhois`, which resolves ASN over
   DNS (Team Cymru) and then RDAP. Options: (a) an HTTPS lookup through the existing
   rustls client to a public ASN API; (b) env-only (`OUTPACE_ACESTREAM_IDENTITY_ASN`/
   `_COUNTRY`), with the fields omitted when unset; (c) DNS TXT to Cymru, which needs
   a TXT-capable resolver dependency.
2. **Windows/Android profiles.** The handshake platform ids are known (1 and 3), but
   the `_p` value, UA platform token and hardware-key derivation for those platforms
   are not captured. Accept them only after a capture, or accept them now
   best-effort?
3. **Background chatter.** The engine also contacts AceStream's servers with no user
   action: `ts.acestream.net/init` every 600 s (answered 451), `r.acestream.net/
   getsettings`, `/.well-known/acestream/<id>` across ~30 domains, `s4.*` data
   downloads, the `/a`–`/c` and `/preload` endpoints, notifications, Google
   Analytics. A server operator could notice a client that searches but never sends
   these. Emulating them means new background loops and outbound traffic.
   Proposed: out of scope here, tracked as a follow-up issue.
