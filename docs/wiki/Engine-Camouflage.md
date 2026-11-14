# Engine Camouflage

Outpace normally labels itself as Outpace. Peers, trackers, the search service and the catalog
can see that. Engine camouflage is an opt-in profile in which those public values match one
official Linux AceStream engine instead.

It is off by default and available from v1.1.0, as well as on current `main` and the `edge` image.
Restart the daemon after changing it. The local API, metrics, logs and the `outpace:` startup
lines stay Outpace's. One extra line confirms the profile:

```text
outpace: AceStream identity enabled (linux 3.2.11)
```

## Turn it on

```bash
export OUTPACE_ACESTREAM_IDENTITY=1
outpace serve
```

`outpace play` reads the same variables. Booleans accept `1`, `true`, `0` or `false`.
Every other `OUTPACE_ACESTREAM_IDENTITY_*` variable is ignored while the switch is off.
A bad value stops startup before Outpace opens a listener, and the error names the variable.
Nothing is replaced with a guess.

Compose does not forward these variables. Put them in `compose.override.yml`; see
[Deployment](Deployment#passing-other-settings). The device id file lives in the data volume,
so keep that volume if you want the same id after a recreate.

## What changes on the network

With the profile on:

- **Search**, when [search](Stream-Search) is also enabled, uses the engine's parameter set
  and order, including the device id and hardware key, and the fixed engine User-Agent.
  Page 0 omits `h`. `s` and `t` are not sent.
- **Catalog** requests use the profile's version and platform in the signed query, a fresh
  Chrome User-Agent on each request, and the engine's header order.
- **Peer handshakes** use the profile's version and platform. After Outpace knows its public
  address, they also carry the ASN and country. Until then those keys are omitted.
- **UPnP** mapping descriptions are `AS` plus the first four hex digits of the device id,
  instead of `outpace peer`. See [NAT and Port Mapping](NAT-and-Port-Mapping).
- **Broadcast descriptors** omit `outpace_name`. The name remains in local state.

These stay the same either way:

- The node signing key and the peer id format.
- Tracker announces (`key=0`, the real listen port).
- Handshakes advertise the listen port you configured, and `ts` is host uptime. See
  [Seeding and Peers](Seeding-and-Peers).

## Values

| Variable | Default | Accepted values |
| --- | --- | --- |
| `OUTPACE_ACESTREAM_IDENTITY_VERSION` | `3.2.11` | `X.Y.Z`. The numeric code is `X*1000000 + Y*10000 + Z*100` (3.2.11 is 3021100). |
| `OUTPACE_ACESTREAM_IDENTITY_PLATFORM` | `linux` | `linux` only. |
| `OUTPACE_ACESTREAM_IDENTITY_DEVICE_ID` | random, then saved | 40 hex digits. |
| `OUTPACE_ACESTREAM_IDENTITY_HARDWARE_KEY` | from this host | 40 hex digits. |
| `OUTPACE_ACESTREAM_IDENTITY_ASN` | looked up | Integer, with or without an `AS` prefix. Not zero. |
| `OUTPACE_ACESTREAM_IDENTITY_COUNTRY` | looked up | Two letters. |
| `OUTPACE_ACESTREAM_IDENTITY_USER_AGENT` | the engine UA for the version | Printable ASCII, at most 256 characters. |

The device id is created the first time camouflage starts and stored as
`acestream-device-id` in `OUTPACE_DATA_DIR` (mode `0600` on Unix), next to the node identity.
It stays fixed across restarts. Setting `OUTPACE_ACESTREAM_IDENTITY_DEVICE_ID` uses that value
for the process and does not replace the saved id.

On Linux the hardware key is derived from the memory size reported in `/proc/meminfo`, which
is what the engine sends when its disk probe matches nothing. Hosts without that file, including
Windows, must set `OUTPACE_ACESTREAM_IDENTITY_HARDWARE_KEY`.

## Network location

The handshake's ASN and country come from `OUTPACE_ACESTREAM_IDENTITY_ASN` and
`OUTPACE_ACESTREAM_IDENTITY_COUNTRY` when both are set. Setting only one is an error.

When neither is set, Outpace waits until two peers report the same public address, then makes
one HTTPS lookup to RIPEstat (`stat.ripe.net`) for the ASN and country. Redirects are not
followed. The fields stay omitted until that lookup succeeds. A failure is logged once as
`[identity] network location lookup failed` and is not fatal. Success is
`[identity] recorded network location`.

## What this does not do

- It does not change what you see locally: routes, the startup banner, or logs.
- It does not emulate the engine's background calls to AceStream's own servers.
- It does not add Windows or Android profiles. Only the captured Linux profile is accepted.
- It does not play premium or encrypted content.

Full setting table: [Configuration](Configuration#acestream-identity).
