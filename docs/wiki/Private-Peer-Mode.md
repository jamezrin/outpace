# Private Peer Mode

> **From v1.0.0:** Private peer mode and local descriptors. See [Releases and Versions](Releases-and-Versions)
> for publication status.

Private peer mode restricts Outpace to an exact list of peers that you choose. It is for setups
such as a home gateway that should only pull from your own origin node, or a lab swarm.

In private mode outpace:

- connects **only** to the IPv4 `address:port` endpoints you list;
- doesn't listen for inbound peers, and doesn't map ports;
- doesn't use public trackers or the DHT;
- doesn't announce itself, and doesn't save peer hints for later;
- doesn't resolve content ids through the public catalog or fetch descriptor URLs. You play
  local descriptor files instead.

## Requirements

- **The peers you list must accept inbound connections.** A private node only connects
  outward. Two private nodes can't connect to each other.
- The allowed peer can be a normal Outpace node (or any compatible engine) with inbound serving
  on, that is already playing or broadcasting the stream.

## Configure it

```bash
export OUTPACE_PRIVATE_PEERS='192.0.2.10:8621,192.0.2.20:8621'
export OUTPACE_PRIVATE_DESCRIPTORS='{"my-feed":"/srv/outpace/my-feed.acelive"}'
outpace serve
```

### `OUTPACE_PRIVATE_PEERS`

- Comma-separated numeric `IPv4:port`. Hostnames, IPv6, wildcard, multicast and broadcast
  addresses are rejected, and the port can't be zero.
- At most 256 entries and 8 KiB. Duplicates are merged.
- **Any value switches private mode on.** An empty value is an error rather than meaning
  "public".

### `OUTPACE_PRIVATE_DESCRIPTORS`

A JSON object mapping an alias to a descriptor file:

- aliases are 1–64 ASCII letters, digits, `_` or `-`;
- at most 64 entries and 64 KiB in total;
- paths are absolute, with no symlinks anywhere in them;
- each file is a regular file of at most 1 MiB, owned by the user Outpace runs as, and not
  writable by group or others;
- files are read once at startup. Changing a file later has no effect until you restart;
- protected or encrypted descriptors are refused at startup;
- Unix only;
- setting it without `OUTPACE_PRIVATE_PEERS` fails startup.

You can use any `.acelive` live descriptor or single-file VOD descriptor, or fetch one from an
Outpace broadcast. Run this as the user Outpace runs as, so that user owns the file:

```bash
install -d -m 0755 /srv/outpace        # create the directory once (as root if needed)
curl --fail -o /srv/outpace/my-feed.acelive "http://192.0.2.10:6878/broadcast/my-channel"
chmod 0644 /srv/outpace/my-feed.acelive
```

### Settings that conflict

These fail startup in private mode, because they would widen the network:

- `OUTPACE_ENABLE_INBOUND=1`
- `OUTPACE_ENABLE_PORT_MAPPING=1`
- any explicit `OUTPACE_TRACKERS`

Leave `OUTPACE_TRACKERS` unset. The two boolean settings may be unset or `0`. Extra `OUTPACE_ACE_PEERS` or `--peer` entries must be in
the allowlist.

## Play an alias

```bash
outpace play local:my-feed | mpv -
outpace play --vod local:my-vod > movie.mp4

curl 'http://127.0.0.1:6878/streams/ace/local%3Amy-feed.ts'
curl 'http://127.0.0.1:6878/vod/ace/local%3Amy-vod'
```

With the compatibility API on, `id=local:my-feed` works too. The descriptor type must match: live
aliases on live routes, VOD on `/vod`.

`OUTPACE_BEP9_CONTENT_ID_FALLBACK=1` still lets you play a content id by asking the allowed
peers for its metadata, but only those peers.

## Check the mode

```bash
curl http://127.0.0.1:6878/network-policy
# {"mode":"private","allowed_peer_count":2,"inbound_peer_connections":false,
#  "public_discovery":false,"public_advertising":false}
```

In public mode this returns `{"mode":"public","allowed_peer_count":null}`. The compatibility
`get_status` reports the same. Neither shows the endpoints or file paths. Peer addresses are
also hidden in private-mode logs.

## What it is not

- **Not anonymity or encryption.** The allowed peers see which stream you request, and can pass
  on what they send you.
- **Not a firewall.** It governs peer connections only. Your HTTP API, RTMP and SRT listeners,
  and any broadcast pull sources, follow their own settings. Keep the HTTP API on loopback or
  behind a firewall.
- **Not a way to broadcast privately.** `outpace broadcast` refuses to run in private mode,
  because distribution needs inbound peers. Run the broadcast on a public-mode origin node, and
  point private nodes at it.
- Changes need a restart. No HTTP request can widen the allowlist while running.

## Related pages

- [Seeding and Peers](Seeding-and-Peers)
- [Broadcasting](Broadcasting)
- [Configuration](Configuration#private-peers-and-descriptors)
