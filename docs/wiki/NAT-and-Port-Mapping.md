# NAT and Port Mapping

Most home and office connections sit behind a router doing NAT. Without help, peers on the
internet can't open connections to you. Playback still works, because Outpace connects out to
peers, but you won't receive inbound peers, and your [broadcasts](Broadcasting) and seeding are
much less useful.

To accept inbound peers, **external TCP port 8621 must reach Outpace's TCP port 8621.** The
port Outpace listens on (`OUTPACE_PEER_LISTEN`) is the port it announces, so all three must
match: the listen port, the announced port and the router's external port.

## Option 1: a manual port forward (recommended)

In your router, forward **TCP** external port `8621` to the LAN address of the machine running
Outpace, port `8621`. Give that machine a fixed LAN address (a DHCP reservation, or a static
address outside the DHCP pool).

- With Docker, forward to the **Docker host**, and publish `8621:8621/tcp`. Keep the host and
  container ports the same. Remapping only the host side makes Outpace announce a port nobody
  can reach.
- Allow inbound TCP 8621 in the host firewall too (ufw, firewalld, nftables, Windows Firewall).
- Some routers take a minute or so before a new rule applies.

To use a different port, change it everywhere:

```bash
# native binary
export OUTPACE_PEER_LISTEN=0.0.0.0:8622
outpace serve

# docker run: set the listener and publish the same port
docker run -d --name outpace -e OUTPACE_PEER_LISTEN=0.0.0.0:8622 -p 8622:8622/tcp \
  -p 127.0.0.1:6878:6878 -v outpace-data:/var/lib/outpace ghcr.io/jamezrin/outpace:0.9.0
```

Then forward external TCP 8622 to port 8622 on the host. The provided `compose.yml` fixes both
`OUTPACE_PEER_LISTEN` and the port mapping at 8621. To change the port there, edit both lines in
your copy of the file: an override file can add a port mapping but can't remove the 8621 one.

## Option 2: automatic mapping with UPnP or NAT-PMP

Outpace can ask the router to open the port itself:

```bash
export OUTPACE_ENABLE_PORT_MAPPING=1
export OUTPACE_PORT_MAP_BACKEND=auto      # or upnp, natpmp, none
# export OUTPACE_PORT_MAP_EXTERNAL_PORT=8621  # optional; defaults to the listen port
outpace serve
```

With Compose, `OUTPACE_ENABLE_PORT_MAPPING` is read from `.env`. Set `OUTPACE_PORT_MAP_BACKEND`
in `compose.override.yml`; see [Deployment](Deployment#passing-other-settings).

- It only acts when inbound serving is on.
- `auto` tries UPnP-IGD first, then NAT-PMP.
- Mappings are leased and renewed, and removed when Outpace stops.
- A failure is logged and Outpace carries on without a mapping.

**Use it only when Outpace owns the machine's LAN address**: a binary running directly on the
host, or a container with host networking. In normal Docker bridge networking, Outpace sees the
container network's gateway rather than your router, so mapping can't work. Use a manual forward
there.

Log lines to look for:

```text
[portmap] mapped external 203.0.113.7:8621 -> local :8621 via <backend> (lease <n>s)
[portmap] renewed mapping for external port 8621
[portmap] no backend could map the inbound port; continuing NAT-bound
```

## Checking it works

Test from a machine **outside** your network, such as a phone on mobile data or a remote server.
Many routers don't support hairpin NAT, so tests from inside the LAN are misleading.

```bash
timeout 8 bash -c 'echo > /dev/tcp/<your-public-ip>/8621' && echo open || echo closed
```

An open port only proves the path. Outpace's own check is the `[reachability]` log line, which
appears every two minutes:

```text
[reachability] externally reachable: yes (3 inbound peer(s), external 203.0.113.7:8621, observed yourip 203.0.113.7)
```

It turns to `yes` once a real peer has dialled in, which can take a while on quiet streams.
After 10 minutes with no inbound peers, a `HINT` line names the advertised port and lists the
usual causes.

## Double NAT and CGNAT

If your router's WAN address is itself private (for example `100.64.x.x` or `10.x.x.x`), your
ISP is doing carrier-grade NAT, and no forward on your router can make you reachable. Outpace
warns when it notices:

```text
[reachability] WARNING: observed public IP ... differs from the mapped external IP ...
```

Your options:

- ask the ISP for a public IPv4 address;
- forward the port on the upstream router too, if you control it;
- run download-only (`OUTPACE_ENABLE_INBOUND=0`).

## VPNs and Cloudflare WARP

Full-tunnel VPNs usually break inbound peers, and some also disturb DHT and UDP traffic.

Cloudflare WARP in particular installs a firewall that drops inbound connections from public
addresses and sends replies into the tunnel. Even a correct forward never completes a handshake.
[Deployment](Deployment#lxc-guest-behind-cloudflare-warp) describes a tested workaround. If you
don't need WARP for Outpace, run Outpace outside it.

## Related pages

- [Seeding and Peers](Seeding-and-Peers)
- [Troubleshooting](Troubleshooting#no-inbound-peers)
- [Configuration](Configuration#nat-and-port-mapping)
