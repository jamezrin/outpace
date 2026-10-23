# LXC deployment behind Cloudflare WARP

This guide records a working setup for running the published image in an unprivileged
Proxmox LXC guest that tracks the moving `edge` image and sits behind Cloudflare WARP. It
extends [`deployment.md`](deployment.md), which covers the standard Compose deployment. The
files referenced here live in [`tools/deploy/lxc/`](../tools/deploy/lxc/).

The one non-obvious part is inbound peers: **with WARP in `Warp` mode, a correctly forwarded
peer port never completes a handshake** unless the guest is told to let it through. That is an
environment property, not an outpace bug, so the fix is an optional helper service and not part
of the daemon. See [Inbound peers behind WARP](#inbound-peers-behind-warp).

Nothing here is required for a normal deployment. Skip it if you do not run WARP or a similar
full-tunnel VPN inside the same network namespace as the daemon.

## Guest layout

Create an unprivileged Debian guest. Docker inside an unprivileged LXC needs `nesting` and
`keyctl`; WARP needs the TUN device passed through:

```ini
# /etc/pve/lxc/<id>.conf (relevant lines)
features: nesting=1,keyctl=1
unprivileged: 1
net0: name=eth0,bridge=vmbr0,gw=<lan-gateway>,ip=<static-lan-ip>/24,type=veth
lxc.cgroup2.devices.allow: c 10:200 rwm
lxc.mount.entry: /dev/net dev/net none bind,create=dir
```

Give the guest a **static LAN address** so the router's port forward keeps pointing at it. Pick
one that is outside the router's DHCP pool *and* not configured on any other guest or VM,
**including stopped ones**: a stopped container still owns its configured address and will
collide the moment it starts. Check every guest, not just what answers a ping:

```bash
grep -hoE 'ip=[0-9.]+' /etc/pve/lxc/*.conf | sort -u    # plus the VM configs
```

Router "port forwarding" rules can also reference an address that no live device is using; look
there too before reusing one.

Install Docker and the Cloudflare WARP client from their vendors' packages, register the client,
and leave it connected in `Warp` mode with always-on enabled.

## Run outpace

Copy [`compose.yml`](../tools/deploy/lxc/compose.yml) to `/opt/outpace/compose.yml` and create
`/opt/outpace/.env`:

```dotenv
OUTPACE_IMAGE=ghcr.io/jamezrin/outpace:edge
```

Use a pinned release tag instead of `edge` if you do not want to follow `main`. The file uses
`network_mode: host`, so the listeners bind the guest's own addresses and WARP applies to the
daemon directly. The router, not Docker, publishes the peer port.

This LXC template explicitly opts into the [supported Ace HTTP subset](protocol/compat-matrix.md)
with `OUTPACE_ACE_COMPAT: "1"`. It replaces the deprecated
`OUTPACE_EXPERIMENTAL_ACE_COMPAT` setting while preserving this template's existing opt-in;
the daemon and generic Compose defaults remain off. To disable it here, change the service's
environment entry to `OUTPACE_ACE_COMPAT: "0"` and recreate the service with
`docker compose up -d --wait`. Setting `.env` alone does not override this literal entry.
For boolean validation and legacy precedence, see the
[configuration contract](deployment.md#ace-http-compatibility).

`OUTPACE_PEER_LISTEN`'s port is also the port outpace announces to trackers. The listen port,
the announced port, and the router's **external** port must therefore be identical; remapping
only one side advertises an endpoint nobody can reach.

## Follow `main` automatically

Every push to `main` publishes `ghcr.io/jamezrin/outpace:edge` (see
[`edge.yml`](../.github/workflows/edge.yml)). A guest cannot receive a GitHub webhook unless it
is publicly reachable, and a push webhook fires before the image exists, so the guest polls the
registry instead. Install the pair from `tools/deploy/lxc/`:

```bash
install -m 755 outpace-deploy /usr/local/sbin/outpace-deploy
install -m 644 outpace-deploy.service outpace-deploy.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now outpace-deploy.timer
```

Once a minute the job pulls the image and runs `docker compose up -d --wait` only if the image
id changed, then prunes old images. It exits quietly while the tag does not exist yet.

## Inbound peers behind WARP

Symptom: the router forward is correct, packets reach the guest (visible with `tcpdump` on the
bridge), yet the connection never opens from the Internet and outpace logs
`externally reachable: no (0 inbound peer(s)…)` indefinitely. Two separate things cause it:

1. **WARP installs a default-drop nft firewall** (`table inet cloudflare-warp`). Its input
   chain accepts only loopback, the tunnel, and private/special source ranges, so a SYN from a
   public address on the LAN interface is dropped before it reaches the listener: no SYN-ACK
   and no RST. Its output chain likewise only lets public destinations out through the tunnel.
2. **WARP's policy rule routes replies into the tunnel.** It adds
   `not from all fwmark <mark> lookup <table>`, sending everything without its bypass mark out
   `CloudflareWARP`.

Both must be handled, and the obvious routing fix does not work. Re-marking the reply in an
output `route` chain reroutes the packet, but nft's `oif` match keeps the *original* device for
the rest of the hook, so WARP's `oif "CloudflareWARP" goto tun` still matches and rejects it. The
mark has to be present when the kernel first picks the route. Linux supports exactly that for
accepted connections: with `net.ipv4.tcp_fwmark_accept=1`, the SYN-ACK and the accepted socket
inherit the mark of the incoming SYN.

A custom `ip rule` priority is also a trap. On every reconnect WARP re-registers its rule at the
priority just below the lowest existing one, so any numbered rule of your own ends up behind it.
Use WARP's own bypass mark instead, which its rule exempts regardless of priority.

[`warp-inbound-fix`](../tools/deploy/lxc/warp-inbound-fix) does the following for one TCP port
on one interface, every five seconds and idempotently:

- sets `net.ipv4.tcp_fwmark_accept=1`;
- reads the bypass mark from the live `not from all fwmark` rule (falls back to `0x100cf`);
- marks inbound packets to `PORT` on `IFACE` in a prerouting chain of its own table;
- inserts one accept rule for that port into WARP's input chain and one into its output chain.

Install it (it needs root and `nft` inside the guest):

```bash
install -m 755 warp-inbound-fix /usr/local/sbin/warp-inbound-fix
install -m 644 warp-inbound-fix.service /etc/systemd/system/
install -m 644 warp-inbound-fix.default.example /etc/default/warp-inbound-fix
# edit /etc/default/warp-inbound-fix so PORT matches OUTPACE_PEER_LISTEN
systemctl daemon-reload
systemctl enable --now warp-inbound-fix
```

It opens exactly one port and changes nothing else about WARP. The workaround depends on WARP's
internals (its table name, chain layout, and bypass-mark rule), which Cloudflare does not
document as a stable interface, so re-check it after a WARP upgrade. Because it only touches the
guest's own network namespace, it works in an unprivileged LXC.

## Router and verification

Forward external TCP `PORT` to the guest's address and the same internal port. Many ISP routers
take a minute or so to start applying a new rule. Prefer a static forward over UPnP here:
mappings that compete with a fixed rule, or that expire, make results hard to attribute.

Verify from a host **outside** your network, not from the LAN (hairpin NAT often hides the
problem):

```bash
timeout 8 bash -c 'echo > /dev/tcp/<wan-ip>/<port>' && echo open || echo closed
```

If it stays closed, locate where the packet stops:

1. `tcpdump -ni <bridge> 'tcp port <port>'` on the Proxmox host. No SYN means the router rule is
   not applied yet or points at the wrong address.
2. SYN arrives but no SYN-ACK leaves: the guest firewall or reply route. Confirm the helper is
   active and that `ip rule` still shows WARP's `not from all fwmark` rule.
3. Add a temporary `meta nftrace set 1` rule at a very early priority and watch
   `nft monitor trace` to see which chain drops the packet.

The helper should survive a WARP reconnect and a full guest reboot; both are worth testing once:

```bash
warp-cli disconnect && sleep 3 && warp-cli connect
```

The port is also open when nothing real has connected yet, so a bare TCP connect only proves the
path. The `[reachability]` log line flips to `yes` after a real peer dials in.

## Running two engines side by side

When comparing outpace against another engine behind the same router, both want TCP 8621 by
default and a router can send one external port to only one guest. Give each its own external
port, with outpace's `OUTPACE_PEER_LISTEN` set to match, and run the helper in each guest with its
own `PORT`. That keeps the comparison fair: both engines get a working inbound path rather than
one winning the default port.
