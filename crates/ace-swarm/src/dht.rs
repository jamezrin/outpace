//! Mainline DHT (BEP-5) `get_peers` — discover swarm peers for an infohash without a
//! tracker. Acestream populates the swarm on the public DHT, so an iterative `get_peers`
//! lookup toward the infohash returns the same peers the official engine connects to.
//!
//! This is a focused, time-bounded *lookup client* (not a full routing-table node): it
//! bootstraps off the well-known routers, walks closer to the target collecting `nodes`,
//! and harvests any `values` (peers) it's handed along the way.

use crate::resolver::Resolver;
use ace_wire::bencode::Bencode;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;

/// Well-known DHT bootstrap routers.
const BOOTSTRAP: &[&str] = &[
    "router.bittorrent.com:6881",
    "router.utorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "dht.libtorrent.org:25401",
];
/// Enough peers to start racing TCP connects. Waiting for a much larger DHT harvest delays
/// first byte even though the engine only needs one good upstream.
const DHT_PEER_TARGET: usize = 8;

/// How long an inflight `(source, txid)` query stays eligible to be answered. Long enough to
/// admit a genuine reply that lands a round or two late, short enough that a stale entry can't
/// be matched by an unrelated packet arriving much later in the walk.
const INFLIGHT_TTL: Duration = Duration::from_secs(3);

/// Build a `get_peers` KRPC query for `infohash` from our `node_id`.
pub fn build_get_peers(node_id: &[u8; 20], infohash: &[u8; 20], txid: &[u8]) -> Vec<u8> {
    let mut a: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
    a.insert(b"id".to_vec(), Bencode::Bytes(node_id.to_vec()));
    a.insert(b"info_hash".to_vec(), Bencode::Bytes(infohash.to_vec()));
    let mut d: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
    d.insert(b"a".to_vec(), Bencode::Dict(a));
    d.insert(b"q".to_vec(), Bencode::Bytes(b"get_peers".to_vec()));
    d.insert(b"t".to_vec(), Bencode::Bytes(txid.to_vec()));
    d.insert(b"y".to_vec(), Bencode::Bytes(b"q".to_vec()));
    Bencode::Dict(d).encode()
}

/// Build an `announce_peer` KRPC query (BEP-5) — the DHT half of "make ourselves
/// discoverable as a peer for `infohash`". Must carry the opaque `token` a node handed us in
/// its own `get_peers` response (anti-spoofing: a node only accepts an announce echoing a
/// token it issued). `implied_port=0` and an explicit `port` — we advertise our real
/// listening port rather than relying on the sender's UDP source port.
pub fn build_announce_peer(
    node_id: &[u8; 20],
    infohash: &[u8; 20],
    port: u16,
    token: &[u8],
    txid: &[u8],
) -> Vec<u8> {
    let mut a: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
    a.insert(b"id".to_vec(), Bencode::Bytes(node_id.to_vec()));
    a.insert(b"info_hash".to_vec(), Bencode::Bytes(infohash.to_vec()));
    a.insert(b"port".to_vec(), Bencode::Int(port as i64));
    a.insert(b"token".to_vec(), Bencode::Bytes(token.to_vec()));
    a.insert(b"implied_port".to_vec(), Bencode::Int(0));
    let mut d: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
    d.insert(b"a".to_vec(), Bencode::Dict(a));
    d.insert(b"q".to_vec(), Bencode::Bytes(b"announce_peer".to_vec()));
    d.insert(b"t".to_vec(), Bencode::Bytes(txid.to_vec()));
    d.insert(b"y".to_vec(), Bencode::Bytes(b"q".to_vec()));
    Bencode::Dict(d).encode()
}

/// Parsed `get_peers` response: any peers handed to us, closer nodes to continue from, and
/// the opaque `token` (if present) needed to `announce_peer` back to this same node.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct GetPeersResponse {
    pub peers: Vec<SocketAddrV4>,
    pub nodes: Vec<([u8; 20], SocketAddrV4)>,
    pub token: Option<Vec<u8>>,
    /// The KRPC transaction id (`t`) this reply echoes, used to correlate it against the
    /// outbound query it answers. Empty if the response omitted `t`.
    pub txid: Vec<u8>,
}

/// Parse a KRPC response, extracting `r.values` (peers), `r.nodes` (compact nodes), and
/// `r.token` (needed to `announce_peer` back to whichever node sent this response).
pub fn parse_response(buf: &[u8]) -> Option<GetPeersResponse> {
    let v = Bencode::parse(buf).ok()?;
    // A KRPC *response* is the message type `y == "r"` (BEP-5). On untrusted public UDP a
    // hostile packet can smuggle an `r` dict under a query (`y == "q"`) or error
    // (`y == "e"`) type, or omit `y` entirely; gate on the type before mining `r` for
    // peers/nodes/tokens so those never come from a non-response.
    if v.get(b"y").and_then(Bencode::as_bytes) != Some(b"r".as_slice()) {
        return None;
    }
    // `r` must be a dict; a response whose `r` is any other shape is malformed, not empty.
    let r = v.get(b"r")?;
    if !matches!(r, Bencode::Dict(_)) {
        return None;
    }
    let mut out = GetPeersResponse::default();
    if let Some(Bencode::List(vals)) = r.get(b"values") {
        for e in vals {
            if let Some(b) = e.as_bytes() {
                if let Some(p) = compact_peer(b) {
                    out.peers.push(p);
                }
            }
        }
    }
    if let Some(nb) = r.get(b"nodes").and_then(Bencode::as_bytes) {
        for c in nb.as_chunks::<26>().0 {
            let mut id = [0u8; 20];
            id.copy_from_slice(&c[0..20]);
            if let Some(addr) = compact_peer(&c[20..26]) {
                out.nodes.push((id, addr));
            }
        }
    }
    out.token = r
        .get(b"token")
        .and_then(Bencode::as_bytes)
        .map(|b| b.to_vec());
    // Transaction id lives at the top level (`t`), alongside `r`/`y` — capture it so the walk
    // can correlate this reply against the query it answers.
    out.txid = v
        .get(b"t")
        .and_then(Bencode::as_bytes)
        .map(|b| b.to_vec())
        .unwrap_or_default();
    Some(out)
}

/// Did a rejected datagram carry a well-formed KRPC error (`y == "e"`), as opposed to being
/// unparseable or the wrong shape? Diagnostics only — used to separate a node that answered
/// with an error from genuinely malformed/hostile traffic.
fn is_krpc_error(buf: &[u8]) -> bool {
    match Bencode::parse(buf) {
        Ok(v) => v.get(b"y").and_then(Bencode::as_bytes) == Some(b"e".as_slice()),
        Err(_) => false,
    }
}

/// Per-walk diagnostics for a single iterative lookup. A raw peer count hides the failure
/// modes that matter when comparing the custom DHT against a candidate crate (#43): silence,
/// malformed traffic, KRPC errors, uncorrelated packets, and frontier exhaustion. Logged once
/// at the end of a walk and returned so tests can assert on it; not part of the public API.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct DhtWalkMetrics {
    /// Bootstrap/seed nodes placed in the initial frontier.
    bootstrap_seeded: usize,
    /// `get_peers` queries sent (one per not-yet-queried node).
    nodes_queried: usize,
    /// Datagrams accepted as correlated `get_peers` responses.
    valid_responses: usize,
    /// Peer records harvested from accepted responses (`r.values`).
    peers_discovered: usize,
    /// Compact node records harvested from accepted responses (`r.nodes`).
    nodes_discovered: usize,
    /// Datagrams that failed to parse as a well-formed `get_peers` response and were not
    /// recognizable KRPC errors.
    malformed_responses: usize,
    /// Datagrams that were well-formed KRPC error messages (`y == "e"`).
    krpc_errors: usize,
    /// Datagrams that parsed as a response but matched no inflight `(source, txid)` query —
    /// late, replayed, or spoofed traffic rejected by correlation (#40).
    uncorrelated_responses: usize,
    /// Query rounds whose response window elapsed without receiving any datagram at all.
    timeouts: usize,
    /// The walk ran out of new frontier nodes before exhausting its time budget.
    frontier_exhausted: bool,
}

/// 6-byte compact endpoint: 4-byte IPv4 + 2-byte big-endian port.
fn compact_peer(b: &[u8]) -> Option<SocketAddrV4> {
    if b.len() != 6 {
        return None;
    }
    let port = u16::from_be_bytes([b[4], b[5]]);
    if port == 0 {
        return None;
    }
    Some(SocketAddrV4::new(
        Ipv4Addr::new(b[0], b[1], b[2], b[3]),
        port,
    ))
}

/// XOR distance between a node id and the target (BTreeSet-orderable key).
fn distance(id: &[u8; 20], target: &[u8; 20]) -> [u8; 20] {
    let mut d = [0u8; 20];
    for i in 0..20 {
        d[i] = id[i] ^ target[i];
    }
    d
}

/// Iterative `get_peers` walk toward `infohash`, shared by `dht_get_peers` (harvest peers)
/// and `dht_announce_peer` (harvest tokens to announce back with). Seeds the frontier from
/// bootstrap routers, sends batched queries to the closest not-yet-queried nodes, and calls
/// `on_response` for each reply — which returns `true` to stop the walk early (e.g. "enough
/// peers" or "enough tokens"). Bounded by `budget` regardless.
async fn dht_walk(
    infohash: &[u8; 20],
    budget: Duration,
    sock: &UdpSocket,
    on_response: impl FnMut(SocketAddrV4, &GetPeersResponse) -> bool,
) -> DhtWalkMetrics {
    dht_walk_with_bootstraps(
        infohash,
        budget,
        sock,
        BOOTSTRAP,
        Resolver::global(),
        on_response,
    )
    .await
}

async fn dht_walk_with_bootstraps(
    infohash: &[u8; 20],
    budget: Duration,
    sock: &UdpSocket,
    bootstraps: &[&str],
    resolver: Resolver,
    on_response: impl FnMut(SocketAddrV4, &GetPeersResponse) -> bool,
) -> DhtWalkMetrics {
    let deadline = tokio::time::Instant::now() + budget;
    let mut lookups = tokio::task::JoinSet::new();
    for (i, host) in bootstraps.iter().take(BOOTSTRAP.len()).enumerate() {
        let resolver = resolver.clone();
        let host = host.to_string();
        lookups.spawn(async move { (i, resolve_bootstrap(host, resolver, deadline).await) });
    }
    dht_walk_owned(
        infohash,
        deadline.into_std(),
        sock,
        BTreeMap::new(),
        lookups,
        on_response,
    )
    .await
}

pub(crate) async fn resolve_bootstrap(
    host: String,
    resolver: Resolver,
    deadline: tokio::time::Instant,
) -> Option<SocketAddrV4> {
    tokio::time::timeout_at(deadline, resolver.lookup(&host))
        .await
        .ok()
        .and_then(Result::ok)
        .and_then(|addresses| {
            addresses.into_iter().find_map(|addr| match addr {
                SocketAddr::V4(v4) => Some(v4),
                _ => None,
            })
        })
}

#[cfg(test)]
async fn dht_walk_from_seeds(
    infohash: &[u8; 20],
    budget: Duration,
    sock: &UdpSocket,
    seeds: Vec<SocketAddrV4>,
    on_response: impl FnMut(SocketAddrV4, &GetPeersResponse) -> bool,
) -> DhtWalkMetrics {
    let mut frontier: BTreeMap<[u8; 20], SocketAddrV4> = BTreeMap::new();
    for (i, seed) in seeds.into_iter().enumerate() {
        let mut key = [0xffu8; 20];
        key[19] = i as u8;
        frontier.insert(key, seed);
    }
    dht_walk_frontier(infohash, budget, sock, frontier, on_response).await
}

#[cfg(test)]
async fn dht_walk_frontier(
    infohash: &[u8; 20],
    budget: Duration,
    sock: &UdpSocket,
    frontier: BTreeMap<[u8; 20], SocketAddrV4>,
    on_response: impl FnMut(SocketAddrV4, &GetPeersResponse) -> bool,
) -> DhtWalkMetrics {
    dht_walk_owned(
        infohash,
        Instant::now() + budget,
        sock,
        frontier,
        tokio::task::JoinSet::new(),
        on_response,
    )
    .await
}

fn add_bootstrap(
    result: Result<(usize, Option<SocketAddrV4>), tokio::task::JoinError>,
    frontier: &mut BTreeMap<[u8; 20], SocketAddrV4>,
    metrics: &mut DhtWalkMetrics,
) {
    if let Ok((i, Some(addr))) = result {
        let mut key = [0xff; 20];
        key[19] = i as u8;
        frontier.insert(key, addr);
        metrics.bootstrap_seeded += 1;
        while frontier.len() > 64 {
            let key = *frontier.keys().next_back().unwrap();
            frontier.remove(&key);
        }
    }
}
async fn dht_walk_owned(
    infohash: &[u8; 20],
    deadline: Instant,
    sock: &UdpSocket,
    mut frontier: BTreeMap<[u8; 20], SocketAddrV4>,
    mut lookups: tokio::task::JoinSet<(usize, Option<SocketAddrV4>)>,
    mut on_response: impl FnMut(SocketAddrV4, &GetPeersResponse) -> bool,
) -> DhtWalkMetrics {
    let node_id: [u8; 20] = rand::random();
    let mut metrics = DhtWalkMetrics {
        bootstrap_seeded: frontier.len(),
        ..Default::default()
    };

    let mut queried: HashSet<SocketAddrV4> = HashSet::new();
    // Inflight queries keyed by (destination, transaction id) → expiry deadline. Public DHT
    // UDP is untrusted, so a reply is only trusted if it arrives from the exact node we asked
    // and echoes the exact transaction id we generated for that query.
    let mut inflight: HashMap<(SocketAddrV4, Vec<u8>), Instant> = HashMap::new();
    // Monotonic per-walk counter → a distinct 2-byte transaction id for every outbound query.
    let mut next_txid: u16 = 0;
    let mut buf = [0u8; 2048];
    // A bootstrap completion may interrupt a response round to send useful new queries.
    // Retain that round when no new batch exists, rather than losing its pending replies.
    let mut interrupted_window: Option<(Instant, bool, bool)> = None;
    crate::alog!("[dht] seeded {} bootstrap node(s)", frontier.len());

    'outer: while Instant::now() < deadline {
        while let Some(result) = lookups.try_join_next() {
            add_bootstrap(result, &mut frontier, &mut metrics);
        }
        // Drop expired inflight entries so a stale (source, txid) can't be matched by a much
        // later packet — an unanswered query is abandoned, not left open for the whole walk.
        let now = Instant::now();
        inflight.retain(|_, exp| *exp > now);

        // Send to up to 8 closest not-yet-queried nodes.
        let batch: Vec<SocketAddrV4> = frontier
            .values()
            .filter(|a| !queried.contains(a))
            .take(8)
            .copied()
            .collect();
        if batch.is_empty()
            && !lookups.is_empty()
            && inflight.is_empty()
            && interrupted_window.is_none()
        {
            // Empty frontier is not exhaustion while a later bootstrap can still resolve.
            tokio::select! {
                result = lookups.join_next() => {
                    if let Some(result) = result { add_bootstrap(result, &mut frontier, &mut metrics); }
                }
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => break,
            }
            continue;
        }
        if batch.is_empty() && lookups.is_empty() && interrupted_window.is_none() {
            metrics.frontier_exhausted = true;
            crate::alog!("[dht] frontier exhausted: queried={}", queried.len());
            break;
        }
        for addr in &batch {
            queried.insert(*addr);
            let txid = next_txid.to_be_bytes().to_vec();
            next_txid = next_txid.wrapping_add(1);
            let q = build_get_peers(&node_id, infohash, &txid);
            if tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                sock.send_to(&q, SocketAddr::V4(*addr)),
            )
            .await
            .is_ok_and(|sent| sent.is_ok())
            {
                metrics.nodes_queried += 1;
                inflight.insert((*addr, txid), Instant::now() + INFLIGHT_TTL);
            }
        }

        // Collect responses for a short window. A round in which no datagram arrives at all is
        // a timeout; datagrams that arrive but are rejected are counted by their failure mode.
        let (window, mut received_any, count_round) = if batch.is_empty() {
            interrupted_window.take().unwrap_or((
                (Instant::now() + Duration::from_millis(1500)).min(deadline),
                false,
                false,
            ))
        } else {
            interrupted_window.take();
            (
                (Instant::now() + Duration::from_millis(1500)).min(deadline),
                false,
                true,
            )
        };
        while Instant::now() < window {
            let received = tokio::select! {
                result = lookups.join_next(), if !lookups.is_empty() => {
                    if let Some(result) = result { add_bootstrap(result, &mut frontier, &mut metrics); }
                    interrupted_window = Some((window, received_any, count_round));
                    // Query the newly useful frontier immediately, keeping existing inflight
                    // correlation and the original absolute deadline in this same walk.
                    continue 'outer;
                }
                received = sock.recv_from(&mut buf) => received,
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(window)) => break,
            };
            match received {
                Ok((n, src)) => {
                    received_any = true;
                    // Bootstrap/frontier addresses are always v4 (see below), so a real reply
                    // is always from a v4 peer; skip (not break — keep collecting within the
                    // window) on the theoretical v6 case rather than treating it as a timeout.
                    let SocketAddr::V4(src) = src else { continue };
                    let Some(resp) = parse_response(&buf[..n]) else {
                        // Rejected before correlation: either a node's KRPC error or genuinely
                        // malformed/hostile traffic. Bucket the two apart for diagnostics.
                        if is_krpc_error(&buf[..n]) {
                            metrics.krpc_errors += 1;
                        } else {
                            metrics.malformed_responses += 1;
                        }
                        continue;
                    };
                    // Correlate: only accept a reply whose (source, txid) matches an inflight
                    // query. Removing the entry also means a duplicate/replayed packet for the
                    // same query is not processed twice. Unmatched packets are dropped, not
                    // treated as end-of-window, so a spoofed packet can't cut the walk short.
                    if inflight.remove(&(src, resp.txid.clone())).is_none() {
                        metrics.uncorrelated_responses += 1;
                        continue;
                    }
                    metrics.valid_responses += 1;
                    metrics.peers_discovered += resp.peers.len();
                    metrics.nodes_discovered += resp.nodes.len();
                    for (id, addr) in &resp.nodes {
                        frontier.entry(distance(id, infohash)).or_insert(*addr);
                    }
                    // Cap frontier growth: keep the 64 closest.
                    while frontier.len() > 64 {
                        let last = *frontier.keys().next_back().unwrap();
                        frontier.remove(&last);
                    }
                    if on_response(src, &resp) {
                        break 'outer;
                    }
                }
                _ => break,
            }
        }
        if count_round && !received_any {
            metrics.timeouts += 1;
        }
    }

    crate::alog!(
        "[dht] walk done: seeded={} queried={} valid={} peers={} nodes={} malformed={} \
         krpc_err={} uncorrelated={} timeouts={} frontier_exhausted={}",
        metrics.bootstrap_seeded,
        metrics.nodes_queried,
        metrics.valid_responses,
        metrics.peers_discovered,
        metrics.nodes_discovered,
        metrics.malformed_responses,
        metrics.krpc_errors,
        metrics.uncorrelated_responses,
        metrics.timeouts,
        metrics.frontier_exhausted,
    );
    metrics
}

/// Iterative `get_peers` toward `infohash`, bounded by `budget`. Returns discovered peers.
pub async fn dht_get_peers(infohash: &[u8; 20], budget: Duration) -> Vec<SocketAddrV4> {
    dht_get_peers_with_target(infohash, budget, DHT_PEER_TARGET).await
}

pub async fn dht_get_peers_with_target(
    infohash: &[u8; 20],
    budget: Duration,
    peer_target: usize,
) -> Vec<SocketAddrV4> {
    dht_get_peers_incremental(infohash, budget, peer_target, |_| true).await
}

/// Publish each unique peer immediately after its source/transaction correlation succeeds.
/// Returning false from the observer cancels further harvesting. The complete set remains
/// available to aggregate callers; at most 1024 addresses are retained per walk.
pub async fn dht_get_peers_incremental(
    infohash: &[u8; 20],
    budget: Duration,
    peer_target: usize,
    mut on_peer: impl FnMut(SocketAddrV4) -> bool,
) -> Vec<SocketAddrV4> {
    let Ok(sock) = UdpSocket::bind("0.0.0.0:0").await else {
        return Vec::new();
    };
    let mut peers = BTreeSet::new();
    dht_walk(infohash, budget, &sock, |_src, response| {
        collect_response_peers(response, &mut peers, peer_target, &mut on_peer)
    })
    .await;
    peers.into_iter().collect()
}

fn collect_response_peers(
    response: &GetPeersResponse,
    peers: &mut BTreeSet<SocketAddrV4>,
    target: usize,
    on_peer: &mut impl FnMut(SocketAddrV4) -> bool,
) -> bool {
    for &peer in &response.peers {
        if peers.len() >= 1024 {
            return true;
        }
        if peers.insert(peer) && !on_peer(peer) {
            return true;
        }
    }
    peers.len() >= target.max(1)
}

#[cfg(test)]
async fn dht_get_peers_from_seeds(
    infohash: &[u8; 20],
    budget: Duration,
    target: usize,
    seeds: Vec<SocketAddrV4>,
) -> Vec<SocketAddrV4> {
    dht_peers_from_seeds_with_observer(infohash, budget, target, seeds, |_| true).await
}

#[cfg(test)]
async fn dht_peers_from_seeds_with_observer(
    infohash: &[u8; 20],
    budget: Duration,
    target: usize,
    seeds: Vec<SocketAddrV4>,
    mut observer: impl FnMut(SocketAddrV4) -> bool,
) -> Vec<SocketAddrV4> {
    let Ok(socket) = UdpSocket::bind("0.0.0.0:0").await else {
        return Vec::new();
    };
    let mut peers = BTreeSet::new();
    dht_walk_from_seeds(infohash, budget, &socket, seeds, |_, response| {
        collect_response_peers(response, &mut peers, target, &mut observer)
    })
    .await;
    peers.into_iter().collect()
}

#[cfg(test)]
async fn dht_stream_from_seeds(
    infohash: &[u8; 20],
    budget: Duration,
    target: usize,
    seeds: Vec<SocketAddrV4>,
    peers: tokio::sync::mpsc::Sender<SocketAddrV4>,
) {
    dht_peers_from_seeds_with_observer(infohash, budget, target, seeds, |peer| {
        peers.try_send(peer).is_ok()
    })
    .await;
}

/// The DHT half of self-announcement (BEP-5's `announce_peer`), never previously
/// implemented — `dht_get_peers` only ever read the swarm, never told it we're in it. Walks
/// toward `infohash` exactly like `dht_get_peers`, and for every node that hands us a
/// `get_peers` token (required — a node only accepts an announce echoing a token it itself
/// issued), sends it `announce_peer` for our `peer_port`. Makes outpace organically
/// discoverable via the DHT, not just tracker-discoverable (`announce_seeder`) — real
/// Acestream swarms are largely DHT-populated (see `README.md`).
///
/// Best-effort and fire-and-forget by nature (DHT is UDP): returns how many `announce_peer`
/// queries were sent, not a delivery/propagation guarantee.
pub async fn dht_announce_peer(infohash: &[u8; 20], peer_port: u16, budget: Duration) -> usize {
    let sock = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s) => s,
        Err(_) => return 0,
    };

    // Harvest (node, token) pairs during the walk; the actual announce send happens after,
    // back in async context (the walk's per-response callback is synchronous).
    let mut tokens: Vec<(SocketAddrV4, Vec<u8>)> = Vec::new();
    dht_walk(infohash, budget, &sock, |src, resp| {
        if let Some(token) = &resp.token {
            tokens.push((src, token.clone()));
        }
        tokens.len() >= 8
    })
    .await;

    let node_id: [u8; 20] = rand::random();
    let mut announced = 0usize;
    for (i, (addr, token)) in tokens.iter().enumerate() {
        let txid = (i as u16).to_be_bytes();
        let q = build_announce_peer(&node_id, infohash, peer_port, token, &txid);
        if sock.send_to(&q, SocketAddr::V4(*addr)).await.is_ok() {
            announced += 1;
        }
    }
    announced
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn bootstrap_resolution_control(slow: bool) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Condvar, Mutex};
        let responder = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let seed = v4_addr(&responder);
        let peer: SocketAddrV4 = "127.0.0.1:43210".parse().unwrap();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let slow_started = Arc::new(AtomicUsize::new(0));
        let slow_finished = Arc::new(AtomicUsize::new(0));
        let resolver = Resolver::controlled({
            let gate = gate.clone();
            let started = slow_started.clone();
            let finished = slow_finished.clone();
            move |host| {
                if host.starts_with("slow.") {
                    started.fetch_add(1, Ordering::SeqCst);
                    let mut released = gate.0.lock().unwrap();
                    while !*released {
                        released = gate.1.wait(released).unwrap();
                    }
                    finished.fetch_add(1, Ordering::SeqCst);
                }
                Ok(vec![SocketAddr::V4(seed)])
            }
        });
        let server = tokio::spawn(async move {
            let mut bytes = [0; 2048];
            let (n, caller) = responder.recv_from(&mut bytes).await.unwrap();
            let reply = response_with(&buf_txid(&bytes[..n]), &[peer], &[]);
            responder.send_to(&reply, caller).await.unwrap();
        });
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let (publication, mut received) = tokio::sync::mpsc::channel(1);
        let terminal_resolver = resolver.clone();
        let run = tokio::spawn(async move {
            let hosts: &[&str] = if slow {
                &["healthy.invalid:1", "slow.invalid:1"]
            } else {
                &["healthy.invalid:1"]
            };
            dht_walk_with_bootstraps(
                &[0; 20],
                Duration::from_millis(500),
                &socket,
                hosts,
                resolver,
                |_, response| {
                    if response.peers.contains(&peer) {
                        let _ = publication.try_send(peer);
                        return true;
                    }
                    false
                },
            )
            .await
        });
        let prompt = tokio::time::timeout(Duration::from_millis(200), received.recv()).await;
        let slow_pending = !slow
            || (slow_started.load(Ordering::SeqCst) == 1
                && slow_finished.load(Ordering::SeqCst) == 0);
        let terminal = tokio::time::timeout(Duration::from_millis(800), run)
            .await
            .unwrap()
            .unwrap();
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        tokio::time::timeout(Duration::from_secs(2), async {
            while slow_finished.load(Ordering::SeqCst) != slow_started.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        terminal_resolver.finish_controlled().await;
        server.abort();
        let _ = server.await;
        assert!(
            slow_pending,
            "slow native lookup was not outstanding at the measured publication boundary"
        );
        assert!(matches!(prompt, Ok(Some(found)) if found == peer), "healthy resolved bootstrap received no prompt correlated query/publication while another lookup remained pending; queried={}", terminal.nodes_queried);
    }
    #[tokio::test]
    async fn healthy_bootstrap_publishes_before_slow_resolution_finishes() {
        bootstrap_resolution_control(true).await;
    }
    #[tokio::test]
    async fn healthy_bootstrap_resolution_positive_control() {
        bootstrap_resolution_control(false).await;
    }

    #[tokio::test]
    async fn late_bootstrap_joins_the_same_walk_after_first_seed_is_unproductive() {
        use std::sync::{Arc, Condvar, Mutex};
        let silent = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let healthy = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let silent_addr = v4_addr(&silent);
        let healthy_addr = v4_addr(&healthy);
        let peer: SocketAddrV4 = "127.0.0.1:43211".parse().unwrap();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let resolver = Resolver::controlled({
            let gate = gate.clone();
            move |host| {
                if host.starts_with("late.") {
                    let mut released = gate.0.lock().unwrap();
                    while !*released {
                        released = gate.1.wait(released).unwrap();
                    }
                    Ok(vec![SocketAddr::V4(healthy_addr)])
                } else {
                    Ok(vec![SocketAddr::V4(silent_addr)])
                }
            }
        });
        let release_gate = gate.clone();
        let servers = tokio::spawn(async move {
            let mut bytes = [0; 2048];
            // Resolve the late useful seed only once the first real UDP query was sent.
            silent.recv_from(&mut bytes).await.unwrap();
            *gate.0.lock().unwrap() = true;
            gate.1.notify_all();
            let (n, caller) = healthy.recv_from(&mut bytes).await.unwrap();
            healthy
                .send_to(&response_with(&buf_txid(&bytes[..n]), &[peer], &[]), caller)
                .await
                .unwrap();
        });
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let started = Instant::now();
        let mut published = false;
        let metrics = dht_walk_with_bootstraps(
            &[0; 20],
            Duration::from_millis(500),
            &socket,
            &["first.invalid:1", "late.invalid:1"],
            resolver.clone(),
            |_, response| {
                published |= response.peers.contains(&peer);
                published
            },
        )
        .await;
        let elapsed = started.elapsed();
        *release_gate.0.lock().unwrap() = true;
        release_gate.1.notify_all();
        servers.abort();
        let _ = servers.await;
        resolver.finish_controlled().await;
        assert!(
            published,
            "later bootstrap was lost after first unproductive seed"
        );
        assert_eq!(metrics.bootstrap_seeded, 2);
        assert_eq!(metrics.nodes_queried, 2);
        assert_eq!(metrics.valid_responses, 1);
        assert!(
            elapsed < Duration::from_millis(300),
            "late seed waited for a renewed/serial response budget"
        );
    }

    #[test]
    fn get_peers_query_roundtrips() {
        let q = build_get_peers(&[1u8; 20], &[2u8; 20], b"aa");
        let d = Bencode::parse(&q).unwrap();
        assert_eq!(
            d.get(b"q").unwrap().as_bytes(),
            Some(b"get_peers".as_slice())
        );
        assert_eq!(d.get(b"y").unwrap().as_bytes(), Some(b"q".as_slice()));
        let a = d.get(b"a").unwrap();
        assert_eq!(
            a.get(b"info_hash").unwrap().as_bytes(),
            Some([2u8; 20].as_slice())
        );
    }

    #[test]
    fn announce_peer_query_roundtrips() {
        let q = build_announce_peer(&[1u8; 20], &[2u8; 20], 8621, b"tok123", b"aa");
        let d = Bencode::parse(&q).unwrap();
        assert_eq!(
            d.get(b"q").unwrap().as_bytes(),
            Some(b"announce_peer".as_slice())
        );
        assert_eq!(d.get(b"y").unwrap().as_bytes(), Some(b"q".as_slice()));
        let a = d.get(b"a").unwrap();
        assert_eq!(
            a.get(b"info_hash").unwrap().as_bytes(),
            Some([2u8; 20].as_slice())
        );
        assert_eq!(a.get(b"id").unwrap().as_bytes(), Some([1u8; 20].as_slice()));
        assert_eq!(a.get(b"port").unwrap().as_int(), Some(8621));
        assert_eq!(
            a.get(b"token").unwrap().as_bytes(),
            Some(b"tok123".as_slice())
        );
        assert_eq!(a.get(b"implied_port").unwrap().as_int(), Some(0));
    }

    #[test]
    fn parses_values_peers() {
        // r = { id:20, token:.., values: [ "<4 ip><2 port>" ] }
        let mut r: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        r.insert(b"id".to_vec(), Bencode::Bytes(vec![0; 20]));
        let peer = vec![87u8, 221, 96, 148, 0x21, 0xAD]; // 87.221.96.148:8621
        r.insert(
            b"values".to_vec(),
            Bencode::List(vec![Bencode::Bytes(peer)]),
        );
        let mut top: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        top.insert(b"r".to_vec(), Bencode::Dict(r));
        top.insert(b"y".to_vec(), Bencode::Bytes(b"r".to_vec()));
        let resp = parse_response(&Bencode::Dict(top).encode()).unwrap();
        assert_eq!(resp.peers, vec!["87.221.96.148:8621".parse().unwrap()]);
    }

    #[test]
    fn parses_token_needed_to_announce_back() {
        let mut r: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        r.insert(b"id".to_vec(), Bencode::Bytes(vec![0; 20]));
        r.insert(b"token".to_vec(), Bencode::Bytes(b"opaque-tok".to_vec()));
        let mut top: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        top.insert(b"r".to_vec(), Bencode::Dict(r));
        top.insert(b"y".to_vec(), Bencode::Bytes(b"r".to_vec()));
        let resp = parse_response(&Bencode::Dict(top).encode()).unwrap();
        assert_eq!(resp.token, Some(b"opaque-tok".to_vec()));
    }

    #[test]
    fn no_token_parses_as_none() {
        let mut r: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        r.insert(b"id".to_vec(), Bencode::Bytes(vec![0; 20]));
        let mut top: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        top.insert(b"r".to_vec(), Bencode::Dict(r));
        top.insert(b"y".to_vec(), Bencode::Bytes(b"r".to_vec()));
        let resp = parse_response(&Bencode::Dict(top).encode()).unwrap();
        assert_eq!(resp.token, None);
    }

    #[test]
    fn parses_compact_nodes() {
        let mut node = vec![9u8; 20]; // node id
        node.extend_from_slice(&[1, 2, 3, 4, 0x1A, 0xE1]); // 1.2.3.4:6881
        let mut r: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        r.insert(b"nodes".to_vec(), Bencode::Bytes(node));
        let mut top: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        top.insert(b"r".to_vec(), Bencode::Dict(r));
        top.insert(b"y".to_vec(), Bencode::Bytes(b"r".to_vec()));
        let resp = parse_response(&Bencode::Dict(top).encode()).unwrap();
        assert_eq!(resp.nodes.len(), 1);
        assert_eq!(resp.nodes[0].1, "1.2.3.4:6881".parse().unwrap());
    }

    // --- Malformed / hostile KRPC response hardening (#41) ---
    //
    // Public DHT UDP is untrusted, so `parse_response` must ignore anything that isn't a
    // well-formed `get_peers` *response*, rather than mining peers/nodes/tokens out of it.

    /// Build a raw top-level KRPC message: an optional `y` (message type), a `t`
    /// transaction id, and the given `r` value — which need not be a dict, so malformed
    /// shapes can be exercised.
    fn krpc_message(y: Option<&[u8]>, r: Bencode) -> Vec<u8> {
        let mut top: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        top.insert(b"t".to_vec(), Bencode::Bytes(b"aa".to_vec()));
        if let Some(y) = y {
            top.insert(b"y".to_vec(), Bencode::Bytes(y.to_vec()));
        }
        top.insert(b"r".to_vec(), r);
        Bencode::Dict(top).encode()
    }

    /// Build an `r` response dict from optional `values`/`nodes`/`token` parts.
    fn r_dict(values: Option<Bencode>, nodes: Option<Bencode>, token: Option<Bencode>) -> Bencode {
        let mut r: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        r.insert(b"id".to_vec(), Bencode::Bytes(vec![7; 20]));
        if let Some(v) = values {
            r.insert(b"values".to_vec(), v);
        }
        if let Some(n) = nodes {
            r.insert(b"nodes".to_vec(), n);
        }
        if let Some(t) = token {
            r.insert(b"token".to_vec(), t);
        }
        Bencode::Dict(r)
    }

    /// A `values` list carrying exactly one valid compact peer (10.0.0.1:1111).
    fn one_valid_peer_values() -> Bencode {
        Bencode::List(vec![Bencode::Bytes(vec![10, 0, 0, 1, 0x04, 0x57])])
    }

    #[test]
    fn ignores_message_whose_type_is_not_a_response() {
        // y = "q" (a query), not "r". A hostile packet can still smuggle an `r` dict with
        // peers and a token; gating on the message type keeps those from being trusted.
        let r = r_dict(
            Some(one_valid_peer_values()),
            None,
            Some(Bencode::Bytes(b"tok".to_vec())),
        );
        assert!(parse_response(&krpc_message(Some(b"q"), r)).is_none());
    }

    #[test]
    fn ignores_response_with_missing_message_type() {
        // BEP-5 requires `y`; a message that omits it is malformed and must be ignored.
        let r = r_dict(Some(one_valid_peer_values()), None, None);
        assert!(parse_response(&krpc_message(None, r)).is_none());
    }

    #[test]
    fn ignores_error_message() {
        // KRPC error: y="e", e=[code, msg], no `r`.
        let mut top: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        top.insert(b"t".to_vec(), Bencode::Bytes(b"aa".to_vec()));
        top.insert(b"y".to_vec(), Bencode::Bytes(b"e".to_vec()));
        top.insert(
            b"e".to_vec(),
            Bencode::List(vec![Bencode::Int(201), Bencode::Bytes(b"err".to_vec())]),
        );
        assert!(parse_response(&Bencode::Dict(top).encode()).is_none());
    }

    #[test]
    fn ignores_response_whose_r_is_not_a_dict() {
        // y="r" but `r` is a byte string, not the expected dict — reject deterministically
        // instead of silently treating it as an empty response.
        let raw = krpc_message(Some(b"r"), Bencode::Bytes(b"not-a-dict".to_vec()));
        assert!(parse_response(&raw).is_none());
    }

    #[test]
    fn ignores_partial_trailing_compact_node_record() {
        // One full 26-byte node followed by a truncated fragment: the fragment is dropped,
        // the valid node survives.
        let mut nodes = vec![9u8; 20];
        nodes.extend_from_slice(&[1, 2, 3, 4, 0x1A, 0xE1]); // 1.2.3.4:6881
        nodes.extend_from_slice(&[0xAB; 10]); // partial record
        let r = r_dict(None, Some(Bencode::Bytes(nodes)), None);
        let resp = parse_response(&krpc_message(Some(b"r"), r)).unwrap();
        assert_eq!(resp.nodes.len(), 1);
        assert_eq!(resp.nodes[0].1, "1.2.3.4:6881".parse().unwrap());
    }

    #[test]
    fn skips_invalid_compact_peer_values() {
        // Wrong-length entries, a port-0 entry, and a non-bytes entry are all skipped;
        // only the one valid compact peer survives.
        let vals = Bencode::List(vec![
            Bencode::Bytes(vec![10, 0, 0, 5]),       // too short (4 bytes)
            Bencode::Bytes(vec![10, 0, 0, 6, 0, 0]), // port 0
            Bencode::Bytes(vec![10, 0, 0, 7, 0x04, 0x57, 0xFF]), // too long (7 bytes)
            Bencode::Int(42),                        // not bytes at all
            Bencode::Bytes(vec![10, 0, 0, 1, 0x04, 0x57]), // valid -> 10.0.0.1:1111
        ]);
        let r = r_dict(Some(vals), None, None);
        let resp = parse_response(&krpc_message(Some(b"r"), r)).unwrap();
        assert_eq!(resp.peers, vec!["10.0.0.1:1111".parse().unwrap()]);
    }

    #[test]
    fn ignores_values_that_are_not_a_list() {
        // `values` as a byte string (not a list) yields no peers.
        let r = r_dict(
            Some(Bencode::Bytes(vec![10, 0, 0, 1, 0x04, 0x57])),
            None,
            None,
        );
        let resp = parse_response(&krpc_message(Some(b"r"), r)).unwrap();
        assert!(resp.peers.is_empty());
    }

    #[test]
    fn ignores_nodes_that_are_not_bytes() {
        // `nodes` as a list (not the compact byte string) yields no nodes.
        let r = r_dict(None, Some(Bencode::List(vec![Bencode::Int(1)])), None);
        let resp = parse_response(&krpc_message(Some(b"r"), r)).unwrap();
        assert!(resp.nodes.is_empty());
    }

    #[test]
    fn ignores_token_that_is_not_bytes() {
        // `token` as an int (not bytes) is treated as absent, not coerced.
        let r = r_dict(None, None, Some(Bencode::Int(42)));
        let resp = parse_response(&krpc_message(Some(b"r"), r)).unwrap();
        assert_eq!(resp.token, None);
    }

    #[test]
    fn drops_payload_truncated_at_the_receive_buffer_boundary() {
        // A datagram larger than the 2048-byte receive buffer arrives truncated; the
        // remaining bencode is incomplete and must be rejected, not partially trusted.
        let big_token = vec![0x5A; 4096];
        let r = r_dict(
            Some(one_valid_peer_values()),
            None,
            Some(Bencode::Bytes(big_token)),
        );
        let raw = krpc_message(Some(b"r"), r);
        assert!(raw.len() > 2048, "fixture must exceed the receive buffer");
        assert!(parse_response(&raw[..2048]).is_none());
    }

    #[test]
    fn distance_is_zero_to_self_and_orders() {
        assert_eq!(distance(&[5; 20], &[5; 20]), [0; 20]);
        assert!(distance(&[0; 20], &[1; 20]) < distance(&[0xff; 20], &[0; 20]));
    }

    /// Extract the KRPC transaction id (`t`) from a raw query/response buffer.
    fn buf_txid(buf: &[u8]) -> Vec<u8> {
        Bencode::parse(buf)
            .unwrap()
            .get(b"t")
            .and_then(Bencode::as_bytes)
            .expect("query must carry a transaction id")
            .to_vec()
    }

    /// Build a `get_peers` `values` response echoing `txid` and carrying `peers`.
    fn values_response(txid: &[u8], peers: &[SocketAddrV4]) -> Vec<u8> {
        let vals: Vec<Bencode> = peers
            .iter()
            .map(|p| {
                let mut b = Vec::new();
                b.extend_from_slice(&p.ip().octets());
                b.extend_from_slice(&p.port().to_be_bytes());
                Bencode::Bytes(b)
            })
            .collect();
        let mut r: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        r.insert(b"id".to_vec(), Bencode::Bytes(vec![7; 20]));
        r.insert(b"values".to_vec(), Bencode::List(vals));
        let mut top: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        top.insert(b"t".to_vec(), Bencode::Bytes(txid.to_vec()));
        top.insert(b"y".to_vec(), Bencode::Bytes(b"r".to_vec()));
        top.insert(b"r".to_vec(), Bencode::Dict(r));
        Bencode::Dict(top).encode()
    }

    #[test]
    fn parse_response_extracts_transaction_id() {
        // The transaction id lives at the top level (`t`), not inside `r`; the walk needs it
        // to correlate a reply against the query it answers.
        let resp = values_response(b"Zx", &["10.0.0.9:9000".parse().unwrap()]);
        let got = parse_response(&resp).unwrap();
        assert_eq!(got.txid, b"Zx".to_vec());
    }

    #[tokio::test]
    async fn dht_stream_publishes_first_correlated_reply_before_later_reply() {
        let mut servers = tokio::task::JoinSet::new();
        let mut seeds = Vec::new();
        for (i, delay) in [Duration::ZERO, Duration::from_millis(600)]
            .into_iter()
            .enumerate()
        {
            let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let SocketAddr::V4(seed) = socket.local_addr().unwrap() else {
                unreachable!()
            };
            seeds.push(seed);
            servers.spawn(async move {
                let mut buf = [0; 2048];
                let (n, client) = socket.recv_from(&mut buf).await.unwrap();
                let txid = buf_txid(&buf[..n]);
                tokio::time::sleep(delay).await;
                let peer = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1000 + i as u16);
                socket
                    .send_to(&values_response(&txid, &[peer]), client)
                    .await
                    .unwrap();
            });
        }
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let walk = tokio::spawn(async move {
            dht_stream_from_seeds(&[9; 20], Duration::from_secs(2), 2, seeds, tx).await;
        });
        let first = tokio::time::timeout(Duration::from_millis(250), rx.recv()).await;
        if first.is_err() {
            walk.abort();
            servers.abort_all();
        }
        if first.is_ok() {
            let second = tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert_ne!(first.as_ref().unwrap().as_ref().unwrap(), &second);
        }
        let _ = walk.await;
        while servers.join_next().await.is_some() {}
        assert!(
            first.is_ok(),
            "first correlated peer was hidden until the walk completed"
        );
    }

    #[tokio::test]
    async fn incremental_observer_rejects_wrong_txid_source_replay_and_duplicate_values() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let spoof = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let SocketAddr::V4(seed) = socket.local_addr().unwrap() else {
            unreachable!()
        };
        let expected = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1234);
        let server = tokio::spawn(async move {
            let mut bytes = [0; 2048];
            let (n, client) = socket.recv_from(&mut bytes).await.unwrap();
            let txid = buf_txid(&bytes[..n]);
            let mut wrong = txid.clone();
            wrong.push(0xff);
            let bad = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4321);
            socket
                .send_to(&values_response(&wrong, &[bad]), client)
                .await
                .unwrap();
            spoof
                .send_to(&values_response(&txid, &[bad]), client)
                .await
                .unwrap();
            socket
                .send_to(&values_response(&txid, &[expected, expected]), client)
                .await
                .unwrap();
            socket
                .send_to(&values_response(&txid, &[bad]), client)
                .await
                .unwrap();
        });
        let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
        dht_stream_from_seeds(&[0; 20], Duration::from_millis(150), 8, vec![seed], sender).await;
        server.await.unwrap();
        assert_eq!(receiver.recv().await, Some(expected));
        assert!(
            receiver.recv().await.is_none(),
            "uncorrelated/replayed/duplicate peer reached production observer"
        );
    }

    #[tokio::test]
    async fn dht_response_window_respects_remaining_walk_budget() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let SocketAddr::V4(seed) = server.local_addr().unwrap() else {
            unreachable!()
        };
        let start = Instant::now();
        let peers =
            dht_get_peers_from_seeds(&[9; 20], Duration::from_millis(40), 8, vec![seed]).await;
        let elapsed = start.elapsed();
        assert!(peers.is_empty());
        let mut query = [0; 2048];
        assert!(
            server.try_recv_from(&mut query).is_ok(),
            "actual UDP query required"
        );
        assert!(
            elapsed < Duration::from_millis(250),
            "40ms walk consumed {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn dht_lookup_stops_once_peer_target_is_met() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let seed = match server.local_addr().unwrap() {
            SocketAddr::V4(v4) => v4,
            _ => panic!("test server must be IPv4"),
        };
        let handle = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let (n, peer) = server.recv_from(&mut buf).await.unwrap();
            // A real node echoes the query's transaction id; the client now requires it.
            let txid = buf_txid(&buf[..n]);
            let resp = values_response(
                &txid,
                &[
                    "10.0.0.1:1111".parse().unwrap(),
                    "10.0.0.2:2222".parse().unwrap(),
                ],
            );
            server.send_to(&resp, peer).await.unwrap();
        });

        let start = Instant::now();
        let peers =
            dht_get_peers_from_seeds(&[9u8; 20], Duration::from_secs(5), 2, vec![seed]).await;
        assert_eq!(
            peers,
            vec![
                "10.0.0.1:1111".parse().unwrap(),
                "10.0.0.2:2222".parse().unwrap()
            ]
        );
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "lookup should return as soon as enough peers are available"
        );
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn dht_ignores_response_with_wrong_transaction_id() {
        // A valid-looking response from the queried node but echoing a transaction id we never
        // sent must not contribute peers — it is unrelated (or forged) traffic.
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let seed = match server.local_addr().unwrap() {
            SocketAddr::V4(v4) => v4,
            _ => panic!("test server must be IPv4"),
        };
        let handle = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let (n, peer) = server.recv_from(&mut buf).await.unwrap();
            let real = buf_txid(&buf[..n]);
            // Deliberately corrupt the echoed txid so it cannot match any inflight query.
            let mut wrong = real.clone();
            wrong.push(0xff);
            let resp = values_response(&wrong, &["10.0.0.1:1111".parse().unwrap()]);
            server.send_to(&resp, peer).await.unwrap();
        });

        let peers =
            dht_get_peers_from_seeds(&[9u8; 20], Duration::from_millis(1200), 2, vec![seed]).await;
        assert!(
            peers.is_empty(),
            "response with a mismatched transaction id must be ignored, got {peers:?}"
        );
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn dht_ignores_response_from_wrong_source() {
        // A response echoing the correct transaction id but arriving from an address we never
        // queried must be ignored: an off-path attacker who guesses/observes the txid still
        // cannot inject peers unless the source also matches the inflight query.
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let spoofer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let seed = match server.local_addr().unwrap() {
            SocketAddr::V4(v4) => v4,
            _ => panic!("test server must be IPv4"),
        };
        let handle = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            // The queried node stays silent; the spoofer (different source) replies with the
            // correct txid it observed from the query.
            let (n, peer) = server.recv_from(&mut buf).await.unwrap();
            let txid = buf_txid(&buf[..n]);
            let resp = values_response(&txid, &["10.0.0.1:1111".parse().unwrap()]);
            spoofer.send_to(&resp, peer).await.unwrap();
        });

        let peers =
            dht_get_peers_from_seeds(&[9u8; 20], Duration::from_millis(1200), 2, vec![seed]).await;
        assert!(
            peers.is_empty(),
            "response from an unqueried source must be ignored, got {peers:?}"
        );
        handle.await.unwrap();
    }

    // --- Walk diagnostics / metrics (#43) ---
    //
    // A raw peer count hides useful failure modes (silence, malformed traffic, KRPC errors,
    // uncorrelated packets, frontier exhaustion). `dht_walk_from_seeds` returns a
    // `DhtWalkMetrics` so those modes are observable and deterministically testable offline.

    /// Build a `get_peers` response echoing `txid` with the given compact peers and nodes.
    fn response_with(
        txid: &[u8],
        peers: &[SocketAddrV4],
        nodes: &[([u8; 20], SocketAddrV4)],
    ) -> Vec<u8> {
        let vals: Vec<Bencode> = peers
            .iter()
            .map(|p| {
                let mut b = Vec::new();
                b.extend_from_slice(&p.ip().octets());
                b.extend_from_slice(&p.port().to_be_bytes());
                Bencode::Bytes(b)
            })
            .collect();
        let mut node_bytes = Vec::new();
        for (id, addr) in nodes {
            node_bytes.extend_from_slice(id);
            node_bytes.extend_from_slice(&addr.ip().octets());
            node_bytes.extend_from_slice(&addr.port().to_be_bytes());
        }
        let mut r: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        r.insert(b"id".to_vec(), Bencode::Bytes(vec![7; 20]));
        if !peers.is_empty() {
            r.insert(b"values".to_vec(), Bencode::List(vals));
        }
        if !nodes.is_empty() {
            r.insert(b"nodes".to_vec(), Bencode::Bytes(node_bytes));
        }
        let mut top: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        top.insert(b"t".to_vec(), Bencode::Bytes(txid.to_vec()));
        top.insert(b"y".to_vec(), Bencode::Bytes(b"r".to_vec()));
        top.insert(b"r".to_vec(), Bencode::Dict(r));
        Bencode::Dict(top).encode()
    }

    /// Build a KRPC error message (`y == "e"`) echoing `txid`.
    fn error_response(txid: &[u8]) -> Vec<u8> {
        let mut top: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        top.insert(b"t".to_vec(), Bencode::Bytes(txid.to_vec()));
        top.insert(b"y".to_vec(), Bencode::Bytes(b"e".to_vec()));
        top.insert(
            b"e".to_vec(),
            Bencode::List(vec![Bencode::Int(201), Bencode::Bytes(b"err".to_vec())]),
        );
        Bencode::Dict(top).encode()
    }

    fn v4_addr(sock: &UdpSocket) -> SocketAddrV4 {
        match sock.local_addr().unwrap() {
            SocketAddr::V4(v4) => v4,
            _ => panic!("test socket must be IPv4"),
        }
    }

    #[tokio::test]
    async fn walk_metrics_count_a_successful_response_path() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let seed = v4_addr(&server);
        let handle = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let (n, peer) = server.recv_from(&mut buf).await.unwrap();
            let txid = buf_txid(&buf[..n]);
            let resp = response_with(
                &txid,
                &[
                    "10.0.0.1:1111".parse().unwrap(),
                    "10.0.0.2:2222".parse().unwrap(),
                ],
                &[([9u8; 20], "1.2.3.4:6881".parse().unwrap())],
            );
            server.send_to(&resp, peer).await.unwrap();
        });

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut seen: BTreeSet<SocketAddrV4> = BTreeSet::new();
        let metrics = dht_walk_from_seeds(
            &[9u8; 20],
            Duration::from_secs(5),
            &client,
            vec![seed],
            |_src, resp| {
                seen.extend(resp.peers.iter().copied());
                seen.len() >= 2
            },
        )
        .await;

        assert_eq!(metrics.bootstrap_seeded, 1);
        assert_eq!(metrics.nodes_queried, 1);
        assert_eq!(metrics.valid_responses, 1);
        assert_eq!(metrics.peers_discovered, 2);
        assert_eq!(metrics.nodes_discovered, 1);
        assert_eq!(metrics.malformed_responses, 0);
        assert_eq!(metrics.krpc_errors, 0);
        assert_eq!(metrics.uncorrelated_responses, 0);
        assert_eq!(metrics.timeouts, 0);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn walk_metrics_count_ignored_malformed_and_error_traffic() {
        // One seed replies with a well-formed KRPC error, another with non-bencode garbage;
        // neither yields peers, and each is bucketed by its own failure mode.
        let err_server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let garbage_server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let err_seed = v4_addr(&err_server);
        let garbage_seed = v4_addr(&garbage_server);

        let h1 = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let (n, peer) = err_server.recv_from(&mut buf).await.unwrap();
            let txid = buf_txid(&buf[..n]);
            err_server
                .send_to(&error_response(&txid), peer)
                .await
                .unwrap();
        });
        let h2 = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let (_n, peer) = garbage_server.recv_from(&mut buf).await.unwrap();
            garbage_server
                .send_to(b"not-a-bencode-datagram", peer)
                .await
                .unwrap();
        });

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let metrics = dht_walk_from_seeds(
            &[9u8; 20],
            Duration::from_secs(5),
            &client,
            vec![err_seed, garbage_seed],
            |_src, _resp| false,
        )
        .await;

        assert_eq!(metrics.valid_responses, 0);
        assert_eq!(metrics.peers_discovered, 0);
        assert_eq!(metrics.krpc_errors, 1);
        assert_eq!(metrics.malformed_responses, 1);
        assert!(metrics.frontier_exhausted);
        h1.await.unwrap();
        h2.await.unwrap();
    }

    #[tokio::test]
    async fn walk_metrics_count_a_silent_round_as_a_timeout() {
        // A seed that receives the query but never answers: the response window elapses in
        // total silence, which is a timeout, and the walk then exhausts its frontier.
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let seed = v4_addr(&server);
        // Keep `server` bound (so the port stays reachable, no ICMP unreachable) but silent.

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let metrics = dht_walk_from_seeds(
            &[9u8; 20],
            Duration::from_secs(5),
            &client,
            vec![seed],
            |_src, _resp| false,
        )
        .await;

        assert_eq!(metrics.valid_responses, 0);
        assert_eq!(metrics.timeouts, 1);
        assert!(metrics.frontier_exhausted);
        drop(server);
    }

    #[tokio::test]
    async fn walk_metrics_count_an_uncorrelated_response() {
        // Correct source, wrong transaction id: the datagram arrives (so it is not a timeout)
        // but fails correlation, so it contributes no peers and is bucketed as uncorrelated.
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let seed = v4_addr(&server);
        let handle = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let (n, peer) = server.recv_from(&mut buf).await.unwrap();
            let mut wrong = buf_txid(&buf[..n]);
            wrong.push(0xff);
            let resp = response_with(&wrong, &["10.0.0.1:1111".parse().unwrap()], &[]);
            server.send_to(&resp, peer).await.unwrap();
        });

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let metrics = dht_walk_from_seeds(
            &[9u8; 20],
            Duration::from_secs(5),
            &client,
            vec![seed],
            |_src, _resp| false,
        )
        .await;

        assert_eq!(metrics.valid_responses, 0);
        assert_eq!(metrics.peers_discovered, 0);
        assert_eq!(metrics.uncorrelated_responses, 1);
        assert_eq!(metrics.timeouts, 0);
        handle.await.unwrap();
    }

    // Live DHT lookup against a real infohash:
    //   ACE_INFOHASH=50e935...2d6e47 cargo test -p ace-swarm dht_live -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn dht_live_finds_peers() {
        let hex = std::env::var("ACE_INFOHASH").expect("set ACE_INFOHASH=40hex");
        let mut ih = [0u8; 20];
        for i in 0..20 {
            ih[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap();
        }
        let peers = dht_get_peers(&ih, std::time::Duration::from_secs(20)).await;
        println!("DHT found {} peer(s):", peers.len());
        for p in &peers {
            println!("  {p}");
        }
        assert!(!peers.is_empty(), "DHT returned no peers");
    }

    // Live DHT self-announce against a real infohash — confirms we can get tokens from real
    // nodes and send announce_peer without erroring. Doesn't (can't, from one host) prove a
    // third party subsequently finds us; that's what Task 7's reverse-direction proof needs.
    //   ACE_INFOHASH=50e935...2d6e47 cargo test -p ace-swarm dht_live_announce -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn dht_live_announce_sends_without_erroring() {
        let hex = std::env::var("ACE_INFOHASH").expect("set ACE_INFOHASH=40hex");
        let mut ih = [0u8; 20];
        for i in 0..20 {
            ih[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap();
        }
        let announced = dht_announce_peer(&ih, 8621, std::time::Duration::from_secs(20)).await;
        println!("DHT announce_peer sent to {announced} node(s)");
        assert!(
            announced > 0,
            "no node handed us a token to announce back with"
        );
    }
}
