//! Mainline DHT (BEP-5) `get_peers` — discover swarm peers for an infohash without a
//! tracker. Acestream populates the swarm on the public DHT, so an iterative `get_peers`
//! lookup toward the infohash returns the same peers the official engine connects to.
//!
//! This is a focused, time-bounded *lookup client* (not a full routing-table node): it
//! bootstraps off the well-known routers, walks closer to the target collecting `nodes`,
//! and harvests any `values` (peers) it's handed along the way.

use ace_wire::bencode::Bencode;
use std::collections::{BTreeMap, BTreeSet, HashSet};
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
}

/// Parse a KRPC response, extracting `r.values` (peers), `r.nodes` (compact nodes), and
/// `r.token` (needed to `announce_peer` back to whichever node sent this response).
pub fn parse_response(buf: &[u8]) -> Option<GetPeersResponse> {
    let v = Bencode::parse(buf).ok()?;
    let r = v.get(b"r")?;
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
        for c in nb.chunks_exact(26) {
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
    Some(out)
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
) {
    // Frontier of candidate nodes keyed by XOR distance; seed with bootstrap routers.
    let mut frontier: BTreeMap<[u8; 20], SocketAddrV4> = BTreeMap::new();
    for (i, host) in BOOTSTRAP.iter().enumerate() {
        if let Ok(addrs) = tokio::net::lookup_host(host).await {
            for a in addrs {
                if let SocketAddr::V4(v4) = a {
                    // Bootstrap ids unknown; use distinct max-distance keys so real nodes
                    // outrank them AND the bootstraps don't collide on one map key.
                    let mut key = [0xffu8; 20];
                    key[19] = i as u8;
                    frontier.insert(key, v4);
                    break;
                }
            }
        }
    }
    dht_walk_frontier(infohash, budget, sock, frontier, on_response).await;
}

#[cfg(test)]
async fn dht_walk_from_seeds(
    infohash: &[u8; 20],
    budget: Duration,
    sock: &UdpSocket,
    seeds: Vec<SocketAddrV4>,
    on_response: impl FnMut(SocketAddrV4, &GetPeersResponse) -> bool,
) {
    let mut frontier: BTreeMap<[u8; 20], SocketAddrV4> = BTreeMap::new();
    for (i, seed) in seeds.into_iter().enumerate() {
        let mut key = [0xffu8; 20];
        key[19] = i as u8;
        frontier.insert(key, seed);
    }
    dht_walk_frontier(infohash, budget, sock, frontier, on_response).await;
}

async fn dht_walk_frontier(
    infohash: &[u8; 20],
    budget: Duration,
    sock: &UdpSocket,
    mut frontier: BTreeMap<[u8; 20], SocketAddrV4>,
    mut on_response: impl FnMut(SocketAddrV4, &GetPeersResponse) -> bool,
) {
    let node_id: [u8; 20] = rand::random();

    let mut queried: HashSet<SocketAddrV4> = HashSet::new();
    let deadline = Instant::now() + budget;
    let mut buf = [0u8; 2048];
    crate::swarm_log!("[dht] seeded {} bootstrap node(s)", frontier.len());

    'outer: while Instant::now() < deadline {
        // Send to up to 8 closest not-yet-queried nodes.
        let batch: Vec<SocketAddrV4> = frontier
            .values()
            .filter(|a| !queried.contains(a))
            .take(8)
            .copied()
            .collect();
        if batch.is_empty() {
            crate::swarm_log!("[dht] frontier exhausted: queried={}", queried.len());
            break;
        }
        for addr in &batch {
            queried.insert(*addr);
            let q = build_get_peers(&node_id, infohash, b"sc");
            let _ = sock.send_to(&q, SocketAddr::V4(*addr)).await;
        }

        // Collect responses for a short window.
        let window = Instant::now() + Duration::from_millis(1500);
        while Instant::now() < window {
            let remaining = window.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, sock.recv_from(&mut buf)).await {
                Ok(Ok((n, src))) => {
                    // Bootstrap/frontier addresses are always v4 (see below), so a real reply
                    // is always from a v4 peer; skip (not break — keep collecting within the
                    // window) on the theoretical v6 case rather than treating it as a timeout.
                    let SocketAddr::V4(src) = src else { continue };
                    if let Some(resp) = parse_response(&buf[..n]) {
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
                }
                _ => break,
            }
        }
    }
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
    let sock = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    let mut peers: BTreeSet<SocketAddrV4> = BTreeSet::new();
    dht_walk(infohash, budget, &sock, |_src, resp| {
        peers.extend(resp.peers.iter().copied());
        peers.len() >= peer_target
    })
    .await;
    peers.into_iter().collect()
}

#[cfg(test)]
async fn dht_get_peers_from_seeds(
    infohash: &[u8; 20],
    budget: Duration,
    peer_target: usize,
    seeds: Vec<SocketAddrV4>,
) -> Vec<SocketAddrV4> {
    let sock = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    let mut peers: BTreeSet<SocketAddrV4> = BTreeSet::new();
    dht_walk_from_seeds(infohash, budget, &sock, seeds, |_src, resp| {
        peers.extend(resp.peers.iter().copied());
        peers.len() >= peer_target
    })
    .await;
    peers.into_iter().collect()
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
    for (addr, token) in &tokens {
        let q = build_announce_peer(&node_id, infohash, peer_port, token, b"sc");
        if sock.send_to(&q, SocketAddr::V4(*addr)).await.is_ok() {
            announced += 1;
        }
    }
    announced
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let resp = parse_response(&Bencode::Dict(top).encode()).unwrap();
        assert_eq!(resp.token, Some(b"opaque-tok".to_vec()));
    }

    #[test]
    fn no_token_parses_as_none() {
        let mut r: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        r.insert(b"id".to_vec(), Bencode::Bytes(vec![0; 20]));
        let mut top: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        top.insert(b"r".to_vec(), Bencode::Dict(r));
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
        let resp = parse_response(&Bencode::Dict(top).encode()).unwrap();
        assert_eq!(resp.nodes.len(), 1);
        assert_eq!(resp.nodes[0].1, "1.2.3.4:6881".parse().unwrap());
    }

    #[test]
    fn distance_is_zero_to_self_and_orders() {
        assert_eq!(distance(&[5; 20], &[5; 20]), [0; 20]);
        assert!(distance(&[0; 20], &[1; 20]) < distance(&[0xff; 20], &[0; 20]));
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
            let (_n, peer) = server.recv_from(&mut buf).await.unwrap();
            let mut peer1 = Vec::new();
            peer1.extend_from_slice(&[10, 0, 0, 1]);
            peer1.extend_from_slice(&1111u16.to_be_bytes());
            let mut peer2 = Vec::new();
            peer2.extend_from_slice(&[10, 0, 0, 2]);
            peer2.extend_from_slice(&2222u16.to_be_bytes());

            let mut r: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
            r.insert(b"id".to_vec(), Bencode::Bytes(vec![7; 20]));
            r.insert(
                b"values".to_vec(),
                Bencode::List(vec![Bencode::Bytes(peer1), Bencode::Bytes(peer2)]),
            );
            let mut top: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
            top.insert(b"r".to_vec(), Bencode::Dict(r));
            server
                .send_to(&Bencode::Dict(top).encode(), peer)
                .await
                .unwrap();
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
