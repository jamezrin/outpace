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

/// Parsed `get_peers` response: any peers handed to us, plus closer nodes to continue from.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct GetPeersResponse {
    pub peers: Vec<SocketAddrV4>,
    pub nodes: Vec<([u8; 20], SocketAddrV4)>,
}

/// Parse a KRPC response, extracting `r.values` (peers) and `r.nodes` (compact nodes).
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
    Some(SocketAddrV4::new(Ipv4Addr::new(b[0], b[1], b[2], b[3]), port))
}

/// XOR distance between a node id and the target (BTreeSet-orderable key).
fn distance(id: &[u8; 20], target: &[u8; 20]) -> [u8; 20] {
    let mut d = [0u8; 20];
    for i in 0..20 {
        d[i] = id[i] ^ target[i];
    }
    d
}

/// Iterative `get_peers` toward `infohash`, bounded by `budget`. Returns discovered peers.
pub async fn dht_get_peers(infohash: &[u8; 20], budget: Duration) -> Vec<SocketAddrV4> {
    let sock = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    let node_id: [u8; 20] = rand::random();

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

    let mut queried: HashSet<SocketAddrV4> = HashSet::new();
    let mut peers: BTreeSet<SocketAddrV4> = BTreeSet::new();
    let deadline = Instant::now() + budget;
    let mut buf = [0u8; 2048];
    eprintln!("[dht] seeded {} bootstrap node(s)", frontier.len());

    while Instant::now() < deadline && peers.len() < 30 {
        // Send to up to 8 closest not-yet-queried nodes.
        let batch: Vec<SocketAddrV4> = frontier
            .values()
            .filter(|a| !queried.contains(a))
            .take(8)
            .copied()
            .collect();
        if batch.is_empty() {
            eprintln!("[dht] frontier exhausted: queried={} peers={}", queried.len(), peers.len());
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
                Ok(Ok((n, _src))) => {
                    if let Some(resp) = parse_response(&buf[..n]) {
                        peers.extend(resp.peers);
                        for (id, addr) in resp.nodes {
                            frontier.entry(distance(&id, infohash)).or_insert(addr);
                        }
                        // Cap frontier growth: keep the 64 closest.
                        while frontier.len() > 64 {
                            let last = *frontier.keys().next_back().unwrap();
                            frontier.remove(&last);
                        }
                    }
                }
                _ => break,
            }
        }
    }

    peers.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_peers_query_roundtrips() {
        let q = build_get_peers(&[1u8; 20], &[2u8; 20], b"aa");
        let d = Bencode::parse(&q).unwrap();
        assert_eq!(d.get(b"q").unwrap().as_bytes(), Some(b"get_peers".as_slice()));
        assert_eq!(d.get(b"y").unwrap().as_bytes(), Some(b"q".as_slice()));
        let a = d.get(b"a").unwrap();
        assert_eq!(a.get(b"info_hash").unwrap().as_bytes(), Some([2u8; 20].as_slice()));
    }

    #[test]
    fn parses_values_peers() {
        // r = { id:20, token:.., values: [ "<4 ip><2 port>" ] }
        let mut r: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        r.insert(b"id".to_vec(), Bencode::Bytes(vec![0; 20]));
        let peer = vec![87u8, 221, 96, 148, 0x21, 0xAD]; // 87.221.96.148:8621
        r.insert(b"values".to_vec(), Bencode::List(vec![Bencode::Bytes(peer)]));
        let mut top: BTreeMap<Vec<u8>, Bencode> = BTreeMap::new();
        top.insert(b"r".to_vec(), Bencode::Dict(r));
        top.insert(b"y".to_vec(), Bencode::Bytes(b"r".to_vec()));
        let resp = parse_response(&Bencode::Dict(top).encode()).unwrap();
        assert_eq!(resp.peers, vec!["87.221.96.148:8621".parse().unwrap()]);
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
}
