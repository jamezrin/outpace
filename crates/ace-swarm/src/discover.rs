//! Peer discovery: announce to the stream's UDP trackers (BEP-15) and aggregate the unique
//! peers they return. DHT discovery is a documented follow-up (see the design spec).

use crate::dht::dht_get_peers;
use ace_tracker::client::announce;
use ace_tracker::codec::TransferState;
use std::collections::BTreeSet;
use std::net::SocketAddrV4;
use std::time::Duration;
use tokio::net::lookup_host;

/// Resolve `udp://host:port[/...]` tracker URLs to socket addresses.
pub async fn resolve_trackers(trackers: &[String]) -> Vec<SocketAddrV4> {
    let mut out = Vec::new();
    for t in trackers {
        let hostport = t
            .strip_prefix("udp://")
            .unwrap_or(t)
            .split('/')
            .next()
            .unwrap_or("");
        if hostport.is_empty() {
            continue;
        }
        if let Ok(addrs) = lookup_host(hostport).await {
            for a in addrs {
                if let std::net::SocketAddr::V4(v4) = a {
                    out.push(v4);
                    break; // one resolved addr per tracker is enough
                }
            }
        }
    }
    out
}

/// Discover peers for `infohash` from both the UDP trackers and the mainline DHT, returning
/// their union. Acestream swarms are largely DHT-populated, so DHT is the primary source;
/// tracker announces are best-effort and skipped on failure.
pub async fn discover_peers(
    trackers: &[String],
    infohash: &[u8; 20],
    peer_id: &[u8; 20],
    port: u16,
) -> Vec<SocketAddrV4> {
    let mut peers: BTreeSet<SocketAddrV4> = BTreeSet::new();
    let transfer = TransferState { downloaded: 0, left: u64::MAX, uploaded: 0 };
    for tracker in resolve_trackers(trackers).await {
        if let Ok(found) = announce(tracker, infohash, peer_id, port, 200, transfer).await {
            peers.extend(found);
        }
    }
    peers.extend(dht_get_peers(infohash, Duration::from_secs(15)).await);
    peers.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolve_strips_scheme_and_path() {
        // 127.0.0.1 resolves without network; the path/scheme must be stripped.
        let got = resolve_trackers(&["udp://127.0.0.1:80/announce".into()]).await;
        assert_eq!(got, vec!["127.0.0.1:80".parse().unwrap()]);
    }

    #[tokio::test]
    async fn resolve_skips_garbage() {
        let got = resolve_trackers(&["".into(), "udp://".into()]).await;
        assert!(got.is_empty());
    }
}
