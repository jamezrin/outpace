//! Peer discovery: announce to the stream's UDP trackers (BEP-15) and aggregate the unique
//! peers they return. Mainline DHT self-announce (`dht_announce_peer`, BEP-5) is a separate,
//! composable primitive in `crate::dht` — callers that want both combine them explicitly
//! (see `ace_engine::ace_provider`'s periodic self-announce), rather than baking a
//! multi-second live network call into this module's fast, offline-testable functions.

use crate::dht::dht_get_peers_with_target;
use ace_tracker::client::announce;
use ace_tracker::codec::{AnnounceEvent, TransferState};
use std::collections::BTreeSet;
use std::future::Future;
use std::net::SocketAddrV4;
use std::time::Duration;
use tokio::net::lookup_host;

const DISCOVERY_PEER_TARGET: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiscoveryOptions {
    pub peer_target: usize,
    pub dht_budget: Duration,
}

impl Default for DiscoveryOptions {
    fn default() -> Self {
        DiscoveryOptions {
            peer_target: DISCOVERY_PEER_TARGET,
            dht_budget: Duration::from_secs(15),
        }
    }
}

/// How the resolver treats a tracker list. Tracker URLs from a `cid:<40hex>` transport come
/// from an untrusted metadata peer, so by default we refuse to turn them into DNS lookups and
/// UDP announce traffic aimed at non-globally-routable hosts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TrackerPolicy {
    /// Allow private/loopback/link-local/multicast destinations. Off by default (`false`); opt
    /// in only for trusted/local deployments (or offline tests).
    pub allow_non_global: bool,
}

/// Maximum number of tracker URLs processed from one (untrusted) list.
pub const MAX_TRACKERS: usize = 64;
/// Maximum accepted length of a single tracker URL string.
pub const MAX_TRACKER_URL_LEN: usize = 256;

/// Resolve `udp://host:port[/...]` tracker URLs to socket addresses under the default policy
/// (reject non-global destinations). See [`resolve_trackers_with_policy`].
pub async fn resolve_trackers(trackers: &[String]) -> Vec<SocketAddrV4> {
    resolve_trackers_with_policy(trackers, TrackerPolicy::default()).await
}

/// Resolve `udp://host:port[/...]` tracker URLs to socket addresses under `policy`.
pub async fn resolve_trackers_with_policy(
    trackers: &[String],
    policy: TrackerPolicy,
) -> Vec<SocketAddrV4> {
    let mut out = Vec::new();
    for t in trackers.iter().take(MAX_TRACKERS) {
        if t.len() > MAX_TRACKER_URL_LEN {
            continue;
        }
        // Require an explicit udp:// scheme; a bare host:port is rejected.
        let Some(rest) = t.strip_prefix("udp://") else {
            continue;
        };
        let hostport = rest.split('/').next().unwrap_or("");
        if hostport.is_empty() {
            continue;
        }
        if let Ok(addrs) = lookup_host(hostport).await {
            for a in addrs {
                if let std::net::SocketAddr::V4(v4) = a {
                    if policy.allow_non_global || !is_non_global_v4(v4.ip()) {
                        out.push(v4);
                    }
                    break; // one resolved addr per tracker is enough
                }
            }
        }
    }
    out
}

/// True for IPv4 destinations we refuse to send untrusted-tracker traffic to by default:
/// loopback, private, link-local (incl. the 169.254.169.254 cloud metadata endpoint),
/// multicast, broadcast, unspecified, and documentation ranges.
fn is_non_global_v4(ip: &std::net::Ipv4Addr) -> bool {
    ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_broadcast()
        || ip.is_unspecified()
        || ip.is_documentation()
}

/// Discover peers for `infohash` from both the UDP trackers and the mainline DHT. A source
/// that reaches the target peer count can return immediately; otherwise we wait for and merge
/// the other source so one weak tracker response does not crowd out the DHT. Acestream swarms
/// are largely DHT-populated, so DHT is the primary source; tracker announces are best-effort
/// and skipped on failure.
pub async fn discover_peers(
    trackers: &[String],
    infohash: &[u8; 20],
    peer_id: &[u8; 20],
    port: u16,
) -> Vec<SocketAddrV4> {
    discover_peers_with_options(
        trackers,
        infohash,
        peer_id,
        port,
        DiscoveryOptions::default(),
    )
    .await
}

pub async fn discover_peers_with_options(
    trackers: &[String],
    infohash: &[u8; 20],
    peer_id: &[u8; 20],
    port: u16,
    options: DiscoveryOptions,
) -> Vec<SocketAddrV4> {
    first_peer_source_with_target(
        discover_tracker_peers(
            trackers,
            infohash,
            peer_id,
            port,
            AnnounceEvent::Started,
            u64::MAX,
        ),
        dht_get_peers_with_target(infohash, options.dht_budget, options.peer_target.max(1)),
        options.peer_target.max(1),
    )
    .await
}

async fn discover_tracker_peers(
    trackers: &[String],
    infohash: &[u8; 20],
    peer_id: &[u8; 20],
    port: u16,
    event: AnnounceEvent,
    left: u64,
) -> Vec<SocketAddrV4> {
    let mut peers: BTreeSet<SocketAddrV4> = BTreeSet::new();
    let transfer = TransferState {
        downloaded: 0,
        left,
        uploaded: 0,
    };
    for tracker in resolve_trackers(trackers).await {
        if let Ok(found) = announce(tracker, infohash, peer_id, port, 200, transfer, event).await {
            peers.extend(found);
        }
    }
    peers.into_iter().collect()
}

async fn first_peer_source_with_target<A, B>(a: A, b: B, peer_target: usize) -> Vec<SocketAddrV4>
where
    A: Future<Output = Vec<SocketAddrV4>>,
    B: Future<Output = Vec<SocketAddrV4>>,
{
    tokio::pin!(a);
    tokio::pin!(b);
    tokio::select! {
        mut peers = &mut a => {
            if peers.len() >= peer_target {
                unique_peers(peers)
            } else {
                peers.extend(b.await);
                unique_peers(peers)
            }
        }
        mut peers = &mut b => {
            if peers.len() >= peer_target {
                unique_peers(peers)
            } else {
                peers.extend(a.await);
                unique_peers(peers)
            }
        }
    }
}

fn unique_peers(peers: Vec<SocketAddrV4>) -> Vec<SocketAddrV4> {
    peers
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Announce ourselves as a SEEDER (`left=0`, event=Completed) for `infohash` to all
/// `trackers`, aggregating the peers each tracker returns (best-effort — a non-responding
/// tracker is skipped, mirroring `discover_peers`). A seeder still benefits from knowing
/// other peers. Tracker-only: see `crate::dht::dht_announce_peer` for the DHT half — real
/// Acestream swarms are largely DHT-populated (see `README.md`), so callers that want
/// full self-announce coverage should call both (as `ace_engine::ace_provider`'s periodic
/// self-announce does, Task 7 approach (2), `docs/protocol/notes/21-seeder-ground-truth.md`).
pub async fn announce_seeder(
    trackers: &[String],
    infohash: &[u8; 20],
    peer_id: &[u8; 20],
    port: u16,
) -> Vec<SocketAddrV4> {
    discover_tracker_peers(
        trackers,
        infohash,
        peer_id,
        port,
        AnnounceEvent::Completed,
        0,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Policy that permits loopback so scheme/path handling can be exercised offline.
    fn local_ok() -> TrackerPolicy {
        TrackerPolicy {
            allow_non_global: true,
        }
    }

    #[tokio::test]
    async fn resolve_strips_scheme_and_path() {
        // 127.0.0.1 resolves without network; the path/scheme must be stripped.
        let got =
            resolve_trackers_with_policy(&["udp://127.0.0.1:80/announce".into()], local_ok()).await;
        assert_eq!(got, vec!["127.0.0.1:80".parse().unwrap()]);
    }

    #[tokio::test]
    async fn resolve_skips_garbage() {
        let got = resolve_trackers(&["".into(), "udp://".into()]).await;
        assert!(got.is_empty());
    }

    #[tokio::test]
    async fn resolve_requires_udp_scheme() {
        // A bare host:port (no udp://) must be rejected even when non-global is allowed.
        let got = resolve_trackers_with_policy(&["127.0.0.1:80".into()], local_ok()).await;
        assert!(got.is_empty());
    }

    #[tokio::test]
    async fn resolve_rejects_non_global_destinations_by_default() {
        // Loopback/private/link-local (incl. the 169.254.169.254 metadata endpoint) must not
        // be contacted for untrusted trackers unless explicitly allowed.
        let got = resolve_trackers(&[
            "udp://127.0.0.1:80/announce".into(),
            "udp://10.0.0.1:80".into(),
            "udp://169.254.169.254:80".into(),
        ])
        .await;
        assert!(got.is_empty());
    }

    #[tokio::test]
    async fn resolve_allows_non_global_when_configured() {
        let got =
            resolve_trackers_with_policy(&["udp://127.0.0.1:80/announce".into()], local_ok()).await;
        assert_eq!(got, vec!["127.0.0.1:80".parse().unwrap()]);
    }

    #[tokio::test]
    async fn resolve_caps_tracker_count() {
        // Distinct ports so dedup does not collapse them; only the first MAX_TRACKERS resolve.
        let trackers: Vec<String> = (0..MAX_TRACKERS + 10)
            .map(|i| format!("udp://127.0.0.1:{}", 1000 + i))
            .collect();
        let got = resolve_trackers_with_policy(&trackers, local_ok()).await;
        assert_eq!(got.len(), MAX_TRACKERS);
    }

    #[tokio::test]
    async fn resolve_rejects_overlong_urls() {
        let overlong = format!("udp://{}:80", "a".repeat(MAX_TRACKER_URL_LEN));
        let got =
            resolve_trackers_with_policy(&[overlong, "udp://127.0.0.1:80".into()], local_ok()).await;
        assert_eq!(got, vec!["127.0.0.1:80".parse().unwrap()]);
    }

    #[tokio::test]
    async fn announce_seeder_returns_empty_on_unreachable_tracker() {
        let peers = announce_seeder(
            &["udp://127.0.0.1:1/announce".into()],
            &[0u8; 20],
            &[0u8; 20],
            6881,
        )
        .await;
        assert!(peers.is_empty());
    }

    #[test]
    fn discovery_options_default_to_fast_start_target() {
        let opts = DiscoveryOptions::default();
        assert_eq!(opts.peer_target, DISCOVERY_PEER_TARGET);
        assert_eq!(opts.dht_budget, Duration::from_secs(15));
    }

    #[tokio::test]
    async fn peer_discovery_returns_fast_nonempty_source_without_waiting_for_slow_source() {
        let fast = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            vec!["10.0.0.1:1111".parse().unwrap()]
        };
        let slow = async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Vec::new()
        };

        let start = std::time::Instant::now();
        let peers = first_peer_source_with_target(slow, fast, 1).await;
        assert_eq!(peers, vec!["10.0.0.1:1111".parse().unwrap()]);
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "discovery should not wait for a slow empty source once another source found peers"
        );
    }

    #[tokio::test]
    async fn peer_discovery_waits_for_second_source_when_first_has_too_few_peers() {
        let weak = async { vec!["10.0.0.1:1111".parse().unwrap()] };
        let strong = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            vec![
                "10.0.0.2:2222".parse().unwrap(),
                "10.0.0.3:3333".parse().unwrap(),
            ]
        };

        let peers = first_peer_source_with_target(weak, strong, 2).await;
        assert_eq!(
            peers,
            vec![
                "10.0.0.1:1111".parse().unwrap(),
                "10.0.0.2:2222".parse().unwrap(),
                "10.0.0.3:3333".parse().unwrap()
            ]
        );
    }
}
