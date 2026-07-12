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
///
/// Set `OUTPACE_TRACKER_ALLOW_NON_GLOBAL=1` to opt into the permissive policy in production
/// (see [`TrackerPolicy::from_env`]) for controlled/self-hosted deployments with a private
/// tracker; any other value, including unset, keeps the default deny.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TrackerPolicy {
    /// Allow private/loopback/link-local/multicast destinations. Off by default (`false`); opt
    /// in only for trusted/local deployments (or offline tests).
    pub allow_non_global: bool,
}

impl TrackerPolicy {
    /// Build a policy from the environment: `OUTPACE_TRACKER_ALLOW_NON_GLOBAL=1` allows
    /// non-globally-routable tracker destinations; anything else (including unset) denies.
    pub fn from_env() -> Self {
        Self::from_env_value(
            std::env::var("OUTPACE_TRACKER_ALLOW_NON_GLOBAL")
                .ok()
                .as_deref(),
        )
    }

    /// Pure parse of the `OUTPACE_TRACKER_ALLOW_NON_GLOBAL` value: only `Some("1")` allows.
    fn from_env_value(value: Option<&str>) -> Self {
        TrackerPolicy {
            allow_non_global: value == Some("1"),
        }
    }
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
    for tracker in resolve_trackers_with_policy(trackers, TrackerPolicy::from_env()).await {
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

    #[test]
    fn tracker_policy_env_value_parses_to_deny_unless_exactly_one() {
        // Pure parse, no env mutation: only the exact value "1" opts in.
        assert!(!TrackerPolicy::from_env_value(None).allow_non_global);
        assert!(!TrackerPolicy::from_env_value(Some("0")).allow_non_global);
        assert!(!TrackerPolicy::from_env_value(Some("true")).allow_non_global);
        assert!(!TrackerPolicy::from_env_value(Some("")).allow_non_global);
        assert!(TrackerPolicy::from_env_value(Some("1")).allow_non_global);
    }

    /// Restores the prior `OUTPACE_TRACKER_ALLOW_NON_GLOBAL` value on drop (even on panic).
    struct EnvGuard(Option<std::ffi::OsString>);

    impl EnvGuard {
        fn capture() -> Self {
            EnvGuard(std::env::var_os("OUTPACE_TRACKER_ALLOW_NON_GLOBAL"))
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match self.0.take() {
                Some(value) => std::env::set_var("OUTPACE_TRACKER_ALLOW_NON_GLOBAL", value),
                None => std::env::remove_var("OUTPACE_TRACKER_ALLOW_NON_GLOBAL"),
            }
        }
    }

    /// Guards the wiring in `discover_tracker_peers`: it must resolve with
    /// `TrackerPolicy::from_env()`, not the default policy. Without the env opt-in a loopback
    /// tracker must never be contacted; with `OUTPACE_TRACKER_ALLOW_NON_GLOBAL=1` the announce
    /// must actually reach the socket. This is the only test that mutates the env var.
    #[tokio::test]
    async fn announce_seeder_contacts_loopback_tracker_only_with_env_opt_in() {
        let _restore = EnvGuard::capture();

        let server = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let tracker_url = format!("udp://{}/announce", server.local_addr().unwrap());

        // Default deny: the resolve must filter loopback, so no packet reaches the socket.
        std::env::remove_var("OUTPACE_TRACKER_ALLOW_NON_GLOBAL");
        let peers = announce_seeder(
            std::slice::from_ref(&tracker_url),
            &[3u8; 20],
            &[4u8; 20],
            6881,
        )
        .await;
        assert!(peers.is_empty());
        let mut buf = [0u8; 2048];
        assert!(
            server.try_recv_from(&mut buf).is_err(),
            "default deny must not send announce traffic to a loopback tracker"
        );

        // Fake BEP-15 tracker: one connect + one announce with 0 peers (layout per
        // `ace_tracker::codec`); completing the exchange proves the announce reached it.
        let served = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let (n, peer) = server.recv_from(&mut buf).await.unwrap();
            assert_eq!(n, 16, "connect request");
            let txid = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
            let mut resp = Vec::new();
            resp.extend_from_slice(&0u32.to_be_bytes()); // action = connect
            resp.extend_from_slice(&txid.to_be_bytes());
            resp.extend_from_slice(&42u64.to_be_bytes()); // connection id
            server.send_to(&resp, peer).await.unwrap();

            let (_n, peer) = server.recv_from(&mut buf).await.unwrap();
            let atxid = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
            let mut ar = Vec::new();
            ar.extend_from_slice(&1u32.to_be_bytes()); // action = announce
            ar.extend_from_slice(&atxid.to_be_bytes());
            ar.extend_from_slice(&1800u32.to_be_bytes()); // interval
            ar.extend_from_slice(&0u32.to_be_bytes()); // leechers
            ar.extend_from_slice(&0u32.to_be_bytes()); // seeders (0 peers follow)
            server.send_to(&ar, peer).await.unwrap();
        });

        // Opt-in: the same call must now complete the connect+announce exchange.
        std::env::set_var("OUTPACE_TRACKER_ALLOW_NON_GLOBAL", "1");
        let peers = announce_seeder(&[tracker_url], &[3u8; 20], &[4u8; 20], 6881).await;
        assert!(peers.is_empty(), "fake tracker returned zero peers");
        tokio::time::timeout(Duration::from_secs(5), served)
            .await
            .expect("tracker was never contacted despite OUTPACE_TRACKER_ALLOW_NON_GLOBAL=1")
            .unwrap();
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
            resolve_trackers_with_policy(&[overlong, "udp://127.0.0.1:80".into()], local_ok())
                .await;
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
