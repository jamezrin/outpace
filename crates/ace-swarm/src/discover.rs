//! Peer discovery: announce to the stream's UDP trackers (BEP-15) and aggregate the unique
//! peers they return. Mainline DHT self-announce (`dht_announce_peer`, BEP-5) is a separate,
//! composable primitive in `crate::dht` — callers that want both combine them explicitly
//! (see `ace_engine::ace_provider`'s periodic self-announce), rather than baking a
//! multi-second live network call into this module's fast, offline-testable functions.

use crate::dht::{dht_get_peers_incremental, dht_get_peers_with_target};
use crate::resolver::Resolver;
use ace_tracker::client::announce;
use ace_tracker::codec::{AnnounceEvent, TransferState};
use std::collections::BTreeSet;
use std::future::Future;
use std::net::SocketAddrV4;
use std::time::Duration;

const DISCOVERY_PEER_TARGET: usize = 8;
const TRACKER_PARALLELISM: usize = 4;
const TRACKER_DEADLINE: Duration = Duration::from_secs(2);
/// Maximum unique addresses retained and queued by one incremental discovery run.
pub const MAX_DISCOVERY_PEERS: usize = 1024;

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
    resolve_trackers_with_resolver(trackers, policy, &Resolver::global()).await
}

pub(crate) async fn resolve_trackers_with_resolver(
    trackers: &[String],
    policy: TrackerPolicy,
    resolver: &Resolver,
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
        if let Ok(addrs) = resolver.lookup(hostport).await {
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
    discover_peers_from_sources(
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
    discover_tracker_peers_with_policy(
        trackers,
        infohash,
        peer_id,
        port,
        event,
        left,
        TrackerPolicy::from_env(),
    )
    .await
}

async fn discover_tracker_peers_with_policy(
    trackers: &[String],
    infohash: &[u8; 20],
    peer_id: &[u8; 20],
    port: u16,
    event: AnnounceEvent,
    left: u64,
    policy: TrackerPolicy,
) -> Vec<SocketAddrV4> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(MAX_DISCOVERY_PEERS);
    let collect = async move {
        let mut peers = BTreeSet::new();
        while let Some(peer) = receiver.recv().await {
            if peers.len() < MAX_DISCOVERY_PEERS {
                peers.insert(peer);
            }
        }
        peers.into_iter().collect()
    };
    let (_, peers) = tokio::join!(
        stream_tracker_peers(trackers, infohash, peer_id, port, event, left, policy, sender),
        collect,
    );
    peers
}

#[allow(clippy::too_many_arguments)]
async fn stream_tracker_peers(
    trackers: &[String],
    infohash: &[u8; 20],
    peer_id: &[u8; 20],
    port: u16,
    event: AnnounceEvent,
    left: u64,
    policy: TrackerPolicy,
    peers: tokio::sync::mpsc::Sender<SocketAddrV4>,
) {
    let mut jobs = tokio::task::JoinSet::new();
    let mut urls = trackers.iter().take(MAX_TRACKERS);
    loop {
        while jobs.len() < TRACKER_PARALLELISM {
            let Some(url) = urls.next() else {
                break;
            };
            if url.len() > MAX_TRACKER_URL_LEN {
                continue;
            }
            let url = url.clone();
            let infohash = *infohash;
            let peer_id = *peer_id;
            jobs.spawn(async move {
                tracker_exchange(
                    resolve_trackers_with_policy(&[url], policy),
                    &infohash,
                    &peer_id,
                    port,
                    event,
                    left,
                )
                .await
            });
        }
        let Some(result) = jobs.join_next().await else {
            break;
        };
        if let Ok(found) = result {
            for peer in found {
                if peers.send(peer).await.is_err() {
                    return;
                }
            }
        }
    }
}

async fn tracker_exchange<F>(
    resolution: F,
    infohash: &[u8; 20],
    peer_id: &[u8; 20],
    port: u16,
    event: AnnounceEvent,
    left: u64,
) -> Vec<SocketAddrV4>
where
    F: Future<Output = Vec<SocketAddrV4>>,
{
    tokio::time::timeout(TRACKER_DEADLINE, async {
        let addresses = resolution.await;
        let Some(addr) = addresses.first() else {
            return Vec::new();
        };
        announce(
            *addr,
            infohash,
            peer_id,
            port,
            200,
            TransferState {
                downloaded: 0,
                left,
                uploaded: 0,
            },
            event,
        )
        .await
        .unwrap_or_default()
    })
    .await
    .unwrap_or_default()
}

/// Stream first and later source results without cancelling useful discovery when another
/// source reaches its target. The receiver owns backpressure; dropping it ends the run.
pub async fn discover_peers_incremental(
    trackers: &[String],
    infohash: &[u8; 20],
    peer_id: &[u8; 20],
    port: u16,
    options: DiscoveryOptions,
    peers: tokio::sync::mpsc::Sender<SocketAddrV4>,
) {
    let (source_tx, source_rx) = tokio::sync::mpsc::channel(MAX_DISCOVERY_PEERS);
    let tracker_tx = source_tx.clone();
    let tracker = stream_tracker_peers(
        trackers,
        infohash,
        peer_id,
        port,
        AnnounceEvent::Started,
        u64::MAX,
        TrackerPolicy::from_env(),
        tracker_tx,
    );
    let dht = async move {
        dht_get_peers_incremental(
            infohash,
            options.dht_budget,
            options.peer_target.max(1),
            |peer| source_tx.try_send(peer).is_ok(),
        )
        .await;
    };
    forward_discovery_sources(tracker, dht, source_rx, peers).await;
}

// Both production UDP sources and injected source futures use this owning combiner.
async fn forward_discovery_sources<A, B>(
    a: A,
    b: B,
    mut source_rx: tokio::sync::mpsc::Receiver<SocketAddrV4>,
    peers: tokio::sync::mpsc::Sender<SocketAddrV4>,
) where
    A: Future<Output = ()>,
    B: Future<Output = ()>,
{
    let output = peers.clone();
    let forward = async move {
        let peers = output;
        let mut seen = BTreeSet::new();
        while let Some(peer) = source_rx.recv().await {
            if seen.insert(peer) && peers.send(peer).await.is_err() {
                return;
            }
            if seen.len() >= MAX_DISCOVERY_PEERS {
                return;
            }
        }
    };
    tokio::pin!(a, b, forward);
    tokio::select! {
        _ = peers.closed() => {},
        _ = &mut forward => {},
        _ = async { tokio::join!(&mut a,&mut b); } => {forward.await;},
    }
}

/// Offline source boundary for the production incremental combiner. Both complete-batch
/// futures are adapted to its bounded source channel; dropping the sink cancels both.
#[doc(hidden)]
pub async fn discover_peers_from_sources_incremental<A, B>(
    a: A,
    b: B,
    peers: tokio::sync::mpsc::Sender<SocketAddrV4>,
) where
    A: Future<Output = Vec<SocketAddrV4>>,
    B: Future<Output = Vec<SocketAddrV4>>,
{
    let (sender, receiver) = tokio::sync::mpsc::channel(MAX_DISCOVERY_PEERS);
    let second_sender = sender.clone();
    let first = async move {
        for peer in a.await {
            if sender.send(peer).await.is_err() {
                return;
            }
        }
    };
    let second = async move {
        for peer in b.await {
            if second_sender.send(peer).await.is_err() {
                return;
            }
        }
    };
    forward_discovery_sources(first, second, receiver, peers).await;
}

/// Combine two discovery sources using the same target policy as tracker/DHT discovery.
/// Sources return their complete candidate sets; a weaker first result retains the second
/// future until completion. The generic boundary permits deterministic offline protocol tests.
#[doc(hidden)]
pub async fn discover_peers_from_sources<A, B>(a: A, b: B, peer_target: usize) -> Vec<SocketAddrV4>
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
    async fn tracker_after_silent_tracker_is_contacted_promptly() {
        let dead = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let healthy = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let urls = vec![
            format!("udp://{}", dead.local_addr().unwrap()),
            format!("udp://{}", healthy.local_addr().unwrap()),
        ];
        let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut packet = [0; 2048];
            healthy.recv_from(&mut packet).await.unwrap();
            seen_tx.send(()).unwrap();
        });
        let discover = tokio::spawn(async move {
            discover_tracker_peers_with_policy(
                &urls,
                &[3; 20],
                &[4; 20],
                0,
                AnnounceEvent::Started,
                u64::MAX,
                local_ok(),
            )
            .await
        });
        let contacted = tokio::time::timeout(Duration::from_millis(300), seen_rx).await;
        discover.abort();
        server.abort();
        let _ = discover.await;
        let _ = server.await;
        let mut packet = [0; 2048];
        assert!(
            dead.try_recv_from(&mut packet).is_ok(),
            "first real tracker must be contacted"
        );
        assert!(
            contacted.is_ok(),
            "silent tracker serialized the later tracker"
        );
    }

    #[tokio::test]
    async fn incremental_cap_cancels_other_owned_source_without_waiting_for_budget() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(MAX_DISCOVERY_PEERS);
        let first = async {
            (0..MAX_DISCOVERY_PEERS as u16)
                .map(|port| SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, port))
                .collect()
        };
        let second = std::future::pending::<Vec<SocketAddrV4>>();
        let run = tokio::spawn(discover_peers_from_sources_incremental(
            first, second, sender,
        ));
        for _ in 0..MAX_DISCOVERY_PEERS {
            assert!(receiver.recv().await.is_some());
        }
        let finished = tokio::time::timeout(Duration::from_millis(100), async {
            while !run.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        run.abort();
        let _ = run.await;
        assert!(
            finished.is_ok(),
            "production combiner retained silent source after unique-peer cap"
        );
    }

    #[tokio::test]
    async fn incremental_normal_completion_drains_final_buffer_and_retains_later_source() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let a = async { vec!["127.0.0.1:1".parse().unwrap()] };
        let b = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            vec![
                "127.0.0.1:1".parse().unwrap(),
                "127.0.0.1:2".parse().unwrap(),
                "127.0.0.1:3".parse().unwrap(),
            ]
        };
        let run = tokio::spawn(discover_peers_from_sources_incremental(a, b, sender));
        let mut peers = Vec::new();
        while let Some(peer) = receiver.recv().await {
            peers.push(peer);
        }
        run.await.unwrap();
        assert_eq!(peers.len(), 3);
        assert_eq!(peers[2].port(), 3);
    }
    #[tokio::test]
    async fn incremental_receiver_close_cancels_pending_sources() {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let run = tokio::spawn(discover_peers_from_sources_incremental(
            std::future::pending::<Vec<SocketAddrV4>>(),
            std::future::pending::<Vec<SocketAddrV4>>(),
            sender,
        ));
        drop(receiver);
        tokio::time::timeout(Duration::from_millis(100), run)
            .await
            .unwrap()
            .unwrap();
    }
    #[tokio::test]
    async fn tracker_deadline_covers_resolution_and_both_actual_udp_exchanges() {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let std::net::SocketAddr::V4(addr) = socket.local_addr().unwrap() else {
            unreachable!()
        };
        let server = tokio::spawn(async move {
            let mut packet = [0; 2048];
            let (_, peer) = socket.recv_from(&mut packet).await.unwrap();
            tokio::time::sleep(Duration::from_millis(700)).await;
            let mut response = 0u32.to_be_bytes().to_vec();
            response.extend(&packet[12..16]);
            response.extend(42u64.to_be_bytes());
            socket.send_to(&response, peer).await.unwrap();
            let (_, peer) = socket.recv_from(&mut packet).await.unwrap();
            tokio::time::sleep(Duration::from_millis(700)).await;
            let mut response = 1u32.to_be_bytes().to_vec();
            response.extend(&packet[12..16]);
            response.extend([0; 12]);
            response.extend([127, 0, 0, 1, 0, 1]);
            socket.send_to(&response, peer).await.unwrap();
        });
        let resolution = async {
            tokio::time::sleep(Duration::from_millis(700)).await;
            vec![addr]
        };
        let start = std::time::Instant::now();
        let found = tracker_exchange(
            resolution,
            &[0; 20],
            &[0; 20],
            0,
            AnnounceEvent::Started,
            u64::MAX,
        )
        .await;
        let elapsed = start.elapsed();
        server.await.unwrap();
        assert!(
            found.is_empty(),
            "2.1s combined exchange must miss the total 2s deadline"
        );
        assert!(elapsed >= Duration::from_millis(1900) && elapsed < Duration::from_millis(2300));
    }
    #[tokio::test]
    async fn tracker_jobs_never_exceed_four_and_release_at_total_deadline() {
        let mut sockets = Vec::new();
        for _ in 0..5 {
            sockets.push(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        }
        let urls = sockets
            .iter()
            .map(|socket| format!("udp://{}", socket.local_addr().unwrap()))
            .collect::<Vec<_>>();
        let run = tokio::spawn(async move {
            discover_tracker_peers_with_policy(
                &urls,
                &[0; 20],
                &[0; 20],
                0,
                AnnounceEvent::Started,
                u64::MAX,
                local_ok(),
            )
            .await
        });
        let start = std::time::Instant::now();
        let mut packet = [0; 2048];
        for socket in sockets.iter().take(4) {
            tokio::time::timeout(Duration::from_millis(300), socket.recv_from(&mut packet))
                .await
                .unwrap()
                .unwrap();
        }
        assert!(
            sockets[4].try_recv_from(&mut packet).is_err(),
            "fifth tracker exceeded four running jobs"
        );
        tokio::time::timeout(
            Duration::from_millis(2300),
            sockets[4].recv_from(&mut packet),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(start.elapsed() >= Duration::from_millis(1900));
        run.abort();
        let _ = run.await;
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
        let peers = discover_peers_from_sources(slow, fast, 1).await;
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

        let peers = discover_peers_from_sources(weak, strong, 2).await;
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
