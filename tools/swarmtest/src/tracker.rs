//! In-process BEP-15 UDP tracker for interop swarms.
//!
//! A tokio task bound to a configurable `SocketAddr` (default `0.0.0.0:7001`) that
//! speaks the BEP-15 UDP tracker protocol using the shared [`ace_tracker::codec`]
//! server functions. It:
//!
//! * answers `connect` by issuing a random connection id (clients expect one), but
//!   does NOT validate that id on later announces — permissive by design for a test
//!   tracker;
//! * answers `announce` by recording the announce into a shared journal, registering
//!   `(src_ip, announced_port)` under the infohash (or deregistering it on a
//!   `stopped` event), and replying with `interval = 15` plus every OTHER registered
//!   peer for that infohash as compact IPv4.
//!
//! The journal is the interop assertion surface for later phases (who announced,
//! when, with what counters).

use std::collections::HashMap;
use std::net::{SocketAddr, SocketAddrV4};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use ace_tracker::codec::{
    build_announce_response, build_connect_response, parse_announce_request, parse_connect_request,
    AnnounceEvent,
};
use rand::Rng;
use serde::Serialize;
use tokio::net::UdpSocket;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// Announce interval advertised to peers (seconds). Deliberately short for tests.
pub const ANNOUNCE_INTERVAL: u32 = 15;

/// One recorded announce, in journal order.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AnnounceRecord {
    /// Wall-clock unix seconds when the announce was processed.
    pub ts_unix: u64,
    /// Source IP the datagram arrived from.
    pub src_ip: String,
    /// Port the peer advertised in the announce body.
    pub announced_port: u16,
    /// Swarm infohash (lowercase hex).
    pub infohash_hex: String,
    /// Peer id (lowercase hex).
    pub peer_id_hex: String,
    /// BEP-15 event ("none" | "completed" | "started" | "stopped").
    pub event: String,
    pub downloaded: u64,
    pub left: u64,
    pub uploaded: u64,
}

fn event_name(e: AnnounceEvent) -> &'static str {
    match e {
        AnnounceEvent::None => "none",
        AnnounceEvent::Completed => "completed",
        AnnounceEvent::Started => "started",
        AnnounceEvent::Stopped => "stopped",
    }
}

/// Shared mutable tracker state.
#[derive(Default)]
struct State {
    /// Registered peers per infohash.
    peers: HashMap<[u8; 20], Vec<SocketAddrV4>>,
}

/// Handle to a running in-process tracker.
pub struct TrackerHandle {
    /// The actual bound address (useful when binding to an ephemeral `:0` port).
    pub local_addr: SocketAddr,
    journal: Arc<Mutex<Vec<AnnounceRecord>>>,
    state: Arc<Mutex<State>>,
    shutdown: Arc<Notify>,
    task: JoinHandle<()>,
}

impl TrackerHandle {
    /// Snapshot the current journal (clone of all recorded announces, in order).
    pub fn journal_snapshot(&self) -> Vec<AnnounceRecord> {
        self.journal.lock().expect("journal mutex").clone()
    }

    /// Pre-register a known peer `(ip, port)` under `infohash` so the tracker ALWAYS
    /// returns it to other announcing peers — even if that peer (e.g. an engine source
    /// node) only ever announced to its own embedded trackers and never to us. This is
    /// how the harness guarantees consumers rendezvous with the source. Idempotent.
    pub fn seed_peer(&self, infohash: [u8; 20], peer: SocketAddrV4) {
        seed_peer_into(&mut self.state.lock().expect("state mutex"), infohash, peer);
    }

    /// Signal the server task to stop and wait for it to finish.
    pub async fn shutdown(self) {
        self.shutdown.notify_waiters();
        let _ = self.task.await;
    }
}

/// Register `peer` under `infohash` unless it is already present (pure; unit-tested via
/// [`TrackerHandle::seed_peer`]).
fn seed_peer_into(state: &mut State, infohash: [u8; 20], peer: SocketAddrV4) {
    let entry = state.peers.entry(infohash).or_default();
    if !entry.contains(&peer) {
        entry.push(peer);
    }
}

/// Bind a UDP socket at `addr` and spawn the tracker task. Returns once bound.
pub async fn start(addr: SocketAddr) -> std::io::Result<TrackerHandle> {
    let socket = UdpSocket::bind(addr).await?;
    let local_addr = socket.local_addr()?;
    let journal = Arc::new(Mutex::new(Vec::new()));
    let state = Arc::new(Mutex::new(State::default()));
    let shutdown = Arc::new(Notify::new());

    let task = tokio::spawn(serve(
        socket,
        Arc::clone(&journal),
        Arc::clone(&state),
        Arc::clone(&shutdown),
    ));

    Ok(TrackerHandle {
        local_addr,
        journal,
        state,
        shutdown,
        task,
    })
}

async fn serve(
    socket: UdpSocket,
    journal: Arc<Mutex<Vec<AnnounceRecord>>>,
    state: Arc<Mutex<State>>,
    shutdown: Arc<Notify>,
) {
    let mut buf = vec![0u8; 2048];
    loop {
        let (len, src) = tokio::select! {
            _ = shutdown.notified() => break,
            r = socket.recv_from(&mut buf) => match r {
                Ok(v) => v,
                Err(_) => continue,
            },
        };
        let datagram = &buf[..len];
        let reply = {
            let mut st = state.lock().expect("state mutex");
            handle_datagram(&mut st, src, datagram, &journal)
        };
        if let Some(reply) = reply {
            let _ = socket.send_to(&reply, src).await;
        }
    }
}

/// Process one datagram, mutating state/journal and returning the reply bytes.
fn handle_datagram(
    state: &mut State,
    src: SocketAddr,
    datagram: &[u8],
    journal: &Arc<Mutex<Vec<AnnounceRecord>>>,
) -> Option<Vec<u8>> {
    // Try connect first (fixed 16-byte layout with the magic protocol id).
    // We issue a connection id (clients expect one) but never validate it on
    // announce — permissive by design for a test tracker.
    if let Ok(req) = parse_connect_request(datagram) {
        let connection_id = rand::thread_rng().gen::<u64>();
        return Some(build_connect_response(req.txid, connection_id).to_vec());
    }

    // Otherwise try announce.
    let ann = parse_announce_request(datagram).ok()?;

    // Record into the journal.
    let record = AnnounceRecord {
        ts_unix: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        src_ip: src.ip().to_string(),
        announced_port: ann.port,
        infohash_hex: hex::encode(ann.infohash),
        peer_id_hex: hex::encode(ann.peer_id),
        event: event_name(ann.event).to_string(),
        downloaded: ann.transfer.downloaded,
        left: ann.transfer.left,
        uploaded: ann.transfer.uploaded,
    };
    journal.lock().expect("journal mutex").push(record);

    // Register this peer (compact IPv4 requires a v4 source address). A `stopped`
    // event deregisters it so Phase-2 peer-list assertions don't see stale peers.
    let this_peer = match src.ip() {
        std::net::IpAddr::V4(v4) => Some(SocketAddrV4::new(v4, ann.port)),
        std::net::IpAddr::V6(_) => None,
    };
    let entry = state.peers.entry(ann.infohash).or_default();
    if let Some(peer) = this_peer {
        if ann.event == AnnounceEvent::Stopped {
            entry.retain(|p| *p != peer);
        } else if !entry.contains(&peer) {
            entry.push(peer);
        }
    }

    // Reply with every OTHER registered peer for this infohash.
    let others: Vec<SocketAddrV4> = entry
        .iter()
        .copied()
        .filter(|p| Some(*p) != this_peer)
        .collect();
    let seeders = entry.len() as u32;
    Some(build_announce_response(
        ann.txid,
        ANNOUNCE_INTERVAL,
        0,
        seeders,
        &others,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ace_tracker::codec::{
        build_announce_request, build_connect_request, parse_announce_response,
        parse_connect_response, TransferState,
    };
    use std::net::Ipv4Addr;
    use tokio::net::UdpSocket;

    /// Drive a full connect handshake from a client socket; return the connection id.
    async fn connect(client: &UdpSocket, server: SocketAddr, txid: u32) -> u64 {
        let req = build_connect_request(txid);
        client.send_to(&req, server).await.unwrap();
        let mut buf = [0u8; 64];
        let (n, _) = client.recv_from(&mut buf).await.unwrap();
        parse_connect_response(&buf[..n], txid).unwrap()
    }

    /// Send a `started` announce, return the parsed (interval, peers).
    #[allow(clippy::too_many_arguments)]
    async fn announce(
        client: &UdpSocket,
        server: SocketAddr,
        connection_id: u64,
        txid: u32,
        infohash: &[u8; 20],
        peer_id: &[u8; 20],
        port: u16,
    ) -> (u32, Vec<SocketAddrV4>) {
        announce_event(
            client,
            server,
            connection_id,
            txid,
            infohash,
            peer_id,
            port,
            AnnounceEvent::Started,
        )
        .await
    }

    /// Send an announce with an explicit event, return the parsed (interval, peers).
    #[allow(clippy::too_many_arguments)]
    async fn announce_event(
        client: &UdpSocket,
        server: SocketAddr,
        connection_id: u64,
        txid: u32,
        infohash: &[u8; 20],
        peer_id: &[u8; 20],
        port: u16,
        event: AnnounceEvent,
    ) -> (u32, Vec<SocketAddrV4>) {
        let req = build_announce_request(
            connection_id,
            txid,
            infohash,
            peer_id,
            port,
            -1,
            &TransferState::default(),
            event,
        );
        client.send_to(&req, server).await.unwrap();
        let mut buf = [0u8; 2048];
        let (n, _) = client.recv_from(&mut buf).await.unwrap();
        parse_announce_response(&buf[..n], txid).unwrap()
    }

    #[tokio::test]
    async fn connect_handshake_issues_connection_id() {
        let tracker = start("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let cid = connect(&client, tracker.local_addr, 0xABCD).await;
        // A (vanishingly unlikely to be zero) random id was issued.
        assert_ne!(cid, 0);
        tracker.shutdown().await;
    }

    #[tokio::test]
    async fn two_peers_same_infohash_learn_each_other() {
        let tracker = start("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let server = tracker.local_addr;
        let infohash = [0x11u8; 20];

        let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let a_port = a.local_addr().unwrap().port();
        let b_port = b.local_addr().unwrap().port();

        let a_cid = connect(&a, server, 1).await;
        let b_cid = connect(&b, server, 2).await;

        // First peer announces: learns nobody yet.
        let (interval, peers) =
            announce(&a, server, a_cid, 10, &infohash, &[0xAA; 20], a_port).await;
        assert_eq!(interval, ANNOUNCE_INTERVAL);
        assert!(peers.is_empty(), "first announcer sees no other peers");

        // Second peer announces: learns peer A.
        let (_, peers_b) = announce(&b, server, b_cid, 11, &infohash, &[0xBB; 20], b_port).await;
        assert_eq!(
            peers_b,
            vec![SocketAddrV4::new(Ipv4Addr::LOCALHOST, a_port)]
        );

        // A re-announces: now learns peer B (and not itself).
        let (_, peers_a2) = announce(&a, server, a_cid, 12, &infohash, &[0xAA; 20], a_port).await;
        assert_eq!(
            peers_a2,
            vec![SocketAddrV4::new(Ipv4Addr::LOCALHOST, b_port)]
        );

        // Journal recorded all three announces.
        let journal = tracker.journal_snapshot();
        assert_eq!(journal.len(), 3);
        assert!(journal
            .iter()
            .all(|r| r.infohash_hex == hex::encode(infohash)));
        assert_eq!(journal[0].announced_port, a_port);
        assert_eq!(journal[0].event, "started");

        tracker.shutdown().await;
    }

    #[tokio::test]
    async fn seeded_peer_is_returned_to_an_announcing_peer() {
        let tracker = start("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let server = tracker.local_addr;
        let infohash = [0x55u8; 20];

        // Pre-register a known source peer that never announces to us.
        let source = SocketAddrV4::new(Ipv4Addr::new(172, 28, 0, 11), 7764);
        tracker.seed_peer(infohash, source);
        // Idempotent: seeding again does not duplicate it.
        tracker.seed_peer(infohash, source);

        // A consumer announcing the same infohash immediately learns the seeded source.
        let c = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let c_port = c.local_addr().unwrap().port();
        let c_cid = connect(&c, server, 1).await;
        let (_, peers) = announce(&c, server, c_cid, 10, &infohash, &[0xCC; 20], c_port).await;
        assert_eq!(
            peers,
            vec![source],
            "seeded source must be returned exactly once"
        );

        // A different infohash still does not see the seeded peer.
        let (_, other) = announce(&c, server, c_cid, 11, &[0x66; 20], &[0xCC; 20], c_port).await;
        assert!(other.is_empty());

        tracker.shutdown().await;
    }

    #[tokio::test]
    async fn different_infohash_is_isolated() {
        let tracker = start("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let server = tracker.local_addr;

        let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let a_port = a.local_addr().unwrap().port();
        let b_port = b.local_addr().unwrap().port();

        let a_cid = connect(&a, server, 1).await;
        let b_cid = connect(&b, server, 2).await;

        announce(&a, server, a_cid, 10, &[0x11; 20], &[0xAA; 20], a_port).await;
        // B announces a DIFFERENT infohash; must not see A.
        let (_, peers_b) = announce(&b, server, b_cid, 11, &[0x22; 20], &[0xBB; 20], b_port).await;
        assert!(peers_b.is_empty(), "different infohash must be isolated");

        tracker.shutdown().await;
    }

    #[tokio::test]
    async fn stopped_event_deregisters_peer_but_is_journaled() {
        let tracker = start("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let server = tracker.local_addr;
        let infohash = [0x33u8; 20];

        let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let a_port = a.local_addr().unwrap().port();
        let b_port = b.local_addr().unwrap().port();

        let a_cid = connect(&a, server, 1).await;
        let b_cid = connect(&b, server, 2).await;

        // A registers, then stops.
        announce(&a, server, a_cid, 10, &infohash, &[0xAA; 20], a_port).await;
        announce_event(
            &a,
            server,
            a_cid,
            11,
            &infohash,
            &[0xAA; 20],
            a_port,
            AnnounceEvent::Stopped,
        )
        .await;

        // B now announces: A has been deregistered, so B sees no peers.
        let (_, peers_b) = announce(&b, server, b_cid, 12, &infohash, &[0xBB; 20], b_port).await;
        assert!(
            peers_b.is_empty(),
            "stopped peer must not linger in the registry"
        );

        // But the stopped announce was still journaled.
        let journal = tracker.journal_snapshot();
        assert_eq!(journal.len(), 3);
        assert_eq!(journal[1].event, "stopped");
        assert_eq!(journal[1].announced_port, a_port);

        tracker.shutdown().await;
    }
}
