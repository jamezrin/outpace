use super::*;

/// Periodically re-announce this infohash as a seeder (`left=0`, event=Completed) to its
/// trackers, so outpace becomes organically discoverable to peers looking for this stream
/// while we're serving it; see `docs/protocol/notes/24-seeder-self-announce.md`.
/// A no-op loop (never announces) when `port` is `None` — i.e. no inbound listener backs an
/// advertisable endpoint (`enable_inbound` off, or a one-shot CLI leech): we must not invite
/// peers to dial a port nobody is serving on. The advertised `port` is always the peer
/// endpoint, never the HTTP API port (issue #21).
pub(super) async fn announce_seeder_periodically(
    info: StreamInfo,
    port: tokio::sync::watch::Receiver<Option<u16>>,
) {
    announce_infohash_periodically_dynamic(info.trackers, info.infohash, port).await
}

/// Dynamic form used by daemon announce paths. Port changes interrupt the normal interval so
/// the newly mapped (or fallback) endpoint is advertised promptly.
pub async fn announce_infohash_periodically_dynamic(
    trackers: Vec<String>,
    infohash: [u8; 20],
    mut port_rx: tokio::sync::watch::Receiver<Option<u16>>,
) {
    let peer_id = random_peer_id();
    loop {
        let port = *port_rx.borrow_and_update();
        if let Some(port) = port {
            announce_infohash_once(&trackers, &infohash, &peer_id, port).await;
        }
        tokio::select! {
            _ = tokio::time::sleep(SEEDER_ANNOUNCE_INTERVAL) => {},
            changed = port_rx.changed() => {
                if changed.is_err() {
                    std::future::pending::<()>().await;
                }
            }
        }
    }
}

/// The tracker+DHT self-announce loop, decoupled from `StreamInfo` so both the leech path
/// (a followed live stream) and B1 origination (a broadcast we minted ourselves, which has
/// no `StreamInfo` at all — just an infohash and trackers) can reuse the same primitive.
pub async fn announce_infohash_periodically(trackers: Vec<String>, infohash: [u8; 20], port: u16) {
    let peer_id = random_peer_id();
    loop {
        announce_infohash_once(&trackers, &infohash, &peer_id, port).await;
        tokio::time::sleep(SEEDER_ANNOUNCE_INTERVAL).await;
    }
}

async fn announce_infohash_once(
    trackers: &[String],
    infohash: &[u8; 20],
    peer_id: &[u8; 20],
    port: u16,
) {
    let peers = announce_seeder(trackers, infohash, peer_id, port).await;
    // DHT self-announce too, not just tracker: real Acestream swarms are largely
    // DHT-populated (README.md), so tracker-only self-announce under-serves
    // discoverability. `dht_announce_peer` is a separate primitive (not folded into
    // `announce_seeder` itself) because it's a multi-second live network call that
    // would otherwise turn `announce_seeder`'s fast offline unit test into a slow,
    // network-dependent one.
    let dht_announced = dht_announce_peer(infohash, port, DHT_ANNOUNCE_BUDGET).await;
    crate::alog!(
            "[ace] seeder self-announce for {}: {} tracker peer(s) seen, DHT announce_peer sent to {dht_announced} node(s)",
            hex_preview(infohash),
            peers.len(),
        );
}
