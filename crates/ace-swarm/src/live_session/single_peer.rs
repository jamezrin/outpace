use super::*;
use crate::seed::protocol::{prepare_reply, ServeProfile};

#[allow(dead_code)]
#[allow(clippy::too_many_arguments)]
async fn follow_one_peer(
    session: &mut PeerSession<TcpStream>,
    info: &StreamInfo,
    identity: &Identity,
    addr: SocketAddrV4,
    window: WireLivePosition,
    chunks_per_piece: u16,
    tx: &mpsc::Sender<Bytes>,
    downloaded: &Arc<AtomicU64>,
    uploaded: &Arc<AtomicU64>,
    peers_served: &Arc<AtomicU32>,
    seed: &SeedConfig,
    continuity: &mut Option<Continuity>,
) -> FollowEnd {
    let _stats_guard = LiveStatsGuard(seed.metrics.clone());
    // 1. Use the peer's advertised live window, already read during upstream selection.
    let min_piece = window.min_piece.max(0) as u64;
    let max_piece = window.max_piece.max(0) as u64;
    let start = match continuity {
        None => {
            let (c, start) = Continuity::fresh(
                info,
                min_piece,
                max_piece,
                seed.prefetch_pieces,
                seed.live_recovery,
            );
            *continuity = Some(c);
            crate::alog!("[ace] {addr}: window min={min_piece} max={max_piece} -> start={start} head={max_piece}");
            start
        }
        Some(c) => {
            let start = c.resume(addr, min_piece, max_piece);
            crate::alog!(
                "[ace] {addr}: reconnected; window min={min_piece} max={max_piece} -> resuming from {start} head={}",
                c.head
            );
            start
        }
    };
    let continuity = continuity.as_mut().expect("initialized just above");
    continuity.register_active_peer(SINGLE_PEER_ID, addr, window);

    // 2. Advertise our matching position + interest.
    let hs = OutgoingExtendedHandshake {
        ace_metadata_version: 1,
        ut_metadata_id: 2,
        mi: Some(WireLivePosition {
            min_piece: start as i64,
            max_piece: continuity.head as i64,
            position: -1,
            distance_from_source: 1,
        }),
        node: NodeFields {
            ts: 5000,
            ..NodeFields::default()
        },
        peer_ip: Some(addr.ip().octets()),
        metadata_size: None,
    };
    if session
        .send_signed_extended_handshake(&hs, identity)
        .await
        .is_err()
        || session.send(&PeerMessage::Interested).await.is_err()
    {
        return FollowEnd::PeerLost(1);
    }

    let mut unchoked = false;
    let peer_min = min_piece;
    let mut peer_max = max_piece;
    let store = seed
        .registry
        .get_or_create(info.infohash, || (seed.store_factory)(info.infohash));
    let mut unchoked_peer = false;
    let mut last_progress = Instant::now();
    // Diagnostic: surface each unmodelled Acestream message id once (note 22 follow-up).
    let mut seen_ids: HashSet<u8> = HashSet::new();

    loop {
        publish_live_stats(continuity, seed.metrics.as_ref(), info);
        let Some(read_budget) = stale_upstream_budget(
            last_progress,
            Instant::now(),
            seed.live_recovery.stale_upstream_timeout(),
        ) else {
            crate::alog!(
                "[ace] {addr}: stale upstream — no live progress for {:?}; reconnecting",
                seed.live_recovery.stale_upstream_timeout()
            );
            return FollowEnd::PeerLost(1);
        };
        let msg = match tokio::time::timeout(read_budget, session.read_message()).await {
            Ok(Ok(m)) => m,
            Ok(Err(_)) => return FollowEnd::PeerLost(1),
            Err(_) => {
                crate::alog!(
                    "[ace] {addr}: stale upstream — no live progress for {:?}; reconnecting",
                    seed.live_recovery.stale_upstream_timeout()
                );
                return FollowEnd::PeerLost(1);
            }
        };
        let mut made_activity = false;
        let mut made_output = false;
        if let PeerMessage::Extended { payload, .. } | PeerMessage::Unknown { payload, .. } = &msg {
            observe_live_window(continuity, SINGLE_PEER_ID, payload);
        }
        match msg {
            PeerMessage::Unchoke => {
                unchoked = true;
                continuity.set_peer_unchoked(SINGLE_PEER_ID, true);
                crate::alog!(
                    "[ace] {addr}: UNCHOKE -> scheduling from piece {} toward head {}",
                    continuity.reasm.next_needed(),
                    continuity.head
                );
                if advance_requests(session, continuity, chunks_per_piece)
                    .await
                    .is_err()
                {
                    return FollowEnd::PeerLost(1);
                }
                made_activity = true;
            }
            PeerMessage::Choke => {
                unchoked = false;
                continuity.set_peer_unchoked(SINGLE_PEER_ID, false);
            }
            PeerMessage::Have(p) => {
                let old_head = continuity.head;
                continuity.head = continuity.head.max(p as u64);
                peer_max = peer_max.max(p as u64);
                continuity.update_peer_window(SINGLE_PEER_ID, peer_min, peer_max);
                made_activity |= continuity.head > old_head;
                if unchoked
                    && advance_requests(session, continuity, chunks_per_piece)
                        .await
                        .is_err()
                {
                    return FollowEnd::PeerLost(1);
                }
            }
            // The live edge advances via a periodic `myinfo` window update (engine symbol
            // `got_myinfo`), NOT a standard `Have` — see note 22. Depending on the peer it
            // arrives as a re-sent extended handshake (ext_id 0) or a custom Acestream
            // message id. Recognize it by content (a bencode window dict carrying
            // `max_piece`) regardless of carrier, advance the head, and request the newly
            // available pieces.
            PeerMessage::Extended { ref payload, .. } => {
                if let Some(new_head) = advance_head_from_window(payload, continuity.head) {
                    continuity.head = new_head;
                    peer_max = peer_max.max(new_head);
                    continuity.update_peer_window(SINGLE_PEER_ID, peer_min, peer_max);
                    made_activity = true;
                    if unchoked
                        && advance_requests(session, continuity, chunks_per_piece)
                            .await
                            .is_err()
                    {
                        return FollowEnd::PeerLost(1);
                    }
                }
            }
            m @ PeerMessage::Piece { .. } => {
                if let Some(lc) = LiveChunk::from_message(&m) {
                    let piece = lc.piece as u64;
                    PieceStore::shared_put_chunk_with_header(
                        &store,
                        piece,
                        lc.chunk,
                        lc.piece_header,
                        &lc.data,
                    )
                    .await;
                    let begin = lc.chunk as u64 * info.chunk_length;
                    if let Err(e) = continuity.reasm.add_block(lc.piece as u64, begin, &lc.data) {
                        // A malformed block, or a completed piece whose live-source signature
                        // didn't verify (#10): drop it rather than emit unauthenticated bytes.
                        // The piece stays incomplete, so it's re-requested from the pool.
                        crate::alog!("[ace] {addr}: dropped piece {piece} block: {e:?}");
                        continuity.release_rejected_piece(piece);
                        if unchoked
                            && advance_requests(session, continuity, chunks_per_piece)
                                .await
                                .is_err()
                        {
                            return FollowEnd::PeerLost(1);
                        }
                        continue;
                    }
                    continuity.note_chunk(piece, lc.chunk, chunks_per_piece);
                    made_activity = true;
                    let ready = take_ready_with_stats(continuity, seed.metrics.as_ref(), info);
                    if !ready.is_empty() {
                        for output in continuity.resync_output(&ready) {
                            let aligned = output.bytes;
                            continuity.emitted += aligned.len() as u64;
                            if continuity.emitted >= continuity.next_log {
                                crate::alog!(
                                    "[ace] {addr}: served {} MiB (head={}, next piece needed={})",
                                    continuity.emitted >> 20,
                                    continuity.head,
                                    continuity.reasm.next_needed()
                                );
                                continuity.next_log = continuity.emitted + (4 << 20);
                            }
                            let len = aligned.len() as u64;
                            if tx.send(aligned).await.is_err() {
                                return FollowEnd::ConsumerGone;
                            }
                            downloaded.fetch_add(len, Ordering::Relaxed);
                            made_output = true;
                        }
                    }
                    if unchoked
                        && advance_requests(session, continuity, chunks_per_piece)
                            .await
                            .is_err()
                    {
                        return FollowEnd::PeerLost(1);
                    }
                }
            }
            PeerMessage::Interested => {
                if seed.enabled && !unchoked_peer {
                    let _ = session.send(&PeerMessage::Unchoke).await;
                    unchoked_peer = true;
                }
            }
            request @ PeerMessage::Unknown { id: 6, .. } if seed.enabled => {
                if let Some(reply) =
                    prepare_reply(&request, Some(&store), ServeProfile::Reciprocal).await
                {
                    if session.send(&reply.message).await.is_ok() {
                        uploaded.fetch_add(reply.media_bytes as u64, Ordering::Relaxed);
                        peers_served.store(1, Ordering::Relaxed); // single-peer follow; multi-peer aggregation is S2
                    }
                }
            }
            // Acestream live HAVE (note 22 capture): an 8-byte `[u32 stream=0][u32 piece]`.
            // This is the live-edge advancement signal — the engine announces each new piece
            // at the head with id=4 (NOT the standard 4-byte BT HAVE, which it never sends),
            // and the advancing trailing edge / eviction pointer with id=10. Advance the head
            // on id=4 and request the newly-available pieces.
            PeerMessage::Unknown { id: 4, ref payload } if payload.len() == 8 => {
                let piece =
                    u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]) as u64;
                if piece > continuity.head {
                    continuity.head = piece;
                    peer_max = peer_max.max(piece);
                    continuity.update_peer_window(SINGLE_PEER_ID, peer_min, peer_max);
                    made_activity = true;
                    if unchoked
                        && advance_requests(session, continuity, chunks_per_piece)
                            .await
                            .is_err()
                    {
                        return FollowEnd::PeerLost(1);
                    }
                }
            }
            // Trailing edge (oldest still-available piece). Informational: if it ever passes
            // what we still need, we've fallen irrecoverably behind this peer's window.
            PeerMessage::Unknown { id: 10, .. } => {}
            // Any other Acestream-custom message: it may be a `myinfo` window update carried
            // as a bencode dict (note 22). Recognize it by content and advance; if it isn't a
            // window update, log the id once so a live run reveals carriers we don't decode.
            PeerMessage::Unknown { id, ref payload } => {
                if let Some(new_head) = advance_head_from_window(payload, continuity.head) {
                    crate::alog!(
                        "[ace] {addr}: live window update (msg id={id}) head {} -> {new_head}",
                        continuity.head
                    );
                    continuity.head = new_head;
                    peer_max = peer_max.max(new_head);
                    continuity.update_peer_window(SINGLE_PEER_ID, peer_min, peer_max);
                    made_activity = true;
                    if unchoked
                        && advance_requests(session, continuity, chunks_per_piece)
                            .await
                            .is_err()
                    {
                        return FollowEnd::PeerLost(1);
                    }
                } else if seen_ids.insert(id) {
                    crate::alog!(
                        "[ace] {addr}: unhandled msg id={id} ({} bytes) {}",
                        payload.len(),
                        hex_preview(payload)
                    );
                }
            }
            _ => {}
        }
        if should_refresh_stale_deadline(continuity.emitted, made_output, made_activity) {
            last_progress = Instant::now();
        }
    }
}

/// If `payload` carries a live-window (`myinfo`) update whose head is beyond `head`, return
/// the new head; otherwise `None`. The single place window recognition feeds the loop.
pub(super) fn advance_head_from_window(payload: &[u8], head: u64) -> Option<u64> {
    let w = LiveWindow::from_myinfo_payload(payload)?;
    (w.max_piece > head && w.max_piece <= u32::MAX as u64).then_some(w.max_piece)
}

pub(super) fn stale_upstream_budget(
    last_progress: Instant,
    now: Instant,
    stale_upstream_timeout: Duration,
) -> Option<Duration> {
    let elapsed = now.saturating_duration_since(last_progress);
    if elapsed >= stale_upstream_timeout {
        None
    } else {
        Some(stale_upstream_timeout - elapsed)
    }
}

pub(super) fn should_refresh_stale_deadline(
    emitted: u64,
    made_output: bool,
    made_activity: bool,
) -> bool {
    made_output || (emitted == 0 && made_activity)
}

pub(super) fn schedule_piece_requests(
    scheduler: &mut Scheduler,
    active_peers: &mut ActivePeers,
    next_needed: u64,
    head: u64,
) -> Vec<u64> {
    schedule_piece_assignments(scheduler, active_peers, next_needed, head)
        .into_iter()
        .filter_map(|assignment| (assignment.peer_id == SINGLE_PEER_ID).then_some(assignment.piece))
        .collect()
}

pub(super) fn schedule_piece_assignments(
    scheduler: &mut Scheduler,
    active_peers: &mut ActivePeers,
    next_needed: u64,
    head: u64,
) -> Vec<PeerAssignment> {
    active_peers.assign(scheduler, next_needed, head)
}

/// Fill the request pipeline from the stream cursor toward the known head, constrained by
/// this peer's advertised window. The scheduler owns in-flight bookkeeping; this function
/// only turns assigned pieces into Acestream chunk requests.
async fn advance_requests(
    session: &mut PeerSession<TcpStream>,
    continuity: &mut Continuity,
    chunks_per_piece: u16,
) -> ace_peer::Result<()> {
    let pieces = schedule_piece_requests(
        &mut continuity.scheduler,
        &mut continuity.active_peers,
        continuity.reasm.next_needed(),
        continuity.head,
    );
    for piece in pieces {
        for chunk in 0..chunks_per_piece {
            session.send(&chunk_request(piece as u32, chunk)).await?;
        }
    }
    Ok(())
}

/// Hex preview of a message prefix for diagnostics (avoids pulling in a hex crate).
pub(super) fn hex_preview(bytes: &[u8]) -> String {
    bytes.iter().take(24).map(|b| format!("{b:02x}")).collect()
}

/// Read messages until the peer's extended handshake arrives; return its live `mi` window and
/// the `yourip` public address it echoed to us (issue #22 — the swarm telling us how it sees us
/// on this outbound connection; `None` when the peer sent no usable `yourip`).
pub(super) async fn read_peer_window(
    session: &mut PeerSession<TcpStream>,
) -> Option<(WireLivePosition, Option<IpAddr>)> {
    for _ in 0..32 {
        let msg = session.read_message().await.ok()?;
        if let PeerMessage::Extended { ext_id: 0, payload } = msg {
            let eh = ExtendedHandshake::parse(&payload).ok()?;
            let yourip = eh.yourip();
            let mi = eh.raw.get(b"mi")?;
            let get = |k: &[u8]| mi.get(k).and_then(|v| v.as_int()).unwrap_or(-1);
            let min_piece = get(b"min_piece");
            let max_piece = get(b"max_piece");
            if min_piece < 0 || max_piece < min_piece || max_piece > u32::MAX as i64 {
                return None;
            }
            return Some((
                WireLivePosition {
                    min_piece,
                    max_piece,
                    position: get(b"position"),
                    distance_from_source: get(b"distance_from_source"),
                },
                yourip,
            ));
        }
    }
    None
}

#[cfg(test)]
#[path = "tests/single_peer_seeding.rs"]
mod seeding_tests;
