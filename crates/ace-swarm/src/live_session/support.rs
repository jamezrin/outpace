use super::*;

/// Read the bounded head from a supported live-window (`myinfo`) update.
pub(super) fn read_window_max(payload: &[u8]) -> Option<u64> {
    let w = LiveWindow::from_myinfo_payload(payload)?;
    (w.max_piece <= u32::MAX as u64).then_some(w.max_piece)
}

/// Return a supported live-window head only when it exceeds the shared head.
#[cfg(test)]
pub(super) fn advance_head_from_window(payload: &[u8], head: u64) -> Option<u64> {
    read_window_max(payload).filter(|&max_piece| max_piece > head)
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

/// Bound each never-emitted pool independently of startup activity bookkeeping.
pub(super) fn pool_progress_budget(
    first_output_deadline: &mut Option<Instant>,
    emitted: u64,
    startup_deadline: Option<Instant>,
    last_progress: Instant,
    now: Instant,
    stale_timeout: Duration,
) -> Option<Duration> {
    if emitted > 0 {
        *first_output_deadline = None;
        return stale_upstream_budget(last_progress, now, stale_timeout);
    }
    if let Some(deadline) = *first_output_deadline {
        return (now < deadline).then(|| deadline.duration_since(now));
    }
    if let Some(deadline) = startup_deadline {
        return Some(deadline.saturating_duration_since(now));
    }
    *first_output_deadline = Some(now + stale_timeout);
    Some(stale_timeout)
}

pub(super) fn should_refresh_stale_deadline(
    emitted: u64,
    made_output: bool,
    made_activity: bool,
) -> bool {
    made_output || (emitted == 0 && made_activity)
}

#[cfg(test)]
pub(super) fn schedule_piece_assignments(
    scheduler: &mut Scheduler,
    active_peers: &mut ActivePeers,
    next_needed: u64,
    head: u64,
) -> Vec<PeerAssignment> {
    active_peers.assign(scheduler, next_needed, head)
}

/// Hex preview of a message prefix for diagnostics (avoids pulling in a hex crate).
pub(super) fn hex_preview(bytes: &[u8]) -> String {
    bytes.iter().take(24).map(|b| format!("{b:02x}")).collect()
}

/// Read messages until the peer's extended handshake arrives; return its live `mi` window and
/// the `yourip` public address it echoed to us (issue #22 — the swarm telling us how it sees us
/// on this outbound connection; `None` when the peer sent no usable `yourip`).
/// Also return the latest Choke/Unchoke state, defaulting to choked on this connection.
pub(super) async fn read_peer_window(
    session: &mut PeerSession<TcpStream>,
) -> Option<(WireLivePosition, Option<IpAddr>, bool)> {
    let mut unchoked = false;
    for _ in 0..32 {
        let msg = session.read_message().await.ok()?;
        match msg {
            PeerMessage::Choke => unchoked = false,
            PeerMessage::Unchoke => unchoked = true,
            PeerMessage::Extended { ext_id: 0, payload } => {
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
                    unchoked,
                ));
            }
            _ => {}
        }
    }
    None
}
