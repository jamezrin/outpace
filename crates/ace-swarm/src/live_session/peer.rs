use super::*;

pub(super) fn prefer_window(candidate: &WireLivePosition, current: &WireLivePosition) -> bool {
    let score = |w: &WireLivePosition| (w.max_piece, w.position, -w.distance_from_source);
    score(candidate) > score(current)
}

pub(super) async fn peer_worker<S>(
    peer_id: u64,
    addr: SocketAddrV4,
    mut session: PeerSession<S>,
    mut commands: mpsc::Receiver<PeerCommand>,
    events: mpsc::Sender<PeerEvent>,
    request_floor: Arc<AtomicU64>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    loop {
        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(PeerCommand::RequestPiece { piece, chunks_per_piece }) => {
                        for chunk in 0..chunks_per_piece {
                            if piece < request_floor.load(Ordering::Relaxed) {
                                break;
                            }
                            if session.send(&chunk_request(piece as u32, chunk)).await.is_err() {
                                let _ = events.send(PeerEvent::Lost { peer_id, addr }).await;
                                return;
                            }
                        }
                    }
                    Some(PeerCommand::Send(msg)) => {
                        if session.send(&msg).await.is_err() {
                            let _ = events.send(PeerEvent::Lost { peer_id, addr }).await;
                            return;
                        }
                    }
                    Some(PeerCommand::Stop) | None => return,
                }
            }
            message = session.read_message() => {
                match message {
                    Ok(msg) => {
                        if events.send(PeerEvent::Message { peer_id, addr, msg }).await.is_err() {
                            return;
                        }
                    }
                    Err(_) => {
                        let _ = events.send(PeerEvent::Lost { peer_id, addr }).await;
                        return;
                    }
                }
            }
        }
    }
}

// Bound the entire handshake/window exchange, not just TCP establishment: a silent
// preferred source must not monopolize a reconnect batch for the peer I/O timeout.
pub(super) async fn connect_upstream(addr: SocketAddrV4, infohash: [u8; 20]) -> PeerConnectAttempt {
    let mut progress = PeerConnectProgress::new();
    let result = match tokio::time::timeout(
        CONNECT_TIMEOUT,
        connect_upstream_inner(addr, infohash, &mut progress),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(PeerConnectStage::TotalTimeout),
    };
    let timing = progress.finish();
    let outcome = match &result {
        Ok(_) => "connected",
        Err(PeerConnectStage::Tcp) => "tcp",
        Err(PeerConnectStage::Handshake) => "handshake",
        Err(PeerConnectStage::Window) => "window",
        Err(PeerConnectStage::TotalTimeout) => "total_timeout",
    };
    crate::alog!(
        "[ace] upstream attempt: result={outcome} {}",
        timing.summary()
    );
    match result {
        Ok(upstream) => PeerConnectAttempt::Connected(upstream, timing),
        Err(stage) => PeerConnectAttempt::Failed(PeerConnectFailure {
            addr,
            stage,
            timing,
        }),
    }
}

async fn connect_upstream_inner(
    addr: SocketAddrV4,
    infohash: [u8; 20],
    progress: &mut PeerConnectProgress,
) -> Result<ConnectedUpstream, PeerConnectStage> {
    crate::alog!("[ace] discovery stage=tcp-attempt");
    let mut session = match tokio::time::timeout(CONNECT_TIMEOUT, connect(&addr.to_string())).await
    {
        Ok(Ok(session)) => session,
        Ok(Err(_)) | Err(_) => {
            return Err(PeerConnectStage::Tcp);
        }
    };
    progress.advance(ConnectPhase::HandshakeSend);
    crate::alog!("[ace] discovery stage=tcp-connected");
    if session
        .perform_handshake_with_observer(infohash, random_peer_id(), || {
            progress.advance(ConnectPhase::HandshakeReply);
        })
        .await
        .is_err()
    {
        return Err(PeerConnectStage::Handshake);
    }
    progress.advance(ConnectPhase::Window);
    crate::alog!("[ace] discovery stage=bt-handshake");
    let Some((window, yourip)) = read_peer_window(&mut session).await else {
        return Err(PeerConnectStage::Window);
    };
    Ok(ConnectedUpstream {
        session,
        addr,
        window,
        yourip,
    })
}

pub(super) fn pool_refill_candidates(
    peers: &[SocketAddrV4],
    active: &HashSet<SocketAddrV4>,
) -> Vec<SocketAddrV4> {
    peers
        .iter()
        .copied()
        .filter(|addr| !active.contains(addr))
        .collect()
}

pub(super) fn background_discovery_options() -> DiscoveryOptions {
    DiscoveryOptions {
        peer_target: BACKGROUND_DISCOVERY_PEER_TARGET,
        dht_budget: BACKGROUND_DISCOVERY_BUDGET,
    }
}

/// Connect eligible peers concurrently, admitting discovery updates into spare slots.
/// Finish an exploration cohort before retrying more explored peers, so fast but
/// unproductive sources cannot repeatedly cancel an untouched alternative. Each address
/// is attempted at most once per round; failed attempts retain their normal cooldown.
pub(super) async fn connect_pool(
    infohash: [u8; 20],
    candidates: &mut SessionCandidates,
    live_recovery: LiveRecoveryConfig,
    discovery: &mut Option<DiscoveryRun>,
) -> Vec<ConnectedUpstream> {
    let mut attempted = HashSet::new();
    let mut stats = PeerConnectStats::default();
    loop {
        drain_discovery(discovery, candidates);
        let Some(cohort) = candidates
            .eligible(Instant::now())
            .into_iter()
            .find(|addr| !attempted.contains(addr))
            .map(|addr| candidates.explorations(addr))
        else {
            break;
        };
        let mut set = tokio::task::JoinSet::new();
        let mut connected: Vec<ConnectedUpstream> = Vec::new();
        let mut deadline = None;
        loop {
            drain_discovery(discovery, candidates);
            // New, less-explored addresses may join this cohort. Already pending
            // attempts keep their slot and are never restarted by a discovery update.
            for addr in candidates.eligible(Instant::now()) {
                if set.len() + connected.len() >= live_recovery.max_parallel_connect {
                    break;
                }
                if candidates.explorations(addr) <= cohort && attempted.insert(addr) {
                    set.spawn(connect_upstream(addr, infohash));
                }
            }
            if set.is_empty() || connected.len() >= live_recovery.max_active_upstreams {
                break;
            }
            tokio::select! {
                _ = async {
                    match deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                } => break,
                peer = next_discovery_peer(discovery), if discovery.is_some() => {
                    if let Some(peer) = peer {
                        candidates.learn(peer, CandidateKind::Discovered);
                    }
                }
                joined = set.join_next() => {
                    match joined {
                        Some(Ok(PeerConnectAttempt::Connected(candidate, timing))) => {
                            stats.record_connected(timing);
                            connected.push(candidate);
                            deadline.get_or_insert_with(|| {
                                tokio::time::Instant::now() + UPSTREAM_SELECTION_GRACE
                            });
                        }
                        Some(Ok(PeerConnectAttempt::Failed(failure))) => {
                            candidates.explored(failure.addr);
                            candidates.failed(failure.addr, Instant::now());
                            stats.record_failure(failure);
                        }
                        Some(Err(_)) => stats.record_task_failure(),
                        None => break,
                    }
                }
            }
        }
        if connected.is_empty() {
            continue;
        }
        connected.sort_by(|a, b| {
            if prefer_window(&a.window, &b.window) {
                std::cmp::Ordering::Less
            } else if prefer_window(&b.window, &a.window) {
                std::cmp::Ordering::Greater
            } else {
                a.addr.cmp(&b.addr)
            }
        });
        connected.truncate(live_recovery.max_active_upstreams);
        if stats.has_observations() {
            crate::alog!("[ace] initial upstream selection: {}", stats.summary());
        }
        // Dropping `set` aborts attempts beyond the selected pool. The caller's
        // cancellation branch drops this entire future and its pending attempts.
        return connected;
    }
    if stats.has_observations() {
        crate::alog!("[ace] no usable upstreams: {}", stats.summary());
    }
    Vec::new()
}
