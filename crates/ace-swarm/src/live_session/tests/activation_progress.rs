use super::*;
use tokio::io::AsyncReadExt;

#[tokio::test]
async fn activation_write_allows_healthy_control() {
    diagnostic(true, false, false).await;
}
#[tokio::test]
async fn activation_write_allows_consumer_close() {
    diagnostic(true, true, false).await;
}
#[tokio::test]
async fn activation_without_backpressure_control() {
    diagnostic(false, false, false).await;
}

#[tokio::test]
async fn activation_pending_allows_timer_output_and_current_cursor_revalidation() {
    diagnostic(true, false, true).await;
}

async fn pair() -> (TcpStream, TcpStream, SocketAddrV4) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(addr) => addr,
        _ => unreachable!(),
    };
    let client = TcpStream::connect(addr).await.unwrap();
    let (remote, _) = listener.accept().await.unwrap();
    (client, remote, addr)
}

async fn unchoke(remote: &mut PeerSession<TcpStream>, budget: Duration) -> bool {
    tokio::time::timeout(budget, async {
        loop {
            match remote.read_message().await {
                Ok(PeerMessage::Unchoke) => return true,
                Ok(_) => (),
                Err(_) => return false,
            }
        }
    })
    .await
    .unwrap_or(false)
}

async fn diagnostic(pressure: bool, close: bool, media: bool) {
    let (candidate, mut remote, addr) = pair().await;
    // Bounded raw padding on the actual candidate transport. WouldBlock is only
    // a prefill stopping condition: the activation future itself must witness
    // Pending later. No socket options or global network settings are changed.
    let mut padding = 0;
    if pressure {
        let deadline = Instant::now() + Duration::from_secs(2);
        let bytes = [0x5a; 65536];
        while padding < 32 * 1024 * 1024 && Instant::now() < deadline {
            match candidate.try_write(&bytes) {
                Ok(n) => padding += n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Allow loopback ACKs to consume transient readiness, then
                    // retry. The final real send witness is still mandatory.
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(e) => panic!("prefill: {e}"),
            }
        }
    }
    let (healthy_client, healthy_remote, healthy_addr) = pair().await;
    let mut healthy = PeerSession::new(healthy_remote);
    let payload: Vec<u8> = (0..4)
        .flat_map(|cc| {
            let mut packet = vec![0x55; 188];
            packet[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10 | cc]);
            packet
        })
        .collect();
    let mut info = info();
    if media {
        info.piece_length = payload.len() as u64;
        info.chunk_length = payload.len() as u64 / 2;
    }

    let policy = LiveRecoveryConfig {
        max_active_upstreams: 2,
        request_timeout_ms: 5000,
        ..default_live_recovery()
    };
    let store = Arc::new(tokio::sync::Mutex::new(PieceStore::new(
        info.piece_length,
        info.chunk_length,
        4096,
    )));
    let monitor = Arc::new(ReachabilityMonitor::default());
    let observed = monitor.clone();
    let pending = Arc::new(tokio::sync::Notify::new());
    let probe = super::super::pool::ActivationProbe {
        candidate: std::cell::RefCell::new(Some(ConnectedUpstream {
            session: PeerSession::new(candidate),
            addr,
            window: live_pos(7, 8),
            unchoked: false,
            yourip: Some(std::net::Ipv4Addr::new(192, 0, 2, 1).into()),
        })),
        addr,
        pending: pending.clone(),
    };
    let (tx, mut rx) = mpsc::channel(4);
    let mut pool = tokio::spawn(
        super::super::pool::ACTIVATION_PROBE.scope(probe, async move {
            let seed = SeedConfig {
                metrics: None,
                registry: SeedRegistry::new(),
                store_factory: test_store_factory(4096, None),
                enabled: true,
                prefetch_pieces: 0,
                live_recovery: policy,
                warm_peers: WarmPeerCache::memory(),
            };
            let mut continuity = None;
            follow_peer_pool(
                vec![ConnectedUpstream {
                    session: PeerSession::new(healthy_client),
                    addr: healthy_addr,
                    window: live_pos(7, 8),
                    unchoked: false,
                    yourip: None,
                }],
                &info,
                &Identity::generate(),
                2,
                &tx,
                &Arc::new(AtomicU64::new(0)),
                &Arc::new(AtomicU64::new(0)),
                &Arc::new(AtomicU32::new(0)),
                &seed,
                &store,
                &mut continuity,
                vec![],
                vec![],
                completed_discovery(|_| Box::pin(async { vec![] })),
                &mut SessionCandidates::default(),
                &Arc::new(AtomicU32::new(0)),
                Some(&monitor),
            )
            .await;
        }),
    );
    let witnessed = if pressure {
        tokio::time::timeout(Duration::from_secs(3), pending.notified())
            .await
            .is_ok()
    } else {
        true
    };
    let mut joined = None;
    let mut control = false;
    if witnessed {
        if close {
            rx.close();
            joined = tokio::time::timeout(Duration::from_millis(250), &mut pool)
                .await
                .ok();
        } else {
            let sent = healthy.send(&PeerMessage::Interested).await.is_ok();
            control = sent && unchoke(&mut healthy, Duration::from_millis(250)).await;
        }
    }
    let observation_before_release = observed.observed().is_some_and(|value| value.total == 1);
    let mut output_before_release = false;
    if media && witnessed {
        // The pending candidate cannot corroborate until commit. Only the actor's
        // existing startup timer releases this healthy stream's output hold.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        for (chunk, data) in payload.chunks(payload.len() / 2).enumerate() {
            let mut block = vec![0; 8];
            block.extend_from_slice(&(chunk as u16).to_be_bytes());
            block.extend_from_slice(data);
            healthy
                .send(&PeerMessage::Piece {
                    index: 0,
                    begin: 8,
                    block,
                })
                .await
                .unwrap();
        }
        output_before_release = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .is_ok_and(|output| {
                output
                    .is_some_and(|output| output.bytes.as_ref() == &payload[..payload.len() - 188])
            });
        healthy.send(&PeerMessage::Have(20)).await.unwrap();
        // Causal control after Have: actor has consumed the preceding cursor/head
        // changes before candidate release, even if it already unchoked once.
        let mut request = vec![0; 4];
        request.extend_from_slice(&8u32.to_be_bytes());
        request.extend_from_slice(&0u16.to_be_bytes());
        healthy
            .send(&PeerMessage::Unknown {
                id: 6,
                payload: request,
            })
            .await
            .unwrap();
        let barrier = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match healthy.read_message().await {
                    Ok(PeerMessage::Piece { begin: 8, .. }) => break true,
                    Ok(_) => (),
                    Err(_) => break false,
                }
            }
        })
        .await
        .unwrap_or(false);
        eprintln!("media barrier={barrier} output={output_before_release}");
        output_before_release &= barrier;
    }
    // Unconditional release before any desired-progress assertion. Drain exactly
    // the raw padding, then decode the production signed handshake + Interested.
    let release = tokio::time::timeout(Duration::from_secs(5), async {
        let mut scratch = [0; 65536];
        let mut remaining = padding;
        while remaining > 0 {
            let n = remaining.min(scratch.len());
            remote.read_exact(&mut scratch[..n]).await.ok()?;
            remaining -= n;
        }
        Some(())
    })
    .await
    .is_ok_and(|v| v.is_some());
    // A timely cancellation may truncate write_all at any byte. Require raw EOF,
    // and validate every complete handshake rather than accepting a decode error.
    let cancelled = close && joined.as_ref().is_some_and(|result| result.is_ok());
    let mut cancelled_eof = false;
    if cancelled {
        let mut bytes = Vec::new();
        cancelled_eof =
            tokio::time::timeout(Duration::from_secs(3), remote.read_to_end(&mut bytes))
                .await
                .is_ok_and(|result| result.is_ok());
        if bytes.len() >= 4 {
            let size = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
            if bytes.len() >= 4 + size {
                assert!(size >= 2 && bytes[4] == 20 && bytes[5] == 0);
                assert!(valid_signature(&bytes[6..4 + size]));
            }
        }
    }
    let mut remote = PeerSession::new(remote);
    let activated = !cancelled
        && tokio::time::timeout(Duration::from_secs(3), async {
            (match remote.read_message().await {
                Ok(PeerMessage::Extended { ext_id: 0, payload }) => valid_signature(&payload),
                _ => false,
            }) && matches!(remote.read_message().await, Ok(PeerMessage::Interested))
        })
        .await
        .unwrap_or(false);
    let obsolete_closed = if media {
        tokio::time::timeout(Duration::from_secs(2), remote.read_message())
            .await
            .is_ok_and(|result| matches!(result, Err(ace_peer::PeerError::Closed)))
            && !pool.is_finished()
    } else {
        true
    };
    let recovered = close || control || unchoke(&mut healthy, Duration::from_secs(3)).await;
    rx.close();
    let exit_before = joined.is_some();
    let cleanup = if let Some(result) = joined {
        result.is_ok()
    } else {
        match tokio::time::timeout(Duration::from_secs(3), &mut pool).await {
            Ok(result) => result.is_ok(),
            Err(_) => {
                pool.abort();
                let _ = pool.await;
                false
            }
        }
    };
    let eof = tokio::time::timeout(Duration::from_secs(3), async {
        if !cancelled {
            loop {
                match remote.read_message().await {
                    Ok(_) => (),
                    Err(ace_peer::PeerError::Closed) => break,
                    Err(error) => panic!("candidate closure was not EOF: {error:?}"),
                }
            }
        }
        loop {
            match healthy.read_message().await {
                Ok(_) => (),
                Err(ace_peer::PeerError::Closed) => break,
                Err(error) => panic!("healthy closure was not EOF: {error:?}"),
            }
        }
    })
    .await
    .is_ok();
    drop(remote);
    drop(healthy);
    eprintln!("activation pressure={pressure} close={close} padding={padding} real_send_pending={} control_before={control} exit_before={exit_before} drain={release} activated={activated} recovered={recovered} cleanup={cleanup} eof={eof}", pressure && witnessed);
    assert!(
        observation_before_release && observed.observed().is_some_and(|value| value.total == 1),
        "exactly one echoed-IP observation even for cancelled writes"
    );
    eprintln!("media={media} output_before_release={output_before_release} obsolete_closed={obsolete_closed}");
    assert!(
        !media || (output_before_release && obsolete_closed),
        "timer/output before release, then obsolete candidate EOF while pool stays live"
    );
    assert!(
        witnessed
            && release
            && (if cancelled { cancelled_eof } else { activated })
            && recovered
            && cleanup
            && eof,
        "fixture/release controls"
    );
    assert!(
        if close { exit_before } else { control },
        "desired actor progress within 250ms while real activation write is Pending"
    );
}

fn valid_signature(payload: &[u8]) -> bool {
    let Ok(parsed) = ace_wire::extended::ExtendedHandshake::parse(payload) else {
        return false;
    };
    if parsed.ace_metadata_version != Some(1) || parsed.ut_metadata_id() != Some(2) {
        return false;
    }
    let Some(mi) = parsed.raw.get(b"mi") else {
        return false;
    };
    for (key, expected) in [
        (b"min_piece".as_slice(), 8),
        (b"max_piece".as_slice(), 8),
        (b"position".as_slice(), -1),
        (b"distance_from_source".as_slice(), 1),
    ] {
        if mi.get(key).and_then(ace_wire::bencode::Bencode::as_int) != Some(expected) {
            return false;
        }
    }
    let ace_wire::bencode::Bencode::Dict(dict) = parsed.raw else {
        return false;
    };
    let Some(node) = dict
        .get(b"node_id".as_slice())
        .and_then(|v| v.as_bytes())
        .and_then(|v| <&[u8; 32]>::try_from(v).ok())
    else {
        return false;
    };
    let Some(sig) = dict
        .get(b"signature".as_slice())
        .and_then(|v| v.as_bytes())
        .and_then(|v| <&[u8; 64]>::try_from(v).ok())
    else {
        return false;
    };
    ace_wire::identity::verify_handshake(node, sig, &dict)
}
