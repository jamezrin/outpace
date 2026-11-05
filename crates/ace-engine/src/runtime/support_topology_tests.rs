//! Opt-in public-mode composition: run only inside a loopback-only network namespace.
use super::*;
use ace_peer::session::PeerSession;
use ace_swarm::{listen::InboundPolicy, store::PieceStore};
use ace_wire::{bencode::Bencode, live_codec::LiveChunk, message::PeerMessage};
use std::{collections::BTreeMap, time::Duration};

fn isolated_loopback() {
    assert_eq!(
        std::env::var("OUTPACE_TEST_ISOLATED_SUPPORT").as_deref(),
        Ok("1")
    );
    let devices = std::fs::read_to_string("/proc/net/dev").unwrap();
    let names: Vec<_> = devices
        .lines()
        .skip(2)
        .filter_map(|line| line.split_once(':').map(|(name, _)| name.trim()))
        .collect();
    assert_eq!(
        names,
        ["lo"],
        "public-mode fixture requires loopback-only namespace"
    );
    let routes = std::fs::read_to_string("/proc/net/route").unwrap();
    assert_eq!(routes.lines().count(), 1, "fixture requires no IPv4 routes");
}
fn free_address() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn cache_piece_files(root: &std::path::Path) -> std::io::Result<Vec<(String, u64)>> {
    let mut pending = vec![root.to_path_buf()];
    let mut pieces = Vec::new();
    while let Some(dir) = pending.pop() {
        if !dir.exists() {
            continue;
        }
        for item in std::fs::read_dir(dir)? {
            let item = item?;
            let kind = item.file_type()?;
            if kind.is_dir() {
                pending.push(item.path());
            } else if item.path().extension().is_some_and(|ext| ext == "piece") {
                pieces.push((
                    item.file_name().to_string_lossy().into_owned(),
                    item.metadata()?.len(),
                ));
            }
        }
    }
    Ok(pieces)
}

async fn support_command(address: SocketAddr, command: &str) -> std::io::Result<bool> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(address).await?;
    stream.write_all(format!("POST /support/relay/{command} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await?;
    let mut status = [0u8; 64];
    let mut used = 0;
    while used < status.len() {
        let count = stream.read(&mut status[used..]).await?;
        if count == 0 {
            break;
        }
        used += count;
        if status[..used].contains(&b'\n') {
            break;
        }
    }
    Ok(status[..used].starts_with(b"HTTP/1.1 202 "))
}

#[tokio::test]
#[ignore = "requires OUTPACE_TEST_ISOLATED_SUPPORT=1 inside loopback-only user/network namespace"]
async fn public_daemon_reseeds_signed_pieces_without_a_viewer() {
    topology(true, false, None).await;
}

#[tokio::test]
#[ignore = "requires OUTPACE_TEST_ISOLATED_SUPPORT=1 inside loopback-only user/network namespace"]
async fn explicit_source_reseeds_signed_pieces_without_supervision() {
    topology(false, false, None).await;
}

fn resource_snapshot() -> serde_json::Value {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let rss = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    serde_json::json!({"tokio_tasks":tokio::runtime::Handle::current().metrics().num_alive_tasks(),"rss_kib":rss, "fds":std::fs::read_dir("/proc/self/fd").unwrap().count(), "threads":std::fs::read_dir("/proc/self/task").unwrap().count()})
}

#[tokio::test]
#[ignore = "requires OUTPACE_TEST_ISOLATED_SUPPORT=1 inside loopback-only user/network namespace"]
async fn signed_reseeding_does_not_require_decoder_startup() {
    topology(true, true, None).await;
}

#[tokio::test]
#[ignore = "requires OUTPACE_TEST_ISOLATED_SUPPORT=1 inside loopback-only user/network namespace"]
async fn signed_support_restart_replaces_memory_generation() {
    topology(true, false, Some(CacheType::Memory)).await;
}

#[tokio::test]
#[ignore = "requires OUTPACE_TEST_ISOLATED_SUPPORT=1 inside loopback-only user/network namespace"]
async fn signed_support_restart_replaces_disk_generation() {
    topology(true, false, Some(CacheType::Disk)).await;
}

async fn topology(supervised: bool, decoder_unready: bool, restart_cache: Option<CacheType>) {
    isolated_loopback();
    let before = resource_snapshot();
    let mut active = serde_json::Value::Null;
    let mut shutdown_at = None;
    // Cache owners can outlive source shutdown. The bounded external launcher owns
    // the temporary directory until the entire test process (including writers) exits.
    let parent_dir = std::env::var_os("OUTPACE_TEST_SUPPORT_DIR");
    assert!(
        restart_cache.is_none() || parent_dir.is_some(),
        "restart fixtures require launcher-owned temporary directory"
    );
    let dir = parent_dir
        .clone()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!(
                "outpace-support-topology-{}",
                rand::random::<u64>()
            ))
        });
    std::fs::create_dir_all(&dir).unwrap();
    let auth = ace_wire::live_auth::LiveSourceAuth::generate();
    let mut fields = BTreeMap::new();
    fields.insert(b"name".to_vec(), Bencode::Bytes(b"synthetic".to_vec()));
    fields.insert(b"piece_length".to_vec(), Bencode::Int(32768));
    fields.insert(b"chunk_length".to_vec(), Bencode::Int(16384));
    fields.insert(b"bitrate".to_vec(), Bencode::Int(1000000));
    fields.insert(b"authmethod".to_vec(), Bencode::Bytes(b"RSA".to_vec()));
    fields.insert(b"pubkey".to_vec(), Bencode::Bytes(auth.pubkey_der()));
    let descriptor = ace_wire::transport::encode_transport(&Bencode::Dict(fields));
    let info = ace_swarm::resolve::stream_info_from_transport(&descriptor).unwrap();
    let no_psi: Vec<u8> = (0..4)
        .flat_map(|counter| {
            let mut packet = vec![0x55; 188];
            packet[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x10 | counter]);
            packet
        })
        .collect();
    let fixture: &[u8] = if decoder_unready {
        &no_psi
    } else {
        include_bytes!("../../../../tests/vectors/media/h264-keyframes.ts")
    };
    let payload: Vec<_> = fixture
        .iter()
        .copied()
        .cycle()
        .take(32768 - auth.signature_len())
        .collect();
    let mut wire = payload.clone();
    wire.extend(auth.sign(&payload));
    let generations = if restart_cache.is_some() { 2 } else { 1 };
    let mut replacement_payload = payload.clone();
    replacement_payload[4] ^= 0x11;
    let mut replacement_wire = replacement_payload.clone();
    replacement_wire.extend(auth.sign(&replacement_payload));
    let header = ace_wire::live_codec::piece_header_from_unix_seconds(1_700_000_000.0);
    let source_registry = ace_swarm::listen::SeedRegistry::new();
    let (store, lease) =
        source_registry.lease_store(info.infohash, || PieceStore::new(32768, 16384, 32768 * 4));
    for piece in [7, 8] {
        PieceStore::shared_put_complete_piece(&store, piece, header, &wire)
            .await
            .unwrap();
    }
    let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let SocketAddr::V4(upstream_address) = upstream.local_addr().unwrap() else {
        unreachable!()
    };
    let admission = InboundPolicy {
        max_inbound: 1,
        max_per_stream: 1,
        max_unchoked: 1,
    }
    .admission();
    let (closed_tx, mut closed_rx) = tokio::sync::mpsc::channel(2);
    let mut source = tokio::spawn(async move {
        let mut result = Ok(());
        for _ in 0..generations {
            let (socket, _) = upstream.accept().await?;
            result = admission
                .accept(
                    socket,
                    source_registry.clone(),
                    ace_wire::handshake::random_peer_id(),
                    [0; 8],
                    Arc::new(ace_wire::identity::Identity::generate()),
                    [127, 0, 0, 1],
                )
                .unwrap()
                .await;
            let closed = matches!(&result, Err(ace_peer::PeerError::Closed));
            let _ = closed_tx.send(closed).await;
            if !closed {
                break;
            }
        }
        result
    });
    let path = dir.join("fixture.acelive");
    std::fs::write(&path, descriptor).unwrap();
    let peer = free_address();
    let http = free_address();
    let config = Config {
        bind: http,
        rtmp_bind: free_address(),
        peer_listen: peer,
        data_dir: dir.join("state"),
        cache_dir: dir.join("cache"),
        enable_port_mapping: false,
        max_sessions: 1,
        cache_type: restart_cache.unwrap_or(CacheType::Memory),
        max_inbound_peers: 1,
        max_inbound_per_stream: 1,
        prefetch_pieces: Some(1),
        support_sources: if supervised {
            crate::support_config::SupportSources::parse(Some(r#"{"relay":"local:fixture"}"#))
                .unwrap()
        } else {
            Default::default()
        },
        ..Default::default()
    };
    let mut config = config;
    config.startup_buffer.target_ms = 0;
    let mut runtime = build_runtime(config, vec![upstream_address]).await.unwrap();
    // Inject only synthetic metadata using existing SDK configuration. Public daemon
    // configuration intentionally forbids local descriptor maps; remote/catalog
    // resolution is independently covered and must not be weakened for this fixture.
    let descriptors = crate::local_descriptors::DescriptorPaths::from_config(Some(
        &serde_json::json!({"fixture": path}).to_string(),
    ))
    .unwrap()
    .load()
    .unwrap();
    let provider = AceProvider::new(Arc::new(ace_wire::identity::Identity::generate()), 0)
        .with_local_descriptors(descriptors)
        .with_bootstrap_peers(vec![upstream_address])
        .with_seed_registry(runtime.seed_registry.clone())
        .with_prefetch_pieces(Some(1))
        .with_startup_buffer(runtime.config.startup_buffer)
        .with_seed_store_bytes(32768 * 4)
        .with_cache(
            runtime.config.cache_type,
            if restart_cache.is_some() {
                runtime.config.cache_dir.clone()
            } else {
                Default::default()
            },
        );
    let mut providers = ProviderRegistry::new();
    providers.register(Arc::new(provider));
    runtime.manager = StreamManager::with_session_limit(providers, 1);
    let runtime_cache = runtime.config.cache_dir.clone();
    let registry = runtime.seed_registry.clone();
    let manager = runtime.manager.clone();
    let _baseline = if supervised {
        None
    } else {
        Some(manager.get_or_start("ace", "local:fixture").await.unwrap())
    };
    let (shutdown, receiver) = tokio::sync::oneshot::channel();
    let observe = async {
        let result = tokio::time::timeout(Duration::from_secs(15), async {
            for generation in 0..generations {
                let requested_piece = if generation == 0 { 7 } else { 17 };
                let expected_header = if generation == 0 {
                    header
                } else {
                    ace_wire::live_codec::piece_header_from_unix_seconds(1_700_000_001.0)
                };
                let expected_wire = if generation == 0 {
                    &wire
                } else {
                    &replacement_wire
                };
                loop {
                    if let Some(store) = registry.get(&info.infohash) {
                        if PieceStore::shared_chunk(&store, requested_piece.into(), 0)
                            .await
                            .is_some()
                        {
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                let sessions = manager.list().await;
                let no_viewers =
                    sessions.len() == 1 && sessions[0].2 == 0 && manager.admission_status().0 == 1;
                let mut leecher = PeerSession::new(tokio::net::TcpStream::connect(peer).await?);
                leecher
                    .perform_handshake(info.infohash, ace_wire::handshake::random_peer_id())
                    .await?;
                leecher
                    .send_signed_extended_handshake(
                        &ace_wire::extended::OutgoingExtendedHandshake {
                            ace_metadata_version: 1,
                            ut_metadata_id: 2,
                            mi: None,
                            node: Default::default(),
                            peer_ip: None,
                            metadata_size: None,
                        },
                        &ace_wire::identity::Identity::generate(),
                    )
                    .await?;
                leecher.send(&PeerMessage::Interested).await?;
                while !matches!(leecher.read_message().await?, PeerMessage::Unchoke) {}
                let mut received = Vec::new();
                for chunk in [0u16, 1] {
                    leecher
                        .send(&ace_wire::live_codec::chunk_request(requested_piece, chunk))
                        .await?;
                    loop {
                        if let Some(reply) = LiveChunk::from_message(&leecher.read_message().await?)
                        {
                            if reply.piece != requested_piece
                                || reply.chunk != chunk
                                || reply.piece_header != expected_header
                            {
                                return Ok::<_, ace_peer::PeerError>(false);
                            }
                            received.extend(reply.data);
                            break;
                        }
                    }
                }
                drop(leecher);
                // A successful new handshake at global cap1 proves the old admitted
                // handler released its permit. accept() owns that permit until its
                // handle_inbound future completes, including its retained store reader.
                let mut reader_released = false;
                for _ in 0..32 {
                    let mut probe = PeerSession::new(tokio::net::TcpStream::connect(peer).await?);
                    if probe
                        .perform_handshake(info.infohash, ace_wire::handshake::random_peer_id())
                        .await
                        .is_ok()
                    {
                        reader_released = true;
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
                active = resource_snapshot();
                let output_policy_satisfied = if decoder_unready {
                    let mut gate = ace_media::mpegts::KeyframeGate::new();
                    let gate_bytes = gate.push(&received[..payload.len()]);
                    let mut ready_control = ace_media::mpegts::KeyframeGate::new();
                    let control_bytes = ready_control.push(include_bytes!(
                        "../../../../tests/vectors/media/h264-keyframes.ts"
                    ));
                    // Inspect the actual native playback gate on the exact served
                    // media, with a ready-fixture positive control. Downloaded byte
                    // counters represent transport output, not decoder readiness.
                    !gate.is_locked()
                        && gate_bytes.is_empty()
                        && ready_control.is_locked()
                        && !control_bytes.is_empty()
                } else {
                    true
                };
                let valid = no_viewers
                    && output_policy_satisfied
                    && reader_released
                    && &received == expected_wire
                    && ace_wire::live_auth::verify_piece(
                        &auth.pubkey_der(),
                        &received[..payload.len()],
                        &received[payload.len()..],
                    );
                let current_store = registry.get(&info.infohash).expect("active producer store");
                let retained = current_store.lock().await.have_pieces();
                let bounded_store =
                    retained.len() <= 4 && (generation == 0 || !retained.contains(&7));
                let piece_files = cache_piece_files(&runtime_cache)?;
                let disk_evidence = if restart_cache == Some(CacheType::Disk) {
                    !piece_files.is_empty()
                        && piece_files.len() <= 4
                        && piece_files.iter().map(|(_, bytes)| bytes).sum::<u64>() <= 32768 * 4
                        && (generation == 0
                            || !piece_files.iter().any(|(name, _)| name == "7.piece"))
                } else {
                    piece_files.is_empty()
                };
                if !valid || !bounded_store || !disk_evidence {
                    return Ok::<_, ace_peer::PeerError>(false);
                }
                drop(current_store);
                if generation == 0 && generations == 2 {
                    if !support_command(http, "stop").await? {
                        return Ok(false);
                    }
                    loop {
                        if !registry.serves(&info.infohash)
                            && manager.admission_status().0 == 0
                            && cache_piece_files(&runtime_cache)?.is_empty()
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    if closed_rx.recv().await != Some(true) {
                        return Ok(false);
                    }
                    // New signed source head is published only after confirmed old source teardown.
                    for piece in [17, 18] {
                        PieceStore::shared_put_complete_piece(
                            &store,
                            piece,
                            ace_wire::live_codec::piece_header_from_unix_seconds(1_700_000_001.0),
                            &replacement_wire,
                        )
                        .await?;
                    }
                    if !support_command(http, "start").await? {
                        return Ok(false);
                    }
                }
            }
            Ok::<_, ace_peer::PeerError>(true)
        })
        .await;
        shutdown_at = Some(std::time::Instant::now());
        let _ = shutdown.send(());
        result
    };
    let (served, observed) = tokio::join!(
        serve_http_until(runtime, async {
            let _ = receiver.await;
        }),
        observe
    );
    let cleanup_us = shutdown_at.unwrap().elapsed().as_micros();
    let source_join = tokio::time::timeout(Duration::from_secs(2), &mut source).await;
    if source_join.is_err() {
        source.abort();
        let _ = source.await;
    }
    let removed = !registry.serves(&info.infohash);
    drop(lease);
    if parent_dir.is_none() {
        std::fs::remove_dir_all(dir).unwrap();
    }
    crate::alog!(
        "[support-fixture] {}",
        serde_json::json!({"supervised":supervised,"decoder_unready":decoder_unready,"before":before,"active":active,"after":resource_snapshot(),"cleanup_us":cleanup_us})
    );
    assert!(served.is_ok(), "runtime: {served:?}");
    assert!(
        matches!(observed, Ok(Ok(true))),
        "signed daemon reseeding: {observed:?}"
    );
    assert!(
        matches!(source_join, Ok(Ok(Err(ace_peer::PeerError::Closed)))),
        "owned upstream: {source_join:?}"
    );
    assert!(removed);
    assert_eq!(manager.admission_status().0, 0);
}
