//! Generated input through the actual SRT, signed peer, provider, HTTP and decoder paths.
use super::{config::SrtConfig, SrtService};
use crate::{
    ace_provider::AceProvider,
    http::{AppState, BroadcastState},
    manager::StreamManager,
    provider::ProviderRegistry,
};
use ace_swarm::{
    listen::{InboundPolicy, PeerListener, SeedRegistry},
    peer_policy::PeerPolicy,
};
use bytes::Bytes;
use std::{
    fs::{File, OpenOptions},
    io,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::sync::watch;

pub(super) struct Artifacts(pub(super) PathBuf);
impl Artifacts {
    pub(super) fn new() -> Self {
        use std::os::unix::fs::PermissionsExt;
        let path =
            std::env::temp_dir().join(format!("outpace-srt-playback-{}", rand::random::<u64>()));
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self(path)
    }
    pub(super) fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
    pub(super) fn file(&self, name: &str) -> File {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(self.path(name))
            .unwrap()
    }
}
impl Drop for Artifacts {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
pub(super) struct OwnedChild(pub(super) Child);
struct OwnedPeer(tokio::task::JoinHandle<()>);
impl Drop for OwnedPeer {
    fn drop(&mut self) {
        self.0.abort();
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}
pub(super) fn bounded_command(command: &mut Command) {
    use std::os::unix::process::CommandExt;
    // All output files are private and capped even if a child fails noisily.
    unsafe {
        command.pre_exec(|| {
            libc::umask(0o077);
            let limit = libc::rlimit {
                rlim_cur: 1 << 20,
                rlim_max: 1 << 20,
            };
            if libc::setrlimit(libc::RLIMIT_FSIZE, &limit) == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        });
    }
    command.stdin(Stdio::null());
}
async fn decode(capture: &Path, artifacts: &Artifacts) -> Result<(usize, u64), String> {
    let mut command = Command::new("ffmpeg");
    command
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-protocol_whitelist",
            "file",
            "-threads",
            "1",
            "-i",
        ])
        .arg(capture)
        .args([
            "-map", "0:v:0", "-t", "1", "-threads", "1", "-f", "framemd5",
        ])
        .arg(artifacts.path("video.md5"))
        .args([
            "-map",
            "0:a:0",
            "-t",
            "1",
            "-c:a",
            "pcm_s16le",
            "-f",
            "s16le",
        ])
        .arg(artifacts.path("audio.pcm"))
        .stdout(Stdio::null())
        .stderr(artifacts.file("decoder.log"));
    bounded_command(&mut command);
    let mut child = OwnedChild(command.spawn().map_err(|e| e.to_string())?);
    let status = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Some(status) = child.0.try_wait().map_err(|e| e.to_string())? {
                return Ok::<_, String>(status);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| "decoder deadline")??;
    if !status.success() {
        return Err(format!("decoder exited {status}"));
    }
    let video = std::fs::read_to_string(artifacts.path("video.md5")).map_err(|e| e.to_string())?;
    let frames = video
        .lines()
        .filter(|line| !line.starts_with('#') && line.contains(','))
        .count();
    let audio = std::fs::read(artifacts.path("audio.pcm")).map_err(|e| e.to_string())?;
    if frames == 0 || audio.len() < 4096 || !audio.iter().any(|byte| *byte != 0) {
        return Err("no decoded video/audio witness".into());
    }
    Ok((frames, audio.len() as u64))
}

fn publish(
    artifacts: &Artifacts,
    address: std::net::SocketAddr,
    name: &str,
    log: &str,
) -> io::Result<OwnedChild> {
    let url = format!("srt://{}?mode=caller&streamid={name}&passphrase=synthetic-loopback-passphrase&pkt_size=1316&connect_timeout=1000", address);
    let mut command = Command::new("ffmpeg");
    command
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-re",
            "-f",
            "lavfi",
            "-i",
            "testsrc=size=160x90:rate=25",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=48000",
            "-t",
            "10",
            "-c:v",
            "libx264",
            "-threads",
            "1",
            "-preset",
            "ultrafast",
            "-tune",
            "zerolatency",
            "-pix_fmt",
            "yuv420p",
            "-g",
            "12",
            "-c:a",
            "aac",
            "-b:a",
            "64k",
            "-muxrate",
            "1000000",
            "-f",
            "mpegts",
            &url,
        ])
        .stdout(Stdio::null())
        .stderr(artifacts.file(log));
    bounded_command(&mut command);
    Ok(OwnedChild(command.spawn()?))
}

#[tokio::test]
#[ignore = "requires OUTPACE_TEST_SRT_FFMPEG=1 and FFmpeg with SRT/libx264/AAC; generated loopback only"]
async fn generated_srt_reaches_signed_peer_http_body_and_decoded_video_audio() {
    use axum::{body::Body, http::Request};
    use futures::StreamExt;
    use tower::ServiceExt;
    assert_eq!(std::env::var("OUTPACE_TEST_SRT_FFMPEG").as_deref(), Ok("1"));
    let artifacts = Artifacts::new();
    std::io::Write::write_all(&mut artifacts.file("key"), b"synthetic-loopback-passphrase")
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let policy = PeerPolicy::from_private_peers(Some(&endpoint.to_string())).unwrap();
    let registry = crate::broadcast::BroadcastRegistry::new();
    let seed = SeedRegistry::new();
    let state = BroadcastState {
        peer_policy: policy.clone(),
        registry: registry.clone(),
        seed_registry: seed.clone(),
        trackers: vec![],
        store_bytes: 4 << 20,
        inbound_peer_port: watch::channel(None).1,
    };
    let prepared = SrtConfig {
        bind: Some("127.0.0.1:0".parse().unwrap()),
        passphrase_file: Some(artifacts.path("key")),
        ..SrtConfig::default()
    }
    .prepare(true)
    .unwrap();
    let mut service = SrtService::start(prepared, state.clone())
        .await
        .unwrap()
        .unwrap();
    let admission = InboundPolicy {
        max_inbound: 2,
        max_per_stream: 2,
        max_unchoked: 2,
    }
    .admission();
    let (stop_peer, stopped) = watch::channel(false);
    let mut peer = OwnedPeer(tokio::spawn(PeerListener::serve_with_policy_until(
        listener,
        seed,
        ace_wire::handshake::random_peer_id(),
        [0; 8],
        admission,
        Arc::new(ace_wire::identity::Identity::generate()),
        None,
        stopped,
    )));
    let publisher = publish(
        &artifacts,
        service.local_addr().unwrap(),
        "synthetic-srt",
        "publisher.log",
    )
    .unwrap();
    let mut manager = None;
    let origin_manager = StreamManager::new(ProviderRegistry::new());
    let observation = tokio::time::timeout(Duration::from_secs(25), async {
        let broadcast = loop {
            if let Some(broadcast) = registry.get("synthetic-srt").await {
                if broadcast.store.lock().await.has_piece(3) {
                    break broadcast;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        let origin = crate::http::router(AppState {
            peer_policy: policy.clone(),
            manager: origin_manager.clone(),
            networks: vec![],
            resolve_content_ids_in_getstream: true,
            ace_sessions: Arc::new(crate::http::AceSessionStore::default()),
            experimental_ace_compat: false,
            origin_hls: None,
            broadcasts: Some(state.clone()),
        });
        let descriptor_response = origin
            .oneshot(
                Request::get("/broadcast/synthetic-srt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .map_err(|e| e.to_string())?;
        if descriptor_response.status() != axum::http::StatusCode::OK {
            return Err("descriptor HTTP status".into());
        }
        let descriptor = axum::body::to_bytes(descriptor_response.into_body(), 1 << 20)
            .await
            .map_err(|e| e.to_string())?;
        if descriptor.as_ref() != broadcast.transport_bytes.as_slice() {
            return Err("descriptor identity mismatch".into());
        }
        let descriptors =
            crate::local_descriptors::LocalDescriptors::from_test_bytes("srt-fixture", &descriptor)
                .map_err(|e| e.to_string())?;
        let provider = AceProvider::new(Arc::new(ace_wire::identity::Identity::generate()), 0)
            .with_peer_policy(policy.clone())
            .with_local_descriptors(descriptors)
            .with_prefetch_pieces(Some(2))
            .with_startup_buffer(crate::config::StartupBufferConfig {
                target_ms: 0,
                max_bytes: 1 << 20,
                timeout_ms: 3000,
            })
            .with_seeding_enabled(false);
        let mut providers = ProviderRegistry::new();
        providers.register(Arc::new(provider));
        let sessions = StreamManager::new(providers);
        manager = Some(sessions.clone());
        let app = crate::http::router(AppState {
            peer_policy: policy.clone(),
            manager: sessions,
            networks: vec!["ace".into()],
            resolve_content_ids_in_getstream: true,
            ace_sessions: Arc::new(crate::http::AceSessionStore::default()),
            experimental_ace_compat: false,
            origin_hls: None,
            broadcasts: None,
        });
        let response = app
            .oneshot(
                Request::get("/streams/ace/local:srt-fixture.ts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .map_err(|e| e.to_string())?;
        if response.status() != axum::http::StatusCode::OK {
            return Err(format!("HTTP status {}", response.status()));
        }
        let mut body = response.into_body().into_data_stream();
        let mut capture = Vec::new();
        while capture.len() < 256 * 1024 {
            let bytes: Bytes = body
                .next()
                .await
                .ok_or("HTTP body ended")?
                .map_err(|e| e.to_string())?;
            if capture.len() + bytes.len() > 512 * 1024 {
                return Err("HTTP capture limit".into());
            }
            capture.extend_from_slice(&bytes);
        }
        drop(body);
        std::io::Write::write_all(&mut artifacts.file("capture.ts"), &capture)
            .map_err(|e| e.to_string())?;
        let decoded = decode(&artifacts.path("capture.ts"), &artifacts).await?;
        Ok::<_, String>((capture.len(), decoded))
    })
    .await;
    // Capture outcomes first, then always close clients, native ingress, and every
    // real accepted peer before assertions or propagating an observation error.
    drop(publisher);
    let manager_stopped = if let Some(manager) = manager {
        manager.shutdown().await
    } else {
        true
    };
    let origin_stopped = origin_manager.shutdown().await;
    let native_stopped = service.shutdown().await;
    stop_peer.send_replace(true);
    let peer_joined = tokio::time::timeout(Duration::from_secs(3), &mut peer.0).await;
    if peer_joined.is_err() {
        peer.0.abort();
        let _ = (&mut peer.0).await;
    }
    let broadcasts_stopped = registry.shutdown().await;
    assert!(manager_stopped && origin_stopped);
    assert!(native_stopped.is_ok(), "native cleanup: {native_stopped:?}");
    assert!(
        matches!(peer_joined, Ok(Ok(()))),
        "owned peer connections must join"
    );
    assert!(broadcasts_stopped.is_ok());
    let (bytes, (frames, audio)) = observation
        .unwrap_or_else(|error| {
            panic!(
                "SRT-to-decoder deadline: {error}; artifacts={}",
                artifacts.0.display()
            )
        })
        .unwrap_or_else(|error| {
            panic!(
                "SRT/peer/HTTP/decoder failure: {error}; artifacts={}",
                artifacts.0.display()
            )
        });
    assert!(bytes >= 256 * 1024 && frames > 0 && audio >= 4096);
}

async fn wait_piece(
    registry: &crate::broadcast::BroadcastRegistry,
    name: &str,
    piece: u64,
) -> Result<crate::broadcast::Broadcast, String> {
    tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            if let Some(broadcast) = registry.get(name).await {
                if broadcast.store.lock().await.has_piece(piece) {
                    return broadcast;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| "signed piece deadline".into())
}
async fn check_signature(
    broadcast: &crate::broadcast::Broadcast,
    piece: u64,
) -> Result<(), String> {
    let store = broadcast.store.lock().await;
    let mut bytes = Vec::new();
    for chunk in 0..store.chunks_per_piece() {
        bytes.extend_from_slice(&store.chunk(piece, chunk).ok_or("missing signed chunk")?);
    }
    let (payload, signature) =
        ace_wire::live_auth::split_piece(&bytes, broadcast.auth.signature_len())
            .ok_or("signed piece shorter than signature")?;
    if !ace_wire::live_auth::verify_piece(&broadcast.auth.pubkey_der(), payload, signature) {
        return Err("signed piece did not verify".into());
    }
    Ok(())
}
#[tokio::test]
#[ignore = "requires OUTPACE_TEST_SRT_FFMPEG=1 and FFmpeg with SRT/libx264/AAC; generated loopback only"]
async fn generated_srt_reconnect_and_restart_preserve_identity_and_monotone_indexes() {
    use crate::broadcast::{BroadcastError, BroadcastRegistry, ProducerKind};
    assert_eq!(std::env::var("OUTPACE_TEST_SRT_FFMPEG").as_deref(), Ok("1"));
    let artifacts = Artifacts::new();
    std::io::Write::write_all(&mut artifacts.file("key"), b"synthetic-loopback-passphrase")
        .unwrap();
    let policy = PeerPolicy::from_private_peers(Some("127.0.0.1:1")).unwrap();
    let registry = BroadcastRegistry::with_persist(
        &artifacts.path("origin"),
        crate::config::CacheType::Memory,
        artifacts.path("cache"),
    );
    let state = BroadcastState {
        peer_policy: policy.clone(),
        registry: registry.clone(),
        seed_registry: SeedRegistry::new(),
        trackers: vec![],
        store_bytes: 4 << 20,
        inbound_peer_port: watch::channel(None).1,
    };
    let prepared = SrtConfig {
        bind: Some("127.0.0.1:0".parse().unwrap()),
        max_connections: 1,
        passphrase_file: Some(artifacts.path("key")),
        ..SrtConfig::default()
    }
    .prepare(true)
    .unwrap();
    let mut service = SrtService::start(prepared, state.clone())
        .await
        .unwrap()
        .unwrap();
    let address = service.local_addr().unwrap();
    let observation = async {
        let first_publisher = publish(&artifacts, address, "synthetic-reconnect", "first.log")
            .map_err(|e| e.to_string())?;
        let first = wait_piece(&registry, "synthetic-reconnect", 2).await?;
        check_signature(&first, 0).await?;
        let identity = first.transport_bytes.clone();
        drop(first_publisher);
        // Observe the public claim boundary rather than sleeping or blindly retrying
        // publishers. This zero-byte probe does not open an ingest or advance a cursor.
        // Abrupt process death has no FIN: allow the configured 5s peer-idle
        // timeout plus SDK garbage collection and a finite scheduling margin.
        let resumed_at = tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                match state
                    .acquire_producer("synthetic-reconnect", ProducerKind::RawPut)
                    .await
                {
                    Ok((broadcast, probe)) => {
                        let next = broadcast.cursor.start_piece();
                        drop(probe);
                        if broadcast.cursor.start_piece() != next {
                            return Err("zero-byte probe changed cursor".to_owned());
                        }
                        return Ok(next);
                    }
                    Err(BroadcastError::ProducerConflict) => {
                        tokio::time::sleep(Duration::from_millis(5)).await
                    }
                    Err(error) => return Err(error.code().to_owned()),
                }
            }
        })
        .await
        .map_err(|_| "physical retirement/claim deadline")??;
        if resumed_at < 3 {
            return Err("first connection did not advance cursor".into());
        }
        let second_publisher = publish(&artifacts, address, "synthetic-reconnect", "second.log")
            .map_err(|e| e.to_string())?;
        let second = wait_piece(&registry, "synthetic-reconnect", resumed_at + 2).await?;
        check_signature(&second, resumed_at).await?;
        if second.transport_bytes != identity {
            return Err("reconnect changed descriptor identity".into());
        }
        drop(second_publisher);
        service.shutdown().await.map_err(|e| e.to_string())?;
        registry.shutdown().await.map_err(|e| e.to_string())?;
        Ok::<_, String>((identity, second.cursor.reserved_until()))
    }
    .await;
    // Finish the first runtime even if observation failed.
    let stopped = service.shutdown().await;
    let registry_stopped = registry.shutdown().await;
    assert!(stopped.is_ok() && registry_stopped.is_ok());
    let (identity, previous_reserved_until) = observation
        .unwrap_or_else(|error| panic!("reconnect: {error}; artifacts={}", artifacts.0.display()));
    let reloaded = BroadcastRegistry::with_persist(
        &artifacts.path("origin"),
        crate::config::CacheType::Memory,
        artifacts.path("cache"),
    );
    let seed = SeedRegistry::new();
    let loaded = reloaded
        .reload_persisted(&seed, 4 << 20, &Default::default())
        .await
        .unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].transport_bytes, identity);
    let restart_at = loaded[0].cursor.start_piece();
    assert!(
        restart_at >= previous_reserved_until,
        "restart cannot reuse consumed/reserved indexes"
    );
    let state = BroadcastState {
        peer_policy: policy,
        registry: reloaded.clone(),
        seed_registry: seed,
        trackers: vec![],
        store_bytes: 4 << 20,
        inbound_peer_port: watch::channel(None).1,
    };
    let prepared = SrtConfig {
        bind: Some(address),
        max_connections: 1,
        passphrase_file: Some(artifacts.path("key")),
        ..SrtConfig::default()
    }
    .prepare(true)
    .unwrap();
    let mut restarted = SrtService::start(prepared, state).await.unwrap().unwrap();
    let publisher = publish(&artifacts, address, "synthetic-reconnect", "restart.log").unwrap();
    let third = wait_piece(&reloaded, "synthetic-reconnect", restart_at + 1).await;
    drop(publisher);
    let stopped = restarted.shutdown().await;
    let registry_stopped = reloaded.shutdown().await;
    assert!(stopped.is_ok() && registry_stopped.is_ok());
    let third = third
        .unwrap_or_else(|error| panic!("restart: {error}; artifacts={}", artifacts.0.display()));
    assert_eq!(third.transport_bytes, identity);
    check_signature(&third, restart_at).await.unwrap();
}
